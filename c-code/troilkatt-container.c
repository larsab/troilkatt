/*
 * A simple container that ensures that an external program started by
 * troilkatt does not crash a machine by using too much memory. It can
 * also be used to limit the runtime for a program.
 *
 * The container works by using setrlimit to set the resource usage limits,
 * and then executing the program (using execve). The executed program will
 * inherit the resource usage limits.
 *
 * The container also has a concept of jobs. It is designed using two
 * rules. First only containers belonging to one job can be run at a time.
 * Any leftover containers from a previous jobs are killed. Second, a job
 * can only run a specified number of containers at once. For the
 * implementation the process information in /proc is used to find container
 * processes and check their job ID (provided as a command line argument).
 *
 * Usage: "./troilkatt-container maxSize maxTime program <args>", where
 * [1] maxSize:  the maximum allowed virtual memory size in GB. If -1 the
 *               operating systems default is used instead.
 * [2] maxTime:  the maximum allowed CPU time in seconds. If -1 the program
 *               can run indefinitely.
 * [3] maxProcs: maximum number of processes that can run at once. If maxProcs
 *               is N, and N containers are already running, then this container
 *               will not start.
 * [4] jobID:    a unique ID for a job. Container processes belonging to a job
 *               with a lower (as reported by strncmp) will be killed.
 * [5] program:  the executable of the external program to run.
 * [6...]:       optional arguments passed to the program.
 *
 * The program exits with a zero return value on success, and 2 in case of an
 * error.
 *
 * To compile use: gcc -Wall troilkatt-container.c -o troilkatt_container -lproc
 */

#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <proc/readproc.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>

/* 
 * Print error message and exit
 * If last character (except '\0' is ':' then print errno.
 * 
 * @param fmt: format string
 */
void eprintf(char *fmt, ...)
{
	va_list args;

	fflush(stdout);
	fprintf(stderr, "Error: ");

	va_start(args, fmt);
	vfprintf(stderr, fmt, args);
	va_end(args);

	if (fmt[0] != '\0' && fmt[strlen(fmt)-1] == ':') {
		fprintf(stderr, " %s", strerror(errno));
	}
	fprintf(stderr, "\n");

	exit(2); /* conventional value for failed execution */
}

/*
 * Set the maximum size of the process virtual memory size.
 *
 * @param size:  size in GB
 * @return none: but the program crash if an error occurs.
 */
void setVMLimit(long long maxSize) {
	struct rlimit memoryLimit;

	if (getrlimit(RLIMIT_AS, &memoryLimit) != 0) {
		eprintf("Could not get memory limit:");
	}

	memoryLimit.rlim_cur = maxSize * 0x40000000LL;

	if (setrlimit(RLIMIT_AS, &memoryLimit) != 0) {
		eprintf("Could not set memory limit:");
	}
	printf("Virtual memory size limited to: %lld GB\n", maxSize);
}

/*
 * Set a CPU time limit
 *
 * @param time: CPU time in seconds
 * @return none: nit the program crash if an error occurs.
 */
void setCPULimit(long long maxTime) {
	struct rlimit cpuLimit;

	if (getrlimit(RLIMIT_CPU, &cpuLimit) != 0) {
		eprintf("Could not get CPU limit:");
	}

	cpuLimit.rlim_cur = maxTime;

	if (setrlimit(RLIMIT_CPU, &cpuLimit) != 0) {
		eprintf("Could not set CPU limit:");
	}
	printf("Maximum CPU time limited to %lld seconds\n", maxTime);
}

/*
 * Check if string a ends with string b
 *
 * Return 1 if string a ends with string b, and 0 otherwise.
 */
int endsWith(const char *a, const char *b) {
	int i;
	int aLen = strlen(a);
	int bLen = strlen(b);

	if (aLen < bLen) {
		// cannot be a troilkatt_container process
		return 0;
	}
	for (i = 1; i <= bLen; i++) {
		if (a[aLen - i] != b[bLen - i]) {
			return 0;
		}
	}

	// a ends with b
	return 1;
}


/*
 * Kill any containers that belong to a previous job. Also returns the number
 * of processes that belong to this job. This number includes this process.
 */
int killPrevious(char *jobID)
{
	const char *bin_name = "troilkatt_container";
	const int jobIDIndex = 4;

	// Number of processes belonging to this job
	int jobProcs = 0;

	// Get handle to /proc info
	PROCTAB* proc = openproc(PROC_FILLSTATUS | PROC_FILLCOM);

	// Parse retrieved inforamation for each of the currently running processes
	proc_t proc_info;
	memset(&proc_info, 0, sizeof(proc_info));
	while (readproc(proc, &proc_info) != NULL) { // more processes to parse


		/*
		 * Traverse command line arguments
		 */
		int currentArg = 0;
		char **cp = proc_info.cmdline;
		while((cp != NULL) && (*cp != NULL)) {
			if (currentArg == 0) {
				/* Parse executable name to find troilkatt containers
				 * This is done by comparing the characters in reverse order to the
				 * troilatt binary name
				 */
				if (! endsWith(*cp, bin_name)) { // not a troilkatt container
					break;
				}
			}
			if (currentArg == jobIDIndex) {
				int rv = strncmp(*cp, jobID, strlen(jobID));
				if (rv < 0) {
					// container belongs to a previous job
					printf("Killing container belonging to job: %s\n", *cp);
					if (kill(proc_info.tid, 9)) {
						eprintf("Could not kill troilkatt-container with pid: %d\n", proc_info.tid);
					}
				}
				else if (rv > 0) {
					eprintf("A newer job has been started...killing this container");
				}
				else {
					jobProcs++;
				}
			}

			*cp++;
			currentArg++;
		} // while more command line arguments
	} // while more processes
	closeproc(proc);

	return jobProcs;
}

/**
 * Command line arguments:
 *
 * [1] maxVM:      the maximum allowed virtual memory size in GB. If -1 the
 *                 operating systems default is used instead.
 * [2] maxTime:    the maximum allowed CPU time in seconds. If -1 the program
 *                 can run indefinitely.
 * [3] maxProcs:   maximum number of processes that can run at once. If maxProcs
 *                 is N, and N containers are already running, then this container
 *                 will not start.
 * [4] jobID:      a unique ID for a job. Container processes belonging to a job
 *                 with a lower (as reported by strncmp) will be killed.
 * [5] executable: the executable of the external program to run.
 * [6...] args:    optional arguments passed to the program.
 */
int main(int argc, char *argv[], char *envp[]) {

	if (argc < 6) {
		eprintf("Usage: %s maxVM maxTime maxProcs jobID executable args", argv[0]);
	}

	sleep(3);

	char *jobID = argv[4];
	// Kill containers from previous jobs and get number of running containers for this
	// job
	int runningProcs = killPrevious(jobID);
	printf("%d troilkatt containers are running\n", runningProcs);
	
	// Make sure too many containers have not been started
	int maxProcs = atoi(argv[3]);
	if (maxProcs < 1) {
		eprintf("Invalid maxProcs value: %d", maxProcs);
	}
	if (runningProcs > maxProcs) {
		eprintf("Too many containers started for job...killing this container");
	}

	// Set maximum virtual memory size for container
	long long maxVM = atoll(argv[1]);
	if (maxVM > 0) {
		setVMLimit(maxVM);
	}
	else if (maxVM == -1) {
		printf("Maximum virtual memory size is not limited\n");
	}
	else {
		eprintf("Invalid maximum virtual memory size: %s\n", argv[1]);
	}

	// Set maximum execution time
	long long maxTime = atoll(argv[2]);
	if (maxTime > 0) {
		setCPULimit(maxTime);
	}
	else if (maxTime == -1) {
		printf("Maximum CPU time is not limited\n");
	}
	else {
		eprintf("Invalid maximum CPU time: %s\n", argv[2]);
	}

	pid_t tid = fork();
	if (tid == 0) { // is child
		printf("Executing: %s", argv[5]);
		int i = 0;
		for (i = 6; i < argc; i++) {
			printf(" %s", argv[i]);
		}
		printf("\n");
		if (execve(argv[5], &argv[5], envp) == -1) {
			eprintf("Could not start program: %s:", argv[5]);
		}
	}
	else { // is parent
		// Wait until child is done
		int status;
		wait(&status);
		return status;

	}
	return 0;
}
