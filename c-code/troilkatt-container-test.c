#include <stdio.h>
#include <stdlib.h>

int main(int argc, char *argv[]) {

	int i = 0;
	printf("My args: ");
	for (i = 0; i < argc; i++) {
		printf("%s,", argv[i]);
	}
	printf("\n");

	void *buf = malloc(0x20000000LL); // 0.5 GB
	if (buf == NULL) {
		fprintf(stderr, "Could not allocate 1GB of memory\n");
	}

	void *buf2 = malloc(0x40000000LL); // 1 GB
	if (buf2 == NULL) {
		fprintf(stderr, "Failed as expected\n");
	}
	else {
		fprintf(stderr, "Malloc did not fail\n");
	}

	while (1) ;

	return 0;
}
