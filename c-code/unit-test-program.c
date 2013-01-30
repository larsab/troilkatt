/* The test program is a simple program that takes N arguments. The first
 * is the total number of arguments, the second is the programs return value,
 * and the following can be anything (they are ignored) */

#include <stdio.h>
#include <stdlib.h>

int main(int argc, char *argv[]) {
    if (argc < 3) {
	fprintf(stderr, "Usage: %s argc rv\n", argv[0]);
	exit(-2);
    }

    int nArgs = atoi(argv[1]);
    if (nArgs != argc - 1) {
	fprintf(stderr, "Invalid argument count %d (specified) != %d (actual)",
		nArgs, argc - 1);
	exit(-1);
    }
	
    int rv = atoi(argv[2]);

    return rv;
}
