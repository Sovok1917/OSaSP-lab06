// src/utils.c
/*
 * Utility functions for the sort_index project.
 * Includes core count retrieval, math helpers, MJD calculation,
 * and error handling.
 */
#define _POSIX_C_SOURCE 200809L // For getopt, sysconf, etc.
#include "utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h> // For sysconf, getpagesize
#include <math.h>   // For floor
#include <time.h>   // For time, gmtime_r

/**
 * Purpose: Gets the number of online processor cores.
 * Returns: The number of online cores, or 1 if an error occurs or detection fails.
 */
long get_core_count(void) {
    long nprocs = sysconf(_SC_NPROCESSORS_ONLN);
    if (nprocs < 1) {
        fprintf(stderr, "Warning: Could not determine number of cores, defaulting to 1.\n");
        return 1;
    }
    return nprocs;
}

/**
 * Purpose: Checks if a number is a power of two.
 * Recieves: n The number to check.
 * Returns: 1 if n is a power of two, 0 otherwise.
 */
int is_power_of_two(unsigned int n) {
    return (n > 0) && ((n & (n - 1)) == 0);
}

/**
 * Purpose: Calculates the maximum integer part of the Modified Julian Date for "yesterday".
 *        The timestamp's integer part should be generated up to this value.
 * Returns: The floor of (MJD of current time - 1 day).
 */
double get_mjd_yesterday_int_max(void) {
    time_t current_seconds_since_epoch;
    // Corrected line:
    if (time(&current_seconds_since_epoch) == (time_t)-1) {
        errno_exit("Failed to get current time");
    }

    // MJD for current time: MJD of epoch + days since epoch
    double current_mjd = MJD_1970_01_01 + (double)current_seconds_since_epoch / 86400.0;

    // MJD for "yesterday"
    double yesterday_mjd = current_mjd - 1.0;

    return floor(yesterday_mjd);
}

/**
 * Purpose: Compares two index_record_t structures based on their time_mark.
 *        Suitable for use with qsort or bsearch.
 * Recieves: a Pointer to the first index_record_t.
 * Recieves: b Pointer to the second index_record_t.
 * Returns: -1 if a < b, 0 if a == b, 1 if a > b.
 */
int compare_index_records(const void* a, const void* b) {
    const index_record_t* rec_a = (const index_record_t*)a;
    const index_record_t* rec_b = (const index_record_t*)b;

    if (rec_a->time_mark < rec_b->time_mark) {
        return -1;
    }
    if (rec_a->time_mark > rec_b->time_mark) {
        return 1;
    }
    // If time_marks are equal, you could define a secondary sort key (e.g., recno)
    // For this problem, equality is fine.
    return 0;
}

/**
 * Purpose: Prints a usage message and exits the program.
 * Recieves: prog_name The name of the program (argv[0]).
 * Recieves: usage_message The specific usage string for the program.
 * Recieves: exit_code The exit code to use.
 */
void print_usage_and_exit(const char* prog_name, const char* usage_message, int exit_code) {
    fprintf(stderr, "Usage: %s %s\n", prog_name, usage_message);
    exit(exit_code);
}

/**
 * Purpose: Prints a custom error message to stderr and exits with EXIT_FAILURE.
 * Recieves: message The error message to print.
 */
void error_exit(const char* message) {
    fprintf(stderr, "Error: %s\n", message);
    exit(EXIT_FAILURE);
}

/**
 * Purpose: Prints a custom error message followed by the system error message
 *        (from errno) to stderr, then exits with EXIT_FAILURE.
 * Recieves: message The custom part of the error message.
 */
void errno_exit(const char* message) {
    fprintf(stderr, "Error: %s: %s\n", message, strerror(errno));
    exit(EXIT_FAILURE);
}
