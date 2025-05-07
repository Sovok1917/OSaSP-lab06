// src/gen.c
/*
 * Program to generate an unsorted index file.
 * The file consists of a header followed by index records.
 */
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include "common.h"
#include "utils.h"

/**
 * Purpose: Main function for the 'gen' program.
 * Recieves: argc Argument count.
 * Recieves: argv Argument vector. Expects: <num_records> <filename>
 * Returns: 0 on success, 1 on failure.
 */
int main(int argc, char *argv[]) {
    if (argc != 3) {
        print_usage_and_exit(argv[0], "<num_records> <filename>", 1);
    }

    char* endptr;
    long long num_records_ll = strtoll(argv[1], &endptr, 10);
    if (*endptr != '\0' || num_records_ll <= 0) {
        error_exit("Invalid number of records. Must be a positive integer.");
    }
    if (num_records_ll % 256 != 0) {
        error_exit("Number of records must be a multiple of 256.");
    }
    uint64_t num_records = (uint64_t)num_records_ll;
    const char* filename = argv[2];

    FILE* fp = fopen(filename, "wb");
    if (!fp) {
        errno_exit("Failed to open output file for writing");
    }

    // Initialize random seed
    srand(time(NULL));

    // Prepare header
    index_hdr_t header;
    header.records = num_records;

    // Write header
    if (fwrite(&header, sizeof(index_hdr_t) - sizeof(index_record_t*), 1, fp) != 1) { // Write only fixed part
        fclose(fp);
        errno_exit("Failed to write header to file");
    }

    double max_mjd_int_part = get_mjd_yesterday_int_max();
    if (max_mjd_int_part < MJD_1900_01_01) {
        fclose(fp);
        error_exit("Calculated max MJD for generation is less than min MJD. Check system time.");
    }

    index_record_t* record_buffer = malloc(sizeof(index_record_t));
    if (!record_buffer) {
        abort(); // Per requirement for non-interactive malloc failure
    }

    printf("Generating %lu records for file '%s'...\n", (unsigned long)num_records, filename);
    printf("MJD range: Integer part from %.1f to %.1f\n", MJD_1900_01_01, max_mjd_int_part);

    for (uint64_t i = 0; i < num_records; ++i) {
        // Generate time_mark
        long rand_int_range = (long)(max_mjd_int_part - MJD_1900_01_01 + 1);
        double int_part = MJD_1900_01_01 + (double)(rand() % rand_int_range);
        double frac_part = (double)rand() / (double)RAND_MAX;
        record_buffer->time_mark = int_part + frac_part;

        // Generate recno (sequential for simplicity)
        record_buffer->recno = i + 1;

        if (fwrite(record_buffer, sizeof(index_record_t), 1, fp) != 1) {
            free(record_buffer);
            fclose(fp);
            errno_exit("Failed to write record to file");
        }
    }

    free(record_buffer);
    if (fclose(fp) != 0) {
        errno_exit("Failed to close output file");
    }

    printf("Successfully generated %lu records into %s.\n", (unsigned long)num_records, filename);
    return 0;
}
