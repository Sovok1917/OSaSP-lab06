// src/view.c
/*
 * Program to view the contents of an index file.
 * Reads the header and all records, printing them to stdout.
 */
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h> // Added for strerror
#include "common.h"
#include "utils.h"

/**
 * Purpose: Main function for the 'view' program.
 * Recieves: argc Argument count.
 * Recieves: argv Argument vector. Expects: <filename>
 * Returns: 0 on success, 1 on failure.
 */
int main(int argc, char *argv[]) {
    if (argc != 2) {
        print_usage_and_exit(argv[0], "<filename>", 1);
    }
    const char* filename = argv[1];

    FILE* fp = fopen(filename, "rb");
    if (!fp) {
        errno_exit("Failed to open input file for reading");
    }

    // Read header
    index_hdr_t header_base; // Read only the fixed part of header
    if (fread(&header_base, sizeof(index_hdr_t) - sizeof(index_record_t*), 1, fp) != 1) {
        fclose(fp);
        errno_exit("Failed to read header from file");
    }

    printf("File: %s\n", filename);
    printf("Total records in header: %lu\n", (unsigned long)header_base.records);
    printf("----------------------------------------\n");
    printf("%-20s | %s\n", "Time Mark (MJD)", "Record Number");
    printf("----------------------------------------\n");

    index_record_t* record_buffer = malloc(sizeof(index_record_t));
    if (!record_buffer) {
        abort(); // Per requirement
    }

    for (uint64_t i = 0; i < header_base.records; ++i) {
        if (fread(record_buffer, sizeof(index_record_t), 1, fp) != 1) {
            if (feof(fp)) {
                fprintf(stderr, "Error: Unexpected end of file. Expected %lu records, found %lu.\n",
                        (unsigned long)header_base.records, (unsigned long)i);
            } else {
                // strerror is now correctly declared
                fprintf(stderr, "Error reading record %lu: %s\n", (unsigned long)i, strerror(errno));
            }
            free(record_buffer);
            fclose(fp);
            exit(EXIT_FAILURE);
        }
        printf("%-20.10f | %lu\n", record_buffer->time_mark, (unsigned long)record_buffer->recno);
    }

    // Check if there's more data than indicated by header (optional check)
    char dummy;
    if (fread(&dummy, 1, 1, fp) == 1) {
        fprintf(stderr, "Warning: File contains more data beyond declared records.\n");
    }


    free(record_buffer);
    if (fclose(fp) != 0) {
        errno_exit("Failed to close input file");
    }

    return 0;
}
