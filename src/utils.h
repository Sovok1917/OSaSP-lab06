// src/utils.h
#ifndef UTILS_H
#define UTILS_H

#include <stddef.h> // For size_t
#include "common.h" // For index_record_t

// Function to get the number of online processors
long get_core_count(void);

// Function to check if a number is a power of two
int is_power_of_two(unsigned int n);

// Function to get the integer part of MJD for "yesterday"
double get_mjd_yesterday_int_max(void);

// Comparison function for index_record_t, for qsort and merging
int compare_index_records(const void* a, const void* b);

// Helper to print usage and exit
void print_usage_and_exit(const char* prog_name, const char* usage_message, int exit_code);

// Helper to print error and exit
void error_exit(const char* message);

// Helper to print error with errno and exit
void errno_exit(const char* message);


#endif // UTILS_H
