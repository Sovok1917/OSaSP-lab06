#ifndef COMMON_H
#define COMMON_H

#include <stdint.h> // For uint64_t
#include <time.h>   // For time_t in MJD calculations

// Modified Julian Date for 1900-01-01 00:00:00.0 UTC
#define MJD_1900_01_01 (15020.0)
// Modified Julian Date for 1970-01-01 00:00:00.0 UTC (Unix epoch)
#define MJD_1970_01_01 (40587.0)

// Structure for an index record
struct index_s {
    double time_mark; // Timestamp (Modified Julian Date)
    uint64_t recno;   // Record number in DB table (primary index)
};
typedef struct index_s index_record_t;

// Structure for the index file header
struct index_hdr_s {
    uint64_t records;     // Total number of records in the file
    index_record_t idx[]; // Flexible array member for records (data starts here)
};
typedef struct index_hdr_s index_hdr_t;

#endif // COMMON_H
