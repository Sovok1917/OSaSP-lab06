# Readme.txt
Index File Processing Utilities (gen, view, sort_index, verify_sorted)
======================================================================

This project provides a suite of command-line tools for generating, viewing,
sorting, and verifying binary index files. The index files consist of a header
indicating the total number of records, followed by a series of index records.
Each index record contains a time mark (as a Modified Julian Date) and a record
number.

The `sort_index` program is a multi-threaded application that can sort large
index files efficiently by processing them in memory-mapped chunks and then
performing a k-way merge if the file is processed in multiple chunks.

Project Components:
-------------------
1.  gen:            Generates a new binary index file with a specified number of
                    randomly generated, unsorted index records.
2.  view:           Reads an existing binary index file and prints its header
                    information and all its records to standard output in a
                    human-readable format.
3.  sort_index:     Sorts an existing binary index file in place (by creating a
                    temporary sorted file and then replacing the original).
                    It uses memory-mapped I/O to process the file in chunks.
                    Each chunk is sorted in memory using multiple threads. If the
                    file is processed in multiple chunks, a final k-way merge
                    is performed.
4.  verify_sorted:  Checks if a given binary index file is correctly sorted by
                    time_mark and if the number of records matches its header.
                    Exits with 0 on success, 1 on failure.
5.  utils:          A common utility module providing helper functions for error
                    handling, system information, mathematical checks, and MJD
                    calculations.
6.  common.h:       Defines common data structures used across the programs.

Build Instructions:
-------------------
The project uses a Makefile for building. Source code is expected in the `src/`
directory. Build artifacts will be placed in `build/debug/` or `build/release/`.

1.  Build Debug Version (Default):
    make
    or
    make MODE=debug
    Executables: `build/debug/gen`, `build/debug/view`, `build/debug/sort_index`, `build/debug/verify_sorted`

2.  Build Release Version:
    make MODE=release
    Executables: `build/release/gen`, `build/release/view`, `build/release/sort_index`, `build/release/verify_sorted`

3.  Clean Build Artifacts:
    make clean
    This removes the entire `build/` directory.

Program Usage:
--------------
Note: Replace `build/debug/` with `build/release/` if you built in release mode.

**1. gen (Generate Index File)**
   Usage: ./build/debug/gen <num_records> <output_filename>
   Example: ./build/debug/gen 10240 index_file.dat

**2. view (View Index File)**
   Usage: ./build/debug/view <filename>
   Example: ./build/debug/view index_file.dat

**3. sort_index (Sort Index File)**
   Usage: ./build/debug/sort_index <memsize> <blocks> <threads> <filename>
   Example: ./build/debug/sort_index 67108864 16 4 index_file.dat

**4. verify_sorted (Verify Sorted Index File)**
   Usage: ./build/debug/verify_sorted <filename>
   Example: ./build/debug/verify_sorted index_file.dat
   (Prints success or error messages and exits 0 if valid, 1 otherwise)

Testing:
--------
A test script `test_suite.sh` is provided to automate testing of the utilities.
It requires the programs to be built first.

To run the tests (e.g., for debug mode):
  chmod +x test_suite.sh
  ./test_suite.sh debug

To run for release mode:
  ./test_suite.sh release

The script will output PASS/FAIL for various scenarios, including multi-threaded
sorting and k-way merge conditions. It uses the `verify_sorted` utility for
checking correctness.

File Format & Sorting Logic:
----------------------------
(Content from your original Readme.txt regarding File Format and Sorting Logic
 can be retained here.)
...

Notes:
------
(Content from your original Readme.txt regarding Notes can be retained here.)
...
