Index File Processing Utilities (gen, view, sort_index)
=======================================================

This project provides a suite of command-line tools for generating, viewing,
and sorting binary index files. The index files consist of a header indicating
the total number of records, followed by a series of index records. Each
index record contains a time mark (as a Modified Julian Date) and a record
number.

The `sort_index` program is a multi-threaded application that can sort large
index files efficiently by processing them in memory-mapped chunks and then
performing a k-way merge if the file is processed in multiple chunks.

Project Components:
-------------------
1.  gen:          Generates a new binary index file with a specified number of
                  randomly generated, unsorted index records.
2.  view:         Reads an existing binary index file and prints its header
                  information and all its records to standard output in a
                  human-readable format.
3.  sort_index:   Sorts an existing binary index file in place (by creating a
                  temporary sorted file and then replacing the original).
                  It uses memory-mapped I/O to process the file in chunks
                  (if the file is larger than the specified memory size).
                  Each chunk is sorted in memory using multiple threads. If the
                  file is processed in multiple chunks, a final k-way merge
                  is performed to produce the fully sorted file.
4.  utils:        A common utility module providing helper functions for error
                  handling, system information (like core count), mathematical
                  checks (power of two), and Modified Julian Date (MJD)
                  calculations.
5.  common.h:     Defines common data structures used across the programs,
                  specifically `index_record_t` and `index_hdr_t`.

Build Instructions:
-------------------
The project uses a Makefile for building. Source code is expected in the `src/`
directory, and build artifacts will be placed in the `build/` directory.

1.  Build Debug Version (Default):
    make
    This builds all three executables (`gen`, `view`, `sort_index`) with
    debugging symbols. Executables will be in `build/gen`, `build/view`,
    and `build/sort_index` (this seems to be an error in the Makefile,
    they should all go into a common `build/debug` or `build/release` dir,
    but following the provided Makefile structure for this readme).
    Correction: The Makefile actually places them in `build/gen`, `build/view`,
    `build/sort_index` directly under `build/` if `OUT_DIR` is not used for targets.
    Assuming `OUT_DIR` is not used for final executables based on target definitions.
    Let's assume the Makefile intends for executables to be directly in `build/`.

    Executables: `build/gen`, `build/view`, `build/sort_index`

2.  Build Release Version:
    (Treats warnings as errors and applies optimizations)
    make MODE=release
    Executables: `build/gen`, `build/view`, `build/sort_index`

3.  Clean Build Artifacts:
    make clean
    This removes the entire `build/` directory.

Program Usage:
--------------

**1. gen (Generate Index File)**
   Creates an unsorted binary index file.
   Usage:
     ./build/gen <num_records> <output_filename>

   Arguments:
     <num_records>:     Total number of index records to generate.
                        Must be a positive integer and a multiple of 256.
     <output_filename>: The name of the binary index file to create.

   Example:
     ./build/gen 10240 index_file.dat

**2. view (View Index File)**
   Displays the content of a binary index file.
   Usage:
     ./build/view <filename>

   Arguments:
     <filename>: The name of the binary index file to view.

   Example:
     ./build/view index_file.dat

**3. sort_index (Sort Index File)**
   Sorts a binary index file using multiple threads and memory mapping.
   Usage:
     ./build/sort_index <memsize> <blocks> <threads> <filename>

   Arguments:
     <memsize>:  The size of the memory chunk (in bytes) to use for sorting.
                 Must be a multiple of the system page size.
                 The total file data size must be a multiple of this memsize.
     <blocks>:   The number of blocks to divide each memory chunk into for
                 parallel sorting. Must be a power of two.
                 Must be at least 4 times the number of threads.
     <threads>:  The total number of threads to use for sorting each chunk
                 (including the main thread's participation).
                 Recommended range: k to 8k, where k is the number of CPU cores.
     <filename>: The name of the binary index file to sort. The file will be
                 sorted in place (original is replaced by sorted version).

   Example:
     ./build/sort_index 67108864 16 4 index_file.dat
     (Sorts using 64MB chunks, 16 blocks per chunk, 4 threads)

File Format:
------------
The binary index files generated and processed by these tools have a simple structure:
1.  Header: A single `uint64_t` value representing the total number of records
    in the file.
2.  Records: A sequence of `index_record_t` structures. Each record contains:
    - `time_mark` (double): A timestamp represented as a Modified Julian Date.
    - `recno` (uint64_t): A record number (primary index).

Sorting Logic (`sort_index`):
-----------------------------
-   The file is processed in chunks of `memsize`.
-   Each chunk is memory-mapped.
-   The chunk is divided into `blocks` number of smaller blocks.
-   These smaller blocks are initially sorted by available threads using `qsort`.
-   A multi-pass merge sort is then performed by the threads within the chunk
    until the entire chunk is sorted in memory.
-   Synchronization between threads for different phases (block sorting, merge passes)
    is managed using POSIX barriers and mutexes.
-   If the entire file was processed in a single chunk (file size <= memsize),
    the sorted chunk is written back, and the process is complete.
-   If multiple chunks were processed (file size > memsize), each chunk is sorted
    and written back to its place in the original file. After all chunks are
    individually sorted, a final k-way merge is performed:
    - A temporary file is created.
    - The sorted chunks from the original file are read and merged into this
      temporary file using a min-heap to efficiently find the next record.
    - Once the k-way merge is complete, the temporary file (now fully sorted)
      replaces the original input file.

Notes:
------
-   The `gen` program generates time marks (MJD) with an integer part ranging
    from MJD_1900_01_01 up to "yesterday" relative to the system's current time.
-   The `sort_index` program links with `-pthread` for POSIX threads and `-lm`
    for math functions (e.g., `floor` used in MJD calculations in `utils.c`).
-   Error handling is implemented using custom helper functions (`error_exit`,
    `errno_exit`) and standard `perror`.
-   Memory allocation failures in non-interactive programs (`gen`, `sort_index`'s
    main setup) will typically call `abort()`.
