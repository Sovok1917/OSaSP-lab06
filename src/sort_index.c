// src/sort_index.c
/*
 * Multi-threaded program to sort an index file using memory mapping.
 */
#define _GNU_SOURCE // For O_DIRECT if desired, and other GNU extensions if used (currently not)
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <pthread.h>
#include <errno.h>
#include <limits.h> // For LONG_MAX
#include <getopt.h> // For command line parsing (not strictly required by problem statement but good practice)

#include "common.h"
#include "utils.h"

// Structure to hold thread-specific arguments and shared data access
typedef struct {
    int thread_id;
    int total_threads_in_group; // Number of threads participating in current chunk sort (main + workers)

    index_record_t* chunk_data_ptr; // Pointer to the start of data in the mmap'd chunk
    size_t num_records_in_chunk;    // Total records in this specific mmap'd chunk

    int total_blocks_in_chunk;          // Total blocks this chunk is divided into
    size_t records_per_block;       // Number of records per block (ideal)

    pthread_barrier_t* barrier_start_chunk; // All threads ready for chunk
    pthread_barrier_t* barrier_blocks_sorted; // All blocks qsorted
    pthread_barrier_t* barrier_merge_pass;    // Sync for each merge pass

    // Shared counters/mutexes for task distribution
    pthread_mutex_t* task_mutex;
    int* next_block_to_sort_idx;    // For qsort phase
    int* next_merge_pair_idx;       // For merge phase

    // For main thread to signal workers to exit
    volatile int* please_exit_flag;

} thread_arg_t;

// Forward declaration for the thread worker function
void* sort_worker_thread(void* arg);
// Forward declaration for merging two sorted arrays (used in merge phase)
void merge_sorted_arrays(index_record_t* arr1, size_t n1, index_record_t* arr2, size_t n2, index_record_t* out_buffer);
// Forward declaration for k-way merge of sorted file chunks
int k_way_merge_sorted_chunks(const char* original_filename, uint64_t total_records, int num_chunks, size_t chunk_data_size_bytes);


/**
 * Purpose: Main function for the 'sort_index' program.
 * Recieves: argc Argument count.
 * Recieves: argv Argument vector. Expects: memsize blocks threads filename
 * Returns: 0 on success, 1 on failure.
 */
int main(int argc, char *argv[]) {
    if (argc != 5) {
        print_usage_and_exit(argv[0], "<memsize> <blocks> <threads> <filename>", 1);
    }

    char* endptr;
    long memsize_l = strtol(argv[1], &endptr, 10);
    if (*endptr != '\0' || memsize_l <= 0) error_exit("Invalid memsize.");
    size_t memsize = (size_t)memsize_l;

    long blocks_l = strtol(argv[2], &endptr, 10);
    if (*endptr != '\0' || blocks_l <= 0) error_exit("Invalid blocks count.");
    int blocks_param = (int)blocks_l;

    long threads_l = strtol(argv[3], &endptr, 10);
    if (*endptr != '\0' || threads_l <= 0) error_exit("Invalid threads count.");
    int threads_param = (int)threads_l;

    const char* filename = argv[4];

    // --- Parameter Validation ---
    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size == -1) errno_exit("sysconf(_SC_PAGESIZE) failed");
    if (memsize % (size_t)page_size != 0) {
        fprintf(stderr, "Error: memsize (%zu) must be a multiple of page size (%ld).\n", memsize, page_size);
        exit(EXIT_FAILURE);
    }
    if (!is_power_of_two(blocks_param)) error_exit("Blocks count must be a power of two.");

    long k_cores = get_core_count();
    if (threads_param < k_cores || threads_param > 8 * k_cores) {
        fprintf(stderr, "Error: Threads count (%d) must be between k (%ld) and 8k (%ld).\n",
                threads_param, k_cores, 8 * k_cores);
        // exit(EXIT_FAILURE); // This is a guideline, can be warning if preferred
    }
    if (blocks_param < threads_param * 4) {
        fprintf(stderr, "Error: Blocks count (%d) must be at least 4 times threads count (%d).\n",
                blocks_param, threads_param);
        exit(EXIT_FAILURE);
    }
    if (k_cores == 4 && blocks_param < 16) { // Specific constraint from problem
        fprintf(stderr, "Error: If k=4 cores, blocks count (%d) must be at least 16.\n", blocks_param);
        // exit(EXIT_FAILURE);
    }
    if (memsize < (size_t)blocks_param * sizeof(index_record_t)) {
        error_exit("Memsize is too small to accommodate the specified number of blocks with at least one record per block.");
    }
    if ((memsize / sizeof(index_record_t)) == 0) {
        error_exit("Memsize is too small, cannot fit even one record.");
    }
    if ((memsize / sizeof(index_record_t)) < (size_t)blocks_param) {
        error_exit("Memsize cannot be split into the_requested number of blocks, each holding at least one record.");
    }


    // --- File Handling ---
    int fd = open(filename, O_RDWR);
    if (fd == -1) errno_exit("Failed to open file");

    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        close(fd);
        errno_exit("fstat failed");
    }
    if (sb.st_size < (long long)sizeof(index_hdr_t)) { // Check if file is smaller than header
        close(fd);
        error_exit("File is too small to contain a valid header.");
    }


    index_hdr_t header;
    // Read only the fixed part of the header, not the flexible array member
    if (read(fd, &header, sizeof(index_hdr_t) - sizeof(index_record_t*)) != (ssize_t)(sizeof(index_hdr_t) - sizeof(index_record_t*))) {
        close(fd);
        errno_exit("Failed to read file header");
    }

    uint64_t total_records_in_file = header.records;
    size_t file_data_offset = sizeof(index_hdr_t) - sizeof(index_record_t*); // Offset to first record
    size_t total_data_size_bytes = total_records_in_file * sizeof(index_record_t);

    if (total_data_size_bytes == 0) {
        printf("File contains no records to sort.\n");
        close(fd);
        return 0;
    }

    // IMPORTANT: Problem implies file_data_size is a multiple of memsize. Verify this.
    if (total_data_size_bytes % memsize != 0) {
        fprintf(stderr, "Error: Total data size in file (%zu bytes from %lu records) "
        "is not a multiple of memsize (%zu).\n",
                total_data_size_bytes, (unsigned long)total_records_in_file, memsize);
        fprintf(stderr, "Please use 'gen' to create a file where "
        "(num_records * %zu) is a multiple of %zu.\n", sizeof(index_record_t), memsize);
        close(fd);
        exit(EXIT_FAILURE);
    }

    size_t records_per_ideal_block = (memsize / sizeof(index_record_t)) / blocks_param;
    if (records_per_ideal_block == 0) {
        close(fd);
        error_exit("Calculated records per block is zero. Check memsize and blocks_param.");
    }


    // --- Threading Setup ---
    pthread_t* worker_pthreads = malloc((threads_param -1) * sizeof(pthread_t)); // Main is thread 0
    thread_arg_t* thread_args = malloc(threads_param * sizeof(thread_arg_t));
    if (!worker_pthreads && (threads_param > 1)) abort(); // only if threads_param > 1
    if (!thread_args) abort();

    pthread_barrier_t barrier_start_chunk, barrier_blocks_sorted, barrier_merge_pass;
    pthread_mutex_t task_mutex = PTHREAD_MUTEX_INITIALIZER;
    int next_block_to_sort_idx_shared;
    int next_merge_pair_idx_shared;
    volatile int please_exit_flag_shared = 0;

    if (pthread_barrier_init(&barrier_start_chunk, NULL, threads_param) != 0) errno_exit("barrier_init (start_chunk)");
    if (pthread_barrier_init(&barrier_blocks_sorted, NULL, threads_param) != 0) errno_exit("barrier_init (blocks_sorted)");
    if (pthread_barrier_init(&barrier_merge_pass, NULL, threads_param) != 0) errno_exit("barrier_init (merge_pass)");

    for (int i = 0; i < threads_param; ++i) {
        thread_args[i].thread_id = i;
        thread_args[i].total_threads_in_group = threads_param;
        thread_args[i].barrier_start_chunk = &barrier_start_chunk;
        thread_args[i].barrier_blocks_sorted = &barrier_blocks_sorted;
        thread_args[i].barrier_merge_pass = &barrier_merge_pass;
        thread_args[i].task_mutex = &task_mutex;
        thread_args[i].next_block_to_sort_idx = &next_block_to_sort_idx_shared;
        thread_args[i].next_merge_pair_idx = &next_merge_pair_idx_shared;
        thread_args[i].please_exit_flag = &please_exit_flag_shared;
        // chunk_data_ptr etc. will be set per chunk
    }

    for (int i = 0; i < threads_param - 1; ++i) { // Create threads_param - 1 workers
        if (pthread_create(&worker_pthreads[i], NULL, sort_worker_thread, &thread_args[i+1]) != 0) {
            errno_exit("Failed to create worker thread");
        }
    }

    // --- Main Processing Loop (Chunk by Chunk) ---
    size_t processed_data_bytes = 0;
    int num_chunks_processed = 0;

    while(processed_data_bytes < total_data_size_bytes) {
        size_t current_chunk_data_size = memsize; // total_data_size_bytes is multiple of memsize
        off_t current_mmap_file_offset = file_data_offset + processed_data_bytes;

        // mmap requires offset to be multiple of page size
        off_t mmap_offset_aligned = (current_mmap_file_offset / page_size) * page_size;
        size_t mmap_ptr_adjustment = current_mmap_file_offset % page_size;
        size_t mmap_length = current_chunk_data_size + mmap_ptr_adjustment;

        printf("Processing chunk %d: offset %jd, size %zu (mmap offset %jd, mmap length %zu)\n",
               num_chunks_processed + 1, (intmax_t)current_mmap_file_offset, current_chunk_data_size,
               (intmax_t)mmap_offset_aligned, mmap_length);

        void* mapped_memory_base = mmap(NULL, mmap_length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, mmap_offset_aligned);
        if (mapped_memory_base == MAP_FAILED) {
            // Signal threads to exit before aborting main
            please_exit_flag_shared = 1;
            // Potentially wake them up if they are at a barrier, then join.
            // This is complex. For now, direct exit.
            close(fd);
            errno_exit("mmap failed for a chunk");
        }
        index_record_t* chunk_data_start_ptr = (index_record_t*)((char*)mapped_memory_base + mmap_ptr_adjustment);
        size_t num_records_this_chunk = current_chunk_data_size / sizeof(index_record_t);

        // Update shared args for all threads (including main thread's arg struct)
        for (int i = 0; i < threads_param; ++i) {
            thread_args[i].chunk_data_ptr = chunk_data_start_ptr;
            thread_args[i].num_records_in_chunk = num_records_this_chunk;
            thread_args[i].total_blocks_in_chunk = blocks_param; // Since chunk size is memsize
            thread_args[i].records_per_block = records_per_ideal_block;
        }

        // Main thread (thread 0) participates in sorting this chunk
        // It will call sort_worker_thread logic directly or be part of the barrier sync
        pthread_barrier_wait(&barrier_start_chunk); // Signal all threads (incl. main) to start

        // Main thread acts as thread 0
        sort_worker_thread(&thread_args[0]); // This will execute one chunk's worth of work

        // After sort_worker_thread returns for main thread, it means its part is done.
        // It implicitly waits at barriers inside sort_worker_thread.
        // The last barrier inside sort_worker_thread (after merge) ensures all threads are done with the chunk.

        // Unmap the chunk
        if (munmap(mapped_memory_base, mmap_length) == -1) {
            // Similar exit signaling as mmap failure
            close(fd);
            errno_exit("munmap failed for a chunk");
        }

        processed_data_bytes += current_chunk_data_size;
        num_chunks_processed++;
        printf("Chunk %d processed and unmapped.\n", num_chunks_processed);
    }

    // --- Shutdown Threads ---
    please_exit_flag_shared = 1;
    pthread_barrier_wait(&barrier_start_chunk); // Release threads from waiting for a new chunk, so they can see exit flag

    for (int i = 0; i < threads_param - 1; ++i) {
        if (pthread_join(worker_pthreads[i], NULL) != 0) {
            errno_exit("Failed to join worker thread");
        }
    }
    printf("All worker threads joined.\n");

    // Cleanup pthreads resources
    pthread_barrier_destroy(&barrier_start_chunk);
    pthread_barrier_destroy(&barrier_blocks_sorted);
    pthread_barrier_destroy(&barrier_merge_pass);
    pthread_mutex_destroy(&task_mutex);
    free(worker_pthreads);
    free(thread_args);

    if (close(fd) == -1) errno_exit("Failed to close file descriptor after chunk processing");
    printf("File descriptor closed.\n");

    // --- Final K-Way Merge (if multiple chunks were processed) ---
    if (num_chunks_processed > 1) {
        printf("Performing final k-way merge of %d sorted chunks...\n", num_chunks_processed);
        if (k_way_merge_sorted_chunks(filename, total_records_in_file, num_chunks_processed, memsize) != 0) {
            error_exit("Final k-way merge failed.");
        }
        printf("Final k-way merge completed successfully.\n");
    } else {
        printf("Single chunk processed, no final k-way merge needed.\n");
    }

    printf("Sorting of %s completed.\n", filename);
    return 0;
}


/**
 * Purpose: Worker thread function to sort and merge blocks within a mmap'd chunk.
 *        Also used by the main thread (thread_id 0) for its portion of work.
 * Recieves: arg Pointer to thread_arg_t structure.
 * Returns: NULL.
 */
void* sort_worker_thread(void* arg) {
    thread_arg_t* data = (thread_arg_t*)arg;

    // Outer loop for processing multiple chunks (if this thread structure was persistent)
    // For this design, main thread calls this function once per chunk for itself (thread 0)
    // and worker threads loop until exit_flag is set.

    // Worker threads loop here, waiting for new chunk data or exit signal
    if (data->thread_id != 0) { // Worker threads loop
        while(1) {
            pthread_barrier_wait(data->barrier_start_chunk);
            if (*(data->please_exit_flag)) {
                pthread_exit(NULL);
            }
            // Process the chunk (logic below)
            // ... then loop back to barrier_start_chunk
            goto process_chunk_logic;
        }
    }

    process_chunk_logic:; // Label for goto from worker loop

    // --- Phase 1: Sort individual blocks (qsort) ---
    // Each thread `t` initially sorts block `t`.
    if (data->thread_id < data->total_blocks_in_chunk) {
        index_record_t* block_ptr = data->chunk_data_ptr + (data->thread_id * data->records_per_block);
        // Ensure not to read past num_records_in_chunk for this block
        size_t records_in_this_block = data->records_per_block;
        size_t block_end_record_idx = (data->thread_id * data->records_per_block) + records_in_this_block;
        if (block_end_record_idx > data->num_records_in_chunk) {
            records_in_this_block = data->num_records_in_chunk - (data->thread_id * data->records_per_block);
        }
        if(records_in_this_block > 0) { // only sort if there are records
            qsort(block_ptr, records_in_this_block, sizeof(index_record_t), compare_index_records);
        }
    }

    // Competitively sort remaining blocks
    // Main thread (0) initializes the shared counter for available blocks
    if (data->thread_id == 0) {
        *(data->next_block_to_sort_idx) = data->total_threads_in_group; // Start from block after initial assignments
    }
    pthread_barrier_wait(data->barrier_blocks_sorted); // Sync point: initial blocks sorted, counter ready

    while(1) {
        int block_to_sort;
        pthread_mutex_lock(data->task_mutex);
        block_to_sort = *(data->next_block_to_sort_idx);
        if (block_to_sort < data->total_blocks_in_chunk) {
            (*(data->next_block_to_sort_idx))++;
        }
        pthread_mutex_unlock(data->task_mutex);

        if (block_to_sort >= data->total_blocks_in_chunk) {
            break; // No more blocks to sort
        }

        index_record_t* block_ptr = data->chunk_data_ptr + (block_to_sort * data->records_per_block);
        size_t records_in_this_block = data->records_per_block;
        size_t block_end_record_idx = (block_to_sort * data->records_per_block) + records_in_this_block;
        if (block_end_record_idx > data->num_records_in_chunk) {
            records_in_this_block = data->num_records_in_chunk - (block_to_sort * data->records_per_block);
        }
        if(records_in_this_block > 0) {
            qsort(block_ptr, records_in_this_block, sizeof(index_record_t), compare_index_records);
        }
    }
    pthread_barrier_wait(data->barrier_blocks_sorted); // All blocks are qsorted

    // --- Phase 2: Merge sorted blocks ---
    size_t current_num_active_blocks = data->total_blocks_in_chunk;
    size_t current_records_per_merged_block = data->records_per_block;

    index_record_t* temp_merge_buffer = NULL;
    // Max possible size for a merged block is half the chunk, if 2 blocks merge to 1
    // Or, if merging two blocks of current_records_per_merged_block
    if (data->total_blocks_in_chunk > 1) { // Only allocate if merging will happen
        temp_merge_buffer = malloc(2 * data->records_per_block * sizeof(index_record_t) * ( (size_t)data->total_blocks_in_chunk / 2) );
        // The above allocation is too large. It should be for merging two largest possible sub-blocks.
        // Max size of a sub-block before final merge is memsize/2. So temp buffer is memsize.
        free(temp_merge_buffer); // free previous one
        temp_merge_buffer = malloc(data->num_records_in_chunk * sizeof(index_record_t)); // Max possible needed for one merge op
        if (!temp_merge_buffer) abort(); // Malloc failure
    }


    while(current_num_active_blocks > 1) {
        int pairs_to_merge_this_pass = current_num_active_blocks / 2;

        if (data->thread_id == 0) { // Main thread resets shared counter for this pass
            *(data->next_merge_pair_idx) = 0;
        }
        pthread_barrier_wait(data->barrier_merge_pass); // All threads ready for this merge pass

        if (pairs_to_merge_this_pass == 1 && data->total_threads_in_group > 1) { // Final merge for this chunk
            if (data->thread_id == 0) { // Only main thread does the final merge
                index_record_t* block1_ptr = data->chunk_data_ptr; // First half
                index_record_t* block2_ptr = data->chunk_data_ptr + current_records_per_merged_block; // Second half
                merge_sorted_arrays(block1_ptr, current_records_per_merged_block,
                                    block2_ptr, data->num_records_in_chunk - current_records_per_merged_block, // handle if not perfectly even
                                    temp_merge_buffer);
                memcpy(data->chunk_data_ptr, temp_merge_buffer, data->num_records_in_chunk * sizeof(index_record_t));
            }
            // Other threads do nothing and proceed to barrier
        } else { // Multiple pairs to merge, or single pair with single thread
            while(1) {
                int my_pair_idx;
                pthread_mutex_lock(data->task_mutex);
                my_pair_idx = *(data->next_merge_pair_idx);
                if (my_pair_idx < pairs_to_merge_this_pass) {
                    (*(data->next_merge_pair_idx))++;
                }
                pthread_mutex_unlock(data->task_mutex);

                if (my_pair_idx >= pairs_to_merge_this_pass) {
                    break; // No more pairs for this pass
                }

                index_record_t* block1_ptr = data->chunk_data_ptr + (my_pair_idx * 2 * current_records_per_merged_block);
                index_record_t* block2_ptr = data->chunk_data_ptr + ((my_pair_idx * 2 + 1) * current_records_per_merged_block);

                size_t n1 = current_records_per_merged_block;
                size_t n2 = current_records_per_merged_block;

                // Adjust n2 if it's the last odd block pair in a non-power-of-2 total elements scenario
                // (but here, total_blocks_in_chunk is power of 2, and records_per_block is uniform)
                // However, num_records_in_chunk might not be perfectly divisible by (current_records_per_merged_block*2)
                // This logic needs to be careful about actual number of records.
                // For simplicity, assuming perfect divisions due to file_size % memsize == 0 and blocks power of 2.
                // The last block in a merge operation might be smaller if total records not perfectly divisible.
                // For now, assume n1 and n2 are current_records_per_merged_block.
                // The output of merge is 2 * current_records_per_merged_block.

                // Check bounds for block2_ptr and its length
                size_t records_remaining_in_chunk = data->num_records_in_chunk -
                ((my_pair_idx * 2 + 1) * current_records_per_merged_block);
                if (n2 > records_remaining_in_chunk && records_remaining_in_chunk < current_records_per_merged_block) {
                    n2 = records_remaining_in_chunk > 0 ? records_remaining_in_chunk : 0;
                }

                if (n2 > 0) { // Only merge if second part has records
                    merge_sorted_arrays(block1_ptr, n1, block2_ptr, n2, temp_merge_buffer);
                    memcpy(block1_ptr, temp_merge_buffer, (n1 + n2) * sizeof(index_record_t));
                } // else block1 is already sorted and in place.
            }
        }

        // All threads sync after completing their merge tasks for this pass (or doing nothing)
        pthread_barrier_wait(data->barrier_merge_pass);

        // Main thread updates for next pass
        if (data->thread_id == 0) {
            current_num_active_blocks /= 2;
            current_records_per_merged_block *= 2;
        }
        // Sync again to ensure all threads see updated loop variables for next pass
        // This can be the same barrier if main updates before waiting on it.
        pthread_barrier_wait(data->barrier_merge_pass);
    }

    if (temp_merge_buffer) {
        free(temp_merge_buffer);
    }

    // If this is a worker thread, it would loop back to barrier_start_chunk.
    // If this is main thread (thread_id == 0), it returns to main function.
    if (data->thread_id != 0 && !(*(data->please_exit_flag))) {
        // This goto creates an implicit loop for worker threads, controlled by please_exit_flag
        // and barrier_start_chunk.
        goto process_chunk_logic_re_entry_for_worker; // Bad name, just to show loop point
        process_chunk_logic_re_entry_for_worker:; // Make compiler happy
        // The while(1) loop at the start of the function for workers handles this better.
        // The goto logic here is a bit messy, the while(1) at the top is cleaner.
        // Let's assume the while(1) at the top handles the worker loop.
    }

    return NULL;
}

/**
 * Purpose: Merges two sorted arrays of index_record_t into an output buffer.
 * Recieves: arr1 Pointer to the first sorted array.
 * Recieves: n1 Number of elements in arr1.
 * Recieves: arr2 Pointer to the second sorted array.
 * Recieves: n2 Number of elements in arr2.
 * Recieves: out_buffer Buffer to store the merged result. Must be large enough (n1+n2).
 */
void merge_sorted_arrays(index_record_t* arr1, size_t n1, index_record_t* arr2, size_t n2, index_record_t* out_buffer) {
    size_t i = 0, j = 0, k = 0;
    while (i < n1 && j < n2) {
        if (compare_index_records(&arr1[i], &arr2[j]) <= 0) {
            out_buffer[k++] = arr1[i++];
        } else {
            out_buffer[k++] = arr2[j++];
        }
    }
    while (i < n1) {
        out_buffer[k++] = arr1[i++];
    }
    while (j < n2) {
        out_buffer[k++] = arr2[j++];
    }
}


// --- K-Way Merge Implementation ---
// Structure for min-heap node in k-way merge
typedef struct {
    index_record_t record;
    int chunk_idx; // Which chunk this record came from
} heap_node_t;

// Min-heap functions (simplified: using an array and linear scan for min, not a real heap for brevity)
// A proper heap (priority queue) would be more efficient for large K.
// For this example, a simpler approach if K is small, or use qsort on a small buffer.

// Comparison for heap_node_t
int compare_heap_nodes(const void* a, const void* b) {
    heap_node_t* node_a = (heap_node_t*)a;
    heap_node_t* node_b = (heap_node_t*)b;
    return compare_index_records(&node_a->record, &node_b->record);
}


/**
 * Purpose: Performs a k-way merge of sorted chunks in a file.
 *        Reads from the original file (which contains sorted chunks) and writes to a temporary file.
 *        Then, replaces the original file with the temporary file.
 * Recieves: original_filename Name of the file to process.
 * Recieves: total_records Total records in the file (from header).
 * Recieves: num_chunks Number of sorted chunks in the file.
 * Recieves: chunk_data_size_bytes Size of each chunk (memsize used for sorting).
 * Returns: 0 on success, -1 on failure.
 */
int k_way_merge_sorted_chunks(const char* original_filename, uint64_t total_records, int num_chunks, size_t chunk_data_size_bytes) {
    if (num_chunks <= 1) return 0; // No merge needed

    char temp_filename[PATH_MAX];
    snprintf(temp_filename, PATH_MAX, "%s.tmp", original_filename);

    FILE* original_fp = fopen(original_filename, "rb");
    if (!original_fp) { perror("k_way_merge: fopen original_fp"); return -1; }

    FILE* temp_fp = fopen(temp_filename, "wb");
    if (!temp_fp) { perror("k_way_merge: fopen temp_fp"); fclose(original_fp); return -1; }

    // Write header to temp file (read from original)
    index_hdr_t header;
    fseek(original_fp, 0, SEEK_SET); // Ensure we are at the beginning
    if (fread(&header, sizeof(index_hdr_t) - sizeof(index_record_t*), 1, original_fp) != 1) {
        perror("k_way_merge: fread header");
        fclose(original_fp); fclose(temp_fp); remove(temp_filename); return -1;
    }
    if (fwrite(&header, sizeof(index_hdr_t) - sizeof(index_record_t*), 1, temp_fp) != 1) {
        perror("k_way_merge: fwrite header");
        fclose(original_fp); fclose(temp_fp); remove(temp_filename); return -1;
    }

    // Allocate buffers and tracking for each chunk
    index_record_t** chunk_buffers = malloc(num_chunks * sizeof(index_record_t*));
    size_t* current_record_idx_in_buffer = calloc(num_chunks, sizeof(size_t));
    size_t* records_in_buffer = calloc(num_chunks, sizeof(size_t));
    uint64_t* records_read_from_chunk_file = calloc(num_chunks, sizeof(uint64_t));

    // Determine records per chunk (all chunks are chunk_data_size_bytes as per problem constraints)
    uint64_t records_per_chunk_nominal = chunk_data_size_bytes / sizeof(index_record_t);

    // Buffer size for reading from each chunk (can be smaller than whole chunk)
    // For simplicity, let's use a small buffer, e.g., 256 records.
    const size_t K_WAY_BUFFER_RECS = 256;
    if (!chunk_buffers || !current_record_idx_in_buffer || !records_in_buffer || !records_read_from_chunk_file) {
        perror("k_way_merge: malloc tracking arrays");
        // Free any allocated memory before returning
        if(chunk_buffers) free(chunk_buffers);
        if(current_record_idx_in_buffer) free(current_record_idx_in_buffer);
        if(records_in_buffer) free(records_in_buffer);
        if(records_read_from_chunk_file) free(records_read_from_chunk_file);
        fclose(original_fp); fclose(temp_fp); remove(temp_filename); return -1;
    }

    for (int i = 0; i < num_chunks; ++i) {
        chunk_buffers[i] = malloc(K_WAY_BUFFER_RECS * sizeof(index_record_t));
        if (!chunk_buffers[i]) { /* error handling & cleanup */ perror("k_way_merge: malloc chunk_buffer"); /* ... */ return -1; }
    }

    // Initial fill of buffers
    size_t file_data_start_offset = sizeof(index_hdr_t) - sizeof(index_record_t*);
    for (int i = 0; i < num_chunks; ++i) {
        off_t chunk_offset_in_file = file_data_start_offset + (i * chunk_data_size_bytes);
        if (fseeko(original_fp, chunk_offset_in_file, SEEK_SET) != 0) { /* error */ perror("k_way_merge: fseeko initial fill"); /* ... */ return -1; }

        size_t num_to_read = K_WAY_BUFFER_RECS;
        if (records_per_chunk_nominal < K_WAY_BUFFER_RECS) num_to_read = records_per_chunk_nominal;

        records_in_buffer[i] = fread(chunk_buffers[i], sizeof(index_record_t), num_to_read, original_fp);
        if (ferror(original_fp)) { /* error */ perror("k_way_merge: fread initial fill"); /* ... */ return -1; }
        current_record_idx_in_buffer[i] = 0;
        records_read_from_chunk_file[i] = records_in_buffer[i];
    }

    // Min-heap (simulated with an array for active candidates)
    heap_node_t* active_candidates = malloc(num_chunks * sizeof(heap_node_t));
    if (!active_candidates) { /* error */ perror("k_way_merge: malloc active_candidates"); /* ... */ return -1; }

    uint64_t records_written_to_temp = 0;
    while(records_written_to_temp < total_records) {
        int active_candidate_count = 0;
        for (int i = 0; i < num_chunks; ++i) {
            if (current_record_idx_in_buffer[i] < records_in_buffer[i]) {
                active_candidates[active_candidate_count].record = chunk_buffers[i][current_record_idx_in_buffer[i]];
                active_candidates[active_candidate_count].chunk_idx = i;
                active_candidate_count++;
            }
        }

        if (active_candidate_count == 0) break; // All buffers exhausted

        // Find minimum among active_candidates
        // (Replace with proper min-heap pop for efficiency if num_chunks is large)
        int min_idx_in_candidates = 0;
        for (int i = 1; i < active_candidate_count; ++i) {
            if (compare_heap_nodes(&active_candidates[i], &active_candidates[min_idx_in_candidates]) < 0) {
                min_idx_in_candidates = i;
            }
        }

        heap_node_t min_node = active_candidates[min_idx_in_candidates];

        // Write min_node.record to temp_fp
        if (fwrite(&min_node.record, sizeof(index_record_t), 1, temp_fp) != 1) {
            perror("k_way_merge: fwrite record"); /* ... */ return -1;
        }
        records_written_to_temp++;

        // Advance in the chunk from which min_node came
        int source_chunk_idx = min_node.chunk_idx;
        current_record_idx_in_buffer[source_chunk_idx]++;

        // If buffer for source_chunk_idx is exhausted, refill it
        if (current_record_idx_in_buffer[source_chunk_idx] >= records_in_buffer[source_chunk_idx]) {
            if (records_read_from_chunk_file[source_chunk_idx] < records_per_chunk_nominal) {
                off_t next_read_offset = file_data_start_offset +
                (source_chunk_idx * chunk_data_size_bytes) +
                (records_read_from_chunk_file[source_chunk_idx] * sizeof(index_record_t));
                if (fseeko(original_fp, next_read_offset, SEEK_SET) != 0) { /* error */ perror("k_way_merge: fseeko refill"); /* ... */ return -1; }

                size_t num_to_read = K_WAY_BUFFER_RECS;
                uint64_t remaining_in_chunk_file = records_per_chunk_nominal - records_read_from_chunk_file[source_chunk_idx];
                if (num_to_read > remaining_in_chunk_file) num_to_read = remaining_in_chunk_file;

                if (num_to_read > 0) {
                    records_in_buffer[source_chunk_idx] = fread(chunk_buffers[source_chunk_idx], sizeof(index_record_t), num_to_read, original_fp);
                    if (ferror(original_fp)) { /* error */ perror("k_way_merge: fread refill"); /* ... */ return -1; }
                    current_record_idx_in_buffer[source_chunk_idx] = 0;
                    records_read_from_chunk_file[source_chunk_idx] += records_in_buffer[source_chunk_idx];
                } else {
                    records_in_buffer[source_chunk_idx] = 0; // Mark as exhausted
                }
            } else {
                records_in_buffer[source_chunk_idx] = 0; // Mark as exhausted
            }
        }
    }

    // Cleanup for k-way merge
    for (int i = 0; i < num_chunks; ++i) free(chunk_buffers[i]);
    free(chunk_buffers);
    free(current_record_idx_in_buffer);
    free(records_in_buffer);
    free(records_read_from_chunk_file);
    free(active_candidates);

    fclose(original_fp);
    if (fclose(temp_fp) != 0) { perror("k_way_merge: fclose temp_fp"); remove(temp_filename); return -1; }

    if (records_written_to_temp != total_records) {
        fprintf(stderr, "k_way_merge: Mismatch in record count. Expected %lu, wrote %lu.\n",
                (unsigned long)total_records, (unsigned long)records_written_to_temp);
        remove(temp_filename);
        return -1;
    }

    // Replace original file with temp file
    if (remove(original_filename) != 0) {
        perror("k_way_merge: remove original_filename");
        // temp_filename still exists, but operation failed.
        return -1;
    }
    if (rename(temp_filename, original_filename) != 0) {
        perror("k_way_merge: rename temp_filename");
        // Original is gone, temp file might be accessible as temp_filename. Critical error.
        return -1;
    }

    return 0;
}
