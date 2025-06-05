// src/sort_index.c
/*
 * This file contains the main logic for the 'sort_index' program,
 * which sorts a binary index file using multiple threads and memory-mapped I/O.
 * The sorting process involves dividing the file into chunks, sorting each
 * chunk in memory using multiple threads, and then merging the sorted chunks
 * if multiple chunks were processed.
 */
#define _GNU_SOURCE /* For strdup, basename, dirname if not otherwise available via POSIX */
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
#include <limits.h> // For PATH_MAX (though fpathconf is better if path is known)
// #include <getopt.h> // Not used for command-line args in this specific program
#include <libgen.h>    // For basename, dirname
#include <stdatomic.h> // For C11 Atomics

#include "common.h"
#include "utils.h"

/*
 * Structure to hold arguments for each thread, including pointers to shared data
 * and synchronization primitives.
 *
 * thread_id: Unique identifier for the thread (0 for the main thread's work portion).
 * total_threads_in_group: Total number of threads participating in sorting a chunk.
 * chunk_data_ptr: Pointer to the start of the current memory-mapped chunk of data.
 * num_records_in_chunk: Number of index records in the current chunk.
 * total_blocks_in_chunk: Total number of blocks the chunk is divided into for sorting.
 * records_per_block: Number of records in each ideal block.
 * barrier_chunk_ready: Barrier to synchronize threads before starting processing of a new chunk.
 * barrier_chunk_done: Barrier to synchronize threads after completing processing of a chunk.
 * barrier_blocks_sorted: Barrier to synchronize threads after all blocks in a chunk are sorted.
 * barrier_merge_pass: Barrier to synchronize threads between merge passes within a chunk.
 * task_mutex: Mutex for controlling access to shared task indices (e.g., next block to sort/merge).
 * next_block_to_sort_idx: Pointer to a shared index for the next block to be sorted competitively.
 * next_merge_pair_idx: Pointer to a shared index for the next pair of blocks to be merged competitively.
 * please_exit_flag: Pointer to a volatile flag indicating threads should terminate.
 * current_num_active_blocks_for_merge: Pointer to a shared atomic variable tracking active blocks in the current merge phase.
 * current_records_per_merged_block_for_merge: Pointer to a shared atomic variable tracking records per block in the current merge phase.
 */
typedef struct {
    int thread_id;
    int total_threads_in_group;
    index_record_t* chunk_data_ptr;
    size_t num_records_in_chunk;
    int total_blocks_in_chunk;
    size_t records_per_block;

    pthread_barrier_t* barrier_chunk_ready;
    pthread_barrier_t* barrier_chunk_done;
    pthread_barrier_t* barrier_blocks_sorted;
    pthread_barrier_t* barrier_merge_pass;

    pthread_mutex_t* task_mutex;
    int* next_block_to_sort_idx;
    int* next_merge_pair_idx;
    volatile int* please_exit_flag;
    _Atomic size_t* current_num_active_blocks_for_merge;
    _Atomic size_t* current_records_per_merged_block_for_merge;
} thread_arg_t;

// Forward declarations
void do_actual_processing_logic(thread_arg_t* data);
void* sort_worker_thread(void* arg);
void merge_sorted_arrays(index_record_t* arr1, size_t n1, index_record_t* arr2, size_t n2, index_record_t* out_buffer);
int k_way_merge_sorted_chunks(const char* original_filename, uint64_t total_records_in_file_from_header, int num_chunks, size_t chunk_data_size_bytes);

/*
 * Structure for a node in the min-heap used during k-way merge.
 * It holds an index record and the index of the chunk from which it originated.
 *
 * record: The index_record_t itself.
 * chunk_idx: The 0-based index of the source chunk/file stream.
 */
typedef struct {
    index_record_t record;
    int chunk_idx;
} heap_node_t;

/*
 * Comparison function for heap_node_t structures, primarily based on the
 * time_mark of their embedded index_record_t. Suitable for qsort or manual heap operations.
 *
 * a: Pointer to the first heap_node_t.
 * b: Pointer to the second heap_node_t.
 *
 * Returns:
 *   A negative value if record in 'a' should come before record in 'b'.
 *   Zero if records are considered equal for sorting purposes.
 *   A positive value if record in 'a' should come after record in 'b'.
 */
int compare_heap_nodes(const void* a, const void* b) {
    heap_node_t* node_a = (heap_node_t*)a;
    heap_node_t* node_b = (heap_node_t*)b;
    return compare_index_records(&node_a->record, &node_b->record);
}

/*
 * Main entry point for the sort_index program.
 * Parses command-line arguments, validates them, sets up threading and
 * memory mapping, processes the file in chunks, and coordinates the final
 * merge if necessary.
 *
 * argc: The number of command-line arguments.
 * argv: An array of command-line argument strings.
 *       Expected: ./sort_index <memsize> <blocks> <threads> <filename>
 *
 * Returns: 0 on successful completion, 1 on usage error, or exits via
 *          error_exit/errno_exit on other failures.
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

    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size == -1) errno_exit("sysconf(_SC_PAGESIZE) failed");
    if (memsize % (size_t)page_size != 0) {
        fprintf(stderr, "Error: memsize (%zu) must be a multiple of page size (%ld).\n", memsize, page_size);
        exit(EXIT_FAILURE);
    }
    if (!is_power_of_two(blocks_param)) error_exit("Blocks count must be a power of two.");

    long k_cores = get_core_count();
    if (threads_param < k_cores || threads_param > 8 * k_cores) {
        fprintf(stderr, "Warning: Threads count (%d) is outside the recommended range k (%ld) to 8k (%ld).\n",
                threads_param, k_cores, 8 * k_cores);
    }
    if (blocks_param < threads_param * 4) {
        fprintf(stderr, "Error: Blocks count (%d) must be at least 4 times threads count (%d).\n",
                blocks_param, threads_param);
        exit(EXIT_FAILURE);
    }

    int fd = open(filename, O_RDWR);
    if (fd == -1) errno_exit("Failed to open file");

    struct stat sb;
    if (fstat(fd, &sb) == -1) { close(fd); errno_exit("fstat failed"); }

    uint64_t total_records_in_file_from_header;
    if (read(fd, &total_records_in_file_from_header, sizeof(total_records_in_file_from_header)) != (ssize_t)sizeof(total_records_in_file_from_header)) {
        close(fd); errno_exit("Failed to read file header (record count)");
    }

    size_t file_data_start_offset = sizeof(total_records_in_file_from_header);
    size_t total_data_size_bytes = total_records_in_file_from_header * sizeof(index_record_t);

    off_t expected_file_size = (off_t)file_data_start_offset + (off_t)total_data_size_bytes;
    if (sb.st_size != expected_file_size) {
        fprintf(stderr, "Error: File size (%jd) does not match expected size based on header (%jd).\n",
                (intmax_t)sb.st_size, (intmax_t)expected_file_size);
        close(fd); exit(EXIT_FAILURE);
    }
    if (total_data_size_bytes == 0) {
        printf("File contains no records to sort.\n"); close(fd); return 0;
    }
    if (memsize == 0) error_exit("Memsize cannot be zero.");
    if (total_data_size_bytes > 0 && total_data_size_bytes % memsize != 0) {
        fprintf(stderr, "Error: Total data size in file (%zu) is not a multiple of memsize (%zu).\n",
                total_data_size_bytes, memsize);
        close(fd); exit(EXIT_FAILURE);
    }

    size_t records_per_ideal_block = (memsize / sizeof(index_record_t)) / blocks_param;
    if (records_per_ideal_block == 0) {
        close(fd); error_exit("Calculated records per block is zero.");
    }

    pthread_t* worker_pthreads = NULL;
    if (threads_param > 1) {
        worker_pthreads = malloc((threads_param - 1) * sizeof(pthread_t));
        if (!worker_pthreads) abort();
    }
    thread_arg_t* thread_args = malloc(threads_param * sizeof(thread_arg_t));
    if (!thread_args) abort();

    pthread_barrier_t barrier_chunk_ready, barrier_chunk_done, barrier_blocks_sorted, barrier_merge_pass;
    pthread_mutex_t task_mutex = PTHREAD_MUTEX_INITIALIZER;
    int next_block_to_sort_idx_shared;
    int next_merge_pair_idx_shared;
    volatile int please_exit_flag_shared = 0;
    _Atomic size_t shared_cnab_for_merge;
    _Atomic size_t shared_crpmb_for_merge;

    if (pthread_barrier_init(&barrier_chunk_ready, NULL, threads_param) != 0) errno_exit("barrier_init (chunk_ready)");
    if (pthread_barrier_init(&barrier_chunk_done, NULL, threads_param) != 0) errno_exit("barrier_init (chunk_done)");
    if (pthread_barrier_init(&barrier_blocks_sorted, NULL, threads_param) != 0) errno_exit("barrier_init (blocks_sorted)");
    if (pthread_barrier_init(&barrier_merge_pass, NULL, threads_param) != 0) errno_exit("barrier_init (merge_pass)");

    for (int i = 0; i < threads_param; ++i) {
        thread_args[i].thread_id = i;
        thread_args[i].total_threads_in_group = threads_param;
        thread_args[i].barrier_chunk_ready = &barrier_chunk_ready;
        thread_args[i].barrier_chunk_done = &barrier_chunk_done;
        thread_args[i].barrier_blocks_sorted = &barrier_blocks_sorted;
        thread_args[i].barrier_merge_pass = &barrier_merge_pass;
        thread_args[i].task_mutex = &task_mutex;
        thread_args[i].next_block_to_sort_idx = &next_block_to_sort_idx_shared;
        thread_args[i].next_merge_pair_idx = &next_merge_pair_idx_shared;
        thread_args[i].please_exit_flag = &please_exit_flag_shared;
    }

    if (threads_param > 1) {
        for (int i = 0; i < threads_param - 1; ++i) {
            if (pthread_create(&worker_pthreads[i], NULL, sort_worker_thread, &thread_args[i+1]) != 0) {
                please_exit_flag_shared = 1;
                for(int j=0; j<i; ++j) pthread_join(worker_pthreads[j], NULL);
                if (worker_pthreads) { free(worker_pthreads); }
                if (thread_args) { free(thread_args); }
                pthread_barrier_destroy(&barrier_chunk_ready); pthread_barrier_destroy(&barrier_chunk_done);
                pthread_barrier_destroy(&barrier_blocks_sorted); pthread_barrier_destroy(&barrier_merge_pass);
                pthread_mutex_destroy(&task_mutex);
                close(fd); errno_exit("Failed to create worker thread");
            }
        }
    }

    size_t processed_data_bytes = 0;
    int num_chunks_processed = 0;

    while(processed_data_bytes < total_data_size_bytes) {
        size_t current_chunk_data_size = memsize;
        off_t current_file_offset_for_data = (off_t)file_data_start_offset + (off_t)processed_data_bytes;
        off_t mmap_offset_aligned = (current_file_offset_for_data / page_size) * page_size;
        size_t mmap_ptr_adjustment = current_file_offset_for_data % page_size;
        size_t mmap_length = current_chunk_data_size + mmap_ptr_adjustment;

        void* mapped_memory_base = mmap(NULL, mmap_length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, mmap_offset_aligned);
        if (mapped_memory_base == MAP_FAILED) {
            please_exit_flag_shared = 1;
            // Attempt to wake and join threads if they were started
            if (threads_param > 1 && worker_pthreads) {
                // This barrier wait is problematic if not all threads will reach it.
                // Consider a more robust non-blocking signal or simply proceed to join.
                for (int i = 0; i < threads_param - 1; ++i) pthread_join(worker_pthreads[i], NULL);
            }
            close(fd); errno_exit("mmap failed");
        }

        index_record_t* chunk_data_start_ptr = (index_record_t*)((char*)mapped_memory_base + mmap_ptr_adjustment);
        size_t num_records_this_chunk = current_chunk_data_size / sizeof(index_record_t);

        atomic_store(&shared_cnab_for_merge, blocks_param);
        atomic_store(&shared_crpmb_for_merge, records_per_ideal_block);

        for (int i = 0; i < threads_param; ++i) {
            thread_args[i].chunk_data_ptr = chunk_data_start_ptr;
            thread_args[i].num_records_in_chunk = num_records_this_chunk;
            thread_args[i].total_blocks_in_chunk = blocks_param;
            thread_args[i].records_per_block = records_per_ideal_block;
            thread_args[i].current_num_active_blocks_for_merge = &shared_cnab_for_merge;
            thread_args[i].current_records_per_merged_block_for_merge = &shared_crpmb_for_merge;
        }

        pthread_barrier_wait(&barrier_chunk_ready);
        sort_worker_thread(&thread_args[0]);
        // sort_worker_thread for thread 0 will block until barrier_chunk_done is passed by all threads for this chunk.

        if (munmap(mapped_memory_base, mmap_length) == -1) {
            please_exit_flag_shared = 1;
            if (threads_param > 1 && worker_pthreads) {
                for (int i = 0; i < threads_param - 1; ++i) pthread_join(worker_pthreads[i], NULL);
            }
            close(fd); errno_exit("munmap failed");
        }
        processed_data_bytes += current_chunk_data_size;
        num_chunks_processed++;
        printf("Main: Chunk %d processed and unmapped.\n", num_chunks_processed); fflush(stdout);
    }
    printf("Main: Chunk processing loop finished.\n"); fflush(stdout);

    please_exit_flag_shared = 1;
    if (threads_param > 1) {
        pthread_barrier_wait(&barrier_chunk_ready);
        for (int i = 0; i < threads_param - 1; ++i) {
            if (pthread_join(worker_pthreads[i], NULL) != 0) {
                perror("Warning: Failed to join worker thread");
            }
        }
        printf("Main: All worker threads joined.\n"); fflush(stdout);
    }

    pthread_barrier_destroy(&barrier_chunk_ready);
    pthread_barrier_destroy(&barrier_chunk_done);
    pthread_barrier_destroy(&barrier_blocks_sorted);
    pthread_barrier_destroy(&barrier_merge_pass);
    pthread_mutex_destroy(&task_mutex);
    if (worker_pthreads) { free(worker_pthreads); worker_pthreads = NULL; }
    if (thread_args) { free(thread_args); thread_args = NULL; }
    if (close(fd) == -1) errno_exit("Failed to close file after chunk processing");

    if (num_chunks_processed > 1) {
        if (k_way_merge_sorted_chunks(filename, total_records_in_file_from_header, num_chunks_processed, memsize) != 0) {
            error_exit("Final k-way merge failed.");
        }
    }
    printf("Main: Sorting of %s completed.\n", filename); fflush(stdout);
    return 0;
}

/*
 * Performs the core sorting and merging logic for a single memory-mapped chunk.
 * This function is executed by all participating threads for the current chunk.
 * It involves:
 *   1. Initial assignment of blocks to threads for sorting via qsort.
 *   2. Competitive sorting of remaining blocks by available threads.
 *   3. Multi-pass merging of sorted blocks until the entire chunk is sorted.
 * Synchronization between these phases and passes is managed by barriers.
 *
 * data: Pointer to a thread_arg_t structure containing all necessary information
 *       for processing the chunk, including data pointers, sizes, shared state
 *       pointers, and synchronization primitives.
 */
void do_actual_processing_logic(thread_arg_t* data) {
    // Phase 1: Sort individual blocks
    if (data->thread_id < data->total_blocks_in_chunk) {
        size_t block_start_record_idx = (size_t)data->thread_id * data->records_per_block;
        if (block_start_record_idx < data->num_records_in_chunk) {
            index_record_t* block_ptr = data->chunk_data_ptr + block_start_record_idx;
            size_t records_in_this_block = data->records_per_block;
            if (block_start_record_idx + records_in_this_block > data->num_records_in_chunk) {
                records_in_this_block = data->num_records_in_chunk - block_start_record_idx;
            }
            if (records_in_this_block > 0) {
                qsort(block_ptr, records_in_this_block, sizeof(index_record_t), compare_index_records);
            }
        }
    }

    if (data->thread_id == 0) {
        *(data->next_block_to_sort_idx) = data->total_threads_in_group;
    }
    pthread_barrier_wait(data->barrier_blocks_sorted);

    while (1) {
        int block_idx_to_sort;
        pthread_mutex_lock(data->task_mutex);
        block_idx_to_sort = *(data->next_block_to_sort_idx);
        if (block_idx_to_sort < data->total_blocks_in_chunk) {
            (*(data->next_block_to_sort_idx))++;
        }
        pthread_mutex_unlock(data->task_mutex);
        if (block_idx_to_sort >= data->total_blocks_in_chunk) {
            break;
        }
        size_t block_start_record_idx = (size_t)block_idx_to_sort * data->records_per_block;
        if (block_start_record_idx < data->num_records_in_chunk) {
            index_record_t* block_ptr = data->chunk_data_ptr + block_start_record_idx;
            size_t records_in_this_block = data->records_per_block;
            if (block_start_record_idx + records_in_this_block > data->num_records_in_chunk) {
                records_in_this_block = data->num_records_in_chunk - block_start_record_idx;
            }
            if (records_in_this_block > 0) {
                qsort(block_ptr, records_in_this_block, sizeof(index_record_t), compare_index_records);
            }
        }
    }
    pthread_barrier_wait(data->barrier_blocks_sorted);

    // Phase 2: Merge blocks
    _Atomic size_t* p_current_num_active_blocks = data->current_num_active_blocks_for_merge;
    _Atomic size_t* p_current_records_per_block = data->current_records_per_merged_block_for_merge;
    index_record_t* temp_merge_buffer = NULL;

    if (data->total_blocks_in_chunk > 1 && data->num_records_in_chunk > 0) {
        size_t merge_buffer_size = data->num_records_in_chunk * sizeof(index_record_t);
        if (data->num_records_in_chunk > 0 && (merge_buffer_size / sizeof(index_record_t)) != data->num_records_in_chunk) { // Check overflow
            fprintf(stderr, "Thread %d: Merge buffer size calculation overflowed.\n", data->thread_id);
            *(data->please_exit_flag) = 1;
            pthread_exit((void*)EXIT_FAILURE);
        }
        temp_merge_buffer = malloc(merge_buffer_size);
        if (!temp_merge_buffer) {
            fprintf(stderr, "Thread %d: Malloc failed for merge buffer.\n", data->thread_id);
            *(data->please_exit_flag) = 1;
            pthread_exit((void*)EXIT_FAILURE);
        }
    }

    while (atomic_load(p_current_num_active_blocks) > 1) {
        size_t current_cnab_val = atomic_load(p_current_num_active_blocks);
        int pairs_to_merge_this_pass = current_cnab_val / 2;

        if (pairs_to_merge_this_pass == 0 && current_cnab_val == 1) { // Should be caught by while condition
            break;
        }

        if (data->thread_id == 0) { *(data->next_merge_pair_idx) = 0; }
        pthread_barrier_wait(data->barrier_merge_pass); // SYNC 1: All threads ready for this merge pass

        if (current_cnab_val == 2 && pairs_to_merge_this_pass == 1) { // Final merge of two halves
            if (data->thread_id == 0) {
                index_record_t* block1_ptr = data->chunk_data_ptr;
                size_t n1 = atomic_load(p_current_records_per_block);
                if (n1 > data->num_records_in_chunk) n1 = data->num_records_in_chunk;
                index_record_t* block2_ptr = data->chunk_data_ptr + n1;
                size_t n2 = 0;
                if (data->num_records_in_chunk > n1) n2 = data->num_records_in_chunk - n1;
                if (n1 > 0 && n2 > 0 && temp_merge_buffer) {
                    merge_sorted_arrays(block1_ptr, n1, block2_ptr, n2, temp_merge_buffer);
                    memcpy(data->chunk_data_ptr, temp_merge_buffer, (n1 + n2) * sizeof(index_record_t));
                }
            }
        } else { // Standard competitive merge pass
            while (1) {
                int my_pair_idx;
                pthread_mutex_lock(data->task_mutex);
                my_pair_idx = *(data->next_merge_pair_idx);
                if (my_pair_idx < pairs_to_merge_this_pass) (*(data->next_merge_pair_idx))++;
                pthread_mutex_unlock(data->task_mutex);
                if (my_pair_idx >= pairs_to_merge_this_pass) break;

                size_t current_rpb_val = atomic_load(p_current_records_per_block);
                size_t base_offset_records = (size_t)my_pair_idx * 2 * current_rpb_val;
                index_record_t* output_ptr = data->chunk_data_ptr + base_offset_records;
                index_record_t* block1_ptr = output_ptr;
                index_record_t* block2_ptr = data->chunk_data_ptr + base_offset_records + current_rpb_val;
                size_t n1 = current_rpb_val; size_t n2 = current_rpb_val;

                if (base_offset_records + n1 > data->num_records_in_chunk) {
                    if (base_offset_records >= data->num_records_in_chunk) n1 = 0;
                    else n1 = data->num_records_in_chunk - base_offset_records;
                }
                size_t block2_start_offset_records = base_offset_records + current_rpb_val;
                if (block2_start_offset_records + n2 > data->num_records_in_chunk) {
                    if (block2_start_offset_records >= data->num_records_in_chunk) n2 = 0;
                    else n2 = data->num_records_in_chunk - block2_start_offset_records;
                }
                if (n1 > 0 && n2 > 0 && temp_merge_buffer) {
                    merge_sorted_arrays(block1_ptr, n1, block2_ptr, n2, temp_merge_buffer);
                    memcpy(output_ptr, temp_merge_buffer, (n1 + n2) * sizeof(index_record_t));
                }
            }
        }
        pthread_barrier_wait(data->barrier_merge_pass); // SYNC 2: All merge operations for this pass complete

        if (data->thread_id == 0) {
            atomic_store(p_current_num_active_blocks, current_cnab_val / 2);
            atomic_store(p_current_records_per_block, atomic_load(p_current_records_per_block) * 2);
        }
        pthread_barrier_wait(data->barrier_merge_pass); // SYNC 3: State updated, all threads see new state before next iteration
    }

    if (temp_merge_buffer) free(temp_merge_buffer);
}

/*
 * Thread function for worker threads.
 * It loops, waiting for chunks to be ready, processes them, and then waits
 * for the chunk processing to be acknowledged as done by all threads.
 * It exits if the please_exit_flag is set.
 * The main thread also uses this function (with thread_id 0) to perform its
 * share of the work on a chunk, but it does not loop.
 *
 * arg: Pointer to a thread_arg_t structure for this thread.
 *
 * Returns: NULL. Worker threads exit via pthread_exit.
 */
void* sort_worker_thread(void* arg) {
    thread_arg_t* data = (thread_arg_t*)arg;

    if (data->thread_id != 0) {
        while (1) {
            pthread_barrier_wait(data->barrier_chunk_ready);
            if (*(data->please_exit_flag)) {
                pthread_exit(NULL);
            }
            do_actual_processing_logic(data);
            pthread_barrier_wait(data->barrier_chunk_done);
        }
    } else {
        do_actual_processing_logic(data);
        pthread_barrier_wait(data->barrier_chunk_done);
        return NULL;
    }
    return NULL; /* Should be unreachable for workers */
}

/*
 * Merges two sorted arrays of index_record_t into a specified output buffer.
 * The output buffer must be large enough to hold all elements from arr1 and arr2.
 *
 * arr1: Pointer to the first sorted array of index records.
 * n1: Number of elements in arr1.
 * arr2: Pointer to the second sorted array of index records.
 * n2: Number of elements in arr2.
 * out_buffer: Pointer to the buffer where the merged sorted records will be stored.
 */
void merge_sorted_arrays(index_record_t* arr1, size_t n1, index_record_t* arr2, size_t n2, index_record_t* out_buffer) {
    size_t i = 0, j = 0, k = 0;
    while (i < n1 && j < n2) {
        if (compare_index_records(&arr1[i], &arr2[j]) <= 0) out_buffer[k++] = arr1[i++];
        else out_buffer[k++] = arr2[j++];
    }
    while (i < n1) out_buffer[k++] = arr1[i++];
    while (j < n2) out_buffer[k++] = arr2[j++];
}

/*
 * Performs a k-way merge of multiple sorted chunks within a single file into a
 * new temporary file, and then replaces the original file with the sorted version.
 * Each chunk is assumed to have been previously sorted (e.g., by processing with memsize).
 *
 * original_filename: The name of the file containing the sorted chunks.
 * total_records_in_file_from_header: The total number of records in the file, as per its header.
 * num_chunks: The number of conceptual sorted chunks the file is divided into.
 * chunk_data_size_bytes: The size (in bytes) of each sorted chunk (typically memsize).
 *
 * Returns: 0 on success, -1 on failure.
 */
int k_way_merge_sorted_chunks(const char* original_filename, uint64_t total_records_in_file_from_header, int num_chunks, size_t chunk_data_size_bytes) {
    if (num_chunks <= 1) return 0;

    char temp_filename_pattern[PATH_MAX];
    char temp_filename[PATH_MAX];
    char* original_filename_dup_for_basename = NULL;
    char* original_basename = NULL;
    char* dir_part = NULL;
    char* original_filename_dup_for_dirname = NULL;

    original_filename_dup_for_dirname = strdup(original_filename);
    if (!original_filename_dup_for_dirname) { perror("k_way_merge: strdup for dirname"); return -1; }
    dir_part = dirname(original_filename_dup_for_dirname);

    original_filename_dup_for_basename = strdup(original_filename);
    if (!original_filename_dup_for_basename) { perror("k_way_merge: strdup for basename"); free(original_filename_dup_for_dirname); return -1; }
    original_basename = basename(original_filename_dup_for_basename);

    if (dir_part && original_basename) {
        snprintf(temp_filename_pattern, PATH_MAX, "%s/%s.sorttmp.XXXXXX", dir_part, original_basename);
    } else {
        const char* tmpdir_env = getenv("TMPDIR");
        const char* tmp_dir_path = (tmpdir_env && strlen(tmpdir_env) > 0) ? tmpdir_env : "/tmp";
        snprintf(temp_filename_pattern, PATH_MAX, "%s/%s.sorttmp.XXXXXX", tmp_dir_path, (original_basename ? original_basename : "unknown_orig_base"));
    }
    free(original_filename_dup_for_dirname);
    free(original_filename_dup_for_basename);

    int temp_fd = mkstemp(temp_filename_pattern);
    if (temp_fd == -1) { perror("k_way_merge: mkstemp failed"); return -1; }
    strncpy(temp_filename, temp_filename_pattern, PATH_MAX -1); temp_filename[PATH_MAX-1] = '\0';

    FILE* original_fp = NULL; FILE* temp_fp = NULL;
    index_record_t** chunk_buffers = NULL; size_t* current_record_idx_in_buffer = NULL;
    size_t* records_in_buffer = NULL; uint64_t* records_read_from_chunk_file = NULL;
    heap_node_t* active_candidates = NULL; int ret_val = -1;

    original_fp = fopen(original_filename, "rb");
    if (!original_fp) { perror("k_way_merge: fopen original_fp"); close(temp_fd); remove(temp_filename); ret_val = -1; goto cleanup_k_way_early_exit_temp_removed; }
    temp_fp = fdopen(temp_fd, "wb");
    if (!temp_fp) { perror("k_way_merge: fdopen temp_fd"); close(temp_fd); remove(temp_filename); ret_val = -1; goto cleanup_k_way_early_exit_temp_removed; }

    uint64_t header_record_count;
    if (fseek(original_fp, 0, SEEK_SET) != 0) { perror("k_way_merge: fseek header original"); goto cleanup_k_way; }
    if (fread(&header_record_count, sizeof(header_record_count), 1, original_fp) != 1) { perror("k_way_merge: fread header from original"); goto cleanup_k_way; }
    if (header_record_count != total_records_in_file_from_header) { goto cleanup_k_way; }
    if (fwrite(&header_record_count, sizeof(header_record_count), 1, temp_fp) != 1) { perror("k_way_merge: fwrite header to temp"); goto cleanup_k_way; }

    chunk_buffers = malloc(num_chunks * sizeof(index_record_t*));
    if (chunk_buffers) { for(int i=0; i<num_chunks; ++i) chunk_buffers[i] = NULL; } else { goto cleanup_k_way; }
    current_record_idx_in_buffer = calloc(num_chunks, sizeof(size_t)); records_in_buffer = calloc(num_chunks, sizeof(size_t));
    records_read_from_chunk_file = calloc(num_chunks, sizeof(uint64_t)); active_candidates = malloc(num_chunks * sizeof(heap_node_t));
    const size_t K_WAY_BUFFER_RECS = 256;
    if (!chunk_buffers || !current_record_idx_in_buffer || !records_in_buffer || !records_read_from_chunk_file || !active_candidates) {
        perror("k_way_merge: malloc tracking arrays"); goto cleanup_k_way;
    }
    for (int i = 0; i < num_chunks; ++i) {
        chunk_buffers[i] = malloc(K_WAY_BUFFER_RECS * sizeof(index_record_t));
        if (!chunk_buffers[i]) { perror("k_way_merge: malloc chunk_buffer"); goto cleanup_k_way; }
    }

    uint64_t records_per_chunk_nominal = chunk_data_size_bytes / sizeof(index_record_t);
    size_t file_data_start_offset_in_original = sizeof(header_record_count);
    for (int i = 0; i < num_chunks; ++i) {
        off_t chunk_offset_in_file = (off_t)file_data_start_offset_in_original + (i * chunk_data_size_bytes);
        if (fseeko(original_fp, chunk_offset_in_file, SEEK_SET) != 0) { perror("k_way_merge: fseeko initial fill"); goto cleanup_k_way; }
        size_t num_to_read = K_WAY_BUFFER_RECS;
        if (num_to_read > records_per_chunk_nominal) num_to_read = records_per_chunk_nominal;
        if (num_to_read > 0) {
            records_in_buffer[i] = fread(chunk_buffers[i], sizeof(index_record_t), num_to_read, original_fp);
            if (ferror(original_fp)) { perror("k_way_merge: fread initial fill"); goto cleanup_k_way; }
            current_record_idx_in_buffer[i] = 0; records_read_from_chunk_file[i] = records_in_buffer[i];
        } else { records_in_buffer[i] = 0; }
    }

    uint64_t records_written_to_temp = 0;
    while(records_written_to_temp < total_records_in_file_from_header) {
        int active_candidate_count = 0;
        for (int i = 0; i < num_chunks; ++i) {
            if (current_record_idx_in_buffer[i] < records_in_buffer[i]) {
                active_candidates[active_candidate_count].record = chunk_buffers[i][current_record_idx_in_buffer[i]];
                active_candidates[active_candidate_count].chunk_idx = i; active_candidate_count++;
            }
        }
        if (active_candidate_count == 0) { if (records_written_to_temp < total_records_in_file_from_header) { /* error */ } break; }
        int min_idx_in_candidates = 0;
        for (int i = 1; i < active_candidate_count; ++i) if (compare_heap_nodes(&active_candidates[i], &active_candidates[min_idx_in_candidates]) < 0) min_idx_in_candidates = i;
        heap_node_t min_node = active_candidates[min_idx_in_candidates];
        if (fwrite(&min_node.record, sizeof(index_record_t), 1, temp_fp) != 1) { perror("k_way_merge: fwrite record"); goto cleanup_k_way; }
        records_written_to_temp++;
        int source_chunk_idx = min_node.chunk_idx; current_record_idx_in_buffer[source_chunk_idx]++;
        if (current_record_idx_in_buffer[source_chunk_idx] >= records_in_buffer[source_chunk_idx]) {
            if (records_read_from_chunk_file[source_chunk_idx] < records_per_chunk_nominal) {
                off_t next_read_offset = (off_t)file_data_start_offset_in_original + (source_chunk_idx * chunk_data_size_bytes) + (records_read_from_chunk_file[source_chunk_idx] * sizeof(index_record_t));
                if (fseeko(original_fp, next_read_offset, SEEK_SET) != 0) { perror("k_way_merge: fseeko refill"); goto cleanup_k_way; }
                size_t num_to_read = K_WAY_BUFFER_RECS; uint64_t remaining_in_physical_chunk = records_per_chunk_nominal - records_read_from_chunk_file[source_chunk_idx];
                if (num_to_read > remaining_in_physical_chunk) num_to_read = remaining_in_physical_chunk;
                if (num_to_read > 0) {
                    records_in_buffer[source_chunk_idx] = fread(chunk_buffers[source_chunk_idx], sizeof(index_record_t), num_to_read, original_fp);
                    if (ferror(original_fp)) { perror("k_way_merge: fread refill"); goto cleanup_k_way; }
                    current_record_idx_in_buffer[source_chunk_idx] = 0; records_read_from_chunk_file[source_chunk_idx] += records_in_buffer[source_chunk_idx];
                } else { records_in_buffer[source_chunk_idx] = 0; }
            } else { records_in_buffer[source_chunk_idx] = 0; }
        }
    }

    if (records_written_to_temp != total_records_in_file_from_header) { goto cleanup_k_way; }
    if (fflush(temp_fp) != 0) { perror("k_way_merge: fflush temp_fp"); goto cleanup_k_way; }
    if (fclose(temp_fp) != 0) { temp_fp = NULL; perror("k_way_merge: fclose temp_fp"); goto cleanup_k_way; }
    temp_fp = NULL;
    if (original_fp) { fclose(original_fp); original_fp = NULL; }


    if (rename(temp_filename, original_filename) != 0) {
        fprintf(stderr, "k_way_merge: rename from %s to %s failed: %s\n", temp_filename, original_filename, strerror(errno));
        // If rename fails, original_filename is (hopefully) untouched from before k_way_merge was called.
        // The temp_filename contains the fully sorted data. We should try to remove it if rename fails.
        goto cleanup_k_way; // This will set ret_val to -1 (if not already) and remove temp_filename
    }
    ret_val = 0; // rename was successful
    // temp_filename is now original_filename, so it should not be removed by cleanup_k_way.
    goto cleanup_k_way_no_remove_temp;

    cleanup_k_way:
    if (ret_val != 0) { // Only remove temp_filename if we are in a failure path
        remove(temp_filename);
    }
    cleanup_k_way_no_remove_temp:
    if (temp_fp) { fclose(temp_fp); }
    if (original_fp) { fclose(original_fp); }
    if (chunk_buffers) { for (int i = 0; i < num_chunks; ++i) if (chunk_buffers[i]) free(chunk_buffers[i]); free(chunk_buffers); }
    if (current_record_idx_in_buffer) free(current_record_idx_in_buffer);
    if (records_in_buffer) free(records_in_buffer);
    if (records_read_from_chunk_file) free(records_read_from_chunk_file);
    if (active_candidates) free(active_candidates);
    return ret_val;
    cleanup_k_way_early_exit_temp_removed: // Temp file already dealt with
    if (original_fp) { fclose(original_fp); }
    return -1;
}
