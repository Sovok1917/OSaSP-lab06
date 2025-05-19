// src/sort_index.c
/*
 * Multi-threaded program to sort an index file using memory mapping.
 */
#define _GNU_SOURCE
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
#include <limits.h>
#include <getopt.h> // Not used for args, but good include for POSIX

#include "common.h" // For index_record_t
#include "utils.h"

// Structure to hold thread-specific arguments and shared data access
typedef struct {
    int thread_id;
    int total_threads_in_group;

    index_record_t* chunk_data_ptr;
    size_t num_records_in_chunk;

    int total_blocks_in_chunk;      // This is blocks_param, constant for the chunk
    size_t records_per_block;       // This is records_per_ideal_block, constant for the chunk

    pthread_barrier_t* barrier_start_chunk;
    pthread_barrier_t* barrier_blocks_sorted;
    pthread_barrier_t* barrier_merge_pass;

    pthread_mutex_t* task_mutex;
    int* next_block_to_sort_idx;
    int* next_merge_pair_idx;

    volatile int* please_exit_flag;

    // Pointers to shared state for the current chunk's merge phase
    size_t* current_num_active_blocks_for_merge;
    size_t* current_records_per_merged_block_for_merge;

} thread_arg_t;

// Forward declarations
void do_actual_processing_logic(thread_arg_t* data);
void* sort_worker_thread(void* arg);
void merge_sorted_arrays(index_record_t* arr1, size_t n1, index_record_t* arr2, size_t n2, index_record_t* out_buffer);
int k_way_merge_sorted_chunks(const char* original_filename, uint64_t total_records_in_file_from_header, int num_chunks, size_t chunk_data_size_bytes);


/*
 * Main function for the 'sort_index' program.
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
    int blocks_param = (int)blocks_l; // This is total_blocks_in_chunk for setup

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
    if (k_cores == 4 && blocks_param < 16) {
        fprintf(stderr, "Warning: With k=4 cores, blocks count (%d) is less than the recommended minimum of 16.\n", blocks_param);
    }
    if (memsize < (size_t)blocks_param * sizeof(index_record_t)) {
        error_exit("Memsize is too small to accommodate the specified number of blocks with at least one record per block.");
    }
    if ((memsize / sizeof(index_record_t)) == 0) {
        error_exit("Memsize is too small, cannot fit even one record.");
    }
    if ((memsize / sizeof(index_record_t)) < (size_t)blocks_param) {
        error_exit("Memsize cannot be split into the requested number of blocks, each holding at least one record.");
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
    if (sb.st_size < (long long)file_data_start_offset) {
        close(fd); error_exit("File is too small to contain a valid header and any records.");
    }

    if (total_data_size_bytes == 0) {
        printf("File contains no records to sort.\n");
        close(fd); return 0;
    }
    if (memsize == 0) error_exit("Memsize cannot be zero.");
    if (total_data_size_bytes > 0 && total_data_size_bytes % memsize != 0) {
        fprintf(stderr, "Error: Total data size in file (%zu bytes from %lu records) "
        "is not a multiple of memsize (%zu).\n",
                total_data_size_bytes, (unsigned long)total_records_in_file_from_header, memsize);
        fprintf(stderr, "Please use 'gen' to create a file where "
        "(num_records * %zu) is a multiple of %zu, or adjust memsize.\n", sizeof(index_record_t), memsize);
        close(fd); exit(EXIT_FAILURE);
    }

    // records_per_ideal_block is constant for the entire sorting process based on initial params
    size_t records_per_ideal_block = (memsize / sizeof(index_record_t)) / blocks_param;
    if (records_per_ideal_block == 0) {
        close(fd); error_exit("Calculated records per block is zero. Check memsize and blocks_param.");
    }

    pthread_t* worker_pthreads = NULL;
    if (threads_param > 1) {
        worker_pthreads = malloc((threads_param - 1) * sizeof(pthread_t));
        if (!worker_pthreads) abort();
    }
    thread_arg_t* thread_args = malloc(threads_param * sizeof(thread_arg_t));
    if (!thread_args) abort();

    pthread_barrier_t barrier_start_chunk, barrier_blocks_sorted, barrier_merge_pass;
    pthread_mutex_t task_mutex = PTHREAD_MUTEX_INITIALIZER;
    int next_block_to_sort_idx_shared;
    int next_merge_pair_idx_shared;
    volatile int please_exit_flag_shared = 0;

    // Shared state for merge phase, re-initialized per chunk
    size_t shared_cnab_for_merge; // current_num_active_blocks_for_merge
    size_t shared_crpmb_for_merge; // current_records_per_merged_block_for_merge

    if (pthread_barrier_init(&barrier_start_chunk, NULL, threads_param) != 0) errno_exit("barrier_init (start_chunk)");
    if (pthread_barrier_init(&barrier_blocks_sorted, NULL, threads_param) != 0) errno_exit("barrier_init (blocks_sorted)");
    if (pthread_barrier_init(&barrier_merge_pass, NULL, threads_param) != 0) errno_exit("barrier_init (merge_pass)");

    // Initialize thread_args fields that are constant across chunks
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
        // These will be set per chunk:
        // thread_args[i].chunk_data_ptr
        // thread_args[i].num_records_in_chunk
        // thread_args[i].total_blocks_in_chunk (this is blocks_param)
        // thread_args[i].records_per_block (this is records_per_ideal_block)
        // thread_args[i].current_num_active_blocks_for_merge
        // thread_args[i].current_records_per_merged_block_for_merge
    }

    if (threads_param > 1) {
        for (int i = 0; i < threads_param - 1; ++i) {
            printf("Main: Creating worker thread %d (arg index %d)\n", i + 1, i + 1); fflush(stdout);
            if (pthread_create(&worker_pthreads[i], NULL, sort_worker_thread, &thread_args[i+1]) != 0) {
                please_exit_flag_shared = 1;
                for(int j=0; j<i; ++j) {
                    printf("Main: Error creating thread, joining successfully created thread %d\n", j+1); fflush(stdout);
                    pthread_join(worker_pthreads[j], NULL);
                }
                if (worker_pthreads) free(worker_pthreads);
                if (thread_args) free(thread_args);
                pthread_barrier_destroy(&barrier_start_chunk);
                pthread_barrier_destroy(&barrier_blocks_sorted);
                pthread_barrier_destroy(&barrier_merge_pass);
                pthread_mutex_destroy(&task_mutex);
                close(fd);
                errno_exit("Failed to create worker thread");
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

        printf("Main: Processing chunk %d: file data offset %jd, size %zu (mmap file offset %jd, mmap length %zu, ptr adjust %zu)\n",
               num_chunks_processed + 1, (intmax_t)current_file_offset_for_data, current_chunk_data_size,
               (intmax_t)mmap_offset_aligned, mmap_length, mmap_ptr_adjustment);
        fflush(stdout);

        void* mapped_memory_base = mmap(NULL, mmap_length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, mmap_offset_aligned);
        if (mapped_memory_base == MAP_FAILED) {
            please_exit_flag_shared = 1;
            if (threads_param > 1) { /* ... attempt to wake/join ... */ }
            close(fd); errno_exit("mmap failed for a chunk");
        }
        index_record_t* chunk_data_start_ptr = (index_record_t*)((char*)mapped_memory_base + mmap_ptr_adjustment);
        size_t num_records_this_chunk = current_chunk_data_size / sizeof(index_record_t);

        // Initialize shared merge state for THIS chunk
        shared_cnab_for_merge = blocks_param;
        shared_crpmb_for_merge = records_per_ideal_block;

        for (int i = 0; i < threads_param; ++i) {
            thread_args[i].chunk_data_ptr = chunk_data_start_ptr;
            thread_args[i].num_records_in_chunk = num_records_this_chunk;
            thread_args[i].total_blocks_in_chunk = blocks_param; // Constant from input
            thread_args[i].records_per_block = records_per_ideal_block; // Constant from input
            thread_args[i].current_num_active_blocks_for_merge = &shared_cnab_for_merge;
            thread_args[i].current_records_per_merged_block_for_merge = &shared_crpmb_for_merge;
        }

        printf("Main: About to wait on barrier_start_chunk for chunk %d processing.\n", num_chunks_processed + 1); fflush(stdout);
        pthread_barrier_wait(&barrier_start_chunk);
        printf("Main: Passed barrier_start_chunk for chunk %d processing. Calling sort_worker_thread for thread 0.\n", num_chunks_processed + 1); fflush(stdout);

        sort_worker_thread(&thread_args[0]);

        printf("Main: Thread 0 finished its work for chunk %d.\n", num_chunks_processed + 1); fflush(stdout);

        if (munmap(mapped_memory_base, mmap_length) == -1) {
            please_exit_flag_shared = 1;
            if (threads_param > 1) { /* ... attempt to wake/join ... */ }
            close(fd); errno_exit("munmap failed for a chunk");
        }
        processed_data_bytes += current_chunk_data_size;
        num_chunks_processed++;
        printf("Main: Chunk %d processed and unmapped.\n", num_chunks_processed); fflush(stdout);
    }
    printf("Main: Chunk processing loop finished. num_chunks_processed = %d\n", num_chunks_processed); fflush(stdout);

    please_exit_flag_shared = 1;
    printf("Main: Set please_exit_flag. please_exit_flag_shared = %d\n", please_exit_flag_shared); fflush(stdout);

    if (threads_param > 1) {
        printf("Main: About to wait on barrier_start_chunk for shutdown.\n"); fflush(stdout);
        int b_res = pthread_barrier_wait(&barrier_start_chunk);
        if (b_res != 0 && b_res != PTHREAD_BARRIER_SERIAL_THREAD) {
            fprintf(stderr, "Main: Error in shutdown barrier_wait: %s (errno: %d)\n", strerror(b_res), b_res);
        } else {
            printf("Main: Passed barrier_start_chunk for shutdown (barrier_wait result: %d).\n", b_res); fflush(stdout);
        }

        printf("Main: About to join threads.\n"); fflush(stdout);
        for (int i = 0; i < threads_param - 1; ++i) {
            printf("Main: Attempting to join worker_pthreads[%d] (actual thread_id %d)\n", i, thread_args[i+1].thread_id); fflush(stdout);
            if (pthread_join(worker_pthreads[i], NULL) != 0) {
                perror("Warning: Failed to join worker thread");
            }
            printf("Main: Successfully joined worker_pthreads[%d] (actual thread_id %d)\n", i, thread_args[i+1].thread_id); fflush(stdout);
        }
        printf("Main: All worker threads joined.\n"); fflush(stdout);
    }

    printf("Main: Destroying barrier_start_chunk.\n"); fflush(stdout);
    pthread_barrier_destroy(&barrier_start_chunk);
    printf("Main: Destroying barrier_blocks_sorted.\n"); fflush(stdout);
    pthread_barrier_destroy(&barrier_blocks_sorted);
    printf("Main: Destroying barrier_merge_pass.\n"); fflush(stdout);
    pthread_barrier_destroy(&barrier_merge_pass);
    printf("Main: Destroying task_mutex.\n"); fflush(stdout);
    pthread_mutex_destroy(&task_mutex);

    if (worker_pthreads) {
        printf("Main: Freeing worker_pthreads array.\n"); fflush(stdout);
        free(worker_pthreads); worker_pthreads = NULL;
    }
    printf("Main: Freeing thread_args array.\n"); fflush(stdout);
    free(thread_args); thread_args = NULL;

    if (close(fd) == -1) errno_exit("Failed to close file descriptor after chunk processing");
    printf("Main: File descriptor closed.\n"); fflush(stdout);

    if (num_chunks_processed > 1) {
        printf("Main: Performing final k-way merge of %d sorted chunks...\n", num_chunks_processed); fflush(stdout);
        if (k_way_merge_sorted_chunks(filename, total_records_in_file_from_header, num_chunks_processed, memsize) != 0) {
            error_exit("Final k-way merge failed.");
        }
        printf("Main: Final k-way merge completed successfully.\n"); fflush(stdout);
    } else {
        printf("Main: Single chunk processed, no final k-way merge needed.\n"); fflush(stdout);
    }

    printf("Main: Sorting of %s completed.\n", filename); fflush(stdout);
    return 0;
}

/*
 * Contains the actual sorting and merging logic for a chunk.
 */
void do_actual_processing_logic(thread_arg_t* data) {
    printf("Thread %d: Starting do_actual_processing_logic.\n", data->thread_id); fflush(stdout);

    // --- Phase 1: Sort individual blocks (qsort) ---
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
        printf("Thread 0: Initialized next_block_to_sort_idx to %d.\n", *(data->next_block_to_sort_idx)); fflush(stdout);
    }
    pthread_barrier_wait(data->barrier_blocks_sorted);

    while (1) {
        int block_idx_to_sort_competitively;
        pthread_mutex_lock(data->task_mutex);
        block_idx_to_sort_competitively = *(data->next_block_to_sort_idx);
        if (block_idx_to_sort_competitively < data->total_blocks_in_chunk) {
            (*(data->next_block_to_sort_idx))++;
        }
        pthread_mutex_unlock(data->task_mutex);

        if (block_idx_to_sort_competitively >= data->total_blocks_in_chunk) {
            break;
        }
        size_t block_start_record_idx = (size_t)block_idx_to_sort_competitively * data->records_per_block;
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
    printf("Thread %d: All blocks sorted. Starting merge phase.\n", data->thread_id); fflush(stdout);

    // --- Phase 2: Merge sorted blocks ---
    // Use pointers to shared merge state variables
    size_t* p_current_num_active_blocks = data->current_num_active_blocks_for_merge;
    size_t* p_current_records_per_block = data->current_records_per_merged_block_for_merge;

    index_record_t* temp_merge_buffer = NULL;
    if (data->total_blocks_in_chunk > 1 && data->num_records_in_chunk > 0) {
        size_t merge_buffer_size = data->num_records_in_chunk * sizeof(index_record_t);
        if (data->num_records_in_chunk > 0 && merge_buffer_size / data->num_records_in_chunk != sizeof(index_record_t)) {
            fprintf(stderr, "Thread %d: Merge buffer size calculation overflowed. Aborting thread.\n", data->thread_id); fflush(stderr);
            *(data->please_exit_flag) = 1;
            pthread_exit((void*)EXIT_FAILURE);
        }
        temp_merge_buffer = malloc(merge_buffer_size);
        if (!temp_merge_buffer) {
            fprintf(stderr, "Thread %d: Malloc failed for merge buffer (%zu bytes). Aborting thread.\n", data->thread_id, merge_buffer_size); fflush(stderr);
            *(data->please_exit_flag) = 1;
            pthread_exit((void*)EXIT_FAILURE);
        }
    }

    int merge_pass_count = 0;
    while (*p_current_num_active_blocks > 1) { // Use shared value for loop condition
        merge_pass_count++;
        int pairs_to_merge_this_pass = *p_current_num_active_blocks / 2;
        printf("Thread %d: Merge Pass %d: active_blocks=%zu, recs/block=%zu, pairs_to_merge=%d\n",
               data->thread_id, merge_pass_count, *p_current_num_active_blocks,
               *p_current_records_per_block, pairs_to_merge_this_pass); fflush(stdout);

               if (pairs_to_merge_this_pass == 0 && *p_current_num_active_blocks == 1) {
                   printf("Thread %d: Breaking merge loop early (pairs_to_merge_this_pass=0, active_blocks=1)\n", data->thread_id); fflush(stdout);
                   break;
               }

               if (data->thread_id == 0) {
                   *(data->next_merge_pair_idx) = 0;
               }
               printf("Thread %d: Merge Pass %d: Waiting at SYNC 1 (barrier_merge_pass).\n", data->thread_id, merge_pass_count); fflush(stdout);
               pthread_barrier_wait(data->barrier_merge_pass); // SYNC 1
               printf("Thread %d: Merge Pass %d: Passed SYNC 1.\n", data->thread_id, merge_pass_count); fflush(stdout);

               if (*p_current_num_active_blocks == 2 && pairs_to_merge_this_pass == 1) {
                   printf("Thread %d: Merge Pass %d: Final merge condition (active_blocks=2).\n", data->thread_id, merge_pass_count); fflush(stdout);
                   if (data->thread_id == 0) {
                       printf("Thread 0: Merge Pass %d: Performing final merge of two halves.\n", merge_pass_count); fflush(stdout);
                       index_record_t* block1_ptr = data->chunk_data_ptr;
                       size_t n1 = *p_current_records_per_block; // Use shared value
                       if (n1 > data->num_records_in_chunk) n1 = data->num_records_in_chunk;

                       index_record_t* block2_ptr = data->chunk_data_ptr + n1;
                       size_t n2 = 0;
                       if (data->num_records_in_chunk > n1) {
                           n2 = data->num_records_in_chunk - n1;
                       }

                       if (n1 > 0 && n2 > 0 && temp_merge_buffer) {
                           merge_sorted_arrays(block1_ptr, n1, block2_ptr, n2, temp_merge_buffer);
                           memcpy(data->chunk_data_ptr, temp_merge_buffer, (n1 + n2) * sizeof(index_record_t));
                       }
                   } else {
                       printf("Worker %d: Merge Pass %d: In final merge, doing nothing, proceeding to SYNC 2.\n", data->thread_id, merge_pass_count); fflush(stdout);
                   }
               } else {
                   printf("Thread %d: Merge Pass %d: Standard competitive merge (active_blocks=%zu).\n", data->thread_id, merge_pass_count, *p_current_num_active_blocks); fflush(stdout);
                   while (1) {
                       int my_pair_idx;
                       pthread_mutex_lock(data->task_mutex);
                       my_pair_idx = *(data->next_merge_pair_idx);
                       if (my_pair_idx < pairs_to_merge_this_pass) {
                           (*(data->next_merge_pair_idx))++;
                       }
                       pthread_mutex_unlock(data->task_mutex);

                       if (my_pair_idx >= pairs_to_merge_this_pass) {
                           break;
                       }
                       size_t base_offset_records = (size_t)my_pair_idx * 2 * (*p_current_records_per_block); // Use shared
                       index_record_t* output_ptr = data->chunk_data_ptr + base_offset_records;
                       index_record_t* block1_ptr = output_ptr;
                       index_record_t* block2_ptr = data->chunk_data_ptr + base_offset_records + (*p_current_records_per_block); // Use shared

                       size_t n1 = *p_current_records_per_block; // Use shared
                       size_t n2 = *p_current_records_per_block; // Use shared

                       if (base_offset_records + n1 > data->num_records_in_chunk) {
                           if (base_offset_records >= data->num_records_in_chunk) n1 = 0;
                           else n1 = data->num_records_in_chunk - base_offset_records;
                       }
                       size_t block2_start_offset_records = base_offset_records + (*p_current_records_per_block); // Use shared
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
               printf("Thread %d: Merge Pass %d: Waiting at SYNC 2 (barrier_merge_pass).\n", data->thread_id, merge_pass_count); fflush(stdout);
               pthread_barrier_wait(data->barrier_merge_pass); // SYNC 2
               printf("Thread %d: Merge Pass %d: Passed SYNC 2.\n", data->thread_id, merge_pass_count); fflush(stdout);

               if (data->thread_id == 0) {
                   *p_current_num_active_blocks /= 2;    // Update shared value
                   *p_current_records_per_block *= 2; // Update shared value
                   printf("Thread 0: Merge Pass %d: Updated state. New active_blocks=%zu, recs/block=%zu.\n",
                          merge_pass_count, *p_current_num_active_blocks, *p_current_records_per_block); fflush(stdout);
               }
               printf("Thread %d: Merge Pass %d: Waiting at SYNC 3 (barrier_merge_pass).\n", data->thread_id, merge_pass_count); fflush(stdout);
               pthread_barrier_wait(data->barrier_merge_pass); // SYNC 3
               printf("Thread %d: Merge Pass %d: Passed SYNC 3.\n", data->thread_id, merge_pass_count); fflush(stdout);
    }

    printf("Thread %d: Exited merge loop.\n", data->thread_id); fflush(stdout);

    if (temp_merge_buffer) {
        free(temp_merge_buffer);
        temp_merge_buffer = NULL;
    }
    printf("Thread %d: Finished do_actual_processing_logic.\n", data->thread_id); fflush(stdout);
}

/*
 * Worker thread function. Loops to process chunks until exit is signaled.
 */
void* sort_worker_thread(void* arg) {
    thread_arg_t* data = (thread_arg_t*)arg;
    printf("Thread %d: Entered sort_worker_thread.\n", data->thread_id); fflush(stdout);

    if (data->thread_id != 0) { // Worker threads loop
        while (1) {
            printf("Worker %d: Reached top of while(1) loop. About to wait on barrier_start_chunk.\n", data->thread_id); fflush(stdout);
            int b_res = pthread_barrier_wait(data->barrier_start_chunk);
            if (b_res != 0 && b_res != PTHREAD_BARRIER_SERIAL_THREAD) {
                fprintf(stderr, "Worker %d: Error in barrier_start_chunk wait: %s (errno: %d)\n", data->thread_id, strerror(b_res), b_res);
            } else {
                // printf("Worker %d: Passed barrier_start_chunk (barrier_wait result: %d).\n", data->thread_id, b_res); fflush(stdout);
            }

            if (*(data->please_exit_flag)) {
                printf("Worker %d: please_exit_flag is set (%d). Exiting thread.\n", data->thread_id, *(data->please_exit_flag)); fflush(stdout);
                pthread_exit(NULL);
            }
            // printf("Worker %d: please_exit_flag is NOT set (%d). Calling do_actual_processing_logic.\n", data->thread_id, *(data->please_exit_flag)); fflush(stdout);
            do_actual_processing_logic(data);
        }
    } else { // This is thread_id == 0 (main thread executing its part of the work for a chunk)
        do_actual_processing_logic(data);
        return NULL;
    }
    return NULL; // Should be unreachable for workers
}


/*
 * Merges two sorted arrays of index_record_t into an output buffer.
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

typedef struct {
    index_record_t record;
    int chunk_idx;
} heap_node_t;

int compare_heap_nodes(const void* a, const void* b) {
    heap_node_t* node_a = (heap_node_t*)a;
    heap_node_t* node_b = (heap_node_t*)b;
    return compare_index_records(&node_a->record, &node_b->record);
}

/*
 * Performs a k-way merge of sorted chunks in a file.
 */
int k_way_merge_sorted_chunks(const char* original_filename, uint64_t total_records_in_file_from_header, int num_chunks, size_t chunk_data_size_bytes) {
    if (num_chunks <= 1) return 0;

    printf("k_way_merge: Starting for %d chunks.\n", num_chunks); fflush(stdout);

    char temp_filename[PATH_MAX];
    const char* tmpdir_env = getenv("TMPDIR");
    const char* tmp_dir_path = (tmpdir_env && strlen(tmpdir_env) > 0) ? tmpdir_env : "/tmp";

    char* original_filename_copy = strdup(original_filename);
    if (!original_filename_copy) {
        perror("k_way_merge: strdup original_filename");
        return -1;
    }
    char* original_basename_ptr = strrchr(original_filename_copy, '/');
    char* effective_basename = original_basename_ptr ? original_basename_ptr + 1 : original_filename_copy;

    snprintf(temp_filename, PATH_MAX, "%s/%s.sorttmp.%d", tmp_dir_path, effective_basename, getpid());
    free(original_filename_copy);

    printf("k_way_merge: Temp file will be %s\n", temp_filename); fflush(stdout);

    FILE* original_fp = NULL;
    FILE* temp_fp = NULL;
    index_record_t** chunk_buffers = NULL;
    size_t* current_record_idx_in_buffer = NULL;
    size_t* records_in_buffer = NULL;
    uint64_t* records_read_from_chunk_file = NULL;
    heap_node_t* active_candidates = NULL;
    int ret_val = -1;

    original_fp = fopen(original_filename, "rb");
    if (!original_fp) { perror("k_way_merge: fopen original_fp"); goto cleanup_k_way; }

    temp_fp = fopen(temp_filename, "wb");
    if (!temp_fp) { perror("k_way_merge: fopen temp_fp"); goto cleanup_k_way; }

    uint64_t header_record_count;
    if (fseek(original_fp, 0, SEEK_SET) != 0) { perror("k_way_merge: fseek header original"); goto cleanup_k_way; }
    if (fread(&header_record_count, sizeof(header_record_count), 1, original_fp) != 1) {
        perror("k_way_merge: fread header from original"); goto cleanup_k_way;
    }
    if (header_record_count != total_records_in_file_from_header) {
        fprintf(stderr, "k_way_merge: Header record count mismatch. Expected %lu, file has %lu\n",
                (unsigned long)total_records_in_file_from_header, (unsigned long)header_record_count);
        goto cleanup_k_way;
    }
    if (fwrite(&header_record_count, sizeof(header_record_count), 1, temp_fp) != 1) {
        perror("k_way_merge: fwrite header to temp"); goto cleanup_k_way;
    }

    chunk_buffers = malloc(num_chunks * sizeof(index_record_t*));
    if (chunk_buffers) { for(int i=0; i<num_chunks; ++i) chunk_buffers[i] = NULL; } else { goto cleanup_k_way; }

    current_record_idx_in_buffer = calloc(num_chunks, sizeof(size_t));
    records_in_buffer = calloc(num_chunks, sizeof(size_t));
    records_read_from_chunk_file = calloc(num_chunks, sizeof(uint64_t));
    active_candidates = malloc(num_chunks * sizeof(heap_node_t));

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
        uint64_t records_in_this_physical_chunk = records_per_chunk_nominal;
        if (num_to_read > records_in_this_physical_chunk) num_to_read = records_in_this_physical_chunk;

        if (num_to_read > 0) {
            records_in_buffer[i] = fread(chunk_buffers[i], sizeof(index_record_t), num_to_read, original_fp);
            if (ferror(original_fp)) { perror("k_way_merge: fread initial fill"); goto cleanup_k_way; }
            current_record_idx_in_buffer[i] = 0;
            records_read_from_chunk_file[i] = records_in_buffer[i];
        } else {
            records_in_buffer[i] = 0;
        }
    }

    uint64_t records_written_to_temp = 0;
    while(records_written_to_temp < total_records_in_file_from_header) {
        int active_candidate_count = 0;
        for (int i = 0; i < num_chunks; ++i) {
            if (current_record_idx_in_buffer[i] < records_in_buffer[i]) {
                active_candidates[active_candidate_count].record = chunk_buffers[i][current_record_idx_in_buffer[i]];
                active_candidates[active_candidate_count].chunk_idx = i;
                active_candidate_count++;
            }
        }

        if (active_candidate_count == 0) {
            if (records_written_to_temp < total_records_in_file_from_header) {
                fprintf(stderr, "k_way_merge: No active candidates but not all records written (%lu of %lu).\n",
                        (unsigned long)records_written_to_temp, (unsigned long)total_records_in_file_from_header);
            }
            break;
        }

        int min_idx_in_candidates = 0;
        for (int i = 1; i < active_candidate_count; ++i) {
            if (compare_heap_nodes(&active_candidates[i], &active_candidates[min_idx_in_candidates]) < 0) {
                min_idx_in_candidates = i;
            }
        }
        heap_node_t min_node = active_candidates[min_idx_in_candidates];

        if (fwrite(&min_node.record, sizeof(index_record_t), 1, temp_fp) != 1) {
            perror("k_way_merge: fwrite record"); goto cleanup_k_way;
        }
        records_written_to_temp++;

        int source_chunk_idx = min_node.chunk_idx;
        current_record_idx_in_buffer[source_chunk_idx]++;

        if (current_record_idx_in_buffer[source_chunk_idx] >= records_in_buffer[source_chunk_idx]) {
            if (records_read_from_chunk_file[source_chunk_idx] < records_per_chunk_nominal) {
                off_t next_read_offset = (off_t)file_data_start_offset_in_original +
                (source_chunk_idx * chunk_data_size_bytes) +
                (records_read_from_chunk_file[source_chunk_idx] * sizeof(index_record_t));
                if (fseeko(original_fp, next_read_offset, SEEK_SET) != 0) { perror("k_way_merge: fseeko refill"); goto cleanup_k_way; }

                size_t num_to_read = K_WAY_BUFFER_RECS;
                uint64_t remaining_in_physical_chunk = records_per_chunk_nominal - records_read_from_chunk_file[source_chunk_idx];
                if (num_to_read > remaining_in_physical_chunk) num_to_read = remaining_in_physical_chunk;

                if (num_to_read > 0) {
                    records_in_buffer[source_chunk_idx] = fread(chunk_buffers[source_chunk_idx], sizeof(index_record_t), num_to_read, original_fp);
                    if (ferror(original_fp)) { perror("k_way_merge: fread refill"); goto cleanup_k_way; }
                    current_record_idx_in_buffer[source_chunk_idx] = 0;
                    records_read_from_chunk_file[source_chunk_idx] += records_in_buffer[source_chunk_idx];
                } else {
                    records_in_buffer[source_chunk_idx] = 0;
                }
            } else {
                records_in_buffer[source_chunk_idx] = 0;
            }
        }
    }

    if (records_written_to_temp != total_records_in_file_from_header) {
        fprintf(stderr, "k_way_merge: Mismatch in record count after loop. Expected %lu, wrote %lu.\n",
                (unsigned long)total_records_in_file_from_header, (unsigned long)records_written_to_temp);
        goto cleanup_k_way;
    }

    printf("k_way_merge: All records written to temp file. Flushing and closing.\n"); fflush(stdout);
    if (fflush(temp_fp) != 0) { perror("k_way_merge: fflush temp_fp"); goto cleanup_k_way; }
    if (fclose(temp_fp) != 0) { temp_fp = NULL; perror("k_way_merge: fclose temp_fp"); goto cleanup_k_way; }
    temp_fp = NULL;

    fclose(original_fp);
    original_fp = NULL;

    printf("k_way_merge: Renaming %s to %s.\n", temp_filename, original_filename); fflush(stdout);
    if (remove(original_filename) != 0) {
        perror("k_way_merge: remove original_filename");
        goto cleanup_k_way_no_remove_temp;
    }
    if (rename(temp_filename, original_filename) != 0) {
        perror("k_way_merge: rename temp_filename to original");
        goto cleanup_k_way_no_remove_temp;
    }

    ret_val = 0;
    printf("k_way_merge: Successfully completed.\n"); fflush(stdout);
    goto cleanup_k_way_no_remove_temp;

    cleanup_k_way:
    if (ret_val != 0) {
        printf("k_way_merge: Cleaning up due to failure (ret_val=%d).\n", ret_val); fflush(stdout);
        remove(temp_filename);
    }
    cleanup_k_way_no_remove_temp:
    if (temp_fp) fclose(temp_fp);
    if (original_fp) fclose(original_fp);
    if (chunk_buffers) {
        for (int i = 0; i < num_chunks; ++i) { if (chunk_buffers[i]) free(chunk_buffers[i]); }
        free(chunk_buffers);
    }
    free(current_record_idx_in_buffer);
    free(records_in_buffer);
    free(records_read_from_chunk_file);
    free(active_candidates);
    return ret_val;
}
