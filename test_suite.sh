#!/usr/bin/env bash

# Exit on error, treat unset variables as an error
set -euo pipefail

# --- Configuration ---
BUILD_DIR="./build"
GEN_PROG="${BUILD_DIR}/gen"
VIEW_PROG="${BUILD_DIR}/view"
SORT_PROG="${BUILD_DIR}/sort_index"

REC_SIZE=16 # sizeof(index_record_t)

# --- Colors for output ---
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# --- Helper Functions ---
info() {
    echo -e "${GREEN}[INFO] ${1}${NC}"
}

warn() {
    echo -e "${YELLOW}[WARN] ${1}${NC}"
}

error() {
    echo -e "${RED}[ERROR] ${1}${NC}"
}

pass() {
    echo -e "${GREEN}[PASS] ${1}${NC}"
}

fail() {
    echo -e "${RED}[FAIL] ${1}${NC}"
    # exit 1 # Optionally exit immediately on first failure
}

run_cmd() {
    local cmd_desc="$1"
    shift
    info "Running: $cmd_desc ($@)"
    if "$@"; then
        pass "$cmd_desc"
        return 0
    else
        local exit_code=$?
        fail "$cmd_desc (Exit code: $exit_code)"
        return "$exit_code"
    fi
}

run_cmd_allow_fail() {
    local cmd_desc="$1"
    shift
    info "Running (failure expected for test): $cmd_desc ($@)"
    if ! "$@"; then
        pass "$cmd_desc (failed as expected)"
        return 0
    else
        fail "$cmd_desc (succeeded but failure was expected)"
        return 1
    fi
}

check_file_exists() {
    local filename="$1"
    if [ -f "$filename" ]; then
        pass "File '$filename' exists."
        return 0
    else
        fail "File '$filename' does not exist."
        return 1
    fi
}

check_sorted() {
    local filename="$1"
    info "Checking if '$filename' is sorted..."
    # Skip header (5 lines), then check if the first column (time_mark) is sorted.
    # NF == 4 ensures we only process valid data lines.
    # The awk script will exit with 0 if sorted, 1 if not.
    local awk_script='
        BEGIN { prev_val = -1; data_lines_processed = 0; errors = 0; }
        NR > 5 { # Skip header lines
            if (NF == 4) { # Expecting "time_mark | rec_no"
                data_lines_processed++;
                current_val = $1;
                # Perform numeric comparison for time_mark
                if (prev_val != -1 && (current_val + 0 < prev_val + 0)) { # Ensure numeric comparison
                    print "Sort error at data line " data_lines_processed ": " current_val " < " prev_val > "/dev/stderr";
                    errors++;
                }
                prev_val = current_val;
            } else if (NF > 0 && length($0) > 0 && $0 !~ /^[[:space:]]*$/) { # Non-empty, non-whitespace line that is not a data line
                # print "Warning: Unexpected line format at input line " NR ": " $0 > "/dev/stderr";
            }
        }
        END {
            if (errors > 0) {
                print "File " FILENAME " is NOT sorted. Found " errors " errors." > "/dev/stderr";
                exit 1;
            } else if (data_lines_processed == 0 && NR > 5) {
                # This means we read past the header but found no valid data lines.
                # Could be an empty data file (after header) or view output issue.
                # For a file with 0 records (only header), this is fine.
                print "File " FILENAME " has no data records after header to check for sorting." > "/dev/stderr";
                exit 0; # Consider this sorted (vacuously true) or handle as warning
            } else if (data_lines_processed == 0 && NR <= 5) {
                # This means the file had 5 or fewer lines (e.g. only header, or less)
                print "File " FILENAME " is too short to contain data records after header." > "/dev/stderr";
                exit 0; # Consider this sorted
            } else {
                # print "File " FILENAME " appears sorted." > "/dev/stderr"; # Optional success message to stderr
                exit 0;
            }
        }'

    if "$VIEW_PROG" "$filename" | awk "$awk_script"; then
        pass "File '$filename' is sorted (or empty/too short)."
        return 0
    else
        fail "File '$filename' is NOT sorted or check script failed."
        return 1
    fi
}


# --- Main Test Logic ---
info "Starting Test Suite..."

# 0. Prerequisites
info "Checking prerequisites..."
PAGE_SIZE=$(getconf PAGESIZE)
if [ -z "$PAGE_SIZE" ] || [ "$PAGE_SIZE" -le 0 ]; then
    error "Could not determine a valid PAGE_SIZE."
    exit 1
fi
info "PAGE_SIZE: $PAGE_SIZE"

CORE_COUNT=$(nproc --all 2>/dev/null || sysconf _SC_NPROCESSORS_ONLN 2>/dev/null || echo 1) # Get core count, default to 1
info "CORE_COUNT (k): $CORE_COUNT"

for prog in "$GEN_PROG" "$VIEW_PROG" "$SORT_PROG"; do
    if [ ! -x "$prog" ]; then
        error "Program '$prog' not found or not executable. Please build first."
        exit 1
    fi
done
pass "Prerequisites met."

# --- Cleanup old test files ---
rm -f ./*.dat ./*.txt

# --- Phase 2: gen Program Testing ---
info "--- Testing 'gen' program ---"
SMALL_RECORDS=256
run_cmd "gen: small file" "$GEN_PROG" "$SMALL_RECORDS" "small_index.dat" && check_file_exists "small_index.dat"

# For single chunk sort: file data size <= memsize
MEMSIZE_SCS_TARGET=$((2 * PAGE_SIZE)) # Target memsize for sort_index
RECORDS_SCS=256 # 256 * 16 = 4096.
# If PAGE_SIZE is 4096, MEMSIZE_SCS_TARGET will be 8192. File data (4096) fits.
# The sort_index constraint is total_data_size % memsize == 0.
# So, if we use MEMSIZE_SCS_TARGET=8192, then 4096 % 8192 != 0, which your program flags.
# For a "single chunk" test where the file is smaller than a typical memsize,
# the most straightforward way to satisfy your program's current strict check is
# to set memsize == total_data_size.
# This also means total_data_size must be a multiple of PAGE_SIZE for memsize to be valid.

RECORDS_FOR_PAGE_ALIGNED_SCS=$(( (PAGE_SIZE / REC_SIZE) * 1 )) # e.g. 4096/16 = 256 records
# Ensure RECORDS_FOR_PAGE_ALIGNED_SCS is a multiple of 256 for gen
if [ "$((RECORDS_FOR_PAGE_ALIGNED_SCS % 256))" -ne 0 ]; then
    RECORDS_FOR_PAGE_ALIGNED_SCS=$(( ((RECORDS_FOR_PAGE_ALIGNED_SCS + 255) / 256) * 256 ))
fi
if [ "$RECORDS_FOR_PAGE_ALIGNED_SCS" -eq 0 ]; then RECORDS_FOR_PAGE_ALIGNED_SCS=256; fi

info "Generating single_chunk_sort_target.dat with $RECORDS_FOR_PAGE_ALIGNED_SCS records."
run_cmd "gen: page-aligned single_chunk_sort_target.dat" "$GEN_PROG" "$RECORDS_FOR_PAGE_ALIGNED_SCS" "single_chunk_sort_target.dat"
MEMSIZE_FOR_SCS_EXACT=$((RECORDS_FOR_PAGE_ALIGNED_SCS * REC_SIZE))
info "MEMSIZE_FOR_SCS_EXACT (for single chunk sort test, must be multiple of PAGE_SIZE): $MEMSIZE_FOR_SCS_EXACT"
if [ "$((MEMSIZE_FOR_SCS_EXACT % PAGE_SIZE))" -ne 0 ]; then
    error "Cannot proceed: Data size for single chunk test ($MEMSIZE_FOR_SCS_EXACT) is not a multiple of PAGE_SIZE ($PAGE_SIZE)."
    # This shouldn't happen if RECORDS_FOR_PAGE_ALIGNED_SCS is calculated correctly based on PAGE_SIZE.
    exit 1
fi


# For multi-chunk sort: total_data_size % memsize == 0
MEMSIZE_MCS=$((2 * PAGE_SIZE)) # Example: 8192 if PAGE_SIZE=4096
RECORDS_PER_MEMSIZE_CHUNK=$((MEMSIZE_MCS / REC_SIZE)) # Example: 8192 / 16 = 512
NUM_CHUNKS_TARGET=2
RECORDS_MCS_UNALIGNED=$((NUM_CHUNKS_TARGET * RECORDS_PER_MEMSIZE_CHUNK)) # Example: 2 * 512 = 1024
RECORDS_MCS=$(( (RECORDS_MCS_UNALIGNED / 256) * 256 ))
if [ "$RECORDS_MCS" -eq 0 ] && [ "$RECORDS_MCS_UNALIGNED" -gt 0 ]; then
    RECORDS_MCS=256
    warn "Adjusted RECORDS_MCS to $RECORDS_MCS as initial calculation was < 256."
fi
info "MEMSIZE_MCS (for multi-chunk sort test): $MEMSIZE_MCS"
info "RECORDS_PER_MEMSIZE_CHUNK: $RECORDS_PER_MEMSIZE_CHUNK"
info "RECORDS_MCS (multiple of 256, for multi-chunk sort): $RECORDS_MCS"

if [ "$RECORDS_MCS" -eq 0 ]; then
    error "Calculated RECORDS_MCS is 0. Cannot proceed with multi-chunk test. Check PAGE_SIZE and calculations."
    exit 1
fi
run_cmd "gen: multi_chunk_sort_target.dat" "$GEN_PROG" "$RECORDS_MCS" "multi_chunk_sort_target.dat" && check_file_exists "multi_chunk_sort_target.dat"

run_cmd_allow_fail "gen: no args" "$GEN_PROG"
run_cmd_allow_fail "gen: too few args" "$GEN_PROG" "256"
run_cmd_allow_fail "gen: non-numeric records" "$GEN_PROG" "abc" "test.dat"
run_cmd_allow_fail "gen: zero records" "$GEN_PROG" "0" "test.dat"
run_cmd_allow_fail "gen: not multiple of 256" "$GEN_PROG" "257" "test.dat"

# --- Phase 3: view Program Testing ---
info "--- Testing 'view' program ---"
run_cmd "view: small_index.dat" "$VIEW_PROG" "small_index.dat"
run_cmd_allow_fail "view: no args" "$VIEW_PROG"
run_cmd_allow_fail "view: non_existent_file.dat" "$VIEW_PROG" "non_existent_file.dat"

# --- Phase 4: sort_index Program Testing ---
info "--- Testing 'sort_index' program ---"
BLOCKS_VALID=16
THREADS_VALID="$CORE_COUNT"

run_cmd_allow_fail "sort: no args" "$SORT_PROG"
run_cmd_allow_fail "sort: memsize not multiple of page size" "$SORT_PROG" "$((PAGE_SIZE + 1))" "$BLOCKS_VALID" "$THREADS_VALID" "small_index.dat"
run_cmd_allow_fail "sort: blocks not power of two" "$SORT_PROG" "$PAGE_SIZE" "15" "$THREADS_VALID" "small_index.dat"

# Test blocks < 4*threads
# Ensure threads > 1 for this test if blocks=4. If CORE_COUNT=1, use 2 threads.
TEST_THREADS_FOR_BLOCK_CHECK=$((CORE_COUNT > 1 ? CORE_COUNT : 2))
run_cmd_allow_fail "sort: blocks < 4*threads" "$SORT_PROG" "$PAGE_SIZE" "4" "$TEST_THREADS_FOR_BLOCK_CHECK" "small_index.dat"

run_cmd_allow_fail "sort: non_existent_file.dat" "$SORT_PROG" "$PAGE_SIZE" "$BLOCKS_VALID" "$THREADS_VALID" "non_existent_file.dat"

# Test: total_data_size not multiple of memsize
# Use single_chunk_sort_target.dat (data size = MEMSIZE_FOR_SCS_EXACT)
# And a memsize that is PAGE_SIZE aligned but not equal to MEMSIZE_FOR_SCS_EXACT (unless they are the same)
MEMSIZE_MISMATCH_PARAM=$((MEMSIZE_FOR_SCS_EXACT + PAGE_SIZE)) # This is page aligned
if [ "$((MEMSIZE_FOR_SCS_EXACT % MEMSIZE_MISMATCH_PARAM))" -ne 0 ]; then
    run_cmd_allow_fail "sort: data size not multiple of memsize" "$SORT_PROG" "$MEMSIZE_MISMATCH_PARAM" "$BLOCKS_VALID" "$THREADS_VALID" "single_chunk_sort_target.dat"
else
    warn "Skipping 'data size not multiple of memsize' test as MEMSIZE_FOR_SCS_EXACT ($MEMSIZE_FOR_SCS_EXACT) was a multiple of MEMSIZE_MISMATCH_PARAM ($MEMSIZE_MISMATCH_PARAM)."
fi


# Single Chunk Sort
info "--- sort: Single Chunk Sort Test (using memsize = data_size) ---"
# File: single_chunk_sort_target.dat (data size = MEMSIZE_FOR_SCS_EXACT)
# Memsize for sort_index: MEMSIZE_FOR_SCS_EXACT
BLOCKS_SCS="$BLOCKS_VALID"
THREADS_SCS="$THREADS_VALID"
if [ "$BLOCKS_SCS" -lt "$((4 * THREADS_SCS))" ]; then # Adjust blocks if needed
    BLOCKS_SCS=$((4 * THREADS_SCS)); power=1; while [ "$power" -lt "$BLOCKS_SCS" ]; do power=$((power * 2)); done; BLOCKS_SCS="$power"
    info "Adjusted BLOCKS_SCS to $BLOCKS_SCS for single chunk sort"
fi
run_cmd "sort: single chunk" "$SORT_PROG" "$MEMSIZE_FOR_SCS_EXACT" "$BLOCKS_SCS" "$THREADS_SCS" "single_chunk_sort_target.dat" \
&& check_sorted "single_chunk_sort_target.dat"


# Multi-Chunk Sort
info "--- sort: Multi-Chunk Sort Test ---"
# File: multi_chunk_sort_target.dat (RECORDS_MCS records)
# Memsize: MEMSIZE_MCS
BLOCKS_MCS="$BLOCKS_VALID"
THREADS_MCS="$THREADS_VALID"
if [ "$BLOCKS_MCS" -lt "$((4 * THREADS_MCS))" ]; then # Adjust blocks if needed
    BLOCKS_MCS=$((4 * THREADS_MCS)); power=1; while [ "$power" -lt "$BLOCKS_MCS" ]; do power=$((power * 2)); done; BLOCKS_MCS="$power"
    info "Adjusted BLOCKS_MCS to $BLOCKS_MCS for multi chunk sort"
fi
run_cmd "sort: multi-chunk ($NUM_CHUNKS_TARGET chunks)" "$SORT_PROG" "$MEMSIZE_MCS" "$BLOCKS_MCS" "$THREADS_MCS" "multi_chunk_sort_target.dat" \
&& check_sorted "multi_chunk_sort_target.dat"

# Varying Threads (on multi_chunk_sort_target.dat)
info "--- sort: Varying Threads Test (on multi_chunk_sort_target.dat) ---"
cp "multi_chunk_sort_target.dat" "multi_chunk_temp_1.dat"
run_cmd "sort: multi-chunk, 1 thread" "$SORT_PROG" "$MEMSIZE_MCS" "$BLOCKS_MCS" "1" "multi_chunk_temp_1.dat" \
&& check_sorted "multi_chunk_temp_1.dat"

if [ "$((2 * CORE_COUNT))" -le "$((8 * CORE_COUNT))" ] && [ "$((2 * CORE_COUNT))" -ge "$CORE_COUNT" ]; then
    THREADS_2K=$((2 * CORE_COUNT))
    BLOCKS_2K="$BLOCKS_MCS"
    if [ "$BLOCKS_2K" -lt "$((4 * THREADS_2K))" ]; then
        BLOCKS_2K=$((4 * THREADS_2K)); power=1; while [ "$power" -lt "$BLOCKS_2K" ]; do power=$((power * 2)); done; BLOCKS_2K="$power"
        info "Adjusted BLOCKS_2K to $BLOCKS_2K for 2k threads test"
    fi
    cp "multi_chunk_sort_target.dat" "multi_chunk_temp_2k.dat"
    run_cmd "sort: multi-chunk, 2k threads" "$SORT_PROG" "$MEMSIZE_MCS" "$BLOCKS_2K" "$THREADS_2K" "multi_chunk_temp_2k.dat" \
    && check_sorted "multi_chunk_temp_2k.dat"
else
    warn "Skipping 2k threads test as 2*CORE_COUNT ($((2*CORE_COUNT))) is not in valid range [k ($CORE_COUNT), 8k ($((8*CORE_COUNT)))]."
fi


# Empty File Test
info "--- sort: Empty File Test ---"
echo -ne '\0\0\0\0\0\0\0\0' > zero_records.dat # 8 zero bytes for uint64_t records = 0
run_cmd "sort: zero_records.dat" "$SORT_PROG" "$PAGE_SIZE" "$BLOCKS_VALID" "$THREADS_VALID" "zero_records.dat"
info "Verify manually: sort_index output for zero_records.dat should indicate no records to sort."


# --- Phase 5: Resource Management (Valgrind) ---
info "--- Valgrind Test (on multi-chunk sort) ---"
if command -v valgrind &> /dev/null; then
    cp "multi_chunk_sort_target.dat" "multi_chunk_valgrind.dat" # Use a fresh copy
    info "Running Valgrind... this may take a while."
    # Run valgrind and capture all output (stdout and stderr) to a file
    if valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes \
        "$SORT_PROG" "$MEMSIZE_MCS" "$BLOCKS_MCS" "$THREADS_MCS" "multi_chunk_valgrind.dat" > valgrind_output.txt 2>&1; then
        pass "Valgrind execution completed without Valgrind itself crashing."
    else
        # This 'else' block means Valgrind itself might have had an issue, or the program exited with non-zero.
        # The valgrind_output.txt will contain the details.
        warn "Valgrind execution finished (program might have errored, or Valgrind found issues). Check valgrind_output.txt."
    fi

    # Check the content of valgrind_output.txt for errors
    if grep -E "definitely lost: 0 bytes in 0 blocks|definitely lost: 0 bytes" valgrind_output.txt > /dev/null && \
       ! grep -q "Invalid read" valgrind_output.txt && \
       ! grep -q "Invalid write" valgrind_output.txt && \
       ! grep -q "Conditional jump or move depends on uninitialised value" valgrind_output.txt; then # Added uninitialized value check
        pass "Valgrind: No critical leaks or memory errors detected."
    else
        fail "Valgrind: Leaks, memory errors, or uninitialized values detected. Check valgrind_output.txt."
    fi
else
    warn "Valgrind not found. Skipping Valgrind test."
fi

# --- Phase 6: Cleanup ---
info "--- Cleaning up test files ---"
rm -f ./*.dat valgrind_output.txt multi_chunk_temp_*.dat
pass "Cleanup complete."

info "Test Suite Finished."
