# Makefile for sort_index project

# Compiler and flags
CC = gcc
CFLAGS_COMMON = -std=c11 -pedantic -W -Wall -Wextra
CFLAGS_DEBUG = -g -ggdb $(CFLAGS_COMMON)
CFLAGS_RELEASE = -O2 $(CFLAGS_COMMON) -Werror

LDFLAGS = -pthread -lm # -lm for math functions like floor, -pthread for pthreads

# Default mode
MODE ?= debug

# Define output directory based on MODE
OUT_DIR = build/$(MODE)

ifeq ($(MODE),debug)
    CFLAGS = $(CFLAGS_DEBUG)
else ifeq ($(MODE),release)
    CFLAGS = $(CFLAGS_RELEASE)
else
    $(error Invalid MODE: $(MODE). Use 'debug' or 'release'.)
endif

# Source files and executables
SRC_DIR = src

TARGET_GEN = $(OUT_DIR)/gen
TARGET_VIEW = $(OUT_DIR)/view
TARGET_SORT_INDEX = $(OUT_DIR)/sort_index
TARGET_VERIFY = $(OUT_DIR)/verify_sorted

GEN_SRCS = $(SRC_DIR)/gen.c $(SRC_DIR)/utils.c
VIEW_SRCS = $(SRC_DIR)/view.c $(SRC_DIR)/utils.c
SORT_INDEX_SRCS = $(SRC_DIR)/sort_index.c $(SRC_DIR)/utils.c
VERIFY_SRCS = $(SRC_DIR)/verify_sorted.c $(SRC_DIR)/utils.c

# Generate object file names by replacing src/ prefix with $(OUT_DIR)/ and .c with .o
GEN_OBJS = $(patsubst $(SRC_DIR)/%.c,$(OUT_DIR)/%.o,$(filter %.c,$(GEN_SRCS)))
VIEW_OBJS = $(patsubst $(SRC_DIR)/%.c,$(OUT_DIR)/%.o,$(filter %.c,$(VIEW_SRCS)))
SORT_INDEX_OBJS = $(patsubst $(SRC_DIR)/%.c,$(OUT_DIR)/%.o,$(filter %.c,$(SORT_INDEX_SRCS)))
VERIFY_OBJS = $(patsubst $(SRC_DIR)/%.c,$(OUT_DIR)/%.o,$(filter %.c,$(VERIFY_SRCS)))


# Targets
all: $(TARGET_GEN) $(TARGET_VIEW) $(TARGET_SORT_INDEX) $(TARGET_VERIFY)

$(TARGET_GEN): $(GEN_OBJS) | $(OUT_DIR)
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) $(filter %.o,$^) -o $@ $(LDFLAGS)

$(TARGET_VIEW): $(VIEW_OBJS) | $(OUT_DIR)
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) $(filter %.o,$^) -o $@ $(LDFLAGS)

$(TARGET_SORT_INDEX): $(SORT_INDEX_OBJS) | $(OUT_DIR)
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) $(filter %.o,$^) -o $@ $(LDFLAGS)

$(TARGET_VERIFY): $(VERIFY_OBJS) | $(OUT_DIR)
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) $(filter %.o,$^) -o $@ $(LDFLAGS)

# Rule to compile .c files from SRC_DIR into .o files in OUT_DIR
$(OUT_DIR)/%.o: $(SRC_DIR)/%.c | $(OUT_DIR)
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -c $< -o $@

$(OUT_DIR):
	mkdir -p $(OUT_DIR)

clean:
	rm -rf build

.PHONY: all clean
