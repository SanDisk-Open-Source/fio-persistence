//-----------------------------------------------------------------------------
// Copyright (c) 2012-2014 Fusion-io, Inc.
// Copyright (c) 2014-2015 SanDisk Corp. or its affiliates.
// Copyright (c) 2015-2016 Western Digital Corporation or its affiliates.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// * Redistributions of source code must retain the above copyright notice,
//   this list of conditions and the following disclaimer.
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
// * Neither the name of the Western Digital Corp. nor the names of its contributors
//   may be used to endorse or promote products derived from this software
//   without specific prior written permission.
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
// OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
// OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//-----------------------------------------------------------------------------

#include <cstdlib>
#include <stdint.h>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <queue>
#include <fcntl.h> /* for O_RDWR */
#include <unistd.h> /* For open(), create() */
#include <pthread.h>
#ifdef LIBAIO_SUPPORTED
#include <libaio.h>
#endif

#include <json/json.h>

#include "indexer.h"

#if defined(__linux__)
#include <linux/ioctl.h>
#endif

#if defined(__OSX__)
#include <sys/disk.h>
#include <sys/ioctl.h>
#endif

#define IO_VECTOR_LIMIT     32
#define IO_VECTOR_MAX_SIZE  (1ULL << 17)//Max. IO size per IOV in bytes (128K)
#define IO_SIZE_MAX         (IO_VECTOR_LIMIT * IO_VECTOR_MAX_SIZE)
#define VSL_IOV_WRITE   0x01
#define VSL_IOV_TRIM    0x02

#ifndef BLKDISCARD
#define BLKDISCARD  _IO(0x12,119)
#endif
#ifndef S_IRUSR
#define S_IRUSR 0400
#endif
#ifndef S_IWUSR
#define S_IWUSR 0600
#endif

#define FAULT_MAX 10
#define EXPECTED_EXTENSION ".expected"
#define OBSERVED_EXTENSION ".observed"

#define SPARSE_THRESHOLD ((uint64_t) 1)<<34
#define DEFAULT_RANGE_RATIO 1.0
#define DEFAULT_RANGE_RATIO_SPARSE 1.5

#define NO_FAULTS_FOUND          0
#define FAULTS_FOUND             1
#define INTERRUPTED              2
#define LOGIC_ERROR              4
#define IO_ERROR                 8
#define FAULTS_FOUND_PREVIOUSLY 16

#define WRITE_INDEFINITELY (~ (uint64_t)0)
#define COMPLETE_PASS (~(uint64_t)0 - 1)

#define NO_FIRST_SECTOR_DEFINED (~(uint64_t)0)

#define MAX_IO_DEPTH 8192
#define DEFAULT_SECTOR_SIZE 512
#define REAP_TIMEOUT 10

// These impact performance, so thus hid behind #defines
#define VERBOSE_PRINTS 0
#define STAMP_SECTORS 1
#define STAMP_MAX_LENGTH 128
// The byte frequency in which stamps will be placed in sectors (byte 0 begins first stamp)
#define STAMP_SECTOR_FREQ 1024
#define C_ASSERT(x) extern int __C_ASSERT__ [(x)?1:-1]

C_ASSERT(STAMP_SECTOR_FREQ >= STAMP_MAX_LENGTH);
C_ASSERT(STAMP_SECTOR_FREQ % STAMP_MAX_LENGTH == 0);

using namespace std;

class Segment;
class Session;
extern volatile bool g_interrupted;

enum extent_operation_t {TRIM_OP, WRITE_OP};
enum extent_status_t {ALL_RECORDED = 0,
                      OPEN_ENDED = 1,
                      // For psync, this means up to one unrecorded write/trim succeeded.
                      // For atomic, this means up to one unrecorded vector group succeeded.
                      // For aio, this means up to iodepth non-contiguous unrecorded I/O's succeeded.
                      LAST_OPERATION_FAILED = 2
                     };

enum verification_result_t {ALL_CORRECT = 0,
                            PARTIAL_FAILURE = 1,
                            COMPLETE_FAILURE = 2
                           };

enum ioengine_t {PSYNC = 0,
                 ATOMIC = 1,
                 LIBAIO = 2
                };

typedef struct
{
    string explanation;
} fault_record;

typedef struct
{
    Segment* segment;
    bool persistent_trim_supported;
    bool skip_unwritten_sectors;
    bool only_unrecorded;
    uint32_t rc;
    vector<fault_record> fault_vec;
} verify_params;

typedef struct
{
    uint64_t extents_written;
    uint64_t extents_trimmed;
    uint64_t sectors_written;
    uint64_t sectors_trimmed;
    uint64_t wlfsr;
    uint64_t tlfsr;
    uint64_t vlfsr;
} state_buffer;

typedef struct
{
    Segment* segment;
    uint64_t extents;
    uint32_t rc;
} random_execute_params;

#ifdef LIBAIO_SUPPORTED
typedef struct
{
    struct iocb io_cb;
    void* write_buffer;
    uint64_t extent_num;
    uint32_t len;
} aio_control_block;

class AIO_batch
{
public:
    deque<aio_control_block*> aio_cbs_;
    stack<uint64_t> acknowledged_extents_;
    state_buffer state_snapshot_;
    bool in_error_;
    extent_operation_t batch_type_;

    AIO_batch();
};
#endif

class IOException : public runtime_error
{
public:
    IOException(string const& error) : runtime_error(error) {}
};

/**
 * @brief A base class providing a common interface for the threads.
 */
class RunnableSegment
{
protected:
    Session * session_;
public:
    RunnableSegment(Session* parent_session);
    virtual ~RunnableSegment() {};
    virtual void random_execute(uint64_t extents) {};
    virtual uint32_t verify(bool persistent_trim_supported,
                            bool skip_unwritten_sectors,
                            bool only_unrecorded,
                            vector<fault_record>* fault_vec)
    {
        throw runtime_error("verify must be overriden.");
    };
    /** @brief Locks the stdout/stderr mutex so the segment can print.
     */
    void lock_print_mutex();
    /** @brief Locks the stdout/stderr mutex so the segment can print.
     */
    void unlock_print_mutex();
};

/**
 * @brief Class that keeps track of populated sectors.
 */
class LBABitmap
{
    vector<bool>* bitmap_;
    uint64_t hamming_weight_;
    uint64_t size_;

public:
    typedef struct
    {
        uint64_t offset;
        uint32_t length;
    } LBA_range;

    LBABitmap(uint64_t size);
    ~LBABitmap();
    /** @brief Sets a range of LBAs as populated.
     * @param offset Beginning sector.
     * @param length Number of following sectors.  */
    void set_range(uint64_t offset, uint32_t length);
    /** @brief Clears a range of LBAs as populated.
     * @param offset Beginning sector.
     * @param length Number of following sectors.
     */
    void clear_range(uint64_t offset, uint32_t length);
    /** @brief Returns the hamming weight of the bitmap (number of ones).
     * @return The hamming weight.
     */
    uint64_t hamming_weight();
    /** @brief Returns the inverse hamming weight (number of zeros).
     * @return The inverse hamming weight.
     */
    uint64_t inverse_hamming_weight();
    /** @brief Retrieves the LBA range of zeros or ones that follows the
     * one referenced by lr.  To begin, pass an lr of {0,0}.
     * @param lr Reference to an LBA_range struct.
     * @param value If false, the next range of zeros will be returned,
     * otherwise, the next range of ones.
     * @param max_length The maximum length a range can be.
     * @param max_offset The upper bound to search.  To search the entire
     * range, pass in ~0LLU.
     * @return Return true if a range was found, false otherwise.
     */
    bool get_next_range(LBA_range* lr, bool value, uint32_t max_length,
                        uint64_t max_offset);
    /** @brief Returns the number of bits at 0 or 1 within a range.
     * @param offset Beginning sector.
     * @param length Number of following sectors.
     * @param value What value where counting, 0 or 1.
     * @return Number of bits matching value in range.
     */
    uint32_t range_sum(uint64_t offset, uint32_t length, bool value);
};


/**
 * @brief Class modeling a persistence pass.  A pass writes enough sectors on a
 * block device to fill the entire underlying capacity of a block device.
 */
class Pass
{
    Segment* segment_;
    pthread_mutex_t committed_buffer_state_lock_;
    bool errored_batch_freed_;
    uint64_t pass_num_;
    uint64_t seed_;
    uint32_t buffer_blocks_in_sector_;
    uint64_t extents_written_;
    uint64_t extents_trimmed_;
    uint64_t sectors_written_;
    uint64_t sectors_trimmed_;

    //@brief A list of acked extents beyond those that are contiguous with the run.
    deque<uint64_t> scattered_acked_write_extents_;
    deque<uint64_t> scattered_acked_trim_extents_;

    /** @brief The Write Linear Feedback Shift Register determines the order in which
      *  extents are written.
    */
    LFSR* wlfsr_;
    /** @brief The TRIM Linear Feedback Shift Register determines the order in which
      *  extents are trimmed.
    */
    LFSR* tlfsr_;
    /** @brief The vector num Linear Feedback Shift Register determines how
      *  many extents to bundle in a vectored atomic write.
    */
    LFSR* vlfsr_;

    /** @brief The state buffer for use by external threads.  This contains the
      *        guaranteed state of the card, a.k.a. everything that has been acked.
    */
    state_buffer committed_buffer_;

    //@brief Determines the layout of the extents on the block device.
    BoundedExtentIndexer* bei_;

    //@brief Common initialization shared between constructors.
    void init();
    //@brief Writes current state to the committed buffer.
    void update_committed_buffer();
    /** @brief Writes provided state to the committed buffer.
        @param state_buffer A pointer to the state buffer to pull from.
    */
    void update_committed_buffer(state_buffer* state_buffer);
    /** @brief Captures current state into a state buffer.
     */
    void snap_state(state_buffer* st_buf);
    /** @brief Captures current trim state into buffer and copies other state from provided snapshot.
     */
    void snap_state_for_trims(state_buffer* st_buf, state_buffer* buf_with_write_state);
    /** @brief Write one extent against the devices.
     */
    void execute_extent();
    /** @brief Executes a synchronous trim.
      * @param rel_offset The sector offset relative to the segment.
      * @param length The length in sectors.
      * @param op The operation type (WRITE_OP || TRIM_OP)
    */
    void execute_sync_operation(uint64_t rel_offset, uint32_t length, extent_operation_t op);
#ifdef LIBAIO_SUPPORTED
    /** @brief Submits an optional trim batch, then a write batch reaping as neccessary until iodepth is allows submission.  Throws IOException on error.
      * @param num_extents The maximum number of extents we are going to trim or write.  Must not be greater than the number of extents left to write in the pass.
      * @param Returns the number of extents submitted.
      * @return Return number of extents written.
    */
    long execute_async_extents(uint64_t num_extents);
    /** @brief Frees aio_cbs in a batch and rolls back their operations.  Then frees the batch.
        @param batch A pointer to the batch.
    */
    void teardown_async_batch(AIO_batch* batch);
    /** @brief Unconditionally reap all outstanding I/Os regardless of errors.
        @return True if successful, False if error occured.
    */
    bool uncond_reap();
    /** @brief Perform a single AIO reap.  Throws a ReapException if the reaped reported an I/O error.
        @param iodepth_batch_complete The suggested number of I/O's to reap.
    */
    void reap(long iodepth_batch_complete);
#endif
    /** @brief Returns the next operation (write or trim) for the device determined by current state.
     * @param dest_offset Where the write or trim offset will be placed.
     * @param dest_length Where the extent length will be placed.
     * @return Returns the extent type (write or trim).
     */
    extent_operation_t get_next_operation(uint64_t* dest_offset, uint32_t* dest_length);
    /** @brief Rolls back state of lfsrs effectively undoing get_next_operation.
     * @param ext_op The extent type.
     * @param bm If a bitmap is passed in, the ranges will be cleared in the rolled back operations.
     */
    void rollback_operation(extent_operation_t ext_op, LBABitmap* bm);
public:
    /** @brief Constructs a Pass instance.
     * @param parent_segment A reference to the parent segment.
     * @param pass_num The pass number we are on.
    */
    Pass(Segment* parent_segment, uint64_t pass_num);
    /** @brief Constructs a Pass instance from json state.
     * @param parent_segment A reference to the parent segment.
     * @param state A json object containing the state to be restored.
    */
    Pass(Segment* parent_segment, Json::Value state);
    ~Pass();
#ifdef LIBAIO_SUPPORTED
    /** @brief Reap all outstanding I/Os, may throw ReapException.
    */
    void reap_all();
#endif
    /** @brief Returns a string reporting the last acked operations.
      */
    string get_last_acked_ops_str();
    /** @brief Write or TRIM extents randomly.
     * @param Number of extents to write or trim.
     * @return Returns true if the pass is complete, false otherwise.
     */
    bool random_execute_extents(uint64_t extents);
    /** @brief Populates the write buffer with the payload.
     * @param write_buffer Pointer to the buffer to be populated.
     * @param extent_num The extent number.
     * @param rel_offset sector offset.
     * @param length Number of sectors.
     */
    void generate_payload(uint64_t* write_buffer, uint64_t extent_num, uint64_t rel_offset, uint32_t length);
    /** @brief Returns number of extents possibly executed.  This is different than the
               sum of extents_written and extents_trimmed in that it includes operations
               which partially completed.
    */
    uint64_t possible_extents_executed();
    /** @brief Returns the number of extents written so far.
     * @return Number of extents written.
     */
    uint64_t extents_written();
    /** @brief Returns the number of extents trimmed so far.
     * @return Number of extents trimmed.
     */
    uint64_t extents_trimmed();
    /** @brief Returns the number of sectors written so far.
     * @return Number of sectors written.
     */
    uint64_t sectors_written();
    /** @brief Returns the number of sectors trimmed so far.
     * @return Number of sectors trimmed.
     */
    uint64_t sectors_trimmed();
    /** @brief Returns the number of extents in this pass.
     * @return Number of extents.
     */
    uint64_t num_extents();
    /** @brief Returns the number assigned to this pass.
     * @return pass number.
     */
    uint64_t pass_num();
    /** @brief Returns the json representing this pass.
     */
    Json::Value get_json();
    /** @brief Validates all data associated with this pass.  On exit, the segment's extent_status will not be OPEN_ENDED.
      * @param bm Reference to the Segment verification LBABitmap.
      * @param fault_vec A reference to a vector where faults are recorded.
      * @param persistent_trim True if we are enforcing that trims be zeros.
      * @param current_pass Whether this is the current pass being verified.
      * @param only_unrecorded only verify extents which are beyond what is recorded in the statefile.
      * @return returns NO_FAULTS_FOUND, FAULTS_FOUND and or INTERRUPPTED.
     */
    uint32_t verify(LBABitmap* bm, vector<fault_record>* fault_vec,
                    bool persistent_trim_supported, bool current_pass, bool only_unrecorded);
    /** @brief Tests the next operation to see if it is complete, or partially complete.
      * @param bm Reference to the Segment verification LBABitmap.
      * @param Pointer to where the operation type should be stored.
      * @param Whether to output findings to stdout.
      * @return Returns ALL_CORRECT, PARTIAL_FAILURE, or COMPLETE_FAILURE
    */
    verification_result_t test_next_op(LBABitmap*bm, extent_operation_t* op, bool quiet);
    /** Moves the state forward searching for the interruption point on synchronous writes.
      * @param bm Reference to the Segment verification LBABitmap.
    */
    void find_psync_interruption_point(LBABitmap* bm);
    /** @brief Moves the state forward searching for the interruption point on async writes.
      * @param bm Reference to the Segment verification LBABitmap.
    */
    void find_async_interruption_point(LBABitmap* bm);
    /** @brief Moves the state forward searching for the interruption point on atomic writes.
      * @param bm Reference to the Segment verification LBABitmap.
      * @param fault_vec A vector to place fault records.
    */
    void find_atomic_interruption_point(LBABitmap* bm, vector<fault_record>* fault_vec);
    /** @brief Verifies the acked extents beyond those covered by the committed state.
      * @param bm Reference to the Segment verification LBABitmap.
      * @param fault_vec A vector to place fault records.
    */
    void verify_scattered_acked_extents(LBABitmap* bm, vector<fault_record>* fault_vec);
    /** @brief Verify all extents covered by the committed state.
    */
    void simple_verify(LBABitmap* bm, uint64_t extents_written, uint64_t extents_trimmed, vector<fault_record>* fault_vec, bool persistent_trim_supported);
    /** @brief Validates all data associated with this extent and records failures to disk and the faul vector.
     * @param offset The sector offset of the extent.
     * @param length The length (in sectors) of the extent.
     * @param bm Reference to the Segment verification LBABitmap.
     * @param validation_buffer A reference to the buffer containing the correct data.
     * @param fault_vec A reference to a vector where multiple faults are recorded.  If NULL, verification will halt on the first fault found.
     * @param extent_type What type this extent is, trim or write.
     * @param extent_num What extent number that is being verified.
     * @param extent_count What number this extent was executed on the pass.  The first extent executed was 1.
     * @return Returns ALL_CORRECT, PARTIAL_FAILURE or COMPLETE_FAILURE
     */
    verification_result_t hard_verify_extent(
        uint64_t offset,
        uint32_t length,
        LBABitmap* bm,
        void* validation_buffer,
        vector<fault_record>* fault_vec,
        extent_operation_t extent_type,
        uint64_t extent_num,
        uint64_t extent_count);
    /** @brief Verifies all data but does not record failures.
     * @param offset The sector offset of the extent.
     * @param length The length (in sectors) of the extent.
     * @param bm Reference to the Segment verification LBABitmap.
     * @param validation_buffer A reference to the buffer containing the correct data.
     * @param extent_type What type this extent is, trim or write.
     * @param extent_num What extent number that is being verified.
     * @param extent_count What number this extent was executed on the pass.  The first extent executed was 1.
     * @param quiet Whether to output to stdout.
     * @return Returns ALL_CORRECT, PARTIAL_FAILURE or COMPLETE_FAILURE
     */
    verification_result_t soft_verify_extent(
        uint64_t offset,
        uint32_t length,
        LBABitmap* bm,
        void* validation_buffer,
        extent_operation_t extent_type,
        uint64_t extent_num,
        uint64_t extent_count,
        bool quiet);
    /** @brief Sets the population state for this pass into the population.
     * Used to rebuild mapping on resume.
     * @param population A reference to the population instance.
     * @param trims Whether to build state for all trim extents performed.
    */
    void build_population(LBABitmap* population, bool trims);
    /** @brief Prints all operations performed by this pass.
        @param end_extent_num How many extents were executed in this pass.
    */
    void print_operations(uint64_t end_extent_num);
    /** @brief Empties the scatter_acked_extents deque.
    */
    void clear_scattered_acked_extents();
};

/**
 * @brief A generic range class
 */
typedef struct
{
    uint64_t base;
    uint64_t length;
} range_t;

/**
 * @brief Atomic write iovec
 */
struct iovec_atomic
{
    range_t  iov_range;
    uint32_t iov_op;
    uint32_t iov_flags;
    uint64_t iov_base;
};

/**
 * @brief Class modeling a segment which usually contain two passes, the
 * former complete pass, and the current partial pass.
 */
class Segment : public RunnableSegment
{
    const uint32_t id_;
    pthread_mutex_t pass_transition_lock_;
    Pass* former_pass_;
    Pass* current_pass_;
    uint64_t* write_buffers_[MAX_IO_DEPTH];
    uint64_t* read_buffer_;
    iovec_atomic* iov_;
    //@brief Records how many sectors are currently populated on the device.
    LBABitmap* population_;
    //@brief Records how many extents have been written, except for those in the current pass.
    uint64_t extents_written_;
    //@brief Records how many extents have been trimmed, except for those in the current pass.
    uint64_t extents_trimmed_;
    //@brief Records how many sectors have been written, except for those in the current pass.
    uint64_t sectors_written_;
    //@brief Records how many sectors have been trimmed, except for those in the current pass.
    uint64_t sectors_trimmed_;
    pthread_t thr_id_;

    //@brief Common initialization shared between constructors.
    void init();

public:
    const uint64_t seed_;
    const ioengine_t ioengine_;
    const uint32_t atomic_min_vectors_;
    const uint32_t atomic_max_vectors_;
    const uint32_t min_extent_len_;
    const uint32_t max_extent_len_;
    const uint64_t first_sector_;
    const uint32_t iodepth_;
    const long iodepth_batch_;
    const long iodepth_batch_complete_;
    const uint64_t population_allowance_;
    const uint64_t length_;
    //@brief The state of the recorded data as it relates to on-card operations.
    extent_status_t extent_status_;
    uint64_t buffer_size_;

#ifdef LIBAIO_SUPPORTED
    io_context_t aio_context_id_;             // Each segment has one aio context.
    aio_control_block* aiocbs_;               // * to first aio control block used to issue I/Os.
    struct io_event* p_aio_events_;           // * to first aio event block used to report completions.
    stack<AIO_batch*> free_batches_;          // A stack of AIO_batch instances free to use.
    deque<AIO_batch*> aio_batches_;           // A FIFO of in use I/O batches.
    stack<aio_control_block*> free_aio_cbs_;  // A stack of aio_control_blocks free to use.
#endif

    /** @brief Constructs a Segment instance.
     * @param parent_session A reference to the parent session object.
     * @param id A unique number assigned to this segment.
     * @param seed A seed value for this segment
     * @param ioengine What ioengine this segment is to use.
     * @param uint32_t The minimum number of vectors in a call.
     * @param uint32_t The maximum number of vectors in a call.
     * @param min_extent_len The minimum extent length usually allowed.
     * @param max_extent_len The maximum extent length allowed.
     * @param first_sector The beginning of this segment in LBA space .
     * @param iodepth The iodepth for async I/O.
     * @param iodepth_batch How many I/Os to submit at once.
     * @param iodepth_batch_complete How many I/Os to reap at once.
     * @param population_allowance How many sectors are allowed to be populated.
     * @param length The length of this segment in LBA space.
    */
    Segment(Session* parent_session, uint32_t id, uint64_t seed,
            ioengine_t ioengine, uint32_t atomic_min_vectors, uint32_t atomic_max_vectors,
            uint32_t min_extent_len, uint32_t max_extent_len, uint64_t first_sector,
            int iodepth, long iodepth_batch, long iodepth_batch_complete,
            uint64_t population_allowance, uint64_t length);
    /** @brief Constructs a Segment instance from json.
     * @param parent_session A reference to the parent session object.
     * @param jsegment A json object containing the segment information.
    */
    Segment(Session* parent_session, Json::Value jsegment);
    virtual ~Segment();
    /** @brief Starts a random write thread.
     * @param params A reference to a random write params struct.
    */
    void start_random_execute(random_execute_params* params);
    /** @brief Starts a verification thread.
     * @param params A reference to a verify params struct.
    */
    void start_verify(verify_params* params);
    /** @brief Joins the outstanding write or verify thread.
    */
    void join();
    /** @brief Writes and trims extents randomly.
     * @param n The number of extents to write or trim. 0 means write indefinately.
     */
    void random_execute(uint64_t extents);
    /** @brief Returns the number of extents left until the current pass is closed.
    */
    uint64_t extents_until_pass_closure();
    /** @brief Returns the number of extents written in this segment.  Including the current pass.
    */
    uint64_t get_extents_written();
    /** @brief Returns the number of extents trimmed in this segment.  Including the current pass.
    */
    uint64_t get_extents_trimmed();
    /** @brief Returns the number of sectors written in this segment.  Including the current pass.
    */
    uint64_t get_sectors_written();
    /** @brief Returns the number of sectors trimmed in this segment.  Including the current pass.
    */
    uint64_t get_sectors_trimmed();
    /** @brief Performs a full verification of the segment based upon loaded state.
     * @param persistent_trim_supported Whether we respect persistent trim.
     * @param skip_unwritten_sectors Whether to skip verification of sectors never written.
     * @param only_unrecorded Only verify the extents that may have been written but not recorded.
     * @param fault_vec A refence to a vector to record faults to.
     * @return returns NO_FAULTS_FOUND, FAULTS_FOUND and or INTERRUPPTED.
     */
    uint32_t verify(bool persistent_trim_supported, bool skip_unwritten_sectors,
                    bool only_unrecorded, vector<fault_record>* fault_vec);
    /** @brief Attempts to verify every non-verified sectors contains zeros.
     * @param bm A reference to the LBABitmap instance.
     * @param fault_vec A reference to a vector of faults.
     * @return returns NO_FAULTS_FOUND, FAULTS_FOUND and or INTERRUPPTED.
     */
    uint32_t verify_remainder_sectors(LBABitmap* bm,
                                      vector<fault_record>* fault_vec);
    /** @brief Returns the current population map.
     * @return Returns a pointer to an LBABitmap instance.
     */
    LBABitmap* get_population();
    /** @brief Invalidate population
     */
    void invalidate_population();
    /** @brief Returns the json representing this pass.
     */
    Json::Value get_json();
    /** @brief Returns the sector size of the device.
     */
    uint32_t get_sector_size();
    /** @brief Returns the file descriptor of the opened file.
     */
    int64_t get_fd();
#ifdef LIBAIO_SUPPORTED
    /** @brief Retrieves a free batch instance, ready for use.
        @param op The operation type this batch will be.
    */
    AIO_batch* get_free_batch(extent_operation_t op);
#endif
    /** @brief Returns the pointer to the specified write buffer.
     *         Maximum is IO_VECTOR_LIMIT if atomics in use.
     */
    void* get_write_buffer(uint32_t buffer_num);
    /** @brief Returns the pointer to the read buffer.
     */
    void* get_read_buffer();
    /** @brief Returns the pointer to the iovec buffer.
     */
    iovec_atomic* get_iovec_buffer();
    /** @brief Returns the id of this segment.
    */
    uint32_t get_id();
    /** @brief Returns True if capacity ratio < 1.0.
    */
    bool sparse_layout();
    /** @brief Steps through operations printing out the operation instead of performing.
         @param num_passes The number of most recent passes to print.  If zero is
          passed, the last one or two passes will be printed.
    */
    void print_operations(uint64_t num_passes);

};

/**
 * @brief Class modeling a session which contains one or more segements as well
   as facilities to write the statefile and report on errors.
 */
class Session
{
    const uint64_t seed_;
    const bool use_o_direct_;
    bool corruptions_found_;
    const string statefile_path_;
    vector<Segment*> segments_;
    uint64_t physical_sector_count_;
    struct timeval statefile_capture_timestamp_;
    pthread_mutex_t write_statefile_mutex_;

public:
    const int64_t fd_;
    uint32_t sector_size_;
    const bool persistent_trim_supported_;
    const bool trims_possible_;
    pthread_mutex_t print_mutex_;

    /** @brief Constructs a Session instance..
     * @param seed Integer seed value for the session.
     * @param segments Number of segments to divide the range into.
     * @param fd The open file descriptor.
     * @param ioengine_vec A vector containing what ioengine each vector should use.
     * @param min_extent_len The minimum extent length normally allowed.
     * @param max_extent_len the maximum extent length normally allowed.
     * @param specified_sector_count The sector count specified by the user.
     * @param sector_size  The sector size specified by the user.
     * @param persistent_trim_supported Whether or not we are going to respect persistent trim.
     * @param use_o_direct Whether O_DIRECT is in use.
     * @param atomic_factor The atomic ratio specified by the user.
     * @param range_ratio The range ratio specified by the user, 0 if not specified.
     * @param capacity_ratio The capacity ratio specified by the user.
     * @param first_sector The first sector to begin diving the ranges from.  If NO_FIRST_SECTOR_DEFINED, then a random first sector will be chosen.
     * @param iodepth The iodepth for async I/O.
     * @param iodepth_batch How many I/Os to submit at once.
     * @param iodepth_batch_complete How many I/Os to reap at once.
     * @param statefile_path The path to the statefile.
     */
    Session(uint64_t seed, uint32_t segments, int64_t fd, vector<ioengine_t>* ioengine_vec,
            uint32_t atomic_min_vectors, uint32_t atomic_max_vectors, uint32_t min_extent_len,
            uint32_t max_extent_len, uint64_t specified_sector_count, uint32_t sector_size,
            bool persistent_trim_supported, bool use_o_direct, double range_ratio,
            double capacity_ratio, uint64_t first_sector, uint32_t iodepth,
            long iodepth_batch, long iodepth_batch_complete, string statefile_path);
    /** @brief Constructs a Session instance..
     * @param fd The opened and valid file descriptor.
     * @param statefile_path The path to the statefile.
     * @param jsession A jsession object containing the session information.
     */
    Session(int fd, string statefile_path, Json::Value jsession);
    ~Session();
    /** @brief Writes extents randomly.
     * @param extents Reference to a vector containing the number of extents each segment
     *                should write.0 really means 0, and ~0LLU means write indefinately.  Vector
     *                must contain exactly one value for each segment.
     * @param elased_time A pointer to a struct where the elapsed time is to be placed.
     * @return Returns A value that may have error categories flagged
     */
    uint32_t random_execute(vector<uint64_t>& extents, struct timeval *elapsed_time);
    /** @brief Performs a full verification on the block device based upon loaded state.
     * @param seg_num The specific segment number to verify.  -1 means all.
     * @param skip_unwritten_sectors Whether to skip sectors never written.
     * @param elapsed_time A pointer to a timeval struct where the elapsed time should be stored.
     * @param only_unrecorded Only verify packets which are beyond what is described in the statefile.
     * @return returns NO_FAULTS_FOUND, FAULTS_FOUND and or INTERRUPPTED.
     */
    uint32_t verify(int seg_num, bool skip_unwritten_sectors, struct timeval * elapsed_time,
                    bool only_unrecorded);
    /** @brief Returns true if corruptions have ever been detected in the past.
     */
    bool corruptions_found();
    /** @brief Writes the current state out to a statefile.
     */
    void write_statefile();
    /** @brief Wrapper to facilitate calling non-static member function in callback.
     */
    static void write_statefile_wrapper(const void* ptr_to_Session);
    /** @brief Prints a report on each fault.
     * @param id The segment ID associated with this vector of fault records.
     * @param fault_vec A reference to a vector of faults.
     */
    void print_fault_report(int id, vector<fault_record> *fault_vec);
    /** @brief Indicates whether presistent trim is supported.
        @return True if persistent trim is supported, false otherwise.
    */
    bool is_pers_trim_supported();
    /** @brief Returns total number of extents written in all segments to date.
    */
    uint64_t get_extents_written();
    /** @brief Returns total number of extents trimmed in all segments to date.
    */
    uint64_t get_extents_trimmed();
    /** @brief Returns total number of sectors written in all segments to date.
    */
    uint64_t get_sectors_written();
    /** @brief Returns total number of sectors trimmed in all segments to date.
    */
    uint64_t get_sectors_trimmed();
    /** @brief Returns total number of segments in the session.
    */
    uint32_t get_num_segments();
    /** @brief Print via stdout the operations for the given segment.
     * @param id The segment ID.
    */
    void print_seg_operations(uint32_t id, uint64_t num_passes);
};


int timeval_subtract (struct timeval* result, struct timeval* x, struct timeval* y);
