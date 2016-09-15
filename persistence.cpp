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

#include <errno.h>
#include <string.h>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <sys/time.h>
#include "persistence.h"
#include "memalign.h"

#if defined(__linux__)
#include <sys/ioctl.h>
#endif

#ifdef LIBAIO_SUPPORTED
AIO_batch::AIO_batch()
{
    memset(&state_snapshot_, 0, sizeof(state_buffer));
    in_error_ = false;
    batch_type_ = WRITE_OP;
}

class ReapException : public runtime_error
{
public:
    ReapException(string const& error) : runtime_error(error) {}
};
#endif

void* randw_executor(void* params)
{
    random_execute_params* rw_params = (random_execute_params*)params;
    RunnableSegment *obj=(RunnableSegment*)(rw_params->segment);
    try
    {
        obj->random_execute(rw_params->extents);
    }
    catch (const IOException &exc)
    {
        obj->lock_print_mutex();
        cerr<<exc.what()<<endl;
        obj->unlock_print_mutex();
        rw_params->rc = IO_ERROR;
    }
    catch (const exception &exc)
    {
        obj->lock_print_mutex();
        cerr<<exc.what()<<endl;
        obj->unlock_print_mutex();
    }
    return obj;
}

void* verify_executor(void* params)
{
    verify_params* v_params = (verify_params*)params;
    RunnableSegment *obj=(RunnableSegment*)v_params->segment;
    try
    {
        v_params->rc = obj->verify(v_params->persistent_trim_supported,
                                   v_params->skip_unwritten_sectors,
                                   v_params->only_unrecorded,
                                   &v_params->fault_vec);
    }
    catch (const exception &exc)
    {
        obj->lock_print_mutex();
        cerr<<exc.what()<<endl;
        obj->unlock_print_mutex();
    }
    return obj;
}


LBABitmap::LBABitmap(uint64_t size)
    : hamming_weight_(0), size_(size)
{
    bitmap_ = new vector<bool>;
    bitmap_->assign(size_, 0);
}

void LBABitmap::set_range(uint64_t offset, uint32_t length)
{
    for(uint32_t i = 0 ; i < length ; i++)
    {
        if (!(*bitmap_)[offset + i])
        {
            bitmap_->at(offset + i).flip();
            hamming_weight_++;
        }
    }
}

void LBABitmap::clear_range(uint64_t offset, uint32_t length)
{
    for(uint32_t i = 0 ; i < length ; i++)
    {
        if ((*bitmap_)[offset + i])
        {
            bitmap_->at(offset + i).flip();
            hamming_weight_--;
        }
    }
}

bool LBABitmap::get_next_range(LBA_range* lr, bool value, uint32_t max_length,
                               uint64_t max_offset)
{
    max_offset = std::min(max_offset, bitmap_->size() - 1);
    lr->offset += lr->length;
    lr->length = 1;

    if (lr->offset > max_offset)
    {
        return false;
    }

    // Move the offset forward until the first match is found.
    while (bitmap_->at(lr->offset) != value)
    {
        lr->offset++;
        if (lr->offset > max_offset)
        {
            return false;
        }
    }

    // Increment the length
    while ((lr->offset + lr->length - 1) < max_offset
            && bitmap_->at((lr->offset + lr->length)) == value
            && lr->length < max_length)
    {
        lr->length++;
    }
    return true;
}

uint32_t LBABitmap::range_sum(uint64_t offset, uint32_t length, bool value)
{
    uint32_t sum = 0;
    for(uint32_t i = 0 ; i < length ; i++)
    {
        if ((*bitmap_)[offset + i] == value)
        {
            sum++;
        }
    }
    return sum;
}

uint64_t LBABitmap::hamming_weight()
{
    return hamming_weight_;
}

uint64_t LBABitmap::inverse_hamming_weight()
{
    return size_ - hamming_weight_;
}


LBABitmap::~LBABitmap()
{
    delete bitmap_;
}


Pass::Pass(Segment* parent_segment, uint64_t pass_num)
    : segment_(parent_segment),
      pass_num_(pass_num),
      seed_(parent_segment->seed_ + pass_num),
      extents_written_(0),
      extents_trimmed_(0),
      sectors_written_(0),
      sectors_trimmed_(0)
{
    init();
    // Dictates the order in which the extents should be written.
    wlfsr_ = new LFSR(seed_ , 1,
                      // LFSRs don't produce zeros, so we - 1 when reading from it.
                      bei_->num_extents());
    wlfsr_->previous(); // Back up one, so the first value will be our seed.
    tlfsr_ = new LFSR(seed_, 1, bei_->num_extents());
    vlfsr_ = new LFSR(seed_, segment_->atomic_min_vectors_, segment_->atomic_max_vectors_);
    update_committed_buffer();
}

Pass::Pass(Segment* parent_segment, Json::Value state)
    : segment_(parent_segment),
      pass_num_(state["pass_num"].asUInt64()),
      seed_(segment_->seed_ + pass_num_),
      extents_written_(state["extents_written"].asUInt64()),
      extents_trimmed_(state["extents_trimmed"].asUInt64()),
      sectors_written_(state["sectors_written"].asUInt64()),
      sectors_trimmed_(state["sectors_trimmed"].asUInt64())
{
    init();
    // Dictates the order in which the extents should be written.
    wlfsr_ = new LFSR(state["wlfsr_current"].asUInt64(), 1,
                      // LFSRs don't produce zeros, so we - 1 when reading from it.
                      bei_->num_extents());
    tlfsr_ = new LFSR(state["tlfsr_current"].asUInt64(), 1, bei_->num_extents());
    vlfsr_ = new LFSR(state["vlfsr_current"].asUInt64(), segment_->atomic_min_vectors_,
                      segment_->atomic_max_vectors_);

    Json::Value scat_ae = state["scattered_acked_write_extents"];
    if (scat_ae.isArray())
    {
        for (Json::Value::ArrayIndex i = 0 ; i < scat_ae.size(); i++)
        {
            scattered_acked_write_extents_.push_back(scat_ae[i].asUInt64());
        }
    }

    scat_ae = state["scattered_acked_trim_extents"];
    if (scat_ae.isArray())
    {
        for (Json::Value::ArrayIndex i = 0 ; i < scat_ae.size(); i++)
        {
            scattered_acked_trim_extents_.push_back(scat_ae[i].asUInt64());
        }
    }
    update_committed_buffer();
}

void Pass::init()
{
    // Dictates layout of extents on media.
    bei_ = new BoundedExtentIndexer(seed_,
                                    segment_->min_extent_len_,
                                    segment_->max_extent_len_,
                                    segment_->length_);
    buffer_blocks_in_sector_ = segment_->get_sector_size() / sizeof(uint64_t);
    errored_batch_freed_ = false;
    pthread_mutex_init(&committed_buffer_state_lock_, 0);
}

Pass::~Pass()
{
    delete bei_;
    delete wlfsr_;
    delete tlfsr_;
    delete vlfsr_;
    pthread_mutex_destroy(&committed_buffer_state_lock_);
}

void Pass::build_population(LBABitmap* population, bool trims)
{
    uint64_t offset;
    uint32_t length;
    LFSR wlfsr(seed_, 1, bei_->num_extents());
    // Back up one so the first value will be our seed.
    wlfsr.previous();
    for(uint64_t i = 0 ; i < extents_written_ ; i++)
    {
        offset = bei_->get_offset_and_length(wlfsr.next() - 1, &length);
        population->set_range(offset, length);
    }

    if (trims)
    {
        LFSR tlfsr(seed_, 1, bei_->num_extents());
        for(uint64_t i = 0 ; i < extents_trimmed_ ; i++)
        {
            offset = bei_->get_offset_and_length(tlfsr.previous() - 1, &length);
            population->clear_range(offset, length);
        }
    }
}

uint64_t Pass::possible_extents_executed()
{
    uint64_t ret = extents_trimmed_ + extents_written_;
    if (segment_->extent_status_ == LAST_OPERATION_FAILED)
    {
        if (segment_->ioengine_ == ATOMIC)
        {
            ret += vlfsr_->next();
            vlfsr_->previous();
            // Handle last truncated group of vectors
            if (ret > num_extents())
            {
                ret = num_extents();
            }
        }
        else
        {
            ret += 1;
        }
    }
    return ret;
}

void Pass::print_operations(uint64_t end_extent_num)
{
    uint64_t rel_offset;
    uint32_t length;

    if (segment_->ioengine_ == ATOMIC)
    {
        while (extents_written_ + extents_trimmed_ < end_extent_num && !g_interrupted)
        {
            // Determine how many vectors
            uint32_t num_vectors = vlfsr_->next();
            if (end_extent_num - (extents_written_ + extents_trimmed_) < num_vectors)
            {
                // Allow a truncation if we're at the end of a pass
                if (end_extent_num == bei_->num_extents())
                {
                    num_vectors = end_extent_num - (extents_written_ + extents_trimmed_);
                }
                else
                {
                    break;
                }
            }
            for (uint32_t i = 0 ; i < num_vectors ; i++)
            {
                if (get_next_operation(&rel_offset, &length) == WRITE_OP)
                {
                    cout<<"qw,"<<(wlfsr_->current() - 1)<<","<<
                        rel_offset + segment_->first_sector_<<","<<length<<endl;
                }
                else
                {
                    cout<<"qt,"<<(tlfsr_->current() - 1)<<","<<
                        rel_offset + segment_->first_sector_<<","<<length<<endl;
                }
            }
            cout<<"submit,"<<num_vectors<<endl;
        }
    }
    else
    {
        while (extents_written_ + extents_trimmed_ < end_extent_num && !g_interrupted)
        {
            if (get_next_operation(&rel_offset, &length) == WRITE_OP)
            {
                cout<<"w,"<<(wlfsr_->current() - 1)<<","<<
                    rel_offset + segment_->first_sector_<<","<<length<<endl;
            }
            else
            {
                cout<<"t,"<<(tlfsr_->current() - 1)<<","<<
                    rel_offset + segment_->first_sector_<<","<<length<<endl;
            }
        }
    }
}

void Pass::generate_payload(uint64_t* write_buffer, uint64_t extent_num,
                            uint64_t rel_offset, uint32_t length)
{
    Base2LFSR payload_lfsr(rel_offset + pass_num_ + segment_->seed_, MAX_DEGREE);
    payload_lfsr.populate_buffer(write_buffer, length * buffer_blocks_in_sector_);

#if STAMP_SECTORS
    uint32_t sector_size = segment_->get_sector_size();
    uint64_t extent_lba = (rel_offset + segment_->first_sector_) * sector_size;

    for (uint32_t sector = 0 ; sector < length ; sector++)
    {
        uint64_t sec_rel_stamp_offset = 0;
        uint64_t extent_loc = sector_size * sector;

        do
        {
            // Stamp format is -global_byte_offset-extent_relative_sector_num-pass_num-extent_num-
            sprintf(&((char*)write_buffer)[extent_loc + sec_rel_stamp_offset],
                    "-%" PRIu64"-%u-%" PRIu64"-%" PRIu64"-",
                    (extent_lba + extent_loc + sec_rel_stamp_offset), sector, pass_num_, extent_num);
            sec_rel_stamp_offset += STAMP_SECTOR_FREQ;
        } while (sec_rel_stamp_offset <= (uint64_t)(sector_size - STAMP_MAX_LENGTH));
    }
#endif
}

void Pass::update_committed_buffer()
{
    pthread_mutex_lock(&committed_buffer_state_lock_);
    snap_state(&committed_buffer_);
    pthread_mutex_unlock(&committed_buffer_state_lock_);
}

void Pass::update_committed_buffer(state_buffer* st_buf)
{
    pthread_mutex_lock(&committed_buffer_state_lock_);
    committed_buffer_.extents_written = st_buf->extents_written;
    committed_buffer_.extents_trimmed = st_buf->extents_trimmed;
    committed_buffer_.sectors_written = st_buf->sectors_written;
    committed_buffer_.sectors_trimmed = st_buf->sectors_trimmed;
    committed_buffer_.wlfsr = st_buf->wlfsr;
    committed_buffer_.tlfsr = st_buf->tlfsr;
    committed_buffer_.vlfsr = st_buf->vlfsr;
    pthread_mutex_unlock(&committed_buffer_state_lock_);
}

void Pass::snap_state(state_buffer* st_buf)
{
    st_buf->extents_written = extents_written_;
    st_buf->extents_trimmed = extents_trimmed_;
    st_buf->sectors_written = sectors_written_;
    st_buf->sectors_trimmed = sectors_trimmed_;
    st_buf->wlfsr = wlfsr_->current();
    st_buf->tlfsr = tlfsr_->current();
    st_buf->vlfsr = vlfsr_->current();
}

void Pass::snap_state_for_trims(state_buffer* st_buf, state_buffer* buf_with_write_state)
{
    st_buf->extents_trimmed = extents_trimmed_;
    st_buf->sectors_trimmed = sectors_trimmed_;
    st_buf->tlfsr = tlfsr_->current();

    // Non-trim related state comes from the provided state.
    st_buf->extents_written = buf_with_write_state->extents_written;
    st_buf->sectors_written = buf_with_write_state->sectors_written;
    st_buf->wlfsr = buf_with_write_state->wlfsr;

    st_buf->vlfsr = buf_with_write_state->vlfsr;
}

void Pass::execute_extent()
{
    uint64_t rel_offset;
    uint32_t length;

    extent_operation_t op = get_next_operation(&rel_offset, &length);
    execute_sync_operation(rel_offset, length, op);
    update_committed_buffer();
}

void Pass::execute_sync_operation(uint64_t rel_offset, uint32_t length, extent_operation_t op)
{
    uint32_t sector_size = segment_->get_sector_size();
    void* write_buffer = segment_->get_write_buffer(0);
    stringstream expl;

    if (op == WRITE_OP)
    {
        uint64_t byte_length = length * sector_size;
        uint64_t byte_offset = (rel_offset + segment_->first_sector_) * sector_size;
        int pwrite_ret;
#if VERBOSE_PRINTS
        segment_->lock_print_mutex();
        cout<<"Seg "<<segment_->get_id()<<": Writing extent #"<<wlfsr_->current() - 1;
        cout<<" offset "<<(rel_offset + segment_->first_sector_)<<" len "<<length<<endl;
        segment_->unlock_print_mutex();
#endif
        generate_payload((uint64_t*)write_buffer, wlfsr_->current() - 1, rel_offset, length);
        pwrite_ret = pwrite(segment_->get_fd(), write_buffer, byte_length, byte_offset);

        if ((uint64_t)pwrite_ret != byte_length)
        {
            expl<<"Seg "<<segment_->get_id()<<": Write of extent #"<<wlfsr_->current() - 1;
            expl<<" located at byte offset "<<byte_offset<<" byte length "<<byte_length;

            if (pwrite_ret < 0) // An error occurred, errno will be set.
            {
                expl<<" failed ("<<strerror(errno)<<".)  ";
                if (errno == EINVAL)
                {
                    expl<<"Is the blocksize set correctly?";
                }
            }
            else // A partial write occurred.
            {
                expl<<" was partial having "<<pwrite_ret<<" of "<<byte_length<<" bytes written.";
            }
            goto exec_sync_oper_failed;
        }
    }
    else // A trim
    {
        uint64_t trim_range[2];
        int ioctl_ret;

#if VERBOSE_PRINTS
        segment_->lock_print_mutex();
        cout<<"Seg "<<segment_->get_id()<<": Trimming extent #"<<tlfsr_->current() - 1;
        cout<<" offset "<<(rel_offset + segment_->first_sector_)<<" len "<<length<<endl;
        segment_->unlock_print_mutex();
#endif
        trim_range[0] = (rel_offset + segment_->first_sector_) * sector_size;
        trim_range[1] = length * sector_size;

        ioctl_ret = ioctl(segment_->get_fd(), BLKDISCARD, trim_range);
        if (ioctl_ret != 0)
        {
            expl<<"Seg "<<segment_->get_id()<<": Trim of extent #"<<tlfsr_->current() - 1;
            expl<<" located at byte offset "<<trim_range[0]<<" byte length "<<trim_range[1];

            if (ioctl_ret < 0) // An error occurred, errno will be set.
            {
                expl<<" failed ("<<strerror(errno)<<".)";
            }
            else // Unknown error, just return code.
            {
                expl<<" failed with unknown code "<<ioctl_ret<<".";
            }
            goto exec_sync_oper_failed;
        }
    }

return;

exec_sync_oper_failed:

    segment_->extent_status_ = LAST_OPERATION_FAILED;
    rollback_operation(op, NULL);

    // We've taken an IO error so stop writing the card
    g_interrupted = true;
    throw IOException(expl.str());
}

#ifdef LIBAIO_SUPPORTED
bool Pass::uncond_reap()
{
    bool error_free = true;
    // Reap all outstanding I/Os, even if they result in errors.
    while (!segment_->aio_batches_.empty())
    {
        try
        {
            reap(1);
        }
        catch (ReapException &err)
        {
            segment_->lock_print_mutex();
            cerr<<err.what()<<endl;
            segment_->unlock_print_mutex();
            error_free = false;
        }
    }
    return error_free;
}


void Pass::reap_all()
{
    while (!segment_->aio_batches_.empty())
    {
        reap(1);
    }
}

void Pass::reap(long iodepth_batch_complete)
{
    struct timespec ts = {REAP_TIMEOUT, 0};
    long events_reaped;
    long num_events_to_process;
    queue<int> err_numbers;

    // If the front is a trim batch, do simulated reaps.
    while (segment_->aio_batches_.front()->batch_type_ == TRIM_OP)
    {
        stringstream err_expl;
        AIO_batch * trim_batch = segment_->aio_batches_.front();

        segment_->aio_batches_.pop_front();

#if VERBOSE_PRINTS
        segment_->lock_print_mutex();
        cout<<"Seg "<<segment_->get_id()<<": Reaping simulated async trim batch with ";
        cout<<trim_batch->aio_cbs_.size()<<" IOs."<<endl;
        segment_->unlock_print_mutex();
#endif

        iodepth_batch_complete -= trim_batch->aio_cbs_.size();

        // For each aio control block in this batch
        while (!trim_batch->aio_cbs_.empty())
        {
            aio_control_block* aio_cb = trim_batch->aio_cbs_.front();
            trim_batch->aio_cbs_.pop_front();

            if (aio_cb->len == 0) // This IO failed
            {
                uint64_t offset;
                uint32_t length;

                trim_batch->in_error_ = true;
                errored_batch_freed_ = true;

                offset = bei_->get_offset_and_length(aio_cb->extent_num, &length);
                err_expl<<"Seg "<<segment_->get_id()<<": Failed trim, extent #"<<aio_cb->extent_num;
                err_expl<<" at sector offset "<<offset<<" len "<<length;
            }
            else if (errored_batch_freed_) // This IO succeeded, but we saw failures previously.
            {
                /* All successful (acked) operations that occur after we have encountered
                 * an error go on the scattered acked trim extents list.
                 */
                scattered_acked_trim_extents_.push_back(aio_cb->extent_num);
            }
            // Free the aio_control_block
            segment_->free_aio_cbs_.push(aio_cb);
        }

        if (!errored_batch_freed_)
        {
            // Commit the state associated with this batch as no errors have been reaped to date.
            update_committed_buffer(&(trim_batch->state_snapshot_));
        }

        if (trim_batch->in_error_)
        {
            segment_->free_batches_.push(trim_batch);
            throw ReapException(err_expl.str());
        }

        // Free the batch for reuse
        segment_->free_batches_.push(trim_batch);

        if (iodepth_batch_complete < 1 || segment_->aio_batches_.empty())
        {
            return;
        }
    }

    // Next batch is a write, so reap writes

    // Discover how many outstanding writes there are.
    uint32_t num_outstanding_writes = 0;
    deque<AIO_batch*>::iterator b_it = segment_->aio_batches_.begin();

    while (b_it != segment_->aio_batches_.end())
    {
        if ((*b_it)->batch_type_ == WRITE_OP)
        {
            num_outstanding_writes += (*b_it)->aio_cbs_.size();
        }
        *b_it++;
    }

    // Don't ask for more I/O's than are outstanding.
    iodepth_batch_complete = std::min(iodepth_batch_complete, (long)num_outstanding_writes);

#if VERBOSE_PRINTS
    segment_->lock_print_mutex();
    cout<<"Seg "<<segment_->get_id()<<": Reaping "<<iodepth_batch_complete<<" writes."<<endl;
    segment_->unlock_print_mutex();
#endif

    for ( ; ; )
    {
        events_reaped = io_getevents(segment_->aio_context_id_, iodepth_batch_complete,
                                     segment_->iodepth_, segment_->p_aio_events_, &ts);

        if (events_reaped == -EINTR) // Occurs if SIGINT hits while in the system call.
        {
            g_interrupted = true;
        }
        else
        {
            break;
        }
    }

    if (events_reaped < 0)
    {
        stringstream expl;
        expl<<"Seg "<<segment_->get_id()<<": "<<strerror(errno)<<" error on io_getevents.";
        throw runtime_error(expl.str());
    }

    num_events_to_process = events_reaped;
    if (events_reaped < iodepth_batch_complete)
    {
        segment_->lock_print_mutex();
        cout<<"Seg "<<segment_->get_id()<<": Reaped "<<events_reaped<<" I/Os when ";
        cout<<iodepth_batch_complete<<" were requested."<<endl;
        segment_->unlock_print_mutex();
    }

#if VERBOSE_PRINTS
    segment_->lock_print_mutex();
    cout<<"Seg "<<segment_->get_id()<<": "<<num_events_to_process<<" events completed."<<endl;
    segment_->unlock_print_mutex();
#endif

    // Begin matching events at the front (oldest) batch, because they're most likely to be complete.
    deque<AIO_batch*>::iterator it = segment_->aio_batches_.begin();
    while (it != segment_->aio_batches_.end() && num_events_to_process)
    {
        int num_matched = 0;
#if VERBOSE_PRINTS
        segment_->lock_print_mutex();
        cout<<"Seg "<<segment_->get_id()<<": Examining batch "<<(*it)<<" with "<<(*it)->aio_cbs_.size()<<" unmatched blocks."<<endl;
        segment_->unlock_print_mutex();
#endif
        deque<aio_control_block*>::iterator cb_it = (*it)->aio_cbs_.begin();

        // For each aio control block in this batch
        while (cb_it != (*it)->aio_cbs_.end())
        {
            bool matched = false;
            aio_control_block* aio_cb = *cb_it;
            for (int ei = 0 ; ei < events_reaped ; ei++)
            {
                // If this control block corresponds to this event.
                if (&(aio_cb->io_cb) == segment_->p_aio_events_[ei].obj)
                {
                    // If there is no error.
                    if (segment_->p_aio_events_[ei].res == aio_cb->len)
                    {
                        /* Record the extent number as acknowledged, that way if an I/O error occurs
                           before this batch is completely acknowledged (without error), we can
                           write out the scattered list of acknowleged extents for later ack
                           verification.
                         */
                        (*it)->acknowledged_extents_.push(aio_cb->extent_num);
                    }
                    else
                    {
                        err_numbers.push(-segment_->p_aio_events_[ei].res);
                        (*it)->in_error_ = true;
                    }

                    // Free the aio_control block for reuse.
                    segment_->free_aio_cbs_.push(aio_cb);
                    cb_it = (*it)->aio_cbs_.erase(cb_it);
                    num_matched++;
                    matched = true;
                    break;
                }
            }
            if (!matched)
            {
                *cb_it++;
            }
        }
        num_events_to_process -= num_matched;
        assert(num_events_to_process >= 0);
        *it++;
    }

    /* Now iterate over the batches beginning at the oldest; freeing and committing the state
     * of the ones who are error free.  If a failed batch is encountered, record any successful
     * portions of it to the scattered acked writes list, then compile an error report and throw
     * a ReapException.
    */
    while (!segment_->aio_batches_.empty() && segment_->aio_batches_.front()->aio_cbs_.empty())
    {
        AIO_batch * batch = segment_->aio_batches_.front();
        segment_->aio_batches_.pop_front();

#if VERBOSE_PRINTS
        segment_->lock_print_mutex();
        cout<<"Seg "<<segment_->get_id()<<": Batch is complete, freeing "<<batch;
        cout<<" now "<<segment_->aio_batches_.size()<<" outstanding batches."<<endl;
        segment_->unlock_print_mutex();
#endif

        if (batch->in_error_)
        {
            errored_batch_freed_ = true;
        }

        if (errored_batch_freed_)
        {
            // Iterate over all successful I/Os in this batch.
            while (!batch->acknowledged_extents_.empty())
            {
                scattered_acked_write_extents_.push_back(batch->acknowledged_extents_.top());
#if VERBOSE_PRINTS
                segment_->lock_print_mutex();
                cout<<"Seg "<<segment_->get_id()<<": Recording extent ";
                cout<<batch->acknowledged_extents_.top();
                cout<<" as a scattered acknowledged extent."<<endl;
                segment_->unlock_print_mutex();
#endif
                batch->acknowledged_extents_.pop();
            }
        }
        else
        {
            /* All IO's to date have been error free, so discard our acknowledged_extents list and
               update the committed buffer using the state snapshot associated with this batch.
            */
            while (!batch->acknowledged_extents_.empty())
            {
                batch->acknowledged_extents_.pop();
            }
            update_committed_buffer(&(batch->state_snapshot_));
        }
        segment_->free_batches_.push(batch);
    }

    // Errors were encountered.  Build user output.
    if (!err_numbers.empty())
    {
        int last_error = 0;
        int error_count = 0;
        bool first_message = true;
        stringstream expl;
        expl<<"Seg "<<segment_->get_id()<<": "<<"A reap of "<<events_reaped<<" resulted in ";
        expl<<err_numbers.size()<<" error(s): ";

        while (!err_numbers.empty())
        {
            if (err_numbers.front() != last_error)
            {
                if (error_count > 1)
                {
                    expl<<" (occurred "<<error_count<<" times) ";
                }
                last_error = err_numbers.front();
                error_count = 0;
                if (first_message)
                {
                    first_message = false;
                }
                else
                {
                    expl<<", ";
                }
                expl<<strerror(err_numbers.front());
            }
            err_numbers.pop();
        }
        throw ReapException(expl.str());
    }
    assert(num_events_to_process == 0);
}

long Pass::execute_async_extents(uint64_t num_extents)
{
    uint32_t sector_size = segment_->get_sector_size();
    uint64_t orig_num_extents = num_extents;
    uint32_t writes_queued = 0;
    uint32_t trims_queued = 0;
    AIO_batch* write_batch = NULL;
    AIO_batch* trim_batch = NULL;
    struct iocb* iocbs_to_submit[MAX_IO_DEPTH];
    state_buffer orig_state;

    snap_state(&orig_state);

    // Iterates once per batch member.
    while ((long)writes_queued < segment_->iodepth_batch_
           && num_extents
           && !g_interrupted
           && (long)trims_queued < segment_->iodepth_batch_)
    {
        uint64_t rel_offset;
        uint32_t length;
        aio_control_block* aio_cb;

        /* Reap if neccessary to free up an aio_cb.

           The number of available aio_cbs in equal to our iodepth, so it naturally
           enforces that we not have more than the allowed outstanding I/O.
         */
        while (segment_->free_aio_cbs_.empty())
        {
            try
            {
                reap(segment_->iodepth_batch_complete_);
            }
            catch(runtime_error &err)
            {
                // Abort and teardown built up write batch since it has not been submitted.
                teardown_async_batch(write_batch);

                // Some trims have already been performed, so push the batch.
                if (trim_batch)
                {
                    snap_state_for_trims(&trim_batch->state_snapshot_, &orig_state);
                    segment_->aio_batches_.push_back(trim_batch);
                }
                throw;
            }
        }

        aio_cb = segment_->free_aio_cbs_.top();
        segment_->free_aio_cbs_.pop();

        if (get_next_operation(&rel_offset, &length) == TRIM_OP)
        {
            // Use the aio_cb to hold trim information, but we actually execute synchronously.
            aio_cb->len = length * sector_size;
            aio_cb->extent_num = tlfsr_->current() - 1;

            try
            {
                execute_sync_operation(rel_offset, length, TRIM_OP);
            }
            catch (IOException &err)
            {
                segment_->lock_print_mutex();
                cerr<<err.what()<<endl;
                segment_->unlock_print_mutex();

                /* Staying true to the fact we're simulating async I/O, catch this on reap.
                 * Set the length to 0 as a signal that the trim failed.
                */
                aio_cb->len = 0;
            }

            if (!trim_batch)
            {
                trim_batch = segment_->get_free_batch(TRIM_OP);
            }

            // Add to trim batch.
            trim_batch->aio_cbs_.push_back(aio_cb);
            trims_queued++;
        }
        else // A write operation
        {
            void* write_buffer = aio_cb->io_cb.data = aio_cb->write_buffer;

            // Now prep the new writes
            generate_payload((uint64_t*)write_buffer, wlfsr_->current() - 1, rel_offset, length);

            // Record additional stats for later use during reap
            aio_cb->len = length * sector_size;
            aio_cb->extent_num = wlfsr_->current() - 1;

            io_prep_pwrite(&aio_cb->io_cb,
                           segment_->get_fd(),
                           write_buffer, (length * sector_size),
                           (rel_offset + segment_->first_sector_) * sector_size);
            iocbs_to_submit[writes_queued] = &aio_cb->io_cb;

            if (!write_batch)
            {
                write_batch = segment_->get_free_batch(WRITE_OP);
            }

            // Add to write batch.
            write_batch->aio_cbs_.push_back(aio_cb);
            writes_queued++;
        }
        num_extents--;
    }

    if (trim_batch) // Record the state and push the batch.
    {
        snap_state_for_trims(&trim_batch->state_snapshot_, &orig_state);
        assert(trims_queued == trim_batch->aio_cbs_.size());
#if VERBOSE_PRINTS
        segment_->lock_print_mutex();
        cout<<"Seg "<<segment_->get_id()<<": Submitting simulated trim batch with ";
        cout<<trim_batch->aio_cbs_.size()<<" trims."<<endl;
        segment_->unlock_print_mutex();
#endif
        segment_->aio_batches_.push_back(trim_batch);
    }

    if (write_batch)
    {
        /* The state snapshot attached to the write batch records current state, which
           are the trims plus the writes in this write batch.
        */
        snap_state(&write_batch->state_snapshot_);

        // Last chance to abort before submit
        if (g_interrupted)
        {
            teardown_async_batch(write_batch);
            write_batch = NULL;
        }
    }

    if (write_batch)
    {
        uint32_t num_ios = write_batch->aio_cbs_.size();

#if VERBOSE_PRINTS
        segment_->lock_print_mutex();
        cout<<"Seg "<<segment_->get_id()<<": Submitting write batch of size "<<num_ios;
        cout<<", with "<<segment_->aio_batches_.size()<<" batches already in flight."<<endl;
        segment_->unlock_print_mutex();
#endif
        segment_->aio_batches_.push_back(write_batch);
        int ios_submitted = io_submit(segment_->aio_context_id_, num_ios, iocbs_to_submit);
        if ((uint32_t)ios_submitted != num_ios)
        {
            segment_->lock_print_mutex();
            if (ios_submitted >=0)
            {
                cerr<<"Seg "<<segment_->get_id()<<": io_submit reported "<<ios_submitted<<" of ";
                cerr<<num_ios<<" as successful."<<endl;
            }
            else
            {
                cerr<<"Seg "<<segment_->get_id()<<": io_submit reported error ";
                cerr<<strerror(ios_submitted)<<"."<<endl;
            }
            segment_->unlock_print_mutex();
            write_batch->in_error_ = true;

            // Stop all future I/O
            g_interrupted = true;
        }
        else
        {
#if VERBOSE_PRINTS
            segment_->lock_print_mutex();
            cout<<"Seg "<<segment_->get_id()<<": iosubmit of "<<num_ios<<" write I/Os was successful."<<endl;
            segment_->unlock_print_mutex();
#endif
        }
    }
    return orig_num_extents - num_extents;
}

void Pass::teardown_async_batch(AIO_batch* batch)
{
    if (batch)
    {
        while (!batch->aio_cbs_.empty())
        {
            segment_->free_aio_cbs_.push(batch->aio_cbs_.back());
            batch->aio_cbs_.pop_back();
            rollback_operation(batch->batch_type_, NULL);
        }
        segment_->free_batches_.push(batch);
    }
}


#endif // LIBAIO_SUPPORTED

extent_operation_t Pass::get_next_operation(uint64_t* dest_offset, uint32_t* dest_length)
{
    LBABitmap * population = segment_->get_population();
    *dest_offset = bei_->get_offset_and_length(wlfsr_->next() - 1, dest_length);

    if (segment_->sparse_layout())
    {
        uint64_t free_sectors = (segment_->population_allowance_ - population->hamming_weight())
                                // Account for sectors that will be overwritten
                                + population->range_sum(*dest_offset, *dest_length, true);
        if (free_sectors < *dest_length)
        {
            wlfsr_->previous(); // Can we cache the next value so we don't have to recompute?
            *dest_offset = bei_->get_offset_and_length(tlfsr_->previous() - 1, dest_length);
            population->clear_range(*dest_offset, *dest_length);
            extents_trimmed_++;
            sectors_trimmed_+= *dest_length;
            return TRIM_OP;
        }
    }
    population->set_range(*dest_offset, *dest_length);
    extents_written_++;
    sectors_written_+= *dest_length;
    return WRITE_OP;
}

void Pass::rollback_operation(extent_operation_t ext_op, LBABitmap* bm)
{
    uint32_t length;
    uint64_t offset;

    if (ext_op == WRITE_OP)
    {
        offset = bei_->get_offset_and_length(wlfsr_->current() - 1, &length);
        sectors_written_ -= length;
        wlfsr_->previous();
        extents_written_--;
    }
    else
    {
        offset = bei_->get_offset_and_length(tlfsr_->current() - 1, &length);
        sectors_trimmed_ -= length;
        tlfsr_->next();
        extents_trimmed_--;
    }

    if (bm != NULL)
    {
        bm->clear_range(offset, length);
    }

    /* The population cannot be rolled back, so invalidate it.  If it is needed
     * we have the technology... it will be rebuilt. */
    segment_->invalidate_population();
}

string Pass::get_last_acked_ops_str()
{
    stringstream expl;
    uint32_t length;
    uint64_t rel_offset;

    expl<<"Last acked operations: ";
    if (extents_written_ > 0)
    {
        expl<<"Write extent #"<<wlfsr_->current() - 1<<" "<<extents_written_<<" writes into the pass, ";
        rel_offset = bei_->get_offset_and_length(wlfsr_->current() - 1, &length);
        expl<<"offset "<<(rel_offset + segment_->first_sector_)<<" len "<<length<<", ";
    }
    else
    {
        expl<<"(No writes) ";
    }
    if (extents_trimmed_ > 0)
    {
        expl<<"Trim extent #"<<tlfsr_->current() - 1<<" "<<extents_trimmed_<<" trims into the pass, ";
        rel_offset = bei_->get_offset_and_length(tlfsr_->current() - 1, &length);
        expl<<"offset "<<(rel_offset + segment_->first_sector_)<<" len "<<length;
    }
    else
    {
        expl<<"(No trims)";
    }
    return expl.str();
}


bool Pass::random_execute_extents(uint64_t extents)
{
    // Determine what extent to halt on.
    uint64_t end_extent_num = bei_->num_extents();
    if (extents > 0 && extents < (bei_->num_extents() - (extents_written_ + extents_trimmed_)))
    {
        end_extent_num = extents_written_ + extents_trimmed_ + extents;
    }
    assert(end_extent_num <= bei_->num_extents());

    // If we should perform an atomic vectored write.
    if (segment_->ioengine_ == ATOMIC)
    {
        cerr<<"Atomic writes are not supported!"<<endl;
    }
    else if (segment_->ioengine_ == PSYNC)
    {
        while (extents_written_ + extents_trimmed_ < end_extent_num && !g_interrupted)
        {
            execute_extent();
        }
    }
#ifdef LIBAIO_SUPPORTED
    else if (segment_->ioengine_ == LIBAIO)
    {
        try
        {
            uint64_t extents_to_write = end_extent_num - (extents_written_ + extents_trimmed_);
            while (extents_to_write && !g_interrupted)
            {
                extents_to_write -= execute_async_extents(extents_to_write);
            }

        }
        catch(runtime_error &err)
        {
            segment_->extent_status_ = LAST_OPERATION_FAILED;
            uncond_reap();
            throw;
        }

        if (!uncond_reap())
        {
            throw IOException("Error occurred while reaping.");
        }
    }
#endif
    else
    {
        stringstream expl;
        expl<<"Unrecognized or unsupported ioengine "<<segment_->ioengine_<<".";
        throw runtime_error(expl.str());
    }

    // All written extents were acked as we did not throw an exception.
    segment_->extent_status_ = ALL_RECORDED;
    assert(extents_written_ + extents_trimmed_ <= bei_->num_extents());
    return extents_written_ + extents_trimmed_ == bei_->num_extents();
}

uint64_t Pass::extents_written()
{
    return committed_buffer_.extents_written;
}

uint64_t Pass::extents_trimmed()
{
    return committed_buffer_.extents_trimmed;
}

uint64_t Pass::sectors_written()
{
    return committed_buffer_.sectors_written;
}

uint64_t Pass::sectors_trimmed()
{
    return committed_buffer_.sectors_trimmed;
}

uint64_t Pass::num_extents()
{
    return bei_->num_extents();
}

Json::Value Pass::get_json()
{
    Json::Value pass;
    Json::Value scatter_acked_write_extents;
    Json::Value scatter_acked_trim_extents;
    deque<uint64_t>::iterator it;

    it = scattered_acked_write_extents_.begin();
    while (it != scattered_acked_write_extents_.end())
    {
        scatter_acked_write_extents.append((Json::UInt64)*it);
        *it++;
    }
    pass["scattered_acked_write_extents"] = scatter_acked_write_extents;

    it = scattered_acked_trim_extents_.begin();
    while (it != scattered_acked_trim_extents_.end())
    {
        scatter_acked_trim_extents.append((Json::UInt64)*it);
        *it++;
    }
    pass["scattered_acked_trim_extents"] = scatter_acked_trim_extents;
    pass["pass_num"] = (Json::UInt64)pass_num_;
    pthread_mutex_lock(&committed_buffer_state_lock_);
    pass["extents_written"] = (Json::UInt64)committed_buffer_.extents_written;
    pass["extents_trimmed"] = (Json::UInt64)committed_buffer_.extents_trimmed;
    pass["sectors_written"] = (Json::UInt64)committed_buffer_.sectors_written;
    pass["sectors_trimmed"] = (Json::UInt64)committed_buffer_.sectors_trimmed;
    pass["wlfsr_current"] = (Json::UInt64)committed_buffer_.wlfsr;
    pass["tlfsr_current"] = (Json::UInt64)committed_buffer_.tlfsr;
    pass["vlfsr_current"] = (Json::UInt64)committed_buffer_.vlfsr;
    pthread_mutex_unlock(&committed_buffer_state_lock_);
    return pass;
}

uint64_t Pass::pass_num()
{
    return pass_num_;
}

verification_result_t Pass::soft_verify_extent(
    uint64_t offset,
    uint32_t length,
    LBABitmap* bm,
    void* validation_buffer,
    extent_operation_t extent_type,
    uint64_t extent_num,
    uint64_t extent_count,
    bool quiet)
{
    uint64_t first_sector = segment_->first_sector_;
    uint32_t sector_size = segment_->get_sector_size();
    void* read_buffer = segment_->get_read_buffer();

    // Only read and verify the sector ranges not yet verified as indicated by the bm.
    LBABitmap::LBA_range lba_r = {offset, 0};
    while(bm->get_next_range(&lba_r, false, length, offset + length - 1))
    {
        // Mark as checked, even if we fault, that way the former pass won't try to verify it.
        bm->set_range(lba_r.offset, lba_r.length);

        uint64_t read_byte_offset = (lba_r.offset + first_sector) * sector_size;
        uint32_t read_byte_length = lba_r.length * sector_size;
        int pread_ret = pread(segment_->get_fd(), read_buffer, read_byte_length, read_byte_offset);

        if ((uint32_t)pread_ret != read_byte_length)
        {
            string extent_type_str = (extent_type == WRITE_OP) ? "write" : "trim";
            stringstream expl;

            if (pread_ret < 0) // A read error occurred
            {
                expl<<"Seg "<<segment_->get_id()<<": "<<strerror(errno)<<" while soft verifying ";
                expl<<"extent #"<<extent_num<<", "<<extent_count<<" "<<extent_type_str;
                expl<<" extents into the pass. Extent information: "<<"byte offset ";
                expl<<read_byte_offset<<" byte length "<<read_byte_length<<".";
            }
            else // The read was incomplete
            {
                expl<<"Seg "<<segment_->get_id()<<": An incomplete read of extent #"<<extent_num;
                expl<<" "<<extent_count<<" "<<extent_type_str<<" extents into the pass occurred. ";
                expl<<pread_ret<<" bytes were returned when "<<read_byte_length<<" were requested. ";
                expl<<"If this occurs only in the last segment, device truncation is likely.";
            }
            g_interrupted = true;
            throw IOException(expl.str());
        }

        uint64_t buf_adv = sector_size * (lba_r.offset - offset);

        if (memcmp(read_buffer, (char*)validation_buffer + buf_adv, read_byte_length))
        {
            string extent_type_str = (extent_type == WRITE_OP) ? "write" : "trim";
            uint32_t verified_sectors = 0;
            stringstream expl;

            expl<<"Seg "<<segment_->get_id()<<": Interruption point found. ";
            if (extent_type == WRITE_OP)
            {
                // Walk each byte and verify each sector is perfect, and
                // ensure that the remaining unmatched sectors will be picked
                // up in the validation of the former pass.

                for (uint32_t i = 0 ; i < read_byte_length ; i++)
                {
                    if (((char*)read_buffer)[i] == ((char*)validation_buffer)[buf_adv + i])
                    {
                        verified_sectors = (i + 1) / sector_size;
                    }
                    else
                    {
                        break;
                    }
                }
                // If this extent is not fragmented
                if (read_byte_length == length * sector_size)
                {
                    expl<<"Write extent #"<<extent_num<<" ("<<extent_count<<" writes into the pass) ";
                }
                else
                {
                    expl<<"A section of write extent #"<<extent_num<<" ("<<extent_count<<" writes into the pass) ";
                }

                if (verified_sectors > 0)
                {
                    expl<<"was partial having "<<verified_sectors<<" of ";
                    expl<<(read_byte_length / sector_size)<<" sectors written."<<endl;
                }
                else
                {
                    expl<<"was not written."<<endl;
                }

                // Clear the "checked" status of the remainder mismatching bytes.
                // That way these bytes will be verified by the former pass verification.
                bm->clear_range(lba_r.offset + verified_sectors, lba_r.length - verified_sectors);
            }
            else // Trims cannot be partial.
            {
                expl<<"Trim extent #"<<extent_num<<" ("<<extent_count<<" trims into the pass) wasn't completed."<<endl;
                // This trim must not have occured, so mark it unchecked.
                bm->clear_range(lba_r.offset, lba_r.length);
            }
            if (!quiet)
            {
                segment_->lock_print_mutex();
                cout<<expl.str();
                segment_->unlock_print_mutex();
            }
            return (verified_sectors > 0) ? PARTIAL_FAILURE : COMPLETE_FAILURE;
        }
    }
    return ALL_CORRECT;
}

verification_result_t Pass::hard_verify_extent(
    uint64_t offset,
    uint32_t length,
    LBABitmap* bm,
    void* validation_buffer,
    vector<fault_record>* fault_vec,
    extent_operation_t extent_type,
    uint64_t extent_num,
    uint64_t extent_count)
{
    assert(bm && fault_vec);
    uint64_t first_sector = segment_->first_sector_;
    uint32_t sector_size = segment_->get_sector_size();
    uint32_t orig_fault_vec_size = fault_vec->size();
    void* read_buffer = segment_->get_read_buffer();

    // Only read and verify the sector ranges not yet verified as indicated by the bm.
    LBABitmap::LBA_range lba_r = {offset, 0};
    while(bm->get_next_range(&lba_r, false, length, offset + length - 1))
    {
        // Mark as checked, even if we fault, that way the former pass won't try to verify it.
        bm->set_range(lba_r.offset, lba_r.length);

        uint64_t read_byte_offset = (lba_r.offset + first_sector) * sector_size;
        uint32_t read_byte_length = lba_r.length * sector_size;
        int pread_ret = pread(segment_->get_fd(), read_buffer, read_byte_length, read_byte_offset);

        if ((uint32_t)pread_ret != read_byte_length)
        {
            string extent_type_str = (extent_type == WRITE_OP) ? "write" : "trim";
            stringstream expl;

            if (pread_ret < 0) // A read error occurred
            {
                expl<<"Seg "<<segment_->get_id()<<": "<<strerror(errno)<<" while hard verifying ";
                expl<<"extent #"<<extent_num<<", "<<extent_count<<" "<<extent_type_str;
                expl<<" extents into the pass. Extent information: "<<"byte offset ";
                expl<<read_byte_offset<<" byte length "<<read_byte_length<<".";
            }
            else // The read was incomplete
            {
                expl<<"Seg "<<segment_->get_id()<<": An incomplete read of extent #"<<extent_num;
                expl<<" "<<extent_count<<" "<<extent_type_str<<" extents into the pass occurred. ";
                expl<<pread_ret<<" bytes were returned when "<<read_byte_length<<" were requested. ";
                expl<<"If this occurs only in the last segment, device truncation is likely.";
            }
            g_interrupted = true;
            throw IOException(expl.str());
        }

        uint64_t buf_adv = sector_size * (lba_r.offset - offset);

        if (memcmp(read_buffer, (char*)validation_buffer + buf_adv, read_byte_length))
        {
            stringstream expl;
            string extent_type_str = (extent_type == WRITE_OP) ? "write" : "trim";
            expl<<"Mismatch while verifying "<<extent_type_str<<" extent #"<<extent_num;
            expl<<", "<<extent_count<<" "<<extent_type_str<<" extents into the pass."<<endl;
            expl<<"Extent information:"<<endl<<"sector offset: ";
            expl<<(offset + first_sector)<<" sector length:"<<length<<endl;

            stringstream ss;
            ss<<"seg"<<segment_->get_id()<<"_"<<extent_type_str<<"_extent_"<<extent_num;
            ss<<"_off"<<(offset + first_sector)<<"_len"<<length;

            // If this extent is not fragmented
            if (read_byte_length == length * sector_size)
            {
                bool success = true;
                FILE* oFile;
                uint32_t write_byte_length = length * sector_size;

                if (extent_type == WRITE_OP)
                {
                    // Write out expected and actual data
                    oFile = fopen((ss.str() + EXPECTED_EXTENSION).c_str(), "wb");
                    uint32_t write_byte_length = length * sector_size;
                    if (fwrite(validation_buffer, 1, write_byte_length, oFile) < write_byte_length)
                    {
                        success = false;
                    }

                    if (fclose(oFile))
                    {
                        success = false;
                    }
                }

                oFile = fopen((ss.str() + OBSERVED_EXTENSION).c_str(), "wb");
                if (fwrite(read_buffer, 1, write_byte_length, oFile) < write_byte_length)
                {
                    success = false;
                }

                if (fclose(oFile))
                {
                    success = false;
                }

                if (success)
                {
                    expl<<"Expected and observed data written to files "<<ss.str()<<".*"<<endl;
                }
                else
                {
                    expl<<"Error while writing expected and or observed data to files."<<endl;
                }
            }
            else
            {
                expl<<"This extent is fragmented. The mismatched sector slice begins at offset ";
                expl<<(lba_r.offset + first_sector)<<" length "<<lba_r.length<<endl;
                ss<<"_slice_off"<<(lba_r.offset + first_sector);
                ss<<"_len"<<lba_r.length;

                bool success = true;
                FILE* oFile;
                if (extent_type == WRITE_OP)
                {
                    // Write out expected and actual data
                    oFile = fopen((ss.str() + EXPECTED_EXTENSION).c_str(), "wb");

                    if (fwrite((char*)validation_buffer + buf_adv, 1, read_byte_length, oFile) < read_byte_length)
                    {
                        success = false;
                    }

                    if (fclose(oFile))
                    {
                        success = false;
                    }
                }

                oFile = fopen((ss.str() + OBSERVED_EXTENSION).c_str(), "wb");
                if (fwrite(read_buffer, 1, read_byte_length, oFile) < read_byte_length)
                {
                    success = false;
                }

                if (fclose(oFile))
                {
                    success = false;
                }

                if (success)
                {
                    expl<<"Expected and observed data written to files "<<ss.str()<<".*"<<endl;
                }
                else
                {
                    expl<<"Error while writing expected and or observed data to files."<<endl;
                }
            }

            fault_record rec = {expl.str()};
            fault_vec->push_back(rec);
            if (fault_vec->size() >= FAULT_MAX)
            {
                return COMPLETE_FAILURE;
            }
        }
    }
    return (fault_vec->size() > orig_fault_vec_size) ? COMPLETE_FAILURE : ALL_CORRECT;
}

uint32_t Pass::verify(LBABitmap* bm,
                      vector<fault_record>* fault_vec,
                      bool persistent_trim_supported,
                      bool current_pass,
                      bool only_unrecorded)
{
    uint64_t orig_extents_written = extents_written_;
    uint64_t orig_extents_trimmed = extents_trimmed_;
    uint32_t orig_num_faults = fault_vec->size();
    uint32_t ret = NO_FAULTS_FOUND;

    if (only_unrecorded)
    {
        assert(current_pass);
    }

    try
    {
        if (segment_->extent_status_ != ALL_RECORDED && current_pass)
        {
            segment_->lock_print_mutex();
            cout<<"Seg "<<segment_->get_id()<<": Searching for write interruption point.  ";
            cout<<extents_written_<<" extents were known to be written and "<<extents_trimmed_;
            cout<<" extents were known to be trimmed in this pass."<<endl;
            segment_->unlock_print_mutex();

            if (segment_->ioengine_ ==  ATOMIC)
            {
                find_atomic_interruption_point(bm, fault_vec);
            }
            else if (segment_->ioengine_== PSYNC)
            {
                find_psync_interruption_point(bm);
            }
            else if (segment_->ioengine_ == LIBAIO)
            {
                find_async_interruption_point(bm);
            }
            else
            {
                throw runtime_error("Unknown ioengine.");
            }
        }

        verify_scattered_acked_extents(bm, fault_vec);

        if (!only_unrecorded)
        {
            simple_verify(bm, orig_extents_written, orig_extents_trimmed, fault_vec, persistent_trim_supported);
        }

    }
    catch(const IOException &err)
    {
        segment_->lock_print_mutex();
        cerr<<err.what()<<endl;
        segment_->unlock_print_mutex();
        ret |= IO_ERROR;
    }

    if (fault_vec->size() > orig_num_faults)
    {
        ret |= FAULTS_FOUND;
    }

    if (g_interrupted)
    {
        ret |= INTERRUPTED;
    }
    update_committed_buffer();
    return ret;
}

verification_result_t Pass::test_next_op(LBABitmap*bm, extent_operation_t* op, bool quiet)
{
    uint32_t length;
    uint64_t offset;
    uint64_t extent_num;
    uint64_t extent_count;
    void* write_buffer = segment_->get_write_buffer(0);

    *op = get_next_operation(&offset, &length);
    if (*op == TRIM_OP)
    {
        extent_num = tlfsr_->current() - 1;
        extent_count = extents_trimmed_;
        memset(write_buffer, 0, segment_->buffer_size_);
    }
    else
    {
        extent_num = wlfsr_->current() - 1;
        extent_count = extents_written_;
        generate_payload((uint64_t*)write_buffer, extent_num, offset, length);
    }

    return soft_verify_extent(offset, length, bm, write_buffer, *op, extent_num, extent_count, quiet);
}

void Pass::find_psync_interruption_point(LBABitmap* bm)
{
    uint64_t max_contiguous_unrecorded_ops;

    if (segment_->extent_status_ == OPEN_ENDED)
    {
        max_contiguous_unrecorded_ops = bei_->num_extents() - (extents_written_ + extents_trimmed_);
    }
    else if (segment_->extent_status_ == LAST_OPERATION_FAILED)
    {
        max_contiguous_unrecorded_ops = std::min(
                                                 1UL,
                                                 bei_->num_extents() - (extents_written_ + extents_trimmed_));
    }
    else
    {
        throw runtime_error("Unknown extent status");
    }

    // Walk forward in order and discover if the unrecorded extents made it to media.
    for (uint64_t i = 0 ; i < max_contiguous_unrecorded_ops && !g_interrupted; i++)
    {
        extent_operation_t op;
        int test_result = test_next_op(bm, &op, false);

        if (test_result != ALL_CORRECT)
        {
            if (test_result == COMPLETE_FAILURE)
            {
                // Assume the operation was never issued.
                segment_->extent_status_ = ALL_RECORDED;
            }
            else if (test_result == PARTIAL_FAILURE)
            {
                segment_->extent_status_ = LAST_OPERATION_FAILED;
            }
            rollback_operation(op, NULL);
            break;
        }
    }
}

/*
    When writing with libaio, members of a batch can be reordered, so the interruption point (as far
    as the state is concerned) is the first in operation order that did not completely hit the media.

    The state is left as if the last incomplete batch was never executed, that way on
    resume, it is rewritten.
*/
void Pass::find_async_interruption_point(LBABitmap* bm)
{
    uint64_t max_contiguous_unrecorded_ops;

    if (segment_->extent_status_ == OPEN_ENDED)
    {
        max_contiguous_unrecorded_ops = bei_->num_extents() - (extents_written_ + extents_trimmed_);
    }
    else if (segment_->extent_status_ == LAST_OPERATION_FAILED)
    {
        max_contiguous_unrecorded_ops = std::min(
                                                 bei_->num_extents() - (extents_written_ + extents_trimmed_),
                                                 (uint64_t)segment_->iodepth_);
    }
    else
    {
        throw runtime_error("Unknown extent status");
    }

    stack<extent_operation_t> ops_to_rollback;

    // Walk forward in order and discover if the unrecorded extents made it to media.
    for (uint64_t i = 0 ; i < max_contiguous_unrecorded_ops && !g_interrupted; i++)
    {
        extent_operation_t op;
        verification_result_t test_result = test_next_op(bm, &op, false);

        if (test_result == ALL_CORRECT)
        {
            /* If the contiguous executed extents covers any of the members of the scattered_acked
               list, remove them from the scattered_acked list.
            */
            deque<uint64_t>* sae_list = (op == TRIM_OP) ? &scattered_acked_trim_extents_ :
                                        &scattered_acked_write_extents_;
            uint64_t extent = (op == TRIM_OP) ? tlfsr_->current() - 1 :
                              wlfsr_->current() - 1;
            deque<uint64_t>::iterator it = sae_list->begin();
            deque<uint64_t>::iterator end = sae_list->end();

            while (!sae_list->empty() && it != end)
            {
                if (*it == extent)
                {
#if VERBOSE_PRINTS
                    segment_->lock_print_mutex();
                    cout<<"Seg "<<segment_->get_id()<<": Removing #"<<*it<<" from the scattered acked ";
                    cout<<(op == TRIM_OP) ? "trim" : "write"<<" extents list"<<endl;
                    segment_->unlock_print_mutex();
#endif
                    it = sae_list->erase(it);
                }
                else
                {
                    it++;
                }
            }
        }
        else
        {
            if (test_result == COMPLETE_FAILURE)
            {
                // Assume the operation was never issued.
                segment_->extent_status_ = ALL_RECORDED;
            }
            else if (test_result == PARTIAL_FAILURE)
            {
                segment_->extent_status_ = LAST_OPERATION_FAILED;
            }
            ops_to_rollback.push(op);
            break;
        }
    }

    // Test up to max_scattered_unrecorded_ops forward to mark them as verified.
    uint64_t max_scattered_unrecorded_ops = std::min(
            bei_->num_extents() - (extents_written_ + extents_trimmed_),
            (uint64_t)segment_->iodepth_);
    uint64_t complete_scattered_unrecorded_ops = 0;
    uint64_t partial_scattered_unrecorded_ops = 0;

    for (uint64_t i = 0 ; i < max_scattered_unrecorded_ops && !g_interrupted; i++)
    {
        extent_operation_t op;
        verification_result_t test_result = test_next_op(bm, &op, true);
        if (test_result == ALL_CORRECT)
        {
            complete_scattered_unrecorded_ops++;
        }
        else if (test_result == PARTIAL_FAILURE)
        {
            partial_scattered_unrecorded_ops++;
        }
        ops_to_rollback.push(op);
    }

    if (complete_scattered_unrecorded_ops > 0 || partial_scattered_unrecorded_ops)
    {
        segment_->lock_print_mutex();
        cout<<"Seg "<<segment_->get_id()<<": Found additional scattered ops, ";
        cout<<complete_scattered_unrecorded_ops<<" complete "<<partial_scattered_unrecorded_ops;
        cout<<" partial."<<endl;
        segment_->unlock_print_mutex();
    }

    while (!ops_to_rollback.empty())
    {
        rollback_operation(ops_to_rollback.top(), NULL);
        ops_to_rollback.pop();
    }
}

void Pass::find_atomic_interruption_point(LBABitmap* bm, vector<fault_record>* fault_vec)
{
    int vectors_until_boundary = 0;
    uint32_t num_vectors = vlfsr_->next();
    uint64_t max_contiguous_unrecorded_ops;
    stack<extent_operation_t> vector_operations;
    bool write_encountered_in_vector = false;

    if (segment_->extent_status_ == OPEN_ENDED)
    {
        max_contiguous_unrecorded_ops = bei_->num_extents() - (extents_written_ + extents_trimmed_);
        vectors_until_boundary = num_vectors;
    }
    else if (segment_->extent_status_ == LAST_OPERATION_FAILED)
    {
        // An entire group of vectors may be valid on media if the write was completed
        // but the ack was interrupted in flight.
        uint32_t num_extents_left = bei_->num_extents() - (extents_written_ + extents_trimmed_);
        max_contiguous_unrecorded_ops = num_vectors;
        if (max_contiguous_unrecorded_ops > num_extents_left)
        {
            max_contiguous_unrecorded_ops = num_extents_left;
        }
        vectors_until_boundary = max_contiguous_unrecorded_ops;
    }
    else
    {
        throw runtime_error("Unknown extent status");
    }

    // Walk forward in order and discover if the unrecorded extents made it to media.
    for (uint64_t i = 0 ; i < max_contiguous_unrecorded_ops && !g_interrupted; i++)
    {
        extent_operation_t op;
        int test_result = test_next_op(bm, &op, false);

        vector_operations.push(op);

        if (test_result != ALL_CORRECT)
        {
            if (test_result == COMPLETE_FAILURE)
            {
                // Assume the operation was never issued.
                segment_->extent_status_ = ALL_RECORDED;
            }
            else if (test_result == PARTIAL_FAILURE)
            {
                segment_->extent_status_ = LAST_OPERATION_FAILED;
            }

            /* Verify we're on an atomic boundary.  Be careful, because trims can accidently pass
             * verifications because they're just zeros after all.  This means that if a vector begins
             * with a trim (which passes) and is followed by a write which fails we can wrongfully
             * identify this as an incomplete atomic sequence.  Therefore only claim an incomplete atomic
             * sequence if a write (which cannot pass validation accidently) was encountered
             * previously in the vector.
             */
            if ((uint32_t)vectors_until_boundary != num_vectors && write_encountered_in_vector)
            {
                uint64_t extent_num = (op == TRIM_OP) ? tlfsr_->current() - 1 : wlfsr_->current() - 1;
                string extent_type_str = (op == WRITE_OP) ? "Write" : "Trim";
                stringstream expl;
                expl<<"Incomplete atomic write found.  "<<extent_type_str<<" extent #"<<extent_num;
                expl<<", was not completed and was vector "<<((num_vectors - vectors_until_boundary) + 1);
                expl<<" of "<<num_vectors<<" in the group."<<endl;
                fault_record rec = {expl.str()};
                fault_vec->push_back(rec);
                vlfsr_->previous();
            }

            // Rollback all previous operations in this vector group
            while (!vector_operations.empty())
            {
                // Partial writes not allowed when using atomics, so pass in the bm to be cleared.
                rollback_operation(vector_operations.top(), bm);
                vector_operations.pop();
            }
            break;
        }
        vectors_until_boundary--;

        if (op == WRITE_OP)
        {
            write_encountered_in_vector = true;
        }

        // If we are on an atomic write boundary, figure out where the next boundary is.
        if (vectors_until_boundary < 1)
        {
            uint32_t num_extents_left = bei_->num_extents() - (extents_written_ + extents_trimmed_);

            num_vectors = vlfsr_->next();
            write_encountered_in_vector = false;

            if (num_vectors > num_extents_left)
            {
                num_vectors = num_extents_left;
            }
            vectors_until_boundary = num_vectors;

            // Clear out stack of vectors
            while (!vector_operations.empty())
            {
                vector_operations.pop();
            }
        }
    }
}

void Pass::verify_scattered_acked_extents(LBABitmap* bm, vector<fault_record>* fault_vec)
{
    uint64_t max_scattered_unrecorded_ops = std::min(
            bei_->num_extents() - (extents_written_ + extents_trimmed_),
            (uint64_t)segment_->iodepth_);

    // Perform a hard verify on any scatter acked write extents
    deque<uint64_t>::iterator it = scattered_acked_write_extents_.begin();
    while (it != scattered_acked_write_extents_.end())
    {
        uint32_t length;
        uint64_t offset = bei_->get_offset_and_length(*it, &length);
        void* write_buffer = segment_->get_write_buffer(0);
        uint64_t extent_count = extents_written_;

        segment_->lock_print_mutex();
        cout<<"Seg "<<segment_->get_id()<<": Checking forward scattered write extent #"<<*it<<endl;
        segment_->unlock_print_mutex();

        // Discover which extent count this extent is.  Not efficient, but this not called often.
        LFSR wlfsr(wlfsr_->current(), 1, bei_->num_extents());

        while (1)
        {
            if (wlfsr.next() - 1 != *it)
            {
                extent_count++;
            }
            else
            {
                break;
            }

            if (extent_count - extents_written_ > max_scattered_unrecorded_ops)
            {
                throw runtime_error("Unable to find extent count.");
            }
        }
        generate_payload((uint64_t*)write_buffer, *it, offset, length);
        hard_verify_extent(offset, length, bm, write_buffer, fault_vec, WRITE_OP, *it, extent_count + 1);
        *it++;

        if (fault_vec->size() >= FAULT_MAX || g_interrupted)
        {
            return;
        }
    }

    // Perform a hard verify on any scatter acked trim extents
    it = scattered_acked_trim_extents_.begin();
    while (it != scattered_acked_trim_extents_.end())
    {
        uint32_t length;
        uint64_t offset = bei_->get_offset_and_length(*it, &length);
        void* write_buffer = segment_->get_write_buffer(0);
        uint64_t extent_count = extents_trimmed_;

        memset(write_buffer, 0, segment_->buffer_size_);

        segment_->lock_print_mutex();
        cout<<"Seg "<<segment_->get_id()<<": Checking forward scattered trim extent #"<<*it<<endl;
        segment_->unlock_print_mutex();

        // Discover which extent count this extent is.  Not efficient, but this not called often.
        LFSR tlfsr(tlfsr_->current(), 1, bei_->num_extents());

        while (1)
        {
            if (tlfsr.previous() - 1 != *it)
            {
                extent_count++;
            }
            else
            {
                break;
            }

            if (extent_count - extents_trimmed_ > max_scattered_unrecorded_ops)
            {
                throw runtime_error("Unable to find extent count.");
            }
        }
        hard_verify_extent(offset, length, bm, write_buffer, fault_vec, TRIM_OP, *it, extent_count + 1);
        *it++;

        if (fault_vec->size() >= FAULT_MAX || g_interrupted)
        {
            return;
        }
    }
}

void Pass::simple_verify(LBABitmap* bm, uint64_t extents_written, uint64_t extents_trimmed, vector<fault_record>* fault_vec, bool persistent_trim_supported)
{
    uint64_t extent_num;
    uint32_t length;
    uint64_t offset;
    void* write_buffer = segment_->get_write_buffer(0);

    LFSR wlfsr(seed_, 1, bei_->num_extents());
    // Back up one so the first value is our seed.
    wlfsr.previous();

    // Verify every extent written
    for (uint64_t extent_count = 0 ; extent_count < extents_written ; extent_count++)
    {
        extent_num = wlfsr.next() - 1;
        offset = bei_->get_offset_and_length(extent_num, &length);

        generate_payload((uint64_t*)write_buffer, extent_num, offset, length);
        hard_verify_extent(offset, length, bm, write_buffer, fault_vec, WRITE_OP, extent_num, extent_count + 1);

        if (fault_vec->size() >= FAULT_MAX || g_interrupted)
        {
            return;
        }
    }

    // Now walk through the trims
    LFSR tlfsr(seed_, 1, bei_->num_extents());
    memset(write_buffer, 0, segment_->buffer_size_);

    for (uint64_t extent_count = 0 ; extent_count < extents_trimmed ; extent_count++)
    {
        extent_num = tlfsr.previous() - 1;
        offset = bei_->get_offset_and_length(extent_num, &length);
        if (persistent_trim_supported)
        {
            hard_verify_extent(offset, length, bm, write_buffer, fault_vec,
                               TRIM_OP, extent_num, extent_count + 1);
            if (fault_vec->size() >= FAULT_MAX)
            {
                return;
            }
        }
        else
        {
            // Mark trimmed sectors as checked without verifying their contents
            bm->set_range(offset, length);
        }
        if (g_interrupted)
        {
            return;
        }
    }
}

Session::Session(uint64_t seed,
                 uint32_t segments,
                 int64_t fd,
                 vector<ioengine_t>* ioengine_vec,
                 uint32_t atomic_min_vectors,
                 uint32_t atomic_max_vectors,
                 uint32_t min_extent_len,
                 uint32_t max_extent_len,
                 uint64_t specified_sector_count,
                 uint32_t sector_size,
                 bool persistent_trim_supported,
                 bool use_o_direct,
                 double range_ratio,
                 double capacity_ratio,
                 uint64_t first_sector,
                 uint32_t iodepth,
                 long iodepth_batch,
                 long iodepth_batch_complete,
                 string statefile_path)
    : seed_(seed),
      use_o_direct_(use_o_direct),
      corruptions_found_(false),
      statefile_path_(statefile_path),
      fd_(fd),
      sector_size_(sector_size),
      persistent_trim_supported_(persistent_trim_supported),
      trims_possible_(capacity_ratio < 1.0)
{
    pthread_mutex_init(&write_statefile_mutex_, 0);
    pthread_mutex_init(&print_mutex_, 0);
    gettimeofday(&statefile_capture_timestamp_, NULL);
    uint64_t advertised_sector_count = lseek(fd_, 0, SEEK_END) / sector_size_;

#if defined(__OSX__)
    if (advertised_sector_count == 0) // File seek method returns zero on block devices on OSX.
    {
        unsigned int sectors = 0;
        // Use special ioctl to get size
        ioctl(fd_, DKIOCGETBLOCKCOUNT, &sectors);
        advertised_sector_count = (uint64_t)sectors;
    }
#endif

    if (advertised_sector_count == 0)
    {
        throw runtime_error("Cannot operate on a device with zero size.");
    }

    // Assume all devices containing 2 ^ 34 sectors or more are sparse.
    if (advertised_sector_count >= SPARSE_THRESHOLD)
    {
        if (specified_sector_count == 0)
        {
            throw runtime_error("--sector-count must be specified on sparsely "
                                "formatted block devices.");
        }
        physical_sector_count_ = specified_sector_count;
        if (range_ratio == 0)
        {
            range_ratio = DEFAULT_RANGE_RATIO_SPARSE;
        }
    }
    else // Normal Format
    {
        physical_sector_count_ = advertised_sector_count;
        if (range_ratio == 0)
        {
            range_ratio = DEFAULT_RANGE_RATIO;
        }
        else if (range_ratio > 1.0)
        {
            throw runtime_error("Range ratio > 1.0 is not allowed on non-sparse formats.");
        }
    }

    uint64_t range_length = static_cast<uint64_t>(double(physical_sector_count_) * range_ratio);
    if (capacity_ratio > 1.0 || capacity_ratio < 0.0)
    {
        throw runtime_error("Capacity ratio must be > 0 and no greater than 1.");
    }

    srand(seed_);

    if (range_ratio == 1.0)
    {
        first_sector = 0;
    }
    else if (first_sector == NO_FIRST_SECTOR_DEFINED)
    {
        // Choose a random start sector
        uint64_t num = rand();
        // Get a uint64_t random value
        double shift_magnitude = log2(RAND_MAX);
        uint32_t num_shifts = static_cast<uint64_t>((double)(sizeof(uint64_t) * 8) / shift_magnitude);
        for(uint32_t i = 0 ; i < num_shifts; i++)
        {
            num = (num << int(shift_magnitude)) | rand();
        }
        first_sector = num % (advertised_sector_count - range_length);
    }
    else if (first_sector > (advertised_sector_count - range_length))
    {
        stringstream expl;
        expl<<"With these settings the first sector must be <= ";
        expl<<advertised_sector_count - range_length<<".";
        throw runtime_error(expl.str());
    }

    uint64_t capacity_quantity = (range_ratio > 1.0) ?
                                 static_cast<uint64_t>(capacity_ratio * double(physical_sector_count_))
                                 : static_cast<uint64_t>(capacity_ratio * double(range_length));
    uint64_t capacity_allowance = capacity_quantity;
    uint64_t range_allowance = range_length;

    if (segments < 1)
    {
        throw runtime_error("Segments must be > 0.");
    }

    // Divide the range into segments.  Last segment gets the remainder.
    for (uint32_t i = 0 ; i < segments ; i++)
    {
        uint64_t seg_len = (i + 1 < segments) ? (range_length / segments) : range_allowance;
        uint64_t seg_pop_allowance = (i + 1 < segments) ? (capacity_quantity / segments) : capacity_allowance;
        ioengine_t ioengine = ioengine_vec->at(i);

        // Loose generation of a 62-bit random number
        uint64_t seg_seed62 = 0;
        seg_seed62 |= (uint64_t)rand();
        seg_seed62 = seg_seed62<<16;
        seg_seed62 |= (uint64_t)rand();
        if (seg_seed62 == 0)
        {
            seg_seed62++;
        }

        if (seg_len == 0)
        {
            throw runtime_error("Segment length zero.  Increase range ratio, or decrease number of segments.");
        }

        segments_.push_back(new Segment(this,
                                        i,
                                        seg_seed62,
                                        ioengine,
                                        atomic_min_vectors,
                                        atomic_max_vectors,
                                        min_extent_len,
                                        max_extent_len,
                                        first_sector,
                                        iodepth,
                                        iodepth_batch,
                                        iodepth_batch_complete,
                                        seg_pop_allowance,
                                        seg_len));
        first_sector += seg_len;
        range_allowance -= seg_len;
        capacity_allowance -= seg_pop_allowance;
    }
    write_statefile();
}


Session::Session(int fd, string statefile_path, Json::Value jsession)
    : seed_(jsession["seed"].asUInt64()),
      use_o_direct_(jsession["use_o_direct"].asBool()),
      corruptions_found_(jsession["corruptions_found"].asBool()),
      statefile_path_(statefile_path),
      physical_sector_count_(jsession["physical_sector_count"].asUInt64()),
      fd_(fd),
      sector_size_(jsession["sector_size"].asUInt()),
      persistent_trim_supported_(jsession["persistent_trim_supported"].asBool()),
      trims_possible_(jsession["trims_possible"].asBool())
{

    pthread_mutex_init(&write_statefile_mutex_, 0);
    pthread_mutex_init(&print_mutex_, 0);
    gettimeofday(&statefile_capture_timestamp_, NULL);
    Json::Value jsegments = jsession["segments"];
    for (Json::ValueIterator itr = jsegments.begin() ; itr != jsegments.end() ; itr++)
    {
        segments_.push_back(new Segment(this, *itr));
    }
}

Session::~Session()
{
    for (vector<Segment*>::iterator seg_ = segments_.begin(); seg_ != segments_.end(); ++seg_)
    {
        delete *seg_;
    }
    pthread_mutex_destroy(&write_statefile_mutex_);
}


void Segment::start_random_execute(random_execute_params* params)
{
    assert(params->segment == this);
    int ret = pthread_create(&thr_id_, NULL, randw_executor, (void*)params);
    if (ret != 0)
    {
        cerr<<"Error "<<ret<<" while creating thread for seg "<<id_<<endl;
        throw runtime_error("Halting due to thread creation error.");
    }
}

void Segment::start_verify(verify_params* params)
{
    assert(params->segment == this);
    int ret = pthread_create(&thr_id_, NULL, verify_executor, (void*)params);
    if (ret != 0)
    {
        cerr<<"Error "<<ret<<" while creating thread for seg "<<id_<<endl;
        throw runtime_error("Halting due to thread creation error.");
    }
}

void Segment::join()
{
    pthread_join(thr_id_, NULL);
}

void Session::print_fault_report(int id, vector<fault_record>* m_vec)
{
    uint32_t fault_count = 1;
    cerr<<"----------- Segment "<<id<<" report -----------"<<endl;
    for(vector<fault_record>::iterator it = m_vec->begin(); it != m_vec->end(); ++it)
    {
        cerr<<"* Fault "<<fault_count++<<":"<<endl;
        cerr<<it->explanation;
    }
    cerr<<"---------- End Segment "<<id<<" report --------"<<endl;
}

bool Session::is_pers_trim_supported()
{
    return persistent_trim_supported_;
}

uint32_t Segment::get_sector_size()
{
    return session_->sector_size_;
}

int64_t Segment::get_fd()
{
    return session_->fd_;
}

#ifdef LIBAIO_SUPPORTED
AIO_batch* Segment::get_free_batch(extent_operation_t op)
{
    AIO_batch* ret;

    if (free_batches_.empty())
    {
        ret = new AIO_batch();
    }
    else
    {
        ret = free_batches_.top();
        free_batches_.pop();
    }

    ret->in_error_ = false;
    ret->batch_type_ = op;
    assert(ret->aio_cbs_.empty());
    return ret;
}
#endif

void* Segment::get_write_buffer(uint32_t buffer_num)
{
    return write_buffers_[buffer_num];
}

void* Segment::get_read_buffer()
{
    return read_buffer_;
}

iovec_atomic* Segment::get_iovec_buffer()
{
    return iov_;
}

uint32_t Session::random_execute(vector<uint64_t>& extents, struct timeval *elapsed_time)
{
    assert(fd_ >= 0);
    if (corruptions_found_)
    {
        throw runtime_error("Cowardly refusing to write on a device with known corruptions.");
    }
    assert(extents.size() == segments_.size());

    vector<random_execute_params*> rw_params;
    for (vector<Segment*>::iterator seg_ = segments_.begin(); seg_ != segments_.end(); ++seg_)
    {
        uint64_t num_extents = extents.at((*seg_)->get_id());
        if (num_extents == 0)
        {
            continue;
        }
        else if (num_extents == WRITE_INDEFINITELY)
        {
            num_extents = 0;
        }
        else if (num_extents == COMPLETE_PASS)
        {
            num_extents = (*seg_)->extents_until_pass_closure();
        }

        random_execute_params* rw_param = new random_execute_params;
        rw_param->segment = *seg_;
        rw_param->extents = num_extents;
        rw_param->rc = 0;

        rw_params.push_back(rw_param);
    }

    struct timeval start, end;
    gettimeofday(&start, NULL);
    uint32_t rc = 0;

    for (vector<random_execute_params*>::iterator p_ = rw_params.begin(); p_ != rw_params.end(); ++p_)
    {
        (*p_)->segment->start_random_execute(*p_);
    }

    for (vector<random_execute_params*>::iterator p_ = rw_params.begin(); p_ != rw_params.end(); ++p_)
    {
        (*p_)->segment->join();
        rc |= (*p_)->rc;
    }

    gettimeofday(&end, NULL);
    timersub(&end, &start, elapsed_time);

    while (!rw_params.empty())
    {
        delete rw_params.back();
        rw_params.pop_back();
    }
    write_statefile();
    return rc;
}

uint64_t Session::get_extents_written()
{
    uint64_t extents_written = 0;
    for (vector<Segment*>::iterator seg_ = segments_.begin(); seg_ != segments_.end(); ++seg_)
        extents_written += (*seg_)->get_extents_written();
    return extents_written;
}

uint64_t Session::get_extents_trimmed()
{
    uint64_t extents_trimmed = 0;
    for (vector<Segment*>::iterator seg_ = segments_.begin(); seg_ != segments_.end(); ++seg_)
        extents_trimmed += (*seg_)->get_extents_trimmed();
    return extents_trimmed;
}

uint64_t Session::get_sectors_written()
{
    uint64_t sectors_written = 0;
    for (vector<Segment*>::iterator seg_ = segments_.begin(); seg_ != segments_.end(); ++seg_)
        sectors_written += (*seg_)->get_sectors_written();
    return sectors_written;
}

uint64_t Session::get_sectors_trimmed()
{
    uint64_t sectors_trimmed = 0;
    for (vector<Segment*>::iterator seg_ = segments_.begin(); seg_ != segments_.end(); ++seg_)
        sectors_trimmed += (*seg_)->get_sectors_trimmed();
    return sectors_trimmed;
}

uint32_t Session::get_num_segments()
{
    return (uint32_t)segments_.size();
}

void Session::print_seg_operations(uint32_t id, uint64_t num_passes)
{
    if (id > get_num_segments() - 1)
    {
        stringstream expl;
        expl<<id<<" is an invalid segment number."<<endl;
        throw runtime_error(expl.str());
    }
    segments_[id]->print_operations(num_passes);
}

uint32_t Session::verify(int seg_num, bool skip_unwritten_sectors, timeval * elapsed_time, bool only_unrecorded)
{
    assert(fd_ >=0);

    if (persistent_trim_supported_ && trims_possible_)
    {
        cout<<"WARNING: Trims will be verified to be persistent since --no-pers-trim ";
        cout<<"was not passed during write the phase.  If the device does not support"<<endl;
        cout<<"persistent trim, false positives will be encountered  on verification."<<endl;
    }

    uint32_t ret = 0;
    vector<verify_params*> v_params;
    bool any_interrupted = false;

    for (vector<Segment*>::iterator seg_ = segments_.begin(); seg_ != segments_.end(); ++seg_)
    {
        if (seg_num == -1 || // means all
            (*seg_)->get_id() == (uint32_t)seg_num)
        {
            verify_params* v_param = new verify_params;
            v_param->segment = *seg_;
            v_param->persistent_trim_supported = persistent_trim_supported_;
            v_param->skip_unwritten_sectors = skip_unwritten_sectors;
            v_param->only_unrecorded = only_unrecorded;
            v_param->rc = LOGIC_ERROR;

            v_params.push_back(v_param);
        }
        if ((*seg_)->extent_status_ != ALL_RECORDED)
        {
            any_interrupted = true;
        }
    }

    if (v_params.size() < 1)
    {
        throw runtime_error("No matching segment. Did you pass a valid segment number?");
    }

    if (any_interrupted)
    {
        cout<<"State file indicates the write session was interrupted."<<endl;
        if (only_unrecorded)
        {
            cout<<"Unacked extents will be verified to re-establish state."<<endl;
        }
    }
    else if (only_unrecorded)
    {
        // There are no unrecorded packets, so short circuit here.
        ret = NO_FAULTS_FOUND;
        goto exit;
    }

    struct timeval start, end;
    gettimeofday(&start, NULL);

    cout<<"Beginning verification using statefile "<<statefile_path_<<"."<<endl;
    for (vector<verify_params*>::iterator p_ = v_params.begin(); p_ != v_params.end(); ++p_)
        (*p_)->segment->start_verify(*p_);

    for (vector<verify_params*>::iterator p_ = v_params.begin(); p_ != v_params.end(); ++p_)
        (*p_)->segment->join();

    gettimeofday(&end, NULL);
    timersub(&end, &start, elapsed_time);

    for (vector<verify_params*>::iterator p_ = v_params.begin(); p_ != v_params.end(); ++p_)
    {
        ret |= (*p_)->rc;

        if ((*p_)->rc == NO_FAULTS_FOUND)
        {
            assert ((*p_)->fault_vec.empty());
        }

        if ((*p_)->rc & FAULTS_FOUND)
        {
            assert (!(*p_)->fault_vec.empty());
            corruptions_found_ = true;
            print_fault_report((*p_)->segment->get_id(), &(*p_)->fault_vec);
        }

        if ((*p_)->rc & LOGIC_ERROR)
        {
            cerr<<"An error occurred in segment "<<(*p_)->segment->get_id()<<endl;
        }
    }
    write_statefile();

exit:

    while (!v_params.empty())
    {
        delete v_params.back();
        v_params.pop_back();
    }
    return ret;
}


bool Session::corruptions_found()
{
    return corruptions_found_;
}


void Session::write_statefile()
{
    // Record the time we began waiting for the lock.
    struct timeval wait_start_time;

    gettimeofday(&wait_start_time, NULL);

    pthread_mutex_lock(&write_statefile_mutex_);

    // Optimization: Skip if the statefile was written by another thread after we requested a write.
    if (timercmp(&statefile_capture_timestamp_, &wait_start_time, >))
    {
        pthread_mutex_unlock(&write_statefile_mutex_);
        return;
    }
    gettimeofday(&statefile_capture_timestamp_, NULL);

    Json::Value jroot;
    Json::Value jsession;
    ostringstream oss (ostringstream::out);
    jsession["seed"] = (Json::UInt64)seed_;
    jsession["sector_size"] = (Json::UInt64)sector_size_;
    jsession["physical_sector_count"] = (Json::UInt64)physical_sector_count_;
    // Jsoncpp fails for some reason when loading bool values, so use int
    jsession["persistent_trim_supported"] = (Json::UInt)persistent_trim_supported_;
    jsession["trims_possible"] = (Json::UInt)trims_possible_;
    jsession["use_o_direct"] = (Json::UInt)use_o_direct_;
    jsession["corruptions_found"] = (Json::UInt)corruptions_found_;

    Json::Value jsegments;
    for (vector<Segment*>::iterator seg_ = segments_.begin(); seg_ != segments_.end(); ++seg_)
    {
        jsegments.append((*seg_)->get_json());
    }
    jsession["segments"] = jsegments;
    jroot["session"] = jsession;
    Json::StyledStreamWriter writer("    ");
    writer.write(oss, jroot);

    // Do this the old C way so we can perform an fsync.
    mode_t mode = S_IRUSR | S_IWUSR;
    int fd = open(statefile_path_.c_str(), O_WRONLY | O_TRUNC | O_SYNC | O_CREAT, mode);
    stringstream expl;
    bool throw_it = false;

    if (fd < 0)
    {
        expl<<strerror(errno)<<" while opening statefile.";
        throw_it = true;
    }
    if(write(fd, oss.str().c_str(), oss.str().size()) < 1)
    {
        expl<<strerror(errno)<<" while writing statefile.";
        throw_it = true;
    }
    if (fsync(fd) != 0)
    {
        expl<<strerror(errno)<<" while fsyncing statefile.";
        throw_it = true;
    }
    if(close(fd) != 0)
    {
        expl<<strerror(errno)<<" while closing statefile.";
        throw_it = true;
    }
    pthread_mutex_unlock(&write_statefile_mutex_);

    if (throw_it)
    {
        throw runtime_error(expl.str());
    }
}

void Session::write_statefile_wrapper(const void* ptr_to_Session)
{
    ((Session*)ptr_to_Session)->write_statefile();
}

uint32_t Segment::verify(bool persistent_trim_supported,
                         bool skip_unwritten_sectors,
                         bool only_unrecorded,
                         vector<fault_record>* fault_vec)
{
    // Records what sectors have been checked
    LBABitmap* bm = new LBABitmap(length_);
    uint32_t fp_result = ~0;
    uint32_t cp_result = ~0;

    try
    {
        cp_result = current_pass_->verify(bm, fault_vec, persistent_trim_supported,
                                          true, only_unrecorded);
        lock_print_mutex();
        cout<<"Seg "<<id_<<": Checked "<<bm->hamming_weight()<<" sectors in the current pass."<<endl;
        if (cp_result & FAULTS_FOUND)
        {
            cout<<"Seg "<<id_<<": Found "<<fault_vec->size()<<" fault(s)."<<endl;
        }
        unlock_print_mutex();

        if (only_unrecorded || fault_vec->size() >= FAULT_MAX || cp_result & INTERRUPTED || cp_result & IO_ERROR)
        {
            goto exit_verify;
        }

        uint64_t temp_weight = bm->hamming_weight();
        uint64_t temp_faults = fault_vec->size();
        if (former_pass_)
        {
            lock_print_mutex();
            cout<<"Seg "<<id_<<": Beginning verification of former pass."<<endl;
            unlock_print_mutex();
            fp_result = former_pass_->verify(bm, fault_vec, persistent_trim_supported,
                                             false, false);
            lock_print_mutex();
            cout<<"Seg "<<id_<<": Checked "<<(bm->hamming_weight()-temp_weight)<<" sectors in the former pass."<<endl;
            if (fp_result & FAULTS_FOUND)
            {
                cout<<"Seg "<<id_<<": Found "<<(fault_vec->size() - temp_faults)<<" fault(s)."<<endl;
            }
            unlock_print_mutex();
        }
        // Verify never written sectors, even if no persistent trim support
        else if (!skip_unwritten_sectors)
        {
            lock_print_mutex();
            cout<<"Seg "<<id_<<": Beginning verification of never written sectors."<<endl;
            unlock_print_mutex();
            fp_result = verify_remainder_sectors(bm, fault_vec);
            lock_print_mutex();
            cout<<"Seg "<<id_<<": Checked "<<(bm->hamming_weight()-temp_weight);
            cout<<" never written sectors."<<endl;
            if (fp_result & FAULTS_FOUND)
            {
                cout<<"Seg "<<id_<<": Found "<<(fault_vec->size() - temp_faults);
                cout<<" fault(s)."<<endl;
            }
            unlock_print_mutex();
        }

        if (cp_result == NO_FAULTS_FOUND
                && fp_result == NO_FAULTS_FOUND
                && !skip_unwritten_sectors)
        {
            // All sectors should have been checked.
            assert(bm->inverse_hamming_weight() == 0);
        }
    }
    catch (...)
    {
        delete bm;
        throw;
    }

exit_verify:
    uint32_t ret = cp_result;

    delete bm;
    if (fp_result != (uint32_t)~0)
    {
        ret |= fp_result;
    }

    return ret;
}


RunnableSegment::RunnableSegment(Session* parent_session)
    : session_(parent_session)
{
}

Segment::Segment(Session* parent_session, Json::Value jsegment)
    : RunnableSegment(parent_session),
      id_(jsegment["id"].asUInt64()),
      former_pass_(NULL),
      population_(NULL),
      extents_written_(jsegment["extents_written"].asUInt64()),
      extents_trimmed_(jsegment["extents_trimmed"].asUInt64()),
      sectors_written_(jsegment["sectors_written"].asUInt64()),
      sectors_trimmed_(jsegment["sectors_trimmed"].asUInt64()),
      seed_(jsegment["seed"].asUInt64()),
      ioengine_((ioengine_t)jsegment["ioengine"].asInt()),
      atomic_min_vectors_(jsegment["atomic_min_vectors"].asUInt()),
      atomic_max_vectors_(jsegment["atomic_max_vectors"].asUInt()),
      min_extent_len_(jsegment["min_extent_len"].asUInt()),
      max_extent_len_(jsegment["max_extent_len"].asUInt()),
      first_sector_(jsegment["first_sector"].asUInt64()),
      iodepth_(jsegment["iodepth"].asInt()),
      iodepth_batch_(jsegment["iodepth_batch"].asInt()),
      iodepth_batch_complete_(jsegment["iodepth_batch_complete"].asInt()),
      population_allowance_(jsegment["population_allowance"].asUInt64()),
      length_(jsegment["length"].asUInt64()),
      extent_status_((extent_status_t)(jsegment["extent_status"].asInt()))
{
    init();
    if (jsegment.isMember("former_pass"))
    {
        former_pass_ = new Pass(this, jsegment["former_pass"]);
    }
    current_pass_ = new Pass(this, jsegment["current_pass"]);
}


Segment::Segment(Session* parent_session,
                 const uint32_t id,
                 const uint64_t seed,
                 const ioengine_t ioengine,
                 const uint32_t atomic_min_vectors,
                 const uint32_t atomic_max_vectors,
                 const uint32_t min_extent_len,
                 const uint32_t max_extent_len,
                 const uint64_t first_sector,
                 const int iodepth,
                 const long iodepth_batch,
                 const long iodepth_batch_complete,
                 const uint64_t population_allowance,
                 const uint64_t length)
    : RunnableSegment(parent_session),
      id_(id),
      former_pass_(NULL),
      population_(NULL),
      extents_written_(0),
      extents_trimmed_(0),
      sectors_written_(0),
      sectors_trimmed_(0),
      seed_(seed),
      ioengine_(ioengine),
      atomic_min_vectors_(atomic_min_vectors),
      atomic_max_vectors_(atomic_max_vectors),
      min_extent_len_(min_extent_len),
      max_extent_len_(max_extent_len),
      first_sector_(first_sector),
      iodepth_(iodepth),
      iodepth_batch_(iodepth_batch),
      iodepth_batch_complete_(iodepth_batch_complete),
      population_allowance_(population_allowance),
      length_(length),
      extent_status_(ALL_RECORDED)
{
    init();
    current_pass_ = new Pass(this, 0);
}

void Segment::init()
{
    // Allocate aligned memory buffers
    int ps = getpagesize();
    int write_buffers_to_allocate = 0;
    iov_ = NULL;
#ifdef LIBAIO_SUPPORTED
    p_aio_events_ = NULL;
    aiocbs_ = NULL;
#endif

    pthread_mutex_init(&pass_transition_lock_, 0);
    /* Calculate the pagesize divisible amount of memory we need to
     * allocate to hold the max_extent_size.  Use ceiling division. */
    buffer_size_ = (((max_extent_len_ * get_sector_size())
                     * sizeof(uint64_t)) / ps) * ps;

    if (ufio_memalign((void**)(&read_buffer_), ps, buffer_size_))
    {
        throw runtime_error("Error while allocating read buffer.");
    }

    if (ioengine_ == ATOMIC)
    {
        assert (max_extent_len_ * get_sector_size() <= IO_VECTOR_MAX_SIZE);
        iov_ = (iovec_atomic *)malloc(IO_VECTOR_LIMIT
                                      * sizeof(iovec_atomic));
        if (iov_ == NULL)
        {
            throw runtime_error("Error while allocating io vector.");
        }
        write_buffers_to_allocate = atomic_max_vectors_;
    }
    else if (ioengine_ == PSYNC)
    {
        write_buffers_to_allocate = 1;
    }
    else if (ioengine_ == LIBAIO)
    {
        assert(iodepth_ > 0 && iodepth_ <= MAX_IO_DEPTH);
        write_buffers_to_allocate = iodepth_;
    }
    else
    {
        throw runtime_error("Unrecognized ioengine.");
    }

    // Allocate as many write buffers as needed, NULL out the remaining pointers.
    for (int i = 0 ; i < MAX_IO_DEPTH; i++)
    {
        if (i < write_buffers_to_allocate)
        {
            if (ufio_memalign((void**)(&write_buffers_[i]),
                              ps, buffer_size_))
            {
                throw runtime_error("Error while allocating write buffer.");
            }
            memset(write_buffers_[i], 0, buffer_size_);
        }
        else
        {
            write_buffers_[i] = NULL;
        }
    }

#ifdef LIBAIO_SUPPORTED
    if (ioengine_ == LIBAIO)
    {
        if (io_queue_init(iodepth_, &aio_context_id_))
        {
            throw runtime_error("Unable to set iodepth.  Check /proc/sys/fs/aio-max-nr");
        }
        p_aio_events_ = (struct io_event*)malloc(iodepth_ * sizeof(struct io_event));
        if (p_aio_events_ == NULL)
        {
            throw runtime_error("Unable to allocate aio_event structures.");
        }
        memset(p_aio_events_, 0, iodepth_ * sizeof(struct io_event));

        aiocbs_ = (aio_control_block*)malloc(iodepth_ * sizeof(aio_control_block));
        if (aiocbs_ == NULL)
        {
            throw runtime_error("Unable to allocate async event control block structures.");
        }
        memset(aiocbs_, 0, iodepth_ * sizeof(aio_control_block));

        // Permanently assign a write buffer to each aio control block.
        for (uint32_t i = 0 ; i < iodepth_ ; i++)
        {
            aiocbs_[i].write_buffer = write_buffers_[i];
            free_aio_cbs_.push(&aiocbs_[i]);
        }
    }
#endif
}

Segment::~Segment()
{
    delete current_pass_;
    if (population_ != NULL)
    {
        delete population_;
    }

    if (former_pass_ != NULL)
        delete former_pass_;

    free (read_buffer_);

    for (int i = 0 ; i < MAX_IO_DEPTH; i++)
    {
        if (write_buffers_[i] != NULL)
        {
            free (write_buffers_[i]);
        }
    }

    if (iov_ != NULL)
    {
        free (iov_);
    }

#ifdef LIBAIO_SUPPORTED
    if (p_aio_events_ != NULL)
    {
        free(p_aio_events_);
    }

    if (aiocbs_ != NULL)
    {
        free(aiocbs_);
        io_destroy(aio_context_id_);
    }

    if (ioengine_ == LIBAIO)
    {
        assert(aio_batches_.empty());
    }

    while (!free_batches_.empty())
    {
        delete free_batches_.top();
        free_batches_.pop();
    }
#endif

    pthread_mutex_destroy(&pass_transition_lock_);
}


void Segment::random_execute(uint64_t extents)
{
    bool run_forever = extents == 0;
    void (*write_sf_callback)(const void* ptr_to_Session) = Session::write_statefile_wrapper;

    while(run_forever || extents != 0)
    {
        uint64_t extents_written_orig = current_pass_->extents_written();
        uint64_t extents_trimmed_orig = current_pass_->extents_trimmed();
        bool pass_complete = false;

        // Record how many extents could be written by this call.
        extent_status_ = OPEN_ENDED;
        current_pass_->clear_scattered_acked_extents();

        // Write statefile using callback.
        write_sf_callback(session_);

        try
        {
            pass_complete = current_pass_->random_execute_extents(extents);
        }
        catch (const IOException &exc)
        {
            // Tack on LFSR positional information.
            throw IOException(string(exc.what()).append(current_pass_->get_last_acked_ops_str()));
        }

        if (pass_complete)
        {
            Pass* new_pass;
            Pass* old_pass;

#ifdef LIBAIO_SUPPORTED
            if (ioengine_ == LIBAIO)
            {
                /* Reap all outstanding before transitioning to next pass.
                 * Since AIO can be reordered, this ensures a write from the
                 * finishing pass cannot come after a write from the next pass.
                 */
                current_pass_->reap_all();
            }
#endif

            new_pass = new Pass(this, current_pass_->pass_num() + 1);
            old_pass = former_pass_;

            pthread_mutex_lock(&pass_transition_lock_);

            // Add this pass' statistics to the grand segment total
            extents_written_ += current_pass_->extents_written();
            extents_trimmed_ += current_pass_->extents_trimmed();
            sectors_written_ += current_pass_->sectors_written();
            sectors_trimmed_ += current_pass_->sectors_trimmed();

            former_pass_ = current_pass_;
            current_pass_ = new_pass;
            pthread_mutex_unlock(&pass_transition_lock_);

            if (old_pass != NULL)
            {
                delete old_pass;
            }

            if (!run_forever)
            {
                extents -= (former_pass_->extents_written() - extents_written_orig);
                extents -= (former_pass_->extents_trimmed() - extents_trimmed_orig);
            }

#if VERBOSE_PRINTS
            lock_print_mutex();
            cout<<"Seg "<<get_id()<<": --- Created new pass ---"<<endl;
            unlock_print_mutex();
#endif
        }
        else
        {
            // Skip writing the statefile here as it will be on Session::random_execute exit.
            break;
        }
    }
}

uint64_t Segment::extents_until_pass_closure()
{
    return current_pass_->num_extents()
           - (current_pass_->extents_written() + current_pass_->extents_trimmed());
}

uint32_t Segment::verify_remainder_sectors(LBABitmap* bm,
        vector<fault_record>* fault_vec)
{
    LBABitmap::LBA_range lr = {0, 0};
    uint32_t sector_size = get_sector_size();
    uint32_t orig_fault_count = fault_vec->size();
    uint32_t ret = NO_FAULTS_FOUND;

    while(bm->get_next_range(&lr, false, max_extent_len_, ~0))
    {
        int pread_ret;

        // Mark as verified
        bm->set_range(lr.offset, lr.length);

        uint64_t bytes_num = lr.length * sector_size;
        pread_ret = pread(get_fd(), read_buffer_, bytes_num, (lr.offset + first_sector_) * sector_size);

        if ((uint32_t)pread_ret != bytes_num)
        {
            stringstream expl;
            if (pread_ret < 0)
            {
                expl<<"Seg "<<get_id()<<": "<<strerror(errno)<<" while verifying remainder sector ";
                expl<<(lr.offset + first_sector_)<<", of byte length "<<bytes_num<<".";
            }
            else
            {
                expl<<"Seg "<<get_id()<<": pread reported "<<pread_ret<<" bytes written when ";
                expl<<bytes_num<<" were expected.";
            }
            g_interrupted = true;
            throw IOException(expl.str());
        }

        for(uint32_t i = 0 ; i < lr.length * sector_size / sizeof(uint64_t); i++)
        {
            if (((uint64_t*)read_buffer_)[i] != (uint64_t)0)
            {
                uint64_t byte_offset = (uint64_t)(lr.offset + first_sector_) * sector_size;
                uint64_t byte_length = (uint64_t)lr.length * sector_size;
                stringstream expl;
                expl<<"Non-zero data found ~"<<(i*sizeof(uint64_t))<<" bytes into a read ";
                expl<<"that began on byte offset "<<byte_offset<<" and extended ";
                expl<<byte_length<<" bytes."<<endl;

                stringstream ss;
                ss<<"seg"<<get_id()<<"_zero_check_byte_off"<<byte_offset<<"_len";
                ss<<byte_length<<OBSERVED_EXTENSION;
                FILE* oFile = fopen(ss.str().c_str(), "wb");
                bool success = true;
                if (fwrite(read_buffer_, 1, byte_length , oFile) < byte_length)
                {
                    success = false;
                }

                if (fclose(oFile))
                {
                    success = false;
                }

                if (success)
                {
                    expl<<"Observed data written to file "<<ss.str()<<endl;
                }
                else
                {
                    expl<<"Error while writing observed data to file."<<endl;
                }

                fault_record rec = {expl.str()};
                fault_vec->push_back(rec);
                if (fault_vec->size() >= FAULT_MAX)
                {
                    lock_print_mutex();
                    cerr<<"Seg "<<get_id()<<": Aborting unwritten sector verify due to number of faults."<<endl;
                    unlock_print_mutex();
                    return FAULTS_FOUND;
                }
                break;
            }
        }
        if (g_interrupted)
        {
            ret |= INTERRUPTED;
            break;
        }
    }

    if (fault_vec->size() > orig_fault_count)
    {
        ret |= FAULTS_FOUND;
    }
    return ret;
}

uint64_t Segment::get_extents_written()
{
    return extents_written_ + current_pass_->extents_written();
}

uint64_t Segment::get_extents_trimmed()
{
    return extents_trimmed_ + current_pass_->extents_trimmed();
}

uint64_t Segment::get_sectors_written()
{
    return sectors_written_ + current_pass_->sectors_written();
}

uint64_t Segment::get_sectors_trimmed()
{
    return sectors_trimmed_ + current_pass_->sectors_trimmed();
}

uint32_t Segment::get_id()
{
    return id_;
}

bool Segment::sparse_layout()
{
    return length_ > population_allowance_;
}

void Segment::print_operations(uint64_t num_passes)
{
    uint64_t pass_num;
    Pass* pass = NULL;

    if (extent_status_ == OPEN_ENDED)
    {
        stringstream expl;
        expl<<"Statefile needs to be synced with the device.  Perform a ";
        expl<<"a verification first, then try again."<<endl;
        throw runtime_error(expl.str());
    }

    if (num_passes > current_pass_->pass_num() + 1)
    {
        stringstream expl;
        expl<<"Cannot print operations for "<<num_passes<<" passes, only ";
        expl<<(current_pass_->pass_num() + 1)<<" have been executed."<<endl;
        throw runtime_error(expl.str());
    }
    if (num_passes == 0)
    {
        num_passes = std::min(current_pass_->pass_num() + 1, 2UL);
    }

    pass_num = (current_pass_->pass_num() + 1) - num_passes;
    assert(pass_num <= current_pass_->pass_num());

    for (; pass_num <= current_pass_->pass_num() ; pass_num++)
    {
        cout<<"new_pass, "<<pass_num<<endl;
        pass = new Pass(this, pass_num);
        uint64_t end_extent_num;
        if(pass_num == current_pass_->pass_num())
        {
            end_extent_num = current_pass_->possible_extents_executed();
        }
        else
        {
            end_extent_num = pass->num_extents();
        }
        pass->print_operations(end_extent_num);
        delete pass;
    }
}

void Pass::clear_scattered_acked_extents()
{
    scattered_acked_write_extents_.clear();
    scattered_acked_trim_extents_.clear();
}

void RunnableSegment::lock_print_mutex()
{
    pthread_mutex_lock(&(session_->print_mutex_));
}

void RunnableSegment::unlock_print_mutex()
{
    pthread_mutex_unlock(&(session_->print_mutex_));
}

void Segment::invalidate_population()
{
    if (population_ != NULL)
    {
        delete population_;
        population_ = NULL;
    }
}

LBABitmap* Segment::get_population()
{
    if (population_ == NULL)
    {
        population_ = new LBABitmap(length_);

        if (former_pass_ != NULL)
        {
            former_pass_->build_population(population_, false);
        }
        current_pass_->build_population(population_, true);
    }
    return population_;
}


Json::Value Segment::get_json()
{
    pthread_mutex_lock(&pass_transition_lock_);
    Json::Value jsegment;
    jsegment["id"] = (Json::UInt)id_;
    jsegment["seed"] = (Json::UInt64)seed_;
    jsegment["ioengine"] = (Json::Int)ioengine_;
    jsegment["atomic_min_vectors"] = (Json::UInt)atomic_min_vectors_;
    jsegment["atomic_max_vectors"] = (Json::UInt)atomic_max_vectors_;
    jsegment["min_extent_len"] = (Json::UInt)min_extent_len_;
    jsegment["max_extent_len"] = (Json::UInt)max_extent_len_;
    jsegment["first_sector"] = (Json::UInt64)first_sector_;
    jsegment["iodepth"] = (Json::Int)iodepth_;
    jsegment["iodepth_batch"] = (Json::Int)iodepth_batch_;
    jsegment["iodepth_batch_complete"] = (Json::Int)iodepth_batch_complete_;
    jsegment["extents_written"] = (Json::UInt64)extents_written_;
    jsegment["extents_trimmed"] = (Json::UInt64)extents_trimmed_;
    jsegment["sectors_written"] = (Json::UInt64)sectors_written_;
    jsegment["sectors_trimmed"] = (Json::UInt64)sectors_trimmed_;
    jsegment["extent_status"] = (Json::Int)extent_status_;
    jsegment["population_allowance"] = (Json::UInt64)population_allowance_;
    jsegment["length"] = (Json::UInt64)length_;
    jsegment["current_pass"] = current_pass_->get_json();
    if (former_pass_)
    {
        jsegment["former_pass"] = former_pass_->get_json();
    }
    pthread_mutex_unlock(&pass_transition_lock_);
    return jsegment;
}
