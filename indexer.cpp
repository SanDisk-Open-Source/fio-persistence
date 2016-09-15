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

#include "indexer.h"


ExtentIndexer::ExtentIndexer(uint32_t seed, uint32_t min_extent_len, uint32_t max_extent_len)
    : seed_(seed), min_extent_len_(min_extent_len), max_extent_len_(max_extent_len), num_extents_(0)
{
    assert(min_extent_len_ > 0);
    assert(max_extent_len_ >= min_extent_len_);
    num_extent_sizes_ = (max_extent_len_ - min_extent_len_) + 1;
    // Formula for calculating triangular numbers
    sectors_in_loop_ = ((uint64_t)max_extent_len_ * ((uint64_t)max_extent_len_ + 1)) / (uint64_t)2;
    sectors_in_loop_ -= (((uint64_t)min_extent_len_ - 1) * ((uint64_t)min_extent_len_)) / (uint64_t)2;
    extents_per_index = (num_extent_sizes_ + max_indexes_ - 1) / max_indexes_;

    // Build the index
    LFSR lfsr(seed_, min_extent_len_, max_extent_len_);
    uint64_t offset = 0;
    for (uint64_t i = 0 ; i < num_extent_sizes_; i++)
    {
        if (i % extents_per_index == 0)
        {
            offset_length ol = {offset, (uint32_t)(lfsr.current())};
            index_.push_back(ol);
        }
        offset += lfsr.current();
        lfsr.next();
    }
}

ExtentIndexer::~ExtentIndexer()
{
}

uint64_t ExtentIndexer::get_offset_and_length(uint64_t extent_num, uint32_t* extent_length)
{
    uint64_t offset = 0;
    uint64_t loops = extent_num / num_extent_sizes_;
    uint64_t loop_remainder = extent_num % num_extent_sizes_;
    uint64_t index_num = loop_remainder / extents_per_index;

    // Jump forward to the correct loop
    offset += loops * sectors_in_loop_;
    *extent_length = index_[0].length;

    // Jump forward to the index preceding the entry we want
    if (index_num > 0)
    {
        offset += index_[index_num].offset;
        *extent_length = index_[index_num].length;
    }

    // Compute the entry
    uint64_t index_rem = loop_remainder % extents_per_index;
    if (index_rem > 0)
    {
        LFSR lfsr(*extent_length, min_extent_len_, max_extent_len_);
        for (uint64_t i = 0 ; i < index_rem ; i++)
        {
            offset += *extent_length;
            *extent_length = lfsr.next();
        }
    }
    return offset;
}

uint64_t ExtentIndexer::num_extents(uint64_t sectors)
{
    if (num_extents_ == 0)
    {
        // Advance to the correct loop
        num_extents_ = ((sectors / sectors_in_loop_) * num_extent_sizes_);
        uint64_t offset;
        uint32_t length;
        while (1)
        {
            offset = ExtentIndexer::get_offset_and_length(num_extents_, &length);
            if (offset + length > sectors)
            {
                break;
            }
            num_extents_++;
        }
    }
    return num_extents_;
}


BoundedExtentIndexer::BoundedExtentIndexer(uint32_t seed,
        uint32_t min_extent_len, uint32_t max_extent_len, uint64_t sectors)
    :ExtentIndexer(seed, min_extent_len, max_extent_len), sectors_(sectors),
     rem_extent_offset_(~0), rem_extent_length_(~0)
{
    last_lfsr_extent_ = ExtentIndexer::num_extents(sectors_);
    uint32_t length;
    uint64_t offset = ExtentIndexer::get_offset_and_length(
                          last_lfsr_extent_, &length);

    // Create one last extent in the remainder if possible by truncating the
    // extent that would have gone over.
    if (offset < sectors_)
    {
        rem_extent_offset_ = offset;
        rem_extent_length_ = sectors_ - offset;
    }
}


uint64_t BoundedExtentIndexer::get_offset_and_length(
    uint64_t extent_num, uint32_t* extent_length)
{
    // If this could possibly be the last remaining extent
    if (extent_num >= last_lfsr_extent_)
    {
        if ((extent_num > last_lfsr_extent_) || (rem_extent_offset_ == (uint64_t)~0))
        {
            stringstream expl;
            expl<<"Extent num "<<extent_num<<" out of range.  Num of extents is ";
            expl<<num_extents()<<endl;
            throw std::runtime_error(expl.str());
        }
        *extent_length = rem_extent_length_;
        return rem_extent_offset_;
    }
    else
    {
        return ExtentIndexer::get_offset_and_length(extent_num, extent_length);
    }
}

uint64_t BoundedExtentIndexer::num_extents()
{
    uint64_t num_extents = ExtentIndexer::num_extents(sectors_);
    if (rem_extent_offset_ != (uint64_t)~0)
    {
        num_extents += 1;
    }
    return num_extents;
}
