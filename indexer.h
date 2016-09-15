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

#include <assert.h>
#include <vector>
#include <stdint.h>
#include <iostream>
#include <sstream>

#include "lfsr.h"
using namespace std;

/**
 * @brief Utility class that maps variable length write extents to an open
 * ended block address space.
 */
class ExtentIndexer
{
    uint32_t seed_;
    uint32_t extents_per_index;
    uint32_t min_extent_len_;
    uint32_t max_extent_len_;
    uint64_t num_extent_sizes_;
    uint64_t sectors_in_loop_;
    uint64_t num_extents_;
    typedef struct
    {
        uint64_t offset;
        uint32_t length;
    } offset_length;

    vector<offset_length> index_;
    // Bounds memory usage at about 16 MB.
    const static uint32_t max_indexes_ = 1000000;

public:
    /** @brief Constructs an  ExtentIndexer instance.
     * @param seed The seed.  Does not need to be in extent range.
     * @param min_extent_len_ The minimum extent size in sectors.
     * @param max_extent_len_ The maximum extent size in sectors.
     */
    ExtentIndexer(uint32_t seed_, uint32_t min_extent_len,
                  uint32_t max_extent_len);
    virtual ~ExtentIndexer();
    /** @brief Returns the sector offset and write extent for a given
     * extent utilizing an index for quick lookup.
     * @param extent_num The extent number to locate.
     * @param extent_length A pointer to where the extent length (in
     * sectors) is to be stored.
     * @return The sector offset.
     */
    virtual uint64_t get_offset_and_length(uint64_t extent_num,
                                           uint32_t* extent_length);
    /** @brief Returns the number of extents a block device of a given
     * sectors size can hold.
     * @param sectors Number of sectors in the block device.
     * @return Number of extents the block device can hold.
     */
    uint64_t num_extents(uint64_t sectors);
};


/**
 * @brief Utility class that maps variable length write extents to a block
 * device.  All sectors are mapped within the block device bounds.
 */
class BoundedExtentIndexer : public ExtentIndexer
{
    uint64_t sectors_;
    uint64_t rem_extent_offset_;
    uint64_t last_lfsr_extent_;
    uint32_t rem_extent_length_;

public:
    /** @brief Constructs a BoundedExtentIndexer instance.
     * @param seed Seed value used for generating extent layout.
     * @param min_extent_len_ The minimum extent size. Note: The last
     * extent may be smaller than this in order to fill the remainder.
     * @param max_extent_len_ The maximum extent size.
     * @param sectors The number of sectors on the block device.
     */
    BoundedExtentIndexer(uint32_t seed, uint32_t min_extent_len,
                         uint32_t max_extent_len, uint64_t sectors);
    /** @brief Returns the sector offset and write extent for a given
     * extent utilizing an index for quick lookup.  Throws a Runtime
     * exception if extent is out of bounds.
     * @param extent_num The extent number to locate.
     * @param extent_length A pointer to where the extent length is to be
     * stored.
     * @return The sector offset.
     */
    virtual uint64_t get_offset_and_length(uint64_t extent_num,
                                           uint32_t* extent_length);
    /** @brief Returns the number of extents a block device of a given
     * sectors size can hold.
     * @return Number of write extents this block device can hold.
     */
    uint64_t num_extents();
};
