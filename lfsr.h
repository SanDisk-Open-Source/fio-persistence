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
#include <exception>
#include <stdexcept>
#include <cmath>
#include <stdint.h>
#include <iostream>
#include <algorithm>

#define MAX_DEGREE 64
#define MIN_DEGREE 3

#ifdef __FreeBSD__
extern double log2(double max);
#endif

using namespace std;

void unit_test_lfsr();
void speed_test_lfsr();

/**
 * @class Base2LFSR
 *
 * @brief A bi-directional Galois LFSR which operates exclusively on all base
 * 2 register degrees betwen 2 ^ MIN_DEGREE and 2 ^ MAX_DEGREE.
 *
 */
class Base2LFSR
{
    // Taps indexed by number of bits in period.
    const static uint64_t taps_ [];

protected:
    uint64_t lfsr_;
    uint64_t tap_;
    uint64_t reversal_mask_;
    uint64_t reversal_ob_;

public:

    /** @brief Constructs a Base2LFSR instance.
      * @param seed The initial value of the LFSR, if it is within the range.
      * @param register_degree The width (int bits) of the LFSR.  Must be
      * between MIN_DEGREE and MAX_DEGREE.
      */
    Base2LFSR(uint64_t seed, uint32_t register_degree);
    virtual ~Base2LFSR();

    // @brief Advances the lfsr one step and returns the result.
    virtual uint64_t next();
    // @brief Backsteps the lfsr and returns the result.
    virtual uint64_t previous();
    /** @brief Populates a buffer with a set of lfsr values.
      * @param buf A pointer to the buffer.
      * @param num Number of uint64_t values to write to the buffer.
      */
    void populate_buffer(uint64_t* buf, uint64_t num);
    // @brief Returns the current value.
    uint64_t current();
};


/**
 * @class LFSR
 *
 * @brief A bi-directional Galois LFSR which operates between values between
 * 2 ^ MIN_DEGREE and 2 ^ MAX_DEGREE.
 *
 */
class LFSR : public Base2LFSR
{
    uint64_t min_value_;
    uint64_t max_value_;
    /** @brief Calculates which register degree is suitable for a given max
      * value.
      * @param max_value The maximum value the LFSR should produce.  Must be
      * less than 2 ^ MAX_DEGREE.
      */
    static uint32_t calc_reg_degree(uint64_t max_value);
    /** @brief Normalizes the seed to be within the min and max range.
     * @param seed The seed.
      * @param min_value The smallest value the LFSR may return. Must be > 0.
      * @param max_value The largest value the LFSR may return.   Must be
      * between 1 and 2 ^ MAX_DEGREE.
     * @return Returns the normalized seed.
     */
    static uint64_t massage_seed(uint64_t seed, uint64_t min_value, uint64_t max_value);

public:
    /** @brief Constructs an LFSR instance.
      * @param seed The initial value of the LFSR.
      * @param min_value The smallest value the LFSR may return. Must be > 0.
      * @param max_value The largest value the LFSR may return.   Must be
      * between 1 and 2 ^ MAX_DEGREE.
      */
    LFSR (uint64_t seed, uint64_t min_value_, uint64_t max_value_);
    // @brief Advances the lfsr one step and returns the result.
    uint64_t next();
    // @brief Backsteps the lfsr and returns the result.
    uint64_t previous();
};
