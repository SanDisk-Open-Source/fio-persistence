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

#include <limits.h>
#include "lfsr.h"

#if defined(__FreeBSD__)
double log2(double max)
{
    return log((double)max)/log((double)2);
}
#endif

Base2LFSR::Base2LFSR (uint64_t seed, uint32_t register_degree)
    : lfsr_(seed)
{
    if (lfsr_ == 0)
    {
        throw std::runtime_error("Seed cannot be zero.");
    }

    if (register_degree < MIN_DEGREE || register_degree > MAX_DEGREE)
    {
        throw std::runtime_error("Register degree out of range.");
    }

    // Massage seed so it is in the valid range and not zero.
    if (register_degree < sizeof(seed) * 8)
    {
        lfsr_ = lfsr_ % ((uint64_t)1<<register_degree);
    }

    if (!lfsr_)
    {
        lfsr_++;
    }

    tap_ = taps_[register_degree];

    reversal_ob_ = (uint64_t)1<<(register_degree - 1);

    if (register_degree == sizeof(uint64_t) * CHAR_BIT)
    {
        reversal_mask_ = ~0x0;
    }
    else
    {
        reversal_mask_ = ((uint64_t)1<<register_degree) - 1;
    }
}

Base2LFSR::~Base2LFSR()
{
}

uint64_t Base2LFSR::next()
{
    lfsr_ = (lfsr_ >> 1) ^ (-((int64_t)lfsr_ & (int64_t)1) & tap_);
    return lfsr_;
}

uint64_t Base2LFSR::previous()
{
    uint64_t osb = lfsr_ & reversal_ob_;
    if (osb != 0)
    {
        lfsr_ ^= tap_;
        lfsr_ <<= 1;
        lfsr_ +=  1;
    }
    else
    {
        lfsr_ <<=  1;
    }
    lfsr_ &= reversal_mask_;
    return lfsr_;
}

void Base2LFSR::populate_buffer(uint64_t* buf, uint64_t num)
{
    // Optimized "next" for buffer output
    buf[0] = (lfsr_ >> 1) ^ (-((int64_t)lfsr_ & (int64_t)1) & tap_);
    for(uint64_t i = 0 ; i <= num ; i++)
    {
        buf[i+1] = (buf[i] >> 1) ^ (-(((int64_t)buf[i]) & 1) & tap_);
    }
    lfsr_ = buf[num-1];
}

uint64_t Base2LFSR::current()
{
    return lfsr_;
}

const uint64_t Base2LFSR::taps_ [] = { 0x0ULL, 0x0ULL, 0x0ULL, // 0,1,2 not valid
                                       0x6ULL,
                                       0xcULL,
                                       0x14ULL,
                                       0x30ULL,
                                       0x60ULL,
                                       0xb8ULL,
                                       0x110ULL,
                                       0x240ULL,
                                       0x500ULL,
                                       0x829ULL,
                                       0x100dULL,
                                       0x2015ULL,
                                       0x6000ULL,
                                       0xd008ULL,
                                       0x12000ULL,
                                       0x20400ULL,
                                       0x40023ULL,
                                       0x90000ULL,
                                       0x140000ULL,
                                       0x300000ULL,
                                       0x420000ULL,
                                       0xe10000ULL,
                                       0x1200000ULL,
                                       0x2000023ULL,
                                       0x4000013ULL,
                                       0x9000000ULL,
                                       0x14000000ULL,
                                       0x20000029ULL,
                                       0x48000000ULL,
                                       0x80200003ULL,
                                       0x100080000ULL,
                                       0x204000003ULL,
                                       0x500000000ULL,
                                       0x801000000ULL,
                                       0x100000001fULL,
                                       0x2000000031ULL,
                                       0x4400000000ULL,
                                       0xa000140000ULL,
                                       0x12000000000ULL,
                                       0x300000c0000ULL,
                                       0x63000000000ULL,
                                       0xc0000030000ULL,
                                       0x1b0000000000ULL,
                                       0x300003000000ULL,
                                       0x420000000000ULL,
                                       0xc00000180000ULL,
                                       0x1008000000000ULL,
                                       0x3000000c00000ULL,
                                       0x6000c00000000ULL,
                                       0x9000000000000ULL,
                                       0x18003000000000ULL,
                                       0x30000000030000ULL,
                                       0x40000040000000ULL,
                                       0xc0000600000000ULL,
                                       0x102000000000000ULL,
                                       0x200004000000000ULL,
                                       0x600003000000000ULL,
                                       0xc00000000000000ULL,
                                       0x1800300000000000ULL,
                                       0x3000000000000030ULL,
                                       0x6000000000000000ULL,
                                       0xd800000000000000ULL
                                     };

LFSR::LFSR (uint64_t seed, uint64_t min_value, uint64_t max_value)
    : Base2LFSR(massage_seed(seed, min_value, max_value), calc_reg_degree(max_value)),
      min_value_(min_value), max_value_(max_value)

{
    if (min_value_ < 1)
    {
        throw std::runtime_error("Minimum value must be > 0.");
    }
}

uint64_t LFSR::massage_seed(uint64_t seed, uint64_t min_value, uint64_t max_value)
{
    if (seed <= max_value && seed >= min_value)
    {
        return seed;
    }
    else if (min_value == max_value)
    {
        return min_value;
    }
    else
    {
        seed = seed % (max_value - min_value);
        return seed + min_value;
    }
}

uint32_t LFSR::calc_reg_degree(uint64_t max_value)
{
    return std::max((int)ceil(log2((double)max_value + 1)), MIN_DEGREE);
}

uint64_t LFSR::next()
{
    do
    {
        Base2LFSR::next();
    }
    while(lfsr_ > max_value_ || lfsr_ < min_value_);
    return lfsr_;
}

uint64_t LFSR::previous()
{
    do
    {
        Base2LFSR::previous();
    }
    while(lfsr_ > max_value_ || lfsr_ < min_value_);
    return lfsr_;
}


void speed_test_lfsr()
{
    uint64_t degree = 30;
    Base2LFSR lfsr(123, degree);
    uint64_t a;
    for(uint64_t i = 0 ; i < ((uint64_t)1<<degree) -1 ; i++)
    {
        a = lfsr.previous();
    }
    cout<<a<<endl;
}

void unit_test_lfsr()
{
    uint64_t seed = 7;
    uint64_t a = 0;

    cout<<"Beginning wraparound test."<<endl;
    for (uint64_t degree = MIN_DEGREE ; degree < 34 ; degree++)
    {
        cout<<"Testing degree "<<degree<<" forwards"<<endl;
        Base2LFSR lfsr(seed, degree);
        for(uint64_t i = 0 ; i < ((uint64_t)1<<degree) - 1 ; i++)
        {
            a=lfsr.next();
        }
        if (a != seed)
        {
            cout<<"Error in forward degree test, "<<a<<" != "<<seed<<endl;
            return;
        }

        cout<<"Testing degree "<<degree<<" reverse"<<endl;

        for(uint64_t i = 0 ; i < ((uint64_t)1<<degree) - 1 ; i++)
        {
            a=lfsr.next();
        }
        if (a != seed)
        {
            cout<<"Error in reverse degree test, "<<a<<" != "<<seed<<endl;
            return;
        }
    }

    cout<<"Testing ranges of remaining degrees."<<endl;
    uint64_t play_length = 1000000;
    for (int degree = 35 ; degree <= MAX_DEGREE ; degree++)
    {
        cout<<"Testing degree "<<degree<<endl;
        Base2LFSR lfsr(seed, degree);
        // Rewind it
        for (uint64_t i = 0 ; i < play_length; i++)
        {
            a=lfsr.previous();
        }
        // Play it
        for (uint64_t i = 0 ; i < play_length ; i++)
        {
            a=lfsr.next();
        }

        if (a != seed)
        {
            cout<<"Error while playing it, "<<a<<" != "<<seed<<endl;
            return;
        }

        // Play it
        for (uint64_t i = 0 ; i < play_length ; i++)
        {
            a=lfsr.next();
        }
        for (uint64_t i = 0 ; i < play_length ; i++)
        {
            a=lfsr.previous();
        }

        if (a != seed)
        {
            cout<<"Error while rewinding it, "<<a<<" != "<<seed<<endl;
            return;
        }
    }
}
