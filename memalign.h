//-----------------------------------------------------------------------------
// Copyright (c) 2016 Western Digital Corporation or its affiliates.
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

/// @brief allocate aligned memory
///
/// @param ptrptr    pointer to the memory that needs to be aligned
/// @param alignment alignment value desired by caller
/// @param size      size of the memory requested by caller
///
/// @return 0      on success
///         EINVAL if alignment is not a power of two or is not a multiple of
///                sizeof(void *)
///         ENOMEM if there was insufficient memory to fulfill the allocation
///                request
#if defined(__linux__) || defined(__OSX__) || defined(__FreeBSD__)
static inline int ufio_memalign(void **ptrptr, size_t alignment, size_t size)
{
    return posix_memalign(ptrptr, alignment, size);
}
#elif defined(__SVR4) && defined(__sun)
static inline int ufio_memalign(void **ptrptr, int alignment, int size)
{
# if !defined(OPENSOLARIS)

    if (alignment == 0 || ((alignment % sizeof(void *)) != 0) ||
        (alignment & (alignment - 1)) != 0)
    {
        return EINVAL;
    }

    *ptrptr = memalign(alignment, size);

    return (*ptrptr != NULL ? 0 : ENOMEM);

# else

    return posix_memalign(ptrptr, alignment, size);

# endif
}
#else
#error "Port is not defined!"
#endif
