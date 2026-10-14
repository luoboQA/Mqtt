/* BSD 2-Clause License
 *
 * Copyright (c) 2023, Andrea Giacomo Baldan All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "memory.h"
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

static atomic_size_t memory = ATOMIC_VAR_INIT(0);

/*
 * The header prepended to every allocation stores the size of the block and
 * forces its alignment: pairing it with max_align_t in a union makes it as
 * wide as the strictest fundamental type, so the pointer handed to the caller
 * (one header past the start of the block) is aligned for any type, exactly
 * like a pointer returned by malloc. A bare size_t header only guaranteed 8
 * bytes, which is a misaligned access for anything asking for 16 (SSE
 * vectors, long double, the memorypool chunk union)
 */
typedef union {
    size_t size;
    max_align_t align;
} alloc_header;

#define ALLOC_HEADER_SIZE (sizeof(alloc_header))

/*
 * Custom malloc function, allocate a defined size of bytes plus the header
 * described above, storing the chosen length at the beginning of the memory
 * chunk and returning the memory that follows it; this way it is possible to
 * track the memory usage at every allocation.
 *
 * This function can fail if not memory is available, interrupting the
 * execution of the program and exiting, hence the prefix "try".
 */
void *try_alloc(size_t size)
{
    void *ptr = malloc(size + ALLOC_HEADER_SIZE);
    if (!ptr) {
        fprintf(stderr, "[%s:%d] Out of memory (%lu bytes)\n", __FILE__,
                __LINE__, (unsigned long)size);
        exit(EXIT_FAILURE);
    }
    memory += size + ALLOC_HEADER_SIZE;
    ((alloc_header *)ptr)->size = size;
    return (char *)ptr + ALLOC_HEADER_SIZE;
}

/*
 * Same as xmalloc, but with calloc, creating chunk o zero'ed memory.
 * TODO: still a suboptimal solution
 *
 * This function can fail if not memory is available, interrupting the
 * execution of the program and exiting, hence the prefix "try".
 */
void *try_calloc(size_t len, size_t size)
{
    void *ptr = try_alloc(len * size);
    memset(ptr, 0x00, len * size);
    return ptr;
}

/*
 * Same of xmalloc but with realloc, resize a chunk of memory pointed by a
 * given pointer, again appends the new size in front of the byte array
 *
 * This function can fail if not memory is available, interrupting the
 * execution of the program and exiting, hence the prefix "try".
 */
void *try_realloc(void *ptr, size_t size)
{
    if (!ptr)
        return try_alloc(size);

    void *realptr    = (char *)ptr - ALLOC_HEADER_SIZE;
    size_t curr_size = ((alloc_header *)realptr)->size;
    if (size == curr_size)
        return ptr;

    void *newptr = realloc(realptr, size + ALLOC_HEADER_SIZE);
    if (!newptr) {
        fprintf(stderr, "[%s:%d] Out of memory (%lu bytes)\n", __FILE__,
                __LINE__, (unsigned long)size);
        exit(EXIT_FAILURE);
    }

    ((alloc_header *)newptr)->size = size;
    /*
     * Account only for the *delta*: the header is present both before and
     * after the realloc, so adding its size here would make the global memory
     * counter drift upward on every reallocation
     */
    memory += size - curr_size;
    return (char *)newptr + ALLOC_HEADER_SIZE;
}

/*
 * Custom free function, must be used on memory chunks allocated with t*
 * functions, it moves the pointer back by the size of the header to reach the
 * start of the allocation, this way it knows how many bytes will be free'ed
 * by the call
 */
void free_memory(void *ptr)
{
    if (!ptr)
        return;

    void *realptr = (char *)ptr - ALLOC_HEADER_SIZE;
    if (!realptr)
        return;

    size_t ptr_size = ((alloc_header *)realptr)->size;
    memory -= ptr_size + ALLOC_HEADER_SIZE;
    free(realptr);
}

/*
 * Retrieve the bytes allocated by t* functions by backwarding the pointer of
 * 8 positions, the size of an unsigned long long in order to read the number
 * of allcated bytes
 */
size_t alloc_size(void *ptr)
{
    if (!ptr)
        return 0L;

    void *realptr = (char *)ptr - ALLOC_HEADER_SIZE;
    if (!realptr)
        return 0L;

    return ((alloc_header *)realptr)->size;
}

/*
 * As strdup but using xmalloc instead of malloc, to track the number of bytes
 * allocated and to enable use of xfree on duplicated strings without having
 * to care when to use a normal free or a xfree
 *
 * This function can fail if not memory is available, interrupting the
 * execution of the program and exiting, hence the prefix "try".
 */
char *try_strdup(const char *s)
{
    size_t len = strlen(s);
    char *ds   = try_alloc(len + 1);
    snprintf(ds, len + 1, "%s", s);
    return ds;
}

size_t memory_used(void) { return memory; }
