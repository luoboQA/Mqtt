/* BSD 2-Clause License
 *
 * Copyright (c) 2023, Andrea Giacomo Baldan
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
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

#ifndef MEMORYPOOL_H
#define MEMORYPOOL_H

#include <stdlib.h>

/*
 * Simple memory object-pool, the purpose is to allow for fixed size objects to
 * be pre-allocated and re-use of memory blocks, so no size have to be
 * specified like in a normal malloc but only alloc and free of a pointer is
 * possible.
 *
 * The blocks are grouped in chunks linked together: a chunk is never moved
 * once allocated (the pool only ever appends a new one), so every pointer
 * handed out stays valid for the lifetime of the pool. The previous
 * implementation grew a single arena with realloc(), which can move it and
 * leave every block already in use dangling. Free blocks are threaded on an
 * intrusive LIFO list, the pointer to the next free block living in the
 * first bytes of the free block itself
 */
struct memorypool_chunk;

struct memorypool {
    void *free;                     /* Head of the free list of blocks */
    struct memorypool_chunk *chunks; /* Every chunk ever allocated */
    size_t blocks_nr;               /* Number of blocks per chunk */
    size_t blocksize;               /* Size of a block, >= sizeof(void *) */
    size_t allocated;               /* Blocks currently handed out */
};

struct memorypool *memorypool_new(size_t, size_t);
void memorypool_destroy(struct memorypool *);
void memorypool_foreach(struct memorypool *, void (*)(void *));
void *memorypool_alloc(struct memorypool *);
void memorypool_free(struct memorypool *, void *);

#endif
