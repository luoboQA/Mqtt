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

#include "memorypool.h"
#include "memory.h"
#include <stddef.h>
#include <string.h>

/*
 * A chunk is one contiguous allocation holding its own link plus a run of
 * blocks. The union keeps the payload aligned to the strictest fundamental
 * type, so the blocks that follow the header keep the alignment malloc gave
 * to the chunk itself
 */
struct memorypool_chunk {
    union {
        struct memorypool_chunk *next;
        max_align_t align;
    } header;
    /* The blocks of the chunk follow the header */
};

/* The free list is stored inside the free blocks themselves */
static inline void *block_next(const void *block)
{
    void *next;
    memcpy(&next, block, sizeof(next));
    return next;
}

static inline void block_set_next(void *block, void *next)
{
    memcpy(block, &next, sizeof(next));
}

/*
 * Allocate a new chunk of blocks_nr blocks and push all of them on the free
 * list. Chunks are only ever appended, never reallocated: a pointer handed
 * out by memorypool_alloc() can therefore never dangle, which the previous
 * single-arena-with-realloc design could do (and did, on every pool growth)
 */
static void memorypool_grow(struct memorypool *pool)
{
    size_t payload = pool->blocks_nr * pool->blocksize;
    struct memorypool_chunk *chunk = try_alloc(sizeof(*chunk) + payload);

    chunk->header.next = pool->chunks;
    pool->chunks        = chunk;

    unsigned char *blocks = (unsigned char *)(chunk + 1);
    /*
     * Zero the run of blocks: the client_init() of a never used block
     * relies on the buffer pointers being NULL to allocate the read and
     * write buffers (the old grow path used realloc, whose fresh region
     * was left uninitialized)
     */
    memset(blocks, 0, payload);
    for (size_t i = 0; i < pool->blocks_nr; ++i) {
        void *block = blocks + i * pool->blocksize;
        block_set_next(block, pool->free);
        pool->free = block;
    }
}

struct memorypool *memorypool_new(size_t blocks_nr, size_t blocksize)
{
    struct memorypool *pool = try_alloc(sizeof(*pool));

    /*
     * The intrusive free list stores a pointer in the first bytes of every
     * free block, so a block can never be smaller than one
     */
    pool->free      = NULL;
    pool->chunks    = NULL;
    pool->blocks_nr = blocks_nr ? blocks_nr : 1;
    pool->blocksize =
        blocksize >= sizeof(void *) ? blocksize : sizeof(void *);
    pool->allocated = 0;

    memorypool_grow(pool);

    return pool;
}

void memorypool_destroy(struct memorypool *pool)
{
    struct memorypool_chunk *chunk = pool->chunks;
    while (chunk) {
        struct memorypool_chunk *next = chunk->header.next;
        free_memory(chunk);
        chunk = next;
    }
    free_memory(pool);
}

void *memorypool_alloc(struct memorypool *pool)
{
    /*
     * The free list is complete: every block ever allocated and released is
     * threaded on it, so an empty list simply means peak usage grew and a
     * new chunk is needed (the old code tried to detect this with a
     * "two blocks left" comparison that fired at the wrong moment and then
     * wrote past the end of the arena)
     */
    if (!pool->free)
        memorypool_grow(pool);

    void *block = pool->free;
    pool->free  = block_next(block);
    pool->allocated++;
    return block;
}

void memorypool_free(struct memorypool *pool, void *ptr)
{
    if (!ptr)
        return;
    /*
     * Push the block at the head of the free list. The old implementation
     * stored the *next* of the current head instead of the head itself,
     * dropping one node from the list on every free: after enough release
     * cycles the list ran dry while blocks were still in use and two
     * different clients ended up sharing the very same struct client
     */
    block_set_next(ptr, pool->free);
    pool->free = ptr;
    if (pool->allocated > 0)
        pool->allocated--;
}
