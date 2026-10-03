/*
 * BSD 2-Clause License
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

#include "list.h"
#include "memory.h"
#include "sol_internal.h"
#include "trie.h"
#include <string.h>

static int wildcard_destructor(struct list_node *);

static bool topic_destructor(struct trie_node *, bool);

static int subscription_cmp(const void *, const void *);

/*
 * ==========================================================
 *  Wildcard subscriptions index
 * ==========================================================
 *
 * A filter holding '+' or '#' can't be registered on a topic of the main
 * trie, because at subscribe time we don't know yet which topic names it
 * will match. They used to be kept in a linked list scanned linearly on
 * every PUBLISH, which became a hotspot as the number of subscriptions grew.
 *
 * They are now indexed in a segment trie: every filter is split by '/' and
 * each level becomes a node, children are looked up by level name through
 * UTHASH. Matching a published topic walks only the levels of that topic,
 * following at each step the literal child or the '+' child, plus the
 * subscriptions sitting on the traversed path: O(levels) instead of
 * O(wildcard subscriptions).
 *
 * A node holds the subscriptions whose filter ends on it. Subscriptions
 * flagged as multilevel ('#') match every topic passing through their node,
 * which is the node *above* the '#' level (a bare '#' sits on the root), so
 * they are emitted while walking, whereas the others are emitted only when
 * the whole topic has been consumed, i.e. when the depths match.
 */
struct wild_level {
    char *level;              /* Level name, NUL terminated copy */
    struct wild_node *node;   /* Child node reached through this level */
    UT_hash_handle hh;        /* UTHASH handle, children are keyed by level */
};

struct wild_node {
    struct wild_level *children; /* Children by level name, UTHASH handle */
    List *subs;                /* Subscriptions ending on this node */
};

struct wild_index {
    struct wild_node *root;
    unsigned long count;       /* Total number of indexed subscriptions */
};

static struct wild_index *wild_index_new(void);

static void wild_index_destroy(struct wild_index *);

static void wild_index_add(struct wild_index *, struct subscription *);

static void wild_index_remove_client(struct wild_index *, const char *);

static bool wild_index_empty(const struct wild_index *);

static void wild_index_match(const struct wild_index *, const char *,
                             wildcard_cb, void *);

static struct wild_node *wild_node_new(void);

static void wild_node_destroy(struct wild_node *);

static void wild_level_destroy(struct wild_level *);

static bool wild_node_empty(const struct wild_node *);

static bool wild_node_remove_client(struct wild_node *, const char *,
                                    unsigned long *);

static void wild_node_emit(const struct wild_node *, bool, wildcard_cb,
                           void *);

static struct wild_node *wild_child(struct wild_node *, const char *, size_t,
                                    bool);

static const char *wild_next_level(const char **, size_t *);

/*
 * Allocate a new empty node of the index
 */
static struct wild_node *wild_node_new(void)
{
    struct wild_node *node = try_alloc(sizeof(*node));
    node->children         = NULL;
    node->subs             = NULL;
    return node;
}

/*
 * A node can be reclaimed when it holds no subscription and no child
 */
static bool wild_node_empty(const struct wild_node *node)
{
    return node->children == NULL &&
           (!node->subs || list_size(node->subs) == 0);
}

/*
 * Release a node with all its children and subscriptions
 */
static void wild_node_destroy(struct wild_node *node)
{
    if (!node)
        return;
    if (node->subs)
        list_destroy(node->subs, 1);
    struct wild_level *lvl, *tmp;
    HASH_ITER(hh, node->children, lvl, tmp)
    {
        HASH_DEL(node->children, lvl);
        wild_level_destroy(lvl);
    }
    free_memory(node);
}

/*
 * Release a level entry along with the child node it points to
 */
static void wild_level_destroy(struct wild_level *level)
{
    wild_node_destroy(level->node);
    free_memory(level->level);
    free_memory(level);
}

/*
 * Extract the next '/' separated level of a normalized topic or filter, both
 * end with a trailing '/' so a trailing empty segment is not a level: "a/b/"
 * holds "a" then "b". Returns NULL when there's no level left.
 */
static const char *wild_next_level(const char **p, size_t *len)
{
    const char *start = *p, *slash;

    if (*start == '\0')
        return NULL;

    slash = strchr(start, '/');
    *len  = slash ? (size_t)(slash - start) : strlen(start);
    *p    = start + *len + (slash ? 1 : 0);

    return start;
}

/*
 * Look up the child node reached by a level, creating it on demand when
 * `create` is set. The lookup is made by raw bytes so that levels, which are
 * substrings of the topic being walked, don't need to be copied.
 */
static struct wild_node *wild_child(struct wild_node *node, const char *level,
                                    size_t len, bool create)
{
    struct wild_level *child = NULL;

    HASH_FIND(hh, node->children, level, (unsigned)len, child);
    if (child)
        return child->node;
    if (!create)
        return NULL;

    child           = try_alloc(sizeof(*child));
    child->level    = try_alloc(len + 1);
    memcpy(child->level, level, len);
    child->level[len] = '\0';
    child->node     = wild_node_new();
    HASH_ADD_STR(node->children, level, child);

    return child->node;
}

/*
 * Allocate a new empty index
 */
static struct wild_index *wild_index_new(void)
{
    struct wild_index *index = try_alloc(sizeof(*index));
    index->root              = wild_node_new();
    index->count             = 0;
    return index;
}

/*
 * Release the whole index
 */
static void wild_index_destroy(struct wild_index *index)
{
    if (!index)
        return;
    wild_node_destroy(index->root);
    free_memory(index);
}

/*
 * True when no wildcard subscription is indexed
 */
static bool wild_index_empty(const struct wild_index *index)
{
    return index->count == 0;
}

/*
 * Index a subscription by walking its filter level by level, taking
 * ownership of it as the previous list based implementation used to do
 */
static void wild_index_add(struct wild_index *index, struct subscription *s)
{
    struct wild_node *node = index->root;
    const char *p          = s->topic;
    size_t len;
    const char *level;

    while ((level = wild_next_level(&p, &len)) != NULL) {
        /*
         * A '#' level matches every remaining level wherever it sits in the
         * filter, the subscription is indexed on the node above it and what
         * follows is meaningless: flag it as multilevel so that it is
         * delivered to every topic passing through this node, keeping the
         * lenient behavior of match_subscription
         */
        if (len == 1 && level[0] == '#') {
            s->multilevel = true;
            break;
        }
        node = wild_child(node, level, len, true);
    }

    if (!node->subs)
        node->subs = list_new(wildcard_destructor);
    node->subs = list_push(node->subs, s);
    index->count++;
}

/*
 * Invoke the callback for the subscriptions held by a node, `prefix` marks a
 * node reached while the topic still has levels left, where only multilevel
 * ('#') subscriptions match
 */
static void wild_node_emit(const struct wild_node *node, bool prefix,
                           wildcard_cb fn, void *arg)
{
    if (!node->subs)
        return;
    list_foreach(item, node->subs)
    {
        struct subscription *s = item->data;
        if (prefix && !s->multilevel)
            continue;
        fn(s, arg);
    }
}

/*
 * A node to be visited paired with the remaining part of the topic it has to
 * match
 */
struct wild_visit {
    struct wild_node *node;
    const char *rest;
};

/*
 * Walk every node compatible with the levels of a published topic: at each
 * level both the literal child and the '+' child are explored (they're the
 * same node when the topic level is a literal '+'), the '#' subscriptions of
 * each visited node are emitted before descending and, once the topic is
 * exhausted, the subscriptions whose filter depth matches it exactly.
 *
 * The walk is iterative on an explicit stack, sized from the number of
 * levels of the topic, so that a deep filter can't exhaust the call stack
 */
static void wild_index_match(const struct wild_index *index, const char *topic,
                             wildcard_cb fn, void *arg)
{
    unsigned long levels = 1;
    for (const char *c = topic; *c; c++)
        if (*c == '/')
            levels++;

    /* Each level branches into at most two nodes, the literal and the '+' */
    size_t cap             = 2 * levels + 2;
    struct wild_visit *stk = try_alloc(cap * sizeof(*stk));
    size_t top             = 0;

    stk[top++] = (struct wild_visit){index->root, topic};

    while (top > 0) {
        struct wild_visit v    = stk[--top];
        struct wild_node *node = v.node;
        const char *p           = v.rest;
        size_t len;
        const char *level;

        /* '#' subscriptions of this node match the rest of the topic */
        wild_node_emit(node, true, fn, arg);

        level = wild_next_level(&p, &len);
        if (!level) {
            /* The topic is exhausted, filters ending here match it */
            wild_node_emit(node, false, fn, arg);
            continue;
        }

        struct wild_node *literal = wild_child(node, level, len, false);
        struct wild_node *single  = wild_child(node, "+", 1, false);

        if (literal) {
            stk[top++] = (struct wild_visit){literal, p};
        }
        if (single && single != literal) {
            stk[top++] = (struct wild_visit){single, p};
        }
    }

    free_memory(stk);
}

/*
 * Drop every subscription of the given client, pruning the nodes left empty,
 * returns true if the node can be reclaimed by the caller
 */
static bool wild_node_remove_client(struct wild_node *node, const char *id,
                                    unsigned long *removed)
{
    if (node->subs) {
        unsigned long before = list_size(node->subs);
        list_remove(node->subs, id, subscription_cmp);
        *removed += before - list_size(node->subs);
    }
    struct wild_level *lvl, *tmp;
    HASH_ITER(hh, node->children, lvl, tmp)
    {
        if (wild_node_remove_client(lvl->node, id, removed)) {
            HASH_DEL(node->children, lvl);
            wild_level_destroy(lvl);
        }
    }
    return wild_node_empty(node);
}

/*
 * Remove all the subscriptions of a client from the index, called when a
 * client is deactivated
 */
static void wild_index_remove_client(struct wild_index *index, const char *id)
{
    unsigned long removed = 0;
    wild_node_remove_client(index->root, id, &removed);
    index->count -= removed;
}

/*
 * Allocate a new store structure on the heap and return it after its
 * initialization, also allocating the wildcard subscriptions index, the
 * function may gracefully crash as the memory allocation may fail.
 */
struct topic_store *topic_store_new(void)
{
    struct topic_store *store = try_alloc(sizeof(*store));
    store->topics             = trie_new(topic_destructor);
    store->wildcards          = wild_index_new();
    return store;
}

/*
 * Deallocate heap memory for the wildcard index and every subscription
 * stored into, also the store is deallocated
 */
void topic_store_destroy(struct topic_store *store)
{
    wild_index_destroy(store->wildcards);
    trie_destroy(store->topics);
    free_memory(store);
}

/*
 * Insert a topic into the store or update it if already present
 */
void topic_store_put(struct topic_store *store, struct topic *t)
{
    trie_insert(store->topics, t->name, t);
}

/*
 * Remove a topic into the store
 */
void topic_store_del(struct topic_store *store, const char *name)
{
    trie_delete(store->topics, name);
}

/*
 * Check if the store contains a topic by name key
 */
bool topic_store_contains(const struct topic_store *store, const char *name)
{
    struct topic *t = topic_store_get(store, name);
    return t != NULL;
}

/*
 * Return a topic associated to a topic name from the store, returns NULL if no
 * topic is found.
 */
struct topic *topic_store_get(const struct topic_store *store, const char *name)
{
    struct topic *ret_topic;
    trie_find(store->topics, name, (void *)&ret_topic);
    return ret_topic;
}

/*
 * Return a topic associated to a topic name from the store, if no topic is
 * insert it into the store before returning it. Like topic_store_get but
 * cannot return NULL.
 * The function may fail as in case of no topic found it tries to allocate
 * space on the heap for the new inserted topic.
 */
struct topic *topic_store_get_or_put(struct topic_store *store,
                                     const char *name)
{
    struct topic *t = topic_store_get(store, name);
    if (t != NULL)
        return t;
    t = topic_new(try_strdup(name));
    topic_store_put(store, t);
    return t;
}

/*
 * Add a wildcard subscription to the index of the topic_store struct, it is
 * keyed by its filter levels, does not check if it already exists
 */
void topic_store_add_wildcard(struct topic_store *store, struct subscription *s)
{
    wild_index_add(store->wildcards, s);
}

/*
 * Remove every wildcard subscription belonging to the given client id from
 * the index, called on client deactivation
 */
void topic_store_remove_wildcard(struct topic_store *store, char *id)
{
    wild_index_remove_client(store->wildcards, id);
}

/*
 * Remove the subscriptions a client registered for one specific filter,
 * mirroring the walk wild_index_add performs but with creation disabled so
 * that a filter that was never indexed (any exact one, or an unknown
 * wildcard) simply misses and leaves the index untouched
 */
void topic_store_remove_wildcard_filter(struct topic_store *store,
                                        const char *filter, const char *id)
{
    struct wild_index *index = store->wildcards;
    struct wild_node *node   = index->root;
    const char *p            = filter;
    size_t len;
    const char *level;

    while ((level = wild_next_level(&p, &len)) != NULL) {
        /* Same stop condition of wild_index_add: '#' sits above the node */
        if (len == 1 && level[0] == '#')
            break;
        node = wild_child(node, level, len, false);
        if (!node)
            return;
    }

    if (!node->subs)
        return;
    unsigned long before = list_size(node->subs);
    list_remove(node->subs, id, subscription_cmp);
    index->count -= before - list_size(node->subs);
}

/*
 * Run a function to each node of the topic_store trie holding the topic
 * entries
 */
void topic_store_map(struct topic_store *store, const char *prefix,
                     void (*fn)(struct trie_node *, void *), void *arg)
{
    trie_prefix_map(store->topics->root, prefix, fn, arg);
}

/*
 * Run the callback for every wildcard subscription matching the topic, the
 * topic is expected to be normalized as all the topics of the codebase
 */
void topic_store_match_wildcards(struct topic_store *store, const char *topic,
                                 wildcard_cb fn, void *arg)
{
    wild_index_match(store->wildcards, topic, fn, arg);
}

/*
 * Check if the store holds any wildcard subscription at all
 */
bool topic_store_wildcards_empty(const struct topic_store *store)
{
    return wild_index_empty(store->wildcards);
}

/*
 * Auxiliary function, destructor to be passed in to init the list of
 * subscriptions held by a node of the wildcard index, used to correctly
 * destroy struct subscription items
 */
static int wildcard_destructor(struct list_node *node)
{
    if (!node)
        return -SOL_ERR;
    struct subscription *s = node->data;
    DECREF(s->subscriber, struct subscriber);
    free_memory((char *)s->topic);
    free_memory(s);
    free_memory(node);
    return SOL_OK;
}

/*
 * Auxiliary function, destructor to be passed in to init a trie structure,
 * used to release topics inside the main topic store
 */
static bool topic_destructor(struct trie_node *node, bool flag)
{
    if (!node || !node->data)
        return false;
    struct topic *t = node->data;
    topic_destroy(t);
    return true;
}

/*
 * Auxiliary compare function to be passed in as comparator to a list_remove
 * call
 */
static int subscription_cmp(const void *ptr_s1, const void *ptr_s2)
{
    struct subscription *s1 = ((struct list_node *)ptr_s1)->data;
    const char *id          = ptr_s2;
    return STREQ(s1->subscriber->id, id, MQTT_CLIENT_ID_LEN);
}
