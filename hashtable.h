#ifndef __HASH_TABLE__
#define __HASH_TABLE__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <assert.h>

#define ALLOC(type, n) ((type *)malloc(sizeof(type) * n))

typedef struct __hashtable {
    unsigned magic;
    size_t ref_cnt, orig_capacity, capacity, size, flag_offset, bucket_offset;
} hashtable;

typedef unsigned u_int32;

typedef struct _ht_str {
    u_int32 size;
    char str[1];
} ht_str;

typedef struct _ht_iter {
    hashtable *ht;
    size_t pos;
    ht_str *key, *value;
} ht_iter;

typedef int BOOL;
#define True    1
#define False   0

ht_iter* ht_get_iterator(hashtable *ht);
int ht_iter_next(ht_iter* iter);

size_t ht_memory_size(size_t capacity);
hashtable* ht_init(void *base_addr, size_t capacity, int force_init);
ht_str* ht_get(hashtable *ht, const char *key, u_int32 key_size);
int ht_set(hashtable *ht, const char *key, u_int32 key_size, const char *value, u_int32 value_size);
int ht_remove(hashtable *ht, const char *key, u_int32 key_size);
int ht_destroy(hashtable *ht);

int ht_is_valid(hashtable *ht);

#endif
