#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

#ifdef __cplusplus
extern "C" {
#endif

#include "hashtable.h"

#ifdef __cplusplus
}
#endif

#define ht_flag_base(ht) ((char *)(ht) + (ht)->flag_offset)
#define ht_bucket_base(ht) ((char *)(ht) + (ht)->bucket_offset)

static const unsigned ht_magic = 0xBFBF;

enum bucket_flag {
    empty = 0, used = 1, removed = 2
};

size_t header_size = 1024;

#define bucket_size     1280
#define max_key_size    256
#define max_value_size  (bucket_size - max_key_size)

const float max_load_factor = 0.65;

static const unsigned int primes[] = { 
    53, 97, 193, 389,
    769, 1543, 3079, 6151,
    12289, 24593, 49157, 98317,
    196613, 393241, 786433, 1572869,
    3145739, 6291469, 12582917, 25165843,
    50331653, 100663319, 201326611, 402653189,
    805306457, 1610612741
};
static const unsigned int prime_table_length = sizeof (primes) / sizeof (primes[0]);

static inline void fill_ht_str(ht_str *s, const char *str, const u_int32 size) {
    s->size = size;
    memcpy(s->str, str, size);
}

static void dump_ht_str(ht_str *s) {
    if (s) {
        printf("%u: %*s\n", s->size, (int)s->size, s->str);
    }
    else {
        printf("(nil)\n");
    }
}

static unsigned int ht_get_prime_by(size_t capacity) {
    unsigned i = 0;
    capacity *= 2;
    for (i = 0; i < prime_table_length; i++) {
        if (primes[i] > capacity)
            return primes[i];
    }
    return 0;
}

size_t ht_memory_size(size_t capacity) {
    const int flag_size = 1; //char
    size_t aligned_capacity = (ht_get_prime_by(capacity) / 4 + 1) * 4; //round up to 4-byte alignment
    return header_size                      //header
         + flag_size * aligned_capacity     //flag
         + bucket_size * aligned_capacity;  //bucket
}

/*dbj2_hash function (copied from libshmht)*/
static unsigned int dbj2_hash (const char *str, size_t size) {
    unsigned long hash = 5381;
    while (size--) {
        char c = *str++;
        hash = ((hash << 5) + hash) + c;    /* hash * 33 + c */
    }
    return (unsigned int) hash;
}

BOOL is_equal(const char *a, size_t asize, const char *b, size_t bsize) {
    if (asize != bsize)
        return False;
    return strncmp(a, b, asize) ? False : True;
}

int ht_is_valid(hashtable *ht) {
    return (ht->magic == ht_magic);
}

/*
 * The caller is responsible for the 4-byte alignment of base_addr
 * and the size of base_addr should be no less than ht_get_prime_by(capacity)
 */
hashtable* ht_init(void *base_addr, size_t capacity, int force_init) {
    hashtable* ht = (hashtable *)base_addr;
    if (force_init || !ht_is_valid(ht)) {
        ht->magic     = ht_magic;
        ht->ref_cnt   = 0;

        ht->orig_capacity = capacity;
        ht->capacity      = ht_get_prime_by(capacity);
        ht->size          = 0;

        ht->flag_offset   = header_size;
        ht->bucket_offset = ht->flag_offset + (ht->capacity / 4 + 1) * 4; //alignment

        bzero(ht_flag_base(ht), ht->capacity);
    }
    ht->ref_cnt += 1;
    return ht;
}

static size_t ht_position(hashtable *ht, const char *key, u_int32 key_size, BOOL treat_removed_as_empty) {
    char *flag_base = ht_flag_base(ht);
    char *bucket_base = ht_bucket_base(ht);
    size_t capacity = ht->capacity;
    unsigned long hval = dbj2_hash(key, key_size) % capacity;

    size_t i = hval, di = 1;
    while (True) {
        if (flag_base[i] == empty)
            break;
        if (flag_base[i] == removed && treat_removed_as_empty)
            break;
        if (flag_base[i] == used)
        {
            char *bucket = bucket_base + i * bucket_size;
            ht_str* bucket_key = (ht_str *)bucket;
            if (is_equal(key, key_size, bucket_key->str, bucket_key->size)) {
                break;
            }
        }
        i = (i + di) % capacity;
        di++;
        if (i == hval) {
            //extreme condition: when all flags are 'removed'
            bzero(flag_base, capacity);
            break;
        }
    }
    return i;
}

ht_str* ht_get(hashtable *ht, const char *key, u_int32 key_size) {
    size_t i = ht_position(ht, key, key_size, False); //'removed' bucket is not 'empty' when searching a chain.
    if (ht_flag_base(ht)[i] != used) {
        return NULL;
    }
    char *bucket = ht_bucket_base(ht) + i * bucket_size;
    return (ht_str*)(bucket + max_key_size);
}

int ht_set(hashtable *ht, const char *key, u_int32 key_size, const char *value, u_int32 value_size) {
    if (sizeof(u_int32) + key_size >= max_key_size || sizeof(u_int32) + value_size >= max_value_size) {
        //the item is too large
        fprintf(stderr, "the item is too large: key_size(%u), value(%u)\n", key_size, value_size);
        return False;
    }

    char *flag_base = ht_flag_base(ht);
    char *bucket_base = ht_bucket_base(ht);

    ht_str *bucket_key = NULL, *bucket_value = NULL;

    //if it exists: just find and modify it's value
    bucket_value = ht_get(ht, key, key_size);
    if (bucket_value) { 
        fill_ht_str(bucket_value, value, value_size);
        return True;
    }

    //else: find an available bucket, which can be both 'empty' or 'removed'
    size_t i = ht_position(ht, key, key_size, True);

    if (ht->capacity * max_load_factor < ht->size) {
        //hash table is over loaded
        fprintf(stderr, "hash table is over loaded, capacity=%lu, size=%lu\n", ht->capacity, ht->size);
        return False;
    }

    ht->size += 1;
    flag_base[i] = used;

    char *bucket = bucket_base + i * bucket_size;
    bucket_key   = (ht_str*)bucket;
    bucket_value = (ht_str*)(bucket + max_key_size);
    fill_ht_str(bucket_key, key, key_size);
    fill_ht_str(bucket_value, value, value_size);
    return True;
}

int ht_remove(hashtable *ht, const char *key, u_int32 key_size) {
    size_t i = ht_position(ht, key, key_size, False); //'removed' bucket is not 'empty' when searching a chain.
    if (ht_flag_base(ht)[i] != used) {
        return False;
    }
    ht_flag_base(ht)[i] = removed;
    ht->size -= 1;
    return True;
}

//don't forget to free(ht_iter)
ht_iter* ht_get_iterator(hashtable *ht) {
    ht_iter* iter = ALLOC(ht_iter, 1);
    assert(iter != NULL);
    iter->ht    = ht;
    iter->pos   = -1;
    return iter;
}

int ht_iter_next(ht_iter* iter) {
    size_t i = 0;
    hashtable *ht = iter->ht;
    char *flag_base = ht_flag_base(ht);
    char *bucket_base = ht_bucket_base(ht);

    for (i = iter->pos + 1; i < ht->capacity; i++) {
        if (flag_base[i] == used) {
            char *bucket = bucket_base + i * bucket_size;
            iter->key = (ht_str*)bucket, iter->value = (ht_str*)(bucket + max_key_size);
            iter->pos = i;
            return True;
        }
    }
    return False;
}

int ht_destroy(hashtable *ht) {
    ht->ref_cnt -= 1;
    return ht->ref_cnt == 0 ? True : False;
}

/*
int main() {
    size_t capacity = 500000;
    printf("%u\n", ht_get_prime_by(capacity));
    printf("%lu\n", ht_memory_size(capacity));
    void *mem = malloc(ht_memory_size(capacity) + 1);
    hashtable *ht = ht_init(mem, capacity, 0);

    ht_set(ht, "hello", 5, "-----", 5);
    ht_set(ht, "hello1", 6, "hello1", 6);
    ht_set(ht, "hello", 5, "hello", 5);
    ht_remove(ht, "hello", 5);

    ht_str* s = NULL;
    
    s = ht_get(ht, "hello", 5);
    dump_ht_str(s);

    s = ht_get(ht, "hello1", 6);
    dump_ht_str(s);

    ht_set(ht, "a", 1, "a", 1);
    ht_set(ht, "b", 1, "b", 1);
    ht_set(ht, "c", 1, "c", 1);
    ht_set(ht, "d", 1, "d", 1);
    printf("ht->size: %lu\n", ht->size);

    ht_remove(ht, "c", 1);

    hashtable* ht1 = ht_init(mem, capacity, 0);

    ht_iter* iter = ht_get_iterator(ht1);
    while (ht_iter_next(iter)) {
        ht_str *key = iter->key, *value = iter->value;
        printf("%*s => %*s\n", (int)key->size, key->str, (int)value->size, value->str);
    }
    free(iter);
    printf("ht_get_iterator test ok\n");

    char x[128];
    int i, len;
    struct timeval begin, end;
#define ts(tv) (tv.tv_sec + tv.tv_usec / 1000000.0)

    gettimeofday(&begin, NULL);
    for (i = 0; i < (int)capacity; i++) {
        len = sprintf(x, "%064d", i);
        if (ht_set(ht, x, len, x, len) == 0) {
            printf("set wrong @ %d\n", i);
            return 1;
        }
    }
    gettimeofday(&end, NULL);
    printf("set test: %.0lf iops\n", capacity / (ts(end) - ts(begin)));

    gettimeofday(&begin, NULL);
    for (i = 0; i < (int)capacity; i++) {
        len = sprintf(x, "%064d", i);
        ht_str* val = ht_get(ht, x, len);
        if (val == NULL || !is_equal(x, len, val->str, val->size)) {
            printf("(after set)get wrong @ %d\n", i);
            return 1;
        }
    }
    gettimeofday(&end, NULL);
    printf("get test: %.0lf iops\n", capacity / (ts(end) - ts(begin)));

    for (i = 0; i < (int)capacity; i += 2) {
        len = sprintf(x, "%064d", i);
        if (ht_remove(ht, x, len) == 0) {
            printf("remove wrong @ %d\n", i);
            return 1;
        }
        len = sprintf(x, "%064d", i + 1);
        ht_str* val = ht_get(ht, x, len);
        if (val == NULL || !is_equal(x, len, val->str, val->size)) {
            printf("(after remove)get wrong @ %d\n", i);
            return 1;
        }
    }
    printf("remove/get test ok\n");

    //while(1) sleep(1000);
    return 0;
}
// */
