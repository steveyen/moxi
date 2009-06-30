#include "libmemcached/memcached.h"
#include <string.h>
#include <stdio.h>

class Memcached
{
  memcached_st memc;
  memcached_result_st result;

public:

  Memcached() : memc(), result()
  {
    memcached_create(&memc);
  }

  Memcached(memcached_st *clone) : memc(), result()
  {
    memcached_clone(&memc, clone);
  }
  char *fetch (char *key, size_t *key_length, size_t *value_length)
  {
    uint32_t flags;
    memcached_return rc;

    return memcached_fetch(&memc, key, key_length,
                    value_length, &flags, &rc);
  }
  char *get(const char *key, size_t *value_length)
  {
    uint32_t flags;
    memcached_return rc;

    return memcached_get(&memc, key, strlen(key),
                         value_length, &flags, &rc);
  }

  char *get_by_key(const char *master_key, const char *key,
                   size_t *value_length)
  {
    uint32_t flags;
    memcached_return rc;

    return memcached_get_by_key(&memc, master_key, strlen(master_key),
                                key, strlen(key),
                                value_length, &flags, &rc);
  }

  memcached_return mget(char **keys, size_t *key_length,
                        unsigned int number_of_keys)
  {

    return memcached_mget(&memc, keys, key_length, number_of_keys);
  }

  memcached_return set(const char *key, const char *value, size_t value_length)
  {
    return memcached_set(&memc, key, strlen(key),
                         value, value_length,
                         time_t(0), uint32_t(0));
  }

  memcached_return set_by_key(const char *master_key, const char *key,
                              const char *value, size_t value_length)
  {
    return memcached_set_by_key(&memc, master_key, strlen(master_key),
                         key, strlen(key),
                         value, value_length,
                         time_t(0),
                         uint32_t(0) );
  }
  memcached_return
    increment(const char *key, unsigned int offset, uint64_t *value)
  {
    return memcached_increment(&memc, key, strlen(key),
                         offset, value);
  }
  memcached_return
    decrement(const char *key, unsigned int offset, uint64_t *value)
  {
    return memcached_decrement(&memc, key, strlen(key),
                         offset, value);
  }


  memcached_return add(const char *key, const char *value, size_t value_length)
  {
    return memcached_add(&memc, key, strlen(key), value, value_length, 0, 0);
  }
  memcached_return add_by_key(const char *master_key, const char *key,
                              const char *value, size_t value_length)
  {
    return memcached_add_by_key(&memc, master_key, strlen(master_key),
                                key, strlen(key),
                                value, value_length,
                                0, 0);
  }

  memcached_return replace(const char *key, const char *value,
                           size_t value_length)
  {
    return memcached_replace(&memc, key, strlen(key),
                     value, value_length,
                     0, 0);
  }
  memcached_return replace_by_key(const char *master_key, const char *key,
                                  const char *value, size_t value_length)
  {
    return memcached_replace_by_key(&memc, master_key, strlen(master_key),
                                    key, strlen(key),
                                    value, value_length, 0, 0);
  }

  memcached_return prepend(const char *key, const char *value,
                           size_t value_length)
  {
    return memcached_prepend(&memc, key, strlen(key),
                    value, value_length, 0, 0);
  }
  memcached_return prepend_by_key(const char *master_key, const char *key,
                                  const char *value, size_t value_length)
  {
    return memcached_prepend_by_key(&memc, master_key, strlen(master_key),
                                    key, strlen(key),
                                    value, value_length,
                                    0,
                                    0);
  }

  memcached_return  append(const char *key, const char *value,
                           size_t value_length)
  {
    return memcached_append(&memc, key, strlen(key),
                    value, value_length, 0, 0);
  }
  memcached_return  append_by_key(const char *master_key, const char *key,
                                  const char *value, size_t value_length)
  {
    return memcached_append_by_key(&memc,
                                   master_key, strlen(master_key),
                                   key, strlen(key),
                                   value, value_length, 0, 0);
  }
  memcached_return  cas(const char *key, const char *value,
                        size_t value_length, uint64_t cas)
  {
    return memcached_cas(&memc, key, strlen(key),
                    value, value_length, 0, 0, cas);
  }
  memcached_return  cas_by_key(const char *master_key, const char *key,
                               const char *value, size_t value_length,
                               uint64_t cas)
  {
    return memcached_cas_by_key(&memc,
                                master_key, strlen(master_key),
                                key, strlen(key),
                                value, value_length,
                                0, 0, cas);
  }
  // using 'remove' vs. 'delete' since 'delete' is a keyword
  memcached_return remove(const char *key)
  {
    return memcached_delete (&memc, key, strlen(key), 0);

  }
  memcached_return delete_by_key(const char *master_key, const char *key)
  {
    return memcached_delete_by_key(&memc, master_key, strlen(master_key),
                           key, strlen(key), 0);
  }
  ~Memcached()
  {
    memcached_free(&memc);
  }
};
