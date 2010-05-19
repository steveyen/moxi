/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef MCS_H
#define MCS_H

#include <libmemcached/memcached.h>

// From libmemcached.
//
memcached_return memcached_connect(memcached_server_st *ptr);
memcached_return memcached_version(memcached_st *ptr);
void             memcached_quit_server(memcached_server_st *ptr,
                                       uint8_t io_death);
memcached_return memcached_safe_read(memcached_server_st *ptr,
                                     void *dta,
                                     size_t size);
ssize_t memcached_io_write(memcached_server_st *ptr,
                           const void *buffer,
                           size_t length, char with_flush);
void memcached_io_reset(memcached_server_st *ptr);
memcached_return memcached_do(memcached_server_st *ptr,
                              const void *command,
                              size_t command_length,
                              uint8_t with_flush);

// The mcs API's are a level of indirection from direct libmemcached
// API usage.
//
#define mcs_return    memcached_return
#define mcs_st        memcached_st
#define mcs_server_st memcached_server_st

mcs_st *mcs_create(mcs_st *ptr);
void    mcs_free(mcs_st *ptr);

mcs_return mcs_behavior_set(mcs_st *ptr, memcached_behavior flag, uint64_t data);

uint32_t       mcs_server_count(mcs_st *ptr);
mcs_return     mcs_server_push(mcs_st *ptr, mcs_server_st *list);
mcs_server_st *mcs_server_index(mcs_st *ptr, int i);

uint32_t mcs_key_hash(mcs_st *ptr, const char *key, size_t key_length);

mcs_server_st *mcs_server_st_parse(const char *server_strings);
void           mcs_server_st_free(mcs_server_st *ptr);
void           mcs_server_st_quit(mcs_server_st *ptr, uint8_t io_death);

mcs_return mcs_server_st_connect(mcs_server_st *ptr);
mcs_return mcs_server_st_do(mcs_server_st *ptr,
                            const void *commmand,
                            size_t command_length,
                            uint8_t with_flush);
ssize_t mcs_server_st_io_write(mcs_server_st *ptr,
                               const void *buffer,
                               size_t length,
                               char with_flush);
mcs_return mcs_server_st_read(mcs_server_st *ptr,
                              void *dta,
                              size_t size);
void mcs_server_st_io_reset(mcs_server_st *ptr);

const char *mcs_server_st_hostname(mcs_server_st *ptr);
int mcs_server_st_port(mcs_server_st *ptr);
int mcs_server_st_fd(mcs_server_st *ptr);

#endif // MCS_H
