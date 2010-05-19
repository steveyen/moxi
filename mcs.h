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
                              const void *commmand,
                              size_t command_length,
                              uint8_t with_flush);

// Some macros as level-of-indirection as opposed to direct
// using libmemcached API.
//
#define mcs_return memcached_return

#define mcs_st        memcached_st
#define mcs_server_st memcached_server_st

#define mcs_create(x)             memcached_create(x)
#define mcs_free(x)               memcached_free(x)
#define mcs_behavior_set(x, b, v) memcached_behavior_set(x, b, v)
#define mcs_server_count(x)       memcached_server_count(x)
#define mcs_server_push(x, s)     memcached_server_push(x, s)
#define mcs_server_index(x, i)    (&((x)->hosts[(i)]))
#define mcs_key_hash(x, k, len)   memcached_generate_hash(x, k, len)

#define mcs_server_st_parse(str)  memcached_servers_parse(str)
#define mcs_server_st_free(s)     memcached_server_list_free(s)
#define mcs_server_st_quit(s, v)  memcached_quit_server(s, v)

#define mcs_server_st_connect  memcached_connect
#define mcs_server_st_do       memcached_do
#define mcs_server_st_io_write memcached_io_write
#define mcs_server_st_io_reset memcached_io_reset
#define mcs_server_st_read     memcached_safe_read

#define mcs_server_st_hostname(s) ((s)->hostname)
#define mcs_server_st_port(s)     ((s)->port)
#define mcs_server_st_fd(s)       ((s)->fd)

#endif // MCS_H
