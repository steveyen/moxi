/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <sys/time.h>
#include <assert.h>
#include "mcs.h"

#ifdef MOXI_USE_VBUCKET

mcs_st *mcs_create(mcs_st *ptr, const char *config) {
    memset(ptr, 0, sizeof(*ptr));
    return ptr;
}

void mcs_free(mcs_st *ptr) {
    if (ptr->servers) {
        free(ptr->servers);
    }
    ptr->servers = NULL;
    vbucket_config_destroy(ptr->vch);
}

uint32_t mcs_server_count(mcs_st *ptr) {
    return (uint32_t) vbucket_config_get_num_servers(*ptr);
}

mcs_server_st *mcs_server_index(mcs_st *ptr, int i) {
    return &ptr->servers[i];
}

uint32_t mcs_key_hash(mcs_st *ptr, const char *key, size_t key_length) {
    return (uint32_t) vbucket_get_master(*ptr,
                                         vbucket_config_get_vbucket_by_key(*ptr,
                                                                           key, key_length));
}

void mcs_server_st_quit(mcs_server_st *ptr, uint8_t io_death) {
    // TODO: memcached_quit_server(ptr, io_death);
}

mcs_return mcs_server_st_connect(mcs_server_st *ptr) {
    // TODO: return memcached_connect(ptr);
    return -1;
}

mcs_return mcs_server_st_do(mcs_server_st *ptr,
                            const void *command,
                            size_t command_length,
                            uint8_t with_flush) {
    return memcached_do(ptr, command, command_length, with_flush);
}

ssize_t mcs_server_st_io_write(mcs_server_st *ptr,
                               const void *buffer,
                               size_t length,
                               char with_flush) {
    return memcached_io_write(ptr, buffer, length, with_flush);
}

mcs_return mcs_server_st_read(mcs_server_st *ptr,
                              void *dta,
                              size_t size) {
    return memcached_safe_read(ptr, dta, size);
}

void mcs_server_st_io_reset(mcs_server_st *ptr) {
    // TODO: memcached_io_reset(ptr);
}

const char *mcs_server_st_hostname(mcs_server_st *ptr) {
    return ptr->hostname;
}

int mcs_server_st_port(mcs_server_st *ptr) {
    return ptr->port;
}

int mcs_server_st_fd(mcs_server_st *ptr) {
    return ptr->fd;
}

#else // !MOXI_USE_VBUCKET

mcs_st *mcs_create(mcs_st *ptr, const char *config) {
    ptr = memcached_create(ptr);
    if (ptr != NULL) {
        memcached_behavior_set(ptr, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);
        memcached_behavior_set(ptr, MEMCACHED_BEHAVIOR_KETAMA, 1);
        memcached_behavior_set(ptr, MEMCACHED_BEHAVIOR_TCP_NODELAY, 1);

        memcached_server_st *mservers;

        mservers = memcached_servers_parse(config);
        if (mservers != NULL) {
            memcached_server_push(ptr, mservers);
            memcached_server_list_free(mservers);

            return ptr;
        }

        mcs_free(ptr);
    }

    return NULL;
}

void mcs_free(mcs_st *ptr) {
    memcached_free(ptr);
}

uint32_t mcs_server_count(mcs_st *ptr) {
    return memcached_server_count(ptr);
}

mcs_server_st *mcs_server_index(mcs_st *ptr, int i) {
    return &ptr->hosts[i];
}

uint32_t mcs_key_hash(mcs_st *ptr, const char *key, size_t key_length) {
    return memcached_generate_hash(ptr, key, key_length);
}

void mcs_server_st_quit(mcs_server_st *ptr, uint8_t io_death) {
    memcached_quit_server(ptr, io_death);
}

mcs_return mcs_server_st_connect(mcs_server_st *ptr) {
    return memcached_connect(ptr);
}

mcs_return mcs_server_st_do(mcs_server_st *ptr,
                            const void *command,
                            size_t command_length,
                            uint8_t with_flush) {
    return memcached_do(ptr, command, command_length, with_flush);
}

ssize_t mcs_server_st_io_write(mcs_server_st *ptr,
                               const void *buffer,
                               size_t length,
                               char with_flush) {
    return memcached_io_write(ptr, buffer, length, with_flush);
}

mcs_return mcs_server_st_read(mcs_server_st *ptr,
                              void *dta,
                              size_t size) {
    return memcached_safe_read(ptr, dta, size);
}

void mcs_server_st_io_reset(mcs_server_st *ptr) {
    memcached_io_reset(ptr);
}

const char *mcs_server_st_hostname(mcs_server_st *ptr) {
    return ptr->hostname;
}

int mcs_server_st_port(mcs_server_st *ptr) {
    return ptr->port;
}

int mcs_server_st_fd(mcs_server_st *ptr) {
    return ptr->fd;
}

#endif // MOXI_USE_VBUCKET
