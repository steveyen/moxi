/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef MCS_H
#define MCS_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// The mcs API's are a level of indirection from direct libmemcached
// and libvbucket API usage.
//
typedef enum {
    MCS_SUCCESS = 0,
    MCS_FAILURE,
    MCS_MAXIMUM_RETURN /* Always add new error code before */
} mcs_return;

typedef enum {
    MCS_KIND_UNKNOWN = 0,
    MCS_KIND_LIBVBUCKET,
    MCS_KIND_LIBMEMCACHED,
    MCS_KIND_MAX
} mcs_kind;

typedef struct {
    char hostname[200];
    int port;
    int fd;
    char *usr;
    char *pwd;
} mcs_server_st;

typedef struct {
    mcs_kind       kind;
    void          *data;     // Depends on kind.
    int            nservers; // Size of servers array.
    mcs_server_st *servers;
} mcs_st;

mcs_st *mcs_create(mcs_st *ptr, const char *config);
void    mcs_free(mcs_st *ptr);

bool mcs_stable_update(mcs_st *curr_version, mcs_st *next_version);

uint32_t       mcs_server_count(mcs_st *ptr);
mcs_server_st *mcs_server_index(mcs_st *ptr, int i);

uint32_t mcs_key_hash(mcs_st *ptr, const char *key, size_t key_length, int *vbucket);

void mcs_server_invalid_vbucket(mcs_st *ptr, int server_index, int vbucket);

void mcs_server_st_quit(mcs_server_st *ptr, uint8_t io_death);

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
const char *mcs_server_st_usr(mcs_server_st *ptr);
const char *mcs_server_st_pwd(mcs_server_st *ptr);

// ----------------------------------------

#define MOXI_DEFAULT_LISTEN_PORT      0
#define MEMCACHED_DEFAULT_LISTEN_PORT 11210

// ----------------------------------------

#ifdef MOXI_USE_LIBMEMCACHED

#include <libmemcached/memcached.h>

#endif // MOXI_USE_LIBMEMCACHED

// ----------------------------------------

#ifdef MOXI_USE_LIBVBUCKET

#include <libvbucket/vbucket.h>

#undef  MOXI_DEFAULT_LISTEN_PORT
#define MOXI_DEFAULT_LISTEN_PORT      11211

#undef  MEMCACHED_DEFAULT_LISTEN_PORT
#define MEMCACHED_DEFAULT_LISTEN_PORT 0

#endif // MOXI_USE_LIBVBUCKET

#endif // MCS_H
