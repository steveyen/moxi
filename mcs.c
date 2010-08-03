/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include "mcs.h"
#include "log.h"

#ifdef MOXI_USE_VBUCKET

mcs_st *mcs_create(mcs_st *ptr, const char *config) {
    assert(ptr);

    memset(ptr, 0, sizeof(*ptr));

    ptr->vch = vbucket_config_parse_string(config);
    if (ptr->vch != NULL) {
        int n = vbucket_config_get_num_servers(ptr->vch);
        if (n > 0) {
            ptr->servers = calloc(sizeof(mcs_server_st), n);
            if (ptr->servers != NULL) {
                for (int i = 0; i < n; i++) {
                    ptr->servers[i].fd = -1;
                }

                int j = 0;
                for (; j < n; j++) {
                    const char *hostport = vbucket_config_get_server(ptr->vch, j);
                    if (hostport != NULL &&
                        strlen(hostport) > 0 &&
                        strlen(hostport) < sizeof(ptr->servers[j].hostname) - 1) {
                        strncpy(ptr->servers[j].hostname,
                                hostport,
                                sizeof(ptr->servers[j].hostname) - 1);
                        char *colon = strchr(ptr->servers[j].hostname, ':');
                        if (colon != NULL) {
                            *colon = '\0';
                            ptr->servers[j].port = atoi(colon + 1);
                            if (ptr->servers[j].port <= 0) {
                                moxi_log_write("mcs_create failed, could not parse port: %s\n",
                                        config);
                                break;
                            }
                        } else {
                            moxi_log_write("mcs_create failed, missing port: %s\n",
                                    config);
                            break;
                        }
                    } else {
                        moxi_log_write("mcs_create failed, unknown server: %s\n",
                                config);
                        break;
                    }

                    const char *user = vbucket_config_get_user(ptr->vch);
                    if (user != NULL) {
                        ptr->servers[j].usr = strdup(user);
                    }

                    const char *password = vbucket_config_get_password(ptr->vch);
                    if (password != NULL) {
                        ptr->servers[j].pwd = strdup(password);
                    }
                }

                if (j >= n) {
                    return ptr;
                }
            }
        }
    } else {
        moxi_log_write("mcs_create failed, vbucket_config_parse_string: %s\n",
                config);
    }

    mcs_free(ptr);

    return NULL;
}

void mcs_free(mcs_st *ptr) {
    if (ptr->servers) {
        if (ptr->vch != NULL) {
            int n = vbucket_config_get_num_servers(ptr->vch);
            for (int i = 0; i < n; i++) {
                if (ptr->servers[i].usr != NULL) {
                    free(ptr->servers[i].usr);
                }
                if (ptr->servers[i].pwd != NULL) {
                    free(ptr->servers[i].pwd);
                }
            }
        }
        free(ptr->servers);
    }
    if (ptr->vch) {
        vbucket_config_destroy(ptr->vch);
    }
    memset(ptr, 0, sizeof(*ptr));
}

uint32_t mcs_server_count(mcs_st *ptr) {
    return (uint32_t) vbucket_config_get_num_servers(ptr->vch);
}

mcs_server_st *mcs_server_index(mcs_st *ptr, int i) {
    return &ptr->servers[i];
}

/* Returns true if curr_version could be updated with next_version in
 * a low-impact stable manner (server-list is the same), allowing the
 * same connections to be reused.  Or returns false if the delta was
 * too large for an in-place updating of curr_version with information
 * from next_version.
 *
 * The next_version may be destroyed in this call, and the caller
 * should afterwards only call mcs_free() on the next_version.
 */
bool mcs_stable_update(mcs_st *curr_version, mcs_st *next_version) {
    bool rv = false;

    VBUCKET_CONFIG_DIFF *diff = vbucket_compare(curr_version->vch, next_version->vch);
    if (diff != NULL) {
        if (!diff->sequence_changed) {
            vbucket_config_destroy(curr_version->vch);
            curr_version->vch = next_version->vch;
            next_version->vch = 0;

            rv = true;
        }

        vbucket_free_diff(diff);
    }

    return rv;
}

uint32_t mcs_key_hash(mcs_st *ptr, const char *key, size_t key_length, int *vbucket) {
    int v = vbucket_get_vbucket_by_key(ptr->vch, key, key_length);
    if (vbucket != NULL) {
        *vbucket = v;
    }

    return (uint32_t) vbucket_get_master(ptr->vch, v);
}

void mcs_server_invalid_vbucket(mcs_st *ptr, int server_index, int vbucket) {
    vbucket_found_incorrect_master(ptr->vch, vbucket, server_index);
}

void mcs_server_st_quit(mcs_server_st *ptr, uint8_t io_death) {
    // TODO: Should send QUIT cmd.
    //
    if (ptr->fd != -1) {
        close(ptr->fd);
    }
    ptr->fd = -1;
}

mcs_return mcs_server_st_connect(mcs_server_st *ptr) {
    if (ptr->fd != -1)
        return MEMCACHED_SUCCESS;

    int ret = MEMCACHED_FAILURE;

    struct addrinfo *ai   = NULL;
    struct addrinfo *next = NULL;

    struct addrinfo hints = { .ai_flags = AI_PASSIVE,
                              .ai_socktype = SOCK_STREAM,
                              .ai_family = AF_UNSPEC };

    char port[50];
    snprintf(port, sizeof(port), "%d", ptr->port);

    int error = getaddrinfo(ptr->hostname, port, &hints, &ai);
    if (error != 0) {
        if (error != EAI_SYSTEM) {
            // settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            //                                 "getaddrinfo(): %s\n", gai_strerror(error));
        } else {
            // settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            //                                 "getaddrinfo(): %s\n", strerror(error));
        }

        return MEMCACHED_FAILURE;
    }

    for (next = ai; next; next = next->ai_next) {
        int sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (sock == -1) {
            // settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            //                                 "Failed to create socket: %s\n",
            //                                 strerror(errno));
            continue;
        }

        if (connect(sock, ai->ai_addr, ai->ai_addrlen) == -1) {
            // settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            //                                 "Failed to connect socket: %s\n",
            //                                 strerror(errno));
            close(sock);
            sock = -1;
            continue;
        }

        int flags = fcntl(sock, F_GETFL, 0);
        if (flags < 0 ||
            fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
            perror("setting O_NONBLOCK");
            close(sock);
            sock = -1;
            continue;
        }

        flags = 1;

        setsockopt(sock, IPPROTO_TCP, TCP_NODELAY,
                   &flags, (socklen_t) sizeof(flags));

        ptr->fd = sock;
        ret = MEMCACHED_SUCCESS;
        break;
    }

    freeaddrinfo(ai);

    return ret;
}

mcs_return mcs_server_st_do(mcs_server_st *ptr,
                            const void *command,
                            size_t command_length,
                            uint8_t with_flush) {
    if (mcs_server_st_connect(ptr) == MEMCACHED_SUCCESS) {
        ssize_t n = mcs_server_st_io_write(ptr, command, command_length, with_flush);
        if (n == command_length) {
            return MEMCACHED_SUCCESS;
        }
    }

    return MEMCACHED_FAILURE;
}

ssize_t mcs_server_st_io_write(mcs_server_st *ptr,
                               const void *buffer,
                               size_t length,
                               char with_flush) {
    assert(ptr->fd != -1);

    return write(ptr->fd, buffer, length);
}

mcs_return mcs_server_st_read(mcs_server_st *ptr,
                              void *dta,
                              size_t size) {
    // We use a blocking read, but reset back to non-blocking
    // or the original state when we're done.
    //
    int flags = fcntl(ptr->fd, F_GETFL, 0);
    if (flags < 0 ||
        fcntl(ptr->fd, F_SETFL, flags & (~O_NONBLOCK)) < 0) {
        return MEMCACHED_FAILURE;
    }

    char *data = dta;
    size_t done = 0;

    while (done < size) {
        size_t n = read(ptr->fd, data + done, size - done);
        if (n == -1) {
            fcntl(ptr->fd, F_SETFL, flags);
            return MEMCACHED_FAILURE;
        }

        done += (size_t) n;
    }

    fcntl(ptr->fd, F_SETFL, flags);
    return MEMCACHED_SUCCESS;
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

const char *mcs_server_st_usr(mcs_server_st *ptr) {
    return ptr->usr;
}

const char *mcs_server_st_pwd(mcs_server_st *ptr) {
    return ptr->pwd;
}

#else // !MOXI_USE_VBUCKET

// The following is abuse of private internals from libmemcached!!!!
// Using these symbols doesn't work if you try to link with a shared
// version of libmemcached, so you need an archive
// It should be fixed ASAP
extern void* memcached_server_instance_fetch(memcached_st *ptr,
                                             uint32_t server_key);
extern void memcached_quit_server(memcached_server_st *ptr, bool io_death);
extern memcached_return_t memcached_connect(void* ptr);
extern memcached_return_t memcached_do(void *ptr, const void *command,
                                       size_t command_length,
                                       bool with_flush);
extern ssize_t memcached_io_write(void *ptr, const void *buffer,
                                  size_t length, bool with_flush);
extern memcached_return_t memcached_safe_read(void *ptr, void *dta,
                                              size_t size);
extern void memcached_io_reset(void *ptr);
// END libmemcached hack

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
    return memcached_server_instance_fetch(ptr, i);
}

bool mcs_stable_update(mcs_st *curr_version, mcs_st *next_version) {
    return false;
}

uint32_t mcs_key_hash(mcs_st *ptr, const char *key, size_t key_length, int *vbucket) {
    if (vbucket != NULL) {
        *vbucket = -1;
    }

    return memcached_generate_hash(ptr, key, key_length);
}

void mcs_server_invalid_vbucket(mcs_st *ptr, int server_index, int vbucket) {
    // NO-OP for libmemcached.
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

const char *mcs_server_st_usr(mcs_server_st *ptr) {
    return NULL;
}

const char *mcs_server_st_pwd(mcs_server_st *ptr) {
    return NULL;
}

#endif // !MOXI_USE_VBUCKET
