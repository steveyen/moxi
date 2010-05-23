/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <assert.h>
#include "mcs.h"

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
                                break;
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }

                if (j >= n) {
                    return ptr;
                }
            }
        }
    }

    mcs_free(ptr);

    return NULL;
}

void mcs_free(mcs_st *ptr) {
    if (ptr->servers) {
        free(ptr->servers);
    }
    vbucket_config_destroy(ptr->vch);
    memset(ptr, 0, sizeof(*ptr));
}

uint32_t mcs_server_count(mcs_st *ptr) {
    return (uint32_t) vbucket_config_get_num_servers(ptr->vch);
}

mcs_server_st *mcs_server_index(mcs_st *ptr, int i) {
    return &ptr->servers[i];
}

uint32_t mcs_key_hash(mcs_st *ptr, const char *key, size_t key_length) {
    return (uint32_t) vbucket_get_master(ptr->vch,
                                         vbucket_get_vbucket_by_key(ptr->vch,
                                                                    key,
                                                                    key_length));
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
    // TODO: return memcached_io_write(ptr, buffer, length, with_flush);
    return -1;
}

mcs_return mcs_server_st_read(mcs_server_st *ptr,
                              void *dta,
                              size_t size) {
    // TODO: return memcached_safe_read(ptr, dta, size);
    return -1;
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

#endif // !MOXI_USE_VBUCKET
