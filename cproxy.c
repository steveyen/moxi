/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sysexits.h>
#include <libmemcached/memcached.h>
#include "memcached.h"
#include "cproxy.h"

/**
 * cfg_in looks ike "local_port=host:port,host:port;local_port=host:port"
 * like "11222=memcached1.foo.net:11211"
 */
int cproxy_init(const char *cfg) {
    char *buff;
    char *next;
    char *proxy_sect;
    char *proxy_port_str;
    int   proxy_port;

    buff = strdup(cfg);
    next = buff;
    while (next != NULL) {
        proxy_sect = strsep(&next, ";");

        proxy_port_str = strsep(&proxy_sect, "=");
        if (proxy_sect == NULL) {
            fprintf(stderr, "bad cproxy config, missing =\n");
            exit(EXIT_FAILURE);
        }
        proxy_port = atoi(proxy_port_str);
        if (proxy_port <= 0) {
            fprintf(stderr, "bad cproxy config, bad proxy port\n");
            exit(EXIT_FAILURE);
        }

        memcached_server_st *mservers;
        memcached_st *mst;

        mservers = memcached_servers_parse(proxy_sect);

        mst = memcached_create(NULL);
        if (!mst) {
            fprintf(stderr, "failed memcached_create.\n");
            exit(EXIT_FAILURE);
        }

        memcached_server_push(mst, mservers);

        if (false && server_socket(proxy_port, proxy_upstream_ascii_prot) != 0) {
            fprintf(stderr, "failed to listen as proxy on TCP port %d\n", proxy_port);
            if (errno != 0)
                perror("tcp listen");
            exit(EX_OSERR);
        }
    }
    free(buff);

    if (settings.verbose > 1)
        fprintf(stderr, "cproxy initted\n");

    return 0;
}
