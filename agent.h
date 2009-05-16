/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef AGENT_H
#define AGENT_H

#include "conflate.h"

int cproxy_init_agent(char *cfg_str,
                      proxy_behavior behavior,
                      int nthreads);

int cproxy_init_agent_start(char *jid, char *jpw,
                            char *config, char *host,
                            proxy_behavior behavior,
                            int nthreads);

void on_conflate_new_config(void *userdata, kvpair_t *config);
void on_conflate_get_stats(void *userdata, void *opaque,
                           char *type, kvpair_t *form,
                           conflate_add_stat add_stat);
void on_conflate_reset_stats(void *userdata,
                             char *type, kvpair_t *form);
void on_conflate_ping_test(void *userdata, void *opaque,
                           kvpair_t *form,
                           conflate_add_ping_report cb);

void cproxy_on_new_config(void *data0, void *data1);

void cproxy_on_new_pool(proxy_main *m,
                        char *name, int port,
                        char *config_str,
                        uint32_t config_ver,
                        int   behaviors_num,
                        proxy_behavior *behaviors);

char **get_key_values(kvpair_t *kvs, char *key);

#endif AGENT_H
