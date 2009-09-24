/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef AGENT_H
#define AGENT_H

#include <conflate.h>
#ifdef REDIRECTS_FOR_MOCKS
#include "redirects.h"
#endif

int cproxy_init_agent(char *cfg_str,
                      proxy_behavior behavior,
                      int nthreads);

proxy_main *cproxy_init_agent_start(char *jid, char *jpw,
                                    char *config, char *host,
                                    proxy_behavior behavior,
                                    int nthreads);

void on_conflate_new_config(void *userdata, kvpair_t *config);
enum conflate_mgmt_cb_result on_conflate_get_stats(void *opaque,
                                                   conflate_handle_t *handle,
                                                   const char *cmd,
                                                   bool direct,
                                                   kvpair_t *form,
                                                   conflate_form_result *);
enum conflate_mgmt_cb_result on_conflate_reset_stats(void *opaque,
                                                     conflate_handle_t *handle,
                                                     const char *cmd,
                                                     bool direct,
                                                     kvpair_t *form,
                                                     conflate_form_result *);
enum conflate_mgmt_cb_result on_conflate_ping_test(void *opaque,
                                                   conflate_handle_t *handle,
                                                   const char *cmd,
                                                   bool direct,
                                                   kvpair_t *form,
                                                   conflate_form_result *);

void cproxy_on_new_pool(proxy_main *m,
                        char *name, int port,
                        char *config_str,
                        uint32_t config_ver,
                        proxy_behavior_pool *behavior_pool);

char **get_key_values(kvpair_t *kvs, char *key);

#endif /* AGENT_H */
