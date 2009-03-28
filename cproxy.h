/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

int cproxy_init(const char *cfg);

#define IS_PROXY(x) (x == proxy_upstream_ascii_prot || x == proxy_downstream_ascii_prot)

