#ifndef REDIRECTS_H
#define REDIRECTS_H
#include "config.h"

#ifdef HAVE_CONFLATE_H
#include <conflate.h>

#define conflate_add_field redirected_conflate_add_field
#define conflate_add_field_multi redirected_conflate_add_field_multi
#define conflate_next_fieldset redirected_conflate_next_fieldset
#define conflate_init_form redirected_conflate_init_form

extern void (*redirected_conflate_add_field_target)(conflate_form_result *r, const char *k, const char *v);
extern void (*redirected_conflate_add_field_multi_target)(conflate_form_result *r, const char *k, const char **v);
extern void (*redirected_conflate_next_fieldset_target)(conflate_form_result *r);
extern void (*redirected_conflate_init_form_target)(conflate_form_result *r);

void redirected_conflate_add_field(conflate_form_result *r, const char *k, const char *v);
void redirected_conflate_add_field_multi(conflate_form_result *r, const char *k, const char **v);
void redirected_conflate_next_fieldset(conflate_form_result *r);
void redirected_conflate_init_form(conflate_form_result *r);

#endif // HAVE_CONFLATE_H

struct main_stats_collect_info;

void collect_memcached_stats_for_proxy(struct main_stats_collect_info *msci, const char *proxy_name, int proxy_port);
void redirected_collect_memcached_stats_for_proxy(struct main_stats_collect_info *msci, const char *proxy_name, int proxy_port);
extern void (*redirected_collect_memcached_stats_for_proxy_target)(struct main_stats_collect_info *msci, const char *proxy_name, int proxy_port);

#define collect_memcached_stats_for_proxy redirected_collect_memcached_stats_for_proxy

void reset_redirections(void);


#endif
