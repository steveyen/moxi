#include "redirects.h"

#ifdef HAVE_CONFLATE_H

#undef conflate_add_field
#undef conflate_add_field_multi
#undef conflate_next_fieldset
#undef conflate_init_form

void (*redirected_conflate_add_field_target)(conflate_form_result *r, const char *k, const char *v) = conflate_add_field;
void (*redirected_conflate_add_field_multi_target)(conflate_form_result *r, const char *k, const char **v) = conflate_add_field_multi;
void (*redirected_conflate_next_fieldset_target)(conflate_form_result *r) = conflate_next_fieldset;
void (*redirected_conflate_init_form_target)(conflate_form_result *r) = conflate_init_form;

#endif

#undef collect_memcached_stats_for_proxy

void (*redirected_collect_memcached_stats_for_proxy_target)(struct main_stats_collect_info *msci,
                                const char *proxy_name,
                                int proxy_port) = collect_memcached_stats_for_proxy;
void redirected_collect_memcached_stats_for_proxy(struct main_stats_collect_info *msci,
                          const char *proxy_name,
                          int proxy_port)
{
  (*redirected_collect_memcached_stats_for_proxy_target)(msci, proxy_name, proxy_port);
}

void reset_redirections(void)
{
  redirected_collect_memcached_stats_for_proxy_target = collect_memcached_stats_for_proxy;
#ifdef HAVE_CONFLATE_H
  redirected_conflate_add_field_target = conflate_add_field;
  redirected_conflate_add_field_multi_target = conflate_add_field_multi;
  redirected_conflate_next_fieldset_target = conflate_next_fieldset;
  redirected_conflate_init_form_target = conflate_init_form;
#endif
}

#ifdef HAVE_CONFLATE_H
void redirected_conflate_add_field(conflate_form_result *r, const char *k, const char *v)
{
  (*redirected_conflate_add_field_target)(r,k,v);
}

void redirected_conflate_add_field_multi(conflate_form_result *r, const char *k, const char **v)
{
  (*redirected_conflate_add_field_multi_target)(r,k,v);
}

void redirected_conflate_next_fieldset(conflate_form_result *r)
{
  (*redirected_conflate_next_fieldset_target)(r);
}

void redirected_conflate_init_form(conflate_form_result *r)
{
  (*redirected_conflate_init_form_target)(r);
}
#endif
