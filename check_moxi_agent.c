#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include <assert.h>
#include <check.h>

#include "memcached.h"
#include "cproxy.h"
#include "agent.h"

static proxy_main *pmain = NULL;

extern proxy_behavior behavior_default_g;

static char *empty[] = { NULL };

static kvpair_t *mk_kvpairs(char *spec[]) {
  kvpair_t *last = NULL;
  kvpair_t *head = NULL;

  int i = 0;
  while (spec[i]) {
    if (head == NULL) {
      assert(last == NULL);
      head = last = mk_kvpair(spec[i], empty);
    } else {
      last->next = mk_kvpair(spec[i], empty);
      last = last->next;
    }

    i++;

    while(spec[i]) {
      add_kvpair_value(last, spec[i]);
      i++;
    }

    i++;
  }

  return head;
}

START_TEST(test_first_config)
{
  kvpair_t *head = mk_kvpair("pools", empty);

  // No crash on weak config.
  //
  on_conflate_new_config(pmain, head);
  on_conflate_new_config(pmain, head);

  sleep(1);

  fail_unless(pmain->proxy_head == NULL, "pools empty");

  // Add a pool.
  //
  char *ca[] = {
    "pools",
    "poolx",
    NULL,
    "behavior-poolx",
    "port_listen=11411",
    NULL,
    "pool-poolx",
    "svr1",
    NULL,
    "svr-svr1",
    "host=localhost",
    "port=11211",
    NULL,
    NULL
  };
  kvpair_t *c = mk_kvpairs(ca);

  on_conflate_new_config(pmain, c);

  sleep(1);

  fail_if(pmain->proxy_head == NULL, "fc");
  fail_if(pmain->proxy_head->next != NULL, "fc");
  fail_unless(pmain->proxy_head->port == 11411, "fc");
  fail_unless(strcmp(pmain->proxy_head->config, "localhost:11211") == 0, "fc");
  fail_unless(pmain->proxy_head->behavior_pool.num == 1, "fc");
  fail_unless(strcmp(pmain->proxy_head->behavior_pool.arr[0].host,
                     "localhost") == 0, "fc");
  fail_unless(pmain->proxy_head->behavior_pool.arr[0].port == 11211, "fc");
}
END_TEST

START_TEST(test_easy_reconfig)
{
  char *ca[] = {
    "pools",
    "poolx",
    NULL,
    "behavior-poolx",
    "port_listen=11411",
    NULL,
    "pool-poolx",
    "svr1",
    NULL,
    "svr-svr1",
    "host=localhost",
    "port=11211",
    NULL,
    NULL
  };
  kvpair_t *c = mk_kvpairs(ca);

  on_conflate_new_config(pmain, c);

  sleep(1);

  fail_if(pmain->proxy_head == NULL, "fc");
  fail_if(pmain->proxy_head->next != NULL, "fc");
  fail_unless(pmain->proxy_head->port == 11411, "fc");
  fail_unless(strcmp(pmain->proxy_head->config, "localhost:11211") == 0, "fc");
  fail_unless(pmain->proxy_head->behavior_pool.num == 1, "fc");
  fail_unless(strcmp(pmain->proxy_head->behavior_pool.arr[0].host,
                     "localhost") == 0, "fc");
  fail_unless(pmain->proxy_head->behavior_pool.arr[0].port == 11211, "fc");

  // Reconfig to d.
  //
  char *da[] = {
    "pools",
    "poolx",
    NULL,
    "behavior-poolx",
    "port_listen=11411",
    NULL,
    "pool-poolx",
    "svr1",
    NULL,
    "svr-svr1",
    "host=host1",
    "port=11111",
    NULL,
    NULL
  };
  kvpair_t *d = mk_kvpairs(da);

  on_conflate_new_config(pmain, d);

  sleep(1);

  fail_if(pmain->proxy_head == NULL, "fc");
  fail_if(pmain->proxy_head->next != NULL, "fc");
  fail_unless(pmain->proxy_head->port == 11411, "fc");
  fail_unless(strcmp(pmain->proxy_head->config, "host1:11111") == 0, "fc");
  fail_unless(pmain->proxy_head->behavior_pool.num == 1, "fc");
  fail_unless(strcmp(pmain->proxy_head->behavior_pool.arr[0].host,
                     "host1") == 0, "fc");
  fail_unless(pmain->proxy_head->behavior_pool.arr[0].port == 11111, "fc");

  // Reconfig to e.
  //
  char *ea[] = {
    "pools",
    "poolx",
    NULL,
    "behavior-poolx",
    "port_listen=11411",
    NULL,
    "pool-poolx",
    "svr1",
    "svr2",
    NULL,
    "svr-svr1",
    "host=mc1",
    "port=1111",
    NULL,
    "svr-svr2",
    "host=mc2",
    "port=2222",
    NULL,
    NULL
  };
  kvpair_t *e = mk_kvpairs(ea);

  on_conflate_new_config(pmain, e);

  sleep(1);

  fail_if(pmain->proxy_head == NULL, "fc");
  fail_if(pmain->proxy_head->next != NULL, "fc");
  fail_unless(pmain->proxy_head->port == 11411, "fc");
  fail_unless(strcmp(pmain->proxy_head->config, "mc1:1111,mc2:2222") == 0, "fc");
  fail_unless(pmain->proxy_head->behavior_pool.num == 2, "fc");
  fail_unless(strcmp(pmain->proxy_head->behavior_pool.arr[0].host,
                     "mc1") == 0, "fc");
  fail_unless(pmain->proxy_head->behavior_pool.arr[0].port == 1111, "fc");
  fail_unless(strcmp(pmain->proxy_head->behavior_pool.arr[1].host,
                     "mc2") == 0, "fc");
  fail_unless(pmain->proxy_head->behavior_pool.arr[1].port == 2222, "fc");

  // Go back to d
  //
  on_conflate_new_config(pmain, d);

  sleep(1);

  fail_if(pmain->proxy_head == NULL, "fc");
  fail_if(pmain->proxy_head->next != NULL, "fc");
  fail_unless(pmain->proxy_head->port == 11411, "fc");
  fail_unless(strcmp(pmain->proxy_head->config, "host1:11111") == 0, "fc");
  fail_unless(pmain->proxy_head->behavior_pool.num == 1, "fc");
  fail_unless(strcmp(pmain->proxy_head->behavior_pool.arr[0].host,
                     "host1") == 0, "fc");
  fail_unless(pmain->proxy_head->behavior_pool.arr[0].port == 11111, "fc");
}
END_TEST

static
void setup(void)
{
  start_main("moxi", NULL);
  assert(!is_listen_thread());
  proxy_behavior pbg = behavior_default_g;

  unlink("t/check_moxi_agent.cfg");
  pmain = cproxy_init_agent_start("check_moxi_agent@localhost", // Fake JID.
				  "password",                   // Fake password.
				  "t/check_moxi_agent.cfg",
				  NULL,
				  pbg,
				  settings.num_threads);
  assert(pmain);
}

static Suite* moxi_agent_suite(void)
{
    Suite *s = suite_create("moxi_agent");

    /* Core test case */
    TCase *tc_core = tcase_create("core");
    tcase_add_checked_fixture(tc_core, setup, NULL);
    tcase_add_test(tc_core, test_first_config);
    tcase_add_test(tc_core, test_easy_reconfig);
    suite_add_tcase(s, tc_core);

    if (getenv("CK_DEFAULT_TIMEOUT") == NULL)
      tcase_set_timeout(tc_core, 20);

    return s;
}

int main(int argc, char **argv)
{
    int number_failed;
    Suite *s = moxi_agent_suite();
    SRunner *sr = srunner_create(s);
    srunner_run_all(sr, CK_ENV);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);

    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
