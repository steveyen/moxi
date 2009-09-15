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

static pthread_t check_thread_tid;
static proxy_main *pmain = NULL;

extern proxy_behavior behavior_default_g;

static char *empty[] = { NULL };

int main_check(int argc, char **argv);

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

static Suite* moxi_agent_suite(void)
{
    Suite *s = suite_create("moxi_agent");

    /* Core test case */
    TCase *tc_core = tcase_create("core");
    tcase_add_test(tc_core, test_first_config);
    tcase_add_test(tc_core, test_easy_reconfig);
    suite_add_tcase(s, tc_core);

    return s;
}

// We have a separate thread here to pretend to be the libconflate thread.
//
static void *check_thread(void *arg) {
    sleep(1);

    assert(!is_listen_thread());

    int number_failed;
    Suite *s = moxi_agent_suite();
    SRunner *sr = srunner_create(s);
    srunner_set_fork_status(sr, CK_NOFORK);
    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);

    int rv = (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
    exit(rv);

    return NULL;
}

/**
 * Run this like...
 *  check_moxi_agent -vvv -p 11211
 */
int main_check(int argc, char **argv)
{
    int ret;

    fprintf(stderr, "thread_id %x\n",
            (int) pthread_self());

    proxy_behavior pbg = behavior_default_g;

    pmain = cproxy_init_agent_start("check_moxi_agent@localhost", // Fake JID.
                                    "password",                   // Fake password.
                                    CONFLATE_DB_PATH "/check_moxi_agent.cfg",
                                    NULL,
                                    pbg,
                                    settings.num_threads);
    assert(pmain);

    ret = pthread_create(&check_thread_tid, NULL,
                         check_thread, NULL);
    if (ret != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        exit(EXIT_FAILURE);
    }

    return 0;
}
