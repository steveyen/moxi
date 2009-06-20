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

int main_check(int argc, char **argv);

START_TEST(test_first_config)
{
  char *v[] = {"poolx", "pooly", NULL};
  kvpair_t *config = mk_kvpair("pools", v);

  on_conflate_new_config(pmain, config);
  sleep(2);
}
END_TEST

static Suite* moxi_agent_suite(void)
{
    Suite *s = suite_create("moxi_agent");

    /* Core test case */
    TCase *tc_core = tcase_create("core");
    tcase_add_test(tc_core, test_first_config);
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
                                    "/var/tmp/check_moxi_agent.cfg",
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
