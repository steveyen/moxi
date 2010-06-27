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
#include "work.h"

struct agg {
  pthread_mutex_t lock;
  int visits;
};

int main_check(int argc, char **argv);

static void test_collect_worker(void *data0, void *data1) {
  fail_unless(data0 != NULL, "wsc");
  fail_unless(data1 != NULL, "wsc");
  fail_unless(data0 == data1, "wsc");

  work_collect *c = data0;
  struct agg *agg = c->data;

  fail_unless(agg != NULL, "agg");

  pthread_mutex_lock(&agg->lock);
  agg->visits++;
  pthread_mutex_unlock(&agg->lock);

  work_collect_one(c);
}

static void test_collect_main(void) {
  int nthreads = settings.num_threads;

  struct agg agg;

  pthread_mutex_init(&agg.lock, NULL);

  agg.visits = 0;

  work_collect c;
  work_collect_init(&c, nthreads - 1, &agg);

  for (int i = 1; i < nthreads; i++) {
    LIBEVENT_THREAD *t = thread_by_index(i);
    fail_unless(NULL != t, "tc");
    fail_unless(NULL != t->work_queue, "tc");
    work_send(t->work_queue, test_collect_worker, &c, &c);
  }

  work_collect_wait(&c);

  fail_unless(agg.visits == nthreads - 1, "collect");
}

START_TEST(test_collect)
{
  test_collect_main();
}
END_TEST

static void test_one_worker(void *data0, void *data1) {
  char *s0 = data0;
  char *s1 = data1;

  fail_unless(strcmp(s0, "hello") == 0, "tcw");
  fail_unless(strcmp(s1, "world") == 0, "tcw");
}

START_TEST(test_one)
{
  LIBEVENT_THREAD *t = thread_by_index(1);
  fail_unless(NULL != t, "tc");
  fail_unless(NULL != t->work_queue, "tc");
  work_send(t->work_queue, test_one_worker, "hello", "world");
  sleep(1);
}
END_TEST

static
void setup(void)
{
    start_main("moxi", NULL);
}

static Suite* work_suite(void)
{
    Suite *s = suite_create("work");

    /* Core test case */
    TCase *tc_core = tcase_create("core");
    tcase_add_checked_fixture(tc_core, setup, NULL);
    tcase_add_test(tc_core, test_one);
    tcase_add_test(tc_core, test_collect);
    suite_add_tcase(s, tc_core);

    return s;
}

/**
 * Run this like...
 *  check_work -vvv -p 11211
 */
int main(int argc, char **argv)
{
    int number_failed;
    Suite *s = work_suite();
    SRunner *sr = srunner_create(s);
    srunner_run_all(sr, CK_ENV);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);

    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
