#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <check.h>

#include "memcached.h"
#include "cproxy.h"

int main_check(int argc, char **argv);

START_TEST(test_skey)
{
    fail_unless(skey_len("123") == 3, "skey_len");
    fail_unless(skey_len("123 ") == 3, "skey_len");
    fail_unless(skey_len("123\t") == 4, "skey_len");
}
END_TEST

static Suite* moxi_suite(void)
{
    Suite *s = suite_create("moxi");

    /* Core test case */
    TCase *tc_core = tcase_create("core");
    tcase_add_test(tc_core, test_skey);
    suite_add_tcase(s, tc_core);

    return s;
}

int main_check(int argc, char **argv)
{
    int number_failed;
    Suite *s = moxi_suite();
    SRunner *sr = srunner_create(s);
    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);
    int rv = (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
    exit(rv);
    return rv;
}
