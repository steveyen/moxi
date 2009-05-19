#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <check.h>

#include "memcached.h"

START_TEST (test_one)
{
    fail_if(false, "False has no truth.");
}
END_TEST

static Suite* util_suite (void)
{
    Suite *s = suite_create ("util");

    /* Core test case */
    TCase *tc_core = tcase_create ("Core");
    tcase_add_test(tc_core, test_one);
    suite_add_tcase(s, tc_core);

    return s;
}

int
main (void)
{
    int number_failed;
    Suite *s = util_suite ();
    SRunner *sr = srunner_create (s);
    srunner_run_all (sr, CK_NORMAL);
    number_failed = srunner_ntests_failed (sr);
    srunner_free (sr);
    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
