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

START_TEST(test_whitespace)
{
    char *s, *t;

    s = "123";
    fail_unless(skipspace(s) == s, "ss");
    s = "  123";
    fail_unless(skipspace(s) == s + 2, "ss");
    fail_unless(trailspace(s) == s, "ss");
    s = "123";
    t = trimstrdup(s);
    fail_unless(t && strcmp(t, "123") == 0, "ss");
    free(t);
    s = "  123  ";
    t = trimstrdup(s);
    fail_unless(t && strcmp(t, "123") == 0, "ss");
    free(t);

    s = "123";
    fail_unless(wordeq(s, "123"), "wordeq");
    s = "  123";
    fail_unless(wordeq(s, "123"), "wordeq");
    s = "  123  ";
    fail_unless(wordeq(s, "123"), "wordeq");
    s = "  123  456 ";
    fail_unless(wordeq(s, "123"), "wordeq");
    s = "   ";
    fail_unless(wordeq(s, ""), "wordeq");
}
END_TEST

START_TEST(test_parse_behavior) {
    proxy_behavior z = {0};
    proxy_behavior b = {0};
    fail_unless(cproxy_equal_behavior(&z, &b), "tpb");

    proxy_behavior x =
      cproxy_parse_behavior("downstream_max=1", b);
    fail_unless(x.downstream_max == 1, "tpb");
    fail_unless(cproxy_equal_behavior(&z, &b), "tpb");
    fail_if(cproxy_equal_behavior(&x, &b), "tpb");

    proxy_behavior y =
      cproxy_parse_behavior("  \n\t\n downstream_max\n\r\n =\n\n  1\n 11  ", b);
    fail_unless(y.downstream_max == 1, "tpb");
    fail_unless(cproxy_equal_behavior(&z, &b), "tpb");
    fail_unless(cproxy_equal_behavior(&y, &x), "tpb");
    fail_if(cproxy_equal_behavior(&y, &b), "tpb");

    proxy_behavior w =
      cproxy_parse_behavior(" ,,,"
                            " cycle = 100, "
                            " downstream_max   = 1,"
                            " downstream_weight = 2 ,"
                            " downstream_retry = 3 ,"
                            " downstream_protocol = binary ,"
                            " downstream_timeout = 4 ,"
                            " front_cache_max = 5 , "
                            " front_cache_lifespan = 6 , "
                            " front_cache_spec = aaa|bbb  , "
                            " front_cache_unspec  =   ,"
                            " key_stats_max = 7 , "
                            " key_stats_lifespan = 8 , "
                            " key_stats_spec = xxx|yyy , "
                            " key_stats_unspec  = zzz ,"
                            " optimize_set =  1|2|3  , "
                            " usr = user  , "
                            " pwd = pswd  , "
                            " host = hostname ,"
                            " port = 4321 , "
                            " bucket = buck , "
                            " UNKNOWN_IGNORED_KEY =, "
                            " ,,,  ",
                            b);
    fail_unless(cproxy_equal_behavior(&z, &b), "tpb");
    fail_if(cproxy_equal_behavior(&w, &b), "tpb");

    fail_unless(w.cycle == 100, "tpb");
    fail_unless(w.downstream_max == 1, "tpb");
    fail_unless(w.downstream_weight == 2, "tpb");
    fail_unless(w.downstream_retry == 3, "tpb");
    fail_unless(w.downstream_protocol == proxy_downstream_binary_prot, "tpb");
    fail_unless(w.front_cache_max == 5, "tpb");
    fail_unless(strcmp(w.front_cache_spec, "aaa|bbb") == 0, "tpb");
    fail_unless(strcmp(w.front_cache_unspec, "") == 0, "tpb");
    fail_unless(w.key_stats_max == 7, "tpb");
    fail_unless(strcmp(w.key_stats_spec, "xxx|yyy") == 0, "tpb");
    fail_unless(strcmp(w.key_stats_unspec, "zzz") == 0, "tpb");
    fail_unless(strcmp(w.optimize_set, "1|2|3") == 0, "tpb");
    fail_unless(strcmp(w.usr, "user") == 0, "tpb");
    fail_unless(strcmp(w.pwd, "pswd") == 0, "tpb");
    fail_unless(strcmp(w.host, "hostname") == 0, "tpb");
    fail_unless(w.port == 4321, "tpb");
    fail_unless(strcmp(w.bucket, "buck") == 0, "tpb");
}
END_TEST

static Suite* moxi_suite(void)
{
    Suite *s = suite_create("moxi");

    /* Core test case */
    TCase *tc_core = tcase_create("core");
    tcase_add_test(tc_core, test_skey);
    tcase_add_test(tc_core, test_whitespace);
    tcase_add_test(tc_core, test_parse_behavior);
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
