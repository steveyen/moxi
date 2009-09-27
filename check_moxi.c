#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <check.h>

#include "memcached.h"
#include "cproxy.h"

#define s_len(str) (str), strlen(str)

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
                            " port_listen = 443322 , "
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
    fail_unless(w.port_listen == 443322, "tpb");

    // Test that things get zero'ed out.
    //
    proxy_behavior u =
      cproxy_parse_behavior(" ,,,"
                            " cycle =  , "
                            " downstream_max   =  ,"
                            " downstream_weight =  ,"
                            " downstream_retry =  ,"
                            " downstream_protocol =  ,"
                            " downstream_timeout =  ,"
                            " front_cache_max =  , "
                            " front_cache_lifespan =  , "
                            " front_cache_spec =   , "
                            " front_cache_unspec  =   ,"
                            " key_stats_max =  , "
                            " key_stats_lifespan =  , "
                            " key_stats_spec =  , "
                            " key_stats_unspec  =  ,"
                            " optimize_set =    , "
                            " usr =   , "
                            " pwd =   , "
                            " host =  ,"
                            " port =  , "
                            " bucket =  , "
                            " port_listen =  , "
                            " UNKNOWN_IGNORED_KEY =, "
                            " ,,,  ",
                            w);
    fail_if(cproxy_equal_behavior(&u, &w), "tpb");

    fail_unless(u.cycle == 0, "tpb");
    fail_unless(u.downstream_max == 0, "tpb");
    fail_unless(u.downstream_weight == 0, "tpb");
    fail_unless(u.downstream_retry == 0, "tpb");
    fail_unless(u.downstream_protocol == proxy_downstream_ascii_prot, "tpb");
    fail_unless(u.front_cache_max == 0, "tpb");
    fail_unless(strcmp(u.front_cache_spec, "") == 0, "tpb");
    fail_unless(strcmp(u.front_cache_unspec, "") == 0, "tpb");
    fail_unless(u.key_stats_max == 0, "");
    fail_unless(strcmp(u.key_stats_spec, "") == 0, "tpb");
    fail_unless(strcmp(u.key_stats_unspec, "") == 0, "tpb");
    fail_unless(strcmp(u.optimize_set, "") == 0, "tpb");
    fail_unless(strcmp(u.usr, "") == 0, "tpb");
    fail_unless(strcmp(u.pwd, "") == 0, "tpb");
    fail_unless(strcmp(u.host, "") == 0, "tpb");
    fail_unless(u.port == 0, "tpb");
    fail_unless(strcmp(u.bucket, "") == 0, "tpb");
    fail_unless(u.port_listen == 0, "tpb");
}
END_TEST

START_TEST(test_mcache) {
    mcache m;
    mcache_init(&m, false, &mcache_key_stats_funcs, false);
    fail_if(mcache_started(&m), "started");

    fail_unless(NULL == mcache_get(&m, s_len("not_there"), 0),
                "miss when unstarted");

    mcache_stop(&m);
    fail_if(mcache_started(&m), "stop idempotent");

    fail_unless(NULL == mcache_get(&m, s_len("not_there"), 0),
                "miss when unstarted");

    key_stats ks1 = {
      .key = "ks1",
      .refcount = 0,
      .exptime = 0,
      .next = NULL,
      .prev = NULL
    };

    mcache_set(&m, &ks1, 0, false, false);
    fail_unless(NULL == mcache_get(&m, s_len("ks1"), 0),
                "empty when not started");

    mcache_start(&m, 100);
    fail_unless(mcache_started(&m), "started");

    fail_unless(NULL == mcache_get(&m, s_len("ks1"), 0),
                "empty after just started");

    mcache_set(&m, &ks1, 0, false, false);
    fail_if(NULL == mcache_get(&m, s_len("ks1"), 0),
            "hit after set");
    fail_unless(NULL == mcache_get(&m, s_len("ks2"), 0),
                "miss");

    fail_unless(mcache_started(&m), "still started");
    mcache_stop(&m);
    fail_if(mcache_started(&m), "stopped");

    fail_unless(NULL == mcache_get(&m, s_len("ks1"), 0),
                "miss after stop");
    fail_unless(NULL == mcache_get(&m, s_len("ks2"), 0),
                "miss after stop");

    mcache_set(&m, &ks1, 0, false, false);
    fail_unless(NULL == mcache_get(&m, s_len("ks1"), 0),
                "empty when not started");

    key_stats ks9 = {
      .key = "ks9",
      .refcount = 0,
      .exptime = 0,
      .next = NULL,
      .prev = NULL
    };

    mcache_start(&m, 100);
    fail_unless(mcache_started(&m), "restarted");

    fail_unless(NULL == mcache_get(&m, s_len("ks1"), 0),
                "empty after just restarted");

    mcache_set(&m, &ks1, 0, false, false);
    mcache_set(&m, &ks9, 0, false, false);
    fail_if(NULL == mcache_get(&m, s_len("ks1"), 0),
            "hit after set");
    fail_if(NULL == mcache_get(&m, s_len("ks9"), 0),
            "hit after set");
    fail_unless(NULL == mcache_get(&m, s_len("ks2"), 0),
                "miss");

    mcache_delete(&m, s_len("ks1")),

    fail_unless(NULL == mcache_get(&m, s_len("ks1"), 0),
            "miss after deleted");
    fail_if(NULL == mcache_get(&m, s_len("ks9"), 0),
            "hit on non-deleted key");

    // Test with tiny capacity of 1 item.
    //
    fail_unless(mcache_started(&m), "still started");
    mcache_stop(&m);
    fail_if(mcache_started(&m), "stopped");
    mcache_start(&m, 1);
    fail_unless(mcache_started(&m), "restarted small");

    fail_unless(NULL == mcache_get(&m, s_len("ks1"), 0),
                "empty after just restarted");
    fail_unless(NULL == mcache_get(&m, s_len("ks9"), 0),
                "empty after just restarted");

    mcache_set(&m, &ks1, 0, false, false);
    fail_if(NULL == mcache_get(&m, s_len("ks1"), 0),
            "hit after set");
    mcache_set(&m, &ks9, 0, false, false);
    fail_if(NULL == mcache_get(&m, s_len("ks9"), 0),
            "hit after set");

    fail_unless(NULL == mcache_get(&m, s_len("ks1"), 0),
                "miss");
    fail_unless(NULL == mcache_get(&m, s_len("ks2"), 0),
                "miss");

    fail_if(NULL == mcache_get(&m, s_len("ks9"), 0),
            "hit after set");

    // Test with tiny capacity of 2 items.
    //
    fail_unless(mcache_started(&m), "still started");
    mcache_stop(&m);
    fail_if(mcache_started(&m), "stopped");
    mcache_start(&m, 2);
    fail_unless(mcache_started(&m), "restarted with 2 slots");

    fail_unless(NULL == mcache_get(&m, s_len("ks1"), 0),
                "empty after just restarted");
    fail_unless(NULL == mcache_get(&m, s_len("ks9"), 0),
                "empty after just restarted");

    mcache_set(&m, &ks1, 0, false, false);
    fail_if(NULL == mcache_get(&m, s_len("ks1"), 0),
            "hit after set");
    mcache_set(&m, &ks9, 0, false, false);
    fail_if(NULL == mcache_get(&m, s_len("ks9"), 0),
            "hit after set");

    fail_if(NULL == mcache_get(&m, s_len("ks1"), 0), // We last touched ks1,
            "hit after set");                        // so ks9 is LRU.

    key_stats ks8 = {
      .key = "ks8",
      .refcount = 0,
      .exptime = 0,
      .next = NULL,
      .prev = NULL
    };

    mcache_set(&m, &ks8, 0, false, false);
    fail_if(NULL == mcache_get(&m, s_len("ks8"), 0),
            "hit after set");
    fail_if(NULL == mcache_get(&m, s_len("ks1"), 0),
            "miss");
    fail_unless(NULL == mcache_get(&m, s_len("ks9"), 0),
                "miss");
}
END_TEST

START_TEST(test_matcher)
{
    matcher m;

    matcher_init(&m, false);
    fail_if(true == matcher_check(&m, s_len("hi"), false),
            "when unstarted");
    fail_if(false == matcher_check(&m, s_len("hi"), true),
            "when unstarted");

    fail_if(matcher_started(&m), "unstarted");
    matcher_stop(&m);
    fail_if(matcher_started(&m), "stop is idempotent");
    matcher_start(&m, NULL);
    fail_if(matcher_started(&m), "unstarted with NULL spec");
    matcher_start(&m, "");
    fail_if(matcher_started(&m), "unstarted with blank spec");

    matcher_start(&m, "pre1:");
    fail_if(matcher_started(&m) == false, "started");

    fail_if(true == matcher_check(&m, s_len(""), false),
            "match");
    fail_if(true == matcher_check(&m, s_len("hi"), false),
            "no match");
    fail_if(true == matcher_check(&m, s_len("pre1"), false),
            "match");
    fail_if(true == matcher_check(&m, s_len("pre1foo"), false),
            "match");
    fail_unless(true == matcher_check(&m, s_len("pre1:"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre1:foo"), false),
                "match");

    matcher_stop(&m);
    fail_if(matcher_started(&m), "unstarted after stop");
    fail_if(true == matcher_check(&m, s_len("pre1:"), false),
            "miss after stop");
    fail_if(true == matcher_check(&m, s_len("pre1:foo"), false),
            "miss after stop");

    matcher_start(&m, "pre1:|pre2:");
    fail_if(matcher_started(&m) == false, "started");

    fail_if(true == matcher_check(&m, s_len(""), false),
            "match");
    fail_if(true == matcher_check(&m, s_len("hi"), false),
            "no match");
    fail_if(true == matcher_check(&m, s_len("pre1"), false),
            "match");
    fail_if(true == matcher_check(&m, s_len("pre1foo"), false),
            "match");
    fail_unless(true == matcher_check(&m, s_len("pre1:"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre1:foo"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre2:"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre2:foo"), false),
                "match");

    matcher_stop(&m);
    fail_if(matcher_started(&m), "unstarted after stop");
    fail_if(true == matcher_check(&m, s_len("pre1:"), false),
            "miss after stop");
    fail_if(true == matcher_check(&m, s_len("pre1:foo"), false),
            "miss after stop");

    matcher_start(&m, "pre1:|pre2:|pre3:");
    fail_if(matcher_started(&m) == false, "started");

    fail_if(true == matcher_check(&m, s_len(""), false),
            "match");
    fail_if(true == matcher_check(&m, s_len("hi"), false),
            "no match");
    fail_if(true == matcher_check(&m, s_len("pre1"), false),
            "match");
    fail_if(true == matcher_check(&m, s_len("pre1foo"), false),
            "match");
    fail_unless(true == matcher_check(&m, s_len("pre1:"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre1:foo"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre2:"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre2:foo"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre3:"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre3:foo"), false),
                "match");

    matcher_stop(&m);
    fail_if(matcher_started(&m), "unstarted after stop");
    fail_if(true == matcher_check(&m, s_len("pre1:"), false),
            "miss after stop");
    fail_if(true == matcher_check(&m, s_len("pre1:foo"), false),
            "miss after stop");

    matcher_start(&m, "|||pre1:|||pre2:|||pre3:|||");
    fail_if(matcher_started(&m) == false, "started");

    fail_if(true == matcher_check(&m, s_len(""), false),
            "match");
    fail_if(true == matcher_check(&m, s_len("hi"), false),
            "no match");
    fail_if(true == matcher_check(&m, s_len("pre1"), false),
            "match");
    fail_if(true == matcher_check(&m, s_len("pre1foo"), false),
            "match");
    fail_unless(true == matcher_check(&m, s_len("pre1:"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre1:foo"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre2:"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre2:foo"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre3:"), false),
                "match");
    fail_unless(true == matcher_check(&m, s_len("pre3:foo"), false),
                "match");
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
    tcase_add_test(tc_core, test_mcache);
    tcase_add_test(tc_core, test_matcher);
    suite_add_tcase(s, tc_core);

    return s;
}

int main(int argc, char **argv)
{
    int number_failed;
    Suite *s = moxi_suite();
    SRunner *sr = srunner_create(s);
    srunner_run_all(sr, CK_ENV);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);
    int rv = (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
    exit(rv);
    return rv;
}
