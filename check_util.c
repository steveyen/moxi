#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <check.h>

#include "memcached.h"

START_TEST (test_safe_strtoul)
{
    uint32_t val;
    fail_unless(safe_strtoul("123", &val), "Failed parsing 123");
    fail_unless(val == 123, "Didn't parse 123 to 123");
    fail_unless(safe_strtoul("+123", &val), "Failed parsing +123");
    fail_unless(val == 123, "Didn't parse +123 to 123");
    fail_if(safe_strtoul("", &val), "Unexpectedly parsed an empty string.");
    fail_if(safe_strtoul("123BOGUS", &val), "Parsed 123BOGUS");
    /* Not sure what it does, but this works with ICC :/
       assert(!safe_strtoul("92837498237498237498029383", &val)); // out of range
    */

    // extremes:
    // 2**32 -1
    fail_unless(safe_strtoul("4294967295", &val), "Failed parsing 4294967295");
    fail_unless(val == 4294967295L, "4294967295 != 4294967295L somehow");
    /* This actually works on 64-bit ubuntu
       assert(!safe_strtoul("4294967296", &val)); // 2**32
    */
    fail_if(safe_strtoul("-1", &val), "Parsed a negative number as unsigned.");
}
END_TEST

START_TEST (test_safe_strtoull)
{
    uint64_t val;

    fail_unless(safe_strtoull("123", &val), "Failed parsing 123");
    fail_unless(val == 123, "Didn't parse 123 to 123");
    fail_unless(safe_strtoull("+123", &val), "Failed parsing +123");
    fail_unless(val == 123, "Didn't parse +123 to 123");
    fail_if(safe_strtoull("", &val), "Unexpectedly parsed an empty string.");
    fail_if(safe_strtoull("123BOGUS", &val), "Parsed 123BOGUS");

    fail_if(safe_strtoull("92837498237498237498029383", &val),
            "Parsed out of range value.");

    // extremes:
    fail_unless(safe_strtoull("18446744073709551615", &val),
                "Failed parsing 18446744073709551615"); // 2**64 - 1
    fail_unless(val == 18446744073709551615ULL,
                "18446744073709551615 != 18446744073709551615ULL");
    fail_if(safe_strtoull("18446744073709551616", &val),
            "Parsed 18446744073709551616"); // 2**64
    fail_if(safe_strtoull("-1", &val), "Parsed a negative number as unsigned.");
}
END_TEST

START_TEST (test_safe_strtoll)
{
    int64_t val;
    fail_unless(safe_strtoll("123", &val), "Failed parsing 123");
    fail_unless(val == 123, "123 != 123");
    fail_unless(safe_strtoll("+123", &val), "Failed parsing +123");
    fail_unless(val == 123, "123 != 123");
    fail_unless(safe_strtoll("-123", &val), "Failed parsing -123");
    fail_unless(val == -123, "-123 != -123");
    fail_if(safe_strtoll("", &val), "Parsed an empty string"); // empty
    fail_if(safe_strtoll("123BOGUS", &val), "Parsed 123BOGUS"); // non-numeric
    fail_if(safe_strtoll("92837498237498237498029383", &val),
            "Parsed out of range value"); // out of range

    // extremes:
    fail_if(safe_strtoll("18446744073709551615", &val),
            "Parsed out of range value"); // 2**64 - 1
    fail_unless(safe_strtoll("9223372036854775807", &val),
                "Failed parsing 9223372036854775807"); // 2**63 - 1
    fail_unless(val == 9223372036854775807LL,
                "9223372036854775807 != 9223372036854775807LL");
    /*
      assert(safe_strtoll("-9223372036854775808", &val)); // -2**63
      assert(val == -9223372036854775808LL);
    */
    fail_if(safe_strtoll("-9223372036854775809", &val),
            "Parsed out of range value"); // -2**63 - 1

    // We'll allow space to terminate the string.  And leading space.
    fail_unless(safe_strtoll(" 123 foo", &val), "Failed parsing \" 123 foo\"");
    fail_unless(val == 123, "\" 123 foo\" != 123");
}
END_TEST

START_TEST (test_safe_strtol)
{
    int32_t val;
    fail_unless(safe_strtol("123", &val), "Failed parsing 123");
    fail_unless(val == 123, "123 != 123");
    fail_unless(safe_strtol("+123", &val), "Failed parsing +123");
    fail_unless(val == 123, "+123 != 123");
    fail_unless(safe_strtol("-123", &val), "Failed parsing -123");
    fail_unless(val == -123, "-123 != -123");
    fail_if(safe_strtol("", &val), "Parsing empty string");
    fail_if(safe_strtol("123BOGUS", &val), "Parsed 123BOGUS");
    fail_if(safe_strtol("92837498237498237498029383", &val),
            "Parsed out of range value.");

    // extremes:
    /* This actually works on 64-bit ubuntu
       assert(!safe_strtol("2147483648", &val)); // (expt 2.0 31.0)
    */
    fail_unless(safe_strtol("2147483647", &val),
                "Failed parsing upper limit."); // (- (expt 2.0 31) 1)
    fail_unless(val == 2147483647L, "2147483647 != 2147483647L");
    /* This actually works on 64-bit ubuntu
       assert(!safe_strtol("-2147483649", &val)); // (- (expt -2.0 31) 1)
    */

    // We'll allow space to terminate the string.  And leading space.
    fail_unless(safe_strtol(" 123 foo", &val), "Failed parsing \" 123 foo\"");
    fail_unless(val == 123, "\" 123 foo\" != 123");
}
END_TEST

static Suite* util_suite (void)
{
    Suite *s = suite_create ("util");

    /* Core test case */
    TCase *tc_core = tcase_create ("Core");
    tcase_add_test(tc_core, test_safe_strtoul);
    tcase_add_test(tc_core, test_safe_strtoull);
    tcase_add_test(tc_core, test_safe_strtoll);
    tcase_add_test(tc_core, test_safe_strtol);
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
