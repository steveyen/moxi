#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include <assert.h>
#include <check.h>
#include <stddef.h>

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

uint64_t random_data[128];
static unsigned random_index;

struct field_description {
  char *name;
  int offset;
};

#define describe_field(struct_name, field) \
  {#field, offsetof(struct_name, field)}

#define arsize(a) (sizeof(a)/sizeof(a[0]))

struct field_description proxy_stats_description[] = {
  describe_field(struct proxy_stats, num_upstream),
  describe_field(struct proxy_stats, tot_upstream),
  describe_field(struct proxy_stats, num_downstream_conn),
  describe_field(struct proxy_stats, tot_downstream_conn),
  describe_field(struct proxy_stats, tot_downstream_released),
  describe_field(struct proxy_stats, tot_downstream_reserved),
  describe_field(struct proxy_stats, tot_downstream_freed),
  describe_field(struct proxy_stats, tot_downstream_quit_server),
  describe_field(struct proxy_stats, tot_downstream_max_reached),
  describe_field(struct proxy_stats, tot_downstream_create_failed),
  describe_field(struct proxy_stats, tot_downstream_connect),
  describe_field(struct proxy_stats, tot_downstream_connect_failed),
  describe_field(struct proxy_stats, tot_downstream_auth),
  describe_field(struct proxy_stats, tot_downstream_auth_failed),
  describe_field(struct proxy_stats, tot_downstream_bucket),
  describe_field(struct proxy_stats, tot_downstream_bucket_failed),
  describe_field(struct proxy_stats, tot_downstream_propagate_failed),
  describe_field(struct proxy_stats, tot_downstream_close_on_upstream_close),
  describe_field(struct proxy_stats, tot_downstream_timeout),
  describe_field(struct proxy_stats, tot_wait_queue_timeout),
  describe_field(struct proxy_stats, tot_assign_downstream),
  describe_field(struct proxy_stats, tot_assign_upstream),
  describe_field(struct proxy_stats, tot_assign_recursion),
  describe_field(struct proxy_stats, tot_reset_upstream_avail),
  describe_field(struct proxy_stats, tot_retry),
  describe_field(struct proxy_stats, tot_multiget_keys),
  describe_field(struct proxy_stats, tot_multiget_keys_dedupe),
  describe_field(struct proxy_stats, tot_multiget_bytes_dedupe),
  describe_field(struct proxy_stats, tot_optimize_sets),
  describe_field(struct proxy_stats, tot_optimize_self),
  describe_field(struct proxy_stats, err_oom),
  describe_field(struct proxy_stats, err_upstream_write_prep),
  describe_field(struct proxy_stats, err_downstream_write_prep)
};

struct field_description proxy_stats_cmd_description[] = {
  describe_field(proxy_stats_cmd, seen),
  describe_field(proxy_stats_cmd, hits),
  describe_field(proxy_stats_cmd, misses),
  describe_field(proxy_stats_cmd, read_bytes),
  describe_field(proxy_stats_cmd, write_bytes),
  describe_field(proxy_stats_cmd, cas)
};

uint64_t *field_ptr(const void *ptr, struct field_description *field);
uint64_t field_read(const void *ptr, struct field_description *field);
void field_write(const void *ptr, struct field_description *field, uint64_t value);

uint64_t *field_ptr(const void *ptr, struct field_description *field)
{
  return (uint64_t *)(((const char *)(ptr))+field->offset);
}

uint64_t field_read(const void *ptr, struct field_description *field)
{
  return *field_ptr(ptr, field);
}

void field_write(const void *ptr, struct field_description *field, uint64_t value)
{
  field_ptr(ptr, field)[0] = value;
}

static
void reset_random(void)
{
  random_index = 0;
}

static
uint64_t random_uint64t(void)
{
  unsigned i = random_index;
  random_index = (random_index + 1) % (sizeof(random_data) / sizeof(uint64_t));
  return random_data[i];
}

static
void randomize_uint64t_struct(void *_ptr, struct field_description *desc, int sizeinbytes)
{
  uint64_t *ptr = _ptr;
  int count = sizeinbytes/sizeof(struct field_description);
  for (int i = 0; i < count; i++)
    field_write(ptr, desc+i, random_uint64t());
}

static
proxy_stats_td gathered_stats;

static char *cmd_names[] = { // Keep sync'ed with enum_stats_cmd.
    "get",
    "get_key",
    "set",
    "add",
    "replace",
    "delete",
    "append",
    "prepend",
    "incr",
    "decr",
    "flush_all",
    "cas",
    "stats",
    "stats_reset",
    "version",
    "verbosity",
    "quit",
    "ERROR"
};

static
void gathering_conflate_add_field(conflate_form_result *r, const char *k, const char *v)
{
  int i;

  ck_assert((intptr_t)r == (intptr_t)gathering_conflate_add_field);
  if (strncmp(k, "11411:poolx:stats_", 18) != 0)
    return;

  char *rest = (char *)v;
  uint64_t value = strtoull(v, &rest, 10);

  fail_if(*rest != '\0', "got invalid value: %s", v);

  for (i = 0; i < arsize(proxy_stats_description); i++) {
    if (strcmp(proxy_stats_description[i].name, k+18) == 0) {
      field_write(&gathered_stats.stats, proxy_stats_description+i, value);
      return;
    }
  }

  const char *rest_of_name = k+18;
  ck_assert(strncmp(rest_of_name, "cmd_", 4) == 0);

  rest_of_name += 4;

  enum_stats_cmd_type type = 0;
  enum_stats_cmd cmd = 0;

  if (strncmp(rest_of_name, "regular_", 8) == 0) {
    type = STATS_CMD_TYPE_REGULAR;
    rest_of_name += 8;
  } else if (strncmp(rest_of_name, "quiet_", 6) == 0) {
    type = STATS_CMD_TYPE_QUIET;
    rest_of_name += 6;
  } else
    fail("unknown command type prefix found %s", rest_of_name);

  for (i = arsize(cmd_names)-1; i >= 0; i--) {
    int l = strlen(cmd_names[i]);
    if (strncmp(rest_of_name, cmd_names[i], l) == 0 && rest_of_name[l] == '_') {
      cmd = i;
      rest_of_name += l+1;
      break;
    }
  }

  fail_if(i < 0, "unknown command prefix: %s", rest_of_name);

  for (i = 0; i < arsize(proxy_stats_cmd_description); i++) {
    if (strcmp(rest_of_name, proxy_stats_cmd_description[i].name) == 0) {
      field_write(&(gathered_stats.stats_cmd[type][cmd]), proxy_stats_cmd_description + i, value);
      return;
    }
  }

  fail("unknown stat field: %s", k);
}

static
int collect_memcached_stats_for_proxy_called;
static
void cmd_stats_gathering_collect_memcached_stats_for_proxy(struct main_stats_collect_info *msci,
							   const char *proxy_name,
							   int proxy_port)
{
  collect_memcached_stats_for_proxy_called = 1;
}

START_TEST(test_cmd_stats_gathering)
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
  int i,j,t;

  // we're hardcoding knowledge of all commands and command types here.
  // this assertions ensure that the tests will be updated when
  // anything of the above will change
  ck_assert(STATS_CMD_ERROR + 1 == STATS_CMD_last);
  ck_assert(STATS_CMD_TYPE_QUIET + 1 == STATS_CMD_TYPE_last);

  on_conflate_new_config(pmain, c);

  proxy *proxy = pmain->proxy_head;
  ck_assert(proxy != NULL);
  ck_assert(proxy->next == NULL);

  ck_assert(pmain->nthreads >= 2);

  reset_random();
  for (t = 1; t < 3; t++) {
    randomize_uint64t_struct(&(proxy->thread_data[t].stats.stats), proxy_stats_description, sizeof(proxy_stats_description));
    for (i = 0; i < STATS_CMD_TYPE_last; i++) {
      for (j = 0; j < STATS_CMD_last; j++) {
	randomize_uint64t_struct(&(proxy->thread_data[t].stats.stats_cmd[i][j]),
				 proxy_stats_cmd_description, sizeof(proxy_stats_cmd_description));
      }
    }
  }

  redirected_conflate_add_field_target = gathering_conflate_add_field;
  redirected_collect_memcached_stats_for_proxy_target = cmd_stats_gathering_collect_memcached_stats_for_proxy;

  ck_assert(memcmp(&(proxy->thread_data[0].stats), &(proxy->thread_data[1].stats), sizeof(proxy->thread_data[1].stats)) != 0);

  on_conflate_get_stats(pmain, NULL, "get-stats", true, NULL, (conflate_form_result *)(intptr_t)gathering_conflate_add_field);

  ck_assert(collect_memcached_stats_for_proxy_called);

  ck_assert(memcmp(&(proxy->thread_data[0].stats), &(proxy->thread_data[1].stats), sizeof(proxy->thread_data[1].stats)) != 0);

  for (i = 0; i < STATS_CMD_TYPE_last; i++) {
    for (j = 0; j < STATS_CMD_last; j++) {
      for (t = 0; t < arsize(proxy_stats_cmd_description); t++) {
	uint64_t t1 = field_read(&(proxy->thread_data[1].stats.stats_cmd[i][j]), proxy_stats_cmd_description + t);
	uint64_t t2 = field_read(&(proxy->thread_data[2].stats.stats_cmd[i][j]), proxy_stats_cmd_description + t);
	uint64_t result = field_read(&(gathered_stats.stats_cmd[i][j]), proxy_stats_cmd_description + t);
	fail_unless(result == t1 + t2, "comparing command stats for %s(%d).%s: 0x%llx = 0x%llx + 0x%llx\n", cmd_names[j], i, proxy_stats_cmd_description[t].name, result, t1, t2);
      }
    }
  }

  for (t = 0; t < arsize(proxy_stats_description); t++) {
    uint64_t t1 = field_read(&(proxy->thread_data[1].stats.stats), proxy_stats_description + t);
    uint64_t t2 = field_read(&(proxy->thread_data[2].stats.stats), proxy_stats_description + t);
    uint64_t result = field_read(&(gathered_stats.stats), proxy_stats_description + t);
    fail_unless(result == t1 + t2, "comparing stats field %s. 0x%llx = 0x%llx + 0x%llx\n", proxy_stats_description[t].name, result, t1, t2);
  }

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
  reset_redirections();
  start_main("moxi", "-t", "2", NULL);
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
    tcase_add_test(tc_core, test_cmd_stats_gathering);
    suite_add_tcase(s, tc_core);

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

uint64_t random_data[128] = {
18367347275435208884ULL, 10959197861943083324ULL, 5593119765411066088ULL, 4998348243644656542ULL,
11068269067176603708ULL, 8419710415668082894ULL, 3240935944006201405ULL, 1580228941575989076ULL,
9006217828825178350ULL, 669564684895417162ULL, 354310505148969833ULL, 2645830147185654428ULL,
4730926621859849203ULL, 3242251988690320054ULL, 9322070514859843369ULL, 16278831664244567021ULL,
3175932778682245111ULL, 2422238717987084569ULL, 52395684291027216ULL, 17178111804907915419ULL,
10735915590377526318ULL, 6762478721045361317ULL, 6974108151888182493ULL, 17528230884729369430ULL,
8041421721698101292ULL, 17966405432064026617ULL, 3980223067037788201ULL, 16167756513312846519ULL,
5820344135954077289ULL, 4818726992198531935ULL, 732733016785015490ULL, 12592334866831876408ULL,
7953788800308466607ULL, 8231488701146323781ULL, 3296601363548593364ULL, 17697551152312742467ULL,
136354765212989388ULL, 11036754658923766216ULL, 14532432972711265503ULL, 11720141176592623289ULL,
10775550801264661690ULL, 18163113894789420292ULL, 13333853164550196973ULL, 10632596020820293136ULL,
15608261007636445221ULL, 6997023147497683615ULL, 4696711144948810845ULL, 11148783061946280804ULL,
4198623906072052219ULL, 11048701350061281255ULL, 12820888071801950721ULL, 17244135334337740496ULL,
4920774687906095645ULL, 8444593176534470608ULL, 16434926849713365058ULL, 5452537965481860955ULL,
1655571050150609916ULL, 12843781290740931168ULL, 18202039614885819919ULL, 2454709974280029882ULL,
14597885920038159810ULL, 3149997776720569196ULL, 10907208180501157763ULL, 14429600300689100223ULL,
7846042083381016964ULL, 8237290898662884561ULL, 9696587308409565965ULL, 13836562465070880634ULL,
10108242782711058839ULL, 10577158921254900780ULL, 12581875996396054966ULL, 14085312468471008805ULL,
4373827108356240835ULL, 12596455833815079631ULL, 13640064068737744222ULL, 16260386400994728580ULL,
14676892544167983624ULL, 2016295704617606590ULL, 11000474295677900325ULL, 8285859821512954310ULL,
15024764759979553345ULL, 14107464221366539629ULL, 4870860459217596678ULL, 2277978264019767085ULL,
15208598770002232831ULL, 12519106413844803924ULL, 4879776385185655369ULL, 7137624817796749303ULL,
4710937923560298886ULL, 16193205352474447193ULL, 17010444273461039271ULL, 1054332460065224177ULL,
17442714080475522681ULL, 15283889498906392133ULL, 13813107438256927473ULL, 2750159079465956708ULL,
4453241790128853440ULL, 708114464079631996ULL, 13521227042706181019ULL, 9808193613088147865ULL,
2634344935903482761ULL, 10007245549142065074ULL, 8861844060786643035ULL, 6567860029679971767ULL,
2436470465807720844ULL, 18343400544739747265ULL, 16365794457061690320ULL, 7884145563392828929ULL,
775486681338696388ULL, 4158116596161542604ULL, 10679217501586584762ULL, 17717674285123557123ULL,
13321713577568981718ULL, 1113088032053117753ULL, 4090384065989852896ULL, 7024931591782390422ULL,
1871421089561788173ULL, 4185748400252248121ULL, 17483305388519538047ULL, 5733680591335328899ULL,
4569108260170843566ULL, 16817067331817265478ULL, 3098739638339733853ULL, 3401885734399878741ULL,
2597014806989463088ULL, 9765373658980159059ULL, 2144014789673694375ULL, 15707482407652022313ULL
};
