/*
  Sample test application.
*/
#include "libmemcached/common.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include "server.h"
#include "clients/generator.h"
#include "clients/execute.h"

#ifndef INT64_MAX
#define INT64_MAX LONG_MAX
#endif
#ifndef INT32_MAX
#define INT32_MAX INT_MAX
#endif


#include "test.h"

#ifdef HAVE_LIBMEMCACHEDUTIL
#include <pthread.h>
#include "libmemcached/memcached_util.h"
#endif

#define GLOBAL_COUNT 10000
#define GLOBAL2_COUNT 100
#define SERVERS_TO_CREATE 5
static uint32_t global_count;

static pairs_st *global_pairs;
static char *global_keys[GLOBAL_COUNT];
static size_t global_keys_length[GLOBAL_COUNT];

static test_return  init_test(memcached_st *not_used __attribute__((unused)))
{
  memcached_st memc;

  (void)memcached_create(&memc);
  memcached_free(&memc);

  return 0;
}

static test_return  server_list_null_test(memcached_st *ptr __attribute__((unused)))
{
  memcached_server_st *server_list;
  memcached_return rc;

  server_list= memcached_server_list_append_with_weight(NULL, NULL, 0, 0, NULL);
  assert(server_list == NULL);

  server_list= memcached_server_list_append_with_weight(NULL, "localhost", 0, 0, NULL);
  assert(server_list == NULL);

  server_list= memcached_server_list_append_with_weight(NULL, NULL, 0, 0, &rc);
  assert(server_list == NULL);

  return 0;
}

#define TEST_PORT_COUNT 7
uint32_t test_ports[TEST_PORT_COUNT];

static memcached_return  server_display_function(memcached_st *ptr __attribute__((unused)), memcached_server_st *server, void *context)
{
  /* Do Nothing */
  uint32_t bigger= *((uint32_t *)(context));
  assert(bigger <= server->port);
  *((uint32_t *)(context))= server->port;

  return MEMCACHED_SUCCESS;
}

static test_return  server_sort_test(memcached_st *ptr __attribute__((unused)))
{
  uint32_t x;
  uint32_t bigger= 0; /* Prime the value for the assert in server_display_function */
  memcached_return rc;
  memcached_server_function callbacks[1];
  memcached_st *local_memc;

  local_memc= memcached_create(NULL);
  assert(local_memc);
  memcached_behavior_set(local_memc, MEMCACHED_BEHAVIOR_SORT_HOSTS, 1);

  for (x= 0; x < TEST_PORT_COUNT; x++)
  {
    test_ports[x]= random() % 64000;
    rc= memcached_server_add_with_weight(local_memc, "localhost", test_ports[x], 0);
    assert(local_memc->number_of_hosts == x + 1);
    assert(local_memc->hosts[0].count == x+1);
    assert(rc == MEMCACHED_SUCCESS);
  }

  callbacks[0]= server_display_function;
  memcached_server_cursor(local_memc, callbacks, (void *)&bigger,  1);


  memcached_free(local_memc);

  return 0;
}

static test_return  server_sort2_test(memcached_st *ptr __attribute__((unused)))
{
  uint32_t bigger= 0; /* Prime the value for the assert in server_display_function */
  memcached_return rc;
  memcached_server_function callbacks[1];
  memcached_st *local_memc;

  local_memc= memcached_create(NULL);
  assert(local_memc);
  rc= memcached_behavior_set(local_memc, MEMCACHED_BEHAVIOR_SORT_HOSTS, 1);
  assert(rc == MEMCACHED_SUCCESS);

  rc= memcached_server_add_with_weight(local_memc, "MEMCACHED_BEHAVIOR_SORT_HOSTS", 43043, 0);
  assert(rc == MEMCACHED_SUCCESS);
  assert(local_memc->hosts[0].port == 43043);

  rc= memcached_server_add_with_weight(local_memc, "MEMCACHED_BEHAVIOR_SORT_HOSTS", 43042, 0);
  assert(rc == MEMCACHED_SUCCESS);
  assert(local_memc->hosts[0].port == 43042);
  assert(local_memc->hosts[1].port == 43043);

  callbacks[0]= server_display_function;
  memcached_server_cursor(local_memc, callbacks, (void *)&bigger,  1);


  memcached_free(local_memc);

  return 0;
}

static memcached_return  server_display_unsort_function(memcached_st *ptr __attribute__((unused)), memcached_server_st *server, void *context)
{
  /* Do Nothing */
  uint32_t x= *((uint32_t *)(context));

  assert(test_ports[x] == server->port);
  *((uint32_t *)(context))= ++x;

  return MEMCACHED_SUCCESS;
}

static test_return  server_unsort_test(memcached_st *ptr __attribute__((unused)))
{
  uint32_t x;
  uint32_t counter= 0; /* Prime the value for the assert in server_display_function */
  uint32_t bigger= 0; /* Prime the value for the assert in server_display_function */
  memcached_return rc;
  memcached_server_function callbacks[1];
  memcached_st *local_memc;

  local_memc= memcached_create(NULL);
  assert(local_memc);

  for (x= 0; x < TEST_PORT_COUNT; x++)
  {
    test_ports[x]= random() % 64000;
    rc= memcached_server_add_with_weight(local_memc, "localhost", test_ports[x], 0);
    assert(local_memc->number_of_hosts == x+1);
    assert(local_memc->hosts[0].count == x+1);
    assert(rc == MEMCACHED_SUCCESS);
  }

  callbacks[0]= server_display_unsort_function;
  memcached_server_cursor(local_memc, callbacks, (void *)&counter,  1);

  /* Now we sort old data! */
  memcached_behavior_set(local_memc, MEMCACHED_BEHAVIOR_SORT_HOSTS, 1);
  callbacks[0]= server_display_function;
  memcached_server_cursor(local_memc, callbacks, (void *)&bigger,  1);


  memcached_free(local_memc);

  return 0;
}

static test_return  allocation_test(memcached_st *not_used __attribute__((unused)))
{
  memcached_st *memc;
  memc= memcached_create(NULL);
  assert(memc);
  memcached_free(memc);

  return 0;
}

static test_return  clone_test(memcached_st *memc)
{
  /* All null? */
  {
    memcached_st *clone;
    clone= memcached_clone(NULL, NULL);
    assert(clone);
    memcached_free(clone);
  }

  /* Can we init from null? */
  {
    memcached_st *clone;
    clone= memcached_clone(NULL, memc);
    assert(clone);

    assert(clone->call_free == memc->call_free);
    assert(clone->call_malloc == memc->call_malloc);
    assert(clone->call_realloc == memc->call_realloc);
    assert(clone->connect_timeout == memc->connect_timeout);
    assert(clone->delete_trigger == memc->delete_trigger);
    assert(clone->distribution == memc->distribution);
    assert(clone->flags == memc->flags);
    assert(clone->get_key_failure == memc->get_key_failure);
    assert(clone->hash == memc->hash);
    assert(clone->hash_continuum == memc->hash_continuum);
    assert(clone->io_bytes_watermark == memc->io_bytes_watermark);
    assert(clone->io_msg_watermark == memc->io_msg_watermark);
    assert(clone->io_key_prefetch == memc->io_key_prefetch);
    assert(clone->on_cleanup == memc->on_cleanup);
    assert(clone->on_clone == memc->on_clone);
    assert(clone->poll_timeout == memc->poll_timeout);
    assert(clone->rcv_timeout == memc->rcv_timeout);
    assert(clone->recv_size == memc->recv_size);
    assert(clone->retry_timeout == memc->retry_timeout);
    assert(clone->send_size == memc->send_size);
    assert(clone->server_failure_limit == memc->server_failure_limit);
    assert(clone->snd_timeout == memc->snd_timeout);
    assert(clone->user_data == memc->user_data);

    memcached_free(clone);
  }

  /* Can we init from struct? */
  {
    memcached_st declared_clone;
    memcached_st *clone;
    memset(&declared_clone, 0 , sizeof(memcached_st));
    clone= memcached_clone(&declared_clone, NULL);
    assert(clone);
    memcached_free(clone);
  }

  /* Can we init from struct? */
  {
    memcached_st declared_clone;
    memcached_st *clone;
    memset(&declared_clone, 0 , sizeof(memcached_st));
    clone= memcached_clone(&declared_clone, memc);
    assert(clone);
    memcached_free(clone);
  }

  return 0;
}

static test_return  connection_test(memcached_st *memc)
{
  memcached_return rc;

  rc= memcached_server_add_with_weight(memc, "localhost", 0, 0);
  assert(rc == MEMCACHED_SUCCESS);

  return 0;
}

static test_return  error_test(memcached_st *memc)
{
  memcached_return rc;

  for (rc= MEMCACHED_SUCCESS; rc < MEMCACHED_MAXIMUM_RETURN; rc++)
  {
    printf("Error %d -> %s\n", rc, memcached_strerror(memc, rc));
  }

  return 0;
}

static test_return  set_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "foo";
  char *value= "when we sanitize";

  rc= memcached_set(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);

  return 0;
}

static test_return  append_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "fig";
  char *value= "we";
  size_t value_length;
  uint32_t flags;

  rc= memcached_flush(memc, 0);
  assert(rc == MEMCACHED_SUCCESS);

  rc= memcached_set(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS);

  rc= memcached_append(memc, key, strlen(key),
                       " the", strlen(" the"),
                       (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS);

  rc= memcached_append(memc, key, strlen(key),
                       " people", strlen(" people"),
                       (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS);

  value= memcached_get(memc, key, strlen(key),
                       &value_length, &flags, &rc);
  assert(!memcmp(value, "we the people", strlen("we the people")));
  assert(strlen("we the people") == value_length);
  assert(rc == MEMCACHED_SUCCESS);
  free(value);

  return 0;
}

static test_return  append_binary_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "numbers";
  unsigned int *store_ptr;
  unsigned int store_list[] = { 23, 56, 499, 98, 32847, 0 };
  char *value;
  size_t value_length;
  uint32_t flags;
  unsigned int x;

  rc= memcached_flush(memc, 0);
  assert(rc == MEMCACHED_SUCCESS);

  rc= memcached_set(memc,
                    key, strlen(key),
                    NULL, 0,
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS);

  for (x= 0; store_list[x] ; x++)
  {
    rc= memcached_append(memc,
                         key, strlen(key),
                         (char *)&store_list[x], sizeof(unsigned int),
                         (time_t)0, (uint32_t)0);
    assert(rc == MEMCACHED_SUCCESS);
  }

  value= memcached_get(memc, key, strlen(key),
                       &value_length, &flags, &rc);
  assert((value_length == (sizeof(unsigned int) * x)));
  assert(rc == MEMCACHED_SUCCESS);

  store_ptr= (unsigned int *)value;
  x= 0;
  while ((size_t)store_ptr < (size_t)(value + value_length))
  {
    assert(*store_ptr == store_list[x++]);
    store_ptr++;
  }
  free(value);

  return 0;
}

static test_return  cas2_test(memcached_st *memc)
{
  memcached_return rc;
  char *keys[]= {"fudge", "son", "food"};
  size_t key_length[]= {5, 3, 4};
  char *value= "we the people";
  size_t value_length= strlen("we the people");
  unsigned int x;
  memcached_result_st results_obj;
  memcached_result_st *results;
  unsigned int set= 1;

  rc= memcached_flush(memc, 0);
  assert(rc == MEMCACHED_SUCCESS);

  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_SUPPORT_CAS, set);

  for (x= 0; x < 3; x++)
  {
    rc= memcached_set(memc, keys[x], key_length[x],
                      keys[x], key_length[x],
                      (time_t)50, (uint32_t)9);
    assert(rc == MEMCACHED_SUCCESS);
  }

  rc= memcached_mget(memc, keys, key_length, 3);

  results= memcached_result_create(memc, &results_obj);

  results= memcached_fetch_result(memc, &results_obj, &rc);
  assert(results);
  assert(results->cas);
  assert(rc == MEMCACHED_SUCCESS);
  WATCHPOINT_ASSERT(memcached_result_cas(results));

  assert(!memcmp(value, "we the people", strlen("we the people")));
  assert(strlen("we the people") == value_length);
  assert(rc == MEMCACHED_SUCCESS);

  memcached_result_free(&results_obj);

  return 0;
}

static test_return  cas_test(memcached_st *memc)
{
  memcached_return rc;
  const char *key= "fun";
  size_t key_length= strlen(key);
  const char *value= "we the people";
  char* keys[2] = { (char*)key, NULL };
  size_t keylengths[2] = { strlen(key), 0 };
  size_t value_length= strlen(value);
  const char *value2= "change the value";
  size_t value2_length= strlen(value2);

  memcached_result_st results_obj;
  memcached_result_st *results;
  unsigned int set= 1;

  rc= memcached_flush(memc, 0);
  assert(rc == MEMCACHED_SUCCESS);

  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_SUPPORT_CAS, set);

  rc= memcached_set(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS);

  rc= memcached_mget(memc, keys, keylengths, 1);

  results= memcached_result_create(memc, &results_obj);

  results= memcached_fetch_result(memc, &results_obj, &rc);
  assert(results);
  assert(rc == MEMCACHED_SUCCESS);
  WATCHPOINT_ASSERT(memcached_result_cas(results));
  assert(!memcmp(value, memcached_result_value(results), value_length));
  assert(strlen(memcached_result_value(results)) == value_length);
  assert(rc == MEMCACHED_SUCCESS);
  uint64_t cas = memcached_result_cas(results);

  #if 0
  results= memcached_fetch_result(memc, &results_obj, &rc);
  assert(rc == MEMCACHED_END);
  assert(results == NULL);
#endif

  rc= memcached_cas(memc, key, key_length, value2, value2_length, 0, 0, cas);
  assert(rc == MEMCACHED_SUCCESS);

  /*
   * The item will have a new cas value, so try to set it again with the old
   * value. This should fail!
   */
  rc= memcached_cas(memc, key, key_length, value2, value2_length, 0, 0, cas);
  assert(rc == MEMCACHED_DATA_EXISTS);

  memcached_result_free(&results_obj);

  return 0;
}

static test_return  prepend_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "fig";
  char *value= "people";
  size_t value_length;
  uint32_t flags;

  rc= memcached_flush(memc, 0);
  assert(rc == MEMCACHED_SUCCESS);

  rc= memcached_set(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS);

  rc= memcached_prepend(memc, key, strlen(key),
                       "the ", strlen("the "),
                       (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS);

  rc= memcached_prepend(memc, key, strlen(key),
                       "we ", strlen("we "),
                       (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS);

  value= memcached_get(memc, key, strlen(key),
                       &value_length, &flags, &rc);
  assert(!memcmp(value, "we the people", strlen("we the people")));
  assert(strlen("we the people") == value_length);
  assert(rc == MEMCACHED_SUCCESS);
  free(value);

  return 0;
}

/*
  Set the value, then quit to make sure it is flushed.
  Come back in and test that add fails.
*/
static test_return  add_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "foo";
  char *value= "when we sanitize";
  unsigned long long setting_value;

  setting_value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_NO_BLOCK);

  rc= memcached_set(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);
  memcached_quit(memc);
  rc= memcached_add(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);

  /* Too many broken OS'es have broken loopback in async, so we can't be sure of the result */
  if (setting_value)
    assert(rc == MEMCACHED_NOTSTORED || rc == MEMCACHED_STORED);
  else
    assert(rc == MEMCACHED_NOTSTORED || rc == MEMCACHED_DATA_EXISTS);

  return 0;
}

/*
** There was a problem of leaking filedescriptors in the initial release
** of MacOSX 10.5. This test case triggers the problem. On some Solaris
** systems it seems that the kernel is slow on reclaiming the resources
** because the connects starts to time out (the test doesn't do much
** anyway, so just loop 10 iterations)
*/
static test_return  add_wrapper(memcached_st *memc)
{
  unsigned int x;
  unsigned int max= 10000;
#ifdef __sun
  max= 10;
#endif

  for (x= 0; x < max; x++)
    add_test(memc);

  return 0;
}

static test_return  replace_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "foo";
  char *value= "when we sanitize";
  char *original= "first we insert some data";

  rc= memcached_set(memc, key, strlen(key),
                    original, strlen(original),
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);

  rc= memcached_replace(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS);

  return 0;
}

static test_return  delete_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "foo";
  char *value= "when we sanitize";

  rc= memcached_set(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);

  rc= memcached_delete(memc, key, strlen(key), (time_t)0);
  assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);

  return 0;
}

static test_return  flush_test(memcached_st *memc)
{
  memcached_return rc;

  rc= memcached_flush(memc, 0);
  assert(rc == MEMCACHED_SUCCESS);

  return 0;
}

static memcached_return  server_function(memcached_st *ptr __attribute__((unused)),
                                         memcached_server_st *server __attribute__((unused)),
                                         void *context __attribute__((unused)))
{
  /* Do Nothing */

  return MEMCACHED_SUCCESS;
}

static test_return  memcached_server_cursor_test(memcached_st *memc)
{
  char *context= "foo bad";
  memcached_server_function callbacks[1];

  callbacks[0]= server_function;
  memcached_server_cursor(memc, callbacks, context,  1);

  return 0;
}

static test_return  bad_key_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "foo bad";
  char *string;
  size_t string_length;
  uint32_t flags;
  memcached_st *clone;
  unsigned int set= 1;
  size_t max_keylen= 0xffff;

  clone= memcached_clone(NULL, memc);
  assert(clone);

  rc= memcached_behavior_set(clone, MEMCACHED_BEHAVIOR_VERIFY_KEY, set);
  assert(rc == MEMCACHED_SUCCESS);

  /* All keys are valid in the binary protocol (except for length) */
  if (memcached_behavior_get(clone, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL) == 0)
  {
    string= memcached_get(clone, key, strlen(key),
                          &string_length, &flags, &rc);
    assert(rc == MEMCACHED_BAD_KEY_PROVIDED);
    assert(string_length ==  0);
    assert(!string);

    set= 0;
    rc= memcached_behavior_set(clone, MEMCACHED_BEHAVIOR_VERIFY_KEY, set);
    assert(rc == MEMCACHED_SUCCESS);
    string= memcached_get(clone, key, strlen(key),
                          &string_length, &flags, &rc);
    assert(rc == MEMCACHED_NOTFOUND);
    assert(string_length ==  0);
    assert(!string);

    /* Test multi key for bad keys */
    char *keys[] = { "GoodKey", "Bad Key", "NotMine" };
    size_t key_lengths[] = { 7, 7, 7 };
    set= 1;
    rc= memcached_behavior_set(clone, MEMCACHED_BEHAVIOR_VERIFY_KEY, set);
    assert(rc == MEMCACHED_SUCCESS);

    rc= memcached_mget(clone, keys, key_lengths, 3);
    assert(rc == MEMCACHED_BAD_KEY_PROVIDED);

    rc= memcached_mget_by_key(clone, "foo daddy", 9, keys, key_lengths, 1);
    assert(rc == MEMCACHED_BAD_KEY_PROVIDED);

    max_keylen= 250;

    /* The following test should be moved to the end of this function when the
       memcached server is updated to allow max size length of the keys in the
       binary protocol
    */
    rc= memcached_callback_set(clone, MEMCACHED_CALLBACK_PREFIX_KEY, NULL);
    assert(rc == MEMCACHED_SUCCESS);

    char *longkey= malloc(max_keylen + 1);
    if (longkey != NULL)
    {
      memset(longkey, 'a', max_keylen + 1);
      string= memcached_get(clone, longkey, max_keylen,
                            &string_length, &flags, &rc);
      assert(rc == MEMCACHED_NOTFOUND);
      assert(string_length ==  0);
      assert(!string);

      string= memcached_get(clone, longkey, max_keylen + 1,
                            &string_length, &flags, &rc);
      assert(rc == MEMCACHED_BAD_KEY_PROVIDED);
      assert(string_length ==  0);
      assert(!string);

      free(longkey);
    }
  }

  /* Make sure zero length keys are marked as bad */
  set= 1;
  rc= memcached_behavior_set(clone, MEMCACHED_BEHAVIOR_VERIFY_KEY, set);
  assert(rc == MEMCACHED_SUCCESS);
  string= memcached_get(clone, key, 0,
                        &string_length, &flags, &rc);
  assert(rc == MEMCACHED_BAD_KEY_PROVIDED);
  assert(string_length ==  0);
  assert(!string);

  memcached_free(clone);

  return 0;
}

#define READ_THROUGH_VALUE "set for me"
static memcached_return  read_through_trigger(memcached_st *memc __attribute__((unused)),
                                      char *key __attribute__((unused)),
                                      size_t key_length __attribute__((unused)),
                                      memcached_result_st *result)
{

  return memcached_result_set_value(result, READ_THROUGH_VALUE, strlen(READ_THROUGH_VALUE));
}

static test_return  read_through(memcached_st *memc)
{
  memcached_return rc;
  char *key= "foo";
  char *string;
  size_t string_length;
  uint32_t flags;
  memcached_trigger_key cb= (memcached_trigger_key)read_through_trigger;

  string= memcached_get(memc, key, strlen(key),
                        &string_length, &flags, &rc);

  assert(rc == MEMCACHED_NOTFOUND);
  assert(string_length ==  0);
  assert(!string);

  rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_GET_FAILURE,
                             *(void **)&cb);
  assert(rc == MEMCACHED_SUCCESS);

  string= memcached_get(memc, key, strlen(key),
                        &string_length, &flags, &rc);

  assert(rc == MEMCACHED_SUCCESS);
  assert(string_length ==  strlen(READ_THROUGH_VALUE));
  assert(!strcmp(READ_THROUGH_VALUE, string));
  free(string);

  string= memcached_get(memc, key, strlen(key),
                        &string_length, &flags, &rc);

  assert(rc == MEMCACHED_SUCCESS);
  assert(string_length ==  strlen(READ_THROUGH_VALUE));
  assert(!strcmp(READ_THROUGH_VALUE, string));
  free(string);

  return 0;
}

static memcached_return  delete_trigger(memcached_st *ptr __attribute__((unused)),
                                        const char *key,
                                        size_t key_length __attribute__((unused)))
{
  assert(key);

  return MEMCACHED_SUCCESS;
}

static test_return  delete_through(memcached_st *memc)
{
  memcached_trigger_delete_key callback;
  memcached_return rc;

  callback= (memcached_trigger_delete_key)delete_trigger;

  rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_DELETE_TRIGGER, *(void**)&callback);
  assert(rc == MEMCACHED_SUCCESS);

  return 0;
}

static test_return  get_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "foo";
  char *string;
  size_t string_length;
  uint32_t flags;

  rc= memcached_delete(memc, key, strlen(key), (time_t)0);
  assert(rc == MEMCACHED_BUFFERED || rc == MEMCACHED_NOTFOUND);

  string= memcached_get(memc, key, strlen(key),
                        &string_length, &flags, &rc);

  assert(rc == MEMCACHED_NOTFOUND);
  assert(string_length ==  0);
  assert(!string);

  return 0;
}

static test_return  get_test2(memcached_st *memc)
{
  memcached_return rc;
  char *key= "foo";
  char *value= "when we sanitize";
  char *string;
  size_t string_length;
  uint32_t flags;

  rc= memcached_set(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);

  string= memcached_get(memc, key, strlen(key),
                        &string_length, &flags, &rc);

  assert(string);
  assert(rc == MEMCACHED_SUCCESS);
  assert(string_length == strlen(value));
  assert(!memcmp(string, value, string_length));

  free(string);

  return 0;
}

static test_return  set_test2(memcached_st *memc)
{
  memcached_return rc;
  char *key= "foo";
  char *value= "train in the brain";
  size_t value_length= strlen(value);
  unsigned int x;

  for (x= 0; x < 10; x++)
  {
    rc= memcached_set(memc, key, strlen(key),
                      value, value_length,
                      (time_t)0, (uint32_t)0);
    assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);
  }

  return 0;
}

static test_return  set_test3(memcached_st *memc)
{
  memcached_return rc;
  char *value;
  size_t value_length= 8191;
  unsigned int x;

  value = (char*)malloc(value_length);
  assert(value);

  for (x= 0; x < value_length; x++)
    value[x] = (char) (x % 127);

  /* The dump test relies on there being at least 32 items in memcached */
  for (x= 0; x < 32; x++)
  {
    char key[16];

    sprintf(key, "foo%u", x);

    rc= memcached_set(memc, key, strlen(key),
                      value, value_length,
                      (time_t)0, (uint32_t)0);
    assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);
  }

  free(value);

  return 0;
}

static test_return  get_test3(memcached_st *memc)
{
  memcached_return rc;
  char *key= "foo";
  char *value;
  size_t value_length= 8191;
  char *string;
  size_t string_length;
  uint32_t flags;
  uint32_t x;

  value = (char*)malloc(value_length);
  assert(value);

  for (x= 0; x < value_length; x++)
    value[x] = (char) (x % 127);

  rc= memcached_set(memc, key, strlen(key),
                    value, value_length,
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);

  string= memcached_get(memc, key, strlen(key),
                        &string_length, &flags, &rc);

  assert(rc == MEMCACHED_SUCCESS);
  assert(string);
  assert(string_length == value_length);
  assert(!memcmp(string, value, string_length));

  free(string);
  free(value);

  return 0;
}

static test_return  get_test4(memcached_st *memc)
{
  memcached_return rc;
  char *key= "foo";
  char *value;
  size_t value_length= 8191;
  char *string;
  size_t string_length;
  uint32_t flags;
  uint32_t x;

  value = (char*)malloc(value_length);
  assert(value);

  for (x= 0; x < value_length; x++)
    value[x] = (char) (x % 127);

  rc= memcached_set(memc, key, strlen(key),
                    value, value_length,
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);

  for (x= 0; x < 10; x++)
  {
    string= memcached_get(memc, key, strlen(key),
                          &string_length, &flags, &rc);

    assert(rc == MEMCACHED_SUCCESS);
    assert(string);
    assert(string_length == value_length);
    assert(!memcmp(string, value, string_length));
    free(string);
  }

  free(value);

  return 0;
}

/*
 * This test verifies that memcached_read_one_response doesn't try to
 * dereference a NIL-pointer if you issue a multi-get and don't read out all
 * responses before you execute a storage command.
 */
static test_return get_test5(memcached_st *memc)
{
  /*
  ** Request the same key twice, to ensure that we hash to the same server
  ** (so that we have multiple response values queued up) ;-)
  */
  char *keys[]= { "key", "key" };
  size_t lengths[]= { 3, 3 };
  uint32_t flags;
  size_t rlen;

  memcached_return rc= memcached_set(memc, keys[0], lengths[0],
                                     keys[0], lengths[0], 0, 0);
  assert(rc == MEMCACHED_SUCCESS);
  rc= memcached_mget(memc, keys, lengths, 2);

  memcached_result_st results_obj;
  memcached_result_st *results;
  results=memcached_result_create(memc, &results_obj);
  assert(results);
  results=memcached_fetch_result(memc, &results_obj, &rc);
  assert(results);
  memcached_result_free(&results_obj);

  /* Don't read out the second result, but issue a set instead.. */
  rc= memcached_set(memc, keys[0], lengths[0], keys[0], lengths[0], 0, 0);
  assert(rc == MEMCACHED_SUCCESS);

  char *val= memcached_get_by_key(memc, keys[0], lengths[0], "yek", 3,
                                  &rlen, &flags, &rc);
  assert(val == NULL);
  assert(rc == MEMCACHED_NOTFOUND);
  val= memcached_get(memc, keys[0], lengths[0], &rlen, &flags, &rc);
  assert(val != NULL);
  assert(rc == MEMCACHED_SUCCESS);
  free(val);

  return TEST_SUCCESS;
}

/* Do not copy the style of this code, I just access hosts to testthis function */
static test_return  stats_servername_test(memcached_st *memc)
{
  memcached_return rc;
  memcached_stat_st stat;
  rc= memcached_stat_servername(&stat, NULL,
                                 memc->hosts[0].hostname,
                                 memc->hosts[0].port);

  return 0;
}

static test_return  increment_test(memcached_st *memc)
{
  uint64_t new_number;
  memcached_return rc;
  char *key= "number";
  char *value= "0";

  rc= memcached_set(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);

  rc= memcached_increment(memc, key, strlen(key),
                          1, &new_number);
  assert(rc == MEMCACHED_SUCCESS);
  assert(new_number == 1);

  rc= memcached_increment(memc, key, strlen(key),
                          1, &new_number);
  assert(rc == MEMCACHED_SUCCESS);
  assert(new_number == 2);

  return 0;
}

static test_return  increment_with_initial_test(memcached_st *memc)
{
  if (memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL) != 0)
  {
    uint64_t new_number;
    memcached_return rc;
    char *key= "number";
    uint64_t initial= 0;

    rc= memcached_increment_with_initial(memc, key, strlen(key),
                                         1, initial, 0, &new_number);
    assert(rc == MEMCACHED_SUCCESS);
    assert(new_number == initial);

    rc= memcached_increment_with_initial(memc, key, strlen(key),
                                         1, initial, 0, &new_number);
    assert(rc == MEMCACHED_SUCCESS);
    assert(new_number == (initial + 1));
  }
  return 0;
}

static test_return  decrement_test(memcached_st *memc)
{
  uint64_t new_number;
  memcached_return rc;
  char *key= "number";
  char *value= "3";

  rc= memcached_set(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);

  rc= memcached_decrement(memc, key, strlen(key),
                          1, &new_number);
  assert(rc == MEMCACHED_SUCCESS);
  assert(new_number == 2);

  rc= memcached_decrement(memc, key, strlen(key),
                          1, &new_number);
  assert(rc == MEMCACHED_SUCCESS);
  assert(new_number == 1);

  return 0;
}

static test_return  decrement_with_initial_test(memcached_st *memc)
{
  if (memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL) != 0)
  {
    uint64_t new_number;
    memcached_return rc;
    char *key= "number";
    uint64_t initial= 3;

    rc= memcached_decrement_with_initial(memc, key, strlen(key),
                                         1, initial, 0, &new_number);
    assert(rc == MEMCACHED_SUCCESS);
    assert(new_number == initial);

    rc= memcached_decrement_with_initial(memc, key, strlen(key),
                                         1, initial, 0, &new_number);
    assert(rc == MEMCACHED_SUCCESS);
    assert(new_number == (initial - 1));
  }
  return 0;
}

static test_return  quit_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "fudge";
  char *value= "sanford and sun";

  rc= memcached_set(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)10, (uint32_t)3);
  assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);
  memcached_quit(memc);

  rc= memcached_set(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)50, (uint32_t)9);
  assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);

  return 0;
}

static test_return  mget_result_test(memcached_st *memc)
{
  memcached_return rc;
  char *keys[]= {"fudge", "son", "food"};
  size_t key_length[]= {5, 3, 4};
  unsigned int x;

  memcached_result_st results_obj;
  memcached_result_st *results;

  results= memcached_result_create(memc, &results_obj);
  assert(results);
  assert(&results_obj == results);

  /* We need to empty the server before continueing test */
  rc= memcached_flush(memc, 0);
  assert(rc == MEMCACHED_SUCCESS);

  rc= memcached_mget(memc, keys, key_length, 3);
  assert(rc == MEMCACHED_SUCCESS);

  while ((results= memcached_fetch_result(memc, &results_obj, &rc)) != NULL)
  {
    assert(results);
  }

  while ((results= memcached_fetch_result(memc, &results_obj, &rc)) != NULL)
  assert(!results);
  assert(rc == MEMCACHED_END);

  for (x= 0; x < 3; x++)
  {
    rc= memcached_set(memc, keys[x], key_length[x],
                      keys[x], key_length[x],
                      (time_t)50, (uint32_t)9);
    assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);
  }

  rc= memcached_mget(memc, keys, key_length, 3);
  assert(rc == MEMCACHED_SUCCESS);

  while ((results= memcached_fetch_result(memc, &results_obj, &rc)))
  {
    assert(results);
    assert(&results_obj == results);
    assert(rc == MEMCACHED_SUCCESS);
    assert(memcached_result_key_length(results) == memcached_result_length(results));
    assert(!memcmp(memcached_result_key_value(results),
                   memcached_result_value(results),
                   memcached_result_length(results)));
  }

  memcached_result_free(&results_obj);

  return 0;
}

static test_return  mget_result_alloc_test(memcached_st *memc)
{
  memcached_return rc;
  char *keys[]= {"fudge", "son", "food"};
  size_t key_length[]= {5, 3, 4};
  unsigned int x;

  memcached_result_st *results;

  /* We need to empty the server before continueing test */
  rc= memcached_flush(memc, 0);
  assert(rc == MEMCACHED_SUCCESS);

  rc= memcached_mget(memc, keys, key_length, 3);
  assert(rc == MEMCACHED_SUCCESS);

  while ((results= memcached_fetch_result(memc, NULL, &rc)) != NULL)
  {
    assert(results);
  }
  assert(!results);
  assert(rc == MEMCACHED_END);

  for (x= 0; x < 3; x++)
  {
    rc= memcached_set(memc, keys[x], key_length[x],
                      keys[x], key_length[x],
                      (time_t)50, (uint32_t)9);
    assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);
  }

  rc= memcached_mget(memc, keys, key_length, 3);
  assert(rc == MEMCACHED_SUCCESS);

  x= 0;
  while ((results= memcached_fetch_result(memc, NULL, &rc)))
  {
    assert(results);
    assert(rc == MEMCACHED_SUCCESS);
    assert(memcached_result_key_length(results) == memcached_result_length(results));
    assert(!memcmp(memcached_result_key_value(results),
                   memcached_result_value(results),
                   memcached_result_length(results)));
    memcached_result_free(results);
    x++;
  }

  return 0;
}

/* Count the results */
static memcached_return callback_counter(memcached_st *ptr __attribute__((unused)),
                                     memcached_result_st *result __attribute__((unused)),
                                     void *context)
{
  unsigned int *counter= (unsigned int *)context;

  *counter= *counter + 1;

  return MEMCACHED_SUCCESS;
}

static test_return  mget_result_function(memcached_st *memc)
{
  memcached_return rc;
  char *keys[]= {"fudge", "son", "food"};
  size_t key_length[]= {5, 3, 4};
  unsigned int x;
  unsigned int counter;
  memcached_execute_function callbacks[1];

  /* We need to empty the server before continueing test */
  rc= memcached_flush(memc, 0);
  for (x= 0; x < 3; x++)
  {
    rc= memcached_set(memc, keys[x], key_length[x],
                      keys[x], key_length[x],
                      (time_t)50, (uint32_t)9);
    assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);
  }

  rc= memcached_mget(memc, keys, key_length, 3);
  assert(rc == MEMCACHED_SUCCESS);

  callbacks[0]= &callback_counter;
  counter= 0;
  rc= memcached_fetch_execute(memc, callbacks, (void *)&counter, 1);

  assert(counter == 3);

  return 0;
}

static test_return  mget_test(memcached_st *memc)
{
  memcached_return rc;
  char *keys[]= {"fudge", "son", "food"};
  size_t key_length[]= {5, 3, 4};
  unsigned int x;
  uint32_t flags;

  char return_key[MEMCACHED_MAX_KEY];
  size_t return_key_length;
  char *return_value;
  size_t return_value_length;

  /* We need to empty the server before continueing test */
  rc= memcached_flush(memc, 0);
  assert(rc == MEMCACHED_SUCCESS);

  rc= memcached_mget(memc, keys, key_length, 3);
  assert(rc == MEMCACHED_SUCCESS);

  while ((return_value= memcached_fetch(memc, return_key, &return_key_length,
                      &return_value_length, &flags, &rc)) != NULL)
  {
    assert(return_value);
  }
  assert(!return_value);
  assert(return_value_length == 0);
  assert(rc == MEMCACHED_END);

  for (x= 0; x < 3; x++)
  {
    rc= memcached_set(memc, keys[x], key_length[x],
                      keys[x], key_length[x],
                      (time_t)50, (uint32_t)9);
    assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);
  }

  rc= memcached_mget(memc, keys, key_length, 3);
  assert(rc == MEMCACHED_SUCCESS);

  x= 0;
  while ((return_value= memcached_fetch(memc, return_key, &return_key_length,
                                        &return_value_length, &flags, &rc)))
  {
    assert(return_value);
    assert(rc == MEMCACHED_SUCCESS);
    assert(return_key_length == return_value_length);
    assert(!memcmp(return_value, return_key, return_value_length));
    free(return_value);
    x++;
  }

  return 0;
}

static test_return  get_stats_keys(memcached_st *memc)
{
 char **list;
 char **ptr;
 memcached_stat_st stat;
 memcached_return rc;

 list= memcached_stat_get_keys(memc, &stat, &rc);
 assert(rc == MEMCACHED_SUCCESS);
 for (ptr= list; *ptr; ptr++)
   assert(*ptr);
 fflush(stdout);

 free(list);

 return 0;
}

static test_return  version_string_test(memcached_st *memc __attribute__((unused)))
{
  const char *version_string;

  version_string= memcached_lib_version();

  assert(!strcmp(version_string, LIBMEMCACHED_VERSION_STRING));

  return 0;
}

static test_return  get_stats(memcached_st *memc)
{
 unsigned int x;
 char **list;
 char **ptr;
 memcached_return rc;
 memcached_stat_st *stat;

 stat= memcached_stat(memc, NULL, &rc);
 assert(rc == MEMCACHED_SUCCESS);

 assert(rc == MEMCACHED_SUCCESS);
 assert(stat);

 for (x= 0; x < memcached_server_count(memc); x++)
 {
   list= memcached_stat_get_keys(memc, stat+x, &rc);
   assert(rc == MEMCACHED_SUCCESS);
   for (ptr= list; *ptr; ptr++);

   free(list);
 }

 memcached_stat_free(NULL, stat);

  return 0;
}

static test_return  add_host_test(memcached_st *memc)
{
  unsigned int x;
  memcached_server_st *servers;
  memcached_return rc;
  char servername[]= "0.example.com";

  servers= memcached_server_list_append_with_weight(NULL, servername, 400, 0, &rc);
  assert(servers);
  assert(1 == memcached_server_list_count(servers));

  for (x= 2; x < 20; x++)
  {
    char buffer[SMALL_STRING_LEN];

    snprintf(buffer, SMALL_STRING_LEN, "%u.example.com", 400+x);
    servers= memcached_server_list_append_with_weight(servers, buffer, 401, 0,
                                     &rc);
    assert(rc == MEMCACHED_SUCCESS);
    assert(x == memcached_server_list_count(servers));
  }

  rc= memcached_server_push(memc, servers);
  assert(rc == MEMCACHED_SUCCESS);
  rc= memcached_server_push(memc, servers);
  assert(rc == MEMCACHED_SUCCESS);

  memcached_server_list_free(servers);

  return 0;
}

static memcached_return  clone_test_callback(memcached_st *parent __attribute__((unused)), memcached_st *clone __attribute__((unused)))
{
  return MEMCACHED_SUCCESS;
}

static memcached_return  cleanup_test_callback(memcached_st *ptr __attribute__((unused)))
{
  return MEMCACHED_SUCCESS;
}

static test_return  callback_test(memcached_st *memc)
{
  /* Test User Data */
  {
    int x= 5;
    int *test_ptr;
    memcached_return rc;

    rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_USER_DATA, &x);
    assert(rc == MEMCACHED_SUCCESS);
    test_ptr= (int *)memcached_callback_get(memc, MEMCACHED_CALLBACK_USER_DATA, &rc);
    assert(*test_ptr == x);
  }

  /* Test Clone Callback */
  {
    memcached_clone_func clone_cb= (memcached_clone_func)clone_test_callback;
    void *clone_cb_ptr= *(void **)&clone_cb;
    void *temp_function= NULL;
    memcached_return rc;

    rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_CLONE_FUNCTION,
                               clone_cb_ptr);
    assert(rc == MEMCACHED_SUCCESS);
    temp_function= memcached_callback_get(memc, MEMCACHED_CALLBACK_CLONE_FUNCTION, &rc);
    assert(temp_function == clone_cb_ptr);
  }

  /* Test Cleanup Callback */
  {
    memcached_cleanup_func cleanup_cb=
      (memcached_cleanup_func)cleanup_test_callback;
    void *cleanup_cb_ptr= *(void **)&cleanup_cb;
    void *temp_function= NULL;
    memcached_return rc;

    rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_CLONE_FUNCTION,
                               cleanup_cb_ptr);
    assert(rc == MEMCACHED_SUCCESS);
    temp_function= memcached_callback_get(memc, MEMCACHED_CALLBACK_CLONE_FUNCTION, &rc);
    assert(temp_function == cleanup_cb_ptr);
  }

  return 0;
}

/* We don't test the behavior itself, we test the switches */
static test_return  behavior_test(memcached_st *memc)
{
  uint64_t value;
  uint32_t set= 1;

  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NO_BLOCK, set);
  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_NO_BLOCK);
  assert(value == 1);

  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_TCP_NODELAY, set);
  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_TCP_NODELAY);
  assert(value == 1);

  set= MEMCACHED_HASH_MD5;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_HASH, set);
  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_HASH);
  assert(value == MEMCACHED_HASH_MD5);

  set= 0;

  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NO_BLOCK, set);
  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_NO_BLOCK);
  assert(value == 0);

  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_TCP_NODELAY, set);
  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_TCP_NODELAY);
  assert(value == 0);

  set= MEMCACHED_HASH_DEFAULT;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_HASH, set);
  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_HASH);
  assert(value == MEMCACHED_HASH_DEFAULT);

  set= MEMCACHED_HASH_CRC;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_HASH, set);
  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_HASH);
  assert(value == MEMCACHED_HASH_CRC);

  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_SOCKET_SEND_SIZE);
  assert(value > 0);

  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_SOCKET_RECV_SIZE);
  assert(value > 0);

  return 0;
}

/* Test case provided by Cal Haldenbrand */
static test_return  user_supplied_bug1(memcached_st *memc)
{
  unsigned int setter= 1;
  unsigned int x;

  unsigned long long total= 0;
  uint32_t size= 0;
  char key[10];
  char randomstuff[6 * 1024];
  memcached_return rc;

  memset(randomstuff, 0, 6 * 1024);

  /* We just keep looking at the same values over and over */
  srandom(10);

  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NO_BLOCK, setter);
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_TCP_NODELAY, setter);


  /* add key */
  for (x= 0 ; total < 20 * 1024576 ; x++ )
  {
    unsigned int j= 0;

    size= (rand() % ( 5 * 1024 ) ) + 400;
    memset(randomstuff, 0, 6 * 1024);
    assert(size < 6 * 1024); /* Being safe here */

    for (j= 0 ; j < size ;j++)
      randomstuff[j] = (char) (rand() % 26) + 97;

    total += size;
    sprintf(key, "%d", x);
    rc = memcached_set(memc, key, strlen(key),
                       randomstuff, strlen(randomstuff), 10, 0);
    assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);
    /* If we fail, lets try again */
    if (rc != MEMCACHED_SUCCESS && rc != MEMCACHED_BUFFERED)
      rc = memcached_set(memc, key, strlen(key),
                         randomstuff, strlen(randomstuff), 10, 0);
    assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);
  }

  return 0;
}

/* Test case provided by Cal Haldenbrand */
static test_return  user_supplied_bug2(memcached_st *memc)
{
  int errors;
  unsigned int setter;
  unsigned int x;
  unsigned long long total;

  setter= 1;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NO_BLOCK, setter);
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_TCP_NODELAY, setter);
#ifdef NOT_YET
  setter = 20 * 1024576;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_SOCKET_SEND_SIZE, setter);
  setter = 20 * 1024576;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_SOCKET_RECV_SIZE, setter);
  getter = memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_SOCKET_SEND_SIZE);
  getter = memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_SOCKET_RECV_SIZE);

  for (x= 0, errors= 0, total= 0 ; total < 20 * 1024576 ; x++)
#endif

  for (x= 0, errors= 0, total= 0 ; total < 24576 ; x++)
  {
    memcached_return rc= MEMCACHED_SUCCESS;
    char buffer[SMALL_STRING_LEN];
    uint32_t flags= 0;
    size_t val_len= 0;
    char *getval;

    memset(buffer, 0, SMALL_STRING_LEN);

    snprintf(buffer, SMALL_STRING_LEN, "%u", x);
    getval= memcached_get(memc, buffer, strlen(buffer),
                           &val_len, &flags, &rc);
    if (rc != MEMCACHED_SUCCESS)
    {
      if (rc == MEMCACHED_NOTFOUND)
        errors++;
      else
      {
        WATCHPOINT_ERROR(rc);
        assert(0);
      }

      continue;
    }
    total+= val_len;
    errors= 0;
    free(getval);
  }

  return 0;
}

/* Do a large mget() over all the keys we think exist */
#define KEY_COUNT 3000 // * 1024576
static test_return  user_supplied_bug3(memcached_st *memc)
{
  memcached_return rc;
  unsigned int setter;
  unsigned int x;
  char **keys;
  size_t key_lengths[KEY_COUNT];

  setter= 1;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NO_BLOCK, setter);
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_TCP_NODELAY, setter);
#ifdef NOT_YET
  setter = 20 * 1024576;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_SOCKET_SEND_SIZE, setter);
  setter = 20 * 1024576;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_SOCKET_RECV_SIZE, setter);
  getter = memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_SOCKET_SEND_SIZE);
  getter = memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_SOCKET_RECV_SIZE);
#endif

  keys= (char **)malloc(sizeof(char *) * KEY_COUNT);
  assert(keys);
  memset(keys, 0, (sizeof(char *) * KEY_COUNT));
  for (x= 0; x < KEY_COUNT; x++)
  {
    char buffer[30];

    snprintf(buffer, 30, "%u", x);
    keys[x]= strdup(buffer);
    key_lengths[x]= strlen(keys[x]);
  }

  rc= memcached_mget(memc, keys, key_lengths, KEY_COUNT);
  assert(rc == MEMCACHED_SUCCESS);

  /* Turn this into a help function */
  {
    char return_key[MEMCACHED_MAX_KEY];
    size_t return_key_length;
    char *return_value;
    size_t return_value_length;
    uint32_t flags;

    while ((return_value= memcached_fetch(memc, return_key, &return_key_length,
                                          &return_value_length, &flags, &rc)))
    {
      assert(return_value);
      assert(rc == MEMCACHED_SUCCESS);
      free(return_value);
    }
  }

  for (x= 0; x < KEY_COUNT; x++)
    free(keys[x]);
  free(keys);

  return 0;
}

/* Make sure we behave properly if server list has no values */
static test_return  user_supplied_bug4(memcached_st *memc)
{
  memcached_return rc;
  char *keys[]= {"fudge", "son", "food"};
  size_t key_length[]= {5, 3, 4};
  unsigned int x;
  uint32_t flags;
  char return_key[MEMCACHED_MAX_KEY];
  size_t return_key_length;
  char *return_value;
  size_t return_value_length;

  /* Here we free everything before running a bunch of mget tests */
  {
    memcached_server_list_free(memc->hosts);
    memc->hosts= NULL;
    memc->number_of_hosts= 0;
  }


  /* We need to empty the server before continueing test */
  rc= memcached_flush(memc, 0);
  assert(rc == MEMCACHED_NO_SERVERS);

  rc= memcached_mget(memc, keys, key_length, 3);
  assert(rc == MEMCACHED_NO_SERVERS);

  while ((return_value= memcached_fetch(memc, return_key, &return_key_length,
                      &return_value_length, &flags, &rc)) != NULL)
  {
    assert(return_value);
  }
  assert(!return_value);
  assert(return_value_length == 0);
  assert(rc == MEMCACHED_NO_SERVERS);

  for (x= 0; x < 3; x++)
  {
    rc= memcached_set(memc, keys[x], key_length[x],
                      keys[x], key_length[x],
                      (time_t)50, (uint32_t)9);
    assert(rc == MEMCACHED_NO_SERVERS);
  }

  rc= memcached_mget(memc, keys, key_length, 3);
  assert(rc == MEMCACHED_NO_SERVERS);

  x= 0;
  while ((return_value= memcached_fetch(memc, return_key, &return_key_length,
                                        &return_value_length, &flags, &rc)))
  {
    assert(return_value);
    assert(rc == MEMCACHED_SUCCESS);
    assert(return_key_length == return_value_length);
    assert(!memcmp(return_value, return_key, return_value_length));
    free(return_value);
    x++;
  }

  return 0;
}

#define VALUE_SIZE_BUG5 1048064
static test_return  user_supplied_bug5(memcached_st *memc)
{
  memcached_return rc;
  char *keys[]= {"036790384900", "036790384902", "036790384904", "036790384906"};
  size_t key_length[]=  {strlen("036790384900"), strlen("036790384902"), strlen("036790384904"), strlen("036790384906")};
  char return_key[MEMCACHED_MAX_KEY];
  size_t return_key_length;
  char *value;
  size_t value_length;
  uint32_t flags;
  unsigned int count;
  unsigned int x;
  char insert_data[VALUE_SIZE_BUG5];

  for (x= 0; x < VALUE_SIZE_BUG5; x++)
    insert_data[x]= rand();

  memcached_flush(memc, 0);
  value= memcached_get(memc, keys[0], key_length[0],
                        &value_length, &flags, &rc);
  assert(value == NULL);
  rc= memcached_mget(memc, keys, key_length, 4);

  count= 0;
  while ((value= memcached_fetch(memc, return_key, &return_key_length,
                                        &value_length, &flags, &rc)))
    count++;
  assert(count == 0);

  for (x= 0; x < 4; x++)
  {
    rc= memcached_set(memc, keys[x], key_length[x],
                      insert_data, VALUE_SIZE_BUG5,
                      (time_t)0, (uint32_t)0);
    assert(rc == MEMCACHED_SUCCESS);
  }

  for (x= 0; x < 10; x++)
  {
    value= memcached_get(memc, keys[0], key_length[0],
                         &value_length, &flags, &rc);
    assert(value);
    free(value);

    rc= memcached_mget(memc, keys, key_length, 4);
    count= 0;
    while ((value= memcached_fetch(memc, return_key, &return_key_length,
                                          &value_length, &flags, &rc)))
    {
      count++;
      free(value);
    }
    assert(count == 4);
  }

  return 0;
}

static test_return  user_supplied_bug6(memcached_st *memc)
{
  memcached_return rc;
  char *keys[]= {"036790384900", "036790384902", "036790384904", "036790384906"};
  size_t key_length[]=  {strlen("036790384900"), strlen("036790384902"), strlen("036790384904"), strlen("036790384906")};
  char return_key[MEMCACHED_MAX_KEY];
  size_t return_key_length;
  char *value;
  size_t value_length;
  uint32_t flags;
  unsigned int count;
  unsigned int x;
  char insert_data[VALUE_SIZE_BUG5];

  for (x= 0; x < VALUE_SIZE_BUG5; x++)
    insert_data[x]= rand();

  memcached_flush(memc, 0);
  value= memcached_get(memc, keys[0], key_length[0],
                        &value_length, &flags, &rc);
  assert(value == NULL);
  assert(rc == MEMCACHED_NOTFOUND);
  rc= memcached_mget(memc, keys, key_length, 4);
  assert(rc == MEMCACHED_SUCCESS);

  count= 0;
  while ((value= memcached_fetch(memc, return_key, &return_key_length,
                                        &value_length, &flags, &rc)))
    count++;
  assert(count == 0);
  assert(rc == MEMCACHED_END);

  for (x= 0; x < 4; x++)
  {
    rc= memcached_set(memc, keys[x], key_length[x],
                      insert_data, VALUE_SIZE_BUG5,
                      (time_t)0, (uint32_t)0);
    assert(rc == MEMCACHED_SUCCESS);
  }

  for (x= 0; x < 2; x++)
  {
    value= memcached_get(memc, keys[0], key_length[0],
                         &value_length, &flags, &rc);
    assert(value);
    free(value);

    rc= memcached_mget(memc, keys, key_length, 4);
    assert(rc == MEMCACHED_SUCCESS);
    count= 3;
    /* We test for purge of partial complete fetches */
    for (count= 3; count; count--)
    {
      value= memcached_fetch(memc, return_key, &return_key_length,
                             &value_length, &flags, &rc);
      assert(rc == MEMCACHED_SUCCESS);
      assert(!(memcmp(value, insert_data, value_length)));
      assert(value_length);
      free(value);
    }
  }

  return 0;
}

static test_return  user_supplied_bug8(memcached_st *memc __attribute__((unused)))
{
  memcached_return rc;
  memcached_st *mine;
  memcached_st *clone;

  memcached_server_st *servers;
  char *server_list= "memcache1.memcache.bk.sapo.pt:11211, memcache1.memcache.bk.sapo.pt:11212, memcache1.memcache.bk.sapo.pt:11213, memcache1.memcache.bk.sapo.pt:11214, memcache2.memcache.bk.sapo.pt:11211, memcache2.memcache.bk.sapo.pt:11212, memcache2.memcache.bk.sapo.pt:11213, memcache2.memcache.bk.sapo.pt:11214";

  servers= memcached_servers_parse(server_list);
  assert(servers);

  mine= memcached_create(NULL);
  rc= memcached_server_push(mine, servers);
  assert(rc == MEMCACHED_SUCCESS);
  memcached_server_list_free(servers);

  assert(mine);
  clone= memcached_clone(NULL, mine);

  memcached_quit(mine);
  memcached_quit(clone);


  memcached_free(mine);
  memcached_free(clone);

  return 0;
}

/* Test flag store/retrieve */
static test_return  user_supplied_bug7(memcached_st *memc)
{
  memcached_return rc;
  char *keys= "036790384900";
  size_t key_length=  strlen("036790384900");
  char return_key[MEMCACHED_MAX_KEY];
  size_t return_key_length;
  char *value;
  size_t value_length;
  uint32_t flags;
  unsigned int x;
  char insert_data[VALUE_SIZE_BUG5];

  for (x= 0; x < VALUE_SIZE_BUG5; x++)
    insert_data[x]= rand();

  memcached_flush(memc, 0);

  flags= 245;
  rc= memcached_set(memc, keys, key_length,
                    insert_data, VALUE_SIZE_BUG5,
                    (time_t)0, flags);
  assert(rc == MEMCACHED_SUCCESS);

  flags= 0;
  value= memcached_get(memc, keys, key_length,
                        &value_length, &flags, &rc);
  assert(flags == 245);
  assert(value);
  free(value);

  rc= memcached_mget(memc, &keys, &key_length, 1);

  flags= 0;
  value= memcached_fetch(memc, return_key, &return_key_length,
                         &value_length, &flags, &rc);
  assert(flags == 245);
  assert(value);
  free(value);


  return 0;
}

static test_return  user_supplied_bug9(memcached_st *memc)
{
  memcached_return rc;
  char *keys[]= {"UDATA:edevil@sapo.pt", "fudge&*@#", "for^#@&$not"};
  size_t key_length[3];
  unsigned int x;
  uint32_t flags;
  unsigned count= 0;

  char return_key[MEMCACHED_MAX_KEY];
  size_t return_key_length;
  char *return_value;
  size_t return_value_length;


  key_length[0]= strlen("UDATA:edevil@sapo.pt");
  key_length[1]= strlen("fudge&*@#");
  key_length[2]= strlen("for^#@&$not");


  for (x= 0; x < 3; x++)
  {
    rc= memcached_set(memc, keys[x], key_length[x],
                      keys[x], key_length[x],
                      (time_t)50, (uint32_t)9);
    assert(rc == MEMCACHED_SUCCESS);
  }

  rc= memcached_mget(memc, keys, key_length, 3);
  assert(rc == MEMCACHED_SUCCESS);

  /* We need to empty the server before continueing test */
  while ((return_value= memcached_fetch(memc, return_key, &return_key_length,
                      &return_value_length, &flags, &rc)) != NULL)
  {
    assert(return_value);
    free(return_value);
    count++;
  }
  assert(count == 3);

  return 0;
}

/* We are testing with aggressive timeout to get failures */
static test_return  user_supplied_bug10(memcached_st *memc)
{
  char *key= "foo";
  char *value;
  size_t value_length= 512;
  unsigned int x;
  int key_len= 3;
  memcached_return rc;
  unsigned int set= 1;
  memcached_st *mclone= memcached_clone(NULL, memc);
  int32_t timeout;

  memcached_behavior_set(mclone, MEMCACHED_BEHAVIOR_NO_BLOCK, set);
  memcached_behavior_set(mclone, MEMCACHED_BEHAVIOR_TCP_NODELAY, set);
  timeout= 2;
  memcached_behavior_set(mclone, MEMCACHED_BEHAVIOR_POLL_TIMEOUT, timeout);

  value = (char*)malloc(value_length * sizeof(char));

  for (x= 0; x < value_length; x++)
    value[x]= (char) (x % 127);

  for (x= 1; x <= 100000; ++x)
  {
    rc= memcached_set(mclone, key, key_len,value, value_length, 0, 0);

    assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_WRITE_FAILURE ||
           rc == MEMCACHED_BUFFERED || rc == MEMCACHED_TIMEOUT);

    if (rc == MEMCACHED_WRITE_FAILURE || rc == MEMCACHED_TIMEOUT)
      x--;
  }

  free(value);
  memcached_free(mclone);

  return 0;
}

/*
  We are looking failures in the async protocol
*/
static test_return  user_supplied_bug11(memcached_st *memc)
{
  char *key= "foo";
  char *value;
  size_t value_length= 512;
  unsigned int x;
  int key_len= 3;
  memcached_return rc;
  unsigned int set= 1;
  int32_t timeout;
  memcached_st *mclone= memcached_clone(NULL, memc);

  memcached_behavior_set(mclone, MEMCACHED_BEHAVIOR_NO_BLOCK, set);
  memcached_behavior_set(mclone, MEMCACHED_BEHAVIOR_TCP_NODELAY, set);
  timeout= -1;
  memcached_behavior_set(mclone, MEMCACHED_BEHAVIOR_POLL_TIMEOUT, timeout);

  timeout= (int32_t)memcached_behavior_get(mclone, MEMCACHED_BEHAVIOR_POLL_TIMEOUT);

  assert(timeout == -1);

  value = (char*)malloc(value_length * sizeof(char));

  for (x= 0; x < value_length; x++)
    value[x]= (char) (x % 127);

  for (x= 1; x <= 100000; ++x)
  {
    rc= memcached_set(mclone, key, key_len,value, value_length, 0, 0);
  }

  free(value);
  memcached_free(mclone);

  return 0;
}

/*
  Bug found where incr was not returning MEMCACHED_NOTFOUND when object did not exist.
*/
static test_return  user_supplied_bug12(memcached_st *memc)
{
  memcached_return rc;
  uint32_t flags;
  size_t value_length;
  char *value;
  uint64_t number_value;

  value= memcached_get(memc, "autoincrement", strlen("autoincrement"),
                        &value_length, &flags, &rc);
  assert(value == NULL);
  assert(rc == MEMCACHED_NOTFOUND);

  rc= memcached_increment(memc, "autoincrement", strlen("autoincrement"),
                          1, &number_value);

  assert(value == NULL);
  /* The binary protocol will set the key if it doesn't exist */
  if (memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL) == 1)
    assert(rc == MEMCACHED_SUCCESS);
  else
    assert(rc == MEMCACHED_NOTFOUND);

  rc= memcached_set(memc, "autoincrement", strlen("autoincrement"), "1", 1, 0, 0);

  value= memcached_get(memc, "autoincrement", strlen("autoincrement"),
                        &value_length, &flags, &rc);
  assert(value);
  assert(rc == MEMCACHED_SUCCESS);
  free(value);

  rc= memcached_increment(memc, "autoincrement", strlen("autoincrement"),
                          1, &number_value);
  assert(number_value == 2);
  assert(rc == MEMCACHED_SUCCESS);

  return 0;
}

/*
  Bug found where command total one more than MEMCACHED_MAX_BUFFER
  set key34567890 0 0 8169 \r\n is sent followed by buffer of size 8169, followed by 8169
 */
static test_return  user_supplied_bug13(memcached_st *memc)
{
  char key[] = "key34567890";
  char *overflow;
  memcached_return rc;
  size_t overflowSize;

  char commandFirst[]= "set key34567890 0 0 ";
  char commandLast[] = " \r\n"; /* first line of command sent to server */
  size_t commandLength;
  size_t testSize;

  commandLength = strlen(commandFirst) + strlen(commandLast) + 4; /* 4 is number of characters in size, probably 8196 */

  overflowSize = MEMCACHED_MAX_BUFFER - commandLength;

  for (testSize= overflowSize - 1; testSize < overflowSize + 1; testSize++)
  {
    overflow= malloc(testSize);
    assert(overflow != NULL);

    memset(overflow, 'x', testSize);
    rc= memcached_set(memc, key, strlen(key),
                      overflow, testSize, 0, 0);
    assert(rc == MEMCACHED_SUCCESS);
    free(overflow);
  }

  return 0;
}


/*
  Test values of many different sizes
  Bug found where command total one more than MEMCACHED_MAX_BUFFER
  set key34567890 0 0 8169 \r\n
  is sent followed by buffer of size 8169, followed by 8169
 */
static test_return  user_supplied_bug14(memcached_st *memc)
{
  int setter= 1;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_TCP_NODELAY, setter);
  memcached_return rc;
  char *key= "foo";
  char *value;
  size_t value_length= 18000;
  char *string;
  size_t string_length;
  uint32_t flags;
  unsigned int x;
  size_t current_length;

  value = (char*)malloc(value_length);
  assert(value);

  for (x= 0; x < value_length; x++)
    value[x] = (char) (x % 127);

  for (current_length= 0; current_length < value_length; current_length++)
  {
    rc= memcached_set(memc, key, strlen(key),
                      value, current_length,
                      (time_t)0, (uint32_t)0);
    assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);

    string= memcached_get(memc, key, strlen(key),
                          &string_length, &flags, &rc);

    assert(rc == MEMCACHED_SUCCESS);
    assert(string_length == current_length);
    assert(!memcmp(string, value, string_length));

    free(string);
  }

  free(value);

  return 0;
}

/*
  Look for zero length value problems
  */
static test_return  user_supplied_bug15(memcached_st *memc)
{
  uint32_t x;
  memcached_return rc;
  char *key= "mykey";
  char *value;
  size_t length;
  uint32_t flags;

  for (x= 0; x < 2; x++)
  {
    rc= memcached_set(memc, key, strlen(key),
                      NULL, 0,
                      (time_t)0, (uint32_t)0);

    assert(rc == MEMCACHED_SUCCESS);

    value= memcached_get(memc, key, strlen(key),
                         &length, &flags, &rc);

    assert(rc == MEMCACHED_SUCCESS);
    assert(value == NULL);
    assert(length == 0);
    assert(flags == 0);

    value= memcached_get(memc, key, strlen(key),
                         &length, &flags, &rc);

    assert(rc == MEMCACHED_SUCCESS);
    assert(value == NULL);
    assert(length == 0);
    assert(flags == 0);
  }

  return 0;
}

/* Check the return sizes on FLAGS to make sure it stores 32bit unsigned values correctly */
static test_return  user_supplied_bug16(memcached_st *memc)
{
  memcached_return rc;
  char *key= "mykey";
  char *value;
  size_t length;
  uint32_t flags;

  rc= memcached_set(memc, key, strlen(key),
                    NULL, 0,
                    (time_t)0, UINT32_MAX);

  assert(rc == MEMCACHED_SUCCESS);

  value= memcached_get(memc, key, strlen(key),
                       &length, &flags, &rc);

  assert(rc == MEMCACHED_SUCCESS);
  assert(value == NULL);
  assert(length == 0);
  assert(flags == UINT32_MAX);

  return 0;
}

/* Check the validity of chinese key*/
static test_return  user_supplied_bug17(memcached_st *memc)
{
    memcached_return rc;
    char *key= "豆瓣";
    char *value="我们在炎热抑郁的夏天无法停止豆瓣";
    char *value2;
    size_t length;
    uint32_t flags;

    rc= memcached_set(memc, key, strlen(key),
            value, strlen(value),
            (time_t)0, 0);

    assert(rc == MEMCACHED_SUCCESS);

    value2= memcached_get(memc, key, strlen(key),
            &length, &flags, &rc);

    assert(length==strlen(value));
    assert(rc == MEMCACHED_SUCCESS);
    assert(memcmp(value, value2, length)==0);
    free(value2);

    return 0;
}

/*
  From Andrei on IRC
*/

static test_return user_supplied_bug19(memcached_st *memc)
{
  memcached_st *m;
  memcached_server_st *s;
  memcached_return res;

  (void)memc;

  m= memcached_create(NULL);
  memcached_server_add_with_weight(m, "localhost", 11311, 100);
  memcached_server_add_with_weight(m, "localhost", 11312, 100);

  s= memcached_server_by_key(m, "a", 1, &res);
  memcached_server_free(s);

  memcached_free(m);

  return 0;
}

/* CAS test from Andei */
static test_return user_supplied_bug20(memcached_st *memc)
{
  memcached_return status;
  memcached_result_st *result, result_obj;
  char *key = "abc";
  size_t key_len = strlen("abc");
  char *value = "foobar";
  size_t value_len = strlen(value);

  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_SUPPORT_CAS, 1);

  status = memcached_set(memc, key, key_len, value, value_len, (time_t)0, (uint32_t)0);
  assert(status == MEMCACHED_SUCCESS);

  status = memcached_mget(memc, &key, &key_len, 1);
  assert(status == MEMCACHED_SUCCESS);

  result= memcached_result_create(memc, &result_obj);
  assert(result);

  memcached_result_create(memc, &result_obj);
  result= memcached_fetch_result(memc, &result_obj, &status);

  assert(result);
  assert(status == MEMCACHED_SUCCESS);

  memcached_result_free(result);

  return 0;
}

#include "ketama_test_cases.h"
static test_return user_supplied_bug18(memcached_st *trash)
{
  memcached_return rc;
  int value;
  int x;
  memcached_server_st *server_pool;
  memcached_st *memc;

  (void)trash;

  memc= memcached_create(NULL);
  assert(memc);

  rc= memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_KETAMA_WEIGHTED, 1);
  assert(rc == MEMCACHED_SUCCESS);

  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_KETAMA_WEIGHTED);
  assert(value == 1);

  rc= memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_KETAMA_HASH, MEMCACHED_HASH_MD5);
  assert(rc == MEMCACHED_SUCCESS);

  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_KETAMA_HASH);
  assert(value == MEMCACHED_HASH_MD5);

  server_pool = memcached_servers_parse("10.0.1.1:11211 600,10.0.1.2:11211 300,10.0.1.3:11211 200,10.0.1.4:11211 350,10.0.1.5:11211 1000,10.0.1.6:11211 800,10.0.1.7:11211 950,10.0.1.8:11211 100");
  memcached_server_push(memc, server_pool);

  /* verify that the server list was parsed okay. */
  assert(memc->number_of_hosts == 8);
  assert(strcmp(server_pool[0].hostname, "10.0.1.1") == 0);
  assert(server_pool[0].port == 11211);
  assert(server_pool[0].weight == 600);
  assert(strcmp(server_pool[2].hostname, "10.0.1.3") == 0);
  assert(server_pool[2].port == 11211);
  assert(server_pool[2].weight == 200);
  assert(strcmp(server_pool[7].hostname, "10.0.1.8") == 0);
  assert(server_pool[7].port == 11211);
  assert(server_pool[7].weight == 100);

  /* VDEAAAAA hashes to fffcd1b5, after the last continuum point, and lets
   * us test the boundary wraparound.
   */
  assert(memcached_generate_hash(memc, (char *)"VDEAAAAA", 8) == memc->continuum[0].index);

  /* verify the standard ketama set. */
  for (x= 0; x < 99; x++)
  {
    uint32_t server_idx = memcached_generate_hash(memc, test_cases[x].key, strlen(test_cases[x].key));
    char *hostname = memc->hosts[server_idx].hostname;
    assert(strcmp(hostname, test_cases[x].server) == 0);
  }

  memcached_server_list_free(server_pool);
  memcached_free(memc);

  return 0;
}

static test_return auto_eject_hosts(memcached_st *trash)
{
  (void) trash;

  memcached_return rc;
  memcached_st *memc= memcached_create(NULL);
  assert(memc);

  rc= memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_KETAMA_WEIGHTED, 1);
  assert(rc == MEMCACHED_SUCCESS);

  uint64_t value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_KETAMA_WEIGHTED);
  assert(value == 1);

  rc= memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_KETAMA_HASH, MEMCACHED_HASH_MD5);
  assert(rc == MEMCACHED_SUCCESS);

  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_KETAMA_HASH);
  assert(value == MEMCACHED_HASH_MD5);

    /* server should be removed when in delay */
  rc= memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_AUTO_EJECT_HOSTS, 1);
  assert(rc == MEMCACHED_SUCCESS);

  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_AUTO_EJECT_HOSTS);
  assert(value == 1);

  memcached_server_st *server_pool;
  server_pool = memcached_servers_parse("10.0.1.1:11211 600,10.0.1.2:11211 300,10.0.1.3:11211 200,10.0.1.4:11211 350,10.0.1.5:11211 1000,10.0.1.6:11211 800,10.0.1.7:11211 950,10.0.1.8:11211 100");
  memcached_server_push(memc, server_pool);

  /* verify that the server list was parsed okay. */
  assert(memc->number_of_hosts == 8);
  assert(strcmp(server_pool[0].hostname, "10.0.1.1") == 0);
  assert(server_pool[0].port == 11211);
  assert(server_pool[0].weight == 600);
  assert(strcmp(server_pool[2].hostname, "10.0.1.3") == 0);
  assert(server_pool[2].port == 11211);
  assert(server_pool[2].weight == 200);
  assert(strcmp(server_pool[7].hostname, "10.0.1.8") == 0);
  assert(server_pool[7].port == 11211);
  assert(server_pool[7].weight == 100);

  memc->hosts[2].next_retry = time(NULL) + 15;
  memc->next_distribution_rebuild= time(NULL) - 1;

  for (int x= 0; x < 99; x++)
  {
    uint32_t server_idx = memcached_generate_hash(memc, test_cases[x].key, strlen(test_cases[x].key));
    assert(server_idx != 2);
  }

  /* and re-added when it's back. */
  memc->hosts[2].next_retry = time(NULL) - 1;
  memc->next_distribution_rebuild= time(NULL) - 1;
  run_distribution(memc);
  for (int x= 0; x < 99; x++)
  {
    uint32_t server_idx = memcached_generate_hash(memc, test_cases[x].key, strlen(test_cases[x].key));
    char *hostname = memc->hosts[server_idx].hostname;
    assert(strcmp(hostname, test_cases[x].server) == 0);
  }

  memcached_server_list_free(server_pool);
  memcached_free(memc);

  return TEST_SUCCESS;
}

static test_return  result_static(memcached_st *memc)
{
  memcached_result_st result;
  memcached_result_st *result_ptr;

  result_ptr= memcached_result_create(memc, &result);
  assert(result.is_allocated == false);
  assert(result_ptr);
  memcached_result_free(&result);

  return 0;
}

static test_return  result_alloc(memcached_st *memc)
{
  memcached_result_st *result;

  result= memcached_result_create(memc, NULL);
  assert(result);
  memcached_result_free(result);

  return 0;
}

static test_return  string_static_null(memcached_st *memc)
{
  memcached_string_st string;
  memcached_string_st *string_ptr;

  string_ptr= memcached_string_create(memc, &string, 0);
  assert(string.is_allocated == false);
  assert(string_ptr);
  memcached_string_free(&string);

  return 0;
}

static test_return  string_alloc_null(memcached_st *memc)
{
  memcached_string_st *string;

  string= memcached_string_create(memc, NULL, 0);
  assert(string);
  memcached_string_free(string);

  return 0;
}

static test_return  string_alloc_with_size(memcached_st *memc)
{
  memcached_string_st *string;

  string= memcached_string_create(memc, NULL, 1024);
  assert(string);
  memcached_string_free(string);

  return 0;
}

static test_return  string_alloc_with_size_toobig(memcached_st *memc)
{
  memcached_string_st *string;

  string= memcached_string_create(memc, NULL, SIZE_MAX);
  assert(string == NULL);

  return 0;
}

static test_return  string_alloc_append(memcached_st *memc)
{
  unsigned int x;
  char buffer[SMALL_STRING_LEN];
  memcached_string_st *string;

  /* Ring the bell! */
  memset(buffer, 6, SMALL_STRING_LEN);

  string= memcached_string_create(memc, NULL, 100);
  assert(string);

  for (x= 0; x < 1024; x++)
  {
    memcached_return rc;
    rc= memcached_string_append(string, buffer, SMALL_STRING_LEN);
    assert(rc == MEMCACHED_SUCCESS);
  }
  memcached_string_free(string);

  return 0;
}

static test_return  string_alloc_append_toobig(memcached_st *memc)
{
  memcached_return rc;
  unsigned int x;
  char buffer[SMALL_STRING_LEN];
  memcached_string_st *string;

  /* Ring the bell! */
  memset(buffer, 6, SMALL_STRING_LEN);

  string= memcached_string_create(memc, NULL, 100);
  assert(string);

  for (x= 0; x < 1024; x++)
  {
    rc= memcached_string_append(string, buffer, SMALL_STRING_LEN);
    assert(rc == MEMCACHED_SUCCESS);
  }
  rc= memcached_string_append(string, buffer, SIZE_MAX);
  assert(rc == MEMCACHED_MEMORY_ALLOCATION_FAILURE);
  memcached_string_free(string);

  return 0;
}

static test_return  cleanup_pairs(memcached_st *memc __attribute__((unused)))
{
  pairs_free(global_pairs);

  return 0;
}

static test_return  generate_pairs(memcached_st *memc __attribute__((unused)))
{
  unsigned long long x;
  global_pairs= pairs_generate(GLOBAL_COUNT, 400);
  global_count= GLOBAL_COUNT;

  for (x= 0; x < global_count; x++)
  {
    global_keys[x]= global_pairs[x].key;
    global_keys_length[x]=  global_pairs[x].key_length;
  }

  return 0;
}

static test_return  generate_large_pairs(memcached_st *memc __attribute__((unused)))
{
  unsigned long long x;
  global_pairs= pairs_generate(GLOBAL2_COUNT, MEMCACHED_MAX_BUFFER+10);
  global_count= GLOBAL2_COUNT;

  for (x= 0; x < global_count; x++)
  {
    global_keys[x]= global_pairs[x].key;
    global_keys_length[x]=  global_pairs[x].key_length;
  }

  return 0;
}

static test_return  generate_data(memcached_st *memc)
{
  execute_set(memc, global_pairs, global_count);

  return 0;
}

static test_return  generate_data_with_stats(memcached_st *memc)
{
  memcached_stat_st *stat_p;
  memcached_return rc;
  uint32_t host_index= 0;
  execute_set(memc, global_pairs, global_count);

  //TODO: hosts used size stats
  stat_p= memcached_stat(memc, NULL, &rc);
  assert(stat_p);

  for (host_index= 0; host_index < SERVERS_TO_CREATE; host_index++)
  {
    /* This test was changes so that "make test" would work properlly */
#ifdef DEBUG
    printf("\nserver %u|%s|%u bytes: %llu\n", host_index, (memc->hosts)[host_index].hostname, (memc->hosts)[host_index].port, (unsigned long long)(stat_p + host_index)->bytes);
#endif
    assert((unsigned long long)(stat_p + host_index)->bytes);
  }

  memcached_stat_free(NULL, stat_p);

  return 0;
}
static test_return  generate_buffer_data(memcached_st *memc)
{
  int latch= 0;

  latch= 1;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BUFFER_REQUESTS, latch);
  generate_data(memc);

  return 0;
}

static test_return  get_read_count(memcached_st *memc)
{
  unsigned int x;
  memcached_return rc;
  memcached_st *clone;

  clone= memcached_clone(NULL, memc);
  assert(clone);

  memcached_server_add_with_weight(clone, "localhost", 6666, 0);

  {
    char *return_value;
    size_t return_value_length;
    uint32_t flags;
    uint32_t count;

    for (x= count= 0; x < global_count; x++)
    {
      return_value= memcached_get(clone, global_keys[x], global_keys_length[x],
                                  &return_value_length, &flags, &rc);
      if (rc == MEMCACHED_SUCCESS)
      {
        count++;
        if (return_value)
          free(return_value);
      }
    }
    fprintf(stderr, "\t%u -> %u", global_count, count);
  }

  memcached_free(clone);

  return 0;
}

static test_return  get_read(memcached_st *memc)
{
  unsigned int x;
  memcached_return rc;

  {
    char *return_value;
    size_t return_value_length;
    uint32_t flags;

    for (x= 0; x < global_count; x++)
    {
      return_value= memcached_get(memc, global_keys[x], global_keys_length[x],
                                  &return_value_length, &flags, &rc);
      /*
      assert(return_value);
      assert(rc == MEMCACHED_SUCCESS);
    */
      if (rc == MEMCACHED_SUCCESS && return_value)
        free(return_value);
    }
  }

  return 0;
}

static test_return  mget_read(memcached_st *memc)
{
  memcached_return rc;

  rc= memcached_mget(memc, global_keys, global_keys_length, global_count);
  assert(rc == MEMCACHED_SUCCESS);
  /* Turn this into a help function */
  {
    char return_key[MEMCACHED_MAX_KEY];
    size_t return_key_length;
    char *return_value;
    size_t return_value_length;
    uint32_t flags;

    while ((return_value= memcached_fetch(memc, return_key, &return_key_length,
                                          &return_value_length, &flags, &rc)))
    {
      assert(return_value);
      assert(rc == MEMCACHED_SUCCESS);
      free(return_value);
    }
  }

  return 0;
}

static test_return  mget_read_result(memcached_st *memc)
{
  memcached_return rc;

  rc= memcached_mget(memc, global_keys, global_keys_length, global_count);
  assert(rc == MEMCACHED_SUCCESS);
  /* Turn this into a help function */
  {
    memcached_result_st results_obj;
    memcached_result_st *results;

    results= memcached_result_create(memc, &results_obj);

    while ((results= memcached_fetch_result(memc, &results_obj, &rc)))
    {
      assert(results);
      assert(rc == MEMCACHED_SUCCESS);
    }

    memcached_result_free(&results_obj);
  }

  return 0;
}

static test_return  mget_read_function(memcached_st *memc)
{
  memcached_return rc;
  unsigned int counter;
  memcached_execute_function callbacks[1];

  rc= memcached_mget(memc, global_keys, global_keys_length, global_count);
  assert(rc == MEMCACHED_SUCCESS);

  callbacks[0]= &callback_counter;
  counter= 0;
  rc= memcached_fetch_execute(memc, callbacks, (void *)&counter, 1);

  return 0;
}

static test_return  delete_generate(memcached_st *memc)
{
  unsigned int x;

  for (x= 0; x < global_count; x++)
  {
    (void)memcached_delete(memc, global_keys[x], global_keys_length[x], (time_t)0);
  }

  return 0;
}

static test_return  delete_buffer_generate(memcached_st *memc)
{
  int latch= 0;
  unsigned int x;

  latch= 1;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BUFFER_REQUESTS, latch);

  for (x= 0; x < global_count; x++)
  {
    (void)memcached_delete(memc, global_keys[x], global_keys_length[x], (time_t)0);
  }

  return 0;
}

static test_return  add_host_test1(memcached_st *memc)
{
  unsigned int x;
  memcached_return rc;
  char servername[]= "0.example.com";
  memcached_server_st *servers;

  servers= memcached_server_list_append_with_weight(NULL, servername, 400, 0, &rc);
  assert(servers);
  assert(1 == memcached_server_list_count(servers));

  for (x= 2; x < 20; x++)
  {
    char buffer[SMALL_STRING_LEN];

    snprintf(buffer, SMALL_STRING_LEN, "%u.example.com", 400+x);
    servers= memcached_server_list_append_with_weight(servers, buffer, 401, 0,
                                     &rc);
    assert(rc == MEMCACHED_SUCCESS);
    assert(x == memcached_server_list_count(servers));
  }

  rc= memcached_server_push(memc, servers);
  assert(rc == MEMCACHED_SUCCESS);
  rc= memcached_server_push(memc, servers);
  assert(rc == MEMCACHED_SUCCESS);

  memcached_server_list_free(servers);

  return 0;
}

static memcached_return  pre_nonblock(memcached_st *memc)
{
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NO_BLOCK, 0);

  return MEMCACHED_SUCCESS;
}

static memcached_return  pre_nonblock_binary(memcached_st *memc)
{
  memcached_return rc= MEMCACHED_FAILURE;
  memcached_st *clone;

  clone= memcached_clone(NULL, memc);
  assert(clone);
  // The memcached_version needs to be done on a clone, because the server
  // will not toggle protocol on an connection.
  memcached_version(clone);

  if (clone->hosts[0].major_version >= 1 && clone->hosts[0].minor_version > 2)
  {
    memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NO_BLOCK, 0);
    rc = memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
    assert(rc == MEMCACHED_SUCCESS);
    assert(memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL) == 1);
  }

  memcached_free(clone);
  return rc;
}

static memcached_return  pre_murmur(memcached_st *memc)
{
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_HASH, (uint64_t)MEMCACHED_HASH_MURMUR);

  return MEMCACHED_SUCCESS;
}

static memcached_return pre_jenkins(memcached_st *memc)
{
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_HASH, (uint64_t)MEMCACHED_HASH_JENKINS);

  return MEMCACHED_SUCCESS;
}


static memcached_return  pre_md5(memcached_st *memc)
{
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_HASH, (uint64_t)MEMCACHED_HASH_MD5);

  return MEMCACHED_SUCCESS;
}

static memcached_return  pre_crc(memcached_st *memc)
{
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_HASH, (uint64_t)MEMCACHED_HASH_CRC);

  return MEMCACHED_SUCCESS;
}

static memcached_return  pre_hsieh(memcached_st *memc)
{
#ifdef HAVE_HSIEH_HASH
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_HASH, (uint64_t)MEMCACHED_HASH_HSIEH);
  return MEMCACHED_SUCCESS;
#else
  (void) memc;
  return MEMCACHED_FAILURE;
#endif
}

static memcached_return  pre_hash_fnv1_64(memcached_st *memc)
{
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_HASH, (uint64_t)MEMCACHED_HASH_FNV1_64);

  return MEMCACHED_SUCCESS;
}

static memcached_return  pre_hash_fnv1a_64(memcached_st *memc)
{
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_HASH, (uint64_t)MEMCACHED_HASH_FNV1A_64);

  return MEMCACHED_SUCCESS;
}

static memcached_return  pre_hash_fnv1_32(memcached_st *memc)
{
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_HASH, (uint64_t)MEMCACHED_HASH_FNV1_32);

  return MEMCACHED_SUCCESS;
}

static memcached_return  pre_hash_fnv1a_32(memcached_st *memc)
{
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_HASH, (uint64_t)MEMCACHED_HASH_FNV1A_32);

  return MEMCACHED_SUCCESS;
}

static memcached_return  pre_behavior_ketama(memcached_st *memc)
{
  memcached_return rc;
  uint64_t value;

  rc= memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_KETAMA, 1);
  assert(rc == MEMCACHED_SUCCESS);

  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_KETAMA);
  assert(value == 1);

  return MEMCACHED_SUCCESS;
}

static memcached_return  pre_behavior_ketama_weighted(memcached_st *memc)
{
  memcached_return rc;
  uint64_t value;

  rc= memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_KETAMA_WEIGHTED, 1);
  assert(rc == MEMCACHED_SUCCESS);

  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_KETAMA_WEIGHTED);
  assert(value == 1);

  rc= memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_KETAMA_HASH, MEMCACHED_HASH_MD5);
  assert(rc == MEMCACHED_SUCCESS);

  value= memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_KETAMA_HASH);
  assert(value == MEMCACHED_HASH_MD5);
  return MEMCACHED_SUCCESS;
}

static memcached_return  pre_binary(memcached_st *memc)
{
  memcached_return rc= MEMCACHED_FAILURE;
  memcached_st *clone;

  clone= memcached_clone(NULL, memc);
  assert(clone);
  // The memcached_version needs to be done on a clone, because the server
  // will not toggle protocol on an connection.
  memcached_version(clone);

  if (clone->hosts[0].major_version >= 1 && clone->hosts[0].minor_version > 2)
  {
    rc = memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
    assert(rc == MEMCACHED_SUCCESS);
    assert(memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL) == 1);
  }

  memcached_free(clone);
  return rc;
}

static void my_free(memcached_st *ptr __attribute__((unused)), void *mem)
{
  free(mem);
}

static void *my_malloc(memcached_st *ptr __attribute__((unused)), const size_t size)
{
  return calloc(1, size);
}

static void *my_realloc(memcached_st *ptr __attribute__((unused)), void *mem, const size_t size)
{
  return realloc(mem, size);
}

static memcached_return set_prefix(memcached_st *memc)
{
  memcached_return rc;
  const char *key= "mine";
  char *value;

  /* Make sure be default none exists */
  value= memcached_callback_get(memc, MEMCACHED_CALLBACK_PREFIX_KEY, &rc);
  assert(rc == MEMCACHED_FAILURE);

  /* Test a clean set */
  rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_PREFIX_KEY, (void *)key);
  assert(rc == MEMCACHED_SUCCESS);

  value= memcached_callback_get(memc, MEMCACHED_CALLBACK_PREFIX_KEY, &rc);
  assert(memcmp(value, key, 4) == 0);
  assert(rc == MEMCACHED_SUCCESS);

  /* Test that we can turn it off */
  rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_PREFIX_KEY, NULL);
  assert(rc == MEMCACHED_SUCCESS);

  value= memcached_callback_get(memc, MEMCACHED_CALLBACK_PREFIX_KEY, &rc);
  assert(rc == MEMCACHED_FAILURE);

  /* Now setup for main test */
  rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_PREFIX_KEY, (void *)key);
  assert(rc == MEMCACHED_SUCCESS);

  value= memcached_callback_get(memc, MEMCACHED_CALLBACK_PREFIX_KEY, &rc);
  assert(rc == MEMCACHED_SUCCESS);
  assert(memcmp(value, key, 4) == 0);

  /* Set to Zero, and then Set to something too large */
  {
    char *long_key;
    rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_PREFIX_KEY, NULL);
    assert(rc == MEMCACHED_SUCCESS);

    value= memcached_callback_get(memc, MEMCACHED_CALLBACK_PREFIX_KEY, &rc);
    assert(rc == MEMCACHED_FAILURE);
    assert(value == NULL);

    /* Test a long key for failure */
    /* TODO, extend test to determine based on setting, what result should be */
    long_key= "Thisismorethentheallottednumberofcharacters";
    rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_PREFIX_KEY, long_key);
    //assert(rc == MEMCACHED_BAD_KEY_PROVIDED);
    assert(rc == MEMCACHED_SUCCESS);

    /* Now test a key with spaces (which will fail from long key, since bad key is not set) */
    long_key= "This is more then the allotted number of characters";
    rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_PREFIX_KEY, long_key);
    assert(rc == MEMCACHED_BAD_KEY_PROVIDED);

    /* Test for a bad prefix, but with a short key */
    rc= memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_VERIFY_KEY, 1);
    assert(rc == MEMCACHED_SUCCESS);

    long_key= "dog cat";
    rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_PREFIX_KEY, long_key);
    assert(rc == MEMCACHED_BAD_KEY_PROVIDED);
  }

  return MEMCACHED_SUCCESS;
}

static memcached_return  set_memory_alloc(memcached_st *memc)
{
  void *test_ptr= NULL;
  void *cb_ptr= NULL;
  {
    memcached_malloc_function malloc_cb=
      (memcached_malloc_function)my_malloc;
    cb_ptr= *(void **)&malloc_cb;
    memcached_return rc;

    rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_MALLOC_FUNCTION, cb_ptr);
    assert(rc == MEMCACHED_SUCCESS);
    test_ptr= memcached_callback_get(memc, MEMCACHED_CALLBACK_MALLOC_FUNCTION, &rc);
    assert(rc == MEMCACHED_SUCCESS);
    assert(test_ptr == cb_ptr);
  }

  {
    memcached_realloc_function realloc_cb=
      (memcached_realloc_function)my_realloc;
    cb_ptr= *(void **)&realloc_cb;
    memcached_return rc;

    rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_REALLOC_FUNCTION, cb_ptr);
    assert(rc == MEMCACHED_SUCCESS);
    test_ptr= memcached_callback_get(memc, MEMCACHED_CALLBACK_REALLOC_FUNCTION, &rc);
    assert(rc == MEMCACHED_SUCCESS);
    assert(test_ptr == cb_ptr);
  }

  {
    memcached_free_function free_cb=
      (memcached_free_function)my_free;
    cb_ptr= *(void **)&free_cb;
    memcached_return rc;

    rc= memcached_callback_set(memc, MEMCACHED_CALLBACK_FREE_FUNCTION, cb_ptr);
    assert(rc == MEMCACHED_SUCCESS);
    test_ptr= memcached_callback_get(memc, MEMCACHED_CALLBACK_FREE_FUNCTION, &rc);
    assert(rc == MEMCACHED_SUCCESS);
    assert(test_ptr == cb_ptr);
  }

  return MEMCACHED_SUCCESS;
}

static memcached_return  enable_consistent(memcached_st *memc)
{
  memcached_server_distribution value= MEMCACHED_DISTRIBUTION_CONSISTENT;
  memcached_hash hash;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_DISTRIBUTION, value);
  if (pre_hsieh(memc) != MEMCACHED_SUCCESS)
    return MEMCACHED_FAILURE;

  value= (memcached_server_distribution)memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_DISTRIBUTION);
  assert(value == MEMCACHED_DISTRIBUTION_CONSISTENT);

  hash= (memcached_hash)memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_HASH);
  assert(hash == MEMCACHED_HASH_HSIEH);


  return MEMCACHED_SUCCESS;
}

static memcached_return  enable_cas(memcached_st *memc)
{
  unsigned int set= 1;

  memcached_version(memc);

  if ((memc->hosts[0].major_version >= 1 && (memc->hosts[0].minor_version == 2 && memc->hosts[0].micro_version >= 4))
      || memc->hosts[0].minor_version > 2)
  {
    memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_SUPPORT_CAS, set);

    return MEMCACHED_SUCCESS;
  }

  return MEMCACHED_FAILURE;
}

static memcached_return  check_for_1_2_3(memcached_st *memc)
{
  memcached_version(memc);

  if ((memc->hosts[0].major_version >= 1 && (memc->hosts[0].minor_version == 2 && memc->hosts[0].micro_version >= 4))
      || memc->hosts[0].minor_version > 2)
    return MEMCACHED_SUCCESS;

  return MEMCACHED_FAILURE;
}

static memcached_return  pre_unix_socket(memcached_st *memc)
{
  memcached_return rc;
  struct stat buf;

  memcached_server_list_free(memc->hosts);
  memc->hosts= NULL;
  memc->number_of_hosts= 0;

  if (stat("/tmp/memcached.socket", &buf))
    return MEMCACHED_FAILURE;

  rc= memcached_server_add_unix_socket_with_weight(memc, "/tmp/memcached.socket", 0);

  return rc;
}

static memcached_return  pre_nodelay(memcached_st *memc)
{
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NO_BLOCK, 0);
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_TCP_NODELAY, 0);

  return MEMCACHED_SUCCESS;
}

static memcached_return  pre_settimer(memcached_st *memc)
{
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_SND_TIMEOUT, 1000);
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_RCV_TIMEOUT, 1000);

  return MEMCACHED_SUCCESS;
}

static memcached_return  poll_timeout(memcached_st *memc)
{
  int32_t timeout;

  timeout= 100;

  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_POLL_TIMEOUT, timeout);

  timeout= (int32_t)memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_POLL_TIMEOUT);

  assert(timeout == 100);

  return MEMCACHED_SUCCESS;
}

static test_return noreply_test(memcached_st *memc)
{
  memcached_return ret;
  ret= memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NOREPLY, 1);
  assert(ret == MEMCACHED_SUCCESS);
  ret= memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BUFFER_REQUESTS, 1);
  assert(ret == MEMCACHED_SUCCESS);
  ret= memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_SUPPORT_CAS, 1);
  assert(ret == MEMCACHED_SUCCESS);
  assert(memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_NOREPLY) == 1);
  assert(memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_BUFFER_REQUESTS) == 1);
  assert(memcached_behavior_get(memc, MEMCACHED_BEHAVIOR_SUPPORT_CAS) == 1);

  for (int count=0; count < 5; ++count)
  {
    for (int x=0; x < 100; ++x)
    {
      char key[10];
      size_t len= sprintf(key, "%d", x);
      switch (count)
      {
      case 0:
        ret=memcached_add(memc, key, len, key, len, 0, 0);
        break;
      case 1:
        ret=memcached_replace(memc, key, len, key, len, 0, 0);
        break;
      case 2:
        ret=memcached_set(memc, key, len, key, len, 0, 0);
        break;
      case 3:
        ret=memcached_append(memc, key, len, key, len, 0, 0);
        break;
      case 4:
        ret=memcached_prepend(memc, key, len, key, len, 0, 0);
        break;
      }
      assert(ret == MEMCACHED_SUCCESS || ret == MEMCACHED_BUFFERED);
    }

    /*
    ** NOTE: Don't ever do this in your code! this is not a supported use of the
    ** API and is _ONLY_ done this way to verify that the library works the
    ** way it is supposed to do!!!!
    */
    int no_msg=0;
    for (uint32_t x=0; x < memc->number_of_hosts; ++x)
      no_msg+=memc->hosts[x].cursor_active;

    assert(no_msg == 0);
    assert(memcached_flush_buffers(memc) == MEMCACHED_SUCCESS);

    /*
     ** Now validate that all items was set properly!
     */
    for (int x=0; x < 100; ++x)
    {
      char key[10];
      size_t len= sprintf(key, "%d", x);
      size_t length;
      uint32_t flags;
      char* value=memcached_get(memc, key, strlen(key),
                                &length, &flags, &ret);
      assert(ret == MEMCACHED_SUCCESS && value != NULL);
      switch (count)
      {
      case 0: /* FALLTHROUGH */
      case 1: /* FALLTHROUGH */
      case 2:
        assert(strncmp(value, key, len) == 0);
        assert(len == length);
        break;
      case 3:
        assert(length == len * 2);
        break;
      case 4:
        assert(length == len * 3);
        break;
      }
      free(value);
    }
  }

  /* Try setting an illegal cas value (should not return an error to
   * the caller (because we don't expect a return message from the server)
   */
  char* keys[]= {"0"};
  size_t lengths[]= {1};
  size_t length;
  uint32_t flags;
  memcached_result_st results_obj;
  memcached_result_st *results;
  ret= memcached_mget(memc, keys, lengths, 1);
  assert(ret == MEMCACHED_SUCCESS);

  results= memcached_result_create(memc, &results_obj);
  assert(results);
  results= memcached_fetch_result(memc, &results_obj, &ret);
  assert(results);
  assert(ret == MEMCACHED_SUCCESS);
  uint64_t cas= memcached_result_cas(results);
  memcached_result_free(&results_obj);

  ret= memcached_cas(memc, keys[0], lengths[0], keys[0], lengths[0], 0, 0, cas);
  assert(ret == MEMCACHED_SUCCESS);

  /*
   * The item will have a new cas value, so try to set it again with the old
   * value. This should fail!
   */
  ret= memcached_cas(memc, keys[0], lengths[0], keys[0], lengths[0], 0, 0, cas);
  assert(ret == MEMCACHED_SUCCESS);
  assert(memcached_flush_buffers(memc) == MEMCACHED_SUCCESS);
  char* value=memcached_get(memc, keys[0], lengths[0], &length, &flags, &ret);
  assert(ret == MEMCACHED_SUCCESS && value != NULL);
  free(value);

  return TEST_SUCCESS;
}

static test_return analyzer_test(memcached_st *memc)
{
  memcached_return rc;
  memcached_stat_st *stat;
  memcached_analysis_st *report;

  stat= memcached_stat(memc, NULL, &rc);
  assert(rc == MEMCACHED_SUCCESS);
  assert(stat);

  report= memcached_analyze(memc, stat, &rc);
  assert(rc == MEMCACHED_SUCCESS);
  assert(report);

  free(report);
  memcached_stat_free(NULL, stat);

  return TEST_SUCCESS;
}

/* Count the objects */
static memcached_return callback_dump_counter(memcached_st *ptr __attribute__((unused)),
                                              const char *key __attribute__((unused)),
                                              size_t key_length __attribute__((unused)),
                                              void *context)
{
  uint32_t *counter= (uint32_t *)context;

  *counter= *counter + 1;

  return MEMCACHED_SUCCESS;
}

static test_return dump_test(memcached_st *memc)
{
  memcached_return rc;
  uint32_t counter= 0;
  memcached_dump_func callbacks[1];
  test_return main_rc;

  callbacks[0]= &callback_dump_counter;

  /* No support for Binary protocol yet */
  if (memc->flags & MEM_BINARY_PROTOCOL)
    return TEST_SUCCESS;

  main_rc= set_test3(memc);

  assert (main_rc == TEST_SUCCESS);

  rc= memcached_dump(memc, callbacks, (void *)&counter, 1);
  assert(rc == MEMCACHED_SUCCESS);

  /* We may have more then 32 if our previous flush has not completed */
  assert(counter >= 32);

  return TEST_SUCCESS;
}

#ifdef HAVE_LIBMEMCACHEDUTIL
static void* connection_release(void *arg) {
  struct {
    memcached_pool_st* pool;
    memcached_st* mmc;
  } *resource= arg;

  usleep(250);
  assert(memcached_pool_push(resource->pool, resource->mmc) == MEMCACHED_SUCCESS);
  return arg;
}

static test_return connection_pool_test(memcached_st *memc)
{
  memcached_pool_st* pool= memcached_pool_create(memc, 5, 10);
  assert(pool != NULL);
  memcached_st* mmc[10];
  memcached_return rc;

  for (int x= 0; x < 10; ++x) {
    mmc[x]= memcached_pool_pop(pool, false, &rc);
    assert(mmc[x] != NULL);
    assert(rc == MEMCACHED_SUCCESS);
  }

  assert(memcached_pool_pop(pool, false, &rc) == NULL);
  assert(rc == MEMCACHED_SUCCESS);

  pthread_t tid;
  struct {
    memcached_pool_st* pool;
    memcached_st* mmc;
  } item= { .pool = pool, .mmc = mmc[9] };
  pthread_create(&tid, NULL, connection_release, &item);
  mmc[9]= memcached_pool_pop(pool, true, &rc);
  assert(rc == MEMCACHED_SUCCESS);
  pthread_join(tid, NULL);
  assert(mmc[9] == item.mmc);
  const char *key= "key";
  size_t keylen= strlen(key);

  // verify that I can do ops with all connections
  rc= memcached_set(mmc[0], key, keylen, "0", 1, 0, 0);
  assert(rc == MEMCACHED_SUCCESS);

  for (unsigned int x= 0; x < 10; ++x) {
    uint64_t number_value;
    rc= memcached_increment(mmc[x], key, keylen, 1, &number_value);
    assert(rc == MEMCACHED_SUCCESS);
    assert(number_value == (x+1));
  }

  // Release them..
  for (int x= 0; x < 10; ++x)
    assert(memcached_pool_push(pool, mmc[x]) == MEMCACHED_SUCCESS);

  assert(memcached_pool_destroy(pool) == memc);
  return TEST_SUCCESS;
}
#endif

static void increment_request_id(uint16_t *id)
{
  (*id)++;
  if ((*id & UDP_REQUEST_ID_THREAD_MASK) != 0)
    *id= 0;
}

static uint16_t *get_udp_request_ids(memcached_st *memc)
{
  uint16_t *ids= malloc(sizeof(uint16_t) * memc->number_of_hosts);
  assert(ids != NULL);
  unsigned int x;
  for (x= 0; x < memc->number_of_hosts; x++)
    ids[x]= get_udp_datagram_request_id((struct udp_datagram_header_st *) memc->hosts[x].write_buffer);

  return ids;
}

static test_return post_udp_op_check(memcached_st *memc, uint16_t *expected_req_ids)
{
  unsigned int x;
  memcached_server_st *cur_server = memc->hosts;
  uint16_t *cur_req_ids = get_udp_request_ids(memc);
  for (x= 0; x < memc->number_of_hosts; x++)
  {
    assert(cur_server[x].cursor_active == 0);
    assert(cur_req_ids[x] == expected_req_ids[x]);
  }
  free(expected_req_ids);
  free(cur_req_ids);
  return TEST_SUCCESS;
}

/*
** There is a little bit of a hack here, instead of removing
** the servers, I just set num host to 0 and them add then new udp servers
**/
static memcached_return init_udp(memcached_st *memc)
{
  memcached_version(memc);
  /* For the time being, only support udp test for >= 1.2.6 && < 1.3 */
  if (memc->hosts[0].major_version != 1 || memc->hosts[0].minor_version != 2
          || memc->hosts[0].micro_version < 6)
    return MEMCACHED_FAILURE;

  uint32_t num_hosts= memc->number_of_hosts;
  unsigned int x= 0;
  memcached_server_st servers[num_hosts];
  memcpy(servers, memc->hosts, sizeof(memcached_server_st) * num_hosts);
  for (x= 0; x < num_hosts; x++)
    memcached_server_free(&memc->hosts[x]);
  memc->number_of_hosts= 0;
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_USE_UDP, 1);
  for (x= 0; x < num_hosts; x++)
  {
    assert(memcached_server_add_udp(memc, servers[x].hostname, servers[x].port) == MEMCACHED_SUCCESS);
    assert(memc->hosts[x].write_buffer_offset == UDP_DATAGRAM_HEADER_LENGTH);
  }
  return MEMCACHED_SUCCESS;
}

static memcached_return binary_init_udp(memcached_st *memc)
{
  pre_binary(memc);
  return init_udp(memc);
}

/* Make sure that I cant add a tcp server to a udp client */
static test_return add_tcp_server_udp_client_test(memcached_st *memc)
{
  memcached_server_st server;
  memcached_server_clone(&server, &memc->hosts[0]);
  assert(memcached_server_remove(&(memc->hosts[0])) == MEMCACHED_SUCCESS);
  assert(memcached_server_add(memc, server.hostname, server.port) == MEMCACHED_INVALID_HOST_PROTOCOL);
  return TEST_SUCCESS;
}

/* Make sure that I cant add a udp server to a tcp client */
static test_return add_udp_server_tcp_client_test(memcached_st *memc)
{
  memcached_server_st server;
  memcached_server_clone(&server, &memc->hosts[0]);
  assert(memcached_server_remove(&(memc->hosts[0])) == MEMCACHED_SUCCESS);

  memcached_st tcp_client;
  memcached_create(&tcp_client);
  assert(memcached_server_add_udp(&tcp_client, server.hostname, server.port) == MEMCACHED_INVALID_HOST_PROTOCOL);
  return TEST_SUCCESS;
}

static test_return set_udp_behavior_test(memcached_st *memc)
{

  memcached_quit(memc);
  memc->number_of_hosts= 0;
  run_distribution(memc);
  assert(memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_USE_UDP, 1) == MEMCACHED_SUCCESS);
  assert(memc->flags & MEM_USE_UDP);
  assert(memc->flags & MEM_NOREPLY);;

  assert(memc->number_of_hosts == 0);

  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_USE_UDP,0);
  assert(!(memc->flags & MEM_USE_UDP));
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NOREPLY,0);
  assert(!(memc->flags & MEM_NOREPLY));
  return TEST_SUCCESS;
}

static test_return udp_set_test(memcached_st *memc)
{
  unsigned int x= 0;
  unsigned int num_iters= 1025; //request id rolls over at 1024
  for (x= 0; x < num_iters;x++)
  {
    memcached_return rc;
    char *key= "foo";
    char *value= "when we sanitize";
    uint16_t *expected_ids= get_udp_request_ids(memc);
    unsigned int server_key= memcached_generate_hash(memc,key,strlen(key));
    size_t init_offset= memc->hosts[server_key].write_buffer_offset;
    rc= memcached_set(memc, key, strlen(key),
                      value, strlen(value),
                      (time_t)0, (uint32_t)0);
    assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);
    /** NB, the check below assumes that if new write_ptr is less than
     *  the original write_ptr that we have flushed. For large payloads, this
     *  maybe an invalid assumption, but for the small payload we have it is OK
     */
    if (rc == MEMCACHED_SUCCESS ||
            memc->hosts[server_key].write_buffer_offset < init_offset)
      increment_request_id(&expected_ids[server_key]);

    if (rc == MEMCACHED_SUCCESS)
    {
      assert(memc->hosts[server_key].write_buffer_offset == UDP_DATAGRAM_HEADER_LENGTH);
    }
    else
    {
      assert(memc->hosts[server_key].write_buffer_offset != UDP_DATAGRAM_HEADER_LENGTH);
      assert(memc->hosts[server_key].write_buffer_offset <= MAX_UDP_DATAGRAM_LENGTH);
    }
    assert(post_udp_op_check(memc,expected_ids) == TEST_SUCCESS);
  }
  return TEST_SUCCESS;
}

static test_return udp_buffered_set_test(memcached_st *memc)
{
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BUFFER_REQUESTS, 1);
  return udp_set_test(memc);
}

static test_return udp_set_too_big_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "bar";
  char value[MAX_UDP_DATAGRAM_LENGTH];
  uint16_t *expected_ids= get_udp_request_ids(memc);
  rc= memcached_set(memc, key, strlen(key),
                    value, MAX_UDP_DATAGRAM_LENGTH,
                    (time_t)0, (uint32_t)0);
  assert(rc == MEMCACHED_WRITE_FAILURE);
  return post_udp_op_check(memc,expected_ids);
}

static test_return udp_delete_test(memcached_st *memc)
{
  unsigned int x= 0;
  unsigned int num_iters= 1025; //request id rolls over at 1024
  for (x= 0; x < num_iters;x++)
  {
    memcached_return rc;
    char *key= "foo";
    uint16_t *expected_ids=get_udp_request_ids(memc);
    unsigned int server_key= memcached_generate_hash(memc, key, strlen(key));
    size_t init_offset= memc->hosts[server_key].write_buffer_offset;
    rc= memcached_delete(memc, key, strlen(key), 0);
    assert(rc == MEMCACHED_SUCCESS || rc == MEMCACHED_BUFFERED);
    if (rc == MEMCACHED_SUCCESS || memc->hosts[server_key].write_buffer_offset < init_offset)
      increment_request_id(&expected_ids[server_key]);
    if (rc == MEMCACHED_SUCCESS)
      assert(memc->hosts[server_key].write_buffer_offset == UDP_DATAGRAM_HEADER_LENGTH);
    else
    {
      assert(memc->hosts[server_key].write_buffer_offset != UDP_DATAGRAM_HEADER_LENGTH);
      assert(memc->hosts[server_key].write_buffer_offset <= MAX_UDP_DATAGRAM_LENGTH);
    }
    assert(post_udp_op_check(memc,expected_ids) == TEST_SUCCESS);
  }
  return TEST_SUCCESS;
}

static test_return udp_buffered_delete_test(memcached_st *memc)
{
  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BUFFER_REQUESTS, 1);
  return udp_delete_test(memc);
}

static test_return udp_verbosity_test(memcached_st *memc)
{
  memcached_return rc;
  uint16_t *expected_ids= get_udp_request_ids(memc);
  unsigned int x;
  for (x= 0; x < memc->number_of_hosts;x++)
    increment_request_id(&expected_ids[x]);

  rc= memcached_verbosity(memc,3);
  assert(rc == MEMCACHED_SUCCESS);
  return post_udp_op_check(memc,expected_ids);
}

static test_return udp_quit_test(memcached_st *memc)
{
  uint16_t *expected_ids= get_udp_request_ids(memc);
  memcached_quit(memc);
  return post_udp_op_check(memc, expected_ids);
}

static test_return udp_flush_test(memcached_st *memc)
{
  memcached_return rc;
  uint16_t *expected_ids= get_udp_request_ids(memc);
  unsigned int x;
  for (x= 0; x < memc->number_of_hosts;x++)
    increment_request_id(&expected_ids[x]);

  rc= memcached_flush(memc,0);
  assert(rc == MEMCACHED_SUCCESS);
  return post_udp_op_check(memc,expected_ids);
}

static test_return udp_incr_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "incr";
  char *value= "1";
  rc= memcached_set(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);

  assert(rc == MEMCACHED_SUCCESS);
  uint16_t *expected_ids= get_udp_request_ids(memc);
  unsigned int server_key= memcached_generate_hash(memc, key, strlen(key));
  increment_request_id(&expected_ids[server_key]);
  uint64_t newvalue;
  rc= memcached_increment(memc, key, strlen(key), 1, &newvalue);
  assert(rc == MEMCACHED_SUCCESS);
  return post_udp_op_check(memc, expected_ids);
}

static test_return udp_decr_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "decr";
  char *value= "1";
  rc= memcached_set(memc, key, strlen(key),
                    value, strlen(value),
                    (time_t)0, (uint32_t)0);

  assert(rc == MEMCACHED_SUCCESS);
  uint16_t *expected_ids= get_udp_request_ids(memc);
  unsigned int server_key= memcached_generate_hash(memc, key, strlen(key));
  increment_request_id(&expected_ids[server_key]);
  uint64_t newvalue;
  rc= memcached_decrement(memc, key, strlen(key), 1, &newvalue);
  assert(rc == MEMCACHED_SUCCESS);
  return post_udp_op_check(memc, expected_ids);
}


static test_return udp_stat_test(memcached_st *memc)
{
  memcached_stat_st * rv= NULL;
  memcached_return rc;
  char args[]= "";
  uint16_t *expected_ids = get_udp_request_ids(memc);
  rv = memcached_stat(memc, args, &rc);
  free(rv);
  assert(rc == MEMCACHED_NOT_SUPPORTED);
  return post_udp_op_check(memc, expected_ids);
}

static test_return udp_version_test(memcached_st *memc)
{
  memcached_return rc;
  uint16_t *expected_ids = get_udp_request_ids(memc);
  rc = memcached_version(memc);
  assert(rc == MEMCACHED_NOT_SUPPORTED);
  return post_udp_op_check(memc, expected_ids);
}

static test_return udp_get_test(memcached_st *memc)
{
  memcached_return rc;
  char *key= "foo";
  size_t vlen;
  uint16_t *expected_ids = get_udp_request_ids(memc);
  char *val= memcached_get(memc, key, strlen(key), &vlen, (uint32_t)0, &rc);
  assert(rc == MEMCACHED_NOT_SUPPORTED);
  assert(val == NULL);
  return post_udp_op_check(memc, expected_ids);
}

static test_return udp_mixed_io_test(memcached_st *memc)
{
  test_st current_op;
  test_st mixed_io_ops [] ={
    {"udp_set_test", 0, udp_set_test},
    {"udp_set_too_big_test", 0, udp_set_too_big_test},
    {"udp_delete_test", 0, udp_delete_test},
    {"udp_verbosity_test", 0, udp_verbosity_test},
    {"udp_quit_test", 0, udp_quit_test},
    {"udp_flush_test", 0, udp_flush_test},
    {"udp_incr_test", 0, udp_incr_test},
    {"udp_decr_test", 0, udp_decr_test},
    {"udp_version_test", 0, udp_version_test}
  };
  unsigned int x= 0;
  for (x= 0; x < 500; x++)
  {
    current_op= mixed_io_ops[random() % 9];
    assert(current_op.function(memc) == TEST_SUCCESS);
  }
  return TEST_SUCCESS;
}

static test_return hsieh_avaibility_test (memcached_st *memc)
{
  memcached_return expected_rc= MEMCACHED_FAILURE;
#ifdef HAVE_HSIEH_HASH
  expected_rc= MEMCACHED_SUCCESS;
#endif
  memcached_return rc= memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_HASH,
                                            (uint64_t)MEMCACHED_HASH_HSIEH);
  assert(rc == expected_rc);
  return TEST_SUCCESS;
}

static char *list[]=
{
  "apple",
  "beat",
  "carrot",
  "daikon",
  "eggplant",
  "flower",
  "green",
  "hide",
  "ick",
  "jack",
  "kick",
  "lime",
  "mushrooms",
  "nectarine",
  "orange",
  "peach",
  "quant",
  "ripen",
  "strawberry",
  "tang",
  "up",
  "volumne",
  "when",
  "yellow",
  "zip",
  NULL
};

static test_return md5_run (memcached_st *memc __attribute__((unused)))
{
  uint32_t x;
  char **ptr;
  uint32_t values[]= {  3195025439, 2556848621, 3724893440, 3332385401, 245758794, 2550894432,
                        121710495, 3053817768, 1250994555, 1862072655, 2631955953, 2951528551,
                        1451250070, 2820856945, 2060845566, 3646985608, 2138080750, 217675895,
                        2230934345, 1234361223, 3968582726, 2455685270, 1293568479, 199067604,
                        2042482093 };


  for (ptr= list, x= 0; *ptr; ptr++, x++)
  {
    uint32_t hash_val;

    hash_val= memcached_generate_hash_value(*ptr, strlen(*ptr), MEMCACHED_HASH_MD5);
    assert(values[x] == hash_val);
  }

  return TEST_SUCCESS;
}

static test_return crc_run (memcached_st *memc __attribute__((unused)))
{
  uint32_t x;
  char **ptr;
  uint32_t values[]= {  10542, 22009, 14526, 19510, 19432, 10199, 20634, 9369, 11511, 10362,
                        7893, 31289, 11313, 9354, 7621, 30628, 15218, 25967, 2695, 9380,
                        17300, 28156, 9192, 20484, 16925 };

  for (ptr= list, x= 0; *ptr; ptr++, x++)
  {
    uint32_t hash_val;

    hash_val= memcached_generate_hash_value(*ptr, strlen(*ptr), MEMCACHED_HASH_CRC);
    assert(values[x] == hash_val);
  }

  return TEST_SUCCESS;
}

static test_return fnv1_64_run (memcached_st *memc __attribute__((unused)))
{
  uint32_t x;
  char **ptr;
  uint32_t values[]= {  473199127, 4148981457, 3971873300, 3257986707, 1722477987, 2991193800,
                        4147007314, 3633179701, 1805162104, 3503289120, 3395702895, 3325073042,
                        2345265314, 3340346032, 2722964135, 1173398992, 2815549194, 2562818319,
                        224996066, 2680194749, 3035305390, 246890365, 2395624193, 4145193337,
                        1801941682 };

  for (ptr= list, x= 0; *ptr; ptr++, x++)
  {
    uint32_t hash_val;

    hash_val= memcached_generate_hash_value(*ptr, strlen(*ptr), MEMCACHED_HASH_FNV1_64);
    assert(values[x] == hash_val);
  }

  return TEST_SUCCESS;
}

static test_return fnv1a_64_run (memcached_st *memc __attribute__((unused)))
{
  uint32_t x;
  char **ptr;
  uint32_t values[]= {  1488911807, 2500855813, 1510099634, 1390325195, 3647689787, 3241528582,
                        1669328060, 2604311949, 734810122, 1516407546, 560948863, 1767346780,
                        561034892, 4156330026, 3716417003, 3475297030, 1518272172, 227211583,
                        3938128828, 126112909, 3043416448, 3131561933, 1328739897, 2455664041,
                        2272238452 };

  for (ptr= list, x= 0; *ptr; ptr++, x++)
  {
    uint32_t hash_val;

    hash_val= memcached_generate_hash_value(*ptr, strlen(*ptr), MEMCACHED_HASH_FNV1A_64);
    assert(values[x] == hash_val);
  }

  return TEST_SUCCESS;
}

static test_return fnv1_32_run (memcached_st *memc __attribute__((unused)))
{
  uint32_t x;
  char **ptr;
  uint32_t values[]= {  67176023, 1190179409, 2043204404, 3221866419, 2567703427, 3787535528, 4147287986,
                        3500475733, 344481048, 3865235296, 2181839183, 119581266, 510234242, 4248244304,
                        1362796839, 103389328, 1449620010, 182962511, 3554262370, 3206747549, 1551306158,
                        4127558461, 1889140833, 2774173721, 1180552018 };


  for (ptr= list, x= 0; *ptr; ptr++, x++)
  {
    uint32_t hash_val;

    hash_val= memcached_generate_hash_value(*ptr, strlen(*ptr), MEMCACHED_HASH_FNV1_32);
    assert(values[x] == hash_val);
  }

  return TEST_SUCCESS;
}

static test_return fnv1a_32_run (memcached_st *memc __attribute__((unused)))
{
  uint32_t x;
  char **ptr;
  uint32_t values[]= {  280767167, 2421315013, 3072375666, 855001899, 459261019, 3521085446, 18738364,
                        1625305005, 2162232970, 777243802, 3323728671, 132336572, 3654473228, 260679466,
                        1169454059, 2698319462, 1062177260, 235516991, 2218399068, 405302637, 1128467232,
                        3579622413, 2138539289, 96429129, 2877453236 };

  for (ptr= list, x= 0; *ptr; ptr++, x++)
  {
    uint32_t hash_val;

    hash_val= memcached_generate_hash_value(*ptr, strlen(*ptr), MEMCACHED_HASH_FNV1A_32);
    assert(values[x] == hash_val);
  }

  return TEST_SUCCESS;
}

static test_return hsieh_run (memcached_st *memc __attribute__((unused)))
{
  uint32_t x;
  char **ptr;
#ifdef HAVE_HSIEH_HASH
  uint32_t values[]= {  3738850110, 3636226060, 3821074029, 3489929160, 3485772682, 80540287,
                        1805464076, 1895033657, 409795758, 979934958, 3634096985, 1284445480,
                        2265380744, 707972988, 353823508, 1549198350, 1327930172, 9304163,
                        4220749037, 2493964934, 2777873870, 2057831732, 1510213931, 2027828987,
                        3395453351 };
#else
  uint32_t values[]= {  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
#endif

  for (ptr= list, x= 0; *ptr; ptr++, x++)
  {
    uint32_t hash_val;

    hash_val= memcached_generate_hash_value(*ptr, strlen(*ptr), MEMCACHED_HASH_HSIEH);
    assert(values[x] == hash_val);
  }

  return TEST_SUCCESS;
}

static test_return murmur_run (memcached_st *memc __attribute__((unused)))
{
  uint32_t x;
  char **ptr;
  uint32_t values[]= { 473199127, 4148981457, 3971873300, 3257986707, 1722477987, 2991193800,
                        4147007314, 3633179701, 1805162104, 3503289120, 3395702895, 3325073042,
                        2345265314, 3340346032, 2722964135, 1173398992, 2815549194, 2562818319,
                        224996066, 2680194749, 3035305390, 246890365, 2395624193, 4145193337,
                        1801941682 };

  for (ptr= list, x= 0; *ptr; ptr++, x++)
  {
    uint32_t hash_val;

    hash_val= memcached_generate_hash_value(*ptr, strlen(*ptr), MEMCACHED_HASH_FNV1_64);
    assert(values[x] == hash_val);
  }

  return TEST_SUCCESS;
}

static test_return jenkins_run (memcached_st *memc __attribute__((unused)))
{
  uint32_t x;
  char **ptr;
  uint32_t values[]= {  1442444624, 4253821186, 1885058256, 2120131735, 3261968576, 3515188778,
                        4232909173, 4288625128, 1812047395, 3689182164, 2502979932, 1214050606,
                        2415988847, 1494268927, 1025545760, 3920481083, 4153263658, 3824871822,
                        3072759809, 798622255, 3065432577, 1453328165, 2691550971, 3408888387,
                        2629893356 };


  for (ptr= list, x= 0; *ptr; ptr++, x++)
  {
    uint32_t hash_val;

    hash_val= memcached_generate_hash_value(*ptr, strlen(*ptr), MEMCACHED_HASH_JENKINS);
    assert(values[x] == hash_val);
  }

  return TEST_SUCCESS;
}

test_st udp_setup_server_tests[] ={
  {"set_udp_behavior_test", 0, set_udp_behavior_test},
  {"add_tcp_server_udp_client_test", 0, add_tcp_server_udp_client_test},
  {"add_udp_server_tcp_client_test", 0, add_udp_server_tcp_client_test},
  {0, 0, 0}
};

test_st upd_io_tests[] ={
  {"udp_set_test", 0, udp_set_test},
  {"udp_buffered_set_test", 0, udp_buffered_set_test},
  {"udp_set_too_big_test", 0, udp_set_too_big_test},
  {"udp_delete_test", 0, udp_delete_test},
  {"udp_buffered_delete_test", 0, udp_buffered_delete_test},
  {"udp_verbosity_test", 0, udp_verbosity_test},
  {"udp_quit_test", 0, udp_quit_test},
  {"udp_flush_test", 0, udp_flush_test},
  {"udp_incr_test", 0, udp_incr_test},
  {"udp_decr_test", 0, udp_decr_test},
  {"udp_stat_test", 0, udp_stat_test},
  {"udp_version_test", 0, udp_version_test},
  {"udp_get_test", 0, udp_get_test},
  {"udp_mixed_io_test", 0, udp_mixed_io_test},
  {0, 0, 0}
};

/* Clean the server before beginning testing */
test_st tests[] ={
  {"flush", 0, flush_test },
  {"init", 0, init_test },
  {"allocation", 0, allocation_test },
  {"server_list_null_test", 0, server_list_null_test},
  {"server_unsort", 0, server_unsort_test},
  {"server_sort", 0, server_sort_test},
  {"server_sort2", 0, server_sort2_test},
  {"clone_test", 0, clone_test },
  {"connection_test", 0, connection_test},
  {"callback_test", 0, callback_test},
  {"behavior_test", 0, behavior_test},
  {"error", 0, error_test },
  {"set", 0, set_test },
  {"set2", 0, set_test2 },
  {"set3", 0, set_test3 },
  {"dump", 1, dump_test},
  {"add", 1, add_test },
  {"replace", 1, replace_test },
  {"delete", 1, delete_test },
  {"get", 1, get_test },
  {"get2", 0, get_test2 },
  {"get3", 0, get_test3 },
  {"get4", 0, get_test4 },
  {"partial mget", 0, get_test5 },
  {"stats_servername", 0, stats_servername_test },
  {"increment", 0, increment_test },
  {"increment_with_initial", 1, increment_with_initial_test },
  {"decrement", 0, decrement_test },
  {"decrement_with_initial", 1, decrement_with_initial_test },
  {"quit", 0, quit_test },
  {"mget", 1, mget_test },
  {"mget_result", 1, mget_result_test },
  {"mget_result_alloc", 1, mget_result_alloc_test },
  {"mget_result_function", 1, mget_result_function },
  {"get_stats", 0, get_stats },
  {"add_host_test", 0, add_host_test },
  {"add_host_test_1", 0, add_host_test1 },
  {"get_stats_keys", 0, get_stats_keys },
  {"behavior_test", 0, get_stats_keys },
  {"callback_test", 0, get_stats_keys },
  {"version_string_test", 0, version_string_test},
  {"bad_key", 1, bad_key_test },
  {"memcached_server_cursor", 1, memcached_server_cursor_test },
  {"read_through", 1, read_through },
  {"delete_through", 1, delete_through },
  {"noreply", 1, noreply_test},
  {"analyzer", 1, analyzer_test},
#ifdef HAVE_LIBMEMCACHEDUTIL
  {"connectionpool", 1, connection_pool_test },
#endif
  {0, 0, 0}
};

test_st async_tests[] ={
  {"add", 1, add_wrapper },
  {0, 0, 0}
};

test_st string_tests[] ={
  {"string static with null", 0, string_static_null },
  {"string alloc with null", 0, string_alloc_null },
  {"string alloc with 1K", 0, string_alloc_with_size },
  {"string alloc with malloc failure", 0, string_alloc_with_size_toobig },
  {"string append", 0, string_alloc_append },
  {"string append failure (too big)", 0, string_alloc_append_toobig },
  {0, 0, 0}
};

test_st result_tests[] ={
  {"result static", 0, result_static},
  {"result alloc", 0, result_alloc},
  {0, 0, 0}
};

test_st version_1_2_3[] ={
  {"append", 0, append_test },
  {"prepend", 0, prepend_test },
  {"cas", 0, cas_test },
  {"cas2", 0, cas2_test },
  {"append_binary", 0, append_binary_test },
  {0, 0, 0}
};

test_st user_tests[] ={
  {"user_supplied_bug1", 0, user_supplied_bug1 },
  {"user_supplied_bug2", 0, user_supplied_bug2 },
  {"user_supplied_bug3", 0, user_supplied_bug3 },
  {"user_supplied_bug4", 0, user_supplied_bug4 },
  {"user_supplied_bug5", 1, user_supplied_bug5 },
  {"user_supplied_bug6", 1, user_supplied_bug6 },
  {"user_supplied_bug7", 1, user_supplied_bug7 },
  {"user_supplied_bug8", 1, user_supplied_bug8 },
  {"user_supplied_bug9", 1, user_supplied_bug9 },
  {"user_supplied_bug10", 1, user_supplied_bug10 },
  {"user_supplied_bug11", 1, user_supplied_bug11 },
  {"user_supplied_bug12", 1, user_supplied_bug12 },
  {"user_supplied_bug13", 1, user_supplied_bug13 },
  {"user_supplied_bug14", 1, user_supplied_bug14 },
  {"user_supplied_bug15", 1, user_supplied_bug15 },
  {"user_supplied_bug16", 1, user_supplied_bug16 },
#ifndef __sun
  /*
  ** It seems to be something weird with the character sets..
  ** value_fetch is unable to parse the value line (iscntrl "fails"), so I
  ** guess I need to find out how this is supposed to work.. Perhaps I need
  ** to run the test in a specific locale (I tried zh_CN.UTF-8 without success,
  ** so just disable the code for now...).
  */
  {"user_supplied_bug17", 1, user_supplied_bug17 },
#endif
  {"user_supplied_bug18", 1, user_supplied_bug18 },
  {"user_supplied_bug19", 1, user_supplied_bug19 },
  {"user_supplied_bug20", 1, user_supplied_bug20 },
  {0, 0, 0}
};

test_st generate_tests[] ={
  {"generate_pairs", 1, generate_pairs },
  {"generate_data", 1, generate_data },
  {"get_read", 0, get_read },
  {"delete_generate", 0, delete_generate },
  {"generate_buffer_data", 1, generate_buffer_data },
  {"delete_buffer", 0, delete_buffer_generate},
  {"generate_data", 1, generate_data },
  {"mget_read", 0, mget_read },
  {"mget_read_result", 0, mget_read_result },
  {"mget_read_function", 0, mget_read_function },
  {"cleanup", 1, cleanup_pairs },
  {"generate_large_pairs", 1, generate_large_pairs },
  {"generate_data", 1, generate_data },
  {"generate_buffer_data", 1, generate_buffer_data },
  {"cleanup", 1, cleanup_pairs },
  {0, 0, 0}
};

test_st consistent_tests[] ={
  {"generate_pairs", 1, generate_pairs },
  {"generate_data", 1, generate_data },
  {"get_read", 0, get_read_count },
  {"cleanup", 1, cleanup_pairs },
  {0, 0, 0}
};

test_st consistent_weighted_tests[] ={
  {"generate_pairs", 1, generate_pairs },
  {"generate_data", 1, generate_data_with_stats },
  {"get_read", 0, get_read_count },
  {"cleanup", 1, cleanup_pairs },
  {0, 0, 0}
};

test_st hsieh_availability[] ={
  {"hsieh_avaibility_test",0,hsieh_avaibility_test},
  {0, 0, 0}
};

test_st ketama_auto_eject_hosts[] ={
  {"auto_eject_hosts", 1, auto_eject_hosts },
  {0, 0, 0}
};

test_st hash_tests[] ={
  {"md5", 0, md5_run },
  {"crc", 0, crc_run },
  {"fnv1_64", 0, fnv1_64_run },
  {"fnv1a_64", 0, fnv1a_64_run },
  {"fnv1_32", 0, fnv1_32_run },
  {"fnv1a_32", 0, fnv1a_32_run },
  {"hsieh", 0, hsieh_run },
  {"murmur", 0, murmur_run },
  {"jenkis", 0, jenkins_run },
  {0, 0, 0}
};

collection_st collection[] ={
  {"hsieh_availability",0,0,hsieh_availability},
  {"udp_setup", init_udp, 0, udp_setup_server_tests},
  {"udp_io", init_udp, 0, upd_io_tests},
  {"udp_binary_io", binary_init_udp, 0, upd_io_tests},
  {"block", 0, 0, tests},
  {"binary", pre_binary, 0, tests},
  {"nonblock", pre_nonblock, 0, tests},
  {"nodelay", pre_nodelay, 0, tests},
  {"settimer", pre_settimer, 0, tests},
  {"md5", pre_md5, 0, tests},
  {"crc", pre_crc, 0, tests},
  {"hsieh", pre_hsieh, 0, tests},
  {"jenkins", pre_jenkins, 0, tests},
  {"fnv1_64", pre_hash_fnv1_64, 0, tests},
  {"fnv1a_64", pre_hash_fnv1a_64, 0, tests},
  {"fnv1_32", pre_hash_fnv1_32, 0, tests},
  {"fnv1a_32", pre_hash_fnv1a_32, 0, tests},
  {"ketama", pre_behavior_ketama, 0, tests},
  {"ketama_auto_eject_hosts", pre_behavior_ketama, 0, ketama_auto_eject_hosts},
  {"unix_socket", pre_unix_socket, 0, tests},
  {"unix_socket_nodelay", pre_nodelay, 0, tests},
  {"poll_timeout", poll_timeout, 0, tests},
  {"gets", enable_cas, 0, tests},
  {"consistent", enable_consistent, 0, tests},
  {"memory_allocators", set_memory_alloc, 0, tests},
  {"prefix", set_prefix, 0, tests},
  {"version_1_2_3", check_for_1_2_3, 0, version_1_2_3},
  {"string", 0, 0, string_tests},
  {"result", 0, 0, result_tests},
  {"async", pre_nonblock, 0, async_tests},
  {"async_binary", pre_nonblock_binary, 0, async_tests},
  {"user", 0, 0, user_tests},
  {"generate", 0, 0, generate_tests},
  {"generate_hsieh", pre_hsieh, 0, generate_tests},
  {"generate_ketama", pre_behavior_ketama, 0, generate_tests},
  {"generate_hsieh_consistent", enable_consistent, 0, generate_tests},
  {"generate_md5", pre_md5, 0, generate_tests},
  {"generate_murmur", pre_murmur, 0, generate_tests},
  {"generate_jenkins", pre_jenkins, 0, generate_tests},
  {"generate_nonblock", pre_nonblock, 0, generate_tests},
  {"consistent_not", 0, 0, consistent_tests},
  {"consistent_ketama", pre_behavior_ketama, 0, consistent_tests},
  {"consistent_ketama_weighted", pre_behavior_ketama_weighted, 0, consistent_weighted_tests},
  {"test_hashes", 0, 0, hash_tests},
  {0, 0, 0, 0}
};

#define SERVERS_TO_CREATE 5

/* Prototypes for functions we will pass to test framework */
void *world_create(void);
void world_destroy(void *p);

void *world_create(void)
{
  server_startup_st *construct;

  construct= (server_startup_st *)malloc(sizeof(server_startup_st));
  memset(construct, 0, sizeof(server_startup_st));
  construct->count= SERVERS_TO_CREATE;
  construct->udp= 0;
  server_startup(construct);

  return construct;
}


void world_destroy(void *p)
{
  server_startup_st *construct= (server_startup_st *)p;
  memcached_server_st *servers= (memcached_server_st *)construct->servers;
  memcached_server_list_free(servers);

  server_shutdown(construct);
  free(construct);
}

void get_world(world_st *world)
{
  world->collections= collection;
  world->create= world_create;
  world->destroy= world_destroy;
}
