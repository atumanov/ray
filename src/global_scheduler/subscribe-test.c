#include <getopt.h>
#include <signal.h>
#include <stdlib.h>

#include "event_loop.h"
#include "net.h"
#include "state/db_client_table.h"
#include "state/object_table.h"
#include "state/task_table.h"

typedef void (*generic_fp)(void);

typedef struct {
  /** The global scheduler event loop. */
  event_loop *loop;
  /** The global state store database. */
  db_handle *db;
} global_scheduler_state;

global_scheduler_state * g_state = NULL;

global_scheduler_state *init_global_scheduler(event_loop *loop,
                                              const char *redis_addr,
                                              int redis_port) {
  global_scheduler_state *state = malloc(sizeof(global_scheduler_state));
  memset(state, 0, sizeof(global_scheduler_state));
  state->db = db_connect(redis_addr, redis_port, "global_scheduler", "", -1);
  db_attach(state->db, loop, false);
  return state;
}

void signal_handler(int signal) {
  if (signal == SIGTERM) {
    if (g_state != NULL) {
      free(g_state);
    }
    exit(0);
  }
}

void object_table_subscribe_callback(
    object_id object_id,
    int64_t data_size,
    int manager_count,
    OWNER const char *manager_vector[],
    void *user_context) {

  printf("XXX: object table subscribe callback called \n");
}

void process_task_waiting(task *task, void *user_context) {
  printf("XXX: task table subscribe callback fired \n");
}

void process_new_db_client(db_client_id db_client_id,
                           const char *client_type,
                           const char *aux_address,
                           void *user_context) {
  printf("XXX: new db client callback called\n");
}

void start_server(const char *redis_addr, int redis_port) {
  event_loop *loop = event_loop_create();
  g_state = init_global_scheduler(loop, redis_addr, redis_port);
  /* Generic retry information for notification subscriptions. */
  retry_info retry = {
      .num_retries = 0, .timeout = 100, .fail_callback = NULL,
  };

  generic_fp redissub_vptrtab[3] = {
      (generic_fp)db_client_table_subscribe,
      (generic_fp)task_table_subscribe,
      (generic_fp)object_table_subscribe_to_notifications };

  db_client_table_subscribe(g_state->db, process_new_db_client,
                            (void *) g_state, &retry, NULL, NULL);

  task_table_subscribe(g_state->db, NIL_ID, TASK_STATUS_WAITING,
                       process_task_waiting, (void *) g_state, &retry, NULL,
                       NULL);

  object_table_subscribe_to_notifications(g_state->db,
                                          true,
                                          object_table_subscribe_callback,
                                          g_state, &retry, NULL, NULL);
  /* Start the event loop. */
  event_loop_run(loop);
}

int main(int argc, char *argv[]) {
  signal(SIGTERM, signal_handler);
  /* IP address and port of redis. */
  char *redis_addr_port = NULL;
  int c;
  while ((c = getopt(argc, argv, "s:m:h:p:r:")) != -1) {
    switch (c) {
    case 'r':
      redis_addr_port = optarg;
      break;
    default:
      LOG_ERROR("unknown option %c", c);
      exit(-1);
    }
  }
  char redis_addr[16];
  int redis_port;
  if (!redis_addr_port ||
      parse_ip_addr_port(redis_addr_port, redis_addr, &redis_port) == -1) {
    LOG_ERROR(
        "need to specify redis address like 127.0.0.1:6379 with -r switch");
    exit(-1);
  }
  start_server(redis_addr, redis_port);
}
