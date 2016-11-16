#include "task.h"
#include "task_table.h"

#include "global_scheduler_algorithm.h"

/** The part of the global scheduler state that is maintained by the scheduling
 *  algorithm. */
struct scheduling_algorithm_state {
};

scheduling_algorithm_state *make_scheduling_algorithm_state(void) {
  scheduling_algorithm_state *state =
      malloc(sizeof(scheduling_algorithm_state));
  return state;
}

void free_scheduling_algorithm_state(scheduling_algorithm_state *s) {
  free(s);
}

void handle_task_waiting(task *original_task, void *user_context) {
  global_scheduler_state *state = (global_scheduler_state *) user_context;
  printf("XXX: WOOHOOOO!!!!!\n");
  if (utarray_len(state->local_schedulers) > 0) {
    local_scheduler *local_sched = (local_scheduler *) utarray_eltptr(state->local_schedulers, 0);
    assign_task_to_local_scheduler(state, original_task, local_sched->id);
  }
}

void handle_object_available(object_id obj_id) {
  /* Do nothing for now. */
}

void handle_object_unavailable(object_id obj_id) {
  /* Do nothing for now. */
}

void handle_local_scheduler_heartbeat(void) {
  /* Do nothing for now. */
}

void handle_new_local_scheduler(client_id client_id, void *user_context) {
  global_scheduler_state *state = (global_scheduler_state *) user_context;
  local_scheduler local_scheduler = {.id = client_id};
  utarray_push_back(state->local_schedulers, &local_scheduler);
  printf("GOT A NEW LOCAL SCHEDULER, CLIENT ID ");
  for (int i = 0; i < 20; ++i) {
    printf("%c", client_id.id[i]);
  }
  printf("\n");
}
