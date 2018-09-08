/*
Copyright 2015 Google Inc. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#include "graphd/graphd.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

/**
 * @brief Stack resource method: free.
 *
 * 	Free the stack and the items it contains.
 * 	This is the asynchronous alternative to graphd_stack_free().
 *
 * @param resource_manager_data	opaque application handle for all
 *		resources in this manager, ignored
 * @param resoure_data	the graphd_stack, cast to void *
 */

static void graphd_stack_resource_free(void *resource_manager_data,
                                       void *resource_data) {
  graphd_stack *stack = resource_data;
  cm_resource *r;

  while ((r = cm_resource_top(&stack->s_resource_manager)) != NULL)
    cm_resource_free(r);
}

/**
 * @brief Stack resource method: list.
 *
 * @param log_data	a cl_handle, cast to void *
 * @param resource_manager_data	opaque application handle for all
 *		resources in this manager, ignored
 * @param resoure_data	the graphd_stack, cast to void *
 */
static void graphd_stack_resource_list(void *log_data,
                                       void *resource_manager_data,
                                       void *resource_data) {
  cl_handle *cl = log_data;
  graphd_stack *stack = resource_data;
  cm_resource *r;

  r = cm_resource_top(&stack->s_resource_manager);

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd-stack @ %p%s", (void *)stack,
         r != NULL ? " top:" : "");

  if (r != NULL) cm_resource_list(r, log_data);
}

/**
 * @brief Stack resource type
 */
static cm_resource_type graphd_stack_resource_type = {
    "graphd-stack", graphd_stack_resource_free, graphd_stack_resource_list};

/**
 * @brief Push a new context on the stack.
 *
 *  This call, if successful, transfers the ownership
 *  of one of the links to the resource in context->sc_resource
 *  to the stack's responsibility.
 *
 *  The resource will be free'ed if
 *	- the stack as a whole gets free'd
 *	- the frame gets popped.
 *
 * @param stack		push on this
 * @param context	add this context
 * @param resource_type	type for the context
 * @param run		function to call when the context runs
 *
 * @return 0 on success
 * @return ENOMEM on allocation error
 */
void graphd_stack_push(graphd_stack *stack, graphd_stack_context *context,
                       cm_resource_type const *resource_type,
                       graphd_stack_type const *type) {
  cm_resource_alloc(&stack->s_resource_manager, &context->sc_resource,
                    resource_type, context);
  context->sc_type = type;
  context->sc_run = type->sct_run_default;
}

/**
 * @brief Return the topmost element of the stack.
 *
 * @param stack		NULL or the stack in question
 * @return NULL if the stack is empty or NULL, otherwise the top context.
 */
graphd_stack_context *graphd_stack_top(graphd_stack *stack) {
  return stack ? (graphd_stack_context *)cm_resource_top(
                     &stack->s_resource_manager)
               : NULL;
}

/**
 * @brief Run the topmost element of the stack.
 *
 * @param stack		stack to run
 *
 * @return the result of the topmost run function
 * @return GRAPHD_ERR_NO if there is no topmost element
 */
int graphd_stack_run(graphd_stack *stack) {
  graphd_stack_context *sc;

  sc = (graphd_stack_context *)cm_resource_top(&stack->s_resource_manager);
  if (sc == NULL) return GRAPHD_ERR_NO;

  if (sc->sc_suspended) {
    int err = (*sc->sc_type->sct_unsuspend)(stack, sc);
    if (err != 0) return err;
  }
  return (*sc->sc_run)(stack, sc);
}

/**
 * @brief Run the topmost element of the stack until a certain time.
 *
 * @param stack		stack to run
 * @param deadline	run until then
 *
 * @return the result of the topmost run function if nonzero
 * @return GRAPHD_ERR_MORE if we ran out of time
 * @return 0 if the stack emptied out.
 */
int graphd_stack_run_until_deadline(graphd_request *greq, graphd_stack *stack,
                                    unsigned long long deadline) {
  srv_handle *srv = graphd_request_graphd(greq)->g_srv;
  unsigned long long now = 0;
  int err;

  unsigned long long start_ticks;
  unsigned long long end_ticks;

  start_ticks = graphd_request_timer_get_tsc();

  /* Run at least once, until you're done or past deadline.
   */
  do {
    err = graphd_stack_run(stack);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) err = 0;

      now = srv_msclock(srv);
      break;
    }

    /*
     * If we have access to the tsc, check it and only bother
     * calling srv_msclock once every 5 million ticks
     * (a little under 1 ms).
     *
     * For some queries, srv_msclock can take more time than
     * graphd_stack_run  to execute!
     *
     * Do not assume that the TSC is always increasing.
     * (Not all chip/OS combinations provide a working TSC)
     */
    end_ticks = graphd_request_timer_get_tsc();
    if (start_ticks + 5000000ull > end_ticks && end_ticks > start_ticks)
      continue;

    start_ticks = graphd_request_timer_get_tsc();
    now = srv_msclock(srv);

  } while (!SRV_PAST_DEADLINE(now, deadline));

  /*  If we're way past deadline, make a note.
   */
  if (deadline != 0 && now > deadline && now - deadline > 5) {
    cl_handle *const cl = graphd_request_cl(greq);
    cl_loglevel lev;

    /*  Very large writes and restores can overshoot
     *  sometimes - we don't care about them quite as much
     *  as we care about reads.
     */
    lev = (greq->greq_request == GRAPHD_REQUEST_READ)
              ? (now > deadline + 500 ? CL_LEVEL_ERROR : CL_LEVEL_DEBUG)
              : CL_LEVEL_DEBUG;

    cl_log(
        cl, lev,
        "graphd_stack_run_until_deadline: "
        "request <%s> overshot "
        "deadline %llu by %llu ms",
        greq->greq_req.req_display_id ? greq->greq_req.req_display_id : "???",
        deadline, now - deadline);
  }

  return graphd_stack_top(stack) ? GRAPHD_ERR_MORE : 0;
}

/**
 * @brief Remove a specific element off the stack.
 *
 * @param stack		stack to pop
 * @param elem		specific element to remove
 */
void graphd_stack_remove(graphd_stack *stack, graphd_stack_context *sc) {
  if (sc != NULL) cm_resource_free(&sc->sc_resource);
}

/**
 * @brief Pop the topmost element off the stack.
 *
 * @param stack		stack to pop
 *
 * @return GRAPHD_ERR_NO if there is no topmost element
 * @return 0 otherwise
 */
int graphd_stack_pop(graphd_stack *stack) {
  cm_resource *r;

  if ((r = cm_resource_top(&stack->s_resource_manager)) == NULL)
    return GRAPHD_ERR_NO;

  if (r != NULL) cm_resource_free(r);
  return 0;
}

/**
 * @brief Free all elements on the stack and
 *  the stack itself (except for its storage).
 *
 *  The actual work happens in the stack's resource method.
 *  This is just a type-specific way of calling resource
 *  destroy on the stack.
 *
 * @param stack		stack to free
 */
void graphd_stack_free(graphd_stack *stack) {
  cm_resource_free(&stack->s_resource);
}

/**
 * @brief Connect a stack to a resource manager.
 *
 *  Note that this call doesn't actually allocate
 *  anything; the memory that the stack data structure
 *  itself lives in is allocated and managed by the
 *  application, independent of this call.
 *
 * @param stack	stack to initialize
 * @param rm	resource manager it will belong to
 * @param cm	allocation handler it will malloc through.
 */
void graphd_stack_alloc(graphd_stack *stack, cm_resource_manager *rm,
                        cm_handle *cm) {
  cm_resource_manager_initialize(&stack->s_resource_manager, stack);
  cm_resource_alloc(rm, &stack->s_resource, &graphd_stack_resource_type, stack);
}

/**
 * @brief List the stack
 *
 *  Like graphd_stack_free, this, too, is a thin wrapper
 *  around the generic resource list function.
 *
 * @param stack	Stack to list
 * @param cl	Log to list it to
 */
void graphd_stack_list(graphd_stack *stack, cl_handle *cl) {
  cm_resource_list(&stack->s_resource, cl);
}

/**
 * @brief Schedule function for execution.
 *
 *  When the specified context next gets control again,
 *  it will execute the specified function.  (Unless it
 *  gets popped without executnig.)
 *
 *  Calling graphd_stack_resume() is the equivalent of a
 *  state transition in an deterministic finite automaton.
 *
 *  Note that this is not a "push" - the function executes
 *  only once control returns to the context.
 *
 * @param stack stack we're talking about
 * @param context context on that stack.
 * @param func	which function to resume with.
 */
void graphd_stack_resume(graphd_stack *stack, graphd_stack_context *context,
                         int (*func)(graphd_stack *, graphd_stack_context *)) {
  if (context != NULL && func != NULL) context->sc_run = func;
}

/**
 * @brief Freeze a stack.
 * @param stack stack we're talking about
 * @param context context on that stack.
 * @param func	which function to resume with.
 */
static void graphd_stack_suspend_callback(void *callback_data, void *rm_data,
                                          void *resource_data) {
  int *err = callback_data;
  graphd_stack_context *sc = resource_data;

  /* Something else failed?
   */
  if (*err) return;

  /* Already done?
   */
  if (sc->sc_suspended) return;

  if (sc->sc_type->sct_suspend == NULL) {
    /*  This frame cannot be suspended?
     */
    *err = GRAPHD_ERR_MORE;
    return;
  }

  *err = (*sc->sc_type->sct_suspend)((graphd_stack *)rm_data, sc);
}

/**
 * @brief Freeze a stack.
 * @param stack stack we're talking about
 * @param context context on that stack.
 * @param func	which function to resume with.
 */
int graphd_stack_suspend(graphd_stack *stack) {
  int err = 0;

  cm_resource_manager_map(&stack->s_resource_manager,
                          graphd_stack_suspend_callback, &err);
  return err;
}

/**
 * @brief Thaw a stack.
 * @param callback_data error state in/out
 * @param rm_data resource manager data: the stack
 * @param resource_data individual resource: the stack context
 */
static void graphd_stack_unsuspend_callback(void *callback_data, void *rm_data,
                                            void *resource_data) {
  int *err = callback_data;
  graphd_stack_context *context = resource_data;

  if (*err) return;

  if (context->sc_type->sct_unsuspend == NULL) {
    *err = EINVAL;
    return;
  }

  *err = (*context->sc_type->sct_unsuspend)((graphd_stack *)rm_data, context);
}

/**
 * @brief Thaw a stack.
 * @param callback_data error state in/out
 * @param rm_data resource manager data: the stack
 * @param resource_data individual resource: the stack context
 */
int graphd_stack_unsuspend(graphd_stack *stack) {
  int err = 0;

  cm_resource_manager_map(&stack->s_resource_manager,
                          graphd_stack_unsuspend_callback, &err);
  return err;
}
