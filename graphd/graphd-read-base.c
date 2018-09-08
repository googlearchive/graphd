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
#include "graphd/graphd-read.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

static void grb_link(graphd_read_base *grb) { grb->grb_link++; }

static void grb_unlink(graphd_read_base *grb) {
  cl_handle *cl = graphd_request_cl(grb->grb_greq);

  cl_log(cl, CL_LEVEL_VERBOSE, "grb_unlink %p (%d links)", (void *)grb,
         grb->grb_link);

  if ((grb->grb_link)-- <= 1) {
    cm_handle *cm = grb->grb_greq->greq_req.req_cm;

    graphd_value_finish(cl, &grb->grb_val);
    cm_free(cm, grb);
  }
}

/*  A little stub that frees the graphd-read-base at
 *  the end of a request.
 */
static void grb_resource_free(void *manager_data, void *resource_data) {
  graphd_read_base *grb = resource_data;
  grb_unlink(grb);
}

static void grb_resource_list(void *call_data, void *manager_data,
                              void *resource_data) {
  graphd_read_base *grb = resource_data;
  cl_handle *cl = call_data;

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_read_base %p", (void *)grb);
}

static const cm_resource_type grb_resource_type = {
    "graphd_read_base", grb_resource_free, grb_resource_list};

/**
 * @brief read-base method: run (1).
 *
 * @param stack		Stack we're running on
 * @param stack_context	Specific context
 */
static int grb_run(graphd_stack *stack, graphd_stack_context *stack_context) {
  graphd_read_base *grb = (void *)stack_context;
  cl_handle *cl = graphd_request_cl(grb->grb_greq);
  char buf[200];

  cl_enter(cl, CL_LEVEL_VERBOSE, "err=%d, deferred=%u, value=%s", grb->grb_err,
           grb->grb_deferred,
           graphd_value_to_string(&grb->grb_val, buf, sizeof buf));

  if (grb->grb_err == 0 && grb->grb_deferred) {
    grb->grb_deferred = false;

    grb->grb_err = graphd_read_base_evaluate_push(grb->grb_greq, &grb->grb_val);
    if (grb->grb_err == 0) {
      /*  We pulled out another value.   It has
       *  been pushed on the stack.  The next stack
       *  run will call into that new frame.
       */
      cl_leave(cl, CL_LEVEL_VERBOSE, "evaluating deferred frame");
      return 0;
    }
  }
  if (grb->grb_err == 0) {
    /*  Move the result from the graphd_read_base
     *  to the waiting result pointer.
     */
    if (grb->grb_val_out != NULL) {
      if (grb->grb_val.val_type == GRAPHD_VALUE_SEQUENCE &&
          grb->grb_val.val_sequence_n == 1) {
        *grb->grb_val_out = grb->grb_val.val_sequence_contents[0];
        graphd_value_initialize(grb->grb_val.val_sequence_contents);
      } else {
        *grb->grb_val_out = grb->grb_val;
        graphd_value_initialize(&grb->grb_val);
      }
    }
  }
  if (grb->grb_err_out != NULL) *grb->grb_err_out = grb->grb_err;
  graphd_stack_pop(stack);

  cl_leave(cl, CL_LEVEL_VERBOSE, "done");
  return 0;
}

static int grb_suspend(graphd_stack *stack,
                       graphd_stack_context *stack_context) {
  graphd_read_base *grb = (graphd_read_base *)stack_context;
  int err;

  err = graphd_value_suspend(grb->grb_greq->greq_req.req_cm,
                             graphd_request_cl(grb->grb_greq), &grb->grb_val);
  return err;
}

static int grb_unsuspend(graphd_stack *stack,
                         graphd_stack_context *stack_context) {
  (void)stack;
  (void)stack_context;

  return 0;
}

static graphd_stack_type grb_type = {grb_run, grb_suspend, grb_unsuspend};

/**
 * @brief Push a context on the stack that will read a constraint tree.
 *
 * @param greq		Request whose stack we're pushing on
 * @param con		Constraint caller wants to match.
 * @param val_out 	return a value tree here.
 * @param err_out 	return errors here.
 */
int graphd_read_base_push(graphd_request *greq, graphd_constraint *con,
                          graphd_value *val_out, int *err_out,
                          graphd_read_base **grb_out) {
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  graphd_read_base *grb;

  cl_enter(cl, CL_LEVEL_SPEW, "enter");
  cl_assert(cl, val_out != NULL);
  cl_assert(cl, err_out != NULL);

  if ((grb = cm_zalloc(cm, sizeof(graphd_read_base))) == NULL) {
    cl_leave(cl, CL_LEVEL_SPEW, "out of memory (2)");
    return errno ? errno : ENOMEM;
  }

  *err_out = 0;

  /* Once for the read stack.
   */
  grb_link(grb);

  /* Once for the request end.
   */
  grb_link(grb);

  /* Normally, the grb is freed at the end of the request.
   *
   * (Some values from the grb's heap end up in the request
   * result value.  If we free it before the request, we'd
   * try to format undefined storage.)
   */
  cm_resource_alloc(&greq->greq_resource, &grb->grb_request_resource,
                    &grb_resource_type, grb);

  grb->grb_greq = greq;
  grb->grb_con = con;

  grb->grb_err_out = err_out;
  grb->grb_val_out = val_out;
  graphd_value_initialize(val_out);
  graphd_value_sequence_set(cm, &grb->grb_val);

  /*  Hook up to the runtime stack.
   */
  graphd_stack_push(&greq->greq_stack, &grb->grb_sc, &grb_resource_type,
                    &grb_type);

  *grb_out = grb;
  cl_leave(cl, CL_LEVEL_SPEW, "pushed");

  return 0;
}
