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

typedef struct stack_frame {
  graphd_value *sf_value;
  size_t sf_i;
} stack_frame;

typedef struct graphd_read_base_evaluate_context {
  graphd_stack_context grbe_sc;

  stack_frame *grbe_stack;
  stack_frame grbe_stack_buf[16];
  size_t grbe_stack_n;
  size_t grbe_stack_m;

  /*  Request we're running for; we use it to get log handles,
   *  heap allocators, and the graphd context.
   */
  graphd_request *grbe_greq;

} graphd_read_base_evaluate_context;

static int grbe_push(graphd_read_base_evaluate_context *grbe, graphd_value *v) {
  cl_handle *cl = graphd_request_cl(grbe->grbe_greq);
  stack_frame *sf;

  cl_assert(cl, v != NULL);
  if (grbe->grbe_stack_n >= grbe->grbe_stack_m) {
    /*  Grow the value processing stack.
     */
    if (grbe->grbe_stack_m == 0) {
      grbe->grbe_stack_m =
          sizeof(grbe->grbe_stack_buf) / sizeof(*grbe->grbe_stack_buf);
      grbe->grbe_stack = grbe->grbe_stack_buf;
    } else {
      sf = grbe->grbe_stack;
      if (sf == grbe->grbe_stack_buf) sf = NULL;

      sf = cm_realloc(grbe->grbe_greq->greq_req.req_cm, sf,
                      sizeof(*grbe->grbe_stack) * (grbe->grbe_stack_m + 16));
      if (sf == NULL) return errno ? errno : ENOMEM;

      if (grbe->grbe_stack == grbe->grbe_stack_buf)
        memcpy(sf, grbe->grbe_stack_buf, sizeof grbe->grbe_stack_buf);
      grbe->grbe_stack = sf;
      grbe->grbe_stack_m += 16;
    }
  }
  sf = grbe->grbe_stack + grbe->grbe_stack_n++;

  sf->sf_value = v;
  sf->sf_i = 0;

  return 0;
}

/*  In pre-order, return the next GRAPHD_VALUE_DEFERRED from
 *  a tree.
 */
static int grbe_next(graphd_read_base_evaluate_context *grbe,
                     graphd_value **v_out) {
  stack_frame *sf;
  int err;

next:
  if (grbe->grbe_stack_n == 0) return GRAPHD_ERR_NO;

  sf = grbe->grbe_stack + grbe->grbe_stack_n - 1;
  if (sf->sf_value->val_type == GRAPHD_VALUE_DEFERRED) {
    *v_out = sf->sf_value;
    return 0;
  }
  if (GRAPHD_VALUE_IS_ARRAY(*sf->sf_value)) {
    while (sf->sf_i < sf->sf_value->val_array_n) {
      graphd_value *v = sf->sf_value->val_array_contents + sf->sf_i;
      if (GRAPHD_VALUE_IS_ARRAY(*v)) {
        /* We'll resume behind the array. */
        sf->sf_i++;

        /* Push the value for evaluation. */
        err = grbe_push(grbe, v);
        if (err != 0) return err;

        goto next;
      } else if (v->val_type == GRAPHD_VALUE_DEFERRED) {
        /*  Don't increment sf->sf_i; we'll
         *  revisit this on the next round.
         */
        *v_out = v;
        return 0;
      } else {
        sf->sf_i++;
      }
    }
  }
  /* Pop one. */
  grbe->grbe_stack_n--;
  goto next;
}

static void grbe_resource_free(void *manager_data, void *resource_data) {
  graphd_read_base_evaluate_context *grbe = resource_data;

  if (grbe->grbe_stack != NULL && grbe->grbe_stack != grbe->grbe_stack_buf)
    cm_free(grbe->grbe_greq->greq_req.req_cm, grbe->grbe_stack);

  cm_free(grbe->grbe_greq->greq_req.req_cm, grbe);
}

static void grbe_resource_list(void *call_data, void *manager_data,
                               void *resource_data) {
  graphd_read_base_evaluate_context *grbe = resource_data;
  cl_handle *cl = call_data;

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_read_base_evaluate_context %p",
         (void *)grbe);
}

static const cm_resource_type grbe_resource_type = {
    "graphd_read_base_evaluate_context", grbe_resource_free,
    grbe_resource_list};

static int grbe_run(graphd_stack *stack, graphd_stack_context *stack_context) {
  graphd_read_base_evaluate_context *grbe = (void *)stack_context;
  cl_handle *cl = graphd_request_cl(grbe->grbe_greq);
  graphd_value *val;
  int err = 0;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  while (grbe_next(grbe, &val) == 0) {
    if (val->val_type != GRAPHD_VALUE_DEFERRED) continue;

    /*  Evaluate the deferred value, then revisit it
     *  to evaluate its contents, and so on.
     */
    err = (*val->val_deferred_base->db_type->dt_push)(grbe->grbe_greq, val);
    if (err != 0) graphd_stack_pop(stack);

    cl_leave(cl, CL_LEVEL_VERBOSE, "%s",
             err ? graphd_strerror(err) : "pushed deferred value");
    return err;
  }

  /*  Done with this context.
   */
  graphd_stack_pop(stack);
  cl_leave(cl, CL_LEVEL_VERBOSE, "done");

  return 0;
}

static int grbe_freeze(graphd_stack *stack,
                       graphd_stack_context *stack_context) {
  /*  Nothing to do.  The values we're working on
   *  are weak pointers.
   */
  return 0;
}

static int grbe_thaw(graphd_stack *stack, graphd_stack_context *stack_context) {
  /*  Nothing to do.  The values we're working on
   *  are weak pointers and are thawed and frozen elsewhere.
   */
  return 0;
}

static graphd_stack_type grbe_type = {grbe_run, grbe_freeze, grbe_thaw};

/**
 * @brief Push a context on the stack that will evaluate deferred values.
 *
 * @param greq		Request whose stack we're pushing on
 * @param val		Value caller wants evaluated and replaced.
 */
int graphd_read_base_evaluate_push(graphd_request *greq, graphd_value *val) {
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  graphd_read_base_evaluate_context *grbe;
  int err;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");
  cl_assert(cl, val != NULL);

  if (val == NULL || graphd_value_locate(val, GRAPHD_VALUE_DEFERRED) == NULL) {
    /* OK, Done! */
    cl_leave(cl, CL_LEVEL_VERBOSE, "nothing to do");
    return 0;
  }

  grbe = cm_zalloc(cm, sizeof(graphd_read_base_evaluate_context));
  if (grbe == NULL) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "out of memory (2)");
    return errno ? errno : ENOMEM;
  }
  grbe->grbe_greq = greq;
  err = grbe_push(grbe, val);
  if (err != 0) {
    cm_free(cm, grbe);
    return err;
  }

  /*  Hook up to the runtime stack.
   */
  graphd_stack_push(&greq->greq_stack, &grbe->grbe_sc, &grbe_resource_type,
                    &grbe_type);

  cl_leave(cl, CL_LEVEL_VERBOSE, "pushed %p", grbe);
  return 0;
}
