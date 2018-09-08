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
#include "libes/esp.h"

#include <stddef.h>
#include <stdlib.h>
#include <time.h>

#ifndef DOCUMENTATION_GENERATOR_ONLY

struct es_idle_callback {
  struct es_idle_callback *ecb_next;

  time_t ecb_deadline;

  es_idle_callback_func *ecb_callback;
  void *ecb_callback_data;
};
#endif

/**
 * @brief Create an idle callback.
 *
 *  The callback will be invoked either seconds from now, or
 *  any time before that if the system becomes idle (none of the
 *  file descriptors usually managed by libes are busy.)
 *
 * @param es opaque module handle, created with es_create()
 * @param seconds number of seconds into the future by which
 *  	we're going to just *make* time.
 * @param callback function to call
 * @param callback_data opaque application pointer to pass to the function
 *
 * @return NULL on allocation error, otherwise a pointer to
 *	a newly allocated timeout descriptor structure.
 */
es_idle_callback *es_idle_callback_create(es_handle *es, unsigned long seconds,
                                          es_idle_callback_func *callback,
                                          void *callback_data) {
  es_idle_callback *ecb;

  if ((ecb = cm_talloc(es->es_cm, es_idle_callback, 1)) == NULL) return ecb;

  memset(ecb, 0, sizeof(*ecb));

  ecb->ecb_next = NULL;
  ecb->ecb_deadline = time((time_t *)0) + seconds;
  ecb->ecb_callback = callback;
  ecb->ecb_callback_data = callback_data;

  *es->es_idle_callback_tail = ecb;
  es->es_idle_callback_tail = &ecb->ecb_next;
  cl_cover(es->es_cl);

  cl_log(es->es_cl, CL_LEVEL_VERBOSE,
         "es_idle: will call %p(%p) in at most %lu seconds",
         (void *)ecb->ecb_callback, (void *)ecb->ecb_callback_data,
         (unsigned long)seconds);

  return ecb;
}

/**
 * @brief Cancel an idle callback.
 *
 *  The cancellation includes invoking the callback's function
 *  with mode=ES_IDLE_CANCEL.
 *
 * @param es   library module handle
 * @param ecb  idle callback to cancel.
 */
void es_idle_callback_cancel(es_handle *es, es_idle_callback *ecb) {
  es_idle_callback **p;

  for (p = &es->es_idle_callback_head; *p != NULL;) {
    if (*p == ecb) {
      if ((*p = ecb->ecb_next) == NULL) es->es_idle_callback_tail = p;

      (*ecb->ecb_callback)(ecb->ecb_callback_data, ES_IDLE_CANCEL);

      cm_free(es->es_cm, ecb);
      cl_cover(es->es_cl);

      return;
    } else {
      p = &(*p)->ecb_next;
      cl_cover(es->es_cl);
    }
  }
}

/**
 * @brief Nothing much happened.  Go be (one flavor of) idle.
 * @param es opaque module handle, allocated with es_create()
 */
void es_idle(es_handle *es) {
  es_idle_callback *ecb;

  if (es == NULL || es->es_idle_callback_head == NULL) return;

  ecb = es->es_idle_callback_head;
  if ((es->es_idle_callback_head = ecb->ecb_next) == NULL) {
    es->es_idle_callback_tail = &es->es_idle_callback_head;
    cl_cover(es->es_cl);
  }

  cl_log(es->es_cl, CL_LEVEL_VERBOSE, "es_idle: calling %p(%p, ES_IDLE_IDLE)",
         (void *)ecb->ecb_callback, (void *)ecb->ecb_callback_data);

  (*ecb->ecb_callback)(ecb->ecb_callback_data, ES_IDLE_IDLE);
  cm_free(es->es_cm, ecb);
  cl_cover(es->es_cl);
}

/**
 * @brief Call timed-out callbacks, and tell us how long to wait.
 * @param es  library module handle
 * @return 0 or the next timeout
 */
time_t es_idle_timeout(es_handle *es) {
  size_t n_called = 0;
  time_t now;
  time_t next;
  es_idle_callback **p;

  next = 0;
  time(&now);

  for (p = &es->es_idle_callback_head; *p != NULL;) {
    if ((*p)->ecb_deadline <= now) {
      es_idle_callback *ecb;

      ecb = *p;
      if ((*p = ecb->ecb_next) == NULL) es->es_idle_callback_tail = p;

      (*ecb->ecb_callback)(ecb->ecb_callback_data, ES_IDLE_TIMED_OUT);

      cm_free(es->es_cm, ecb);
      cl_cover(es->es_cl);

      n_called++;
    } else {
      if (next == 0 || (*p)->ecb_deadline < next) {
        next = (*p)->ecb_deadline;
        cl_cover(es->es_cl);
      }
      p = &(*p)->ecb_next;
      cl_cover(es->es_cl);
    }
  }

  if (n_called > 0)
    cl_log(es->es_cl, CL_LEVEL_VERBOSE,
           "es_idle_timeout: called %lu idle handler%s; "
           "next timeout in %lu second%s",
           (unsigned long)n_called, n_called == 1 ? "" : "s",
           (unsigned long)(next == 0 ? 0 : next - now),
           next == now + 1 ? "" : "s");
  return next;
}

/**
 * @brief Free pending idle callbacks without calling them.
 * @param es  library module handle
 */
void es_idle_flush(es_handle *es) {
  es_idle_callback *ecb, *ecb_next;

  if (es != NULL) {
    size_t n_flushed = 0;

    ecb_next = es->es_idle_callback_head;

    es->es_idle_callback_head = NULL;
    es->es_idle_callback_tail = &es->es_idle_callback_head;

    while ((ecb = ecb_next) != NULL) {
      ecb_next = ecb->ecb_next;

      (*ecb->ecb_callback)(ecb->ecb_callback_data, ES_IDLE_CANCEL);

      cm_free(es->es_cm, ecb);
      cl_cover(es->es_cl);

      n_flushed++;
    }
    if (n_flushed > 0)
      cl_log(es->es_cl, CL_LEVEL_DEBUG, "es_idle_flush: cancelled %lu call%s",
             (unsigned long)n_flushed, n_flushed == 1 ? "" : "s");
  }
}
