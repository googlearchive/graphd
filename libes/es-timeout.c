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
#include <string.h>
#include <stdlib.h>
#include <stddef.h>

#include "libes/esp.h"

static const cm_list_offsets es_descriptor_offsets =
    CM_LIST_OFFSET_INIT(es_descriptor, ed_timeout_next, ed_timeout_prev);

/**
 * @brief Create a timeout.  A timeout specifies a number of seconds.
 *
 *  Individual es_descriptors later associate themselves with a timeout
 *  using es_timeout_add().
 *
 *  @param es opaque module handle, created with es_create()
 *  @param seconds number of seconds.
 *  @return NULL on allocation error, otherwise a pointer to
 *	a newly allocated timeout descriptor structure.
 */
es_timeout *es_timeout_create(es_handle *es, unsigned long seconds) {
  es_timeout *et, **etp;
  if ((et = cm_talloc(es->es_cm, es_timeout, 1)) == NULL) return et;

  memset(et, 0, sizeof(*et));
  et->et_seconds = seconds;

  /* Initialize the empty chain of descriptors that have this timeout.
   */
  et->et_head = 0;
  et->et_tail = 0;

  /* Find the address of a pointer to a timeout that's longer
   * than ours.
   * (I just want *some* sorting criterion.  Putting things that
   * move a lot to the front of the list seems to make sense.)
   */
  for (etp = &es->es_timeout_head; *etp != NULL; etp = &(*etp)->et_next)
    if ((*etp)->et_seconds > seconds) {
      cl_cover(es->es_cl);
      break;
    }
  et->et_next = *etp;
  *etp = et;

  cl_log(es->es_cl, CL_LEVEL_VERBOSE, "es_timeout_create(%lu seconds): %p",
         seconds, (void *)et);

  cl_cover(es->es_cl);
  return et;
}

/**
 * @brief Destroy a timeout.
 *
 *  The timeout must have been valid or NULL.
 *  At the time of destruction, it must not have descriptors associated
 *  with it.
 *
 * @param es opaque module handle, allocated with es_create()
 * @param et NULL or a valid timeout allocated with es_timeout_create()
 */
void es_timeout_destroy(es_handle *es, es_timeout *et) {
  es_timeout **etp;

  if (es == NULL || et == NULL) return;

  /* You can't destroy timeouts that still have customers!
  */
  cl_assert(es->es_cl, !et->et_head);
  cl_assert(es->es_cl, !et->et_tail);

  /* Find the timeout's address in the list of all timeouts.
   */
  for (etp = &es->es_timeout_head; *etp != et; etp = &(*etp)->et_next) {
    cl_assert(es->es_cl, *etp != NULL);
    cl_cover(es->es_cl);
  }

  cl_assert(es->es_cl, *etp != NULL);
  *etp = (*etp)->et_next;

  cl_log(es->es_cl, CL_LEVEL_VERBOSE, "es_timeout_destroy (%p: %lu seconds)",
         (void *)et, et->et_seconds);
  cm_free(es->es_cm, et);
}

/**
 * @brief Disassociate a descriptor from its timeout.
 *
 *  The descriptor must be valid.
 *  It is safe, and does nothing, to call this function on
 *  a descriptor that is not associated with a timeout.
 *
 *  The descriptor and the timeout continue to exist as objects.
 *
 * @param es opaque module handle, allocated with es_create()
 * @param ed descriptor initialized with es_open()
 */
void es_timeout_delete(es_handle *es, es_descriptor *ed) {
  es_timeout *et;

  if (!es) return;
  cl_assert(es->es_cl, ed);

  et = ed->ed_timeout;
  if (!et) return;

  cl_log(es->es_cl, CL_LEVEL_VERBOSE,
         "es_timeout_delete (ed=%p, et=%p, prev=%p, next=%p)", (void *)ed,
         (void *)et, (void *)ed->ed_timeout_prev, (void *)ed->ed_timeout_next);

  cm_list_remove(es_descriptor, es_descriptor_offsets, &et->et_head,
                 &et->et_tail, ed);
  ed->ed_timeout = 0;
}

/**
 * @brief Associate a descriptor with a timeout.
 *
 *  Any descriptor can only be associated with one timeout
 *  at a time.
 *
 * @param es opaque module handle, allocated with es_create()
 * @param et a valid timeout, created with es_timeout_create()
 * @param ed descriptor initialized with es_open()
 */
void es_timeout_add(es_handle *es, es_timeout *et, es_descriptor *ed) {
  if (es == NULL) return;

  cl_log(es->es_cl, CL_LEVEL_VERBOSE,
         "es_timeout_add: et=%p, ed=%p, et->et_head=%p", (void *)et, (void *)ed,
         (void *)et->et_head);

  cl_assert(es->es_cl, et != NULL);
  cl_assert(es->es_cl, ed != NULL);

  ed->ed_timeout = et;
  ed->ed_activity = es_now(es);

  cm_list_push(es_descriptor, es_descriptor_offsets, &et->et_head, &et->et_tail,
               ed);
}

/*  When does the next descriptor time out
 *  (and which descriptor is that)?
 */
time_t es_timeout_wakeup(es_handle const *es,
                         es_descriptor **newest_descriptor) {
  es_timeout const *et;
  time_t wakeup, wakeup_best = 0;
  es_descriptor *ed_best = NULL;

  /* For all timeouts */
  for (et = es->es_timeout_head; et != NULL; et = et->et_next) {
    es_descriptor *ed;

    /* No customers */
    if (!et->et_tail) continue;

    cl_assert(es->es_cl, et->et_head);

    ed = et->et_tail;
    wakeup = ed->ed_activity + et->et_seconds;
    if (wakeup_best == 0 || wakeup < wakeup_best) {
      wakeup_best = wakeup;
      ed_best = ed;

      cl_cover(es->es_cl);
    }
  }

  *newest_descriptor = ed_best;
  return wakeup_best;
}
