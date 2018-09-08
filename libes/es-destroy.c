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

/**
 * @brief Destroy the resources allocated for an es_handle
 *
 * It is safe to call this function from within a callback
 * invoked by es_loop().
 *
 * @param es opaque module handle allocated with es_create()
 */
void es_destroy(es_handle *es) {
  int i;
  es_descriptor *ed;

  if (!es) return;

  cl_enter(es->es_cl, CL_LEVEL_SPEW, "enter");

  /*  Pass an error event to all descriptors.  Each descriptor
   *  must react to that by destroying itself (and possibly others.)
   */
  for (i = 0; i < es->es_desc_m; i++)
    if ((ed = es->es_desc[i]) != NULL) {
      cl_cover(es->es_cl);
      (*ed->ed_callback)(ed, es->es_poll[ed->ed_poll].fd, ES_EXIT);
    }

  /* Shut down non-connection-related timeouts.
   */
  for (i = es->es_null_n; i-- > 0;) {
    ed = es->es_null[0];
    cl_assert(es->es_cl, ed != NULL);
    cl_assert(es->es_cl, ed->ed_callback);

    /*  If everything goes well, the callback will delete
     *  itself in response to this call.  Iterate from the
     *  back to the front so that the deletion fill-in
     *  doesn't change the unvisited records.
     */
    (*ed->ed_callback)(ed, -1, ES_EXIT);
  }

  /*  If there is a post-iteration callback, and we're in
   *  the middle of a loop, call that callback, and uninstall
   *  it so that the outgoing es_loop() won't call it.
   */
  if (es->es_dispatching && es->es_post_dispatch != NULL) {
    if (es->es_post_dispatch != NULL)
      (*es->es_post_dispatch)(es->es_post_dispatch_data, es);
    es->es_post_dispatch = NULL;
    cl_cover(es->es_cl);
  }

  /* Free the descriptor and pollfd tables.
   */
  if (es->es_desc_m > 0) {
    cm_free(es->es_cm, es->es_desc);
    es->es_desc = NULL;
    es->es_desc_n = es->es_desc_m = 0;
    cl_cover(es->es_cl);
  }
  if (es->es_poll_m > 0) {
    cm_free(es->es_cm, es->es_poll);
    es->es_poll = NULL;
    es->es_poll_n = es->es_poll_m = 0;
    cl_cover(es->es_cl);
  }
  while (es->es_null_n > 0) es_close(es, es->es_null[0]);

  if (es->es_null != NULL) {
    cm_free(es->es_cm, es->es_null);
    es->es_null = NULL;
    es->es_null_m = 0;
    es->es_null_n = 0;
  }

  /* Destroy the timeout structures that survived this purge;
   * they must be empty by now.
   */
  while (es->es_timeout_head != NULL) {
    es_timeout_destroy(es, es->es_timeout_head);
    cl_cover(es->es_cl);
  }

  /* Destroy the idle-callback structures that survived this purge.
   */
  es_idle_flush(es);

  /* Destroy the handle structure itself.
   */
  es->es_destroyed = 1;
  if (!es->es_looping) {
    cl_cover(es->es_cl);
    cl_leave(es->es_cl, CL_LEVEL_SPEW, "leave");
    cm_free(es->es_cm, es);
  } else {
    cl_leave(es->es_cl, CL_LEVEL_SPEW,
             "free you later, iterator (still looping)");
  }
}
