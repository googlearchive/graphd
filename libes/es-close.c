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
 * @brief Close one descriptor's session.
 *
 * The caller is responsible for free'ing the session
 * descriptor memory.  No callbacks are executed in response
 * to the call.
 *
 * It is safe, and does nothing, to call this function
 * with NULL module handle or session descriptor.
 *
 * @param es opaque module handle allocated with es_create()
 * @param ed descriptor the application wishes to shut down.
 */
void es_close(es_handle *es, es_descriptor *ed) {
  struct pollfd *pfd;

  if (es == NULL || ed == NULL) return;

  if (ed->ed_poll != (size_t)-1) {
    cl_assert(es->es_cl, ed->ed_poll >= 0);
    cl_assert(es->es_cl, ed->ed_poll < es->es_poll_n);

    pfd = es->es_poll + ed->ed_poll;

    cl_assert(es->es_cl, pfd->fd >= 0);
    cl_assert(es->es_cl, pfd->fd < es->es_desc_m);
    cl_assert(es->es_cl, es->es_desc_n > 0);

    cl_log(es->es_cl, CL_LEVEL_DEBUG, "poll: close fd=%d (%p)", pfd->fd,
           (void *)ed);
    if (ed->ed_demon) es->es_demon_n--;

    cl_assert(es->es_cl, es->es_desc[pfd->fd] != NULL);
    es->es_desc[pfd->fd] = NULL;
    es->es_desc_n--;

    /*  During the event processing loop, closed pollfd
     *  structs are not overwritten, only marked as free.
     *
     *  After the loop, pollfds that haven't been reused
     *  are filled in  with those from the back, closing
     *  the field.
     */

    pfd->revents = 0;
    pfd->events = 0;

    /* Chain the pollfd slot into the freelist. */
    pfd->fd = es->es_poll_free;
    es->es_poll_free = ed->ed_poll;
  } else {
    size_t i;

    for (i = 0; i < es->es_null_n; i++)
      if (es->es_null[i] == ed) break;

    /*  If this assertion fails, you're freeing a
     *  record that has already been free'd.
     */
    cl_assert(es->es_cl, i < es->es_null_n);

    /* Overwrite the deleted record with the last one.
     */
    if (i < es->es_null_n - 1) es->es_null[i] = es->es_null[es->es_null_n - 1];
    es->es_null_n--;
  }

  /* If the session had an application event, unlink from that. */
  es_application_event_clear(es, ed);

  /* If the session had a timeout, unlink from that. */
  es_timeout_delete(es, ed);
  cl_cover(es->es_cl);
}
