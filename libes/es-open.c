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

#include <errno.h>
#include <stdlib.h>

/**
 * @brief Initialize a descriptor.
 * @param es opaque module handle, created with es_create()
 * @param fd file descriptor, result of open(), socket(), or accept()
 * @param events mask of the events the application is no longer
 *	interested in.
 * @param ed descriptor structure, embedded at the beginning of
 *	application-specific connection structure
 *
 * @return 0 on success, a nonzero error code on error.
 */

int es_open(es_handle *es, int fd, unsigned int events, es_descriptor *ed) {
  int poll_i;
  struct pollfd *pfd;

  if (es == NULL || fd < 0) return errno ? errno : EINVAL;

  /*  Make sure the slot we're trying to allocate is actually empty.
   */
  if (fd < es->es_desc_m && es->es_desc[fd] != NULL) {
    cl_notreached(es->es_cl,
                  "es_open: file descriptor "
                  "%d already associated with %s!",
                  fd, es->es_desc[fd]->ed_displayname
                          ? es->es_desc[fd]->ed_displayname
                          : "(null)");
    return EINVAL;
  }

  /*  Set poll_i to the index of a pollfd context to
   *  associate with fd.
   */
  if (es->es_poll_free >= 0) {
    /* Get one from the freelist. */

    poll_i = es->es_poll_free;
    es->es_poll_free = es->es_poll[poll_i].fd;
    cl_cover(es->es_cl);
  } else {
    /* Grow the array, if needed, and associate the top one. */

    if (es->es_poll_n >= es->es_poll_m) {
      struct pollfd *tmp;
      tmp = cm_trealloc(es->es_cm, struct pollfd, es->es_poll,
                        es->es_poll_m + 1024);
      if (tmp == NULL) return ENOMEM;
      es->es_poll = tmp;
      es->es_poll_m += 1024;
      cl_cover(es->es_cl);
    }
    poll_i = es->es_poll_n++;
  }

  /* Grow es->es_desc to include ed. */

  if (fd >= es->es_desc_m) {
    es_descriptor **tmp, **e;

    tmp = cm_trealloc(es->es_cm, es_descriptor *, es->es_desc, fd + 1024);
    if (tmp == NULL) {
      /* free the pollfd we just allocated. */

      es->es_poll[poll_i].fd = es->es_poll_free;
      es->es_poll_free = poll_i;

      return ENOMEM;
    }

    es->es_desc = tmp;
    tmp += es->es_desc_m;
    e = es->es_desc + (es->es_desc_m = fd + 1024);
    while (tmp < e) {
      *tmp++ = NULL;
      cl_cover(es->es_cl);
    }
  }

  /* Associate the file descriptor with the service */
  es->es_desc[fd] = ed;
  es->es_desc_n++;

  /* Associate the service with the file descriptor slot */
  ed->ed_poll = (size_t)poll_i;
  ed->ed_demon = false;

  pfd = es->es_poll + poll_i;

  /* Set up the next round of poll input. */
  pfd->fd = fd;
  pfd->events = events;
  pfd->revents = 0;

  /* Initialize the timeout management structure as empty. */
  ed->ed_timeout = NULL;
  ed->ed_timeout_prev = NULL;
  ed->ed_timeout_next = NULL;
  ed->ed_activity = es->es_now;

  return 0;
}

/**
 * @brief Initialize a virtual descriptor for use with timeouts.
 * @param es opaque module handle, created with es_create()
 * @param ed descriptor structure, embedded at the beginning of
 *	application-specific structure
 *
 * @return 0 on success, a nonzero error code on error.
 */

int es_open_null(es_handle *es, es_descriptor *ed) {
  es_descriptor **e;

  ed->ed_poll = (size_t)-1;
  ed->ed_demon = true;

  ed->ed_timeout = NULL;
  ed->ed_timeout_prev = NULL;
  ed->ed_timeout_next = NULL;

  ed->ed_activity = es->es_now;

  if (es->es_null_n >= es->es_null_m) {
    e = cm_realloc(es->es_cm, es->es_null,
                   sizeof(*es->es_null) * (es->es_null_m + 16));
    if (e == NULL) return ENOMEM;
    es->es_null_m += 16;
    es->es_null = e;
  }
  es->es_null[es->es_null_n++] = ed;
  return 0;
}
