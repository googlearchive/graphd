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
#ifndef ESP_H
#define ESP_H

#include "libes/es.h"

#include <stdlib.h>

#include "libcm/cm.h"
#include "libcl/cl.h"

struct es_timeout {
  unsigned long et_seconds;

  /* A doubly linked list of descriptors with this timeout. */
  struct es_descriptor *et_head;
  struct es_descriptor *et_tail;

  /* We ourselves are members of a linked list of timeouts. */
  struct es_timeout *et_next;
};

struct es_handle {
  cm_handle *es_cm;
  cl_handle *es_cl;

  /*  A packed array of pollfd structures for the
   *  poll system call.  While they're being
   *  deleted, a free list runs through them,
   *  starting with es_poll_free (if >= 0) and
   *  continuing via es->poll[].fd.
   */
  struct pollfd *es_poll;
  size_t es_poll_n;
  size_t es_poll_m;
  int es_poll_free;

  /*  An array indexed with file descriptors.
   *  <es_desc_m> is the number of slots allocated.
   *  Unused slots are NULL.
   *  <es_desc_n> is the number of slots actually
   *  allocated, regardless of where.
   */
  es_descriptor **es_desc;
  size_t es_desc_n;
  size_t es_desc_m;

  /*  An array similar to es_desc, but for
   *  descriptors that don't correspond to a
   *  file descriptor.
   */
  es_descriptor **es_null;
  size_t es_null_n;
  size_t es_null_m;

  /*  How many of the descriptors currently allocated are demon
   *  descriptors?
   */
  size_t es_demon_n;

  /*  How many of the descriptors currently allocated have an
   *  application event set?
   */
  size_t es_application_event_n;

  time_t es_now;

  /*  es-timeout.c manages a singly linked list of timeout queues
   *  that starts here.
   */
  es_timeout *es_timeout_head;

  void (*es_pre_dispatch)(void *, es_handle *);
  void *es_pre_dispatch_data;
  void (*es_post_dispatch)(void *, es_handle *);
  void *es_post_dispatch_data;

  /* es-idle.c manages a tail queue of idle callbacks that
   * run when the system isn't otherwise busy.
   */
  es_idle_callback *es_idle_callback_head;
  es_idle_callback **es_idle_callback_tail;

  unsigned int es_looping : 1;
  unsigned int es_interrupted : 1;
  unsigned int es_destroyed : 1;
  unsigned int es_dispatching : 1;
};

/* es-application-event.c */

void es_application_event_clear(es_handle *, es_descriptor *);

/* es-idle.c */

void es_idle(es_handle *);
time_t es_idle_timeout(es_handle *);
void es_idle_flush(es_handle *);

/* es-timeout.c */

time_t es_timeout_wakeup(es_handle const *es,
                         es_descriptor **newest_descriptor);

#endif /* ESP_H */
