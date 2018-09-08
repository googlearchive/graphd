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

#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sysexits.h> /* EX_OSERR */

struct graphd_xstate_ticket {
  graphd_xstate_ticket *x_next, *x_prev;
  unsigned long long x_number;

  /* Who holds that ticket?
   */
  void *x_data;
  void (*x_callback)(void *data);
};

static const cm_list_offsets graphd_xstate_ticket_offsets =
    CM_LIST_OFFSET_INIT(graphd_xstate_ticket, x_next, x_prev);

/**
 * @brief Utility: chain in a ticket.
 */
static void graphd_xstate_ticket_chain_in(graphd_handle *g,
                                          graphd_xstate_ticket *x) {
  graphd_xstate_ticket *loc;

  for (loc = g->g_xstate_tail; loc != NULL; loc = loc->x_prev)
    if (loc->x_number <= x->x_number) break;

  /* Append after <loc> (which may be NULL.)
   */
  cm_list_insert_after(graphd_xstate_ticket, graphd_xstate_ticket_offsets,
                       &g->g_xstate_head, &g->g_xstate_tail, loc, x);
}

/**
 * @brief Utility: chain out a ticket.
 */
static void graphd_xstate_ticket_chain_out(graphd_handle *g,
                                           graphd_xstate_ticket *x) {
  cm_list_remove(graphd_xstate_ticket, graphd_xstate_ticket_offsets,
                 &g->g_xstate_head, &g->g_xstate_tail, x);
}

/**
 * @brief Utility: allocate a ticket.  Returns 0 on success, errno on error.
 */
static int graphd_xstate_ticket_alloc(graphd_handle *g, unsigned long long num,
                                      void (*callback)(void *), void *data,
                                      char const *kind,
                                      graphd_xstate_ticket **tick_out) {
  graphd_xstate_ticket *x;
  cl_handle *const cl = g->g_cl;

  x = cm_malloc(g->g_cm, sizeof(*x));
  if (x == NULL) return errno ? errno : ENOMEM;

  memset(x, 0, sizeof(*x));

  x->x_number = num;
  x->x_data = data;
  x->x_callback = callback;

  graphd_xstate_ticket_chain_in(g, x);
  *tick_out = x;

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_xstate_ticket_alloc %s %llu for %p/%p",
         kind, num, (void *)data, (void *)callback);

  if (g->g_xstate_head->x_number == x->x_number) (*callback)(data);
  return 0;
}

/**
 * @brief Give up a ticket.
 */
void graphd_xstate_ticket_delete(graphd_handle *g, graphd_xstate_ticket **x) {
  if (x != NULL && *x != NULL) {
    unsigned long long old = g->g_xstate_head->x_number;
    cl_handle *const cl = g->g_cl;

    cl_log(cl, CL_LEVEL_VERBOSE, "graphd_xstate_ticket_delete %llu for %p/%p",
           (*x)->x_number, (void *)(*x)->x_data, (void *)(*x)->x_callback);

    graphd_xstate_ticket_chain_out(g, *x);
    cm_free(g->g_cm, *x);

    *x = NULL;

    if (g->g_xstate_head != NULL && old != g->g_xstate_head->x_number)
      graphd_xstate_notify_ticketholders(g);
  }
}

/**
 * @brief Is the ticket I'm holding a current one?
 *	True: yes, you can run; False: no, stay in line.
 */
bool graphd_xstate_ticket_is_running(graphd_handle *g,
                                     graphd_xstate_ticket const *x) {
  cl_log(g->g_cl, CL_LEVEL_VERBOSE,
         "graphd_xstate_ticket_is_running: running: %llu, "
         "request: %llu",
         g->g_xstate_head->x_number, x->x_number);

  return x->x_number <= g->g_xstate_head->x_number;
}

bool graphd_xstate_any_waiting_behind(graphd_xstate_ticket const *x) {
  graphd_xstate_ticket const *y;

  for (y = x->x_next; y != NULL; y = y->x_next)
    if (y->x_number != x->x_number) return true;

  return false;
}

/**
 * @brief Get an exclusive ticket.
 */
int graphd_xstate_ticket_get_exclusive(graphd_handle *g,
                                       void (*callback)(void *), void *data,
                                       graphd_xstate_ticket **tick_out) {
  unsigned long long num;

  g->g_xstate_ticket_printer++;
  num = g->g_xstate_ticket_printer++;

  return graphd_xstate_ticket_alloc(g, num, callback, data, "exclusive ticket",
                                    tick_out);
}

/**
 * @brief Get a shared ticket.
 */
int graphd_xstate_ticket_get_shared(graphd_handle *g, void (*callback)(void *),
                                    void *data,
                                    graphd_xstate_ticket **tick_out) {
  return graphd_xstate_ticket_alloc(g, g->g_xstate_ticket_printer, callback,
                                    data, "shared ticket", tick_out);
}

/**
 * @brief Mark holders of a current ticket as runnable.
 */
void graphd_xstate_notify_ticketholders(graphd_handle *g) {
  graphd_xstate_ticket *x, *x_next;

  for (x = g->g_xstate_head; x != NULL; x = x_next) {
    if (!graphd_xstate_ticket_is_running(g, x)) break;

    x_next = x->x_next;
    (*x->x_callback)(x->x_data);
  }
}

/*  Assign a fresh ticket over an existing one.
 */
void graphd_xstate_ticket_reissue(graphd_handle *g, graphd_xstate_ticket *x,
                                  int type) {
  if (type == GRAPHD_XSTATE_NONE) {
    graphd_xstate_ticket_delete(g, &x);
    return;
  }

  graphd_xstate_ticket_chain_out(g, x);

  if (type == GRAPHD_XSTATE_EXCLUSIVE) g->g_xstate_ticket_printer++;

  x->x_number = g->g_xstate_ticket_printer;

  cl_log(g->g_cl, CL_LEVEL_VERBOSE,
         "graphd_xstate_ticket_reissue: new ticket number %llu", x->x_number);

  if (type == GRAPHD_XSTATE_EXCLUSIVE) g->g_xstate_ticket_printer++;

  graphd_xstate_ticket_chain_in(g, x);
  graphd_xstate_notify_ticketholders(g);
}
