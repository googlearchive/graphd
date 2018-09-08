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
#include <sys/resource.h>
#include <sys/time.h>

#include "libcm/cm.h"
#include "libsrv/srv.h"

static const cm_list_offsets graphd_startup_todo_offsets =
    CM_LIST_OFFSET_INIT(graphd_startup_todo_item, sti_next, sti_prev);

#define GRAPHD_DESIRED_RLIMIT_NPROC 4096

int graphd_startup_check_max_procs(graphd_handle *g) {
/* On some older *NIXes and Solaris, there's no such thing */
#if !defined(RLIMIT_NPROC) || defined(__APPLE__)
  return 0;
#else
  struct rlimit lim;
  int rv;
  rv = getrlimit(RLIMIT_NPROC, &lim);

  if (rv < 0) {
    int err = errno;
    cl_log(g->g_cl, CL_LEVEL_ERROR,
           "Cannot query resource limit for RLIMIT_NPROC:%s", strerror(err));
    return GRAPHD_ERR_NOT_SUPPORTED;
  }

  if (lim.rlim_cur < GRAPHD_DESIRED_RLIMIT_NPROC) {
    if (lim.rlim_max < GRAPHD_DESIRED_RLIMIT_NPROC) {
      /* We're REALLY hoping we've got privileges */
      lim.rlim_max = GRAPHD_DESIRED_RLIMIT_NPROC;
    }

    lim.rlim_cur = lim.rlim_max;

    rv = setrlimit(RLIMIT_NPROC, &lim);
    if (rv < 0) {
      int err = errno;
      cl_log_errno(g->g_cl, CL_LEVEL_ERROR, "setrlimit", err,
                   "Cannot change resource limit for RLIMIT_NPROC");
      /* We probably don't have privileges. We'll learn this soon */
    }

  } else {
    return 0;
  }

  rv = getrlimit(RLIMIT_NPROC, &lim);

  if (rv < 0) {
    int err = errno;
    cl_log(g->g_cl, CL_LEVEL_ERROR,
           "Cannot query resource limit for RLIMIT_NPROC:%s", strerror(err));
    return GRAPHD_ERR_NOT_SUPPORTED;
  }

  if (lim.rlim_cur < GRAPHD_DESIRED_RLIMIT_NPROC) {
    cl_log(g->g_cl, CL_LEVEL_ERROR,
           "The number of available processes, %d, is less "
           "than the desired number, %d\n"
           "Please use 'limit' or 'ulimit' to give graphd more "
           "subprocesses. Starting anyway...",
           (int)lim.rlim_cur, GRAPHD_DESIRED_RLIMIT_NPROC);
  }

  return 0;

#endif
}

/*  If we're done starting up (our list of things to
 *  do before startup is NULL), tell libsrv to open
 *  the front doors.
 */
void graphd_startup_todo_check(graphd_handle *g) {
  if (g->g_startup_todo_head == NULL) srv_startup_now_complete(g->g_srv);
}

/*  Add an item to the startup todo list.
 */
void graphd_startup_todo_add(graphd_handle *g, graphd_startup_todo_item *sti) {
  if (sti->sti_done || sti->sti_requested) return;

  cl_log(g->g_cl, CL_LEVEL_VERBOSE, "graphd_startup_todo_add(%p)", (void *)sti);

  sti->sti_requested = true;
  cm_list_enqueue(graphd_startup_todo_item, graphd_startup_todo_offsets,
                  &g->g_startup_todo_head, &g->g_startup_todo_tail, sti);
}

void graphd_startup_todo_initialize(graphd_startup_todo_item *sti) {
  memset(sti, 0, sizeof(*sti));
}

/*  Mark a startup item as completed.
 */
void graphd_startup_todo_complete(graphd_handle *g,
                                  graphd_startup_todo_item *sti) {
  cl_assert(g->g_cl, sti != NULL);
  cl_log(g->g_cl, CL_LEVEL_VERBOSE, "graphd_startup_todo_complete(%p)",
         (void *)sti);

  if (!sti->sti_requested || sti->sti_done) return;

  sti->sti_done = true;
  cm_list_remove(graphd_startup_todo_item, graphd_startup_todo_offsets,
                 &g->g_startup_todo_head, &g->g_startup_todo_tail, sti);

  graphd_startup_todo_check(g);
}

/*  A startup element has failed.  Remove it from the list of
 *  things to do without marking it as "done".
 */
void graphd_startup_todo_cancel(graphd_handle *g,
                                graphd_startup_todo_item *sti) {
  cl_assert(g->g_cl, sti != NULL);
  cl_log(g->g_cl, CL_LEVEL_VERBOSE, "graphd_startup_todo_cancel(%p)",
         (void *)sti);

  if (!sti->sti_requested || sti->sti_done) return;

  sti->sti_requested = false;
  cm_list_remove(graphd_startup_todo_item, graphd_startup_todo_offsets,
                 &g->g_startup_todo_head, &g->g_startup_todo_tail, sti);

  graphd_startup_todo_check(g);
}
