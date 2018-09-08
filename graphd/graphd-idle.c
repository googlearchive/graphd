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
#include <errno.h>
#include <stdio.h>
#include <stdbool.h>

/**
 * @brief Commit index changes to disk
 *
 *  This callback is installed with a 10-second timeout by writes
 *  to the database, and reschedules itself at 2-second intervals
 *  until we finish a checkpoint.
 *
 * @param data	the specific idle context.
 */
static void graphd_idle_callback_checkpoint(void* data,
                                            es_idle_callback_timed_out mode) {
  graphd_idle_checkpoint_context* gic = data;
  cl_handle* cl = gic->gic_g->g_cl;
  int err;

  gic->gic_g->g_checkpoint_delay = NULL;
  if ((err = graphd_checkpoint_optional(gic->gic_g)) == GRAPHD_ERR_MORE) {
    if (mode == ES_IDLE_CANCEL) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_idle_callback_checkpoint: cancelling "
             "in mid-write.");
      return;
    }

    /*  We're in the middle of writing something, and want
     *  to wait for the traffic to disk to complete.
     *
     *  Repost this callback, but with a two-second delay.
     */
    gic->gic_g->g_checkpoint_delay = srv_delay_create(
        gic->gic_g->g_srv, 2, 10, graphd_idle_callback_checkpoint, gic,
        "checkpoint delay");
    if (gic->gic_g->g_checkpoint_delay != NULL) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_idle_callback_checkpoint: still writing; "
             "reposted a delay call between "
             "2 and 10 seconds into the future.");
      return;
    } else
      cl_log_errno(cl, CL_LEVEL_ERROR, "srv_delay_create", ENOMEM, "(lost)");
  } else if (err != 0)
    cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_checkpoint_optional", err,
                 "asynchronous write %lu", gic->gic_g->g_asynchronous_write_id);

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_idle_callback_checkpoint: "
         "asynchronous write %lu completed.",
         gic->gic_g->g_asynchronous_write_id);

  gic->gic_g->g_asynchronous_write_in_progress = false;

  /*  So, that gets us to the end of the point from which
   *  we started, way back when.  Now, do we still need
   *  checkpointing?  If yes, restart another batch.
   */
  if (pdb_checkpoint_deficit(gic->gic_g->g_pdb) > 0)
    graphd_idle_install_checkpoint(gic->gic_g);
}

/* Install an idle callback which which will, eventually,
 * write index changes to disk.
 */
int graphd_idle_install_checkpoint(graphd_handle* g) {
  int err;

  if (g->g_asynchronous_write_in_progress) return 0;

  err = srv_idle_set(g->g_srv, &g->g_idle_checkpoint.gic_srv, 10);
  if (err) {
    cl_log_errno(g->g_cl, CL_LEVEL_ERROR, "srv_idle_set", err,
                 "failed to install asynchronous "
                 "write timer");
    return err;
  }
  cl_log(g->g_cl, CL_LEVEL_VERBOSE,
         "graphd_idle_install_checkpoint: starting "
         "asynchronous write %lu",
         g->g_asynchronous_write_id);

  g->g_asynchronous_write_in_progress = true;
  g->g_asynchronous_write_id++;

  return 0;
}

static void graphd_idle_callback_islink(void* data,
                                        es_idle_callback_timed_out mode) {
  graphd_idle_islink_context* gii = data;
  graphd_handle* g = gii->gii_g;
  cl_handle* cl = g->g_cl;
#if 0
	int		        	  err;
#endif

  if (mode == ES_IDLE_CANCEL) {
    cl_log(cl, CL_LEVEL_VERBOSE, "graphd_idle_callback_checkpoint: cancel");
    return;
  }

  if (mode == ES_IDLE_TIMED_OUT)
    cl_log(cl, CL_LEVEL_ERROR,
           "graphd_idle_callback_checkpoint: "
           "Not a single idle second for "
           "11 days? Really???");

#if 0
	if ((err = graphd_islink_idle(g)) == GRAPHD_ERR_MORE)
	{
		/* Repost yourself.
		 */
		err = srv_idle_set(g->g_srv,
			&g->g_idle_islink.gii_srv,
			999999);
		if (err != 0)
			cl_log_errno(cl, CL_LEVEL_FAIL,
				"srv_idle_set", err,
				"can't re-install idle callback?");
	}
	else if (err != 0)
		cl_log_errno(cl, CL_LEVEL_FAIL,
			"graphd_islink_idle", err,
			"unexpected error");
#endif
  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_idle_callback_islink: done.");
}

int graphd_idle_install_islink(graphd_handle* g) {
  int err;

  err = srv_idle_set(g->g_srv, &g->g_idle_islink.gii_srv, 999999);
  if (err == SRV_ERR_ALREADY) err = 0;
  return err;
}

void graphd_idle_initialize(graphd_handle* g) {
  srv_idle_initialize(g->g_srv, &g->g_idle_checkpoint.gic_srv,
                      graphd_idle_callback_checkpoint);
  g->g_idle_checkpoint.gic_g = g;

  srv_idle_initialize(g->g_srv, &g->g_idle_islink.gii_srv,
                      graphd_idle_callback_islink);
  g->g_idle_islink.gii_g = g;
}

void graphd_idle_finish(graphd_handle* g) {
  srv_idle_delete(g->g_srv, &g->g_idle_checkpoint.gic_srv);
  srv_idle_delete(g->g_srv, &g->g_idle_islink.gii_srv);
}
