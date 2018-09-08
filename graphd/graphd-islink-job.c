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
#include "graphd/graphd-islink.h"

#include <errno.h>

/*  Islink jobs run when the system has time or donates some budget.
 *  Eventually, they terminate and create groups and relationship entries.
 *  While they're running, the system knows what the job is doing,
 *  and knows not to restart it.
 */

/*  Jobs revolve around:
 *
 *  a typeguid
 *  a typeguid and a linkage
 *  a typeguid, an endpoint, and a linkage
 *
 *  In cases 2 and 3, the job is reconstructing an IDSET that
 *  previously existed, and that is needed for an iterator cursor
 *  that encoded a reference to it.
 *
 *  As their output, jobs create and update a per-typeguid
 *  record.
 */

/* Given a key, look up a job.
 */
graphd_islink_job *graphd_islink_job_lookup(graphd_handle *g,
                                            graphd_islink_key const *key) {
  graphd_islink_handle *ih = g->g_islink;

  if (ih == NULL) return NULL;
  return cm_haccess(&ih->ih_job, graphd_islink_job, key, sizeof *key);
}

graphd_islink_job *graphd_islink_job_alloc(graphd_handle *g,
                                           graphd_islink_key const *key) {
  graphd_islink_handle *ih = g->g_islink;
  graphd_islink_job *job;

  /*  Hey, server engine!  Call us when you're idle.
   *  (Duplicate calls are fine.)
   */
  (void)graphd_idle_install_islink(g);

  /* Allocate or retrieve this job.
   */
  job = cm_hnew(&ih->ih_job, graphd_islink_job, key, sizeof *key);
  if (job == NULL) {
    int err = errno ? errno : ENOMEM;

    cl_log_errno(g->g_cl, CL_LEVEL_ERROR, "graphd_islink_job_alloc", err,
                 "failed to allocate new job in "
                 "hashtable");
    errno = err;

    return NULL;
  }

  /*  Create an idset to collect the results in.
   */
  if (job->job_idset == NULL) {
    job->job_idset = graph_idset_tile_create(g->g_graph);
    if (job->job_idset == NULL) {
      cm_hdelete(&ih->ih_job, graphd_islink_job, job);
      return NULL;
    }
  }
  return job;
}

void graphd_islink_job_finish(graphd_handle *g, graphd_islink_job *job) {
  if (job != NULL) {
    /*  Free the job's results.
     */
    if (job->job_idset != NULL) {
      graph_idset_free(job->job_idset);
      job->job_idset = NULL;
    }
  }
}

void graphd_islink_job_free(graphd_handle *g, graphd_islink_job *job) {
  graphd_islink_handle *ih = g->g_islink;

  if (job == NULL) return;

  graphd_islink_job_finish(g, job);
  cm_hdelete(&ih->ih_job, graphd_islink_job, job);
}

/* Work on the job for a specific key.  Results:
 *
 * 	0  			We're done.
 *	GRAPHD_ERR_NO		There was no such job in progress to begin with.
 * 	GRAPHD_ERR_MORE		The job exists and needs some more time.
 */
int graphd_islink_job_run(graphd_handle *g, graphd_islink_key const *key,
                          pdb_budget *budget_inout) {
  graphd_islink_handle *ih = g->g_islink;
  graphd_islink_job *job;
  int err;
  char buf[200];

  if (ih == NULL) return GRAPHD_ERR_NO;

  if (key == NULL)
    job = cm_hnext(&ih->ih_job, graphd_islink_job, NULL);
  else
    job = cm_haccess(&ih->ih_job, graphd_islink_job, key, sizeof(*key));

  /* Done already? */
  if (job == NULL) return 0;

  /* Run the job.
   */
  cl_enter(g->g_cl, CL_LEVEL_VERBOSE, "run %s",
           graphd_islink_key_to_string(key, buf, sizeof buf));

  cl_assert(g->g_cl, job->job_run != NULL);
  err = (*job->job_run)(job, g, budget_inout);

  cl_leave(g->g_cl, CL_LEVEL_VERBOSE, "run: %s",
           err == 0 ? "ok" : graphd_strerror(err));

  if (err != 0 && err != GRAPHD_ERR_MORE) {
    cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "job->job_run()", err, "job %p",
                 (void *)job);
    graphd_islink_job_free(g, job);
    return err;
  }

  /* Are we done *now*?
   */
  if ((key == NULL ? cm_hnext(&ih->ih_job, graphd_islink_job, NULL)
                   : cm_haccess(&ih->ih_job, graphd_islink_job, key,
                                sizeof *key)) != NULL)

    /* No, just out of budget. */
    return GRAPHD_ERR_MORE;

  /* All done! */
  return 0;
}
