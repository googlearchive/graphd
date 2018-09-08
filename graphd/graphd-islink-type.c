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

#define GRAPHD_ISLINK_TYPE_SMALL 50

static graphd_islink_key* type_job_key(graphd_handle* g, pdb_id type_id,
                                       graphd_islink_key* buf) {
  return graphd_islink_key_make(g, PDB_LINKAGE_N, type_id, PDB_ID_NONE, buf);
}

/*  Return the ID of a type.  (The type is hashed by the ID, but
 *  the bytes in the hash key may not be aligned for casting
 *  to a (pdb_id *).)
 */
pdb_id graphd_islink_type_id(graphd_handle* g, graphd_islink_type const* tp) {
  pdb_id id;
  graphd_islink_handle* ih = g->g_islink;

  cl_assert(g->g_cl, ih != NULL);
  cl_assert(g->g_cl, tp != NULL);
  cl_assert(g->g_cl,
            cm_hsize(&ih->ih_type, graphd_islink_type, tp) == sizeof id);

  memcpy(&id, cm_hmem(&ih->ih_type, graphd_islink_type, tp), sizeof id);
  return id;
}

/*  Free resources allocated for a type entry.
 */
void graphd_islink_type_finish(graphd_handle* g, graphd_islink_type* tp) {
  pdb_id type_id;
  graphd_islink_handle* ih;

  if (tp == NULL || !tp->tp_initialized || (ih = g->g_islink) == NULL) return;

  type_id = graphd_islink_type_id(g, tp);

  graphd_islink_side_finish(g, tp->tp_side + GRAPHD_ISLINK_RIGHT,
                            PDB_LINKAGE_RIGHT, type_id);

  graphd_islink_side_finish(g, tp->tp_side + GRAPHD_ISLINK_LEFT,
                            PDB_LINKAGE_LEFT, type_id);

  tp->tp_initialized = false;
}

/*  Initialize resources allocated for a type entry.
 */
static int graphd_islink_type_initialize(graphd_handle* g,
                                         graphd_islink_type* tp) {
  int err;

  if (tp->tp_initialized) return 0;

  err = graphd_islink_side_initialize(g, tp->tp_side + GRAPHD_ISLINK_RIGHT);
  if (err != 0) {
    cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "graphd_islink_side_initialize", err,
                 "can't initialize right");
    return err;
  }
  err = graphd_islink_side_initialize(g, tp->tp_side + GRAPHD_ISLINK_LEFT);
  if (err != 0) {
    pdb_id type_id;

    type_id = graphd_islink_type_id(g, tp);

    graphd_islink_side_finish(g, tp->tp_side + GRAPHD_ISLINK_RIGHT,
                              PDB_LINKAGE_RIGHT, type_id);

    cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "graphd_islink_side_initialize", err,
                 "can't initialize left");
    return err;
  }

  tp->tp_initialized = true;
  return 0;
}

static int graphd_islink_type_complete(graphd_handle* g,
                                       graphd_islink_type* tp) {
  int err = 0;
  pdb_id type_id;

  type_id = graphd_islink_type_id(g, tp);

  cl_enter(g->g_cl, CL_LEVEL_VERBOSE, "type=%llx", (unsigned long long)type_id);

  err = graphd_islink_side_complete(g, tp->tp_side + GRAPHD_ISLINK_RIGHT,
                                    PDB_LINKAGE_RIGHT, type_id);
  if (err == 0)
    err = graphd_islink_side_complete(g, tp->tp_side + GRAPHD_ISLINK_LEFT,
                                      PDB_LINKAGE_LEFT, type_id);

  cl_leave(g->g_cl, CL_LEVEL_VERBOSE, "%s",
           err ? graphd_strerror(err) : "done");
  return err;
}

/*  Look up a type entry.
 */
graphd_islink_type* graphd_islink_type_lookup(graphd_handle* g,
                                              pdb_id type_id) {
  graphd_islink_handle* ih;

  if ((ih = g->g_islink) == NULL) return NULL;

  return cm_haccess(&ih->ih_type, graphd_islink_type, &type_id, sizeof type_id);
}

/*  Find or make a type entry.
 */
static graphd_islink_type* graphd_islink_type_make(graphd_handle* g,
                                                   pdb_id type_id) {
  graphd_islink_type* tp;
  graphd_islink_handle* ih;

  if ((ih = g->g_islink) == NULL) return NULL;

  tp = cm_hnew(&ih->ih_type, graphd_islink_type, &type_id, sizeof type_id);
  if (tp == NULL) {
    cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "cm_hnew", errno,
                 "can't allocate type "
                 "table");
    return NULL;
  }

  if (!tp->tp_initialized) {
    int err = graphd_islink_type_initialize(g, tp);
    if (err != 0) {
      cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "graphd_islink_type_initialize", err,
                   "unexpected error");
      return NULL;
    }
  }
  return tp;
}

static int type_job_run(graphd_islink_job* job, graphd_handle* g,
                        pdb_budget* budget_inout) {
  pdb_handle* pdb = g->g_pdb;
  cl_handle* cl = g->g_cl;
  graphd_islink_handle* ih = g->g_islink;
  pdb_iterator* it = NULL;
  graphd_islink_type* tp;
  int err;
  graphd_islink_key key;
  pdb_budget budget_in = *budget_inout;
  char buf[200];

  /*  Get a well-aligned key for this job.
   */
  memcpy(&key, cm_hmem(&ih->ih_job, graphd_islink_job, job), sizeof(key));

  /*  Get the type storage for this job.
   */
  tp = graphd_islink_type_make(g, key.key_type_id);
  if (tp == NULL) return errno ? errno : ENOMEM;

  /*  Make an iterator that produces instances of this
   *  type.  If we had one previously, job->job_low is where
   *  we should (re-)start this time.
   */
  err = pdb_linkage_id_iterator(pdb, PDB_LINKAGE_TYPEGUID, key.key_type_id,
                                job->job_low, PDB_ITERATOR_HIGH_ANY,
                                /* forward */ true,
                                /* error-if-null */ true, &it);
  if (err != 0) {
    if (err == PDB_ERR_NO) goto done;

    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_linkage_id_iterator", err, "type=%llx",
                 (unsigned long long)key.key_type_id);
    cl_leave(cl, CL_LEVEL_VERBOSE, "%s", graphd_strerror(err));
    return err;
  }

  /*  As long as we have budget, pull IDs out of the iterator.
   */
  for (;;) {
    pdb_primitive pr;
    pdb_id id, left_id, right_id;

    err = pdb_iterator_next(pdb, it, &id, budget_inout);
    if (err != 0) {
      if (err != PDB_ERR_NO && err != PDB_ERR_MORE) {
        char buf[200];
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "it=%s",
                     pdb_iterator_to_string(pdb, it, buf, sizeof buf));
        return err;
      }
      break;
    }

    /*  We'll take just half a hit for the primitive - we're
     *  reading them in order.
     */
    *budget_inout -= PDB_COST_PRIMITIVE / 2;

    /*  Read the primitive.
     */
    if ((err = pdb_id_read(pdb, id, &pr)) != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%llx",
                   (unsigned long long)id);
      pdb_iterator_destroy(pdb, &it);
      return err;
    }

    job->job_n++;

    /*  Right and left side?
     */
    if (!pdb_primitive_has_left(&pr))
      left_id = PDB_ID_NONE;
    else {
      graph_guid guid;
      pdb_primitive_left_get(&pr, guid);
      err = pdb_id_from_guid(pdb, &left_id, &guid);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s",
                     graph_guid_to_string(&guid, buf, sizeof buf));
        pdb_primitive_finish(pdb, &pr);
        pdb_iterator_destroy(pdb, &it);
        return err;
      }
    }

    if (!pdb_primitive_has_right(&pr))
      right_id = PDB_ID_NONE;
    else {
      graph_guid guid;
      pdb_primitive_right_get(&pr, guid);
      err = pdb_id_from_guid(pdb, &right_id, &guid);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s",
                     graph_guid_to_string(&guid, buf, sizeof buf));
        pdb_primitive_finish(pdb, &pr);
        pdb_iterator_destroy(pdb, &it);
        return err;
      }
    }
    pdb_primitive_finish(pdb, &pr);

    /* Add the right ID to the right side.
     */
    if (right_id != PDB_ID_NONE &&
        !tp->tp_side[GRAPHD_ISLINK_RIGHT].side_vast) {
      err = graphd_islink_side_add(g, tp->tp_side + GRAPHD_ISLINK_RIGHT,
                                   PDB_LINKAGE_RIGHT, right_id, key.key_type_id,
                                   left_id, id);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_islink_side_add", err,
                     "right_id=%llx", (unsigned long long)right_id);
        pdb_iterator_destroy(pdb, &it);
        return err;
      }
    }

    /* Add the left ID to the left side.
     */
    if (left_id != PDB_ID_NONE && !tp->tp_side[GRAPHD_ISLINK_LEFT].side_vast) {
      err = graphd_islink_side_add(g, tp->tp_side + GRAPHD_ISLINK_LEFT,
                                   PDB_LINKAGE_LEFT, left_id, key.key_type_id,
                                   right_id, id);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_islink_side_add", err,
                     "left_id=%llx", (unsigned long long)left_id);
        pdb_iterator_destroy(pdb, &it);
        return err;
      }
    }

    /*  Done with this primitive.  Update the
     *  low end of the iterator.
     */
    job->job_low = id + 1;

    /*  Stop searching if the number of unique entries
     *  on both sides has risen above the interesting point.
     */
    if (tp->tp_side[GRAPHD_ISLINK_RIGHT].side_vast &&
        tp->tp_side[GRAPHD_ISLINK_LEFT].side_vast) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "type_job_run: both sides are vast - "
             "aborting.");
      err = 0;
      break;
    }

    if (*budget_inout < 0) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "type_job_run: out of budget at job_low=%llx",
             (unsigned long long)job->job_low);
      err = GRAPHD_ERR_MORE;
      break;
    }
  }
  pdb_iterator_destroy(pdb, &it);

done: /* We're done (possibly because we overshot).
       */
  job->job_budget += budget_in - *budget_inout;

  if (err == PDB_ERR_NO || err == 0) {
    err = graphd_islink_type_complete(g, tp);
    if (err != 0) return err;

    graphd_islink_job_free(g, job);
    return 0;
  }

  /*  We ran out of budget.
   */
  else if (err == PDB_ERR_MORE)
    return 0;

  /*  Unexpected error.
   */
  return err;
}

/* Look up an existing type job.
 */
graphd_islink_job* graphd_islink_type_job_lookup(graphd_handle* g,
                                                 pdb_id type_id) {
  graphd_islink_key key;

  return graphd_islink_job_lookup(g, type_job_key(g, type_id, &key));
}

/* Create a job that iterates over all instances of a typeguid.
 */
static graphd_islink_job* graphd_islink_type_job_make(graphd_handle* g,
                                                      pdb_id type_id) {
  graphd_islink_job* job;
  graphd_islink_key key;

  /*  Create a job for constructing the type.
   */
  job = graphd_islink_job_alloc(g, type_job_key(g, type_id, &key));
  if (job == NULL) return NULL;

  /* Already initialized?
   */
  if (job->job_run != NULL) return job;

  job->job_low = PDB_ITERATOR_LOW_ANY;
  job->job_run = type_job_run;

  return job;
}

/*  Add a new type_id to the table.  If it's actually new,
 *  create a job to fill in the details.
 */
int graphd_islink_type_add_id(graphd_handle* g, pdb_id type_id) {
  graphd_islink_type* tp;

  /*  If we don't already have a complete type
   *  record for this typeguid, create one and
   *  proceed.
   */
  tp = graphd_islink_type_lookup(g, type_id);
  if (tp != NULL) return 0;

  /* Create a type and a job to fill it in.
   */
  tp = graphd_islink_type_make(g, type_id);
  if (tp == NULL) return errno ? errno : ENOMEM;

  if (graphd_islink_type_job_make(g, type_id) == NULL)
    return errno ? errno : ENOMEM;

  cl_log(g->g_cl, CL_LEVEL_VERBOSE, "graphd_islink_type_add_id %llx",
         (unsigned long long)type_id);
  return 0;
}

/*  A typeguid is being noticed.  If this is the first time we
 *  see that typeguid, add a type and a job to fill in
 *  its details.
 *
 *  The caller - probably graphd_islink_typeguid() - makes sure
 *  that the typeguid type is large enough to be valueable.
 */
int graphd_islink_type_add_guid(graphd_handle* g, graph_guid const* type_guid) {
  pdb_id type_id;
  int err;

  err = pdb_id_from_guid(g->g_pdb, &type_id, type_guid);
  if (err != 0) return err;

  return graphd_islink_type_add_id(g, type_id);
}
