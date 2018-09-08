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

/*  Look up an existing group in the group hashtable.
 *  Return NULL if it does't exist.
 */
graphd_islink_group* graphd_islink_group_lookup(graphd_handle* g,
                                                graphd_islink_key const* key) {
  if (g->g_islink == NULL) return NULL;

  return cm_haccess(&g->g_islink->ih_group, graphd_islink_group, key,
                    sizeof *key);
}

/*  Create a new group, based on its idset.
 *  It the group existed, the idset is just freed.
 */
int graphd_islink_group_create(graphd_handle* g, graphd_islink_key const* key,
                               graph_idset* idset) {
  graphd_islink_group* group;

  group =
      cm_hnew(&g->g_islink->ih_group, graphd_islink_group, key, sizeof *key);
  if (group == NULL) return errno ? errno : ENOMEM;

  if (group->group_idset == NULL) {
    group->group_idset = idset;
    graph_idset_link(idset);
  }
  return 0;
}

void graphd_islink_group_finish(graphd_handle* g, graphd_islink_group* group) {
  if (group != NULL) {
    if (group->group_idset != NULL) {
      graph_idset_free(group->group_idset);
      group->group_idset = NULL;
    }
  }
}

/*  Get the cached set for a given key.
 */
graph_idset* graphd_islink_group_idset(graphd_handle* g,
                                       graphd_islink_key const* key) {
  graphd_islink_group* group;

  if ((group = graphd_islink_group_lookup(g, key)) == NULL) return NULL;

  return group->group_idset;
}

/*  Check whether the id <id> is in the cached set for <key>,
 *  if we have one.
 *
 *  @return GRAPHD_ERR_NO if no
 *  @return 0 if yes
 *  @return GRAPHD_ERR_MORE if we don't have that cached.
 */
int graphd_islink_group_check(graphd_handle* g, graphd_islink_key const* key,
                              pdb_id id) {
  graphd_islink_group* group;

  /*  Doesn't exist?  -> We don't know.
   */
  if ((group = graphd_islink_group_lookup(g, key)) == NULL)
    return GRAPHD_ERR_MORE;

  /*  Yes or no.
   */
  return graph_idset_check(group->group_idset, id) ? 0 : GRAPHD_ERR_NO;
}

/*  Add an ID to a group, if that group already exists.
 */
int graphd_islink_group_update(graphd_handle* g, pdb_id result_id,
                               int result_linkage, pdb_id type_id,
                               pdb_id endpoint_id) {
  cl_handle* cl = g->g_cl;
  graphd_islink_group* group;
  graphd_islink_key key;
  int err;

  /*  Are we tracking that group?
   */
  group = graphd_islink_group_lookup(
      g, graphd_islink_key_make(g, result_linkage, type_id, endpoint_id, &key));
  if (group == NULL) return 0;

  /*  Add the ID to the group's idset.
   */
  err = graph_idset_insert(group->group_idset, result_id);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graph_idset_insert", err, "id=%llu",
                 (unsigned long long)result_id);
    return err;
  }
  return 0;
}

static int graphd_islink_group_job_complete(graphd_handle* g,
                                            graphd_islink_job* job) {
  cl_handle* cl = g->g_cl;
  graphd_islink_handle* ih = g->g_islink;
  graphd_islink_key key;
  int err;

  /*  Get a well-aligned key for this job.
   */
  memcpy(&key, cm_hmem(&ih->ih_job, graphd_islink_job, job), sizeof(key));

  /*  Create or access the group for this key.
   */
  err = graphd_islink_group_create(g, &key, job->job_idset);
  if (err != 0) {
    char buf[200];
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_islink_group_create", err, "key=%s",
                 graphd_islink_key_to_string(&key, buf, sizeof buf));

    /* Continue */
  }

  /* Destroy the job.
   */
  graphd_islink_job_free(g, job);

  return err;
}

static int group_job_run(graphd_islink_job* job, graphd_handle* g,
                         pdb_budget* budget_inout) {
  pdb_handle* pdb = g->g_pdb;
  cl_handle* cl = g->g_cl;
  graphd_islink_handle* ih = g->g_islink;
  pdb_iterator* it = NULL;
  int err;
  graphd_islink_key key;
  pdb_budget budget_in = *budget_inout;
  char buf[200];
  bool true_vip;

  /*  Get a well-aligned key for this job.
   */
  memcpy(&key, cm_hmem(&ih->ih_job, graphd_islink_job, job), sizeof(key));

  /*  Open the VIP or regular iterator for this type
   *  and this endpoint (if specified).
   */

  if (key.key_endpoint_id != PDB_ID_NONE) {
    int endpoint_linkage;
    graph_guid type_guid;

    endpoint_linkage = graphd_islink_key_endpoint_linkage(&key);
    err = pdb_id_to_guid(pdb, key.key_type_id, &type_guid);
    if (err != 0) return err;

    err = pdb_vip_linkage_id_iterator(pdb, key.key_endpoint_id,
                                      endpoint_linkage, &type_guid,
                                      job->job_low, PDB_ITERATOR_HIGH_ANY,
                                      /* forward */ true,
                                      /* error-if-null */ true, &it, &true_vip);
  } else {
    err = pdb_linkage_id_iterator(pdb, PDB_LINKAGE_TYPEGUID, key.key_type_id,
                                  job->job_low, PDB_ITERATOR_HIGH_ANY,
                                  /* forward */ true,
                                  /* error-if-null */ true, &it);
  }

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
    pdb_id id, result_id;
    graph_guid guid;

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
     *  reading lots of them in order.
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

    /*  Result linkage?
     */
    if (!pdb_primitive_has_linkage(&pr, key.key_result_linkage)) continue;

    pdb_primitive_linkage_get(&pr, key.key_result_linkage, guid);
    err = pdb_id_from_guid(pdb, &result_id, &guid);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "guid=%s",
                   graph_guid_to_string(&guid, buf, sizeof buf));
      pdb_primitive_finish(pdb, &pr);
      pdb_iterator_destroy(pdb, &it);
      return err;
    }

    pdb_primitive_finish(pdb, &pr);

    err = graph_idset_insert(job->job_idset, result_id);
    if (err != 0) return err;

    /*  Done with this primitive.  Update the
     *  low end of the iterator.
     */
    job->job_low = id + 1;

    if (*budget_inout < 0) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "type_job_run: out of budget at job_low=%llx",
             (unsigned long long)job->job_low);
      err = GRAPHD_ERR_MORE;
      break;
    }
  }
  pdb_iterator_destroy(pdb, &it);

done: /* We're done (possibly because we overshot)?
       */
  job->job_budget += budget_in - *budget_inout;

  if (err == PDB_ERR_NO || err == 0)
    return graphd_islink_group_job_complete(g, job);

  /*  We ran out of budget?
   */
  else if (err == PDB_ERR_MORE)
    return 0;

  /*  Unexpected error.
   */
  return err;
}

/*  Given vip key ingredients, create the group.
 *  If the endpoint_id is PDB_ID_NONE, the key is not VIP,
 *  just the result-linkage endpoints of <type>.
 *
 *  This function takes however much time it takes, and
 *  can be used to fine-tune the cache contents from the
 *  command line.
 */
int graphd_islink_group_job_make(graphd_handle* g, int result_linkage,
                                 pdb_id type_id, pdb_id endpoint_id) {
  graphd_islink_job* job;
  graphd_islink_key key;

  /*  Create a job for constructing just
   *  this specific group.
   */
  job = graphd_islink_job_alloc(
      g, graphd_islink_key_make(g, result_linkage, type_id, endpoint_id, &key));
  if (job == NULL) return errno ? errno : ENOMEM;

  /* Was this one new?
   */
  if (job->job_run == NULL) {
    job->job_low = PDB_ITERATOR_LOW_ANY;
    job->job_run = group_job_run;
  }
  return 0;
}
