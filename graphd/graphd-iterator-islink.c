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

static int islink_recover_callback(void *data, graphd_handle *g,
                                   graph_idset **idset_inout,
                                   pdb_budget *budget_inout);

static void islink_finish_callback(void *data, graphd_handle *g,
                                   graph_idset *idset) {
  cm_free(g->g_cm, data);
}

/*  Given a key, try to create an iterator for it.
 *  If we have no set for this on file, the create fails
 *  with GRAPHD_ERR_NO.
 */
static int islink_make_loc(graphd_handle *g, graph_idset *idset,
                           unsigned long long low, unsigned long long high,
                           bool forward, graphd_islink_key const *key,
                           pdb_iterator **it_out, char const *file, int line) {
  char buf[1024];
  char idbuf[200];

  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = g->g_cl;

  pdb_primitive_summary psum;
  int err;
  graphd_islink_key *key_dup;

  if ((err = graphd_islink_key_psum(g, key, &psum)) != 0) {
    *it_out = NULL;
    return err;
  }

  key_dup = cm_malloc(g->g_cm, sizeof(*key));
  if (key_dup == NULL) return errno ? errno : ENOMEM;
  *key_dup = *key;

  if (high == PDB_ITERATOR_HIGH_ANY)
    snprintf(buf, sizeof buf, "islink:%s%llu:%s<-%s(%llu)", forward ? "" : "~",
             low,
             pdb_id_to_string(pdb, key->key_endpoint_id, idbuf, sizeof idbuf),
             pdb_linkage_to_string(key->key_result_linkage),
             (unsigned long long)key->key_type_id);
  else
    snprintf(buf, sizeof buf, "islink:%s%llu-%llu:%s<-%s(%llu)",
             forward ? "" : "~", low, high,
             pdb_id_to_string(pdb, key->key_endpoint_id, idbuf, sizeof idbuf),
             pdb_linkage_to_string(key->key_result_linkage),
             (unsigned long long)key->key_type_id);

  /* Delegate everything to the idset.
   */
  err = graphd_iterator_idset_create_loc(
      g, low, high, forward, idset, buf, &psum, islink_recover_callback,
      (void *)key_dup, islink_finish_callback, (void *)key_dup, it_out, file,
      line);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_idset_create_loc", err,
                 "%s [from %s:%d]", buf, file, line);
    *it_out = NULL;

    cm_free(g->g_cm, key_dup);
  }
  return err;
}

/*  Given a key, try to create an iterator for it.
 *  If we have no set for this on file, the create fails
 *  with GRAPHD_ERR_NO.
 */
int graphd_iterator_islink_create_loc(graphd_handle *g, unsigned long long low,
                                      unsigned long long high, bool forward,
                                      graphd_islink_key const *key,
                                      pdb_iterator **it_out, char const *file,
                                      int line) {
  graph_idset *idset;

  if ((idset = graphd_islink_group_idset(g, key)) == NULL) {
    *it_out = NULL;
    return GRAPHD_ERR_NO;
  }

  return islink_make_loc(g, idset, low, high, forward, key, it_out, file, line);
}

static int islink_recover_callback(void *data, graphd_handle *g,
                                   graph_idset **idset_inout,
                                   pdb_budget *budget_inout) {
  graphd_islink_key const *key = data;
  int err;

  /*  When an idset iterator was thawed, its underlying set
   *  was gone.  We need to rebuild it before resuming the
   *  iterator's work.
   */
  err = graphd_islink_job_run(g, key, budget_inout);
  if (err != 0 && err != GRAPHD_ERR_NO) return err;

  /* Does the job still exist?
   */
  if (graphd_islink_job_lookup(g, key) != NULL) return GRAPHD_ERR_MORE;

  /* Either the group exists, or the result is empty.
   */
  *idset_inout = graphd_islink_group_idset(g, key);
  if (*idset_inout != NULL)
    graph_idset_link(*idset_inout);
  else {
    *idset_inout = graph_idset_tile_create(g->g_graph);
    if (*idset_inout == NULL) return errno ? errno : ENOMEM;
  }
  return 0;
}

/*  Given a key, try to create an iterator for it.
 *  If we have no set for this on file, it is recreated from scratch.
 */
int graphd_iterator_islink_thaw_loc(graphd_handle *g,
                                    pdb_iterator_text const *pit,
                                    pdb_iterator_base *pib,
                                    cl_loglevel loglevel, pdb_iterator **it_out,
                                    char const *file, int line) {
  graphd_islink_key key;
  graph_idset *idset = NULL;
  unsigned long long low, high;
  pdb_id end_id, type_id;
  char const *s, *e;
  int linkage, err;
  cl_handle *cl = g->g_cl;
  bool forward;

  s = pit->pit_set_s;
  e = pit->pit_set_e;
  cl_assert(cl, s && e);

  err = pdb_iterator_util_thaw(
      g->g_pdb, &s, e, "%{forward}%{low[-high]}:%{id}<-%{linkage}(%{id})",
      &forward, &low, &high, &end_id, &linkage, &type_id);
  if (err != 0) {
    return err;
  }

  /*  If the group doesn't exist yet, we start up
   *  with recovery.
   */
  idset = graphd_islink_group_idset(
      g, graphd_islink_key_make(g, linkage, type_id, end_id, &key));

  err = islink_make_loc(g, idset, low, high, forward, &key, it_out, file, line);
  if (err != 0) {
    cl_log_errno(cl, loglevel, "graphd_iterator_idset_position_thaw", err,
                 "text: \"%.*s\" [from %s:%d]",
                 (int)(pit->pit_position_e - pit->pit_position_s),
                 pit->pit_position_s, file, line);
    return err;
  }
  err = graphd_iterator_idset_position_thaw_loc(g, *it_out, pit, loglevel, file,
                                                line);
  if (err != 0) {
    cl_log_errno(cl, loglevel, "graphd_iterator_idset_position_thaw", err,
                 "text: \"%.*s\" [from %s:%d]",
                 (int)(pit->pit_position_e - pit->pit_position_s),
                 pit->pit_position_s, file, line);
    pdb_iterator_destroy(g->g_pdb, it_out);

    return err;
  }
  return 0;
}
