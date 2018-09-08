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

/*
 *  key: (null/linkage type-id null/endpoint-id)
 */
static int graphd_islink_status_key(graphd_request* greq, graphd_value* val,
                                    graphd_islink_key const* key) {
  cl_handle* cl = graphd_request_cl(greq);
  graphd_handle* g = graphd_request_graphd(greq);

  int err;

  err = graphd_value_list_alloc(g, greq->greq_req.req_cm, cl, val, 3);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_value_list_alloc", err,
                 "can't allocate three-element list");
    return err;
  }

  if (key->key_result_linkage == PDB_LINKAGE_N)
    graphd_value_null_set(val->val_list_contents);
  else {
    char const* linkage_str;

    linkage_str = pdb_linkage_to_string(key->key_result_linkage);
    graphd_value_text_set(val->val_list_contents, GRAPHD_VALUE_STRING,
                          linkage_str, linkage_str + strlen(linkage_str), NULL);
  }

  graphd_value_number_set(val->val_list_contents + 1, key->key_type_id);

  if (key->key_endpoint_id == PDB_ID_NONE)
    graphd_value_null_set(val->val_list_contents + 2);
  else
    graphd_value_number_set(val->val_list_contents + 2, key->key_endpoint_id);
  return 0;
}

/*
 *  group: (
 *		key
 *		null/nelems
 *	)
 */
static int graphd_islink_status_group(graphd_request* greq, graphd_value* val,
                                      graphd_islink_group const* group) {
  cl_handle* cl = graphd_request_cl(greq);
  cm_handle* cm = greq->greq_req.req_cm;
  graphd_handle* g = graphd_request_graphd(greq);
  graphd_islink_handle* ih = g->g_islink;

  graphd_islink_key key;
  int err;

  if ((err = graphd_value_list_alloc(g, cm, cl, val, 2)) != 0) return err;

  memcpy(&key, cm_hmem(&ih->ih_group, graphd_islink_group, group), sizeof key);
  err = graphd_islink_status_key(greq, val->val_list_contents, &key);
  if (err != 0) return err;

  if (group->group_idset == NULL)
    graphd_value_null_set(val->val_list_contents + 1);
  else
    graphd_value_number_set(val->val_list_contents + 1,
                            group->group_idset->gi_n);
  return 0;
}

/*
 *  type: (
 *		type-id
 *		(["draft"] / ["ok"])
 *		(null / #left-unique / "vast")
 *		(null / #right-unique / "vast")
 *	)
 */
static void graphd_islink_status_type_side(graphd_request* greq,
                                           graphd_value* val,
                                           graphd_islink_side const* side) {
  static char const vast_str[4] = "vast";

  if (side->side_vast)
    graphd_value_text_set(val, GRAPHD_VALUE_STRING, vast_str, vast_str + 4,
                          NULL);
  else if (side->side_idset == NULL)
    graphd_value_null_set(val);
  else
    graphd_value_number_set(val, side->side_idset->gi_n);
}

static int graphd_islink_status_type(graphd_request* greq, graphd_value* val,
                                     graphd_islink_type const* tp) {
  static char const draft_str[5] = "draft";
  static char const ok_str[5] = "ok";

  cl_handle* cl = graphd_request_cl(greq);
  cm_handle* cm = greq->greq_req.req_cm;
  graphd_handle* g = graphd_request_graphd(greq);
  graphd_islink_job* job;

  pdb_id type_id;
  int err;

  if ((err = graphd_value_list_alloc(g, cm, cl, val, 4)) != 0) return err;

  type_id = graphd_islink_type_id(g, tp);
  graphd_value_number_set(val->val_list_contents, type_id);

  /*  Is there a job still dealing with this type?
   */
  job = graphd_islink_type_job_lookup(g, type_id);
  if (job == NULL)
    graphd_value_text_set(val->val_list_contents + 1, GRAPHD_VALUE_STRING,
                          ok_str, ok_str + 2, NULL);
  else
    graphd_value_text_set(val->val_list_contents + 1, GRAPHD_VALUE_STRING,
                          draft_str, draft_str + 5, NULL);

  graphd_islink_status_type_side(greq, val->val_list_contents + 2, tp->tp_side);

  graphd_islink_status_type_side(greq, val->val_list_contents + 3,
                                 tp->tp_side + 1);

  return 0;
}

/*
 * job: (key count low)
 */
static int graphd_islink_status_job(graphd_request* greq, graphd_value* val,
                                    graphd_islink_job const* job) {
  cl_handle* cl = graphd_request_cl(greq);
  cm_handle* cm = greq->greq_req.req_cm;
  graphd_handle* g = graphd_request_graphd(greq);
  graphd_islink_handle* ih = g->g_islink;
  int err;
  graphd_islink_key key;

  if ((err = graphd_value_list_alloc(g, cm, cl, val, 4)) != 0) return err;

  memcpy(&key, cm_hmem(&ih->ih_job, graphd_islink_job, job), sizeof key);
  err = graphd_islink_status_key(greq, val->val_list_contents, &key);
  if (err != 0) return err;

  graphd_value_number_set(val->val_list_contents + 1, job->job_n);

  graphd_value_number_set(val->val_list_contents + 2, job->job_budget);

  graphd_value_number_set(val->val_list_contents + 3, job->job_low);

  return 0;
}

/* ((j1..jN) (t1...tN) (g1...gN))
 *
 * job:
 * type:
 * group:
 */
int graphd_islink_status(graphd_request* greq, graphd_value* val) {
  cl_handle* cl = graphd_request_cl(greq);
  cm_handle* cm = greq->greq_req.req_cm;
  graphd_handle* g = graphd_request_graphd(greq);
  graphd_islink_handle* ih = g->g_islink;
  graphd_islink_group const* group;
  graphd_islink_type const* tp;
  graphd_islink_job const* job;
  graphd_value* el;
  int err;

  if (ih == NULL) {
    graphd_value_null_set(val);
    return 0;
  }

  if ((err = graphd_value_list_alloc(g, cm, cl, val, 3)) != 0 ||
      (err = graphd_value_list_alloc(g, cm, cl, val->val_list_contents,
                                     cm_hashnelems(&ih->ih_job))) != 0 ||
      (err = graphd_value_list_alloc(g, cm, cl, val->val_list_contents + 1,
                                     cm_hashnelems(&ih->ih_type))) != 0 ||
      (err = graphd_value_list_alloc(g, cm, cl, val->val_list_contents + 2,
                                     cm_hashnelems(&ih->ih_group))) != 0)
    goto err;

  el = val->val_list_contents[0].val_list_contents;
  for (job = NULL;
       (job = cm_hnext(&ih->ih_job, graphd_islink_job, job)) != NULL; el++) {
    err = graphd_islink_status_job(greq, el, job);
    if (err != 0) goto err;
  }

  el = val->val_list_contents[1].val_list_contents;
  for (tp = NULL; (tp = cm_hnext(&ih->ih_type, graphd_islink_type, tp)) != NULL;
       el++) {
    err = graphd_islink_status_type(greq, el, tp);
    if (err != 0) goto err;
  }

  el = val->val_list_contents[2].val_list_contents;
  for (group = NULL;
       (group = cm_hnext(&ih->ih_group, graphd_islink_group, group)) != NULL;
       el++) {
    err = graphd_islink_status_group(greq, el, group);
    if (err != 0) goto err;
  }

  return 0;

err:
  graphd_value_finish(cl, val);
  return err;
}
