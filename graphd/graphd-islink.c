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

#define GRAPHD_ISLINK_IDLE_BUDGET 100000

#define GUIDCON_IS_UNSPECIFIED(guidcon)                                  \
  (!(guidcon).guidcon_match_valid && !(guidcon).guidcon_include_valid && \
   !(guidcon).guidcon_exclude_valid)

typedef struct graphd_islink_context {
  graphd_stack_context gic_sc;

  /*  Which job (if any) are we running here?
   */
  graphd_islink_key gic_key;
  unsigned int gic_key_valid : 1;

  graphd_request *gic_greq;
  int gic_err;
  int *gic_err_out;

} graphd_islink_context;

/*  Given a new primitive, update any cache that fits it.
 */
static int islink_primitive_callback(void *data, pdb_handle *pdb, pdb_id id,
                                     pdb_primitive const *pr) {
  graphd_handle *g = data;
  graph_guid r_guid, l_guid, t_guid;
  pdb_id r_id, l_id, t_id;
  int err;

  if (id == PDB_ID_NONE) {
    /*  Truncate the cache - we're emptying out our database.
     */
    err = graphd_islink_truncate(g);
    if (err != 0)
      cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "graphd_islink_truncate", err,
                   "can't reallocate islink database "
                   "after truncate?");
    return err;
  }

  /*  Ignore additions that aren't typed links;
   *  also ignore things if islink isn't on.
   */
  if (g->g_islink == NULL || !pdb_primitive_has_typeguid(pr) ||
      (!pdb_primitive_has_left(pr) && !pdb_primitive_has_right(pr)))
    return 0;

  pdb_primitive_typeguid_get(pr, t_guid);
  err = pdb_id_from_guid(pdb, &t_id, &t_guid);
  if (err != 0) return err;

  if (pdb_primitive_has_right(pr)) {
    pdb_primitive_right_get(pr, r_guid);
    err = pdb_id_from_guid(pdb, &r_id, &r_guid);
    if (err != 0) return err;
  } else
    r_id = PDB_ID_NONE;

  if (pdb_primitive_has_left(pr)) {
    pdb_primitive_left_get(pr, l_guid);
    err = pdb_id_from_guid(pdb, &l_id, &l_guid);
    if (err != 0) return err;
  } else
    l_id = PDB_ID_NONE;

  /*  The ID may fit in four places:
   *
   *  - all left,
   *  - left for this right
   *  - all right,
   *  - right for this left.
   */
  if (r_id != PDB_ID_NONE && l_id != PDB_ID_NONE) {
    err = graphd_islink_group_update(g, r_id, PDB_LINKAGE_RIGHT, t_id, l_id);
    if (err != 0) return err;

    err = graphd_islink_group_update(g, l_id, PDB_LINKAGE_LEFT, t_id, r_id);
    if (err != 0) return err;
  }
  if (r_id != PDB_ID_NONE) {
    err = graphd_islink_group_update(g, r_id, PDB_LINKAGE_RIGHT, t_id,
                                     PDB_ID_NONE);
    if (err != 0) return err;
  }
  if (l_id != PDB_ID_NONE) {
    err = graphd_islink_group_update(g, l_id, PDB_LINKAGE_LEFT, t_id, r_id);
    if (err != 0) return err;
  }

  return 0;
}

/*  Add a subscription.
 */
int graphd_islink_subscribe(graphd_handle *g) {
  int err;

  cl_assert(g->g_cl, g->g_islink != NULL);

  if (g->g_islink->ih_subscribed) return 0;

  err = pdb_primitive_alloc_subscription_add(g->g_pdb,
                                             islink_primitive_callback, g);
  if (err != 0) return err;

  g->g_islink->ih_subscribed = true;
  return 0;
}

/*  Initialize the islink module.
 */
int graphd_islink_initialize(graphd_handle *g) {
  graphd_islink_handle *ih;
  int err;

  if ((ih = cm_malloc(g->g_cm, sizeof(*ih))) == NULL)
    return errno ? errno : ENOMEM;
  memset(ih, 0, sizeof(*ih));

  err = cm_hashinit(g->g_cm, &ih->ih_group, sizeof(graphd_islink_group), 100);
  if (err != 0) {
    cm_free(g->g_cm, ih);
    return err;
  }

  err = cm_hashinit(g->g_cm, &ih->ih_job, sizeof(graphd_islink_job), 100);
  if (err != 0) {
    cm_hashfinish(&ih->ih_group);
    cm_free(g->g_cm, ih);

    return err;
  }

  err = cm_hashinit(g->g_cm, &ih->ih_type, sizeof(graphd_islink_type), 1000);
  if (err != 0) {
    cm_hashfinish(&ih->ih_group);
    cm_hashfinish(&ih->ih_job);
    cm_free(g->g_cm, ih);

    return err;
  }
  g->g_islink = ih;
  return 0;
}

/*  Free the resources of the islink module.
 */
void graphd_islink_finish(graphd_handle *g) {
  graphd_islink_handle *ih = g->g_islink;

  if (ih != NULL) {
    graphd_islink_group *group;
    graphd_islink_type *tp;
    graphd_islink_job *job;
    graphd_islink_intersect *ii;

    cl_enter(g->g_cl, CL_LEVEL_VERBOSE, "enter");

    job = NULL;
    while ((job = cm_hnext(&ih->ih_job, graphd_islink_job, job)) != NULL)
      graphd_islink_job_finish(g, job);
    cm_hashfinish(&ih->ih_job);

    tp = NULL;
    while ((tp = cm_hnext(&ih->ih_type, graphd_islink_type, tp)) != NULL)
      graphd_islink_type_finish(g, tp);
    cm_hashfinish(&ih->ih_type);

    group = NULL;
    while ((group = cm_hnext(&ih->ih_group, graphd_islink_group, group)) !=
           NULL)
      graphd_islink_group_finish(g, group);
    cm_hashfinish(&ih->ih_group);

    ii = NULL;
    while ((ii = cm_hnext(&ih->ih_intersect, graphd_islink_intersect, ii)) !=
           NULL)
      graphd_islink_intersect_finish(g, ii);
    cm_hashfinish(&ih->ih_intersect);

    cm_free(g->g_cm, ih);

    g->g_islink = NULL;

    cl_leave(g->g_cl, CL_LEVEL_VERBOSE, "leave");
  }
}

/*  Free the resources of the islink module, then recreate them.
 */
int graphd_islink_truncate(graphd_handle *g) {
  if (g->g_islink == NULL) return 0;

  graphd_islink_finish(g);
  return graphd_islink_initialize(g);
}

/*  Same as graphd_islink_truncate, but in a panic.
 */
void graphd_islink_panic(graphd_handle *g) {
  cl_log(g->g_cl, CL_LEVEL_ERROR,
         "graphd_islink_panic: freeing islink resources "
         "and starting over.");

  (void)graphd_islink_truncate(g);
}

/*  A little stub that frees the graphd-read-base at
 *  the end of a request.
 */
static void gic_resource_free(void *manager_data, void *resource_data) {
  graphd_islink_context *gic = resource_data;
  cm_free(gic->gic_greq->greq_req.req_cm, gic);
}

static void gic_resource_list(void *call_data, void *manager_data,
                              void *resource_data) {
  graphd_islink_context *gic = resource_data;
  cl_handle *cl = call_data;
  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_islink_context %p", (void *)gic);
}

static const cm_resource_type gic_resource_type = {
    "graphd_islink_context", gic_resource_free, gic_resource_list};

static int gic_run(graphd_stack *stack, graphd_stack_context *stack_context) {
  graphd_islink_context *gic = (graphd_islink_context *)stack_context;
  graphd_request *greq = gic->gic_greq;
  cl_handle *cl = graphd_request_cl(greq);
  char buf[200];

  cl_enter(cl, CL_LEVEL_VERBOSE, "gic=%s",
           gic->gic_key_valid
               ? graphd_islink_key_to_string(&gic->gic_key, buf, sizeof buf)
               : "*");

  if (gic->gic_err == 0) {
    graphd_handle *g = graphd_request_graphd(gic->gic_greq);
    pdb_budget budget = 100000;
    int err;

    /* Run.
     */
    if (!gic->gic_key_valid)
      err = graphd_islink_donate(g, &budget);
    else {
      err = graphd_islink_job_run(g, &gic->gic_key, &budget);
      if (err == GRAPHD_ERR_NO) err = 0;
    }
    if (err == GRAPHD_ERR_MORE) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "more");
      return 0;
    }
    gic->gic_err = err;
  }

  if (gic->gic_err_out != NULL) *gic->gic_err_out = gic->gic_err;

  if (gic->gic_err == 0 &&
      greq->greq_reply.val_type == GRAPHD_VALUE_UNSPECIFIED) {
    /* Return the size of the set that we just created
     * or revisited.
     */
    if (gic->gic_key_valid) {
      graphd_islink_group const *group;
      graphd_handle *g = graphd_request_graphd(greq);

      group = graphd_islink_group_lookup(g, &gic->gic_key);

      if (group == NULL || group->group_idset == NULL)
        graphd_value_null_set(&greq->greq_reply);
      else
        graphd_value_number_set(&greq->greq_reply, group->group_idset->gi_n);
    }
  }

  graphd_stack_pop(stack);

  cl_leave(cl, CL_LEVEL_VERBOSE, "done");
  return 0;
}

static int gic_freeze(graphd_stack *stack,
                      graphd_stack_context *stack_context) {
  /* nothing to do */
  return 0;
}

static int gic_thaw(graphd_stack *stack, graphd_stack_context *stack_context) {
  /* nothing to do */
  return 0;
}

static graphd_stack_type gic_type = {gic_run, gic_freeze, gic_thaw};

static int graphd_islink_push(graphd_request *greq, graphd_constraint *con,
                              int *err_out) {
  graphd_islink_context *gic;
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_id type_id;
  pdb_id endpoint_id;
  graph_guid type_guid;
  int result_linkage;
  int err;

  /*  Convert string types to GUIDs, datelines,
   *  and generations to their current instances.
   */
  if ((err = graphd_read_convert_types(greq, con)) != 0 ||
      (err = graphd_guid_constraint_convert(greq, con, true)) != 0)
    return err;

  if ((gic = cm_zalloc(greq->greq_req.req_cm, sizeof(*gic))) == NULL)
    return errno ? errno : ENOMEM;

  gic->gic_greq = greq;
  gic->gic_err_out = err_out;

  /*  If we don't have a typeguid, we'll just wait
   *  for everything that's going on right now to complete.
   */
  if (GUIDCON_IS_UNSPECIFIED(con->con_typeguid)) {
    gic->gic_key_valid = false;

    /* The result, if successful, will be the number
     * of islink jobs that completed while we were waiting.
     */
    graphd_value_number_set(&greq->greq_reply,
                            cm_hashnelems(&g->g_islink->ih_job));
  } else {
    /*  Extract endpoint and typeguid from the constraints.
     */
    if (!graphd_guid_constraint_single_linkage(con, PDB_LINKAGE_TYPEGUID,
                                               &type_guid)) {
      graphd_request_error(greq,
                           "SEMANTICS ISLINK requires "
                           "at most a single, non-null TYPEGUID= argument");
      return 0;
    }
    err = pdb_id_from_guid(g->g_pdb, &type_id, &type_guid);
    if (err != 0) {
      char guid_buf[GRAPH_GUID_SIZE];
      graphd_request_errprintf(
          greq, 0,
          "SYSTEM can't convert "
          "TYPEGUID \"%s\" to a local id: %s",
          graph_guid_to_string(&type_guid, guid_buf, sizeof guid_buf),
          graphd_strerror(err));
      return 0;
    }

    /*  We have a type-id.
     *  If we don't have an endpoint, work on the type
     *  as a whole.
     */
    if (GUIDCON_IS_UNSPECIFIED(con->con_left) &&
        GUIDCON_IS_UNSPECIFIED(con->con_right)) {
      err = graphd_islink_add_type_id(g, type_id);
      if (err != 0) return err;

      graphd_islink_key_make(g, PDB_LINKAGE_N, type_id, PDB_ID_NONE,
                             &gic->gic_key);
      gic->gic_key_valid = true;
    } else {
      graph_guid endpoint_guid;
      int endpoint_linkage;

      if (!graphd_guid_constraint_single_linkage(
              con, endpoint_linkage = PDB_LINKAGE_RIGHT, &endpoint_guid) &&
          !graphd_guid_constraint_single_linkage(
              con, endpoint_linkage = PDB_LINKAGE_LEFT, &endpoint_guid)) {
        graphd_request_error(greq,
                             "SEMANTICS ISLINK requires "
                             "at most a single, non-null RIGHT= "
                             "or LEFT= argument");
        return 0;
      }

      /*  Convert the endpoint to an ID.
       */
      err = pdb_id_from_guid(g->g_pdb, &endpoint_id, &endpoint_guid);
      if (err != 0) {
        char guid_buf[GRAPH_GUID_SIZE];
        graphd_request_errprintf(
            greq, 0,
            "SYSTEM can't convert "
            "%s GUID \"%s\" to a local id: %s",
            pdb_linkage_to_string(endpoint_linkage),
            graph_guid_to_string(&endpoint_guid, guid_buf, sizeof guid_buf),
            graphd_strerror(err));
        return 0;
      }

      /*  Convert all that into an islink key.
       */
      result_linkage =
          (endpoint_linkage == PDB_LINKAGE_LEFT ? PDB_LINKAGE_RIGHT
                                                : PDB_LINKAGE_LEFT);
      graphd_islink_key_make(g, result_linkage, type_id, endpoint_id,
                             &gic->gic_key);
      gic->gic_key_valid = true;

      /*  If that group doesn't exist yet,
       *  start a job with that key.
       */
      if (graphd_islink_group_lookup(g, &gic->gic_key) == NULL) {
        err = graphd_islink_group_job_make(g, result_linkage, type_id,
                                           endpoint_id);
        if (err != 0) {
          char buf[200];
          graphd_request_errprintf(
              greq, 0,
              "SYSTEM can't start a job for "
              "%s GUID \"%s\" to a local id: %s",
              pdb_linkage_to_string(result_linkage),
              graph_guid_to_string(&endpoint_guid, buf, sizeof buf),
              graphd_strerror(err));
          return 0;
        }
      }
    }
  }
  graphd_stack_push(&greq->greq_stack, &gic->gic_sc, &gic_resource_type,
                    &gic_type);
  return 0;
}

/**
 * @brief Force a cache entry.
 *
 *  This is for testing purposes only.  The ISLINK request
 *  (same general syntax as "READ") creates a cache entry
 *  for the described value set.
 *
 * @param greq		Request whose stack we're pushing on
 * @param deadline	If running beyond this, return.
 *
 * @return GRAPHD_ERR_MORE	to continue later
 * @return 0		if the request is done.
 * @return other nonzero error codes for system errors.
 *
 */
int graphd_islink(graphd_request *greq, unsigned long long deadline) {
  cl_handle *cl = graphd_request_cl(greq);
  int err = 0;

  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  /*  Nothing on the stack?
   */
  if (graphd_stack_top(&greq->greq_stack) == NULL)
    graphd_islink_push(greq, greq->greq_constraint, &greq->greq_reply_err);

  if (greq->greq_reply_err == 0)
    err = graphd_stack_run_until_deadline(greq, &greq->greq_stack, deadline);

  if (err == 0 /* we ran to completion */
      && greq->greq_reply_err != 0 && greq->greq_error_message == NULL) {
    /*  There was an error, but the code that bailed
     *  out didn't leave us an error message.
     */
    if (greq->greq_reply_err == GRAPHD_ERR_NO)
      graphd_request_error(greq, "EMPTY not found");

    else if (greq->greq_reply_err == GRAPHD_ERR_TOO_MANY_MATCHES)
      graphd_request_error(greq, "TOOMANY too many matches");

    else if (greq->greq_reply_err == GRAPHD_ERR_LEXICAL ||
             greq->greq_reply_err == PDB_ERR_SYNTAX ||
             greq->greq_reply_err == GRAPHD_ERR_SYNTAX)
      graphd_request_error(greq,
                           "SYNTAX bad arguments "
                           "to server request");
    else
      graphd_request_errprintf(greq, 0, "SEMANTICS %s",
                               graphd_strerror(greq->greq_reply_err));
  }
  cl_leave(cl, CL_LEVEL_SPEW, "%s",
           err == 0 ? "done" : (err == GRAPHD_ERR_MORE ? "(to be continued...)"
                                                       : graphd_strerror(err)));
  return err;
}

/*  The caller hereby donates to the "islink" fund for the
 *  general study of types.
 *
 *  (If you want to donate to a specific job instead, have its key
 *  ready and use graphd_islink_job_run instead.)
 *
 * @param g		  the graphd context
 * @param budget_inout	  donation; will be decremented
 *			  as time is used.
 *
 * @return 0 on success, a nonzero error code on unexpected error.
 */
int graphd_islink_donate(graphd_handle *g, pdb_budget *budget_inout) {
  graphd_islink_handle *ih = g->g_islink;
  graphd_islink_job *job;

  if (ih == NULL) return 0;

  while ((job = cm_hnext(&ih->ih_job, graphd_islink_job, NULL)) != NULL) {
    int err;

    if (*budget_inout < 0) return GRAPHD_ERR_MORE;

    /*  Make sure that we'll run out of budget,
     *  even if the jobs don't charge us.
     */
    --(*budget_inout);

    /*  Normally, a terminating job frees itself.
     *  If there was some other error, we must
     *  manually remove it from its queue.
     */
    err = (*job->job_run)(job, g, budget_inout);
    if (err != 0) {
      graphd_islink_job_free(g, job);
      return err;
    }
  }
  return 0;
}

/*  The system is idle.  Donate some time.
 */
int graphd_islink_idle(graphd_handle *g) {
  pdb_budget budget = GRAPHD_ISLINK_IDLE_BUDGET;
  return graphd_islink_donate(g, &budget);
}

/*  The system is seeing this type id flow past.
 *  Make sure we know about it.
 */
int graphd_islink_add_type_id(graphd_handle *g, pdb_id type_id) {
  int err;
  unsigned long long n;

  /*  If this type has too few instances, don't bother.
   */
  err = pdb_linkage_count(g->g_pdb, PDB_LINKAGE_TYPEGUID, type_id,
                          PDB_ITERATOR_LOW_ANY, PDB_ITERATOR_HIGH_ANY,
                          2 * GRAPHD_ISLINK_INTERESTING_MIN, &n);
  if (err != 0) return err;

  /*  Too few elements.  This isn't worth caching.
   */
  if (n < GRAPHD_ISLINK_INTERESTING_MIN) return 0;

  /* Subscribe to updates.
   */
  err = graphd_islink_subscribe(g);
  if (err != 0) return 0;

  return graphd_islink_type_add_id(g, type_id);
}

/*  The system is seeing this type guid flow past.
 *  Make sure we know about it.
 */
int graphd_islink_add_type_guid(graphd_handle *g, graph_guid const *type_guid) {
  int err;
  pdb_id type_id;

  /*  If this type_guid has too few instances, don't bother.
   */
  err = pdb_id_from_guid(g->g_pdb, &type_id, type_guid);
  if (err != 0) return err;

  return graphd_islink_add_type_id(g, type_id);
}

/*  The caller hereby donates to the "islink" fund for the
 *  study of a specific type.
 */
int graphd_islink_type_id_donate(graphd_handle *g, pdb_id type_id,
                                 pdb_budget *budget_inout) {
  graphd_islink_job *job;

  while (((job = graphd_islink_type_job_lookup(g, type_id)) != NULL) &&
         *budget_inout > 0) {
    int err;

    /*  Make sure that we'll run out of budget,
     *  even if the jobs don't charge us.
     */
    --*budget_inout;

    /*  Normally, a terminating job frees itself.
     *  If there was some other error, we must
     *  manually remove it from its queue.
     */
    err = (*job->job_run)(job, g, budget_inout);
    if (err != 0) {
      job = graphd_islink_type_job_lookup(g, type_id);
      if (job != NULL) graphd_islink_job_free(g, job);

      return err;
    }
  }
  return job == NULL ? 0 : GRAPHD_ERR_MORE;
}

/*  The caller hereby donates to the "islink" fund for the
 *  study of a specific type.
 */
int graphd_islink_type_guid_donate(graphd_handle *g,
                                   graph_guid const *type_guid,
                                   pdb_budget *budget_inout) {
  pdb_id id;
  int err;

  err = pdb_id_from_guid(g->g_pdb, &id, type_guid);
  if (err != 0) return err;

  return graphd_islink_type_id_donate(g, id, budget_inout);
}

static int graphd_islink_examine_guidset(graphd_request *greq,
                                         graphd_guid_set const *gs) {
  size_t i;
  int err;

  for (i = 0; i < gs->gs_n; i++) {
    err = graphd_islink_add_type_guid(graphd_request_graphd(greq),
                                      gs->gs_guid + i);

    if (err == PDB_ERR_NO) {
      /*  The GUID was bogus or premature.
       *  Never mind, we'll still convert
       *  the others.
       */
      continue;
    }

    if (err != 0) {
      char buf[200];
      cl_log_errno(graphd_request_cl(greq), CL_LEVEL_FAIL,
                   "graphd_islink_add_type_guid", err, "guid=%s",
                   graph_guid_to_string(gs->gs_guid + i, buf, sizeof buf));
      return err;
    }
  }
  return 0;
}

/*  Visit all constraints in the subtree, and hand all typeguids
 *  used to the islink typeguid cache for further study.
 */
static int graphd_islink_examine_guidcon(
    graphd_request *const greq, graphd_guid_constraint const *const gc) {
  int err;

  if (gc->guidcon_include_valid) {
    err = graphd_islink_examine_guidset(greq, &gc->guidcon_include);
    if (err != 0) return err;
  }
  if (gc->guidcon_exclude_valid) {
    err = graphd_islink_examine_guidset(greq, &gc->guidcon_exclude);
    if (err != 0) return err;
  }
  if (gc->guidcon_match_valid) {
    err = graphd_islink_examine_guidset(greq, &gc->guidcon_match);
    if (err != 0) return err;
  }
  return 0;
}

/*  Visit all constraints in the subtree, and hand all typeguids
 *  used to the islink typeguid cache for further study.
 */
int graphd_islink_examine_constraint(graphd_request *greq,
                                     graphd_constraint const *con) {
  int err;
  graphd_constraint const *sub;
  graphd_constraint_or const *cor;

  /*  We're including the constraint's guid if
   *
   *  - any of its subconstraints point to it with "typeguid"
   *  - its parent points to it with "typeguid".
   */
  if (graphd_linkage_is_i_am(con->con_linkage) &&
      graphd_linkage_i_am(con->con_linkage) == PDB_LINKAGE_TYPEGUID) {
  i_am_a_typeguid:
    err = graphd_islink_examine_guidcon(greq, &con->con_guid);
    if (err != 0) return err;
  } else
    for (sub = con->con_head; sub != NULL && con->con_tail != &sub->con_next;
         sub = sub->con_next)

      if (graphd_linkage_is_my(con->con_linkage) &&
          graphd_linkage_my(con->con_linkage) == PDB_LINKAGE_TYPEGUID)

        goto i_am_a_typeguid;

  /*  If we have typeguids inside the constraint, visit those.
   */
  err = graphd_islink_examine_guidcon(greq, &con->con_typeguid);
  if (err != 0) return err;

  /*  Recurse into "or" constraints and subconstraints.
   */
  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    err = graphd_islink_examine_constraint(greq, &cor->or_head);
    if (err != 0) return err;
    if (cor->or_tail != NULL) {
      err = graphd_islink_examine_constraint(greq, cor->or_tail);
      if (err != 0) return err;
    }
  }

  for (sub = con->con_head; sub != *con->con_tail; sub = sub->con_next) {
    /*  If this subconstraint is part of an or-branch, skip it;
     *  we already visited it while visiting the or branch.
     */
    if (sub->con_parent != con) continue;

    if ((err = graphd_islink_examine_constraint(greq, sub)) != 0) return err;
  }
  return 0;
}
