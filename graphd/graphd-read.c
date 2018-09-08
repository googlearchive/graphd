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
#include "graphd/graphd-read.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

/**
 * @file graphd-read.c
 *
 * @brief Match a constraint subtree against the database.
 */

/**
 * @brief Convert type names into type GUIDs in a constraint.
 *
 * @param g	graphd we're doing this for
 * @param asof	behave as if this is now
 * @param con	constraint whose types we're converting.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int graphd_read_convert_types_to_guids(
    graphd_request* greq, graphd_constraint* con,
    graphd_string_constraint const* strcon, graphd_guid_set* gs) {
  bool has_null = false;
  bool has_non_null = false;
  graph_guid typeguid;
  int err = 0;
  cl_handle* cl = graphd_request_cl(greq);

  graphd_guid_set_initialize(gs);

  /* No constraint */
  if (strcon == NULL) return 0;

  if (strcon->strcon_head == NULL)
    has_null = true;
  else {
    graphd_string_constraint_element* strcel;

    for (strcel = strcon->strcon_head; strcel != NULL;
         strcel = strcel->strcel_next) {
      if (strcel->strcel_s == NULL) {
        /* Null is a member of the result set.
         */
        has_null = true;
        continue;
      }

      /*  We were at least trying to
       *  include a non-null result.
       */
      has_non_null = true;

      /* Look up the GUID for the type
         in the strqueue.
       */
      err = graphd_type_guid_from_name(
          graphd_request_graphd(greq), greq->greq_asof, strcel->strcel_s,
          strcel->strcel_e - strcel->strcel_s, &typeguid);

      /* It's not an error for the type
       * not to exist, although it may
       * result in the constraint not
       * matching...
       */
      if (err == GRAPHD_ERR_NO) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_read_convert_types_to_guids: "
               "could not resolve type \"%.*s\"",
               (int)(strcel->strcel_e - strcel->strcel_s), strcel->strcel_s);
        err = 0;
        continue;
      }
      if (err != 0) return err;

      /*  Append the typeguid we just resolved
       *  to the guid queue.
       */
      err = graphd_guid_set_add(greq, gs, &typeguid);
      if (err != 0) return err;
    }
  }

  /*  Keep apart the following cases:
   *
   *  - none of the types the user specified exist, so they
   *    ended up specifying an empty set that can never match.
   *
   *  - the user explicitly specified "null", matching primitives
   *    that have a typeguid of NULL.
   */

  if (has_non_null && !has_null && gs->gs_n == 0) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "FALSE [%s:%d] graphd_read_convert_types_to_guids: no "
           "valid types in %s",
           __FILE__, __LINE__, graphd_constraint_to_string(con));
    con->con_false = true;
  }

  if (has_null && has_non_null) {
    /*  Explicitly add a "null" GUID
     *  to the list of permissible typeguids.
     */
    err = graphd_guid_set_add(greq, gs, NULL);
    if (err != 0) return err;
  }
  return 0;
}

/**
 * @brief Convert type names into type GUIDs in a constraint.
 *
 * @param g	graphd we're doing this for
 * @param asof	behave as if this is now
 * @param con	constraint whose types we're converting.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_read_convert_types(graphd_request* greq, graphd_constraint* con) {
  graphd_string_constraint const* strcon;
  graphd_constraint* sub;
  int err = 0;
  cl_handle* cl = graphd_request_cl(greq);

  cl_enter(cl, CL_LEVEL_VERBOSE, "con=%s", graphd_constraint_to_string(con));

  /*  It has a string type constraint, but no typeguid constraint?
   *  Assign a typeguid constraint.
   */
  if ((strcon = con->con_type.strqueue_head) != NULL) {
    /*  Convert each type=() set into a separate GUID set,
     *  then merge that set into the typeguid accumulator.
     */
    for (; strcon != NULL; strcon = strcon->strcon_next) {
      graphd_guid_set tmp;

      err = graphd_read_convert_types_to_guids(greq, con, strcon, &tmp);
      if (err != 0) {
        cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
        return err;
      }
      err = graphd_guid_constraint_merge(greq, con, &con->con_typeguid,
                                         strcon->strcon_op, &tmp);

      if (err != 0) {
        cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
        return err;
      }
      if (con->con_false) break;
    }

    /*  Drop the string types,
     *  now that we have the real types.
     */
    con->con_type.strqueue_head = NULL;
    con->con_type.strqueue_tail = &con->con_type.strqueue_head;
  }

  /*  Process all the subconstraints.
   */
  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    err = graphd_read_convert_types(greq, sub);
    if (err != 0) break;
  }

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", err ? graphd_strerror(err) : "ok");
  return err;
}

/*
 *  POINTER points to TARGET.
 *  That means that POINTER was created after TARGET.
 *
 *  That means that if there's a dateline_min (>dateline) attached
 *  to TARGET, it applies to POINTER as well, because if
 *  POINTER.date > TARGET.date, and TARGET.date > dateline, then
 *  PIONTER.date > dateline.
 *
 *  Conversely, if there is a dateline max (<dateline) attached to
 *  POINTER, it applies to TARGET as well -- if the
 *  TARGET.date < POINTER.date, and POINTER.date < dateline,
 *  then TARGET.date < dateline.
 *
 *  Independently, everybody also has an implied maximum of greq_asof,
 *  if there is one.
 */
static int graphd_read_push_dateline_across_pointer(graphd_request* greq,
                                                    graphd_constraint* pointer,
                                                    graphd_constraint* target) {
  int err;
  cl_handle* cl = graphd_request_cl(greq);

  if (pointer == NULL || target == NULL) return 0;

  if (target->con_dateline.dateline_min != NULL &&
      (target->con_dateline.dateline_min !=
       pointer->con_dateline.dateline_min)) {
    if (pointer->con_dateline.dateline_min == NULL) {
      cl_cover(cl);
      pointer->con_dateline.dateline_min = graph_dateline_copy(
          greq->greq_req.req_cm, target->con_dateline.dateline_min);
      if (pointer->con_dateline.dateline_min == NULL) return ENOMEM;
    } else {
      cl_cover(cl);
      err = graph_dateline_merge_minimum(&pointer->con_dateline.dateline_min,
                                         target->con_dateline.dateline_min);
      if (err != 0) return err;
    }
  }

  if (pointer->con_dateline.dateline_max != NULL &&
      (target->con_dateline.dateline_max !=
       pointer->con_dateline.dateline_max)) {
    if (target->con_dateline.dateline_max == NULL) {
      cl_cover(cl);
      target->con_dateline.dateline_max = graph_dateline_copy(
          greq->greq_req.req_cm, pointer->con_dateline.dateline_max);
      if (target->con_dateline.dateline_max == NULL) return ENOMEM;
    } else {
      cl_cover(cl);
      err = graph_dateline_merge(&target->con_dateline.dateline_max,
                                 pointer->con_dateline.dateline_max);
      if (err != 0) return err;
    }
  }
  return 0;
}

/**
 * @brief Recursively push "asof" constraints into dateline maxima,
 *  	and comile dateline maxima and minima.
 *
 * @param greq	Request we're doing this for.
 * @param con	Toplevel constraint we're working on (recurse over this)
 * @return 0 on success, nonzero error code for unexpected errors.
 */
static int graphd_read_compile_datelines(graphd_request* greq,
                                         graphd_constraint* con) {
  int err;
  cl_handle* cl = graphd_request_cl(greq);
  graphd_constraint* sub;

  if (con->con_parent != NULL) {
    if (graphd_linkage_is_i_am(con->con_linkage)) {
      err =
          graphd_read_push_dateline_across_pointer(greq, con->con_parent, con);
      if (err != 0) return err;
    } else if (GRAPHD_CONSTRAINT_IS_MANDATORY(con)) {
      cl_assert(cl, graphd_linkage_is_my(con->con_linkage));
      err =
          graphd_read_push_dateline_across_pointer(greq, con, con->con_parent);
      if (err != 0) return err;
    }
  }
  if (greq->greq_asof != NULL) {
    if (con->con_dateline.dateline_max == NULL) {
      con->con_dateline.dateline_max =
          graph_dateline_copy(greq->greq_req.req_cm, greq->greq_asof);
      if (con->con_dateline.dateline_max == NULL) return ENOMEM;
    } else {
      err = graph_dateline_merge_minimum(&con->con_dateline.dateline_max,
                                         greq->greq_asof);
      if (err != 0) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_read_compile_datelines: "
               "unexpected error from "
               "graph_dateline_merge_minimum: %s",
               strerror(err));
        return err;
      }
    }
  }

  /*  Recurse.
   */
  for (sub = con->con_head; sub != NULL; sub = sub->con_next)
    if ((err = graphd_read_compile_datelines(greq, sub)) != 0) return err;
  return 0;
}

/**
 * @brief Translate timestamp constraints into local dateline constraints.
 *
 *  This is roughly the same as the corresponding "asof" constraint,
 *  but the endpoint of the translation yields a dateline, not an asof.
 *
 * @param greq	Request we're doing this for.
 * @param con	Constraint somewhere below greq.
 *
 * @return 0 on success, nonzero error code for unexpected errors.
 */
static int graphd_read_compile_timestamps(graphd_request* greq,
                                          graphd_constraint* con) {
  int err;
  cl_handle* cl = graphd_request_cl(greq);
  graphd_handle* g = graphd_request_graphd(greq);
  pdb_handle* pdb = g->g_pdb;
  graphd_constraint* sub;
  char buf[200];
  pdb_id id;

  /*  Recurse.
   */
  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    err = graphd_read_compile_timestamps(greq, sub);
    if (err != 0) return err;
  }

  if (!con->con_timestamp_valid || con->con_false) return 0;

  /*  Find the last GUID that was created within this time.
   */
  if (con->con_timestamp_max != GRAPH_TIMESTAMP_MAX) {
    err = graphd_timestamp_to_id(pdb, &con->con_timestamp_max, GRAPHD_OP_LE,
                                 &id, NULL);

    if (err != 0) {
      if (err != GRAPHD_ERR_NO) {
        cl_log_errno(
            cl, CL_LEVEL_FAIL, "graphd_timestamp_to_id", err, "timestamp=%s",
            graph_timestamp_to_string(con->con_timestamp_max, buf, sizeof buf));
        return err;
      }

      cl_log(cl, CL_LEVEL_DEBUG,
             "FALSE [%s:%d] graphd_read_compile_timestamps: "
             "no primitives created before \"%s\"",
             __FILE__, __LINE__, graph_timestamp_to_string(
                                     con->con_timestamp_max, buf, sizeof buf));
      con->con_false = true;
      return 0;
    }

    if (con->con_dateline.dateline_max == NULL) {
      con->con_dateline.dateline_max =
          graph_dateline_create(greq->greq_req.req_cm);
      if (con->con_dateline.dateline_max == NULL) {
        int err = errno ? errno : ENOMEM;
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_read_compile_timestamps"
               ": unexpected error from "
               "graph_dateline_create: %s",
               strerror(errno));
        return err;
      }
    }

    /* XXX is this id or id+1 ? */

    err = graph_dateline_add_minimum(&con->con_dateline.dateline_max,
                                     pdb_database_id(pdb), id + 1,
                                     g->g_instance_id);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graph_dateline_add", err, "id=%llx",
                   (unsigned long long)id);
      return err;
    }
  }

  /*  Minimum timestamp -- all result timestamps will
   *  be >= this.
   */
  if (con->con_timestamp_min != GRAPH_TIMESTAMP_MIN) {
    err = graphd_timestamp_to_id(pdb, &con->con_timestamp_min, GRAPHD_OP_GE,
                                 &id, NULL);

    if (err != 0) {
      if (err != GRAPHD_ERR_NO) {
        cl_log_errno(
            cl, CL_LEVEL_FAIL, "graphd_timestamp_to_id", err, "timestamp=%s",
            graph_timestamp_to_string(con->con_timestamp_max, buf, sizeof buf));
        return err;
      }

      cl_log(cl, CL_LEVEL_DEBUG,
             "FALSE [%s:%d] graphd_read_compile_timestamps: "
             "no primitives created after \"%s\"",
             __FILE__, __LINE__, graph_timestamp_to_string(
                                     con->con_timestamp_min, buf, sizeof buf));
      con->con_false = true;
      return 0;
    }
    if (con->con_dateline.dateline_min == NULL) {
      con->con_dateline.dateline_min =
          graph_dateline_create(greq->greq_req.req_cm);
      if (con->con_dateline.dateline_min == NULL) {
        int err = errno ? errno : ENOMEM;
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_read_compile_timestamps"
               ": unexpected error from "
               "graph_dateline_create: %s",
               strerror(errno));
        return err;
      }
    }

    /* XXX should this be id or id+1 ? */

    err = graph_dateline_add(&con->con_dateline.dateline_min,
                             pdb_database_id(pdb), id, g->g_instance_id);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graph_dateline_add", err, "id=%llx",
                   (unsigned long long)id);
      return err;
    }
  }
  return 0;
}

/**
 * @brief Translate "asof" from a graphd_value into a dateline.
 *
 *  We can't do this during the parse because the parse may happen
 *  before the values that a timestamp would refer to are entered
 *  into the database.
 *
 * @param greq	Request we're doing this for.
 * @return 0 on success, nonzero error code for unexpected errors.
 */
static int graphd_read_compile_asof(graphd_request* greq) {
  cl_handle* cl = graphd_request_cl(greq);
  graphd_handle* g = graphd_request_graphd(greq);
  pdb_handle* pdb = g->g_pdb;
  int err;

  if (greq->greq_asof_value != NULL) {
    pdb_id id;

    cl_assert(cl, greq->greq_asof_value->val_type == GRAPHD_VALUE_TIMESTAMP);

    /*  Find the latest GUID that was created on or before
     *  this time, and use it as a timestamp.
     */
    err = graphd_timestamp_to_id(pdb, &greq->greq_asof_value->val_timestamp,
                                 GRAPHD_OP_LE, &id, NULL);
    if (err == GRAPHD_ERR_NO) {
      err = graph_dateline_add(&greq->greq_asof, pdb_database_id(pdb), 0,
                               g->g_instance_id);
      if (err != 0) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_read_compile_asof: unexpected "
               "error from graph_dateline_add: "
               "%s",
               strerror(err));
        return err;
      }
    } else if (err != 0) {
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_read_compile_asof: unexpected "
             "error from graphd_timestamp_to_id: %s",
             strerror(err));
      return err;
    } else {
      err = graph_dateline_add(&greq->greq_asof, pdb_database_id(pdb), id + 1,
                               g->g_instance_id);
      if (err != 0) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_read_compile_asof: unexpected "
               "error from graph_dateline_add: %s",
               strerror(err));
        return err;
      }
    }
    cm_free(greq->greq_req.req_cm, greq->greq_asof_value);
    greq->greq_asof_value = NULL;
  }
  return 0;
}

/*  Receive a result from a previously pushed tree.
 */
static void graphd_read_push_callback(void* data, int err,
                                      graphd_constraint const* con,
                                      graphd_value* res) {
  graphd_read_base* grb = data;

  grb->grb_err = err;
  if (grb->grb_err_out != NULL) *grb->grb_err_out = err;
  if (err == 0) {
    grb->grb_val = res[con->con_assignment_n];
    graphd_value_initialize(res + con->con_assignment_n);
  }
}

/**
 * @brief Push a context on the stack that will read a constraint tree.
 *
 *  This module reads primitives that match a constraint.
 *
 * @param greq		Request whose stack we're pushing on
 * @param con		Constraint caller wants to match.
 * @param val_out 	return a value tree here.
 * @param err_out 	return errors here.
 */
void graphd_read_push(graphd_request* greq, graphd_constraint* con,
                      graphd_value* val_out, int* err_out) {
  cl_handle* cl = graphd_request_cl(greq);
  graphd_read_base* grb;
  int err;

  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  err = graphd_read_base_push(greq, con, val_out, err_out, &grb);
  if (err != 0) goto err;

  /*  Convert string types to GUIDs, datelines,
   *  and generations to their current instances.
   */
  if ((err = graphd_read_compile_timestamps(greq, con)) != 0 ||
      (err = graphd_read_compile_asof(greq)) != 0 ||
      (err = graphd_read_compile_datelines(greq, con)) != 0 ||
      (err = graphd_read_convert_types(greq, con)) != 0)
    goto err;

  err = graphd_guid_constraint_convert(greq, con, true /* read */);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_guid_constraint_convert", err,
                 "unexpected");
    goto err;
  }

  err = graphd_islink_examine_constraint(greq, con);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_islink_examine_constraint", err,
                 "unexpected");
    goto err;
  }

  /*  Annotate the constraint subtree with iterators.
   */
  err = graphd_constraint_iterator(greq, con);
  if (err != 0) goto err;

  PDB_IS_ITERATOR(cl, con->con_it);

  /*  Specifically evaluate the topmost context.
   */
  if (greq->greq_request == GRAPHD_REQUEST_ITERATE)
    graphd_iterate_constraint_push(greq, con, grb, &grb->grb_val,
                                   &grb->grb_err);
  else
    graphd_read_set_push(grb, con, PDB_ID_NONE, NULL, graphd_read_push_callback,
                         grb);
  if ((err = grb->grb_err) != 0) goto err;

  cl_leave(cl, CL_LEVEL_SPEW, "see you in read_run");
  return;

err:
  *err_out = err;

  graphd_stack_pop(&greq->greq_stack);
  cl_leave(cl, CL_LEVEL_SPEW, "%s", err ? graphd_strerror(err) : "ok");
}

/**
 * @brief Push a context on the stack that will read a constraint tree.
 *
 * @param greq		Request whose stack we're pushing on
 * @param deadline	If running beyond this, return.
 *
 * @return PDB_ERR_MORE	to continue later
 * @return 0		if the request is done.
 * @return other nonzero error codes for system errors.
 *
 */
int graphd_read(graphd_request* greq, unsigned long long deadline) {
  int err = 0;
  cl_handle* cl = graphd_request_cl(greq);

  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  graphd_request_diary_log(greq, 0, "RUN");

  /*  Nothing on the stack?
   */
  if (graphd_stack_top(&greq->greq_stack) == NULL)
    graphd_read_push(greq, greq->greq_constraint, &greq->greq_reply,
                     &greq->greq_reply_err);

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
             greq->greq_reply_err == GRAPHD_ERR_SYNTAX)
      graphd_request_error(greq,
                           "SYNTAX bad arguments "
                           "to server request");

    else if (greq->greq_reply_err == GRAPHD_ERR_SEMANTICS)
      graphd_request_error(greq,
                           "SEMANTICS bad arguments "
                           "to server request");
    else
      graphd_request_errprintf(greq, 0, "SYSTEM %s",
                               graphd_strerror(greq->greq_reply_err));
  }
  cl_leave(cl, CL_LEVEL_SPEW, "%s",
           err == 0 ? "done" : (err == PDB_ERR_MORE ? "(to be continued...)"
                                                    : graphd_strerror(err)));

  return err;
}

/**
 * @brief Freeze a read.
 *
 * @param greq	Request whose read we're freezing.
 * @return 0 on success
 * @return other nonzero error codes for system errors.
 *
 */
int graphd_read_suspend(graphd_request* greq) {
  /*  If we don't have an "as-of" deadline, add one.
   *  That way, the request's results are going to stay
   *  as if it had run all the way through.
   */
  if (greq->greq_asof == NULL)
    greq->greq_asof = graphd_dateline(graphd_request_graphd(greq));

  return graphd_stack_suspend(&greq->greq_stack);
}

/**
 * @brief Thaw a read.
 *
 * @param greq	Request we're thawing.
 * @return 0 on success
 * @return other nonzero error codes for system errors.
 *
 */
int graphd_read_unsuspend(graphd_request* greq) {
  /* Thaw values that are part of the data on the stack.
   */
  return graphd_stack_unsuspend(&greq->greq_stack);
}
