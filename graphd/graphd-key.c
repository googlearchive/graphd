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

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

#define KEY_MISSING_ERROR(req, keyname)                                   \
  do {                                                                    \
    graphd_request_error((greq), "SEMANTICS " keyname                     \
                                 " is used "                              \
                                 "as a key without specifying a " keyname \
                                 " in the constraint");                   \
    return GRAPHD_ERR_SEMANTICS;                                          \
  } while (0)
#define GUIDCON_HAS_ANNOTATED_GUID(x)                                \
  ((x).guidcon_include_annotated && (x).guidcon_include.gs_n == 1 && \
   !GRAPH_GUID_IS_NULL((x).guidcon_include.gs_guid[0]))
#define GUIDCON_HAS_GUID(x)                                       \
  ((x).guidcon_include_valid && !(x).guidcon_include_annotated && \
   (x).guidcon_include.gs_n == 1 &&                               \
   !GRAPH_GUID_IS_NULL((x).guidcon_include.gs_guid[0]))
#define GUIDCON_GUID(x) ((x).guidcon_include.gs_guid[0])

/**
 * @brief Write a primitive with a given set of linkages.
 *
 * @param greq		request
 * @param con		constraint to write.
 * @param prev_guid	predecessor GUID or NULL
 * @param linkcon	four constraint GUIDs (may be null constraints).
 * @param guid_out	Assign the created GUID to here.
 */
static int graphd_key_write_constraint_with_linkage(
    graphd_request *greq, graphd_constraint const *con,
    graph_guid const *prev_guid, graph_guid *linkcon, graph_guid *guid_out) {
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  char const *name_s = NULL, *value_s = NULL;
  size_t name_n = 0, value_n = 0;
  size_t i;
  pdb_primitive pr;
  int err;
  char errbuf[200];

  cl_assert(cl, guid_out != NULL);
  cl_assert(cl, con != NULL);
  cl_assert(cl, greq != NULL);

  /*  Add direct guidcons to linkage, if we're that kind of constraint.
   */
  for (i = 0; i < PDB_LINKAGE_N; i++)
    if (GUIDCON_HAS_GUID(con->con_linkcon[i])) {
      cl_assert(cl, GRAPH_GUID_IS_NULL(linkcon[i]));
      linkcon[i] = GUIDCON_GUID(con->con_linkcon[i]);
    }

  if (prev_guid == NULL && !con->con_guid.guidcon_include_annotated &&
      con->con_guid.guidcon_include_valid &&
      con->con_guid.guidcon_include.gs_n >= 1) {
    prev_guid = con->con_guid.guidcon_include.gs_guid;
  }
  if (prev_guid == NULL && con->con_guid.guidcon_match_valid &&
      con->con_guid.guidcon_match.gs_n >= 1) {
    prev_guid = con->con_guid.guidcon_match.gs_guid;
  }

  /*  How much space are we going to need?
   *
   *  There are dynamic-size elements: the type, name, value,
   *  and the (up to) four links.
   */
  if (con->con_name.strqueue_head != NULL &&
      con->con_name.strqueue_head->strcon_head != NULL) {
    char const *name_e;
    name_s = con->con_name.strqueue_head->strcon_head->strcel_s;
    name_e = con->con_name.strqueue_head->strcon_head->strcel_e;
    if (name_s != NULL) {
      cl_assert(cl, name_s <= name_e);
      name_n = (name_e - name_s) + 1;
    }
  }
  if (con->con_value.strqueue_head != NULL &&
      con->con_value.strqueue_head->strcon_head != NULL) {
    char const *value_e;
    value_s = con->con_value.strqueue_head->strcon_head->strcel_s;
    value_e = con->con_value.strqueue_head->strcon_head->strcel_e;
    if (value_s != NULL) {
      cl_assert(cl, value_s <= value_e);
      value_n = (value_e - value_s) + 1;
    }
  }

  /* Allocate the primitive */
  graphd_dateline_expire(g);
  err = pdb_primitive_alloc(
      g->g_pdb, g->g_now, prev_guid, &pr, guid_out,
      con->con_timestamp_valid ? con->con_timestamp_min : g->g_now,
      con->con_valuetype == GRAPH_DATA_UNSPECIFIED
          ? (con->con_value.strqueue_head == NULL ? GRAPH_DATA_NULL
                                                  : GRAPH_DATA_STRING)
          : con->con_valuetype,
      (con->con_live != GRAPHD_FLAG_FALSE ? PDB_PRIMITIVE_BIT_LIVE : 0) |
          (con->con_archival != GRAPHD_FLAG_FALSE ? PDB_PRIMITIVE_BIT_ARCHIVAL
                                                  : 0) |
          (!greq->greq_data.gd_write.gdw_txstart_written
               ? PDB_PRIMITIVE_BIT_TXSTART
               : 0),
      name_n, value_n, name_s, value_s, linkcon + PDB_LINKAGE_TYPEGUID,
      linkcon + PDB_LINKAGE_RIGHT, linkcon + PDB_LINKAGE_LEFT,
      linkcon + PDB_LINKAGE_SCOPE, NULL, errbuf, sizeof errbuf);
  if (err != 0)
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_primitive_alloc", err, "errbuf=%s",
                 errbuf);

  /*  Finish writing the primitive.
   */
  if (err == 0) {
    err = pdb_primitive_alloc_commit(g->g_pdb, prev_guid, guid_out, &pr, errbuf,
                                     sizeof errbuf);
    if (err != 0)
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_primitive_alloc_commit", err,
                   "errbuf=%s", errbuf);
  }

  /*  An error anywhere along the line?
   */
  if (err != 0) {
    if (greq->greq_error_message == NULL) {
      if (err == GRAPHD_ERR_PRIMITIVE_TOO_LARGE)
        graphd_request_errprintf(greq, 0, "TOOBIG %s",
                                 *errbuf ? errbuf : "primitive too big");
      else
        graphd_request_errprintf(
            greq, 0, "SEMANTICS %s%s%s",
            err == PDB_ERR_NO ? "not found" : graphd_strerror(err),
            *errbuf ? ": " : "", errbuf);
    }
    return err;
  }

  greq->greq_data.gd_write.gdw_txstart_written = 1;

  /*  Since we're done writing this primitive, increment our
   *  timestamp's subjective sub-second counter.
   */
  graph_timestamp_next(&g->g_now);
  return 0;
}

/**
 * @brief Make an existing primitive fit a constraint, or create a new one.
 *
 *  If pr is non-NULL, there was a partial match between a constraint con
 *  and a primitive pr.  Make pr fit con completely, versioning it if needed,
 *  and versioning or creating its subconstraints if needed.
 *
 * @param greq  	caller's request
 * @param con		constraint caller wants us to fit to.
 * @param pr_parent 	NULL, or a pointer to a parent primitive
 * @param pr		NULL, or a primitive that has dibs on this spot.
 * @param reply 	possibly preallocated reply array, if
 *			of type GRAPHD_VALUE_LIST, or a space
 *			initialized with GRAPHD_VALUE_UNSPECIFIED.
 *			In case of error, the caller frees the
 *			value data.
 *
 * @return 0 on success.
 * @return other nonzero error codes on system error.
 */
int graphd_key_align(graphd_request *greq, graphd_constraint *con,
                     graph_guid const *guid_parent, pdb_primitive *pr,
                     graphd_value *reply) {
  graphd_session *gses = graphd_request_session(greq);
  graphd_handle *g = gses->gses_graphd;
  cl_handle *cl = gses->gses_cl;
  cm_handle *cm = greq->greq_req.req_cm;
  graphd_constraint *sub;
  size_t i;
  pdb_primitive pr_sub, pr_key, pr_new;
  graph_guid linkcon[PDB_LINKAGE_N];
  char buf[200], buf2[200];
  bool needs_writing = (pr == NULL);
  int err;

  cl_enter(cl, CL_LEVEL_SPEW, "(con=%s pr=%s parent=%s)",
           graphd_constraint_to_string(con),
           pdb_primitive_to_string(pr, buf, sizeof buf),
           graph_guid_to_string(guid_parent, buf2, sizeof buf2));
  cl_assert(cl, reply != NULL);

  /*  Initialize the temporaries, so it's safe to free
   *  them even if they weren't assigned.
   */
  pdb_primitive_initialize(&pr_key);
  pdb_primitive_initialize(&pr_new);

  /*  If we have a key that matched a primitive during the key
   *  cluster or anchor resolution phase, _and_ we have a subprimitive
   *  suggestion, use the suggestion only if its GUID matches
   *  the key annotation.  Otherwise, move to the key.
   */
  if (con->con_key && GUIDCON_HAS_ANNOTATED_GUID(con->con_guid)) {
    bool need_key = true;

    if (pr != NULL) {
      graph_guid guid;
      pdb_primitive_guid_get(pr, guid);
      if (GRAPH_GUID_EQ(guid, GUIDCON_GUID(con->con_guid))) need_key = false;
    }

    if (need_key) {
      err = pdb_primitive_read(g->g_pdb, &GUIDCON_GUID(con->con_guid), &pr_key);
      if (err != 0) {
        char guidbuf[GRAPH_GUID_SIZE];
        cl_leave(cl, CL_LEVEL_ERROR, "error reading key primitive %s: %s",
                 graph_guid_to_string(&GUIDCON_GUID(con->con_guid), guidbuf,
                                      sizeof guidbuf),
                 graphd_strerror(err));
        return err;
      }
      pr = &pr_key;
    }
  }

  /*  If we don't have an incoming primitive, but we
   *  do have a suggestion from the key or anchor resolution phase,
   *  use the suggestion.
   */
  if (pr == NULL && GUIDCON_HAS_ANNOTATED_GUID(con->con_guid)) {
    err = pdb_primitive_read(g->g_pdb, &GUIDCON_GUID(con->con_guid), &pr_key);
    if (err != 0) {
      char guidbuf[GRAPH_GUID_SIZE];
      cl_leave(cl, CL_LEVEL_ERROR, "error reading key primitive %s: %s",
               graph_guid_to_string(&GUIDCON_GUID(con->con_guid), guidbuf,
                                    sizeof guidbuf),
               graphd_strerror(err));
      return err;
    }
    pr = &pr_key;
  }

  needs_writing = (pr == NULL);
  if (reply->val_type != GRAPHD_VALUE_LIST) {
    cl_assert(cl, reply->val_type == GRAPHD_VALUE_UNSPECIFIED);

    /*  Allocate space for the result: one GUID for this,
     *  one each for each subconstraint.
     */
    err = graphd_value_list_alloc(g, cm, cl, reply, 1 + con->con_subcon_n);
    if (err != 0) {
      pdb_primitive_finish(g->g_pdb, &pr_key);
      cl_leave(cl, CL_LEVEL_SPEW, "graphd_value_list_alloc fails: %s",
               strerror(err));
      return err;
    }
  }
  reply = reply->val_list_contents;
  reply->val_type = GRAPHD_VALUE_GUID;

  /*  We'll store linkage GUIDs in the linkcon array as we
   *  create them.  Initialize it to empty.
   */
  for (i = 0; i < PDB_LINKAGE_N; i++) GRAPH_GUID_MAKE_NULL(linkcon[i]);

  /*  If we point to our parent,
   *  the parent must have already been written.
   */
  if (graphd_linkage_is_my(con->con_linkage)) {
    int l = graphd_linkage_my(con->con_linkage);
    cl_assert(cl, guid_parent != NULL);
    linkcon[l] = *guid_parent;
  }

  /*  1. Fill in the typeguid from a literal type, if needed.
   *
   *  (I'd normally delay this to the creation of the top node in
   *  graphd_key_write_constraint_with_linkage(), but we started
   *  out with preorder types, and the GUIDs will change if I change
   *  it to inorder.)
   */
  if (con->con_type.strqueue_head != NULL &&
      con->con_type.strqueue_head->strcon_head != NULL &&
      con->con_type.strqueue_head->strcon_head->strcel_s != NULL &&
      !(graphd_constraint_linkage_pattern(con) &
        (1 << GRAPHD_PATTERN_LINKAGE(PDB_LINKAGE_TYPEGUID)))) {
    char const *type_s;
    char const *type_e;
    size_t type_n;

    type_s = con->con_type.strqueue_head->strcon_head->strcel_s;
    type_e = con->con_type.strqueue_head->strcon_head->strcel_e;
    cl_assert(cl, type_s <= type_e);
    type_n = (type_e - type_s);

    err = graphd_type_make_name(greq, type_s, type_n,
                                linkcon + PDB_LINKAGE_TYPEGUID);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_type_make_name", err,
                   "can't create type \"%.*s%s\"",
                   (int)(type_n > 80 ? 80 : type_n), (char const *)type_s,
                   type_n > 80 ? "..." : "");

      pdb_primitive_finish(g->g_pdb, &pr_key);
      cl_leave(cl, CL_LEVEL_SPEW,
               "can't "
               "create type \"%.*s%s\": %s",
               (int)(type_n > 80 ? 80 : type_n), (char const *)type_s,
               type_n > 80 ? "..." : "", graphd_strerror(err));
      return err;
    }
  }

  /*  2. Bind/version/insert the subconstraints that our
   *     constraint points to.
   *     (We can only point to them once they exist!)
   */
  for (sub = con->con_head, i = 1; sub != NULL; sub = sub->con_next, i++) {
    int linkage;
    bool subconstraint_needs_writing = false;
    graphd_value *sv = reply + i, *gv;

    /*  This time around, we're considering only primitives
     *  that say: "I am my container's left/right/type/scope!"
     */
    if (!graphd_linkage_is_i_am(sub->con_linkage)) continue;

    linkage = graphd_linkage_i_am(sub->con_linkage);
    if (pr == NULL || !pdb_primitive_has_linkage(pr, linkage))

      /* The subconstraint wants to be pointed to,
       * but the primitive matching the constraint
       * container's key doesn't point there.
       */
      subconstraint_needs_writing = true;
    else {
      graph_guid guid_sub;

      cl_assert(cl, pr != NULL);

      pdb_primitive_linkage_get(pr, linkage, guid_sub);
      err = pdb_primitive_read(g->g_pdb, &guid_sub, &pr_sub);
      if (err != 0) {
        /* Unexpected error. */
        pdb_primitive_finish(g->g_pdb, &pr_key);
        cl_leave(cl, CL_LEVEL_ERROR,
                 "unexpected error "
                 "from pdb_primitive_read: %s",
                 graphd_strerror(err));
        return err;
      }

      /*  If this subentry completely matches the one we're
       *  trying to write, we don't have to do anything.
       */
      cl_assert(cl, sv != NULL);
      err = graphd_key_align(greq, sub, NULL, &pr_sub, sv);
      pdb_primitive_finish(g->g_pdb, &pr_sub);

      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_key_align", err,
                     "recursive call fails");

        pdb_primitive_finish(g->g_pdb, &pr_key);
        cl_leave(cl, CL_LEVEL_SPEW,
                 "unexpected error from recursive call: "
                 "%s",
                 graphd_strerror(err));
        return err;
      }
      cl_assert(cl, sv->val_type == GRAPHD_VALUE_LIST);
      cl_assert(cl, sv->val_list_n >= 1);

      gv = sv->val_list_contents;
      cl_assert(cl, gv->val_type == GRAPHD_VALUE_GUID);
      linkcon[linkage] = gv->val_guid;

      /*  If graphd_key_align had to version or
       *  rewrite the subentry, the containing entry
       *  will have to be versioned and rewritten
       *  with the new GUID in its linkage.
       */
      needs_writing |= !GRAPH_GUID_EQ(gv->val_guid, guid_sub);
    }

    if (subconstraint_needs_writing) {
      cl_assert(cl, i < 1 + con->con_subcon_n);
      err = graphd_write_constraint(greq, sub, NULL, NULL, sv);
      if (err != 0) {
        pdb_primitive_finish(g->g_pdb, &pr_key);
        cl_leave(cl, CL_LEVEL_SPEW,
                 "unexpected error "
                 "from graphd_write_constraint: %s",
                 strerror(err));
        return err;
      }
      cl_assert(cl, sv->val_type == GRAPHD_VALUE_LIST);
      cl_assert(cl, sv->val_list_n >= 1);

      gv = sv->val_list_contents;
      cl_assert(cl, gv->val_type == GRAPHD_VALUE_GUID);

      linkcon[linkage] = gv->val_guid;
      cl_assert(cl, !GRAPH_GUID_IS_NULL(linkcon[linkage]));

      needs_writing = true;
    }
  }

  /*  3. Bind/version/insert the top primitive.
   */
  if (!needs_writing) {
    cl_assert(cl, pr != NULL);

    err = graphd_match_intrinsics(greq, con, pr);
    if (err == GRAPHD_ERR_NO)
      needs_writing = true;

    else if (err != 0) {
      pdb_primitive_finish(g->g_pdb, &pr_key);
      cl_leave(cl, CL_LEVEL_SPEW,
               "unexpected error "
               "from graphd_match_intrinsics: %s",
               strerror(err));
      return err;
    }
  }

  if (needs_writing) {
    graph_guid guid_prev_buf, *guid_prev = NULL;

    /*  Only version a preexisting primitive if the
     *  primitive has a key.
     *
     *  We pick up an existing unchanged primitive
     *  if it's pointed to by a keyed primitive, but
     *  we don't version them.
     */
    if (con->con_key == 0) pr = NULL;

    if (pr != NULL) {
      pdb_primitive_guid_get(pr, guid_prev_buf);
      guid_prev = &guid_prev_buf;
    }

    err = graphd_key_write_constraint_with_linkage(greq, con, guid_prev,
                                                   linkcon, &reply->val_guid);
    if (err != 0) {
      /* Unexpected error. */
      pdb_primitive_finish(g->g_pdb, &pr_key);
      cl_leave(cl, CL_LEVEL_SPEW,
               "unexpected error "
               "from graphd_write_constraint_with_linkage: %s",
               strerror(err));
      return err;
    }

    /*  Update <pr> to point to the newly written
     *  primitive.
     */
    err = pdb_primitive_read(g->g_pdb, &reply->val_guid, &pr_new);
    if (err != 0) {
      /* Unexpected error. */
      pdb_primitive_finish(g->g_pdb, &pr_key);
      cl_leave(cl, CL_LEVEL_SPEW,
               "unexpected error "
               "from pdb_primitive_read: %s",
               strerror(err));
      return err;
    }
    pr = &pr_new;
  } else {
    /* Just assign the GUID we already know. */
    cl_assert(cl, pr != NULL);
    pdb_primitive_guid_get(pr, reply->val_guid);
  }

  /*  3. Bind/version/insert the subconstraints that point to
   *     this primitive.
   */
  for (sub = con->con_head, i = 1; sub != NULL; sub = sub->con_next, i++) {
    /*  We're considering only primitives that say:
     *  "My container is my left/right/type/scope."
     *
     *  (That's the opposite of the ones we considered in
     *  phase 1, above.)
     */

    if (!graphd_linkage_is_my(sub->con_linkage)) continue;

    /*  If
     *   - we didn't version the topmost primitive,
     *   - this subconstraint has no key constraint,
     *   - and an old primitive that matches it exist,
     *  don't create a new one.
     *
     *  Otherwise, write as if it were independent,
     *  following the key constraint instructions if
     *  there are any.  (It still may latch onto existing
     *  material -- depends on what the key constraints say.)
     */
    cl_assert(cl, i < 1 + con->con_subcon_n);
    err = graphd_write_constraint(greq, sub, &reply->val_guid, pr, reply + i);
    if (err != 0) {
      pdb_primitive_finish(g->g_pdb, &pr_key);
      pdb_primitive_finish(g->g_pdb, &pr_new);
      cl_leave(cl, CL_LEVEL_SPEW,
               "recursive graphd_key_bind/align "
               "fails: %s",
               strerror(err));
      return err;
    }
  }

  pdb_primitive_finish(g->g_pdb, &pr_key);
  pdb_primitive_finish(g->g_pdb, &pr_new);

  cl_cover(cl);
  {
    char buf[200];
    cl_leave(cl, CL_LEVEL_SPEW, "done (%s)",
             graphd_value_to_string(reply, buf, sizeof buf));
  }
  return 0;
}

/**
 * @brief Bind to a key.
 *
 *  This call recurses through a constraint tree or subtree,
 *  writing the primitives. If possible, they are bound to or
 *  versioned rather than created anew.  (To only bind or version
 *  when directed by a key, use graphd_write_constraint()).
 *
 *  Precondition: If there is a parent constraint that we link to,
 *  it has been written or existed all along, and it's in <pr_parent>
 *
 *  Postcondition: This primitive and all the primitives in the
 *  constraints below it have been written or identified, and their
 *  GUIDs have been stored in a list created in val_out.
 *
 * @param greq		request
 * @param con		constraint to write.
 * @param pr_parent	NULL or pointer to the parent primitive
 * @param val_out	Assign the value of the created GUID to here.
 *
 * @return 0 on success, a nonzero errno value on error.
 */
int graphd_key_bind(graphd_request *greq, graphd_constraint *con,
                    pdb_primitive const *pr_parent, graphd_value *reply) {
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  int err;
  pdb_primitive pr_buf, *pr = NULL;
  graph_guid guid_parent_buf, *guid_parent = NULL;

  cl_enter(cl, CL_LEVEL_SPEW, "(%s)", graphd_constraint_to_string(con));

  err = 0;

  if (con->con_guid.guidcon_include_valid &&
      con->con_guid.guidcon_include.gs_n == 1) {
    pr = &pr_buf;
    err = pdb_primitive_read(g->g_pdb, &GUIDCON_GUID(con->con_guid), pr);
  }
  if (err != 0 && err != GRAPHD_ERR_NO) {
    cl_leave(cl, CL_LEVEL_SPEW, "unexpected error from match_constraint: %s",
             strerror(err));
    return err;
  }
  if (err == GRAPHD_ERR_NO) pr = NULL;

  if (pr_parent != NULL) {
    pdb_primitive_guid_get(pr_parent, guid_parent_buf);
    guid_parent = &guid_parent_buf;
  }

  /*  Use graphd_key_align to do the real work, relative
   *  to the existing primitive or predecessor we may just
   *  have found.
   */
  cl_assert(cl, reply != NULL);
  err = graphd_key_align(greq, con, guid_parent, pr, reply);
  if (err != 0)
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_key_align", err,
                 "unexpected error");

  if (pr) pdb_primitive_finish(graphd_request_graphd(greq)->g_pdb, pr);

  cl_leave(cl, CL_LEVEL_SPEW, "%s", err ? graphd_strerror(err) : "ok");

  return err;
}

/**
 * @brief Did the application specify values for the criteria
 *  	whose key it wants to pivot around?
 * @param g	graph handle
 * @param gses	session that's asking
 * @param greq	request that's asking
 * @param u	parsed list of criteria
 */
int graphd_key_parse_check(graphd_request *greq, graphd_constraint const *con,
                           int k) {
  cl_handle *cl = graphd_request_cl(greq);
  int pat = graphd_constraint_linkage_pattern(con);
  int linkage;

  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++) {
    if ((k & (1 << GRAPHD_PATTERN_LINKAGE(linkage))) &&
        !(pat & (1 << GRAPHD_PATTERN_LINKAGE(linkage)))) {
      if (linkage == PDB_LINKAGE_TYPEGUID &&
          con->con_type.strqueue_head != NULL)
        continue;

      cl_cover(cl);
      graphd_request_errprintf(greq, 0,
                               "SEMANTICS %s is used as a key without "
                               "specifying a %s linkage for the constraint.",
                               pdb_linkage_to_string(linkage),
                               pdb_linkage_to_string(linkage));
      return GRAPHD_ERR_SEMANTICS;
    }
    cl_cover(cl);
  }

  if (k & ((1 << GRAPHD_PATTERN_DATATYPE) | (1 << GRAPHD_PATTERN_VALUETYPE))) {
    if (con->con_valuetype == GRAPH_DATA_UNSPECIFIED) {
      cl_cover(cl);
      KEY_MISSING_ERROR(greq, "data- or valuetype");
    }
    cl_cover(cl);
  }

  if (k & (1 << GRAPHD_PATTERN_TIMESTAMP)) {
    if (!con->con_timestamp_valid) {
      cl_cover(cl);
      KEY_MISSING_ERROR(greq, "timestamp");
    }
    cl_cover(cl);
  }
  if (k & (1 << GRAPHD_PATTERN_NAME)) {
    if (con->con_name.strqueue_head == NULL) {
      cl_cover(cl);
      KEY_MISSING_ERROR(greq, "name");
    }
  }
  if (k & (1 << GRAPHD_PATTERN_VALUE)) {
    if (con->con_value.strqueue_head == NULL) {
      cl_cover(cl);
      KEY_MISSING_ERROR(greq, "value");
    }
    cl_cover(cl);
  }

  return 0;
}
