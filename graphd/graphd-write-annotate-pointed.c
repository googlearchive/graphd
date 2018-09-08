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
#include "graphd/graphd-write.h"

#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdbool.h>

/*  A "pointed cluster" is a tree of constraints along outgoing
 *  linkage connections:  the root, the root's left/right/typeguid/scope
 *  (if specified in the constraint), and in turn their
 *  left/right/typeguid/scope, and so on.
 *
 *  (But not subconstraints that point to them --
 *   this is a single cluster:
 *
 *		(name="foo"
 *			right->(name="bar")
 *			typeguid->(name="baz"))
 *
 *  this isn't:
 *		(name="foo"
 *			(<-right name="bar"))
 *
 *  In this particular context, we're interested in pointed clusters
 *  that are pointed-to by key constraints that have been matched to
 *  existing primitives.  If those primitives point to another primitive,
 *  and the key constraint points to another constraint, and the
 *  pointed-to constraint matches the pointed-to primitive, we don't
 *  version the pointed-to primitive, but just keep it.
 *
 *  This attempt to keep things if they're already the way we want
 *  them extends outwards along pointers.  So, in
 *
 *		(key="name" name="foo"
 *			right->(name="bar"
 *				(<-left name="quux")
 *				left->(name="baz")))
 *
 *  the record "baz" is included (because "bar", which points to
 *  "baz" with its left field, is included), but "quux" isn't,
 *  because it only points, not is pointed to.)
 *
 *  This gets complicated by the potential presence of other
 *  keyed clusters in the pointed clusters.  The rule here is
 *  simple (I hope it works!): a keyed record in a pointed cluster
 *  only matches if the GUID that the key-cluster algorithm assigned
 *  it is the same that the pointed-cluster algorithm is assigning it.
 */

#define CONSTRAINT_HAS_GUID(con)                \
  ((con)->con_guid.guidcon_include_annotated && \
   (con)->con_guid.guidcon_include.gs_n == 1)

/*  Utility: Is the connection between parent and child part of
 *  the key clause in parent or child?
 */
bool graphd_write_is_keyed_parent_connection(graphd_constraint* con) {
  if (con == NULL || con->con_parent == NULL) return false;

  return !!(graphd_linkage_is_my(con->con_linkage)
                ? (con->con_key & (1 << GRAPHD_PATTERN_LINKAGE(
                                       graphd_linkage_my(con->con_linkage))))
                : (con->con_parent->con_key &
                   (1 << GRAPHD_PATTERN_LINKAGE(
                        graphd_linkage_i_am(con->con_linkage)))));
}

/* Utility: Set a graphd_constraint's guid constraint to a single GUID.
 *
 *  The GUID may be from an "anchor" or "key" request.
 */
void graphd_write_annotate_guid(graphd_constraint* con,
                                graph_guid const* guid) {
  /*  If there was a match, it is now invalid;
   *  we've found a GUID to match here.
   */
  con->con_guid.guidcon_match_valid = false;

  con->con_guid.guidcon_include_annotated = true;
  con->con_guid.guidcon_include_valid = true;
  con->con_guid.guidcon_include.gs_n = 1;
  con->con_guid.guidcon_include.gs_guid = con->con_guid.guidcon_include.gs_buf;
  *con->con_guid.guidcon_include.gs_guid = *guid;
}

/*  Annotate a record with its GUID, and recurse to annotate
 *  its pointed-to subrecords with their GUIDs, given matching
 *  primitives.
 */
static int match_pointed_annotate(graphd_request* greq, graphd_constraint* con,
                                  graph_guid const* guid) {
  graphd_handle* g = graphd_request_graphd(greq);
  cl_handle* cl = graphd_request_cl(greq);
  int linkage;
  graphd_constraint* sub;
  bool first = true;
  pdb_primitive pr;
  int err = 0;

  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  cl_assert(cl, con != NULL);
  graphd_write_annotate_guid(con, guid);
  pdb_primitive_initialize(&pr);

  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    graph_guid sub_guid;

    if (!graphd_linkage_is_i_am(sub->con_linkage)) continue;

    if (first) {
      first = false;

      err = pdb_primitive_read(g->g_pdb, guid, &pr);
      if (err != 0) {
        cl_leave(cl, CL_LEVEL_SPEW, "failed to read primitive: %s",
                 graphd_strerror(err));
        return err;
      }
    }
    linkage = graphd_linkage_i_am(sub->con_linkage);

    pdb_primitive_linkage_get(&pr, linkage, sub_guid);
    if ((err = match_pointed_annotate(greq, sub, &sub_guid)) != 0) break;
  }

  pdb_primitive_finish(g->g_pdb, &pr);

  cl_leave(cl, CL_LEVEL_SPEW, "%s", err ? graphd_strerror(err) : "ok");
  return err;
}

/**
 * @brief Utility -- does a pointed cluster match a certain primitive?
 *
 * @param greq	Request for which we're doing this
 * @param con 	Constraint to match
 * @param guid	GUID of the primitive to match against.
 *
 * @return 0 if they match
 * @return GRAPHD_ERR_NO on mismatch
 * @return other nonzero error codes on system error.
 */
static int match_pointed(graphd_request* greq, graphd_constraint* con,
                         graph_guid const* guid) {
  graphd_handle* g = graphd_request_graphd(greq);
  cl_handle* cl = graphd_request_cl(greq);
  int linkage;
  graphd_constraint* sub;
  int err;
  pdb_primitive pr;

  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  cl_assert(cl, con != NULL);

  err = pdb_primitive_read(g->g_pdb, guid, &pr);
  if (err != 0) {
    cl_leave(cl, CL_LEVEL_SPEW, "failed to read primitive: %s",
             graphd_strerror(err));
    return err;
  }

  /*  match_pointed(), should it extend into keyed territory,
   *  only works for keys that happen to have matched, and
   *  happen to have exactly the value that their pointing
   *  primitive wants them to have.
   */
  if (con->con_key != 0 &&
      (!CONSTRAINT_HAS_GUID(con) ||
       !GRAPH_GUID_EQ(con->con_guid.guidcon_include.gs_guid[0], *guid))) {
    pdb_primitive_finish(g->g_pdb, &pr);
    cl_leave(cl, CL_LEVEL_SPEW, "mismatch against keyed primitive");
    return GRAPHD_ERR_NO;
  }

  err = graphd_match_intrinsics(greq, con, &pr);
  if (err != 0) {
    pdb_primitive_finish(g->g_pdb, &pr);
    cl_leave(cl, CL_LEVEL_SPEW, "intrinsic mismatch");
    return err;
  }

  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    graph_guid sub_guid;

    if (!graphd_linkage_is_i_am(sub->con_linkage)) continue;
    linkage = graphd_linkage_i_am(sub->con_linkage);
    if (!pdb_primitive_has_linkage(&pr, linkage)) {
      err = GRAPHD_ERR_NO;
      break;
    }
    pdb_primitive_linkage_get(&pr, linkage, sub_guid);
    err = match_pointed(greq, sub, &sub_guid);
    if (err != 0) break;
  }
  pdb_primitive_finish(g->g_pdb, &pr);

  cl_leave(cl, CL_LEVEL_SPEW, "%s", err ? graphd_strerror(err) : "ok");
  return err;
}

/**
 * @brief Match the child of a matched, keyed constraint to its primitive
 *
 *  See if the real-world child of an annotated constraint
 *  matches its constraint, and the same for its pointed-to children.
 *
 *  If they do match, the constraints are annotated with the GUIDs
 *  of their primitive equivalents.
 *
 *  Note that this call returns 0 if it either didn't match anything
 *  or matched and annotated something.  It's only an error if something
 *  unexpected and system failure-y happens -- something matched on
 *  the first round, but couldn't be annotated on the second, etc.
 *
 * @param greq	Request for which we're doing this
 * @param sub	Subconstraint to match.
 *
 * @return 0 if matching subconstraints have been annotated as planned
 * @return a nonzero error code on unexpected error.
 */
static int match_pointed_dependent(graphd_request* greq, graphd_constraint* sub,
                                   graphd_constraint* keyed, int linkage) {
  graphd_handle* g = graphd_request_graphd(greq);
  cl_handle* cl = graphd_request_cl(greq);
  graph_guid guid;
  int err;
  pdb_primitive pr;

  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  cl_assert(cl, sub != NULL);
  cl_assert(cl, keyed != NULL);

  /*  This function is called only for the root of the
   *  pointed cluster, not for each subcluster.
   *
   *  Consequently, _its_ node definitely has a keyed parent,
   *  although the subnodes we pull in below may not have one.
   */
  cl_assert(cl, keyed->con_key != 0);
  cl_assert(cl, CONSTRAINT_HAS_GUID(keyed));

  err = pdb_primitive_read(g->g_pdb, keyed->con_guid.guidcon_include.gs_guid,
                           &pr);
  if (err != 0) {
    cl_leave(cl, CL_LEVEL_SPEW, "can't read keyed primitive: %s",
             graphd_strerror(err));
    return err;
  }

  if (!pdb_primitive_has_linkage(&pr, linkage)) {
    pdb_primitive_finish(g->g_pdb, &pr);
    cl_leave(cl, CL_LEVEL_SPEW, "keyed primitive doesn't even have %s linkage",
             pdb_linkage_to_string(linkage));
    return 0;
  }

  /* Stage I: Does the primitive network under keyed
   *	match sub's pointed constraint cluster?
   */
  pdb_primitive_linkage_get(&pr, linkage, guid);
  pdb_primitive_finish(g->g_pdb, &pr);

  err = match_pointed(greq, sub, &guid);

  /* Stage II: If yes, annotate the constraints in the
   *	cluster with their respective equivalents.
   */
  if (err == 0)
    err = match_pointed_annotate(greq, sub, &guid);
  else if (err == GRAPHD_ERR_NO)
    err = 0; /* It's okay to not match. */
  else
    cl_log(cl, CL_LEVEL_FAIL,
           "match_pointed_dependent: unexpected error "
           "from match_pointed: %s",
           graphd_strerror(err));

  cl_leave(cl, CL_LEVEL_SPEW, "%s", err ? graphd_strerror(err) : "ok");
  return err;
}

/**
 * @brief Annotate pointed clusters under matched keys with their GUIDs.
 *
 * @param greq	Request for which we're doing this
 * @param con 	Constraint to match
 *
 * @return 0 on success
 * @return a nonzero error code on system error.
 */
int graphd_write_annotate_pointed(graphd_request* greq,
                                  graphd_constraint* con) {
  graphd_constraint* sub;
  int err;

  if (con->con_parent != NULL && con->con_parent->con_key != 0 &&
      CONSTRAINT_HAS_GUID(con->con_parent) &&
      !graphd_write_is_keyed_parent_connection(con) &&
      graphd_linkage_is_i_am(con->con_linkage)) {
    /*  Con's keyed, matched parent points to con.
     *  The connection itself is not keyed.
     *
     *  We want to know whether <con> and its pointed
     *  cluster (how's that for a band name?) match
     *  the primitives they'd be if the parent's GUID
     *  would, in fact, be used.
     */
    err = match_pointed_dependent(greq, con, con->con_parent,
                                  graphd_linkage_i_am(con->con_linkage));
    if (err != 0) return err;
  } else if (con->con_key != 0 && CONSTRAINT_HAS_GUID(con) &&
             con->con_parent != NULL && con->con_parent->con_key == 0 &&
             graphd_linkage_is_my(con->con_linkage) &&
             !(con->con_key & (1 << GRAPHD_PATTERN_LINKAGE(
                                   graphd_linkage_my(con->con_linkage))))) {
    /*  Con is keyed, its parent is unkeyed.
     *  The connection itself is not keyed.
     */
    err = match_pointed_dependent(greq, con->con_parent, con,
                                  graphd_linkage_my(con->con_linkage));
    if (err != 0) return err;
  }

  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    /*  Do this operation everywhere in the tree.
     */
    err = graphd_write_annotate_pointed(greq, sub);
    if (err != 0) return err;
  }
  return 0;
}
