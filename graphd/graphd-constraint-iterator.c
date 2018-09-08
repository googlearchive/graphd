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

#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "libpdb/pdb.h"

#define GRAPHD_MULTIPLE_LINKSTO_MAX 100
#define GRAPHD_MULTIPLE_NEXTCOST_MAX 100

#define MAX_HULLSET_SIZE (32 * 1024)
#define SUBNODE_LOOKUP_COST 1
#define TOPLEVEL_BONUS 1
#define GRAPHD_OPTIMIZE_MAX (32 * 1024)

#define HAS_GUIDS(guidcon)                                                   \
  ((guidcon).guidcon_include_valid && (guidcon).guidcon_include.gs_n >= 1 && \
   !graphd_guid_set_contains_null(&(guidcon).guidcon_include))

#define HAS_GUID(guidcon)                                                    \
  ((guidcon).guidcon_include_valid && (guidcon).guidcon_include.gs_n == 1 && \
   !(guidcon).guidcon_include.gs_null)

#define GUID_POINTER(guidcon) (guidcon).guidcon_include.gs_guid

static int set_linkage(graphd_request *greq, graphd_constraint *con,
                       int linkage, graph_guid const *guid);

static int set_linkage_id(graphd_request *greq, graphd_constraint *con,
                          int linkage, pdb_id id);

static int set_guid(graphd_request *greq, graphd_constraint *con,
                    graph_guid const *guid);

static int add_subcondition(graphd_request *greq, graphd_constraint *con,
                            pdb_iterator **sub_it) {
  graphd_handle *g = graphd_request_graphd(greq);

  graphd_constraint_account(greq, con, *sub_it);

  if (pdb_iterator_n_valid(pdb, *sub_it) &&
      con->con_setsize > pdb_iterator_n(pdb, *sub_it))
    con->con_setsize = pdb_iterator_n(pdb, *sub_it);

  return graphd_iterator_and_add_subcondition(g, con->con_it, sub_it);
}

static char const *direction_to_string(graphd_direction dir) {
  switch (dir) {
    case GRAPHD_DIRECTION_FORWARD:
      return "forward";
    case GRAPHD_DIRECTION_BACKWARD:
      return "backward";
    case GRAPHD_DIRECTION_ORDERING:
      return "ordered";
    case GRAPHD_DIRECTION_ANY:
      return "any";
  }

  return "???";
}

/**
 * @brief Initialize the iterator subtree.
 * @param greq	- request for which this calculation is being performed
 * @param con 	- constraint subtree to annotate.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
static int initialize(graphd_request *greq, graphd_constraint *con) {
  int err = 0;
  cl_handle *const cl = graphd_request_cl(greq);
  graphd_handle *g = graphd_request_graphd(greq);
  graphd_constraint *sub;
  graphd_constraint_or *cor;
  graphd_direction direction;
  char const *ordering;
  int linkage;
  unsigned long long high;

  cl_assert(cl, con->con_it == NULL);

  /*  Make sure the constraint's iterator accounting
   *  information has its ID; that'll let it get found
   *  after freezing/thawing.
   */
  con->con_iterator_account.ia_id = con->con_id;

  /*  Initialize con->con_linkguid with NULL.
   */
  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++)
    GRAPH_GUID_MAKE_NULL(con->con_linkguid[linkage]);

  /*  Set low, high, and the direction.
   */
  con->con_low = graphd_dateline_low(g, con);

  high = graphd_dateline_high(g, con);
  if (high < con->con_high) con->con_high = high;

  direction = graphd_sort_root_iterator_direction(greq, con, &ordering);

  cl_assert(cl, GRAPHD_DIRECTION_VALID(direction));

  con->con_forward = direction != GRAPHD_DIRECTION_BACKWARD;

  cl_log(cl, CL_LEVEL_VERBOSE, "initialize(%s): direction=%s",
         graphd_constraint_to_string(con), direction_to_string(direction));

  graphd_constraint_setsize_initialize(g, con);

  /*  Do we have a cursor that isn't controlled by a sort?
   *  If yes, stick with the iterator tree encoded in the cursor.
   */
  if (con->con_cursor_s != NULL) {
    if (graphd_sort_is_cursor(con->con_cursor_s, con->con_cursor_e)) {
      err = graphd_sort_cursor_peek(greq, con);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_VERBOSE, "graphd_sort_cursor_peek", err,
                     "con=%s", graphd_constraint_to_string(con));
        return err;
      }
    } else {
      err = graphd_constraint_cursor_thaw(greq, con, &con->con_it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_VERBOSE, "graphd_constraint_cursor_thaw", err,
                     "con=%s", graphd_constraint_to_string(con));
        return err;
      }
      cl_assert(cl, con->con_it != NULL);
    }
  }

  /*  OR branches.
   */
  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next)

    if ((err = initialize(greq, &cor->or_head)) != 0 ||
        (cor->or_tail != NULL && (err = initialize(greq, cor->or_tail)) != 0)) {
      cl_log_errno(cl, CL_LEVEL_VERBOSE, "initialize", err, "con=%s",
                   graphd_constraint_to_string(con));
      return err;
    }

  /*  Subtree.
   */
  for (sub = con->con_head; sub != *con->con_tail; sub = sub->con_next) {
    /*  Skip the subconstraints of or branches - we already
     *  visited those while doing the "or".
     */
    if (sub->con_parent != con) continue;

    if ((err = initialize(greq, sub)) != 0) return err;
  }
  return 0;
}

/**
 * @brief An iterator is empty.  Draw conclusions from that.
 *
 * @param greq		request we're working for.
 * @param con		constraint that we know to have no solution.
 *
 * @return 0 on success, a nonzero error code on unexpected error.
 */
static int set_empty(graphd_request *greq, graphd_constraint *con) {
  graphd_handle *const g = graphd_request_graphd(greq);
  cl_handle *const cl = graphd_request_cl(greq);
  pdb_handle *const pdb = g->g_pdb;
  int err;

  do {
    bool changed = false;

    if (con->con_it == NULL ||
        !pdb_iterator_null_is_instance(pdb, con->con_it)) {
      pdb_iterator *null_it;

      err = pdb_iterator_null_create(pdb, &null_it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_null_create", err,
                     "for con=%s", graphd_constraint_to_string(con));
        return err;
      }
      pdb_iterator_destroy(pdb, &con->con_it);
      graphd_constraint_account(greq, con, null_it);

      con->con_it = null_it;
      changed = true;
    }
    if (!con->con_false) {
      con->con_false = true;
      changed = true;
    }
    if (!changed) /* We already knew. */
      return 0;

    cl_log(cl, CL_LEVEL_VERBOSE, "set_empty %s",
           graphd_constraint_to_string(con));

  } while (GRAPHD_CONSTRAINT_IS_MANDATORY(con) && con->con_or == NULL &&
           (con = con->con_parent) != NULL);

  return 0;
}

/**
 * @brief We've learned something new about an iterator's boundaries.
 *
 * @param greq		request we're working for.
 * @param con		constraint whose boundaries we're adjusting
 * @param low		new low  (first ID included)
 * @param high		new high (first ID not included)
 *
 * @return 0 on success, a nonzero error code on unexpected error.
 */
static int set_boundary(graphd_request *greq, graphd_constraint *con,
                        unsigned long long low, unsigned long long high) {
  int err = 0;
  graphd_constraint *sub;

  if (low > con->con_low) {
    if ((con->con_low = low) >= con->con_high) return set_empty(greq, con);

    /*  Increase the "low" of everything that points to
     *  me to low + 1.
     */

    if (con->con_parent != NULL && graphd_linkage_is_i_am(con->con_linkage) &&
        GRAPHD_CONSTRAINT_IS_MANDATORY(con) && con->con_or == NULL) {
      err = set_boundary(greq, con->con_parent, low + 1, PDB_ITERATOR_HIGH_ANY);
      if (err != 0) return err;
    }
    for (sub = con->con_head; sub != *con->con_tail; sub = sub->con_next)
      if (graphd_linkage_is_my(sub->con_linkage)) {
        err = set_boundary(greq, sub, low + 1, PDB_ITERATOR_HIGH_ANY);
        if (err != 0) return err;
      }
  }
  if (high < con->con_high) {
    if (con->con_low >= (con->con_high = high)) return set_empty(greq, con);

    /*  Decrease the "high" of everything I point to
     *  to high - 1.
     */
    if (con->con_parent != NULL && graphd_linkage_is_my(con->con_linkage) &&
        GRAPHD_CONSTRAINT_IS_MANDATORY(con) && con->con_or == NULL) {
      err = set_boundary(greq, con->con_parent, PDB_ITERATOR_LOW_ANY,
                         con->con_high - 1);
      if (err != 0) return err;
    }
    for (sub = con->con_head; sub != *con->con_tail; sub = sub->con_next)
      if (graphd_linkage_is_i_am(sub->con_linkage)) {
        err = set_boundary(greq, sub, PDB_ITERATOR_LOW_ANY, con->con_high - 1);
        if (err != 0) return err;
      }
  }
  return 0;
}

static int id_to_linkage_guid(graphd_request *greq, pdb_id id, int linkage,
                              graph_guid *guid_out) {
  graphd_handle *const g = graphd_request_graphd(greq);
  cl_handle *const cl = graphd_request_cl(greq);
  pdb_handle *const pdb = g->g_pdb;
  char buf[200];
  pdb_primitive pr;
  int err;

  cl_assert(cl, PDB_IS_LINKAGE(linkage));
  if ((err = pdb_id_read(pdb, id, &pr)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err, "%s(%lld)",
                 pdb_linkage_to_string(linkage), (long long)id);
    return err;
  }
  if (!pdb_primitive_has_linkage(&pr, linkage)) {
    pdb_primitive_finish(pdb, &pr);
    cl_log(cl, CL_LEVEL_VERBOSE, "id_to_linkage_guid: no %s(%lld)",
           pdb_linkage_to_string(linkage), (long long)id);
    return GRAPHD_ERR_NO;
  }
  pdb_primitive_linkage_get(&pr, linkage, *guid_out);
  pdb_primitive_finish(pdb, &pr);

  cl_log(cl, CL_LEVEL_SPEW, "%s(%lld) -> %s", pdb_linkage_to_string(linkage),
         (long long)id, graph_guid_to_string(guid_out, buf, sizeof buf));
  return 0;
}

static int guid_to_linkage_guid(graphd_request *greq, graph_guid const *guid,
                                int linkage, graph_guid *guid_out) {
  graphd_handle *const g = graphd_request_graphd(greq);
  cl_handle *const cl = graphd_request_cl(greq);
  pdb_handle *const pdb = g->g_pdb;

  char buf[200];
  pdb_id id;
  int err;

  if ((err = pdb_id_from_guid(pdb, &id, guid)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err, "%s",
                 graph_guid_to_string(guid, buf, sizeof buf));
    return err;
  }
  return id_to_linkage_guid(greq, id, linkage, guid_out);
}

static int set_guid_consequences(graphd_request *greq, graphd_constraint *con,
                                 graph_guid const *guid) {
  graphd_constraint *sub;
  graph_guid sub_guid;
  int err = 0;
  graphd_handle *const g = graphd_request_graphd(greq);
  cl_handle *const cl = graphd_request_cl(greq);
  int linkage;

  cl_assert(g->g_cl, con->con_it != NULL);
  set_boundary(greq, con, con->con_it->it_low, con->con_it->it_high);

  /* Push knowledge into the surrounding network.
   *
   * Adjacent iterators now have either an additional
   * linkage point or are completely determined.
   */
  for (sub = con->con_head; sub != *con->con_tail; sub = sub->con_next) {
    int linkage;

    if (graphd_linkage_is_my(sub->con_linkage)) {
      /* sub points to con.
       */
      linkage = graphd_linkage_my(sub->con_linkage);
      err = set_linkage(greq, sub, graphd_linkage_my(sub->con_linkage), guid);
      if (err != 0) return err;
    } else if (sub->con_it == NULL) {
      linkage = graphd_linkage_i_am(sub->con_linkage);

      /*  Con is a single GUID, and points to sub.
       *  Therefore, sub is at most a single GUID as well.
       */
      err = guid_to_linkage_guid(greq, guid, linkage, &sub_guid);
      if (err != 0) {
        if (err != GRAPHD_ERR_NO) return err;

        err = set_empty(greq, sub);
        if (err != 0) return err;
      } else {
        err = set_guid(greq, sub, &sub_guid);
        if (err != 0) return err;
      }
    }
  }

  if (!GRAPHD_CONSTRAINT_IS_MANDATORY(con) || con->con_or != NULL ||
      con->con_parent == NULL)
    return 0;

  if (con->con_parent->con_it == NULL) {
    graphd_constraint *const parent = con->con_parent;

    /* Push towards the parent.
     */
    if (graphd_linkage_is_i_am(con->con_linkage)) {
      err = set_linkage(greq, parent, graphd_linkage_i_am(con->con_linkage),
                        guid);
      if (err != 0) return err;
    } else {
      graph_guid parent_guid;

      /*  Con is a single GUID, and points to its parent.
       *  Therefore, its parent is at most single GUID
       *  as well.
       */
      cl_assert(cl, graphd_linkage_is_my(con->con_linkage));

      linkage = graphd_linkage_my(con->con_linkage);
      err = guid_to_linkage_guid(greq, guid, linkage, &parent_guid);
      if (err != 0) {
        if (err != GRAPHD_ERR_NO) return err;

        err = set_empty(greq, parent);
        if (err != 0) return err;
      } else {
        err = set_guid(greq, parent, &parent_guid);
        if (err != 0) return err;
      }
    }
  }
  return 0;
}

/**
 * @brief An iterator matches a single GUID.  Draw conclusions from that.
 *
 * @param greq	- request for which we're working
 * @param con 	- constraint to annotate.
 * @param guid	- pointer to the GUID it matches.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
static int set_guid(graphd_request *greq, graphd_constraint *con,
                    graph_guid const *guid) {
  graph_guid sub_guid;
  int err = 0;
  graphd_handle *const g = graphd_request_graphd(greq);
  cl_handle *const cl = graphd_request_cl(greq);
  pdb_handle *const pdb = g->g_pdb;
  char buf[GRAPH_GUID_SIZE];

  cl_log(cl, CL_LEVEL_VERBOSE, "set_guid %s=%s",
         graphd_constraint_to_string(con),
         graph_guid_to_string(guid, buf, sizeof buf));

  if (con->con_it != NULL) {
    pdb_id sub_id;
    char buf1[GRAPH_GUID_SIZE], buf2[GRAPH_GUID_SIZE];

    /*  We already know what this is supposed to be.
     *
     *  If what we know doesn't match what we're being
     *  told right now, the constraint has no match.
     */
    cl_assert(cl, con->con_it != NULL);
    if (con->con_false) return 0;

    err = pdb_iterator_single_id(pdb, con->con_it, &sub_id);
    if (err != 0) {
      if (err == PDB_ERR_MORE || err == PDB_ERR_TOO_MANY)
        err = 0;

      else if (err == GRAPHD_ERR_NO)
        err = set_empty(greq, con);
      else
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_single_id", err, "it=%s",
                     pdb_iterator_to_string(pdb, con->con_it, buf, sizeof buf));
      return err;
    }

    err = pdb_id_to_guid(pdb, sub_id, &sub_guid);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_to_guid", err, "id=%lld",
                   (long long)sub_id);
      return err;
    }

    /*  They match? */
    if (GRAPH_GUID_EQ(*guid, sub_guid)) return 0;

    cl_log(cl, CL_LEVEL_DEBUG,
           "set_guid: "
           "conflicting GUIDs %s and %s -> empty",
           graph_guid_to_string(guid, buf1, sizeof buf1),
           graph_guid_to_string(&sub_guid, buf2, sizeof buf2));

    return set_empty(greq, con);
  }

  if (con->con_it) pdb_iterator_destroy(pdb, &con->con_it);

  err = graphd_iterator_fixed_create_guid_array(
      g, guid, 1, PDB_ITERATOR_LOW_ANY, PDB_ITERATOR_HIGH_ANY, con->con_forward,
      &con->con_it);
  if (err != 0) return err;

  cl_assert(g->g_cl, con->con_it != NULL);
  return set_guid_consequences(greq, con, guid);
}

static int set_linkage(graphd_request *greq, graphd_constraint *con,
                       int linkage, graph_guid const *guid) {
  char buf1[GRAPH_GUID_SIZE], buf2[GRAPH_GUID_SIZE];
  cl_handle *cl = graphd_request_cl(greq);
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_handle *const pdb = g->g_pdb;

  cl_log(cl, CL_LEVEL_VERBOSE, "set_linkage %s(%s)=%s",
         pdb_linkage_to_string(linkage), graphd_constraint_to_string(con),
         graph_guid_to_string(guid, buf1, sizeof buf1));

  /* You can't set NULL linkage - it's not a valid GUID! */
  if (GRAPH_GUID_IS_NULL(*guid)) {
    con->con_false = true;
    return 0;
  }

  /* We didn't know? */
  if (GRAPH_GUID_IS_NULL(con->con_linkguid[linkage])) {
    int err;
    pdb_id id;

    con->con_linkguid[linkage] = *guid;

    /*  If I'm pointing to an ID X, my own value
     *  is > X.  In other words, X is the new low.
     */
    err = pdb_id_from_guid(pdb, &id, guid);
    if (err != 0) return set_empty(greq, con);

    err = set_boundary(greq, con, id + 1, PDB_ITERATOR_HIGH_ANY);
    if (err != 0) return err;

    /*  If I'm pointing to my parent with the same
     *  linkage that just got set, we now know
     *  the parent's GUID.
     */
    if (con->con_parent != NULL && graphd_linkage_is_my(con->con_linkage) &&
        graphd_linkage_my(con->con_linkage) == linkage &&
        GRAPHD_CONSTRAINT_IS_MANDATORY(con) && con->con_or != NULL) {
      err = set_guid(greq, con->con_parent, guid);
      if (err != 0) return err;
    }
  }

  /* We already knew? */
  if (GRAPH_GUID_EQ(*guid, con->con_linkguid[linkage])) return 0;

  cl_log(cl, CL_LEVEL_DEBUG,
         "set_linkage: conflicting GUIDs for %s linkage: "
         "%s and %s -> empty",
         pdb_linkage_to_string(linkage),
         graph_guid_to_string(guid, buf1, sizeof buf1),
         graph_guid_to_string(con->con_linkguid + linkage, buf2, sizeof buf2));

  return set_empty(greq, con);
}

static int set_linkage_id(graphd_request *greq, graphd_constraint *con,
                          int linkage, pdb_id id) {
  pdb_handle *const pdb = graphd_request_graphd(greq)->g_pdb;
  cl_handle *const cl = graphd_request_cl(greq);
  graph_guid guid;
  int err;

  err = pdb_id_to_guid(pdb, id, &guid);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_to_guid", err, "%lld",
                 (long long)id);
    return err;
  }
  return set_linkage(greq, con, linkage, &guid);
}

/**
 * @brief Create cheap details,
 *	and fill in single-element constraint linkage.
 *
 * 	Pre:
 *		- function has run over parent constraints, if any.
 *		- con->con_forward is set correctly.
 * 	Post:
 *		- fixed iterators for things with fixed GUID sets
 *		- single-GUID constraints get a fixed iterator and a lock
 *		  to prevent further changes to the iterator
 *		- con_linkguid[] is filled in as best we can.
 *
 * @param greq	- request for which this calculation is being performed
 * @param con 	- constraint subtree to annotate.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
static int cheap(graphd_request *greq, graphd_constraint *con) {
  int err = 0;
  graphd_handle *const g = graphd_request_graphd(greq);
  cl_handle *const cl = graphd_request_cl(greq);
  pdb_handle *const pdb = g->g_pdb;
  graphd_constraint *sub;
  graphd_string_constraint const *strcon;
  graphd_constraint_or *cor;
  int linkage;

  cl_enter(cl, CL_LEVEL_VERBOSE, "con=%s", graphd_constraint_to_string(con));

  /*  Is this constraint impossible?
   */
  if (con->con_false) {
    cl_leave(cl, CL_LEVEL_SPEW, "false/null");
    return set_empty(greq, con);
  }

  /*  Set GUIDs directly specified in linkage constraints.
   *
   *  After this, all write accesses to con_linkguid should
   *  go through set_linkage.
   */

  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++)

    if (HAS_GUID(con->con_linkcon[linkage])) {
      err = set_linkage(greq, con, linkage,
                        GUID_POINTER(con->con_linkcon[linkage]));
      if (err != 0) {
        cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
        return err;
      }
    }

  /*  Pull out GUIDs directly specified in the GUID constraints.
   */
  if (con->con_it == NULL && HAS_GUID(con->con_guid)) {
    err = set_guid(greq, con, GUID_POINTER(con->con_guid));
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "error from set_guid: %s",
               graphd_strerror(err));
      return err;
    }
    cl_assert(cl, con->con_it != NULL);
  }

  if (con->con_it == NULL && HAS_GUIDS(con->con_guid)) {
    err = graphd_iterator_fixed_create_guid_array(
        g, GUID_POINTER(con->con_guid), con->con_guid.guidcon_include.gs_n,
        PDB_ITERATOR_LOW_ANY, con->con_high, con->con_forward, &con->con_it);
    if (err != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "error from fixed guid array: %s",
               graphd_strerror(err));
      return err;
    }
    cl_assert(cl, con->con_it != NULL);
  }

  if (con->con_it == NULL && con->con_parent != NULL) {
    /*  Con has a parent, and doesn't yet know
     *  for sure who it is.
     */
    graphd_constraint *parent = con->con_parent;

    if (graphd_linkage_is_i_am(con->con_linkage)) {
      /*  The parent points to con.
       */
      linkage = graphd_linkage_i_am(con->con_linkage);
      if (!GRAPH_GUID_IS_NULL(parent->con_linkguid[linkage])) {
        /*  The parent knows who it
         *  points to.
         */
        err = set_guid(greq, con, con->con_parent->con_linkguid + linkage);
        if (err != 0) {
          char buf[GRAPH_GUID_SIZE];
          cl_log_errno(cl, CL_LEVEL_FAIL, "set_guid", err, "%s",
                       graph_guid_to_string(parent->con_linkguid + linkage, buf,
                                            sizeof buf));
          cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
          return err;
        }
        cl_assert(cl, con->con_it != NULL);
      }
    } else {
      /*  Con points to the parent.
       */
      pdb_id id;
      cl_assert(cl, graphd_linkage_is_my(con->con_linkage));

      linkage = graphd_linkage_my(con->con_linkage);

      if (GRAPH_GUID_IS_NULL(con->con_linkguid[linkage]) &&
          parent->con_it != NULL &&
          (err = pdb_iterator_single_id(pdb, parent->con_it, &id)) == 0) {
        /*  The parent knows who it is.  Therefore,
         *  con learns where it is pointing.
         */
        err = set_linkage_id(greq, con, linkage, id);
        if (err != 0) {
          cl_leave(cl, CL_LEVEL_VERBOSE, "error: %s", graphd_strerror(err));
          return err;
        }
      }
    }
  }

  /* name = "SINGLE_STRING_HERE"
   */
  if (con->con_it == NULL && (strcon = con->con_name.strqueue_head) != NULL &&
      strcon->strcon_next == NULL && strcon->strcon_op == GRAPHD_OP_EQ &&
      strcon->strcon_head != NULL && strcon->strcon_head->strcel_s != NULL &&
      strcon->strcon_head->strcel_next == NULL) {
    pdb_iterator *it;
    pdb_id id;

    graphd_string_constraint_element const *const strcel = strcon->strcon_head;

    err = pdb_hash_iterator(pdb, PDB_HASH_NAME, strcel->strcel_s,
                            strcel->strcel_e - strcel->strcel_s, con->con_low,
                            con->con_high, con->con_forward, &it);
    if (err == GRAPHD_ERR_NO) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "empty");
      return set_empty(greq, con);
    }

    if (!pdb_iterator_n_valid(pdb, it))
      pdb_iterator_destroy(pdb, &it);
    else
      switch (pdb_iterator_n(pdb, it)) {
        case 1:
          err = pdb_iterator_single_id(pdb, it, &id);
          if (err == 0) {
            graph_guid guid;

            err = pdb_id_to_guid(pdb, id, &guid);
            if (err == 0) {
              err = pdb_iterator_reset(pdb, it);
              if (err != 0) {
                pdb_iterator_destroy(pdb, &it);
                cl_leave(cl, CL_LEVEL_VERBOSE, "reset fails");
                return err;
              }
              con->con_it = it;
              cl_leave(cl, CL_LEVEL_VERBOSE, "single name");
              return set_guid_consequences(greq, con, &guid);
            } else {
              pdb_iterator_destroy(pdb, &it);
              if (err == GRAPHD_ERR_NO) {
                cl_leave(cl, CL_LEVEL_VERBOSE, "empty");
                return set_empty(greq, con);
              }
              cl_leave(cl, CL_LEVEL_VERBOSE, "error in single");
              return set_empty(greq, con);
            }
          }
        default:
          break;
        case 0:
          pdb_iterator_destroy(pdb, &it);
          cl_leave(cl, CL_LEVEL_VERBOSE, "empty");
          return set_empty(greq, con);
      }
    pdb_iterator_destroy(pdb, &it);
  }

  /*  OR branches
   */
  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    if ((err = cheap(greq, &cor->or_head)) != 0 ||
        (cor->or_tail != NULL && (err = cheap(greq, cor->or_tail)) != 0)) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "recursive call (or branch): %s",
               graphd_strerror(err));
      return err;
    }
  }

  /*  Subtree.
   */
  for (sub = con->con_head; sub != *con->con_tail; sub = sub->con_next) {
    /*  If this subconstraint is part of an or-branch, skip it;
     *  we already visited it while visiting the or branch's
     *  sub-branch.
     */
    if (sub->con_parent != con) continue;

    if ((err = cheap(greq, sub)) != 0) {
      cl_leave(cl, CL_LEVEL_VERBOSE, "recursive call: %s",
               graphd_strerror(err));
      return err;
    }
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "ok");
  return 0;
}

/**
 * @brief 	Create an iterator that embodies pointing to something
 *  		else with a right/left/type/scope link.
 *
 * @param greq			Request that supplies allocators
 * @param con			constraint that does the pointing
 * @param guid			GUID the iterator points to with its linkage.
 * @param linkage		which pointer are we dealing with?
 * @param my_linkage_guid 	Array of up to four single guids that we
 *				*know* are at the end of type/scope/right/left
 *				links (unknown = null guid)
 * @param it_out		assign the iterator to this
 * @param good_iterator_inout	have we seen an efficient, correctly
 *				ordered iterator yet?
 *				When set, prevents the addition
 *				of an "all" iterator.
 * @param vip_inout		Has a VIP iterator been created for this yet?
 *				If yes, we won't need a type iterator.
 */
static int linkage_single_iterator(graphd_request *greq, graphd_constraint *con,
                                   graph_guid const *guid, int linkage,
                                   graphd_direction direction,
                                   char const *ordering, pdb_iterator **it_out,
                                   bool *good_iterator_inout, bool *vip_inout) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_handle *pdb = g->g_pdb;
  int err;

  cl_enter(cl, CL_LEVEL_SPEW,
           "linkage=%s, constraint=%s %lld..%lld, dir=%d, ordering=%s",
           pdb_linkage_to_string(linkage), graphd_constraint_to_string(con),
           (long long)con->con_low, (long long)con->con_high, direction,
           ordering ? ordering : "null");

  cl_assert(cl, GRAPHD_DIRECTION_VALID(direction));

  *it_out = NULL;
  *good_iterator_inout = true;

  if (linkage == PDB_LINKAGE_TYPEGUID && *vip_inout) {
    cl_leave(cl, CL_LEVEL_SPEW, "typeguid already in vip");
    return GRAPHD_ERR_ALREADY;
  }

  if (!GRAPH_GUID_IS_NULL(con->con_linkguid[PDB_LINKAGE_TYPEGUID]) &&
      (linkage == PDB_LINKAGE_LEFT || linkage == PDB_LINKAGE_RIGHT)) {
    pdb_id type_id, source_id;
    char buf[200];
    graph_guid const *const type_guid =
        con->con_linkguid + PDB_LINKAGE_TYPEGUID;

    /*  Candidate for a VIP link.
     */
    err = pdb_id_from_guid(pdb, &type_id, type_guid);
    if (err != 0) {
      cl_leave_err(cl, CL_LEVEL_VERBOSE, err,
                   "pdb_id_from_guid(type_guid=%s) failed",
                   graph_guid_to_string(type_guid, buf, sizeof buf));
      return err;
    }

    err = pdb_id_from_guid(pdb, &source_id, guid);
    if (err != 0) {
      cl_leave_err(cl, CL_LEVEL_VERBOSE, err,
                   "pdb_id_from_guid(source_guid=%s) "
                   "failed",
                   graph_guid_to_string(guid, buf, sizeof buf));
      return err;
    }
    err = graphd_iterator_vip_create(g, source_id, linkage, type_id, type_guid,
                                     con->con_low, con->con_high,
                                     direction != GRAPHD_DIRECTION_BACKWARD,
                                     /* error-if-null */ false, it_out);
    if (err == 0) {
      *vip_inout |= true;

      graphd_iterator_set_direction_ordering(pdb, *it_out, direction, ordering);
      graphd_constraint_account(greq, con, *it_out);
    }
    cl_leave(cl, CL_LEVEL_SPEW, "vip: %s (it=%p)",
             err ? graphd_strerror(err) : "ok", (void *)*it_out);
    return err;
  }
  err = pdb_linkage_iterator(pdb, linkage, guid, con->con_low, con->con_high,
                             direction != GRAPHD_DIRECTION_BACKWARD,
                             /* error-if-null */ false, it_out);
  if (err == 0) {
    graphd_iterator_set_direction_ordering(pdb, *it_out, direction, ordering);
    graphd_constraint_account(greq, con, *it_out);
  }
  cl_leave(cl, CL_LEVEL_SPEW, "-> pdb_linkage");
  return err;
}

static int multiple_linksto_create(
    graphd_request *greq, graphd_constraint *con, int linkage, int hint_linkage,
    graph_guid *hint_guid, size_t hint_guid_n, pdb_iterator **sub,
    unsigned long long low, unsigned long long high, graphd_direction direction,
    char const *ordering, pdb_iterator **it_out) {
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  pdb_handle *pdb = g->g_pdb;
  pdb_iterator *or_it = NULL, *sub_it = NULL, *lto_it = NULL;
  int err;
  size_t i;
  char buf[200];

  if (hint_guid_n >= GRAPHD_MULTIPLE_LINKSTO_MAX || hint_guid_n == 0 ||
      (!(linkage == PDB_LINKAGE_TYPEGUID &&
         (hint_linkage == PDB_LINKAGE_RIGHT ||
          hint_linkage == PDB_LINKAGE_LEFT)) &&
       !(hint_linkage == PDB_LINKAGE_TYPEGUID &&
         (linkage == PDB_LINKAGE_RIGHT || linkage == PDB_LINKAGE_LEFT)))) {
    /*  Can't do it; just use a straight iterator.
     */
    err =
        graphd_iterator_linksto_create(greq, linkage, PDB_LINKAGE_N, NULL, sub,
                                       low, high, direction, ordering, it_out);
    if (err == 0) graphd_constraint_account(greq, con, *it_out);
    return err;
  }

  /*  Create an "OR" iterator to hold the multiple linkstos.
   */
  err = graphd_iterator_or_create(
      greq, hint_guid_n, direction != GRAPHD_DIRECTION_BACKWARD, &or_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_create", err, "n=%zu",
                 hint_guid_n);
    goto err;
  }

  for (i = 0; i < hint_guid_n; i++) {
    /*  Create the specific linksto.
     */
    if (i == hint_guid_n - 1) {
      sub_it = *sub;
      *sub = NULL;
    } else {
      err = pdb_iterator_clone(pdb, *sub, &sub_it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "sub=%s",
                     pdb_iterator_to_string(pdb, *sub, buf, sizeof buf));
        goto err;
      }
    }
    graphd_constraint_account(greq, con, sub_it);

    err = graphd_iterator_linksto_create(greq, linkage, hint_linkage,
                                         hint_guid + i, &sub_it, low, high,
                                         direction, ordering, &lto_it);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_linksto_create", err,
                   "sub=%s",
                   pdb_iterator_to_string(pdb, *sub, buf, sizeof buf));
      goto err;
    }

    graphd_constraint_account(greq, con, lto_it);

    /*  Add the specific linksto to the "or".
     */
    err = graphd_iterator_or_add_subcondition(or_it, &lto_it);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_add_subcondition",
                   err, "sub=%s",
                   pdb_iterator_to_string(pdb, lto_it, buf, sizeof buf));
      goto err;
    }

    pdb_iterator_destroy(pdb, &lto_it);
  }

  /*  Finish constructing the  "or".
   */
  err = graphd_iterator_or_create_commit(or_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iteartor_or_create_commit", err,
                 "sub=%s", pdb_iterator_to_string(pdb, or_it, buf, sizeof buf));
    goto err;
  }

  graphd_constraint_account(greq, con, or_it);
  *it_out = or_it;
  return 0;

err:
  pdb_iterator_destroy(pdb, &or_it);
  pdb_iterator_destroy(pdb, &sub_it);
  pdb_iterator_destroy(pdb, &lto_it);

  return err;
}

static int graphd_constraint_iterator_hint_linkage(graphd_request *greq,
                                                   graphd_constraint *con,
                                                   int linkage) {
  if (linkage == PDB_LINKAGE_TYPEGUID) {
    /* XXX which one is smaller? */

    if (!GRAPH_GUID_IS_NULL(con->con_linkguid[PDB_LINKAGE_LEFT]))
      return PDB_LINKAGE_LEFT;

    else if (!GRAPH_GUID_IS_NULL(con->con_linkguid[PDB_LINKAGE_RIGHT]))
      return PDB_LINKAGE_RIGHT;
  } else if (linkage == PDB_LINKAGE_RIGHT || linkage == PDB_LINKAGE_LEFT) {
    if (!GRAPH_GUID_IS_NULL(con->con_linkguid[PDB_LINKAGE_TYPEGUID]))
      return PDB_LINKAGE_TYPEGUID;
  }
  return PDB_LINKAGE_N;
}

static int extract_guids_from_constraint(graphd_constraint const *con,
                                         int linkage, graph_guid *guid_out,
                                         size_t *n_out) {
  graphd_guid_set const *gs;

  if (con->con_linkcon[linkage].guidcon_include_valid)
    gs = &con->con_linkcon[linkage].guidcon_include;

  else if (con->con_parent != NULL && graphd_linkage_is_my(con->con_linkage) &&
           graphd_linkage_my(con->con_linkage) == linkage &&
           con->con_parent->con_guid.guidcon_include_valid)
    gs = &con->con_parent->con_guid.guidcon_include;
  else
    return GRAPHD_ERR_MORE;

  if (gs->gs_n > *n_out || gs->gs_null) return GRAPHD_ERR_MORE;

  memcpy(guid_out, gs->gs_guid, gs->gs_n * sizeof(*guid_out));
  *n_out = gs->gs_n;

  return 0;
}

static int extract_guids_from_iterator(graphd_request *greq, pdb_iterator *it,
                                       graph_guid *guid_out, size_t *n_out) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_handle *pdb = g->g_pdb;
  pdb_budget budget =
      GRAPHD_MULTIPLE_LINKSTO_MAX * GRAPHD_MULTIPLE_NEXTCOST_MAX;
  int err, e2;
  size_t i;

  err = pdb_iterator_reset(pdb, it);
  if (err != 0) return err;

  for (i = 0; i < *n_out && budget > 0 && err != GRAPHD_ERR_NO; i++) {
    pdb_id id;
    err = pdb_iterator_next(pdb, it, &id, &budget);
    if (err != 0) {
      char buf[200];

      if (err == GRAPHD_ERR_NO || err == GRAPHD_ERR_MORE) break;

      /* Unexpected error.
       */
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next", err, "i=%zu, it=%s",
                   i, pdb_iterator_to_string(pdb, it, buf, sizeof buf));
      return err;
    }

    err = pdb_id_to_guid(pdb, id, guid_out + i);
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) {
        i--;
        continue;
      }

      /* Unexpected error.
       */
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_to_guid", err, "id=%llx",
                   (unsigned long long)id);
      return err;
    }
  }

  e2 = pdb_iterator_reset(pdb, it);
  if (e2 != 0) return e2;

  /*  Desired result: we've exhausted the iterator.
   */
  if (err == GRAPHD_ERR_NO) {
    *n_out = i;
    return 0;
  }

  /*  Possible: we ran over or too long.
   *  There are too many values here.
   */
  return GRAPHD_ERR_MORE;
}

/* One part of our linkage is a fixed GUID.  If we can get a limited
 * set of GUIDs for a matching part of our linkage (typeguid -> L/R;
 * L/R -> typeguid), we can build an OR iterator of a small set.
 */
static int multiple_hint_linkage(graphd_request *greq, graphd_constraint *con,
                                 int linkage, graph_guid *guid_out,
                                 size_t *n_out) {
  graphd_constraint *sub;
  int err;
  cl_handle *cl = graphd_request_cl(greq);
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_handle *pdb = g->g_pdb;

  (void)pdb;

  cl_log(cl, CL_LEVEL_VERBOSE, "multiple_hint_linkage linkage=%s con=%s",
         pdb_linkage_to_string(linkage), graphd_constraint_to_string(con));

  if (linkage == PDB_LINKAGE_TYPEGUID) {
    if ((err = extract_guids_from_constraint(con, PDB_LINKAGE_RIGHT, guid_out,
                                             n_out)) == 0)
      return PDB_LINKAGE_RIGHT;

    if ((err = extract_guids_from_constraint(con, PDB_LINKAGE_LEFT, guid_out,
                                             n_out)) == 0)
      return PDB_LINKAGE_LEFT;

    for (sub = con->con_head; sub != *con->con_tail; sub = sub->con_next) {
      /*  We're only interested in left->() or right->()
       *  subconstraints.
       */
      if (!graphd_linkage_is_i_am(sub->con_linkage) ||
          (graphd_linkage_i_am(sub->con_linkage) != PDB_LINKAGE_RIGHT &&
           graphd_linkage_i_am(sub->con_linkage) != PDB_LINKAGE_LEFT))
        continue;

      /*  We're only interested in mandatory subconstraints
       *  outside of "or"s.
       */
      if (!GRAPHD_CONSTRAINT_IS_MANDATORY(sub) || sub->con_parent != con)
        continue;

      /*  Finally, we need a subconstraint with one or more
       *  fixed values.
       */

      err = 0;
      if (sub->con_it != NULL && pdb_iterator_n_valid(pdb, sub->con_it) &&
          pdb_iterator_n(pdb, sub->con_it) <= *n_out &&
          pdb_iterator_next_cost_valid(pdb, sub->con_it) &&
          pdb_iterator_next_cost(pdb, sub->con_it) <=
              GRAPHD_MULTIPLE_NEXTCOST_MAX &&
          (err = extract_guids_from_iterator(greq, sub->con_it, guid_out,
                                             n_out)) == 0)

        return graphd_linkage_i_am(sub->con_linkage);

      if (err != 0 && err != GRAPHD_ERR_MORE) return PDB_LINKAGE_N;
    }
  } else if (linkage == PDB_LINKAGE_RIGHT || linkage == PDB_LINKAGE_LEFT) {
    if ((err = extract_guids_from_constraint(con, PDB_LINKAGE_TYPEGUID,
                                             guid_out, n_out)) == 0)
      return PDB_LINKAGE_TYPEGUID;

    for (sub = con->con_head; sub != *con->con_tail; sub = sub->con_next) {
      /*  We're only interested in typeguid->()
       *  subconstraints.
       */
      if (!graphd_linkage_is_i_am(sub->con_linkage) ||
          graphd_linkage_i_am(sub->con_linkage) != PDB_LINKAGE_TYPEGUID)
        continue;

      /*  We're only interested in mandatory subconstraints
       *  outside of "or"s.
       */
      if (!GRAPHD_CONSTRAINT_IS_MANDATORY(sub) || sub->con_parent != con)
        continue;

      /*  Finally, we need a subconstraint with one or more
       *  fixed values.
       */
      if (sub->con_it != NULL && pdb_iterator_n_valid(pdb, sub->con_it) &&
          pdb_iterator_n(pdb, sub->con_it) <= *n_out &&
          pdb_iterator_next_cost_valid(pdb, sub->con_it) &&
          pdb_iterator_next_cost(pdb, sub->con_it) <=
              GRAPHD_MULTIPLE_NEXTCOST_MAX &&
          (err = extract_guids_from_iterator(greq, sub->con_it, guid_out,
                                             n_out)) == 0)

        return PDB_LINKAGE_TYPEGUID;

      if (err != 0 && err != GRAPHD_ERR_MORE) return PDB_LINKAGE_N;
    }
  }
  return PDB_LINKAGE_N;
}

/**
 * @brief 	Create an iterator that embodies pointing to something
 *  		else with a right/left/type/scope link.
 *
 * @param greq			Request that supplies allocators
 * @param con			constraint that does the pointing
 * @param linkage		which pointer are we dealing with?
 * @param my_linkage_guid 	Array of up to four single guids that we
 *				*know* are at the end of type/scope/right/left
 *				links (unknown = null guid)
 * @param it_out		assign the iterator to this
 * @param good_iterator_inout	have we seen an efficient, correctly
 *				ordered iterator yet?
 *				When set, prevents the addition
 *				of an "all" iterator.
 * @param vip_inout		Has a VIP iterator been created for this yet?
 *				If yes, we won't need a type iterator.
 */
static int linkage_iterator(graphd_request *greq, graphd_constraint *con,
                            int linkage, pdb_id low, pdb_id high,
                            graphd_direction direction, pdb_iterator **it_out,
                            bool *good_iterator_inout, bool *vip_inout) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_handle *g = graphd_request_graphd(greq);
  graphd_guid_constraint *guidcon;
  pdb_handle *pdb = g->g_pdb;
  pdb_iterator *tmp;
  int hint_linkage = PDB_LINKAGE_N;
  int err;
  char const *ordering = NULL;
  graph_guid multi_guid[GRAPHD_MULTIPLE_LINKSTO_MAX];
  size_t multi_guid_n = sizeof(multi_guid) / sizeof(*multi_guid);

  cl_enter(cl, CL_LEVEL_SPEW, "linkage=%s, constraint=%s %lld..%lld",
           pdb_linkage_to_string(linkage), graphd_constraint_to_string(con),
           (long long)low, (long long)high);

  cl_assert(cl, GRAPHD_DIRECTION_VALID(direction));
  cl_assert(cl, HAS_GUIDS(con->con_linkcon[linkage]));

  *it_out = NULL;

  /*  Special case: just a single GUID at the end of the linkage.
   */
  guidcon = con->con_linkcon + linkage;
  if (HAS_GUID(*guidcon) &&
      !graphd_guid_set_contains_null(
          &con->con_linkcon[linkage].guidcon_include))
    return linkage_single_iterator(greq, con, GUID_POINTER(*guidcon), linkage,
                                   direction, ordering, it_out,
                                   good_iterator_inout, vip_inout);

  /*  Multiple GUIDs.
   */
  if (graphd_guid_set_contains_null(
          &con->con_linkcon[linkage].guidcon_include)) {
    cl_leave(cl, CL_LEVEL_SPEW, "null linked guid - not indexed");
    return 0;
  }

  err = graphd_iterator_fixed_create_guid_array(
      g, con->con_linkcon[linkage].guidcon_include.gs_guid,
      con->con_linkcon[linkage].guidcon_include.gs_n, PDB_ITERATOR_LOW_ANY,
      high, direction != GRAPHD_DIRECTION_BACKWARD, &tmp);
  if (err != 0) {
    cl_leave(cl, CL_LEVEL_SPEW,
             "unexpected error creating fixed GUID array: %s",
             graphd_strerror(err));
    return err;
  }

  cl_assert(cl, direction != GRAPHD_DIRECTION_ORDERING || ordering != NULL);

  hint_linkage = graphd_constraint_iterator_hint_linkage(greq, con, linkage);
  *it_out = NULL;

  if (hint_linkage == PDB_LINKAGE_N &&
      (hint_linkage = multiple_hint_linkage(greq, con, linkage, multi_guid,
                                            &multi_guid_n)) != PDB_LINKAGE_N) {
    err = multiple_linksto_create(greq, con, linkage, hint_linkage, multi_guid,
                                  multi_guid_n, &tmp, low, high, direction,
                                  ordering, it_out);

    /*  This didn't work?  OK, go with the original
     *  un-hinted iterator.
     */
    if (err != 0 || *it_out == NULL) hint_linkage = PDB_LINKAGE_N;
  }
  if (*it_out == NULL) {
    err = graphd_iterator_linksto_create(
        greq, linkage, hint_linkage,
        hint_linkage >= PDB_LINKAGE_N ? NULL : con->con_linkguid + hint_linkage,
        &tmp, low, high, direction, ordering, it_out);
    if (err != 0)
      pdb_iterator_destroy(pdb, &tmp);
    else
      graphd_constraint_account(greq, con, *it_out);
  }

  cl_leave(cl, CL_LEVEL_SPEW, "%s", err ? graphd_strerror(err) : "done");
  return err;
}

static int value_eq_match(graphd_request *greq, graphd_constraint *con,
                          graphd_string_constraint const *strcon,
                          graphd_direction direction, const char *ordering,
                          bool *indexed_inout) {
  pdb_iterator *it;
  pdb_iterator *or_it;
  const graphd_comparator *cmp;
  bool indexed_all = false;

  graphd_handle *g = graphd_request_graphd(greq);
  graphd_string_constraint_element *strcel;
  cl_handle *cl = graphd_request_cl(greq);
  int err;

  pdb_handle *pdb = g->g_pdb;

  int n = 0;

  cmp = con->con_value_comparator;
  cl_assert(cl, strcon != NULL);
  cl_assert(cl, cmp);

  /* value=() .. not very useful */
  if (strcon->strcon_head == NULL) return 0;

  /*
   * Just one value. Throw it directly into the AND
   */
  if (strcon->strcon_head->strcel_next == NULL) {
    strcel = strcon->strcon_head;
    err = cmp->cmp_eq_iterator(greq, strcon->strcon_op, strcel->strcel_s,
                               strcel->strcel_e, con->con_low, con->con_high,
                               direction, ordering, indexed_inout, &it);

    if (err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "cmp_eq_iterator", err,
                   "cannot get = or ~= iterator for comparator %s",
                   graphd_comparator_to_string(cmp));
      return err;
    }
    if (it == NULL) {
      /*
       * This is pretty benign. There's plenty of cases
       * (for example inequalities. Or some globs)
       * where cmp_eq_iterator isn't supposed to generate
       * any output
       */
      cl_log(cl, CL_LEVEL_VERBOSE,
             "value_eq_match: cmp_eq_iterator for %s didn't"
             " generate an iterator for value %.*s",
             graphd_comparator_to_string(cmp),
             (int)(strcel->strcel_e - strcel->strcel_s), strcel->strcel_s);
      return 0;
    }

    err = add_subcondition(greq, con, &it);
    if (err) {
      char buf[200];
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_and_fail", err,
                   "Cannot at %s to constraint and",
                   pdb_iterator_to_string(pdb, it, buf, sizeof buf));

      return err;
    }
    return 0;
  }

  /*
   * Multiple values value=("a" "b" "c");
   * create an or with one iterator for each value and stuff that into
   * the constraint AND.
   */

  for (strcel = strcon->strcon_head; strcel != NULL;
       strcel = strcel->strcel_next)
    n++;

  err = graphd_iterator_or_create(
      greq, n, direction != GRAPHD_DIRECTION_BACKWARD, &or_it);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_create", err,
                 "Cannot create value=(...) or "
                 "iterator with %i subiterators",
                 n);
    return err;
  }

  for (strcel = strcon->strcon_head; strcel != NULL;
       strcel = strcel->strcel_next) {
    bool indexed_this = false;
    cmp->cmp_eq_iterator(greq, strcon->strcon_op, strcel->strcel_s,
                         strcel->strcel_e, con->con_low, con->con_high,
                         direction, ordering, &indexed_this, &it);

    if (err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "cmp_eq_iterator", err,
                   "Cannot create iterator for  value='%.*s' for"
                   " comparator %s",
                   (int)(strcel->strcel_e - strcel->strcel_s), strcel->strcel_s,
                   graphd_comparator_to_string(cmp));
      pdb_iterator_destroy(pdb, &or_it);

      return err;
    }

    if (it == NULL) {
      /*
       * Benign. Inequalities among other things cause this
       */
      cl_log(cl, CL_LEVEL_VERBOSE,
             "value_eq_match: cmp_eq_iterator for "
             "comparator %s did not produce a "
             "subiterator for '%.*s'",
             graphd_comparator_to_string(cmp),
             (int)(strcel->strcel_e - strcel->strcel_s), strcel->strcel_s);

      pdb_iterator_destroy(pdb, &or_it);
      /*
       * For some reason, the comparator can't handle this
       * case.  That's okay: we give up and hope something
       * else can index this.
       */
      return 0;
    }
    indexed_all &= indexed_this;

    graphd_constraint_account(greq, con, it);

    err = graphd_iterator_or_add_subcondition(or_it, &it);
    if (err) {
      char buf[200];
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_add_subcondition",
                   err, "Cannot add iterator %s or OR sc",
                   pdb_iterator_to_string(pdb, it, buf, sizeof buf));

      pdb_iterator_destroy(pdb, &it);
      pdb_iterator_destroy(pdb, &or_it);
      return err;
    }
  }

  *indexed_inout |= indexed_all;
  err = graphd_iterator_or_create_commit(or_it);
  if (err) {
    pdb_iterator_destroy(pdb, &it);
    pdb_iterator_destroy(pdb, &or_it);

    return err;
  }
  err = graphd_iterator_and_add_subcondition(g, con->con_it, &or_it);

  if (err) {
    pdb_iterator_destroy(pdb, &it);
    pdb_iterator_destroy(pdb, &or_it);
    /*
     * Our parent should kill the dead AND for us
     */
    return err;
  }

  return 0;
}

static bool sc_contains_null(cl_handle *cl,
                             graphd_string_constraint_element *strcel) {
  if (strcel == NULL) {
    return true;
  }
  for (; strcel; strcel = strcel->strcel_next) {
    if (strcel->strcel_s == NULL) return true;
  }
  return false;
}

static int graphd_constraint_value_subconditions(graphd_request *greq,
                                                 graphd_constraint *con,
                                                 bool *good_iterator_inout) {
  graphd_string_constraint *strcon;
  graphd_comparator const *cmp = con->con_value_comparator;
  graphd_direction direction;
  char const *ordering;
  graphd_string_constraint_queue *q = &con->con_value;
  graphd_string_constraint const *hi_sc = NULL, *lo_sc = NULL;
  cl_handle *cl = graphd_request_cl(greq);
  int err;
  bool value_sort;
  bool value_forward;
  bool lo_strict = false;
  bool hi_strict = false;
  bool value_ne_null = false;
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_handle *pdb = g->g_pdb;

  direction = graphd_sort_root_iterator_direction(greq, con, &ordering);

  cl_assert(cl, GRAPHD_DIRECTION_VALID(direction));
  cl_assert(cl, con->con_it != NULL);

  /*
   * XXX Do we have to?
   * I'd really like to guarantee that by the time you get here you
   * *always* have a comparator! If the query has sorts or value
   * constraints, the semantic code should be in charge of figuring
   * out what comparator to use.
   */
  if (!cmp) cmp = graphd_comparator_unspecified;

  for (strcon = q->strqueue_head; strcon != NULL;
       strcon = strcon->strcon_next) {
    switch (strcon->strcon_op) {
      case GRAPHD_OP_LT:
        hi_strict = true;
      case GRAPHD_OP_LE:

        if (hi_sc) {
          cl_log(cl, CL_LEVEL_FAIL, "only one upper inequality per con!");
          return GRAPHD_ERR_SEMANTICS;
        }

        if (strcon->strcon_head) hi_sc = strcon;
        break;

      case GRAPHD_OP_GT:
        lo_strict = true;
      case GRAPHD_OP_GE:
        if (lo_sc) {
          cl_log(cl, CL_LEVEL_FAIL, "only one lower equality per con!");
          return GRAPHD_ERR_SEMANTICS;
        }

        if (strcon->strcon_head) lo_sc = strcon;
        break;

      case GRAPHD_OP_NE:
        value_ne_null |= sc_contains_null(cl, strcon->strcon_head);
        break;

      default:
        break;
    }

    err = value_eq_match(greq, con, strcon, direction, ordering,
                         good_iterator_inout);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "cmp->cmp_iterator", err,
                   "comparator=\"%s\"", cmp->cmp_name);
      return err;
    }
  }
  value_sort = false;
  value_forward = true;

  /*
   * Try to insert a vrange iterator if we're sorted by value
   */
  if ((con->con_sort_root.sr_con == NULL || con->con_sort_root.sr_con == con) &&
      (con->con_sort_root.sr_pat.pat_type == GRAPHD_PATTERN_VALUE)) {
    /*
     * If there's no inequality then use the sort comparator.
     * If there is an inequality, only mark it as ordered
     * if we use the same comparator.
     */
    if (hi_sc == NULL && lo_sc == NULL)
      cmp = con->con_sort_root.sr_pat.pat_comparator;

    if (cmp == con->con_sort_root.sr_pat.pat_comparator) {
      cl_assert(cl, cmp);
      value_sort = true;
      direction = GRAPHD_DIRECTION_ORDERING;
      value_forward = con->con_sort_root.sr_pat.pat_sort_forward;
    }
  }

  /*  If there is a range iterator,
   *  and we're either sorted or in a subrange...
   */
  if ((lo_sc != NULL || hi_sc != NULL || value_sort)) {
    pdb_iterator *range_it;
    pdb_iterator *best_sub;
    int best_sub_i;
    const char *hi_s, *lo_s, *hi_e, *lo_e;

    if (value_sort) {
      cl_log(cl, CL_LEVEL_SPEW,
             "graphd_constraint_value_subconditions: "
             "adding a ranged iterator for sorting");
    }

    if (lo_sc) {
      cl_assert(cl, lo_sc->strcon_head->strcel_next == NULL);

      lo_s = lo_sc->strcon_head->strcel_s;
      lo_e = lo_sc->strcon_head->strcel_e;
    } else {
      lo_s = cmp->cmp_lowest_string;
      if (lo_s)
        lo_e = lo_s + strlen(lo_s);
      else
        lo_e = NULL;
    }

    if (hi_sc) {
      cl_assert(cl, hi_sc->strcon_head->strcel_next == NULL);
      hi_s = hi_sc->strcon_head->strcel_s;
      hi_e = hi_sc->strcon_head->strcel_e;
    } else {
      hi_s = cmp->cmp_highest_string;
      if (hi_s)
        hi_e = hi_s + strlen(hi_s);
      else
        hi_e = NULL;
    }

    if (value_ne_null && !hi_s) {
      /*
       * Translate value!=null into value < null
       */
      hi_strict = true;
    }

    /*
     * Take the smallest sorted subiterator from the AND and pass is
     * through to the vrange, if any.  this lets us pre-compute
     * set intersections with the individual bin hmaps while
     * things are still sorted
     */
    err = graphd_iterator_and_cheapest_subiterator(greq, con->con_it, 32 * 1024,
                                                   &best_sub, &best_sub_i);

    if (err) return err;

    if (direction != GRAPHD_DIRECTION_ORDERING) ordering = NULL;

    err = graphd_iterator_vrange_create(
        greq, lo_s, lo_e, lo_strict, hi_s, hi_e, hi_strict, con->con_low,
        con->con_high, value_forward, cmp, ordering, best_sub, &range_it);

    if (err == ENOTSUP) {
      /*
       * vrange_create doesn't know how to index us.
       * give up but don't abort the query
       */
      pdb_iterator_destroy(pdb, &best_sub);
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_constraint_value_subconditions: "
             "graphd_iterator_vrange_create "
             "does not wish to index this constraint.");

      return 0;
    } else if (err) {
      /*
       * something broke. Give up and abort the query
       */
      pdb_iterator_destroy(pdb, &best_sub);
      cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_iterator_vrange_create", err,
                   "Can't make vrange iterator for comparator %s",
                   graphd_comparator_to_string(cmp));
      return err;
    }

    graphd_constraint_account(greq, con, range_it);

    err = graphd_iterator_and_add_subcondition(g, con->con_it, &range_it);

    if (err) {
      pdb_iterator_destroy(pdb, &range_it);
      cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_iterator_and_add_subcondition",
                   err, "can't add vrange to sc and!");
      return err;
    }
    *good_iterator_inout = true;
  }
  return 0;
}

static int or_of_clones(graphd_request *greq, graphd_constraint *con,
                        graphd_direction direction, pdb_iterator *a_it,
                        pdb_iterator *b_it, pdb_iterator **or_out) {
  graphd_handle *const g = graphd_request_graphd(greq);
  cl_handle *const cl = graphd_request_cl(greq);
  pdb_handle *const pdb = g->g_pdb;

  pdb_iterator *or_it = NULL, *sub_it = NULL;
  int err;
  char buf[200];

  /*  Make an "or".
   */
  err = graphd_iterator_or_create(
      greq, 2, direction != GRAPHD_DIRECTION_BACKWARD, &or_it);
  if (err != 0) goto error;

  /*  Clone both ingredients.
   */
  err = pdb_iterator_clone(pdb, a_it, &sub_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "it=%s",
                 pdb_iterator_to_string(pdb, a_it, buf, sizeof buf));
    goto error;
  }

  graphd_constraint_account(greq, con, sub_it);
  err = graphd_iterator_or_add_subcondition(or_it, &sub_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_add_subcondition", err,
                 "or=%s", pdb_iterator_to_string(pdb, or_it, buf, sizeof buf));
    goto error;
  }
  pdb_iterator_destroy(pdb, &sub_it);

  err = pdb_iterator_clone(pdb, b_it, &sub_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_clone", err, "it=%s",
                 pdb_iterator_to_string(pdb, b_it, buf, sizeof buf));
    goto error;
  }
  graphd_constraint_account(greq, con, sub_it);
  err = graphd_iterator_or_add_subcondition(or_it, &sub_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_add_subcondition", err,
                 "or=%s", pdb_iterator_to_string(pdb, or_it, buf, sizeof buf));
    goto error;
  }
  pdb_iterator_destroy(pdb, &sub_it);

  /*  Commit the "or".
   */
  err = graphd_iterator_or_create_commit(or_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_create_commit", err,
                 "it=%s", pdb_iterator_to_string(pdb, or_it, buf, sizeof buf));
    goto error;
  }
  *or_out = or_it;
  graphd_constraint_account(greq, con, *or_out);
  return 0;

error:
  if (or_it != NULL) pdb_iterator_destroy(pdb, &or_it);
  if (sub_it != NULL) pdb_iterator_destroy(pdb, &sub_it);

  return err;
}

/**
 * @brief Assign to con-&gt;con_it a pdb_iterator that will produce
 *	good candidates for matching con.
 *
 *  In the course of this, subconstraints are also annoated with their
 *  own iterators - unless con is always false.
 *
 *  This happens once in a constraint tree's lifetime.
 *
 * @param greq	- request for which this calculation is being performed
 * @param con 	- constraint we'd like to annotate.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
static int finish(graphd_request *greq, graphd_constraint *con) {
  graphd_handle *const g = graphd_request_graphd(greq);
  cl_handle *const cl = graphd_request_cl(greq);
  pdb_handle *const pdb = g->g_pdb;

  graphd_constraint_or *cor;
  graphd_constraint *sub;
  int err = 0, linkage;
  pdb_iterator *sub_it = NULL, *or_it = NULL;
  graphd_direction direction;
  char buf[200];
  bool good_iterator = false;
  bool have_vip = false;
  char const *ordering = NULL;
  graph_guid multi_guid[GRAPHD_MULTIPLE_LINKSTO_MAX];
  size_t multi_guid_n = sizeof(multi_guid) / sizeof(*multi_guid);

  cl_enter(cl, CL_LEVEL_VERBOSE, "(request %llu, con=%s, it=%p)",
           (unsigned long long)greq->greq_req.req_id,
           graphd_constraint_to_string(con), (void *)con->con_it);

  if (con->con_it != NULL) {
    graphd_constraint_account(greq, con, con->con_it);
    goto post_reduce;
  }

  /*  Is this constraint impossible?
   */
  if (con->con_false ||
      (con->con_parent != NULL && con->con_parent->con_false)) {
  null_iterator:
    err = set_empty(greq, con);
    goto post_reduce;
  }

  /*  Create an "AND" iterator for this constraint.
   */
  direction = graphd_sort_root_iterator_direction(greq, con, &ordering);
  cl_assert(cl, direction != GRAPHD_DIRECTION_ORDERING || ordering != NULL);

  err = graphd_iterator_and_create(greq,
                                   /* wild guess at # of subiterators, */ 3,
                                   con->con_low, con->con_high, direction,
                                   ordering, &con->con_it);
  if (err != 0) goto error;

  graphd_constraint_account(greq, con, con->con_it);

  /*  Tell the AND iterator the page size, if we have one.
   */
  if (con->con_resultpagesize_valid) {
    long page_limit = con->con_resultpagesize;

    /*  If we're counting, we'll pull out up to
     *  <countlimit> items - the page limit doesn't
     *  help us.
     */
    if (graphd_constraint_uses_pattern(con, GRAPHD_PATTERN_COUNT)) {
      if (!con->con_countlimit_valid)
        page_limit = -1;
      else if (con->con_countlimit > page_limit)
        page_limit = con->con_countlimit;
    }
    if (page_limit >= 0)
      graphd_iterator_and_set_context_pagesize(g, con->con_it, page_limit);
  }

  /*  On top of this frame, fill in the details of the AND iterator.
   *
   *  1 GUIDs we know.
   */
  if (HAS_GUIDS(con->con_guid)) {
    err = graphd_iterator_fixed_create_guid_array(
        g, con->con_guid.guidcon_include.gs_guid,
        con->con_guid.guidcon_include.gs_n, con->con_it->it_low,
        con->con_it->it_high, direction != GRAPHD_DIRECTION_BACKWARD, &sub_it);
    if (err != 0) goto error;

    err = add_subcondition(greq, con, &sub_it);
    pdb_iterator_destroy(pdb, &sub_it);
    if (err != 0) goto error;

    good_iterator = true;
  }

  /*
   * 2 "first" level constraints like left/right/type/scope=
   *
   * Do this early so that we can take advantage of any small
   * sets those produce in value ranges/sorting
   */

  for (linkage = PDB_LINKAGE_TYPEGUID + 1;
       linkage < PDB_LINKAGE_TYPEGUID + 1 + PDB_LINKAGE_N; linkage++) {
    if (have_vip && linkage == PDB_LINKAGE_TYPEGUID) continue;

    if (!GRAPH_GUID_IS_NULL(con->con_linkguid[linkage % PDB_LINKAGE_N])) {
      err = linkage_single_iterator(
          greq, con, con->con_linkguid + linkage % PDB_LINKAGE_N,
          linkage % PDB_LINKAGE_N, direction, ordering, &sub_it, &good_iterator,
          &have_vip);
    } else {
      if (!HAS_GUIDS(con->con_linkcon[linkage % PDB_LINKAGE_N])) continue;

      err = linkage_iterator(greq, con, linkage % PDB_LINKAGE_N,
                             con->con_it->it_low, con->con_it->it_high,
                             direction, &sub_it, &good_iterator, &have_vip);
    }

    if (err == GRAPHD_ERR_ALREADY) {
      err = 0;
      continue;
    }
    if (err != 0) goto error;
    if (sub_it == NULL) continue;

    err = add_subcondition(greq, con, &sub_it);
    if (err != 0) goto error;
  }

  /*  3 Intrinsics.
   */
  err = graphd_constraint_value_subconditions(greq, con, &good_iterator);
  if (err != 0) goto error;

  if (con->con_name.strqueue_head != NULL) {
    err = graphd_comparator_default_name_iterator(
        greq, con->con_name.strqueue_head, con->con_it, con->con_it->it_low,
        con->con_it->it_high, direction, ordering, &good_iterator);
    if (err != 0) goto error;
  }

  if (pdb_iterator_null_is_instance(pdb, con->con_it)) {
    con->con_false = true;
    goto post_reduce;
  }

  /*  4 Mandatory subconstraints
   */
  for (sub = con->con_head; sub != *con->con_tail; sub = sub->con_next) {
    pdb_iterator *red;

    /*  If the subconstraint is part of an "or" branch,
     *  its parent points to the branch, not to <con>.
     *  We use that to make sure we skip the branch.
     */
    if (!GRAPHD_CONSTRAINT_IS_MANDATORY(sub) || sub->con_parent != con)
      continue;

    if (sub->con_false) {
      cl_log(cl, CL_LEVEL_DEBUG,
             "FALSE [%s:%d] inherited from non-optional "
             "subconstraint",
             __FILE__, __LINE__);
      con->con_false = true;
      goto null_iterator;
    }

    if (graphd_linkage_is_i_am(sub->con_linkage) &&
        !GRAPH_GUID_IS_NULL(
            con->con_linkguid[graphd_linkage_i_am(sub->con_linkage)]))

      /* Already taken care of. */
      continue;

    if (!sub->con_it) {
      err = finish(greq, sub);
      if (err != 0) goto error;
    }
    PDB_IS_ITERATOR(cl, sub->con_it);

    err = pdb_iterator_clone(pdb, sub->con_it, &red);
    if (err != 0) goto error;

    PDB_IS_ITERATOR(cl, sub->con_it);
    PDB_IS_ITERATOR(cl, red);

    if (graphd_linkage_is_my(sub->con_linkage)) {
      /* The subconstraint points to the parent.
       */
      err = graphd_iterator_isa_create(
          greq, graphd_linkage_my(sub->con_linkage), &red, con->con_low,
          con->con_high, direction, ordering,
          sub->con_cursor_usable ? GRAPHD_ITERATOR_ISA_HINT_CURSOR : 0,
          &sub_it);

      pdb_iterator_destroy(pdb, &red);
      if (err != 0) goto error;
    } else {
      int hint_linkage = PDB_LINKAGE_N;

      /* The parent "con" points to the subconstraint.
       */
      cl_assert(cl, graphd_linkage_is_i_am(sub->con_linkage));

      cl_assert(cl, direction != GRAPHD_DIRECTION_ORDERING || ordering != NULL);
      hint_linkage = graphd_constraint_iterator_hint_linkage(
          greq, con, graphd_linkage_i_am(sub->con_linkage));

      if (hint_linkage == PDB_LINKAGE_N &&
          (hint_linkage = multiple_hint_linkage(
               greq, con, graphd_linkage_i_am(sub->con_linkage), multi_guid,
               &multi_guid_n)) != PDB_LINKAGE_N) {
        err = multiple_linksto_create(
            greq, con, graphd_linkage_i_am(sub->con_linkage), hint_linkage,
            multi_guid, multi_guid_n, &red, con->con_it->it_low,
            con->con_it->it_high, direction, ordering, &sub_it);

        /*  This didn't work?  OK, go with the original
         *  un-hinted iterator.
         */
        if (err != 0 || sub_it == NULL) hint_linkage = PDB_LINKAGE_N;
      }

      if (sub_it == NULL)
        err = graphd_iterator_linksto_create(
            greq, graphd_linkage_i_am(sub->con_linkage), hint_linkage,
            hint_linkage >= PDB_LINKAGE_N ? NULL
                                          : con->con_linkguid + hint_linkage,
            &red, con->con_it->it_low, con->con_it->it_high, direction,
            ordering, &sub_it);
      pdb_iterator_destroy(pdb, &red);
      if (err != 0) goto error;
    }

    err = add_subcondition(greq, con, &sub_it);
    if (err != 0) goto error;

    if (pdb_iterator_null_is_instance(pdb, con->con_it)) goto done;
  }

  /*  5 "OR" branches.  If we have iterators for either branch
   *  	of an or, we can add their "OR" to the big iterator
   *      as an "AND" .
   */
  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    if (cor->or_tail == NULL) continue;

    if (cor->or_head.con_it == NULL) {
      err = finish(greq, &cor->or_head);
      if (err != 0) goto error;

      cl_assert(cl, cor->or_head.con_it != NULL);
    }
    if (cor->or_tail->con_it == NULL) {
      err = finish(greq, cor->or_tail);
      if (err != 0) goto error;

      cl_assert(cl, cor->or_tail->con_it != NULL);
    }

    /*  Make an "or" of these two.
     */
    err = or_of_clones(greq, con, direction, cor->or_head.con_it,
                       cor->or_tail->con_it, &or_it);
    if (err != 0) goto error;

    /*  Add that iterator to the "and" we're building
     */
    err = add_subcondition(greq, con, &or_it);
    if (err != 0) goto error;

    if (pdb_iterator_null_is_instance(pdb, con->con_it)) goto done;
  }

  /*  6 Everything
   *
   *   	Unless we managed to pick up a Good Iterator(TM),
   *  	add an "everything" iterator.
   *
   *	Adding this ensures that if our other sub- and constraint
   * 	producers are idiotically inconsistent, we always have a
   *	more efficient path to fall back on.
   */
  if (!good_iterator) {
    err = pdb_iterator_all_create(
        pdb, con->con_it->it_low, con->con_it->it_high,
        direction != GRAPHD_DIRECTION_BACKWARD, &sub_it);
    if (err != 0) goto error;

    graphd_iterator_set_direction_ordering(pdb, sub_it, direction, ordering);

    err = add_subcondition(greq, con, &sub_it);
    if (err != 0) goto error;
  }

done:
  if (err == 0) {
    pdb_id id;

    /*  While adding subiterators, the "and" iterator on
     *  con->con_it may have turned into NULL - but
     *  graphd_iterator_and_create_commit() handles that
     *  gracefully.
     */
    err = graphd_iterator_and_create_commit(g, con->con_it);
    if (err != 0) goto error;

    err = pdb_iterator_single_id(pdb, con->con_it, &id);
    if (err == 0) {
      graph_guid guid;

      err = pdb_id_to_guid(pdb, id, &guid);
      if (err != 0) {
        err = set_empty(greq, con);
      } else
        err = set_guid_consequences(greq, con, &guid);
    } else if (err == PDB_ERR_NO)
      err = set_empty(greq, con);

    else if (err == PDB_ERR_MORE || err == PDB_ERR_TOO_MANY)
      err = 0;
    else {
      cl_log_errno(g->g_cl, CL_LEVEL_FAIL, "pdb_iterator_single_id", err,
                   "for it=%s",
                   pdb_iterator_to_string(pdb, con->con_it, buf, sizeof buf));
    }
    cl_assert(cl, err || con->con_it->it_type != NULL);
  }

post_reduce:

  /*  Annotate all subconstraints with iterators,
   *  even the optional ones; but not the "or" subconstraints --
   *  those are integrated by now.
   */
  if (err == 0) {
    graphd_constraint_setsize(g, con);

    for (sub = con->con_head; sub != *con->con_tail; sub = sub->con_next) {
      if ((err = finish(greq, sub)) != 0) {
        cl_leave(cl, CL_LEVEL_SPEW, "error in recursion: %s", strerror(err));
        return err;
      }
    }
  }

  if (err != 0) {
  error:
    pdb_iterator_destroy(pdb, &sub_it);
    pdb_iterator_destroy(pdb, &con->con_it);
  }

  cl_leave(cl, CL_LEVEL_SPEW, "con=%p, con_it=%s@%p", (void *)con,
           err ? graphd_strerror(err)
               : pdb_iterator_to_string(pdb, con->con_it, buf, sizeof buf),
           (void *)con->con_it);
  return err;
}

int graphd_constraint_iterator(graphd_request *greq, graphd_constraint *con) {
  int err;

  if ((err = initialize(greq, con)) != 0) return err;

  err = cheap(greq, con);
  if (err != 0) return err;

  err = finish(greq, con);
  if (err != 0) return err;

  /*  Connect the topmost iterator of the tree
   *  to the heatmap accounting.
   */
  if (con->con_it != NULL) graphd_constraint_account(greq, con, con->con_it);

  return 0;
}

/* If this request requires constraint accounting, connect the iterator
 * to the constraint account.
 */
void graphd_constraint_account(graphd_request *greq, graphd_constraint *con,
                               pdb_iterator *it) {
  if (it != NULL && greq->greq_heatmap &&
      pdb_iterator_account(graphd_request_graphd(greq)->g_pdb, it) == NULL)

    pdb_iterator_account_set(graphd_request_graphd(greq)->g_pdb, it,
                             &con->con_iterator_account);
}
