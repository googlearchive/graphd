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

/*  Is the caller asking for a fixed count that we already have
*   in our indices?
 *
 *  If so, just return the count now -- don't actually do the
 *  work of walking the primitives.
 *
 *  This allows graphd applications who know what they're doing
 *  to get database-level metrics without incurring the penalty
 *  of iterating over primitives one by one.
 */
int graphd_read_set_count_fast(graphd_read_set_context* grsc,
                               unsigned long long* count_out) {
  graphd_constraint* const con = grsc->grsc_con;
  graphd_request* const greq = grsc->grsc_base->grb_greq;
  cl_handle* const cl = graphd_request_cl(greq);
  graphd_handle* const g = graphd_request_graphd(greq);
  int linkage;
  int n_approaches = 0;
  char buf[200];

  /*  Caller checked all these.
   */
  cl_assert(cl, !con->con_newest.gencon_valid);
  cl_assert(cl, !con->con_oldest.gencon_valid);
  cl_assert(cl, con->con_subcon_n == 0);
  cl_assert(cl, con->con_live == GRAPHD_FLAG_DONTCARE);
  cl_assert(cl, con->con_archival == GRAPHD_FLAG_DONTCARE);
  cl_assert(cl, con->con_valuetype == GRAPH_DATA_UNSPECIFIED);
  cl_assert(cl, con->con_cursor_s == NULL);
  cl_assert(cl, con->con_guid.guidcon_include.gs_n == 0);

  cl_assert(cl, grsc->grsc_it != NULL);

  /*  Constraints as evaluated by the optimizer were too complicated?
   */
  if (!pdb_iterator_n_valid(g->g_pdb, grsc->grsc_it)) return PDB_ERR_MORE;

  /* Good: we have a count from the iterator.
   *  	But is that actually the final count?  Or are
   *  	there little extra constraints that *do* require
   *  	primitive-by-primitive testing?
   */
  *count_out = pdb_iterator_n(graphd->g_pdb, grsc->grsc_it);

  /*  If we know we've got a VIP iterator, we can stomach two
   *  constraints -- typeguid and an endpoint.
   *  Otherwise, we can stomach one.
   */
  if (graphd_iterator_vip_is_instance(g->g_pdb, grsc->grsc_it))
    n_approaches = -1;
  else
    n_approaches = 0;

  /*  Count things that we *know* are single-index constraints
   *  in the expression.  (I.e., "approaches".)  If we only find
   *  0 or 1 of them, the fast-count works - we know the optimizer
   *  didn't do any worse than the obvious case.
   *  If we find more than 1, it's too complicated, and we give up.
   */

  /*  Case: we know the parent, and the parent is at one
   *  	end of a linkage relationship.
   */
  if (grsc->grsc_parent_id != PDB_ID_NONE &&
      graphd_linkage_is_my(con->con_linkage))
    if (n_approaches++ > 0) {
      return PDB_ERR_MORE;
    }

  /*  Case: we have one specified linkage relationship.
   */
  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++)
    if (con->con_linkcon[linkage].guidcon_include_valid &&
        con->con_linkcon[linkage].guidcon_include.gs_n == 1 &&
        !GRAPH_GUID_IS_NULL(
            con->con_linkcon[linkage].guidcon_include.gs_guid[0])) {
      if (n_approaches++ > 0) {
        return PDB_ERR_MORE;
      }
    } else if (con->con_linkcon[linkage].guidcon_include_valid ||
               con->con_linkcon[linkage].guidcon_exclude_valid ||
               con->con_linkcon[linkage].guidcon_match_valid)
      return PDB_ERR_MORE;

  /*  Case: we have one specific value.  The value match is exact.
   */
  if (con->con_value.strqueue_head != NULL) {
    graphd_string_constraint* str = con->con_value.strqueue_head;

    if (str->strcon_op != GRAPHD_OP_EQ || str->strcon_next != NULL) {
      return PDB_ERR_MORE;
    }
    if (n_approaches++ > 0) {
      return PDB_ERR_MORE;
    }
  }

  /*  Case: we have one specific name.  The value match is exact.
   */
  if (con->con_name.strqueue_head != NULL) {
    graphd_string_constraint* str = con->con_name.strqueue_head;

    if (str->strcon_op != GRAPHD_OP_EQ || str->strcon_next != NULL) {
      return PDB_ERR_MORE;
    }

    if (n_approaches++ > 0) {
      return PDB_ERR_MORE;
    }
  }

  cl_log(cl, CL_LEVEL_DEBUG,
         "fast_count: "
         "getting a fast count of %llu for %s from %s",
         *count_out, graphd_constraint_to_string(con),
         pdb_iterator_to_string(g->g_pdb, grsc->grsc_it, buf, sizeof buf));
  return 0;
}

void graphd_read_set_count_get_atom(graphd_read_set_context* grsc,
                                    graphd_value* val) {
  unsigned long long c;
  graphd_constraint const* con = grsc->grsc_con;
  graphd_request* const greq = grsc->grsc_base->grb_greq;
  cl_handle* const cl = graphd_request_cl(greq);

  cl_log(cl, CL_LEVEL_DEBUG,
         "graphd_read_set_count_get_atom: "
         "count %ld, total %ld, start %ld, pagesize %ld",
         (long)grsc->grsc_count, (long)grsc->grsc_count_total,
         (long)con->con_pagesize, (long)con->con_start);

  c = (grsc->grsc_count_total != (unsigned long long)-1)
          ? grsc->grsc_count_total
          : grsc->grsc_count + con->con_cursor_offset;

  /*  Cap the reported count at countlimit - even
   *  if we know the true total.
   */
  if (con->con_countlimit_valid && c > con->con_countlimit) {
    c = con->con_countlimit;
  }

  graphd_value_number_set(val, c);
}
