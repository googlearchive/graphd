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

/*  Cursors
 */

/**
 * @brief Assign a cursor to a value.
 *
 * @param grsc  read set context
 * @param val	the value for this result
 *
 * @return 0 on success
 * @return nonzero error codes on error.
 */
int graphd_read_set_cursor_get_value(graphd_read_set_context* grsc,
                                     graphd_value* val) {
  graphd_request* greq = grsc->grsc_base->grb_greq;
  graphd_handle* g = graphd_request_graphd(greq);
  char prefix[200];

  snprintf(prefix, sizeof prefix, "[o:%llu]",
           (unsigned long long)(grsc->grsc_con->con_cursor_offset +
                                (grsc->grsc_sort
                                     ? graphd_sort_n_results(grsc->grsc_sort)
                                     : grsc->grsc_count)));

  /*  If the query has no native dateline constraint, store
   *  the current one in the cursor.
   */
  if (greq->greq_asof == NULL) {
    size_t n = strlen(prefix);
    snprintf(
        prefix + n, sizeof(prefix) - n, "[n:%llu]",
        (grsc->grsc_con && grsc->grsc_con->con_high != PDB_ITERATOR_HIGH_ANY)
            ? grsc->grsc_con->con_high
            : pdb_primitive_n(g->g_pdb));
  }

  return grsc->grsc_sort
             ? graphd_sort_cursor_get(grsc->grsc_sort, prefix, val)
             : graphd_constraint_cursor_from_iterator(
                   greq, grsc->grsc_con, prefix, grsc->grsc_it, val);
}

/**
 * @brief If the result instructions involve a cursor, assign one.
 *
 *  The cursor is assigned to the spot in the preallocated
 *  result corresponding to the "cursor" result=() parameter.
 *
 *  This is called from code that detects the completion of
 *  a page size.  It differs from result_complete_atom()
 *  in that it only addresses cursors, nothing else.
 *
 *  result_complete_atom() happens at the end of the traversal.
 *  graphd_read_set_complete_cursor() happens once the specified
 *  pagesize has been filled.
 *
 *  If we actually run out of elements, we'll later override the
 *  cursors we're setting right now with empty ones.
 *
 * @param grsc  read set context
 * @param pf	the pattern frame for this result
 * @param val	the value for this result
 *
 * @return 0 on success (including no cursor instruction)
 * @return nonzero error codes on error.
 */
int graphd_read_set_cursor_get(graphd_read_set_context* grsc,
                               graphd_pattern_frame const* pf,
                               graphd_value* val) {
  graphd_request* greq = grsc->grsc_base->grb_greq;
  cl_handle* cl = graphd_request_cl(greq);
  int err = 0;
  graphd_pattern const* pat;
  graphd_pattern const* ric;
  graphd_value* valc;

  /* The default result instructions don't include a cursor. */

  if (pf == NULL || pf->pf_set == NULL) return 0;

  pat = pf->pf_set;
  if (pat->pat_type == GRAPHD_PATTERN_CURSOR &&
      val->val_type == GRAPHD_VALUE_UNSPECIFIED)
    return graphd_read_set_cursor_get_value(grsc, val);

  else if (pat->pat_type != GRAPHD_PATTERN_LIST)
    return 0;

  for (ric = pat->pat_list_head, valc = val->val_list_contents; ric != NULL;
       ric = ric->pat_next, valc++) {
    cl_assert(cl, valc < val->val_list_contents + val->val_list_n);

    if (ric->pat_type == GRAPHD_PATTERN_CURSOR &&
        valc->val_type == GRAPHD_VALUE_UNSPECIFIED) {
      err = graphd_read_set_cursor_get_value(grsc, valc);
      if (err) return err;
    }
  }
  return 0;
}

/**
 * @brief Clear all cursors.
 *
 *  When we hit our pagesize, we assigned cursors.   But now
 *  we've gone over, and noticed that there isn't actually anything
 *  else in the pipeline - so, let's throw away those cursors.
 *
 * @param grsc read context
 * @param res assign to this result's frame
 */
void graphd_read_set_cursor_clear(graphd_read_set_context* grsc,
                                  graphd_pattern_frame const* pf,
                                  graphd_value* val) {
  graphd_request* greq = grsc->grsc_base->grb_greq;
  cl_handle* cl = graphd_request_cl(greq);
  graphd_pattern const* pat;
  graphd_pattern const* ric;
  graphd_value* valc;
  static char const null_atom[] = "null:";

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_read_result_clear_cursor");

  if (pf == NULL || pf->pf_set == NULL) return;

  if (pf->pf_set->pat_type == GRAPHD_PATTERN_CURSOR) {
    graphd_value_finish(cl, val);
    graphd_value_text_set(val, GRAPHD_VALUE_STRING, null_atom,
                          null_atom + sizeof null_atom - 1, NULL);
    return;
  } else if (pf->pf_set->pat_type != GRAPHD_PATTERN_LIST)
    return;

  cl_assert(cl, val->val_type == GRAPHD_VALUE_LIST);
  cl_assert(cl, val->val_list_n == pf->pf_set->pat_list_n);

  pat = pf->pf_set;
  for (ric = pat->pat_list_head, valc = val->val_list_contents; ric != NULL;
       ric = ric->pat_next, valc++) {
    cl_assert(cl, valc < val->val_list_contents + val->val_list_n);

    if (ric->pat_type == GRAPHD_PATTERN_CURSOR) {
      graphd_value_finish(cl, valc);
      graphd_value_text_set(valc, GRAPHD_VALUE_STRING, null_atom,
                            null_atom + sizeof null_atom - 1, NULL);
    }
  }
}
