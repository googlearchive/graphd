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
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#define GRAPHD_MAX_PAGE_SIZE (64 * 1024)
#define GRAPHD_DEFAULT_PAGE_SIZE (1024)

#define GRAPHD_SORT_CURSOR_PREFIX "sort:"

#define GSC_PER_ID_SEQUENCE(gsc, i)                    \
  ((gsc)->gsc_con->con_pframe[i].pf_one                \
       ? ((gsc)->gsc_result[i].val_list_contents +     \
          (gsc)->gsc_con->con_pframe[i].pf_one_offset) \
       : NULL)

#define IS_LIT(s, e, lit)                     \
  ((s) != NULL && e - s == sizeof(lit) - 1 && \
   !strncasecmp(s, lit, sizeof(lit) - 1))

/*  Incremental sorter
 *
 *  There is a virtual page size P (gsc->gsc_pagesize), P >= 1.
 *  We keep P*2 candidates around in an array A (gsc->gsc_order_to_location):
 *
 *  A [0 ..................... P-1][P ...................... 2P-1]
 *    |<----------sorted--------->||<- unsorted, but <= A[P-1] ->|
 *
 *  The sort algorithm proceeds as follows:
 *
 *   Seeding:
 *  	1. Gather up to 2*P elements.
 * 	2. Sort them.
 *      3. Throw out elements [P..2P-1]
 *
 *   Loop:
 *	4. Gather Q more elements into A that are < A[P-1], with Q <= P.
 *      5. Sort A[P..P+Q-1]
 *	6. Merge A[0..P-1] and [P..P+Q-1] until you have P sorted
 *	   first elements in A'[0..P-1]: O old ones from A[0..P-1]
 *	   and N new ones from A[P..P+Q-1].
 *	7. Throw out the P-O old ones and the Q-N new ones that
 *	   didn't make it into A'[0...P].
 *	8. A := A'; continue with step 4.
 *
 *   Discussion:
 *	The algorithm doesn't have to keep all the candiates in
 *	storage, just two pages at a time.
 *
 *	The more iterations of this run, the more often the
 *	cut-off value [P...2P-1] causes an item to be thrown away
 *	outright, possibly (when sorting by primitive attributes)
 *	even before computing contents for an item.  (In this regard,
 *	each new iteration on average covers twice as much ground
 *	as the previous one.)
 *
 * 	The cut-off value stays the same until P new candidates
 *	have been found, making some sort of precompilation for
 * 	efficient comparisons a possibility.
 *
 *	The merge has O(P), but happens half as often with each
 * 	iteration of loop 4..8, as A[P-1] gets better and better.
 */

struct graphd_sort_context {
  graphd_handle *gsc_graphd;
  cm_handle *gsc_cm;
  cl_handle *gsc_cl;
  graphd_request *gsc_greq;

  /*  Map the sort order to the location of the individual element in
   *  the gsc_result[...] sequences.
   *
   *  Until graphd_sort_finish is called, the first element in
   *  sort-order is stored at offset gsc_order_to_location[0]
   *  from the beginning of the sequences in the result and
   *  variable assignments.  (Offset counted in objects, not bytes.)
   */
  unsigned long *gsc_order_to_location;

  /*  # of occupied slots in gsc->gsc_order_to_location.
   *  At most 2 * gsc->gsc_pagesize.
   */
  size_t gsc_n;

  /*  Page size.  Must be >= 1, <= offset+GRAPHD_MAX_PAGE_SIZE.
   *
   *  If you're specifying an offset and a pagesize at the server
   *  protocol interface, gsc_pagesize will be offset+pagesize.
   */
  size_t gsc_pagesize;

  /*  Flag; set after we see the first value that's too large
   *  to store.  If we're asked for a cursor, and we have no trailing
   *  data, we can safely return null.
   */
  unsigned int gsc_have_trailing : 1;

  /*  Flag; set once we move into the "loop" phase (4..8) of the
   *  sorting algorithm.
   */
  unsigned int gsc_have_median : 1;

  /*  Flag; set once graphd_sort_finish() has been called.
   *  	Used for assertions only.
   */
  unsigned int gsc_finished : 1;

  /*  Flag; if set, the most recent candidate passed to
   *  precompare already sorted as within the set, and there's
   *  no need to compare again.
   */
  unsigned int gsc_blind_accept : 1;

  /**
   * @brief Flag; set as soon as we've started sorting.
   *  	Used for assertions only.
   */
  unsigned int gsc_started : 1;

  /**
   * @brief Flag; if set, precompare rejects any candidate.
   */
  unsigned int gsc_ended : 1;

  /*  Result parameters of the calling read context.  That's
   *  one for the overall result, and one each for each variable
   *  that's being assigned to.
   *
   *  These are weak pointers into a grsc context.
   */
  graphd_value *gsc_result;
  size_t gsc_result_n;

  /*  Constraint that contains the sort=... clause we're executing.
   */
  graphd_constraint *gsc_con;

  graphd_value *gsc_cursor_grid;
  size_t gsc_cursor_grid_width;
  size_t gsc_cursor_grid_n;
};

static graphd_sort_context *graphd_sort_qsort_context;

static graphd_pattern const *sort_instructions(graphd_sort_context const *gsc) {
  graphd_pattern const *pat;

  if (!gsc->gsc_con->con_sort_valid) return NULL;

  pat = gsc->gsc_con->con_sort;
  if (pat != NULL && pat->pat_type == GRAPHD_PATTERN_LIST)
    pat = pat->pat_list_head;
  return pat;
}

/**
 * @brief Return the graphd_value corresponding to pat in nth position.
 *
 * @param gsc	A sort context, created with graphd_sort_create()
 * @param pat 	The pattern for this value
 * @param nth	-1 or the position we're interested in; 0 is the first.
 *		-1 addresses the corresponding entry in the cursor,
 *		if there was a cursor.
 *
 * @return NULL on (programmer) error,
 * 	otherwise a pointer to the requested value.
 *
 */
graphd_value *graphd_sort_value(graphd_sort_context *gsc,
                                graphd_pattern const *pat, long nth) {
  cl_handle *cl = gsc->gsc_cl;
  graphd_value *val;
  short element_offset;

  cl_log(cl, CL_LEVEL_SPEW,
         "graphd_sort_value: pat %p, nth %ld, result_offset %zu", (void *)pat,
         nth, pat->pat_result_offset);

  cl_assert(cl, pat->pat_type != GRAPHD_PATTERN_LIST);
  cl_assert(cl, pat->pat_result_offset < gsc->gsc_result_n);

  if (nth < 0) {
    cl_assert(cl, nth == -1);
    cl_assert(cl, gsc->gsc_cursor_grid != NULL);

    if (gsc->gsc_cursor_grid == NULL) return NULL;

    return gsc->gsc_cursor_grid +
           pat->pat_result_offset * gsc->gsc_cursor_grid_width +
           pat->pat_element_offset;
  }

  val = GSC_PER_ID_SEQUENCE(gsc, pat->pat_result_offset);
  cl_assert(cl, val != NULL);
  cl_assert(cl, val->val_type == GRAPHD_VALUE_SEQUENCE);

  if (nth >= val->val_sequence_n) {
    cl_log(cl, CL_LEVEL_ERROR,
           "graphd_sort_value: looking for #%lu in a sequence of %lu",
           (unsigned long)nth, (unsigned long)val->val_sequence_n);
    return NULL;
  }

  cl_assert(cl, nth < val->val_sequence_n);

  val = val->val_sequence_contents + nth;
  cl_assert(cl, val->val_type == GRAPHD_VALUE_LIST);

  element_offset = pat->pat_element_offset;

  if (element_offset < 0 || element_offset >= val->val_list_n) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "graphd_sort_value: element offset %hu out of "
           "range 0..%zu",
           element_offset, val->val_list_n);

    return NULL;
  }

  /*
  { char buf[200];
    cl_log(cl, CL_LEVEL_SPEW, "graphd_sort_value [%d]: %s",
          (int)nth, graphd_value_to_string(
            val->val_sequence_contents + element_offset, buf, sizeof buf));
  }
  */

  return val->val_list_contents + element_offset;
}

/*  Return a negative value if pr < val, positive if pr > 0 val,
 *  zero if they're equal.
 */
static int sort_precompare_pr_val_valuetype(cl_handle *cl,
                                            pdb_primitive const *pr,
                                            graphd_value const *val) {
  /*  Three-state comparators are stories about A and B,
   *  where a value less than, greater than, or equal to 0
   *  is returend depending on whether A is less than,
   *  equal to, or greater than B.
   *
   *  In our case, <pr> is the A, and <val> is the B.
   */
  int A, B;

  cl_assert(cl, val != NULL);

  switch (val->val_type) {
    default:
    case GRAPHD_VALUE_NULL:
      B = GRAPH_DATA_NULL;
      break;

    case GRAPHD_VALUE_DATATYPE:
      B = val->val_datatype;
      break;

    case GRAPHD_VALUE_NUMBER:
      B = val->val_number;
      break;
  }
  cl_assert(cl, B != GRAPH_DATA_UNSPECIFIED);

  A = pdb_primitive_valuetype_get(pr);
  if (A == GRAPH_DATA_UNSPECIFIED) {
    graph_guid g;
    char buf[GRAPH_GUID_SIZE];

    pdb_primitive_guid_get(pr, g);
    cl_log(cl, CL_LEVEL_ERROR, "bad valuetype %d from primitive %s", A,
           graph_guid_to_string(&g, buf, sizeof buf));
    return 1;
  }

  cl_assert(cl, A != GRAPH_DATA_UNSPECIFIED);
  return A - B;
}

/**
 * @brief Precompare.
 *
 *  There's a new primitive we're looking at in pr, and an
 *  existing value at loc.
 *
 *  There may be things about the primitive that we don't
 *  know (e.g., variable values passed up from subconstraints.)
 *
 *  In addition to the regular three values returned by a comparator
 *  function (< 0, 0, > 0), this one has a fourth value, meaning
 *  "I don't know."; that's expressed by setting *known to false.
 *
 * @param gsc		the sort context
 * @param pr		the primitive we're comparing
 * @param loc		location of the median element we're comparing against
 * @param which_out	if non-NULL, store the index of the differing pattern
 *			here
 * @param known 	set to false if we don't know for sure
 *
 * @return -1 if pr is < loc
 * @return 1 if pr is > loc
 * @return 0 if they're equal
 */
static int sort_precompare_pr_loc(graphd_sort_context *gsc,
                                  pdb_primitive const *pr, long loc,
                                  size_t *which_out, bool *known) {
  cl_handle *cl = gsc->gsc_cl;
  graphd_pattern const *pat;
  graphd_value *val = NULL;

  int pr_bool, res, err;
  unsigned long long loc_num, pr_num;
  graph_guid const *loc_guid_ptr;
  graph_guid pr_guid;
  char const *pr_str;
  size_t pr_str_n;
  graph_timestamp_t pr_ts;
  size_t which = 0;

  /*
   * cmp should never be assumed here. It should always be derived from
   * the sort pattern.
   */
  graphd_comparator const *cmp = NULL;

  cl_assert(cl, gsc->gsc_result != NULL);
  cl_log(cl, CL_LEVEL_SPEW, "sort_precompare_pr_loc(loc=%ld)", loc);

  /* By default.  We'll set it to false if we don't know something. */
  *known = true;

  for (pat = sort_instructions(gsc); pat != NULL;
       pat = pat->pat_next, which++) {
    int factor = pat->pat_sort_forward ? 1 : -1;

    switch (pat->pat_type) {
      case GRAPHD_PATTERN_ARCHIVAL:
        pr_bool = pdb_primitive_is_archival(pr);
        goto have_bool;

      case GRAPHD_PATTERN_DATATYPE:
      case GRAPHD_PATTERN_VALUETYPE:
        val = graphd_sort_value(gsc, pat, loc);
        cl_assert(cl, val != NULL);

        res = sort_precompare_pr_val_valuetype(cl, pr, val);
        if (res != 0) {
          res *= factor;
          goto have_result;
        }
        continue;

      case GRAPHD_PATTERN_GENERATION:
        pr_num =
            (pdb_primitive_has_previous(pr) ? pdb_primitive_generation_get(pr)
                                            : 0);
        loc_num = val != NULL && val->val_type == GRAPHD_VALUE_NUMBER
                      ? val->val_number
                      : 0;
        goto have_nums;

      case GRAPHD_PATTERN_GUID:

        pdb_primitive_guid_get(pr, pr_guid);
        goto have_guid;

      case GRAPHD_PATTERN_LEFT:

        if (pdb_primitive_has_left(pr))
          pdb_primitive_left_get(pr, pr_guid);
        else
          pr_guid = graph_guid_null;

        goto have_guid;

      case GRAPHD_PATTERN_LIVE:
        pr_bool = pdb_primitive_is_live(pr);
        goto have_bool;

      case GRAPHD_PATTERN_META:

        val = graphd_sort_value(gsc, pat, loc);
        cl_assert(cl, val != NULL);

        /* Nodes < links. */
        pr_num = !pdb_primitive_is_node(pr);
        loc_num = val && val->val_type == GRAPHD_VALUE_ATOM &&
                  val->val_text_s < val->val_text_e &&
                  val->val_text_s[0] != 'n' && val->val_text_s[0] != 'N';
        goto have_nums;

      case GRAPHD_PATTERN_NAME:
        pr_str = pdb_primitive_name_get_memory(pr);
        pr_str_n = pdb_primitive_name_get_size(pr);
        goto have_default_string;

      case GRAPHD_PATTERN_NEXT:
        if (pdb_primitive_has_previous(pr)) {
          graph_guid g;
          unsigned long long gen;

          pdb_primitive_guid_get(pr, g);
          gen = pdb_primitive_generation_get(pr);
          err = pdb_generation_nth(gsc->gsc_graphd->g_pdb,
                                   gsc->gsc_greq->greq_asof, &g, false, gen + 1,
                                   NULL, &pr_guid);
          cl_cover(cl);
        } else {
          graph_guid g;

          /* Either an original or unversioned. */

          pdb_primitive_guid_get(pr, g);
          err = pdb_generation_nth(gsc->gsc_graphd->g_pdb,
                                   gsc->gsc_greq->greq_asof, &g, false, 1, NULL,
                                   &pr_guid);

          cl_cover(cl);
        }
        if (err != 0) {
          if (err != GRAPHD_ERR_NO)
            cl_log(cl, CL_LEVEL_ERROR,
                   "unexpected error from "
                   "pdb_generation_nth: %s",
                   graphd_strerror(err));
          pr_guid = graph_guid_null;
        }
        goto have_guid;

      case GRAPHD_PATTERN_PREVIOUS:
        if (pdb_primitive_has_previous(pr))
          pdb_primitive_previous_guid(gsc->gsc_graphd->g_pdb, pr, &pr_guid);
        else
          pr_guid = graph_guid_null;

        goto have_guid;

      case GRAPHD_PATTERN_RIGHT:
        if (pdb_primitive_has_right(pr))
          pdb_primitive_right_get(pr, pr_guid);
        else
          pr_guid = graph_guid_null;

        goto have_guid;

      case GRAPHD_PATTERN_SCOPE:
        if (pdb_primitive_has_scope(pr))
          pdb_primitive_scope_get(pr, pr_guid);
        else
          pr_guid = graph_guid_null;
        goto have_guid;

      case GRAPHD_PATTERN_TIMESTAMP:
        pr_ts = pdb_primitive_timestamp_get(pr);
        val = graphd_sort_value(gsc, pat, loc);
        cl_assert(cl, val != NULL);
        cl_assert(cl, val->val_type == GRAPHD_VALUE_TIMESTAMP);
        if (pr_ts != val->val_timestamp) {
          res = factor * (pr_ts < val->val_timestamp ? -1 : 1);
          goto have_result;
        }
        continue;

      case GRAPHD_PATTERN_TYPE:
        if (!pdb_primitive_has_typeguid(pr)) {
          pr_str = NULL;
          pr_str_n = 0;

          goto have_default_string;
        } else {
          graphd_value pr_value;
          graph_guid guid;

          pdb_primitive_typeguid_get(pr, guid);
          graphd_value_null_set(&pr_value);
          if (graphd_type_value_from_guid(gsc->gsc_graphd,
                                          gsc->gsc_greq->greq_asof, &guid,
                                          &pr_value) != 0) {
            pr_str = NULL;
            pr_str_n = 0;

            goto have_default_string;
          }
          val = graphd_sort_value(gsc, pat, loc);
          res = graphd_value_compare(gsc->gsc_greq, pat->pat_comparator,
                                     &pr_value, val);

          graphd_value_finish(cl, &pr_value);
          if (res != 0) goto have_result;
        }
        continue;

      case GRAPHD_PATTERN_TYPEGUID:
        if (pdb_primitive_has_typeguid(pr))
          pdb_primitive_typeguid_get(pr, pr_guid);
        else
          pr_guid = graph_guid_null;
        goto have_guid;

      case GRAPHD_PATTERN_VALUE:
        pr_str = pdb_primitive_value_get_memory(pr);
        pr_str_n = pdb_primitive_value_get_size(pr);
        goto have_comparator_string;

      case GRAPHD_PATTERN_VARIABLE:
        /* later */
        goto unknown;

      case GRAPHD_PATTERN_CURSOR:
      case GRAPHD_PATTERN_TIMEOUT:
      case GRAPHD_PATTERN_LIST:
      case GRAPHD_PATTERN_PICK:
        /* user error? */
        goto unknown;

      case GRAPHD_PATTERN_CONTENTS:
      case GRAPHD_PATTERN_COUNT:
        /* ??? */
        /* for now, fall through */
        goto unknown;

      default:
      case GRAPHD_PATTERN_UNSPECIFIED:
      case GRAPHD_PATTERN_LITERAL:
      case GRAPHD_PATTERN_NONE:
        cl_notreached(cl, "unexpected sort instruction type %d", pat->pat_type);
    }
    continue;

  have_bool:
    /* True sorts before -- less than -- false. */
    val = graphd_sort_value(gsc, pat, loc);

    cl_assert(cl, val != NULL);
    cl_assert(cl, val->val_type == GRAPHD_VALUE_BOOLEAN);

    if (!pr_bool != !val->val_boolean) {
      res = (pr_bool ? -1 : 1) * factor;
      goto have_result;
    }
    continue;

  have_nums:
    if (pr_num != loc_num) {
      res = (pr_num > loc_num ? 1 : -1) * factor;
      goto have_result;
    }
    continue;

  have_guid:
    val = graphd_sort_value(gsc, pat, loc);
    if (val == NULL || val->val_type != GRAPHD_VALUE_GUID)
      loc_guid_ptr = &graph_guid_null;
    else
      loc_guid_ptr = &val->val_guid;

    res = graph_guid_compare(&pr_guid, loc_guid_ptr);
    if (res != 0) {
      res *= factor;
      goto have_result;
    }
    continue;

  have_default_string:
    cmp = graphd_comparator_default;
    cl_assert(cl, cmp);
    goto have_string;

  have_comparator_string:
    cmp = pat->pat_comparator;
    cl_assert(cl, cmp);

  have_string:
    val = graphd_sort_value(gsc, pat, loc);
    cl_assert(cl, val != NULL);
    if (pr_str_n == 0) {
      if (val->val_type == GRAPHD_VALUE_NULL) continue;
      res = -factor;
      goto have_result;
    }
    if (val->val_type == GRAPHD_VALUE_NULL) {
      res = factor;
      goto have_result;
    }

    if (val->val_type != GRAPHD_VALUE_STRING &&
        val->val_type != GRAPHD_VALUE_ATOM)
      cl_notreached(cl, "unexpected result token type %d", val->val_type);

    cl_assert(cl, val->val_text_s <= val->val_text_e);
    cl_assert(cl, cmp);

    res = (*cmp->cmp_sort_compare)(gsc->gsc_greq, pr_str, pr_str + pr_str_n - 1,
                                   val->val_text_s, val->val_text_e);
    if (res) {
      res *= factor;
      goto have_result;
    }
    continue;
  }

  /*  We have run out of criteria, and the two objects
  *   have sorted equal.
   */
  res = 0;
have_result:
  if (which_out != NULL) *which_out = which;
  return res;

unknown:
  *known = false;
  res = 0;
  goto have_result;
}

/**
 * @brief Check the sort value at a location for deferred elements.
 */
static graphd_value *sort_check_for_deferred(graphd_sort_context *gsc,
                                             unsigned long loc) {
  cl_handle *cl = gsc->gsc_cl;
  graphd_pattern const *pat;
  graphd_value *val;

  pat = sort_instructions(gsc);

  cl_assert(cl, gsc->gsc_result != NULL);

  for (; pat != NULL; pat = pat->pat_next) {
    val = graphd_sort_value(gsc, pat, loc);
    cl_assert(cl, val != NULL);

    char buf[200];
    cl_log(cl, CL_LEVEL_VERBOSE, "sort_check_for_deferred [%lu]: %s", loc,
           graphd_value_to_string(val, buf, sizeof buf));

    val = graphd_value_locate(val, GRAPHD_VALUE_DEFERRED);
    if (val != NULL) return val;
  }
  return NULL;
}

/**
 * @brief Is this a perfect pattern?
 *
 *  A perfect pattern is one where, iff we have an ordered iterator,
 *  that iterator is *perfectly* ordered - there's no need to sort.
 */
static bool pattern_is_perfect(graphd_pattern_type type) {
  return type == GRAPHD_PATTERN_GUID || type == GRAPHD_PATTERN_TIMESTAMP;
}

/**
 * @brief Is the iterator done yet?
 *
 *  Generally, an iterator is "done" if we can call its "beyond" method
 *  with our current sort boundary and have it tell us that it'll never
 *  again produce anything that's smaller than that.
 *
 *  If we don't have a sort boundary yet, we're not done; and some
 *  sort patterns and iterator orderings imply that we're done once
 *  we go past a boundary, no matter what the "beyond" call says.
 *
 * @param gsc	sort context
 * @param loc	location of the record we're talking about.
 * @param it	iterator that we're using to pull out new candidates.
 *
 * @return false if the iterator hasn't finished yet.
 * @return true  if the iterator has moved beyond our sort frame.
 */
static bool sort_root_value_beyond(graphd_sort_context *gsc, unsigned long loc,
                                   pdb_iterator *it) {
  int err;
  char const *val_s = NULL;
  char const *val_e = NULL;
  graphd_value const *val;
  graphd_pattern const *pat;
  bool beyond;
  char buf[200];
  pdb_id id;

  pat = sort_instructions(gsc);
  if (pat == NULL) {
    cl_log(gsc->gsc_cl, CL_LEVEL_VERBOSE,
           "sort_root_value_beyond: no instructions");
    return false;
  }

  switch (gsc->gsc_con->con_sort_root.sr_pat.pat_type) {
    default:
    case GRAPHD_PATTERN_UNSPECIFIED:
    case GRAPHD_PATTERN_ARCHIVAL:
    case GRAPHD_PATTERN_DATATYPE:
    case GRAPHD_PATTERN_GENERATION:
    case GRAPHD_PATTERN_LITERAL:
    case GRAPHD_PATTERN_NONE:
    case GRAPHD_PATTERN_LIVE:
    case GRAPHD_PATTERN_META:
    case GRAPHD_PATTERN_NEXT:
    case GRAPHD_PATTERN_PREVIOUS:
    case GRAPHD_PATTERN_TYPE:
    case GRAPHD_PATTERN_VARIABLE:
    case GRAPHD_PATTERN_LIST:
    case GRAPHD_PATTERN_PICK:
    case GRAPHD_PATTERN_COUNT:
    case GRAPHD_PATTERN_CURSOR:
    case GRAPHD_PATTERN_CONTENTS:
    case GRAPHD_PATTERN_ESTIMATE:
    case GRAPHD_PATTERN_ESTIMATE_COUNT:
    case GRAPHD_PATTERN_VALUETYPE:
    case GRAPHD_PATTERN_ITERATOR:
    case GRAPHD_PATTERN_TIMEOUT:
      cl_log(gsc->gsc_cl, CL_LEVEL_VERBOSE,
             "sort_root_value_beyond: don't know "
             "how to deal with pattern %s",
             graphd_pattern_dump(&gsc->gsc_con->con_sort_root.sr_pat, buf,
                                 sizeof buf));
      return false;

    case GRAPHD_PATTERN_TIMESTAMP:

      val = graphd_sort_value(gsc, pat, loc);
      if (val == NULL || val->val_type != GRAPHD_VALUE_TIMESTAMP) {
        char pbuf[200], vbuf[200];
        cl_log(gsc->gsc_cl, CL_LEVEL_FAIL,
               "sort_root_value_beyond: looking for "
               "a TIMESTAMP to go with pattern %s, "
               "but value is %s",
               graphd_pattern_dump(&gsc->gsc_con->con_sort_root.sr_pat, pbuf,
                                   sizeof pbuf),
               graphd_value_to_string(val, vbuf, sizeof vbuf));
        return false;
      }

      id = val->val_timestamp_id;
      if (id == PDB_ID_NONE) {
        err =
            graphd_timestamp_to_id(gsc->gsc_graphd->g_pdb, &val->val_timestamp,
                                   GRAPHD_OP_EQ, &id, NULL);
        if (err != 0) {
          char buf[GRAPH_GUID_SIZE];
          cl_log_errno(
              gsc->gsc_cl, CL_LEVEL_FAIL, "graphd_timestamp_to_id", err,
              "timestamp=%s",
              graph_timestamp_to_string(val->val_timestamp, buf, sizeof buf));
          return false;
        }
      }

      val_s = (char *)&id;
      val_e = (char *)(&id + 1);

      err = pdb_iterator_beyond(gsc->gsc_graphd->g_pdb, it, val_s, val_e,
                                &beyond);
      if (err != 0) {
        cl_log_errno(gsc->gsc_cl, CL_LEVEL_VERBOSE, "pdb_iterator_beyond", err,
                     "id=%llx: it=%s", (unsigned long long)id,
                     pdb_iterator_to_string(gsc->gsc_graphd->g_pdb, it, buf,
                                            sizeof buf));
        return false;
      }
      cl_log(gsc->gsc_cl, CL_LEVEL_VERBOSE,
             "sort_root_value_beyond: iterator says %s",
             beyond ? "true" : "false");
      return beyond;

    case GRAPHD_PATTERN_GUID:
    case GRAPHD_PATTERN_LEFT:
    case GRAPHD_PATTERN_RIGHT:
    case GRAPHD_PATTERN_SCOPE:
    case GRAPHD_PATTERN_TYPEGUID:
      val = graphd_sort_value(gsc, pat, loc);
      if (val == NULL || val->val_type != GRAPHD_VALUE_GUID) {
        char pbuf[200], vbuf[200];
        cl_log(gsc->gsc_cl, CL_LEVEL_FAIL,
               "sort_root_value_beyond: looking for "
               "a GUID to go with pattern %s, "
               "but value is %s",
               graphd_pattern_dump(&gsc->gsc_con->con_sort_root.sr_pat, pbuf,
                                   sizeof pbuf),
               graphd_value_to_string(val, vbuf, sizeof vbuf));
        return false;
      }
      err = pdb_id_from_guid(gsc->gsc_graphd->g_pdb, &id, &val->val_guid);
      if (err != 0) {
        char buf[GRAPH_GUID_SIZE];
        cl_log_errno(gsc->gsc_cl, CL_LEVEL_FAIL, "pdb_id_from_guid", err,
                     "guid=%s",
                     graph_guid_to_string(&val->val_guid, buf, sizeof buf));
        return false;
      }

      val_s = (char *)&id;
      val_e = (char *)(&id + 1);

      err = pdb_iterator_beyond(gsc->gsc_graphd->g_pdb, it, val_s, val_e,
                                &beyond);
      if (err != 0) {
        cl_log_errno(gsc->gsc_cl, CL_LEVEL_VERBOSE, "sort_root_value_beyond",
                     err, "id=%llx: it=%s", (unsigned long long)id,
                     pdb_iterator_to_string(gsc->gsc_graphd->g_pdb, it, buf,
                                            sizeof buf));
        return false;
      }
      cl_log(gsc->gsc_cl, CL_LEVEL_VERBOSE,
             "sort_root_value_beyond: iterator says %s",
             beyond ? "true" : "false");
      return beyond;

    case GRAPHD_PATTERN_NAME:
    case GRAPHD_PATTERN_VALUE:
      break;
  }
  val = graphd_sort_value(gsc, pat, loc);
  if (val == NULL || val->val_type != GRAPHD_VALUE_STRING) {
    cl_log(gsc->gsc_cl, CL_LEVEL_FAIL,
           "sort_root_value_beyond: don't know how "
           "to deal with value %s",
           graphd_value_to_string(val, buf, sizeof buf));
    return false;
  }

  val_s = val->val_text_s;
  val_e = val->val_text_e;

  err = pdb_iterator_beyond(gsc->gsc_graphd->g_pdb, it, val_s, val_e, &beyond);
  if (err != 0) {
    cl_log(gsc->gsc_cl, CL_LEVEL_VERBOSE,
           "sort_root_value_beyond %.*s: %s: error: %s",
           (int)(val->val_text_e - val->val_text_s), val->val_text_s,
           pdb_iterator_to_string(gsc->gsc_graphd->g_pdb, it, buf, sizeof buf),
           graphd_strerror(err));

    return false;
  }

  cl_log(gsc->gsc_cl, CL_LEVEL_VERBOSE, "sort_root_value_beyond %.*s: %s: %s",
         (int)(val->val_text_e - val->val_text_s), val->val_text_s,
         pdb_iterator_to_string(gsc->gsc_graphd->g_pdb, it, buf, sizeof buf),
         beyond ? "yes, we're done" : "no, we can still go below that.");
  return beyond;
}

/**
 * @brief Compare two result values at two locations
 *
 *  The comparison uses the values listed by gsc->gsc_con->con_sort,
 *  in the order listed.
 *
 * @return 0 if the values compare equal
 * @return -1 if the value at a_loc is smaller than the one at b_loc
 * @return 1 if the value at a_loc is greater than the one at b_loc
 */
static int sort_compare_loc_loc(graphd_sort_context *gsc, unsigned long a_loc,
                                unsigned long b_loc, size_t *which_out) {
  cl_handle *cl = gsc->gsc_cl;
  graphd_pattern const *pat;
  int res = 0;
  size_t which = 0;

  pat = sort_instructions(gsc);

  cl_assert(cl, gsc->gsc_result != NULL);

  cl_log(cl, CL_LEVEL_SPEW, "sort_compare_loc_loc(%ld, %ld)", a_loc, b_loc);

  for (; pat != NULL; pat = pat->pat_next, which++) {
    cl_assert(cl,
              (pat->pat_type != GRAPHD_PATTERN_VALUE) || pat->pat_comparator);
    res = graphd_value_compare(
        gsc->gsc_greq,
        pat->pat_comparator ? pat->pat_comparator : graphd_comparator_default,
        graphd_sort_value(gsc, pat, a_loc), graphd_sort_value(gsc, pat, b_loc));
    if (res != 0) {
      if (!pat->pat_sort_forward) res = -res;
      break;
    }
  }

  if (which_out != NULL) *which_out = which;

  cl_log(cl, CL_LEVEL_SPEW, "result: %d", res);
  return res;
}

static int sort_qsort_compare_loc_loc(void const *aptr, void const *bptr) {
  int res;

  if (*(unsigned long const *)aptr == *(unsigned long const *)bptr) return 0;

  res = sort_compare_loc_loc(graphd_sort_qsort_context,
                             *(unsigned long const *)aptr,
                             *(unsigned long const *)bptr, NULL);
  if (res) return res;

  /* preserve the existing order. */
  return *(unsigned long const *)aptr < *(unsigned long const *)bptr ? -1 : 1;
}

/**
 * @brief Sort some candidate elements.
 *
 *  The elements are specified as indices in a fragment of the
 *  gsc->gsc_order_to_location array.
 *
 * @param gsc	Overall sort operation context.
 * @param start	Index of the first element to sort
 * @param end 	First index that's excluded from the sort.
 */
static void sort_candidates(graphd_sort_context *gsc, size_t start,
                            size_t end) {
  /* More than one element in start..end?
   */
  if (end - start > 1) {
    graphd_sort_qsort_context = gsc;
    qsort(gsc->gsc_order_to_location + start, end - start,
          sizeof(*gsc->gsc_order_to_location), sort_qsort_compare_loc_loc);
  }
}

/*  We've got an array of up to 2*gsc->gsc_pagesize candidates.
 *  The first and second half are sorted amongst each other.
 *  All the elements of the second are < our median
 *  element[gsc->gsc_pagesize - 1].
 */
static void sort_merge_new_candidate_set(graphd_sort_context *gsc) {
  size_t old_i = 0, new_i = gsc->gsc_pagesize, write_i = 0;
  int result;
  unsigned long *new_order;

  new_order = gsc->gsc_order_to_location + 2 * gsc->gsc_pagesize;
  while (write_i < gsc->gsc_pagesize) {
    cl_assert(gsc->gsc_cl, old_i < gsc->gsc_pagesize);

    if (new_i >= gsc->gsc_n) {
      /*  There were fewer than gsc->gsc_pagesize new
       *  elements, and we've already inserted them all.
       *  Fill up with old elements.
       */
      size_t need = gsc->gsc_pagesize - write_i;
      cl_assert(gsc->gsc_cl, old_i + need <= gsc->gsc_pagesize);
      memcpy(new_order + write_i, gsc->gsc_order_to_location + old_i,
             need * sizeof(*new_order));

      old_i += need;
      write_i += need;
      break;
    }

    /*  Pick up the smaller one of the two candidates.
     *  (The other one may be picked up next time around.)
     */
    result = sort_compare_loc_loc(gsc, gsc->gsc_order_to_location[new_i],
                                  gsc->gsc_order_to_location[old_i], NULL);

    new_order[write_i++] =
        gsc->gsc_order_to_location[result < 0 ? new_i++ : old_i++];
  }

  cl_assert(gsc->gsc_cl, write_i == gsc->gsc_pagesize);
  cl_assert(gsc->gsc_cl,
            (new_i - gsc->gsc_pagesize) + old_i == gsc->gsc_pagesize);

  /*  Move the indices we didn't pick up to the back of the
   *  array.  We'll reuse their slots when they're returned
   *  as locations for newly prefiltered data.
   */
  if (old_i < gsc->gsc_pagesize)

    /*  This overwrites the first gsc->gsc_pagesize - old_i
     *  elements in the new section of the array.
     *
     *  Because old_i + new_i == pagesize, pagesize - old_i
     *  is new_i -- we're overwriting the first new_i elements
     *  of the array, exactly those new_i elements that made
     *  it into <new_order>.
     */
    memmove(gsc->gsc_order_to_location + gsc->gsc_pagesize,
            gsc->gsc_order_to_location + old_i,
            sizeof(*gsc->gsc_order_to_location) * (gsc->gsc_pagesize - old_i));

  memcpy(gsc->gsc_order_to_location, new_order,
         sizeof(*new_order) * gsc->gsc_pagesize);
}

/*  We've stored up to 2*gsc->gsc_pagesize candidate results in our
 *  variable and result arrays.  It's time to look at those results
 *  and keep only the best gsc->gsc_pagesize ones.
 */
static void sort_condense(graphd_sort_context *gsc) {
  cl_enter(gsc->gsc_cl, CL_LEVEL_SPEW, "enter");

  if (gsc->gsc_have_median) {
    /*  Sort the candidate set above the median.
     */
    sort_candidates(gsc, gsc->gsc_pagesize, gsc->gsc_n);

    /*  Merge the sorted new candidate set into the already
     *  sorted old one.
     */
    sort_merge_new_candidate_set(gsc);
  } else {
    /*  We don't have a median yet.  Sort all our candidates.
     */
    sort_candidates(gsc, 0, gsc->gsc_n);

    /*  If we have enough candidates to contain a median,
     *  mark that we have one.
     */
    gsc->gsc_have_median |= (gsc->gsc_n >= gsc->gsc_pagesize);
  }

  /*  Keep only up to {pagesize} entries.  (Don't bother
   *  freeing the overhang; the "set a sequence element" function
   *  will automatically free a preexisting element if it
   *  encounters one.)
   */
  if (gsc->gsc_n > gsc->gsc_pagesize) {
    gsc->gsc_have_trailing = true;
    gsc->gsc_n = gsc->gsc_pagesize;
  }
  cl_leave(gsc->gsc_cl, CL_LEVEL_SPEW, "leave");
}

/**
 * @brief Create a new sort context.
 *
 * At the end, the alternatives in each result's ID sequence
 * will be sorted according to con_sort.
 *
 * @param greq		request the sort is for
 * @param con		constraint whose matches will be sorted
 * @param result	storage for result
 *
 * @return a sort context on success, otherwise NULL.
 */
graphd_sort_context *graphd_sort_create(graphd_request *greq,
                                        graphd_constraint *con,
                                        graphd_value *result) {
  size_t i;
  graphd_sort_context *gsc;
  cl_handle *cl = graphd_request_cl(greq);

  gsc = cm_malloc(greq->greq_req.req_cm, sizeof(*gsc));
  if (gsc == NULL) {
    cl_log(cl, CL_LEVEL_ERROR,
           "graphd_sort_create: failed to allocate %d bytes "
           "for sort context [%s:%d]",
           (int)sizeof(*gsc), __FILE__, __LINE__);
    return gsc;
  }

  memset(gsc, 0, sizeof(*gsc));

  gsc->gsc_greq = greq;
  gsc->gsc_graphd = graphd_request_graphd(greq);
  gsc->gsc_cl = graphd_request_cl(greq);
  gsc->gsc_cm = greq->greq_req.req_cm;
  gsc->gsc_con = con;
  gsc->gsc_result = result;
  gsc->gsc_result_n = con->con_pframe_n;
  gsc->gsc_cursor_grid = NULL;

  /*  Make temporary space to sort 2 * pagesize results.
   *  (With another <pagesize> temporary results used during
   *  merge, for a total of 3 * pagesize.)
   */
  cl_assert(cl, con->con_resultpagesize_valid);
  gsc->gsc_pagesize = con->con_resultpagesize + con->con_start;

  gsc->gsc_order_to_location =
      cm_malloc(gsc->gsc_cm, sizeof(unsigned long) * gsc->gsc_pagesize * 3);
  if (gsc->gsc_order_to_location == NULL) {
    cl_log(cl, CL_LEVEL_ERROR,
           "graphd_sort_create: failed to allocate %lu bytes "
           "for sort indices [%s:%d]",
           (unsigned long)(sizeof(unsigned long) * gsc->gsc_pagesize * 3),
           __FILE__, __LINE__);
    cm_free(gsc->gsc_cm, gsc);
    return NULL;
  }
  for (i = gsc->gsc_pagesize * 2; i-- > 0;) gsc->gsc_order_to_location[i] = i;

  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_sort_create pagesize=%zu",
         gsc->gsc_pagesize);
  return gsc;
}

/*  Prefilter: called after reading a primitive, but before computing
 *  its full contents.  (In other words, it isn't at all certain that this
 *  node will actually meet constraints, although it may have met some.)
 *
 *  Assign (size_t)-1 to discard, [0...2*pagesize) to accept into that slot.
 *  Return 0 to accept, GRAPHD_ERR_TOO_SMALL to discard as smaller,
 *  GRAPHD_ERR_TOO_LARGE to discard as too large.
 */

int graphd_sort_accept_prefilter(graphd_sort_context *gsc, pdb_iterator *it,
                                 pdb_primitive const *pr,
                                 size_t *position_out) {
  int res;
  bool known;
  char const *ord;
  size_t which = 0;

  cl_assert(gsc->gsc_cl, gsc->gsc_n < 2 * gsc->gsc_pagesize);

  gsc->gsc_started = 1;
  if (gsc->gsc_ended) {
    cl_log(gsc->gsc_cl, CL_LEVEL_SPEW, "graphd_sort_accept_prefilter: ended.");
    *position_out = (size_t)-1;
    return GRAPHD_ERR_TOO_LARGE;
  }

  /*  If we have a cursor grid, compare the primitive to the
   *  cursor grid.  If it's smaller or equal, throw it out.
   */
  if (gsc->gsc_cursor_grid != NULL) {
    res = sort_precompare_pr_loc(gsc, pr, -1, NULL, &known);
    if (known && res <= 0) {
      cl_log(gsc->gsc_cl, CL_LEVEL_SPEW,
             "graphd_sort_accept_prefilter: "
             "sort_precompare_pr_loc: "
             "known and too small.");
      *position_out = (size_t)-1;
      return GRAPHD_ERR_TOO_SMALL;
    }
  }

  if (!gsc->gsc_have_median) {
    *position_out = gsc->gsc_order_to_location[gsc->gsc_n];
    return 0;
  }

  /*  If this isn't the first time, compare the primitive with the
   *  current median result element.
   */
  res = sort_precompare_pr_loc(
      gsc, pr, gsc->gsc_order_to_location[gsc->gsc_pagesize - 1], &which,
      &known);

  cl_log(gsc->gsc_cl, CL_LEVEL_SPEW,
         "graphd_sort_accept_prefilter: %d %s (gsc_n is %d)", res,
         known ? "known" : "unknown", (int)gsc->gsc_n);

  gsc->gsc_blind_accept = known && res < 0;
  if (!known || res <= 0) {
    cl_log(gsc->gsc_cl, CL_LEVEL_SPEW,
           "graphd_sort_accept_prefilter: not known / smaller");
    *position_out = gsc->gsc_order_to_location[gsc->gsc_n];
    return 0;
  }

  /*  We're over.  If the iterator who produced this is ordered
   *  with respect to this constraint's sort root, we can stop
   *  now.
   *
   *  We can be done one of two ways:
   *
   *  (1) There's sorting, and a sort root, and the iterator
   *  	knows that it's ordered according to the sort root,
   *	and the iterator's "beyond" callback tells us that
   * 	it's done.  That's what is happening here.
   *
   *  (2) There's a sort constraint, but no actual manual
   *  	sorting - instead, the iterator is sorted in ascending
   *  	or descending ID order, and that was good enough for
   *	us (so we never created a sort root or a sort context).
   *
   *  	That's happening in the caller.
   */
  if (which == 0 && it != NULL && gsc->gsc_con->con_sort != NULL &&
      gsc->gsc_con->con_sort_valid &&
      pdb_iterator_ordered_valid(gsc->gsc_graphd->g_pdb, it) &&
      pdb_iterator_ordered(gsc->gsc_graphd->g_pdb, it) &&
      (ord = pdb_iterator_ordering(pdb, it)) != NULL &&
      graphd_sort_root_has_ordering(&gsc->gsc_con->con_sort_root, ord) &&
      gsc->gsc_con->con_sort_root.sr_con == gsc->gsc_con &&
      sort_root_value_beyond(
          gsc, gsc->gsc_order_to_location[gsc->gsc_pagesize - 1], it)) {
    char buf[200];
    cl_log(gsc->gsc_cl, CL_LEVEL_SPEW,
           "graphd_sort_accept_prefilter: ending gsc=%p - %s, which is "
           "ordered by %s, returns beyond: true (%s)",
           (void *)gsc,
           pdb_iterator_to_string(gsc->gsc_graphd->g_pdb, it, buf, sizeof buf),
           ord, it->it_type->itt_beyond == NULL ? "defaulted" : "method call");
    gsc->gsc_ended = true;
  }

  *position_out = (size_t)-1;
  return GRAPHD_ERR_TOO_LARGE;
}

bool graphd_sort_accept_ended(graphd_sort_context *gsc) {
  return gsc->gsc_ended;
}

/*  The primitive most recently offered to graphd_sort_accept_prefilter
 *  was acecpted by it, and has now grown into a full-fledged result token
 *  with contents, stored in the position returned by graphd_sort_accept(),
 *  which always happens to be gsc->gsc_order_to_location[gsc->gsc_n].
 *
 *  Check again whether it truly fits into the sort order.
 *  @return GRAPHD_ERR_MORE	a value has been assigned to *deferred_out and
 *needs
 *			to be evaluated before making the sorting decision.
 *  @return GRAPHD_ERR_NO	to reject
 *  @return 0			to accept
 */
int graphd_sort_accept(graphd_sort_context *gsc, pdb_iterator *it,
                       graphd_value **deferred_out) {
  size_t which;
  pdb_handle *pdb = gsc->gsc_graphd->g_pdb;

  cl_log(gsc->gsc_cl, CL_LEVEL_SPEW,
         "graphd_sort_accept: blind? %d, gsc_n %d, location %d",
         (int)gsc->gsc_blind_accept, (int)gsc->gsc_n,
         (int)gsc->gsc_order_to_location[gsc->gsc_n]);

  *deferred_out =
      sort_check_for_deferred(gsc, gsc->gsc_order_to_location[gsc->gsc_n]);
  if (*deferred_out != NULL) return GRAPHD_ERR_MORE;

  /*  If we have a cursor cut-off grid, and this compares
   *  <= to the cursor, throw it out.
   */
  if (!gsc->gsc_blind_accept && gsc->gsc_cursor_grid != NULL &&
      sort_compare_loc_loc(gsc, gsc->gsc_order_to_location[gsc->gsc_n], -1,
                           NULL) <= 0)
    return GRAPHD_ERR_NO;

  if (!gsc->gsc_have_median || gsc->gsc_blind_accept ||
      sort_compare_loc_loc(gsc, gsc->gsc_order_to_location[gsc->gsc_n],
                           gsc->gsc_order_to_location[gsc->gsc_pagesize - 1],
                           &which) < 0) {
    /* The new entry is smaller than the median! */
    if (++gsc->gsc_n >= gsc->gsc_pagesize * 2) sort_condense(gsc);
    return 0;
  } else {
    char const *ord;

    /*  The entry was larger than the median.
     *
     *  If the constraint has a sort root, and the
     *  iterator underlying it is ordered, and it
     *  failed in the first comparison, mark the
     *  underlying sort as ended.
     */
    if (it != NULL && which == 0 &&
        pdb_iterator_ordered_valid(gsc->gsc_graphd->g_pdb, it) &&
        pdb_iterator_ordered(gsc->gsc_graphd->g_pdb, it) &&
        (ord = pdb_iterator_ordering(pdb, it)) != NULL &&
        graphd_sort_root_has_ordering(&gsc->gsc_con->con_sort_root, ord) &&
        gsc->gsc_n >= gsc->gsc_pagesize && gsc->gsc_pagesize > 0 &&
        sort_root_value_beyond(
            gsc, gsc->gsc_order_to_location[gsc->gsc_pagesize - 1], it)) {
      char buf[200];
      cl_log(gsc->gsc_cl, CL_LEVEL_SPEW,
             "graphd_sort_accept: ending (gsc=%p, it=(%p)%s)", (void *)gsc,
             (void *)it, pdb_iterator_to_string(pdb, it, buf, sizeof buf));
      gsc->gsc_ended = true;
    }
  }
  return GRAPHD_ERR_NO;
}

static void sort_finish_slot(graphd_sort_context *gsc, unsigned long *ord,
                             unsigned long *loc, size_t dst_i) {
  size_t src_i, res_i;
  cl_handle *cl = gsc->gsc_cl;
  graphd_value tmp;

  cl_assert(cl, dst_i < gsc->gsc_pagesize);

  /*  We want to give dst_i the value that belongs there.
   *  Where did that value come from?
   */
  src_i = loc[dst_i];
  if (src_i == dst_i) return;
  cl_assert(cl, ord[src_i] == dst_i);

  /*  All the array elements up to src_i are sorted.  We're going
   *  to swap src_i for the array element that belongs there.
   *  That means that it, too, has to be out of place.  Since all
   *  array elements < src_i are already in their place, the out-of
   *  place element has to come from behind src_i, not from the
   *  already-sorted chunk.
   */
  for (res_i = 0; res_i < gsc->gsc_result_n; res_i++) {
    graphd_value *seq;

    /*  If the ID sequence is NULL, then this particular context
     *  doesn't collect per-alternative results, and we
     *  don't need to move anything around.
     */
    if (gsc->gsc_con->con_pframe[res_i].pf_one == NULL) continue;

    seq = GSC_PER_ID_SEQUENCE(gsc, res_i);
    cl_assert(cl, seq != NULL);

    if (seq == NULL) continue;

    cl_assert(cl, seq->val_type == GRAPHD_VALUE_SEQUENCE);
    cl_assert(cl, seq->val_sequence_n > dst_i);
    cl_assert(cl, seq->val_sequence_n > src_i);

    seq = seq->val_sequence_contents;

    /*  swap the values at src_i and dst_i. */

    tmp = seq[dst_i];
    seq[dst_i] = seq[src_i];
    seq[src_i] = tmp;
  }

  /*  Swap the routing information to match the swapped contents.
   */
  if ((ord[src_i] = ord[dst_i]) != (unsigned long)-1) loc[ord[src_i]] = src_i;

  ord[dst_i] = loc[dst_i] = dst_i;
}

/**
 * @brief Produce a sort result.
 *
 *  We've finished sifting through alternatives.
 *  Actually sort the contents stored in gsc->gsc_result
 *  according to the sort order in gsc->gsc_order_to_location.
 */
void graphd_sort_finish(graphd_sort_context *gsc) {
  unsigned long *const loc = gsc->gsc_order_to_location;
  unsigned long *const ord = loc + gsc->gsc_pagesize;
  cl_handle *const cl = gsc->gsc_cl;
  size_t i;

  /*
  cl_log(cl, CL_LEVEL_SPEW, "%s:%d, graphd_sort_finish", __FILE__, __LINE__);
  for (i = 0; i < gsc->gsc_n; i++) cl_log(cl, CL_LEVEL_SPEW, "sort: position %d
  <- location %d", (int)i, (int)gsc->gsc_order_to_location[i]);
  */

  if (!gsc->gsc_have_median || gsc->gsc_n > gsc->gsc_pagesize)
    sort_condense(gsc);

  cl_assert(cl, !gsc->gsc_finished);
  cl_assert(cl, gsc->gsc_n <= gsc->gsc_pagesize);

  for (i = 0; i < gsc->gsc_n; i++)
    cl_log(cl, CL_LEVEL_SPEW, "sort: position %d <- location %d", (int)i,
           (int)gsc->gsc_order_to_location[i]);

  /*  Build the inverse mapping to <loc>.
   *
   *  "loc" maps order to location.  loc[i] says where the
   *  i'th value current resides.
   *
   *  "ord", once we're done here, maps location to order.
   *  ord[i] says how the value at [i] placed in the overall
   *  sort order.
   *
   *  There are 2*{pagesize} values, but we're only interested
   *  in the {pagesize} first finishers.
   *
   *  When we allocated gsc->gsc_order_to_location, we allocated
   *  3*{pagesize} slots.  We're using the last 2*{pagesize} slots
   *  as temporary storage {ord} for this calculation.
   */
  for (i = 0; i < 2 * gsc->gsc_pagesize; i++) ord[i] = (unsigned long)-1;

  for (i = 0; i < gsc->gsc_n; i++) {
    cl_assert(cl, loc[i] < 2 * gsc->gsc_pagesize);
    ord[loc[i]] = i;
  }

  /*  Move things around so that the actual location of
   *  values in their sequences matches their position in the
   *  sort order.
   */
  for (i = 0; i < gsc->gsc_n; i++) {
    sort_finish_slot(gsc, ord, loc, i);

    cl_assert(cl, loc[i] == ord[i]);
    cl_assert(cl, loc[i] == i);
  }

  /*  Truncate the result arrays to gsc->gsc_n elements, and
   *  throw out the con_start first ones.
   */
  for (i = 0; i < gsc->gsc_result_n; i++) {
    graphd_value *seq;

    if ((seq = GSC_PER_ID_SEQUENCE(gsc, i)) != NULL) {
      cl_assert(cl, seq->val_type == GRAPHD_VALUE_SEQUENCE);
      graphd_value_array_truncate(cl, seq, gsc->gsc_n);
      graphd_value_array_delete_range(cl, seq, 0, gsc->gsc_con->con_start);
    }
  }

  /*  Update the number of valid elements in our array to no
   *  longer include the con_start first ones.
   */
  if (gsc->gsc_n > gsc->gsc_con->con_start)
    gsc->gsc_n -= gsc->gsc_con->con_start;
  else
    gsc->gsc_n = 0;

  gsc->gsc_finished = true;
}

/**
 * @brief Free the sort context.
 *
 *  This is necessary because, while sort contexts are allocated in
 *  a heap, they may contain values that hold references to database tiles.
 *
 * @param gsc sort context allocated with graphd_sort_create()
 */
void graphd_sort_destroy(graphd_sort_context *gsc) {
  if (gsc != NULL) {
    cm_handle *cm = gsc->gsc_cm;

    if (gsc->gsc_order_to_location != NULL)
      cm_free(cm, gsc->gsc_order_to_location);

    if (gsc->gsc_cursor_grid != NULL) {
      size_t i;
      graphd_value *v;

      v = gsc->gsc_cursor_grid;
      i = gsc->gsc_cursor_grid_n;

      while (i-- > 0) graphd_value_finish(gsc->gsc_cl, v++);

      cm_free(cm, gsc->gsc_cursor_grid);
    }
    cm_free(cm, gsc);
  }
}

size_t graphd_sort_n_results(graphd_sort_context *gsc) {
  if (gsc == NULL) return 0;

  return (gsc->gsc_n < gsc->gsc_pagesize) ? gsc->gsc_n : gsc->gsc_pagesize;
}

int graphd_sort_cursor_get(graphd_sort_context *gsc, char const *prefix,
                           graphd_value *val_out) {
  cl_handle *cl;
  cm_buffer buf;
  graphd_pattern const *pat;
  int err;

  if (gsc == NULL) return EINVAL;

  cl = gsc->gsc_cl;

  cl_assert(cl, gsc->gsc_finished);
  cl_assert(cl, gsc->gsc_pagesize > 0);

  /*  We've run out of items to return before running
   *  out of space.
   */
  if (gsc->gsc_n < gsc->gsc_pagesize ||
      (gsc->gsc_n == gsc->gsc_pagesize && !gsc->gsc_have_trailing)) {
    static char const null_string[] = "null:";

    cl_log(cl, CL_LEVEL_SPEW,
           "grapdh_sort_cursor_get: "
           "have %llu items, could have %llu, -> null:",
           (unsigned long long)gsc->gsc_n,
           (unsigned long long)gsc->gsc_pagesize);

    graphd_value_text_set(val_out, GRAPHD_VALUE_STRING, null_string,
                          null_string + 5, NULL);
    return 0;
  }
  cm_buffer_initialize(&buf, gsc->gsc_cm);
  cm_buffer_add_string(&buf, GRAPHD_SORT_CURSOR_PREFIX);
  if (prefix != NULL) cm_buffer_add_string(&buf, prefix);

  for (pat = sort_instructions(gsc); pat != NULL; pat = pat->pat_next) {
    graphd_value *val;
    char pat_buf[200], vbuf[200];

    val = graphd_sort_value(gsc, pat, gsc->gsc_pagesize - 1);
    if ((err = graphd_value_serialize(cl, val, &buf)) != 0) goto err;

    cl_log(cl, CL_LEVEL_SPEW,
           "grapdh_sort_cursor_get: "
           "serialized %s according to %s",
           graphd_value_to_string(val, vbuf, sizeof vbuf),
           graphd_pattern_dump(pat, pat_buf, sizeof pat_buf));
  }

  graphd_value_text_set_cm(val_out, GRAPHD_VALUE_STRING, buf.buf_s, buf.buf_n,
                           buf.buf_cm);
  return 0;

err:
  cm_buffer_finish(&buf);
  return err;
}

/**
 * @brief Is this a sort cursor?
 *
 * @param s	beginning of the cursor string
 * @param e	pointer just after the end of the cursor string
 *
 * @return true if this cursor came from a "sort" module, false otherwise.
 */
int graphd_sort_is_cursor(char const *s, char const *e) {
  return e - s >= sizeof(GRAPHD_SORT_CURSOR_PREFIX) - 1 &&
         strncasecmp(GRAPHD_SORT_CURSOR_PREFIX, s,
                     sizeof(GRAPHD_SORT_CURSOR_PREFIX) - 1) == 0;
}

/**
 * @brief Set position according to a cursor.
 *
 *  The recognized cursors are the null cursor ("null:")
 *  and any cursor prefixed with "sort:".
 *
 * @param gsc	 context for ongoing sort operation
 * @param cur_s	 beginning of cursor string
 * @param cur_e  end of cursor string
 */
int graphd_sort_cursor_peek(graphd_request *greq, graphd_constraint *con) {
  cl_handle *cl = graphd_request_cl(greq);
  char const *cur_s, *cur_e;

  cur_s = con->con_cursor_s;
  cur_e = con->con_cursor_e;

  if (cur_s == NULL || cur_e == NULL) return 0;

  if (IS_LIT("null:", cur_s, cur_e)) return 0;

  if (!graphd_sort_is_cursor(cur_s, cur_e)) {
    cl_log(cl, CL_LEVEL_ERROR,
           "graphd_sort_cursor_set: don't recognize "
           "cursor \"%.*s\"",
           (int)(cur_e - cur_s), cur_s);
    return GRAPHD_ERR_LEXICAL;
  }
  cur_s += sizeof(GRAPHD_SORT_CURSOR_PREFIX) - 1;

  return graphd_constraint_cursor_scan_prefix(greq, con, &cur_s, cur_e);
}

/**
 * @brief Set position according to a cursor.
 *
 *  The recognized cursors are the null cursor ("null:")
 *  and any cursor prefixed with "sort:".
 *
 * @param gsc	 context for ongoing sort operation
 * @param cur_s	 beginning of cursor string
 * @param cur_e  end of cursor string
 */
int graphd_sort_cursor_set(graphd_sort_context *gsc, char const *cur_s,
                           char const *cur_e) {
  cl_handle *cl;
  cm_handle *cm;
  graphd_pattern const *pat, *head;
  size_t rmax, emax, i;
  int err;

  if (gsc == NULL) return EINVAL;

  cl = gsc->gsc_cl;
  cm = gsc->gsc_cm;

  cl_assert(cl, !gsc->gsc_started);

  if (IS_LIT("null:", cur_s, cur_e)) {
    cl_cover(cl);
    gsc->gsc_ended = 1;
    return 0;
  }

  if (!graphd_sort_is_cursor(cur_s, cur_e)) {
    cl_log(cl, CL_LEVEL_ERROR,
           "graphd_sort_cursor_set: don't recognize "
           "cursor \"%.*s\"",
           (int)(cur_e - cur_s), cur_s);
    return GRAPHD_ERR_LEXICAL;
  }
  cur_s += sizeof(GRAPHD_SORT_CURSOR_PREFIX) - 1;

  err = graphd_constraint_cursor_scan_prefix(gsc->gsc_greq, gsc->gsc_con,
                                             &cur_s, cur_e);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_constraint_scan_cursor_prefix", err,
                 "cursor=\"%.*s\"", (int)(cur_e - cur_s), cur_s);
    return err;
  }

  if (IS_LIT("END", cur_s, cur_e)) {
    cl_cover(cl);
    gsc->gsc_ended = 1;
    return 0;
  } else if (IS_LIT("START", cur_s, cur_e)) {
    cl_cover(cl);
    return 0;
  }
  head = sort_instructions(gsc);
  rmax = 0;
  emax = 0;
  for (pat = head; pat != NULL; pat = pat->pat_next) {
    if (pat->pat_element_offset > emax) {
      cl_cover(cl);
      emax = pat->pat_element_offset;
    }
    if (pat->pat_result_offset > rmax) {
      cl_cover(cl);
      rmax = pat->pat_result_offset;
    }
  }

  gsc->gsc_cursor_grid =
      cm_malloc(cm, sizeof(graphd_value) * (rmax + 1) * (emax + 1));

  if (gsc->gsc_cursor_grid == NULL) {
    cl_log(cl, CL_LEVEL_ERROR, "failed to allocate %lu bytes for cursor",
           (unsigned long)(sizeof(graphd_value) * (rmax + 1) * (emax + 1)));
    return ENOMEM;
  }
  gsc->gsc_cursor_grid_width = emax + 1;
  gsc->gsc_cursor_grid_n = (rmax + 1) * (emax + 1);
  for (i = 0; i < gsc->gsc_cursor_grid_n; i++)
    graphd_value_initialize(gsc->gsc_cursor_grid + i);

  for (pat = head; pat != NULL; pat = pat->pat_next) {
    graphd_value *val;
    char ribuf[200];

    cl_assert(cl, pat->pat_element_offset <= emax);
    cl_assert(cl, pat->pat_result_offset <= rmax);

    cl_log(cl, CL_LEVEL_SPEW, "cursor text: %.*s, sort instruction %s",
           (int)(cur_e - cur_s), cur_s,
           graphd_pattern_dump(pat, ribuf, sizeof ribuf));

    val = gsc->gsc_cursor_grid +
          gsc->gsc_cursor_grid_width * pat->pat_result_offset +
          pat->pat_element_offset;

    graphd_value_finish(cl, val);
    err = graphd_value_deserialize(gsc->gsc_graphd, cm, cl, val, &cur_s, cur_e);
    if (err != 0) {
      cl_cover(cl);
      return err;
    }

    cl_log(cl, CL_LEVEL_SPEW, "deserialized cursor element %s",
           graphd_value_to_string(val, ribuf, sizeof ribuf));
    cl_cover(cl);
  }

  if (cur_s < cur_e) {
    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_sort_cursor_set: leftover text in cursor: %.*s",
           (int)(cur_e - cur_s), cur_s);
    cl_cover(cl);
    return GRAPHD_ERR_LEXICAL;
  }
  return 0;
}

/**
 * @brief What direction should iterators be run for this pattern?
 *
 * @param pat	NULL or a sort pattern
 *
 * @return GRAPHD_DIRECTION_ANY if there is no preferred ordering.
 * @return GRAPHD_DIRECTION_BACKWARD if it would be good
 *	to produce results highest IDs (most recently added) first.
 * @return GRAPHD_DIRECTION_FORWARD if it would be good to produce
 *	results smallest IDs (first added) first.
 */
graphd_direction graphd_sort_iterator_direction(graphd_pattern const *pat) {
  if (pat == NULL) return GRAPHD_DIRECTION_ANY;

  if (pat->pat_type == GRAPHD_PATTERN_LIST)
    if ((pat = pat->pat_list_head) == NULL) return GRAPHD_DIRECTION_FORWARD;

  if (pat->pat_type == GRAPHD_PATTERN_TIMESTAMP ||
      pat->pat_type == GRAPHD_PATTERN_GUID)
    return pat->pat_sort_forward ? GRAPHD_DIRECTION_FORWARD
                                 : GRAPHD_DIRECTION_BACKWARD;

  return GRAPHD_DIRECTION_ANY;
}

/**
 * @brief Is sorted iterator output already sorted for this pattern?
 *
 *  Given this sort pattern and this iterator, do we still need
 *  to sort the results coming out of the iterator?
 *
 * @param g		database module handle
 * @param pat		NULL or a sort pattern
 * @param it		iterator created based on the pattern.
 *
 * @return true 	if a sort context is needed to sort the iterator output
 */
bool graphd_sort_needed(graphd_request *greq, graphd_constraint const *con,
                        pdb_iterator const *it) {
  graphd_handle *g = graphd_request_graphd(greq);
  graphd_pattern const *pat = con->con_sort;
  pdb_handle *pdb = g->g_pdb;
  cl_handle *cl = graphd_request_cl(greq);

  if (!con->con_sort_valid) return false;

  if (pat == NULL || pdb_iterator_null_is_instance(pdb, it)) return false;

  if (pat->pat_type == GRAPHD_PATTERN_LIST)
    if ((pat = pat->pat_list_head) == NULL) return false;

  if (pat->pat_type == GRAPHD_PATTERN_TIMESTAMP ||
      pat->pat_type == GRAPHD_PATTERN_GUID)

    return !pdb_iterator_sorted(g->g_pdb, it) ||
           pdb_iterator_forward(g->g_pdb, it) != pat->pat_sort_forward;

  /*  If the constraint has a sort root, and its
   *  sort root constraint is itself, and the
   *  iterator is ordered, and the ordering has
   *  the same sort root as the constraint,
   *  and this is a GUID- or timestamp-based sort,
   *  we're good, too.
   */
  if (con->con_sort_root.sr_con == con && pdb_iterator_ordered(g->g_pdb, it) &&
      pattern_is_perfect(con->con_sort_root.sr_pat.pat_type)) {
    char const *s = pdb_iterator_ordering(g->g_pdb, it);
    char const *e = s ? s + strlen(s) : NULL;
    graphd_sort_root sr;

    if (s != NULL && graphd_sort_root_from_string(greq, s, e, &sr) == 0 &&
        graphd_sort_root_equal(cl, &sr, &con->con_sort_root))

      return false;
  }
  return true;
}

int graphd_sort_suspend(cm_handle *cm, cl_handle *cl,
                        graphd_sort_context *gsc) {
  if (gsc != NULL && gsc->gsc_cursor_grid != NULL) {
    size_t i;
    graphd_value *v;

    v = gsc->gsc_cursor_grid;
    i = gsc->gsc_cursor_grid_n;

    while (i-- > 0) {
      int err = graphd_value_suspend(cm, cl, v++);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_value_suspend", err,
                     "can't suspend cursorgrid "
                     "value");
        return err;
      }
    }
  }
  return 0;
}

int graphd_sort_unsuspend(cm_handle *cm, cl_handle *cl,
                          graphd_sort_context *gsc) {
  return 0;
}

static bool graphd_sort_check_pattern(graphd_request *greq,
                                      graphd_constraint const *con,
                                      graphd_pattern const *head,
                                      bool in_pick) {
  if (head == NULL) return true;

  if (GRAPHD_PATTERN_IS_SET_VALUE(head->pat_type) ||
      (!in_pick && (head->pat_type == GRAPHD_PATTERN_LITERAL ||
                    head->pat_type == GRAPHD_PATTERN_NONE))) {
    char buf[200];
    graphd_request_errprintf(greq, false, "SEMANTICS cannot sort by %s",
                             graphd_pattern_dump(head, buf, sizeof buf));
    return false;
  }

  if (GRAPHD_PATTERN_IS_COMPOUND(head->pat_type)) {
    graphd_pattern *sub;
    for (sub = head->pat_list_head; sub != NULL; sub = sub->pat_next) {
      if (!graphd_sort_check_pattern(
              greq, con, sub, in_pick || head->pat_type == GRAPHD_PATTERN_PICK))
        return false;
    }
  }
  return true;
}

/* @brief Check whether sort constraints are semantically valid.
 */
int graphd_sort_check(graphd_request *greq, graphd_constraint const *con) {
  graphd_constraint *sub;

  /* Recurse into subconstraints.
   */
  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    int err = graphd_sort_check(greq, sub);
    if (err != 0) return err;
  }

  if (!graphd_sort_check_pattern(greq, con, con->con_sort, false))
    return GRAPHD_ERR_SEMANTICS;

  return 0;
}
