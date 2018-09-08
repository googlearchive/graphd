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

#include "libpdb/pdb.h"
#include "libcm/cm.h"

#define CVS_MAGIC 0x5faed32

#define GREQ_PDB(r) (graphd_request_graphd(r)->g_pdb)

/*
 * Keep track of state when iterating over a range of bins
 */
typedef struct case_vrange_state {
  /* The magic number above */
  unsigned long cvs_magic;

  /* The lowest bin we wish to iterator over */
  int cvs_lo_bin;
  /* The highest bin we with to iterator over */
  int cvs_hi_bin;
  /* The next bin to iterator over */
  int cvs_cur_bin;

} case_vrange_state;

static int case_syntax(graphd_request *greq,
                       graphd_string_constraint const *strcon) {
  cl_handle *cl = cl = graphd_request_cl(greq);

  /*
   * The case comparator doesn't support ~=
   */
  if (strcon->strcon_op == GRAPHD_OP_MATCH) {
    graphd_request_error(greq, "SYNTAX cannot use ~= with comparator=\"case\"");
    return GRAPHD_ERR_SEMANTICS;
  }

  return 0;
}
static int equality_iterator(graphd_request *greq, int operation, const char *s,
                             const char *e, unsigned long long low,
                             unsigned long long high,
                             graphd_direction direction, const char *ordering,
                             bool *indexed_inout, pdb_iterator **it_out) {
  pdb_handle *pdb = graphd_request_graphd(greq)->g_pdb;
  cl_handle *cl = graphd_request_cl(greq);
  pdb_iterator *sub_it;
  int err;
  bool forward = direction != GRAPHD_DIRECTION_BACKWARD;

  /*  If this returns 0 and *it_out is NULL,
   *  it just couldn't make a useful iterator restriction
   *  for this expression -- that's not an error.
   */
  *it_out = NULL;
  cl_assert(cl, GRAPHD_DIRECTION_VALID(direction));

  /*
   * This comparator doesn't support ~= and the parser should
   * enforce that.
   */
  cl_assert(cl, operation != GRAPHD_OP_MATCH);
  if (operation == GRAPHD_OP_EQ) {
    /*
     * We don't have any iterators for value=NULL so get out
     * now if thats what you're asking for
     */
    if (s == NULL) return 0;

    err = pdb_hash_iterator(pdb, PDB_HASH_VALUE, s, (e - s), low, high, forward,
                            &sub_it);
    if (err) return err;

    if (err == 0) {
      *it_out = sub_it;
      *indexed_inout = true;
    }
    return err;
  }

  return 0;
}

static int case_sort_compare(graphd_request *greq, char const *s1,
                             char const *e1, char const *s2, char const *e2) {
  return graph_strcasecmp(s1, e1, s2, e2);
}

static size_t case_vrange_size(graphd_request *greq, const char *lo_s,
                               const char *lo_e, const char *hi_s,
                               const char *hi_e) {
  return sizeof(case_vrange_state);
}

static int case_vrange_start(graphd_request *greq, graphd_value_range *vr,
                             void *private_data) {
  case_vrange_state *state = private_data;

  if (state->cvs_magic == CVS_MAGIC) {
    if (graphd_vrange_forward(greq, vr)) {
      state->cvs_cur_bin = state->cvs_lo_bin;
    } else {
      state->cvs_cur_bin = state->cvs_hi_bin;
    }

    cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW, "case_vrange resetting %p",
           private_data);
    return 0;
  }

  cl_assert(graphd_request_cl(greq), state->cvs_magic == 0);

  state->cvs_magic = CVS_MAGIC;

  /*
   * if hi < low, report that we'll never return anything.
   */
  if (case_sort_compare(greq, vr->vr_lo_s, vr->vr_lo_e, vr->vr_hi_s,
                        vr->vr_hi_e) > 0)
    return GRAPHD_ERR_NO;

  state->cvs_lo_bin = pdb_bin_lookup(GREQ_PDB(greq), PDB_BINSET_STRINGS,
                                     vr->vr_lo_s, vr->vr_lo_e, NULL);

  cl_log(graphd_request_cl(greq), CL_LEVEL_VERBOSE,
         "case_vrange_start: low bin \"%.*s\" is %d",
         (int)(vr->vr_lo_e - vr->vr_lo_s), vr->vr_lo_s, state->cvs_lo_bin);

  if (vr->vr_hi_s == NULL) {
    state->cvs_hi_bin = pdb_bin_end(GREQ_PDB(greq), PDB_BINSET_STRINGS) +
                        (vr->vr_hi_strict ? 0 : 1);
  } else {
    state->cvs_hi_bin = pdb_bin_lookup(GREQ_PDB(greq), PDB_BINSET_STRINGS,
                                       vr->vr_hi_s, vr->vr_hi_e, NULL) +
                        1;

    cl_log(graphd_request_cl(greq), CL_LEVEL_VERBOSE,
           "case_vrange_start: high bin \"%.*s\" is %d",
           (int)(vr->vr_hi_e - vr->vr_hi_s), vr->vr_hi_s, state->cvs_hi_bin);
  }

  if (graphd_vrange_forward(greq, vr))
    state->cvs_cur_bin = state->cvs_lo_bin;
  else
    state->cvs_cur_bin = state->cvs_hi_bin - 1;
  cl_assert(graphd_request_cl(greq), state->cvs_hi_bin >= state->cvs_lo_bin);
  cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW,
         "case_vrange_start: will iterate from bin %i to %i", state->cvs_lo_bin,
         state->cvs_hi_bin);
  return 0;
}

static int case_vrange_it_next(graphd_request *greq, graphd_value_range *vr,
                               void *private_data, pdb_id low, pdb_id high,
                               pdb_iterator **it_out, pdb_budget *budget)

{
  cl_handle *cl = graphd_request_cl(greq);
  pdb_handle *pdb = GREQ_PDB(greq);
  int err = 0;
  case_vrange_state *state = private_data;

  cl_assert(cl, state->cvs_magic == CVS_MAGIC);

  *it_out = NULL;
  for (;;) {
    if (graphd_vrange_forward(greq, vr) &&
        state->cvs_cur_bin >= state->cvs_hi_bin)
      return GRAPHD_ERR_NO;

    /*
     * XXX
     *
     * we compare against -1 here.  That's okay although a
     * bit of a kludge, but I'm not sure what happens when we
     * freeze something that's about to say GRAPHD_ERR_NO.
     */
    if (!graphd_vrange_forward(greq, vr) &&
        state->cvs_cur_bin < state->cvs_lo_bin)
      return GRAPHD_ERR_NO;

    if (state->cvs_cur_bin == pdb_bin_end(pdb, PDB_BINSET_STRINGS)) {
      err = graphd_iterator_null_value_create(greq, low, high, it_out);
    } else {
      err = pdb_bin_to_iterator(pdb, state->cvs_cur_bin, low, high,
                                true, /* forward */
                                true, /* error-if-null */
                                it_out);
    }

    *budget -= PDB_COST_ITERATOR;

    if (err != 0 && err != GRAPHD_ERR_NO) return err;

    if (err == 0 && pdb_iterator_null_is_instance(pdb, *it_out)) {
      cl_log(cl, CL_LEVEL_ERROR, "Unexpected NULL iterator. Continuing.");
      pdb_iterator_destroy(pdb, it_out);
      cl_assert(cl, *it_out == NULL);
    }
    err = 0;

    if (graphd_vrange_forward(greq, vr))
      state->cvs_cur_bin++;
    else
      state->cvs_cur_bin--;
    if (err == 0 && *it_out != NULL) return 0;

    if (*budget <= 0) return PDB_ERR_MORE;
  }

  return err;
}

static int case_vrange_statistics(graphd_request *greq, graphd_value_range *vr,
                                  void *private_state,
                                  unsigned long long *total_ids,
                                  pdb_budget *next_cost, pdb_budget *budget) {
  cl_handle *cl;
  case_vrange_state *state;

  int sit;

  cl = graphd_request_cl(greq);
  state = private_state;

  cl_assert(cl, state->cvs_magic == CVS_MAGIC);

  cl_assert(cl, state->cvs_lo_bin <= state->cvs_hi_bin);

  sit = state->cvs_hi_bin - state->cvs_lo_bin + 1;

  *next_cost = PDB_COST_HMAP_ELEMENT;

  /*
   * Estimate the total number if IDs that we'll return.
   * Assume that each bin has at least one ID, and that
   * roughly half of the primitive in graphd have values.
   */
  *total_ids =
      sit * (1 +
             pdb_primitive_n(GREQ_PDB(greq)) /
                 (pdb_bin_end(GREQ_PDB(greq), PDB_BINSET_STRINGS) * 2));
  return 0;
}

static int case_vrange_seek(graphd_request *greq, graphd_value_range *vr,
                            void *private_data, const char *s, const char *e,
                            pdb_id id, pdb_id low, pdb_id high,
                            pdb_iterator **it_out) {
  cl_handle *cl = graphd_request_cl(greq);
  pdb_handle *pdb = GREQ_PDB(greq);
  int err;
  int bin;
  pdb_iterator *it;

  case_vrange_state *state = private_data;

  cl_assert(cl, state->cvs_magic == CVS_MAGIC);

  bin = pdb_bin_lookup(pdb, PDB_BINSET_STRINGS, s, e, NULL);

  cl_log(cl, CL_LEVEL_SPEW, "case_vrange_seek[%llu]: %.*s seeks to bin %i",
         (unsigned long long)id, (int)(e - s), s, bin);

  err = pdb_bin_to_iterator(pdb, bin, low, high, true, /*forward */
                            false,                     /* error-if-null */
                            &it);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_VERBOSE, "pdb_bin_to_iterator", err,
                 "Can't thaw iterator for bin %i", (int)(bin));
    return err;
  }
  err = pdb_iterator_find_nonstep(pdb, it, id, &id);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_VERBOSE, "pdb_iterator_find_nonstep", err,
                 "error while fast-forwading vrange iterator over bin"
                 " bin %i to %llu",
                 bin, (unsigned long long)id);
    return err;
  }

  cl_log(cl, CL_LEVEL_SPEW, "vrange_seek: input: %.*s: moved to %i,%llu",
         (int)(e - s), s, bin, (unsigned long long)id);

  state->cvs_cur_bin = bin + (graphd_vrange_forward(greq, vr) ? 1 : -1);
  *it_out = it;

  return 0;
}

static int case_value_in_range(graphd_request *greq, graphd_value_range *vr,
                               void *private_state, const char *s,
                               const char *e, bool *string_in_range)

{
  cl_handle *cl;
  case_vrange_state *state;
  const char *bs, *be;
  int bin;

  cl = graphd_request_cl(greq);
  state = private_state;

  cl_assert(cl, state->cvs_magic == CVS_MAGIC);

  bin = state->cvs_cur_bin;

  if (bin == 0 && graphd_vrange_forward(greq, vr)) {
    *string_in_range = false;
    return 0;
  }
  if (bin == (pdb_bin_end(GREQ_PDB(greq), PDB_BINSET_STRINGS)) &&
      !graphd_vrange_forward(greq, vr)) {
    *string_in_range = false;
    return 0;
  }

  bin += graphd_vrange_forward(greq, vr) ? -1 : 1;

  {
    const char **v;
    pdb_bin_value(GREQ_PDB(greq), PDB_BINSET_STRINGS, bin, (void *)&v);

    bs = *v;
    if (bs)
      be = bs + strlen(bs);
    else
      be = NULL;
  }

  if (graphd_vrange_forward(greq, vr)) {
    if (graph_strcasecmp(s, e, bs, be) < 0)
      *string_in_range = true;
    else
      *string_in_range = false;
  } else {
    if (graph_strcasecmp(s, e, bs, be) >= 0)
      *string_in_range = true;
    else
      *string_in_range = false;
  }

  cl_log(cl, CL_LEVEL_SPEW, "check range %.*s vs %.*s: result %s", (int)(e - s),
         s, (int)(be - bs), bs, *string_in_range ? "true" : "false");

  return 0;
}

static int case_vrange_freeze(graphd_request *greq, graphd_value_range *vr,
                              void *private_data, cm_buffer *buf) {
  cl_handle *cl;
  case_vrange_state *state;

  int err;
  cl = graphd_request_cl(greq);
  state = private_data;

  cl_assert(cl, state->cvs_magic == CVS_MAGIC);

  err = cm_buffer_sprintf(buf, "%i", state->cvs_cur_bin);
  return err;
}

static int case_vrange_thaw(graphd_request *greq, graphd_value_range *vr,
                            void *private_data, const char *s, const char *e) {
  int err;
  case_vrange_state *state;
  cl_handle *cl;
  cl = graphd_request_cl(greq);

  state = private_data;

  cl_assert(cl, state->cvs_magic == CVS_MAGIC);

  err =
      pdb_iterator_util_thaw(GREQ_PDB(greq), &s, e, "%d", &state->cvs_cur_bin);

  if (err) {
    cl_log(graphd_request_cl(greq), CL_LEVEL_ERROR,
           "case_vrange_thaw: can't parse integer out of: %.*s", (int)(e - s),
           s);
    return GRAPHD_ERR_LEXICAL;
  }

  if (state->cvs_cur_bin < (state->cvs_lo_bin - 1)) {
    cl_log(graphd_request_cl(greq), CL_LEVEL_ERROR,
           "case_vrange_thaw: %i is outside range %i - %i", state->cvs_cur_bin,
           state->cvs_lo_bin, state->cvs_hi_bin);
    return GRAPHD_ERR_LEXICAL;
  }

  if (state->cvs_cur_bin > (state->cvs_hi_bin + 1)) {
    cl_log(graphd_request_cl(greq), CL_LEVEL_ERROR,
           "case_vrange_thaw: %i is outside range %i - %i", state->cvs_cur_bin,
           state->cvs_lo_bin, state->cvs_hi_bin);
    return GRAPHD_ERR_LEXICAL;
  }

  return 0;
}

/*
 * The datetime comparator is a comparator to compare values as iso dates.
 * For now, this can simply use the case-insensitive comparator which will
 * have the correct behavior. (But undefined-but-mostly-sane behavior if you
 * have invalid datetimes.)
 *
 * datetime is expected to evolve into its own comparator with specialized
 * knowledge.
 */

static char const *const graphd_comparator_case_aliases[] = {"case", NULL};

graphd_comparator const graphd_comparator_case[1] = {
    {.cmp_locale = "",
     .cmp_name = "case-insensitive",
     .cmp_alias = graphd_comparator_case_aliases,
     .cmp_syntax = case_syntax,
     .cmp_eq_iterator = equality_iterator,
     .cmp_iterator_range = NULL,
     .cmp_glob = NULL,
     .cmp_sort_compare = case_sort_compare,
     .cmp_vrange_size = case_vrange_size,
     .cmp_vrange_start = case_vrange_start,
     .cmp_vrange_it_next = case_vrange_it_next,
     .cmp_vrange_statistics = case_vrange_statistics,
     .cmp_vrange_seek = case_vrange_seek,
     .cmp_value_in_range = case_value_in_range,
     .cmp_vrange_freeze = case_vrange_freeze,
     .cmp_vrange_thaw = case_vrange_thaw,
     .cmp_lowest_string = "",
     .cmp_highest_string = NULL

    }};
