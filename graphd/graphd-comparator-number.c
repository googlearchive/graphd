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

#define NVS_MAGIC 0xa9aef6c

#define GREQ_PDB(r) (graphd_request_graphd(r)->g_pdb)

/*
 * Keep track of state when iterating over a range of bins
 */
typedef struct number_vrange_state {
  /* The magic number above */
  unsigned long nvs_magic;

  /* The lowest bin we wish to iterator over */
  int nvs_lo_bin;
  /* The highest bin we with to iterator over */
  int nvs_hi_bin;
  /* The next bin to iterator over */
  int nvs_cur_bin;

  enum { HMAP, BINS } nvs_cur_mode;

  enum {
    NUMBERS,
    STRINGS

  } nvs_cur_binset;
  graph_number nvs_lo_num;
  graph_number nvs_hi_num;

} number_vrange_state;

#if 0
int number_advance(cl_handle * cl, number_vrange_state * state, bool forward)
{




}
#endif

static int number_syntax(graphd_request *greq,
                         graphd_string_constraint const *strcon) {
  /*
   * The case comparator doesn't support ~=
   */
  if (strcon->strcon_op == GRAPHD_OP_MATCH) {
    graphd_request_error(greq,
                         "SEMANTICS cannot use ~= with comparator=\"number\"");
    return GRAPHD_ERR_SEMANTICS;
  }

  return 0;
}

static int equality_iterator(graphd_request *greq, int operation, const char *s,
                             const char *e, unsigned long long low,
                             unsigned long long high,
                             graphd_direction direction, const char *ordering,
                             bool *indexed_inout, pdb_iterator **it_out) {
  pdb_handle *pdb;
  pdb_iterator *sub_it;
  cl_handle *cl = graphd_request_cl(greq);
  int err;
  bool forward = direction != GRAPHD_DIRECTION_BACKWARD;

  /*  If this returns 0 and *it_out is NULL,
   *  it just couldn't make a useful iterator restriction
   *  for this expression -- that's not an error.
   */
  *it_out = NULL;
  pdb = graphd_request_graphd(greq)->g_pdb;
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

static int graphd_number_compare(graphd_request *greq, const char *a_s,
                                 const char *a_e, const char *b_s,
                                 const char *b_e) {
  graph_number an, bn;

  int ea, eb;

  if ((a_s == NULL) && (b_s == NULL)) return 0;

  if (a_s == NULL) return 1;

  if (b_s == NULL) return -1;

  ea = graph_decode_number(a_s, a_e, &an, true);

  eb = graph_decode_number(b_s, b_e, &bn, true);

  if ((!ea) && (!eb)) {
    /* both are actually numbers */
    return graph_number_compare(&an, &bn);
  } else {
    int r;
    /*
     * if A is a number and B isn't, A goes first.
     */
    if (!ea) return -1;

    /*
     * if B is a number and A isn't, B goes first.
     */
    if (!eb) return 1;

    /*
     * If neither is a number, revert to casecmp
     */
    r = graph_strcasecmp(a_s, a_e, b_s, b_e);
    return r;
  }
}

static size_t number_vrange_size(graphd_request *greq, const char *lo_s,
                                 const char *lo_e, const char *hi_s,
                                 const char *hi_e) {
  return sizeof(number_vrange_state);
}

static int number_vrange_start(graphd_request *greq, graphd_value_range *vr,
                               void *private_data)

{
  pdb_handle *pdb = GREQ_PDB(greq);
  number_vrange_state *state = private_data;
  int err;

  if (state->nvs_magic == NVS_MAGIC) {
    state->nvs_cur_bin = graphd_vrange_forward(greq, vr)
                             ? state->nvs_lo_bin
                             : state->nvs_hi_bin - 1;
    state->nvs_cur_mode = HMAP;
    cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW, "number_vrange resetting %p",
           private_data);
    return 0;
  }

  cl_assert(graphd_request_cl(greq), state->nvs_magic == 0);

  /*
   * If the range doesn't include anything, we need to get out
   * of here now
   */
  if (graphd_number_compare(greq, vr->vr_lo_s, vr->vr_lo_e, vr->vr_hi_s,
                            vr->vr_hi_e) > 0)
    return GRAPHD_ERR_NO;

  err = graph_decode_number(vr->vr_lo_s, vr->vr_lo_e, &state->nvs_lo_num, true);

  if (err) {
    cl_log_errno(graphd_request_cl(greq), CL_LEVEL_VERBOSE,
                 "graph_decode_number", err, "%.*s is not a number",
                 (int)(vr->vr_lo_e - vr->vr_lo_s), vr->vr_lo_s);

    /*
     * Everything is okay, but I don't feel like
     * indexing you.
     */
    return ENOTSUP;
  }

  err = graph_decode_number(vr->vr_hi_s, vr->vr_hi_e, &state->nvs_hi_num, true);

  if (err) {
    cl_log_errno(graphd_request_cl(greq), CL_LEVEL_VERBOSE,
                 "graph_decode_number:", err, "%.*s is not a number",
                 (int)(vr->vr_hi_e - vr->vr_hi_s), vr->vr_hi_s);

    /*
     * Everything is okay, but I don't feel like
     * indexing you.
     */
    return ENOTSUP;
  }

  state->nvs_lo_bin =
      pdb_bin_lookup(pdb, PDB_BINSET_NUMBERS, &state->nvs_lo_num,
                     &state->nvs_lo_num + 1, NULL);

  cl_assert(graphd_request_cl(greq), state->nvs_lo_bin > 0);

  state->nvs_hi_bin =
      pdb_bin_lookup(pdb, PDB_BINSET_NUMBERS, &state->nvs_hi_num,
                     &state->nvs_hi_num + 1, NULL) +
      1;

  cl_assert(graphd_request_cl(greq), state->nvs_hi_bin > 0);

  if (graphd_vrange_forward(greq, vr)) {
    state->nvs_cur_bin = state->nvs_lo_bin;
    state->nvs_cur_mode = HMAP;
  } else {
    state->nvs_cur_bin = state->nvs_hi_bin - 1;
    state->nvs_cur_mode = BINS;
  }

  cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW,
         "NUMBERS VRANGE: %.*s (%i) to %.*s (%i)",
         (int)(vr->vr_lo_e - vr->vr_lo_s), vr->vr_lo_s, state->nvs_lo_bin,
         (int)(vr->vr_hi_e - vr->vr_hi_s), vr->vr_hi_s, state->nvs_hi_bin);

  cl_assert(graphd_request_cl(greq), state->nvs_hi_bin >= state->nvs_lo_bin);

  state->nvs_magic = NVS_MAGIC;

  return 0;
}

static int number_vrange_it_next(graphd_request *greq, graphd_value_range *vr,
                                 void *private_data, pdb_id low, pdb_id high,
                                 pdb_iterator **it_out, pdb_budget *budget) {
  int err = 0;
  number_vrange_state *state = private_data;
  cl_assert(graphd_request_cl(greq), state->nvs_magic == NVS_MAGIC);

  if ((state->nvs_cur_bin == state->nvs_hi_bin)) return GRAPHD_ERR_NO;
  if (state->nvs_cur_bin < state->nvs_lo_bin) return GRAPHD_ERR_NO;

  if (state->nvs_cur_mode == BINS) {
    err = pdb_bin_to_iterator(GREQ_PDB(greq), state->nvs_cur_bin, low, high,
                              true, false, it_out);
    if (err && err != GRAPHD_ERR_NO) {
      cl_log_errno(
          graphd_request_cl(greq), CL_LEVEL_FAIL, "pdb_bin_to_iterator", err,
          "Can't get iterator for (number) bin %i", state->nvs_cur_bin);
      *budget -= PDB_COST_ITERATOR;
      return err;
    }
  } else if (state->nvs_cur_mode == HMAP) {
    graph_number *num;

    pdb_bin_value(GREQ_PDB(greq), PDB_BINSET_NUMBERS, state->nvs_cur_bin,
                  (void *)&num);

    err =
        pdb_hash_number_iterator(GREQ_PDB(greq), num, low, high, true, it_out);

    if (err && err != GRAPHD_ERR_NO) {
      cl_log_errno(graphd_request_cl(greq), CL_LEVEL_FAIL,
                   "pdb_hash_number_iterator", err,
                   "can't get hmap for number equality");
      return err;
    }
  } else
    cl_notreached(graphd_request_cl(greq), "eieie! nvs_cur_mode is illega: %i",
                  state->nvs_cur_mode);

  cl_assert(graphd_request_cl(greq), (err == 0) || (err == GRAPHD_ERR_NO));

  budget -= PDB_COST_ITERATOR;

  if (state->nvs_cur_mode == HMAP) {
    state->nvs_cur_mode = BINS;

    if (!graphd_vrange_forward(greq, vr)) state->nvs_cur_bin--;
  } else {
    state->nvs_cur_mode = HMAP;
    if (graphd_vrange_forward(greq, vr)) state->nvs_cur_bin++;
  }

  cl_assert(graphd_request_cl(greq), *it_out);
  return 0;
}

static int number_vrange_statistics(graphd_request *greq,
                                    graphd_value_range *vr, void *private_data,
                                    unsigned long long *total_ids,
                                    pdb_budget *next_cost, pdb_budget *budget)

{
  pdb_handle *pdb = GREQ_PDB(greq);
  long long diff, tot;
  number_vrange_state *state = private_data;
  cl_handle *cl = graphd_request_cl(greq);

  cl_assert(cl, state->nvs_magic == NVS_MAGIC);

  diff = state->nvs_hi_bin - state->nvs_lo_bin + 1;

  tot = pdb_bin_end(pdb, PDB_BINSET_NUMBERS) -
        pdb_bin_start(pdb, PDB_BINSET_NUMBERS);

  cl_assert(cl, tot > 0);

  *total_ids = diff * (1 + pdb_primitive_n(pdb) / (tot * 4));
  *next_cost = PDB_COST_HMAP_ELEMENT;

  return 0;
}

static int number_vrange_seek(graphd_request *greq, graphd_value_range *vr,
                              void *private_data, const char *s, const char *e,
                              pdb_id id, pdb_id low, pdb_id high,
                              pdb_iterator **it_out) {
  graph_number curn;

  int err;
  int bin;
  pdb_iterator *it;
  cl_handle *cl = graphd_request_cl(greq);
  pdb_handle *pdb = GREQ_PDB(greq);
  number_vrange_state *state = private_data;
  bool exact;

  cl_assert(cl, state->nvs_magic == NVS_MAGIC);

  err = graph_decode_number(s, e, &curn, true);

  /*
   * This should only happen if we've someone corrupted a
   * cursor.
   */
  if (err) {
    cl_log_errno(cl, CL_LEVEL_INFO, "graph_decode_number", err,
                 "%.*s isn't a number", (int)(e - s), s);
    return err;
  }

  bin = pdb_bin_lookup(pdb, PDB_BINSET_NUMBERS, s, e, &exact);

  if (bin < state->nvs_lo_bin || bin > state->nvs_hi_bin) {
    cl_log(cl, CL_LEVEL_INFO,
           "number_vrange_seek: bin %i is out of range %i..%i", bin,
           state->nvs_lo_bin, state->nvs_hi_bin);
    return GRAPHD_ERR_SEMANTICS;
  }

  if (exact) {
    err = pdb_hash_number_iterator(GREQ_PDB(greq), &curn, low, high, true, &it);
    state->nvs_cur_mode = HMAP;
  } else {
    /*
     * BIN
     */
    err = pdb_bin_to_iterator(pdb, bin, low, high, true, false, &it);
    state->nvs_cur_mode = BINS;
  }

  if (err) {
    cl_log_errno(cl, CL_LEVEL_VERBOSE, "pdb_bin_to_iterator", err,
                 "Can't thaw iterator for bin %i", (int)(bin));
    return err;
  }

  err = pdb_iterator_find_nonstep(pdb, it, id, &id);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_VERBOSE, "pdb_iterator_find_nonstep", err,
                 "Error while fast-forwarding vrange oterator over"
                 " bin %i to id %llu",
                 bin, (unsigned long long)id);
    return err;
  }

  state->nvs_cur_bin = bin;
  /*
   * We recovered the current bin. Now move our state
   * to the next one
   */
  if (state->nvs_cur_mode == HMAP) {
    state->nvs_cur_mode = BINS;

    if (!graphd_vrange_forward(greq, vr)) state->nvs_cur_bin--;
  } else {
    state->nvs_cur_mode = HMAP;
    if (graphd_vrange_forward(greq, vr)) state->nvs_cur_bin++;
  }

  *it_out = it;

  return 0;
}

/*
 * Return true if s..e cur is _beyond_ s..e. I.e. nothing <=
 * s..e will ever be returned in the future.
 */
static int number_value_in_range(graphd_request *greq, graphd_value_range *vr,
                                 void *private_data, const char *s,
                                 const char *e, bool *string_in_range)

{
  graph_number test_n;
  graph_number const *bin_n;
  number_vrange_state *state = private_data;
  int bin;
  int err;
  int rel;
  cl_handle *cl = graphd_request_cl(greq);

  cl_assert(graphd_request_cl(greq), state->nvs_magic == NVS_MAGIC);

  bin = state->nvs_cur_bin;

  if (bin == 0) {
    if (graphd_vrange_forward(greq, vr)) {
      *string_in_range = false;
      return 0;
    }
  }

  cl_assert(cl, bin < pdb_bin_end(GREQ_PDB(greq), PDB_BINSET_NUMBERS));

  /*
   * Find the bin that we're currently returning
   */
  bin += graphd_vrange_forward(greq, vr) ? -1 : 1;

  pdb_bin_value(GREQ_PDB(greq), PDB_BINSET_NUMBERS, bin, (void *)&bin_n);

  err = graph_decode_number(s, e, &test_n, true);
  if (err) {
    cl_log(graphd_request_cl(greq), CL_LEVEL_ERROR,
           "number_value_in_range: Got non number '%.*s'"
           " (corrupt database or comparator bug)",
           (int)(e - s), s);
    return GRAPHD_ERR_LEXICAL;
  }

  rel = graph_number_compare(&test_n, bin_n);
  if (!graphd_vrange_forward(greq, vr)) rel *= -1;

  if (rel < 0) {
    cl_log(graphd_request_cl(greq), CL_LEVEL_DETAIL,
           "number_value_in_range. %.*s is in range (bin %i). yay!",
           (int)(e - s), s, bin);
    *string_in_range = true;
    return 0;
  } else {
    *string_in_range = false;
    cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW,
           "number_value_in_rnage: %.*s not in range (bin %i). more work!",
           (int)(e - s), s, bin);
    return 0;
  }

  return 0;
}

static int number_vrange_freeze(graphd_request *greq, graphd_value_range *vr,
                                void *private_data, cm_buffer *buf)

{
  int err;
  number_vrange_state *state = private_data;

  cl_assert(graphd_request_cl(greq), state->nvs_magic == NVS_MAGIC);
  err =
      cm_buffer_sprintf(buf, "%d,%d", state->nvs_cur_mode, state->nvs_cur_bin);

  return err;
}

static int number_vrange_thaw(graphd_request *greq, graphd_value_range *vr,
                              void *private_data, const char *s,
                              const char *e) {
  int err;
  number_vrange_state *state = private_data;
  cl_handle *cl;

  cl = graphd_request_cl(greq);
  cl_assert(cl, state->nvs_magic == NVS_MAGIC);

  err = pdb_iterator_util_thaw(GREQ_PDB(greq), &s, e, "%d,%d",
                               &state->nvs_cur_mode, &state->nvs_cur_bin);

  if (err) {
    cl_log(graphd_request_cl(greq), CL_LEVEL_ERROR,
           "number_vrange_thaw: can't parse %%d,%%d out of '%.*s'",
           (int)(e - s), s);
    return GRAPHD_ERR_LEXICAL;
  }

  switch (state->nvs_cur_mode) {
    case BINS:
    case HMAP:
      break;

    default:
      cl_log(cl, CL_LEVEL_ERROR, "number_vrange_thaw: cur_mode(%i) is illegal",
             state->nvs_cur_mode);
      return GRAPHD_ERR_LEXICAL;
  }

  if (state->nvs_cur_bin < state->nvs_lo_bin ||
      state->nvs_cur_bin > state->nvs_hi_bin) {
    cl_log(cl, CL_LEVEL_ERROR,
           "number_vrange_thaw: cur bin(%i) is outside lo..hi "
           "(%i..%i)",
           state->nvs_cur_bin, state->nvs_lo_bin, state->nvs_hi_bin);
    return GRAPHD_ERR_LEXICAL;
  }

  return 0;
}

static char const *const graphd_comparator_number_aliases[] = {"numeric", NULL};

graphd_comparator const graphd_comparator_number[1] = {
    {.cmp_locale = "",
     .cmp_name = "number",
     .cmp_alias = graphd_comparator_number_aliases,
     .cmp_syntax = number_syntax,
     .cmp_eq_iterator = equality_iterator,
     .cmp_iterator_range = NULL,
     .cmp_glob = NULL,
     .cmp_sort_compare = graphd_number_compare,
     .cmp_vrange_size = number_vrange_size,
     .cmp_vrange_start = number_vrange_start,
     .cmp_vrange_it_next = number_vrange_it_next,
     .cmp_vrange_statistics = number_vrange_statistics,
     .cmp_vrange_seek = number_vrange_seek,
     .cmp_value_in_range = number_value_in_range,
     .cmp_vrange_freeze = number_vrange_freeze,
     .cmp_vrange_thaw = number_vrange_thaw,
     .cmp_lowest_string = "-inf",
     .cmp_highest_string = "inf"

    }};
