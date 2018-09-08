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

#define DTS_MAGIC 0x10face81

#define GREQ_PDB(r) (graphd_request_graphd(r)->g_pdb)

typedef enum {
  DTS_NEGATIVE,
  DTS_POSITIVE,
  DTS_TIME

} dts_mode_t;

typedef struct {
  dts_mode_t dp_mode;
  int dp_bin;

} datetime_position;

/*
 * Keep track of state when iterating over a range of bins
 */
typedef struct datetime_vrange_state {
  /* The magic number above */
  unsigned long dts_magic;

  datetime_position dts_lo;
  datetime_position dts_hi;
  datetime_position dts_cur;

  bool dts_eof;

} datetime_vrange_state;

/*
 * We calculate these limits once, the first time you try to
 * 'start' a datetime comparator
 */

static int minimum_negative_year_bin; /* the bin before -0 */
static int maximum_negative_year_bin; /* the last bin starting with -9999 */
static int minimum_positive_year_bin; /* the bin before 0 */
static int maximum_positive_year_bin; /* the last bin starting with  9999 */
static int minimum_time_bin;          /* the bin before T0 */
static int maximum_time_bin;          /* the bin T99 */

static bool generated_bin_limits = false;

static int generate_bin_limits(pdb_handle *pdb) {
  const char *negative0 = "-0";
  const char *negative9999 = "-999/";
  const char *positive0 = "0";
  const char *positive9999 = "999:";
  const char *t00 = "T00";
  const char *t24 = "T24";

  if (generated_bin_limits) return 0;

  generated_bin_limits = true;

  minimum_negative_year_bin =
      pdb_bin_lookup(pdb, PDB_BINSET_STRINGS, negative0,
                     negative0 + strlen(negative0), NULL) -
      1;

  maximum_negative_year_bin =
      pdb_bin_lookup(pdb, PDB_BINSET_STRINGS, negative9999,
                     negative9999 + strlen(negative9999), NULL) +
      1;

  minimum_positive_year_bin =
      pdb_bin_lookup(pdb, PDB_BINSET_STRINGS, positive0,
                     positive0 + strlen(positive0), NULL) -
      1;

  maximum_positive_year_bin =
      pdb_bin_lookup(pdb, PDB_BINSET_STRINGS, positive9999,
                     positive9999 + strlen(positive9999), NULL) +
      1;

  minimum_time_bin =
      pdb_bin_lookup(pdb, PDB_BINSET_STRINGS, t00, t00 + strlen(t00), NULL) - 1;

  maximum_time_bin =
      pdb_bin_lookup(pdb, PDB_BINSET_STRINGS, t24, t24 + strlen(t24), NULL) - 1;

  cl_log(pdb_log(pdb), CL_LEVEL_SPEW,
         "generate_bin_limits: datetime limits are"
         " mny: %i Mny: %i mpy: %i Mpy: %i mt: %i Mt: %i",
         minimum_negative_year_bin, maximum_negative_year_bin,
         minimum_positive_year_bin, maximum_positive_year_bin, minimum_time_bin,
         maximum_time_bin);

  return 0;
}

static const char *bin_to_string(pdb_handle *pdb, int bin) {
  const char **v;
  pdb_bin_value(pdb, PDB_BINSET_STRINGS, bin, (void *)&v);
  return *v;
}

/*
 * Degrement pm to the next bin we should look at. Return false if pm is
 * now less than *end
 */
static bool datetime_dec(pdb_handle *pdb, datetime_position *pm,
                         datetime_position *end) {
  cl_log(pdb_log(pdb), CL_LEVEL_SPEW, "Decrement: %i/%i (to %i/%i)", pm->dp_bin,
         pm->dp_mode, end->dp_bin, end->dp_mode);
  switch (pm->dp_mode) {
    case DTS_NEGATIVE:
      pm->dp_bin++;
      if (pm->dp_bin >= maximum_negative_year_bin) return true;
      break;

    case DTS_POSITIVE:
      pm->dp_bin--;
      if (pm->dp_bin < minimum_positive_year_bin) {
        pm->dp_mode = DTS_NEGATIVE;
        pm->dp_bin = minimum_negative_year_bin;
      }
      break;
    case DTS_TIME:
      pm->dp_bin--;
      if (pm->dp_bin < minimum_time_bin) {
        pm->dp_mode = DTS_POSITIVE;
        pm->dp_bin = maximum_positive_year_bin;
      }
      break;
    default:
      cl_notreached(pdb_log(pdb), "invald dp_mode: %i", pm->dp_mode);
  }

  if (end) {
    if (pm->dp_mode < end->dp_mode) return true;
    if (pm->dp_mode > end->dp_mode) return false;

    return (pm->dp_mode == DTS_NEGATIVE) ^ (pm->dp_bin < end->dp_bin);
  } else {
    return false;
  }
}

/*
 * Sometimes (about 30% of the time) we can prove that a particular bin
 * range simply cannot contain any legal dates.
 */
static bool datetime_skip(pdb_handle *pdb, datetime_position *p) {
  const char *before, *after;
  char test[5];
  size_t diff = 0;

  /*  If we're searching for repeated time periods,
   *  we have to look everywhere.
   */
  if (p->dp_mode == DTS_TIME) return false;

  before = bin_to_string(pdb, p->dp_bin);
  after = bin_to_string(pdb, p->dp_bin + 1);

  cl_assert(pdb_log(pdb), before != NULL);
  cl_assert(pdb_log(pdb), after != NULL);

  if (before[0] == '-' || after[0] == '-') return false;

  for (;; diff++) {
    if (!isdigit(before[diff]) || !isdigit(after[diff])) break;
  }

  if (diff >= 4) {
    cl_log(pdb_log(pdb), CL_LEVEL_SPEW,
           "datetime_skip: READ %s and %s are too long", before, after);
    return false;
  }

  strcpy(test, "0000");
  memcpy(test, after, diff);

  if (strcasecmp(before, test) <= 0 && strcasecmp(after, test) >= 0) {
    cl_log(pdb_log(pdb), CL_LEVEL_SPEW,
           "date_time_skip: READ %s sorts between %s and %s", test, before,
           after);
    return false;
  }
  /*
  else
  {
          cl_log(pdb_log(pdb), CL_LEVEL_SPEW,
                  "datetime_skip: ???? %s does not sort between %s and %s",
                  test, before, after);
  }
  */
  strcpy(test, "9999");
  memcpy(test, before, diff);

  if (strcasecmp(before, test) <= 0 && strcasecmp(after, test) >= 0) {
    cl_log(pdb_log(pdb), CL_LEVEL_SPEW,
           "date_time_skip: READ %s sorts between %s and %s", test, before,
           after);
    return false;
  } else
    /*
{

    cl_log(pdb_log(pdb), CL_LEVEL_SPEW,
            "datetime_skip: ???? %s does not sort between %s and %s",
            test, before, after);
}
*/
    cl_log(pdb_log(pdb), CL_LEVEL_SPEW,
           "datetime_skip: SKIP: no date sorts between %s and %s", before,
           after);

  return true;
}

/*
 * Increment pm to the next bin that we should look at.
 * Return true if pm is now equal to or greater than end.
 */

static bool datetime_inc(pdb_handle *pdb, datetime_position *pm,
                         datetime_position *end) {
  cl_log(pdb_log(pdb), CL_LEVEL_SPEW, "Increment: %i/%i", pm->dp_bin,
         pm->dp_mode);

  switch (pm->dp_mode) {
    case DTS_NEGATIVE:
      pm->dp_bin--;
      if (pm->dp_bin < minimum_negative_year_bin) {
        pm->dp_mode = DTS_POSITIVE;
        pm->dp_bin = minimum_positive_year_bin;
      }
      break;

    case DTS_POSITIVE:
      pm->dp_bin++;
      if (pm->dp_bin >= maximum_positive_year_bin) {
        pm->dp_mode = DTS_TIME;
        pm->dp_bin = minimum_time_bin;
      }
      break;

    case DTS_TIME:
      pm->dp_bin++;
      /*
       * Lets assume that we're bounded.
       */
      if (pm->dp_bin >= maximum_time_bin) return true;
      break;

    default:
      cl_notreached(pdb_log(pdb), "invalid dp_mode: %i", pm->dp_mode);
  }

  if (end) {
    if (pm->dp_mode > end->dp_mode) return true;
    if (pm->dp_mode < end->dp_mode) return false;

    /*
     * Reverse the test when we're going backwards through
     * negative dates
     */
    return (pm->dp_mode == DTS_NEGATIVE) ^ (pm->dp_bin >= end->dp_bin);
  } else {
    return false;
  }
}

static int datetime_syntax(graphd_request *greq,
                           graphd_string_constraint const *strcon) {
  cl_handle *cl = cl = graphd_request_cl(greq);

  /*
   * The datetime comparator doesn't support ~=
   */
  return 0;
}

static bool delimited_string_match(graphd_request *greq, const char *pat_s,
                                   const char *pat_e, const char *s,
                                   const char *e) {
  const char *p = pat_s, *c = s;

  char delim;

  cl_log(graphd_request_cl(greq), CL_LEVEL_INFO,
         "delimited_string_match: compare %.*s vs %.*s", (int)(pat_e - pat_s),
         pat_s, (int)(e - s), s);

  for (; p < pat_e; p++) {
    if (*p == '*') {
      if (p == (pat_e - 1)) return true;
      /*
       * Special case for negative years
       */
      if ((c == s) && (c[0] == '-')) c++;

      /*
       * XXX! if pat has a trailing delimiter that s doesn't have,
       * this still okay:
       * *-11-* should match 2000-11
       */
      for (delim = p[1]; (c < e) && (*c != delim); c++)
        ;
    } else {
      if (c >= e) return false;
      if (*p != *c) return false;
      c++;
    }
  }
  /*
   * Suffixes on s are okay
   */
  return true;
}

/*
 * Look at a date regex and come up with some iterators for it
 */
static int date_pattern_iterator(graphd_request *greq, const char *pat_s,
                                 const char *pat_e, pdb_id low, pdb_id high,
                                 graphd_direction direction,
                                 const char *ordering, pdb_iterator **it_out) {
  int err;
  const char *p; /* start of number fragment */
  const char *e; /* end of number fragment */
  graphd_handle *graphd = graphd_request_graphd(greq);

  /*
   * Find every sequence of numbers separated by a non-number
   * and throw it into an AND iterator
   */

  pdb_iterator *and_it;

  err = graphd_iterator_and_create(greq, 3, low, high, direction, ordering,
                                   &and_it);

  cl_assert(graphd_request_cl(greq), !err);

  for (p = pat_s; p < pat_e; p = e + 1) {
    pdb_iterator *newit;
    for (e = p; (e < pat_e) && isdigit(*e); e++)
      ;

    /*
     * No numbers? try again
     */
    if (e == p) continue;

    cl_log(graphd_request_cl(greq), CL_LEVEL_INFO,
           "add word iterator for: '%.*s'", (int)(e - p), p);

    if (e == pat_e) {
      err = graphd_iterator_prefix_create(greq, p, e, low, high, direction,
                                          &newit);
    } else {
      err = pdb_iterator_word_create(GREQ_PDB(greq), p, e, low, high,
                                     direction != GRAPHD_DIRECTION_BACKWARD,
                                     false, &newit);
    }

    if (err) {
      cl_log_errno(graphd_request_cl(greq), CL_LEVEL_ERROR,
                   "pdb_iterator_word_create", err,
                   "Cannot get word iterator for %.*s", (int)(e - p), p);

      goto free_and;
    }

    err = graphd_iterator_and_add_subcondition(graphd, and_it, &newit);

    pdb_iterator_destroy(GREQ_PDB(greq), &newit);
    if (err) {
      cl_log_errno(graphd_request_cl(greq), CL_LEVEL_ERROR,
                   "pdb_iterator_and_add_aubcondition", err,
                   "Cannot add new word iterator to and");

      goto free_and;
    }
  }

  err = graphd_iterator_and_create_commit(graphd, and_it);

  if (err) {
    cl_log_errno(graphd_request_cl(greq), CL_LEVEL_ERROR,
                 "pdb_iterator_and_create_commit", err,
                 "Can't commit this and iterator");
    goto free_and;
  }
  *it_out = and_it;

  return 0;

free_and:

  pdb_iterator_destroy(GREQ_PDB(greq), &and_it);
  cl_notreached(graphd_request_cl(greq), "eieieie!");
  return err;
}

static int equality_iterator(graphd_request *greq, int operation, const char *s,
                             const char *e, unsigned long long low,
                             unsigned long long high,
                             graphd_direction direction, const char *ordering,
                             bool *indexed_inout, pdb_iterator **it_out) {
  pdb_handle *pdb = graphd_request_graphd(greq)->g_pdb;
  pdb_iterator *sub_it;
  cl_handle *cl = graphd_request_cl(greq);
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
  if (operation == GRAPHD_OP_MATCH) {
    err = date_pattern_iterator(greq, s, e, low, high, direction, ordering,
                                it_out);
    return err;
  } else if (operation == GRAPHD_OP_EQ) {
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
  } else {
    return 0;
  }
}

static int datetime_sort_compare(graphd_request *greq, char const *s1,
                                 char const *e1, char const *s2,
                                 char const *e2) {
  /*  If both are BCE dates, reverse the normal sort order.
   *  (5 BCE came after 6 BCE)
   */
  if (s1 < e1 && s1[0] == '-' && s2 < e2 && s2[0] == '-')
    /*  Reverse the sort order by swapping sides.
     */
    return graph_strcasecmp(s2 + 1, e2, s1 + 1, e1);

  return graph_strcasecmp(s1, e1, s2, e2);
}

static size_t datetime_vrange_size(graphd_request *greq, const char *lo_s,
                                   const char *lo_e, const char *hi_s,
                                   const char *hi_e) {
  return sizeof(datetime_vrange_state);
}

static int datetime_string_to_bin(pdb_handle *pdb, const char *s, const char *e,
                                  bool forward, datetime_position *p) {
  int bin;

  if (s == NULL) {
    p->dp_mode = DTS_TIME;
    p->dp_bin = maximum_time_bin;
    return 0;
  }
  if (s == e) {
    p->dp_mode = DTS_NEGATIVE;
    p->dp_bin = maximum_negative_year_bin;
    return 0;
  }
  bin = pdb_bin_lookup(pdb, PDB_BINSET_STRINGS, s, e, NULL);
  if (s[0] == '-') {
    p->dp_mode = DTS_NEGATIVE;
    p->dp_bin = bin + 1;
  } else if (s[0] == 'T') {
    p->dp_mode = DTS_TIME;
    p->dp_bin = bin;
  } else if (isdigit(s[0])) {
    p->dp_mode = DTS_POSITIVE;
    p->dp_bin = bin;
  } else {
    cl_log(pdb_log(pdb), CL_LEVEL_ERROR,
           "datetime_string_to_bin: syntax_error : %.*s", (int)(e - s), s);
    return EILSEQ;
  }

  cl_log(pdb_log(pdb), CL_LEVEL_SPEW,
         "datetime_string_to_bin: string %.*s at %i/%i", (int)(e - s), s,
         p->dp_bin, p->dp_mode);
  return 0;
}

static int datetime_vrange_start(graphd_request *greq, graphd_value_range *vr,
                                 void *private_data) {
  datetime_vrange_state *state = private_data;
  int err;

  generate_bin_limits(GREQ_PDB(greq));

  if (state->dts_magic == DTS_MAGIC) {
    state->dts_eof = false;
    if (graphd_vrange_forward(greq, vr)) {
      state->dts_cur = state->dts_lo;
    } else {
      state->dts_cur = state->dts_hi;
    }

    state->dts_eof = false;
    cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW,
           "datetime_vrange resetting %p", private_data);
    return 0;
  }

  cl_assert(graphd_request_cl(greq), state->dts_magic == 0);

  state->dts_magic = DTS_MAGIC;

  /*
   * if hi < low, report that we'll never return anything.
   */
  if (datetime_sort_compare(greq, vr->vr_lo_s, vr->vr_lo_e, vr->vr_hi_s,
                            vr->vr_hi_e) > 0)
    return GRAPHD_ERR_NO;

  err = datetime_string_to_bin(GREQ_PDB(greq), vr->vr_lo_s, vr->vr_lo_e,
                               graphd_vrange_forward(greq, vr), &state->dts_lo);

  err = datetime_string_to_bin(GREQ_PDB(greq), vr->vr_hi_s, vr->vr_hi_e,
                               graphd_vrange_forward(greq, vr), &state->dts_hi);

  datetime_inc(GREQ_PDB(greq), &state->dts_hi, NULL);

  if (graphd_vrange_forward(greq, vr))
    state->dts_cur = state->dts_lo;
  else
    state->dts_cur = state->dts_hi;

  cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW,
         "datetime_vrange_start: will iterate from bin %i to %i",
         state->dts_lo.dp_bin, state->dts_hi.dp_bin);
  state->dts_eof = false;

  return 0;
}

static int datetime_vrange_it_next(graphd_request *greq, graphd_value_range *vr,
                                   void *private_data, pdb_id low, pdb_id high,
                                   pdb_iterator **it_out, pdb_budget *budget)

{
  cl_handle *cl = graphd_request_cl(greq);
  pdb_handle *pdb = GREQ_PDB(greq);
  int err = 0;
  datetime_vrange_state *state = private_data;

  cl_assert(cl, state->dts_magic == DTS_MAGIC);

  *it_out = NULL;
  for (;;) {
    if (state->dts_eof) return GRAPHD_ERR_NO;

    cl_log(cl, CL_LEVEL_VERBOSE, "datetiem_vrange_next: bin %i",
           state->dts_cur.dp_bin);

    if (datetime_skip(pdb, &state->dts_cur)) {
      err = GRAPHD_ERR_NO;
    } else {
      err = pdb_bin_to_iterator(pdb, state->dts_cur.dp_bin, low, high,
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

    state->dts_eof = graphd_vrange_forward(greq, vr)
                         ? datetime_inc(pdb, &state->dts_cur, &state->dts_hi)
                         : datetime_dec(pdb, &state->dts_cur, &state->dts_lo);

    if (*it_out) return 0;

    if (*budget <= 0) return PDB_ERR_MORE;
  }

  return err;
}

static int datetime_vrange_statistics(
    graphd_request *greq, graphd_value_range *vr, void *private_state,
    unsigned long long *total_ids, pdb_budget *next_cost, pdb_budget *budget) {
  cl_handle *cl;
  datetime_vrange_state *state;
  int count = 0;
  cl = graphd_request_cl(greq);
  state = private_state;

  cl_assert(cl, state->dts_magic == DTS_MAGIC);

  count = (maximum_negative_year_bin - minimum_negative_year_bin) +
          (maximum_positive_year_bin - minimum_positive_year_bin) +
          (maximum_time_bin - minimum_time_bin) + 1;

  switch (state->dts_lo.dp_mode) {
    case DTS_NEGATIVE:
      count -= state->dts_lo.dp_bin - maximum_negative_year_bin;
      break;
    case DTS_POSITIVE:
      count -= state->dts_lo.dp_bin - minimum_positive_year_bin;
      count -= maximum_negative_year_bin - minimum_negative_year_bin;
      break;
    case DTS_TIME:

      count -= state->dts_lo.dp_bin - minimum_time_bin;
      count -= maximum_negative_year_bin - minimum_negative_year_bin;
      count -= maximum_positive_year_bin - minimum_positive_year_bin;
      break;
    default:
      cl_notreached(cl, "dts_lo.dp_mode has invalid value: %i",
                    state->dts_lo.dp_mode);
  }

  switch (state->dts_hi.dp_mode) {
    case DTS_NEGATIVE:
      count -= minimum_negative_year_bin - state->dts_hi.dp_bin;
      count -= maximum_time_bin - minimum_time_bin;
      count -= maximum_positive_year_bin - minimum_positive_year_bin;
      break;
    case DTS_POSITIVE:
      count -= maximum_time_bin - minimum_time_bin;
      count -= maximum_positive_year_bin - state->dts_hi.dp_bin;
      break;
    case DTS_TIME:
      count -= maximum_time_bin - state->dts_hi.dp_bin;
      break;
    default:
      cl_notreached(cl, "dts_hi.dp_mode has invalid value: %i",
                    state->dts_hi.dp_mode);
  }

  cl_assert(cl, count > 0);

  *next_cost = PDB_COST_HMAP_ELEMENT;
  *total_ids =
      count * (1 +
               pdb_primitive_n(GREQ_PDB(greq)) /
                   (pdb_bin_end(GREQ_PDB(greq), PDB_BINSET_STRINGS) * 2));

  return 0;
}

static int datetime_vrange_seek(graphd_request *greq, graphd_value_range *vr,
                                void *private_data, const char *s,
                                const char *e, pdb_id id, pdb_id low,
                                pdb_id high, pdb_iterator **it_out) {
  datetime_vrange_state *state = private_data;
  cl_handle *cl = graphd_request_cl(greq);
  int err;
  pdb_id idout;

  cl_assert(cl, state->dts_magic == DTS_MAGIC);

  err = datetime_string_to_bin(GREQ_PDB(greq), s, e, true, &state->dts_cur);

  if (err) return err;

  err = pdb_bin_to_iterator(GREQ_PDB(greq), state->dts_cur.dp_bin, low, high,
                            true, false, it_out);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_bin_to_iterator", err,
                 "Can't make hmap iterator for bin %i", state->dts_cur.dp_bin);

    return err;
  }

  pdb_iterator_find_nonstep(GREQ_PDB(greq), *it_out, id, &idout);
  if (err) {
    char buf[200];
    cl_log_errno(
        cl, CL_LEVEL_FAIL, "pdb_iterator_find_nonstep", err,
        "Can't find it %llu in %s", (unsigned long long)id,
        pdb_iterator_to_string(GREQ_PDB(greq), *it_out, buf, sizeof buf));

    pdb_iterator_destroy(GREQ_PDB(greq), it_out);
    return err;
  }

  if (idout != id) {
    cl_log(cl, CL_LEVEL_INFO,
           "pdb_iterator_find changed IDs from %llu to %llu"
           " during datetiem_vrange_seek",
           (unsigned long long)id, (unsigned long long)idout);
  }

  if (graphd_vrange_forward(greq, vr))
    state->dts_eof =
        datetime_inc(GREQ_PDB(greq), &state->dts_cur, &state->dts_hi);
  else
    state->dts_eof =
        datetime_dec(GREQ_PDB(greq), &state->dts_cur, &state->dts_lo);
  return 0;
}

static int datetime_value_in_range(graphd_request *greq, graphd_value_range *vr,
                                   void *private_state, const char *s,
                                   const char *e, bool *string_in_range)

{
  datetime_position bin;
  pdb_handle *pdb;
  cl_handle *cl;
  datetime_vrange_state *state = private_state;
  const char *bs, *be;

  cl = graphd_request_cl(greq);
  pdb = GREQ_PDB(greq);
  cl_assert(cl, state->dts_magic == DTS_MAGIC);

  bin = state->dts_cur;

  /*
   * Bounds check
   */
  if (graphd_vrange_forward(greq, vr)) {
    if (datetime_dec(pdb, &bin, &state->dts_lo)) {
      *string_in_range = false;
      return 0;
    }
  } else {
    if (datetime_inc(pdb, &bin, &state->dts_hi)) {
      *string_in_range = false;
      return 0;
    }
  }

  /*
   * Get the value of the last bin that we completly
   * evaluated.
   */
  {
    const char **v;
    pdb_bin_value(pdb, PDB_BINSET_STRINGS, bin.dp_bin, (void *)&v);
    bs = *v;
    if (bs)
      be = bs + strlen(bs);
    else
      be = NULL;
  }

  if (graphd_vrange_forward(greq, vr)) {
    if (datetime_sort_compare(greq, s, e, bs, be) < 0)
      *string_in_range = true;
    else
      *string_in_range = false;
  } else {
    if (datetime_sort_compare(greq, s, e, bs, be) >= 0)
      *string_in_range = true;
    else
      *string_in_range = false;
  }

  return 0;
}

static int datetime_vrange_freeze(graphd_request *greq, graphd_value_range *vr,
                                  void *private_data, cm_buffer *buf) {
  cl_handle *cl;
  int err;
  datetime_vrange_state *state = private_data;
  cl = graphd_request_cl(greq);

  cl_assert(cl, state->dts_magic == DTS_MAGIC);
  err = cm_buffer_sprintf(buf, "%i,%i,%i", state->dts_cur.dp_bin,
                          state->dts_cur.dp_mode, state->dts_eof ? 1 : 0);
  return 0;
}

static int datetime_vrange_thaw(graphd_request *greq, graphd_value_range *vr,
                                void *private_data, const char *s,
                                const char *e) {
  int err;
  datetime_vrange_state *state;

  cl_handle *cl;
  cl = graphd_request_cl(greq);

  state = private_data;

  cl_assert(cl, state->dts_magic == DTS_MAGIC);

  err = pdb_iterator_util_thaw(GREQ_PDB(greq), &s, e, "%d,%d,%d",
                               &state->dts_cur.dp_bin, &state->dts_cur.dp_mode,
                               &state->dts_eof);

  if (err) {
    cl_log(cl, CL_LEVEL_ERROR,
           "datetime_vrange_thaw: can't parse datetime cursor"
           " out of %.*s",
           (int)(e - s), s);
    return err;
  }

  if (state->dts_eof) return 0;

  if ((state->dts_cur.dp_mode > DTS_TIME) || (state->dts_cur.dp_mode < 0) ||
      (state->dts_cur.dp_bin < 0) ||
      (state->dts_cur.dp_bin > pdb_bin_end(GREQ_PDB(greq), PDB_BINSET_STRINGS)))
    return GRAPHD_ERR_SYNTAX;

  return 0;
}

graphd_comparator const graphd_comparator_datetime[1] = {
    {.cmp_locale = "",
     .cmp_name = "datetime",
     .cmp_alias = NULL,
     .cmp_syntax = datetime_syntax,
     .cmp_eq_iterator = equality_iterator,
     .cmp_iterator_range = NULL,
     .cmp_glob = delimited_string_match,
     .cmp_sort_compare = datetime_sort_compare,
     .cmp_vrange_size = datetime_vrange_size,
     .cmp_vrange_start = datetime_vrange_start,
     .cmp_vrange_it_next = datetime_vrange_it_next,
     .cmp_vrange_statistics = datetime_vrange_statistics,
     .cmp_vrange_seek = datetime_vrange_seek,
     .cmp_value_in_range = datetime_value_in_range,
     .cmp_vrange_freeze = datetime_vrange_freeze,
     .cmp_vrange_thaw = datetime_vrange_thaw,
     .cmp_lowest_string = "",
     .cmp_highest_string = NULL

    }};
