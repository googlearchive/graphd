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

#define ISWORD(a) (!isascii(a) || isalnum(a))
#define ISSPACE(a) (isascii(a) && isspace(a))
#define ISPUNCT(a) (isascii(a) && ispunct(a))

#define GREQ_CL(r) (graphd_request_cl(r))
#define GREQ_PDB(r) (graphd_request_graphd(r)->g_pdb)

/*
 *  Rules for the octet string match ~=:
 *
 *  - Matching is case-sensitive.
 *
 *  - "^" and "$" anchor and front and back; by default, it's unanchored.
 *
 *  - White space in the pattern matches arbitrary whitespace and punctuation
 *    in the string.
 *
 *  - Punctuation in the pattern works like optional white space.
 *    (Soo "foo-bar" matches "foobar", "foo-bar", and "foo bar")
 *
 *  - Pattern boundaries without * must match word boundaries.
 *    So, "foo" doesn't match "foot".
 *
 *  - Pattern boundary with * matches in word.
 *    (So, "foo*" matches "foot" but not "pfoo".)
 *
 *  - "*" matches word characters, but not white space.  (So "foo * baz"
 *    matches "foo bar baz" but not "foo baz".  "foo*baz" matches
 *    "foonitzbaz" but not "foo/baz")
 *
 *  - To make a character significant as a literal character to be matched,
 *    prefix it with a \.  So, to match a literal \, write \\.  To match
 *    a literal *, write \*.  To match a literal -, write \-.
 *
 *  - Adjacent literal characters are not matched if they're separated
 *    by white space or punctuation, even if they would be matched that
 *    way if they were unescaped.  So, "\(\-\:" doesn't match "(--:"
 */
static bool octet_glob_step(char const *pat_r, char const *pat_e, char const *r,
                            char const *e) {
  int in_word = 0;

  for (;;) {
    /*  Whitespace or end of pattern: if we're in a word,
     *  that word must end here.
     */
    if (pat_r >= pat_e || ISSPACE(*pat_r)) {
      if (in_word) {
        if (r < e && ISWORD(*r)) return false;
        in_word = 0;
      }

      if (pat_r >= pat_e) return true;
      pat_r++;
      continue;
    }

    /* $ at the end of the pattern: optional whitespace, then end */
    if (*pat_r == '$' && pat_r + 1 == pat_e) {
      while (r < e && !ISWORD(*r)) r++;
      return r >= e;
    }

    /* asterisk(*): A word (when used alone) or word fragment
     * (when used as part of a word).  Must be in or part of a
     * word.
     */

    if (*pat_r == '*') {
      int ch;
      while (pat_r < pat_e && *pat_r == '*') pat_r++;

      if (!in_word) {
        while (r < e && !ISWORD(*r)) r++;
        if (r >= e) return false;

        in_word = 1;
      }

      if (pat_r >= pat_e || (*pat_r != '\\' && !ISWORD(*pat_r))) {
        /* "*" alone -- skip a word. */
        while (r < e && ISWORD(*r)) r++;
        in_word = 0;

        continue;
      }

      /* "*" as part of a word. */
      ch = 'a';
      if (pat_e - pat_r >= 2 && *pat_r == '\\') ch = pat_r[1];
      while (r < e && (*r == ch || ISWORD(*r))) {
        if (octet_glob_step(pat_r, pat_e, r, e)) return true;
        r++;
      }
      continue;
    }

    /*  Escaped character match pretty much like regular
     *  characters.
     */
    if (pat_r + 1 < pat_e && *pat_r == '\\') {
      pat_r++;
      if (!in_word) {
        while (r < e && !ISWORD(*r) && *r != *pat_r) r++;
        in_word = 1;
      }
      if (r < e && *r == *pat_r) {
        pat_r++;
        r++;

        continue;
      }
      return false;
    }

    /*  Punctuation in the pattern: if we're in a word,
     *  and that word ends here, that's okay; otherwise,
     *  stay with the word.
     */
    if (ISPUNCT(*pat_r)) {
      if (in_word) {
        if (r >= e || !ISWORD(*r)) in_word = 0;
      }
      pat_r++;
      continue;
    }

    /*  Other word characters: match literally; skip leading
     *  whitespace if we're not in a word.
     */
    if (!in_word) {
      while (r < e && !ISWORD(*r) && *r != *pat_r) r++;
      in_word = 1;
    }
    while (ISWORD(*pat_r)) {
      if (r >= e || *r != *pat_r) return false;
      pat_r++;
      r++;
    }
  }
}

static bool octet_glob_match(graphd_request *greq, char const *pat_s,
                             char const *pat_e, char const *s, char const *e) {
  int ch;

  if (pat_s < pat_e && *pat_s == '^')
    return octet_glob_step(pat_s + 1, pat_e, s, e);

  if (pat_e - pat_s >= 2 && *pat_s == '\\')
    ch = pat_s[1];
  else
    ch = 'a';

  while (s < e) {
    while (s < e && *s != ch && !ISWORD(*s)) s++;

    if (octet_glob_step(pat_s, pat_e, s, e)) return true;

    s++;
    while (s < e && ISWORD(*s)) s++;
  }

  while (pat_s < pat_e && *pat_s != '\\' && !ISWORD(*pat_s)) pat_s++;
  return pat_s >= pat_e;
}

static int octet_syntax(graphd_request *greq,
                        graphd_string_constraint const *strcon) {
  (void)greq;
  (void)strcon;

  /* Accept anything. */

  return 0;
}

static int octet_sort_compare(graphd_request *greq, char const *as,
                              char const *ae, char const *bs, char const *be) {
  /*  NULL sorts after (greater than) everything.
   */
  if (as == NULL) return (bs == NULL) ? 0 : 1;

  if (bs == NULL) return -1;

  while (as < ae && bs < be) {
    if (*as != *bs) return *as < *bs ? -1 : 1;

    as++;
    bs++;
  }
  if (as == ae) return (bs == be) ? 0 : -1;
  if (bs == be) return 1;

  return 0;
}

static int octet_iterator_range(graphd_request *greq, const char *lo_s,
                                const char *lo_e, const char *hi_s,
                                const char *hi_e, pdb_iterator *and_it,
                                unsigned long long low, unsigned long long high,
                                graphd_direction direction, bool value_forward,
                                char const *ordering, bool *indexed_inout) {
  graphd_session *gses = graphd_request_session(greq);
  cl_handle *cl = gses->gses_cl;
  graphd_handle *graphd = gses->gses_graphd;
  pdb_handle *pdb = graphd->g_pdb;
  int err;
  char const *word_s, *word_e, *hi_p, *lo_p, *word_p;
  bool prefix, forward;
  pdb_iterator *sub_it;

  forward = direction != GRAPHD_DIRECTION_BACKWARD;

  if (lo_s == NULL || hi_s == NULL) return 0;

  /*  The parser makes sure that inequality constraints
   *  only have single elements.
   */

  /*  Find the first character that's different.
   */
  for (hi_p = hi_s, lo_p = lo_s; hi_p < hi_e && lo_p < lo_e && *hi_p == *lo_p;
       hi_p++, lo_p++)
    ;

  /*  Extract any markers out of the common prefix.
   */
  word_p = NULL;
  while (graphd_comparator_default_prefix_word_next(
      hi_s, hi_p, &word_s, &word_e, &prefix, &word_p)) {
    if (prefix) {
      err = graphd_iterator_prefix_create(greq, word_s, word_e, low, high,
                                          direction, &sub_it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_prefix_create", err,
                     "word=\"%.*s\"", (int)(word_e - word_s), word_s);
        return err;
      }
      graphd_iterator_set_direction_ordering(pdb, sub_it, direction, ordering);
    } else {
      err = pdb_iterator_word_create(pdb, word_s, word_e, low, high, forward,
                                     false, &sub_it);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_word_create", err,
                     "word=\"%.*s\"", (int)(word_e - word_s), word_s);
        return err;
      }
      graphd_iterator_set_direction_ordering(pdb, sub_it, direction, ordering);
      *indexed_inout = true;
    }
    cl_assert(cl, sub_it != NULL);

    err = graphd_iterator_and_add_subcondition(graphd, and_it, &sub_it);
    if (err != 0) {
      char buf[200];
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_add_subcondition", err,
                   "iterator=%s",
                   pdb_iterator_to_string(pdb, sub_it, buf, sizeof buf));

      pdb_iterator_destroy(pdb, &sub_it);
      return err;
    }
  }
  return 0;
}

#define OVS_MAGIC 0x19fe5cc3

typedef struct octet_vrange_state {
  unsigned long ovs_magic;
  int ovs_lo_bin;
  int ovs_hi_bin;
  int ovs_cur_bin;
} octet_vrange_state;

static size_t octet_vrange_size(graphd_request *greq, const char *lo_s,
                                const char *lo_e, const char *hi_s,
                                const char *hi_e) {
  return sizeof(octet_vrange_state);
}

static int octet_vrange_start(graphd_request *greq, graphd_value_range *vr,
                              void *private_data) {
  octet_vrange_state *state = private_data;

  if (state->ovs_magic == OVS_MAGIC) {
    if (graphd_vrange_forward(greq, vr)) {
      state->ovs_cur_bin = state->ovs_lo_bin;
    } else {
      state->ovs_cur_bin = state->ovs_hi_bin;
    }

    cl_log(GREQ_CL(greq), CL_LEVEL_SPEW, "octet_vragne resetting %p",
           private_data);
    return 0;
  }

  cl_assert(GREQ_CL(greq), state->ovs_magic == 0);

  state->ovs_magic = OVS_MAGIC;

  if (octet_sort_compare(greq, vr->vr_lo_s, vr->vr_lo_e, vr->vr_hi_s,
                         vr->vr_hi_e) > 0)
    return GRAPHD_ERR_NO;

  state->ovs_lo_bin = pdb_bin_lookup(GREQ_PDB(greq), PDB_BINSET_STRINGS,
                                     vr->vr_lo_s, vr->vr_lo_e, NULL);

  cl_log(GREQ_CL(greq), CL_LEVEL_VERBOSE,
         "octet_vragne_start: low bin: \"%.*s\" is %i",
         (int)(vr->vr_lo_e - vr->vr_lo_s), vr->vr_lo_s, state->ovs_lo_bin);

  if (vr->vr_hi_s == NULL) {
    state->ovs_hi_bin = pdb_bin_end(GREQ_PDB(greq), PDB_BINSET_STRINGS) + 1;
  } else {
    state->ovs_hi_bin = pdb_bin_lookup(GREQ_PDB(greq), PDB_BINSET_STRINGS,
                                       vr->vr_hi_s, vr->vr_hi_e, NULL) +
                        1;
    cl_log(GREQ_CL(greq), CL_LEVEL_VERBOSE,
           "coctet_vrange_start: high bin: \"%.*s\" is %i",
           (int)(vr->vr_hi_e - vr->vr_hi_s), vr->vr_hi_s, state->ovs_hi_bin);
  }

  if (graphd_vrange_forward(greq, vr)) {
    state->ovs_cur_bin = state->ovs_lo_bin;
  } else {
    state->ovs_cur_bin = state->ovs_hi_bin - 1;
  }

  cl_assert(GREQ_CL(greq), state->ovs_hi_bin >= state->ovs_lo_bin);

  return 0;
}

static int octet_vrange_it_next(graphd_request *greq, graphd_value_range *vr,
                                void *private_data, pdb_id low, pdb_id high,
                                pdb_iterator **it_out, pdb_budget *budget)

{
  cl_handle *cl = GREQ_CL(greq);
  pdb_handle *pdb = GREQ_PDB(greq);
  int err = 0;
  octet_vrange_state *state = private_data;

  cl_assert(cl, state->ovs_magic == OVS_MAGIC);

  *it_out = NULL;
  for (;;) {
    if (graphd_vrange_forward(greq, vr) &&
        state->ovs_cur_bin >= state->ovs_hi_bin)
      return GRAPHD_ERR_NO;

    /*
     * XXX
     *
     * we compare against -1 here.  That's okay although a
     * bit of a kludge, but I'm not sure what happens when we
     * freeze something that's about to say GRAPHD_ERR_NO.
     */
    if (!graphd_vrange_forward(greq, vr) &&
        state->ovs_cur_bin < state->ovs_lo_bin)
      return GRAPHD_ERR_NO;

    if (state->ovs_cur_bin == pdb_bin_end(pdb, PDB_BINSET_STRINGS)) {
      err = graphd_iterator_null_value_create(greq, low, high, it_out);
    } else {
      err = pdb_bin_to_iterator(pdb, state->ovs_cur_bin, low, high,
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
      state->ovs_cur_bin++;
    else
      state->ovs_cur_bin--;
    if (err == 0 && *it_out != NULL) return 0;

    if (*budget <= 0) return PDB_ERR_MORE;
  }

  return err;
}

static int octet_vrange_statistics(graphd_request *greq, graphd_value_range *vr,
                                   void *private_state,
                                   unsigned long long *total_ids,
                                   pdb_budget *next_cost, pdb_budget *budget) {
  cl_handle *cl;
  octet_vrange_state *state;

  int sit;

  cl = GREQ_CL(greq);
  state = private_state;

  cl_assert(cl, state->ovs_magic == OVS_MAGIC);

  cl_assert(cl, state->ovs_lo_bin <= state->ovs_hi_bin);

  sit = state->ovs_hi_bin - state->ovs_lo_bin + 1;

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

static int octet_vrange_seek(graphd_request *greq, graphd_value_range *vr,
                             void *private_data, const char *s, const char *e,
                             pdb_id id, pdb_id low, pdb_id high,
                             pdb_iterator **it_out) {
  cl_handle *cl = GREQ_CL(greq);
  pdb_handle *pdb = GREQ_PDB(greq);
  int err;
  int bin;
  pdb_iterator *it;

  octet_vrange_state *state = private_data;

  cl_assert(cl, state->ovs_magic == OVS_MAGIC);

  bin = pdb_bin_lookup(pdb, PDB_BINSET_STRINGS, s, e, NULL);

  cl_log(cl, CL_LEVEL_SPEW, "octet_vrange_seek[%llu]: %.*s seeks to bin %i",
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

  state->ovs_cur_bin = bin + (graphd_vrange_forward(greq, vr) ? 1 : -1);
  *it_out = it;

  return 0;
}

static int octet_vrange_freeze(graphd_request *greq, graphd_value_range *vr,
                               void *private_data, cm_buffer *buf) {
  cl_handle *cl;
  octet_vrange_state *state;

  int err;
  cl = GREQ_CL(greq);
  state = private_data;

  cl_assert(cl, state->ovs_magic == OVS_MAGIC);

  err = cm_buffer_sprintf(buf, "%i", state->ovs_cur_bin);
  return err;
}

static int octet_vrange_thaw(graphd_request *greq, graphd_value_range *vr,
                             void *private_data, const char *s, const char *e) {
  int err;
  octet_vrange_state *state;
  cl_handle *cl;
  cl = GREQ_CL(greq);

  state = private_data;

  cl_assert(cl, state->ovs_magic == OVS_MAGIC);

  err =
      pdb_iterator_util_thaw(GREQ_PDB(greq), &s, e, "%d", &state->ovs_cur_bin);

  if (err) {
    cl_log(GREQ_CL(greq), CL_LEVEL_ERROR,
           "octet_vrange_thaw: can't parse integer out of: %.*s", (int)(e - s),
           s);
    return GRAPHD_ERR_LEXICAL;
  }

  if (state->ovs_cur_bin < (state->ovs_lo_bin - 1)) {
    cl_log(GREQ_CL(greq), CL_LEVEL_ERROR,
           "octet_vrange_thaw: %i is outside range %i - %i", state->ovs_cur_bin,
           state->ovs_lo_bin, state->ovs_hi_bin);
    return GRAPHD_ERR_LEXICAL;
  }

  if (state->ovs_cur_bin > (state->ovs_hi_bin + 1)) {
    cl_log(GREQ_CL(greq), CL_LEVEL_ERROR,
           "octet_vrange_thaw: %i is outside range %i - %i", state->ovs_cur_bin,
           state->ovs_lo_bin, state->ovs_hi_bin);
    return GRAPHD_ERR_LEXICAL;
  }

  return 0;
}

static char const *const graphd_comparator_octet_aliases[] = {"case-sensitive",
                                                              NULL};

graphd_comparator const graphd_comparator_octet[1] = {
    {.cmp_locale = "",
     .cmp_name = "octet",
     .cmp_alias = graphd_comparator_octet_aliases,
     .cmp_syntax = octet_syntax,
     .cmp_eq_iterator = graphd_value_default_iterator,
     .cmp_iterator_range = octet_iterator_range,
     .cmp_glob = octet_glob_match,
     .cmp_sort_compare = octet_sort_compare,
     .cmp_lowest_string = "",
     .cmp_highest_string = NULL,
     .cmp_vrange_size = octet_vrange_size,
     .cmp_vrange_start = octet_vrange_start,
     .cmp_vrange_it_next = octet_vrange_it_next,
     .cmp_vrange_statistics = octet_vrange_statistics,
     .cmp_vrange_seek = octet_vrange_seek,
     .cmp_value_in_range = NULL,
     .cmp_vrange_freeze = octet_vrange_freeze,
     .cmp_vrange_thaw = octet_vrange_thaw,
     .cmp_lowest_string = "",
     .cmp_highest_string = NULL}};
