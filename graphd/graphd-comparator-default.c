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
#include <stdio.h>
#include <errno.h>

#define TOLOWER(a) (isascii(a) ? tolower(a) : (a))
#define EQ(a, b) (TOLOWER(a) == TOLOWER(b))
#define ISWORD(a) (!isascii(a) || isalnum(a))
#define ISNUMWORD(a) (ISWORD(a) || ISPOINT(a) || ISSIGN(a))
#define ISALNUM(a) (isascii(a) && isalnum(a))
#define ISSPACE(a) (isascii(a) && isspace(a))
#define ISDIGIT(a) (isascii(a) && isdigit(a))
#define ISPUNCT(a) (isascii(a) && !(isalnum(a) || isspace(a)))
#define ISPOINT(a) ((a) == '.')
#define ISSIGN(a) ((a) == '+' || (a) == '-')
#define ISALPHA(a) (isascii(a) && isalpha(a))

#define ISBREAK(r, e) \
  (ISSPACE(*(r)) || (*(r) == '\\' && (r) + 1 < (e) && ISPUNCT((r)[1])))
#define ENDBREAK(r) ((r) + (ISSPACE(*(r)) ? 1 : 2))

#define GREQ_PDB(r) (graphd_request_graphd(r)->g_pdb)

#define DVS_MAGIC 0xe34a5123
typedef struct {
  unsigned long dvs_magic;

  /*
   * Low and hi bins/number
   */
  int dvs_lo;
  int dvs_hi;

  /*
   * Current bin/number
   */
  int dvs_cur;

  size_t dvs_test_len;
  char dvs_test_string[];
} default_vrange_state;

/*
 *  Rules for the default string match ~=:
 *
 *  - Matching is case-insensitive except for characters escaped with \
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
 *
 *  - Numbers that don't have \ in them match other numbers either directly
 *    or after the non-pattern number has been normalized.
 */

static bool active_slash(char const *s, char const *e, char const *p) {
  if (p >= e || *p != '\\') return false;

  /* Odd number of slashes = magic.
   */
  for (;;) {
    if (p == s || *--p != '\\') return true;

    if (p == s || *--p != '\\') return false;
  }
}

static bool active_asterisk(char const *s, char const *e, char const *p) {
  if (p >= e || *p != '*') return false;

  return p == s || p[-1] != '\\' || !active_slash(s, e, p - 1);
}

/*  If there is a number that could be construed as
 *  starting the pattern at pat_s (while interpreting the asterisk
 *  character as a wildcard), set pw_s...pw_e to surround it.
 *
 *  shadow_s is a string with the same number of characters
 *  as pat_s, but with each unescaped * replaced by 0.
 */
static bool number_pattern(char const *shadow_s, char const *pat_s,
                           char const **pat_rr, char const *pat_e,
                           char const **pw_s, char const **pw_e) {
  char const *shadow_r, *shadow_e, *word_s, *word_e;
  int type;

  /* Get the shadow version of the pattern.
   */
  shadow_r = shadow_s + (*pat_rr - pat_s);
  shadow_e = shadow_s + (pat_e - pat_s);

  if (!pdb_word_fragment_next(shadow_s, &shadow_r, shadow_e, &word_s, &word_e,
                              &type))
    return false;

  if (type != PDB_WORD_NUMBER) return false;

  *pw_s = pat_s + (word_s - shadow_s);
  *pw_e = pat_s + (word_e - shadow_s);
  *pat_rr = pat_s + (shadow_r - shadow_s);

  return true;
}

/*  Turn a pattern into its shadow pattern.
 */
static char *pattern_shadow(cm_handle *cm, char const *s, char const *e) {
  char *w, *buf = cm_malcpy(cm, s, (size_t)(e - s + 1));
  char const *r;
  bool escaped = false;

  if (buf == NULL) return NULL;
  buf[e - s] = '\0';

  for (r = s, w = buf; r < e; r++) {
    if (!escaped) {
      if (*r == '\\') {
        escaped = 1;
        *w++ = 'x';
      } else
        *w++ = (*r == '*') ? '0' : *r;
    } else {
      *w++ = 'x';
      escaped = false;
    }
  }
  *w = '\0';
  return buf;
}

static bool only_zeroes_and_asterisks(char const *s, char const *e) {
  while (s < e) {
    if (*s != '0' && *s != '*') return false;
    s++;
  }
  return true;
}

static bool asterisks(char const *s, char const *e) {
  if (s >= e) return false;

  while (s < e) {
    if (*s != '*') return false;
    s++;
  }
  return true;
}

/*  Match a pattern and a text that are thought of
 *  as numbers.
 */
static bool number_match(char const *pat_r, char const *pat_e, char const *s,
                         char const *r, char const *e) {
  int in_word = false;

  /*  Explicit plus in the pattern: match anything that
   *  isn't negative.
   */
  if (pat_r < pat_e && *pat_r == '+') {
    if (r < e && *r == '-') return false;
    if (r < e && *r == '+') r++;
    pat_r++;
  }

  /*
   * Discard leading zeroes in the pattern.
   */
  while (pat_e - pat_r >= 2 && *pat_r == '0' && ISDIGIT(pat_r[1])) pat_r++;

  for (;;) {
    /*  Whitespace or end of pattern: if we're in a word,
     *  that word must end here.
     */
    if (pat_r >= pat_e || ISSPACE(*pat_r)) {
      if (in_word) {
        if (r < e && ISNUMWORD(*r)) {
          return false;
        }
        in_word = false;
      }

      if (pat_r >= pat_e) {
        while (r < e && !ISNUMWORD(*r)) r++;
        return true;
      }
      pat_r++;
      continue;
    }

    /* $ at the end of the pattern: optional whitespace, then end */
    if (*pat_r == '$' && pat_r + 1 == pat_e) {
      while (r < e && !ISNUMWORD(*r)) r++;
      return r >= e;
    }

    /* asterisk(*): A word (when used alone) or word fragment
     * (when used as part of a word).  Must be in or part of a
     * word.
     */

    if (*pat_r == '*') {
      int ch;

      /* Go to the end of any sequence of * */
      while (pat_r < pat_e && *pat_r == '*') pat_r++;

      if (!in_word) {
        /* Move to the beginning of a word. */

        while (r < e && !ISNUMWORD(*r)) r++;
        if (r >= e) return false;
        in_word = true;
      }

      if (pat_r >= pat_e || (*pat_r != '\\' && !ISNUMWORD(*pat_r))) {
        /* "*" alone -- skip signs and digits. */
        while (r < e && ISNUMWORD(*r) && !ISPOINT(*r)) r++;
        in_word = false;

        continue;
      }

      /* "*" as part of a word. */
      ch = 'a';
      if (pat_e - pat_r >= 2 && *pat_r == '\\') ch = pat_r[1];
      while (r < e && (*r == ch || (ISNUMWORD(*r) && !ISPOINT(*r)))) {
        if (number_match(pat_r, pat_e, s, r, e)) return true;
        r++;
      }
      continue;
    }

    /*  Escaped character match pretty much like regular
     *  characters.  Except without the tolower.
     */
    if (pat_r + 1 < pat_e && *pat_r == '\\') {
      pat_r++;
      if (!in_word) {
        while (r < e && !ISNUMWORD(*r) && *r != *pat_r) r++;
        in_word = true;
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
    if (ISPUNCT(*pat_r) && !ISNUMWORD(*pat_r)) {
      if (in_word) {
        if (r >= e || !ISWORD(*r)) in_word = false;
      }
      pat_r++;
      continue;
    }

    /*  Other word characters: match literally; skip leading
     *  whitespace if we're not in a word.
     */
    if (!in_word) {
      while (r < e && !ISNUMWORD(*r) && *r != *pat_r) r++;
      in_word = true;
    }
    while (pat_r < pat_e && ISNUMWORD(*pat_r)) {
      if (r >= e && *pat_r == '.') {
        /*  Decimal point, followed by only asterisk
         *  and zeros, matches the end of the text if
         *  we didn't yet pass a decimal point.
         */
        if (pat_r < pat_e && ISPOINT(*pat_r) && r == e &&
            only_zeroes_and_asterisks(pat_r + 1, pat_e) &&
            !memchr(s, '.', e - s)) {
          return true;
        }
      }

      if (r >= e || !EQ(*r, *pat_r)) {
        return false;
      }
      pat_r++;
      r++;
    }
  }
}

static bool glob_step(char const *shadow_s, char const *pat_s,
                      char const *pat_r, char const *pat_e, char const *s,
                      char const *r, char const *e) {
  int in_word = 0;
  cm_handle *cm = cm_c();

  for (;;) {
    char const *my_pat_r, *my_r;
    char const *pw_s, *pw_e;
    char const *tw_s, *tw_e;
    int tw_type;

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

    my_pat_r = pat_r;
    my_r = r;

    if (number_pattern(shadow_s, pat_s, &my_pat_r, pat_e, &pw_s, &pw_e)) {
      char const *my_r = r;

      if (!in_word)
        while (my_r < e && !ISNUMWORD(*my_r)) my_r++;

      if (pdb_word_fragment_next(s, &my_r, e, &tw_s, &tw_e, &tw_type) &&
          tw_type == PDB_WORD_NUMBER) {
        bool matched;
        int err;
        char *norm_buf;
        char const *norm_s, *norm_e;

        if (asterisks(pw_s, pw_e) ||
            number_match(pw_s, pw_e, tw_s, tw_s, tw_e)) {
          pat_r = my_pat_r;
          r = my_r;

          continue;
        }

        /*  They didn't match as written; but ...
         *  what if I normalize the text?
         */
        err = pdb_word_number_normalize(cm, tw_s, tw_e, &norm_buf, &norm_s,
                                        &norm_e);
        if (err != 0) return err;
        matched = number_match(pw_s, pw_e, norm_s, norm_s, norm_e);

        if (norm_buf != NULL) cm_free(cm, norm_buf);

        if (matched) {
          pat_r = my_pat_r;
          r = my_r;

          continue;
        }
        return false;
      }
    }

    /* asterisk(*): A word (when used alone) or word fragment
     * (when used as part of a word).  Must be in or part of a
     * word.
     */

    if (*pat_r == '*') {
      int ch = 'a';
      char const *r0;

      /* Go to the end of any sequence of * */
      while (pat_r < pat_e && *pat_r == '*') pat_r++;

      if (pat_e - pat_r >= 2 && *pat_r == '\\') ch = pat_r[1];

      if (!in_word) {
        /* Move to the beginning of a word. */

        while (r < e && !ISWORD(*r) && *r != ch) r++;
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
      r0 = r;
      while (r < e && ((r == r0 && *r == ch) || ISWORD(*r))) {
        if (glob_step(shadow_s, pat_s, pat_r, pat_e, s, r, e)) return true;
        r++;
      }
      continue;
    }

    /*  Escaped character match pretty much like regular
     *  characters.  Except without the tolower.
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
    while (pat_r < pat_e && ISWORD(*pat_r)) {
      if (r >= e || !EQ(*r, *pat_r)) return false;
      pat_r++;
      r++;
    }
  }
}

static bool glob_match(graphd_request *greq, char const *pat_s,
                       char const *pat_e, char const *s, char const *e) {
  int ch = 'a', ch_pot = 'a';
  char const *s0 = s, *my_pat_r, *pw_s, *pw_e;
  char const *pat_pot;
  char *shadow_s;
  bool pat_is_number;
  cm_handle *cm = greq->greq_req.req_cm;
  shadow_s = pattern_shadow(cm, pat_s, pat_e);
  if (shadow_s == NULL) return false;

  if (pat_s < pat_e && *pat_s == '^') {
    bool result = glob_step(shadow_s, pat_s, pat_s + 1, pat_e, s0, s, e);
    cm_free(cm, shadow_s);
    return result;
  }

  for (pat_pot = pat_s; pat_pot < pat_e && *pat_pot == '*'; pat_pot++)
    ;

  if (pat_e - pat_s >= 2 && *pat_s == '\\')
    ch = ch_pot = pat_s[1];
  else if (pat_e - pat_pot >= 2 && *pat_pot == '\\')
    ch_pot = pat_pot[1];

  my_pat_r = pat_s;
  pat_is_number =
      number_pattern(shadow_s, pat_s, &my_pat_r, pat_e, &pw_s, &pw_e);

  while (s < e) {
    /*  Skip non-word-, non-number material
     *  in the destination.
     */
    while (s < e && *s != ch_pot &&
           (pat_is_number ? !ISNUMWORD(*s) : !ISWORD(*s)))
      s++;

    /* Try to match where we are. */
    if (glob_step(shadow_s, pat_s, pat_s, pat_e, s0, s, e)) {
      cm_free(cm, shadow_s);
      return true;
    }

    if (pat_is_number) {
      char const *my_r = s;
      char const *tw_s, *tw_e;
      int tw_type;

      /*  Remove a number or word from the input
       *  stream.
       */
      if (pdb_word_fragment_next(s0, &my_r, e, &tw_s, &tw_e, &tw_type))
        s = my_r;
      else {
        s++;
        while (s < e && ISWORD(*s)) s++;
      }
    } else {
      /*  Remove a fragment from the input stream.
       */

      s++;
      /* Skip the rest of this word. */
      while (s < e && (*s == ch || ISWORD(*s))) s++;
    }
  }
  cm_free(cm, shadow_s);

  while (pat_s < pat_e && *pat_s != '\\' && !ISWORD(*pat_s)) pat_s++;
  return pat_s >= pat_e;
}

static int default_syntax(graphd_request *greq,
                          graphd_string_constraint const *strcon) {
  (void)greq;
  (void)strcon;

  /* Accept anything. */

  return 0;
}

static int graphd_comparator_default_andor(
    graphd_request *greq, unsigned long long low, unsigned long long high,
    graphd_direction direction, char const *ordering, char const *word_s,
    char const *word_e, char const *andor_s, char const *andor_e,
    bool andor_prefix, pdb_iterator **it_out) {
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  pdb_handle *pdb = g->g_pdb;
  pdb_iterator *or_it = NULL;
  pdb_iterator *and_it = NULL;
  pdb_iterator *sub_it = NULL;
  bool forward = direction != GRAPHD_DIRECTION_BACKWARD;
  char *wordandor_s = NULL;
  int err;
  char buf[200];

  cl_assert(cl, GRAPHD_DIRECTION_VALID(direction));
  if (pdb_word_utf8len(pdb, word_s, word_e) >= 5) {
    /*  Just use the prefix completion of word[].
     *  The rest is too far back to matter.
     */
    err = graphd_iterator_prefix_create(greq, word_s, word_e, low, high,
                                        direction, it_out);
    if (err != 0)
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_prefix_create", err,
                   "word=\"%.*s\"", (int)(word_e - word_s), word_s);
    return err;
  }

  /*  either	or(a::b*,  and(a, b*))
   *  or 		or(a::b, and(a, b))
   *
   *  (if andor_prefix or not)
   */
  wordandor_s = cm_sprintf(cm, "%.*s%.*s", (int)(word_e - word_s), word_s,
                           (int)(andor_e - andor_s), andor_s);
  if (wordandor_s == NULL) return ENOMEM;

  /*  Make <word_s> as a word.
   */
  err = pdb_iterator_word_create(pdb, word_s, word_e, low, high, forward, false,
                                 &sub_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_word_create", err,
                 "word=\"%.*s\"", (int)(word_e - word_s), word_s);
    goto err;
  }
  graphd_iterator_set_direction_ordering(pdb, sub_it, direction, ordering);

  /*  Make an <and>, and stick <word_s> under it.
   */
  err = graphd_iterator_and_create(greq, 2, low, high, direction, ordering,
                                   &and_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_and_create", err, "n=2");
    goto err;
  }
  err = graphd_iterator_and_add_subcondition(g, and_it, &sub_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_and_add_subcondition", err,
                 "sub_it=%s",
                 pdb_iterator_to_string(pdb, sub_it, buf, sizeof buf));
    goto err;
  }

  /*  Make <andor>, either as a word or prefix, depending
   *  on andor_prefix.  Then stick it under the same <and>.
   */
  err = andor_prefix
            ? graphd_iterator_prefix_create(greq, andor_s, andor_e, low, high,
                                            direction, &sub_it)
            : pdb_iterator_word_create(pdb, andor_s, andor_e, low, high,
                                       direction, false, &sub_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL,
                 andor_prefix ? "graphd_iterator_prefix_create"
                              : "pdb_iterator_word_create",
                 err, "andor=\"%.*s\"", (int)(andor_e - andor_s), andor_s);
    goto err;
  }
  graphd_iterator_set_direction_ordering(pdb, sub_it, direction, ordering);

  err = graphd_iterator_and_add_subcondition(g, and_it, &sub_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_and_add_subcondition", err,
                 "sub_it=\"%s\"",
                 pdb_iterator_to_string(pdb, sub_it, buf, sizeof buf));
    goto err;
  }

  /*  Finish the <and>.
   */
  if ((err = graphd_iterator_and_create_commit(g, and_it)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_and_create_commit", err,
                 "and_it=\"%s\"",
                 pdb_iterator_to_string(pdb, and_it, buf, sizeof buf));
    goto err;
  }

  /*  Make an <or>, and hang the <and> under it.
   */
  if ((err = graphd_iterator_or_create(greq, 2, forward, &or_it)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_create", err, "n=2");
    goto err;
  }
  if ((err = graphd_iterator_or_add_subcondition(or_it, &and_it)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_add_subcondition", err,
                 "sub_it=\"%s\"",
                 pdb_iterator_to_string(pdb, and_it, buf, sizeof buf));
    goto err;
  }

  /*  Hang <word::andor> on the other side of the <or>.
   */
  err = andor_prefix
            ? graphd_iterator_prefix_create(greq, wordandor_s,
                                            wordandor_s + strlen(wordandor_s),
                                            low, high, direction, &sub_it)
            : pdb_iterator_word_create(pdb, wordandor_s,
                                       wordandor_s + strlen(wordandor_s), low,
                                       high, direction, false, &sub_it);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL,
                 andor_prefix ? "graphd_iterator_prefix_create"
                              : "graphd_iterator_word_create",
                 err, "wordandor=\"%s\"", wordandor_s);
    goto err;
  }

  graphd_iterator_set_direction_ordering(pdb, sub_it, direction, ordering);
  if ((err = graphd_iterator_or_add_subcondition(or_it, &sub_it)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_add_subcondition", err,
                 "sub_it=\"%s\"",
                 pdb_iterator_to_string(pdb, sub_it, buf, sizeof buf));
    goto err;
  }

  /*  And complete the or.
   */
  if ((err = graphd_iterator_or_create_commit(or_it)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_create_commit", err,
                 "or_it=\"%s\"",
                 pdb_iterator_to_string(pdb, or_it, buf, sizeof buf));
    goto err;
  }

  cm_free(cm, wordandor_s);
  *it_out = or_it;
  return 0;

err:
  pdb_iterator_destroy(pdb, &or_it);
  pdb_iterator_destroy(pdb, &and_it);
  pdb_iterator_destroy(pdb, &sub_it);
  cm_free(cm, wordandor_s);

  return err;
}

/* @brief Given a match constraint, extract the next word from a query
 *
 *  Words are extracted to turn them into iterators that produce
 *  candidates for matching the query.
 *
 *  When primitives are indexed, their "words" are extracted and indexed
 *  separately. (There, words are consecutive sequences of non-ascii or
 * alphanumeric
 *  characters.)  In addition, numbers in the primitives are indexed as a whole,
 *  as normalized numbers, and as integer and fraction parts.
 *
 *  When primitives are queried, the query string is more complicated than
 *  just a copy of the primitive text - it has "magic" query punctuation
 *  and escaped real punctuation in it.
 *
 *  Given the query string, this function identifies substrings that must
 *  have occurred in all matching primitives, and return those substrings
 *  with a flag that says whether they're prefixes or full subword matches.
 *
 *  If *prefix is true, the *andor_s, *andor_e, *andor_prefix variables are
 *  relevant.  If *andor_s and *andor_e are non-NULL, they delimit the word
 *  following word_s/word_e, separated by optional punctuation.  The
 *  match text must contain either both "word" and "andor", or "wordandor".
 *  In either case, the "andor" part may be followed by more characters iff
 *  *andor_prefix is true.
 *
 * @param greq		Request for which this happens
 * @param s 		beginning of the query string
 * @param e		end of the query string
 * @param word_s	out: word to search for, beginning
 * @param word_e	out: word to search for, end
 * @param prefix	out: is this a prefix or do we want the whole word?
 * @param andor_s	out: second word, beginning
 * @param andor_e	out: second word, end
 * @param andor_prefix	out: is the second word a prefix, too?
 * @param state		in/out: iterator state
 *
 * @return true if another word has been assigned to word_s, word_e, and prefix
 * @return false once the iteration runs out of words.
 */
static bool graphd_match_subword_next(graphd_request *greq, char const *s,
                                      char const *e, char const **word_s,
                                      char const **word_e, bool *prefix,
                                      char const **andor_s,
                                      char const **andor_e, bool *andor_prefix,
                                      char const **state) {
  char const *r;
  cl_handle *cl = graphd_request_cl(greq);

  cl_assert(cl, state != NULL);
  cl_assert(cl, s != NULL);
  cl_assert(cl, e != NULL);
  cl_assert(cl, word_s != NULL);
  cl_assert(cl, word_e != NULL);
  cl_assert(cl, prefix != NULL);

  if ((r = *state) == NULL) *state = r = s;

  cl_assert(cl, r >= s && r <= e);

  /* Find the start and end of a word. */

  *andor_s = NULL;
  *andor_e = NULL;
  *word_s = NULL;
  while (r < e) {
    char *number_norm_buf = NULL;
    char const *number_r, *number_s, *number_e;
    char const *number_norm_s, *number_norm_e;
    int number_type;

    if (ISBREAK(r, e)) {
      /*  An unconditional non-word character.
       *  For example, white space, or an escaped
       *  punctuation character.
       *
       *  If we were inside a word, finish it here
       *  (as a non-prefix) and send it off.
       */
      if (*word_s != NULL) {
        *prefix = false;
        *word_e = r;
        *state = ENDBREAK(r);

        return true;
      }
      r = ENDBREAK(r);
      continue;
    }

    /*  We're standing on a number, and it's not adjacent to
     *  magic punctuation (*) -> treat it as a whole number.
     */
    number_r = r;
    if (!*word_s &&
        !(r > s && ((r[-1] == '*' && active_asterisk(s, r - 1, e)) ||
                    (r[-1] == '\\' && active_slash(s, r - 1, e)))) &&
        pdb_word_fragment_next(s, &number_r, e, &number_s, &number_e,
                               &number_type) &&
        number_type == PDB_WORD_NUMBER && (number_e == e || *number_e != '*')) {
      int err;

      /* Normalize the number. */

      err = pdb_word_number_normalize(greq->greq_req.req_cm, number_s, number_e,
                                      &number_norm_buf, &number_norm_s,
                                      &number_norm_e);
      if (err != 0) return err;

      /* The caller will look up the normalized number. */

      *word_s = number_norm_s;
      *word_e = number_norm_e;
      *state = number_r;

      return true;
    }

    if (ISPUNCT(*r)) {
      /*  A magic character ^ $ * or punctuation.
       *
       *  If we're inside a word, end it here, either
       *  as a prefix (for * and punctuation followed
       *  by a non-break) or a full word (if followed
       *  by break, $, or unconditional non-word character.)
       */
      if (*word_s != NULL) {
        *word_e = r;

        /* Skip past optional punctuation other
         * than '*'.
         */
        while (r < e && *r != '*' && !ISBREAK(r, e) && ISPUNCT(*r)) r++;

        /*  If we're not at the end of a word
         *  or on escaped punctuation,
         *  this may only be a prefix of something.
         */
        *prefix = r < e && !ISBREAK(r, e) && *r != '$';

        /*  We don't know where in the word the rest
         *  of this punctuated phrase will be; skip
         *  ahead to the next break.
         */
        if (*prefix) {
          if (r < e && ISWORD(*r)) {
            *andor_s = r;
            while (r < e && ISWORD(*r)) r++;
            *andor_e = r;

            while (r < e && *r != '*' && !ISBREAK(r, e) && ISPUNCT(*r)) r++;
            *andor_prefix = r < e && !ISBREAK(r, e) && *r != '$';
          }
          while (r < e && !ISBREAK(r, e)) {
            if (*r == '\\' && r + 1 < e) r++;
            r++;
          }
        }
        *state = r;
        return true;
      }

      /*  Punctuation at the beginning of a break-delimited
       *  area doesn't turn its right side into a suffix
       *  unless it's '*'.
       */
      if (*r == '*') {
        while (r < e && !ISBREAK(r, e)) {
          if (*r == '\\' && r + 1 < e) r++;
          r++;
        }
      } else {
        /* Just ignore it.  It wasn't indexed. */
        r++;
      }
    } else {
      if (ISWORD(*r)) {
        /*  Beginning of a word.  Stop
         *  recognizing numbers.
         */
        if (*word_s == NULL) *word_s = r;
      } else {
        if (*word_s != NULL) {
          *word_e = *state = r;
          return true;
        }
      }
      r++;
    }
  }
  if (*word_s != NULL) {
    *word_e = r;
    *state = r;

    return true;
  }

  /* Out of words. */
  return false;
}

/*
 * Strip off the non-alphanumeric at the end of a string and then
 * increment the string s.t. it is greater than the string that came
 * before. We expect a purley alphabetic sequence of characters, except
 * for *where.
 *
 * The return value is the "carry" bit: a string with all Z's can't be
 * incremented this way.
 */
static bool stringplusplus(cl_handle *cl, char *s, char *where) {
  cl_assert(cl, ISALPHA(*s));
  cl_assert(cl, where > s);
  cl_assert(cl, !ISALPHA(*where));
  cl_assert(cl, ISALPHA(where[-1]));

  where--;
  while (!ISALPHA(++(*where))) {
    /*
     * *where could have be {z|Z}, in that case it is no
     * longer alphanumeric and we need to go back and try again
     * on the previous byte.
     */
    if (where == s) return false;
    where--;
  }
  where[1] = 0;
  return true;
}

/*
 *
 * Find a superset of bins that match this constraint.
 * for > , we must cut the string at the first number or
 * space. i.e.
 * foo345		--> foo
 * test test test	--> test
 * If < we get even trickier
 * foo345		--> fop
 * test test test	--> tesu
 *
 * This is potentially grossly inefficeint if the first character is
 * [0-9].
 */

static int comparator_default_range_bins(graphd_request *greq, const char *lo_s,
                                         const char *lo_e, bool lo_strict,
                                         const char *hi_s, const char *hi_e,
                                         bool hi_strict, int *lo_bin,
                                         int *hi_bin) {
  graphd_session *gses = graphd_request_session(greq);
  cl_handle *cl = gses->gses_cl;
  cm_handle *cm = greq->greq_req.req_cm;
  graphd_handle *g = gses->gses_graphd;
  pdb_handle *pdb = g->g_pdb;
  char *low, *high;

  if (lo_s) {
    char *endat;
    low = cm_malcpy(cm, lo_s, lo_e - lo_s);

    if (low == NULL) {
      cl_log(cl, CL_LEVEL_ERROR,
             "comparator_default_range_bins:"
             " Cannot allocate %lli bytes",
             (long long)(lo_e - lo_s));
      return ENOMEM;
    }

    for (endat = low; *endat; endat++) {
      if (!ISALPHA(*endat)) {
        *endat = 0;
        break;
      }
    }

  } else {
    low = NULL;
  }

  if (hi_s) {
    char *endat;

    high = cm_malcpy(cm, hi_s, hi_e - hi_s);

    if (high == NULL) {
      cl_log(cl, CL_LEVEL_ERROR,
             "comparator_default_range_bins:"
             " Cannot allocate %lli bytes",
             (long long)(hi_e - hi_s));
      if (low) cm_free(cm, low);
      return ENOMEM;
    }
    if (!ISALPHA(*high)) {
      cm_free(cm, high);
      high = NULL;
    } else {
      const char *high_e = high + strlen(high);
      for (endat = high + 1; endat < (high_e); endat++) {
        if (!ISALPHA(*endat)) {
          if (!stringplusplus(cl, high, endat)) {
            cm_free(cm, high);
            high = NULL;
          }
          break;
        }
      }
    }
  } else {
    high = NULL;
  }
  cl_log(cl, CL_LEVEL_VERBOSE,
         "comparator_default_range_bins: expanded range to %s to %s",
         low ? low : "(null)", high ? high : "(null)");

  *lo_bin = pdb_bin_lookup(pdb, PDB_BINSET_STRINGS, low,
                           low ? low + strlen(low) : NULL, NULL);

  if (high) {
    *hi_bin = pdb_bin_lookup(pdb, PDB_BINSET_STRINGS, high,
                             high ? high + strlen(high) : NULL, NULL) +
              1;
  } else {
    *hi_bin = pdb_bin_end(pdb, PDB_BINSET_STRINGS) + (hi_strict ? 0 : 1);
  }

  if (low) cm_free(cm, low);

  if (high) cm_free(cm, high);
  return 0;
}

static int comparator_default_iterator(
    graphd_request *greq, int operation, const char *strcel_s,
    const char *strcel_e, int hash_type, unsigned long long low,
    unsigned long long high, graphd_direction direction, char const *ordering,
    bool *indexed_inout, pdb_iterator **it_out) {
  pdb_iterator *and_it = NULL;
  pdb_iterator *other_it = NULL;
  graphd_handle *g = graphd_request_graphd(greq);
  pdb_handle *pdb;
  pdb_iterator *sub_it;
  cl_handle *cl = graphd_request_cl(greq);
  int err;
  char const *substr = NULL, *word_s, *word_e = NULL;
  char const *andor_s, *andor_e;
  bool prefix = NULL, andor_prefix = false;
  bool forward = direction != GRAPHD_DIRECTION_BACKWARD;

  /*  If this returns 0 and *it_out is NULL,
   *  it just couldn't make a useful iterator restriction
   *  for this expression -- that's not an error.
   */
  *it_out = NULL;
  pdb = graphd_request_graphd(greq)->g_pdb;
  cl_assert(cl, GRAPHD_DIRECTION_VALID(direction));

  /*
   * We don't do value=NULL indexing here.
   */
  if (operation == GRAPHD_OP_EQ) {
    if (strcel_s == NULL) return 0;

    err = pdb_hash_iterator(pdb, hash_type, strcel_s, (strcel_e - strcel_s),
                            low, high, forward, &sub_it);
    if (err == 0) {
      *it_out = sub_it;
      *indexed_inout = true;
    }
    return err;
  }

  if (operation != GRAPHD_OP_MATCH || strcel_s == NULL) return 0;

  cl_assert(cl, strcel_s != NULL && strcel_e != NULL);

  while (graphd_match_subword_next(greq, strcel_s, strcel_e, &word_s, &word_e,
                                   &prefix, &andor_s, &andor_e, &andor_prefix,
                                   &substr)) {
    if (prefix) {
      if (andor_s != NULL) {
        err = graphd_comparator_default_andor(greq, low, high, direction,
                                              ordering, word_s, word_e, andor_s,
                                              andor_e, andor_prefix, &sub_it);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_comparator_default_andor",
                       err, "word=\"%.*s\"", (int)(word_e - word_s), word_s);
          return err;
        }
      } else {
        err = graphd_iterator_prefix_create(
            greq, word_s, word_e, low, high,
            forward ? GRAPHD_DIRECTION_FORWARD : GRAPHD_DIRECTION_BACKWARD,
            &sub_it);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_prefix_create", err,
                       "word=\"%.*s\"", (int)(word_e - word_s), word_s);
          return err;
        }
      }
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

    /*  If this is the second subiterator, move it
     *  under an "and" with the first.
     */
    if (and_it == NULL && other_it == NULL)
      other_it = sub_it;
    else {
      if (and_it == NULL) {
        err = graphd_iterator_and_create(greq, 2, low, high, direction,
                                         ordering, &and_it);
        if (err != 0) {
          pdb_iterator_destroy(pdb, &other_it);
          return err;
        }

        err = graphd_iterator_and_add_subcondition(g, and_it, &other_it);
        if (err != 0) {
          pdb_iterator_destroy(pdb, &and_it);
          pdb_iterator_destroy(pdb, &other_it);
          return err;
        }
      }
      err = graphd_iterator_and_add_subcondition(g, and_it, &sub_it);
      if (err != 0) {
        char buf[200];
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_add_subcondition", err,
                     "iterator=%s",
                     pdb_iterator_to_string(pdb, sub_it, buf, sizeof buf));

        pdb_iterator_destroy(pdb, &sub_it);
        pdb_iterator_destroy(pdb, &and_it);

        return err;
      }
    }
  }
  if (and_it != NULL) {
    err = graphd_iterator_and_create_commit(g, and_it);
    if (err != 0) {
      char buf[200];
      cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_and_create_commit", err,
                   "iterator=%s",
                   pdb_iterator_to_string(pdb, and_it, buf, sizeof buf));

      pdb_iterator_destroy(pdb, &and_it);
      return err;
    }
  }
  *it_out = (and_it == NULL) ? other_it : and_it;
  return 0;
}

/*
 * Make an iterator that is a superset of results for s..e under
 * operation. Operation should be GRAPHD_OP_EQ or GRAPHD_OP_MATCH
 */
int graphd_value_default_iterator(
    graphd_request *greq, int operation, const char *s, const char *e,
    unsigned long long low, unsigned long long high, graphd_direction direction,
    char const *ordering, bool *indexed_inout, pdb_iterator **it_out) {
  return comparator_default_iterator(greq, operation, s, e, PDB_HASH_VALUE, low,
                                     high, direction, ordering, indexed_inout,
                                     it_out);
}

/*
 * Get an iterator for a particular NAME
 */
int graphd_comparator_default_name_iterator(
    graphd_request *greq, graphd_string_constraint const *strcon,
    pdb_iterator *and_it, unsigned long long low, unsigned long long high,
    graphd_direction direction, char const *ordering, bool *indexed_inout) {
  graphd_handle *g = graphd_request_graphd(greq);
  cl_handle *cl = graphd_request_cl(greq);
  pdb_handle *pdb = g->g_pdb;
  pdb_iterator *sub_it = NULL;
  pdb_iterator *or_it = NULL;
  size_t n = 0;
  bool all_indexed = true;
  int err;

  graphd_string_constraint_element const *strcel;

  /*
   * We don't support any kind of special indexing for names
   */
  if (strcon->strcon_op != GRAPHD_OP_EQ) return 0;

  cl_assert(cl, strcon != NULL);

  if (strcon->strcon_head == NULL) {
    /* Nothing to match against. */
    return 0;
  }

  if (strcon->strcon_head->strcel_next == NULL) {
    /* A single constraint.
     */
    strcel = strcon->strcon_head;
    err = comparator_default_iterator(
        greq, strcon->strcon_op, strcel->strcel_s, strcel->strcel_e,
        PDB_HASH_NAME, low, high, direction, ordering, indexed_inout, &sub_it);
    if (err != 0) return err;

    if (sub_it != NULL) {
      err = graphd_iterator_and_add_subcondition(g, and_it, &sub_it);
      if (err != 0) pdb_iterator_destroy(pdb, &sub_it);
    }
    return err;
  }

  /* Multiple constraints. ("one of")
   *  	value=("un" "deux" "trois")
   * Primitives match an OR of the values.
   */

  /* How many result sets will we merge? */
  n = 0;
  for (strcel = strcon->strcon_head; strcel != NULL;
       strcel = strcel->strcel_next)
    n++;

  /* Create the OR that will merge them. */
  err = graphd_iterator_or_create(
      greq, n, direction != GRAPHD_DIRECTION_BACKWARD, &or_it);
  if (err != 0) return err;

  /* All the elements of the list... */
  for (strcel = strcon->strcon_head; strcel != NULL;
       strcel = strcel->strcel_next) {
    bool one_index;

    one_index = false;

    /* sub_it implements this subconstraint. */
    err = comparator_default_iterator(
        greq, strcon->strcon_op, strcel->strcel_s, strcel->strcel_e,
        PDB_HASH_NAME, low, high, direction, ordering, indexed_inout, &sub_it);
    if (err != 0) {
      pdb_iterator_destroy(pdb, &or_it);
      return err;
    }
    if (sub_it == NULL) {
      /* Not an error - but also means that
       * this string constraint doesn't translate
       * into anything we can use.
       */
      pdb_iterator_destroy(pdb, &or_it);
      return 0;
    }
    all_indexed &= one_index;
    err = graphd_iterator_or_add_subcondition(or_it, &sub_it);
    if (err != 0) {
      pdb_iterator_destroy(pdb, &sub_it);
      pdb_iterator_destroy(pdb, &or_it);

      return err;
    }
  }
  *indexed_inout |= all_indexed;

  /* Move the OR below the calling "AND".
  */
  if ((err = graphd_iterator_or_create_commit(or_it)) != 0 ||
      (err = graphd_iterator_and_add_subcondition(g, and_it, &or_it)) != 0) {
    pdb_iterator_destroy(pdb, &or_it);
    return err;
  }
  return 0;
}

#if 0
static bool small_int_part(
	cl_handle	* cl,
	char const	* s,
	char const	* e,
	long		* num)
{
	char const	* pre_s, * point_s, * post_s;
	char const	* r;
	long	  	  val = 0;
	int 		  err;

	err = pdb_word_number_split(s, e, &pre_s, &point_s, &post_s);
	if (err != 0)
		return false;

	for (r = pre_s; r < point_s; r++)
	{
		val *= 10;
		val += *r - '0';
		if (val > 99999)
			return false;
	}
	*num = (*s == '-') ? -val : val;
	return true;
}
#endif
bool graphd_comparator_default_prefix_word_next(char const *s, char const *e,
                                                char const **word_s,
                                                char const **word_e,
                                                bool *prefix,
                                                char const **state) {
  char const *r;

  if ((r = *state) == NULL) *state = r = s;

  /* Find the start and end of a word. */

  *prefix = false;
  *word_s = NULL;
  for (; r < e; r++) {
    if (ISWORD(*r)) {
      if (*word_s == NULL) *word_s = r;
    } else {
      /* Are we inside a word? */
      if (*word_s != NULL) {
        *word_e = r;
        *state = r + 1;

        return true;
      }
    }
  }
  if (*word_s != NULL) {
    *word_e = e;
    *state = e;

    /* We can't tell whether this is a prefix
     * or a full word!
     */
    *prefix = true;

    return true;
  }

  /* Out of words. */
  return false;
}

#if 0
static int add_range_or(
	graphd_request 		* greq,
	pdb_iterator		* and_it,
	unsigned long long	  low,
	unsigned long long	  high,
	long			  lo_val,
	long			  hi_val,
	graphd_direction	  direction)
{
	graphd_handle		* g = graphd_request_graphd(greq);
	pdb_handle		* pdb = g->g_pdb;
	cl_handle		* cl  = graphd_request_cl(greq);
	long			  v;
	pdb_iterator		* or_it = NULL;
	pdb_iterator		* sub_it = NULL;
	int			  err;

	err = graphd_iterator_or_create(greq, 1 + hi_val - lo_val,
		direction != GRAPHD_DIRECTION_BACKWARD, &or_it);
	if (err != 0)
	{
		cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_or_create",
			err, "%ld..%ld", lo_val, hi_val);
		return err;
	}

	for (v = lo_val; v <= hi_val; v++)
	{
		char 	buf[200];
		snprintf(buf, sizeof buf, "%ld", v);

		err = pdb_iterator_word_create(pdb, buf, buf + strlen(buf),
			low, high, direction != GRAPHD_DIRECTION_BACKWARD,
			false, &sub_it);
		if (err != 0)
		{
			cl_log_errno(cl, CL_LEVEL_FAIL,
				"pdb_iterator_word_create", err,
				"word=\"%s\"", buf);
			pdb_iterator_destroy(pdb, &or_it);
			pdb_iterator_destroy(pdb, &sub_it);
			return err;
		}
		err = graphd_iterator_or_add_subcondition(or_it, &sub_it);
		if (err != 0)
		{
			cl_log_errno(cl, CL_LEVEL_FAIL,
				"pdb_iterator_or_add_subcondition",
				err, "word=\"%s\"", buf);
			pdb_iterator_destroy(pdb, &or_it);
			pdb_iterator_destroy(pdb, &sub_it);
			return err;
		}
	}
	if ((err = graphd_iterator_or_create_commit(or_it)) != 0)
	{
		cl_log_errno(cl, CL_LEVEL_FAIL,
			"graphd_iterator_or_create_commit", err,
			"%ld..%ld", lo_val, hi_val);
		pdb_iterator_destroy(pdb, &or_it);
		return err;
	}
	if ((err = graphd_iterator_and_add_subcondition(g, and_it, &or_it)) != 0)
	{
		cl_log_errno(cl, CL_LEVEL_FAIL,
			"graphd_iterator_and_add_subcondition", err,
			"%ld..%ld", lo_val, hi_val);
		pdb_iterator_destroy(pdb, &or_it);
		return err;
	}
	return 0;
}
#endif
#if 0
static size_t first_difference(
	char const	* a_s,
	char const	* a_e,
	char const	* b_s,
	char const	* b_e)
{
	char const	* a_r = a_s,
			* b_r = b_s;

	for (a_r = a_s, b_r = b_s; a_r < a_e && b_r < b_e; a_r++, b_r++)
		if (TOLOWER(*a_r) != TOLOWER(*b_r))
			break;

	return a_r - a_s;
}
#endif

#if 0
/*  Create iterators that match values from the
 *  range between lo_sc and hi_sc.
 */
static int graphd_comparator_default_iterator_range(
	graphd_request 		* greq,
	const char 		* lo_s,
	const char 		* lo_e,
	const char 		* hi_s,
	const char 		* hi_e,
	pdb_iterator		* and_it,
	unsigned long long 	  low,
	unsigned long long 	  high,
	graphd_direction	  direction,
	bool			  valueforward,
	char const		* ordering,
	bool			* indexed_inout)
{
	graphd_session		* gses = greq->greq_session;
	cl_handle		* cl = gses->gses_cl;
	cm_handle		* cm = greq->greq_req.req_cm;
	graphd_handle		* g = gses->gses_graphd;
	pdb_handle		* pdb = g->g_pdb;
	char			  buf[200];
	int			  err;
	int 			  hi_w_type, lo_w_type;
	char const		* hi_w_s, * lo_w_s,
				* hi_w_e, * lo_w_e,
				* hi_p, * lo_p;
	size_t			  diff;
	int 			  i;



	if (lo_s == NULL || hi_s == NULL)
	{
		pdb_iterator * sub_it;
		/*
		 * A one sided or 0-sided iterator
		 */

		sub_it = NULL;
		err = comparator_default_binned_range(
			greq,
			lo_s,
			lo_e,
			hi_s,
			hi_e,
			low,
			high,
			direction,
			valueforward,
			ordering,
			and_it,
			&sub_it);
		if (err)
			return err;

		cl_assert(cl, sub_it);
		err = graphd_iterator_and_add_subcondition(
			g, and_it, &sub_it);
		if (err != 0)
		{
			cl_log_errno(cl, CL_LEVEL_FAIL,
				"graphd_iterator_add_subcondition", err,
				"iterator=%s",
				pdb_iterator_to_string(pdb,
					sub_it, buf, sizeof buf));
			pdb_iterator_destroy(pdb, &sub_it);
			return err;
		}


		*indexed_inout = true;
		return 0;


	}
	lo_p = lo_s;
	hi_p = hi_s;

	for (i = 0; ; i++)
	{
		bool		  lo_there, hi_there;
		char	 	* lo_norm_buf = NULL, * hi_norm_buf = NULL;
		char const	* word_s, * word_e;
		char const 	* lo_norm_s, * lo_norm_e,
			   	* hi_norm_s, * hi_norm_e;
		pdb_iterator	* sub_it = NULL;

		lo_there = pdb_word_fragment_next(
			lo_s, &lo_p, lo_e, &lo_w_s, &lo_w_e, &lo_w_type);
		hi_there = pdb_word_fragment_next(
			hi_s, &hi_p, hi_e, &hi_w_s, &hi_w_e, &hi_w_type);

		if (!lo_there || !hi_there)
			break;
		if (lo_w_type != hi_w_type)
		{
			/*  Treat numbers as atoms, and pick up possible
			 *  prefix overlaps that way.
			 */
			if (lo_w_type == PDB_WORD_NUMBER)
				lo_w_type = PDB_WORD_ATOM;
			if (hi_w_type == PDB_WORD_NUMBER)
				hi_w_type = PDB_WORD_ATOM;

			if (lo_w_type != hi_w_type)
				break;
		}

		if (  lo_w_type != PDB_WORD_NUMBER
		   && lo_w_type != PDB_WORD_ATOM)
		   	continue;

		if (lo_w_type == PDB_WORD_NUMBER)
		{
			long 	hi_val, lo_val;

			/*  Normalize both.
			 */
			err = pdb_word_number_normalize(cm, lo_w_s, lo_w_e,
				&lo_norm_buf, &lo_norm_s, &lo_norm_e);
			if (err != 0)
				return err;

			err = pdb_word_number_normalize(cm, hi_w_s, hi_w_e,
				&hi_norm_buf, &hi_norm_s, &hi_norm_e);
			if (err != 0)
				return err;

			if (  hi_norm_e - hi_norm_s == lo_norm_e - lo_norm_s
			   && memcmp(hi_norm_s,
			   	     lo_norm_s,
				     lo_norm_e - lo_norm_s) == 0)
			{
				/*  Normalized, they're the same.  Add the
				 *  normalized number text to the substring
				 *  match candidates.
				 */
				word_s = lo_norm_s;
				word_e = lo_norm_e;

				goto word_match;
			}

			/*  The normalized values differ.
			 *  Does their integer part have a tractable range?
			 */
			if (  small_int_part(cl, lo_norm_s, lo_norm_e, &lo_val)
			   && small_int_part(cl, hi_norm_s, hi_norm_e, &hi_val)
			   && hi_val >= lo_val
			   && hi_val - lo_val < 1000)
			{
				err = add_range_or(greq, and_it, low, high,
					lo_val, hi_val, direction);
				if (err != 0)
				{
					cl_log_errno(cl, CL_LEVEL_FAIL,
						"add_range_or", err, "%ld..%ld",
						lo_val, hi_val);
					return err;
				}
				break;
			}

			/* Use the normalized values in a prefix match.
			 */
			hi_w_s = hi_norm_s;
			hi_w_e = hi_norm_e;
			lo_w_s = lo_norm_s;
			lo_w_e = lo_norm_e;

			goto prefix_match;
		}
		else if (lo_w_type == PDB_WORD_ATOM)
		{
			/*  If high and low contain the same prefix, that
			 *  prefix will appear in all matches, too.
			 */
			if (  lo_w_e - lo_w_s == hi_w_e - hi_w_s
			   && strncasecmp(hi_w_s, lo_w_s, lo_w_e - lo_w_s) == 0)
			{
				word_s = lo_w_s;
				word_e = lo_w_e;

				goto word_match;
			}

			/*  Words are different.
			 *  If there's a first point that they
			 *  differ on, add the prefix to the
			 *  substring match candidates.
			 */
			goto prefix_match;
		}
		continue;

		/* Word match.
		 */
	word_match:
		err = pdb_iterator_word_create(pdb, word_s, word_e, low, high,
			direction != GRAPHD_DIRECTION_BACKWARD, false, &sub_it);
		if (err != 0)
		{
			cl_log_errno(cl, CL_LEVEL_FAIL,
				"pdb_iterator_word_create",
				err, "word=\"%.*s\"",
				(int)(word_e - word_s), word_s);
			return err;
		}
		graphd_iterator_set_direction_ordering(
			pdb, sub_it, direction, ordering);

		err = graphd_iterator_and_add_subcondition(g, and_it, &sub_it);
		if (err != 0)
		{
			cl_log_errno(cl, CL_LEVEL_FAIL,
				"graphd_iterator_and_add_subcondition",
				err, "for %s", buf);
			pdb_iterator_destroy(pdb, &sub_it);
			return err;
		}

		cl_log(cl, CL_LEVEL_VERBOSE,
			"graphd_comparator_default_iterator_range: "
			"matches contain word \"%.*s\"",
			(int)(word_e - word_s), word_s);
		continue;

		/*
		 *  Prefix match.
		 *
		 *  We can do this with either a prefix or a binned iterator.
		 *  If we have a long prefix, the prefix iterator is likely
		 *  to do a better job than binning will.
		 *
		 */
	prefix_match:
		diff = first_difference(
			lo_w_s, lo_w_e,
			hi_w_s, hi_w_e);
		if ((diff > 4) || i)
		{
			err = graphd_iterator_prefix_create(greq,
				lo_w_s, lo_w_s + diff, low, high,
				direction, &sub_it);
			if (err != 0)
			{
				cl_log_errno(cl, CL_LEVEL_FAIL,
				       "graphd_iterator_prefix_create",
					err, "word=\"%.*s\"",
					(int)diff, lo_w_s);
				return err;
			}
			err = graphd_iterator_and_add_subcondition(
				g, and_it, &sub_it);
			if (err != 0)
			{
				cl_log_errno(cl, CL_LEVEL_FAIL,
					"graphd_iterator_add_subcondition", err,
					"iterator=%s",
					pdb_iterator_to_string(pdb,
						sub_it, buf, sizeof buf));
				pdb_iterator_destroy(pdb, &sub_it);
				return err;
			}
		}
		else
		{
			err = comparator_default_binned_range(
				greq,
				lo_s,
				lo_e,
				hi_s,
				hi_e,
				low,
				high,
				direction,
				valueforward,
				ordering,
				and_it,
				&sub_it);
			if (err)
				return err;
			err = graphd_iterator_and_add_subcondition(
				g, and_it, &sub_it);
			if (err != 0)
			{
				cl_log_errno(cl, CL_LEVEL_FAIL,
					"graphd_iterator_add_subcondition", err,
					"iterator=%s",
					pdb_iterator_to_string(pdb,
						sub_it, buf, sizeof buf));
				pdb_iterator_destroy(pdb, &sub_it);
				return err;
			}

			*indexed_inout = true;
		}
		break;
	}
	return 0;
}

#endif

static int default_sort_compare(graphd_request *greq, char const *s1,
                                char const *e1, char const *s2,
                                char const *e2) {
  int i;
  i = graphd_text_compare(s1, e1, s2, e2);
  return i;
}

int graphd_iterator_null_value_create(graphd_request *greq,
                                      unsigned long long low,
                                      unsigned long long high,
                                      pdb_iterator **it_out) {
  int err;

  pdb_iterator *all_it;
  cl_handle *cl = graphd_request_cl(greq);
  err = pdb_iterator_all_create(GREQ_PDB(greq), low, high, true, &all_it);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_iterator_all_create", err,
                 "Can't create all from %llx to %llx", low, high);
    return err;
  }

  err = graphd_iterator_without_any_value_create(greq, &all_it, it_out);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_iterator_without_any_value_create",
                 err, "Can't create without-value iterator");
    return err;
  }

  return 0;
}

/*
 * How much space will we need?
 * Give ourselves enough space to handle our input
 */
#define MAX(a, b) ((a) > (b) ? (a) : (b))
static size_t default_vrange_size(graphd_request *greq, const char *lo_s,
                                  const char *lo_e, const char *hi_s,
                                  const char *hi_e) {
  size_t n = MAX(hi_e - hi_s, lo_e - lo_s) + 1;
  return sizeof(default_vrange_state) + MAX(n, 32);
}

static int default_vrange_start(graphd_request *greq, graphd_value_range *vr,
                                void *private_data)

{
  default_vrange_state *state = private_data;
  int err;

  if (state->dvs_magic == DVS_MAGIC) {
    if (graphd_vrange_forward(greq, vr))
      state->dvs_cur = state->dvs_lo;
    else
      state->dvs_cur = state->dvs_hi - 1;

    cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW,
           "default_vrange_start: Resetting iterator");
    return 0;
  }

  cl_assert(graphd_request_cl(greq), state->dvs_magic == 0);

  state->dvs_magic = DVS_MAGIC;

  state->dvs_test_len =
      MAX(32, MAX(vr->vr_hi_e - vr->vr_hi_s, vr->vr_lo_e - vr->vr_lo_s));
  if (default_sort_compare(greq, vr->vr_lo_s, vr->vr_lo_e, vr->vr_hi_s,
                           vr->vr_hi_e) > 0)
    return GRAPHD_ERR_NO;

  /*
   * XXX Right now we only know how to do this via binning.
   * Later, learn how to use the number word hash if its a wise
   * idea
   */

  /*
   * Expand the lo--hi range to include everything we might return
   */

  err = comparator_default_range_bins(
      greq, vr->vr_lo_s, vr->vr_lo_e, vr->vr_lo_strict, vr->vr_hi_s,
      vr->vr_hi_e, vr->vr_hi_strict, &(state->dvs_lo), &(state->dvs_hi));

  if (err) return err;

  cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW, "default_vrange_start: %i %i",
         state->dvs_lo, state->dvs_hi);

  if (graphd_vrange_forward(greq, vr))
    state->dvs_cur = state->dvs_lo;
  else
    state->dvs_cur = state->dvs_hi - 1;
  return 0;
}

static int default_vrange_it_next(graphd_request *greq, graphd_value_range *vr,
                                  void *private_data, pdb_id low, pdb_id high,
                                  pdb_iterator **it_out, pdb_budget *budget) {
  cl_handle *cl = graphd_request_cl(greq);
  pdb_handle *pdb = GREQ_PDB(greq);
  default_vrange_state *state = private_data;
  int err = 0;

  cl_assert(cl, state->dvs_magic == DVS_MAGIC);
  *it_out = NULL;

  for (;;) {
    if (graphd_vrange_forward(greq, vr) ? state->dvs_cur >= state->dvs_hi
                                        : state->dvs_cur < state->dvs_lo) {
      cl_log(cl, CL_LEVEL_VERBOSE, "default_vrange_it_next: end of range");
      return GRAPHD_ERR_NO;
    }

    cl_log(cl, CL_LEVEL_SPEW, "default_vrange_it_next: now on bin %i",
           state->dvs_cur);

    if (state->dvs_cur == pdb_bin_end(pdb, PDB_BINSET_STRINGS)) {
      err = graphd_iterator_null_value_create(greq, low, high, it_out);
    } else {
      err = pdb_bin_to_iterator(pdb, state->dvs_cur, low, high, true, true,
                                it_out);
    }

    *budget -= PDB_COST_ITERATOR;
    if (err != 0 && err != GRAPHD_ERR_NO) return err;

    if (err == 0 && pdb_iterator_null_is_instance(pdb, *it_out)) {
      pdb_iterator_destroy(pdb, it_out);
      cl_assert(cl, *it_out == NULL);
    }
    err = 0;

    if (graphd_vrange_forward(greq, vr))
      state->dvs_cur++;
    else
      state->dvs_cur--;

    if (err == 0 && *it_out != NULL) return 0;

    if (*budget <= 0) return PDB_ERR_MORE;

    if (err != 0) return err;
  }

  return -1;
}

static int default_vrange_statistics(graphd_request *greq,
                                     graphd_value_range *vr,
                                     void *private_state,
                                     unsigned long long *total_ids,
                                     pdb_budget *next_cost, pdb_budget *budget)

{
  cl_handle *cl = graphd_request_cl(greq);
  int its;
  default_vrange_state *state;
  state = private_state;

  cl_assert(cl, state->dvs_magic == DVS_MAGIC);
  cl_assert(cl, state->dvs_lo <= state->dvs_hi);

  its = state->dvs_hi - state->dvs_lo + 1;

  *next_cost = PDB_COST_HMAP_ELEMENT;
  *total_ids =
      its * (1 +
             pdb_primitive_n(GREQ_PDB(greq)) /
                 (pdb_bin_end(GREQ_PDB(greq), PDB_BINSET_STRINGS) * 2));

  return 0;
}

static int default_vrange_seek(graphd_request *greq, graphd_value_range *vr,
                               void *private_data, const char *s, const char *e,
                               pdb_id id, pdb_id low, pdb_id high,
                               pdb_iterator **it_out) {
  cl_handle *cl = graphd_request_cl(greq);
  pdb_handle *pdb = GREQ_PDB(greq);
  int err;
  int bin;
  char buf[200];
  pdb_iterator *it;

  default_vrange_state *state = private_data;

  cl_assert(cl, state->dvs_magic == DVS_MAGIC);

  if (s == NULL) {
    if (!vr->vr_hi_s) {
      state->dvs_cur = pdb_bin_end(pdb, PDB_BINSET_STRINGS);
      err = graphd_iterator_null_value_create(greq, low, high, it_out);
      if (err) return err;
      err = pdb_iterator_find_nonstep(pdb, *it_out, id, &id);

      if (err) {
        cl_log_errno(cl, CL_LEVEL_VERBOSE, "pdb_iterator_find_nonstep", err,
                     "Can't find %llx in %s", (unsigned long long)id,
                     pdb_iterator_to_string(pdb, *it_out, buf, sizeof buf));
        return err;
      }
      state->dvs_cur =
          state->dvs_cur + (graphd_vrange_forward(greq, vr) ? 1 : -1);

      return 0;

    } else {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "default_vrange_seek: got null value but "
             "dvs_include_null is  false");
    }
  }

  bin = pdb_bin_lookup(pdb, PDB_BINSET_STRINGS, s, e, NULL);

  cl_log(cl, CL_LEVEL_SPEW, "default_vrange_seek[%llx] %.*s seeks to be %i",
         (unsigned long long)id, (int)(e - s), s, bin);

  err = pdb_bin_to_iterator(pdb, bin, low, high, true, false, &it);
  if (err) return err;
  err = pdb_iterator_find_nonstep(pdb, it, id, &id);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_VERBOSE, "pdb_iterator_find_nonstep", err,
                 "error while fast-forwarding to %llx in bin %i",
                 (unsigned long long)(id), bin);
    return err;
  }
  state->dvs_cur = bin + (graphd_vrange_forward(greq, vr) ? 1 : -1);
  *it_out = it;
  return 0;
}

/*
 * Is a string equivalent to the empty string?
 * (null string or string of all spaces?
 */
static bool isempty(const char *s, const char *e) {
  for (; s < e; s++) {
    if (!isspace(*s)) return false;
  }
  return true;
}

static int default_value_in_range(graphd_request *greq, graphd_value_range *vr,
                                  void *private_data, const char *s,
                                  const char *e, bool *string_in_range) {
  const char *cs;
  int i = 0;
  const char *bs, *be;
  int bin;

  default_vrange_state *state = private_data;
  cl_assert(graphd_request_cl(greq), state->dvs_magic == DVS_MAGIC);
  if (s == NULL) {
    if (!vr->vr_hi_s) {
      *string_in_range = false;
      return 0;
    } else {
      if (graphd_vrange_forward(greq, vr)) {
        *string_in_range = true;
      } else {
        *string_in_range =
            state->dvs_cur == pdb_bin_end(GREQ_PDB(greq), PDB_BINSET_STRINGS);
      }
    }
  }

  bin = state->dvs_cur;
  if (bin == 0 && graphd_vrange_forward(greq, vr)) {
    *string_in_range = false;
    return 0;
  }
  if (bin == (pdb_bin_end(GREQ_PDB(greq), PDB_BINSET_STRINGS)) &&
      graphd_vrange_forward(greq, vr)) {
    *string_in_range = false;
    return 0;
  }

  bin += graphd_vrange_forward(greq, vr) ? -1 : 1;

  /*
   * Remove leading spaces from the test string
   */
  while (isspace(*s) && s < e) s++;
  /*
   * Find the starting string for the current bin
   */
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
    /*
     * Calculate the string that is the first string that we
     * know sorts >= s..e
     *
     * Converts:
     * "foo0" to "fop"
     * "foo mars" to "fop"
     * "frollic" to "frollic"
     */
    for (cs = s; (cs < e) && (i < state->dvs_test_len); cs++) {
      if (!ISALPHA(*cs)) {
        if (i) state->dvs_test_string[i - 1]++;
        break;
      }
      state->dvs_test_string[i] = *cs;
      i++;
    }
    cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW,
           "default_value_in_range: comparing '%.*s' vs '%.*s'", i,
           state->dvs_test_string, (int)(be - bs), bs);

    if (i == 0) {
      /*
       * Must use unsigned for char comparisons
       * or unicode will bite you
       */

      unsigned char tfirst;
      unsigned char bfirst;
      /*
       * Hacks to deal with the first char being
       * !alpha so we don't have to search over
       * everything
       */

      if ((be - bs) == 0) {
        *string_in_range = false;
        return 0;
      }
      bfirst = *bs;

      if (isempty(s, e)) {
        if (*bs > ' ') {
          *string_in_range = true;
          return 0;
        } else {
          *string_in_range = false;
          return 0;
        }
      }

      /*
       * If our test value is numeric but the bin value
       * comes after all numbers, we're done.
       */
      tfirst = *s;
      if (isdigit(tfirst) && (bfirst >= 'A')) {
        *string_in_range = true;
        return 0;
      }

      cl_assert(graphd_request_cl(greq), !ISALPHA(tfirst));

      /*
       * our test value is not alphanumeric.
       * Once we're beyond the numeric range, we can
       * compare this directly
       */
      if (bfirst >= 'A') {
        if (tfirst <= bfirst)
          *string_in_range = true;
        else
          *string_in_range = false;

        return 0;
      }

      *string_in_range = false;
      return 0;
    }

    if (graphd_text_compare(state->dvs_test_string, state->dvs_test_string + i,
                            bs, be) < 0)
      *string_in_range = true;
    else
      *string_in_range = false;
  } else {
    /*
     * Calculate the first string that we know sorts <= to s..e
     * so,
     * "foo0" becomes "foo"
     * "frollic" becomes "frollic"
     */
    for (cs = s; (cs < e) && (i < state->dvs_test_len); cs++) {
      if (!ISALPHA(*cs)) break;
      state->dvs_test_string[i] = *cs;
      i++;
    }

    cl_log(graphd_request_cl(greq), CL_LEVEL_SPEW,
           "default_value_in_range: comparing '%.*s' vs '%.*s'", i,
           state->dvs_test_string, (int)(be - bs), bs);
    if (graphd_text_compare(state->dvs_test_string, i + state->dvs_test_string,
                            bs, be) >= 0)
      *string_in_range = true;
    else
      *string_in_range = false;
  }
  return 0;
}

static int default_vrange_freeze(graphd_request *greq, graphd_value_range *vr,
                                 void *private_data, cm_buffer *buf)

{
  default_vrange_state *state = private_data;
  int err;
  cl_assert(graphd_request_cl(greq), state->dvs_magic == DVS_MAGIC);

  err = cm_buffer_sprintf(buf, "%i", state->dvs_cur);

  return err;
}

static int default_vrange_thaw(graphd_request *greq, graphd_value_range *vr,
                               void *private_data, const char *s,
                               const char *e) {
  default_vrange_state *state = private_data;
  int err;

  cl_assert(graphd_request_cl(greq), state->dvs_magic == DVS_MAGIC);

  err = pdb_iterator_util_thaw(GREQ_PDB(greq), &s, e, "%d", &(state->dvs_cur));

  if (err) {
    cl_log(graphd_request_cl(greq), CL_LEVEL_FAIL,
           "default_vrange_thaw: Can't parse integer out of %.*s", (int)(e - s),
           s);
    return GRAPHD_ERR_LEXICAL;
  }

  if (s != e) {
    cl_log(graphd_request_cl(greq), CL_LEVEL_FAIL,
           "default_vrange_thaw: extra bytes after integer");
    return GRAPHD_ERR_LEXICAL;
  }

  if (state->dvs_cur < (state->dvs_lo - 1)) {
    cl_log(graphd_request_cl(greq), CL_LEVEL_FAIL,
           "default_vrange_thaw: %i is outside range %i - %i", state->dvs_cur,
           state->dvs_lo, state->dvs_hi);
    return GRAPHD_ERR_LEXICAL;
  }

  /*
   * dvs_cur gets to be dvs_hi + 1 after we've read the last bin,
   * but before we've tried to read the after-last bin and had a change
   * to return GRAPHD_ERR_NO
   */
  if (state->dvs_cur > state->dvs_hi + 1) {
    cl_log(graphd_request_cl(greq), CL_LEVEL_FAIL,
           "default_vrange_thaw: %i is outside range %i - %i", state->dvs_cur,
           state->dvs_lo, state->dvs_hi);
    return GRAPHD_ERR_LEXICAL;
  }

  return 0;
}

graphd_comparator const graphd_comparator_default[1] = {
    {.cmp_locale = "",
     .cmp_name = "default",
     .cmp_alias = NULL,
     .cmp_syntax = default_syntax,
     .cmp_eq_iterator = graphd_value_default_iterator,
     .cmp_iterator_range = NULL,
     .cmp_glob = glob_match,
     .cmp_sort_compare = default_sort_compare,
     .cmp_vrange_size = default_vrange_size,
     .cmp_vrange_start = default_vrange_start,
     .cmp_vrange_it_next = default_vrange_it_next,
     .cmp_vrange_statistics = default_vrange_statistics,
     .cmp_vrange_seek = default_vrange_seek,
     .cmp_value_in_range = default_value_in_range,
     .cmp_vrange_freeze = default_vrange_freeze,
     .cmp_vrange_thaw = default_vrange_thaw,
     .cmp_lowest_string = "",
     .cmp_highest_string = NULL}};

graphd_comparator const graphd_comparator_unspecified[1] = {
    {.cmp_locale = "",
     .cmp_name = "unspecified",
     .cmp_alias = NULL,
     .cmp_syntax = default_syntax,
     .cmp_eq_iterator = graphd_value_default_iterator,
     .cmp_iterator_range = NULL,
     .cmp_glob = glob_match,
     .cmp_sort_compare = default_sort_compare,
     .cmp_vrange_size = default_vrange_size,
     .cmp_vrange_start = default_vrange_start,
     .cmp_vrange_it_next = default_vrange_it_next,
     .cmp_vrange_statistics = default_vrange_statistics,
     .cmp_vrange_seek = default_vrange_seek,
     .cmp_value_in_range = default_value_in_range,
     .cmp_vrange_freeze = default_vrange_freeze,
     .cmp_vrange_thaw = default_vrange_thaw,
     .cmp_lowest_string = "",
     .cmp_highest_string = NULL

    }};
