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
#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include "libgraph/graph.h"

#define WORD_NUMBER 1
#define WORD_SPACE 2
#define WORD_ATOM 3
#define WORD_PUNCTUATION 4

/* Treat any Unicode characters as word characters. */
#define ISWORD(x) ((unsigned char)(x) >= 0x80 || isalnum(x))
#define ISDIGIT(x) ((unsigned char)(x) < 0x80 && isdigit(x))

#define ISPUNCT(a) (isascii(a) && ispunct(a))
#define ISSPACE(a) (isascii(a) && isspace(a))
#define ISBREAK(a) (ISSPACE(a) || (ISPUNCT(a) && (a) != '-' && (a) != '+'))

#define ISSIGN(a) ((a) == '-' || (a) == '+')
#define ISSIGNPTR(s, s0) \
  ((*(s) == '-' || *(s) == '+') && (s == s0 || ISBREAK(s[-1])))
#define ISPOINT(a) ((a) == '.')

static bool isnum(char c) {
  if (strchr("1234567890.", c))
    return true;
  else
    return false;
}

/*
 * Everythign except for period. period is special because its also
 * the decimal point
 */
#define PUNCTUATORS "`~!@#$%^&*()_|=\\[]{};':\",/<>?"
#define SPACES " \t\v\n\r"

/**
 * @brief Return the next fragment from a text value.
 *
 * @param s0 	the begin of the entire text
 * @param s	in/out: the beginning of the text to parse
 * @param e	end of the text to parse
 * @param word_s_out	out: beginning of a fragment
 * @param word_e_out	out: end of a fragment
 * @param word_type_out	out: type of word
 *
 * @return true if a fragment has been extracted
 * @return false if we're out of words.
 */
static bool word_fragment_next(char const* s0, char const** s, char const* e,
                               char const** word_s_out, char const** word_e_out,
                               int* word_type_out) {
  char const* r;
  char const *pre_s, *pre_e;
  char const *post_s, *post_e;

  if ((r = *s) == NULL) r = *s = s0;

  if (r >= e) return false;

  *word_s_out = r;

  /* What's the longest number that we can pull out of this?
   */
  if (ISSIGNPTR(r, s0)) r++;
  pre_s = r;
  while (r < e && ISDIGIT(*r)) r++;
  pre_e = r;
  if ((pre_s == s0 || !ISPOINT(pre_s[-1])) &&
      (pre_s < pre_e || r == s0 || !ISDIGIT(r[-1])) && r < e && ISPOINT(*r)) {
    r++;
    post_s = r;
    while (r < e && ISDIGIT(*r)) r++;
    post_e = r;

    if ((r >= e || !ISWORD(*r)) && ((post_e > post_s) || (pre_e > pre_s))) {
      /*   5.
       *  +1.
       *  -.01
       */

      /* There isn't another dot after this
       * number, right?
       */
      if (r >= e || !ISPOINT(*r)) {
        /* Regular floating point number.
         */
        *word_e_out = *s = r;
        *word_type_out = WORD_NUMBER;

        return true;
      }

      /* IP addresses and dot-separated hierarchial
       * names are not floating point numbers -
       * take them one segment at a time.
       */
      if (pre_s < pre_e) {
        *word_e_out = *s = pre_e;
        *word_type_out = WORD_NUMBER;

        return true;
      }

      /*  Weirdness of the form [+-].34. -- skip
       *  punctuation, let the next iteration
       *  take care of the number.
       */
      *word_e_out = *s = post_s;
      *word_type_out = WORD_PUNCTUATION;

      return true;
    }
  }

  if (pre_s < pre_e && (pre_e == e || !ISWORD(*pre_e))) {
    *word_e_out = *s = pre_e;
    *word_type_out = WORD_NUMBER;

    return true;
  }

  /*  OK, that didn't work.  Whatever this is, we're
   *  not standing on a number.  Just pull out a normal
   *  word or nonword.
   */
  r = *s;
  if (ISWORD(*r)) {
    do
      r++;
    while (r < e && ISWORD(*r));

    *word_type_out = WORD_ATOM;
  } else if (ISSPACE(*r)) {
    do
      r++;
    while (r < e && ISSPACE(*r));

    *word_type_out = WORD_SPACE;
  } else {
    do
      r++;
    while (r < e && ISPUNCT(*r) && !ISSIGNPTR(r, s0));

    *word_type_out = WORD_PUNCTUATION;
  }

  *word_e_out = *s = r;
  return true;
}

static int strntoi(const char* s, const char* e, int* out) {
  int n = 0;
  int on;
  bool positive;

  if (s == e) return GRAPH_ERR_LEXICAL;

  if (*s == '-') {
    positive = false;
    s++;
  } else if (*s == '+') {
    positive = true;
    s++;
  } else
    positive = true;

  if (s == e) return GRAPH_ERR_LEXICAL;

  for (; s < e; s++) {
    if (!isdigit(*s)) return GRAPH_ERR_LEXICAL;

    on = n;
    n = n * 10 + *s - '0';

    /* overflow
     */
    if (on > n) return GRAPH_ERR_LEXICAL;
  }
  *out = positive ? n : -n;
  return 0;
}

static bool streq(const char* s, const char* e, const char* token) {
  if (strncasecmp(s, token, e - s)) return false;

  if (strlen(token) != (e - s)) return false;

  return true;
}

/*
 * Take a string which may or may not be a number and fill in a
 * number structure with information about the number.
 * Return GRAPH_ERR_LEXICAL if it isn't a number.
 *
 *
 */
int graph_decode_number(const char* s, const char* e, graph_number* n,
                        bool scientific) {
  const char* fnz;
  const char* lnz;
  const char* dot;
  const char* t;
  const char* exp_start;
  int i;
  int exp;
  int err;

  if (s == e) return GRAPH_ERR_LEXICAL;

  if (scientific) {
    if (streq(s, e, "inf") || streq(s, e, "+inf")) {
      n->num_positive = true;
      n->num_infinity = true;
      n->num_zero = false;
      return 0;
    }

    if (streq(s, e, "-inf")) {
      n->num_positive = false;
      n->num_infinity = true;
      n->num_zero = false;
      return 0;
    }
    exp_start = memchr(s, 'e', e - s);
    if (!exp_start) exp_start = memchr(s, 'E', e - s);

    if (exp_start == NULL) exp_start = e;
  } else
    exp_start = e;

  /*
   * Deal with leading sign
   */
  if (*s == '-') {
    n->num_positive = false;
    s++;
  } else if (*s == '+') {
    n->num_positive = true;
    s++;
  } else {
    n->num_positive = true;
  }

  n->num_zero = false;
  n->num_infinity = false;

  /*
   * A number must have at least one digit
   */

  if (s == exp_start) return GRAPH_ERR_LEXICAL;

  /*
   * After a leading sign, a number should consits of only
   * [0-9.]
   */
  for (t = s; t < exp_start; t++) {
    if (!isnum(*t)) {
      return GRAPH_ERR_LEXICAL;
    }
  }

  /*
   * Locate the first significant digit.
   */
  for (fnz = s; fnz < exp_start; fnz++) {
    if (*fnz == '0') continue;
    if (*fnz == '.') continue;
    if (isnum(*fnz)) break;

    { return GRAPH_ERR_LEXICAL; }
  }

  /*
   * Locate the last significant digit.
   */
  for (lnz = exp_start - 1; lnz >= s; lnz--) {
    if (*lnz == '0') continue;
    if (*lnz == '.') continue;
    if (isnum(*lnz)) break;
    { return GRAPH_ERR_LEXICAL; }
  }

  /*
   * Warning: it is possible that lnz < fnz at this point.
   * That happens in most forms of zero.
   */

  /*
   * Locate the decimal point and complain if there is more than
   * one
   */
  dot = NULL;
  for (i = 0; (i < (exp_start - s)); i++) {
    if (s[i] == '.') {
      if (dot)
        return GRAPH_ERR_LEXICAL;
      else
        dot = s + i;
    }
  }

  if ((dot == s) && (dot == (exp_start - 1))) {
    /*
     * Dot by itsself isn't a number.
     */
    return GRAPH_ERR_LEXICAL;
  }

  /*
   * Calculate the exponent.
   */
  if (!dot) {
    exp = exp_start - fnz - 1;
  } else {
    if (dot > fnz) {
      exp = dot - fnz - 1;
    } else {
      exp = dot - fnz;
    }
  }

  n->num_exponent = exp;
  n->num_fnz = fnz;
  if ((dot > fnz) && (dot < lnz))
    n->num_dot = dot;
  else
    n->num_dot = NULL;

  /*
   * lnz is the first byte AFTER the last numeral
   */
  n->num_lnz = lnz + 1;

  /*
   * Oh hey! Its zero
   */
  if (fnz == exp_start) {
    n->num_zero = true;
    n->num_positive = true;
  } else
    n->num_zero = false;

  if (scientific && (exp_start != e)) {
    /*
     * If exp_start > e, you've done something weird.
     */
    if (exp_start >= e) return GRAPH_ERR_LEXICAL;

    err = strntoi(exp_start + 1, e, &exp);
    if (err) return GRAPH_ERR_LEXICAL;

    n->num_exponent += exp;
  }

  return 0;
}

/*
 * Compare the string part of a number structures: lexographically but
 * skip '.'
 */
static int dotstrcmp(const char* a_s, const char* a_e, const char* b_s,
                     const char* b_e) {
  do {
    if ((a_s != a_e) && (*a_s == '.')) {
      a_s++;
      continue;
    }

    if ((b_s != b_e) && (*b_s == '.')) {
      b_s++;
      continue;
    }

    if ((a_s == a_e) && (b_s == b_e)) return 0;
    if (a_s == a_e) return -1;
    if (b_s == b_e) return 1;

    if (*a_s > *b_s) return 1;

    if (*b_s > *a_s) return -1;
    a_s++;
    b_s++;
  } while (true);
}

/*
 * Compare two numbers. The comparisons must be done in this
 * order because not all members are defined in all cases.
 * (For example, if the zero flag is set, the positive flag is
 * undefined)
 */
int graph_number_compare(graph_number const* a, graph_number const* b) {
  int r;
  if (a->num_zero && b->num_zero) return 0;

  if (a->num_zero && b->num_positive) return -1;
  if (a->num_zero && !b->num_positive) return 1;
  if (b->num_zero && a->num_positive) return 1;
  if (b->num_zero && !a->num_positive) return -1;

  if (a->num_positive && !b->num_positive) return 1;
  if (!a->num_positive && b->num_positive) return -1;
  if (a->num_infinity && b->num_infinity) return 0;
  if (a->num_infinity) return a->num_positive ? 1 : -1;
  if (b->num_infinity) return a->num_positive ? -1 : 1;
  if (a->num_exponent > b->num_exponent) return a->num_positive ? 1 : -1;

  if (b->num_exponent > a->num_exponent) return a->num_positive ? -1 : 1;

  r = dotstrcmp(a->num_fnz, a->num_lnz, b->num_fnz, b->num_lnz);

  return a->num_positive ? r : -r;
}

/*
 * Compare two strings lexographically while ignoring case.
 */
int graph_strcasecmp(const char* a_s, const char* a_e, const char* b_s,
                     const char* b_e) {
  unsigned char ac, bc;

  if (a_s == NULL && b_s == NULL) return 0;
  if (a_s == NULL) return 1;
  if (b_s == NULL) return -1;

  for (;;) {
    if (a_s == a_e) {
      if (b_s == b_e) return 0;
      return -1;
    }
    if (b_s == b_e) return 1;

    ac = tolower((unsigned char)*a_s);
    bc = tolower((unsigned char)*b_s);

    if (ac > bc) return 1;
    if (bc > ac) return -1;
    a_s++;
    b_s++;
  }
}

/*
 * "intelligently" compare two strings
 */
int graph_fuzzycmp(const char* a_s, const char* a_e, const char* b_s,
                   const char* b_e) {
  const char *a_tok_s, *a_tok_e;
  const char *a_cur, *b_cur;
  const char *b_tok_s, *b_tok_e;
  graph_number a_n, b_n;
  bool a_good, b_good;
  int a_type, b_type;
  int a_err, b_err;

  if (a_s == NULL && b_s == NULL) return 0;
  if (a_s == NULL) return 1;
  if (b_s == NULL) return -1;

  a_tok_s = b_tok_s = a_tok_e = b_tok_e = NULL;
  a_cur = b_cur = NULL;
  while (true) {
    while ((a_good = word_fragment_next(a_s, &a_cur, a_e, &a_tok_s, &a_tok_e,
                                        &a_type)) &&
           a_type == WORD_SPACE)
      ;

    while ((b_good = word_fragment_next(b_s, &b_cur, b_e, &b_tok_s, &b_tok_e,
                                        &b_type)) &&
           b_type == WORD_SPACE)
      ;

    if (!a_good && !b_good) return 0;

    if (!a_good) return -1;

    if (!b_good) return 1;

    if (a_type == WORD_NUMBER)
      a_err = graph_decode_number(a_tok_s, a_tok_e, &a_n, false);
    else
      a_err = GRAPH_ERR_SEMANTICS;

    if (b_type == WORD_NUMBER)
      b_err = graph_decode_number(b_tok_s, b_tok_e, &b_n, false);
    else
      b_err = GRAPH_ERR_SEMANTICS;

    /*
     * Both are numbers
     */
    if (!a_err && !b_err) {
      int r;

      r = graph_number_compare(&a_n, &b_n);
      if (r) return r;

      continue;
    }

    /*
     * If one is a number, but the other isn't, put the number first.
     */
    else if (!a_err && b_err)
      return -1;
    else if (!b_err && a_err)
      return 1;
    else {
      int r;
      r = graph_strcasecmp(a_tok_s, a_tok_e, b_tok_s, b_tok_e);
      if (r) return r;
    }
  }
}
