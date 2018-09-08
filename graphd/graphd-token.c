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
#include <stdio.h>
#include <string.h>

/**
 * @brief Return a pointer to the first ch between s and e,
 * 	outside of matching () or [].
 * @param s	pointer to the beginning of a text fragment
 * @param e	pointer just after the last character of the text fragment
 * @param ch	character to find
 *
 * @return NULL if the character does not occur (outside matching parentheses).
 * @return otherwise, a pointer to the first occurrence
 */
char const* graphd_unparenthesized_curchr(char const* s, char const* e,
                                          char const ch) {
  size_t paren = 0;

  for (; s < e; s++) {
    if (paren == 0 && *s == ch)
      return s;
    else if (*s == '(' || *s == '[')
      paren++;
    else if (*s == ')' || *s == ']') {
      if (paren > 0)
        paren--;
      else
        return NULL;
    }
  }
  return NULL;
}

/**
 * @brief Given the start of a double-quoted string, return a pointer to its
 * end.
 * @param s	pointer to the beginning of a text fragment
 * @param e	pointer just after the last character of the text fragment
 *
 *  If the string has no closing double quote, the call returns a pointer to e.
 *  Otherwise, the returned pointer points to the closing <"> .
 */
char const* graphd_string_end(char const* s, char const* e) {
  if (s >= e || *s != '"') return e;
  s++;

  for (; s < e; s++) {
    if (*s == '"')
      return s;

    else if (*s == '\\' && s + 1 < e)
      s++;
  }
  return s;
}

/**
 * @brief Given the start of whitespace, return a pointer to its end.
 * @param s	pointer to the beginning of a text fragment
 * @param e	pointer just after the last character of the text fragment
 *
 * The returned pointer points to the first non-whitespace character on or after
 * s,
 * or e if there is no such character.
 */
char const* graphd_whitespace_end(char const* s, char const* e) {
  while (s < e && isascii(*s) && isspace(*s)) s++;
  return s;
}

/**
 * @brief Return a pointer to the first ch between s and e,
 * 	outside of matching (), [], or "" (with \ escaping).
 * @param s	pointer to the beginning of a text fragment
 * @param e	pointer just after the last character of the text fragment
 * @param ch	character to find
 *
 * @return NULL if the character does not occur (outside matching parentheses,
 * etc).
 * @return otherwise, a pointer to the first occurrence
 */
char const* graphd_unparenthesized_textchr(char const* s, char const* e,
                                           char const ch) {
  size_t paren = 0;

  for (; s < e; s++) {
    if (paren == 0 && *s == ch)
      return s;
    else if (*s == '(' || *s == '[')
      paren++;
    else if (*s == ')' || *s == ']') {
      if (paren > 0)
        paren--;
      else
        return NULL;
    } else if (*s == '"') {
      s = graphd_string_end(s, e);
      if (s >= e) return NULL;
    }
  }
  return NULL;
}

/**
 * @brief Render an arbitrary byte string safe for inclusion in, say, a cursor.
 *
 *	Escaped characters: anything not isprint() or isascii(), ( ) : % " \
 *	Escape mechanism is %XX, where X is a hex digit.
 *
 * @param cl	assert through this
 * @param s	pointer to the beginning of the byte string
 * @param e	pointer just after the last character of the byte string
 * @param w	start writing here
 * @param w_e	end of writeable area, must be >= 1 + 3 * (e - s).
 *
 * @return a pointer to the '\0' written at the end of the escaped text.
 */
char* graphd_escape(cl_handle* cl, char const* s, char const* e, char* w,
                    char* w_e) {
  cl_assert(cl, w_e - w > 3 * (e - s));

  while (s < e) {
    cl_assert(cl, w + 3 < w_e);

    if (isascii(*s) && isprint(*s) && *s != ':' && *s != '(' && *s != ')' &&
        *s != '%' && *s != '"' && *s != '\\')

      *w++ = *s++;
    else {
      snprintf(w, w_e - w, "%%%2.2hx", (unsigned char)*s++);
      w += 3;
    }
  }
  cl_assert(cl, w < w_e);
  *w = '\0';
  return w;
}

static int atox(int const ch) {
  if (isdigit(ch)) return ch - '0';
  if (isupper(ch)) return 10 + (ch - 'A');
  return 10 + (ch - 'a');
}

/**
 * @brief Undo graph_escape().
 *
 * @param cl	assert through this
 * @param s	pointer to the beginning of the byte string
 * @param e	pointer just after the last character of the byte string
 * @param w	start writing here
 * @param w_e	end of writeable area, must be >= 1 + (e - s).
 *
 * @return a pointer to the '\0' written at the end of the unescaped text,
 *	or NULL on syntax error.
 */
char* graphd_unescape(cl_handle* cl, char const* s, char const* e, char* w,
                      char* w_e) {
  cl_assert(cl, w_e - w > (e - s));

  while (s < e) {
    cl_assert(cl, w + 3 < w_e);

    if (*s == '%') {
      s++;
      if (s + 2 > e || !isxdigit(s[0]) || !isxdigit(s[1])) return NULL;
      *w++ = (atox(s[0]) << 4) | atox(s[1]);
      s += 2;
    } else {
      *w++ = *s++;
    }
  }
  *w = '\0';
  return w;
}

/**
 * @brief Scan an unsigned long long.
 *
 *  Deserializes a number and advances *s past the terminating
 *  punctuation character.
 *
 * @param s	in: the beginning of the available bytes; out: a pointer
 * 		just past the terminating (arbitrary) punctuation character.
 * @param e 	end of input.
 * @param n_out out: the scanned value.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
int graphd_bytes_to_ull(char const** s, char const* e,
                        unsigned long long* n_out) {
  char const* r = *s;
  unsigned long long n = 0;

  while (r < e && isascii((unsigned char)*r) && isdigit(*r)) {
    if (n > ((unsigned long long)-1 - (*r - '0')) / 10) return ERANGE;

    n = n * 10;
    n += *r - '0';

    r++;
  }
  if (r == *s) return GRAPHD_ERR_LEXICAL;
  if (r >= e) return GRAPHD_ERR_LEXICAL;

  *s = r + 1;
  *n_out = n;

  return 0;
}

/**
 * @brief Return the next expression of a sequence.
 *
 * @param s	pointer to the first byte of the remaining sequence,
 *		advanced by the call.
 * @param e 	pointer just past the last byte of the remaining sequence.
 * @param s_out out: pointer to the first octet of the expression
 * @param e_out out: pointer just after the last octet of the expression
 *
 * @return 0 on success
 * @return GRAPHD_ERR_NO if we're out of text
 * @return GRAPHD_ERR_SYNTAX on syntax error.
 */
int graphd_next_expression(char const** s, char const* e, char const** s_out,
                           char const** e_out) {
  char const* r = *s;

  r = graphd_whitespace_end(r, e);
  if (r >= e) return GRAPHD_ERR_NO;

  *s_out = r;
  switch (*r) {
    case '"':
      r = graphd_string_end(r, e);
      break;

    case '(':
      r = graphd_unparenthesized_textchr(r + 1, e, ')');
      break;

    case '[':
      r = graphd_unparenthesized_textchr(r + 1, e, ']');
      break;

    default:
      r++;
      while (r < e && *r != '(' && *r != ')' && *r != '[' && *r != ']' &&
             (!isascii(*r) || !isspace(*r)))
        r++;
      r--;
      break;
  }

  if (r == NULL) return GRAPHD_ERR_SYNTAX;
  *e_out = *s = r + 1;
  return 0;
}
