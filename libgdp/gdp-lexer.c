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
#include "libgdp/gdp.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>

#define GDP_MAX_COMMENT_LENGTH 65536
static inline int get(gdp_input *in, int *ch) {
  int err;

  /* fetch character */
  if ((err = gdp_input_getch(in, ch))) return err;

  /* keep track or row and column */
  if
    unlikely(*ch == '\n') {
      in->in_col = 1;
      in->in_row++;
    }
  else if
    unlikely(*ch == '\t') in->in_col += 8;
  else
    in->in_col++;

  return 0;
}

static inline int put(gdp_input *in, int ch) {
  if
    unlikely(ch == '\n') {
      in->in_col = 1;  // (unknown!)
      in->in_row--;
    }
  else if
    unlikely(ch == '\t') in->in_col -= 8;
  else
    in->in_col--;

  return gdp_input_putch(in, ch);
}

/**
 * Is the end-of-file.
 */
static inline bool is_eof(int ch) { return (ch == GDP_EOF_CHAR); }

/**
 * Marks the begining of an alphanumeric token. Alphanumeric tokens must start
 * with a letter or `_'.
 */
static inline bool is_alnum(int ch) { return isalpha(ch) || (ch == '_'); }

/**
 * Belongs to an alphanumberic token.
 */
static inline bool is_alnum_c(int ch) { return isalnum(ch) || (ch == '_'); }

/**
 * Marks the beginning of a numeric literal. This includes decimal,
 * hexadecimal, and timestamp values.
 */
static inline bool is_num(int ch) { return isdigit(ch); }

/**
 * Belongs to a numeric literal.
 */
static inline bool is_num_c(int ch) {
  static const unsigned char charset[256] = {
          ['-'] = 1, ['.'] = 1, ['0'] = 1, ['1'] = 1, ['2'] = 1, ['3'] = 1,
          ['4'] = 1, ['5'] = 1, ['6'] = 1, ['7'] = 1, ['8'] = 1, ['9'] = 1,
          [':'] = 1, ['A'] = 1, ['B'] = 1, ['C'] = 1, ['D'] = 1, ['E'] = 1,
          ['F'] = 1, ['T'] = 1, ['Z'] = 1, ['a'] = 1, ['b'] = 1, ['c'] = 1,
          ['d'] = 1, ['e'] = 1, ['f'] = 1, ['t'] = 1, ['z'] = 1,
  };
  return !is_eof(ch) && (is_alnum_c(ch) || charset[ch]);
}

/**
 * Is the beginning of a TOK_VAR token.
 */
static inline bool is_var(int ch) { return (ch == '$'); }

/**
 * Belongs to a TOK_VAR token.
 */
static inline bool is_var_c(int ch) {
  return isalnum(ch) || (ch == '_') || (ch == '-');
}

/**
 * Is an empty space.
 */
static inline bool is_space(int ch) { return isspace(ch); }

/**
 * Is the beginning of a string token.
 */
static inline bool is_str(int ch) { return (ch == '"'); }

/**
 * Is a symbol.
 */
static inline bool is_sym(int ch) {
  return ispunct(ch) && !is_var(ch) && !is_str(ch);
}

/**
 * Encodes special characters in strings.
 */
static void fix_string(char **start, char **end) {
  char *r, *w;
  bool escaped;

  escaped = false;
  for (r = w = *start; r < *end; r++) {
    char ch = *r;
    if (escaped) {
      switch (ch) {
        case '\\':
          *(w++) = '\\';
          break;
        case '"':
          *(w++) = '"';
          break;
        case 'n':
          *(w++) = '\n';
          break;
        default:
          *(w++) = ch;
      }
      escaped = false;
    } else if (ch == '\\')
      escaped = true;
    else
      *(w++) = ch;
  }

  *end = w;
}

/**
 * Consume empty space (newlines, spaces, etc).
 */
static int consume_space(gdp_input *in) {
  int ch;
  int err;

  do {
    if ((err = get(in, &ch))) return err;
  } while (is_space(ch));

  put(in, ch);

  return 0;
}

/**
 * Consume an atom that begins with a letter.
 */
static int consume_atom_alnum(gdp_input *in, gdp_token_kind *kind) {
  int ch;
  int err;
  size_t dashes = 0;

  for (;;) {
    if ((err = get(in, &ch))) return err;

    if (ch == '-')
      dashes++;

    else if (is_alnum_c(ch))
      dashes = 0;

    else {
      put(in, ch);
      while (dashes--) put(in, '-');
      break;
    }
  }
  *kind = TOK_ATOM;
  return 0;
}

/**
 * Consume an atom that begins with a decimal digit.
 */
static int consume_atom_num(gdp_input *in, gdp_token_kind *kind) {
  int ch;
  int err;

  do {
    if ((err = get(in, &ch))) return err;
  } while (is_num_c(ch));

  put(in, ch);

  *kind = TOK_ATOM;
  return 0;
}

static int consume_variable(gdp_input *in, gdp_token_kind *kind) {
  int ch;
  int err;

  /* (First character `$' already consumed)
   */

  /* second character (required) */
  if ((err = get(in, &ch))) return err;
  if (!is_alnum(ch)) return GDP_ERR_LEXICAL;

  err = consume_atom_alnum(in, kind);
  if (err) return GDP_ERR_LEXICAL;

  *kind = TOK_VAR;
  return 0;
}

static int consume_string(gdp_input *in, gdp_token_kind *kind,
                          bool *special  // has special chars ('\n', '\\', ..)
                          ) {
  int ch;
  bool esc;
  bool finish;
  int err;

  /* (First character `"' already consumed) */

  *special = false;

  esc = false;
  finish = false;
  while (!finish) {
    if ((err = get(in, &ch))) return err;
    switch (ch) {
      case GDP_EOF_CHAR:
        put(in, ch);
        return GDP_ERR_LEXICAL;
      case '"':
        finish = !esc;
        esc = false;
        break;
      case '\\':
        *special = true;
        esc = !esc;
        break;
      default:
        esc = false;
    }
  }

  *kind = TOK_STR;
  return 0;
}

static int consume_symbol(gdp_input *in, int ch, gdp_token_kind *kind) {
  gdp_token_kind k;
  int err;

  switch (ch) {
    case '(':
      if ((err = get(in, &ch))) return err;
      k = (ch == ':') ? TOK_CBEGIN : (put(in, ch), TOK_OPAR);
      break;
    case ')':
      if ((err = get(in, &ch))) return err;
      k = (ch == ':') ? TOK_CEND : (put(in, ch), TOK_CPAR);
      break;
    case '{':
      k = TOK_OBRC;
      break;
    case '}':
      k = TOK_CBRC;
      break;
    case '=':  // =
      k = TOK_EQ;
      break;
    case '-':  // -, ->
      if ((err = get(in, &ch))) return err;
      k = (ch == '>') ? TOK_RARR : (put(in, ch), TOK_MINUS);
      break;
    case '<':  // <, <=, <-
      if ((err = get(in, &ch))) return err;
      switch (ch) {
        case '=':  // <=
          k = TOK_LE;
          break;
        case '-':  // <-
          k = TOK_LARR;
          break;
        default:  // <
          put(in, ch);
          k = TOK_LT;
      }
      break;
    case '|':  // |, ||
      if ((err = get(in, &ch))) return err;
      k = (ch == '|') ? TOK_LOR : (put(in, ch), TOK_BOR);
      break;
    case '>':  // >, >=
      if ((err = get(in, &ch))) return err;
      k = (ch == '=') ? TOK_GE : (put(in, ch), TOK_GT);
      break;
    case '~':  // ~=
      if ((err = get(in, &ch))) return err;
      if (ch != '=') {
        put(in, ch);
        return GDP_ERR_LEXICAL;
      }
      k = TOK_FE;
      break;
    case '!':  // !=
      if ((err = get(in, &ch))) return err;
      if (ch != '=') {
        put(in, ch);
        return GDP_ERR_LEXICAL;
      }
      k = TOK_NE;
      break;
    case '+':
      k = TOK_PLUS;
      break;
    default:
      return GDP_ERR_LEXICAL;
  }

  *kind = k;
  return 0;
}

static inline int consume_eof(gdp_input *in, gdp_token_kind *kind) {
  *kind = TOK_END;
  return 0;
}

/*
 * Remove comments and whitespace at the begining of in and position it
 * such that get() will return the first non-comment, non-whitespace character.
 *
 * You must call consume_comments_and_space in a loop until it returns something
 * other than GDP_ERR_AGAIN.
 */

static int consume_comments_and_space(gdp_input *in) {
  int ch, err;
  int limit = 0;
  if ((err = consume_space(in))) return err;

  err = get(in, &ch);
  if (err) return err;

  if (ch == '(') {
    int ch2;
    err = get(in, &ch2);
    if (err) return err;
    if (ch2 == ':') {
      char last[2] = {0, 0};
      for (;;) {
        int ch3;

        err = get(in, &ch3);
        limit++;
        if (limit > GDP_MAX_COMMENT_LENGTH) return GDP_ERR_TOO_LONG;

        /*
         * get returns EIO on EOF
         */
        if (err == EIO) {
          err = GDP_ERR_LEXICAL;
          return err;
        } else if (err)
          return err;

        last[0] = last[1];
        last[1] = ch3;

        if (!strncmp(last, ":)", 2)) break;
      }

      if ((err = consume_space(in))) return err;

      return GDP_ERR_AGAIN;
    } else {
      put(in, ch);
      put(in, ch2);
    }
    return 0;
  } else {
    put(in, ch);
    return 0;
  }
}

int gdp_lexer_consume(gdp *parser, gdp_input *in, gdp_token *tok) {
  gdp_token_kind kind = 0;  // (spurious GCC warning if not init'ed)
  char *s, *e;              // start and end of token image
  bool special;             // token contains special chars ('\n', '\\', etc)
  int row = 0;
  int col = 0;
  int ch;
  int err;

  /* this boolean is set to `true' by the consume_string() function if a
   * string token contains special character sequences such as "\n",
   * "\\", etc.  that need to be processed */
  special = false;

  while ((err = consume_comments_and_space(in)) == GDP_ERR_AGAIN)
    ;

  if (err) return err;

  row = in->in_row;
  col = in->in_col;

  /* (beginning of token) */
  gdp_input_tokbegin(in);

  /* read the first character of the new token */
  if ((err = get(in, &ch))) goto fail;

  /* an atom that starts with a letter */
  if (is_alnum(ch)) err = consume_atom_alnum(in, &kind);
  /* an atom that starts with a digit */
  else if (is_num(ch))
    err = consume_atom_num(in, &kind);
  /* a symbol */
  else if (is_sym(ch))
    err = consume_symbol(in, ch, &kind);
  /* a string */
  else if (is_str(ch))
    err = consume_string(in, &kind, &special);
  /* a variable */
  else if (is_var(ch))
    err = consume_variable(in, &kind);
  /* the end-of-file */
  else if (is_eof(ch))
    err = consume_eof(in, &kind);
  else
    err = GDP_ERR_LEXICAL;
  if (err) goto fail;

  /* end of token; if special character sequences were found, then the
   * token's image is moved to a malloc'ed memory space so that it can be
   * processed */
  if ((err = gdp_input_tokend(in, special, &s, &e))) return err;
  /* perform some string-specific adjustments */
  if (kind == TOK_STR) {
    s++;
    e--;  // discard initial and final " characters
    if
      unlikely(special) fix_string(&s, &e);  // encode scape sequences
  }

  /* return token */
  if
    likely(tok != NULL) {
      *tok = (gdp_token){
          .tkn_kind = kind,
          .tkn_start = s,
          .tkn_end = e,
          .tkn_row = row,
          .tkn_col = col,
      };
      /* the special atom `null' has its own token kind (the bitwise
       * OR below converts the character to lower-case) */
      if ((kind == TOK_ATOM) && ((*s | 0x20) == 'n'))
        if (gdp_token_matches(tok, "null")) {
          /*
           * NOTE: Most of the time, graphd expects
           * `tkn_start' and `tkn_end' to be NULL for
           * null tokens. However, there are special
           * cases in which these two fields must point
           * to a string (e.g. "null" or "0"). We deal
           * with these in the conversion routines
           * implemented in `gdp-token.c'
           */
          tok->tkn_kind = TOK_NULL;
          tok->tkn_start = NULL;
          tok->tkn_end = NULL;
        }
      /* print debug information */
      if
        unlikely(parser->dbglex) gdp_token_printf(parser->dbgf, "[$n $i]", tok);
    }

  return 0;

fail:
  if (err == GDP_ERR_LEXICAL) {
    char const *const UNK = "(unknown)";
    /* try to return the offending token */
    if (gdp_input_tokend(in, false, &s, &e)) s = e = NULL;
    *tok = (gdp_token){
        .tkn_kind = TOK_ATOM,
        .tkn_start = s ?: UNK,
        .tkn_end = e ?: UNK + strlen(UNK),
        .tkn_row = row,
        .tkn_col = col,
    };
  }
  return err;
}
