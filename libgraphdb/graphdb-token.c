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
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>

#include "libgraphdb/graphdbp.h"

#define IS_SPACE(x) ((x) == ' ' || (x) == '\t')

static char const graphdb_char_class[256] = {
    /*  The mechanism this code is using is called "designated
     *  initializers".  They were added to C in 1999. Each expression
     *
     * 	[ x ] = y
     *
     *  initializes the array element with index x to the value y.
     *  Elements not mentioned are initialized with 0.

    ['\n'] = 1,	['\r'] = 1,	[0]    = 1,	[' '] = 1,
    ['\t'] = 1, 	['(']  = 1, 	[')']  = 1,	[','] = 1,
    ['"'] = 1, 	['%']  = 1,

    ['<'] = 3, 	['>'] = 3, 	['='] = 3,
    ['*'] = 3, 	['~'] = 3,

    ['-'] = 2

     Translated into pre-C9X, this amounts to:
     */

    1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 1, 3, 0,
    1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 3, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0

    /* And more zeroes here. */
};

enum graphdb_tok_state {
  GRAPHDB_TOK_INITIAL = 0,
  GRAPHDB_TOK_CR,
  GRAPHDB_TOK_ATOM,
  GRAPHDB_TOK_STRING_ESCAPED,
  GRAPHDB_TOK_STRING
};

#define CLASS(ch) (graphdb_char_class[(unsigned char)(ch)] ^ 1)

char const* graphdb_token_atom_end(char const* s) {
  if (*s != '\0') do
      s++;
    while (*s != '\0' && CLASS(*s) & 1);

  return s;
}

static int buffer_append(graphdb_handle* graphdb, cm_handle* cm,
                         graphdb_tokenizer* state, char const* s,
                         char const* e) {
  graphdb_assert(graphdb, s <= e);
  if (s >= e) {
    graphdb_log(graphdb, CL_LEVEL_ULTRA, "GT: append zero bytes to \"%.*s\"",
                (int)state->tok_buf_n, state->tok_buf);
    return 0;
  }

  graphdb_log(graphdb, CL_LEVEL_ULTRA, "GT: buffer \"%.*s\" :: \"%.*s\"",
              (int)state->tok_buf_n, state->tok_buf, (int)(e - s), s);

  if (state->tok_buf_m - state->tok_buf_n <= e - s) {
    size_t need = state->tok_buf_m + (e - s) + 1;
    char* tmp;

    tmp = cm_realloc(cm, state->tok_buf, need);
    if (tmp == NULL) return ENOMEM;

    state->tok_buf = tmp;
    state->tok_buf_m = need;
  }

  memcpy(state->tok_buf + state->tok_buf_n, s, e - s);
  state->tok_buf[state->tok_buf_n += e - s] = '\0';

  return 0;
}

static int buffer_get(graphdb_tokenizer* state, cm_handle* heap,
                      char const** s_out, char const** e_out) {
  *s_out = cm_substr(heap, state->tok_buf, state->tok_buf + state->tok_buf_n);

  if (!*s_out) return ENOMEM;
  *e_out = *s_out + state->tok_buf_n;

  state->tok_buf_n = 0;

  return 0;
}

/**
 * @brief Push tokenizer (Internal utility)
 *
 *   Called with successive byte strings, the push tokenizer
 *   returns tokens pulled from those byte strings, and advances
 *   the leading byte string pointer to indicate the number of bytes
 *   consumed.
 *
 *   There is no token boundary between fragments passed to successive
 *   calls to graphdb_token().  If it returns GRAPHDB_TOKENIZE_MORE,
 *   no token is available at this time; the tokenizer needs more input
 *   to decide whether the current token, if any, ends at this point
 *   or continues into the next fragment.
 *
 *   While tokens can span call boundaries, they are always
 *   returned as single strings; tokens that need to be joined are
 *   returned as pointers into an internal buffer.
 *
 *  @param graphdb	opaque module handle
 *  @param state	tokenizer state
 *  @param heap		allocate a buffer here
 *  @param s		in/out: NULL or beginning of input bytes
 *  @param e		end of input bytes
 *  @param tok_s_out	on success, store start of token data here
 *  @param tok_e_out	on success, store end of token data here
 *
 *  @return GRAPHDB_TOKENIZE_MORE after running out of input.
 *  @return GRAPHDB_TOKENIZE_ERROR_MEMORY if running out of memory.
 *  @return \\n for a newline
 *  @return " for a string
 *  @return the first character for atoms and punctuation
 */

int graphdb_token(graphdb_handle* graphdb, graphdb_tokenizer* state,
                  cm_handle* heap, char const** s, char const* e,
                  char const** tok_s_out, char const** tok_e_out) {
  char const *p, *p0;
  int err;

  graphdb_assert(graphdb, heap != NULL);
  graphdb_assert(graphdb, tok_s_out != NULL);
  graphdb_assert(graphdb, tok_e_out != NULL);

  if (state->tok_lookahead != GRAPHDB_TOKENIZE_MORE) {
    int tok;

    *tok_s_out = state->tok_s;
    *tok_e_out = state->tok_e;
    tok = state->tok_lookahead;

    state->tok_lookahead = GRAPHDB_TOKENIZE_MORE;
    graphdb_log(graphdb, CL_LEVEL_ULTRA, "GT: %c \"%.*s\" (from lookahead)",
                (tok == GRAPHDB_TOKENIZE_EOF ? '_' : tok),
                (int)(*tok_e_out - *tok_s_out), *tok_s_out);

    return tok;
  }

  *tok_s_out = NULL;
  *tok_e_out = NULL;

  /* EOF */
  if (s == NULL || *s == NULL) {
    switch (state->tok_state) {
      case GRAPHDB_TOK_INITIAL:
        break;

      case GRAPHDB_TOK_CR:
        *tok_s_out = "\r";
        *tok_e_out = *tok_s_out + 1;

        state->tok_state = GRAPHDB_TOK_INITIAL;

        graphdb_log(graphdb, CL_LEVEL_ULTRA, "GT: \\n (at EOF)");
        return '\n';

      case GRAPHDB_TOK_ATOM:

        err = buffer_get(state, heap, tok_s_out, tok_e_out);
        if (err) return GRAPHDB_TOKENIZE_ERROR_MEMORY;

        state->tok_state = GRAPHDB_TOK_INITIAL;
        graphdb_assert(graphdb, **tok_s_out != 0);

        graphdb_log(graphdb, CL_LEVEL_ULTRA, "GT: atom \"%.*s\" (at EOF)",
                    (int)(*tok_e_out - *tok_s_out), *tok_s_out);

        return (unsigned char)**tok_s_out;

      case GRAPHDB_TOK_STRING_ESCAPED:
      case GRAPHDB_TOK_STRING:
        state->tok_state = GRAPHDB_TOK_INITIAL;

        err = buffer_get(state, heap, tok_s_out, tok_e_out);
        if (err) return err;

        graphdb_log(graphdb, CL_LEVEL_ULTRA, "GT: string \"%.*s\" (at EOF)",
                    (int)(*tok_e_out - *tok_s_out), *tok_s_out);
        return '"';

      default:
        graphdb_notreached(graphdb, "unexpected tokenizer state %d",
                           state->tok_state);
    }
    graphdb_log(graphdb, CL_LEVEL_ULTRA, "GT: EOF");
    return GRAPHDB_TOKENIZE_EOF;
  }

  p0 = p = *s;
  graphdb_assert(graphdb, p != NULL);
  graphdb_assert(graphdb, p < e);

  switch (state->tok_state) {
    case GRAPHDB_TOK_INITIAL:
      while (p < e && IS_SPACE(*p)) p++;
      if (p >= e) break;
      if (*p == '\r') {
        state->tok_state = GRAPHDB_TOK_CR;
        if (++p >= e) break;

        /* FALL THROUGH */

        case GRAPHDB_TOK_CR:
          if (*p == '\n') {
            *tok_s_out = "\r\n";
            *tok_e_out = *tok_s_out + 2;

            *s = p + 1;
          } else {
            *tok_s_out = "\r";
            *tok_e_out = *tok_s_out + 1;
            *s = p;
          }
          state->tok_state = GRAPHDB_TOK_INITIAL;
          graphdb_log(graphdb, CL_LEVEL_ULTRA, "GT: \\n");
          return '\n';
      }
      if (*p == '"') {
        p0 = ++p;
        state->tok_state = GRAPHDB_TOK_STRING;
        if (p >= e) break;
        goto graphdb_tok_string;
      } else if (!(state->tok_char_class = CLASS(*p))) {
        *tok_s_out = p;
        *tok_e_out = *s = p + 1;

        graphdb_log(graphdb, CL_LEVEL_ULTRA, "GT: %c", *p);
        return (unsigned char)*p;
      }

      p0 = p;
      state->tok_state = GRAPHDB_TOK_ATOM;

    /* FALL THROUGH  */

    case GRAPHDB_TOK_ATOM:

      while (p < e && (state->tok_char_class &= CLASS(*p))) p++;
      if (p < e) {
        *s = p;
        if (!state->tok_buf_n) {
          *tok_s_out = p0;
          *tok_e_out = p;
          state->tok_state = GRAPHDB_TOK_INITIAL;

          graphdb_log(graphdb, CL_LEVEL_ULTRA, "GT: atom \"%.*s\"",
                      (int)(*tok_e_out - *tok_s_out), *tok_s_out);
          return (unsigned char)*p0;
        }

        /* Concatenate with old buffer contents. */
        err = buffer_append(graphdb, heap, state, p0, p);
        if (err) return GRAPHDB_TOKENIZE_ERROR_MEMORY;

        err = buffer_get(state, heap, tok_s_out, tok_e_out);
        if (err) return GRAPHDB_TOKENIZE_ERROR_MEMORY;

        state->tok_state = GRAPHDB_TOK_INITIAL;
        graphdb_assert(graphdb, **tok_s_out != 0);

        graphdb_log(graphdb, CL_LEVEL_ULTRA, "GT: atom \"%.*s\" (buffered)",
                    (int)(*tok_e_out - *tok_s_out), *tok_s_out);

        return (unsigned char)**tok_s_out;
      }

      /* This may or may not span read boundaries.  Buffer it. */

      err = buffer_append(graphdb, heap, state, p0, e);
      if (err) return GRAPHDB_TOKENIZE_ERROR_MEMORY;
      break;

    case GRAPHDB_TOK_STRING_ESCAPED:
    graphdb_tok_string_escaped:

      if (*p != 'n')
        p0 = p;
      else {
        /*  \n -> '\n' is special, because there
         *  a character isn't just escaped - its
         *  value changes.
         */
        char nl = '\n';
        p0 = p + 1;
        err = buffer_append(graphdb, heap, state, &nl, &nl + 1);
        if (err != 0) return err;
      }
      p++;
      state->tok_state = GRAPHDB_TOK_STRING;

    /* FALL THROUGH */

    case GRAPHDB_TOK_STRING:
    graphdb_tok_string:

      while (p < e && *p != '"' && *p != '\\') p++;
      if (p < e && *p == '"') {
        p++;

        state->tok_state = GRAPHDB_TOK_INITIAL;
        *s = p;

        if (!state->tok_buf_n) {
          *tok_s_out = p0;
          *tok_e_out = p - 1;
        } else {
          err = buffer_append(graphdb, heap, state, p0, p - 1);
          if (err) return err;

          err = buffer_get(state, heap, tok_s_out, tok_e_out);
          if (err) return err;
        }
        graphdb_log(graphdb, CL_LEVEL_ULTRA, "GT: string <%.*s>",
                    (int)(*tok_e_out - *tok_s_out), *tok_s_out);

        return '"';
      }
      if (p > p0) {
        err = buffer_append(graphdb, heap, state, p0, p);
        if (err) return err;
      }
      if (p >= e) break;

      graphdb_assert(graphdb, p < e && *p == '\\');
      state->tok_state = GRAPHDB_TOK_STRING_ESCAPED;
      if (++p >= e) break;
      goto graphdb_tok_string_escaped;

    default:
      graphdb_notreached(graphdb, "unexpected tokenizer state %d",
                         state->tok_state);
  }

  graphdb_log(graphdb, CL_LEVEL_ULTRA,
              "GT: more (buffered: \"%.*s\"; state %d)", (int)state->tok_buf_n,
              state->tok_buf, state->tok_state);

  *s = e;
  return GRAPHDB_TOKENIZE_MORE;
}

/**
 * @brief Push tokenizer, superficial read-only version.
 *
 *  @param graphdb	opaque module handle
 *  @param state	tokenizer state
 *  @param s		in/out: NULL or beginning of input bytes
 *  @param e		end of input bytes
 *
 *  @return GRAPHDB_TOKENIZE_ERROR_MEMORY if running out of memory.
 *  @return GRAPHDB_TOKENIZE_EOF for EOF
 *  @return ( for an opening (
 *  @return ) for a closing )
 *  @return " for anything else
 */

int graphdb_token_skip(graphdb_handle* graphdb, graphdb_tokenizer* state,
                       char const** s, char const* e) {
  char const* p;
  char const* s0;

  if (state->tok_lookahead != GRAPHDB_TOKENIZE_MORE) {
    int tok;

    tok = state->tok_lookahead;
    if (tok != GRAPHDB_TOKENIZE_EOF)
      state->tok_lookahead = GRAPHDB_TOKENIZE_MORE;

    graphdb_log(graphdb, CL_LEVEL_ULTRA, "sGT: %c (from lookahead)",
                (tok == GRAPHDB_TOKENIZE_EOF ? '_' : tok));

    return tok == '(' || tok == ')' || tok == GRAPHDB_TOKENIZE_EOF ? tok : '"';
  }

  /* EOF */
  if (s == NULL || *s == NULL) {
    graphdb_log(graphdb, CL_LEVEL_ULTRA, "sGT: EOF");
    return GRAPHDB_TOKENIZE_EOF;
  }

  s0 = p = *s;

  graphdb_assert(graphdb, p != NULL);
  graphdb_assert(graphdb, p < e);

again:
  switch (state->tok_state) {
    case GRAPHDB_TOK_INITIAL:

      /* Advance p until we see something of skip interest. */
      while (p < e && *p != '(' && *p != ')' && *p != '"') p++;
      if (p >= e) break;

      graphdb_assert(graphdb, p < e);
      if (*p == '"') {
        state->tok_state = GRAPHDB_TOK_STRING;
        if (++p >= e) break;
        goto graphdb_tok_string;
      }

      /* We're looking at ( or ) */
      if (s0 == p) {
        /* There was nothing else interesting on the way.
         */
        *s = p + 1;
        graphdb_log(graphdb, CL_LEVEL_ULTRA, "sGT: %c", *p);

        return *p;
      }
      /*  Before that, we saw some general data.
       *  Do the interesting thing seperately, on
       *  the next call.
       */
      *s = p;
      graphdb_log(graphdb, CL_LEVEL_ULTRA, "sGT: \" %.*s", (int)(p - s0), s0);
      return '"';

    case GRAPHDB_TOK_STRING_ESCAPED:
    graphdb_tok_string_escaped:

      graphdb_assert(graphdb, p < e);
      p++;
      state->tok_state = GRAPHDB_TOK_STRING;

    /* FALL THROUGH */

    case GRAPHDB_TOK_STRING:
    graphdb_tok_string:

      while (p < e) switch (*p++) {
          case '\\':
            state->tok_state = GRAPHDB_TOK_STRING_ESCAPED;
            if (p >= e) break;
            goto graphdb_tok_string_escaped;

          case '"':
            state->tok_state = GRAPHDB_TOK_INITIAL;
            if (p >= e) break;
            goto again;

          default:
            break;
        }
      break;

    default:
      graphdb_notreached(graphdb, "unexpected tokenizer state %d",
                         state->tok_state);
  }

  *s = p;
  return '"';
}

/*  Push back a token into the call stream.
 */

void graphdb_token_unget(graphdb_handle* graphdb, graphdb_tokenizer* state,
                         int tok, char const* tok_s, char const* tok_e) {
  graphdb_assert(graphdb, state->tok_lookahead == GRAPHDB_TOKENIZE_MORE);

  graphdb_log(graphdb, CL_LEVEL_ULTRA, "GT: unget \"%.*s\" (%c)",
              (int)(tok_e - tok_s), tok_s, tok);

  state->tok_s = tok_s;
  state->tok_e = tok_e;
  state->tok_lookahead = tok;
}

void graphdb_token_initialize(graphdb_tokenizer* state) {
  memset(state, 0, sizeof(*state));
  state->tok_buf = NULL;
  state->tok_lookahead = GRAPHDB_TOKENIZE_MORE;
}

/**
 * @brief What's the next lookahead or real token?
 *
 *  If the caller passes in EOF (NULL s/e), the call will
 *  never return GRAPHDB_TOKENIZE_MORE.
 *
 * @param graphdb	opaque module pointer
 * @param state		tokenizer state
 * @param s		beginning of pending input
 * @param e 		end of pending input.
 *
 * @return GRAPHDB_TOKENIZE_MORE -- Can't tell.  Feed me more input.
 * @return GRAPHDB_TOKENIZE_EOF -- EOF
 * @return '"' -- a string
 * @return otherwise, the first character of the pending token.
 */
int graphdb_token_peek(graphdb_handle* graphdb, graphdb_tokenizer const* state,
                       char const* s, char const* e) {
  if (state->tok_lookahead != GRAPHDB_TOKENIZE_MORE)
    return state->tok_lookahead;

  /* EOF */
  if (s == NULL) {
    switch (state->tok_state) {
      case GRAPHDB_TOK_INITIAL:
        break;

      case GRAPHDB_TOK_CR:
        return '\n';
      case GRAPHDB_TOK_ATOM:
        return state->tok_buf[0];
      case GRAPHDB_TOK_STRING_ESCAPED:
      case GRAPHDB_TOK_STRING:
        return '"';
      default:
        graphdb_notreached(graphdb, "unexpected tokenizer state %d",
                           state->tok_state);
    }
    return GRAPHDB_TOKENIZE_EOF;
  }

  graphdb_assert(graphdb, e != NULL);
  graphdb_assert(graphdb, s <= e);

  if (state->tok_state != GRAPHDB_TOK_INITIAL) return GRAPHDB_TOKENIZE_MORE;

  graphdb_assert(graphdb, state->tok_state == GRAPHDB_TOK_INITIAL);

  while (s < e && IS_SPACE(*s)) s++;
  if (s >= e) return GRAPHDB_TOKENIZE_MORE;

  if (*s == '\r') return '\n';
  return *s;
}
