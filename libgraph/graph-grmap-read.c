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
#include <time.h>
#include <errno.h>
#include <ctype.h>

#include "libgraph/graphp.h"

/*
  grmap {
    XNUM {
        XNUM-XNUM: XNUM -XNUM
        XNUM-XNUM: XNUM +XNUM
    }
    XNUM {
        XNUM-XNUM: XNUM -XNUM
        XNUM-XNUM: XNUM +XNUM
    }
  }
*/

void graph_grmap_read_initialize(graph_grmap const* grm,
                                 graph_grmap_read_state* state) {
  memset(state, 0, sizeof(*state));
  state->grs_literal = " grmap";
  cl_cover(grm->grm_graph->graph_cl);
}

static unsigned int atox(int ch) {
  if (isascii(ch)) {
    if (isdigit(ch)) return ch - '0';
    if (isalpha(ch)) return 10 + ch - (islower(ch) ? 'a' : 'A');
  }
  return 16;
}

static int end_number(graph_grmap* grm, graph_grmap_read_state* state) {
  int err;

  if (state->grs_in_dbid) {
    state->grs_num[state->grs_num_i++] =
        (long long)state->grs_number * (long long)state->grs_sign;

    if (state->grs_in_dbid && state->grs_num_i >= 4) {
      graph_guid source, destination;

      cl_cover(grm->grm_graph->graph_cl);
      graph_guid_from_db_serial(&source, state->grs_dbid, state->grs_num[0]);

      graph_guid_from_db_serial(&destination, state->grs_num[2],
                                state->grs_num[0] + state->grs_num[3]);

      err = graph_grmap_add_range(grm, &source, &destination,
                                  state->grs_num[1] - state->grs_num[0]);

      state->grs_num_i = 0;
      if (err != 0) return err;
    } else {
      cl_cover(grm->grm_graph->graph_cl);
    }
  } else {
    cl_cover(grm->grm_graph->graph_cl);
    state->grs_dbid = state->grs_number;
    state->grs_num_i = 0;
  }

  state->grs_sign = 1;
  state->grs_number = 0;
  state->grs_in_number = false;

  return 0;
}

int graph_grmap_read_next(graph_grmap* grm, char const** s, char const* const e,
                          graph_grmap_read_state* state) {
  int err;
  cl_handle* const cl = grm->grm_graph->graph_cl;

  if (s == NULL) {
    if (state->grs_in_map) {
      cl_cover(cl);
      return GRAPH_ERR_LEXICAL;
    }
    cl_cover(cl);
    return 0;
  }

  for (; *s < e; ++*s) {
    if (!isascii(**s)) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_FAIL,
             "graph_grmap_read_next: unexpected "
             "non-ascii character '%c' (%x) in input",
             (int)**s, (unsigned int)**s);
      return GRAPH_ERR_LEXICAL;
    }

    if (state->grs_in_number) {
      if (!isascii(**s) || !isalnum(**s)) {
        err = end_number(grm, state);
        if (err != 0) {
          cl_log(cl, CL_LEVEL_FAIL,
                 "graph_grmap_read_next: error "
                 "from end_number: %s",
                 strerror(err));
          cl_cover(cl);
          return err;
        }

        /* Continue to interpret the
         * white space or punctuation that
         * ended this number.
         */
      } else {
        /* Keep parsing.
         */
        int ch = atox(**s);
        if (ch >= 16) {
          cl_log(cl, CL_LEVEL_FAIL,
                 "graph_grmap_read_next: unexpected "
                 "non-hex character '%c' (%x) in input",
                 (int)**s, (unsigned int)**s);
          cl_cover(cl);
          return GRAPH_ERR_LEXICAL;
        }

        state->grs_number *= 16;
        state->grs_number += ch;

        continue;
      }
    }

    if (state->grs_literal != NULL) {
      if (*state->grs_literal == '\0') {
        if (isascii(**s) && isalnum(**s)) {
          cl_cover(cl);
          cl_log(cl, CL_LEVEL_FAIL,
                 "graph_grmap_read_next: unexpected "
                 "non-literal character '%c' (%x) in input",
                 (int)**s, (unsigned int)**s);
          return GRAPH_ERR_LEXICAL;
        }
        state->grs_literal = NULL;
        cl_cover(cl);

        /*  Continue parsing what interrupted us.
         */
      } else {
        /* ' ' means: skip spaces while there are any.
         */
        if (*state->grs_literal == ' ') {
          if (isascii(**s) && isspace(**s)) {
            cl_cover(cl);
            continue;
          }
          state->grs_literal++;
        }

        if (isascii(**s) && tolower(**s) == *state->grs_literal) {
          state->grs_literal++;

          /* Punctuation ends itself. */

          if (*state->grs_literal == '\0' && ispunct(state->grs_literal[-1]))
            state->grs_literal = NULL;
          continue;
        }
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_FAIL,
               "graph_grmap_read_next: unexpected "
               "non-literal character '%c' (%x) in input; "
               "expected %c (%x)",
               (int)**s, (unsigned int)**s, *state->grs_literal,
               *state->grs_literal);
        return GRAPH_ERR_LEXICAL;
      }
    }

    if (isspace(**s)) {
      cl_cover(cl);
      continue;
    }

    if (isalnum(**s)) {
      int ch = atox(**s);
      if (ch >= 16) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graph_grmap_read_next: unexpected "
               "non-hex character '%c' (%x) in input",
               (int)**s, (unsigned int)**s);
        cl_cover(cl);
        return GRAPH_ERR_LEXICAL;
      }
      state->grs_number = ch;
      state->grs_in_number = true;

      continue;
    }

    switch (**s) {
      case ':':
        if (state->grs_num_i == 2) {
          cl_cover(cl);
          continue;
        }
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_FAIL,
               "graph_grmap_read_next: "
               "unexpected ':' in input; num_i is %d",
               state->grs_num_i);
        return GRAPH_ERR_LEXICAL;

      case '-':
        if (state->grs_num_i == 1) {
          cl_cover(cl);
          continue;
        }

        if (state->grs_num_i == 3) {
          cl_cover(cl);
          state->grs_sign = -1;
          continue;
        }

        cl_cover(cl);
        cl_log(cl, CL_LEVEL_FAIL,
               "graph_grmap_read_next: "
               "unexpected '-' in input; num_i is %d",
               state->grs_num_i);
        return GRAPH_ERR_LEXICAL;

      case '+':
        if (state->grs_num_i == 3) {
          cl_cover(cl);
          continue;
        }
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_FAIL,
               "graph_grmap_read_next: "
               "unexpected '+' in input; num_i is %d",
               state->grs_num_i);
        return GRAPH_ERR_LEXICAL;

      case '{':
        if (!state->grs_in_map) {
          cl_cover(cl);
          state->grs_in_map = true;
        } else if (state->grs_in_dbid || state->grs_num_i != 0) {
          cl_cover(cl);
          cl_log(cl, CL_LEVEL_FAIL,
                 "graph_grmap_read_next: "
                 "unexpected '{' in input; num_i is %d",
                 state->grs_num_i);
          return GRAPH_ERR_LEXICAL;
        } else {
          cl_cover(cl);
          state->grs_in_dbid = true;
        }
        continue;

      case '}':
        if (state->grs_in_dbid) {
          if (state->grs_num_i != 0) {
            cl_cover(cl);
            cl_log(cl, CL_LEVEL_FAIL,
                   "graph_grmap_read_next: "
                   "unexpected '}' in input; num_i is %d",
                   state->grs_num_i);
            return GRAPH_ERR_LEXICAL;
          }
          cl_cover(cl);
          state->grs_in_dbid = false;
          cl_cover(cl);
        } else {
          if (!state->grs_in_map) {
            cl_cover(cl);
            return GRAPH_ERR_NO;
          }
          state->grs_in_map = false;
          ++*s;

          /* Done; nothing else to read. */
          cl_cover(cl);
          return GRAPH_ERR_DONE;
        }
        continue;

      default:
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_FAIL,
               "graph_grmap_read_next: unexpected "
               "character '%c' (%x)",
               **s, **s);
        return GRAPH_ERR_LEXICAL;
    }
  }

  /* Out of stuff to read, but could handle more.
   */
  cl_cover(cl);
  return 0;
}
