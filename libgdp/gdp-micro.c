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
#include "libgdp/gdp-micro.h"
#include "libgdp/gdp-error.h"

#include <ctype.h>
#include <stdio.h>

/*
 * NOTE: `gdp_micro_row' and `gdp_micro_col' are currently used only for
 * debug purposes (see graphd-ast-debug.c).
 */

/** Current row number relative to the beginning of the input */
unsigned int gdp_micro_row = 1;

/** Current column number */
unsigned int gdp_micro_col = 1;

/**
 * Automaton used to determine wether multiple requests have been merged
 * together.
 */
static const unsigned char gdp_micro_automaton[][26] = {
    /*           a   b   c   d   e   f   g   h   i   j   k   l   m   n   o   p
       q   r   s   t   u   v   w   x   y   z */
    /*  0: */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 12, 0, 0,
               18, 23, 0, 0, 0},  // .[rsvw]
    /*  1: */ {0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0},  // r.e
    /*  2: */ {3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 8, 0, 0, 0,
               0, 0, 0, 0},  // re.[aps]
    /*  3: */ {0, 0, 0, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0},  // rea.d$
    /*  4: */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0},  // rep.l
    /*  5: */ {0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0},  // repl.i
    /*  6: */ {0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0},  // repli.c
    /*  7: */ {99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0},  // replic.a$
    /*  8: */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0,
               0, 0, 0, 0},  // res.t
    /*  9: */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0},  // rest.o
    /* 10: */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0,
               0, 0, 0, 0, 0},  // resto.r
    /* 11: */ {0, 0, 0, 0, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0},  // restor.e$
    /* 12: */ {0, 0, 0, 0, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 14, 0,
               0, 0, 0, 0, 0},  // s.[et]
    /* 13: */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 99, 0,
               0, 0, 0, 0, 0},  // se.t$
    /* 14: */ {15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0},  // st.a
    /* 15: */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0,
               0, 0, 0, 0, 0},  // sta.t
    /* 16: */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17,
               0, 0, 0, 0, 0},  // stat.u
    /* 17: */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 99, 0, 0,
               0, 0, 0, 0, 0},  // statu.s$
    /* 18: */ {0, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0},  // v.e
    /* 19: */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0,
               0, 0, 0, 0, 0},  // ve.r
    /* 20: */ {0, 0, 0, 0, 0, 0, 0, 0, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0},  // ver.i
    /* 21: */ {0, 0, 0, 0, 0, 22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0},  // veri.f
    /* 22: */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 99, 0},  // verif.y$
    /* 23: */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0,
               0, 0, 0, 0, 0},  // w.r
    /* 24: */ {0, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0},  // wr.i
    /* 25: */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 26, 0,
               0, 0, 0, 0, 0},  // wri.t
    /* 26: */ {0, 0, 0, 0, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0},  // writ.e$
};

/**
 * Update row and column number.
 */
static inline void consume(char c) {
  switch (c) {
    case '\n':
      gdp_micro_row++;
      gdp_micro_col = 1;
      break;
    default:
      gdp_micro_col++;
  }
}

/**
 * Detect the beginning of a new request, possibly within an existing request.
 *
 * Returns `true' if a new request has been detected.
 *
 * NOTE: This function will return a false positive in the following cases:
 *
 *    set (access=read-only ..)
 *    set (access=read-write ..)
 *    set (access=replica ..)
 *    set (access=restore ..)
 */
static bool detect_new_request(gdp_micro* micro, char const c) {
  unsigned int next;

  /* current automaton state */
  const unsigned int state = micro->micro_autostate;

  /* determine next automaton state */
  if (isascii(c) && isalpha(c)) {
    const unsigned int i = tolower(c) - 'a';

    /* next state */
    switch ((next = gdp_micro_automaton[state][i])) {
      case 99:
        /* end state reached */
        return true;
      case 0:
        /* start over */
        if (state > 0) next = gdp_micro_automaton[0][i];
    }
  } else
    next = 0;

  micro->micro_autostate = next;

  return false;
}

void gdp_micro_init(gdp_micro* micro) {
  *micro = (struct gdp_micro){
      .micro_level = 0,
      .micro_lastch = 0,
      .micro_autostate = 0,
      .micro_string = false,
      .micro_ready = false,
      .micro_escape = false,
      .micro_malformed = false,
  };
}

int gdp_micro_parse(gdp_micro* micro, char** s, char* e) {
  char* p;

  for (p = *s; p < e && !micro->micro_ready; p++) {
    const char c = *p;

    /* update line and column coordinates */
    consume(c);

    /* if we are parsing a potentially malformed request, stop at
     * the next newline and ignore anything in between */
    if (micro->micro_malformed) {
      if (c == '\n') micro->micro_ready = true;
      continue;
    }

    /*
     * parsing a string..
     */

    if (micro->micro_string) {
      if (micro->micro_escape)
        micro->micro_escape = false;
      else
        switch (c) {
          case '"':
            micro->micro_string = false;
            break;
          case '\\':
            micro->micro_escape = true;
            break;
          case '\n':  // not good...
            micro->micro_ready = true;
            micro->micro_malformed = true;
            break;
        }

      continue;
    }

    /*
     * parsing anything but a string..
     */

    /* detect the beginning of a request (this happens when
     * multiple requests end up merged together for some reason) */
    if (micro->micro_level && detect_new_request(micro, c)) {
      micro->micro_malformed = true;
      continue;
    }

    switch (c) {
      case '(':
        if (micro->micro_level < MAX_PAREN_DEPTH)
          micro->micro_level++;
        else
          return GDP_ERR_MALFORMED;
        break;
      case ')':
        if (micro->micro_level >= 0)
          micro->micro_level--;
        else
          return GDP_ERR_MALFORMED;
        break;
      case '"':
        micro->micro_string = true;
        break;
      case '\n':
        /* finish parsing if we are outside a parenthesized
         * list and at least one non-space character has been
         * consumed */
        if (micro->micro_level <= 0 && micro->micro_lastch != 0)
          micro->micro_ready = true;
        break;
    }

    /* remember the last non-space character */
    if (!isspace(c)) micro->micro_lastch = c;
  }

  *s = p;

  return 0;
}
