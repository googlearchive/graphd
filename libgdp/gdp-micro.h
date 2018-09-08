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
#ifndef __GDP_MICRO_H__
#define __GDP_MICRO_H__

#include <stdbool.h>

#define MAX_PAREN_DEPTH 4096

/**
 * Micro-parser state.
 */
struct gdp_micro {
  int micro_level;                ///< Netsing level
  char micro_lastch;              ///< Last non-empty char
  unsigned char micro_autostate;  ///< Automaton state
  bool micro_string : 1;          ///< Parsing a string?
  bool micro_ready : 1;           ///< Done micro-parsing?
  bool micro_escape : 1;          ///< Parsing an escape char?
  bool micro_malformed : 1;       ///< A malformed request?
};

typedef struct gdp_micro gdp_micro;

/**
 * Initialize micro-parser state.
 */
extern void gdp_micro_init(gdp_micro* micro);

/**
 * Micro-parse input.
 *
 * Invoke this function on consecutive blocks of input until a positive return
 * value indicates that the end of a request has been detected.
 *
 * @param micro
 *	Micro-parser state.
 *
 * @param [in/out] s
 *	On input, `*s' specifies the beginning of an input block. On output,
 *	`*s' points to the last parsed character in the block plus one. If the
 *	end of a request has been detected, `*s' points to the last character
 *	of the request plus one (which could be anywhere in the input block).
 *
 * @param e
 *	Points to the last character of the input block plus one.
 *
 * @return
 *	0 on success, a nonzero error code on error.
 */
extern int gdp_micro_parse(gdp_micro* micro, char** s, char* e);

#endif
