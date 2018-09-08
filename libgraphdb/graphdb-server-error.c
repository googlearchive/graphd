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
#include "libgraphdb/graphdbp.h"

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>

#define TOK_IS_LIT(s, e, lit) \
  ((e) - (s) == sizeof(lit) - 1 && sm_strncasecmp(s, lit, sizeof(lit) - 1))

/**
 * @brief Hash a string into an error code.
 *
 * @param s 	first byte
 * @param e 	pointer just after last byte
 *
 * @return the hashed string.
 */
graphdb_server_error graphdb_server_error_hash_bytes(char const* s,
                                                     char const* e) {
  graphdb_server_error sec;

  for (sec = 0; s < e; s++) {
    sec = sec << 5;
    sec ^= (unsigned char)(isascii(*s) ? tolower(*s) : *s);
  }
  return sec;
}

/**
 * @brief Hash a '\\0'-terminated string into an error code.
 * @param s 	string
 * @return the error code.
 */
graphdb_server_error graphdb_server_error_hash_literal(char const* s) {
  return graphdb_server_error_hash_bytes(s, s + strlen(s));
}
