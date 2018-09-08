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
#include <string.h>
#include <ctype.h>

#include "libgraph/graph.h"

#define IS_LIT(lit, s, e) \
  ((e) - (s) == sizeof(lit) - 1 && !strncasecmp(s, lit, sizeof(lit) - 1))

/**
 * @brief Convert a string to a graph_datatype enum value
 * @param buf 	store the resulting datatype here.
 * @param s 	beginning of the string to be scanned.
 * @param e 	pointer just past the end of the string to be scanned.
 * @return 0 on success
 * @return EINVAL if s or e is a null pointer
 * @return EINVAL if the text between s and e doesn't
 *		case-insensitively match a datatype name (e.g., "float")
 */
int graph_datatype_from_string(graph_datatype* buf, char const* s,
                               char const* e) {
  if (!s || s >= e) return EINVAL;
  switch (isascii(*s) ? tolower(*s) : *s) {
    case 'b':
      if (IS_LIT("boolean", s, e)) {
        *buf = GRAPH_DATA_BOOLEAN;
        break;
      } else if (IS_LIT("bytestring", s, e)) {
        *buf = GRAPH_DATA_BYTESTRING;
        break;
      }
      return EINVAL;
    case 'f':
      if (IS_LIT("float", s, e)) {
        *buf = GRAPH_DATA_FLOAT;
        break;
      }
      return EINVAL;
    case 'g':
      if (IS_LIT("guid", s, e)) {
        *buf = GRAPH_DATA_GUID;
        break;
      }
      return EINVAL;
    case 'i':
      if (IS_LIT("integer", s, e)) {
        *buf = GRAPH_DATA_INTEGER;
        break;
      }
      return EINVAL;
    case 'n':
      if (IS_LIT("null", s, e)) {
        *buf = GRAPH_DATA_NULL;
        break;
      }
      return EINVAL;
    case 's':
      if (IS_LIT("string", s, e)) {
        *buf = GRAPH_DATA_STRING;
        break;
      }
      return EINVAL;
    case 't':
      if (IS_LIT("timestamp", s, e)) {
        *buf = GRAPH_DATA_TIMESTAMP;
        break;
      }
      return EINVAL;
    case 'u':
      if (IS_LIT("url", s, e)) {
        *buf = GRAPH_DATA_URL;
        break;
      }
      return EINVAL;
    default:
      /*  Apart from the quaint string notation,
       *  datatypes can also be specified as small
       *  numbers between 1 and 255, inclusive.
       */
      if (isascii(*s) && isdigit(*s)) {
        unsigned long n = 0;

        while (s < e && isascii(*s) && isdigit(*s)) {
          n = n * 10 + *s - '0';
          if (n >= 256) return ERANGE;
          s++;
        }
        if (n == 0 || s < e) return EINVAL;
        *buf = n;
        break;
      }
      return EINVAL;
  }
  return 0;
}

static char const* graph_datatype_names[] = {
    "unspecified", "null",      "string", "integer",    "float",
    "guid",        "timestamp", "url",    "bytestring", "boolean"};

/**
 * @brief Convert a datatype enum value to a string
 * @param dt 	the datatype enum value, e.g. GRAPH_DATA_FLOAT
 * @return NULL if the passed-in parameter is not a valid datatype
 * @return otherwise, a string that names the datatype, e.g. "float"
 */
char const* graph_datatype_to_string(graph_datatype dt) {
  if (dt < 0 ||
      dt >= sizeof(graph_datatype_names) / sizeof(*graph_datatype_names))
    return NULL;
  return graph_datatype_names[dt];
}
