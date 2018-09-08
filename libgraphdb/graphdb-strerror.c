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

#include <string.h>
#include <stdio.h>

/**
 * @brief Translate an error number into a human-readable error message.
 *
 * As long as buf is either NULL or a valid pointer, a call
 * always returns a valid string pointer, even if the error
 * number is not valid.
 *
 * @param err  	error number returned by a library function.
 * @param buf   buffer that can be used for formatting
 * @param bufsize 	# of bytes pointed to by buffer
 * @return a pointer to a string that can be printed to indicate the error.
 */

char const* graphdb_strerror(int err, char* buf, size_t bufsize) {
  if (err == 0) return "no error";
  if (err > 0) return strerror(err);
  if (buf == NULL) return "unexpected error";

  snprintf(buf, bufsize, "unexpected error %d", err);
  return buf;
}
