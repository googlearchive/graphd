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
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>

#include "libcm/cm.h"

/**
 * @brief Text formatting function that dynamically allocates memory
 *	for its result.
 *
 *  Sample usage:
 * <pre>
 * 	path =  cm_sprintf(cm, "%s/%s.%s", dirname, file, suffix);
 * 	if (path == NULL) error...
 * </pre>
 *
 * @param cm	Handle to allocate through
 * @param fmt	Format string.
 *
 * @return On success, a '\\0' terminated string with the result
 * 	of the snprintf, must be cm_free()d by the caller.
 * @return NULL if the call ran out of memory.
 */
char* cm_sprintf(cm_handle* cm, char const* fmt, ...) {
  va_list ap;

  char* buffer = NULL;
  size_t buffer_size = 0, increment = 128;

  if (cm == NULL || fmt == NULL) {
    errno = EINVAL;
    return NULL;
  }

  for (;;) {
    int result;
    char* tmp;

    /* Grow the buffer to the next larger size. */

    tmp = cm_realloc(cm, buffer, buffer_size + increment);
    if (tmp == NULL) {
      cm_free(cm, buffer);
      return NULL;
    }
    buffer = tmp;
    buffer_size += increment;

    /*  In the early stages, double the increment size;
     *  later we grow linearly.
     */
    if (increment < 1024 * 64) increment *= 2;

    /*  Format into the buffer.
     */
    errno = 0;

    va_start(ap, fmt);
    result = vsnprintf(buffer, buffer_size, fmt, ap);
    va_end(ap);

    /*  Errors related to problems other than size.
     */
    if (result < 0 && (errno == EILSEQ || errno == ENOMEM)) {
      int err = errno;
      cm_free(cm, buffer);
      errno = err;

      return NULL;
    }

    if (result >= 0 && result < buffer_size) break;

    /*  If we happen to be dealing with a modern vsnprintf
     *  implementation, allocate exactly as much as we're missing.
     */
    if (result >= buffer_size) increment = (result + 1) - buffer_size;
  }
  return buffer;
}

#ifdef TEST

#include "libcm/cm.h"

int main(void) {
  cm_handle* cm = cm_c();
  char* buf;

  buf = cm_sprintf(cm, "%1024s", "Hello, world!");
  printf("%d\n", buf ? strlen(buf) : NULL);
  cm_free(cm, buf);
  return 0;
}
#endif
