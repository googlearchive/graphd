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
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "libcm/cm.h"

/**
 * @brief Create a new prefix by appending to an old one.
 *
 *  The new prefix shares a buffer with the old prefix.
 *  (Only one such prefix buffer can ever be used at any
 *  one time.)
 *
 * @param source	the previous indentation level
 * @param segment	a word or dot-separated subpath to append
 *
 * @return source with the segment appended.
 */
cm_prefix cm_prefix_push(cm_prefix const* source, char const* segment) {
  cm_prefix p;
  size_t n = strlen(segment);

  p = *source;
  if (p.pre_offset + n + 2 + 5 <= p.pre_size) {
    if (p.pre_offset > 0) p.pre_buffer[p.pre_offset++] = '.';
    memcpy(p.pre_buffer + p.pre_offset, segment, n);
    p.pre_offset += n;
  } else if (p.pre_offset + 4 <= p.pre_size) {
    memcpy(p.pre_buffer + p.pre_offset, "...", 4);
    p.pre_offset += 3;
  }
  return p;
}

/**
 * @brief Add to a prefix based on formatted text.
 *
 *  Sample usage:
 * <pre>
 * 	prefix = cm_prefix_push_sprintf(prefix, "%d", n);
 * </pre>
 *
 * @param source	Prefix to this.
 * @param fmt		Format string.
 * @return source with the segment appended.
 */
cm_prefix cm_prefix_pushf(cm_prefix const* source, char const* fmt, ...) {
  va_list ap;
  cm_prefix p;

  if (*fmt == '\0') return *source;

  p = *source;
  if (p.pre_offset + 2 + 5 < p.pre_size) {
    if (p.pre_offset > 0) p.pre_buffer[p.pre_offset++] = '.';

    va_start(ap, fmt);
    vsnprintf(p.pre_buffer + p.pre_offset, p.pre_size - p.pre_offset, fmt, ap);
    va_end(ap);
    p.pre_offset += strlen(p.pre_buffer + p.pre_offset);
  } else if (p.pre_offset + 4 <= p.pre_size) {
    memcpy(p.pre_buffer + p.pre_offset, "...", 4);
    p.pre_offset += 3;
  }
  return p;
}

/**
 * @brief append to a prefix, and return the full resulting string.
 *
 *  Usually called via the cm_prefix_end macro.
 *
 * @param pre		prefix to append to.
 * @param s		the first byte of the segment to append.
 * @param n	 	number of bytes.
 *
 * @return the text of the prefix, with the last segment as
 *  	specified; or an error message if the prefix outgrew
 *	the buffer.
 */
char const* cm_prefix_end_bytes(cm_prefix const* pre, char const* s, size_t n) {
  if (pre->pre_offset + n + 2 >= pre->pre_size) return "prefix too long!";

  if (pre->pre_offset > 0 && n > 0 && *s != '.') {
    pre->pre_buffer[pre->pre_offset] = '.';
    memcpy(pre->pre_buffer + pre->pre_offset + 1, s, n);
    pre->pre_buffer[pre->pre_offset + 1 + n] = '\0';
  } else {
    memcpy(pre->pre_buffer + pre->pre_offset, s, n);
    pre->pre_buffer[pre->pre_offset + n] = '\0';
  }
  return pre->pre_buffer;
}

/**
 * @brief append to a prefix, and return the full resulting string.
 *
 *  This is the dynamic, '\\0'-terminated version of cm_prefix_end.
 *  Use it for non-literal strings.
 *
 * @param pre		prefix to append to.
 * @param segment	pointer to the segment to append.
 *
 * @return the text of the prefix, with the last segment as
 *  	specified; or an error message if the prefix outgrew
 *	the buffer.
 */
char const* cm_prefix_end_string(cm_prefix const* pre, char const* segment) {
  return cm_prefix_end_bytes(pre, segment, strlen(segment));
}

/**
 * @brief Initialize a prefix stack.
 *
 *  Prefixes built based on the result of this call
 *  will share the passed-in buffer. Only one such
 *  prefix can be used (in the sense of printed as a
 *  string) at any one time.
 *
 * @param buffer	use this buffer
 * @param size		number of bytes pointed to by buffer.
 *
 * @return the new (empty) prefix.
 */
cm_prefix cm_prefix_initialize(char* buffer, size_t size) {
  cm_prefix p;

  p.pre_buffer = buffer;
  p.pre_size = size;
  p.pre_offset = 0;

  return p;
}
