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
#include "libcm/cm.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


/**
 * @brief Initialize a general-use malloc buffer
 * @param cm	Handle to allocate through
 * @param buf	Buffer to initialize
 */
void cm_buffer_initialize(cm_buffer* buf, cm_handle* cm) {
  buf->buf_cm = cm;
  buf->buf_s = NULL;
  buf->buf_n = 0;
  buf->buf_m = 0;
}

/**
 * @brief Free a general-use malloc buffer
 * @param buf	NULL or buffer whose resources are free'd
 */
void cm_buffer_finish(cm_buffer* buf) {
  if (buf != NULL) {
    if (buf->buf_cm != NULL && buf->buf_s != NULL && buf->buf_m > 0) {
      cm_free(buf->buf_cm, buf->buf_s);

      buf->buf_cm = NULL;
      buf->buf_s = NULL;
      buf->buf_n = 0;
      buf->buf_m = 0;
    }
  }
}

/**
 * @brief Allocate more space in a buffer
 *
 * @param buf	buffer to allocate in
 * @param size	number of empty bytes to allocate
 */
int cm_buffer_alloc_loc(cm_buffer* buf, size_t size, char const* file,
                        int line) {
  char* tmp;

  if (buf == NULL) return EINVAL;

  if (buf->buf_n + size <= buf->buf_m) return 0;

  errno = 0;
  tmp = (*buf->buf_cm->cm_realloc_loc)(buf->buf_cm, buf->buf_s,
                                       buf->buf_n + (size < 1024 ? 1024 : size),
                                       file, line);
  if (tmp == NULL) return errno ? errno : EINVAL;

  buf->buf_s = tmp;
  buf->buf_m = buf->buf_n + (size < 1024 ? 1024 : size);

  return 0;
}

static char const cm_buffer_empty[] = "";

/**
 * @brief Free a general-use malloc buffer
 * @param buf	NULL or buffer whose resources are free'd
 */
char const* cm_buffer_memory(cm_buffer const* buf) {
  return buf ? buf->buf_s : cm_buffer_empty;
}

/**
 * @brief Return a buffer's end.
 * @param buf	NULL or buffer
 * @return A pointer just after the end of the text in the buffer.
 *	(Even for a NULL buffer, a pointer just after the end of
 *	the zero-length string returned by cm_buffer_memory() is returned.)
 */
char const* cm_buffer_memory_end(cm_buffer const* buf) {
  return buf ? buf->buf_s + buf->buf_n : cm_buffer_empty;
}

/**
 * @brief How many bytes of text are in this buffer?
 * @param buf	NULL or buffer
 * @return The number of bytes in the buffer.
 */
size_t cm_buffer_length(cm_buffer const* buf) { return buf ? buf->buf_n : 0; }

/**
 * @brief Text formatting function that dynamically allocates memory
 *	for its result.
 *
 *  Sample usage:
 * <pre>
 * 	err = cm_buffer_sprintf(buf, "%s/%s.%s", dirname, file, suffix);
 * 	if (err != 0) error...
 * </pre>
 *
 * @param buf	Buffer to allocate in
 * @param fmt	Format string.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
int cm_buffer_sprintf_loc(char const* file, int line, cm_buffer* buf,
                          char const* fmt, ...) {
  va_list ap;

  size_t buffer_size = 0, increment = 128;

  if (buf == NULL || fmt == NULL) return EINVAL;

  for (;;) {
    int result;
    int err;

    /* Grow the buffer to the next larger size. */

    err = cm_buffer_alloc_loc(buf, buffer_size + increment + 1, file, line);
    if (err != 0) return err;

    buffer_size += increment;

    /*  In the early stages, double the increment size;
     *  later we grow linearly.
     */
    if (increment < 1024 * 64) increment *= 2;

    /*  Format into the buffer.
     */
    errno = 0;

    va_start(ap, fmt);
    result =
        vsnprintf(buf->buf_s + buf->buf_n, buf->buf_m - buf->buf_n, fmt, ap);
    va_end(ap);

    /*  Errors related to problems other than size.
     */
    if (result < 0 && (errno == EILSEQ || errno == ENOMEM)) {
      return errno;
    }

    if (result >= 0 && result < buffer_size) break;

    /*  If we happen to be dealing with a modern vsnprintf
     *  implementation, allocate exactly as much as we're missing.
     */
    if (result >= buffer_size) increment = (result + 1) - buffer_size;
  }
  buf->buf_n += strlen(buf->buf_s + buf->buf_n);
  return 0;
}

/**
 * @brief Text formatting function that dynamically allocates memory
 *	for its result.
 *
 *  Sample usage:
 * <pre>
 * 	err = cm_buffer_sprintf(buf, "%s/%s.%s", dirname, file, suffix);
 * 	if (err != 0) error...
 * </pre>
 *
 * @param buf	Buffer to allocate in
 * @param fmt	Format string.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
#undef cm_buffer_sprintf
int cm_buffer_sprintf(cm_buffer* buf, char const* fmt, ...) {
  va_list ap;

  size_t buffer_size = 0, increment = 128;

  if (buf == NULL || fmt == NULL) return EINVAL;

  for (;;) {
    int result;
    int err;

    /* Grow the buffer to the next larger size. */

    err = cm_buffer_alloc(buf, buffer_size + increment + 1);
    if (err != 0) return err;

    buffer_size += increment;

    /*  In the early stages, double the increment size;
     *  later we grow linearly.
     */
    if (increment < 1024 * 64) increment *= 2;

    /*  Format into the buffer.
     */
    errno = 0;

    va_start(ap, fmt);
    result =
        vsnprintf(buf->buf_s + buf->buf_n, buf->buf_m - buf->buf_n, fmt, ap);
    va_end(ap);

    /*  Errors related to problems other than size.
     */
    if (result < 0 && (errno == EILSEQ || errno == ENOMEM)) {
      return errno;
    }

    if (result >= 0 && result < buffer_size) break;

    /*  If we happen to be dealing with a modern vsnprintf
     *  implementation, allocate exactly as much as we're missing.
     */
    if (result >= buffer_size) increment = (result + 1) - buffer_size;
  }
  buf->buf_n += strlen(buf->buf_s + buf->buf_n);
  return 0;
}

/**
 * @brief Append a non-0-terminated string to a buffer.
 *
 * @param buf	Buffer to allocate in
 * @param str	String to add.
 * @param n	Number of bytes to add from str.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
int cm_buffer_add_bytes_loc(cm_buffer* buf, char const* str, size_t n,
                            char const* file, int line) {
  int err;

  if (str == NULL || n == 0) return 0;

  if ((err = cm_buffer_alloc_loc(buf, n + 1, file, line)) != 0) return err;

  memcpy(buf->buf_s + buf->buf_n, str, n);
  buf->buf_s[buf->buf_n += n] = '\0';

  return 0;
}

/**
 * @brief Compute a checksum for the passed-in text.
 *
 *  The quality of the checksum doesn't particularly
 *  matter - this isn't cryptographic or used to hash
 *  the strings, just a guard against accidental misuse.
 *
 * @param s	beginning of text to check
 * @param e	end of text to check (exclusive)
 * @param n	Bits in the checksum.
 *
 * @return The checksum.
 */
unsigned long long cm_buffer_checksum_text(char const* s, char const* e,
                                           int bits) {
  unsigned long long sum = 0;
  unsigned long long mask = (1ull << bits) - 1;
  char const* p;

  if (bits == 0) return 0;

  for (p = s; p < e; p++) {
    /* Exor in another byte. */
    sum ^= *p;
    sum &= mask;

    /* Rotate by 1 */
    if (bits > 1) sum = mask & ((sum << 1) | (1 & (sum >> (bits - 1))));
  }
  return sum;
}

/**
 * @brief Compute a checksum for the buffer.
 *
 *  The quality of the checksum doesn't particularly
 *  matter - this isn't cryptographic or used to hash
 *  the strings.
 *
 * @param buf	Buffer to allocate in
 * @param n	Bits in the checksum.
 *
 * @return The checksum.
 */
unsigned long long cm_buffer_checksum(cm_buffer const* buf, int bits) {
  return cm_buffer_checksum_text(buf->buf_s, buf->buf_s + buf->buf_n, bits);
}

/**
 * @brief Append a string to a buffer.
 *
 * @param buf	Buffer to allocate in
 * @param str	String to add.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
int cm_buffer_add_string(cm_buffer* const buf, char const* const str) {
  if (str == NULL) return 0;
  return cm_buffer_add_bytes(buf, str, strlen(str));
}

/**
 * @brief Truncate a buffer.
 * @param buf	Buffer to truncate.
 */
void cm_buffer_truncate(cm_buffer* const buf) {
  if (buf != NULL) buf->buf_n = 0;
}

#ifdef TEST

#include "libcm/cm.h"
#include "libcm/cm-c.c"

int main(void) {
  cm_handle* cm = cm_c();
  cm_buffer buf;

  cm_buffer_initialize(&buf, cm);
  cm_buffer_sprintf(&buf, "%s", "Hello, ");
  cm_buffer_sprintf(&buf, "%s", "World!");

  printf("%d\n", (int)cm_buffer_length(&buf));
  printf("%s\n", cm_buffer_memory(&buf));

  cm_buffer_finish(&buf);

  cm_buffer_initialize(&buf, cm);
  cm_buffer_finish(&buf);

  return 0;
}
#endif
