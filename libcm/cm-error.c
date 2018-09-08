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


typedef struct cm_error_handle {
  cm_handle me_handle;
  cm_handle *me_source;

  void (*me_log)(void *, int, char const *, ...);
  void *me_log_data;

} cm_error_handle;

#define ME_HANDLE(cm) ((cm_error_handle *)(cm))

static void cm_error_log_stderr(void *dummy, int level, char const *str, ...) {
  va_list ap;

  va_start(ap, str);

  vfprintf(stderr, str, ap);
  putc('\n', stderr);

  va_end(ap);
}

/*  Allocate, reallocate, or free a fragment of memory.
 */
static void *cm_error_realloc_loc(cm_handle *cm, void *ptr, size_t size,
                                  char const *file, int line) {
  cm_error_handle *h = ME_HANDLE(cm);
  void *result;

  result = h->me_source->cm_realloc_loc(h->me_source, ptr, size, file, line);
  if (size != 0 && result == NULL) {
    char bigbuf[1024];

    if (ptr != NULL)
      snprintf(bigbuf, sizeof bigbuf,
               "\"%s\", line %d: failed to reallocate %p "
               "to %lu bytes: %s",
               file, line, ptr, (unsigned long)size, strerror(errno));
    else
      snprintf(bigbuf, sizeof bigbuf,
               "\"%s\", line %d: failed to allocate "
               "%lu bytes: %s",
               file, line, (unsigned long)size, strerror(errno));
    (*h->me_log)(h->me_log_data, CM_LOG_ERROR, bigbuf);
    abort();
  }
  return result;
}

/**
 * @brief Create an error-reporting allocator.
 *
 * This allocator calls another allocator and aborts if the other
 * allocator fails.  This is useful if, in a testing
 * context, a developer wants to exclude failing allocations as a
 * source of error; or if a piece of software that requires allocations
 * to never fail has to be used for some reason.
 *
 * @param source  the allocator that does the actual allocating
 * @return an allocator that prints error messages and aborts
 *	if allocations fail.
 * @return NULL on allocation failure (oh the irony!)
 *
 */
cm_handle *cm_error(cm_handle *source) {
  cm_error_handle *h;

  h = cm_talloc(source, cm_error_handle, 1);
  if (h == NULL) return NULL;
  memset(h, 0, sizeof(*h));

  h->me_source = source;
  h->me_handle.cm_realloc_loc = cm_error_realloc_loc;

  /* By default, log to stderr. */
  h->me_log = cm_error_log_stderr;
  h->me_log_data = NULL;

  return &h->me_handle;
}

/**
 * @brief Set the log callback and its opaque data poitner.
 *
 * By default, the allocator prints its error message to
 * stderr.  The using application can override that by
 * setting this callback and its data pointer.
 *
 * @param cm  the allocator handle; it must have been allocated
 * 	using cm_error()
 * @param callback 	the log callback to call to report an error
 * @param callback_data	opaque pointer passed to the log callback as
 *			its first argument.
 */
void cm_error_set_log_callback(cm_handle *cm, cm_log_callback *callback,
                               void *callback_data) {
  cm_error_handle *h = ME_HANDLE(cm);

  h->me_log = callback;
  h->me_log_data = callback;
}

void cm_error_destroy_loc(cm_handle *cm, char const *file, int line) {
  if (cm) {
    cm_error_handle *h = ME_HANDLE(cm);
    h->me_source->cm_realloc_loc(h->me_source, h, 0, file, line);
  }
}
