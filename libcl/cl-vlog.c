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
#define _GNU_SOURCE

#include "libcl/clp.h"

#ifdef __GLIBC__
#include <execinfo.h>
#include <dlfcn.h>
#define HAVE_GLIBC_BACKTRACE
#define CL_INTERNAL_STACK_DEPTH 2
#endif

static void render_stacktrace(cl_handle* cl, char* buffer, size_t size) {
#ifdef HAVE_GLIBC_BACKTRACE
  size_t backtrace_n;
  void* backtrace_pointers[50];
  size_t co;
  int i;
  int have_syminfo;
  void* stack_top;
  Dl_info symbol;

  stack_top = dlsym(NULL, "main");

  if (!stack_top) return; /* Impressive. Send ld-linux to the hospital now */

  backtrace_n = backtrace(backtrace_pointers,
                          sizeof(backtrace_pointers) / (sizeof(void*)));

  co = strlen(buffer);

  /*
   * Start at two because there's at least two cl_error functions
   * including this one.
   */
  for (i = CL_INTERNAL_STACK_DEPTH; i < backtrace_n; i++) {
    if (!backtrace_pointers[i]) break;

    have_syminfo = dladdr(backtrace_pointers[i], &symbol);

    /*
     * Don't crawl beyond main() into libc
     */
    if (have_syminfo && (symbol.dli_saddr == stack_top)) return;

    if (i == CL_INTERNAL_STACK_DEPTH) {
      /* First time.  Add a prompt.
       */
      if (size - co > sizeof("\nStacktrace: ")) {
        snprintf(buffer + co, size - co, "\nStacktrace: ");
        co += strlen(buffer + co);
      }
    } else {
      if (co < (size - 2)) {
        buffer[co] = ',';
        buffer[co + 1] = ' ';
        co += 2;
      }
    }

    if (!have_syminfo || !symbol.dli_sname) {
      const char* file = NULL;
      /*
       * If we found the symbol, but don't have
       * a name for it, use the file name and
       * an absolute address, if we can.
       *
       * If we got nothing at all, just use the
       * absolute address.
       */
      if (have_syminfo && symbol.dli_fname) {
        /*
         * Only the last part of a file
         * name is interesting
         */
        file = strrchr(symbol.dli_fname, '/');
        if (!file)
          file = symbol.dli_fname;
        else
          file++;
      }

      snprintf(buffer + co, size - co, "%s[%p]", file ? file : "",
               backtrace_pointers[i]);
      co += strlen(buffer + co);
    } else {
      snprintf(buffer + co, size - co, "%s+%i", symbol.dli_sname,
               (int)(backtrace_pointers[i] - symbol.dli_saddr));
      co += strlen(buffer + co);
    }
  }
#endif
  return;
}

void cl_vlog_func(cl_handle* cl, cl_loglevel level, char const* func,
                  bool entering, char const* fmt, va_list ap) {
  char bigbuf[16 * 1024];
  va_list aq;

  char* buf_ptr = bigbuf;
  size_t buf_size = sizeof bigbuf;

  size_t indent = 0;

  if (!cl_is_logged(cl, level)) return;

  /* Low-level (debug and below) messages are
   * indented according to the current enter/leave
   * nesting depth.
   */
  if (cl != NULL && !CL_IS_LOGGED(CL_LEVEL_DETAIL, level))
    indent = cl->cl_indent;

  if (indent >= sizeof(bigbuf) / 2) indent = sizeof(bigbuf) / 2;

  if (indent > 0) memset(bigbuf, ' ', indent);
  if (func) {
    snprintf(buf_ptr + indent, buf_size - indent, "%c %s ",
             entering ? '{' : '}', func);
    indent += strlen(buf_ptr + indent);
  }

  for (;;) {
    va_copy(aq, ap);
    vsnprintf(buf_ptr + indent, buf_size - indent, fmt, aq);
    va_end(aq);

    buf_ptr[buf_size - 1] = '\0';

    /* We fit it all in?
     */
    if (strlen(buf_ptr) < buf_size - 1) break;

    /*  Double the buffer size and try again.
     */
    if (buf_ptr == bigbuf) {
      buf_ptr = malloc(buf_size * 2);
      if (buf_ptr == NULL) {
        buf_ptr = bigbuf;
        strncpy(buf_ptr + buf_size - 6, "[...]", 6);
        break;
      }
      if (indent > 0) memcpy(buf_ptr, bigbuf, indent);
    } else {
      char* tmp = realloc(buf_ptr, buf_size * 2);
      if (tmp == NULL) {
        strncpy(buf_ptr + buf_size - 6, "[...]", 6);
        break;
      }
      buf_ptr = tmp;
    }
    buf_size *= 2;
  }
  if (CL_IS_LOGGED(CL_LEVEL_ERROR, level) && cl->cl_stacktrace)
    render_stacktrace(cl, buf_ptr, buf_size);

  if (cl) {
    (*cl->cl_write)(cl->cl_write_data, level, buf_ptr);
    if (cl->cl_siphon && CL_IS_LOGGED(cl->cl_siphon_level, level))
      (*cl->cl_siphon)(cl->cl_siphon_data, level, buf_ptr);
  } else
    (void)fprintf(stderr,
                  "%s\n(and by the way, your "
                  "log handle is NULL, too.)\n",
                  buf_ptr);

  if (buf_ptr != bigbuf) free(buf_ptr);
}

/**
 * @brief Log a message.
 * This is the explicit var-args version of cl_log().
 *
 * @param cl a log-handle created with cl_create().
 * @param level the loglevel of the message, e.g. CL_LEVEL_DEBUG.
 * @param fmt a printf-style format string.
 * @param ap va_start()ed arguments for the format string.
 */
void cl_vlog(cl_handle* cl, cl_loglevel level, char const* fmt, va_list ap) {
  cl_vlog_func(cl, level, (char*)0, 0, fmt, ap);
}
