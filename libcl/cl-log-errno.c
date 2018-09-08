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
#include "libcl/clp.h"

#include <stdarg.h>
#include <string.h>


/**
 * @brief Log a message about a failed call.
 *
 *  This is a thin wrapper around a call to cl_log() that
 *  standardizes how errors are reported.
 *
 * @param cl a log-handle created with cl_create().
 * @param level the loglevel of the message, e.g. #CL_LEVEL_DEBUG.
 * @param file source file name of the calling code, inserted via macro
 * @param line source file line of the calling code, inserted via macro
 * @param caller the function reporting the error, inserted via macro
 * @param called the function causing the error, typically a unix system call
 * @param err error code returned by the function call
 * @param fmt NULL or a printf-style format string, followed by its arguments.
 */

void cl_log_errno_loc(cl_handle* cl, cl_loglevel level, char const* file,
                      int line, char const* caller, char const* called, int err,
                      char const* fmt, ...) {
  char bigbuf[16 * 1024];
  va_list ap;

  if (!(cl == NULL || cl_is_logged(cl, level))) return;

  if (fmt == NULL)
    bigbuf[0] = '\0';
  else {
    bigbuf[0] = ':';
    bigbuf[1] = ' ';

    va_start(ap, fmt);
    vsnprintf(bigbuf + 2, sizeof(bigbuf) - 2, fmt, ap);
    va_end(ap);
  }

  cl_log(cl, level, "%s:%d: %s: %s failed; errno=%d (%s)%s", file, line, caller,
         called, err, cl->cl_strerror(cl->cl_strerror_data, err), bigbuf);
}

#if !CL_HAVE_C9X_VA_ARGS

/**
 * @brief Log a message about a failed call.
 *
 *  Normally, uses of cl_log_errno are redirected to cl_log_errno_loc()
 *  by a C9X varargs macro that inserts __FILE__, __LINE__, and
 *  __function__.  This function is for installations that don't have
 *  varargs macros.
 *
 * @param cl a log-handle created with cl_create().
 * @param level the loglevel of the message, e.g. #CL_LEVEL_DEBUG.
 * @param called the function causing the error, typically a unix system call
 * @param err the value of errno
 * @param fmt a printf-style format string, followed by its arguments.
 */

void cl_log_errno(cl_handle* cl, cl_loglevel level, char const* called, int err,
                  char const* fmt, ...) {
  char bigbuf[16 * 1024];
  va_list ap;

  if (!(cl == NULL || cl_is_logged(cl, level))) return;

  if (fmt == NULL)
    bigbuf[0] = '\0';
  else {
    bigbuf[0] = ':';
    bigbuf[1] = ' ';

    va_start(ap, fmt);
    vsnprintf(bigbuf + 2, sizeof(bigbuf) - 2, fmt, ap);
    va_end(ap);
  }

  cl_log(cl, level, "%s failed; errno=%d (%s): %s", called, err, strerror(err),
         fmt);
}

#endif /* ! CL_HAVE_C9X_VA_ARGS */
