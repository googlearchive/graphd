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

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>


void(cl_indent)(cl_handle* cl, cl_loglevel lev, int indent) {
  if (!cl_is_logged(cl, lev)) return;

  if (indent < 0 && cl->cl_indent < -indent)
    cl_log(cl, lev, "cl_indent mismatch");
  else
    cl->cl_indent += indent;
}

static void cl_venter_func(cl_handle* cl, cl_loglevel lev, char const* func,
                           char const* fmt, va_list ap) {
  cl_vlog_func(cl, lev, func, true, fmt, ap);
  cl->cl_indent++;
}

/**
 * @brief Enter a new function or section.
 *
 *  Slow. Only used if we don't have varargs macros.
 *
 *  The format string must begin with an identifier literal
 *  (i.e, the function name).
 *  There must be a corresponding cl_(v)leave().
 *
 * @param cl	Log handle through which to log
 * @param lev	Loglevel for leave message
 * @param fmt	Format string
 */

#if !CL_HAVE_C9X_VA_ARGS

void(cl_enter)(cl_handle* cl, cl_loglevel lev, char const* fmt, ...) {
  va_list ap;

  va_start(ap, fmt);
  cl_venter_func(cl, lev, "no __func__", fmt, ap);
  va_end(ap);
}

#endif

void cl_enter_func(cl_handle* cl, cl_loglevel lev, char const* func,
                   char const* fmt, ...) {
  va_list ap;

  va_start(ap, fmt);
  cl_venter_func(cl, lev, func, fmt, ap);
  va_end(ap);
}

/**
 * @brief Leave a function or section previously entered with cl_enter().
 *
 *	Slow. Only used if we don't have varargs macros.
 *
 * @param lev	Loglevel for leave message
 * @param cl	Log handle through which to log
 * @param fmt	Format string
 */

static int cl_vleave_err_func(cl_handle* cl, cl_loglevel lev, int err,
                              char const* func, char const* fmt, va_list ap) {
  if (cl) {
    if (cl->cl_indent > 0)
      cl->cl_indent--;
    else
      cl_log(cl, lev, "%s cl_push/cl_pop mismatch", func);
  }

  cl_vlog_func(cl, lev, func, false, fmt, ap);

  return err;
}

int cl_leave_err_func(cl_handle* cl, cl_loglevel lev, int err, char const* func,
                      char const* fmt, ...) {
  va_list ap;
  int e;

  va_start(ap, fmt);
  e = cl_vleave_err_func(cl, lev, err, func, fmt, ap);
  va_end(ap);

  return e;
}

#if !CL_HAVE_C9X_VA_ARGS

void cl_leave_err(cl_handle* cl, cl_loglevel lev, int err, char const* fmt,
                  ...) {
  va_list ap;

  va_start(ap, fmt);
  (void)cl_vleave_err_func(cl, lev, err, "no __func__", fmt, ap);
  va_end(ap);
}

void(cl_leave)(cl_handle* cl, cl_loglevel lev, char const* fmt, ...) {
  va_list ap;

  va_start(ap, fmt);
  (void)cl_vleave_err_func(cl, lev, 0, "no __func__", fmt, ap);
  va_end(ap);
}

#endif
