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

/**
 * @brief The non-fatal version of cl_notreached().
 *
 * This statement should never be executed.
 * Print an error message about where in the code you are (automatic),
 * and what is wrong (programmer-supplied), then continue execution.
 *
 * @param cl the module handle
 * @param file	The caller's source file
 *	(usually inserted by the cl_wnotreached() macro)
 * @param line	The caller's line in their source file
 *	(usually inserted by the cl_wnotreached() macro)
 * @param fmt a printf(3)-style format string,
 *	followed by its parameters
 */
void cl_wnotreached_loc(cl_handle* cl, char const* file, int line,
                        char const* fmt, ...) {
  char big_buf[1024 * 8];
  va_list ap;

  va_start(ap, fmt);
  vsnprintf(big_buf, sizeof big_buf, fmt, ap);
  va_end(ap);

  cl_log(cl, CL_LEVEL_ERROR, "\"%s\", line %d: %s", file, line, big_buf);
}

#undef cl_wnotreached

/**
 * @brief The non-fatal version of cl_notreached().
 * This statement should never be executed.
 * Print an error message about where in the code you are (automatic),
 * and what is wrong (programmer-supplied), then continue execution.
 *
 * @param cl the module handle
 * @param fmt a printf(3)-style format string,
 *	followed by its parameters
 */
void cl_wnotreached(cl_handle* cl, char const* fmt, ...) {
  char big_buf[1024 * 8];
  va_list ap;

  va_start(ap, fmt);
  vsnprintf(big_buf, sizeof big_buf, fmt, ap);
  va_end(ap);

  cl_log(cl, CL_LEVEL_FATAL, "warning: notreached: %s", big_buf);
}
