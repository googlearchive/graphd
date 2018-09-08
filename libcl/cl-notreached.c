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
 * @brief Print an error message that includes the current location, then exit.
 * @param cl	The log handle through which to log
 * @param file	The caller's source file
 * @param line	The caller's line in their source file
 * @param fmt	format string for the error message.
 */
void cl_notreached_loc(cl_handle* cl, char const* file, int line,
                       char const* fmt, ...) {
  char big_buf[1024 * 8];
  va_list ap;

  va_start(ap, fmt);
  vsnprintf(big_buf, sizeof big_buf, fmt, ap);
  va_end(ap);

  cl_log(cl, CL_LEVEL_FATAL, "\"%s\", line %d: %s", file, line, big_buf);

  if (cl) (*cl->cl_abort)(cl->cl_abort_data);
  cl_abort_c(NULL);
}

#undef cl_notreached

/**
 * @brief Print an error message, then exit.
 * @param cl	The log handle through which to log
 * @param fmt	format string for the error message.
 */
void cl_notreached(cl_handle* cl, char const* fmt, ...) {
  char big_buf[1024 * 8];
  va_list ap;

  va_start(ap, fmt);
  vsnprintf(big_buf, sizeof big_buf, fmt, ap);
  va_end(ap);

  cl_log(cl, CL_LEVEL_FATAL, "notreached: %s", big_buf);
  if (cl) (*cl->cl_abort)(cl->cl_abort_data);
  cl_abort_c(NULL);
}
