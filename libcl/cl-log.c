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
 * @brief Log a message.
 *
 * The message is logged only if its log level is better or equal to
 * the loglevel of the handle, as set with cl_set_loglevel_full().
 * The logging subsystem is free to truncate log messages if they're
 * too long; currently, that limit is at 1024.

 * @param cl a log-handle created with cl_create().
 * @param level the loglevel of the message, e.g. #CL_LEVEL_DEBUG.
 * @param fmt a printf-style format string, followed by its arguments.
 */
void(cl_log)(cl_handle* cl, cl_loglevel level, char const* fmt, ...) {
  if (cl == NULL || cl_is_logged(cl, level)) {
    va_list ap;

    va_start(ap, fmt);
    cl_vlog_func(cl, level, 0, 0, fmt, ap);
    va_end(ap);
  }
}
