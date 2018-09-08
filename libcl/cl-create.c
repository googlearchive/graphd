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

#include <errno.h>
#include <stdlib.h>
#include <string.h>


static char const *cl_strerror_c(void *p, int err) { return strerror(err); }

/**
 * @brief Create a new log module handle.
 *
 * An unconfigured log handle such as the one
 * returned by this call logs to standard error
 * at loglevel #CL_LEVEL_OPERATOR_ERROR or better.  A newline
 * is appended to printed messages by the library.
 *
 * Once the application stops logging output, the handle
 * must be destroyed using cl_destroy().
 *
 * @returns NULL on allocation error, otherwise a
 * log handle.
 */
cl_handle *cl_create(void) {
  cl_handle *cl;

  cl = malloc(sizeof(*cl));
  if (cl == NULL) return NULL;

  memset(cl, 0, sizeof(*cl));

  cl->cl_syslog_ident = NULL;

  cl->cl_file_name_fmt = NULL;
  cl->cl_file_name = NULL;
  cl->cl_file_minute = -1;
  cl->cl_file_pid = (pid_t)-1;
  cl->cl_fp = NULL;

  cl->cl_netlog_host = NULL;
  cl->cl_netlog_ciid = NULL;

  cl->cl_level = CL_LEVEL_OPERATOR_ERROR;

  cl->cl_write = cl_write_stderr;
  cl->cl_write_data = cl;

  cl->cl_abort = cl_abort_c;
  cl->cl_abort_data = NULL;

  cl->cl_strerror = cl_strerror_c;
  cl->cl_strerror_data = NULL;

  cl->cl_destroy = NULL;
  cl->cl_diary = NULL;
  cl->cl_stacktrace = true;

  return cl;
}
