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
#include <syslog.h>


void cl_write_syslog(void *p, cl_loglevel lev, char const *str) {
  cl_handle *cl = p;

  CL_DIARY_CHECK(cl, lev, str)

  if (!cl->cl_syslog_open) {
    openlog(cl->cl_syslog_ident, 0, cl->cl_syslog_facility);
    cl->cl_syslog_open = 1;
  }

  syslog(
      !CL_IS_LOGGED(CL_LEVEL_INFO, lev)
          ? LOG_DEBUG
          : !CL_IS_LOGGED(CL_LEVEL_OPERATOR_ERROR, lev)
                ? LOG_INFO
                : !CL_IS_LOGGED(CL_LEVEL_FATAL, lev) ? LOG_NOTICE : LOG_WARNING,
      "%s%s", CL_IS_LOGGED(CL_LEVEL_OPERATOR_ERROR, lev) ? "ERROR: " : "", str);
}
