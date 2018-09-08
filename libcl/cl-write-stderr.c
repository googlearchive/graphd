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
#include <stdarg.h>
#include <syslog.h>
#include <sys/resource.h>
#include <sys/time.h>


void cl_write_stderr(void *p, cl_loglevel lev, char const *str) {
  CL_DIARY_CHECK((cl_handle *)p, lev, str)

  if (CL_IS_LOGGED(CL_LEVEL_OPERATOR_ERROR, lev)) fputs("ERROR: ", stderr);
  fputs(str, stderr);
  putc('\n', stderr);
}
