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
#include "libgraphdb/graphdbp.h"

#include <stdio.h>
#include <syslog.h>

#include "libcl/cl.h"

void graphdb_log(graphdb_handle* graphdb, cl_loglevel lev, char const* fmt,
                 ...) {
  if (graphdb == NULL || graphdb->graphdb_loglevel >= lev) {
    va_list ap;
    va_start(ap, fmt);

    if (graphdb != NULL && graphdb->graphdb_vlog)
      (*graphdb->graphdb_vlog)(graphdb->graphdb_cl, lev, fmt, ap);
    else {
      char buf[10 * 1024];

      vsnprintf(buf, sizeof buf, fmt, ap);

      if (graphdb != NULL) {
        if (!graphdb->graphdb_syslog_open) {
          openlog("graphdb", 0, LOG_USER);
          graphdb->graphdb_syslog_open = 1;
        }
        syslog(LOG_USER, "%s", buf);
      }

      /* print to stderr as well */
      if (graphdb == NULL || lev >= CL_LEVEL_ERROR)
        fprintf(stderr, "%s\n", buf);
    }

    va_end(ap);
  }
}
