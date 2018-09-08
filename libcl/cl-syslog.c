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

#include <stdlib.h>
#include <string.h>


/**
 * @brief Configure logging to use syslog,
 * the traditional Unix logging mechanism for unattended
 * servers.
 *
 * This function can be called at any time.
 * Usually, it should be called when a program transits
 * from its start-up phase, where a human operator might be
 * expected to watch standard error output, to a "background"
 * phase where nobody is watching.
 *
 * @warning
 *
 * Sylog has various interesting concurrency  issues that
 * this library, if configured to use syslog, inherits.
 * For example, only one identity can be used with syslog
 * per application (different libraries cannot log
 * under different identities), and how it behaves in a multithreaded
 * environment is anyone's guess.
 * If this becomes an issue, a mutex should be added within
 * this library, and used to log around calls to openlog() and
 * syslog().
 *
 * @param cl a log handle created using cl_create().
 * @param ident the syslog "identity", typically the application name
 *	(without a path).  It may be included in the syslog messages.
 * @param facility one of a small set of predefined "facilities"
 *	(roughly, system components) that the application should
 *	log as.  See the manual page for syslog(3) for a list.
 *
 */
void cl_syslog(cl_handle *cl, char const *ident, int facility) {
  cl->cl_syslog_open = 0;
  cl->cl_syslog_facility = facility;

  if (ident != NULL &&
      (cl->cl_syslog_ident = malloc(strlen(ident) + 1)) != NULL)
    strcpy(cl->cl_syslog_ident, ident);

  cl->cl_write = cl_write_syslog;
  cl->cl_write_data = cl;
}
