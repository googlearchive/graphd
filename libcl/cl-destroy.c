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
#include <syslog.h>


/**
 * @brief Destroy a log module handle.
 *
 * Frees all resources allocated for a log module.
 * If it had been switched to syslog, the library calls closelog(3).
 * If it had been switched to writing to a file, the library closes
 * (and implicitly complete writing to) that file.
 *
 * @param cl NULL or a valid log handle created with cl_create().
 */
void cl_destroy(cl_handle *cl) {
  if (!cl) return;

  if (cl->cl_destroy != NULL) (*cl->cl_destroy)(cl);

  if (cl->cl_diary != NULL) {
    cl_diary_destroy(cl->cl_diary);
    cl->cl_diary = NULL;
  }

  if (cl->cl_netlog_host != NULL) free(cl->cl_netlog_host);
  if (cl->cl_netlog_ciid != NULL) free(cl->cl_netlog_ciid);

  if (cl->cl_file_name_fmt != NULL) free(cl->cl_file_name_fmt);
  if (cl->cl_file_name != NULL) free(cl->cl_file_name);
  if (cl->cl_fp != NULL) fclose(cl->cl_fp);

  if (cl->cl_syslog_ident != NULL) free(cl->cl_syslog_ident);

  if (cl->cl_syslog_open) closelog();

  free(cl);
}
