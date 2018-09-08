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
#include <time.h>
#include <unistd.h>


/**
 * Write log entry.
 */
void cl_write_file(void *callback_data, cl_loglevel lev, char const *str) {
  cl_handle *cl = callback_data;

  CL_DIARY_CHECK(cl, lev, str)

  /* current time in seconds since the Epoch */
  const time_t now = time(NULL);

  /* rotate log-file if timer has expired; on failure, continue logging
   * to the previous file (if any) */
  if (cl_timer_check(cl, now) || cl_pid_check(cl))
    (void)cl_file_rotate(cl, now);  // ignore error

  if (cl->cl_fp != NULL) {
    struct tm tm;
    char buf[128];
    size_t len;

    localtime_r(&now, &tm);

    /*
     * Include timestamp and PID.
     */
    if ((len = strftime(buf, sizeof buf, "%b %d %H:%M:%S ", &tm))) {
      if (len < sizeof buf)
        len += snprintf(buf + len, sizeof(buf) - len, "[%u] ",
                        (unsigned int)getpid());
      (void)fwrite(buf, len > sizeof buf ? sizeof buf : len, 1, cl->cl_fp);
    }

    if (CL_IS_LOGGED(CL_LEVEL_OPERATOR_ERROR, lev))
      (void)fputs("ERROR: ", cl->cl_fp);
    (void)fputs(str, cl->cl_fp);
    (void)putc('\n', cl->cl_fp);

    if (cl->cl_flush == CL_FLUSH_ALWAYS) (void)fflush(cl->cl_fp);
  }
}
