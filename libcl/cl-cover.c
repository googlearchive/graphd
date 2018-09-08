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
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

/**
 * @brief Write a code coverage record.
 *
 *  To do code coverage, configure a code coverage path in the
 *  cl handle with cl_set_coverage(), and drop code coverage statements
 *
 *	cl_cover(cl);
 *
 *  into the source.
 *
 *  Below the code coverage directory, files named after source
 *  files and line numbers are created or updated (once per run) as the
 *  corresponding code coverage points are hit.
 *
 * 	-rw-r--r--   1 user  admin  1338 Jul 14 06:31 cl-cover.c:1234
 *
 *  means that line 1234 of file cl-cover.c was hit on Jul 14 06:31.
 *
 * @param cl 	a log-handle created with cl_create().
 * @param file 	basename or pathname of the call's source file.
 * @param line	line number of the call, relative to the source file.
 */
void cl_cover_loc(cl_handle* cl, char const* file, unsigned long line) {
  char buf[1024];
  char const* basename;
  int errno_tmp = errno;
  int fd;

  if (cl == NULL || cl->cl_coverage_path == NULL) return;

  cl_assert(cl, file);

  if ((basename = strrchr(file, '/')) != NULL)
    basename++;
  else
    basename = file;

  snprintf(buf, sizeof buf, "%s/%s:%lu", cl->cl_coverage_path, basename, line);
  (void)unlink(buf);

  if ((fd = open(buf, O_CREAT, 0666)) != -1) close(fd);

  errno = errno_tmp;
}
