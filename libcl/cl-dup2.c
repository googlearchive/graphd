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
#include <unistd.h>

/**
 * @brief Make a given file descriptor - typically stdout (fd 1)
 *   or stderr (fd 2) - redirect to the file descriptor written to
 *   by this log stream, if any.
 *
 *   Such a redirection survives file reopening (as with
 *   patterned filenames).
 *
 * @param cl 		the log module handle
 * @param filedes2  	the file descriptor to dup to.
 *
 * @return E2BIG  - you can just have two of those open at a time.
 * @return EALREADY - this file descriptor is already dup'ed to.
 * @return EINVAL - this isn't a log stream to a file
 * @return 0 on success
 * @return other nonzero error codes if dup2() fails.
 */
int cl_dup2(cl_handle* cl, int filedes2) {
  size_t i;

  if (cl->cl_file_dup_n >=
      sizeof(cl->cl_file_dup_buf) / sizeof(cl->cl_file_dup_buf[0]))
    return E2BIG;

  for (i = 0; i < cl->cl_file_dup_n; i++)
    if (cl->cl_file_dup_buf[i] == filedes2) return EALREADY;

  /*  Remember who we dup to, so we can redo that when
   *  reopening the file later.
   */
  cl->cl_file_dup_buf[cl->cl_file_dup_n++] = filedes2;

  if (cl != NULL && cl->cl_fp != NULL && fileno(cl->cl_fp) != -1) {
    if (dup2(fileno(cl->cl_fp), filedes2) < 0) {
      cl->cl_file_dup_n--;
      return errno;
    }
    return 0;
  }

  return EINVAL;
}

int cl_dup2_install(cl_handle* cl) {
  size_t i;
  int err = 0;

  for (i = 0; i < cl->cl_file_dup_n; i++)
    if (dup2(fileno(cl->cl_fp), cl->cl_file_dup_buf[i]) < 0) {
      if (err == 0) err = errno;

      /* but continue... */
    }
  return err;
}
