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
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/**
 * @brief Register a code coverage directory.
 *
 *  Calls to cl_cover() record code coverage checkpoints being hit.
 *  Those hits are recorded as files below a special code coverage directory
 *  that serves as an ad-hoc map { file, line } -> timestamp.
 *  This call establishes the name of the directory and creates it
 *  if it doesn't exist yet.
 *
 * @param cl 		a log-handle created with cl_create().
 * @param directory 	pathname of a code coverage directory
 */
int cl_set_coverage(cl_handle* cl, char const* directory) {
  char *p, *dir_dup;
  size_t n;

  if (directory == NULL) {
    if (cl->cl_coverage_path != NULL) {
      free(cl->cl_coverage_path);
      cl->cl_coverage_path = NULL;
    }
    return 0;
  }

  /* "" means ".", not "/" */
  if (*directory == '\0') directory = ".";

  n = strlen(directory);
  if ((dir_dup = malloc(n + 1)) == NULL) return ENOMEM;
  memcpy(dir_dup, directory, n + 1);

  /* Remove trailing slashes from the copy.  */
  p = dir_dup + n;
  while (p > dir_dup && p[-1] == '/') p--;
  *p = '\0';

  /* Create the directory.  It's okay if it exists. */
  if (mkdir(dir_dup, 0777) == -1 && errno != EEXIST) {
    int err = errno;
    free(dir_dup);

    return err;
  }

  /* Remember the coverage path. */
  if (cl->cl_coverage_path != NULL) free(cl->cl_coverage_path);
  cl->cl_coverage_path = dir_dup;

  return 0;
}
