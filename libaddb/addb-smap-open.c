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
#include "libaddb/addbp.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "libaddb/addb-smap.h"

/*  SMAP: Map IDs to lists of IDs.
 *
 *  This module translates a 34-bit "application ID" into a list of
 *  other IDs.  The lists can be appended to after they've been created.
 */

/**
 * @brief Create or open an "smap" database.
 *
 *  If the mode is ADDB_MODE_WRITE_ONLY or ADDB_MODE_READ_WRITE,
 *  the call attempts to create the database directory if it
 *  doesn't exist yet.
 *
 * @param addb	opaque database module handle
 * @param path	directory that the database files live in
 * @param mode one of #ADDB_MODE_READ_ONLY, #ADDB_MODE_WRITE_ONLY,
 *	or #ADDB_MODE_READ_WRITE.
 * @return NULL on error, otherwise a freshly opened smap database.
 *	If the call returns NULL, errno is set to indicate the source
 *  	of the error.
 */

addb_smap *addb_smap_open(addb_handle *addb, char const *path, int mode,
                          unsigned long long horizon,
                          addb_smap_configuration *scf) {
  size_t path_n;
  addb_smap_partition *part, *part_end;
  addb_smap *sm;
  int err = 0;
  struct stat st;

  cl_assert(addb->addb_cl, path != NULL);
  cl_assert(addb->addb_cl, mode != 0);

  /* If the directory doesn't yet exist, try to create it.
   */
  if ((mode & ADDB_MODE_WRITE) && mkdir(path, 0755) == -1) {
    if (errno != EEXIST) {
      err = errno;
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb: failed to create smap database"
             " directory \"%s\": %s [%s:%d]",
             path, strerror(errno), __FILE__, __LINE__);
      errno = err;
      cl_cover(addb->addb_cl);

      return NULL;
    }
    cl_cover(addb->addb_cl);
  }

  if (stat(path, &st) != 0) {
    err = errno;

    cl_cover(addb->addb_cl);
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: can't stat smap database "
           "directory \"%s\": %s [%s:%d]",
           path, strerror(errno), __FILE__, __LINE__);

    errno = err;
    return NULL;
  }
  if (!S_ISDIR(st.st_mode)) {
    cl_cover(addb->addb_cl);
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: \"%s\" exists, but is not "
           "a directory. [%s:%d]",
           path, __FILE__, __LINE__);
    errno = ENOTDIR;
    return NULL;
  }

  path_n = strlen(path);
  sm = cm_zalloc(addb->addb_cm, sizeof(addb_smap) + path_n + 80);
  if (sm == NULL) {
    err = errno;
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: failed to allocate %lu bytes for "
           "smap database structure for \"%s\" [%s:%d]",
           (unsigned long)(sizeof(addb_smap) + path_n + 80), path, __FILE__,
           __LINE__);

    errno = err;
    return sm;
  }

  sm->sm_addb = addb;
  sm->sm_horizon = horizon;
  sm->sm_backup = false;

  /*
   * Copy in our configuration information.
   */
  if (scf) sm->sm_cf = *scf;

  /* Set up the generator for partition filenames.
   */
  sm->sm_path = (char *)(sm + 1);
  memcpy(sm->sm_path, path, path_n);
  sm->sm_base = sm->sm_path + path_n;
  sm->sm_base_n = 80;

  if (sm->sm_base > sm->sm_path && sm->sm_base[-1] != '/') {
    cl_cover(addb->addb_cl);
    *sm->sm_base++ = '/';
    sm->sm_base_n--;
  }
  *sm->sm_base = '\0';

  cl_assert(addb->addb_cl, addb->addb_master_tiled_pool);

  sm->sm_tiled_pool = addb->addb_master_tiled_pool;

  part = sm->sm_partition;
  part_end = sm->sm_partition +
             (sizeof(sm->sm_partition) / sizeof(sm->sm_partition[0]));
  for (; part < part_end; part++) addb_smap_partition_initialize(sm, part);
  sm->sm_partition_n = 0;

  /*  Load partitions from disk.
   */
  if ((err = addb_smap_partitions_read(sm, mode)) != 0) {
    cl_cover(addb->addb_cl);
    cm_free(addb->addb_cm, sm);
    errno = err;
    return NULL;
  }

  return sm;
}
