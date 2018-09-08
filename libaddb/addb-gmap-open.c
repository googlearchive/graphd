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
#include "libaddb/addb-bgmap.h"
#include "libaddb/addbp.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


/*  GMAP: Map IDs to lists of IDs.
 *
 *  This module translates a 34-bit "application ID" into a list of
 *  other IDs.  The lists can be appended to after they've been created.
 */

/**
 * @brief Create or open an "gmap" database.
 *
 *  If the mode is ADDB_MODE_WRITE_ONLY or ADDB_MODE_READ_WRITE,
 *  the call attempts to create the database directory if it
 *  doesn't exist yet.
 *
 * @param addb	opaque database module handle
 * @param path	directory that the database files live in
 * @param mode one of #ADDB_MODE_READ_ONLY, #ADDB_MODE_WRITE_ONLY,
 *	or #ADDB_MODE_READ_WRITE.
 * @return NULL on error, otherwise a freshly opened gmap database.
 *	If the call returns NULL, errno is set to indicate the source
 *  	of the error.
 */

addb_gmap *addb_gmap_open(addb_handle *addb, char const *path, int mode,
                          unsigned long long horizon,
                          addb_gmap_configuration *gcf) {
  size_t path_n;
  addb_gmap_partition *part, *part_end;
  addb_gmap *gm;
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
             "addb: failed to create gmap database"
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
           "addb: can't stat gmap database "
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
  gm = cm_zalloc(addb->addb_cm, sizeof(addb_gmap) + path_n + 80);
  if (gm == NULL) {
    err = errno;
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: failed to allocate %lu bytes for "
           "gmap database structure for \"%s\" [%s:%d]",
           (unsigned long)(sizeof(addb_gmap) + path_n + 80), path, __FILE__,
           __LINE__);

    errno = err;
    return gm;
  }

  gm->gm_addb = addb;
  gm->gm_horizon = horizon;
  gm->gm_backup = false;

  /*
   * Copy in our configuration information.
   */
  if (gcf) gm->gm_cf = *gcf;

  /* Set up the generator for partition filenames.
   */
  gm->gm_path = (char *)(gm + 1);
  memcpy(gm->gm_path, path, path_n);
  gm->gm_base = gm->gm_path + path_n;
  gm->gm_base_n = 80;

  gm->gm_dir_fd = open(path, O_RDONLY);
  if (gm->gm_dir_fd < 0) {
    err = errno;
    cl_log_errno(addb->addb_cl, CL_LEVEL_FAIL, "open", err,
                 "Failed to open directory %s", path);
    errno = err;
    cm_free(addb->addb_cm, gm);
    return NULL;
  }

  if (gm->gm_base > gm->gm_path && gm->gm_base[-1] != '/') {
    cl_cover(addb->addb_cl);
    *gm->gm_base++ = '/';
    gm->gm_base_n--;
  }
  *gm->gm_base = '\0';

  gm->gm_tiled_pool = addb->addb_master_tiled_pool;
  cl_assert(addb->addb_cl, gm->gm_tiled_pool);

  part = gm->gm_partition;
  part_end = gm->gm_partition +
             (sizeof(gm->gm_partition) / sizeof(gm->gm_partition[0]));
  for (; part < part_end; part++) addb_gmap_partition_initialize(gm, part);
  gm->gm_partition_n = 0;

  /*  Load partitions from disk.
   */
  if ((err = addb_gmap_partitions_read(gm, mode)) != 0) {
    cl_cover(addb->addb_cl);
    cm_free(addb->addb_cm, gm);
    errno = err;
    return NULL;
  }
  gm->gm_lfhandle = addb_largefile_init(
      gm->gm_path, gm->gm_addb, addb->addb_cl, addb->addb_cm,
      addb_gmap_largefile_size_get, addb_gmap_largefile_size_set, gm);

  if (!gm->gm_lfhandle) {
    addb_gmap_close(gm);
    return (NULL);
  }

  /*
   * Do any configuration work that needs to be done after
   * we've setup everything else.
   */
  if (gcf != NULL) addb_gmap_configure(gm, gcf);

  gm->gm_bgmap_handle = addb_bgmap_create(addb, path);
  addb_file_sync_initialize(addb, &gm->gm_dir_fsync_ctx);

  cl_log(addb->addb_cl, CL_LEVEL_SPEW, "addb_gmap_open(%s): %p", path,
         (void *)gm);

  return gm;
}
