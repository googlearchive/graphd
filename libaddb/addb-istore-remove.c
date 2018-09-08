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

/**
 * @brief Remove an istore database from a file tree.
 *
 *  Don't do this while the database is open.
 *
 * @param addb	module handle, opened with addb_create()
 * @param path	directroy pathname of the database to be removed.
 *
 * @return 0 on success, a nonzero error number on error.
 */

int addb_istore_remove(addb_handle* addb, char const* path) {
  unsigned int partition = 0;
  char* partition_path;
  char* partition_base;
  size_t partition_base_n;
  size_t path_n;
  int err;

  path_n = strlen(path);

  partition_path = cm_malloc(addb->addb_cm, path_n + 80);
  if (partition_path == NULL) {
    err = errno;
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: failed to allocate %lu bytes for partition "
           "file name [%s:%d]",
           (unsigned long)path_n + 80, __FILE__, __LINE__);
    return err;
  }
  memcpy(partition_path, path, path_n);
  partition_base = partition_path + path_n;
  partition_base_n = 80;
  if (partition_base > partition_path && partition_base[-1] != '/') {
    cl_cover(addb->addb_cl);
    *partition_base++ = '/';
    partition_base_n--;
  }

  for (; partition <= ADDB_ISTORE_PARTITIONS_MAX; partition++) {
    addb_istore_partition_basename(addb, partition, partition_base,
                                   partition_base_n);
    cl_cover(addb->addb_cl);

    if (unlink(partition_path)) {
      if ((err = errno) == ENOENT) break;

      cl_cover(addb->addb_cl);
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb: can't remove istore "
             "partition \"%s\": %s [%s:%d]",
             partition_path, strerror(err), __FILE__, __LINE__);
      cm_free(addb->addb_cm, partition_path);
      return err;
    }
  }

  /*  Remove the "marker" file that tells us what our file size
   *  and highest record number are.
   */
  strcpy(partition_base, "next");
  if (unlink(partition_path)) {
    if ((err = errno) != ENOENT) {
      cl_cover(addb->addb_cl);
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb: can't remove istore "
             "marker file \"%s\": %s [%s:%d]",
             partition_path, strerror(errno), __FILE__, __LINE__);
      cm_free(addb->addb_cm, partition_path);
      return err;
    }
  }
  strcpy(partition_base, "horizon");
  if (unlink(partition_path)) {
    if ((err = errno) != ENOENT) {
      cl_cover(addb->addb_cl);
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb: can't remove istore "
             "marker file \"%s\": %s [%s:%d]",
             partition_path, strerror(errno), __FILE__, __LINE__);
      cm_free(addb->addb_cm, partition_path);
      return err;
    }
  }

  if (rmdir(path) != 0) {
    err = errno;
    cl_cover(addb->addb_cl);
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: can't remove istore directory "
           "\"%s\": %s [%s:%d]",
           path, strerror(errno), __FILE__, __LINE__);
    cm_free(addb->addb_cm, partition_path);
    return err;
  }

  cm_free(addb->addb_cm, partition_path);
  cl_cover(addb->addb_cl);

  return 0;
}

int addb_istore_truncate(addb_istore* is, char const* path) {
  if (is) {
    addb_handle* const addb = is->is_addb;
    addb_istore_partition* part = is->is_partition;
    addb_istore_partition* const part_end =
        is->is_partition + is->is_partition_n;
    int err = 0;
    int e;

    for (; part < part_end; part++)
      if (part->ipart_td) {
        e = addb_tiled_backup(part->ipart_td, 0);
        if (e) err = e;
      }

    e = addb_istore_close(is);
    if (e) err = e;

    e = addb_istore_remove(addb, path);
    if (e) err = e;

    return err;
  }

  return 0;
}
