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

/*  ISTORE: Simple indexed database store.
 *
 *  This module translates a 34-bit "application ID" into a chunk of data.
 *  The chunks are variable-length at some minimum granularity and
 *  approximate (greater or equal) length.
 *  That's all it does; it doesn't search, or sort, or any of that.
 *
 *  In core, we keep a table of "database partitions".
 *
 *  Each partition is a big file (wrapped by a little in-core
 *  structure).  The file is mapped into memory when it is used,
 *  split into tiles.  (addb-tile.c manages that.)
 *
 *  Each such partition file contains up to 2^24, or 16 M, primitives.
 *  Freshly created, it's about 64M large (the space needed for its
 *  index table); we expect it to grow to 1-2 GB, once object data
 *  accumulates, but can accomodate up to 32 GB (that's 2k per primitive
 *  object.)
 *
 *  Reads, writes, and appends are cheap.  Deletions are impossible.
 *
 *  Once it has been written, an a piece of data may theoretically be
 *  written again, but it does not change its size, and it never is
 *  deleted.
 */

/**
 * @brief create or open an "istore" database.
 *
 *  An istore database translates a 34-bit "application ID" into a
 *  chunk of data.  The chunks are variable-length (with a certain
 *  minimum granularity).  Chunks never change and don't move around;
 *  once allocated, they stay forever.
 *
 * @param addb opaque addb module handle created with addb_create()
 * @param path name of the directory the database partitions live in
 * @param mode one of #ADDB_MODE_READ_ONLY, #ADDB_MODE_WRITE_ONLY, or
 *  	#ADDB_MODE_READ_WRITE.
 *
 * @return NULL on allocation or access error.  The global
 *	variable errno will be set to indicate the error.
 * @return otherwise, an opaque handle toa new istore database that
 * 	must be free'd with addb_istore_close().
 */

addb_istore *addb_istore_open(addb_handle *addb, char const *path, int mode,
                              addb_istore_configuration *icf) {
  size_t path_n;
  addb_istore_partition *part, *part_end;
  addb_istore *is;
  int err = 0;
  struct stat st;
  char *heap;

  cl_assert(addb->addb_cl, path != NULL);
  cl_assert(addb->addb_cl, mode != 0);

  cl_enter(addb->addb_cl, CL_LEVEL_SPEW, "(%s, %s)", path,
           mode & ADDB_MODE_WRITE ? "read-write" : "read-only");

  /* If the directory doesn't yet exist, try to create it.
   */
  if ((mode & ADDB_MODE_WRITE) && mkdir(path, 0755) == -1) {
    if (errno != EEXIST) {
      err = errno;

      cl_cover(addb->addb_cl);
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb: failed to create istore database"
             " directory \"%s\": %s [%s:%d]",
             path, strerror(err), __FILE__, __LINE__);

      cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "mkdir fails: %s", strerror(err));

      errno = err;
      return NULL;
    }
    cl_cover(addb->addb_cl);
  }

  if (stat(path, &st) != 0) {
    err = errno;

    cl_cover(addb->addb_cl);
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: can't stat istore database "
           "directory \"%s\": %s [%s:%d]",
           path, strerror(errno), __FILE__, __LINE__);
    cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "stat fails: %s", strerror(err));
    errno = err;
    return NULL;
  }
  if (!S_ISDIR(st.st_mode)) {
    cl_cover(addb->addb_cl);
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: \"%s\" exists, but is not "
           "a directory. [%s:%d]",
           path, __FILE__, __LINE__);
    cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "not a directory");
    errno = ENOTDIR;
    return NULL;
  }

  path_n = strlen(path);
  is = cm_zalloc(addb->addb_cm, sizeof(addb_istore) + 5 * (path_n + 80));
  if (is == NULL) {
    err = errno;

    cl_cover(addb->addb_cl);
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: failed to allocate %lu bytes for "
           "istore database structure for \"%s\" [%s:%d]",
           (unsigned long)(sizeof(addb_istore) + path_n + 80), path, __FILE__,
           __LINE__);
    cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "can't allocate structure: %s",
             strerror(err));

    errno = err;
    return is;
  }

  is->is_addb = addb;
  is->is_tiled_pool = NULL;

  /* Set up the generator for partition filenames.
   */
  is->is_path = (char *)(is + 1);
  memcpy(is->is_path, path, path_n);
  is->is_base = is->is_path + path_n;
  is->is_base_n = 80;
  if (is->is_base > is->is_path && is->is_base[-1] != '/') {
    cl_cover(addb->addb_cl);

    *is->is_base++ = '/';
    is->is_base_n--;
  }
  *is->is_base = '\0';
  is->is_next.ism_memory_value = 0;
  heap = is->is_base + 80;

  /* Set up the names of the temporary and permanent fill level
   * marker.
   */
  is->is_next.ism_tmp_path = heap;
  heap += path_n + 80;
  is->is_next.ism_path = heap;
  heap += path_n + 80;

  is->is_horizon.ism_tmp_path = heap;
  heap += path_n + 80;
  is->is_horizon.ism_path = heap;
  heap += path_n + 80;

  snprintf(is->is_next.ism_path, path_n + 80, "%s%s", is->is_path, "next");
  snprintf(is->is_next.ism_tmp_path, path_n + 80, "%s%s", is->is_path,
           "next.TMP");

  snprintf(is->is_horizon.ism_path, path_n + 80, "%s%s", is->is_path,
           "horizon");
  snprintf(is->is_horizon.ism_tmp_path, path_n + 80, "%s%s", is->is_path,
           "horizon.TMP");

  is->is_horizon.ism_fd = -1;
  is->is_horizon.ism_magic = ADDB_ISTORE_HORIZON_MAGIC;
  is->is_horizon.ism_name = "horizon";
  addb_file_sync_initialize(addb, &is->is_horizon.ism_write_fsc);

  is->is_next.ism_fd = -1;
  is->is_next.ism_magic = ADDB_ISTORE_NEXT_MAGIC;
  is->is_next.ism_name = "next";
  addb_file_sync_initialize(addb, &is->is_next.ism_write_fsc);

  /*  Initialize an empty partition table.
   */
  part = is->is_partition;
  part_end = is->is_partition +
             (sizeof(is->is_partition) / sizeof(is->is_partition[0]));
  for (; part < part_end; part++) addb_istore_partition_initialize(is, part);
  is->is_partition_n = 0;

  /*  Load fill level marker from disk.
   */
  err = addb_istore_marker_read(is, &is->is_next);
  if (err && err != ENOENT) {
    cm_free(addb->addb_cm, is);
    cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "can't read next marker file: %s",
             err == ERANGE ? "premature EOF" : addb_xstrerror(err));

    errno = err;
    return NULL;
  }
  err = addb_istore_marker_read(is, &is->is_horizon);
  if (err && err != ENOENT) {
    cm_free(addb->addb_cm, is);
    cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "can't read horizon file: %s",
             err == ERANGE ? "premature EOF" : addb_xstrerror(err));

    errno = err;
    return NULL;
  }

  cl_log(addb->addb_cl, CL_LEVEL_DEBUG,
         "addb_istore_open: next_id=%llu horizon=%llu",
         (unsigned long long)is->is_next.ism_memory_value,
         (unsigned long long)is->is_horizon.ism_memory_value);

  if (icf != NULL) addb_istore_configure(is, icf);

  /*  Load partitions from disk.
   */
  if ((err = addb_istore_partitions_read(is, mode)) != 0) {
    cl_cover(addb->addb_cl);
    cm_free(addb->addb_cm, is);
    cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "can't read partitions: %s",
             addb_xstrerror(err));

    errno = err;
    return NULL;
  }

  cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "is=%p", (void *)is);
  return is;
}
