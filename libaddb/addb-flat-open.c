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
#include "libaddb/addb-flat.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/*  FLAT: A fixed-size, small, chunk of storage.
 */

/**
 * @brief Create or open a "flat" database.
 * @param addb 	opaque database handle for the container
 * @param path 	pathname of the database file.
 * @param mode 	one of ADDB_MODE_READ_ONLY, ADDB_MODE_WRITE_ONLY,
 *  		or ADDB_MODE_READ_WRITE
 * @param data if non-NULL, when initially creating the database,
 *		initialize it with this - does not include the
 *		magic number, which is automatically prefixed.
 * @param size number of bytes pointed to by data
 */

addb_flat *addb_flat_open(addb_handle *addb, char const *path, int mode,
                          char const *data, size_t size) {
  size_t path_n, mem_size, page_size;
  void *mem;
  addb_flat *fl;
  int err = 0;
  struct stat st;
  int fd;

  cl_assert(addb->addb_cl, path != NULL);
  cl_assert(addb->addb_cl, mode != 0);

  fd = open(path, mode & ADDB_MODE_WRITE ? (O_RDWR | O_CREAT) : O_RDONLY, 0666);
  if (fd == -1) {
    err = errno;
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: failed to open flat database"
           " file \"%s\": %s [%s:%d]",
           path, strerror(errno), __FILE__, __LINE__);
    cl_cover(addb->addb_cl);
    errno = err;
    return NULL;
  }
  err = addb_file_fstat(addb->addb_cl, fd, path, &st);
  if (err != 0) {
    (void)close(fd);
    errno = err;

    return NULL;
  }

  if (st.st_size == 0) {
    if (!(mode & ADDB_MODE_WRITE)) {
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb: expected read-only flat database "
             "file \"%s\" to be of size %llu; "
             "seeing only %llu [%s:%d]",
             path, (unsigned long long)size + ADDB_FLAT_HEADER_SIZE,
             (unsigned long long)st.st_size, __FILE__, __LINE__);
      cl_cover(addb->addb_cl);
      (void)close(fd);
      return NULL;
    }

    /* OK, write it. */

    err = addb_file_write(addb, fd, path, ADDB_FLAT_MAGIC, 4);
    if (err) {
      errno = err;
      (void)close(fd);
      return NULL;
    }
    err = (data != NULL) ? addb_file_write(addb, fd, path, data, size)
                         : addb_file_grow(addb->addb_cl, fd, path,
                                          size + ADDB_FLAT_HEADER_SIZE);
    if (err) {
      errno = err;
      (void)close(fd);
      return NULL;
    }
    st.st_size = ADDB_FLAT_HEADER_SIZE + size;
  }

  /*  Map it in.
   */
  page_size = getpagesize();
  mem_size = addb_round_up(st.st_size, page_size);
  mem = mmap(0, mem_size,
             mode == ADDB_MODE_READ_ONLY ? PROT_READ : PROT_READ | PROT_WRITE,
             MAP_SHARED, fd, 0);
  if (mem == MAP_FAILED) {
    err = errno ? errno : -1;
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: mmap of "
           "%llu bytes from \"%s\" failed: %s [%s:%d]",
           (unsigned long long)mem_size, path, strerror(errno), __FILE__,
           __LINE__);
    errno = err;
    (void)close(fd);
    return NULL;
  }

  /* Is the magic number correct?
   */
  if (memcmp(mem, ADDB_FLAT_MAGIC, ADDB_MAGIC_SIZE) != 0) {
    unsigned char const *mag = mem;

    cl_cover(addb->addb_cl);

    err = errno ? errno : -1;
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: \"%s\" "
           "doesn't appear to be a flat database; expected "
           "magic number \"%s\", but it starts with "
           "%2.2x %2.2x %2.2x %2.2x [%s:%d]",
           path, ADDB_FLAT_MAGIC, mag[0], mag[1], mag[2], mag[3], __FILE__,
           __LINE__);
    errno = EINVAL;

    if (munmap(mem, mem_size) < 0)
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb %s[%d]: munmap failed(%s), leaked %p!", __FILE__, __LINE__,
             strerror(errno), mem);
    (void)close(fd);

    return NULL;
  }

  path_n = strlen(path) + 1;
  fl = cm_malloc(addb->addb_cm, sizeof(addb_flat) + path_n);
  if (fl == NULL) {
    err = errno;
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: failed to allocate %lu bytes for "
           "flat database structure for \"%s\" [%s:%d]",
           (unsigned long)(sizeof(addb_flat) + path_n + 80), path, __FILE__,
           __LINE__);
    cl_cover(addb->addb_cl);

    if (munmap(mem, mem_size) < 0)
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb %s[%d]: munmap failed(%s), leaked %p!", __FILE__, __LINE__,
             strerror(errno), mem);
    errno = err;
    return NULL;
  }

  fl->fl_addb = addb;
  fl->fl_fd = fd;
  fl->fl_memory = mem;
  fl->fl_memory_size = mem_size;
  fl->fl_path = (char *)(fl + 1);
  memcpy(fl->fl_path, path, path_n);

  cl_cover(addb->addb_cl);

  return fl;
}
