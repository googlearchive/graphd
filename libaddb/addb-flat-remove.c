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
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/**
 * @brief Remove a flat database.
 *
 *  This removes the file in the file system that backs
 *  a flat database.
 *
 *  The call only succeeds if the database exists and has
 *  the right magic number in its header - to unconditionally
 *  remove a file, just use unlink() or remove().
 *
 * @param addb	the containing database.
 * @param path	pathname of the flat database file.
 * @return 0 on success.
 * @return EINVAL if the file didn't exist, wasn't accessible,
 * 	or had the wrong format.
 */
int addb_flat_remove(addb_handle *addb, char const *path) {
  int fd;
  int err;
  char magic[4];

  /* before we unlink, make sure this is actually
   * a flat database file.
   */
  if ((fd = open(path, O_RDONLY)) == -1) {
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: failed to open flat database"
           " file \"%s\": %s [%s:%d]",
           path, strerror(errno), __FILE__, __LINE__);
    cl_cover(addb->addb_cl);

    return EINVAL;
  }
  err = addb_file_read(addb, fd, path, magic, sizeof magic, false);
  close(fd);

  if (err != 0) return err;
  if (memcmp(magic, ADDB_FLAT_MAGIC, ADDB_MAGIC_SIZE) != 0) {
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: alleged flat database"
           " file \"%s\" doesn't look like a flat "
           "database file; starts with %2.2x %2.2x %2.2x %2.2x "
           "(not removed) [%s:%d]",
           path, (unsigned char)magic[0], (unsigned char)magic[1],
           (unsigned char)magic[2], (unsigned char)magic[3], __FILE__,
           __LINE__);
    cl_cover(addb->addb_cl);
    return EINVAL;
  }

  return unlink(path);
}
