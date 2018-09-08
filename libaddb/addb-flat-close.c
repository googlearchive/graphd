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
#include "libaddb/addb-flat.h"
#include "libaddb/addbp.h"

#include <sys/mman.h>
#include <unistd.h>

/**
 * @brief Free resources associated with a flat file database.
 *
 *  It is safe, and does nothing, to invoke addb_flat_close
 *  with a NULL pointer.
 *
 * @param fl 	opaque database pointer
 * @return 0 on success, a nonzero error code on error.
 */
int addb_flat_close(addb_flat *fl) {
  int result = 0, err;
  if (fl != NULL) {
    if (fl->fl_memory != NULL && fl->fl_memory != MAP_FAILED) {
      cl_cover(fl->fl_addb->addb_cl);
      err = addb_file_munmap(fl->fl_addb->addb_cl, fl->fl_path, fl->fl_memory,
                             fl->fl_memory_size);
      if (err != 0 && result == 0) result = err;
    }
    if (fl->fl_fd != -1) {
      cl_cover(fl->fl_addb->addb_cl);
      err = addb_file_close(fl->fl_addb, fl->fl_fd, fl->fl_path);
      fl->fl_fd = -1;
      if (err != 0 && result == 0) result = err;
    }
    cm_free(fl->fl_addb->addb_cm, fl);
  }
  return result;
}
