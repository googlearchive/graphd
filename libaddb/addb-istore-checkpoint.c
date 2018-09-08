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

#include "libcm/cm.h"

/**
 * @brief Checkpoint an istore.
 *
 *  Atomically increment the on-disk horizon to the current
 *  in-memory state.
 *
 * @param is 		opaque istore module handle
 * @param sync		should we schedule a sync to disk?
 * @param block		should we wait until the sync finishes?
 * @return 0 on success, ADDB_ERR_MORE if the sync started but didn't finish
 * (yet).
 *
 */
int addb_istore_checkpoint(addb_istore* is, int sync, int block) {
  addb_handle* addb;
  addb_istore_partition *part, *part_end;
  int err = 0;
  bool all_writes_finished = true;

  if (is == NULL) return 0;

  addb = is->is_addb;
  cl_enter(addb->addb_cl, CL_LEVEL_VERBOSE, "enter");

  part = is->is_partition;
  part_end = is->is_partition + is->is_partition_n;

  /*  Synchronize partitions, newest first;
   */
  while (part_end > part) {
    part_end--;

    /*  If the highest possible ID in this partition is
     *  <= the highest ID we know to be on disk by now,
     *  stop syncing - we're not going to tell the system
     *  anything new.
     */
    if (((unsigned long long)(1 + (part_end - is->is_partition)) *
         ADDB_ISTORE_INDEX_N) <= is->is_next.ism_writing_value)

      break;

    if (part_end->ipart_td != NULL) {
      err = addb_tiled_checkpoint_write(part_end->ipart_td, sync, block);
      if (err && err != ADDB_ERR_ALREADY && err != ADDB_ERR_MORE) {
        cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR,
                     "addb_tiled_checkpoint_write", err,
                     "unexpected write error");
        cl_leave(addb->addb_cl, CL_LEVEL_VERBOSE, "%s", addb_xstrerror(err));
        return err;
      }

      if (block) cl_assert(addb->addb_cl, err != ADDB_ERR_MORE);

      if (err == ADDB_ERR_MORE) all_writes_finished = false;
    }
  }

  if (!all_writes_finished) return ADDB_ERR_MORE;

  /*  Since all the updates have gone well, atomically update the
   *  marker that tells us what our highest id is.
   */
  err = addb_istore_marker_checkpoint(is, &is->is_next, sync || block);
  cl_leave(addb->addb_cl, CL_LEVEL_VERBOSE, "%s",
           err ? addb_xstrerror(err) : "ok");
  if (err == ADDB_ERR_ALREADY) err = 0;
  return err;
}

/**
 * @brief Roll back to the previous checkpoint.
 *
 * @param is opaque istore module handle
 * @param horizon go back to this checkpoint.
 *
 * @return 0 on success, otherwise an error code.
 */
int addb_istore_checkpoint_rollback(addb_istore* is,
                                    unsigned long long horizon) {
  cl_handle* cl = is->is_addb->addb_cl;
  size_t part_start = horizon / ADDB_ISTORE_INDEX_N;
  size_t part_end = is->is_next.ism_memory_value / ADDB_ISTORE_INDEX_N;
  size_t i;
  int err = 0;
  int e;

  if (is->is_next.ism_memory_value == horizon) return 0;

  if (!is->is_addb->addb_transactional) {
    cl_log(cl, CL_LEVEL_FATAL,
           "cannot rollback: transactional support was disabled "
           "when graphd was started!");
    return ENOTSUP;
  }

  cl_enter(cl, CL_LEVEL_SPEW, "next_id = %llu, horizon=%llu",
           (unsigned long long)is->is_next.ism_memory_value, horizon);

  for (i = part_start; i <= part_end; i++) {
    e = addb_istore_partition_rollback(is, &is->is_partition[i], horizon);
    if (e) {
      if (!err) err = e;
      cl_log_errno(cl, CL_LEVEL_ERROR, "addb_istore_checkpoint_rollback", e,
                   "Unable to rollback partition: %s",
                   is->is_partition[i].ipart_path);
    }
  }

  is->is_partition_n = part_start + 1;
  is->is_next.ism_memory_value = horizon;

  cl_leave(cl, CL_LEVEL_SPEW, "rolled back partitions %zu through %zu",
           part_start, part_end);

  return err;
}
