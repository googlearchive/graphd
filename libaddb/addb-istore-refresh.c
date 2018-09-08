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
#include "libaddb/addb-istore.h"
#include "libaddb/addbp.h"

#include "libcl/cl.h"

int addb_istore_refresh(addb_istore* is, unsigned long long pdb_n) {
  size_t i;
  int err;
  cl_handle* cl;

  cl = is->is_addb->addb_cl;

  addb_istore_partition* ip;
  cl_assert(cl, is->is_partition_n <= 1024);

  /*
   * The last partition may have grown. Stretch it.
   */
  if (is->is_partition_n) {
    ip = is->is_partition + is->is_partition_n - 1;

    cl_log(cl, CL_LEVEL_VERBOSE, "stretching istore partition %i",
           (int)(is->is_partition_n - 1));

    err = addb_tiled_stretch(ip->ipart_td);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_tiled_stretch", err,
                   "last istore partition");
      return err;
    }

    /*
     * XXX ipart_size is not updated, but I don't think we
     * use it for anything...
     */

    /* XXX   Yeah, we're using it.  Update it!
     */
  }

  /*
   * New partitions may have been added at or after is_partition_n.
   *
   * Integer division of pdb_n / ADDB_ISTORE_INDEX_N gives the number of
   * -filled- partitions, not open partitions.
   *
   * This is a problem when we fill a partition. In that case, the number of
   * open partitions is equal to the number of full ones.
   *
   * We want this to refresh when we begin writing into a new partition.
   * This means that we do so when our id_max fills a partition (pdb_n is a full
   * parititon plus one). Otherwise, the new partition will not exist and we
   * will crash.
   */

  unsigned long long id_max = pdb_n - 1;

  for (i = is->is_partition_n; i <= id_max / ADDB_ISTORE_INDEX_N; i++) {
    ip = is->is_partition + i;
    cl_assert(cl, ip->ipart_td == NULL);
    cl_assert(cl, ip->ipart_path == NULL);
    cl_assert(cl, i < 1024);
    addb_istore_partition_name(is, ip, i);

    cl_log(cl, CL_LEVEL_VERBOSE, "trying to open istore partition %s",
           ip->ipart_path);

    err = addb_istore_partition_open(is, ip, ADDB_MODE_READ_ONLY);
    if (err) {
      cl_log_errno(is->is_addb->addb_cl, CL_LEVEL_ERROR,
                   "addb_istore_partition_open", err,
                   "Could not open partition %s for id %llu that "
                   "should now exist.",
                   ip->ipart_path, (unsigned long long)pdb_n);

      addb_istore_partition_finish(is, ip);
      return err;
    }
    is->is_partition_n = i + 1;
  }

  return 0;
}
