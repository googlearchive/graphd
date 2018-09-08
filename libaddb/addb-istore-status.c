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

#include <stdio.h>
#include <string.h>

static int addb_istore_status_partition(addb_istore* is,
                                        addb_istore_partition* part,
                                        cm_prefix const* pre,
                                        addb_status_callback* cb,
                                        void* cb_data) {
  int err;
  char num_buf[42];

  cl_assert(is->is_addb->addb_cl, part->ipart_td);

  err = (*cb)(cb_data, cm_prefix_end(pre, "path"), part->ipart_path);
  if (err) return err;

  snprintf(num_buf, sizeof num_buf, "%llu",
           (unsigned long long)part->ipart_size);
  err = (*cb)(cb_data, cm_prefix_end(pre, "size"), num_buf);
  if (err) return err;

  return addb_tiled_status(part->ipart_td, pre, cb, cb_data);
}

/**
 * @brief Report on the state of an istore database.
 * @param is		database handle, created with addb_istore_open()
 * @param prefix	construct a hierarchical prefix in here.
 * @param cb		call this with each name/value pair
 * @param cb_data	opaque application pointer passed to the callback
 * @result 0 on success, a nonzero error number on error.
 */
int addb_istore_status(addb_istore* is, cm_prefix const* prefix,
                       addb_status_callback* cb, void* cb_data) {
  int err;
  char num_buf[42];
  size_t part_i;
  addb_istore_partition* part;
  cm_prefix i_pre;

  i_pre = cm_prefix_push(prefix, "istore");

  snprintf(num_buf, sizeof num_buf, "%llu",
           (unsigned long long)is->is_next.ism_memory_value);
  err = (*cb)(cb_data, cm_prefix_end(&i_pre, "n"), num_buf);
  if (err) return err;

  snprintf(num_buf, sizeof num_buf, "%llu",
           (unsigned long long)is->is_horizon.ism_memory_value);
  err = (*cb)(cb_data, cm_prefix_end(&i_pre, "horizon"), num_buf);
  if (err) return err;

  for (part_i = 0, part = is->is_partition; part_i < ADDB_GMAP_PARTITIONS_MAX;
       part_i++, part++) {
    cm_prefix part_pre;

    if (!part->ipart_path || !part->ipart_td) continue;

    part_pre = cm_prefix_pushf(&i_pre, "partition.%d", (int)part_i);
    err = addb_istore_status_partition(is, part, &part_pre, cb, cb_data);
    if (err) return err;

    cl_cover(is->is_addb->addb_cl);
  }
  return 0;
}

/**
 * @brief Report on the state of an istore database.
 *
 * @param is		database handle, created with addb_istore_open()
 * @param prefix	construct a hierarchical prefix in here.
 * @param cb		call this with each name/value pair
 * @param cb_data	opaque application pointer passed to the callback
 *
 * @result 0 on success, a nonzero error number on error.
 */
int addb_istore_status_tiles(addb_istore* is, cm_prefix const* prefix,
                             addb_status_callback* cb, void* cb_data) {
  int err;
  size_t part_i;
  addb_istore_partition* part;
  cm_prefix i_pre;

  i_pre = cm_prefix_push(prefix, "istore");

  for (part_i = 0, part = is->is_partition; part_i < ADDB_GMAP_PARTITIONS_MAX;
       part_i++, part++) {
    cm_prefix part_pre;

    if (!part->ipart_path || !part->ipart_td) continue;

    part_pre = cm_prefix_pushf(&i_pre, "partition.%d", (int)part_i);
    err = addb_tiled_status_tiles(part->ipart_td, &part_pre, cb, cb_data);
    if (err) return err;

    cl_cover(is->is_addb->addb_cl);
  }
  return 0;
}
