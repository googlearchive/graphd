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
#include "libaddb/addb.h"
#include "libaddb/addb-smap.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

static int addb_smap_status_partition(addb_smap_partition* part,
                                      cm_prefix const* prefix,
                                      addb_status_callback* cb, void* cb_data) {
  char num_buf[42];
  int err;

  err = (*cb)(cb_data, cm_prefix_end(prefix, "path"), part->part_path);
  if (err) return err;

  snprintf(num_buf, sizeof num_buf, "%llu", part->part_size);
  err = (*cb)(cb_data, cm_prefix_end(prefix, "size"), num_buf);
  if (err) return err;

  return addb_tiled_status(part->part_td, prefix, cb, cb_data);
}

/**
 * @brief Report on the state of a smap database.
 * @param sm		database handle, created with addb_smap_open()
 * @param callback	call this with each name/value pair
 * @param callback_data	opaque application pointer passed to the callback
 * @result 0 on success, a nonzero error number on error.
 */
int addb_smap_status(addb_smap* sm, cm_prefix const* prefix,
                     addb_status_callback* cb, void* cb_data) {
  int err;
  size_t part_i;
  addb_smap_partition* part;
  cm_prefix smap_pre, part_pre;

  if (!sm) return EINVAL;

  smap_pre = cm_prefix_pushf(prefix, "smap");

  for (part_i = 0, part = sm->sm_partition; part_i < ADDB_GMAP_PARTITIONS_MAX;
       part_i++, part++) {
    if (part->part_path == NULL) continue;

    part_pre = cm_prefix_pushf(&smap_pre, "partition.%d", (int)part_i);
    err = addb_smap_status_partition(part, &part_pre, cb, cb_data);
    if (err) return err;
  }

  return 0;
}

/**
 * @brief Report on the state of a smap database.
 * @param sm		database handle, created with addb_smap_open()
 * @param callback	call this with each name/value pair
 * @param callback_data	opaque application pointer passed to the callback
 * @result 0 on success, a nonzero error number on error.
 */
int addb_smap_status_tiles(addb_smap* sm, cm_prefix const* prefix,
                           addb_status_callback* cb, void* cb_data) {
  int err;
  size_t part_i;
  addb_smap_partition* part;
  cm_prefix smap_pre, part_pre;

  if (!sm) return EINVAL;

  smap_pre = cm_prefix_pushf(prefix, "smap");

  for (part_i = 0, part = sm->sm_partition; part_i < ADDB_SMAP_PARTITIONS_MAX;
       part_i++, part++) {
    if (part->part_path == NULL) continue;

    part_pre = cm_prefix_pushf(&smap_pre, "partition.%d", (int)part_i);
    err = addb_tiled_status_tiles(part->part_td, &part_pre, cb, cb_data);
    if (err) return err;
  }

  return 0;
}
