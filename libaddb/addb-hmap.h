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
#ifndef ADDB_HMAP_H
#define ADDB_HMAP_H

#include "libaddb/addb.h"
#include "libaddb/addb-hmap-file.h"

#include <stdio.h>

struct addb_hmap {
  addb_handle* hmap_addb;
  addb_hmap_configuration hmap_cf;
  char* hmap_dir_path;
  char* hmap_file_path;
  addb_tiled* hmap_td;
  unsigned long long hmap_horizon;
  int hmap_backup;              /* is this hmap backed up */
  size_t hmap_bucket_page_size; /* size of a bucket page */
  unsigned long long
      hmap_n_slots; /* number of slots (bucket pages) in the table */
  unsigned long long hmap_first_slot_offset; /* offset of first slot from BOF */
  size_t hmap_tile_size;
  struct addb_tiled_pool* hmap_tiled_pool;
  char* hmap_gm_path;
  addb_gmap* hmap_gm;

  /*
   * File descriptor fort he directory this hmap lives in
   */
  int hmap_dir_fd;
  /*
   * Aync context for syncing the directory (not its files)
   */
  addb_fsync_ctx hmap_dir_fsync_ctx;
};

#define ADDB_HMAP_TYPE_VALID(t) ((t) >= addb_hmt_name && (t) < addb_hmt_LAST)
#define ADDB_HMAP_TYPE_KEY_IS_STRING(t) \
  ((t) >= addb_hmt_name && (t) < addb_hmt_word)

void addb_hmap_slot_dump(addb_hmap* hm, unsigned long long hash_of_key,
                         FILE* f);

#endif
