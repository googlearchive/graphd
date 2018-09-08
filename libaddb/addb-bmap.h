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
#ifndef ADDB_BMAP_H
#define ADDB_BMAP_H

#include "libaddb/addb.h"

#include "libcm/cm.h"

#define ADDB_BMAP_MAGIC "abv1"

#define ADDB_BMAP_MAGIC_OFFSET 0
#define ADDB_BMAP_MAGIC_LEN 4
#define ADDB_BMAP_SIZE_OFFSET 4
#define ADDB_BMAP_SIZE_LEN 8

#define ADDB_BMAP_HEADER 16

struct addb_bmap {
  /* The addb handle we are part of */
  struct addb_handle *bmap_addb;

  /* The tiled pool this bmap is part of. */
  struct addb_tiled_pool *bmap_tdp;
  cl_handle *bmap_cl;
  cm_handle *bmap_cm;

  /* The path to the bmap file */
  char *bmap_path;

  /* THe tiled accessor to the file */
  addb_tiled *bmap_tiled;

  /* Highest bit in the file
   */
  unsigned long long bmap_bits;

  /* What is our horizon */
  unsigned long long bmap_horizon;

  /*
   * Set if this bmap only appends to the end
   */
  bool bmap_linear;
};

int addb_bmap_check_and_set(addb_bmap *bmap, unsigned long long bit,
                            bool *value);

int addb_bmap_open(addb_handle *addb, const char *path, unsigned long long size,
                   unsigned long long horizon, bool linear, addb_bmap **out);

int addb_bmap_check(addb_bmap *bmap, unsigned long long bit, bool *value);

int addb_bmap_set(addb_bmap *bmap, unsigned long long bit);

int addb_bmap_scan(addb_bmap *bmap, unsigned long long start,
                   unsigned long long end, unsigned long long *result,
                   bool forward);

int addb_bmap_close(addb_bmap *bmap);
int addb_bmap_truncate(addb_bmap *bmap);
int addb_bmap_status(addb_bmap *bmap, cm_prefix const *prefix,
                     addb_status_callback *callback, void *cb_data);

int addb_bmap_status_tiles(addb_bmap *bmap, cm_prefix const *prefix,
                           addb_status_callback *callback, void *cb_data);
unsigned long long addb_bmap_horizon(addb_bmap *bmap);
void addb_bmap_horizon_set(addb_bmap *bmap, unsigned long long h);

int addb_bmap_checkpoint_sync_backup(addb_bmap *bmap, bool hard_sync,
                                     bool block);

int addb_bmap_checkpoint_finish_backup(addb_bmap *bmap, bool hard_sync,
                                       bool block);

int addb_bmap_checkpoint_remove_backup(addb_bmap *bmap, bool hard_sync,
                                       bool block);

int addb_bmap_checkpoint_start_writes(addb_bmap *bmap, bool hard_sync,
                                      bool block);

int addb_bmap_checkpoint_finish_writes(addb_bmap *bmap, bool hard_sync,
                                       bool block);

int addb_bmap_checkpoint_rollback(addb_bmap *bmap);

int addb_bmap_refresh(addb_bmap *bmap, unsigned long long max);
#endif
