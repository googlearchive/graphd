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
#ifndef ADDB_ISTORE_H
#define ADDB_ISTORE_H

#include "libaddb/addb-istore-file.h"
#include "libaddb/addbp.h"

#include <stdlib.h> /* size_t */
#include <unistd.h> /* off_t */

/* Store at most 16 gig structures in 1024 partitions, with 16 meg small
 *  structures (ADDB_ISTORE_INDEX_N) per partition.
 */
#define ADDB_ISTORE_PARTITIONS_MAX 1024
#define ADDB_ISTORE_INDEX_MAX \
  (((unsigned long long)ADDB_ISTORE_INDEX_N * ADDB_ISTORE_PARTITIONS_MAX) - 1)

/*  Tiles are addressed relative to ADDB_ISTORE_INDEX_BASE, the
 *  same base used for the index units.  (Defined in addb-istore-file.h.)
 */
#define ADDB_ISTORE_TILE_FROM_BYTES(off) \
  (((off)-ADDB_ISTORE_INDEX_BASE) / ADDB_ISTORE_TILE_SIZE)

#define ADDB_ISTORE_TILE_TO_BYTES(tile) \
  (ADDB_ISTORE_INDEX_BASE + ((tile)*ADDB_ISTORE_TILE_SIZE))

/*  By default, we keep this many cached tiles around in the system
 *  (Each of them uses 64k+)
 */
#define ADDB_ISTORE_TILE_CACHED_MAX 256

/**
 * @brief A single istore partition; private to the istore implementation.
 */
typedef struct addb_istore_partition {
  /**
   * @brief Malloc'ed copy of the specific database file's name,
   * 	for logging.
   */
  char *ipart_path;

  /**
   * @brief Tile cache for file access without mapping all of it
   * 	into memory.
   */
  addb_tiled *ipart_td;

  /**
   * @brief Virtual file size.
   *
   * Cached from file header
   */
  off_t ipart_size;

} addb_istore_partition;

struct addb_istore_marker {
  /* @brief For error messages - which one is this?
   */
  char const *ism_name;

  /**
   * @brief 4-byte magic number in the file.
   */
  char const *ism_magic;

  /**
   * @brief The current value in the database.
   */
  addb_istore_id ism_memory_value;

  /**
   * @brief The current value being written to the file.
   */
  addb_istore_id ism_writing_value;

  /**
   * @brief Pathname of a temporary marker file that is later renamed
   * 	to the marker file.
   */
  char *ism_tmp_path;

  /**
   * @brief Pathname of a marker file.
   */
  char *ism_path;

  /**
   * @brief The cached istore marker file descriptor.
   */
  int ism_fd;

  /**
   * @brief How many times can we append to the marker file
   * 	before truncating it again?
   */
  int ism_n_appends;

  /**
   * @brief Callback to call to wait for a marker write to complete.
   */
  int (*ism_write_finish)(addb_istore *, addb_istore_marker *, bool);

  /**
   * @brief Asynchronous I/O buffer for the marker write.
   */
  addb_fsync_ctx ism_write_fsc;
};

/**
 * @brief  An istore table; exported as the opaque addb_istore pointer.
 */
struct addb_istore {
  /**
   * @brief Opaque database pointer for the containing databse.
   */
  addb_handle *is_addb;

  /**
   * @brief User-supplied configuration.
   */
  addb_istore_configuration is_cf;

  /**
   * @brief Pathname of the containing directory.
   */
  char *is_path;

  /**
   * @brief Buffer used to create filenames for the
   *  istore partition files.
   */
  char *is_base;

  /**
   * @brief Number of bytes before the varying part of the
   *  	istore partition file naems.
   */
  size_t is_base_n;

  addb_istore_marker is_horizon;
  addb_istore_marker is_next;

  /**
   * @brief tile pool shared by the individual partitions.
   */
  addb_tiled_pool *is_tiled_pool;

  /**
   * @brief Number of allocated partitions, <= 1024.
   */
  size_t is_partition_n;

  /**
   * @brief Space for partitions.  Only the first is_partition_n
   * 	are actually valid.
   */
  addb_istore_partition is_partition[1024];
};

/* addb-istore-partition.c */

void addb_istore_partition_initialize(addb_istore *_is,
                                      addb_istore_partition *_part);

int addb_istore_partition_finish(addb_istore *_is,
                                 addb_istore_partition *_part);

int addb_istore_partition_open(addb_istore *_is, addb_istore_partition *_part,
                               int _mode);

int addb_istore_partitions_read(addb_istore *, int);

int addb_istore_partition_data_loc(addb_istore *_is,
                                   addb_istore_partition *_part,
                                   uint_fast64_t _id, addb_data *_data,
                                   char const *_file, int _line);

void addb_istore_partition_basename(addb_handle *_addb, size_t _i, char *_buf,
                                    size_t _bufsize);

int addb_istore_partition_name(addb_istore *_is, addb_istore_partition *_part,
                               size_t _i);

int addb_istore_partition_next_id_set(addb_istore *_is,
                                      addb_istore_partition *_part,
                                      addb_istore_id _val);

int addb_istore_partition_rollback(addb_istore *_is,
                                   addb_istore_partition *_part,
                                   unsigned long long _horizon);

/* addb-istore-index.c */

int addb_istore_index_get(addb_istore *_is, addb_istore_partition *_part,
                          addb_istore_id _id, off_t *_out);

int addb_istore_index_set(addb_istore *_is, addb_istore_partition *_part,
                          addb_istore_id _id, off_t _val);

int addb_istore_index_boundaries_get(addb_istore *_is,
                                     addb_istore_partition *_part,
                                     addb_istore_id _id, off_t *_start_out,
                                     off_t *_end_out);

/* addb-istore-marker.c */

int addb_istore_marker_write_start(addb_istore *_is, addb_istore_marker *_ism,
                                   bool _hard_sync);

int addb_istore_marker_write_finish(addb_istore *_is, addb_istore_marker *_ism,
                                    bool _block);

int addb_istore_marker_checkpoint(addb_istore *_is, addb_istore_marker *_ism,
                                  bool _hard_sync);

int addb_istore_marker_read(addb_istore *_is, addb_istore_marker *_ism);

#endif /* ADDBP_ISTORE_H */
