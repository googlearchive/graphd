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
#ifndef ADDB_GMAP_H
#define ADDB_GMAP_H

#include "libaddb/addb-gmap-file.h"
#include "libaddb/addbp.h"

#include <stdbool.h> /* bool */
#include <stdlib.h>  /* size_t */
#include <unistd.h>  /* off_t */

#define ADDB_GMAP_BACKING_LF 1
#define ADDB_GMAP_BACKING_BGMAP 2
#define ADDB_GMAP_BACKING_GMAP 3

/*  In the index table, a single occupied in-place slot is marked by
 *  a pattern of 111111.  Anything else is an external reference.
 */
#define ADDB_GMAP_MAKE_6_34(hi, lo) \
  (((unsigned long long)((hi)&0x3F) << 34) | ((lo) & ((1ull << 34) - 1)))
#define ADDB_GMAP_LOW_34(x) ((unsigned long long)(x) & ((1ull << 34) - 1))
#define ADDB_GMAP_HIGH_6(x) (((x) >> 34) & 0x3F)

#define ADDB_GMAP_IVAL_IS_SINGLE(ival) (ADDB_GMAP_HIGH_6(ival) == 0x3F)
#define ADDB_GMAP_IVAL_IS_MULTI(ival) (ADDB_GMAP_HIGH_6(ival) < 0x3D)
#define ADDB_GMAP_IVAL_IS_FILE(ival) (ADDB_GMAP_HIGH_6(ival) == 0x3E)
#define ADDB_GMAP_IVAL_IS_BGMAP(ival) (ADDB_GMAP_HIGH_6(ival) == 0x3D)

#define ADDB_GMAP_IVAL_IS_EMPTY(x) ((x) == 0)

#define ADDB_GMAP_IVAL_SINGLE(ival) (ADDB_GMAP_LOW_34(ival))

#define ADDB_GMAP_IVAL_MAKE_BGMAP(x) ADDB_GMAP_MAKE_6_34(0x3D, (x))
#define ADDB_GMAP_IVAL_MAKE_FILE(x) ADDB_GMAP_MAKE_6_34(0x3E, (x))
#define ADDB_GMAP_IVAL_MAKE_SINGLE(x) ADDB_GMAP_MAKE_6_34(0x3F, (x))
#define ADDB_GMAP_IVAL_MAKE_MULTI_OFFSET_EXP(off, ex) \
  ADDB_GMAP_MAKE_6_34((ex),                           \
                      ((off)-ADDB_GMAP_MULTI_OFFSET) / ADDB_GMAP_MULTI_FACTOR)
#define ADDB_GMAP_IVAL_MAKE_EMPTY() (0)

#define ADDB_GMAP_IVAL_M_EXP_TO_NELEMS(ex) (1 << (ex))
#define ADDB_GMAP_IVAL_M_EXP_TO_SIZE(ex) \
  (ADDB_GMAP_IVAL_M_EXP_TO_NELEMS(ex) * ADDB_GMAP_ENTRY_SIZE)
#define ADDB_GMAP_IVAL_M_EXP(x) (ADDB_GMAP_HIGH_6(x))
#define ADDB_GMAP_IVAL_M_NELEMS(x) (1 << ADDB_GMAP_IVAL_M_EXP(x))
#define ADDB_GMAP_IVAL_M_SIZE(x) \
  (ADDB_GMAP_IVAL_M_NELEMS(x) * ADDB_GMAP_ENTRY_SIZE)

#define ADDB_GMAP_IVAL_FILE_LENGTH(x) ((x)&0xffffffffull)

#define ADDB_GMAP_MVAL_INDEX(x) ADDB_GMAP_LOW_34(x)
#define ADDB_GMAP_MVAL_S_MAKE_NELEMS(x) (ADDB_GMAP_MAKE_6_34(0x20, x))
#define ADDB_GMAP_MVAL_S_MAKE_LAST(x) (ADDB_GMAP_MAKE_6_34(0x00, x))

/*  The "sentinel" is a pointer to the last possible element in an array.
 *  It can be derived from the array's base and its size, as encoded
 *  in the IVAL that points to it.  (The size can be given as bytes,
 *  nelems, or as the 2^exponent of the nelems.)
 */
#define ADDB_GMAP_MVAL_S_IS_FULL(ival) ((((ival) >> 34) & 0x3F) == 0)
#define ADDB_GMAP_MVAL_S_NELEMS(mval) ADDB_GMAP_LOW_34(mval)

#define ADDB_GMAP_INDEX_SINGLE_VALUE(ptr)                             \
  ((unsigned long long)(((unsigned char *)(ptr))[0] & 3) << (4 * 8) | \
   (unsigned long)((unsigned char *)(ptr))[0] << (3 * 8) |            \
   (unsigned long)((unsigned char *)(ptr))[0] << (2 * 8) |            \
   (unsigned int)((unsigned char *)(ptr))[0] << (1 * 8) |             \
   ((unsigned char *)(ptr))[0])

/* Store at most 16 gig structures in 1024 partitions, with 16 meg small
 *  structures (ADDB_GMAP_INDEX_N) per partition.
 */
#define ADDB_GMAP_PARTITIONS_MAX 1024
#define ADDB_GMAP_INDEX_N (64ul * 1024 * 1024)

struct addb_gmap_partition;
struct addb_tiled;
struct addb_tiled_pool;

/**
 * @brief A GMAP table is stored as up to 1024 partitions; each
 * 	partition corresponds to a single file.
 */
typedef struct addb_gmap_partition {
  /**
   * @brief The table that this partitionis part of.
   */
  addb_gmap *part_gm;

  /**
   * @brief Malloc'ed copy of the specific database file's name,
   *	for logging.
   */
  char *part_path;

  /**
   * @brief The tile manager for the file; shares a tile pool
   *  	with its siblings.
   *
   *  If a partition hasn't yet been opened or doesn't exist,
   *  this pointer is NULL.
   */
  struct addb_tiled *part_td;

  /**
   * @brief The virtual file size.  When appending, data is
   * 	written after this offset, and it is incremented.
   *
   *  The actual underlying file storage is allocated in
   *  page size increments.
   */
  unsigned long long part_size;

} addb_gmap_partition;

/**
 * @brief A GMAP table; exported as the opaque addb_gmap pointer from addb.h.
 */
struct addb_gmap {
  /**
   * @brief Pointer to the overall database that this map is part of.
   */
  addb_handle *gm_addb;

  /**
   * @brief Configuration data.
   */
  addb_gmap_configuration gm_cf;

  /**
   * @brief Filename of the partition directory
   */
  char *gm_path;

  /**
   * @brief Basename.
   *
   * Partition filenames are generated by appending numbers to
   * gm_base at gm_base_n.
   */
  char *gm_base;

  /**
   * @brief Length of the basename, in bytes.
   */
  size_t gm_base_n;

  /**
   * @brief Index of first unoccupied partition with no higher
   *  occupied partition.
   */
  size_t gm_partition_n;

  /**
   * @brief Partitions of this GMAP; can be unoccupied.
   */
  addb_gmap_partition gm_partition[1024];

  /**
   * @brief Tiled pool shared by all partitions.
   */
  struct addb_tiled_pool *gm_tiled_pool;

  /**
   * @brief The last time the GMAP index was in sync with the
   * 	istore, the istore was in this consistent state.
   *
   *  	This is the state the GMAP would go back to if it used
   * 	its backup and forgot the changes made in temporarily
   * 	allocated memory tiles overlapping file tiles.
   */
  unsigned long long gm_horizon;

  struct addb_largefile_handle *gm_lfhandle;

  /**
   * @brief Is this GMAP backed up?
   */
  unsigned int gm_backup : 1;

  struct addb_bgmap_handle *gm_bgmap_handle;

  /*
   * Should this gmap create bitmaps for things that are dense enough
   * for bitmaps to warrant a space savings?
   */
  bool gm_bitmap;

  /*
   * Async context for syncing this gmap directory (not the files)
   */
  addb_fsync_ctx gm_dir_fsync_ctx;

  /*
   * File descriptor to the directory for use with above
   */
  int gm_dir_fd;
};

/* addb-gmap-freelist.c */

int addb_gmap_freelist_alloc(addb_gmap_partition *_part, size_t _ex,
                             unsigned long long *_offset_out);

int addb_gmap_freelist_free(addb_gmap_partition *_part, unsigned long long _off,
                            size_t _ex);

struct addb_largefile;
/* addb-gmap-partition.c */

void addb_gmap_partition_initialize(addb_gmap *_gm, addb_gmap_partition *_part);

int addb_gmap_partition_finish(addb_gmap_partition *);

int addb_gmap_partition_grow(addb_gmap_partition *, off_t);
void addb_gmap_partition_basename(addb_handle *_addb, size_t _i, char *_buf,
                                  size_t _bufsize);

int addb_gmap_partition_name(addb_gmap_partition *_part, size_t _i);

int addb_gmap_partition_open(addb_gmap_partition *_gm, int _mode);
int addb_gmap_partitions_read(addb_gmap *, int);

#if 0
int 		  addb_gmap_partition_last(
			addb_gmap_partition	* _part,
			addb_gmap_id 		  _id,
			addb_gmap_id		* _val_out);
#endif

int addb_gmap_partition_get(addb_gmap_partition *part,
                            unsigned long long offset, unsigned long long *out);

int addb_gmap_partition_get_chunk(addb_gmap_partition *part,
                                  unsigned long long offset_s,
                                  char const **data_s_out,
                                  char const **data_e_out,
                                  addb_tiled_reference *tref_out);

int addb_gmap_partition_put(addb_gmap_partition *part,
                            unsigned long long offset, unsigned long long val);

int addb_gmap_partition_mem_to_file(addb_gmap_partition *_part,
                                    unsigned long long _offset,
                                    char const *_source, size_t _n);

int addb_gmap_partition_copy(addb_gmap_partition *_part,
                             unsigned long long _destination,
                             unsigned long long _source, unsigned long long _n);

int addb_gmap_partition_data(addb_gmap_partition *_part, addb_gmap_id _id,
                             unsigned long long *_offset_out,
                             unsigned long long *_n_out,
                             unsigned long long *_hint_out);

int addb_gmap_partition_read_raw_loc(addb_gmap_partition *_part,
                                     unsigned long long _offset,
                                     unsigned long long _end,
                                     unsigned char const **_ptr_out,
                                     unsigned long long *_end_out,
                                     addb_tiled_reference *_tref,
                                     char const *_file, int _line);
#define addb_gmap_partition_read_raw(a, b, c, d, e, f) \
  addb_gmap_partition_read_raw_loc(a, b, c, d, e, f, __FILE__, __LINE__)

addb_gmap_partition *addb_gmap_partition_by_id(addb_gmap *_gm,
                                               addb_gmap_id _id);

int addb_gmap_largefile_size_get(void *cookie, unsigned long long id,
                                 size_t *size);

int addb_gmap_largefile_size_set(void *cookie, unsigned long long id,
                                 size_t size);

int addb_gmap_backing(addb_gmap *gm, addb_gmap_id id, int *out);
#endif /* ADDBP_GMAP_H */
