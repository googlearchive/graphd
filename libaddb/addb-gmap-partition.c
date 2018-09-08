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
#include "libaddb/addb-gmap.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static char const addb_gmap_partition_alphabet32[32] =
    "0123456789ABCDEFGHIJKLMNOPQRSTUV";

/**
 * @brief Initialize a partition slot.
 *
 *  This happens at start-up.  An initialized slot just has an
 *  initialized, empty, data value.
 *
 * @param gm database table that the partition is for
 * @param part slot in the database's partition array.
 */

void addb_gmap_partition_initialize(addb_gmap *gm, addb_gmap_partition *part) {
  part->part_path = NULL;
  part->part_td = NULL;
  part->part_gm = gm;

  cl_cover(gm->gm_addb->addb_cl);
}

/**
 * @brief Free resources allocated for a partition.
 *
 * @param part	the partition.  Must have been initialized with
 *  	addb_gmap_partition_initialize()
 * @return 0 on success, a nonzero error code on error.
 */
int addb_gmap_partition_finish(addb_gmap_partition *part) {
  addb_handle *addb = part->part_gm->gm_addb;
  int err;

  err = 0;
  if (part->part_td != NULL) {
    err = addb_tiled_destroy(part->part_td);
    part->part_td = NULL;
  }
  if (part->part_path != NULL) {
    cl_cover(addb->addb_cl);
    cm_free(addb->addb_cm, part->part_path);
    part->part_path = NULL;
  }
  part->part_gm = NULL;
  return err;
}

/**
 * @brief Print the filename for a partition's data file into a location.
 *
 * @param addb	the databasemodule handle.
 * @param i	partition number, 0..ADDB_GMAP_PARTITIONS_MAX-1
 * @param buf	buffer to format into
 * @param bufsize	number of bytes pointed to by buf
 */
void addb_gmap_partition_basename(addb_handle *addb, size_t i, char *buf,
                                  size_t bufsize) {
  cl_cover(addb->addb_cl);
  cl_assert(addb->addb_cl, i < ADDB_GMAP_PARTITIONS_MAX);

  snprintf(buf, bufsize, "g-%c%c.addb",
           addb_gmap_partition_alphabet32[0x1F & (i >> 5)],
           addb_gmap_partition_alphabet32[0x1F & i]);
}

/**
 * @brief Make sure a partition's database file has a name.
 *
 *  If the file already has a name, the call is harmless and does nothing.
 *
 * @param gm 	whole database pointer
 * @param part	partition to be named
 * @param i	partition's index in the database
 */
int addb_gmap_partition_name(addb_gmap_partition *part, size_t i) {
  addb_gmap *gm = part->part_gm;
  addb_handle *addb = gm->gm_addb;
  cl_handle *cl = addb->addb_cl;

  cl_assert(cl, i < ADDB_GMAP_PARTITIONS_MAX);

  if (part->part_path == NULL) {
    /* Generate the filename for this partition file.
     */
    cl_assert(cl, gm->gm_path != NULL);
    cl_assert(cl, gm->gm_base != NULL);
    cl_assert(cl, gm->gm_base_n >= sizeof("g-xx.addb"));
    cl_assert(cl, i < ADDB_GMAP_PARTITIONS_MAX);
    cl_cover(cl);

    addb_gmap_partition_basename(addb, i, gm->gm_base, gm->gm_base_n);
    part->part_path = cm_strmalcpy(addb->addb_cm, gm->gm_path);
    if (part->part_path == NULL) {
      int err = errno ? errno : ENOMEM;
      cl_log(cl, CL_LEVEL_ERROR,
             "addb: failed "
             "to duplicate path \"%s\": %s [%s:%d]",
             gm->gm_path, addb_xstrerror(errno), __FILE__, __LINE__);
      return err;
    }
  }
  return 0;
}

/**
 * @brief Update the virtual file size of a partition.
 *
 *  This actually doesn't grow the underlying file; only
 *  the tile cache does that.
 *
 * @param part	partition to grow
 * @param fd	partition's file descriptor
 * @param size  size we want to grow to.
 */
int addb_gmap_partition_grow(addb_gmap_partition *part, off_t size) {
  cl_handle *cl = part->part_gm->gm_addb->addb_cl;

  if (size > part->part_size) {
    char *header;
    addb_tiled_reference header_tref;

    cl_cover(cl);
    part->part_size = size;

    /* Update the header size on the tile, and mark
     * the tile as modified.
     */
    header = addb_tiled_get(part->part_td, 0, ADDB_GMAP_HEADER_SIZE,
                            ADDB_MODE_WRITE, &header_tref);
    if (header == NULL) return errno;

    ADDB_PUT_U8(header + ADDB_GMAP_VSIZE_OFFSET, (unsigned long long)size);
    addb_tiled_free(part->part_td, &header_tref);
  }
  return 0;
}

/**
 * @brief Open a GMAP partition file.
 *
 *  If the file doesn't exist yet and the mode includes ADDB_MODE_WRITE,
 *  it is created.
 *
 * @param part	partition slot we'd like to open,
  *		must have been initialized with gmap_partition_initialize().
 * @param mode	access mode for the underlying file --
 *		ADDB_MODE_READONLY or ADDB_MODE_READ_WRITE.
 * @return 0 on success, a nonzero error number on error.
 */
int addb_gmap_partition_open(addb_gmap_partition *part, int mode) {
  addb_gmap *gm = part->part_gm;
  addb_handle *addb = gm->gm_addb;
  cl_handle *cl = addb->addb_cl;
  struct stat st;
  char header[ADDB_GMAP_HEADER_SIZE];
  unsigned long long min_size;
  off_t phys_size;
  int err = 0;
  int fd;

  min_size = ADDB_GMAP_SINGLE_OFFSET;

  cl_assert(cl, addb != NULL);
  cl_assert(cl, part->part_path != NULL);
  cl_assert(cl, part->part_td == NULL);

  fd = open(part->part_path,
            mode == ADDB_MODE_READ_ONLY ? O_RDONLY : (O_RDWR | O_CREAT), 0666);
  if (fd == -1) {
    err = errno ? errno : -1;
    cl_log(cl, CL_LEVEL_ERROR, "addb: open \"%s\" fails: %s [%s:%d]",
           part->part_path, strerror(errno), __FILE__, __LINE__);
    cl_cover(cl);
    return err;
  }

  err = addb_file_fstat(cl, fd, part->part_path, &st);
  if (err != 0) {
    (void)close(fd);
    return err;
  }

  part->part_size = st.st_size;
  if (st.st_size < min_size) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "addb: create or rewrite %s from %llu to %llu bytes",
           part->part_path, (unsigned long long)st.st_size,
           (unsigned long long)min_size);
    cl_cover(cl);

    if (!(mode & ADDB_MODE_WRITE)) {
      cl_cover(cl);
      (void)close(fd);
      return EINVAL;
    }

    /*  The file is broken or didn't exist yet.
     *
     *  Write the header:
     *
     *   4 byte magic number
     *   8 byte virtual file size
     *   20 bytes padding.
     *   34 * 5 byte empty free list entries.
     */

    memset(header, 0, sizeof header);
    memcpy(header, ADDB_GMAP_MAGIC, sizeof(ADDB_GMAP_MAGIC));

    /*  Initialize the virtual file size as the start
     *  of the dynamic allocation area.
     */
    ADDB_PUT_U8(header + ADDB_GMAP_VSIZE_OFFSET, ADDB_GMAP_MULTI_OFFSET);

    part->part_size = ADDB_GMAP_MULTI_OFFSET;

    err = addb_file_write(addb, fd, part->part_path, header, sizeof header);
    if (err) {
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_ERROR, "%s: addb_file_write fails: %s [%s:%d]",
             part->part_path, addb_xstrerror(err), __FILE__, __LINE__);
      (void)close(fd);
      return err;
    }

    /* Grow the underlying file to a multiple of the tile size. */
    st.st_size = min_size;

    phys_size = addb_round_up(min_size, ADDB_TILE_SIZE);
    if (ftruncate(fd, phys_size) != 0) {
      err = errno;
      cl_log(cl, CL_LEVEL_ERROR,
             "%s: can't extend file to %llu bytes: "
             "%s [%s:%d]",
             part->part_path, (unsigned long long)phys_size, strerror(err),
             __FILE__, __LINE__);
      (void)close(fd);
      return err;
    }
  } else {
    /*  read the first 32 bytes of header information to
     *  initialize the maintenance data structure.
     */
    err = addb_file_read(addb, fd, part->part_path, header, sizeof header,
                         false /* don't expect EOF */);
    if (err != 0) {
      cl_cover(cl);
      (void)close(fd);
      return err;
    }

    /* Check the magic number.
     */
    if (memcmp(header, ADDB_GMAP_MAGIC, sizeof(ADDB_GMAP_MAGIC) - 1) != 0) {
      cl_log(cl, CL_LEVEL_ERROR,
             "%s: invalid magic number in "
             "database partition file (want: %s, got %.4s)",
             part->part_path, ADDB_GMAP_MAGIC, header);
      return EINVAL;
    }

    cl_assert(cl, ADDB_GMAP_VSIZE_SIZE == 8);
    part->part_size = ADDB_GET_U8(header + ADDB_GMAP_VSIZE_OFFSET);

    /* Check the virtual file size.
     */
    if (part->part_size < ADDB_GMAP_MULTI_OFFSET) {
      cl_log(cl, CL_LEVEL_ERROR,
             "%s: invalid virtual file size in "
             "database partition file (want: %llu or "
             "larger, got %llu) [adjusted upwards]",
             part->part_path, (unsigned long long)ADDB_GMAP_MULTI_OFFSET,
             (unsigned long long)part->part_size);
      part->part_size = ADDB_GMAP_MULTI_OFFSET;
    }
    if (((part->part_size - ADDB_GMAP_MULTI_OFFSET) % ADDB_GMAP_ENTRY_SIZE) !=
        0) {
      cl_log(cl, CL_LEVEL_ERROR,
             "%s: invalid virtual file size in "
             "database partition file (want: %llu+5n, "
             "got %llu) [adjusted]",
             part->part_path, (unsigned long long)ADDB_GMAP_MULTI_OFFSET,
             (unsigned long long)part->part_size);

      part->part_size +=
          ADDB_GMAP_ENTRY_SIZE -
          ((part->part_size - ADDB_GMAP_MULTI_OFFSET) % ADDB_GMAP_ENTRY_SIZE);
    }

    /* Grow the underlying file to a multiple of the tile size. */
    phys_size = addb_round_up(st.st_size, ADDB_TILE_SIZE);
    if (st.st_size != phys_size) {
      cl_log(cl, CL_LEVEL_DEBUG,
             "%s: adjusting physical file size %llu to an "
             "even multiple of the tile size %llu, %llu",
             part->part_path, (unsigned long long)st.st_size,
             (unsigned long long)ADDB_TILE_SIZE, (unsigned long long)phys_size);

      err = addb_file_truncate(addb, fd, part->part_path, phys_size);
      if (err) except_throw(err);
    }
  }

  /*  Initialize the tile pool.
   */
  cl_assert(cl, gm->gm_tiled_pool);

  /* Create the tiled accessor
   */
  part->part_td = addb_tiled_create(gm->gm_tiled_pool, part->part_path, O_RDWR,
                                    gm->gm_cf.gcf_init_map);
  if (part->part_td == NULL) {
    err = ENOMEM;
    goto err;
  }

  addb_tiled_set_mlock(part->part_td, gm->gm_cf.gcf_mlock);

  err = addb_tiled_backup(part->part_td, 1);
  if (err) goto err;
  err = addb_tiled_read_backup(part->part_td, gm->gm_horizon);
  if (err) goto err;

  cl_log(cl, CL_LEVEL_DEBUG, "addb: open \"%s\": size %llu", part->part_path,
         (unsigned long long)part->part_size);

  close(fd);

  return 0;

err:
  if (-1 != fd) close(fd);

  return err;
}

/**
 * @brief Read object partitions from our database directory.
 * @param gm	database to read
 * @param mode	how to open it - ADDB_GMAP_READ_WRITE or
 * 	ADDB_GMAP_READ_ONLY are popular choices...
 * @return 0 on success, a nonzero error code on error.
 */

int addb_gmap_partitions_read(addb_gmap *gm, int mode) {
  addb_handle *addb = gm->gm_addb;
  addb_gmap_partition *part, *part_end;
  size_t i;

  part_end = gm->gm_partition +
             (sizeof(gm->gm_partition) / sizeof(gm->gm_partition[0]));

  for (part = gm->gm_partition, i = 0; part < part_end; i++, part++) {
    int err;
    struct stat st;

    err = addb_gmap_partition_name(part, i);
    if (err != 0) {
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb: addb_gmap_partition_name fails: %s", addb_xstrerror(err));
      return err;
    }

    if (stat(part->part_path, &st) != 0) {
      if ((err = errno) == ENOENT) {
        cm_free(gm->gm_addb->addb_cm, part->part_path);
        part->part_path = NULL;

        continue;
      }

      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb: stat \"%s\" fails: %s [%s:%d]", part->part_path,
             strerror(errno), __FILE__, __LINE__);
      return err;
    }

    err = addb_gmap_partition_open(part, mode);
    if (err != 0) {
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb: "
             "addb_gmap_partition_open(%s) fails: %s",
             part->part_path, addb_xstrerror(err));
      return err;
    }
  }

  gm->gm_partition_n = i;
  return 0;
}

/**
 * @brief Read a 5-byte datum from a GMAP, as a long long.
 *
 * @param part		The GMAP partition to read from
 * @param offset	Byte offset from the start of the file.
 * @param out		Assign the 5 bytes to this variable as a big-endian.
 *
 * @return 0 on success, an error number on error.
 * @return ADDB_ERR_NO if the accessed part is outside of the file.
 */
int addb_gmap_partition_get(addb_gmap_partition *part,
                            unsigned long long offset,
                            unsigned long long *out) {
  unsigned char const *data;
  addb_tiled_reference tref;

  if (addb_tiled_peek5(part->part_td, offset, out)) return 0;

  /* else, fall through to slow, tile-aware case */

  if (offset / ADDB_TILE_SIZE ==
      (offset + (ADDB_GMAP_ENTRY_SIZE - 1)) / ADDB_TILE_SIZE) {
    data = addb_tiled_get(part->part_td, offset, offset + ADDB_GMAP_ENTRY_SIZE,
                          ADDB_MODE_READ_ONLY, &tref);
    if (data == NULL) return ADDB_ERR_NO;

    *out = ADDB_GET_U5(data);

    addb_tiled_free(part->part_td, &tref);
    cl_cover(part->part_gm->gm_addb->addb_cl);
  } else {
    /*  5 bytes split across two tiles.
     *  This is fairly rare (about 1 : 1500) and doesn't have
     *  to be super fast.
     */
    unsigned long long i, boundary;

    /*   Front half.
     */
    boundary = ((offset / ADDB_TILE_SIZE) + 1) * ADDB_TILE_SIZE;
    cl_assert(part->part_gm->gm_addb->addb_cl, boundary > offset);
    cl_assert(part->part_gm->gm_addb->addb_cl,
              boundary < offset + ADDB_GMAP_ENTRY_SIZE);

    data = addb_tiled_get(part->part_td, offset, boundary, ADDB_MODE_READ_ONLY,
                          &tref);
    if (data == NULL) return ADDB_ERR_NO;

    *out = 0;

    for (i = offset; i < boundary; i++) *out = (*out << 8) | *data++;
    addb_tiled_free(part->part_td, &tref);

    /*  Back half.
     */
    data =
        addb_tiled_get(part->part_td, boundary, offset + ADDB_GMAP_ENTRY_SIZE,
                       ADDB_MODE_READ_ONLY, &tref);
    if (data == NULL) return ADDB_ERR_NO;

    for (i = boundary; i < offset + ADDB_GMAP_ENTRY_SIZE; i++)
      *out = (*out << 8) | *data++;
    addb_tiled_free(part->part_td, &tref);

    cl_cover(part->part_gm->gm_addb->addb_cl);
  }

  /*
  cl_log(part->part_gm->gm_addb->addb_cl, CL_LEVEL_SPEW,
          "gmap: %llu -> %llx",
          (unsigned long long)offset,
          (unsigned long long)*out);
  */

  return 0;
}

/**
 * @brief Read the largest leading possible tile fragment starting
 * 	at an address.
 *
 *  This is part of a fast, special-purpose mass-transfer of indexes,
 *  such as when turning an array of 5-byte GMAP IDs into a hullset.
 *
 * @param part		The GMAP partition to read from
 * @param offset_s	byte offset from the start of the file
 *				(beginning of tile)
 * @param data_s_out	out: beginning of the result data
 * @param data_e_out	out: end of the result tile's data
 * @param tref_out	out: tile reference to *data_s_out..*data_e_out.
 *
 * @return 0 on success, an error number on error.
 * @return ADDB_ERR_NO if the accessed part is outside of the file.
 */
int addb_gmap_partition_get_chunk(addb_gmap_partition *part,
                                  unsigned long long offset_s,
                                  char const **data_s_out,
                                  char const **data_e_out,
                                  addb_tiled_reference *tref_out) {
  size_t tile_i = offset_s / ADDB_TILE_SIZE;
  unsigned long long offset_e = (tile_i + 1) * ADDB_TILE_SIZE;

  *data_s_out = addb_tiled_get(part->part_td, offset_s, offset_e,
                               ADDB_MODE_READ_ONLY, tref_out);
  if (*data_s_out == NULL) return ADDB_ERR_NO;

  *data_e_out = *data_s_out + (ADDB_TILE_SIZE - (offset_s % ADDB_TILE_SIZE));

  cl_log(part->part_gm->gm_addb->addb_cl, CL_LEVEL_SPEW,
         "gmap: addb_gmap_partition_get_chunk %llu -> %p..%p", offset_s,
         (void *)*data_s_out, (void *)*data_e_out);

  return 0;
}

/**
 * @brief Return a pointer to a raw chunk of bytes from the
 * 	specified GMAP partition
 *
 *  These offsets are counting from the beginning of the file.
 *  It's up to the caller to pick out one of the GMAP result arrays
 *  from the tangle of arrays in the whole partition!
 *
 *  The returned chunk may be less than what's requested, but
 *  will always be at least one byte large (if there is any data
 *  there at all and the specified range isn't empty).
 *
 * @param part		partition management structure.
 * @param offset	first byte to retrieve
 * @param end		first byte to not retrieve
 * @param ptr_out	assign data pointer to this
 * @param end_out	assign adjusted "end" offset to this
 * @param tref		keep track of the reference in this.
 *
 * @return 0 on success,
 * @return ADDB_ERR_NO if we're out of data to return
 */
int addb_gmap_partition_read_raw_loc(addb_gmap_partition *part,
                                     unsigned long long offset,
                                     unsigned long long end,
                                     unsigned char const **ptr_out,
                                     unsigned long long *end_out,
                                     addb_tiled_reference *ref_out,
                                     char const *file, int line) {
  *ptr_out = addb_tiled_read_array_loc(part->part_td, offset, end, end_out,
                                       ref_out, file, line);

  if (!*ptr_out) return errno ? errno : ENOMEM;

  return 0;
}

/**
 * @brief Write a 5-byte datum to a GMAP.
 *
 * @param part		The GMAP partition to write to
 * @param offset	Byte offset from the start of the file.
 * @param val		Value to write
 *
 * @return 0 on success, an error number on error.
 */
int addb_gmap_partition_put(addb_gmap_partition *part,
                            unsigned long long offset, unsigned long long val) {
  unsigned char *data;
  addb_tiled_reference tref;
  int err;

  if ((offset / ADDB_TILE_SIZE) ==
      (offset + (ADDB_GMAP_ENTRY_SIZE - 1)) / ADDB_TILE_SIZE) {
    errno = 0;
    data = addb_tiled_alloc(part->part_td, offset,
                            offset + ADDB_GMAP_ENTRY_SIZE, &tref);
    if (data == NULL) {
      err = errno ? errno : ENOMEM;
      cl_log(part->part_gm->gm_addb->addb_cl, CL_LEVEL_ERROR,
             "addb_gmap_partition_put: failed to "
             "allocate a tile for %s:%llu: %s",
             part->part_path, (unsigned long long)offset, addb_xstrerror(err));
      return err;
    }

    data[4] = val & 0xFF;
    val >>= 8;
    data[3] = val & 0xFF;
    val >>= 8;
    data[2] = val & 0xFF;
    val >>= 8;
    data[1] = val & 0xFF;
    val >>= 8;
    data[0] = val & 0xFF;

    addb_tiled_free(part->part_td, &tref);
  } else {
    unsigned long long i, boundary;

    boundary = ((offset / ADDB_TILE_SIZE) + 1) * ADDB_TILE_SIZE;
    cl_assert(part->part_gm->gm_addb->addb_cl, boundary > offset);
    cl_assert(part->part_gm->gm_addb->addb_cl,
              boundary < offset + ADDB_GMAP_ENTRY_SIZE);

    /*   Back end.
     */
    data = addb_tiled_alloc(part->part_td, boundary,
                            offset + ADDB_GMAP_ENTRY_SIZE, &tref);
    if (data == NULL) {
      err = errno ? errno : ENOMEM;
      cl_log(part->part_gm->gm_addb->addb_cl, CL_LEVEL_ERROR,
             "addb_gmap_partition_put: failed to "
             "allocate a back-end tile for %s:%llu: %s",
             part->part_path, (unsigned long long)boundary,
             addb_xstrerror(err));
      return err;
    }

    data += ((offset + ADDB_GMAP_ENTRY_SIZE) - boundary);
    for (i = boundary; i < offset + ADDB_GMAP_ENTRY_SIZE; i++) {
      *--data = val;
      val >>= 8;
    }
    addb_tiled_free(part->part_td, &tref);

    /*   Front end.
     */
    data = addb_tiled_alloc(part->part_td, offset, boundary, &tref);
    if (data == NULL) {
      err = errno ? errno : ENOMEM;
      cl_log(part->part_gm->gm_addb->addb_cl, CL_LEVEL_ERROR,
             "addb_gmap_partition_put: failed to "
             "allocate a front-end tile for %s:%llu: %s",
             part->part_path, (unsigned long long)offset, addb_xstrerror(err));
      return err;
    }

    data += boundary - offset;

    for (i = offset; i < boundary; i++) {
      *--data = val;
      val >>= 8;
    }
    addb_tiled_free(part->part_td, &tref);
  }
  return 0;
}

/**
 * @brief Copy bytes from a buffer to a location in a partition.
 *
 *  If the destination space doesn't exist yet, it is allocated.
 *
 * @param part		The GMAP partition to write to
 * @param offset	Byte offset from the start of the file.
 * @param source	source
 * @param n		Number of bytes to move
 *
 * @return 0 on success, an error number on error.
 */
int addb_gmap_partition_mem_to_file(addb_gmap_partition *part,
                                    unsigned long long offset,
                                    char const *source, size_t n) {
  addb_handle *addb = part->part_gm->gm_addb;
  unsigned char *data;
  addb_tiled_reference tref;

  while (n > 0) {
    unsigned long long boundary;

    if ((offset / ADDB_TILE_SIZE) == ((offset + n) - 1) / ADDB_TILE_SIZE) {
      data = addb_tiled_alloc(part->part_td, offset, offset + n, &tref);
      if (data == NULL) return errno ? errno : ENOMEM;

      memcpy(data, source, n);
      addb_tiled_free(part->part_td, &tref);

      break;
    }

    boundary = ((offset / ADDB_TILE_SIZE) + 1) * ADDB_TILE_SIZE;
    cl_assert(addb->addb_cl, boundary > offset);
    cl_assert(addb->addb_cl, boundary < offset + n);

    /*   Partial.
     */
    data = addb_tiled_alloc(part->part_td, offset, boundary, &tref);
    if (data == NULL) return errno ? errno : ENOMEM;

    memcpy(data, source, boundary - offset);

    source += (boundary - offset);
    n -= boundary - offset;
    offset = boundary;

    addb_tiled_free(part->part_td, &tref);
  }
  return 0;
}

/**
 * @brief Copy bytes from one location in a partition to another.
 *
 *  The two ranges must not overlap.
 *
 * @param part		The GMAP partition to write to
 * @param destination	Byte offset from the start of the file.
 * @param source	source
 * @param n		Number of bytes to move
 *
 * @return 0 on success, an error number on error.
 */
int addb_gmap_partition_copy(addb_gmap_partition *part,
                             unsigned long long destination,
                             unsigned long long source, unsigned long long n) {
  cl_handle *cl = part->part_gm->gm_addb->addb_cl;
  void *data;
  addb_tiled_reference tref;
  int err;

  while (n > 0) {
    unsigned long long boundary;

    /* get the next source fragment. */

    if ((source / ADDB_TILE_SIZE) == (source + n - 1) / ADDB_TILE_SIZE) {
      data = addb_tiled_alloc(part->part_td, source, source + n, &tref);
      if (data == NULL) return errno ? errno : ENOMEM;

      err = addb_gmap_partition_mem_to_file(part, destination, data, n);
      addb_tiled_free(part->part_td, &tref);
      return err;
    }

    boundary = ((source / ADDB_TILE_SIZE) + 1) * ADDB_TILE_SIZE;
    cl_assert(cl, boundary < source + n);
    cl_assert(cl, boundary > source);

    data = addb_tiled_alloc(part->part_td, source, boundary, &tref);
    if (data == NULL) return errno ? errno : ENOMEM;

    err = addb_gmap_partition_mem_to_file(part, destination, data,
                                          boundary - source);
    addb_tiled_free(part->part_td, &tref);

    destination += boundary - source;
    n -= boundary - source;
    source += boundary - source;
  }
  return 0;
}

/**
 * @brief Return the { offset, length } pair for a source id.
 *
 * @param part partition we're in
 * @param id	source id to look up
 * @param offset_out assign the offset of the first index to this.
 * @param n_out NULL, or assign the number of indices in the
 *		returned array to this.
 */
int addb_gmap_partition_data(addb_gmap_partition *part, addb_gmap_id id,
                             unsigned long long *offset_out,
                             unsigned long long *n_out,
                             unsigned long long *index_out) {
  cl_handle *cl = part->part_gm->gm_addb->addb_cl;
  unsigned long long i_val, i_offset;
  int err;

  *n_out = 0;

  id %= ADDB_GMAP_SINGLE_ENTRY_N;
  i_offset = ADDB_GMAP_SINGLE_ENTRY_OFFSET(id);

  if ((err = addb_gmap_partition_get(part, i_offset, &i_val)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_get", err,
                 "i_offset=%llu", i_offset);
    return err;
  }

  if (ADDB_GMAP_IVAL_IS_EMPTY(i_val))
    return ADDB_ERR_NO;

  else if (ADDB_GMAP_IVAL_IS_SINGLE(i_val) || ADDB_GMAP_IVAL_IS_FILE(i_val) ||
           ADDB_GMAP_IVAL_IS_BGMAP(i_val)) {
    *index_out = i_val;
    *offset_out = i_offset;
    *n_out = 1;
    cl_cover(cl);
  } else {
    unsigned long long m_offset, s_offset, s_val;

    m_offset = *offset_out = ADDB_GMAP_MULTI_ENTRY_OFFSET(i_val);
    if (n_out == NULL) return 0;

    /*  Read the sentinel value to determine the true
     *  size of the array.
     */
    s_offset = m_offset + ADDB_GMAP_IVAL_M_SIZE(i_val) - ADDB_GMAP_ENTRY_SIZE;

    err = addb_gmap_partition_get(part, s_offset, &s_val);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_gmap_partition_get", err,
                   "s_offset=%llu", s_offset);
      return err;
    }

    cl_cover(cl);
    *n_out = ADDB_GMAP_MVAL_S_IS_FULL(s_val) ? ADDB_GMAP_IVAL_M_NELEMS(i_val)
                                             : ADDB_GMAP_MVAL_S_NELEMS(s_val);

    /*
    cl_log(cl, CL_LEVEL_SPEW,
            "addb_gmap_partition_data: i_val %llx: %llu[%llu]",
            i_val, *offset_out, *n_out);
    */
  }
  return 0;
}

/**
 * @brief Get the partition for a source ID.
 * @param gm		database we'd like ot access
 * @param id		source id
 */
addb_gmap_partition *addb_gmap_partition_by_id(addb_gmap *gm, addb_gmap_id id) {
  cl_handle *cl = gm->gm_addb->addb_cl;
  addb_gmap_partition *part;

  if (id > ADDB_GMAP_ID_MAX) {
    cl_log(cl, CL_LEVEL_FAIL,
           "addb: cannot translate %s[%llu] into a partition", gm->gm_path,
           (unsigned long long)id);
    cl_cover(cl);
    return NULL;
  }
  part = gm->gm_partition + id / ADDB_GMAP_SINGLE_ENTRY_N;
  if (part->part_td == NULL) {
    cl_cover(cl);
    return NULL;
  }
  return part;
}
