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

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static char const addb_istore_partition_alphabet32[32] =
    "0123456789ABCDEFGHIJKLMNOPQRSTUV";

/* Initialize an object partition slot.
 */

void addb_istore_partition_initialize(addb_istore* is,
                                      addb_istore_partition* part) {
  memset(part, 0, sizeof(*part));

  part->ipart_path = NULL;
  part->ipart_td = NULL;

  cl_cover(is->is_addb->addb_cl);
}

/**
 * @brief Free a partition
 * @param is 	istore database
 * @param part	istore partition
 * @return 0 on success, a nonzero error code on error.
 */
int addb_istore_partition_finish(addb_istore* is, addb_istore_partition* part) {
  addb_handle* addb = is->is_addb;
  int err, result = 0;

  cl_assert(addb->addb_cl, part != NULL);
  cl_enter(addb->addb_cl, CL_LEVEL_SPEW, "enter");

  if (part->ipart_path != NULL) {
    cl_cover(addb->addb_cl);
    cm_free(addb->addb_cm, part->ipart_path);
    part->ipart_path = NULL;
  }

  if (part->ipart_td != NULL) {
    if ((err = addb_tiled_destroy(part->ipart_td)) != 0 && result == 0)
      result = err;

    part->ipart_td = NULL;
  }

  /*  If we just closed the last partition, adjust is->is_partition_n
   *  to point just after the last open partition.
   */
  if (is->is_partition_n == 1 + (part - is->is_partition)) {
    while (is->is_partition_n > 0 &&
           !is->is_partition[is->is_partition_n - 1].ipart_td &&
           !is->is_partition[is->is_partition_n - 1].ipart_path)

      is->is_partition_n--;
  }

  cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "leave");
  return result;
}

void addb_istore_partition_basename(addb_handle* addb, size_t i, char* buf,
                                    size_t bufsize) {
  (void)addb;

  cl_cover(addb->addb_cl);
  snprintf(buf, bufsize, "i-%c%c.addb",
           addb_istore_partition_alphabet32[0x1F & (i >> 5)],
           addb_istore_partition_alphabet32[0x1F & i]);
}

int addb_istore_partition_name(addb_istore* is, addb_istore_partition* part,
                               size_t i) {
  addb_handle* addb = is->is_addb;

  /* Generate the filename for this partition file.
   */
  cl_assert(addb->addb_cl, is->is_path != NULL);
  cl_assert(addb->addb_cl, is->is_base != NULL);
  cl_assert(addb->addb_cl, is->is_base_n >= sizeof("i-xx.addb"));
  cl_assert(addb->addb_cl, i < ADDB_ISTORE_PARTITIONS_MAX);

  if (part->ipart_path == NULL) {
    addb_istore_partition_basename(addb, i, is->is_base, is->is_base_n);
    part->ipart_path = cm_strmalcpy(addb->addb_cm, is->is_path);
    if (part->ipart_path == NULL) {
      int err = errno ? errno : ENOMEM;
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb: failed to duplicate path \"%s\": "
             "%s [%s:%d]",
             is->is_path, strerror(errno), __FILE__, __LINE__);
      return err;
    }
    cl_cover(addb->addb_cl);
  }
  return 0;
}

static int addb_istore_partition_grow(addb_istore* is,
                                      addb_istore_partition* part, int fd,
                                      off_t size) {
  addb_handle* addb = is->is_addb;
  int err = 0;

  size = addb_round_up(size, ADDB_ISTORE_TILE_SIZE);

  err = addb_file_grow(addb->addb_cl, fd, part->ipart_path, size);
  if (err) return err;

  cl_cover(is->is_addb->addb_cl);

  return 0;
}

int addb_istore_partition_open(addb_istore* is, addb_istore_partition* part,
                               int mode) {
  int const open_mode =
      mode == ADDB_MODE_READ_ONLY ? O_RDONLY : (O_RDWR | O_CREAT);
  unsigned long long const min_size =
      addb_round_up(ADDB_ISTORE_DATA_OFFSET_0, ADDB_ISTORE_TILE_SIZE);
  addb_handle* const addb = is->is_addb;
  char header[ADDB_ISTORE_HEADER_SIZE];
  struct stat st;
  int err = 0;
  int fd;

  cl_enter(addb->addb_cl, CL_LEVEL_SPEW, "(%s, %s)", part->ipart_path,
           mode == ADDB_MODE_READ_ONLY ? "read-only" : "read-write");

  cl_assert(addb->addb_cl, addb);
  cl_assert(addb->addb_cl, part->ipart_path);

  if (part->ipart_td) {
    cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "-- already open");
    return 0;
  }

  fd = open(part->ipart_path, open_mode, 0666);
  if (-1 == fd) {
    err = errno;
    cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "open", err,
                 "unable to open istore partition: \"%s\"", part->ipart_path);
    except_throw(err);
  }

  err = addb_file_fstat(addb->addb_cl, fd, part->ipart_path, &st);
  if (err != 0) except_throw(err);

  if (st.st_size < min_size) {
    cl_log(addb->addb_cl, CL_LEVEL_DEBUG,
           "addb: create or rewrite %s from %llu to %llu bytes",
           part->ipart_path, (unsigned long long)st.st_size,
           (unsigned long long)min_size);

    if (!(mode & ADDB_MODE_WRITE)) {
      cl_cover(addb->addb_cl);
      close(fd);

      if (part->ipart_td != NULL) {
        cl_leave(addb->addb_cl, CL_LEVEL_SPEW,
                 "existing partition too small + "
                 "read-only");
        return 0;
      }
      cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "file too small + read-only");
      return EINVAL;
    }

    /*  The file is broken or didn't exist yet.
     *
     *  Write the header:
     *
     *   4 byte magic number
     *   4 byte highest ID in the file (0 initially)
     */

    memset(header, 0, sizeof header);
    memcpy(header, ADDB_ISTORE_MAGIC, sizeof(ADDB_ISTORE_MAGIC));

    err = addb_file_write(addb, fd, part->ipart_path, header, sizeof header);
    if (err) {
      cl_cover(addb->addb_cl);
      (void)close(fd);

      cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "addb_file_write fails");
      return err;
    }

    /*  Now that that's out of the way, grow to min_size,
     *  filling up with zeros.
     */
    err = addb_istore_partition_grow(is, part, fd, min_size);
    if (err) except_throw(err);

    st.st_size = min_size;
  } else {
    off_t rounded = addb_round_up(st.st_size, ADDB_ISTORE_TILE_SIZE);
    if (st.st_size != rounded) {
      if (!(mode & ADDB_MODE_WRITE)) {
        cl_cover(addb->addb_cl);
        cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "bad file size + read-only");
        return EINVAL;
      }

      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb: adjust size of \"%s\" from %llu to %llu"
             " to make it a multiple of %lu "
             "(corrupted database?) [%s:%d]",
             part->ipart_path, (unsigned long long)st.st_size,
             (unsigned long long)rounded, (unsigned long)ADDB_ISTORE_TILE_SIZE,
             __FILE__, __LINE__);

      err = addb_istore_partition_grow(is, part, fd, rounded);
    }
  }

  if (is->is_tiled_pool == NULL) {
    errno = 0;
    is->is_tiled_pool = addb->addb_master_tiled_pool;
    cl_assert(addb->addb_cl, is->is_tiled_pool != NULL);
    cl_cover(addb->addb_cl);
  }

  /* Compute the size of the partition file by looking up the offset
   * to the end of the last id in the partition file.  The last id
   * in a partition is either partition size - 1, if the partition
   * is full, or next_id - 1, if the partition is the last one.
   */
  {
    unsigned long long const next_id = addb_istore_next_id(is);
    unsigned long long const part_next_id = next_id % ADDB_ISTORE_INDEX_N;
    bool const writable_partition =
        next_id / ADDB_ISTORE_INDEX_N == part - is->is_partition;

    errno = 0;
    part->ipart_td = addb_tiled_create(is->is_tiled_pool, part->ipart_path,
                                       writable_partition ? O_RDWR : O_RDONLY,
                                       is->is_cf.icf_init_map);
    if (part->ipart_td == NULL) {
      err = errno ? errno : ENOMEM;
      cl_leave(addb->addb_cl, CL_LEVEL_SPEW,
               "failed to allocate tiled partition: %s", strerror(err));
      except_throw(err);
    }
    addb_tiled_set_mlock(part->ipart_td, is->is_cf.icf_mlock);

    /* Figure out the partition size
     */
    if (!writable_partition) {
      err = addb_istore_index_get(is, part, ADDB_ISTORE_INDEX_N - 1,
                                  &part->ipart_size);
      if (err) {
        cl_log_errno(
            addb->addb_cl, CL_LEVEL_ERROR, "addb_istore_index_get", err,
            "Unable to set partition size, "
            "last id in partition=%llu next_id= %llu.  Istore corrupt?",
            part_next_id - 1, (unsigned long long)is->is_next.ism_memory_value);
        cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "leave");
        except_throw(err);
      }
    } else if (part_next_id == 0)
      part->ipart_size = ADDB_ISTORE_DATA_OFFSET_0;
    else {
      err =
          addb_istore_index_get(is, part, part_next_id - 1, &part->ipart_size);
      if (err) {
        cl_log_errno(
            addb->addb_cl, CL_LEVEL_ERROR, "addb_istore_index_get", err,
            "Unable to set partition size, "
            "last id in partition=%llu next_id= %llu.  Istore corrupt?",
            part_next_id - 1, (unsigned long long)is->is_next.ism_memory_value);
        cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "leave");
        except_throw(err);
      }
    }
  }

  cl_log(addb->addb_cl, CL_LEVEL_DEBUG, "addb: open \"%s\": virtual size %llu",
         part->ipart_path, (unsigned long long)part->ipart_size);

  if (is->is_partition_n <= part - is->is_partition)
    is->is_partition_n = 1 + (part - is->is_partition);

  cl_assert(addb->addb_cl, is->is_partition_n >= part - is->is_partition);

  except_catch(err) {
    if (part->ipart_path != NULL) {
      cm_free(addb->addb_cm, part->ipart_path);
      part->ipart_path = NULL;
    }
  }
  if (fd != -1) close(fd);

  cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "%s", err ? strerror(err) : "done");
  return err;
}

/**
 * @brief Revert a partition to a previous state.
 *
 *  When rolling back, all partitions currently in play are
 *  called to roll back their particular partition file.
 *
 *  If the partition is ahead of the partition that contains the
 *  rollback-target, it must remove itself completely.
 *  If the partition contains the rollback-target, it should
 *  rewind to that point.
 *  Partitions behind the rollback-target don't have to do
 *  anything.
 *
 * @param 	is 	istore database
 * @param	part	this partition
 * @param	horizon	roll back to this point.
 *
 * @return 0 on success, a nonzero error code on
 *	unexpected error.
 *
 */
int addb_istore_partition_rollback(addb_istore* is, addb_istore_partition* part,
                                   unsigned long long horizon) {
  addb_handle* addb = is->is_addb;
  cl_handle* cl = addb->addb_cl;
  size_t part_i;
  unsigned long long last_id;
  int err = 0;
  int e;

  /*  Which partition is the one that contains the
   *  rollback point?
   *
   *  (The rollback point is the id of the first
   *  primitive that will be created after the
   *  rollback is completed.)
   */
  if (0 == horizon) {
    part_i = 0;
    last_id = 0;
  } else {
    part_i = (horizon - 1) / ADDB_ISTORE_INDEX_N;
    last_id = (horizon - 1) % ADDB_ISTORE_INDEX_N;
  }

  if (part_i == part - is->is_partition) {
    err = addb_istore_index_get(is, part, last_id, &part->ipart_size);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "addb_istore_index_get", err,
                   "Unable to reset partition size for \"%s\" "
                   "part->next_id=%llu next_id= %llu.  Istore corrupt?",
                   part->ipart_path, last_id,
                   (unsigned long long)is->is_next.ism_memory_value);
    }
  } else if (part_i < part - is->is_partition) {
    /* Remove this partition's file completely. */

    err = addb_file_unlink(addb, part->ipart_path);

    e = addb_istore_partition_finish(is, part);
    if (e) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "addb_istore_partition_finish", err,
                   "Unable to finish partition \"%s\"", part->ipart_path);
      if (!err) err = e;
    }

    addb_istore_partition_initialize(is, part);
  }

  return err;
}

/* Read object partitions from our actual database directory.
 */

int addb_istore_partitions_read(addb_istore* is, int mode) {
  addb_handle* addb = is->is_addb;
  addb_istore_partition *part, *part_end;
  size_t i;
  int err = 0;

  cl_enter(addb->addb_cl, CL_LEVEL_SPEW, "(%s, %s)", is->is_path,
           mode == ADDB_MODE_READ_ONLY ? "read-only" : "read-write");

  part_end = is->is_partition +
             (is->is_next.ism_memory_value + (ADDB_ISTORE_INDEX_N - 1)) /
                 ADDB_ISTORE_INDEX_N;

  for (part = is->is_partition, i = 0; part < part_end; i++, part++) {
    struct stat st;

    if ((err = addb_istore_partition_name(is, part, i)) != 0) {
      cl_leave(addb->addb_cl, CL_LEVEL_SPEW,
               "addb_istore_partition_name fails: %s", addb_xstrerror(err));
      return err;
    }

    if (stat(part->ipart_path, &st) != 0) {
      if ((err = errno) == ENOENT) {
        cl_cover(addb->addb_cl);
        cl_log(addb->addb_cl, CL_LEVEL_DEBUG, "addb: no file \"%s\"",
               part->ipart_path);

        err = ADDB_ERR_NO;
        break;
      }

      cl_cover(addb->addb_cl);
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb: stat \"%s\" fails: %s [%s:%d]", part->ipart_path,
             strerror(errno), __FILE__, __LINE__);

      cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "stat fails: %s", strerror(err));
      return err;
    }

    err = addb_istore_partition_open(is, part, mode);
    if (err) {
      cl_cover(addb->addb_cl);
      cl_leave(addb->addb_cl, CL_LEVEL_SPEW,
               "addb_istore_partition_open fails: %s", addb_xstrerror(err));
      return err;
    }
  }

  is->is_partition_n = i;

  /*  If there are partitions beyond the ones we just read, remove them;
   *  we're rolling back past their creation.
   *
   *  (To test this, create 16 million records, then create another
   *  million, dying mid-way through; then restart.)
   */

  part_end = is->is_partition + ADDB_ISTORE_PARTITIONS_MAX;
  for (part = is->is_partition + i; part < part_end; i++, part++) {
    bool unlink_failed;

    if ((err = addb_istore_partition_name(is, part, i)) != 0) {
      cl_leave(addb->addb_cl, CL_LEVEL_SPEW,
               "addb_istore_partition_name #%d fails: %s", (int)i,
               addb_xstrerror(err));
      return err;
    }
    unlink_failed = unlink(part->ipart_path) != 0;
    if (unlink_failed && errno != ENOENT) {
      err = errno;
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "%s: cannot unlink spurious partition "
             "during rollback at startup: %s",
             part->ipart_path, strerror(err));
    }

    addb_istore_partition_finish(is, part);
    if (unlink_failed) break;
  }
  cl_leave(addb->addb_cl, CL_LEVEL_SPEW, "%s", err ? strerror(err) : "done");
  return err;
}

/*  Read the storage pointed to by <id>, and return it in the
 *  filled-out <data>.
 *
 *  Once the caller is done with <data>, it must be released
 *  with a call to addb_istore_free() or addb_istore_reference_free().
 */
int addb_istore_partition_data_loc(addb_istore* is, addb_istore_partition* part,
                                   addb_istore_id id, addb_data* data,
                                   char const* file, int line) {
  addb_handle* addb = is->is_addb;
  off_t b_start, b_end; /* ixunits */
  int err;

  data->data_type = ADDB_DATA_NONE;
  cl_assert(addb->addb_cl, id < (1ull << 34));
  if (!part || !part->ipart_td) {
    cl_log(addb->addb_cl, CL_LEVEL_FAIL,
           "addb: istore read: id %llu would be in "
           "partition %llu, which doesn't exist [%s:%d]",
           (unsigned long long)id, (unsigned long long)(id >> 24), __FILE__,
           __LINE__);

    return ADDB_ERR_NO;
  }

  /*  Where is it within that partition?
   */
  err = addb_istore_index_boundaries_get(is, part, id, &b_start, &b_end);
  if (err) {
    cl_cover(addb->addb_cl);
    return err;
  }

  cl_assert(addb->addb_cl, b_start >= ADDB_ISTORE_DATA_OFFSET_0);
  cl_assert(addb->addb_cl, b_end >= b_start);
  cl_assert(addb->addb_cl, b_start % 8 == 0);
  cl_assert(addb->addb_cl, b_end % 8 == 0);

  /*
  cl_log(addb->addb_cl, CL_LEVEL_SPEW,
          "addb_istore_partition_data(b_start %llu, b_end %llu) [%s:%d]",
          (unsigned long long)b_start,
          (unsigned long long)b_end,
          file,
          line );
  */

  if ((data->data_size = b_end - b_start) == 0) {
    data->data_type = ADDB_DATA_NONE;
    data->data_memory = NULL;

    cl_cover(addb->addb_cl);
    return 0;
  }

  data->data_memory = addb_tiled_get_loc(
      data->data_iref.iref_td = part->ipart_td, b_start, b_end, ADDB_MODE_READ,
      &data->data_iref.iref_tref, file, line);

  if (data->data_memory == NULL) return errno ? errno : ENOMEM;

  cl_cover(addb->addb_cl);
  data->data_type = ADDB_DATA_ISTORE;
  return 0;
}

/**
 * @brief Set the first unallocated id of a partition to a given value.
 *
 * @param is an opaque istore handle opened with addb_istore_open()
 * @param part an opaque istore handle opened with addb_istore_open()
 * @param val the new next-id
 * @return 0 on success, otherwise a nonzero error value.
 */
int addb_istore_partition_next_id_set(addb_istore* is,
                                      addb_istore_partition* part,
                                      addb_istore_id val) {
  addb_handle* addb = is->is_addb;
  unsigned char* ptr;
  addb_tiled_reference tref;
  unsigned long long s, e;

  cl_assert(addb->addb_cl, part != NULL);

  /*  This access can't cross tile boundaries -- it and its
   *  offsets are small divisors of the tile size, and it's
   *  somewhere very early in the file (bytes [4...8) at
   *  the time of this writing.)
   */
  s = ADDB_ISTORE_NEXT_OFFSET;
  e = s + ADDB_ISTORE_INDEX_SIZE;

  ptr = addb_tiled_get(part->ipart_td, s, e, ADDB_MODE_WRITE, &tref);
  if (ptr == NULL) return ENOMEM;

  ADDB_PUT_U4(ptr, val);

  cl_cover(addb->addb_cl);
  addb_tiled_free(part->ipart_td, &tref);

  return 0;
}
