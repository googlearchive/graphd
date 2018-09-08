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
#include "libaddb/addb-bmap.h"
#include "libaddb/addbp.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>


/* Open a new  BMAP, creating it if need be.
 *
 * If creating a new bmap, it will be of size bits rounded up
 * to 8 bits. If we're opening a new bmap, the size must match
 * the size of the existing bmap.
 */
int addb_bmap_open(addb_handle *addb, const char *path, unsigned long long size,
                   unsigned long long horizon, bool linear, addb_bmap **out) {
  addb_bmap *bmap;
  int err = 0;
  bool create;
  struct stat sb;
  addb_tiled_reference r;
  unsigned char *p;
  unsigned long long filesize;

  cl_log(addb->addb_cl, CL_LEVEL_DEBUG, "addb_bmap_open: open %s. size: %llx",
         path, size);

  /*
   * Create the bmap structure and fill in some values.
   */
  bmap = cm_malloc(addb->addb_cm, sizeof(addb_bmap));
  if (!bmap) return ENOMEM;

  bmap->bmap_addb = addb;
  bmap->bmap_tdp = addb->addb_master_tiled_pool;
  cl_assert(addb->addb_cl, bmap->bmap_tdp != NULL);

  /* Set the max and initial map sizes to the size of
   * the bmap. We assume it is small enough for this
   * to be reasonable.
   */

  bmap->bmap_path = cm_strmalcpy(addb->addb_cm, path);
  if (bmap->bmap_path == NULL) {
    err = errno ? errno : ENOMEM;
    goto free_bmap;
  }

  bmap->bmap_bits = (size + 7ull) & (~7ull);
  bmap->bmap_cl = addb->addb_cl;
  bmap->bmap_cm = addb->addb_cm;
  bmap->bmap_horizon = horizon;
  bmap->bmap_linear = linear;

  if (stat(bmap->bmap_path, &sb)) {
    if (errno != ENOENT) {
      cl_log_errno(bmap->bmap_cl, CL_LEVEL_ERROR, "stat", errno,
                   "unexpected error stating file: %s", bmap->bmap_path);
      err = errno;
      goto free_bmap;
    }

    create = true;
    filesize = 25 * 1024 * 1024; /* good for 200M primitives */
  } else {
    filesize = sb.st_size;
    create = false;
  }
  if (size == 0) bmap->bmap_bits = (filesize - ADDB_BMAP_HEADER) * 8 - 1;
  bmap->bmap_tiled =
      addb_tiled_create(bmap->bmap_tdp, bmap->bmap_path, O_RDWR,
                        size ? ((size + 7) / 8 + ADDB_BMAP_HEADER) : filesize);

  if (!bmap->bmap_tiled) {
    err = errno;
    cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "addb_tiled_create", err,
                 "Can't open %s", bmap->bmap_path);
    goto free_bmap;
  }

  /*
   * If the file didn't exist, write the header and size
   * information
   */
  if (create) {
    cl_log(addb->addb_cl, CL_LEVEL_DEBUG,
           "file %s did not exist. Creating and initializing", bmap->bmap_path);

    p = addb_tiled_alloc(bmap->bmap_tiled, 0, ADDB_BMAP_HEADER, &r);

    if (!p) {
      err = errno;
      goto free_tiled;
    }

    memcpy(p + ADDB_BMAP_MAGIC_OFFSET, ADDB_BMAP_MAGIC, ADDB_BMAP_MAGIC_LEN);

    ADDB_PUT_U8(p + ADDB_BMAP_SIZE_OFFSET, bmap->bmap_bits);

    addb_tiled_free(bmap->bmap_tiled, &r);
    /*
     * Set the length of the on-disk bitmap to its
     * final length now instead of some random time
     * in the future when a bit in the last block is
     * finally set.
     */
    p = addb_tiled_alloc(bmap->bmap_tiled,
                         bmap->bmap_bits / 8 - 1 + ADDB_BMAP_HEADER,
                         bmap->bmap_bits / 8 + ADDB_BMAP_HEADER, &r);
    if (!p) {
      err = errno;
      goto free_tiled;
    }
    *p = 0;
    addb_tiled_free(bmap->bmap_tiled, &r);
  }

  if (!linear) {
    addb_tiled_backup(bmap->bmap_tiled, true);
    err = addb_tiled_read_backup(bmap->bmap_tiled, horizon);
    if (err) {
      cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "addb_tiled_read_backup", err,
                   "Can't read backup file for %s", bmap->bmap_path);
      return err;
    }
  }

  /*
   * Check the header and that the size matches
   */
  p = addb_tiled_get(bmap->bmap_tiled, 0, ADDB_BMAP_HEADER, ADDB_MODE_READ, &r);
  if (!p) {
    err = errno;
    cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "addb_tiled_get", err,
                 "Can't get first tile of bmap: %s", bmap->bmap_path);

    goto free_tiled;
  }

  if (memcmp(p + ADDB_BMAP_MAGIC_OFFSET, ADDB_BMAP_MAGIC,
             ADDB_BMAP_MAGIC_LEN)) {
    err = EINVAL;
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "BMAP magic for %s is %c%c%c%c. Should be %s.", bmap->bmap_path,
           p[0], p[1], p[2], p[3], ADDB_BMAP_MAGIC);

    goto free_tile;
  }
  *out = bmap;
  addb_tiled_free(bmap->bmap_tiled, &r);
  cl_log(addb->addb_cl, CL_LEVEL_DEBUG, "Successfully initialized bmap: %s",
         bmap->bmap_path);

  return 0;

free_tile:
  addb_tiled_free(bmap->bmap_tiled, &r);

free_tiled : {
  int othererror;

  othererror = addb_tiled_destroy(bmap->bmap_tiled);
  if (othererror)
    cl_log_errno(
        addb->addb_cl, CL_LEVEL_ERROR, "addb_tiled_destroy", othererror,
        "can't destroy tile for %s. (Funny, I just made it)", bmap->bmap_path);
}

free_bmap:
  cm_free(addb->addb_cm, bmap);

  return err;
}

/*
 * File may have changed on disk. Refresh as needbe.
 */
int addb_bmap_refresh(addb_bmap *bmap, unsigned long long max_id) {
  int err;

  err = addb_tiled_stretch(bmap->bmap_tiled);
  if (err) {
    cl_log_errno(bmap->bmap_addb->addb_cl, CL_LEVEL_ERROR, "addb_tiled_stretch",
                 err, "Unable to stretch tile for bitmap %s", bmap->bmap_path);
    return err;
  }

  /*
   * Recalculate file size
   */
  bmap->bmap_bits =
      (addb_tiled_physical_file_size(bmap->bmap_tiled) - ADDB_BMAP_HEADER) * 8;

  /* bmap_bits is defined as the last writable position,
   * which is one less than the size
   */
  bmap->bmap_bits--;
  return 0;
}

static void addb_bmap_free(addb_bmap *bmap) {
  cm_free(bmap->bmap_cm, bmap->bmap_path);
  cm_free(bmap->bmap_cm, bmap);
}

int addb_bmap_close(addb_bmap *bmap) {
  int err;

  if (!bmap) return 0;

  cl_log(bmap->bmap_cl, CL_LEVEL_DEBUG, "Closing bmap: %s", bmap->bmap_path);

  err = addb_tiled_destroy(bmap->bmap_tiled);
  if (err) {
    cl_log_errno(bmap->bmap_cl, CL_LEVEL_FAIL, "addb_tiled_destroy", err,
                 "Cannot destroy tile for %s", bmap->bmap_path);
    return err;
  }

  addb_bmap_free(bmap);
  return 0;
}

/*
 * Close and delete a bmap.
 */
int addb_bmap_truncate(addb_bmap *bmap) {
  int err;

  err = addb_tiled_backup(bmap->bmap_tiled, 0);
  if (err) {
    cl_log_errno(bmap->bmap_cl, CL_LEVEL_ERROR, "addb_tiled_backup", err,
                 "addb_bmap_truncate: cannot turn off "
                 "backups for %s",
                 bmap->bmap_path);
    return err;
  }

  err = addb_tiled_destroy(bmap->bmap_tiled);

  if (err) {
    cl_log_errno(bmap->bmap_cl, CL_LEVEL_ERROR, "addb_tiled_destroy", err,
                 "Can't get rid of tiles for %s", bmap->bmap_path);
    return err;
  }

  err = unlink(bmap->bmap_path);

  if (err) {
    err = errno;
    cl_log_errno(bmap->bmap_cl, CL_LEVEL_ERROR, "unlink", err,
                 "addb_bmap_truncate: cannot unlink %s"
                 " (which is funny because I just opened it)",
                 bmap->bmap_path);
    return err;
  }

  addb_bmap_free(bmap);

  return 0;
}

int addb_bmap_status(addb_bmap *bmap, cm_prefix const *prefix,
                     addb_status_callback *callback, void *cb_data) {
  cm_prefix bmap_pre;
  char data[100];

  if (!bmap) return 0;

  bmap_pre = cm_prefix_pushf(prefix, "bmap:%s", bmap->bmap_path);

  snprintf(data, sizeof data, "%llx", bmap->bmap_bits);

  return (*callback)(cb_data, cm_prefix_end(&bmap_pre, "size"), data);
}

int addb_bmap_status_tiles(addb_bmap *bmap, cm_prefix const *prefix,
                           addb_status_callback *callback, void *cb_data) {
  cm_prefix bmap_pre;

  if (!bmap) return 0;

  bmap_pre = cm_prefix_pushf(prefix, "bmap:%s", bmap->bmap_path);
  return addb_tiled_status_tiles(bmap->bmap_tiled, &bmap_pre, callback,
                                 cb_data);
}

unsigned long long addb_bmap_horizon(addb_bmap *bmap) {
  return bmap->bmap_horizon;
}

void addb_bmap_horizon_set(addb_bmap *bmap, unsigned long long h) {
  bmap->bmap_horizon = h;
}

int addb_bmap_checkpoint_rollback(addb_bmap *bmap) {
  int err;

  err = addb_tiled_read_backup(bmap->bmap_tiled, bmap->bmap_horizon);

  if (err && (err != EALREADY) && (err != ENOENT)) {
    cl_log_errno(bmap->bmap_cl, CL_LEVEL_ERROR, "addb_tiled_read_backup", err,
                 "unable to roll back %s", bmap->bmap_path);
  }

  return err;
}

int addb_bmap_checkpoint(struct addb_bmap *bmap, bool hard_sync, bool block,
                         addb_tiled_checkpoint_fn *cpfn) {
  int err = 0;
  if (bmap->bmap_linear) {
    if (cpfn == addb_tiled_checkpoint_start_writes)
      err = addb_tiled_checkpoint_linear_start(bmap->bmap_tiled, hard_sync,
                                               block);
    else if (cpfn == addb_tiled_checkpoint_finish_writes)
      err = addb_tiled_checkpoint_linear_finish(bmap->bmap_tiled, hard_sync,
                                                block);
  } else {
    if (cpfn == addb_tiled_checkpoint_start_writes)
      err = addb_bmap_checkpoint_start_writes(bmap, hard_sync, block);

    else if (cpfn == addb_tiled_checkpoint_finish_writes)
      err = addb_bmap_checkpoint_finish_writes(bmap, hard_sync, block);

    else if (cpfn == addb_tiled_checkpoint_sync_backup)
      err = addb_bmap_checkpoint_sync_backup(bmap, hard_sync, block);

    else if (cpfn == addb_tiled_checkpoint_finish_backup)
      err = addb_bmap_checkpoint_finish_backup(bmap, hard_sync, block);

    else if (cpfn == addb_tiled_checkpoint_remove_backup)
      err = addb_bmap_checkpoint_remove_backup(bmap, hard_sync, block);
    else
      cl_notreached(bmap->bmap_cl, "%p is not a checkpoint function", cpfn);
  }

  return err;
}

int addb_bmap_checkpoint_sync_backup(addb_bmap *bmap, bool hard_sync,
                                     bool block) {
  return addb_tiled_checkpoint_sync_backup(bmap->bmap_tiled, bmap->bmap_horizon,
                                           hard_sync, block);
}

int addb_bmap_checkpoint_finish_backup(addb_bmap *bmap, bool hard_sync,
                                       bool block) {
  return addb_tiled_checkpoint_finish_backup(
      bmap->bmap_tiled, bmap->bmap_horizon, hard_sync, block);
}

int addb_bmap_checkpoint_start_writes(addb_bmap *bmap, bool hard_sync,
                                      bool block) {
  return addb_tiled_checkpoint_start_writes(
      bmap->bmap_tiled, bmap->bmap_horizon, hard_sync, block);
}

int addb_bmap_checkpoint_finish_writes(addb_bmap *bmap, bool hard_sync,
                                       bool block) {
  return addb_tiled_checkpoint_finish_writes(
      bmap->bmap_tiled, bmap->bmap_horizon, hard_sync, block);
}

int addb_bmap_checkpoint_remove_backup(addb_bmap *bmap, bool hard_sync,
                                       bool block) {
  return addb_tiled_checkpoint_remove_backup(
      bmap->bmap_tiled, bmap->bmap_horizon, hard_sync, block);
}

/*
 * @brief Read a single bit from a BMAP.
 *
 * @param bmap	the bitmap
 * @param bit	the index of the bit
 * @param value	out: the value of the requested bit.
 *
 * @return 0 on success (and *value contains the boolean result),
 * @return a nonzero error code on error.
 */
int addb_bmap_check(addb_bmap *bmap, unsigned long long bit, bool *value) {
  addb_tiled_reference r;
  unsigned const char *p;
  unsigned char c;
  int err;

  if (bit > bmap->bmap_bits) {
    /*
     * This isn't an error. With bgmaps, the last bit of the
     * bgmap may be behind the last primitive of the database
     * so its possible to ask about bits we don't know about
     * yet. Treat them as unset.
     */
    cl_log(bmap->bmap_cl, CL_LEVEL_SPEW,
           "addb_bmap_check[%s]: %llx is past the logical"
           "end of the bitmap (%llx)",
           bmap->bmap_path, bit, bmap->bmap_bits);
    *value = false;
    return 0;
  }

  if ((p = addb_tiled_peek(bmap->bmap_tiled, (bit >> 3) + ADDB_BMAP_HEADER,
                           1))) {
    *value = !!(*p & (1 << (bit & 7)));
    return 0;
  }

  p = addb_tiled_get(bmap->bmap_tiled, (bit >> 3) + ADDB_BMAP_HEADER,
                     (bit >> 3) + ADDB_BMAP_HEADER + 1, ADDB_MODE_READ, &r);
  if (!p) {
    err = errno;
    /*
     * Accesses past the end of the file just mean that we've
     * never written a 1 that far yet. Return zero without
     * complaining.
     */
    if (err == E2BIG) {
      *value = false;
      return 0;
    }
    cl_log_errno(bmap->bmap_cl, CL_LEVEL_ERROR, "addb_tiled_get", err,
                 "tiled got for bit %llx gave unexpected error", bit);
    return err;
  }

  c = *p;
  addb_tiled_free(bmap->bmap_tiled, &r);

  *value = !!(c & (1 << (bit & 0x7)));
  return 0;
}

/*
 * Set a bit and return its previous value or -1 on error.
 */
int addb_bmap_check_and_set(addb_bmap *bmap, unsigned long long bit,
                            bool *value) {
  int err;
  err = addb_bmap_check(bmap, bit, value);

  if (err) return err;

  if (*value) return 0;

  return (addb_bmap_set(bmap, bit));
}

/*
 * Set a bit. Return zero on success and -1 on error.
 */
int addb_bmap_set(addb_bmap *bmap, unsigned long long bit) {
  addb_tiled_reference r;
  unsigned char *p;

  cl_log(bmap->bmap_cl, CL_LEVEL_SPEW, "addb_bmap_set: %llx", bit);

  p = addb_tiled_alloc(bmap->bmap_tiled, bit / 8 + ADDB_BMAP_HEADER,
                       bit / 8 + ADDB_BMAP_HEADER + 1, &r);
  if (!p) return errno;

  if (bit > bmap->bmap_bits) bmap->bmap_bits = bit;

  *p |= 1u << (bit % 8);
  addb_tiled_free(bmap->bmap_tiled, &r);

  return 0;
}

/*
 * Search qword aligned data for the first occurence of a non-zero qword
 * wills earch forwards or backwards and returns a pointer to the qword
 */
static unsigned long long *qmemzero(unsigned long long *start,
                                    unsigned long long *end, bool forward) {
  if (forward) {
    for (; start < end; start++)
      if (*start) return start;
    return NULL;
  } else {
    while (start < end)
      if (*--end) return end;
    return NULL;
  }
}

/*
 * Table of the position of the most significant bit for numbers in the range
 * 0..255 (value is wrong for zero).
 */
static const char msb_table[] = {
    0, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
    5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7};

/*
 * Table for the position of the least significant bit for numbers in the range
 * 0..255. (value is wrong for zero)
 */
static const char lsb_table[] = {
    8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0,
    3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0,
    3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0,
    3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0,
    3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0,
    3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0};

/*
 * Calculate the position of the most significant bit of u
 */
static int msb(unsigned long long u) {
  int o = 56;

  while (u) {
    if (u & (0xff00000000000000ull)) {
      o += msb_table[u >> 56];
      return o;
    }
    u <<= 8;
    o -= 8;
  }
  return -1;
}

/*
 * Calculate the position of the least significant bit of u
 */
static int lsb(unsigned long long u) {
  int o = 0;
  while (u) {
    if (u & (0xffull)) {
      o += lsb_table[u & 0xff];
      return o;
    }
    u >>= 8;
    o += 8;
  }
  return -1;
}

/*
 * Mask out all of the bits of u except those between s and e
 * (including s and excluding e).
 * if s and e overlap or are equal, return 0.
 */
static unsigned long long extract_bit_range(unsigned long long u, int s,
                                            int e) {
  if (s > 63) return 0;

  u &= ~((1ull << (unsigned long long)(s)) - 1ull);

  if (e < 64) u &= ((1ull << (unsigned long long)(e)) - 1ull);

  return u;
}

/*
 * Scan a bitmap pointed to by bp (which must be qword aligned) from start to
 * end finding the
 * first or last one in the map.
 * start and end should be bit offsets into the bitmap.
 */
static int bitscan(unsigned char *bp, size_t start, size_t end, bool forward,
                   unsigned long long *result_out) {
  size_t es; /* This is the bit offset of the first full qword to scan */
  size_t ee; /* This is the bit offset of the last full qword to scan */
  unsigned long long *res;
  size_t r;

  /*
   * Check the first qword as start or end may not be aligned to a 64 bit
   * boundary
   */
  if (forward) {
    /*
     * One after the last bit we will check here.
     * This is either the bit offset of the second qword, or
     * end
     */
    size_t e;

    size_t s; /* Bit offset of the first qword */

    unsigned long long data;

    e = (start + 64) & (~63ull);
    if (e > end) e = end;

    s = (start & (~63ull));

    /*
     * Extract the qword that start fits in
     */
    data = ((unsigned long long *)(void *)(bp))[s / 64];

    /*
     * Remove any bits that come before start, or after end
     * (start and end may be in the same qword
     */
    data = extract_bit_range(data, start - s, e - s);
    if (data) {
      *result_out = lsb(data) + s;
      return 0;
    }
    if (e == end) return ADDB_ERR_NO;
    es = e;
    ee = (end + 63) & (~63ull);
  } else {
    size_t s;
    size_t e;
    unsigned long long data;
    s = (end - 1) & (~63ull);

    if (s < start)
      e = start;
    else
      e = s;

    data = ((unsigned long long *)(void *)bp)[s / 64];

    data = extract_bit_range(data, e - s, end - s);

    if (data) {
      *result_out = msb(data) + s;
      return 0;
    }
    if (e == start) return ADDB_ERR_NO;
    es = start & (~63ull);
    ee = s;
  }

  /*
   * qmemzero takes aligned byte offsets marking the first and one-after-last
   * qword to scan
   */
  res = qmemzero((unsigned long long *)(bp + es / 8ull),
                 (unsigned long long *)(bp + ee / 8ull), forward);

  if (!res) return ADDB_ERR_NO;

  /*
   * We have a qword with a set bit. Find out which one it is and add
   * the bit address of the qword it lives in to get the result
   */
  if (forward) {
    r = lsb(*res) + ((void *)res - (void *)bp) * 8;
    if (r >= end) return ADDB_ERR_NO;
  } else {
    r = msb(*res) + ((void *)res - (void *)bp) * 8;
    if (r < start) return ADDB_ERR_NO;
  }

  *result_out = r;
  return 0;
}

int addb_bmap_scan(addb_bmap *bmap, unsigned long long start,
                   unsigned long long end, unsigned long long *result_out,
                   bool forward) {
  /*
   * Starting offset bit relative to the beginning of the file
   */
  unsigned long long phys_bit_s;

  /*
   * Ending offset bit relative to the beginning of the file
   */
  unsigned long long phys_bit_e;

  /*
   * The first bit (relative to the beginning of the file)
   * of the tile we're looking at.
   */

  unsigned long long phys_tile_bit;

  /*
   * The tile byte offset (relative to the beginning of the file)
   */
  unsigned long long tile_byte;

  /*
   * Starting bit offset relative to the tile
   */
  unsigned long long tile_offset_s;

  /*
   * Ending bit offset relative to the tile
   */
  unsigned long long tile_offset_e;

  unsigned char *tp;
  addb_tiled_reference r;

  int err;

  /*
   * If we're going backwards, we need to include the bit at
   * end.
   */
  if (!forward) end++;

  if (end > bmap->bmap_bits) end = bmap->bmap_bits + 1;

  while (start < end) {
    phys_bit_s = start + ADDB_BMAP_HEADER * 8;
    phys_bit_e = end + ADDB_BMAP_HEADER * 8;

    if (forward) {
      tile_byte = (phys_bit_s / 8) & ~32767ull;

    } else {
      tile_byte = ((phys_bit_e - 1) / 8) & ~32767ull;
    }

    phys_tile_bit = tile_byte * 8;

    /*
     * searching forwards. Calculate starting offset into the
     * tile. Choose the tighter of end or end-of-tile
     */
    if (forward) {
      cl_assert(bmap->bmap_cl, phys_tile_bit <= phys_bit_s);
      cl_assert(bmap->bmap_cl, phys_tile_bit < phys_bit_e);
      tile_offset_s = phys_bit_s - phys_tile_bit;
      tile_offset_e = phys_bit_e - phys_tile_bit;
      if (tile_offset_e > (32768 * 8)) {
        tile_offset_e = (32768 * 8);
      }
    } else {
      tile_offset_e = phys_bit_e - phys_tile_bit;

      if (phys_bit_s > phys_tile_bit) {
        tile_offset_s = phys_bit_s - phys_tile_bit;
      } else {
        tile_offset_s = 0;
      }
    }

    cl_assert(bmap->bmap_cl, tile_offset_s < tile_offset_e);

    tp = addb_tiled_get(bmap->bmap_tiled, tile_byte, tile_byte + 32768,
                        ADDB_MODE_READ, &r);

    /*
     * This should never happen because we rebound end
     * by the bmap's internal  bit count
     */
    cl_assert(bmap->bmap_cl, tp);

    err = bitscan(tp, tile_offset_s, tile_offset_e, forward, result_out);

    addb_tiled_free(bmap->bmap_tiled, &r);
    /*
     * Found it!
     */
    if (err == 0) {
      *result_out += phys_tile_bit - ADDB_BMAP_HEADER * 8;
      return 0;
    }

    if (err != ADDB_ERR_NO) {
      return err;
    }

    /*
     * ADDB_ERR_NO: move on to the next tile
     */
    if (forward) {
      start = phys_tile_bit - ADDB_BMAP_HEADER * 8 + tile_offset_e;
    } else {
      end = phys_tile_bit - ADDB_BMAP_HEADER * 8 + tile_offset_s;
    }
  }
  return ADDB_ERR_NO;
}

int addb_bmap_fixed_intersect(addb_handle *addb, struct addb_bmap *bm,
                              addb_id const *id_in, size_t n_in,
                              addb_id *id_out, size_t *n_out, size_t m) {
  addb_id *const id_out_start = id_out;
  addb_id *const id_out_end = id_out + m;

  for (*n_out = 0; n_in-- > 0; id_in++) {
    /* This is initialized so blaze will compile
     * in an optimizing fashion,
     * the value _always_ set in addb_bmap_check, so
     * don't worry about it.
     */
    bool val = false;
    int err;

    if ((err = addb_bmap_check(bm, *id_in, &val)) != 0) return err;

    if (val) {
      if (id_out >= id_out_end) {
        *n_out = id_out - id_out_start;
        return ADDB_ERR_MORE;
      }
      *id_out++ = *id_in;
    }
  }
  *n_out = id_out - id_out_start;

  return 0;
}
