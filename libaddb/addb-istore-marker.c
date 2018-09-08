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

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <unistd.h>

/*  There are two marker files.
 *  Each consists of
 *
 *   - a four-byte magic number:		ai1i (id)
 *					    and ai1h (horizon)
 *
 *   - one or more repetitions of:
 *	{
 *		five-byte ID
 *	}
 *
 *  	Of those repetitions, the LAST one fully
 *	contained in the file is valid.
 *
 *  Internally, we append to the marker files (and sync
 *  after each append); every 200 appends, we'll truncate
 *  the file instead of appending.
 */

#define ADDB_ISTORE_MARKER_RECORD_SIZE (5)

/**
 * @brief finish what istore_marker_write_replace_start() started.
 */
static int istore_marker_write_replace_finish(addb_istore* is,
                                              addb_istore_marker* ism,
                                              bool block) {
  int err;
  cl_handle* const cl = is->is_addb->addb_cl;

  cl_assert(cl, is != NULL);
  cl_assert(cl, ism != NULL);

  err = addb_file_sync_finish(cl, &ism->ism_write_fsc, block, ism->ism_path);

  if (err == ADDB_ERR_MORE) {
    cl_assert(cl, !block);
    return err;
  }

  ism->ism_write_finish = NULL;
  if (err != 0) {
    (void)close(ism->ism_fd);
    ism->ism_fd = -1;

    (void)unlink(ism->ism_tmp_path);
    cl_log_errno(cl, CL_LEVEL_ERROR | ADDB_FACILITY_RECOVERY,
                 "addb_file_sync_finish", err,
                 "%s: failed to sync "
                 "temporary istore marker file after writing",
                 ism->ism_tmp_path);
    return err;
  }

  /*  Rename that file to its official name, replacing
   *  a previous version (if any).
   */
  err = addb_file_rename(is->is_addb, ism->ism_tmp_path, ism->ism_path, true);
  if (err) {
    (void)close(ism->ism_fd);
    ism->ism_fd = -1;

    (void)unlink(ism->ism_tmp_path);

    return err;
  }

  /*  Keep the file descriptor around for
   *  istore_marker_write_append(), below.
   */
  cl_log(cl, CL_LEVEL_DEBUG | ADDB_FACILITY_RECOVERY,
         "%s: istore marker %s:  memory %llu, written %llu", is->is_path,
         ism->ism_name, (unsigned long long)ism->ism_memory_value,
         (unsigned long long)ism->ism_writing_value);
  return 0;
}

/**
 * @brief replace the old marker file with a brand-new version
 * 	containing a single record.
 */
static int istore_marker_write_replace_start(addb_istore* is,
                                             addb_istore_marker* ism,
                                             bool hard_sync) {
  cl_handle* const cl = is->is_addb->addb_cl;
  char header[ADDB_MAGIC_SIZE + ADDB_ISTORE_MARKER_RECORD_SIZE];
  int err;

  if (is == NULL) return EINVAL;

  /* There's no other write in progress.  Right??
   */
  cl_assert(cl, ism->ism_write_finish == NULL);

  /* We're already caught up?
   */
  if (ism->ism_memory_value == ism->ism_writing_value) return ADDB_ERR_ALREADY;

  /*  If we had an old marker file open,
   *  close its file descriptor.
   */
  if (ism->ism_fd != -1) {
    err = addb_file_close(is->is_addb, ism->ism_fd, ism->ism_path);
    if (err != 0) return err;
  }

  /*  Make a new index counter tmpfile.
   */
  ism->ism_fd = open(ism->ism_tmp_path, O_WRONLY | O_CREAT, 0666);
  if (ism->ism_fd == -1) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR | ADDB_FACILITY_RECOVERY, "open", err,
                 "%s: failed to open temporary "
                 "istore marker file for writing",
                 ism->ism_tmp_path);
    return err;
  }
  (void)addb_file_advise_log(cl, ism->ism_fd, ism->ism_tmp_path);

  memcpy(header, ism->ism_magic, ADDB_MAGIC_SIZE);
  ADDB_PUT_U5(header + ADDB_MAGIC_SIZE, ism->ism_memory_value);

  err = addb_file_write(is->is_addb, ism->ism_fd, ism->ism_tmp_path, header,
                        sizeof header);
  if (err) {
    (void)close(ism->ism_fd);
    ism->ism_fd = -1;

    (void)unlink(ism->ism_tmp_path);
    cl_log_errno(cl, CL_LEVEL_ERROR | ADDB_FACILITY_RECOVERY, "addb_file_write",
                 err, "%s: failed to write temporary istore marker file",
                 ism->ism_tmp_path);
    return err;
  }
  if (!hard_sync) {
    /*  Rename that file to its official name, replacing
     *  a previous version (if any).
     */
    err =
        addb_file_rename(is->is_addb, ism->ism_tmp_path, ism->ism_path, false);
    if (err) {
      (void)close(ism->ism_fd);
      ism->ism_fd = -1;

      (void)unlink(ism->ism_tmp_path);
      return err;
    }
  } else {
    err = addb_file_sync_start(cl, ism->ism_fd, &ism->ism_write_fsc,
                               ism->ism_tmp_path, false);
    if (err) {
      (void)close(ism->ism_fd);
      ism->ism_fd = -1;

      (void)unlink(ism->ism_tmp_path);
      cl_log_errno(cl, CL_LEVEL_ERROR | ADDB_FACILITY_RECOVERY,
                   "addb_file_sync", err,
                   "%s: failed to sync "
                   "temporary istore marker file after writing",
                   ism->ism_tmp_path);
      return err;
    }

    /* The caller will call this until it stops returning
     * ADDB_ERR_MORE.
     */
    ism->ism_write_finish = istore_marker_write_replace_finish;
  }

  /* Remember what we just wrote.
   */
  ism->ism_writing_value = ism->ism_memory_value;

  cl_log(cl, CL_LEVEL_DEBUG | ADDB_FACILITY_RECOVERY,
         "%s: istore marker %s %llu%s", is->is_path, ism->ism_name,
         (unsigned long long)ism->ism_writing_value,
         ism->ism_write_finish ? " (in progress)" : "");
  return 0;
}

/**
 * @brief Finish what istore_marker_write_append_start() started.
 */
static int istore_marker_write_append_finish(addb_istore* is,
                                             addb_istore_marker* ism,
                                             bool block) {
  int err;

  if (is == NULL) return EINVAL;

  err = addb_file_sync_finish(is->is_addb->addb_cl, &ism->ism_write_fsc, block,
                              ism->ism_path);
  if (err) {
    if (err != ADDB_ERR_MORE)
      cl_log_errno(is->is_addb->addb_cl,
                   CL_LEVEL_ERROR | ADDB_FACILITY_RECOVERY,
                   "addb_file_sync_finish", err,
                   "%s: failed to sync "
                   "istore marker file after appending",
                   ism->ism_path);
    return err;
  }

  cl_log(is->is_addb->addb_cl, CL_LEVEL_DEBUG | ADDB_FACILITY_RECOVERY,
         "%s: istore marker: %s %llu (completed)", is->is_path, ism->ism_name,
         (unsigned long long)ism->ism_writing_value);
  return 0;
}

/**
 * @brief Append a record to an existing marker file, or
 *	create and keep open a fresh one.
 */
static int istore_marker_write_append_start(addb_istore* is,
                                            addb_istore_marker* ism,
                                            bool hard_sync) {
  cl_handle* const cl = is->is_addb->addb_cl;
  char header[ADDB_MAGIC_SIZE + ADDB_ISTORE_MARKER_RECORD_SIZE];
  char* header_end = header;
  int err;

  if (is == NULL) return EINVAL;

  /*  If we don't have an open marker file, open one now.
   */
  if (ism->ism_fd == -1) {
    ism->ism_fd = open(ism->ism_path, O_APPEND | O_WRONLY, 0666);
    if (ism->ism_fd == -1) {
      if (errno == ENOENT) {
        ism->ism_fd = open(ism->ism_path, O_CREAT | O_WRONLY, 0666);
      }
      if (ism->ism_fd == -1) {
        err = errno;
        cl_log_errno(cl, CL_LEVEL_ERROR | ADDB_FACILITY_RECOVERY, "open", err,
                     "%s: failed to open istore "
                     "marker file for writing",
                     ism->ism_path);
        return err;
      }

      /*  We just created this file.  It's empty.
       *  Add a magic number to the stuff we'll write.
       */
      memcpy(header_end, ism->ism_magic, ADDB_MAGIC_SIZE);
      header_end += ADDB_MAGIC_SIZE;
    } else {
      struct stat st;

      /*  We opened an existing file.
       *
       *  Sanity check - this file has the right
       *  number of bytes, right?
       */
      err = addb_file_fstat(cl, ism->ism_fd, ism->ism_path, &st);
      if (err != 0) {
        (void)close(ism->ism_fd);
        ism->ism_fd = -1;

        return err;
      }

      if (st.st_size < ADDB_MAGIC_SIZE + ADDB_ISTORE_MARKER_RECORD_SIZE) {
        cl_log(cl, CL_LEVEL_ERROR,
               "unexpected size for marker file "
               "\"%s\"; got %d bytes, expected at "
               "least %d (recovering)",
               ism->ism_path, (int)st.st_size,
               ADDB_MAGIC_SIZE + ADDB_ISTORE_MARKER_RECORD_SIZE);
        goto recover;
      }
      if ((st.st_size - ADDB_MAGIC_SIZE) % ADDB_ISTORE_MARKER_RECORD_SIZE !=
          0) {
        cl_log(cl, CL_LEVEL_ERROR,
               "unexpected size for marker "
               "file \"%s\"; got %d bytes, "
               "expected %d + N * %d (recovering)",
               ism->ism_path, (int)st.st_size, ADDB_MAGIC_SIZE,
               ADDB_ISTORE_MARKER_RECORD_SIZE);
        goto recover;
      }
    }
    (void)addb_file_advise_log(cl, ism->ism_fd, ism->ism_path);
  }
  cl_assert(cl, ism->ism_fd != -1);

  ADDB_PUT_U5(header_end, ism->ism_memory_value);
  header_end += 5;

  cl_assert(cl, header_end - header <= sizeof header);
  err = addb_file_write(is->is_addb, ism->ism_fd, ism->ism_path, header,
                        header_end - header);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR | ADDB_FACILITY_RECOVERY, "addb_file_write",
                 err, "%s: failed to write istore marker file", ism->ism_path);
    return err;
  }

  if (hard_sync) {
    err = addb_file_sync_start(cl, ism->ism_fd, &ism->ism_write_fsc,
                               ism->ism_path, false);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_ERROR | ADDB_FACILITY_RECOVERY,
                   "addb_file_sync", err,
                   "%s: failed to sync "
                   "istore marker file (fd: %d) after writing",
                   ism->ism_path, ism->ism_fd);
      return err;
    }
    ism->ism_write_finish = istore_marker_write_append_finish;
  }
  ism->ism_writing_value = ism->ism_memory_value;

  cl_log(cl, CL_LEVEL_VERBOSE | ADDB_FACILITY_RECOVERY,
         "%s: istore marker: %s %llu%s", is->is_path, ism->ism_name,
         (unsigned long long)ism->ism_memory_value,
         ism->ism_write_finish ? " (in progress)" : "");
  return 0;

recover:
  if (ism->ism_fd != -1) {
    err = addb_file_close(is->is_addb, ism->ism_fd, ism->ism_path);
    if (err != 0) return err;
    ism->ism_fd = -1;
  }
  return istore_marker_write_replace_start(is, ism, hard_sync);
}

/**
 * @brief Finish writing an externally visible checkpoint (part A)
 *
 *  Zero or more primitives have been added to the istore.
 *  The corresponding tiles have been flushed to disk.  If the system
 *  were to crash right now and reboot, we'd have enough information
 *  to reconstruct a consistent state that includes the new arrivals,
 *  except that we don't yet know we have that information.
 *
 *  Replace the "high watermark" counter of the database on file
 *  with one that contains the new maximum.
 *
 * @param is 	the database to write to
 * @result 0 on success, otherwise a nonzero error number.
 */
int addb_istore_marker_write_start(addb_istore* is, addb_istore_marker* ism,
                                   bool hard_sync) {
  if (is == NULL) return EINVAL;

  if (ism->ism_memory_value == ism->ism_writing_value) return ADDB_ERR_ALREADY;

  cl_assert(is->is_addb->addb_cl, ism->ism_write_finish == NULL);

  if (ism->ism_n_appends-- <= 0) {
    /* Staying below 4096 bytes seems like a reasonable target.
     * The first record is 4 bytes long, each following 5.
     */
    ism->ism_n_appends = 800;
    return istore_marker_write_replace_start(is, ism, hard_sync);
  }
  return istore_marker_write_append_start(is, ism, hard_sync);
}

/**
 * @brief Finish writing an externally visible checkpoint (part B).
 *
 *  Zero or more primitives have been added to the istore.
 *  The corresponding tiles have been flushed to disk.  If the system
 *  were to crash right now and reboot, we'd have enough information
 *  to reconstruct a consistent state that includes the new arrivals,
 *  except that we don't yet know we have that information.
 *
 *  Replace the "high watermark" counter of the database on file
 *  with one that contains the new maximum.
 *
 * @param is 		the database to write to
 * @param block 	should we wait for completion, or just check?
 *
 * @result ADDB_ERR_MORE if block was false, and the
 *	marker write has not yet completed.   Call this function
 *	until it returns something other than ADDB_ERR_MORE.
 * @result 0 on success, otherwise a nonzero error number.
 */
int addb_istore_marker_write_finish(addb_istore* is, addb_istore_marker* ism,
                                    bool block) {
  int err = 0;

  if (is == NULL) return EINVAL;

  if (ism->ism_write_finish != NULL) {
    err = (*ism->ism_write_finish)(is, ism, block);
    if (err != ADDB_ERR_MORE) ism->ism_write_finish = NULL;
  }

  cl_assert(is->is_addb->addb_cl, !(block && err == ADDB_ERR_MORE));
  return err;
}

/**
 * @brief Synchronously update a marker file.
 *  This is called by the end of the primitive write code.
 */
int addb_istore_marker_checkpoint(addb_istore* is, addb_istore_marker* ism,
                                  bool hard_sync) {
  int err;

  /*  If there's a previous flush in progress,
   *  wait for it to finish.
   */
  if (ism->ism_write_finish != NULL) {
    err = (*ism->ism_write_finish)(is, ism,
                                   /* blocking: */ true);
    if (err != 0) return err;

    ism->ism_write_finish = NULL;

    /*  Continue; things may have changed since
     *  we last wrote something.
     */
  }

  /* We're already caught up?
   */
  if (ism->ism_writing_value == ism->ism_memory_value) return ADDB_ERR_ALREADY;

  /*  Start writing.
   */
  err = addb_istore_marker_write_start(is, ism, hard_sync);
  if (err != 0) {
    if (err == ADDB_ERR_ALREADY) err = 0;
    return err;
  }

  /*  ... and wait for it to finish.
   */
  err = addb_istore_marker_write_finish(is, ism, true);
  if (err == ADDB_ERR_ALREADY) err = 0;

  return err;
}

/**
 * @brief Read the marker files.
 *
 * @param is 	the database to read for
 * @result 0 on success, otherwise a nonzero error number.
 * @result ENOENT if there was no marker file (i.e., it's a new partition.)
 */
int addb_istore_marker_read(addb_istore* is, addb_istore_marker* ism) {
  int err, fd;
  char record[ADDB_ISTORE_MARKER_RECORD_SIZE];
  char magic[ADDB_MAGIC_SIZE];
  off_t last_offset;
  struct stat st;
  cl_handle* const cl = is->is_addb->addb_cl;
  unsigned long long value;

  if (is == NULL) return EINVAL;

  /*  Open the file.
   */
  if ((fd = open(ism->ism_path, O_RDONLY)) == -1) {
    err = errno;
    if (err == ENOENT) {
      ism->ism_memory_value = 0;
      ism->ism_writing_value = 0;
    } else
      cl_log_errno(cl, CL_LEVEL_ERROR, "open", err,
                   "failed to open istore marker file, \"%s\" "
                   "for reading",
                   ism->ism_path);
    return err;
  }

  /*  Check the magic number.
   */
  err = addb_file_read(is->is_addb, fd, ism->ism_path, magic, sizeof magic,
                       false);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR | ADDB_FACILITY_RECOVERY, "addb_file_read",
                 err,
                 "%s: failed to read "
                 "istore marker magic number",
                 ism->ism_path);
    (void)close(fd);
    return err;
  }
  if (memcmp(magic, ism->ism_magic, ADDB_MAGIC_SIZE) != 0) {
    (void)close(fd);
    cl_log(cl, CL_LEVEL_ERROR,
           "%s: version error in marker magic number: "
           "expected \"%s\", got \"%.4s\"",
           ism->ism_path, ism->ism_magic, magic);
    return EINVAL;
  }

  /* Seek to the last completely written marker record.
   */
  err = addb_file_fstat(cl, fd, ism->ism_path, &st);
  if (err != 0) {
    (void)close(fd);
    return err;
  }
  if (st.st_size < ADDB_MAGIC_SIZE + ADDB_ISTORE_MARKER_RECORD_SIZE) {
    (void)close(fd);
    cl_log(cl, CL_LEVEL_ERROR,
           "unexpected size for marker file \"%s\"; "
           "got %d bytes, expected at least %d",
           ism->ism_path, (int)st.st_size,
           ADDB_MAGIC_SIZE + ADDB_ISTORE_MARKER_RECORD_SIZE);
    return EINVAL;
  }

  last_offset =
      ADDB_MAGIC_SIZE +
      (((st.st_size - ADDB_MAGIC_SIZE) / ADDB_ISTORE_MARKER_RECORD_SIZE) - 1) *
          ADDB_ISTORE_MARKER_RECORD_SIZE;

  if (last_offset != st.st_size - ADDB_ISTORE_MARKER_RECORD_SIZE)
    /* Complain, but continue. */
    cl_log(cl, CL_LEVEL_ERROR,
           "unexpected marker file size %lu bytes; expected: %lu"
           " (either something is very wrong, or graphd crashed "
           "during a partial marker file write.)",
           (unsigned long)st.st_size,
           (unsigned long)last_offset + ADDB_ISTORE_MARKER_RECORD_SIZE);
  err = addb_file_lseek(is->is_addb, fd, ism->ism_path, last_offset, SEEK_SET);
  if (err != 0) {
    (void)close(fd);
    return err;
  }

  /* Read the last marker record.
   */
  err = addb_file_read(is->is_addb, fd, ism->ism_path, record, sizeof record,
                       false);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_ERROR | ADDB_FACILITY_RECOVERY, "addb_file_read",
                 err,
                 "%s: failed to read "
                 "istore marker file",
                 ism->ism_path);
    (void)close(fd);
    return err;
  }

  /* Close the marker file.
   */
  err = addb_file_close(is->is_addb, fd, ism->ism_path);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_ERROR | ADDB_FACILITY_RECOVERY, "addb_file_read",
                 err,
                 "%s: failed to close "
                 "istore marker file after reading",
                 ism->ism_path);
    return err;
  }

  /* Typecheck the value.
   */
  value = ADDB_GET_U5(record);
  if (value > (1ull << 34)) {
    cl_log(cl, CL_LEVEL_ERROR,
           "%s: error in marker next_id: "
           "expected number <= %llu, got %llu",
           ism->ism_path, (1ull << 34),
           (unsigned long long)is->is_next.ism_memory_value);
    return EINVAL;
  }
  ism->ism_memory_value = ism->ism_writing_value = value;

  cl_log(cl, CL_LEVEL_VERBOSE,
         "addb_istore_marker_read: "
         "%s=%llu",
         ism->ism_name, (unsigned long long)ism->ism_memory_value);
  return 0;
}

addb_istore_id addb_istore_marker_next(addb_istore* is) {
  if (is != NULL) return is->is_next.ism_writing_value;

  return 0;
}

addb_istore_id addb_istore_marker_horizon(addb_istore* is) {
  if (is != NULL) return is->is_horizon.ism_writing_value;

  return 0;
}

/**
 * @brief Finish writing an externally visible checkpoint (part A)
 *
 *  Zero or more primitives have been added to the istore.
 *  The corresponding tiles have been flushed to disk.  If the system
 *  were to crash right now and reboot, we'd have enough information
 *  to reconstruct a consistent state that includes the new arrivals,
 *  except that we don't yet know we have that information.
 *
 *  Replace the "high watermark" counter of the database on file
 *  with one that contains the new maximum.
 *
 * @param is 	the database to write to
 * @result 0 on success, otherwise a nonzero error number.
 */
int addb_istore_marker_horizon_write_start(addb_istore* is, bool hard_sync) {
  if (is == NULL) return EINVAL;

  return addb_istore_marker_write_start(is, &is->is_horizon, hard_sync);
}

/**
 * @brief Finish writing an externally visible checkpoint (part B).
 *
 *  Zero or more primitives have been added to the istore.
 *  The corresponding tiles have been flushed to disk.  If the system
 *  were to crash right now and reboot, we'd have enough information
 *  to reconstruct a consistent state that includes the new arrivals,
 *  except that we don't yet know we have that information.
 *
 *  Replace the "high watermark" counter of the database on file
 *  with one that contains the new maximum.
 *
 * @param is 		the database to write to
 * @param block 	should we wait for completion, or just check?
 *
 * @result ADDB_ERR_MORE if block was false, and the
 *	marker write has not yet completed.   Call this function
 *	until it returns something other than ADDB_ERR_MORE.
 * @result 0 on success, otherwise a nonzero error number.
 */
int addb_istore_marker_horizon_write_finish(addb_istore* is, bool block) {
  if (is == NULL) return EINVAL;
  return addb_istore_marker_write_finish(is, &is->is_horizon, block);
}

/**
 * @brief Synchronously update a marker file.
 *  This is called by the end of the primitive write code.
 */
int addb_istore_marker_next_checkpoint(addb_istore* is, bool hard_sync) {
  return addb_istore_marker_checkpoint(is, &is->is_next, hard_sync);
}
