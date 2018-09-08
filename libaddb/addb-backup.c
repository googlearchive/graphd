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
#include <stddef.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#ifndef MREMAP_MAYMOVE
#define MREMAP_MAYMOVE 1
#endif

/** Double Buffered (Asynchronous) Backup Files
 *
 *  Backup information for a tiled file is contained
 *  in the addb_tbk structure.  That structure keeps
 *  tabs on two backup files, the first, tbk_a{.fd,path}, is the
 *  active backup file which is accumulating changes, and the
 *  second, tbk_w, is the waiting backup file which is being
 *  written (asynchronously) to disk.
 *
 *  In the usual order of use, the following functions provide
 *  the interface to the backup system:
 *
 *  addb_backup_tbk_init
 *	Initialize the addb_tbk structure.
 *
 *  addb_backup_write
 *	Write a record, (offset, data) pair, to the backup file.
 *	If necessary a new backup file is created using whichever
 *      of the two names in the tbk_a_path array is available.
 *
 *  addb_backup_finish
 *	Finish writing the backup file.  The header is updated.
 *	The active backup file becomes the waiting backup
 *	file.
 *
 *  addb_backup_sync_start
 *	Initiate the asynchronous fsync operation on the waiting
 *	backup file.
 *
 *  addb_backup_sync_finish
 *	Check the asynchronous fsync operation for completion.
 *	This should be called until it no longer returns ADDB_ERR_MORE.
 *
 *  addb_backup_close
 *	Close the waiting backup file.
 *
 *  addb_backup_publish
 *	Publish the backup file by renaming *.clx to *.cln
 *	After publication, the backup file is valid and will
 *	be used, if we abort an operation, or if graphd discovers
 *	it when starting up.
 *
 *  addb_backup_publish
 *	Remove the backup file.  It is no longer necessary.
 */

/** the backup file header
 *
 *	Backup file content is a series of backup records.
 */

typedef struct addb_bkh {
  addb_u4 bkh_magic;   /* backup magic */
  addb_u5 bkh_horizon; /* horizon this file backs up to */
} addb_bkh;

#define ADDB_BKH_HORIZON(B__) ADDB_GET_U5((B__)->bkh_horizon)
#define ADDB_BKH_HORIZON_SET(B__, V__) ADDB_PUT_U5((B__)->bkh_horizon, (V__))

#define ADDB_BKH_CONTENTS(B__) (addb_bkr*)(bkh + 1)

/** @brief a backup record header
 *
 *	Backup data (length bkr_size) follows.
 */

typedef struct addb_bkr {
  /**
   * @brief offset in file being backed up
   */
  addb_u8 bkr_offset;

  /**
   * @brief size of following backup data (usually 1 page), in bytes.
   */
  addb_u8 bkr_size;

} addb_bkr;

#define ADDB_BKR_OFFSET(B__) ADDB_GET_U8((B__)->bkr_offset)
#define ADDB_BKR_OFFSET_SET(B__, V__) ADDB_PUT_U8((B__)->bkr_offset, (V__))

#define ADDB_BKR_SIZE(B__) ADDB_GET_U8((B__)->bkr_size)
#define ADDB_BKR_SIZE_SET(B__, V__) ADDB_PUT_U8((B__)->bkr_size, (V__))

#define ADDB_BKR_DATA(B__) ((char*)((B__) + 1))

/**
 * @brief Initialize backup information for a tiled file
 */

int addb_backup_init(addb_handle* addb, addb_tbk* tbk, char const* a0_path,
                     char const* a1_path, char const* v_path) {
  struct stat st;
  int i;

  tbk->tbk_a_path[0] = a0_path;
  tbk->tbk_a_path[1] = a1_path;
  tbk->tbk_v_path = v_path;
  tbk->tbk_a.fd = -1;
  tbk->tbk_a.path = (char*)0;
  tbk->tbk_w.fd = -1;
  tbk->tbk_w.path = (char*)0;
  tbk->tbk_do_backup = 0;
  tbk->tbk_published = 0;
  addb_file_sync_initialize(addb, &tbk->tbk_fsc);

  /* Remove any lingering .clx files
   */
  for (i = 0; i < 2; i++)
    if (!stat(tbk->tbk_a_path[i], &st)) {
      if (!S_ISREG(st.st_mode)) {
        cl_log(addb->addb_cl, CL_LEVEL_ERROR, "%s is not a file",
               tbk->tbk_a_path[i]);

        return ENOENT;
      }
      if (addb_file_unlink(addb, tbk->tbk_a_path[i])) return ENOENT;

      cl_log(addb->addb_cl, CL_LEVEL_FAIL,
             "addb_backup_init: removing \"%s\" Previous database must have "
             "crashed",
             tbk->tbk_a_path[i]);
    } else if (ENOENT != errno) {
      cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "stat", errno,
                   "addb_backup_init: unable to stat %s", tbk->tbk_a_path[i]);
    }

  return 0;
}

/* Something bad happened, try and clean up
 */

void addb_backup_punt(addb_tbk* tbk) {
  if (-1 != tbk->tbk_a.fd) (void)close(tbk->tbk_a.fd);

  if (tbk->tbk_a.path != NULL) (void)unlink(tbk->tbk_a.path);

  tbk->tbk_a.fd = -1;
  tbk->tbk_a.path = (char*)0;
}

/**
 * @brief Open a backup file for writing.
 *
 *  After a successful call, the file is open, and a magic
 *  number and "horizon" have been written.  The file descriptor
 *  can be closed with close(2).  Since backup files are double
 *  buffered (one being synced while the next is being written)
 *  we try opening both file names.
 *
 * @param addb 		Database handle
 * @param tbk		tiled backup information
 *
 * @return 0 success, a nonzero error code for error.
 */
static int addb_backup_open(addb_handle* addb, addb_tbk* tbk) {
  cl_handle* const cl = addb->addb_cl;
  addb_bkh bkh;
  int err = 0;
  ssize_t bytes;

  cl_assert(cl, -1 == tbk->tbk_a.fd);
  cl_assert(cl, !tbk->tbk_a.path);

  tbk->tbk_a.path = tbk->tbk_a_path[0];
  tbk->tbk_a.fd = open(tbk->tbk_a.path, O_CREAT | O_RDWR | O_EXCL, 0666);

  if (-1 == tbk->tbk_a.fd) {
    if (errno != EEXIST) {
      err = errno;
      goto open_fail;
    }

    /* If we couldn't open a_path[0] it had better be because
     * we were waiting to write it...
     */
    cl_assert(cl, tbk->tbk_w.path == tbk->tbk_a_path[0]);

    tbk->tbk_a.path = tbk->tbk_a_path[1];
    tbk->tbk_a.fd = open(tbk->tbk_a.path, O_CREAT | O_RDWR | O_EXCL, 0666);

    if (-1 == tbk->tbk_a.fd) {
      err = errno;
      goto open_fail;
    }

    (void)addb_file_advise_log(cl, tbk->tbk_a.fd, tbk->tbk_a.path);
  }

  cl_log(addb->addb_cl, CL_LEVEL_SPEW, "addb_backup_open: \"%s\" fd=%d",
         tbk->tbk_a.path, tbk->tbk_a.fd);

  memcpy(bkh.bkh_magic, ADDB_BACKUP_MAGIC, sizeof bkh.bkh_magic);
  ADDB_BKH_HORIZON_SET(&bkh, ADDB_U5_MAX);
  errno = 0;
  bytes = write(tbk->tbk_a.fd, &bkh, sizeof bkh);
  if (bytes < 0) {
    err = errno;
    cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "write", err,
                 "cannot write %zu byte header to \"%s\"", sizeof bkh,
                 tbk->tbk_a.path);
  } else if (bytes != sizeof bkh) {
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "cannot write %zu byte header to \"%s\" "
           "(only wrote %ld bytes)",
           sizeof bkh, tbk->tbk_a.path, (long)bytes);
    err = errno ? errno : ENOSPC;
  }

  return err;

open_fail:
  cl_assert(cl, err);
  cl_log_errno(cl, CL_LEVEL_ERROR, "open", err,
               "cannot open backup file for writing: %s", tbk->tbk_a.path);

  if (tbk->tbk_a.fd != -1) {
    (void)close(tbk->tbk_a.fd);
    tbk->tbk_a.fd = -1;
  }
  (void)unlink(tbk->tbk_a.path);

  return err;
}

/**
 * @brief Back up a single chunk of memory.
 *
 * @param addb 		addb context
 * @param tbk		tiled backup information
 * @param offset	offset in original file of mem
 * @param mem		memory being backed up
 * @param size		# of bytes pointed to by mem.
 *
 * @return 0 success, a nonzero error code for write error.
 */

int addb_backup_write(addb_handle* addb, addb_tbk* tbk,
                      unsigned long long offset, char const* mem, size_t size) {
  int err = 0;

  cl_assert(addb->addb_cl, getpagesize() == size);
  cl_assert(addb->addb_cl, 0 == offset % getpagesize());

  /* If the backup file isn't open yet, open it now.
   */
  if (-1 == tbk->tbk_a.fd) {
    err = addb_backup_open(addb, tbk);
    if (err) return err;
  }

  cl_assert(addb->addb_cl, -1 != tbk->tbk_a.fd);
  cl_assert(addb->addb_cl, tbk->tbk_a.path);

  {
    addb_bkr bkr;
    struct iovec iov[2];
    ssize_t bytes;

    ADDB_BKR_OFFSET_SET(&bkr, offset);
    ADDB_BKR_SIZE_SET(&bkr, size);

    iov[0].iov_base = (void*)&bkr;
    iov[0].iov_len = sizeof bkr;
    iov[1].iov_base = (void*)mem;
    iov[1].iov_len = size;

    errno = 0;
    bytes = writev(tbk->tbk_a.fd, iov, sizeof iov / sizeof iov[0]);
    if (bytes < 0) {
      err = errno ? errno : ENOSPC;
      cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "writev", err,
                   "cannot write %zu bytes from offset "
                   "%llu to \"%s\"",
                   size + sizeof bkr, (unsigned long long)offset,
                   tbk->tbk_a.path);
    } else if (bytes != iov[0].iov_len + iov[1].iov_len) {
      err = errno ? errno : ENOSPC;
      cl_log(addb->addb_cl, CL_LEVEL_ERROR,
             "addb_backup_write: short return "
             "%ld != %lu bytes from offset %llu to \"%s\"",
             (long)bytes, (unsigned long)(size + sizeof bkr), offset,
             tbk->tbk_a.path);
    }
  }

  cl_log(addb->addb_cl, CL_LEVEL_SPEW, "addb_backup_write: %s: %llu[%zu]",
         tbk->tbk_a.path, offset, size);

  return err;
}

/* Finish writing a backup file by overwriting the bogus (ADDB_U5_MAX)
 * horizon with the real one.
 *
 * The active path/fd is copied to the waiting path/fd
 */
int addb_backup_finish(addb_handle* addb, addb_tbk* tbk,
                       unsigned long long horizon) {
  addb_bkh bkh;
  int err;

  cl_assert(addb->addb_cl, -1 != tbk->tbk_a.fd);
  cl_assert(addb->addb_cl, -1 == tbk->tbk_w.fd);

  ADDB_BKH_HORIZON_SET(&bkh, horizon);

  err = addb_file_lseek(addb, tbk->tbk_a.fd, tbk->tbk_a.path,
                        offsetof(addb_bkh, bkh_horizon), SEEK_SET);
  if (err) {
    addb_backup_punt(tbk);
    return err;
  }

  err = addb_file_write(addb, tbk->tbk_a.fd, tbk->tbk_a.path,
                        (char*)bkh.bkh_horizon, sizeof bkh.bkh_horizon);
  if (err) {
    addb_backup_punt(tbk);
    return err;
  }

  cl_log(addb->addb_cl, CL_LEVEL_SPEW, "addb_backup_finish: \"%s\"",
         tbk->tbk_a.path);

  tbk->tbk_w.fd = tbk->tbk_a.fd;
  tbk->tbk_w.path = tbk->tbk_a.path;
  tbk->tbk_a.fd = -1;
  tbk->tbk_a.path = (char*)0;

  return 0;
}

/* Make a backup file available for use by renaming it: *.clx -> *.cln
 * Since empty backup files will be removed by addb_backup_close, we
 * ignore ENOENT errors.
 */
int addb_backup_publish(addb_handle* addb, addb_tbk* tbk) {
  int err;

  cl_assert(addb->addb_cl, -1 == tbk->tbk_w.fd);
  cl_assert(addb->addb_cl, tbk->tbk_w.path);

  cl_log(addb->addb_cl, CL_LEVEL_SPEW, "addb_backup_publish: \"%s\"",
         tbk->tbk_w.path);

  /*
   * Use a non-sync, rename because we know we're going to go off and sync the
   * directory soon
   */
  err = addb_file_rename(addb, tbk->tbk_w.path, tbk->tbk_v_path, false);
  if (err && ENOENT != err) return err;

  tbk->tbk_w.path = (char*)0;
  tbk->tbk_published = 1;

  return 0;
}

/**
 * @brief A published backup file is no longer needed.  Remove it.
 *
 *  If this fails, it's usually because a human has rm -rf'ed the
 *  database behind the server's back.
 *
 * @param addb	module handle
 * @param tbk	tiled backup file to remove.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int addb_backup_unpublish(addb_handle* addb, addb_tbk* tbk) {
  int err = 0;

  if (tbk->tbk_published) {
    err = addb_file_unlink(addb, tbk->tbk_v_path);
    tbk->tbk_published = 0;
    cl_log(addb->addb_cl, CL_LEVEL_SPEW, "addb_backup_unpublish: \"%s\"",
           tbk->tbk_v_path);
  }

  return err;
}

int addb_backup_sync_start(addb_handle* addb, addb_tbk* tbk) {
  cl_assert(addb->addb_cl, -1 != tbk->tbk_w.fd);
  cl_assert(addb->addb_cl, tbk->tbk_w.path);

  cl_log(addb->addb_cl, CL_LEVEL_SPEW, "addb_backup_sync_start: \"%s\"",
         tbk->tbk_w.path);

  return addb_file_sync_start(addb->addb_cl, tbk->tbk_w.fd, &tbk->tbk_fsc,
                              tbk->tbk_w.path, false);
}

int addb_backup_sync_finish(addb_handle* addb, addb_tbk* tbk, bool block) {
  int err;

  cl_assert(addb->addb_cl, -1 != tbk->tbk_w.fd);
  cl_assert(addb->addb_cl, tbk->tbk_w.path);

  err = addb_file_sync_finish(addb->addb_cl, &tbk->tbk_fsc, block,
                              tbk->tbk_w.path);

  cl_log(addb->addb_cl, CL_LEVEL_SPEW, "addb_backup_sync_finish(\"%s\"): %s",
         tbk->tbk_w.path, err ? addb_xstrerror(err) : "ok");

  return err;
}

/**
 * @brief Truncate the active backup file
 *
 *  The file must have been opened earlier with addb_backup_open().
 *
 *  Truncation happens during a rollback when changes are going
 *  to be discarded.
 *
 * @param addb 		addb context
 * @param tbk 		backup context (from containing tiled)
 *
 * @return 0 success, a nonzero error code for error.
 * @return ENOENT	if the backup file didn't exist.
 * @return ECANCELED	if a sync was canceled.
 */
int addb_backup_abort(addb_handle* addb, addb_tbk* tbk) {
  int err = 0;
  int e;

  if (-1 != tbk->tbk_a.fd) {
    cl_log(addb->addb_cl, CL_LEVEL_SPEW, "addb_backup_abort: removing \"%s\"",
           tbk->tbk_a.path);

    e = addb_file_close(addb, tbk->tbk_a.fd, tbk->tbk_a.path);
    if (!err) err = e;

    e = addb_file_unlink(addb, tbk->tbk_a.path);
    if (!err) err = e;

    tbk->tbk_a.fd = -1;
    tbk->tbk_a.path = (char*)0;
  }
  if (-1 != tbk->tbk_w.fd) {
    cl_log(addb->addb_cl, CL_LEVEL_SPEW, "addb_backup_abort: canceling \"%s\"",
           tbk->tbk_w.path);

    err = addb_file_sync_cancel(addb->addb_cl, tbk->tbk_w.fd, &tbk->tbk_fsc,
                                tbk->tbk_w.path);
    if (err && err != ADDB_ERR_MORE) return err;
    err = 0;

    e = addb_file_sync_finish(addb->addb_cl, &tbk->tbk_fsc, true, /* block */
                              tbk->tbk_w.path);
    if (!err && e != 0 && e != ECANCELED) err = e;

    e = addb_file_unlink(addb, tbk->tbk_w.path);
    if (!err) err = e;

    tbk->tbk_w.fd = -1;
    tbk->tbk_w.path = (char*)0;
  }

  return err;
}

/**
 * @brief Stop using a backup file.
 *
 *  If the backup file holds no records, it is also removed.
 */
int addb_backup_close(addb_handle* addb, addb_tbk* tbk,
                      unsigned long long* bytes_written) {
  int err = 0;
  int e;
  off_t off;

  if (-1 == tbk->tbk_w.fd) return ENOENT;

  cl_log(addb->addb_cl, CL_LEVEL_SPEW, "addb_backup_close \"%s\"",
         tbk->tbk_w.path);

  off = lseek(tbk->tbk_w.fd, 0, SEEK_END);
  if (off < 0) {
    err = errno;
    cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "lseek", err,
                 "Unable seek end of backup file: %s", tbk->tbk_w.path);
  }
  *bytes_written = off;

  e = addb_file_close(addb, tbk->tbk_w.fd, tbk->tbk_w.path);
  if (e && !err) err = e;

  /*  Unlink the file if there was no error,
   *  and if we only wrote a header.
   */
  if (!err && sizeof(addb_bkh) == off)
    err = addb_file_unlink(addb, tbk->tbk_w.path);

  tbk->tbk_w.fd = -1;

  return err;
}

/**
 * @brief Read a backup file.
 *
 *  It's okay for the backup file to not exist.  In fact, that is
 *  the usual case.
 *
 * @param addb 		Addb handle for cl
 * @param horizon	We *know* we're sync'ed up to this point.
 *			Ignore backups that go before this state.
 *
 * @return 0 success, a nonzero error code for error.
 * @return ENOENT if the backup file didn't exist.
 */
int addb_backup_read(addb_handle* addb, addb_tiled* td, addb_tbk* tbk,
                     unsigned long long horizon) {
  cl_handle* const cl = addb->addb_cl;
  unsigned long long file_horizon;
  addb_bkh* bkh;
  struct stat st;
  char* mem = MAP_FAILED;
  int err = 0;
  int fd;

  if (-1 == stat(tbk->tbk_v_path, &st)) {
    err = errno;
    if (ENOENT != err)
      cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "stat", err,
                   "Unable to stat backup file: %s", tbk->tbk_v_path);

    return err;
  }

  if (!S_ISREG(st.st_mode)) {
    cl_log(cl, CL_LEVEL_ERROR, "%s is not a file", tbk->tbk_v_path);
    return err;
  }

  fd = open(tbk->tbk_v_path, O_RDONLY, 0666);
  if (-1 == fd) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "open", err,
                 "%s: failed to open backup file for reading", tbk->tbk_v_path);

    return err;
  }

  mem = mmap((void*)0, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
  if (MAP_FAILED == mem) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "mmap", err,
                 "cannot map backup file for reading: %s, %llu",
                 tbk->tbk_v_path, (unsigned long long)st.st_size);
    goto unmap;
  }

  bkh = (addb_bkh*)mem;
  if (memcmp(bkh->bkh_magic, ADDB_BACKUP_MAGIC, 4) != 0) {
    cl_log(cl, CL_LEVEL_ERROR, "%s: unexpected magic; expected %s, got %.4s",
           tbk->tbk_v_path, ADDB_BACKUP_MAGIC, mem);
    err = EINVAL;
    goto unmap;
  }

  file_horizon = ADDB_BKH_HORIZON(bkh);
  if (file_horizon < horizon || ADDB_U5_MAX == file_horizon) {
    /*  This backup brings us back to a state that
     *  we don't need to go back to.
     *
     *  OR (horizon == U5_MAX) this backup file wasn't
     *  completely written and hence can be ignored
     *  because the backed up changes were never made.
     *
     *  Long story:
     *
     *  The horizon that the caller asks for comes from
     *  a file that is saved after all the index horizons
     *  have been saved.
     *
     *  If the application crashes between the index save
     *  and the caller's marker file save, it is possible
     *  that the horizon the outdated marker file asks
     *  for is lower than all the horizons saved by the
     *  index files.  I.e., it's asking us to roll back
     *  further than we can.
     *
     *  In that situation, the istore primitive file and
     *  the gmap index files all have horizons greater
     *  than the requested horizon, and it's safe to ignore
     *  the outdated horizon.  The next thing the caller
     *  does after the failed rollback is to roll forward
     *  again, at least up to the actual horizon.
     */
    if (file_horizon < horizon)
      cl_log(cl, CL_LEVEL_ERROR,
             "%s: backup file horizon %llu "
             "< runtime horizon %llu - "
             "ignoring backup file.",
             tbk->tbk_v_path, file_horizon, horizon);
    else
      cl_log(cl, CL_LEVEL_DEBUG,
             "%s: backup file horizon %llu "
             " == ADDB_U5_MAX - ignoring incomplete "
             "backup file.",
             tbk->tbk_v_path, file_horizon);

    err = addb_file_close(addb, fd, tbk->tbk_v_path);
    if (err) return err;

    err = addb_file_unlink(addb, tbk->tbk_v_path);
    if (err) return err;

    return ENOENT;
  }

  /*  Read read and apply backup records until we run out
   */
  {
    char* const end = mem + st.st_size;
    addb_bkr* bkr = ADDB_BKH_CONTENTS(bkh);

    while ((char*)bkr < end) {
      unsigned long long const offset = ADDB_BKR_OFFSET(bkr);
      unsigned long long const size = ADDB_BKR_SIZE(bkr);
      char* const data = ADDB_BKR_DATA(bkr);

      cl_log(cl, CL_LEVEL_SPEW, "%s: restore offset %llu[%llu] to horizon %llu",
             tbk->tbk_v_path, offset, size, horizon);

      cl_assert(cl, data + size <= end);
      err = addb_tiled_apply_backup_record(td, offset, data, size);
      if (err) {
        cl_log_errno(
            cl, CL_LEVEL_FAIL, "addb_tiled_read_backup_record", err,
            "%s: error while restoring page %llu[%llu] to horizon %llu",
            tbk->tbk_v_path, offset, size, horizon);
        break;
      }
      bkr = (addb_bkr*)(data + size);
    }
  }

unmap : {
  int e;

  if (mem != MAP_FAILED) {
    if (munmap(mem, st.st_size)) {
      e = errno;
      cl_log_errno(cl, CL_LEVEL_ERROR, "munmap", e,
                   "cannot unmap backup file: %s", tbk->tbk_v_path);

      if (!err) err = e;
    }
  }

  e = addb_file_close(addb, fd, tbk->tbk_v_path);
  if (e && !err) err = e;
}

  return err;
}
