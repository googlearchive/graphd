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
#define CL_LEVEL_FILE_SYNC CL_LEVEL_DEBUG
#define _XOPEN_SOURCE 600 /* needed for posix_fadvise */

#define _GNU_SOURCE /* needed for sched_setaffinity*/

#define _DARWIN_C_SOURCE /* needed for getrlimit(RLIMIT_NPROC...) on OS X */

#include "libaddb/addbp.h"
#include <stdio.h>
#include <stdbool.h>

#undef _POSIX_C_SOURCE
#include <unistd.h>
#include <aio.h>
#include <errno.h>
#include <string.h>
#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <string.h>
#include <limits.h>

#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/resource.h>

#ifdef __linux__
#define HAVE_SCHED_SETAFFINITY
#endif

#ifndef _D_EXACT_NAMLEN
#define _D_EXACT_NAMLEN(de) strlen((de)->d_name)
#endif

int addb_file_sabotage(cl_handle* cl, char const* file, int line,
                       char const* func) {
  static bool tried = false;
  static unsigned long countdown = 0;

/*  This is where we go into our treasure trove
 *  of errors least likely to be returned by graphd.
 */
#ifdef EBADMACHO
  static int result = EBADMACHO;
#elif EPWROFF
  static int result = EPWROFF;
#else
  static int result = ENOEXEC;
#endif

  if (!tried) {
    char const* s = getenv("GRAPHD_SABOTAGE");

    if (s == NULL || sscanf(s, "%lu:%d", &countdown, &result) < 1)
      countdown = 0;
    tried = true;
  }

  if (countdown > 0 && !--countdown) {
    cl_log(cl, CL_LEVEL_ERROR,
           "sabotage: returning error %d (%s) from %s in %s:%d", result,
           strerror(result), func, file, line);
    return result;
  }
  return 0;
}

#define SABOTAGE(cl)                                                      \
  do {                                                                    \
    int sabot_err = addb_file_sabotage(cl, __FILE__, __LINE__, __func__); \
    if (sabot_err != 0) return sabot_err;                                 \
  } while (0)

/*  File utilities
 *
 *  These are mostly simple wrappers around basic posix routines
 *  with some logging added in case of error.
 */

/**
 * @brief Read n bytes from the current position of fd into s.
 *
 * @param addb 		database we're doing this for.
 * @param fd 		file descriptor to read from
 * @param name 		name of the underlying resource, for error messages.
 * @param s 		buffer to read to
 * @param n 		number of bytes to read..
 * @param expect_eof 	is it okay if the requested data can't be read?
 *
 * @return 0 on success
 * @return ADDB_ERR_NO if the file ends at the beginning of the read.
 * @return ERANGE if the file ends in mid-read
 * @return other nonzero error numbers on system error.
 */

int addb_file_read(addb_handle* addb, int fd, char const* name, char* s,
                   size_t n, bool expect_eof) {
  int err = 0;
  ssize_t cc;
  size_t off = 0;

  SABOTAGE(addb->addb_cl);

  for (off = 0; off < n; off += cc) {
    if ((cc = read(fd, s + off, n - off)) <= 0) {
      if (cc < 0) {
        err = errno ? errno : -1;
        cl_log(addb->addb_cl, CL_LEVEL_FAIL, "addb: read(%s, %lu): %s [%s:%d]",
               name, (unsigned long)n, strerror(errno), __FILE__, __LINE__);
      } else {
        if (expect_eof && off == 0)
          err = ADDB_ERR_NO;
        else {
          cl_log(addb->addb_cl, CL_LEVEL_FAIL,
                 "addb: read(%s, %lu): "
                 "premature EOF [%s:%d]",
                 name, (unsigned long)n, __FILE__, __LINE__);
          err = ERANGE;
        }
      }
      cl_cover(addb->addb_cl);
      break;
    }
  }

  cl_cover(addb->addb_cl);
  return err;
}

/**
 * @brief Seek to a position.  Log if anything goes wrong.
 *
 * @param addb 	database that asks for it
 * @param fd	file descriptor to seek on
 * @param name	name of hte underlying resource
 * @param off	offset to seek to
 * @param whence direction to seek into - SEEK_CUR, SEEK_SET, or SEEK_END.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
int addb_file_lseek(addb_handle* addb, int fd, char const* name, off_t off,
                    int whence) {
  int err = 0;

  SABOTAGE(addb->addb_cl);

  if (lseek(fd, off, whence) < 0) {
    err = errno ? errno : -1;
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: lseek(%s, off=%lld, %d): %s [%s:%d]", name, (long long)off,
           whence, strerror(errno), __FILE__, __LINE__);
  }
  cl_cover(addb->addb_cl);
  return err;
}

/**
 * @brief Truncate at a position.  Log if anything goes wrong.
 *
 * @param addb 	database we're doing this for
 * @param fd	file descriptor to seek on
 * @param name	name of the underlying resource
 * @param off	offset to truncate to
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
int addb_file_truncate(addb_handle* addb, int fd, char const* name, off_t off) {
  int err = 0;

  SABOTAGE(addb->addb_cl);

  if (ftruncate(fd, off) < 0) {
    err = errno ? errno : -1;
    cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "ftruncate", err,
                 "addb: ftruncate(%s, off=%lld)", name, (long long)off);
  }
  cl_cover(addb->addb_cl);
  return err;
}

/**
 * @brief Rename a file; log if anything goes wrong.
 *
 * @param addb 	 database we're doing this for
 * @param source rename from this name
 * @param target rename to this name
 * @param sync	 fsync after the rename?
 *
 * @return 0 on success, a nonzero error code on error.
 */
int addb_file_rename(addb_handle* addb, char const* source, char const* target,
                     bool sync) {
  int err = 0;
  int dir_fd;
  char dirname[PATH_MAX];
  char* last_slash;

  SABOTAGE(addb->addb_cl);

  if (rename(source, target) < 0) {
    err = errno ? errno : -1;
    cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "rename", err,
                 "source: %s target: %s", source, target);
    return err;
  }

#if ADDB_FSYNC_DIRECTORY
  if (!sync)
#endif
    return 0;

  /*
   * Calculate the directory that target is in and sync it
   */
  last_slash = strrchr(target, '/');

  if (last_slash) {
    if (last_slash - target > PATH_MAX) {
      cl_log(addb->addb_cl, CL_LEVEL_FAIL,
             "addb_file_rename: path %s is too "
             "long or malformed."
             "Can't sync directory",
             target);
      return EINVAL;
    }

    memcpy(dirname, target, last_slash - target);
    dirname[last_slash - target] = 0;

    dir_fd = open(dirname, O_RDONLY);
    if (dir_fd < 0) {
      err = errno;
      cl_log_errno(addb->addb_cl, CL_LEVEL_FAIL, "open", err,
                   "can't open directory %s", dirname);
      return err;
    }
  } else {
    dir_fd = open(".", O_RDONLY);
    if (dir_fd < 0) {
      err = errno;
      cl_log_errno(addb->addb_cl, CL_LEVEL_FAIL, "open", err,
                   "Can't open CWD as '.'");
      return err;
    }
  }

  cl_assert(addb->addb_cl, dir_fd >= 0);

  if (fsync(dir_fd)) {
    err = errno;
    close(dir_fd);
    cl_log_errno(addb->addb_cl, CL_LEVEL_FAIL, "fsync", err,
                 "Can't fsync descriptor %i", dir_fd);

    return err;
  }
  close(dir_fd);

  return 0;
}

/**
 * @brief Write bytes; log a failure.
 *
 * @param addb 	database we're doing this for
 * @param fd	file descriptor to seek on
 * @param name	name of the underlying resource
 * @param s	bytes to write
 * @param n	number of bytes to write
 *
 * @return 0 on success, a nonzero error code on error.
 */
int addb_file_write(addb_handle* addb, int fd, char const* name, char const* s,
                    size_t n) {
  size_t off = 0;
  size_t cc;

  SABOTAGE(addb->addb_cl);

  for (off = 0; off < n; off += cc) {
    if ((cc = write(fd, s + off, n - off)) <= 0) {
      int err = errno ? errno : -1;
      cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "write", err,
                   "addb_file_write(%s, %lu)", name, (unsigned long)(n - off));
      return err;
    }
    cl_cover(addb->addb_cl);
  }

  return 0;
}

/**
 * @brief Flush file descriptor writes to disk.  Log if anything goes wrong.
 */
int addb_file_sync(addb_handle* addb, int fd, char const* name) {
  int err = 0;

  cl_assert(addb->addb_cl, fd != -1);
  SABOTAGE(addb->addb_cl);

#if defined(_POSIX_SYNCHRONIZED_IO) && _POSIX_SYNCHRONIZED_IO > 0
  if (fdatasync(fd) != 0) {
    err = errno;
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: failed to flush %s to disk: %s", name, strerror(err));
  }
#else
#if defined(F_FULLFSYNC)
  if (fcntl(fd, F_FULLFSYNC, 0) != 0)
#else
  if (fsync(fd) != 0)
#endif
  {
    err = errno;
    cl_log(addb->addb_cl, CL_LEVEL_ERROR,
           "addb: failed to flush %s to disk: %s", name, strerror(err));
  }
#endif

  return err;
}

/* Unlink <name> for <addb>.
 * Log if anything goes wrong.
 */
int addb_file_unlink(addb_handle* addb, char const* name) {
  int err = 0;

  SABOTAGE(addb->addb_cl);
  if (unlink(name) < 0) {
    err = errno ? errno : -1;
    cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "unlink", err,
                 "can not remove %s", name);
  }
  cl_cover(addb->addb_cl);
  return err;
}

int addb_file_munmap(cl_handle* cl, char const* name, char* ptr, size_t size) {
  int err = 0;

  SABOTAGE(cl);
  cl_log(cl, CL_LEVEL_SPEW, "munmap %p[%lu]", (void*)ptr, (unsigned long)size);

  if (munmap(ptr, size) == -1) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "munmap", err, "addb: munmap %p[%lu]",
                 (void*)ptr, (unsigned long)size);
  }
  cl_cover(cl);
  return err;
}

int addb_file_close(addb_handle* addb, int fd, char const* name) {
  int err = 0;

  SABOTAGE(addb->addb_cl);
  cl_assert(addb->addb_cl, fd >= 0);
  if (fd >= 0 && close(fd) < 0) {
    err = errno ? errno : -1;
    cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "close", err,
                 "addb_file_close \"%s\" fails", name);
  }
  cl_cover(addb->addb_cl);
  return err;
}

int addb_file_mkdir(addb_handle* addb, char const* name) {
  int err;
  SABOTAGE(addb->addb_cl);
  if (mkdir(name, 0755) < 0) {
    err = errno ? errno : -1;
    cl_log(addb->addb_cl, CL_LEVEL_ERROR, "addb: mkdir \"%s\" fails: %s", name,
           strerror(errno));
    return err;
  }
  cl_cover(addb->addb_cl);
  return 0;
}

int addb_file_next(addb_handle* addb, char const* name, char** filename_inout,
                   void** pointer_inout) {
  int err = 0;
  struct dirent* de;

  SABOTAGE(addb->addb_cl);
  if (name != NULL) {
    if (!*pointer_inout && !(*pointer_inout = opendir(name))) {
      cl_log(addb->addb_cl, CL_LEVEL_ERROR, "addb: opendir \"%s\" fails: %s",
             name, strerror(errno));
      cl_cover(addb->addb_cl);
      return errno ? errno : -1;
    }

    if (!(de = readdir((DIR*)*pointer_inout))) {
      err = errno ? errno : -1;
      cl_cover(addb->addb_cl);
    } else {
      size_t size = strlen(name) + _D_EXACT_NAMLEN(de) + 2;
      *filename_inout = cm_realloc(addb->addb_cm, *filename_inout, size);
      if (!*filename_inout)
        err = ENOMEM;
      else
        snprintf(*filename_inout, size, "%s/%.*s", name,
                 (int)_D_EXACT_NAMLEN(de), de->d_name);

      cl_cover(addb->addb_cl);
    }
  }

  if (err || !name) {
    if (*filename_inout) {
      cm_free(addb->addb_cm, *filename_inout);
      *filename_inout = 0;
    }
    if (*pointer_inout) {
      (void)closedir((DIR*)*pointer_inout);
      *pointer_inout = NULL;
    }
  }
  cl_cover(addb->addb_cl);
  return err;
}

/*  Given a file descriptor opened for (at least) writing, make sure
 *  the underlying file is at least of a certain size.
 */
int addb_file_grow(cl_handle* cl, int fd, char const* name, off_t size) {
  struct stat st;

  SABOTAGE(cl);
  if (fstat(fd, &st) != 0) {
    int err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "fstat", err, "addb: stat \"%s\" failed",
                 name);
    cl_cover(cl);
    return err;
  }
  if (st.st_size >= size) {
    cl_cover(cl);
    return 0;
  }

  cl_assert(cl, size >= 1);

  if (lseek(fd, size - 1, SEEK_SET) == -1) {
    int err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "lseek", err,
                 "addb: lseek %s, %llu failed", name,
                 (unsigned long long)size - 1);
    return err;
  }

  /* Write a single byte to make the file size stick. */
  if (write(fd, "", 1) != 1) {
    int err = errno;
    cl_log(cl, CL_LEVEL_ERROR, "addb: write %s at offset #%llu failed", name,
           (unsigned long long)size);
    return err;
  }

  cl_log(cl, CL_LEVEL_DEBUG, "addb: growing \"%s\" to %llu bytes", name,
         (unsigned long long)size);
  cl_cover(cl);

  return 0;
}

/* Advice for log-structured files: write them asap
 */
int addb_file_advise_log(cl_handle* cl, int fd, char const* filename) {
  int err = 0;

  /* 	The code doesn't quite do what we hoped - advise the operating
   *	system to not bother keeping file contents around for further
   *  	READs.
   *
   *	if (posix_fadvise( fd, 0, 0, POSIX_FADV_DONTNEED ))
   *	{
   *		err = errno;
   *		cl_log_errno( cl, CL_LEVEL_ERROR, "posix_fadvise", err,
   *			"advise DONTNEED \"%s\" failed", filename );
   *	}
   *
   * 	If anyone ever comes up with a way of hinting at that,
   *	here's the place to put it!
   */

  return err;
}

/* Advice for index files: keep them in memory, support random access
 */
int addb_file_advise_random(cl_handle* cl, int fd, char const* filename) {
  int err = 0;

#ifdef POSIX_FADV_WILLNEED
  if ((err = posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED)) != 0) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "posix_fadvise", err,
                 "advise WILLNEED \"%s\" failed", filename);
  }

  if ((err = posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM)) != 0) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "posix_fadvise", err,
                 "advise RANDOM \"%s\" failed", filename);
  }
#endif /* POSIX_FADV_WILLNEED */

  return err;
}

static void* do_pthread_fsync(void* arg) {
  addb_fsync_ctx* fsc = arg;

#ifdef HAVE_SCHED_SETAFFINITY
  unsigned long cpu_mask = 0xffffffff;
  /*
   * Unbind us from the first processor.
   */
  sched_setaffinity(0, sizeof cpu_mask, (cpu_set_t*)&cpu_mask);
/*
 * If this doesn't work, that's okay
 */
#endif

#if ADDB_HAVE_FDATASYNC
  if (fdatasync(fsc->fsc_fd))
#else
  if (fsync(fsc->fsc_fd))
#endif
  {
    fsc->fsc_done = 1;
    return (void*)((long)errno);
  }
  fsc->fsc_done = 1;
  return (void*)(0);
}

/**
 * @brief Initialize an asynchronous fsync operation context.
 *
 *  After a call to addb_file_sync_initialize, it is
 *  possible to call addb_file_sync_end on the fsc
 *  parameter and not have that do anything.
 *
 * @param fsc		save state here
 */
void addb_file_sync_initialize(addb_handle* addb, addb_fsync_ctx* fsc) {
  fsc->fsc_addb = addb;
  fsc->fsc_thread = 0;
  fsc->fsc_fd = -1;
}

/**
 * @brief Start an asynchronous fsync operation
 *
 * @param cl	 	log through this
 * @param fd		file descriptor to sync
 * @param cb		save state here
 * @param filename	for documentation: pathname of fd
 * @param is_directory	is this a directory?  Some systems can't sync that.
 */
int addb_file_sync_start(cl_handle* cl, int fd, addb_fsync_ctx* fsc,
                         char const* filename, bool is_directory) {
  SABOTAGE(cl);

  int err;

  /*
   * If we have a thread for this, we're already trying to sync
   * it
   */
  if (fsc->fsc_thread) return 0;

#if !ADDB_FSYNC_DIRECTORY
  if (is_directory) {
    fsc->fsc_fd = -1;
    return 0;
  }
#endif

  fsc->fsc_fd = fd;
  fsc->fsc_guard = 0;

  /*
   * fsc_done is set to 1 by the thread before
   * termination. We can use this to check if the thread has completed
   * in a non-blocking manner.  This is safe because and does not require
   * a lock because their are no read-modify-write dependencies.
   *
   * If the child sets the flag, we'll _eventually_ see the new value
   * and we _know_ that the child will exit soon.
   */
  fsc->fsc_done = 0;

#ifdef RLIMIT_NPROC
  /* RLIMIT_NPROC doesn't exist on older *NIX and Solaris */
  if (fsc->fsc_addb->addb_fsync_started == 0) {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NPROC, &rl) != 0)
      cl_log_errno(cl, CL_LEVEL_ERROR, "setrlimit", errno,
                   "cannot get RLIMIT_NPROC [ignored]");

    else if (rl.rlim_cur < rl.rlim_max) {
      rl.rlim_cur = rl.rlim_max;
      if (setrlimit(RLIMIT_NPROC, &rl) != 0)
        cl_log_errno(cl, CL_LEVEL_ERROR, "setrlimit", errno,
                     "cannot set RLIMIT_NPROC to "
                     "rl_max %llu [ignored]",
                     (unsigned long long)rl.rlim_cur);
      else
        cl_log(cl, CL_LEVEL_DEBUG, "increased RLIMIT_NPROC to %llu",
               (unsigned long long)rl.rlim_cur);
    }
  }
#endif
  /*
   * Yes, pthread functions return an error code and do not set errno
   */
  err = pthread_create(&fsc->fsc_thread, NULL, do_pthread_fsync, fsc);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "pthread_create", err,
                 "Unable to clone process to fsync( %d ) \"%s\" "
                 "(started: %zu; finished: %zu)",
                 fsc->fsc_fd, filename, fsc->fsc_addb->addb_fsync_started,
                 fsc->fsc_addb->addb_fsync_finished);
    fsc->fsc_fd = -1;
    return err;
  }
  fsc->fsc_addb->addb_fsync_started++;
  cl_log(cl, CL_LEVEL_FILE_SYNC,
         "addb_file_sync_start: file %s begins (started: %zu, done %zu)",
         filename, fsc->fsc_addb->addb_fsync_started,
         fsc->fsc_addb->addb_fsync_finished);
  return 0;
}

/**
 * @brief Complete an asynchronous fsync operation
 *
 * 	Call this repeatedly with the cb from addb_file_sync_start
 * 	until it returns something other than ADDB_ERR_MORE
 *
 * @param cl		log through this
 * @param cb		asynchronous IO operation to complete
 * @param block		if true, don't just check - wait until it's finished!
 * @param filename	for documentation, which file are we waiting for?
 *
 * @return 0 on successful completion.
 * @return ADDB_ERR_MORE if the operation is still going on
 * @return other nonzero errors on system error.
 */
int addb_file_sync_finish(cl_handle* cl, addb_fsync_ctx* fsc, bool block,
                          char const* filename) {
  int err;
  long thread_errno;
  void* ptr;

  /*
   * You're trying to finish something that either
   * hasn't started yet or already finished. That's okay.
   */
  if (fsc->fsc_thread == 0 || fsc->fsc_fd == -1) return 0;

  if (!block) {
    if (!fsc->fsc_done) {
      cl_log(cl, CL_LEVEL_VERBOSE, "addb_file_sync_finish: %s is not yet done",
             filename);

      return ADDB_ERR_MORE;
    }
  }

  /*
   * This sync thread returns the error code from fsync or fdatasync
   */
  ptr = &thread_errno;
  err = pthread_join(fsc->fsc_thread, (void**)ptr);
  fsc->fsc_thread = 0;

  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "pthread_join", err,
                 "Unable to block on thread for fd %i", fsc->fsc_fd);
    return err;
  }

  fsc->fsc_addb->addb_fsync_finished++;
  cl_log(cl, CL_LEVEL_FILE_SYNC,
         "addb_file_sync_finish: file %s ends (started: %zu, done %zu)",
         filename, fsc->fsc_addb->addb_fsync_started,
         fsc->fsc_addb->addb_fsync_finished);

  if (thread_errno) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "do_cloned_sync", thread_errno,
                 "sync thread reports error syncing %i", fsc->fsc_fd);
    return EIO;
  }
  cl_assert(cl, fsc->fsc_done);

  cl_log(cl, CL_LEVEL_VERBOSE, "addb_file_sync_finish: file %s finished",
         filename);

  return 0;
}

int addb_file_sync_cancel(cl_handle* cl, int fd, addb_fsync_ctx* fsc,
                          char const* filename) {
  int err;

  if (fsc->fsc_thread == 0) {
    cl_log(cl, CL_LEVEL_FILE_SYNC,
           "Tried to cancel a thread that already finished");
    return 0;
  }

  /*
   * I no longer care what happens to this thread
   */
  err = pthread_detach(fsc->fsc_thread);

  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "pthread_detach", err,
                 "Can't detach from thread at %p for fd %i",
                 (void*)(size_t)(fsc->fsc_thread), fsc->fsc_fd);
    fsc->fsc_thread = 0;
    return err;
  }

  fsc->fsc_thread = 0;

  return 0;
}

int addb_file_fstat(cl_handle* cl, int fd, char const* path, struct stat* buf) {
  int err;

  if (fstat(fd, buf) == 0) return 0;

  err = errno;
  cl_log_errno(cl, CL_LEVEL_ERROR, "fstat", err, "fd=%d (\"%s\")", fd, path);
  return err;
}
