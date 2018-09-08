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
#include "libpdb/pdbp.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/*
 *  Lock file:
 *
 *  	A file with three elements:
 *
 *		inode number of the file itself
 *		PID of a running process
 *		hostname
 *
 *      found in a well-defined location. (<database-directory>/LOCK)
 *
 * 	Used to ensure that only one process at a time is
 * 	accessing a database.
 *
 *      Mustn't be NFS-mounted.
 *
 *  Validity of lock files:
 *
 *      Erring on the side of caution:
 *	It's possible that a lock file is accidentally, falsely,
 *	found to be valid; in which case the pdb initialization fails,
 * 	and a human needs to intervene.
 *	That's the safe mode of failure -- the other case, where we
 *	misinterpret a valid lock as nonexistant, would be worse.
 *
 * 	When is a lock valid:
 *
 *		If a file exists, and the inode number in the file matches that
 * 		of the file, and
 *
 *		- the hostname matches the current hostname
 *		- a process of that pid can be kill(pid, 0)'ed,
 *
 *		then a process already holds a lock on that database,
 *		and a second access is denied.
 *
 *	Inode numbers:
 *
 *		If the file exists and the inode number doesn't match that
 * 		of the file, the database is assumed to have been copied from
 *		somewhere else, and the lock is ignored.
 *
 *	Pid:
 *		We assume that we can test whether the owning process is
 *		still running by kill -0'ing the pid.
 *
 *
 *  Replacement of invalid locks:
 *
 *	unlink(REPLACEMENT);
 *
 *	fd := destructively open(REPLACEMENT.$$);
 *	write REPLACEMENT.$$;
 *	rename REPLACEMENT.$$ -> REPLACEMENT
 *
 *	read fl and determine whether the lock is valid.
 *	if it is valid and it's not us, give up.
 *	if it is valid and it's us, we're done.
 *
 *	rename(REPLACMENT, LOCK)
 *
 *		If that fails, either (a) someone else has unlinked
 *		REPLACEMENT between now and when we renamed to it,
 *		or (b) someone has beaten us to the rename.  Which is fine.
 *
 *	if (fd's inode's linkcount isn't 1)
 *	{
 *		The replacement that made it to LOCK isn't us.
 *		Give up.
 *	}
 *
 *  Why not just use file locks:
 *
 *  I don't want to rely on shared locks; one of the systems I want to
 *  run on doesn't have them.  So, we're using "link" and "rename" as
 *  atomic operations instead.
 *
 *  If we see a lock from another machine, we always assume
 *  that the lock is valid - unless the inode number is wrong.
 *
 *  It's a bad idea to use the database from a machine that its
 *  file system isn't native to.  (I.e., don't NFS-mount the
 *  database file system!)
 *
 *  I don't think mmaps will work correctly across NFS mounts, and
 *  I'm not too clear on the atomicity of the operations in this
 *  algorithm either.  I'm trying to degrade nicely, but don't
 *  test it too much.
 */

/* A lockfile.
 */
typedef struct pdb_lockfile {
  char *lock_hostname;
  unsigned long long lock_pid;
  unsigned long long lock_inode;
  ino_t lock_inode_real;

} pdb_lockfile;

/**
 * @brief Get the hostname of the owning system, as defined there.
 *
 *  This name is written out in the database lockfile.  If the
 *  lockfile's host differs from the one of the running process,
 *  the lockfile is old and can be disregarded.
 *
 * @param pdb	The database that's asking, in particular its
 *		allocator and log module handles
 * @param hostname_out	assign the malloc'ed result buffer to this.
 * @return 0 on success, a nonzero error code on error.
 * @return ENOMEM on memory error
 */
static int gethostname_malloc(pdb_handle *pdb, char **hostname_out) {
  size_t hostname_size;
  char *hostname;
  int err;

/*  Dear members of the academy, I'd like to thank the original
 *  authors of gethostname() for imposing an absurdly small limit
 *  on hostname lengths, and the original authors of POSIX for
 *  abolishing the compile time constant and instead replacing it
 *  with a call to sysconf().
 */
#ifdef HOST_NAME_MAX
  hostname_size = HOST_NAME_MAX;
#elif defined(_SC_HOST_NAME_MAX)
  long sysconf_result;

  sysconf_result = sysconf(_SC_HOST_NAME_MAX);
  hostname_size = (sysconf_result == -1L) ? 64 : sysconf_result;
#else
  hostname_size = 64;
#endif

  for (; hostname_size <= (size_t)-1 / 2; hostname_size *= 2) {
    hostname = cm_malloc(pdb->pdb_cm, hostname_size);
    if (gethostname(hostname, hostname_size) == 0) {
      *hostname_out = hostname;
      return 0;
    }

    if ((err = errno) != ENAMETOOLONG) {
      cl_log(pdb->pdb_cl, CL_LEVEL_ERROR, "gethostname fails: %s",
             strerror(err));
      *hostname_out = NULL;
      return err;
    }
    cm_free(pdb->pdb_cm, hostname);
  }
  *hostname_out = 0;
  return ERANGE;
}

static int pdb_lockfile_read(pdb_handle *pdb, char const *lockfile_path,
                             pdb_lockfile **out) {
  int fd;
  ssize_t cc;
  size_t n, i;
  char *lockfile_data;
  struct stat st;
  unsigned long long pid, ino;
  char *sp, *nl;

  if ((fd = open(lockfile_path, O_RDONLY)) == -1) return errno;

  fstat(fd, &st);
  if (st.st_size >= 1024) {
    cl_cover(pdb->pdb_cl);
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_lockfile_read: unexpectedly large lockfile "
           "%s of size %lu???",
           lockfile_path, (unsigned long)st.st_size);
    (void)close(fd);

    return ERANGE;
  }

  n = st.st_size;
  i = 0;

  lockfile_data = cm_malloc(pdb->pdb_cm, st.st_size + 2);
  if (lockfile_data == NULL) {
    close(fd);
    return ENOMEM;
  }

  for (i = 0; i < n + 1; i += cc)
    if ((cc = read(fd, lockfile_data + i, n - i)) <= 0) break;
  if (close(fd) != 0) {
    int err = errno;
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_lockfile_read: error closing lockfile %s: %s", lockfile_path,
           strerror(err));
    cm_free(pdb->pdb_cm, lockfile_data);
    return err;
  }

  if (i != n) {
    cl_cover(pdb->pdb_cl);
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "lockfile \"%s\" changed from size %lu to %lu???", lockfile_path,
           (unsigned long)st.st_size, (unsigned long)i);
    cm_free(pdb->pdb_cm, lockfile_data);
    return ERANGE;
  }

  lockfile_data[i] = '\0';
  if ((sp = strchr(lockfile_data, ' ')) != NULL &&
      (sp = strchr(sp + 1, ' ')) != NULL &&
      (nl = strchr(sp + 1, '\n')) != NULL &&
      sscanf(lockfile_data, "%llu %llu", &pid, &ino) == 2) {
    *nl = '\0';
    sp++;

    *out = cm_malloc(pdb->pdb_cm, sizeof(**out) + 1 + (nl - sp));
    if (*out == NULL) return ENOMEM;

    (*out)->lock_hostname = (char *)(*out + 1);
    memcpy((*out)->lock_hostname, sp, (nl + 1) - sp);

    (*out)->lock_inode = ino;
    (*out)->lock_pid = pid;
    (*out)->lock_inode_real = st.st_ino;

    cm_free(pdb->pdb_cm, lockfile_data);
    return 0;
  }
  cl_cover(pdb->pdb_cl);
  cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
         "\"%s\": unexpected lockfile contents  \"%s\" "
         "-- expected two numbers and a hostname\n",
         lockfile_path, lockfile_data);
  cm_free(pdb->pdb_cm, lockfile_data);

  return PDB_ERR_DATABASE;
}

static int pdb_lockfile_write(pdb_handle *pdb, char const *lockfile_path,
                              char const *hostname, pid_t pid, int *fd_out) {
  int fd;
  ssize_t cc;
  size_t n, i;
  int err;
  struct stat st;
  char *lockfile_data;
  size_t lockfile_data_size;

  *fd_out = -1;

  lockfile_data_size = strlen(hostname) + 2 * 42 + 10;
  lockfile_data = cm_malloc(pdb->pdb_cm, lockfile_data_size);
  if (lockfile_data == NULL) {
    err = errno ? errno : ENOMEM;
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_lockfile_write(%s): failed to allocate %llu bytes "
           "for lockfile data: %s",
           lockfile_path, (unsigned long long)lockfile_data_size,
           strerror(err));
    return err;
  }

  fd = open(lockfile_path, O_WRONLY | O_TRUNC | O_CREAT, 0666);
  if (fd == -1) {
    err = errno;
    cl_cover(pdb->pdb_cl);
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_lockfile_write: cannot create or open "
           "temporary lockfile \"%s\": %s",
           lockfile_path, strerror(err));
    cm_free(pdb->pdb_cm, lockfile_data);

    return err;
  }
  if (fstat(fd, &st) != 0) {
    err = errno;

    cl_cover(pdb->pdb_cl);
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_lockfile_write: "
           "cannot stat temporary lockfile \"%s\": %s",
           lockfile_path, strerror(err));
    (void)close(fd);
    unlink(lockfile_path);
    cm_free(pdb->pdb_cm, lockfile_data);

    return err;
  }

  snprintf(lockfile_data, lockfile_data_size, "%llu %llu %s\n",
           (unsigned long long)pid, (unsigned long long)st.st_ino, hostname);

  n = strlen(lockfile_data);
  i = 0;

  while (i < n) {
    cc = write(fd, lockfile_data + i, n - i);
    if (cc <= 0) {
      err = errno ? errno : EINVAL;
      cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
             "pdb_lockfile_write: failed to write "
             "lockfile file \"%s\": %s",
             lockfile_path, strerror(errno));
      (void)close(fd);
      (void)unlink(lockfile_path);

      errno = err;
      cm_free(pdb->pdb_cm, lockfile_data);

      return err;
    }
    i += cc;
    cl_cover(pdb->pdb_cl);
  }

  *fd_out = fd;
  cm_free(pdb->pdb_cm, lockfile_data);

  return 0;
}

int pdb_lockfile_create(pdb_handle *pdb, char const *lockfile_path) {
  char *unique_path = NULL;
  char *replacement_path = NULL;
  size_t suffixed_size;
  int retry = 3;
  int err = 0, lockfile_fd = -1;
  pdb_lockfile *lockfile = NULL;
  char *hostname = NULL;
  struct stat st;

  cl_enter(pdb->pdb_cl, CL_LEVEL_SPEW, "%s", lockfile_path);

  cl_assert(pdb->pdb_cl, lockfile_path != NULL);

  if ((err = gethostname_malloc(pdb, &hostname)) != 0) {
    cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "%s malloc failed", lockfile_path);
    return err;
  }
  cl_assert(pdb->pdb_cl, hostname != NULL);

  suffixed_size = strlen(lockfile_path) + 80;
  if ((unique_path = cm_malloc(pdb->pdb_cm, suffixed_size)) == NULL) {
    err = errno ? errno : ENOMEM;
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb: failed to allocate %lu bytes for unique file "
           "name buffer for lockfile \"%s\": %s",
           (unsigned long)suffixed_size, lockfile_path, strerror(errno));
    cm_free(pdb->pdb_cm, hostname);

    cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "malloc for unique_path failed");

    return err;
  }
  if ((replacement_path = cm_malloc(pdb->pdb_cm, suffixed_size)) == NULL) {
    err = errno ? errno : ENOMEM;
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb: failed to allocate %lu bytes for "
           "replacement file name buffer: %s",
           (unsigned long)suffixed_size, strerror(errno));
    cm_free(pdb->pdb_cm, unique_path);
    cm_free(pdb->pdb_cm, hostname);

    cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "malloc for replacement path failed");

    return err;
  }

  snprintf(unique_path, suffixed_size, "%s-%lu", lockfile_path,
           (unsigned long)getpid());
  snprintf(replacement_path, suffixed_size, "%s-REPLACEMENT", lockfile_path);

  /* Write a lockfile to LOCK.$$.  Keep the file descriptor around.
   */
  err = pdb_lockfile_write(pdb, unique_path, hostname, getpid(), &lockfile_fd);

  if (err != 0) {
    (void)unlink(unique_path);

    cm_free(pdb->pdb_cm, unique_path);
    cm_free(pdb->pdb_cm, replacement_path);
    cm_free(pdb->pdb_cm, hostname);

    cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "pdb_lockfile_write fails: %s",
             strerror(err));

    return err;
  }

  /*  Now that we definitely have a file, check that it's on the
   *  local system - we don't want to run a database server on a
   *  remote database.
   */
  if (pdb_is_remote_mounted(unique_path)) {
    char const *dir_s = unique_path, *dir_e;

    if ((dir_e = strrchr(dir_s, '/')) == NULL) dir_e = (dir_s = ".") + 1;

    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb: \"%.*s\": "
           "database directory can't be remote-mounted.",
           (int)(dir_e - dir_s), dir_s);

    (void)unlink(unique_path);

    cm_free(pdb->pdb_cm, unique_path);
    cm_free(pdb->pdb_cm, replacement_path);
    cm_free(pdb->pdb_cm, hostname);

#ifdef EREMOTE
    return EREMOTE;
#else
    return EINVAL;
#endif
  }

  for (;;) {
    cl_assert(pdb->pdb_cl, hostname != NULL);
    cl_assert(pdb->pdb_cl, replacement_path != NULL);
    cl_assert(pdb->pdb_cl, lockfile_path != NULL);
    cl_assert(pdb->pdb_cl, unique_path != NULL);
    cl_assert(pdb->pdb_cl, lockfile_fd >= 0);

    if (retry-- <= 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
             "pdb: too many retries -- giving up on "
             "acquiring lock \"%s\"",
             lockfile_path);
      err = EBUSY;
      break;
    }

    /*  Try acquiring the lock by linking to the fixed lock
     *  filename.  The link fails if the destination exists.
     */

    if (link(unique_path, lockfile_path) == 0) {
      /*  We have the lock.  Yay.
       */
      cl_cover(pdb->pdb_cl);
      err = 0;

      break;
    }

    /*	unlink(REPLACEMENT);
     *
     * 	We expect this to either (a) succeed or (b) fail
     *	because the replacement file doesn't exist.
     * 	Any other sort of failure is a problem.
     */
    if (unlink(replacement_path) != 0 && errno != ENOENT) {
      err = errno;
      cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
             "pdb: failed "
             "to unlink replacement file \"%s\": %s",
             replacement_path, strerror(err));
      break;
    }

    /* 	rename LOCK.$$ -> REPLACEMENT
     */
    if (rename(unique_path, replacement_path) != 0) {
      err = errno;

      /*  This shouldn't fail - the most likely reasons
       *  for it to fail are bad permissions for the directory
       *  or a mysteriously "vanished" unique file.  In both
       *  cases, something is very, very wrong.
       */
      cl_log(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb: failed to rename %s to %s: %s",
             unique_path, replacement_path, strerror(err));
      break;
    }

    /* 	read LOCK and determine whether the lock is valid.
     */

    err = pdb_lockfile_read(pdb, lockfile_path, &lockfile);

    /* (1) lockfile couldn't be read because it doesn't exist
     *	-> retry
     */
    if (err == ENOENT) {
      continue;
    }

    /* (2) lockfile couldn't be read because of some other
     * 	problem -> error
     */
    if (err != 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
             "pdb_lockfile_create: pdb_lockfile_read "
             "fails: %s",
             strerror(err));
      break;
    }

    cl_assert(pdb->pdb_cl, lockfile != NULL);

    /* (3)  lockfile is valid, and it is me -- someone else
     * 	did the rename for us!
     */
    if (lockfile->lock_pid == getpid() &&
        lockfile->lock_inode_real == lockfile->lock_inode &&
        strcasecmp(lockfile->lock_hostname, hostname) == 0) {
      fstat(lockfile_fd, &st);

      if (st.st_nlink == 1 && st.st_ino == lockfile->lock_inode) {
        err = 0;
        break;
      }
    }

    /* (4) lockfile is on the same machine, and is someone else,
     * 	and we can kill -0 that other process and get something
     *	other than ESRCH; or lockfile is on another machine.
     *	-> give up.
     */

    if (lockfile->lock_inode == lockfile->lock_inode_real &&
        strcasecmp(lockfile->lock_hostname, hostname) == 0 &&
        (kill(lockfile->lock_pid, 0) == 0 || errno != ESRCH)) {
      err = EBUSY;
      break;
    }

    /*  (5) Lockfile is invalid.
     */
    if (!pdb_transactional(pdb)) {
      cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
             "pdb_lockfile_create: stale lock-file detected, "
             "database is probably corrupted");
      err = ENODATA;
      break;
    } else if (rename(replacement_path, lockfile_path) != 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
             "pdb_lockfile_create: rename fails: %s", strerror(err));

      /*  It's okay for this to fail because the
       *  replacement_path doesn't exist; maybe a
       *  parallel process did our rename for us.
       *
       *  That's okay; all we want is for our file to
       *  make it to lockfile_path; we don't care
       *  who moves it!
       */
      if (errno != ENOENT) {
        err = errno;
        break;
      }
    }

    if (fstat(lockfile_fd, &st) != 0) {
      err = errno;
      cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
             "pdb: cannot fstat \"%s\" (nee \"%s\"): %s", lockfile_path,
             unique_path, strerror(err));
    } else if (st.st_nlink == 1) {
      err = 0;
    } else {
      cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
             "pdb: parallel attempts at acquiring %s", lockfile_path);
      err = EBUSY;
    }

    break;
  }

  (void)close(lockfile_fd);
  (void)unlink(unique_path);

  cm_free(pdb->pdb_cm, unique_path);
  cm_free(pdb->pdb_cm, replacement_path);
  cm_free(pdb->pdb_cm, hostname);

  if (lockfile != NULL) cm_free(pdb->pdb_cm, lockfile);

  cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW, "%s", err ? strerror(err) : "ok");
  return err;
}

int pdb_lockfile_rewrite(pdb_handle *pdb, char const *lockfile_path,
                         pid_t pid) {
  char *unique_path = NULL;
  size_t suffixed_size;
  int err = 0, lockfile_fd = -1;
  char *hostname = NULL;

  cl_assert(pdb->pdb_cl, lockfile_path != NULL);

  cl_log(pdb->pdb_cl, CL_LEVEL_DEBUG, "pdb: rewrite lockfile %s to pid %lu",
         lockfile_path, (unsigned long)pid);

  if ((err = gethostname_malloc(pdb, &hostname)) != 0) return err;
  cl_assert(pdb->pdb_cl, hostname != NULL);

  suffixed_size = strlen(lockfile_path) + 80;
  if ((unique_path = cm_malloc(pdb->pdb_cm, suffixed_size)) == NULL) {
    err = errno ? errno : ENOMEM;
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb: failed to allocate %lu bytes for unique file "
           "name buffer for lockfile \"%s\": %s",
           (unsigned long)suffixed_size, lockfile_path, strerror(errno));
    cm_free(pdb->pdb_cm, hostname);

    return err;
  }
  snprintf(unique_path, suffixed_size, "%s-%lu", lockfile_path,
           (unsigned long)getpid());

  /* Write a lockfile to LOCK.$$.
   */
  err = pdb_lockfile_write(pdb, unique_path, hostname, pid, &lockfile_fd);
  if (err != 0) {
    (void)unlink(unique_path);

    cm_free(pdb->pdb_cm, unique_path);
    cm_free(pdb->pdb_cm, hostname);

    return err;
  }

  /*  Rename that file to the proper lockfile name.
   */

  if (rename(unique_path, lockfile_path) != 0) {
    err = errno;
    (void)unlink(unique_path);

    /*  This shouldn't fail - the most likely reasons
     *  for it to fail are bad permissions for the directory
     *  or a mysteriously "vanished" unique file.  In both
     *  cases, something is very, very wrong.
     */
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb: failed to rename %s to %s: %s",
           unique_path, lockfile_path, strerror(err));
  }

  (void)close(lockfile_fd);

  cm_free(pdb->pdb_cm, unique_path);
  cm_free(pdb->pdb_cm, hostname);

  return err;
}
