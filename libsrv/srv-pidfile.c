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
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>
#include <fcntl.h>
#include <unistd.h>

#include "srvp.h"

/*
 *  PID file:
 *
 *  	A file with the PID of a running server, found in a
 * 	well-defined location (-p when starting a server).
 * 	Used to
 * 	(a) ensure that only one server runs at a time,
 *	(b) find that server process to shut it down
 *	    (when starting a server with -z).
 *
 *	If a file exists and a process of that pid can be
 * 	kill(pid, 0)'ed, the existing process is legitimate,
 *	and another process must refuse to start.
 *
 *
 *  Janitorial lock:
 *
 *	A second file, name derived from the PID file.
 *	(pidfile.CLEANUP).
 *
 *	The process successfully exclusively creating
 *	the janitorial lock file holds the janitorial lock.
 *
 *	A pid file can be removed by a process who
 *	holds the janitorial lock if either:
 *
 *	- the caller has the PID in the pid file.
 *	- the process with the PID in the pid file doesn't
 *	  exist anymore (kill(.., 0) returns ESRCH).
 *
 * 	This is to ensure that multiple secondary servers
 *	don't start up at the same time.
 *
 *
 *  Implementor notes:
 *
 *  The "lock against multiple parallel writes" functionality
 *  should very much move into the pdb subdirectory.
 *
 *  I don't want to rely on shared locks, some systems don't
 *  have them.  So, we're using "link" as the atomic operation
 *  du jour.
 *
 *  This does *not* lock across machine boundaries.
 *
 *  A process that discovers an existing pid file
 *  can try to grab a janitorial lockfile and test
 *  for existence of the process (using kill -0).
 *
 *  If that test fails, the process with the janitorial
 *  lock is entitled to remove the lockfile; then it gives
 *  up the janitorial lock and retries.
 *
 *  A process that tries to obtain a janitorial lock and
 *  fails just gives up.
 *
 *  (It's not a good idea for processes to die while holding
 *  the janitorial lock -- fixing _that_ takes human
 *  intervention.)
 */

int srv_pidfile_read(cl_handle* cl, char const* pidfile_path, pid_t* out) {
  int fd;
  ssize_t cc;
  size_t n, i;
  char id_buf[50];
  struct stat st;
  unsigned long ul;

  if ((fd = open(pidfile_path, O_RDONLY)) == -1) return errno;

  if (fstat(fd, &st) == -1) {
    int err = errno;

    cl_cover(cl);
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR, "can't stat pid-file %s: %s",
           pidfile_path, strerror(err));
    (void)close(fd);
    return err;
  }
  if (st.st_size >= sizeof id_buf) {
    cl_cover(cl);
    cl_log(cl, CL_LEVEL_ERROR, "pid-file %s of size %lu???", pidfile_path,
           (unsigned long)st.st_size);
    (void)close(fd);
    return SRV_ERR_SYNTAX;
  }

  n = st.st_size;
  i = 0;
  while (i < n) {
    cc = read(fd, id_buf + i, n - i);
    if (cc <= 0) break;
    i += cc;
  }
  close(fd);

  if (i != n) {
    cl_cover(cl);
    cl_log(cl, CL_LEVEL_ERROR, "pid-file \"%s\" shrunk from size %lu to %lu???",
           pidfile_path, (unsigned long)st.st_size, (unsigned long)i);
    return SRV_ERR_SYNTAX;
  }

  id_buf[i] = '\0';
  if (sscanf(id_buf, "%lu", &ul) != 1) {
    cl_cover(cl);
    cl_log(cl, CL_LEVEL_ERROR,
           "\"%s\": unexpected pid-file contents  \"%s\" "
           "-- expected a number\n",
           pidfile_path, id_buf);
    return SRV_ERR_SYNTAX;
  }

  cl_cover(cl);
  *out = (pid_t)ul;
  return 0;
}

static int srv_pidfile_write(char const* progname, cl_handle* cl,
                             char const* pidfile_path, pid_t pid) {
  int fd;
  ssize_t cc;
  size_t n, i;
  char my_id[50];
  int err;

  fd = open(pidfile_path, O_WRONLY | O_TRUNC | O_CREAT, 0666);
  if (fd == -1) {
    err = errno;
    cl_cover(cl);
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "%s: cannot create or open "
           "temporary pid-file \"%s\": %s",
           progname, pidfile_path, strerror(err));
    return err;
  }

  snprintf(my_id, sizeof my_id, "%lu", (unsigned long)pid);
  n = strlen(my_id);
  i = 0;

  while (i < n) {
    cc = write(fd, my_id + i, n - i);
    if (cc <= 0) {
      err = errno ? errno : EINVAL;
      cl_log(cl, CL_LEVEL_ERROR, "failed to write pid file \"%s\": %s",
             pidfile_path, strerror(errno));
      (void)close(fd);
      (void)unlink(pidfile_path);
      return err;
    }
    i += cc;
    cl_cover(cl);
  }
  if (close(fd) != 0) {
    err = errno;
    cl_log(cl, CL_LEVEL_ERROR, "error closing pid file \"%s\": %s",
           pidfile_path, strerror(errno));
    (void)unlink(pidfile_path);
    return err;
  }
  return 0;
}

int srv_pidfile_create(char const* progname, cl_handle* cl,
                       char const* pidfile_path) {
  char* unique_name = NULL;
  char* janitor_name = NULL;
  size_t suffixed_size;
  int retry = 0;
  int err;
  pid_t pid;

  cl_assert(cl, pidfile_path);

  suffixed_size = strlen(pidfile_path) + 80;
  if (!(unique_name = malloc(suffixed_size))) {
    err = errno ? errno : ENOMEM;
    cl_log(cl, CL_LEVEL_ERROR,
           "%s: failed to allocate %lu bytes for unique file name "
           "buffer for pid file \"%s\": %s",
           progname, (unsigned long)suffixed_size, pidfile_path,
           strerror(errno));
    return err;
  }

  snprintf(unique_name, suffixed_size, "%s-%lu", pidfile_path,
           (unsigned long)getpid());
  if ((err = srv_pidfile_write(progname, cl, unique_name, getpid())) != 0) {
    cl_cover(cl);
    free(unique_name);
    return err;
  }

  for (retry = 0; retry < 3; retry++) {
    if (link(unique_name, pidfile_path) == 0) {
      cl_cover(cl);
      (void)unlink(unique_name);
      free(unique_name);

      if (janitor_name) {
        (void)unlink(janitor_name);
        free(janitor_name);
      }
      return 0;
    }

    /* Can we get the janitorial lock? */
    janitor_name = malloc(suffixed_size);
    if (janitor_name == NULL) {
      cl_log(cl, CL_LEVEL_ERROR,
             "%s: failed to allocate %lu bytes "
             "for janitorial file name "
             "buffer for pid file \"%s\": %s",
             progname, (unsigned long)suffixed_size, pidfile_path,
             strerror(errno));
      free(unique_name);
      return ENOMEM;
    }
    snprintf(janitor_name, suffixed_size, "%s.CLEANUP", pidfile_path);
    if (link(unique_name, janitor_name) != 0) {
      err = errno;
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "%s: can get neither regular lock \"%s\" "
             "nor clean-up lock \"%s\": %s",
             progname, pidfile_path, janitor_name, strerror(errno));
      goto err;
    }

    err = srv_pidfile_read(cl, pidfile_path, &pid);
    if (err) {
      cl_cover(cl);
      if (err == ENOENT) continue;
      goto err;
    }

    if (kill(pid, 0) == -1) {
      cl_assert(cl, errno != EINVAL);
      if (errno == ESRCH) {
        cl_cover(cl);
        unlink(pidfile_path);

        unlink(janitor_name);
        free(janitor_name);
        janitor_name = NULL;

        continue;
      }
      cl_cover(cl);
    }

    unlink(janitor_name);
    free(janitor_name);
    janitor_name = NULL;

    cl_cover(cl);
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "%s: server is already running (with process-id %lu).  [Startup "
           "aborted.]\n"
           "\tTo stop the old server, run %s with the -z option.\n"
           "\tTo start a second server on a different database in parallel,\n"
           "\tuse a pid-file other than \"%s\" (option -p <filename>).",
           progname, (unsigned long)pid, progname, pidfile_path);

    err = EBUSY;
    goto err;
  }

err:
  if (janitor_name) {
    cl_cover(cl);
    (void)unlink(janitor_name);
    free(janitor_name);
  }
  if (unique_name) {
    cl_cover(cl);
    (void)unlink(unique_name);
    free(unique_name);
  }
  return err;
}

/* Change a pidfile to someone else's pid */
int srv_pidfile_update(char const* progname, cl_handle* cl,
                       char const* pidfile_path, pid_t new_pid) {
  char* unique_name = NULL;
  size_t suffixed_size;
  int err;

  suffixed_size = strlen(pidfile_path) + 80;
  if (!(unique_name = malloc(suffixed_size))) {
    err = errno ? errno : ENOMEM;
    cl_log(cl, CL_LEVEL_ERROR,
           "%s: failed to allocate %lu bytes for unique file name "
           "buffer for pid file \"%s\": %s",
           progname, (unsigned long)suffixed_size, pidfile_path,
           strerror(errno));
    return err;
  }

  snprintf(unique_name, suffixed_size, "%s-%lu", pidfile_path,
           (unsigned long)getpid());
  if ((err = srv_pidfile_write(progname, cl, unique_name, new_pid)) != 0) {
    cl_cover(cl);
    free(unique_name);
    return err;
  }

  if (rename(unique_name, pidfile_path)) {
    err = errno;

    cl_cover(cl);
    cl_log(cl, CL_LEVEL_ERROR, "%s: failed to rename from \"%s\" to \"%s\": %s",
           progname, unique_name, pidfile_path, strerror(errno));

    (void)unlink(unique_name);
    free(unique_name);

    return err;
  }
  return 0;
}

/**
 * @brief Read contents of a pidfile, and send that process a
 *	signal to kill it.
 *
 *  If the pdifile itself doesn't exist, it is treated as if
 *  the listed process didn't exist.
 *
 *  If the signal is nonfatal, the calling process must schedule
 *  a timeout with something like alarm() - this call won't
 *  return unless the calling process exits (or kill(0, pid) fails
 *  for some other reason, e.g. lack of permissions).
 *
 * @param progname	calling program's name, for error messages
 * @param cl		log through this
 * @param pidfile_path	use this pidfile
 * @param sig		signal to send
 *
 * @return 0 if the process was running and has been terminated.
 * @return ENOENT if the process doesn't exist.
 * @return other nonzero error codes on error.
 */

int srv_pidfile_kill(char const* progname, cl_handle* cl,
                     char const* pidfile_path, int sig) {
  pid_t pid;
  int err;

  pid = 0;

  cl_assert(cl, pidfile_path != NULL);

  if ((err = srv_pidfile_read(cl, pidfile_path, &pid)) != 0) {
    cl_cover(cl);
    if (err == ENOENT)
      err = 0;
    else {
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR, "%s: can't read pid-file \"%s\": %s",
             progname, pidfile_path, strerror(err));
    }
  } else {
    if (kill(pid, sig)) {
      err = errno;
      if (err != ESRCH) {
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_ERROR,
               "%s: failed to send SIGTERM "
               "to process %lu: %s",
               pidfile_path, (unsigned long)pid, strerror(err));
      } else {
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "%s: Could not find process %lu. "
               "%s not running?",
               pidfile_path, (unsigned long)pid, progname);
      }
      return err;
    }

    /* Wait for the process to actually stop. */
    errno = 0;
    while (kill(pid, 0) == 0) sleep(1);

    if (errno != ESRCH) {
      err = errno;
      fprintf(stderr, "%s: waiting for %lu: %s\n", progname, (unsigned long)pid,
              strerror(err));
    }
  }
  return err;
}

/**
 * @brief Read contents of a pidfile, and test whether the corresponding
 * 	process exists.
 *
 *  If the pidfile itself doesn't exist, it is treated as if
 *  the listed process didn't exist.
 *
 *  If the result isn't 0 or ENOENT, an error message is logged.
 *
 * @param progname	calling program's name, for error messages
 * @param cl		log through this
 * @param pidfile_path	use this pidfile
 *
 * @return 0 if the process is running.
 * @return ENOENT if the process doesn't exist.
 * @return other nonzero error codes on error.
 */

int srv_pidfile_test(char const* progname, cl_handle* cl,
                     char const* pidfile_path) {
  pid_t pid;
  int err;

  pid = 0;

  cl_assert(cl, pidfile_path != NULL);

  if ((err = srv_pidfile_read(cl, pidfile_path, &pid)) != 0) {
    cl_cover(cl);
    if (err != ENOENT)
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR, "%s: can't read pid-file \"%s\": %s",
             progname, pidfile_path, strerror(err));
  } else if (kill(pid, 0) != 0) {
    if (errno == ESRCH) {
      err = ENOENT;
      cl_cover(cl);
    } else {
      err = errno;
      cl_cover(cl);
      cl_log(cl, CL_LEVEL_ERROR,
             "%s: unexpected error while sending SIGTERM to "
             "process %lu: %s",
             pidfile_path, (unsigned long)pid, strerror(errno));
    }
  } else
    cl_cover(cl);
  return err;
}
