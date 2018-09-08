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
#include <stdarg.h>
#include <ctype.h>

#include "srvp.h"

static char const *epitaph_word(char **sp, char const *e) {
  char *s = *sp;
  char const *result;

  if (s == NULL) return NULL;

  while (s < e && isascii(*s) && isspace(*s)) s++;
  if (s >= e) return NULL;

  result = s;

  while (s < e && (!isascii(*s) || !isspace(*s))) s++;
  if (s < e) {
    *s = '\0';
    *sp = s + 1;
  } else {
    *sp = NULL;
  }
  return result;
}

/**
 * @brief Read an epitaph
 *
 *  If the call is successful (and the program died leaving
 *  an epitaph), the caller must free the epitaph structure
 *  pointed to by epitaph_out with a cm_free against the
 *  passed-in cm handle.
 *
 * @param srv_handle 	libsrv module handle
 * @param cm		allocate result here
 * @param epitaph_out	resulting epitaph structure.
 * @return 0 on success,  a nonzero error code for error.
 * @return ENOENT if there was no epitaph (this is a good thing)
 * @return EINVAL if the server handle hasn't been configured yet.
 */
int srv_epitaph_read(srv_handle *srv, cm_handle *cm,
                     srv_epitaph **epitaph_out) {
  char const *pid_file;
  int fd, cc, n;
  int err;
  size_t size;
  char *path, *s, *e;
  char const *w;
  srv_epitaph *epi = NULL;
  struct stat st;

  if (srv->srv_config == NULL) return EINVAL;

  pid_file = srv->srv_config->cf_pid_file ? srv->srv_config->cf_pid_file
                                          : SRV_PIDFILE_DEFAULT;
  *epitaph_out = NULL;

  size = strlen(pid_file);
  if ((path = cm_malloc(cm, size + 10)) == NULL) return errno ? errno : ENOMEM;
  snprintf(path, size + 10, "%s.RIP", pid_file);

  /*  Read.
   */
  if ((fd = open(path, O_RDONLY)) == -1) {
    err = errno;
    if (err != ENOENT)
      cl_log_errno(srv->srv_cl, CL_LEVEL_ERROR, "open", err,
                   "%s: failed to open epitaph file \"%s\"", srv->srv_progname,
                   path);
    cm_free(cm, path);
    return err;
  }

  if (fstat(fd, &st) == -1) {
    err = errno ? errno : ENOENT;
    if (err != ENOENT)
      cl_log_errno(srv->srv_cl, CL_LEVEL_ERROR, "stat", err,
                   "%s: failed to stat epitaph file \"%s\"", srv->srv_progname,
                   path);
    cm_free(cm, path);
    (void)close(fd);

    return err;
  }

  if ((epi = cm_malloc(cm, sizeof(*epi) + st.st_size + 1)) == NULL) {
    err = errno ? errno : ENOMEM;

    cm_free(cm, path);
    (void)close(fd);

    return err;
  }

  for (cc = 0; cc < st.st_size; cc += n) {
    n = read(fd, (char *)(epi + 1) + cc, st.st_size - cc);
    if (n <= 0) {
      err = n < 0 && errno ? errno : 0;
      if (err)
        cl_log_errno(srv->srv_cl, CL_LEVEL_ERROR, "read", err,
                     "failed to read %lu bytes from epitaph "
                     "file \"%s\"",
                     (unsigned long)(st.st_size - cc), path);
      else {
        cl_log(srv->srv_cl, CL_LEVEL_ERROR,
               "%s: short epitaph file \"%s\" -- "
               "expecting %lu bytes, read %lu",
               srv->srv_progname, path, (unsigned long)st.st_size,
               (unsigned long)cc);
        break;
      }

      cm_free(cm, path);
      (void)close(fd);

      return err;
    }
  }

  if ((n = read(fd, (char *)(epi + 1) + cc, 1)) > 0)
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "%s: long epitaph file \"%s\" -- "
           "expecting %lu bytes, found at least %lu [ignored]",
           srv->srv_progname, path, (unsigned long)st.st_size,
           (unsigned long)cc + 1);
  ((char *)(epi + 1))[cc] = '\0';

  (void)close(fd);

  epi->epi_pid = 0;
  epi->epi_exit = 0;
  epi->epi_message = NULL;
  epi->epi_time = st.st_mtime;

  s = (char *)(epi + 1);
  e = s + cc;

  if ((w = epitaph_word(&s, e)) != NULL) {
    unsigned long ul;

    if (sscanf(w, "%lu", &ul) == 1)
      epi->epi_pid = ul;
    else {
      cl_log(srv->srv_cl, CL_LEVEL_ERROR,
             "%s: syntax error "
             "in epitaph file \"%s\": expected pid, got %s",
             srv->srv_progname, path, w);
      epi->epi_pid = 0;
    }
  } else {
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "%s: short epitaph file \"%s\" "
           "(expected pid)",
           srv->srv_progname, path);
    cm_free(cm, path);
    return 0;
  }

  if ((w = epitaph_word(&s, e)) != NULL) {
    if (sscanf(w, "%d", &epi->epi_exit) != 1) {
      cl_log(srv->srv_cl, CL_LEVEL_ERROR,
             "%s: syntax error "
             "in epitaph file \"%s\": expected integer exit "
             "code, got %s",
             srv->srv_progname, path, w);
      epi->epi_exit = 0;
    }
  } else {
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "%s: short epitaph file \"%s\" (expected exit code)",
           srv->srv_progname, path);
    cm_free(cm, path);
    return 0;
  }

  epi->epi_message = s;
  if (unlink(path) != 0)
    cl_log_errno(srv->srv_cl, CL_LEVEL_ERROR, "unlink", errno,
                 "%s: failed to remove epitaph file \"%s\" [ignored]",
                 srv->srv_progname, path);
  cm_free(cm, path);
  *epitaph_out = epi;

  return 0;
}

/**
 * @brief Write an epitaph
 *
 *  Fails if an epitaph file already exists.
 *
 * @param srv_handle 	libsrv module handle
 * @param exit_code	desired exit code for shutdown
 * @param fmt		desired error message for shutdown
 * @param ...		arguments
 *
 * @return 0 on success, a nonzero error code on system error.
 */
int srv_epitaph_print(srv_handle *srv, int exit_code, char const *fmt, ...) {
  char const *pid_file;
  pid_t pid;
  va_list ap;
  char bigbuf[1024 * 8];
  int fd, cc, n;
  int err;
  size_t size;
  char *path = NULL;

  /*  If this call is happening early enough for the
   *  handle not to be configured, we're still in the
   *  interactive start context and can just print
   *  to stderr and die.
   */
  if (srv->srv_config == NULL || srv->srv_interactive) {
    fprintf(stderr, "%s: ", srv->srv_progname);

    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);

    putc('\n', stderr);
    srv_shutdown_now(srv);
    srv_finish(srv, true);
    exit(exit_code);
  }

  /*  If this call is happenening while there's still a tty-connected
   *  process waiting for final results, print to that process and die.
   */
  if (srv->srv_settle_pipe[1] != -1) {
    va_start(ap, fmt);
    vsnprintf(bigbuf, sizeof(bigbuf), fmt, ap);
    va_end(ap);

    bigbuf[sizeof bigbuf - 1] = '\0';

    srv_settle_error(srv, "%s", bigbuf);
    srv_shutdown_now(srv);
    srv_finish(srv, true);
    exit(exit_code);
  }

  pid_file = srv->srv_config->cf_pid_file ? srv->srv_config->cf_pid_file
                                          : SRV_PIDFILE_DEFAULT;
  pid = srv->srv_pid ? srv->srv_pid : getpid();
  size = strlen(pid_file);

  path = malloc(size + 10);
  if (path == NULL) return errno ? errno : ENOMEM;
  snprintf(path, size + 10, "%s.RIP", pid_file);

  /*  Open the epitaph file.
   */
  if ((fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0666)) == -1) {
    err = errno;
    if (err != EEXIST)
      cl_log_errno(srv->srv_cl, CL_LEVEL_ERROR, "open", err,
                   "%s: failed to create temporary epitaph "
                   "file \"%s\" for writing",
                   srv->srv_progname, path);
    free(path);
    return err;
  }

  /*  Compose the text to write in <bigbuf>.
   *  <pid> <desired exit code> <message>
   */
  snprintf(bigbuf, sizeof bigbuf, "%lu %d ",
           (unsigned long)(srv->srv_pid ? srv->srv_pid : getpid()), exit_code);

  size = strlen(bigbuf);
  va_start(ap, fmt);
  vsnprintf(bigbuf + size, sizeof(bigbuf) - size, fmt, ap);
  va_end(ap);

  size = strlen(bigbuf);
  for (cc = 0; cc < size; cc += n) {
    n = write(fd, bigbuf + cc, size - cc);
    if (n <= 0) {
      err = errno ? errno : -1;
      cl_log_errno(srv->srv_cl, CL_LEVEL_ERROR, "write", err,
                   "failed to write to temporary epitaph "
                   "file \"%s\"",
                   path);
      break;
    }
  }
  if (close(fd) < 0) {
    err = errno;
    cl_log_errno(srv->srv_cl, CL_LEVEL_ERROR, "close", err,
                 "failed to close temporary epitaph file \"%s\""
                 "after writing",
                 path);
    free(path);
    return err;
  }
  free(path);
  return 0;
}

/**
 * @brief Clear away a previous epitaph file.
 * @param srv_handle 	libsrv module handle
 * @return 0 on success, a nonzero error code on system error.
 */
int srv_epitaph_clear(srv_handle *srv) {
  char const *pid_file;
  int err;
  size_t size;
  char *path = NULL;

  /*  If this call is happening early enough for the
   *  handle not to be configured, we're still in the
   *  interactive start context and can just print
   *  to stderr and die.
   */
  if (srv->srv_config == NULL) return EINVAL;

  pid_file = srv->srv_config->cf_pid_file ? srv->srv_config->cf_pid_file
                                          : SRV_PIDFILE_DEFAULT;
  size = strlen(pid_file);

  path = malloc(size + 10);
  if (path == NULL) return errno ? errno : ENOMEM;
  snprintf(path, size + 10, "%s.RIP", pid_file);

  err = 0;

  if (unlink(path) == -1 && errno != ENOENT) err = errno;

  free(path);
  return err;
}
