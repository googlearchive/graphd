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
#include "libcl/clp.h"

#include <errno.h>
#include <limits.h>
#include <string.h>
#include <unistd.h>


/**
 * Start timer.
 */
static void cl_timer_start(cl_handle *cl, time_t now) {
  cl->cl_file_minute = now / 60;
}

/**
 * Disable timer.
 */
static void cl_timer_disable(cl_handle *cl) { cl->cl_file_minute = -1; }

/**
 * Implement a simple timer of 1 minute on the given handle.
 *
 * The function returns true if the timer has expired, in which case the timer
 * is also automatically restarted.
 */
bool cl_timer_check(cl_handle *cl, time_t now) {
  if (cl->cl_file_minute != -1) {
    time_t minute = now / 60;
    if (minute > cl->cl_file_minute) {
      cl->cl_file_minute = minute;
      return true;
    }
  }
  return false;
}

/**
 * Start pid monitor.
 */
static void cl_pid_start(cl_handle *cl) { cl->cl_file_pid = getpid(); }

/**
 * Disable timer.
 */
static void cl_pid_disable(cl_handle *cl) { cl->cl_file_pid = 0; }

/**
 * Implement a simple timer of 1 minute on the given handle.
 *
 * The function returns true if the timer has expired, in which case the timer
 * is also automatically restarted.
 */
bool cl_pid_check(cl_handle *cl) {
  pid_t pid = getpid();

  if (cl->cl_file_pid == 0) return false;

  if (cl->cl_file_pid != pid) {
    cl->cl_file_pid = pid;
    return true;
  }
  return false;
}

static size_t cl_pid_count(char const *r) {
  size_t n = 0;

  for (; *r != '\0'; r++)
    if (*r == '%') {
      if (r[1] == '%')
        r++;
      else
        n += (r[1] == '$');
    }
  return n;
}

/**
 * Evaluate a strftime() format string.
 */
static int cl_file_format(char *buf, size_t size, const char *fmt, time_t now) {
  struct tm tm;
  char const *r;
  size_t need = cl_pid_count(fmt) * 42;

  localtime_r(&now, &tm);

  if (need == 0) {
    if (strftime(buf, size, fmt, &tm) == 0) return ENAMETOOLONG;
  } else {
    char bigfmt[strlen(fmt) + need]; /* C9X. */
    char *w = bigfmt;

    for (r = fmt; *r != '\0';) {
      if (*r != '%')
        *w++ = *r++;
      else {
        if (r[1] == '%') {
          *w++ = *r++;
          *w++ = *r++;
        } else if (r[1] == '$') {
          snprintf(w, sizeof bigfmt - (w - bigfmt), "%lu",
                   (unsigned long)getpid());
          w += strlen(w);
          r += 2;
        } else
          *w++ = *r++;
      }
    }
    *w = '\0';
    if (strftime(buf, size, bigfmt, &tm) == 0) return ENAMETOOLONG;
  }
  return 0;
}

/**
 * Close log file associated with given handle.
 *
 * Nothing happens if a log file is not in use.
 */
static void cl_file_close(cl_handle *cl) {
  if (cl->cl_file_name != NULL) {
    fclose(cl->cl_fp);
    cl->cl_fp = NULL;
    free(cl->cl_file_name);
    cl->cl_file_name = NULL;
  }
}

/**
 * Open or create log file.
 *
 * On success, the old log file (if any) is automatically closed. Otherwise,
 * the log handle is unchanged.
 */
static int cl_file_open(cl_handle *cl, const char *path) {
  const size_t lenz = strlen(path) + 1;  // includes final \0 char
  char *new_path;
  FILE *fp;

  /* allocate memory for new path */
  errno = 0;
  if ((new_path = malloc(lenz)) == NULL) return errno ? errno : ENOMEM;
  memcpy(new_path, path, lenz);

  /* open log file */
  errno = 0;
  if ((fp = fopen(path, "a+")) == NULL) {
    free(new_path);
    return errno ? errno : EACCES;
  }

  /* Commit...
   */

  cl_file_close(cl);  // close old log file, if any
  cl->cl_file_name = new_path;
  cl->cl_fp = fp;

  /*  Install redirects, if we had any.
   */
  (void)cl_dup2_install(cl);

  /* add an empty line to the end of the file if necessary */
  if (fseek(fp, -1, SEEK_END) == 0)
    if (getc(fp) != '\n') putc('\n', fp);

  return 0;
}

/**
 * Evaluate the log file format string and open a new log file if necessary.
 *
 * @return
 *	Zero on success, otherwise a positive error code.
 *
 * On error, the @a cl handle is unchanged.
 */
int cl_file_rotate(cl_handle *cl, time_t now) {
  char path[PATH_MAX];  // temp buffer for the file name
  int err;

  /* do nothing if using standard error */
  if (cl->cl_file_name_fmt == NULL) return 0;

  /* generate file name from format string */
  if ((err = cl_file_format(path, sizeof(path), cl->cl_file_name_fmt, now)))
    return err;

  /* compare new name with old one; do nothing if they are the same */
  if (cl->cl_file_name != NULL)
    if (strcasecmp(path, cl->cl_file_name) == 0) return 0;

  /* open new log file */
  return cl_file_open(cl, path);
}

/**
 * @brief
 *	Change the file name for the given log handle.
 * @param cl
 *	A log handle created using cl_create().
 * @param fmt
 *	The name of the log file.
 * @return
 *	Zero on success, otherwise a non-zero error code.
 *
 * The @a name argument may contain format specifications recognized by the
 * @c strftime() function. In this case, the name will be evaluated every
 * minute, causing new log files to be created automatically, if necessary.
 *
 * On success, a new log file is opened immediately. If the file exists, it is
 * opened in append mode, otherwise it is created.
 *
 * On failure, the handle is unchanged and the old file is kept open for
 * logging.
 */
int cl_file_set_name(cl_handle *cl, const char *fmt) {
  char *new_fmt;

  /* if format string is absent, use standard error... */
  if (fmt == NULL) {
    new_fmt = NULL;
    cl_file_close(cl);  // close current log file, if any
    cl->cl_fp = stderr;

    /* disable log rotation timer and pid tracking. */
    cl_timer_disable(cl);
    cl_pid_disable(cl);

    /* If needed, switch write functions.
     */
    if (cl->cl_write == cl_write_file) {
      cl->cl_write = cl_write_stderr;
      cl->cl_write_data = cl;
    }
  }
  /* ...otherwise, generate an appropriate file name based on the given
   * pattern, and open the log file immediately */
  else {
    const time_t now = time(NULL);
    const size_t lenz = strlen(fmt) + 1;
    char path[PATH_MAX];
    int err;

    /* determine initial log file name */
    if ((err = cl_file_format(path, sizeof(path), fmt, now))) return err;

    /* save a copy of the format for later */
    if ((new_fmt = malloc(lenz)) == NULL) return ENOMEM;
    memcpy(new_fmt, fmt, lenz);

    /* open new log file, and close old one if necessary (notice
     * that if the call below fails, the old log file is kept open
     * so that logging will continue) */
    if ((err = cl_file_open(cl, path))) {
      free(new_fmt);
      return err;
    }

    /* start log rotation timer */
    cl_timer_start(cl, now);

    if (cl_pid_count(new_fmt))
      cl_pid_start(cl);
    else
      cl_pid_disable(cl);

    /* If needed, switch write functions.
     */
    if (cl->cl_write != cl_write_file) {
      cl->cl_write = cl_write_file;
      cl->cl_write_data = cl;
    }
  }

  /* Commit remaining fields...
   */

  if (cl->cl_file_name_fmt != NULL) free(cl->cl_file_name_fmt);
  cl->cl_file_name_fmt = new_fmt;

  return 0;
}

/**
 * Return the name of the current log file, or @c NULL if standard error is
 * being used.
 *
 * @param cl
 *	A log handle created using cl_create().
 */
const char *cl_file_get_name(cl_handle *cl) { return cl->cl_file_name_fmt; }

/**
 * @brief
 *	Initialize logging to a stdio-buffered file.
 * @param cl
 *	A log handle created using cl_create().
 * @param name
 *	The name of the log file.
 * @return
 *	Zero on success, otherwise a non-zer error code.
 * @see
 *	cl_file_set_name()
 *
 * The @a cl handle is unmodified in case of an error.
 *
 * See cl_file_set_name() function for more information about the @a name
 * parameter.
 */
int cl_file(cl_handle *cl, char const *name) {
  int err;

  /* set format string and open initial log file */
  if ((err = cl_file_set_name(cl, name))) return err;

  /* Commit...
   */

  cl->cl_write = cl_write_file;
  cl->cl_flush = CL_FLUSH_ALWAYS;
  cl->cl_write_data = cl;

  return 0;
}
