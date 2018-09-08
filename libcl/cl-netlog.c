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

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/**
 * @brief Add a "Component Instance Identifier" to a netlog handle
 *
 *  The "CIID" is printed instead of the HOST, if one is set.
 *
 * @param cl 	a log handle that has previously been passed to cl_netlog()
 * @param ciid	a '\\0'-terminated CIID.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int cl_netlog_set_ciid(cl_handle *cl, char const *ciid) {
  char *buf;
  size_t n;

  if (ciid == NULL) {
    if (cl->cl_netlog_ciid != NULL) {
      free(cl->cl_netlog_ciid);
      cl->cl_netlog_ciid = NULL;
    }
    return 0;
  }

  n = strlen(ciid) + 1;
  buf = malloc(n);
  if (buf == NULL) return errno ? errno : ENOMEM;

  memcpy(buf, ciid, n);

  if (cl->cl_netlog_ciid != NULL) free(cl->cl_netlog_ciid);
  cl->cl_netlog_ciid = buf;

  return 0;
}

/**
 * @brief Set the netlog filename pattern.
 *
 *  This may happen at creation time, or while logging is already
 *  in progress.
 *
 *  %%-sequences in the pattern parameter are filled in
 *  using <i>strftime(3)</i>.  Good sequences to use are
 *  "%Y-%m-%d" for the date (with 4-digit year; "%y"
 *  would be a two-digit year), "%H:%M" for a time with 24-hour
 *  clock (and no seconds).
 *
 *  When, at run time, the expansion of the filename
 *  pattern changes, the old log file is closed, and
 *  a new file is opened and logged to.  (This test is
 *  performed roughly once per minute; please don't expect
 *  sub-minute granularity in the file name changes.)
 *  For the sake of efficiency, the log stream is not
 *  flushed after each record is written.
 *
 *  Do not use "/" in the changing part of the pattern,
 *  unless  the different directories needed already exist;
 *  cl_netlog() does not automatically create directories.
 *
 * @param cl 	   a log handle that has previously been passed to cl_netlog()
 * @param pattern  a '\\0'-terminated filename pattern
 *
 * @return 0 on success, a nonzero error code on error.
 * @return EINVAL if the passed-in pattern is NULL or contains
 *	unrecognized escape sequences.
 */
int cl_netlog_set_filename(cl_handle *cl, char const *pattern) {
  if (pattern == NULL) return EINVAL;

  return cl_file_set_name(cl, pattern);
}

/**
 * @brief Get the netlog filename pattern.
 * @param cl 	   a log handle that has previously been passed to cl_netlog()
 * @see   cl_file_get_name()
 */
char const *cl_netlog_get_filename(cl_handle *cl) {
  return cl_file_get_name(cl);
}

/**
 * @brief Configure logging to use netlog.
 *
 *  When logging to a cl_netlog stream, the logged string
 *  (once all printf-style formatting is done) is chooped
 *  into words and expanded to fit into netlog-style output.
 *
 *  For example, a string
 *
 *  <pre>
 *	"start ID: foo, NUMBER: 32"
 *  </pre>
 *
 *  results in a record
 *
 *  <pre>
 *	t DATE: 2006-06-28T03:51:50.639119Z
 *	s HOST: user
 *	s EVNT: start
 *	s ID: foo
 *	d NUMBER: 32
 *  </pre>
 *
 *  The first two lines - the host and date - are always
 *  added.  (Unless the application sets a "CIID", which may
 *  take the role of the host.) The third line, EVNT, takes the text
 *  up to the first parameter name from the log line and makes
 *  it the "event".  After that, each word before a ": " (that's
 *  colon-space, not just colon) introduces a separate "parameter"
 *  in the netlog file;  trailing punctuation is chopped off the
 *  parameter value (except in a special case, see below.)
 *
 *  Each message can have at most one last parameter that turns off
 *  the interpretation and just copies text until the end of the log
 *  line; that's useful for e.g. logging free-form user input without
 *  worrying about it being misunderstood as formatting instructions.
 *  The syntax for that last parameter is ":: " (colon, colon, space):
 *
 *  <pre>
 *	"start ID: foo, TEXT:: This is: some, arbitrary:: text"
 *  </pre>
 *
 *  results in
 *
 *  <pre>
 *	t DATE: 2006-06-28T03:51:50.639119Z
 *	s HOST: user
 *	s EVNT: start
 *	s ID: foo
 *	s TEXT: This is: some, arbitray:: text
 *  </pre>
 *
 *  The "d", "l", and "s" format indicators required by the netlog
 *  format are normally induced based on the field contents: decimal
 *  numbers below 2^32 are tagged as "d", above as "l"; text that
 *  isn't a number is tagged as "s".
 *
 *  To force a field tag other than the obvious one, include the
 *  tag value in parentheses before the prompt.  For example, to
 *  print a session id as "l" (64-bit long), even though it usually
 *  is a small number, write
 *  <pre>
 * 	"session.start (l)ID: 1234"
 *  </pre>
 *
 *  resulting in
 * <pre>
 *	t DATE: 2006-06-28T03:51:50.639119Z
 *	s HOST: user
 *	s EVNT: session.start
 *	l ID: 1234
 * </pre>
 *
 *  The formal log message grammar:
 * <pre>
 *   message:
 *	event [parameter..] [textparameter]
 *
 *   parameter:
 *	name ": " arbitrary text that doesn't contain ": "
 *
 *   textparameter:
 *      name ":: " arbitrary text
 *
 *   name:
 *	[ "(" format ")" ] non-whitespace-character+
 *
 *   format:
 * 	s 	; for a string
 *	l	; for a long (64-bit) integer
 * 	d	; for a 32-bit integer
 * </pre>
 *
 * @param cl a log handle created using cl_create().
 * @param filename a format string for the netlog input files
 *
 * @return 0 on success, a nonzero error code on error.
 */
int cl_netlog(cl_handle *cl, char const *filename) {
  char host_buf[1024], domain_buf[1024];
  char *w;
  char *buf;
  size_t buf_size;
  int err;

  if (filename == NULL) return EINVAL;  // stderr not supported

  /* get our host name */
  if (gethostname(host_buf, sizeof host_buf) != 0)
    snprintf(host_buf, sizeof host_buf, "???");

  /* get our domain name */
  *domain_buf = '\0';
  if (strchr(host_buf, '.') != NULL ||
      getdomainname(domain_buf, sizeof domain_buf) != 0 ||
      *domain_buf == '\0' || strcasecmp(domain_buf, "(none)") == 0)
    *domain_buf = '\0';

  else if ((w = strrchr(domain_buf, '(')) != NULL &&
           strcasecmp(w, "(none)") == 0) {
    *w = '\0';
    if (w > domain_buf && w[-1] == '.') w[-1] = '\0';
  }

  /* combine host and domain names into a single string */
  buf_size = strlen(host_buf) + strlen(domain_buf) + 2;
  if ((buf = malloc(buf_size)) == NULL) return ENOMEM;
  if (*domain_buf != '\0')
    snprintf(buf, buf_size, "%s.%s", host_buf, domain_buf);
  else
    snprintf(buf, buf_size, "%s", host_buf);

  /* open initial log file */
  if ((err = cl_file(cl, filename))) {
    free(buf);
    return err;
  }

  /* Commit...
   */

  cl->cl_netlog_host = buf;
  cl->cl_write = cl_write_netlog;
  cl->cl_flush = CL_FLUSH_NEVER;
  cl->cl_level = CL_LEVEL_DETAIL;

  return 0;
}
