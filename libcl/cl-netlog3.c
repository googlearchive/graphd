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
 * @brief Configure logging to use netlog3.
 *
 *  When logging to a cl_netlog stream, the logged string
 *  (once all printf-style formatting is done) is chooped
 *  into words and expanded to fit into netlog-style output.
 *
 *  For example, a string
 *
 *  <pre>
 *	"start TID: foo, NUMBER: 32"
 *  </pre>
 *
 *  results in a record
 *
 *  <pre>
 *	D: 2006-06-28T03:51:50.639119Z
 *	E: start
 *	C: user
 *	s TID: foo
 *	NUMBER: 32
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
int cl_netlog3(cl_handle *cl, char const *filename) {
  int err;

  err = cl_netlog(cl, filename);
  if (err != 0) return err;

  /* override the "write" method only.
   */
  cl->cl_write = cl_write_netlog3;
  return 0;
}
