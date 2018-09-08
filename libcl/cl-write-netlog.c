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
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>


/*  Output a string with newline quoted as \n, backslash as \\.
 *
 *  This is used by netlog and netlog3.
 */
int cl_write_netlog_quoted(FILE *fp, char const *s, char const *e) {
  for (; s < e; s++)

    if (*s == '\n'
            ? (putc('\\', fp) == EOF || putc('n', fp) == EOF)
            : (*s == '\t' ? (putc('\\', fp) == EOF || putc('t', fp) == EOF)
                          : ((*s == '\\' && putc('\\', fp) == EOF) ||
                             putc(*s, fp) == EOF)))

      return errno;
  return 0;
}

/*  Remove spaces from an expression's boundaries.
 */
int cl_write_netlog_trim(char const **s, char const **e) {
  while (*s < *e && isascii(**s) && isspace(**s)) (*s)++;

  while (*s < *e && isascii((*e)[-1]) && isspace((*e)[-1])) (*e)--;

  return 0;
}

/*  Given its contents, what format would this expression be written in?
 */
static int cl_write_netlog_format(char const *s, char const *e) {
  long long ll = 0;

  if (s >= e) return 's';

  for (; s < e; s++) {
    if (!isascii(*s) || !isdigit(*s)) return 's';
    ll = ll * 10 + (*s - '0');
  }
  if (ll < (1ull << 32)) return 'i';
  return 'l';
}

/*  Write a format/key/value triple in netlog format.
 *  If the key starts with a three-letter format sequence (x),
 * 	it has format x.
 *  Otherwise, if the format is predefined by the caller, that's the format.
 *  Otherwise, the format i/l/s is deduced from the string.
 *  (There is no floating point support.)
 */
static int cl_write_netlog_key(FILE *fp, int format, char const *key_s,
                               char const *key_e, char const *value_s,
                               char const *value_e) {
  /*  Unless our job is printing some trailing text, strip trailing
   *  punctuation from the value.
   */
  if (format != 's' && value_e > value_s && *value_e != '\0' &&
      (value_e[-1] == ',' || value_e[-1] == ';'))
    value_e--;

  if (key_e - key_s > 3 && key_s[0] == '(' && key_s[2] == ')') {
    format = key_s[1];
    key_s += 3;
  } else {
    if (format == 0) format = cl_write_netlog_format(value_s, value_e);
  }

  if (putc(format, fp) == EOF || putc(' ', fp) == EOF ||
      fwrite(key_s, key_e - key_s, 1, fp) == EOF || putc(':', fp) == EOF ||
      putc(' ', fp) == EOF ||
      cl_write_netlog_quoted(fp, value_s, value_e) != 0 ||
      putc('\n', fp) == EOF)
    return errno;

  return 0;
}

/*  Write a formatted log string as a netlog record.
 *
 *  The string has the form
 *	event (key: text)*
 *
 *  DATE, HOST, and EVNT tags are added implicitly; the other
  * keys are used directly as netlog keys.
 */
void cl_write_netlog(void *data, cl_loglevel lev, char const *str) {
  cl_handle *cl = data;
  struct tm tm;
  struct timeval tv;
  time_t now;
  char isodate[80];
  char const *p, *last_space_end, *key_s, *key_e, *value_s;

  CL_DIARY_CHECK(cl, lev, str)

  /*  Find the event start and end.
   */
  p = str;
  /* skip non-ascii, space, and punctuation. */
  while (*p != '\0' && (!isascii(*p) || isspace(*p) || ispunct(*p))) p++;
  key_s = p;

  /*  Skip ahead to the next space.  Allow non-trailing punctuation
   *  to be included in the event name.
   */
  while (*p != '\0' && (!isascii(*p) || !isspace(*p))) p++;
  key_e = p;

  /*  Remove trailing punctuation.
   */
  while (key_e > key_s && isascii(key_e[-1]) && ispunct(key_e[-1])) key_e--;
  if (key_s >= key_e) {
    /* We don't have an event. */
    return;
  }

  /* get current time */
  gettimeofday(&tv, NULL);
  /* in seconds, since the Epoch */
  now = tv.tv_sec;

  /* Rotate log-file if timer has expired; on failure, continue logging
   * to the previous file (if any)
   */
  if (cl_timer_check(cl, now)) (void)cl_file_rotate(cl, now);  // ignore error

  /* are we logging? */
  if (cl->cl_fp == NULL) return;

  /*  Write DATE, HOST (or CCID), and EVNT
   */

  localtime_r(&now, &tm);
  if (strftime(isodate, sizeof isodate, "%Y-%m-%dT%H:%M:%S", &tm) == 0)
    return;  // error

  fprintf(cl->cl_fp,
          "t DATE: %s.%6.6luZ\n"
          "s EVNT: %.*s\n",
          isodate, (unsigned long)tv.tv_usec, (int)(key_e - key_s), key_s);

  if (cl->cl_netlog_ciid != NULL)
    fprintf(cl->cl_fp, "s CIID: %s\n", cl->cl_netlog_ciid);
  else
    fprintf(cl->cl_fp, "s HOST: %s\n",
            cl->cl_netlog_host ? cl->cl_netlog_host : "localhost");

  /*  Write details from the rest of the format string, if any.
   */

  value_s = NULL;
  key_e = NULL;

  while (*p != '\0' && isascii(*p) && isspace(*p)) p++;
  key_s = p;
  last_space_end = p;

  for (; *p != '\0'; p++) {
    if (*p == ' ' || *p == '\t')
      last_space_end = p + 1;

    else if (*p == ':' && (p[1] == ' ' || (p[1] == ':' && p[2] == ' '))) {
      if (key_s != NULL && key_e != NULL && key_s < key_e && value_s != NULL &&
          last_space_end != NULL && last_space_end >= value_s) {
        char const *value_e = last_space_end;

        cl_write_netlog_trim(&value_s, &value_e);
        (void)cl_write_netlog_key(cl->cl_fp, 0, key_s, key_e, value_s, value_e);
      }

      key_s = last_space_end;
      key_e = p;
      value_s = p + 2 + (p[1] == ':');

      if (p[1] == ':' && p[2] == ' ') {
        (void)cl_write_netlog_key(cl->cl_fp, 's', key_s, key_e, value_s,
                                  p + strlen(p));

        key_s = NULL;
        break;
      }
    }
  }

  if (key_s != NULL && key_e != NULL && key_s < key_e && value_s != NULL &&
      p >= value_s)

    (void)cl_write_netlog_key(cl->cl_fp, 0, key_s, key_e, value_s, p);

  (void)putc('\n', cl->cl_fp);

  if (cl->cl_flush == CL_FLUSH_ALWAYS) (void)fflush(cl->cl_fp);
}
