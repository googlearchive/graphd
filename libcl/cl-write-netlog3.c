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
#include <unistd.h>


#define IS_LIT(lit, s, e) \
  (sizeof(lit) - 1 == (e) - (s) && !strncasecmp((lit), (s), sizeof(lit) - 1))

/*  Write a format/key/value triple in netlog3 format.
 *  The optional parenthesized format string is a leftover
 *  from the original netlog; we pretty much ignore it, although
 *  we do allow trailing punctuation for strings.
 */
static int cl_write_netlog3_key(FILE *fp, int format, char const *key_s,
                                char const *key_e, char const *value_s,
                                char const *value_e) {
  /*  Unless our job is printing some trailing text, strip trailing
   *  punctuation from the value.
   */
  if (format != 's' && value_e > value_s && *value_e != '\0' &&
      (value_e[-1] == ',' || value_e[-1] == ';'))
    value_e--;

  if (key_e - key_s > 3 && key_s[0] == '(' && key_s[2] == ')') key_s += 3;

  if (putc('\t', fp) == EOF || fwrite(key_s, key_e - key_s, 1, fp) == EOF ||
      putc(':', fp) == EOF || cl_write_netlog_quoted(fp, value_s, value_e) != 0)
    return errno;

  return 0;
}

/*  Write a formatted log string as a netlog record.
 *
 *  The string has the form
 *	event (key: text)*
 *
 *  The netlog-style encoding for CID, EVNT, TID and DATE
 *  tags is added implicitly; the other keys are used
 *  directly as trailing netlog keys.
 */
void cl_write_netlog3(void *data, cl_loglevel lev, char const *str) {
  cl_handle *cl = data;
  struct tm tm;
  struct timeval tv;
  time_t now;
  char isodate[80];
  char tid_buf[200];
  char const *p, *q, *last_space_end = NULL, *key_s, *key_e, *value_s;
  char const *tid_s, *tid_e;

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
  if (cl_timer_check(cl, now) || cl_pid_check(cl))
    (void)cl_file_rotate(cl, now);  // ignore error

  /* are we logging? */
  if (cl->cl_fp == NULL) return;

  /*  Write D:<date><TAB>E:<event><TAB>C:<CID><TAB>T:<TID>
   */
  localtime_r(&now, &tm);
  if (strftime(isodate, sizeof isodate, "%Y-%m-%dT%H:%M:%S", &tm) == 0)
    return;  // error

  fprintf(cl->cl_fp, "D:%s.%6.6luZ\tE:%.*s\tC:%s%s\tT:", isodate,
          (unsigned long)tv.tv_usec, (int)(key_e - key_s), key_s,

          /* A  ciid will include an application identifier.
           * If we have none, libcl inserts itself.
           */
          cl->cl_netlog_ciid != NULL ? "" : "libcl:",
          cl->cl_netlog_ciid != NULL
              ? cl->cl_netlog_ciid
              : (cl->cl_netlog_host ? cl->cl_netlog_host : "localhost"));

  while (*p != '\0' && isascii(*p) && isspace(*p)) p++;

  /*  Find the transaction ID in the string.
   */
  tid_s = NULL;
  tid_e = NULL;
  value_s = NULL;
  key_e = NULL;
  last_space_end = p;

  for (q = p; *q != '\0'; q++) {
    if (*q == ' ' || *q == '\t')
      last_space_end = q + 1;

    else if (*q == ':' && (q[1] == ' ' || (q[1] == ':' && q[2] == ' '))) {
      if (key_s != NULL && key_e != NULL && key_s < key_e && value_s != NULL &&
          last_space_end != NULL && last_space_end >= value_s) {
        if (IS_LIT("TID", key_s, key_e)) {
          tid_s = value_s;
          tid_e = last_space_end;

          break;
        }
      }

      key_s = last_space_end;
      key_e = q;
      value_s = q + 2 + (q[1] == ':');

      if (q[1] == ':' && q[2] == ' ') {
        if (IS_LIT("TID", key_s, key_e)) {
          tid_s = key_s;
          tid_e = q + strlen(q);

          break;
        }
        key_s = NULL;
        break;
      }
    }
  }

  if (tid_s == NULL) {
    if (key_s != NULL && key_e != NULL && key_s < key_e && value_s != NULL &&
        p >= value_s && IS_LIT("TID", key_s, key_e)) {
      tid_s = value_s;
      tid_e = p;
    } else {
      char const *component_s, *component_e;
      char const *service_s, *service_e;

      /*  Default to cl;localhost
       */
      component_s = "cl";
      component_e = component_s + 2;
      service_s = "localhost";
      service_e = service_s + strlen(service_s);

      /*  Try to improve on what we know by pulling
       *  details out of the configured ciid or
       *  the hostname.
       */
      if (cl->cl_netlog_ciid != NULL) {
        char const *colon;

        if ((colon = strchr(cl->cl_netlog_ciid, ':')) != NULL) {
          component_s = cl->cl_netlog_ciid;
          component_e = colon;
          service_s = colon + 1;
          service_e = cl->cl_netlog_ciid + strlen(cl->cl_netlog_ciid);
        } else {
          service_s = cl->cl_netlog_ciid;
          service_e = service_s + strlen(service_s);
        }
      } else if (cl->cl_netlog_host != NULL) {
        service_s = cl->cl_netlog_host;
        service_e = cl->cl_netlog_host + strlen(cl->cl_netlog_host);
      }
      snprintf(tid_buf, sizeof tid_buf, "%.*s;%.*s;%lu;%sZ;%lu",
               (int)(component_e - component_s), component_s,
               (int)(service_e - service_s), service_s, (unsigned long)getpid(),
               isodate, cl->cl_netlog_n++);
      tid_s = tid_buf;
      tid_e = tid_buf + strlen(tid_buf);
    }
  }

  cl_assert(cl, tid_e >= tid_s);
  /*
   * Get rid of trailing spaces.
   */
  while (tid_e > tid_s && tid_e[-1] == ' ') tid_e--;

  /*  Write T:<TID>
   */
  (void)fwrite(tid_s, tid_e - tid_s, 1, cl->cl_fp);

  /*  Write details from the rest of the format string, if any.
   *  Skip the transaction ID.
   */
  value_s = NULL;
  key_e = NULL;

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
        if (value_s != tid_s)
          (void)cl_write_netlog3_key(cl->cl_fp, 0, key_s, key_e, value_s,
                                     value_e);
      }

      key_s = last_space_end;
      key_e = p;
      value_s = p + 2 + (p[1] == ':');

      if (p[1] == ':' && p[2] == ' ') {
        if (value_s != tid_s)
          (void)cl_write_netlog3_key(cl->cl_fp, 's', key_s, key_e, value_s,
                                     p + strlen(p));

        key_s = NULL;
        break;
      }
    }
  }

  if (key_s != NULL && key_e != NULL && key_s < key_e && value_s != NULL &&
      p >= value_s)

    if (value_s != tid_s)
      (void)cl_write_netlog3_key(cl->cl_fp, 0, key_s, key_e, value_s, p);

  (void)putc('\n', cl->cl_fp);

  if (cl->cl_flush == CL_FLUSH_ALWAYS) (void)fflush(cl->cl_fp);
}
