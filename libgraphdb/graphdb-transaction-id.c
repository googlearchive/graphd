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
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>

#include "libgraphdb/graphdbp.h"

/*  Return a malloc'ed copy of the first three segments of
 *  this machine's domain name, for use in informational
 *  messages.
 *
 *  Heuristic.
 */
static char *my_domainname(cm_handle *cm) {
  char host_buf[1024], domain_buf[1024];
  char *paren, *res, *p;

  /*  Get hostname ingredients.
   */
  *domain_buf = '\0';
  if (gethostname(host_buf, sizeof host_buf) != 0)
    snprintf(host_buf, sizeof host_buf, "???");

  if (strchr(host_buf, '.') != NULL) {
    res = cm_bufmalcpy(cm, host_buf);
    goto have_result;
  }

  if (getdomainname(domain_buf, sizeof domain_buf) != 0 ||
      *domain_buf == '\0' || strcasecmp(domain_buf, "(none)") == 0) {
    struct hostent *he;

    he = gethostbyname(host_buf);
    if (he != NULL && he->h_name != NULL && he->h_name[0] != '\0')
      res = cm_bufmalcpy(cm, he->h_name);
    else
      res = cm_bufmalcpy(cm, host_buf);
    goto have_result;
  }

  if ((paren = strrchr(domain_buf, '(')) != NULL &&
      strcasecmp(paren, "(none)") == 0) {
    *paren = '\0';
    if (paren > domain_buf && paren[-1] == '.') paren[-1] = '\0';

    if (*domain_buf == '\0') {
      res = cm_bufmalcpy(cm, host_buf);
      goto have_result;
    }
  }

  /* Cut at the third dot. */
  res = cm_sprintf(cm, "%s.%s", host_buf, domain_buf);

have_result:
  if (res != NULL && (p = strchr(res, '.')) != NULL &&
      (p = strchr(p + 1, '.')) != NULL && (p = strchr(p + 1, '.')) != NULL)
    *p = '\0';
  return res;
}

/**
 * @brief Allocate a transaction ID for use by a client.
 *
 * @param graphdb	initialized graphdb handle
 * @param app		short of the application (e.g., "gname")
 * @param dom_buf	NULL or a place to store a malloc'ed
 *			copy of the domain name.
 * @param sequence	current sequence number; caller increments.
 * @param buf		a buffer to produce the transaction id into
 * @param size		number of bytes pointed to by buf.
 *
 * @return A pointer to a transaction ID, which may or may not
 *  	be stored in <buf>.
 */
char const *graphdb_transaction_id(graphdb_handle *graphdb, char const *app,
                                   char **dom_buf, unsigned long sequence,
                                   char *buf, size_t size) {
  char *dom;
  char isodate[100];
  struct tm tmbuf, *tm;
  time_t t;
  static char const dom_unavailable[] = "???";

  if (dom_buf == NULL || (dom = *dom_buf) == NULL) {
    dom = my_domainname(graphdb->graphdb_cm);
    if (dom == NULL) dom = (char *)dom_unavailable;
  }

  time(&t);
  tm = gmtime_r(&t, &tmbuf);
  if (tm == NULL ||
      strftime(isodate, sizeof isodate, "%Y-%m-%dT%H:%M:%S", tm) == 0)
    strcpy(isodate, "???");

  snprintf(buf, size, "%s:%s;%lu;%sZ;%lu", app, dom, (unsigned long)getpid(),
           isodate, sequence);
  return buf;
}
