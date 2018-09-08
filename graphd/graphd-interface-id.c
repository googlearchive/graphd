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
#include "graphd/graphd.h"

#include <stdio.h>

static char const* summarize(char const* in, char* buf, size_t size) {
  char const* s = in;
  char const* e = in + strlen(in);
  int a, b, c, d;
  char const *col, *com, *dot;
  char const* next_quote;

  /* strip outer quotes.
   */
  if (s < e && *s == '"') s++;
  if ((next_quote = memchr(s, '"', e - s)) != NULL) e = next_quote;

  /*  If there's more than one, just use the first.
   */
  if ((com = memchr(s, ',', e - s)) != NULL) e = com;

  /* strip a leading "tcp:"
   */
  if (e - s >= 4 && strncasecmp(s, "tcp:", 4) == 0) s += 4;

  if ((col = memchr(s, ':', e - s)) == NULL) col = e;

  /*  If this is a hostname and port pair, and
   *  the hostname is a fully qualified domain
   *  name, strip the end of the domain name.
   */
  if (sscanf(s, "%d.%d.%d.%d", &a, &b, &c, &d) != 4 &&
      (dot = memchr(s, '.', col - s)))
    snprintf(buf, size, "%.*s%.*s", (int)(dot - s), s, (int)(e - col), col);
  else
    snprintf(buf, size, "%.*s", (int)(e - s), s);
  return buf;
}

/**
 * @brief Make an interface ID.
 *
 *  The ID is intended to be unique per graphd instance
 *  (a server on a host, listening on a port).
 *
 * @param g	Graphd ID to cache it in
 * @param srv	Server to allocate memory in, if needed
 *
 * @return a reasonably unique graphd interface ID.
 */
char const* graphd_interface_id(graphd_handle* g) {
  if (g->g_interface_id == NULL) {
    cm_handle* cm = srv_mem(g->g_srv);

    char* host;
    char ibuf[1024];
    char iibuf[1024];
    char const* i;
    char const* host_ptr;

    i = summarize(srv_interface_to_string(g->g_srv, ibuf, sizeof ibuf), iibuf,
                  sizeof iibuf);
    host = srv_address_fully_qualified_domainname(cm);
    if (host == NULL) host_ptr = NULL;

    /*  Don't throw out the first hop of the hostname..
    else if ((host_ptr = strchr(host, '.')) != NULL)
            host_ptr++;
    */
    else
      host_ptr = host;

    g->g_interface_id = cm_sprintf(cm, "graphd;%s:%.32s",
                                   host_ptr ? host_ptr : "???", i ? i : "???");
    cm_free(cm, host);
  }
  return g->g_interface_id ? g->g_interface_id : "???";
}
