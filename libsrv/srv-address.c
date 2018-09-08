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
#include "srvp.h"

#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#define ISDIGIT(ch) (isascii(ch) && isdigit(ch))
#define ISALNUM(ch) (isascii(ch) && isalnum(ch))

#define IS_LIT(lit, s, e) \
  ((e) - (s) == sizeof(lit) - 1 && !strncasecmp(lit, s, sizeof(lit) - 1))

static char const* end_port(char const* s, char const* e) {
  if (s >= e) return NULL;
  if (!ISDIGIT(*s)) return NULL;
  while (s < e && ISDIGIT(*s)) s++;
  if (s < e && *s == '.') return NULL;
  return s;
}

static char const* end_ip(char const* s, char const* e) {
  int i;

  if (s >= e) return NULL;

  if (!ISDIGIT(*s)) return NULL;
  s++;
  if (s < e && ISDIGIT(*s)) {
    s++;
    if (s < e && ISDIGIT(*s)) s++;
  }

  for (i = 0; i < 3; i++) {
    if (s >= e || *s != '.') return NULL;
    s++;
    if (s >= e || !ISDIGIT(*s)) return NULL;
    s++;
    if (s < e && ISDIGIT(*s)) {
      s++;
      if (s < e && ISDIGIT(*s)) s++;
    }
  }

  if (s < e && ISDIGIT(*s)) return NULL;
  return s;
}

/**
 * @brief Break an arbitrary address into "IP" and "port".
 *
 *  Pointers bracketing the IP and port parts of an
 *  address are assigned to output parameters.  No
 *  allocations or '\0'-terminations happen.
 *
 *  If an address doesn't fit into the IP + port schema,
 *  the parts that couldn't be recovered are set to
 *  empty strings.
 *
 *  Even in case of failure, the output parameters are
 *  always assigned *something* - it is safe to call
 *  the function and use the resulting parameters,
 *  regardless of input.
 *
 *  General design note:
 *  ====================
 *
 *  If you do have a choice, do NOT publish or consume
 *  addresses split into IP address and port - there are
 *  many ways of contacting a server, and only one of them
 *  (TCP sockets) have this structure.
 *  (For example, Unix domain sockets have just a pathname.)
 *
 *  Instead, publish addresses as compound addresses with
 *  a URL-like schema for distinguishing the different
 *  networking mechanisms.
 *
 * @param s		'\0'-terminated address
 * @param ip_s		out: beginning of the "IP" part
 * @param ip_e 		out: pointer just after last byte of IP
 * @param port_s	out: beginning of the "port" part
 * @param port_e 	out: pointer just after last byte of port
 */
void srv_address_ip_port(char const* s, char const** ip_s, char const** ip_e,
                         char const** port_s, char const** port_e) {
  char const* e = s + strlen(s);
  char const *p, *q;

  *port_s = *port_e = *ip_s = *ip_e = "";
  p = s;
  while (p < e) {
    if (ip_s != NULL && ISDIGIT(*p) && (p == s || !ISALNUM(p[-1]))) {
      q = end_ip(p, e);
      if (q != NULL) {
        *ip_s = p;
        *ip_e = q;

        ip_s = NULL;
        ip_e = NULL;

        p = q;
        continue;
      }
    }
    if (port_s != NULL && ISDIGIT(*p) && (p == s || !ISALNUM(p[-1]))) {
      q = end_port(p, e);
      if (q != NULL) {
        *port_s = p;
        *port_e = q;

        p = q;
        continue;
      }
    }
    p++;
  }
}

/**
 * @brief Utility: Return the fully qualified domainname of a system.
 *
 * @param cm 	allocate via this
 * @return NULL on allocation error, otherwise a copy of the
 *  	hostname allocated in cm.
 */
char* srv_address_fully_qualified_domainname(cm_handle* cm) {
  char host_buf[1024], domain_buf[1024];
  char* paren;

  /*  Get hostname ingredients.
   */
  *domain_buf = '\0';
  if (gethostname(host_buf, sizeof host_buf) != 0)
    snprintf(host_buf, sizeof host_buf, "???");

  if (strchr(host_buf, '.') != NULL) return cm_bufmalcpy(cm, host_buf);

  if (getdomainname(domain_buf, sizeof domain_buf) != 0 ||
      *domain_buf == '\0' || strcasecmp(domain_buf, "(none)") == 0) {
    struct hostent* he;

    he = gethostbyname(host_buf);
    if (he != NULL && he->h_name != NULL && he->h_name[0] != '\0')
      return cm_bufmalcpy(cm, he->h_name);

    return cm_bufmalcpy(cm, host_buf);
  }

  if ((paren = strrchr(domain_buf, '(')) != NULL &&
      strcasecmp(paren, "(none)") == 0) {
    *paren = '\0';
    if (paren > domain_buf && paren[-1] == '.') paren[-1] = '\0';

    if (*domain_buf == '\0') return cm_bufmalcpy(cm, host_buf);
  }
  return cm_sprintf(cm, "%s.%s", host_buf, domain_buf);
}

static bool could_be_hostname(char const* s, char const* e) {
  char const* const s0 = s;

  for (; s < e; s++) {
    if (!isascii(*s) || (!isalnum(*s) && *s != '-' && *s != '.')) return false;

    if (s > s0 && !isalnum(s[0]) && !isalnum(s[-1])) return false;
  }
  return true;
}

#define TCPC "tcp:"

/* Scan the host and port from a tcp:// format url
 *
 * The url and port strings will be copied into individual
 * strings allocated from the storage provided.  The
 * storage parameter should point to at least url_e - url_s
 * bytes.
 */
static int srv_address_scan_url(cl_handle* cl, char const* const url_s,
                                char const* const url_e, char** host,
                                char** port, char* storage) {
  char const* host_s = 0;
  char const* host_e = 0;
  char const* port_s = 0;
  char const* port_e = 0;
  char const* last_colon;

  *host = 0;
  *port = 0;

  if (!strncasecmp(url_s, TCPC, sizeof TCPC - 1))
    host_s = url_s + sizeof TCPC - 1;
  else
    return SRV_ERR_SYNTAX; /* no tcp: */

  while (*host_s && host_s < url_e && '/' == *host_s) host_s++;

  if (host_s >= url_e) return 0; /* default host and port */

  last_colon = url_e - 1;
  while (':' != *last_colon && last_colon > host_s) last_colon--;
  if (last_colon != host_s) {
    host_e = last_colon;
    if (!could_be_hostname(host_s, host_e)) return SRV_ERR_SYNTAX;

    port_s = last_colon + 1;
    port_e = url_e;
  } else
    host_e = url_e; /* just a hostname, default port */

  if (host_s && host_e) {
    size_t n = host_e - host_s;

    if (n) {
      memcpy(storage, host_s, host_e - host_s);
      storage[n] = '\0';
      *host = storage;
      storage += n + 1;
    }
  }
  if (port_s && port_e) {
    size_t n = port_e - port_s;

    if (n) {
      memcpy(storage, port_s, port_e - port_s);
      storage[n] = '\0';
      *port = storage;
      storage += n + 1;
    }
  }

  return 0;
}

#undef TCPC

/**
 * @brief Destroy a service address
 * @param addr	NULL or address to destroy
 */
void srv_address_destroy(srv_address* sa) {
  if (!sa) return;

  cm_free(sa->addr_cm, sa);
}

/* Create an address based on a tcp://.. url
 */
int srv_address_create_url(cm_handle* cm, cl_handle* cl, char const* url_s,
                           char const* url_e, srv_address** sa_out) {
  size_t n = url_s ? (size_t)(1 + (url_e - url_s)) : 0;
  srv_address* sa = cm_malloc(cm, sizeof *sa + n * 2);
  int err;
  char* url;
  char* host;
  char* port;

  cl_assert(cl, n > 0);

  *sa_out = 0;
  if (!sa) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "cm_malloc", err,
                 "%.*s: failed to allocate an address structure",
                 (int)(url_e - url_s), url_s);

    return err;
  }

  url = memcpy((char*)(sa + 1), url_s, n - 1);
  url[n - 1] = 0;

  err = srv_address_scan_url(cl, url_s, url_e, &host, &port, url + n);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "srv_address_scan_url", err,
                 "Unable to scan host/port from %s", url);
    cm_free(cm, sa);

    return err;
  }

  sa->addr_cm = cm;
  sa->addr_url = url;
  sa->addr_host = host;
  sa->addr_port = port;
  *sa_out = sa;

  return 0;
}

/* Create an addressed based on host and port strings.
 */
int srv_address_create_host_port(cm_handle* cm, cl_handle* cl,
                                 char const* host_s, char const* host_e,
                                 char const* port_s, char const* port_e,
                                 srv_address** sa_out) {
  size_t const host_n = host_e ? host_e - host_s : 0;
  size_t const port_n = port_e ? port_e - port_s : 0;
  srv_address* sa =
      cm_malloc(cm, sizeof *sa + sizeof "tcp://:" + 2 * (host_n + port_n + 2));
  char* url;
  int url_n;
  char* host;
  char* port;

  if (!sa) return ENOMEM;

  url = (char*)(sa + 1);
  url_n = sprintf(url, "tcp://%.*s:%.*s", (int)host_n, host_s ? host_s : "",
                  (int)port_n, port_s ? port_s : "");

  host = host_n > 0 ? memcpy(url + url_n + 1, host_s, host_n) : (char*)0;
  if (host != NULL) host[host_n] = 0;

  port = port_n > 0 ? memcpy(url + url_n + 1 + host_n + !!host, port_s, port_n)
                    : (char*)0;
  if (port != NULL) port[port_n] = 0;

  sa->addr_cm = cm;
  sa->addr_url = url;
  sa->addr_host = host;
  sa->addr_port = port;
  *sa_out = sa;

  return 0;
}

int srv_address_copy(cm_handle* cm, cl_handle* cl, srv_address* from,
                     srv_address** to) {
  return srv_address_create_url(cm, cl, from->addr_url,
                                from->addr_url + strlen(from->addr_url), to);
}
