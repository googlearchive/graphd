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

#include "srvp.h"
#include "srv-interface.h"

static srv_interface_type const *const srv_interface_types[] = {
    srv_interface_type_tty, srv_interface_type_tcp, srv_interface_type_unix,
    /*
            srv_interface_type_local,
    */
    NULL};

srv_interface_type const *srv_interface_type_match(char const *if_s,
                                                   char const *if_e) {
  srv_interface_type const *const *sit;

  for (sit = srv_interface_types; *sit; sit++)
    if ((*(*sit)->sit_match)(if_s, if_e)) return *sit;
  return NULL;
}

int srv_interface_add_and_listen(srv_handle *srv, char const *address) {
  srv_interface_config *icf;
  srv_interface_type const *sit;
  char const *s, *e;

  if (address == NULL) {
    cl_cover(srv->srv_cl);
    address = "";
  }

  s = address;
  e = address + strlen(address);

  if (!(sit = srv_interface_type_match(s, e))) {
    cl_log(srv->srv_cl, CL_LEVEL_OPERATOR_ERROR,
           "unknown interface type \"%s\"", address);
    cl_cover(srv->srv_cl);
    return EINVAL;
  }

  if (!(icf = srv_interface_config_alloc(srv->srv_config, srv->srv_cl, s, e)))
    return ENOMEM;

  icf->icf_type = sit;

  srv_interface_config_chain_in(srv->srv_config, icf);

  return srv_interface_create(srv, icf);
}

srv_interface_config *srv_interface_config_alloc(srv_config *cf, cl_handle *cl,
                                                 char const *if_s,
                                                 char const *if_e) {
  srv_interface_config *icf;
  char *heap;

  cl_assert(cl, cf != NULL);
  cl_assert(cl, if_s != NULL);
  cl_assert(cl, if_e != NULL);
  cl_assert(cl, cf->cf_cm != NULL);
  cl_cover(cl);

  if (!(icf = cm_malloc(cf->cf_cm, sizeof(*icf) + (if_e - if_s) + 1))) {
    cl_log(cl, CL_LEVEL_ERROR,
           "srv: failed to allocate %lu bytes for "
           "interface \"%.*s\"'s configuration\n",
           (unsigned long)(sizeof(*icf) + (if_e - if_s) + 1),
           (int)(if_e - if_s), if_s);
    return NULL;
  }

  heap = (char *)(icf + 1);
  memcpy(heap, if_s, if_e - if_s);
  heap[if_e - if_s] = '\0';
  icf->icf_address = heap;
  icf->icf_next = NULL;
  icf->icf_config = cf;

  return icf;
}

void srv_interface_config_chain_in(srv_config *cf, srv_interface_config *icf) {
  *cf->cf_interface_tail = icf;
  cf->cf_interface_tail = &icf->icf_next;
}

int srv_interface_config_read(srv_config *cf, cl_handle *cl,
                              srv_interface_config *icf, char **s,
                              char const *e) {
  cl_assert(cl, icf);
  cl_assert(cl, icf->icf_type);
  cl_cover(cl);

  return (*icf->icf_type->sit_config_read)(cf, cl, icf, s, e);
}

int srv_interface_config_add(srv_config *cf, cl_handle *cl,
                             char const *address) {
  srv_interface_config *icf;
  srv_interface_type const *sit;
  char const *s, *e;

  cl_assert(cl, cf != NULL);
  cl_assert(cl, cf->cf_cm != NULL);

  if (address == NULL) {
    cl_cover(cl);
    address = "";
  }

  s = address;
  e = address + strlen(address);

  if (!(sit = srv_interface_type_match(s, e))) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR, "unknown interface type \"%s\"",
           address);
    cl_cover(cl);
    return EINVAL;
  }

  if (!(icf = srv_interface_config_alloc(cf, cl, s, e))) return ENOMEM;

  icf->icf_type = sit;

  srv_interface_config_chain_in(cf, icf);
  cl_cover(cl);
  return 0;
}

int srv_interface_create(srv_handle *srv, srv_interface_config *icf) {
  srv_interface *i;
  void *app_data;
  int err;

  cl_assert(srv->srv_cl, icf != NULL);
  cl_assert(srv->srv_cl, icf->icf_type != NULL);
  cl_cover(srv->srv_cl);

  if ((i = cm_zalloc(srv->srv_cm, sizeof(*i))) == NULL) return ENOMEM;
  if ((err = (*icf->icf_type->sit_open)(srv, icf, &app_data)) != 0) {
    cm_free(srv->srv_cm, i);
    return err;
  }

  i->i_config = icf;
  i->i_data = app_data;
  i->i_next = NULL;

  *srv->srv_if_tail = i;
  srv->srv_if_tail = &i->i_next;

  cl_log(srv->srv_cl, CL_LEVEL_DEBUG, "srv_interface_create: created %p", i);
  return 0;
}

/**
 * @brief Shut down the interfaces we opened.
 *
 *  Call the sit_close() callback of the interfaces previously
 *  created with sit_open().
 *
 *  This is the code that closes a server socket while a
 *  server is shut down using -z.
 *
 * @param srv	server we're doing this for
 */
void srv_interface_shutdown(srv_handle *srv) {
  srv_interface *i;

  cl_log(srv->srv_cl, CL_LEVEL_DEBUG,
         "srv_interface_shutdown: shutting down interfaces (head %p)",
         (void *)srv->srv_if_head);

  while ((i = srv->srv_if_head) != NULL) {
    if ((srv->srv_if_head = i->i_next) == NULL)
      srv->srv_if_tail = &srv->srv_if_head;

    cl_log(srv->srv_cl, CL_LEVEL_DEBUG, "srv_interface_shutdown: shutdown %p",
           i);
    (*i->i_config->icf_type->sit_close)(srv, i->i_config, i->i_data);
    cm_free(srv->srv_cm, i);
  }

  cl_log(srv->srv_cl, CL_LEVEL_DEBUG, "srv_interface_shutdown: done");
}

/**
 * @brief List the interfaces installed in a server, as a string.
 *
 *  For use in human-readable debug- or informational messages.
 *
 * @param srv	server module handle
 * @param buf	buffer to use while formatting
 * @param size	number of bytes pointed to by the buffer
 * @return a '\0' terminated string of comma-separated interface
 *	specifications as given on the commandline, with duration
 *	equal to or greater than the buffer, or an approximation
 *	if insufficient space was available.
 */
char const *srv_interface_to_string(srv_handle *srv, char *buf, size_t size) {
  srv_interface *i;
  char *e = buf + size, *w = buf;
  char const *r;

  if (srv->srv_if_head == NULL) return "(none)";

  for (i = srv->srv_if_head; i != NULL; i = i->i_next) {
    cl_assert(srv->srv_cl, i->i_config != NULL);

    /* comma + space + quote + icf->icf_address + quote + NUL
     */
    if (5 + strlen(i->i_config->icf_address) > e - w) {
      if (w == buf) return "...";

      if (e - w > 4) {
        *w++ = '.';
        *w++ = '.';
        *w++ = '.';
        *w++ = '\0';
      }
      return buf;
    }

    r = i->i_config->icf_address;
    if (w > buf) {
      *w++ = ',';
      *w++ = ' ';
    }
    *w++ = '"';
    while ((*w = *r++) != '\0') w++;
    *w++ = '"';
  }
  if (w < buf + size)
    *w = '\0';
  else if (size > 0)
    buf[size - 1] = '\0';

  return buf;
}

/**
 * @brief Make an outgoing session
 *
 * @param srv	server module handle
 * @param url	where to connect to
 * @param ses_out assign the (partially connected) session here
 * @return 0 on (partial?) success, otherwise a nonzero error code.
 */
int srv_interface_connect(srv_handle *srv, char const *url,
                          srv_session **ses_out) {
  cl_handle *const cl = srv->srv_cl;
  srv_interface_type const *sit;
  int err;

  /*
          if (srv->srv_if_head == NULL)
                  return SRV_ERR_NO;
  */

  /*  Get the right interface type for this address.
   */
  sit = srv_interface_type_match(url, url + strlen(url));
  if (sit == NULL) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "srv_interface_connect: unknown interface "
           "specification \"%s\"",
           url);
    cl_cover(cl);

    return SRV_ERR_ADDRESS;
  }

  /*  Have the interface connect, whatever that means.
   */
  if (sit->sit_connect == NULL) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "srv_interface_connect: %s: interface %s "
           "doesn't support outgoing connections",
           url, sit->sit_type);
    cl_cover(cl);

    return SRV_ERR_NOT_SUPPORTED;
  }

  err = (*sit->sit_connect)(srv, url, ses_out);
  if (err != 0)
    cl_log(cl, CL_LEVEL_FAIL, "[interface %s].sit_connect(%s): %s",
           sit->sit_type, url, cl_strerror(srv->srv_cl, err));
  return err;
}

void srv_interface_balance(srv_handle *srv, bool activate) {
  srv_shared_connection_activate_index(srv, srv->srv_smp_index, activate);
}
