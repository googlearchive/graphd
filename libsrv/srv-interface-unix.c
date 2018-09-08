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
#include <string.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/fcntl.h>
#if __sun__
#include <fcntl.h>
#endif

#include "srvp.h"
#include "srv-interface.h"
#include "srv-interface-socket.h"

/*  Per-server session structure.  Just used to accept() and
 *  start new connections.
 */
typedef struct unix_server_session {
  es_descriptor unixs_ed;
  srv_handle *unixs_srv;
  es_handle *unixs_es;
  char const *unixs_name;
  struct sockaddr_un unixs_sun;
  int unixs_sock;

} unix_server_session;

#define IS_LIT(lit, s, e) \
  ((e) - (s) == sizeof(lit) - 1 && !strncasecmp(lit, s, sizeof(lit) - 1))

/**
 * @brief Return whether an address is a unix socket address.
 * @param s beginning of the text to scan.
 * @param e pointer just after the end of the text.
 * @return 0 if the address isn't a unix socket address, 1 if it is.
 */
static int unix_match(char const *s, char const *e) {
  char const *col;

  if (e - s >= 4 && !strncasecmp(s, "unix", 4) && (e - s == 4 || s[4] == ':'))
    return true;

  if ((col = memchr(s, ':', e - s)) == NULL) col = e;

  if (memchr(s, '/', col - s) == NULL) return false;

  return true;
}

/* Scan interface-specific configuration data beyond
 * the mere address, if any.
 */
static int unix_config_read(srv_config *cf, cl_handle *cl,
                            srv_interface_config *icf, char **s,
                            char const *e) {
  return 0;
}

/**
 * @brief Utility: translate a parameter string to a socket address.
 * @param cm allocate here
 * @param cl log via this stream
 * @param text scan this
 * @param out Write the socket address to this.
 */
static int unix_scan(cm_handle *cm, cl_handle *cl, char const *text,
                     struct sockaddr_un *out) {
  size_t len;

  len = strlen(text);
  if (len >= 108) {
    // Unix domain sockets are weird -- 108 char limit
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
           "unix: path for socket \"%s\" is too long", text);
    return SRV_ERR_ADDRESS;
  }

  if (len == 0) {
    cl_log(cl, CL_LEVEL_OPERATOR_ERROR, "unix: no path for socket");
    return SRV_ERR_ADDRESS;
  }

  memset(out, 0, sizeof(*out));
  out->sun_family = AF_UNIX;
  strcpy(out->sun_path, text);

  return 0;
}

#define unix_socket_is_lost(x) false

static const srv_session_interface_type unix_session_interface_type = {
    srv_socket_run, srv_socket_listen, srv_socket_set_timeout};

static int unix_new_conn(srv_handle *srv, int sock, struct sockaddr_un *peer,
                         bool is_server, srv_session **ses,
                         char const *displayname) {
  int err = 0;
  // unsigned long  		  in;
  // struct sockaddr_un	  addr;
  // socklen_t  		  addrlen = sizeof addr;
  size_t n = (displayname != NULL ? (strlen(displayname) + 1) : 0);
  unix_connection *uconn = cm_malloc(srv->srv_cm, sizeof *uconn + n);

  if (uconn == NULL) return ENOMEM;

  memset(uconn, 0, sizeof *uconn);

  socket_connection *conn = (socket_connection *)uconn;

  conn->conn_sock = sock;
  conn->conn_srv = srv;
  conn->conn_es = srv->srv_es;
  conn->conn_ed.ed_callback = srv_socket_es_connection_callback;
  conn->conn_socket_type = SRV_SOCKET_LOCAL;

  if (displayname != NULL)
    conn->conn_displayname = memcpy((char *)(conn + 1), displayname, n);
  else
    conn->conn_displayname = "";

  conn->conn_peername = (char *)conn->conn_displayname;
  conn->conn_ed.ed_displayname = conn->conn_displayname;

  /*
  snprintf(conn->conn_peername, sizeof conn->conn_peername,
                   "unix-socket/fd:%d", conn->conn_sock);
  snprintf(conn->conn_sockname, sizeof conn->conn_sockname,
                   "unix-socket/fd:%d", conn->conn_sock);
  */

  err = es_open(srv->srv_es, conn->conn_sock, ES_INPUT, &conn->conn_ed);

  if (err) {
    cl_log_errno(srv->srv_cl, CL_LEVEL_ERROR, "es_open", err,
                 "Unable to register %s for polling", conn->conn_displayname);
    goto free_sock;
  }

  /*  Create a new protocol session.  This must happen after the
   *  interfaces have been hooked up to the event loop system,
   *  in case the session startup creates a new request.
   *
   *  (Requests produce output; output must be written; creating
   *  a request causes us to wait for output capacity.  We can't
   *  update the events we wait for if we haven't initialized the
   *  event system yet!)
   */
  conn->conn_protocol_session = srv_session_create(
      srv->srv_cm, srv, &unix_session_interface_type, (void *)conn, is_server,
      conn->conn_displayname, conn->conn_displayname);
  if (!conn->conn_protocol_session) {
    err = errno;
    cl_log_errno(srv->srv_cl, CL_LEVEL_ERROR, "srv_session_create", err,
                 "Unable to allocate protocol session for %s",
                 conn->conn_displayname);
    goto free_sock;
  }

  if (ses != NULL) *ses = conn->conn_protocol_session;

  srv_session_schedule(conn->conn_protocol_session);

  cl_log(srv->srv_cl, CL_LEVEL_INFO, "%s: C: [new unix connection on fd %d]",
         conn->conn_displayname, conn->conn_sock);

  return 0;

free_sock:
  srv_socket_close(srv->srv_cl, sock, true);
  cm_free(srv->srv_cm, conn);

  return err;
}

static int unix_accept(unix_server_session *unixs) {
  srv_handle *const srv = unixs->unixs_srv;
  /* addr (the peer address) is junk for unix sockets */
  struct sockaddr_un addr;
  socklen_t addrlen = sizeof addr;
  int sock;
  int err;
  char my_displayname[200];

  sock = accept(unixs->unixs_sock, (struct sockaddr *)&addr, &addrlen);

  if (sock < 0) {
    /*  Might happen during normal connection processing
     *  if client disconnects between initial connect
     *  and our accept -- just log it.
     *
     *  Silently ignore an EWOULDBLOCK
     */
    err = errno;
    if (err != EWOULDBLOCK) {
      cl_log_errno(srv->srv_cl, CL_LEVEL_ERROR, "accept", err,
                   "conn server: accept %s failed [ignored]",
                   unixs->unixs_name);
    }
    return 0;
  }

  err = srv_socket_block(srv->srv_cl, sock, false);
  if (err) {
    cl_log_errno(srv_log(srv), CL_LEVEL_ERROR, "srv_socket_block", err,
                 "Unable to make socket %d non-blocking", sock);
    srv_socket_close(srv->srv_cl, sock, true);
    return err;
  }

  snprintf(my_displayname, sizeof my_displayname, "[accept for %s fd:%hu]",
           unixs->unixs_name, sock);
  return unix_new_conn(srv, sock, &addr,
                       /* server? */ true, 0, my_displayname);
}

static void unix_es_server_callback(es_descriptor *ed, int fd,
                                    unsigned int events) {
  unix_server_session *unixs = (struct unix_server_session *)ed;
  srv_handle *srv = unixs->unixs_srv;

  cl_log(srv->srv_cl, CL_LEVEL_DEBUG,
         "unix event (server): ed=%p, fd=%d, events=%x", (void *)ed, fd,
         events);

  if (events & ES_INPUT) {
    unix_accept(unixs);
    cl_cover(srv->srv_cl);
  }
  if (events & (ES_ERROR | ES_EXIT)) {
    es_close(srv->srv_es, ed);
    cl_cover(srv->srv_cl);
  }
}

/* Create event handlers for the interface.
 */
static int unix_open(srv_handle *srv, srv_interface_config *icf, void **out) {
  int err, one = 1;
  unix_server_session *unixs;

  unixs = cm_malloc(srv->srv_cm, sizeof(*unixs));
  if (unixs == NULL) return ENOMEM;

  memset(unixs, 0, sizeof(*unixs));

  cl_assert(srv->srv_cl, icf);
  cl_assert(srv->srv_cl, srv->srv_es);

  unixs->unixs_srv = srv;
  unixs->unixs_ed.ed_callback = unix_es_server_callback;
  unixs->unixs_ed.ed_displayname = icf->icf_address;
  unixs->unixs_name = icf->icf_address;

  if (strncasecmp(unixs->unixs_name, "unix://", 7) == 0) {
    cl_cover(srv->srv_cl);
    unixs->unixs_name += 7;
  } else if (strncasecmp(unixs->unixs_name, "unix:", 5) == 0) {
    cl_cover(srv->srv_cl);
    unixs->unixs_name += 5;
  }

  err =
      unix_scan(srv->srv_cm, srv->srv_cl, unixs->unixs_name, &unixs->unixs_sun);

  if (err) {
    cm_free(srv->srv_cm, unixs);
    return err;
  }

  /* Open the server socket.
   */

  if ((unixs->unixs_sock = socket(PF_LOCAL, SOCK_STREAM, 0)) == -1) {
    err = errno;
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "unix_open: can't create server socket: %s", strerror(err));
    cl_cover(srv->srv_cl);
    cm_free(srv->srv_cm, unixs);
    return err;
  }

  if (setsockopt(unixs->unixs_sock, SOL_SOCKET, SO_REUSEADDR, (void *)&one,
                 sizeof(one)) != 0)
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "unix_open: setsockopt(%s, "
           "SO_REUSEADDR, 1) fails: %s [ignored]",
           icf->icf_address, strerror(err));

  /* Before binding, access(2) the file to see if it exists.
   * If it does, unlink it. This is harmless to anything using it,
   * and allows us to recreate and bind to it if it exists. */
  if (access(unixs->unixs_sun.sun_path, F_OK | W_OK) == 0) {
    unlink(unixs->unixs_sun.sun_path);
  }

  if (bind(unixs->unixs_sock, (struct sockaddr *)&unixs->unixs_sun,
           sizeof(unixs->unixs_sun))) {
    err = errno;
    cl_log(srv->srv_cl, CL_LEVEL_OPERATOR_ERROR,
           "unix_open: can't bind server socket to \"%s\": %s",
           icf->icf_address, strerror(err));
    cl_cover(srv->srv_cl);

  err:
    srv_socket_close(srv->srv_cl, unixs->unixs_sock, true);
    unlink(unixs->unixs_name);
    cl_cover(srv->srv_cl);
    cm_free(srv->srv_cm, unixs);

    return err;
  }
  if (listen(unixs->unixs_sock, 20)) {
    err = errno;
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "unix_open: can't listen(20) to \"%s\": %s", icf->icf_address,
           strerror(err));
    cl_cover(srv->srv_cl);
    goto err;
  }
  /* Set the listen-port file descriptor
     to non-blocking */
  if (fcntl(unixs->unixs_sock, F_SETFL, O_NONBLOCK)) {
    err = errno;
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "unix_open: can't set listen port to non-blocking (fd: %d) : %s",
           unixs->unixs_sock, strerror(err));
    cl_cover(srv->srv_cl);
    goto err;
  }

  /*  Listen to incoming input.
   */

  err = es_open(srv->srv_es, unixs->unixs_sock, ES_INPUT, &unixs->unixs_ed);
  if (err != 0) {
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "unix_open: can't es_open \"%s\" for input: %s", icf->icf_address,
           strerror(err));
    cl_cover(srv->srv_cl);
    goto err;
  }

  cl_log(srv->srv_cl, CL_LEVEL_INFO, "%s listening on %s (fd %d)",
         srv_program_name(srv), unixs->unixs_sun.sun_path, unixs->unixs_sock);
  cl_cover(srv->srv_cl);
  *out = unixs;
  return 0;
}

/* Release resources connected to a specific interface.
 */
static void unix_close(srv_handle *srv, srv_interface_config *icf, void *data) {
  unix_server_session *unixs = data;

  cl_assert(srv->srv_cl, unixs != NULL);

  if (srv->srv_es != NULL) es_close(srv->srv_es, &unixs->unixs_ed);

  srv_socket_close(srv->srv_cl, unixs->unixs_sock, /* block? */ true);

  unlink(unixs->unixs_name);

  cm_free(srv->srv_cm, unixs);
}

/* Create a session connected to the passed-in address.
 *
 */
static int unix_connect(srv_handle *srv, char const *url,
                        srv_session **ses_out) {
  cl_handle *const cl = srv_log(srv);
  struct sockaddr_un sock_un;
  bool pending_connect = true;
  int sock = -1;
  int err;

  cl_assert(cl, ses_out != NULL);
  *ses_out = NULL;

  /* Skip a leading protocol prefix.
   */
  if (strncasecmp(url, "unix://", 7) == 0)
    url += 7;
  else if (strncasecmp(url, "unix:", 5) == 0)
    url += 5;

  err = unix_scan(srv->srv_cm, srv->srv_cl, url, &sock_un);

  if (err != 0) return err;

  sock = socket(PF_LOCAL, SOCK_STREAM, 0);
  if (-1 == sock) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "socket", err,
                 "Unable to create outbound socket for %s", url);

    return err;
  }

  err = srv_socket_block(srv->srv_cl, sock, false);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "srv_socket_block", err,
                 "Unable to make socket %d non-blocking", sock);
    goto close_socket;
  }

  if (!connect(sock, (struct sockaddr *)&sock_un, sizeof(sock_un)))
    pending_connect = false;

  else if (errno != EINPROGRESS) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_OPERATOR_ERROR, "connect", err,
                 "Unable to connect to %s", url);
    goto close_socket;
  }

  err =
      unix_new_conn(srv, sock, &sock_un, /* is server? */ false, ses_out, url);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "unix_new_conn", err,
                 "Unable to create session for %s", url);
    goto close_socket;
  }

  cl_assert(cl, *ses_out != NULL);
  (*ses_out)->ses_pending_connect = pending_connect;

  return 0;

close_socket:
  cl_assert(srv->srv_cl, err != 0);
  srv_socket_close(srv->srv_cl, sock, true);

  return err;
}

/**
 * @brief Interface plugin structure for the "unix" interface.
 */
const srv_interface_type srv_interface_type_unix[1] = {{

    "unix", unix_match, unix_config_read, unix_open, unix_close, unix_connect}};
