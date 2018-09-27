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
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <netdb.h>
#include <sys/fcntl.h>
#if __sun__
#include <fcntl.h>
#endif

#include "srvp.h"
#include "srv-interface.h"
#include "srv-interface-socket.h"
#include "srv-interface-tcp.h"

/*  Per-server session structure.  Just used to accept() and
 *  start new connections.
 */
typedef struct tcp_server_session {
  es_descriptor tcps_ed;
  srv_handle *tcps_srv;
  es_handle *tcps_es;
  char const *tcps_name;
  struct sockaddr_in tcps_sin;
  int tcps_sock;

} tcp_server_session;

/*  Per-connection session structure.  Buffered connection passing
 *  data back and forth.
 */
typedef struct tcp_connection {
  socket_connection tconn_connection;
  struct sockaddr_in tconn_peer;
  char tconn_sockname_buf[sizeof("123.123.123.123:12345")];
  char tconn_peername_buf[sizeof("123.123.123.123:12345")];

/*  Every 50 rounds of formatting, poll for whether we've lost
 *  our connection.
 */
#define SRV_INTERFACE_TCP_POLL_LOST 50
  unsigned int tconn_poll_lost;

} tcp_connection;

#define IS_LIT(lit, s, e) \
  ((e) - (s) == sizeof(lit) - 1 && !strncasecmp(lit, s, sizeof(lit) - 1))

/**
 * @brief Return whether an address is a TCP address.
 * @param s beginning of the text to scan.
 * @param e pointer just after the end of the text.
 * @return 0 if the address isn't a TCP address, 1 if it is.
 */
static int tcp_match(char const *s, char const *e) {
  char const *col;

  if (e - s >= 3 && !strncasecmp(s, "tcp", 3) && (e - s == 3 || s[3] == ':'))
    return true;

  if ((col = memchr(s, ':', e - s)) == NULL) col = e;

  if (memchr(s, '.', col - s) == NULL || memchr(s, '/', col - s) != NULL)
    return false;

  return true;
}

/* Scan interface-specific configuration data beyond
 * the mere address, if any.
 */
static int tcp_config_read(srv_config *cf, cl_handle *cl,
                           srv_interface_config *icf, char **s, char const *e) {
  char *s_orig = *s;
  char const *tok_s, *tok_e;
  char const *host_s = NULL, *host_e = NULL;
  char const *port_s = NULL, *port_e = NULL;
  char const *colon = ":";
  char *url;
  int tok;
  size_t need;

  if ((tok = srv_config_get_token(s, e, &tok_s, &tok_e)) != '{') {
    *s = s_orig;
    return 0;
  }

  while ((tok = srv_config_get_token(s, e, &tok_s, &tok_e)) != '}' &&
         tok != EOF) {
    if (IS_LIT("host", tok_s, tok_e)) {
      tok = srv_config_get_token(s, e, &tok_s, &tok_e);
      if (tok != '"' && tok != 'a') {
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "configuration file \"%s\", "
               "line %d: expected "
               "IP address or host name, "
               "got \"%.*s\"\n",
               cf->cf_file, srv_config_line_number(cf, *s),
               (int)(tok_e - tok_s), tok_s);
        return EINVAL;
      }
      if (host_s != NULL) {
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "configuration file \"%s\", "
               "line %d: duplicate \"host\" "
               "(%.*s and %.*s) ",
               cf->cf_file, srv_config_line_number(cf, *s),
               (int)(host_e - host_s), host_s, (int)(tok_e - tok_s), tok_s);
        return EINVAL;
      }
      host_s = tok_s;
      host_e = tok_e;
    } else if (IS_LIT("port", tok_s, tok_e)) {
      tok = srv_config_get_token(s, e, &tok_s, &tok_e);
      if (tok != '"' && tok != 'a') {
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "configuration file \"%s\", "
               "line %d: expected "
               "port number or name, got \"%.*s\"\n",
               cf->cf_file, srv_config_line_number(cf, *s),
               (int)(tok_e - tok_s), tok_s);
        return EINVAL;
      }
      if (port_s != NULL) {
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "configuration file \"%s\", "
               "line %d: duplicate \"port\" "
               "(%.*s and %.*s) ",
               cf->cf_file, srv_config_line_number(cf, *s),
               (int)(port_e - port_s), port_s, (int)(tok_e - tok_s), tok_s);
        return EINVAL;
      }
      port_s = tok_s;
      port_e = tok_e;
    } else {
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "configuration file \"%s\", line %d: expected "
             "\"port\" or \"host\", got \"%.*s\"\n",
             cf->cf_file, srv_config_line_number(cf, *s), (int)(tok_e - tok_s),
             tok_s);
    }
  }

  if (!host_s) {
    host_s = host_e = "";
  }
  if (!port_s) {
    port_s = port_e = "";
    colon = "";
  }

  need = (host_e - host_s) + (port_e - port_s) + sizeof("tcp://:");
  if ((url = cm_malloc(cf->cf_cm, need)) == NULL) return ENOMEM;

  snprintf(url, need, "tcp://%.*s%s%.*s", (int)(host_e - host_s), host_s, colon,
           (int)(port_e - port_s), port_s);
  icf->icf_address = url;

  return 0;
}

/**
 * @brief Utility: translate a parameter string to a socket address.
 * @param cm allocate here
 * @param cl log via this stream
 * @param text scan this
 * @param out Write the socket address to this.
 */
static int tcp_scan(cm_handle *cm, cl_handle *cl, char const *text,
                    struct sockaddr_in *out) {
  char *text_dup, *port;

  text_dup = cm_strmalcpy(cm, text);
  if (text_dup == NULL) return ENOMEM;

  port = strchr(text_dup, ':');
  if (port != NULL) *port++ = '\0';

  memset(out, 0, sizeof(*out));

  out->sin_family = AF_INET;
  out->sin_addr.s_addr = INADDR_ANY;
  out->sin_port = htons(0);

  if (text_dup[0] != '\0') {
#ifdef __sun__
    out->sin_addr.s_addr = inet_addr(text_dup);
    if ((in_addr_t)(-1) != out->sin_addr.s_addr)
#else
    if (!inet_aton(text_dup, &out->sin_addr))
#endif
    {
      struct hostent *he;

      if ((he = gethostbyname(text_dup)) == NULL) {
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "tcp: can't resolve hostname \"%s\"", text_dup);
        cm_free(cm, text_dup);
        return SRV_ERR_ADDRESS;
      }

      if (he->h_addrtype != AF_INET) {
        cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
               "tcp: unfamiliar addrtype %d for \"%s\""
               " (XXX port me to IPV6!)",
               he->h_addrtype, text_dup);
        cm_free(cm, text_dup);
        return SRV_ERR_ADDRESS;
      }
      memcpy(&out->sin_addr.s_addr, he->h_addr, sizeof(out->sin_addr.s_addr));
    }
  }
  if (port != NULL && *port != '\0') {
    struct servent *se;

    unsigned short hu;
    if (sscanf(port, "%hu", &hu) == 1) {
      out->sin_port = htons(hu);
    } else if ((se = getservbyname(port, "tcp")) != NULL) {
      out->sin_port = se->s_port;
    } else {
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "tcp: cannot resolve service name \"%s\" "
             "(try using a number?)",
             port);
      cm_free(cm, text_dup);
      return SRV_ERR_ADDRESS;
    }
  }
  cm_free(cm, text_dup);
  return 0;
}

/*  Disable/Enable "Nagle's algorithm" on the server socket.
 *
 *  Nagle's algorithm in the TCP stack delays sending of packets
 *  until either
 *  - there are no outstanding acknowledgements or
 *  - enough data has been buffered to fill an IP packet.
 *
 *  This helps make interactive applications over a network
 *  not swamp the network with packets carrying a single
 *  keystroke as payload; but for server applications like
 *  ours, it can - given patience and just the right client
 *  behavior (ask Tim Sturge for help) - produce noticable
 *  TCP delays.
 *
 *  Setting TCP_NODELAY to 1 turns *off* Nagle's algorithm.
 */
static int tcp_socket_nagle(cl_handle *cl, int fd, bool on) {
  int flag = !on;
  int err = 0;

  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(flag)) !=
      0) {
    err = errno;
    cl_log(cl, CL_LEVEL_ERROR, "can't set TCP_NODELAY for fd %d: %s (ignored)",
           fd, strerror(errno));
  }
  return err;
}

static bool tcp_socket_is_lost(cl_handle *cl, int fd) {
#ifdef TCP_INFO

  static int level_tcp = -1;
  struct tcp_info inf;
  socklen_t inf_len;
  char const *state;

  cl_log(cl, CL_LEVEL_VERBOSE, "tcp_socket_is_lost(%d)", fd);
  if (level_tcp == -1) {
    struct protoent *pe;
    // IPPROTO_TCP should be more portable than TCP_SOL.
    level_tcp = ((pe = getprotobyname("TCP")) == NULL) ? IPPROTO_TCP : pe->p_proto;
  }

  memset(&inf, 0, sizeof inf);
  inf_len = sizeof(inf);

  if (getsockopt(fd, level_tcp, TCP_INFO, &inf, &inf_len) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "getsockopt", errno, "fd=%d", fd);
    return false;
  }

  if (inf.tcpi_state == ((state = "TCP_CLOSE_WAIT"), TCP_CLOSE_WAIT) ||
      inf.tcpi_state == ((state = "TCP_FIN_WAIT1"), TCP_FIN_WAIT1) ||
      inf.tcpi_state == ((state = "TCP_FIN_WAIT2"), TCP_FIN_WAIT2) ||
      inf.tcpi_state == ((state = "TCP_TIME_WAIT"), TCP_TIME_WAIT) ||
      inf.tcpi_state == ((state = "TCP_CLOSING"), TCP_CLOSING)) {
    cl_log(cl, CL_LEVEL_FAIL, "tcp_socket_is_lost: fd %d in state %s", fd,
           state);
    return true;
  }
  return false;

#else
  cl_log(cl, CL_LEVEL_VERBOSE,
         "tcp_socket_is_lost(%d) - can't check, don't have TCP_INFO", fd);
  return false;

#endif /* TCP_INFO available */
}

/**
 * @brief Run.
 *
 *  The server is giving this connection a time slice to run in.
 *  Use it to read and answer requests.
 *
 *  This "run" concept is different from the session (or graphd)
 *  "run" concept in that here, it's about dealing with the signals
 *  and connection buffers - all the running really has to do
 *  with input and output.
 *
 * @param conn_data connection object.
 * @param srv server module handle
 * @param ses generic session
 * @param deadline deadline in clock_t - if we run that long,
 *		we've been running too long.
 */

static void tcp_balance_decrement(srv_handle *srv) {
  int process_n = srv->srv_config->cf_processes;
  if (process_n == 1 || srv->srv_smp_index == -1) return;

  srv_shared_connection_decrement(srv, srv->srv_smp_index);
}

static bool tcp_run(void *conn_data, srv_handle *srv, srv_session *ses,
                    srv_msclock_t deadline) {
  tcp_connection *tcp_conn = conn_data;
  socket_connection *sock_conn = &tcp_conn->tconn_connection;
  bool any;
  bool connected;

  connected = false;

  if (sock_conn->conn_sock != -1) {
    connected = true;
  }

  any = srv_socket_run(conn_data, srv, ses, deadline);

  if (connected &&
      (ses->ses_interface_type == NULL || sock_conn->conn_sock == -1)) {
    // We have lost the connection. Decrement
    tcp_balance_decrement(srv);
  }
  /* Every once in a while, check whether our socket is lost.
   */
  if (ses->ses_interface_type != NULL && sock_conn->conn_sock != -1 &&
      ses->ses_bc.bc_error != SRV_BCERR_SOCKET && !ses->ses_pending_connect &&
      ses->ses_bc.bc_write_capacity_available &&
      tcp_conn->tconn_poll_lost-- < 1) {
    tcp_conn->tconn_poll_lost = SRV_INTERFACE_TCP_POLL_LOST;
    if (tcp_socket_is_lost(srv->srv_cl, sock_conn->conn_sock)) {
      ses->ses_bc.bc_error = SRV_BCERR_SOCKET;
      cl_log(srv->srv_cl, CL_LEVEL_DEBUG, "tcp_run: %s lost connection",
             ses->ses_displayname);
      any |= true;
    }
  }
  return any;
}

static const srv_session_interface_type tcp_session_interface_type = {
    tcp_run, srv_socket_listen, srv_socket_set_timeout};

/* Return true to accept the connection
   Return false to pass on accepting the connection */
static bool tcp_balance(srv_handle *srv) {
  int process_n = srv->srv_config->cf_processes;
  int max, min;
  int conn_n, my_conn_n;
  int i;

  my_conn_n = srv_shared_get_connection_count(srv, srv->srv_smp_index);

  if (process_n == 1 || srv->srv_smp_index == -1 || my_conn_n == -1)
    return true;

  min = -1;
  max = -1;

  for (i = 0; i < process_n; i++) {
    conn_n = srv_shared_get_connection_count(srv, i);
    if (conn_n == -1) continue;
    if (max == -1 || conn_n > max) max = conn_n;
    if (min == -1 || conn_n < min) min = conn_n;
  }

  cl_log(srv->srv_cl, CL_LEVEL_DEBUG, "Balancing -- my: %d, min: %d, max: %d",
         my_conn_n, min, max);

  if (my_conn_n == max && my_conn_n != min) return false;

  return true;
}

static void tcp_balance_increment(srv_handle *srv) {
  int process_n = srv->srv_config->cf_processes;
  if (process_n == 1 || srv->srv_smp_index == -1) return;

  srv_shared_connection_increment(srv, srv->srv_smp_index);
}

static int tcp_new_conn(srv_handle *srv, int sock, struct sockaddr_in *peer,
                        srv_session **ses, char const *displayname,
                        bool is_server) {
  int err = 0;
  unsigned long in;
  struct sockaddr_in addr;
  socklen_t addrlen = sizeof addr;
  size_t n = (displayname != NULL ? (strlen(displayname) + 1) : 0);
  tcp_connection *tconn = cm_malloc(srv->srv_cm, sizeof *tconn + n);

  if (tconn == NULL) return ENOMEM;

  memset(tconn, 0, sizeof *tconn);

  tconn->tconn_connection.conn_sock = sock;
  tconn->tconn_connection.conn_srv = srv;
  tconn->tconn_connection.conn_es = srv->srv_es;

  tconn->tconn_peer = *peer;
  tconn->tconn_connection.conn_ed.ed_displayname = tconn->tconn_peername_buf;

  tconn->tconn_connection.conn_ed.ed_callback =
      srv_socket_es_connection_callback;

  if (displayname != NULL)
    tconn->tconn_connection.conn_displayname =
        memcpy((char *)(tconn + 1), displayname, n);
  else
    tconn->tconn_connection.conn_displayname = "";

  /* Turn off batching of packets. */
  (void)tcp_socket_nagle(srv->srv_cl, tconn->tconn_connection.conn_sock, false);

  in = ntohl(tconn->tconn_peer.sin_addr.s_addr);
  snprintf(tconn->tconn_peername_buf, sizeof tconn->tconn_peername_buf,
           "%hu.%hu.%hu.%hu:%hu", (unsigned short)(0xFF & (in >> (8 * 3))),
           (unsigned short)(0xFF & (in >> (8 * 2))),
           (unsigned short)(0xFF & (in >> (8 * 1))),
           (unsigned short)(0xFF & in),
           (unsigned short)ntohs(tconn->tconn_peer.sin_port));
  tconn->tconn_connection.conn_peername = tconn->tconn_peername_buf;

  if (getsockname(tconn->tconn_connection.conn_sock, (struct sockaddr *)&addr,
                  &addrlen)) {
    err = errno;
    cl_log_errno(srv->srv_cl, CL_LEVEL_FAIL, "getsockname", err,
                 "Unable to get local address for %s",
                 tconn->tconn_peername_buf);
    snprintf(tconn->tconn_sockname_buf, sizeof tconn->tconn_sockname_buf,
             "[getsockname: %s]", strerror(err));
  } else {
    unsigned long const in = ntohl(addr.sin_addr.s_addr);
    snprintf(tconn->tconn_sockname_buf, sizeof tconn->tconn_sockname_buf,
             "%hu.%hu.%hu.%hu:%hu", (unsigned short)(0xFF & (in >> (8 * 3))),
             (unsigned short)(0xFF & (in >> (8 * 2))),
             (unsigned short)(0xFF & (in >> (8 * 1))),
             (unsigned short)(0xFF & in), (unsigned short)ntohs(addr.sin_port));
  }

  err = es_open(srv->srv_es, tconn->tconn_connection.conn_sock, ES_INPUT,
                &tconn->tconn_connection.conn_ed);
  if (err) {
    cl_log_errno(srv->srv_cl, CL_LEVEL_ERROR, "es_open", err,
                 "Unable to register %s for polling",
                 tconn->tconn_peername_buf);
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
  tconn->tconn_connection.conn_protocol_session = srv_session_create(
      srv->srv_cm, srv, &tcp_session_interface_type, (void *)tconn, is_server,
      tconn->tconn_peername_buf, tconn->tconn_sockname_buf);
  if (!tconn->tconn_connection.conn_protocol_session) {
    err = errno;
    cl_log_errno(srv->srv_cl, CL_LEVEL_ERROR, "srv_session_create", err,
                 "Unable to allocate protocol session for %s",
                 tconn->tconn_peername_buf);
    goto free_sock;
  }

  if (ses != NULL) *ses = tconn->tconn_connection.conn_protocol_session;

  srv_session_schedule(tconn->tconn_connection.conn_protocol_session);

  cl_log(srv->srv_cl, CL_LEVEL_INFO, "%s: C: [new TCP connection on fd %d]",
         tconn->tconn_peername_buf, tconn->tconn_connection.conn_sock);
  return 0;

free_sock:
  srv_socket_close(srv->srv_cl, sock, true);
  cm_free(srv->srv_cm, tconn);

  return err;
}

static int tcp_accept(tcp_server_session *tcps) {
  srv_handle *const srv = tcps->tcps_srv;
  struct sockaddr_in addr;
  socklen_t addrlen = sizeof addr;
  int sock;
  int err;
  char my_displayname[200];

  sock = accept(tcps->tcps_sock, (struct sockaddr *)&addr, &addrlen);
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
                   "conn server: accept %s failed [ignored]", tcps->tcps_name);
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

  snprintf(my_displayname, sizeof my_displayname, "[accept from %s:%hu]",
           inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

  tcp_balance_increment(srv);

  return tcp_new_conn(srv, sock, &addr, 0, my_displayname,
                      /* server? */ true);
}

static void tcp_es_server_callback(es_descriptor *ed, int fd,
                                   unsigned int events) {
  tcp_server_session *tcps = (struct tcp_server_session *)ed;
  srv_handle *srv = tcps->tcps_srv;

  cl_log(srv->srv_cl, CL_LEVEL_DEBUG,
         "tcp event (server): ed=%p, fd=%d, events=%x", (void *)ed, fd, events);

  if (events & ES_INPUT) {
    if (tcp_balance(srv)) tcp_accept(tcps);
  }
  if (events & ES_ERROR) {
    es_close(srv->srv_es, ed);
  }
}

/* Create event handlers for the interface.
 */
static int tcp_open(srv_handle *srv, srv_interface_config *icf, void **out) {
  int err, one = 1;
  tcp_server_session *tcps;

  tcps = cm_malloc(srv->srv_cm, sizeof(*tcps));
  if (tcps == NULL) return ENOMEM;

  memset(tcps, 0, sizeof(*tcps));

  cl_assert(srv->srv_cl, icf);
  cl_assert(srv->srv_cl, srv->srv_es);

  tcps->tcps_srv = srv;
  tcps->tcps_ed.ed_callback = tcp_es_server_callback;
  tcps->tcps_ed.ed_displayname = icf->icf_address;
  tcps->tcps_name = icf->icf_address;

  if (strncasecmp(tcps->tcps_name, "tcp://", 6) == 0) {
    tcps->tcps_name += 6;
  } else if (strncasecmp(tcps->tcps_name, "tcp:", 4) == 0) {
    tcps->tcps_name += 4;
  }

  err = tcp_scan(srv->srv_cm, srv->srv_cl, tcps->tcps_name, &tcps->tcps_sin);
  if (err) {
    cm_free(srv->srv_cm, tcps);
    return err;
  }

  if (tcps->tcps_sin.sin_port == htons(0)) {
    tcps->tcps_sin.sin_port = htons(srv->srv_app->app_default_port);
  }

  /* Open the server socket.
   */

  if ((tcps->tcps_sock = socket(PF_INET, SOCK_STREAM, 0)) == -1) {
    err = errno;
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "tcp_open: can't create server socket: %s", strerror(err));
    cm_free(srv->srv_cm, tcps);
    return err;
  }

  if (setsockopt(tcps->tcps_sock, SOL_SOCKET, SO_REUSEADDR, (void *)&one,
                 sizeof(one)) != 0)
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "tcp_open: setsockopt(%s, "
           "SO_REUSEADDR, 1) fails: %s [ignored]",
           icf->icf_address, strerror(err));

  if (bind(tcps->tcps_sock, (struct sockaddr *)&tcps->tcps_sin,
           sizeof(tcps->tcps_sin))) {
    err = errno;
    cl_log(srv->srv_cl, CL_LEVEL_OPERATOR_ERROR,
           "tcp_open: can't bind server socket to \"%s\": %s", icf->icf_address,
           strerror(err));

  err:
    srv_socket_close(srv->srv_cl, tcps->tcps_sock, true);
    cm_free(srv->srv_cm, tcps);

    return err;
  }
  if (listen(tcps->tcps_sock, 10)) {
    err = errno;
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "tcp_open: can't listen(10) to \"%s\": %s", icf->icf_address,
           strerror(err));
    goto err;
  }
  /* Set the listen-port file descriptor
     to non-blocking */
  if (fcntl(tcps->tcps_sock, F_SETFL, O_NONBLOCK)) {
    err = errno;
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "tcp_open: can't set listen port to "
           "non-blocking (fd: %d) : %s",
           tcps->tcps_sock, strerror(err));
    goto err;
  }

  /*  Listen to incoming input.
   */

  err = es_open(srv->srv_es, tcps->tcps_sock, ES_INPUT | ES_OUTPUT,
                &tcps->tcps_ed);
  if (err != 0) {
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "tc_open: can't es_open \"%s\" for input: %s", icf->icf_address,
           strerror(err));
    goto err;
  }

  cl_log(srv->srv_cl, CL_LEVEL_INFO, "%s listening on %s:%d (fd %d)",
         srv_program_name(srv), inet_ntoa(tcps->tcps_sin.sin_addr),
         ntohs(tcps->tcps_sin.sin_port), tcps->tcps_sock);
  *out = tcps;
  return 0;
}

/* Release resources connected to a specific interface.
 */
static void tcp_close(srv_handle *srv, srv_interface_config *icf, void *data) {
  tcp_server_session *tcps = data;

  cl_assert(srv->srv_cl, tcps != NULL);
  if (srv->srv_es != NULL) es_close(srv->srv_es, &tcps->tcps_ed);

  srv_socket_close(srv->srv_cl, tcps->tcps_sock, /* block? */ false);

  cm_free(srv->srv_cm, tcps);
}

/* Create a session connected to the passed-in address.
 *
 */
static int tcp_connect(srv_handle *srv, char const *url,
                       srv_session **ses_out) {
  cl_handle *const cl = srv_log(srv);
  struct sockaddr_in sock_in;
  bool pending_connect = true;
  int sock = -1;
  int err;

  cl_assert(cl, ses_out != NULL);
  *ses_out = NULL;

  /* Skip a leading protocol prefix.
   */
  if (strncasecmp(url, "tcp://", 6) == 0)
    url += 6;
  else if (strncasecmp(url, "tcp:", 4) == 0)
    url += 4;

  err = tcp_scan(srv->srv_cm, srv->srv_cl, url, &sock_in);
  if (err != 0) return err;

  if (sock_in.sin_port == htons(0))
    sock_in.sin_port = htons(srv->srv_app->app_default_port);

  sock = socket(PF_INET, SOCK_STREAM, 0);
  if (-1 == sock) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "socket", err,
                 "Unable to create outbound socket for %s",
                 inet_ntoa(sock_in.sin_addr));

    return err;
  }

  err = srv_socket_block(srv->srv_cl, sock, false);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "srv_socket_block", err,
                 "Unable to make socket %d non-blocking", sock);
    goto close_socket;
  }

  if (!connect(sock, (struct sockaddr *)&sock_in, sizeof(sock_in)))
    pending_connect = false;

  else if (errno != EINPROGRESS) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_OPERATOR_ERROR, "connect", err,
                 "Unable to connect to %s [%s:%d]", url,
                 inet_ntoa(sock_in.sin_addr), htons(sock_in.sin_port));
    goto close_socket;
  }

  err = tcp_new_conn(srv, sock, &sock_in, ses_out, url,
                     /* server? */ false);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "tcp_new_conn", err,
                 "Unable to create session for %s [%s:%u]", url,
                 inet_ntoa(sock_in.sin_addr), ntohs(sock_in.sin_port));
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
 * @brief Interface plugin structure for the "tcp" interface.
 */
const srv_interface_type srv_interface_type_tcp[1] = {
    {"tcp", tcp_match, tcp_config_read, tcp_open, tcp_close, tcp_connect}};
