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
#include "srv-interface-socket.h"

#define SRV_INTERFACE_TCP_POLL_LOST 50

#define STR(a) #a
#define CHANGE(ses, a, val) \
  (((a) == (val))           \
       ? false              \
       : (((a) = (val)),    \
          srv_session_change(ses, true, STR(a) " := " STR(val)), true))

int srv_socket_block(cl_handle *cl, int fd, bool block) {
  int flags;
  int err = 0;

  /* Switch input to asynchronous. */
  if ((flags = fcntl(fd, F_GETFL, 0)) < 0) {
    err = errno;
    cl_log(cl, CL_LEVEL_ERROR, "can't get flags for fd %d: %s (ignored)", fd,
           strerror(errno));
    cl_cover(cl);
  } else {
    if (!block) {
      flags |= O_NONBLOCK;
      cl_cover(cl);
    } else {
      flags &= ~O_NONBLOCK;
      cl_cover(cl);
    }
    if (fcntl(fd, F_SETFL, flags) != 0) {
      err = errno;
      cl_log(cl, CL_LEVEL_ERROR, "can't set fd %d to %s: %s (ignored)", fd,
             block ? "blocking" : "non-blocking", strerror(errno));
      cl_cover(cl);
    }
  }
  return err;
}

void srv_socket_close(cl_handle *cl, int fd, bool block) {
  if (fd != -1) {
    if (block) srv_socket_block(cl, fd, true);
    if (!block) cl_log(cl, CL_LEVEL_VERBOSE, "fd closed non-blockingly!");
    if (close(fd) != 0)
      cl_log(cl, CL_LEVEL_ERROR, "socket: failed to close %d: %s", fd,
             strerror(errno));
    else
      cl_log(cl, CL_LEVEL_SPEW, "socket: close fd %d", fd);
  }
}

/**
 * @brief Run.
 *
 *  The server is giving this connection a time slice to run in.
 *  Use it to read and answer requests.
 *
 * @param conn_data connection object.
 * @param srv server module handle
 * @param ses generic session
 * @param deadline deadline in clock_t - if we run that long,
 *		we've been running too long.
 *
 * @return true if something actually changed/happened, false otherwise.
 */
bool srv_socket_run(void *conn_data, srv_handle *srv, srv_session *ses,
                    srv_msclock_t deadline) {
  socket_connection *conn = conn_data;
  int err;
  bool any = false;
  srv_msclock_t clock_var;

  cl_assert(srv->srv_cl, srv != NULL);
  cl_assert(srv->srv_cl, ses != NULL);

  if (conn->conn_sock == -1) {
    cl_log(srv->srv_cl, CL_LEVEL_DEBUG, "srv_socket_run: dead connection");
    cl_cover(srv->srv_cl);
    return false;
  }

  if (ses->ses_pending_connect) {
    int so = 0;
    socklen_t so_len = sizeof so;

    if (getsockopt(conn->conn_sock, SOL_SOCKET, SO_ERROR, &so, &so_len)) {
      err = errno;
      cl_log_errno(srv->srv_cl, CL_LEVEL_ERROR, "getsockopt", err,
                   "Unable to check status of pending connect");

      ses->ses_pending_connect = false;
      ses->ses_bc.bc_errno = so;
      srv_session_abort(ses);

      return true;
    }
    if (so) {
      cl_log(srv->srv_cl,
             ses->ses_retry_connect ? CL_LEVEL_FAIL : CL_LEVEL_OPERATOR_ERROR,
             "Connect to %s[%s] failed: %s", conn->conn_displayname,
             conn->conn_peername, strerror(so));
      ses->ses_bc.bc_errno = so;
      srv_session_abort(ses);
    }
    CHANGE(ses, ses->ses_pending_connect, false);
  }

  /*  Behave opportunistically: if there's buffer
   *  space available, fill it, etc.
   *
   *  Terminate if
   *  (a) we've destroyed the session;
   *  (b) state stops changing (we should be waiting for something)
   *  (c) we've run past our deadline.   (BTW, this deadline is just
   *  	 the one for running without interruption; it's not the
   *   	 request timeout as a whole.)
   */
  for (;;) {
    bool loop_any = false;

    /*  <any> is set if state changes.  We'll break out
     *  of the loop if we run out of time or if the state
     *  stops changing.
     */
    if ((ses->ses_bc.bc_error & SRV_BCERR_WRITE) ||
        ((ses->ses_bc.bc_error & SRV_BCERR_READ) &&
         ses->ses_request_head == NULL &&
         !ses->ses_bc.bc_input_waiting_to_be_parsed)) {
      /* close the interface */

      if (conn->conn_sock != -1) {
        es_unsubscribe(srv->srv_es, &conn->conn_ed, ES_INPUT | ES_OUTPUT);

        cl_log(srv->srv_cl, CL_LEVEL_INFO, "%s: S: [close fd %d]",
               conn->conn_peername, conn->conn_sock);
        // XXX

        srv_socket_close(srv->srv_cl, conn->conn_sock,
                         /* block? */
                         !(ses->ses_bc.bc_error & SRV_BCERR_WRITE));
        conn->conn_sock = -1;
        es_close(srv->srv_es, &conn->conn_ed);
      }

      /*  Disconnect the session from the interface
       *  and return to the caller for cleanup.
       */
      cl_log(srv->srv_cl, CL_LEVEL_DEBUG,
             "Disconnecting session: %s %s. Linkcount = %d",
             ses->ses_displayname, conn->conn_peername, (int)ses->ses_refcount);

      cm_free(srv->srv_cm, conn);
      ses->ses_interface_type = NULL;
      ses->ses_interface_data = NULL;
      srv_session_unlink(ses);

      return true;
    }

    if (ses->ses_bc.bc_write_capacity_available) {
      /* Do we have anything to write?  Write it.
       */
      if (ses->ses_bc.bc_output_waiting_to_be_written) {
        bool write_any;
        err = srv_buffered_connection_write_ready(&ses->ses_bc, &conn->conn_ed,
                                                  &write_any);
        if (err == 0) {
          srv_buffered_connection_write(srv, &ses->ses_bc, conn->conn_sock,
                                        srv->srv_es, &conn->conn_ed,
                                        &write_any);
          loop_any |= write_any;
        } else if (err == SRV_ERR_MORE)
          ses->ses_bc.bc_write_capacity_available = 0;
      }
    }

    if (ses->ses_bc.bc_data_waiting_to_be_read &&
        ses->ses_bc.bc_input_buffer_capacity_available) {
      (void)srv_request_priority_get(*ses->ses_request_input);

      loop_any |=
          srv_buffered_connection_read(ses, conn->conn_sock, &conn->conn_ed);
    }

    loop_any |= srv_session_status(ses);

    any |= loop_any;
    if (!loop_any) break;

    clock_var = srv_msclock(srv);
    if (SRV_PAST_DEADLINE(clock_var, deadline)) break;
  }
  return any;
}

/**
 * @brief Listen.
 *
 *  Hook file descriptors into the event management system
 *  according to the scheduling decisions made by the server.
 *
 * @param conn_data connection object.
 * @param srv server module handle
 * @param ses generic session
 */

void srv_socket_listen(void *conn_data, srv_handle *srv, srv_session *ses) {
  socket_connection *conn = conn_data;

  cl_assert(srv->srv_cl, srv != NULL);
  cl_assert(srv->srv_cl, ses != NULL);

  if (conn->conn_sock == -1) {
    cl_log(srv->srv_cl, CL_LEVEL_DEBUG, "srv_socket_listen: dead connection");
    return;
  }

  if (ses->ses_bc.bc_data_waiting_to_be_read ||
      (ses->ses_bc.bc_error & SRV_BCERR_READ))
    es_unsubscribe(srv->srv_es, &conn->conn_ed, ES_INPUT);
  else
    es_subscribe(srv->srv_es, &conn->conn_ed, ES_INPUT);

  if (ses->ses_bc.bc_write_capacity_available ||
      (ses->ses_bc.bc_error & SRV_BCERR_WRITE))
    es_unsubscribe(srv->srv_es, &conn->conn_ed, ES_OUTPUT);
  else
    es_subscribe(srv->srv_es, &conn->conn_ed, ES_OUTPUT);

  if ((ses->ses_want & (1 << SRV_RUN)) || ses->ses_bc.bc_processing) {
    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
           "srv_socket_listen: "
           "sending application event to %s because it %s",
           ses->ses_displayname,
           ses->ses_want & (1 << SRV_RUN) ? "wants to run"
                                          : "is still processing");
    es_application_event(srv->srv_es, &conn->conn_ed);
  }
}

void srv_socket_set_timeout(void *data, srv_timeout *timeout) {
  socket_connection *conn = data;

  cl_log(conn->conn_srv->srv_cl, CL_LEVEL_SPEW,
         "srv_socket_set_timeout: %s, %s", conn->conn_displayname,
         timeout == NULL ? "NULL" : "T");

  if (timeout == NULL) {
    es_timeout_delete(conn->conn_es, &conn->conn_ed);
  } else {
    es_timeout_add(conn->conn_es, (es_timeout *)timeout, &conn->conn_ed);
  }
}

void srv_socket_es_connection_callback(es_descriptor *ed, int fd,
                                       unsigned int events) {
  socket_connection *const conn = (struct socket_connection *)ed;
  srv_handle *const srv = conn->conn_srv;
  srv_session *const ses = conn->conn_protocol_session;

  cl_log(srv->srv_cl, CL_LEVEL_DEBUG, "SOCKET interface %s(%d):%s%s%s%s%s%s",
         ed->ed_displayname, fd, events & ES_OUTPUT ? " OUT" : "",
         events & ES_INPUT ? " IN" : "", events & ES_ERROR ? " ERR" : "",
         events & ES_EXIT ? " EXT" : "", events & ES_TIMEOUT ? " TMT" : "",
         events & ES_APPLICATION ? " APP" : "");

  cl_assert(srv->srv_cl, srv != NULL);
  cl_assert(srv->srv_cl, ses != NULL);

  /* For sessions wanting input or output on a socket, errors
   * will be handled by the application code at the point where
   * read or write fails.
   */
  if (events & ES_OUTPUT)
    CHANGE(ses, ses->ses_bc.bc_write_capacity_available, true);

  if (events & ES_INPUT)
    CHANGE(ses, ses->ses_bc.bc_data_waiting_to_be_read, true);

  if (events & (ES_TIMEOUT | ES_EXIT | ES_ERROR)) {
    ses->ses_bc.bc_error |= SRV_BCERR_SOCKET;
    srv_session_change(ses, true, "forcing SRV_BCERR_SOCKET");
    if (events & ES_EXIT) srv_socket_set_timeout(conn, NULL);
  }
  if (events & ES_APPLICATION)
    srv_session_change(ses, true, "ES_APPLICATION event");
}
