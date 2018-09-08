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
#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/poll.h>
#include <signal.h>
#include <stdio.h>

#include "libes/es.h"
#include "srvp.h"
#include "srv-interface.h"

typedef struct tty_session {
  srv_session *tty_protocol_session;
  srv_handle *tty_srv;
  es_handle *tty_es;
  unsigned int tty_open : 1;
  unsigned int tty_prompted : 1;
  unsigned int tty_prompting : 1;
  es_descriptor tty_ed_in;
  struct tty_session *tty_ed_in_session;
  es_descriptor tty_ed_out;
  struct tty_session *tty_ed_out_session;

} tty_session;

#define STR(x) #x
#define CHANGE(ses, x, val)                                               \
  ((x) != (val) &&                                                        \
   ((x) = (val), srv_session_change((ses), true, STR(x) " := " STR(val)), \
    true))

static tty_session tty_session_buf[1] = {{0}};

static void nonblocking_fd0(int signum) {
  int flags;

  if ((flags = fcntl(0, F_GETFL, 0)) >= 0) {
    flags |= O_NONBLOCK;
    (void)fcntl(0, F_SETFL, flags);
  }
}

/**
 * @brief You've been woken up by notifications.  Run.
 *
 * @param cb_data the local connection
 * @param srv opaque server module habdle
 * @param ses server session
 * @param deadline deadline in clock_t; if we're running that long,
 *	we've been running too long.
 */
static bool tty_run(void *cb_data, srv_handle *srv, srv_session *ses,
                    srv_msclock_t deadline) {
  cl_handle *cl = ses->ses_bc.bc_cl;
  tty_session *tty = cb_data;
  int err = 0;
  srv_msclock_t clock_var;
  char buf[200];
  bool any = false;

  cl_enter(cl, CL_LEVEL_VERBOSE, "ses=%s",
           srv_session_to_string(ses, buf, sizeof buf));

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

    if (!tty->tty_open) break;

    /* Update the bc bits.
     */
    srv_session_status(ses);

    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE, "tty_run %s want:%s%s%s",
           srv_session_to_string(ses, buf, sizeof buf),
           (ses->ses_want & (1 << SRV_INPUT)) ? " input" : "",
           (ses->ses_want & (1 << SRV_OUTPUT)) ? " output" : "",
           (ses->ses_want & (1 << SRV_RUN)) ? " run" : "");

    if (ses->ses_bc.bc_write_capacity_available &&
        ses->ses_bc.bc_output_waiting_to_be_written &&
        (!ses->ses_bc.bc_output_buffer_capacity_available ||
         !srv_session_ready_to_format(ses))) {
      bool write_any = false;

      /*  Let's go and write what we got.
       */
      err = srv_buffered_connection_write_ready(&ses->ses_bc, &tty->tty_ed_out,
                                                &write_any);
      loop_any |= write_any;
      if (err == 0) {
        srv_buffered_connection_write(srv, &ses->ses_bc, 1, srv->srv_es,
                                      &tty->tty_ed_out, &write_any);
        loop_any |= write_any;
      } else if (err == SRV_ERR_MORE) {
        /*  Write-ready says: "I can't write,
         *  I need more time!"
         */
        CHANGE(ses, ses->ses_bc.bc_write_capacity_available, false);
      } else {
        cl_assert(ses->ses_bc.bc_cl, ses->ses_bc.bc_errno != 0);
        cl_assert(ses->ses_bc.bc_cl, ses->ses_bc.bc_error & SRV_BCERR_WRITE);
      }
    }

    if (ses->ses_bc.bc_data_waiting_to_be_read &&
        ses->ses_bc.bc_input_buffer_capacity_available) {
      if (srv_buffered_connection_read(ses, 0, &tty->tty_ed_in)) {
        tty->tty_prompted = 0;
        loop_any = true;
      }

      /* Update the bc bits.
       */
      srv_session_status(ses);
    }

    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
           "[%s:%d] err %x, head %p, input waiting %d", __FILE__, __LINE__,
           ses->ses_bc.bc_error, (void *)ses->ses_request_head,
           ses->ses_bc.bc_input_waiting_to_be_parsed);

    if ((ses->ses_bc.bc_error & SRV_BCERR_WRITE) ||
        ((ses->ses_bc.bc_error & SRV_BCERR_READ) &&
         ses->ses_request_head == NULL &&
         !ses->ses_bc.bc_input_waiting_to_be_parsed)) {
      /*  Close both interfaces.
       *  After all non-deamon interfaces are closed,
       *  es_loop() returns.
       */

      es_close(srv->srv_es, &tty->tty_ed_in);
      es_close(srv->srv_es, &tty->tty_ed_out);
      tty->tty_open = false;

      cl_log(srv->srv_cl, CL_LEVEL_DEBUG,
             "tty: closed both interfaces; "
             "ending session.");

      /* Unlink the session from this interface.
       */
      ses->ses_interface_type = NULL;
      ses->ses_interface_data = NULL;
      srv_session_unlink(ses);
      any = true;

      break;
    }

    any |= loop_any;
    if (!loop_any) break;

    clock_var = srv_msclock(srv);
    if (SRV_PAST_DEADLINE(clock_var, deadline)) break;
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", any ? "changed" : "no change");
  return any;
}

/**
 * @brief Update a session's notification status to match its connection status
 *
 * @param cb_data the local connection
 * @param srv opaque server module habdle
 * @param ses server session
 */
static void tty_listen(void *cb_data, srv_handle *srv, srv_session *ses) {
  tty_session *tty = cb_data;
  char buf[200];

  if (!tty->tty_open) {
    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_DEBUG, "tty_listen: not open yet.");
    return;
  }

  cl_log(ses->ses_bc.bc_cl, CL_LEVEL_DEBUG, "tty_listen: session %s %s",
         ses->ses_displayname, srv_session_to_string(ses, buf, sizeof buf));

  /*  We subscribe to events until we get them.
   */
  if (ses->ses_bc.bc_data_waiting_to_be_read ||
      (ses->ses_bc.bc_error & SRV_BCERR_READ))
    es_unsubscribe(srv->srv_es, &tty->tty_ed_in, ES_INPUT);
  else {
    es_subscribe(srv->srv_es, &tty->tty_ed_in, ES_INPUT);

    /*  We generally tend to prompt, and haven't yet, ..
     */
    if (tty->tty_prompting && !tty->tty_prompted &&
        srv->srv_app->app_session_interactive_prompt != NULL

        /*  .. there's no more output waiting, not even
         *  if we did the work ..
         */
        && ses->ses_bc.bc_write_capacity_available &&
        !ses->ses_bc.bc_output_waiting_to_be_written &&
        (!*ses->ses_request_output ||
         !((*ses->ses_request_output)->req_done & (1 << SRV_INPUT)))) {
      char prompt_buf[200];
      char const *prompt;

      tty->tty_prompted = 1;

      if ((prompt = (*srv->srv_app->app_session_interactive_prompt)(
               srv->srv_app_data, srv, ses, prompt_buf, sizeof prompt_buf)) !=
          NULL) {
        struct pollfd pfd;

        pfd.fd = 0;
        pfd.events = POLLIN;

        if (poll(&pfd, 1, 0) == 0) {
          fflush(stdout);

          fputs(prompt, stderr);
          fflush(stderr);
        }
      }
    }
  }

  if (ses->ses_bc.bc_write_capacity_available)
    es_unsubscribe(srv->srv_es, &tty->tty_ed_out, ES_OUTPUT);
  else
    es_subscribe(srv->srv_es, &tty->tty_ed_out, ES_OUTPUT);

  if ((ses->ses_want & (1 << SRV_RUN)) || ses->ses_bc.bc_processing) {
    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
           "tty_listen: sending application "
           "event to %s because it %s",
           ses->ses_displayname,
           ses->ses_want & (1 << SRV_RUN) ? "wants to run"
                                          : "is still processing");
    es_application_event(srv->srv_es, &tty->tty_ed_out);
  }
}

static const srv_session_interface_type tty_session_interface_type = {

    tty_run, tty_listen, NULL};

static void tty_es_callback(es_descriptor *ed, int fd, unsigned int events) {
  tty_session *const tty = *(struct tty_session **)(ed + 1);
  srv_handle *const srv = tty->tty_srv;
  srv_session *const ses = tty->tty_protocol_session;

  cl_log(srv->srv_cl, CL_LEVEL_DEBUG,
         "tty event: ed=%p, fd=%d, events=%s%s%s%s (%x)", (void *)ed, fd,
         events & ES_INPUT ? "input " : "", events & ES_OUTPUT ? "output " : "",
         events & ES_APPLICATION ? "app " : "",
         !(events & (ES_OUTPUT | ES_INPUT | ES_APPLICATION)) ? "error" : "",
         (int)events);

  if (events & ES_OUTPUT) {
    if (!ses->ses_bc.bc_write_capacity_available) {
      ses->ses_bc.bc_write_capacity_available = true;
      srv_session_change(ses, true, "bc_write_capacity_available");
    }
  }
  if (events & (ES_INPUT | ES_ERROR)) {
    if (!ses->ses_bc.bc_data_waiting_to_be_read) {
      ses->ses_bc.bc_data_waiting_to_be_read = true;
      srv_session_change(ses, true, "bc_data_waiting_to_be_read");
    }
  }
  if (events & (ES_TIMEOUT | ES_EXIT)) {
    cl_log(srv->srv_cl, CL_LEVEL_VERBOSE, "tty event %x -> SRV_BCERR_SOCKET",
           (int)events);
    if ((ses->ses_bc.bc_error & SRV_BCERR_SOCKET) != SRV_BCERR_SOCKET) {
      ses->ses_bc.bc_error |= SRV_BCERR_SOCKET;
      srv_session_change(ses, true, "bc_error for ES_TIMEOUT | ES_EXIT");
    }
  }
}

static int tty_match(char const *s, char const *e) {
  return s == NULL || s == e;
}

/* Scan interface-specific configuration data beyond
 * the mere address, if any.
 */
static int tty_config_read(srv_config *cf, cl_handle *cl,
                           srv_interface_config *icf, char **s, char const *e) {
  return 0;
}

/* Create event handlers for the interface.
 */
static int tty_open(srv_handle *srv, srv_interface_config *icf,
                    void **data_out) {
  int err, flags;
  tty_session *ts = tty_session_buf;

  cl_assert(srv->srv_cl, icf);
  cl_assert(srv->srv_cl, srv->srv_es);

  ts->tty_srv = srv;

  ts->tty_ed_in.ed_callback = tty_es_callback;
  ts->tty_ed_in.ed_displayname = "*standard input*";
  ts->tty_ed_in_session = ts;

  ts->tty_ed_out.ed_callback = tty_es_callback;
  ts->tty_ed_out.ed_displayname = "*standard output*";
  ts->tty_ed_out_session = ts;

  ts->tty_prompting = isatty(0) && isatty(2);
  ts->tty_prompted = 0;

  (void)signal(SIGCONT, nonblocking_fd0);

  /* Switch input to asynchronous. */
  if ((flags = fcntl(0, F_GETFL, 0)) < 0)
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "tty_open: can't get flags for fd 0: %s (ignored)", strerror(errno));
  else {
    flags |= O_NONBLOCK;
    if (fcntl(0, F_SETFL, flags) != 0) {
      cl_log(srv->srv_cl, CL_LEVEL_ERROR,
             "tty_open: can't set fd 0 to "
             "nonblocking: %s (ignored)",
             strerror(errno));
    }
  }

  /* Hook into fds 0 and 1. */

  if ((err = es_open(srv->srv_es, 0, ES_INPUT, &ts->tty_ed_in)) != 0) {
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "tty_open: can't poll *standard input* "
           "for read events: %s",
           strerror(err));
    return err;
  }
  if ((err = es_open(srv->srv_es, 1, 0, &ts->tty_ed_out)) != 0) {
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "tty_open: can't poll *standard output* "
           "for write events: %s",
           strerror(err));

    es_close(srv->srv_es, &ts->tty_ed_in);
    return err;
  }
  ts->tty_open = 1;

  /*  Create a new session.  This must happen after the
   *  interfaces have been hooked up to the event loop system,
   *  in case the session startup creates a new request.
   *
   *  (Requests produce output; output must be written; creating
   *  a request causes us to wait for output capacity.  We can't
   *  update the events we wait for if we haven't initialized the
   *  event system yet!)
   */
  ts->tty_protocol_session =
      srv_session_create(srv->srv_cm, srv, &tty_session_interface_type, ts,
                         /* is_server */ true, "*interactive*", "/dev/tty");
  if (!ts->tty_protocol_session) {
    es_close(srv->srv_es, &ts->tty_ed_in);
    es_close(srv->srv_es, &ts->tty_ed_out);

    return ENOMEM;
  }
  srv_session_schedule(ts->tty_protocol_session);
  return 0;
}

/* Create event handlers for the interface.
 */
static void tty_close(srv_handle *srv, srv_interface_config *icf, void *data) {
  /* nothing to do here. */
}

/**
 * @brief Interface plugin structure for the "tty" interface.
 */
const srv_interface_type srv_interface_type_tty[1] = {{

    "tty", tty_match, tty_config_read, tty_open, tty_close}};
