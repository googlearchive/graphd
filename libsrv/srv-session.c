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
#include <stdio.h>
#include <stddef.h>

#include "srvp.h"

/*  If we have already this many requests enqueued, don't
 *  create new ones just to read pending input.
 */
#define SRV_SESSION_MAX_INPUT_QUEUE 10

/*  If we already have this many input buffers employed,
 *  don't create new ones just to read a fresh request.
 * (Ongoing requests still get however many buffers they want.)
 */
#define SRV_SESSION_MAX_INPUT_BUFFERS_USED 2

#define STR(a) #a
#define CHANGE(ses, a, val) \
  (((a) == (val))           \
       ? false              \
       : (((a) = (val)),    \
          srv_session_change(ses, true, STR(a) " := " STR(val)), true))

static const cm_list_offsets srv_session_offsets =
    CM_LIST_OFFSET_INIT(srv_session, ses_next, ses_prev);

void srv_session_change_loc(srv_session *ses, bool value, char const *what,
                            char const *file, int line) {
  if (value && !ses->ses_changed) {
    char buf[200];
    ses->ses_changed = true;
    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
           "srv_session_change %s: %s [from %s:%d]",
           srv_session_to_string(ses, buf, sizeof buf), what, file, line);
  }
}

static srv_request *srv_session_waiting_request(srv_session *ses,
                                                unsigned int flag) {
  srv_request *req;

  for (req = ses->ses_request_head; req != NULL; req = req->req_next)
    if ((req->req_done & flag) == 0) return req;
  return NULL;
}

/**
 * @brief Is this session ready to have output capacity requested
 * 	on its behalf?
 */
bool srv_session_ready_to_format(srv_session *ses) {
  return ses != NULL && ses->ses_request_output != NULL &&
         *ses->ses_request_output != NULL &&
         !((*ses->ses_request_output)->req_done & (1 << SRV_OUTPUT)) &&
         ((*ses->ses_request_output)->req_ready & (1 << SRV_OUTPUT));
}

/**
 * @brief Is this session ready to have input requested on its behalf?
 */
bool srv_session_ready_to_parse(srv_session *ses) {
  return ses != NULL && ses->ses_request_input != NULL &&
         *ses->ses_request_input != NULL &&
         !((*ses->ses_request_input)->req_done & (1 << SRV_INPUT)) &&
         ((*ses->ses_request_input)->req_ready & (1 << SRV_INPUT));
}

/**
 * @brief Update the session's ses_request_output and
 *	ses_request_input pointers, and ses_want, and ses_changed
 *	if anything changed.
 */
static bool srv_session_update_io_chain(srv_session *ses) {
  srv_request const *req;
  char buf[200];
  bool any = false;
  unsigned int want;
  unsigned int interest = (1 << SRV_INPUT) | (1 << SRV_OUTPUT) | (1 << SRV_RUN);

  want = ses->ses_want & ~interest;

  for (req = ses->ses_request_head; req != NULL; req = req->req_next) {
    want |= (req->req_ready & ~req->req_done) & interest;
    if (!(interest &= ~req->req_done)) break;
  }

  /*  Slide both pointers forward until they encounter NULL
   *  or a request that hasn't yet done its input/output.
   */
  while (*ses->ses_request_output != NULL &&
         (*ses->ses_request_output)->req_done & (1 << SRV_OUTPUT))
    ses->ses_request_output = &(*ses->ses_request_output)->req_next;

  while (*ses->ses_request_input != NULL &&
         (*ses->ses_request_input)->req_done & (1 << SRV_INPUT))
    ses->ses_request_input = &(*ses->ses_request_input)->req_next;

  if (ses->ses_want != want) {
    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
           "srv_session_update_io_chain ses->ses_want: %x -> %x", ses->ses_want,
           want);

    CHANGE(ses, ses->ses_want, want);
    any = true;

    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
           "srv_session_update_io_chain %s",
           srv_session_to_string(ses, buf, sizeof buf));
  }
  return any;
}

/**
 * @brief Enqueue a session into the server handle.
 */
static void srv_session_chain_in(srv_handle *srv, srv_session *ses) {
  cl_assert(srv->srv_cl, ses->ses_prev == NULL && ses->ses_next == NULL &&
                             srv->srv_session_head != ses);

  cm_list_push(srv_session, srv_session_offsets, &srv->srv_session_head,
               &srv->srv_session_tail, ses);
}

/**
 * @brief Remove a session from its wait chain.
 */
static void srv_session_chain_out(srv_session *ses) {
  srv_handle *srv;

  if (!ses || !(srv = ses->ses_srv)) return;

  cl_assert(srv->srv_cl, ses->ses_prev || srv->srv_session_head == ses);

  cm_list_remove(srv_session, srv_session_offsets, &srv->srv_session_head,
                 &srv->srv_session_tail, ses);
  ses->ses_next = ses->ses_prev = NULL;
  srv_session_change(ses, true, "srv_session_chain_out");
}

void srv_session_link_request(srv_session *ses, srv_request *req) {
  srv_request_link(req);

  *ses->ses_request_tail = req;
  ses->ses_request_tail = &req->req_next;
  srv_session_change(ses, true, "srv_session_link_request");
}

void srv_session_unlink_request(srv_session *ses, srv_request *req) {
  srv_request **rp;

  cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
         "srv_session_unlink_request ses=%p (%llu), req=%p (%llu)", (void *)ses,
         ses->ses_id, (void *)req, req->req_id);

  for (rp = &ses->ses_request_head; *rp != NULL; rp = &(*rp)->req_next)
    if (*rp == req) break;

  cl_assert(ses->ses_bc.bc_cl, *rp != NULL);

  if ((*rp = req->req_next) == NULL) ses->ses_request_tail = rp;
  if (ses->ses_request_input == &req->req_next) ses->ses_request_input = rp;
  if (ses->ses_request_output == &req->req_next) ses->ses_request_output = rp;

  srv_session_change(ses, true, "srv_session_unlink_request");
  srv_request_unlink(req);
}

/*  @brief return the three-letter abbreviation of a session's current queue.
 */
char const *srv_session_chain_name(srv_session const *ses) {
  if (ses->ses_want & ((1 << SRV_INPUT) | (1 << SRV_OUTPUT))) return "I/O";

  if (ses->ses_want & (1 << SRV_RUN)) return "RUN";

  if (ses->ses_want & (1 << SRV_BUFFER)) return "MEM";

  if (ses->ses_want & (1 << SRV_EXTERNAL)) return "WTG";

  if (!ses->ses_want) return "NUL";

  return "???";
}

void srv_session_set_server(srv_session *ses, bool new_value) {
  if (ses->ses_server != !!new_value) {
    char buf[200];

    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
           "srv_session_set_server %s := %s",
           srv_session_to_string(ses, buf, sizeof buf),
           new_value ? "true" : "false");

    ses->ses_server = !!new_value;
  }
}

/**
 * @brief Create a new session.
 *
 *  Called by the interface; calls into the application's
 *  app_session_initialize callback after allocating and zero-initializing
 *  the amount of heap-based session memory specified by the application.
 *
 *  The caller must call srv_session_schedule() after completing their
 *  initialization.
 *
 * @param cm create the session heap on top of this allocator.
 * @param srv the opaque server module handle
 * @param session_type interface callbacks used to consume
 *		I/O events ot the interface
 * @param session_data first argument passed to the interface type callbacks,
 *		typically the interface-specific connection object
 * @param displayname	Displayname of the new session
 * @param ifname	Interface name of the new session
 *
 * @return NULL on allocation/initialization error,
 *	otherwise a pointer to the new session.
 */
srv_session *srv_session_create(cm_handle *cm, srv_handle *srv,
                                srv_session_interface_type const *session_type,
                                void *session_data, bool is_server,
                                char const *displayname,
                                char const *interfacename) {
  srv_session *ses;
  cm_handle *heap;
  int err;
  char *dup_cli, *dup_if;
  char const *cip_s, *cip_e;
  char const *cport_s, *cport_e;
  char const *sip_s, *sip_e;
  char const *sport_s, *sport_e;

  if ((heap = cm_heap(cm)) == NULL) return NULL;
  if ((ses = cm_malloc(heap, srv->srv_app->app_session_size)) == NULL) {
    cm_heap_destroy(heap);
    return NULL;
  }
  memset(ses, 0, srv->srv_app->app_session_size);

  if ((dup_cli = cm_strmalcpy(heap, displayname)) == NULL ||
      (dup_if = cm_strmalcpy(heap, interfacename)) == NULL) {
    cm_heap_destroy(heap);
    return NULL;
  }

  ses->ses_changed = true;
  ses->ses_needs_interface_update = true;
  ses->ses_server = is_server;
  ses->ses_displayname = dup_cli;
  ses->ses_interface_name = dup_if;
  ses->ses_interface_type = session_type;
  ses->ses_interface_data = session_data;
  ses->ses_refcount = 1;
  ses->ses_cm = heap;
  ses->ses_request_head = NULL;
  ses->ses_request_tail = &ses->ses_request_head;
  ses->ses_request_input = &ses->ses_request_head;
  ses->ses_request_output = &ses->ses_request_head;
  ses->ses_srv = srv;
  ses->ses_id =
      srv->srv_config->cf_processes > 1
          ? (srv->srv_id * srv->srv_config->cf_processes) + srv->srv_smp_index
          : srv->srv_id;
  srv->srv_id++;

  ses->ses_timeslice = srv->srv_config->cf_short_timeslice_ms;

  srv_address_ip_port(displayname, &cip_s, &cip_e, &cport_s, &cport_e);
  srv_address_ip_port(interfacename, &sip_s, &sip_e, &sport_s, &sport_e);

  ses->ses_netlog_header = cm_sprintf(
      ses->ses_cm,
      " %s%.*s"
      "%s%.*s"
      "%s%.*s"
      "%s%.*s ",

      *cip_s ? " (s)client.ip: " : "", (int)(cip_e - cip_s), cip_s,

      *cport_s ? " (i)client.port: " : "", (int)(cport_e - cport_s), cport_s,

      *sip_s ? " (s)server.ip: " : "", (int)(sip_e - sip_s), sip_s,

      *sport_s ? " (i)server.port: " : "", (int)(sport_e - sport_s), sport_s);

  if (ses->ses_netlog_header == NULL) {
    cm_heap_destroy(heap);
    return NULL;
  }
  srv_buffered_connection_initialize(&ses->ses_bc, srv->srv_cl, &srv->srv_pool);

  srv_session_chain_in(srv, ses);

  err = (*srv->srv_app->app_session_initialize)(srv->srv_app_data, srv, ses);
  if (err) {
    srv_session_chain_out(ses);
    cm_heap_destroy(heap);

    return NULL;
  }
  cl_assert(srv->srv_cl, ses->ses_refcount >= 1);
  cl_assert(srv->srv_cl, ses->ses_srv != NULL);

  if (srv->srv_diary != NULL)
    cl_log(srv->srv_diary, CL_LEVEL_INFO, "SESSION(%s,%llu).START", displayname,
           (unsigned long long)ses->ses_id);

  if (srv->srv_netlog != NULL)
    cl_log(srv->srv_netlog, CL_LEVEL_INFO,
           "%s.session.start %s (l)%s.sesid: %llu", srv->srv_progname,
           ses->ses_netlog_header, srv->srv_progname,
           (unsigned long long)ses->ses_id);

  cl_log(srv->srv_cl, CL_LEVEL_DEBUG, "+++ session %p +++", (void *)ses);

  return ses;
}

static void srv_session_destroy(srv_session *ses) {
  srv_handle *srv;

  if (ses == NULL) return;

  srv = ses->ses_srv; /* save a copy of the handle !*/

  cl_log(srv->srv_cl, CL_LEVEL_DEBUG, "--- %s --- (srv_session_destroy)",
         ses->ses_displayname ? ses->ses_displayname : "[unnamed]");

  cl_assert(srv->srv_cl, ses->ses_refcount == 0);

  /*  If we still had request, we'd still have links.
   */
  cl_assert(ses->ses_bc.bc_cl, ses->ses_request_head == NULL);
  srv_session_chain_out(ses);

  if (srv->srv_app != NULL && srv->srv_app->app_session_shutdown != NULL) {
    (*srv->srv_app->app_session_shutdown)(srv->srv_app_data, srv, ses);
  }

  srv_buffered_connection_shutdown(srv, &ses->ses_bc);

  if (srv->srv_diary != NULL)
    cl_log(srv->srv_diary, CL_LEVEL_INFO, "SESSION(%s,%llu).END",
           ses->ses_displayname, (unsigned long long)ses->ses_id);

  if (srv->srv_netlog != NULL)
    cl_log(srv->srv_netlog, CL_LEVEL_INFO,
           "%s.session.end %s (l)%s.sesid: %llu", srv->srv_progname,
           ses->ses_netlog_header, srv->srv_progname,
           (unsigned long long)ses->ses_id);

  cm_heap_destroy(ses->ses_cm);
}

/**
 * @brief Add a link to a session
 *
 *	This mechanism allows sessions to "destroy themselves" without
 * 	having their surrounding event loop collapse on them.
 *
 * @param ses	session we'd like to have a reference to.
 * @param file	name of calling code's file, usually filled in by a macro
 * @param line  line of calling code, usually filled in by a macro
 */
void srv_session_link_loc(srv_session *ses, char const *file, int line) {
  if (ses != NULL) {
    ses->ses_refcount++;
    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
           "srv_session_link ses %llu:%p %d -> %d [from %s:%d]", ses->ses_id,
           (void *)ses, (int)ses->ses_refcount - 1, (int)ses->ses_refcount,
           file, line);
  }
}

/**
 * @brief Remove a link from a session
 *
 *	Only after the last link to a session is removed does the
 * 	session actually get freed.
 *
 * @param ses	session to unlink.
 * @param file	name of calling code's file, usually filled in by a macro
 * @param line  line of calling code, usually filled in by a macro
 *
 * Returns true iff the session was deleted.
 */
bool srv_session_unlink_loc(srv_session *ses, char const *file, int line) {
  if (ses != NULL) {
    cl_assert(ses->ses_srv->srv_cl, ses->ses_refcount > 0);
    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
           "srv_session_unlink ses %llu:%p %d -> %d [from %s:%d]", ses->ses_id,
           (void *)ses, (int)ses->ses_refcount, (int)ses->ses_refcount - 1,
           file, line);

    if (ses->ses_refcount-- <= 1) {
      srv_session_destroy(ses);
      return true;
    }
  }

  return false;
}

void srv_session_process_start(srv_session *ses) {
  /*  Make a note of the session we're running.
   */
  if (ses != NULL) {
    int err;

    err = gettimeofday(&ses->ses_requests_millis_before, (void *)NULL);
    cl_assert(ses->ses_bc.bc_cl, err == 0);

    ses->ses_srv->srv_session = ses;
  }
}

static unsigned long long tv_diff_millis(struct timeval *a, struct timeval *b) {
  return (b->tv_sec - a->tv_sec) * 1000ull + (b->tv_usec - a->tv_usec) / 1000ll;
}

void srv_session_process_stop(srv_session *ses) {
  if (ses != NULL && ses->ses_srv->srv_session == ses) {
    struct timeval after;
    int err;

    err = gettimeofday(&after, (void *)NULL);
    cl_assert(ses->ses_bc.bc_cl, err == 0);

    ses->ses_requests_millis +=
        tv_diff_millis(&ses->ses_requests_millis_before, &after);
    cl_assert(ses->ses_bc.bc_cl, err == 0);

    ses->ses_srv->srv_session = NULL;
  }
}

/* Return <true> if something actually ran, <false> otherwise.
 */
bool srv_session_run(srv_session *ses, unsigned long long deadline) {
  srv_request *req;
  srv_handle *srv = ses->ses_srv;
  cl_handle *const cl = ses->ses_bc.bc_cl;
  int err;
  char buf[200];
  bool any = false;

  req = srv_session_waiting_request(ses, 1 << SRV_RUN);
  if (req == NULL || !(req->req_ready & (1 << SRV_RUN))) return any;

  /*  If this is the first time this request runs,
   *  give it more time.
   */
  if (req->req_n_timeslices++ == 0)
    deadline += srv->srv_config->cf_long_timeslice_ms -
                srv->srv_config->cf_short_timeslice_ms;

  srv_request_link(req);
  cl_enter(cl, CL_LEVEL_VERBOSE, "%s->app_request_run %s",
           srv->srv_app->app_name, srv_request_to_string(req, buf, sizeof buf));

  err = (*srv->srv_app->app_request_run)(srv->srv_app_data, srv, ses, req,
                                         deadline);

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s->app_request_run: %s %s",
           srv->srv_app->app_name, err ? srv_xstrerror(err) : "ok",
           srv_request_to_string(req, buf, sizeof buf));

  if (err != 0)
    srv_request_done(req, ~req->req_done & ((1 << SRV_RUN) | (1 << SRV_INPUT) |
                                            (1 << SRV_OUTPUT)));
  srv_request_unlink(req);
  return true;
}

static bool srv_session_queue_shorter_than(srv_session const *const ses,
                                           int n) {
  srv_request const *req;

  for (req = ses->ses_request_head; n-- > 0; req = req->req_next)
    if (req == NULL) return true;
  return false;
}

/*  How many input buffers are already used by queued-in requests?
 */
static bool srv_session_input_buffers_used_fewer_than(
    srv_session const *const ses, int n) {
  srv_buffer const *buf = NULL;
  srv_request const *req;

  for (req = ses->ses_request_head;; req = req->req_next) {
    if (req == NULL || (req->req_ready & (1 << SRV_INPUT))) return true;

    if (buf != req->req_first) {
      buf = req->req_first;
      if (--n <= 0) return false;
    }

    while (buf != req->req_last) {
      buf = buf->b_next;
      if (--n <= 0) return false;
    }
  }
  return true;
}

static bool srv_session_has_errors(srv_session *ses) {
  srv_buffered_connection *bc = &ses->ses_bc;

  if (bc->bc_error == SRV_BCERR_SOCKET) return true;

  return false;
}

/*  What's the priority for giving this session one more
 *  buffer?
 *
 *  - we have global priority?                 -> 0
 *  - we aren't using more than our fair share -> 1
 *  - we just have a lot of traffic            -> 2
 */
static int srv_session_input_priority(srv_session *ses) {
  srv_buffered_connection *bc = &ses->ses_bc;
  srv_buffer *buf = NULL;

  /* We have priority?
   */
  if (*ses->ses_request_input != NULL &&
      srv_request_priority_get(*ses->ses_request_input))
    return 0;

  /* We don't have space, and don't have anything
   * better to do.
   */
  if (bc->bc_data_waiting_to_be_read &&
      !bc->bc_input_buffer_capacity_available &&
      (ses->ses_want & (1 << SRV_INPUT)) &&
      !(ses->ses_want & (1 << SRV_OUTPUT)) &&
      ((buf = srv_buffer_queue_tail(&bc->bc_input)) == NULL ||
       buf->b_n >= buf->b_m)) {
    return 0; /* Urgent */
  }
  return 2; /* Oh, just, you know. */
}

static bool srv_session_reap_dead_requests(srv_session *ses) {
  srv_request *req;
  cl_handle *const cl = ses->ses_bc.bc_cl;

  /*  Reap dead requests
   */
  if ((req = ses->ses_request_head) != NULL &&
      req->req_done ==
          ((1 << SRV_RUN) | (1 << SRV_INPUT) | (1 << SRV_OUTPUT))) {
    cl_log(cl, CL_LEVEL_VERBOSE, "Reaping a request. Session: %s:%llu",
           ses->ses_displayname, (unsigned long long)ses->ses_id);

    srv_session_unlink_request(ses, req);
    return true;
  }
  return false;
}

/**
 * @brief Run a session.
 *
 *  Called from srv_es_post_dispatch to consume the events
 *  that were marked on the sessions during libes event processing.
 *  This does all we can do without waiting for new (poll-) events,
 *  or running out of time.
 *
 * @param ses the session that gets to run.
 */
void srv_session_process_events(srv_session *ses) {
  srv_handle *srv;
  srv_buffered_connection *bc;
  srv_msclock_t deadline, clock_var;
  size_t round = 0;

  if (ses == NULL || (srv = ses->ses_srv) == NULL) return;
  bc = &ses->ses_bc;

  cl_enter(bc->bc_cl, CL_LEVEL_DEBUG,
           "%s; refcount: %d; run for %lu milliseconds", ses->ses_displayname,
           (int)ses->ses_refcount, (unsigned long)ses->ses_timeslice);

  /*  The ses_changed flags cause this function to be called.
   *  Now that we're inside, we're looking at details.
   */
  srv_session_process_start(ses);
  srv_session_link(ses);

  deadline = srv_msclock(srv) + ses->ses_timeslice;
  ses->ses_changed = false;

  /*  We'll set bc_processing to true if we run out of time
   *  and could theoretically keep processing.
   */
  for (;;) {
    bool any = false;
    bool little_any;

    /* This flag will get set again with "any".
     */
    round++;

    any |= srv_session_reap_dead_requests(ses);

    /*  If the session has requests that just want to run
     *  for a while, let them, assuming there are no errors.
     */
    if (!srv_session_has_errors(ses)) any |= srv_session_run(ses, deadline);

    /*  Process events in the interface - read, write,
     *  that kind of thing.
     */
    if (ses->ses_interface_type != NULL) {
      cl_log(bc->bc_cl, CL_LEVEL_VERBOSE,
             "srv_session_process_events: running interface "
             "for session %s @ %p",
             ses->ses_displayname, ses);
      any |= (*ses->ses_interface_type->sit_run)(ses->ses_interface_data, srv,
                                                 ses, deadline);
    } else
      cl_log(bc->bc_cl, CL_LEVEL_VERBOSE,
             "How strange. A session wants to run without "
             "an interface type");

    /*  Lightweight post processing for the interface.
     *  We read something or wrote something, now what?
     */
    do {
      little_any = false;

      /*  INCOMING REQUESTS
       *  =================
       *  If there's raw input, but not requests,
       *  and we could use more requests, create
       *  a request.
       */

      if (ses->ses_server && ((bc->bc_data_waiting_to_be_read &&
                               !(bc->bc_error & SRV_BCERR_READ)) ||
                              bc->bc_input_waiting_to_be_parsed) &&
          *ses->ses_request_input == NULL &&
          srv_session_queue_shorter_than(ses, SRV_SESSION_MAX_INPUT_QUEUE) &&
          srv_session_input_buffers_used_fewer_than(
              ses, SRV_SESSION_MAX_INPUT_BUFFERS_USED)) {
        if (srv_request_create_incoming(ses) != NULL) little_any = true;
      }
      little_any |= srv_session_update_io_chain(ses);

      /*  INPUT BUFFERS
       *  =============
       *  If there's a request and raw input, but no buffers
       *  to read into, make some buffers.
       */
      if (!bc->bc_input_buffer_capacity_available &&
          !bc->bc_input_waiting_to_be_parsed &&
          bc->bc_data_waiting_to_be_read && !(bc->bc_error & SRV_BCERR_READ) &&
          (ses->ses_want & (1 << SRV_INPUT))) {
        srv_buffer *buf;

        buf = srv_buffer_queue_tail(&bc->bc_input);
        if (buf != NULL && buf->b_m - buf->b_n >= SRV_MIN_BUFFER_SIZE) {
          little_any = true;
          CHANGE(ses, bc->bc_input_buffer_capacity_available, true);
        } else {
          buf = srv_buffered_connection_policy_alloc(
              bc, srv_session_input_priority(ses), "input", __LINE__);
          if (buf != NULL) {
            CHANGE(ses, bc->bc_input_buffer_capacity_available, true);
            srv_buffer_queue_append(&bc->bc_input, buf);
            srv_buffer_link(buf);
          } else {
            srv_request *req;
            req = srv_session_waiting_request(ses, 1 << SRV_INPUT);

            /*  we checked ses->ses_want
             *  & (1 << SRV_INPUT),
             *  above.
             */
            cl_assert(bc->bc_cl, req != NULL);
            cl_log(bc->bc_cl, CL_LEVEL_VERBOSE,
                   "srv_session_process_events: "
                   "buffer allocation "
                   "failed - still no "
                   "input capacity");

            /*  Enqueue the input request,
             *  if any, as waiting for a
             *  buffer.
             */
            srv_request_buffer_wait(req);
            srv_session_update_io_chain(ses);
            cl_assert(bc->bc_cl, !(ses->ses_want & (1 << SRV_INPUT)));
          }
          little_any = true;
        }
      }

      /*  OUTPUT BUFFERS
       *  ==============
       *  If there's a request that wants to write, but no
       *  output buffers, and no existing output buffers
       *  that are waiting for a chance to be sent, make
       *  some buffers.
       */
      if (!bc->bc_output_waiting_to_be_written &&
          !bc->bc_output_buffer_capacity_available &&
          !(bc->bc_error & SRV_BCERR_WRITE) &&
          (ses->ses_want & (1 << SRV_OUTPUT))) {
        srv_buffer *buf;

        buf = srv_buffered_connection_policy_alloc(
            bc, srv_session_output_priority(ses), "output", __LINE__);
        if (buf != NULL) {
          srv_buffer_queue_append(&bc->bc_output, buf);
          CHANGE(ses, bc->bc_output_buffer_capacity_available, true);
        } else {
          srv_request *req;

          req = srv_session_waiting_request(ses, 1 << SRV_INPUT);
          cl_assert(bc->bc_cl, req != NULL);

          /*  Enqueue the output request, if any,
           *  as waiting for a buffer.
           */
          srv_request_buffer_wait(req);
          srv_session_update_io_chain(ses);
        }
        little_any = true;
      }

      /*  OUTPUT
       *  ======
       *  If we have output buffer space and requests
       *  that want to write, output.
       */
      if (bc->bc_output_buffer_capacity_available &&
          !(bc->bc_error & SRV_BCERR_WRITE) &&
          (ses->ses_want & (1 << SRV_OUTPUT)))
        little_any |= srv_session_output(ses, deadline);

      /*  PARSING
       *  =======
       *  If we have input buffered and requests that
       *  want input, parse it.
       */
      if (bc->bc_input_waiting_to_be_parsed && *ses->ses_request_input != NULL)
        little_any |= srv_session_input(ses, deadline);

      /*  READ ERRORS
       *  ===========
       *  Notify pending requests of read errors as soon
       *  as the old input is used up.
       */
      if ((bc->bc_error & SRV_BCERR_READ) &&
          !srv_buffered_connection_input_waiting_to_be_parsed(srv,
                                                              &ses->ses_bc)) {
        if (srv_session_input_error(ses, deadline)) {
          little_any |= srv_session_update_io_chain(ses);
        }
      }

      /*  WRITE ERRORS
       *  ============
       *  Notify output-not-done requests of write errors.
       */
      if (bc->bc_error & SRV_BCERR_WRITE) {
        if (srv_session_output_error(ses, deadline)) {
          little_any |= srv_session_update_io_chain(ses);
        }
      }

      /*  If "any" is set, buffer contents/flags have changed
       *  as a result of running this loop.
       */
      any |= little_any;
      srv_session_change(ses, any, "srv_session_process_events");

    } while (little_any);

    /*  Did anything change on the last run?
     */
    if (!any) {
      bc->bc_processing = false;
      break;
    }

    clock_var = srv_msclock(srv);
    if (SRV_PAST_DEADLINE(clock_var, deadline)) {
      /*  Send ourselves an application event,
       *  to make sure we run again.
       */
      bc->bc_processing = true;
      break;
    }
  }

  cl_leave(bc->bc_cl, CL_LEVEL_DEBUG, "ran out of %s (after %zu round%s)",
           ses->ses_bc.bc_processing ? "time" : "work", round,
           round != 1 ? "s" : "");

  srv_session_process_stop(ses);
  srv_session_unlink(ses);
}

#if DEADBEEF_STACK
static unsigned int *deadbeef_stack(int depth) {
  unsigned int page[1024 * 4];
  unsigned int *w = 0;
  unsigned int *s = 0;
  unsigned int *e = 0;

  if (depth <= 0)
    return 0;
  else
    w = deadbeef_stack(depth - 1);

  s = (w && w < s) ? w : &page[0];
  e = &page[sizeof page / sizeof page[0]];
  if (s > e) {
    w = s;
    s = e;
    e = w;
  }

  while (s < e) *s++ = 0xDEADBEEF;

  return s - 1;
}
#endif

/**
 * @brief Fill up to one waiting output buffer with formatted requests.
 *
 * @param ses Session to work on.
 * @param deadline Return (possibly to resume later)
 *	after msclock() advances past this.
 * @return true if the session state may have changed,
 * 	false if it definitely hasn't.
 */
bool srv_session_output(srv_session *ses, srv_msclock_t deadline) {
  srv_handle *srv = ses->ses_srv;
  bool any = false;
  int err = 0;
  srv_request *req;
  char *s0, *s, *e;
  cl_handle *const cl = ses->ses_bc.bc_cl;
  char buf[200];

  (void)srv_session_status(ses);

  /*  First request that isn't done with output yet.
   */
  req = srv_session_waiting_request(ses, 1 << SRV_OUTPUT);
  if (req == NULL || !(req->req_ready & (1 << SRV_OUTPUT))) return false;

  /*  Get a chunk of buffer to write into.
   */
  err = srv_buffered_connection_output_lookahead(ses, SRV_MIN_BUFFER_SIZE, &s,
                                                 &e);
  if (err != 0) {
    cl_log_errno(ses->ses_bc.bc_cl, CL_LEVEL_FAIL,
                 "srv_buffered_connection_output_lookahead", err,
                 "session %s, %s", ses->ses_displayname,
                 ses->ses_netlog_header);

    /*  We can't get an output buffer to write to.
     *
     *  If output is all we could be doing, mark this
     *  request as not ready to output, and wait for
     *  the buffer situation to improve; that'll
     *  mark us as write-ready again.
     */
    if (req->req_ready == (1 << SRV_OUTPUT)) srv_request_buffer_wait(req);
    return false;
  }

  s0 = s;

  srv_request_link(req);
  srv_request_run_start(req);

  cl_enter(cl, CL_LEVEL_VERBOSE, "%s->app_request_output(%s)",
           srv->srv_app->app_name, srv_request_to_string(req, buf, sizeof buf));

#if DEADBEEF_STACK
  (void)deadbeef_stack(20);
#endif
  err = (*srv->srv_app->app_request_output)(srv->srv_app_data, srv, ses, req,
                                            &s, e, deadline);

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s->app_request_output(%s): %s",
           srv->srv_app->app_name, srv_request_to_string(req, buf, sizeof buf),
           err ? srv_xstrerror(err) : "ok");

  if (err && err != SRV_ERR_MORE) {
    cl_log(srv->srv_cl, CL_LEVEL_ERROR,
           "%s: unexpected formatting error: %s "
           "[dropping connection]",
           ses->ses_displayname, srv_xstrerror(err));

    cl_leave(ses->ses_bc.bc_cl, CL_LEVEL_SPEW, "error %s", srv_xstrerror(err));

    /*  The cl_leave() happens before the srv_session_abort
     *  because the abort might destroy the session object.
     */
    srv_request_run_stop(req);
    srv_request_complete(req);
    srv_request_unlink(req);

    srv_session_abort(ses);
    return true;
  }

  any |= s > s0; /* wrote something */
  any |= req != *ses->ses_request_output;

  /*  If output was written, log that.
   */
  if (s > s0) {
    cl_log(srv->srv_cl, CL_LEVEL_VERBOSE, "%s: formatted %lu more byte%s",
           ses->ses_displayname, (unsigned long)(s - s0),
           s - s0 == 1 ? "" : "s");

    if (srv->srv_diary != NULL && req->req_log_output)

      cl_log(srv->srv_diary, CL_LEVEL_VERBOSE, "REQUEST(%s,%llu,%llu).OUT=%.*s",
             ses->ses_displayname, (unsigned long long)ses->ses_id,
             (unsigned long long)req->req_id, (int)(s - s0), s0);
  }
  srv_request_run_stop(req);
  srv_request_unlink(req);

  srv_buffered_connection_output_commit(&ses->ses_bc, s);
  return any;
}

/**
 * @brief Distribute input errors to the waiting requests.
 *
 * @param ses		session handle
 * @param deadline	if we run past this, return so someone else can.
 *
 * @return true if something changed, false if nothing happened.
 */
bool srv_session_input_error(srv_session *ses, srv_msclock_t deadline) {
  srv_handle *srv = ses->ses_srv;
  int err = 0;
  srv_msclock_t clock_var;
  cl_handle *cl = ses->ses_bc.bc_cl;
  unsigned int n = 0;

  for (;;) {
    char buf[200];
    srv_request *req;

    (void)srv_session_status(ses);

    req = srv_session_waiting_request(ses, 1 << SRV_INPUT);
    if (req == NULL) break;

    srv_request_link(req);

    cl_enter(cl, CL_LEVEL_VERBOSE, "%s->app_request_input(%s, ..NULL..)",
             srv->srv_app->app_name,
             srv_request_to_string(req, buf, sizeof buf));

    err = (*srv->srv_app->app_request_input)(srv->srv_app_data, srv, ses, req,
                                             NULL, NULL, deadline);

    cl_leave(cl, CL_LEVEL_VERBOSE, "%s->app_request_input(%s, ..NULL..): %s",
             srv->srv_app->app_name,
             srv_request_to_string(req, buf, sizeof buf),
             err ? srv_xstrerror(err) : "ok");

    if (!(req->req_done & (1 << SRV_INPUT))) {
      char buf[200];
      cl_notreached(cl,
                    "srv_session_input_error: "
                    "request input for %s not done even after call"
                    " with NULL buffer.",
                    srv_request_to_string(req, buf, sizeof buf));
    }
    n++;

    srv_request_unlink(req);

    if (err != 0) {
      cl_log(srv->srv_cl, CL_LEVEL_OPERATOR_ERROR,
             "%s: error while parsing session error: %s (%d)."
             "Terminating connection.",
             ses->ses_displayname, srv_xstrerror(err), err);
      srv_session_abort(ses);
    }

    clock_var = srv_msclock(srv);
    if (SRV_PAST_DEADLINE(clock_var, deadline)) break;
  }

  if (n > 0)
    cl_log(cl, CL_LEVEL_DEBUG,
           "srv_session_input_error: terminated %hu request%s", n,
           n == 1 ? "" : "s");

  return n > 0;
}

/**
 * @brief Distribute output errors to the waiting requests.
 *
 * @param ses		session handle
 * @param deadline	if we run past this, return so someone else can.
 *
 * @return true if something changed, false if nothing happened.
 */
bool srv_session_output_error(srv_session *ses, srv_msclock_t deadline) {
  srv_handle *srv = ses->ses_srv;
  int err = 0;
  srv_msclock_t clock_var;
  cl_handle *cl = ses->ses_bc.bc_cl;
  unsigned int n = 0;

  for (;;) {
    srv_request *req;
    char buf[200];

    (void)srv_session_status(ses);

    req = srv_session_waiting_request(ses, 1 << SRV_OUTPUT);
    if (req == NULL) break;

    srv_request_link(req);

    /*  If this request was also waiting for *input*, we now
     *  have permission to throw away any waiting-to-be parsed
     *  input and close the input line.
     *
     *  This request could have read it, and it's not going to,
     *  so feeding that same input to anything else will only
     *  mess things up - and place us in an endless loop of
     *  creating new requests to read the input, then throwing
     *  them out because they can't write before they actually
     *  do read any of it.
     */
    if (!(req->req_done & (1 << SRV_INPUT)) &&
        (!(ses->ses_bc.bc_error & SRV_BCERR_READ) ||
         srv_buffered_connection_input_waiting_to_be_parsed(srv,
                                                            &ses->ses_bc))) {
      srv_session_change(ses, true, "throw away input buffer");
      ses->ses_bc.bc_error |= SRV_BCERR_READ;
      srv_buffered_connection_clear_unparsed_input(srv, &ses->ses_bc);
    }

    cl_enter(cl, CL_LEVEL_VERBOSE, "%s->app_request_output(%s, ... NULL ...)",
             srv->srv_app->app_name,
             srv_request_to_string(req, buf, sizeof buf));

    err = (*srv->srv_app->app_request_output)(srv->srv_app_data, srv, ses, req,
                                              NULL, NULL, deadline);

    cl_leave(
        cl, CL_LEVEL_VERBOSE, "%s->app_request_output(%s, ... NULL ...): %s",
        srv->srv_app->app_name, srv_request_to_string(req, buf, sizeof buf),
        err ? srv_xstrerror(err) : "ok");

    if (!(req->req_done & (1 << SRV_OUTPUT))) {
      char buf[200];
      cl_notreached(cl,
                    "srv_session_output_error: "
                    "request output for %s not done even after call"
                    " with NULL buffer.",
                    srv_request_to_string(req, buf, sizeof buf));
    }
    n++;

    srv_request_unlink(req);

    if (err != 0) {
      cl_log(srv->srv_cl, CL_LEVEL_OPERATOR_ERROR,
             "%s: error while formatting into error: %s (%d)."
             "Terminating connection.",
             ses->ses_displayname, srv_xstrerror(err), err);
      srv_session_abort(ses);
    }
    clock_var = srv_msclock(srv);
    if (SRV_PAST_DEADLINE(clock_var, deadline)) break;
  }

  if (n > 0)
    cl_log(cl, CL_LEVEL_DEBUG, "terminated %hu request%s", n,
           n == 1 ? "" : "s");

  return n > 0;
}

/**
 * @brief Parse input that just arrived at a session.
 *
 * @param ses		session handle
 * @param deadline	if we run past this, return so someone else can.
 *
 * @return true if we actually parsed something.
 * @return false if there was nothing to do.
 */
bool srv_session_input(srv_session *ses, srv_msclock_t deadline) {
  srv_handle *srv = ses->ses_srv;
  int err = 0;
  char *s, *e, *s0;
  srv_buffer *buf;
  cl_handle *cl = ses->ses_bc.bc_cl;
  char b2[200];
  srv_request *req;

  /*  Parse what we just read.
   */
  (void)srv_session_status(ses);
  if (!ses->ses_bc.bc_input_waiting_to_be_parsed) return false;

  req = srv_session_waiting_request(ses, 1 << SRV_INPUT);
  if (req == NULL || !(req->req_ready & (1 << SRV_INPUT))) return false;

  err = srv_buffered_connection_input_lookahead(&ses->ses_bc, &s, &e, &buf);
  if (err != 0) return false;

  s0 = s;
  cl_assert(
      ses->ses_bc.bc_cl,
      s == ses->ses_bc.bc_input.q_head->b_s + ses->ses_bc.bc_input.q_head->b_i);
  cl_assert(
      ses->ses_bc.bc_cl,
      e == ses->ses_bc.bc_input.q_head->b_s + ses->ses_bc.bc_input.q_head->b_n);

  srv_request_link(req);
  cl_enter(cl, CL_LEVEL_VERBOSE, "%s->srv_app->app_request_input %s",
           srv->srv_app->app_name, srv_request_to_string(req, b2, sizeof b2));

  srv_request_attach(ses, *ses->ses_request_input, buf);
  err = (*srv->srv_app->app_request_input)(srv->srv_app_data, srv, ses, req, &s,
                                           e, deadline);

  cl_leave(cl, CL_LEVEL_VERBOSE, "%s->srv_app->app_request_input %s",
           srv->srv_app->app_name, srv_request_to_string(req, b2, sizeof b2));
  srv_request_unlink(req);

  if (err != 0) {
    cl_log(srv->srv_cl, CL_LEVEL_OPERATOR_ERROR,
           "%s: error while parsing session input: %s (%d)."
           "Terminating connection.",
           ses->ses_displayname, srv_xstrerror(err), err);
    srv_session_abort(ses);

    /*  Even if we didn't strictly parse anything,
     *  running into an error counts as a state
     *  change in that sense - we'll return true.
     */
    return true;
  }
  return s > s0;
}

int srv_session_output_priority(srv_session const *ses) {
  srv_buffered_connection const *bc = &ses->ses_bc;

  /*  1. Does anyone want to listen?
   *  2. Do we have a chance of doing anything that
   *	produces output?
   *  3. Do we not already have something else we
   *	could be saying?
   */

  /* Urgent */
  if (bc->bc_write_capacity_available &&
      !bc->bc_output_buffer_capacity_available &&
      !bc->bc_output_waiting_to_be_written &&
      (ses->ses_want & (1 << SRV_OUTPUT)))
    return 0;

  /* Fair */
  if (!bc->bc_output_buffer_capacity_available &&
      !bc->bc_output_waiting_to_be_written)
    return 1;

  /* Optional. */
  return 2;
}

/* Returns true if any of the session flags changed,
 * false otherwise.
 */
bool srv_session_status(srv_session *ses) {
  bool any = false;
  srv_handle *srv = ses->ses_srv;
  srv_buffered_connection *bc = &ses->ses_bc;
  srv_buffer *buf;

  cl_assert(bc->bc_cl, bc->bc_pool);

  /* Error conditions are always a "change"
    */
  if (bc->bc_error & SRV_BCERR_SOCKET) {
    srv_session_change(ses, true, "error on connection");
    any = true;
  }

  /*  Translate requests into session want flags.
   */
  any |= srv_session_update_io_chain(ses);

  if (!(bc->bc_error & SRV_BCERR_WRITE)) {
    /* Data has been formatted and is waiting to be written?
     */
    any |= CHANGE(ses, bc->bc_output_waiting_to_be_written,
                  bc->bc_output.q_head != NULL &&
                      bc->bc_output.q_head->b_i < bc->bc_output.q_head->b_n);

    /* Capacity for output to be formatted into?
     */
    buf = srv_buffer_queue_tail(&bc->bc_output);
    any |= CHANGE(ses, bc->bc_output_buffer_capacity_available,
                  buf != NULL && buf->b_m - buf->b_n >= SRV_MIN_BUFFER_SIZE);
  }

  /* Drop emptied input buffers if
   *	- there is a buffer
   *  	- we've parsed everything that's in it  	(i >= n)
   * 	- for reading, there's already a next buffer 	(b_next)
   *	- or we've read so much into the existing one
   *		that we're almost full. 		(m - n < MIN)
   */
  while ((buf = bc->bc_input.q_head) != NULL && buf->b_i >= buf->b_n &&
         (buf->b_next != NULL || buf->b_m - buf->b_n < SRV_MIN_BUFFER_SIZE)) {
    cl_assert(bc->bc_cl, buf->b_i == buf->b_n);
    cl_assert(bc->bc_cl, buf->b_n <= buf->b_m);

    bc->bc_input.q_head = buf->b_next;

    /*  Unlink the buffer here.  If pending requests used their
     *  contents, the requests still link to them, and they'll
     *  only be free'd once the request is serviced.
     */
    if (srv_buffer_unlink(buf)) srv_buffer_pool_free(srv, bc->bc_pool, buf);
  }
  any |= CHANGE(ses, bc->bc_input_waiting_to_be_parsed,
                buf != NULL && buf->b_i < buf->b_n);

  return any;
}

/**
 * @brief What is this session waiting for?
 *
 *  This is called when the scheduling status of a session may have
 *  changed, e.g. afer running it or when a session waiting for memory
 *  has received a buffer.
 *
 *  Uses the session interface type to implement its decisions.
 *
 * @param ses session we're asking about.
 */

void srv_session_schedule(srv_session *ses) {
  srv_handle *srv;

  if (ses == NULL || (srv = ses->ses_srv) == NULL) return;

  (void)srv_session_status(ses);

  /*  We're waiting for memory if:
   *
   *  - we could write
   *  - we're ready to output
   *  - there's no buffer capacity
   *
   *  We're also waiting for memory if
   *  - we have a request waiting for input
   *  - there's no buffer capacity
   *  - there's no input waiting to be parsed.
   */
  if (ses->ses_bc.bc_write_capacity_available &&
      !(ses->ses_bc.bc_error & SRV_BCERR_WRITE) &&
      (ses->ses_want & (1 << SRV_OUTPUT)) &&
      !ses->ses_bc.bc_output_buffer_capacity_available &&
      !ses->ses_bc.bc_output_waiting_to_be_written)

    ses->ses_want |= 1 << SRV_BUFFER;

  if (ses->ses_bc.bc_data_waiting_to_be_read &&
      !(ses->ses_bc.bc_error & SRV_BCERR_READ) &&
      !ses->ses_bc.bc_input_waiting_to_be_parsed &&
      !ses->ses_bc.bc_input_buffer_capacity_available)

    ses->ses_want |= 1 << SRV_BUFFER;

  /*  We're waiting for external events if we're not done
   *  and not ready to run.
   */
  if (srv_session_is_suspended(ses)) ses->ses_want |= (1 << SRV_EXTERNAL);

  /*  If waiting for memory isn't the only thing we're
   *  doing, ... then we're not really waiting for memory.
   */
  if (ses->ses_want != (1 << SRV_BUFFER)) ses->ses_want &= ~(1 << SRV_BUFFER);

  /*  Tell the service system to start looking for hungry
   *  sessions if buffers become free.
   */
  if (ses->ses_want & (1 << SRV_BUFFER))
    srv->srv_requests_waiting_for_buffers = true;

  /* We always want to run if we have an error
   */
  if (ses->ses_bc.bc_error & SRV_BCERR_SOCKET) ses->ses_want |= (1 << SRV_RUN);

  /*  Update the per-interface data structures to wait for the
   *  right kinds of events.
   */
  if (ses->ses_interface_type != NULL)
    (*ses->ses_interface_type->sit_listen)(ses->ses_interface_data, srv, ses);
}

/**
 * @brief Move a session into "suspended" state.
 *
 *  While sessions are suspended, they're waiting for something
 *  other than input.
 *
 * @param ses	The session to suspend.
 */
void srv_session_suspend(srv_session *ses) {
  srv_request *req;

  if (ses == NULL) return;

  /* Find the first request that's not done running,
   * and mark it not ready to run.  That's all "being suspended" means.
   */
  req = srv_session_waiting_request(ses, 1 << SRV_RUN);
  if (req != NULL) req->req_ready &= ~(1 << SRV_RUN);

  /* If we had priority, give it up.
   */
  srv_request_priority_release(req);
  srv_session_schedule(ses);

  cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE, "srv_session_suspend %s",
         ses->ses_displayname);
}

/**
 * @brief Move a session from "suspended" state to running state.
 *
 *  The real move must have happened in the caller's
 *  session request chain - a request that was previously
 *  marked not ready to run has been marked runnable.
 *
 * @param ses	Session to move.
 */
void srv_session_resume(srv_session *ses) {
  if (ses != NULL) {
    srv_request *req;

    /*  Find the first request that's not done running,
     *  and mark it ready to run.  That's all "not being
     *  suspended" means.
     */
    req = srv_session_waiting_request(ses, 1 << SRV_RUN);
    if (req != NULL) srv_request_run_ready(req);

    srv_session_schedule(ses);
  }
}

/**
 * @brief List sessions.
 *
 * @param srv 		server whose sessions we're listing.
 * @param callback	callback invoked with each session
 * @param callback_data	first argument passed to callback
 * @return 0 on success, otherwise the result of the first
 *	callback that returns something other than 0.
 */

int srv_session_list(srv_handle *srv, srv_session_list_callback *callback,
                     void *callback_data) {
  srv_session *ses, *next;
  int err = 0;

  if (srv == NULL || callback == NULL) return EINVAL;

  for (ses = srv->srv_session_head; ses != NULL; ses = next) {
    next = ses->ses_next;
    if ((err = (*callback)(callback_data, ses)) != 0) break;
  }
  return err;
}

/*  A suspended session is one that has a request that isn't
 *  completely done, but isn't able to input, output, or run.
 */
bool srv_session_is_suspended(srv_session const *ses) {
  srv_request *req;
  unsigned int interest;

  interest = (1 << SRV_INPUT) | (1 << SRV_OUTPUT) | (1 << SRV_RUN);

  for (req = ses->ses_request_head; req != NULL; req = req->req_next) {
    if (interest & (~req->req_done & req->req_ready)) return false;
    interest &= req->req_done;
  }
  return true;
}

/**
 * @brief Internal, hard shutdown of a session.
 *
 *  This doesn't destroy the session; it just marks it
 *  for destruction next time it runs, and causes it to be
 *  scheduled to run.
 *
 * @param ses	session to shut down
 */

void srv_session_abort(srv_session *ses) {
  /*  Since we're no longer waiting for anything, schedule
   *  the session to run.
   *
   *  Once it runs, its interface's "run" function will
   *  notice the aborted buffered connection and close
   *  the underlying socket.
   */

  if (ses->ses_bc.bc_error != SRV_BCERR_SOCKET) {
    ses->ses_bc.bc_error = SRV_BCERR_SOCKET;
    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_DEBUG, "srv_session_abort: aborting");
  }
  srv_session_schedule(ses);
}

/**
 * @brief Regular quit.
 * @param ses	session to shut down
 */

void srv_session_quit(srv_session *ses) { srv_session_abort(ses); }

/**
 * @brief Intermediate commit of a request's end to the connection.
 *
 *  The application calls this to report that it has consumed
 *  data from an input buffer, typically from within application
 *  parser code before calling srv_request_arrived().
 *
 *  The call triggers advancement of the buffer pointer b_i,
 *  as well as moves to the next buffer in a chain.
 *
 * @param ses	session
 * @param e	pointer just after the last consumed byte.
 */
void srv_session_input_commit(srv_session *ses, char const *e) {
  srv_buffered_connection_input_commit(ses->ses_srv, &ses->ses_bc, e);
}

/**
 * @brief Associate a closure with an output buffer.
 *
 *  The caller is in the middle of their "format" callback.
 *  There is a current output buffer, and it's in the process of
 *  being appended to.
 *
 *  Before sending any of the output that's being written right now,
 *  libsrv must also invoke callback with callback_data.
 *  If that invocation fails, the connection will be broken,
 *  and the data will not be sent.
 *
 * @param ses			The session whose request is being formatted
 * @param callback		callback to invoke
 * @param callback_data_size	size of the closure to allocate
 *
 * @return NULL on memory error, otherwise a per-buffer closure.
 * 	If the callback didn't have a closure, a new closure
 *	of the given size was allocated.
 */
void *srv_session_allocate_pre_hook(srv_session *ses,
                                    srv_pre_callback *callback,
                                    size_t callback_data_size) {
  return srv_buffered_connection_output_alloc_pre_hook(&ses->ses_bc, callback,
                                                       callback_data_size);
}

/**
 * @brief How many requests does this session currently have?
 *
 * @param ses	the session
 * @returns the number of requests in the session.
 */
size_t srv_session_n_requests(srv_session const *ses) {
  srv_request const *req;
  size_t n = 0;

  if (ses == NULL) return 0;

  for (req = ses->ses_request_head; req != NULL; req = req->req_next) n++;
  return n;
}

static char const *const session_interest_name[] = {[SRV_BUFFER] = "buffer",
                                                    [SRV_EXTERNAL] = "external",
                                                    [SRV_OUTPUT] = "output",
                                                    [SRV_INPUT] = "input",
                                                    [SRV_RUN] = "run"};

static char const *srv_session_interest_to_string(unsigned int want, char *buf,
                                                  size_t size) {
  char *w;
  char const *e, *sep = "";
  size_t i;
  static const short interests[5] = {SRV_INPUT, SRV_RUN, SRV_OUTPUT, SRV_BUFFER,
                                     SRV_EXTERNAL};

  if (want == 0) return "-";

  if (want == (1 << SRV_BUFFER)) return "buffer (blocked)";

  if (want == (1 << SRV_EXTERNAL)) return "external (blocked)";

  w = buf;
  e = buf + size;

  for (i = 0; i < sizeof(interests) / sizeof(*interests); i++) {
    int inter = interests[i];

    if (want == (1 << inter))
      if (want == (1 << inter)) return session_interest_name[inter];

    if (want & (1 << inter)) {
      if (w < e) {
        snprintf(w, e - w, "%s%s", sep, session_interest_name[i]);
        w += strlen(w);
        sep = ", ";
      }
    }
  }
  if (w >= e) return "... complicated ...";

  *w = '\0';
  return buf;
}

char const *srv_session_to_string(srv_session const *ses, char *buf,
                                  size_t size) {
  char b1[200];
  char b2[200];

  if (ses == NULL) return "null";

  snprintf(buf, size, "%llu@%p want:%s; bc:%s", (unsigned long long)ses->ses_id,
           (void *)ses,
           srv_session_interest_to_string(ses->ses_want, b1, sizeof b1),
           srv_buffered_connection_to_string(&ses->ses_bc, b2, sizeof b2));
  return buf;
}

bool srv_any_sessions_ready_for(srv_handle *srv, int flags) {
  srv_session *ses = srv->srv_session_head;
  srv_request *req = NULL;

  while (ses != NULL) {
    for (req = ses->ses_request_head; req != NULL; req = req->req_next) {
      if ((req->req_ready & flags) && !(req->req_done & flags)) return true;
      if (!(req->req_ready & flags)) break;
    }
    ses = ses->ses_next;
  }
  return false;
}
