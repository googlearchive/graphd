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
#include <string.h>
#include <stdio.h>
#include <errno.h>

static const cm_list_offsets srv_request_buffer_waiting_offsets =
    CM_LIST_OFFSET_INIT(srv_request, req_buffer_waiting_next,
                        req_buffer_waiting_prev);

void srv_request_buffer_wait(srv_request *req) {
  srv_handle *srv = req->req_session->ses_srv;
  cl_handle *cl = req->req_session->ses_bc.bc_cl;
  char buf[200];

  cl_assert(cl, req->req_buffer_waiting == 0);
  cl_assert(cl, (req->req_done & req->req_ready) == 0);
  cl_assert(cl, req->req_ready != 0);

  cl_log(cl, CL_LEVEL_VERBOSE, "srv_request_buffer_wait: suspend %s",
         srv_request_to_string(req, buf, sizeof buf));

  cm_list_enqueue(srv_request, srv_request_buffer_waiting_offsets,
                  &srv->srv_buffer_waiting_head, &srv->srv_buffer_waiting_tail,
                  req);
  req->req_buffer_waiting = req->req_ready;
  req->req_ready = 0;

  srv_session_change(req->req_session, true, "srv_request_buffer_wait");

  cl_assert(cl, req->req_buffer_waiting != 0);
}

void srv_request_buffer_wakeup(srv_request *req) {
  srv_handle *srv = req->req_session->ses_srv;
  cl_handle *cl = req->req_session->ses_bc.bc_cl;
  char buf[200];

  if (req->req_buffer_waiting == 0) return;

  cl_assert(cl, req->req_buffer_waiting != 0);

  cm_list_remove(srv_request, srv_request_buffer_waiting_offsets,
                 &srv->srv_buffer_waiting_head, &srv->srv_buffer_waiting_tail,
                 req);

  req->req_ready |= req->req_buffer_waiting;
  req->req_buffer_waiting = 0;

  cl_assert(cl, req->req_ready != 0);
  srv_session_change(req->req_session, true, "srv_request_buffer_wakeup");

  cl_log(cl, CL_LEVEL_VERBOSE, "srv_request_buffer_wakeup: resume %s",
         srv_request_to_string(req, buf, sizeof buf));
}

/* This function can be called because
 *
 * ... "priority" has become available
 * ... "buffer" has become available.
 */
void srv_request_buffer_wakeup_all(srv_handle *srv) {
  size_t n = 0;

  /*  Wake up everybody who was waiting for a buffer.
   */
  while (srv->srv_buffer_waiting_head) {
    n++;
    srv_request_buffer_wakeup(srv->srv_buffer_waiting_head);
  }

  if (n > 0)
    cl_log(srv->srv_cl, CL_LEVEL_DEBUG,
           "srv_request_buffer_wakeup_all: woke up %zu "
           "processes waiting for buffers",
           n);
}

/**
 * @brief Try to obtain priority for a request.
 *
 *  Policies in a system change depending on how much memory
 *  there is.  In a system flush with memory, everybody gets
 *  to use as much as they like ("generous").  In a system with a medium
 *  amount of memory, requests can use a reasonable amount - if
 *  they already  have some, they're expected to use it before
 *  asking for more ("fair").  In a system where memory is scarce,
 *  memory allocation policy changes to favor a rigid order -
 *  requests are given a (random) priority, and requests who have
 *  priority can complete before requests who don't.
 *
 *  When squeezed for space, we're using a very simple form of
 *  priorisation -- one request at a time gets preferential treatment
 *  over all the others, until that request gives it up.  (Who's
 *  really getting priority is the session that is working on the
 *  request; it will hold priority until the request is done, and
 *  give it up once the request has finished.)
 *
 *  A request must give up priority before it waits for I/O events.
 *
 * @param req   requests that's asking to get priority.
 * @param file 	file name of calling code (usually inserted by a macro)
 * @param line 	line number of calling code (usually inserted by a macro)
 *
 * @return whether the caller has priority or not.
 */
bool srv_request_priority_get_loc(srv_request *req, char const *file,
                                  int line) {
  srv_handle *srv;
  srv_request *dep;
  char buf[200];

  if (req == NULL) return false;

  srv = req->req_session->ses_srv;
  if (srv->srv_priority == req) return true;

  if (srv->srv_priority == NULL) goto have_priority;

  for (dep = req->req_dependent; dep != NULL; dep = dep->req_dependent)
    if (dep == srv->srv_priority) goto have_priority;

  return false;

have_priority:
  srv->srv_priority = req;
  srv_buffered_connection_have_priority(&req->req_session->ses_bc, 1);

  cl_log(req->req_session->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
         "ses: %s gets priority [%s:%d]",
         srv_request_to_string(req, buf, sizeof buf), file, line);

  return true;
}

/**
 * @brief Give up priority.
 *
 *  A session gives up priority when it finishes processing a request
 *  and when it has to wait for I/O.
 *
 * @param ses the session that's giving up priority.
 * @param file 	file name of calling code (usually inserted by a macro)
 * @param line 	line number of calling code (usually inserted by a macro)
 */
void srv_request_priority_release_loc(srv_request *req, char const *file,
                                      int line) {
  srv_handle *srv;

  if (req != NULL && req->req_session != NULL &&
      (srv = req->req_session->ses_srv) != NULL && srv->srv_priority == req) {
    char buf[200];

    cl_log(srv->srv_cl, CL_LEVEL_VERBOSE, "req: %s gives up priority [%s:%d]",
           srv_request_to_string(req, buf, sizeof buf), file, line);

    srv_buffered_connection_have_priority(&req->req_session->ses_bc, 0);
    srv->srv_priority = NULL;

    srv_request_buffer_wakeup_all(srv);
  }
}

/*  Release the buffers attached to request <req>.
 */
static void srv_request_release(srv_session *ses, srv_request *req) {
  cl_handle *cl;
  int any = 0;

  cl = ses->ses_bc.bc_cl;

  cl_assert(cl, req != NULL);
  if (req->req_first) {
    srv_buffer *buf, *next;
    bool last = false;

    for (buf = req->req_first; !last; buf = next) {
      last = (buf == req->req_last);
      next = buf->b_next;

      /*  Was this the last reference to
       *  this buffer?
       */
      if (srv_buffer_unlink(buf)) {
        /* release the buffer back into its pool */

        srv_buffer_pool_free(ses->ses_srv, ses->ses_bc.bc_pool, buf);
        any++;
      }
    }
    req->req_first = NULL;
  }
  req->req_last = NULL;
}

/**
 * @brief The first request depends on the second one.
 *
 *  That means that in order for the first to complete,
 *  the second one must run to completion.  If the first
 *  one has priority, the second one can take priority
 *  from it.
 *
 * @param dep  NULL or a request that depends on the second one.
 * @param rep  The request that the first depends on.
 */
void srv_request_depend(srv_request *dep, srv_request *req) {
  /*  Add a link to the request that depends on us.
   */
  if (dep != NULL) srv_request_link(dep);

  /*  If we are tracking someone else, stop
   *  tracking them.
   */
  if (req->req_dependent != NULL) {
    srv_request *dep = req->req_dependent;

    req->req_dependent = NULL;

    cl_assert(req->req_session->ses_bc.bc_cl, dep->req_refcount >= 1);
    srv_request_unlink(dep);
  }

  req->req_dependent = dep;
}

/**
 * @brief Get the next piece of input attached to a request.
 *
 *  Successive calls to this function return successive pieces
  * of the text for a server request.
 *
 * @param req	request we're asking about
 * @param s_out	out: first byte of input data chunk
 * @param n_out	out: number of bytes pointed to by *s_out
 * @param state	in/out: iterator state; must be set to NULL
 * 	by the caller for the first call.
 *
 * @return 0 on success
 * @return SRV_ERR_NO after running out of data to return.
 */
int srv_request_text_next(srv_request *req, char const **s_out, size_t *n_out,
                          void **state) {
  srv_buffer *buf;

  if (*state == NULL) {
    *state = buf = req->req_first;
    if (buf == NULL) return SRV_ERR_NO;

    *s_out = buf->b_s + req->req_first_offset;
    *n_out = (buf == req->req_last ? req->req_last_n : buf->b_n) -
             req->req_first_offset;
  } else {
    buf = (srv_buffer *)*state;
    if (buf == req->req_last) return SRV_ERR_NO;

    *state = buf = buf->b_next;
    *s_out = buf->b_s;
    *n_out = (buf == req->req_last ? req->req_last_n : buf->b_n);
  }
  return 0;
}

/*  The request <req> involves incoming data from <buf>.
 *  Make sure <req> points to <buf>, so that <buf> doesn't
 *  get free'ed before <req> has been answered.
 *
 *  Buffers are attached in sequential order - a later buffer
 *  is never attached before an earlier buffer.
 */
void srv_request_attach(srv_session *ses, srv_request *req, srv_buffer *buf) {
  cl_handle *cl = ses->ses_bc.bc_cl;

  cl_assert(cl, buf != NULL);
  cl_assert(cl, req != NULL);

  req->req_last_n = buf->b_i;
  if (req->req_last == buf) return;

  if (req->req_last != NULL && req->req_last->b_next == NULL)
    req->req_last->b_next = buf;

  cl_assert(cl, req->req_last == NULL || req->req_last->b_next == buf);

  req->req_last = buf;
  if (req->req_first == NULL) {
    req->req_first = buf;
    req->req_first_offset = buf->b_i;
  }

  /*  Increment the buffer's linkcounter.
   */
  srv_buffer_link(buf);
}

/**
 * @brief Create a new request structure
 * @param ses	session that wants to create the new request
 * @return NULL on allocation failure
 * @return a new request structure on success.  The request
 *  	points to its session and its heap; its application
 *	data has been initialized with the app_request_initialize
 *	callback.
 */
static srv_request *srv_request_create(srv_session *ses) {
  srv_handle *srv = ses->ses_srv;
  srv_request *req;
  cm_handle *heap;
  int err = 0;

  /* Create a new heap to allocate the request's stuff in.
   */
  if (!(heap = cm_heap(srv->srv_cm))) return NULL;

  /*  Allocate the request, and as much extra space as
   *  the application claimed it needed.
   */
  cl_assert(ses->ses_bc.bc_cl, srv != NULL);
  cl_assert(ses->ses_bc.bc_cl, srv->srv_app->app_request_size >= sizeof(*req));

  if (!(req = cm_malloc(heap, srv->srv_app->app_request_size))) {
    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_ERROR,
           "failed to allocate data for new request: %s [%s:%d]",
           strerror(errno), __FILE__, __LINE__);
    cm_heap_destroy(heap);
    return NULL;
  }
  memset(req, 0, srv->srv_app->app_request_size);

  req->req_next = NULL;
  req->req_first = NULL;
  req->req_last = NULL;
  req->req_session = ses;
  req->req_cm = heap;
  req->req_log_output = true;
  req->req_display_id = NULL;
  req->req_id =
      srv->srv_config->cf_processes > 1
          ? (srv->srv_id * srv->srv_config->cf_processes) + srv->srv_smp_index
          : srv->srv_id;
  srv->srv_id++;

  /*  Increment the session's linkcounter, since the
   *  request points to it.
   */
  srv_session_link(ses);

  if (srv->srv_app->app_request_initialize != NULL) {
    err = (*srv->srv_app->app_request_initialize)(srv->srv_app_data, srv, ses,
                                                  req);
    if (err) {
      cl_log(ses->ses_bc.bc_cl, CL_LEVEL_ERROR,
             "application request initialization "
             "fails: %s [%s:%d]",
             strerror(err), __FILE__, __LINE__);

      srv_session_unlink(ses);
      cm_heap_destroy(heap);
      return NULL;
    }
  }
  if (srv->srv_diary != NULL)
    cl_log(srv->srv_diary, CL_LEVEL_DETAIL, "REQUEST(%s,%llu,%llu).START",
           ses->ses_displayname, (unsigned long long)ses->ses_id,
           (unsigned long long)req->req_id);

  return req;
}

/**
 * @brief Create an asynchronous request.
 *
 *  The server wants to spontaneously say something to its client.
 *  (For example, a greeting to a client that just connected.)
 *  An asynchronous request wraps that output to shepherd it through
 *  the usual session formatting process.
 *
 * @param ses 	session to create a request for
 * @result NULL on allocation error, otherwise a request pointer.
 */
srv_request *srv_request_create_asynchronous(srv_session *ses) {
  srv_request *req;

  req = srv_request_create(ses);
  if (req == NULL) return NULL;

  /*  Because this is an asynchronous request, we know at time of
   *  its creation that we have data that we want to send, and that
   *  we won't want input.  Or runtime, for that matter.
   */
  req->req_ready = 1 << SRV_OUTPUT;
  req->req_done = (1 << SRV_INPUT) | (1 << SRV_RUN);

  srv_session_link_request(ses, req);
  cl_assert(ses->ses_bc.bc_cl, ses->ses_request_head != NULL);

  srv_session_schedule(ses);

  cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
         "new request %p for "
         "session %s",
         (void *)req, ses->ses_displayname);
  return req;
}

/**
 * @brief Create a client request.
 *
 *  The server wants to send something to some other server, and
 *  get a reply back.
 *
 * @param ses 	session to create a request for
 * @result NULL on allocation error, otherwise a request pointer.
 */
srv_request *srv_request_create_outgoing(srv_session *ses) {
  srv_request *req;

  req = srv_request_create(ses);
  if (req == NULL) return NULL;

  /*  Because this is an outgoing request, we already know
   *  at time of its creation that we have data that we want to send.
   */
  req->req_ready = 1 << SRV_OUTPUT;

  srv_session_link_request(ses, req);
  cl_assert(ses->ses_bc.bc_cl, ses->ses_request_head != NULL);

  srv_session_schedule(ses);

  cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
         "new request %p for "
         "session %s",
         (void *)req, ses->ses_displayname);
  return req;
}

/**
 * @brief Create a new incoming request structure
 * @param ses	session that wants to create the new request
 * @return NULL on allocation failure
 * @return a new request structure on success.  The request
 *  	points to its session and its heap; its application
 *	data has been initialized with the app_request_initialize
 *	callback.
 */
srv_request *srv_request_create_incoming(srv_session *ses) {
  char buf[200];
  srv_request *req;
  cl_handle *const cl = ses->ses_bc.bc_cl;

  cl_assert(cl, *ses->ses_request_input == NULL);
  cl_assert(cl, ses->ses_request_input == ses->ses_request_tail);

  if ((req = srv_request_create(ses)) == NULL) return NULL;

  req->req_ready = 1 << SRV_INPUT;
  req->req_done = 0;

  /*  Attach the buffer we're currently parsing.
   */
  if (ses->ses_bc.bc_input.q_head != NULL)
    srv_request_attach(ses, req, ses->ses_bc.bc_input.q_head);

  /*  Add it to the chain of requests.
   */
  srv_session_link_request(ses, req);

  cl_assert(cl, *ses->ses_request_input == req);
  cl_assert(cl, ses->ses_request_tail == &req->req_next);

  cl_log(cl, CL_LEVEL_DEBUG,
         "srv_request_create_incoming %s "
         "[%zu in session]",
         srv_request_to_string(req, buf, sizeof buf),
         srv_session_n_requests(ses));

  return req;
}

/**
 * @brief Hold a link on a request.
 * @param req	request to link to.
 */
void srv_request_link(srv_request *req) {
  if (req != NULL) {
    req->req_refcount++;
    cl_log(req->req_session->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
           "srv_request_link req %llu:%p %d -> %d",
           (unsigned long long)req->req_id, (void *)req,
           (int)req->req_refcount - 1, (int)req->req_refcount);
  }
}

void srv_request_run_start(srv_request *req) {
  /*  Make a note of the request ID we're running.
   */
  if (req != NULL) req->req_session->ses_srv->srv_request = req;
}

void srv_request_run_stop(srv_request *req) {
  if (req != NULL && req->req_session->ses_srv->srv_request == req)
    req->req_session->ses_srv->srv_request = NULL;
}

/**
 * @brief Complete processing of a request
 *
 *  Don't call this, call srv_request_unlink (or srv_request_done()) instead.
 *
 *  This also frees all memory and all other resources allocated for
 *  the request.
 *
 * @param req	request to free.
 */
static void srv_request_destroy(srv_request *req) {
  srv_session *ses = req->req_session;
  srv_handle *srv = ses->ses_srv;

  if (ses != NULL) srv_session_change(ses, true, "srv_request_destroy");

  if (req->req_buffer_waiting != 0) srv_request_buffer_wakeup(req);

  /* If someone depended on us, unlink them.
   */
  srv_request_depend(NULL, req);

  /* Free the protocol component of this request.
   */
  (*srv->srv_app->app_request_finish)(srv->srv_app_data, srv, ses, req);

  /*  If this request had priority, give it up now.
   */
  srv_request_priority_release(req);

  /*  Release the buffers owned by the request.
   *
   *  That must follow the priority release, so that a connection
   *  that was waiting for contentious buffers freed during
   *  srv_request_relaese() can get priority to grab them.
   */
  srv_request_release(ses, req);

  if (srv->srv_diary != NULL)
    cl_log(srv->srv_diary, CL_LEVEL_DETAIL, "REQUEST(%s,%llu,%llu).END",
           ses->ses_displayname, (unsigned long long)ses->ses_id,
           (unsigned long long)req->req_id);

  /*  Unlink the request from its session.
   */
  srv_session_unlink(req->req_session);

  /* Free the request and all resources and memory allocated for it.
   */
  cm_heap_destroy(req->req_cm);
}

/**
 * @brief Complete processing of a request
 *
 * @param req	request to free.
 */
void srv_request_unlink(srv_request *req) {
  if (req != NULL) {
    cl_log(req->req_session->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
           "srv_request_unlink req %llu:%p %d -> %d",
           (unsigned long long)req->req_id, (void *)req, (int)req->req_refcount,
           (int)req->req_refcount - 1);

    if (req->req_refcount-- <= 1) srv_request_destroy(req);
  }
}

/**
 * @brief A request has been fully parsed by the protocol engine and
 *  is considered "arrived" at the server.
 *
 *  We can start calling "run" on it, then "format" to
 *  get the results.
 *
 * @param ses	session whose request has arrived.
 *
 *          client   server
 *            |        |
 *            |------->|
 *	      |        X <--- YOU ARE HERE
 *            |<-------|
 *            |        |
 *
 */
void srv_request_arrived(srv_request *req) {
  srv_session *ses = req->req_session;
  srv_handle *srv = ses->ses_srv;
  char buf[200];

  cl_enter(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE, "req=%s",
           srv_request_to_string(req, buf, sizeof buf));

  srv_request_done(req, 1 << SRV_INPUT);

  if (!(req->req_done & (1 << SRV_RUN)))
    srv_request_ready(req, 1 << SRV_RUN);
  else if (!(req->req_done & (1 << SRV_OUTPUT)))
    srv_request_ready(req, 1 << SRV_OUTPUT);

  /*  req->req_last_n is the index+1 of the last byte
   *  in the last buffer of the request.
   *
   *  If the last request buffer is still being read into,
   *  that's the first unread byte, b_i.
   *  Otherwise, its size is its recorded size.
   */
  req->req_last_n =
      (req->req_last == NULL || req->req_last == ses->ses_bc.bc_input.q_head)
          ? (ses->ses_bc.bc_input.q_head != NULL
                 ? ses->ses_bc.bc_input.q_head->b_i
                 : 0)
          : req->req_last->b_n;

  cl_assert(ses->ses_bc.bc_cl, ses->ses_request_head != NULL);

  /*  If it has text, log the request's text.
   *  Unless it's really, _really_ long.
   */
  if (srv->srv_diary != NULL && req->req_last != NULL) {
    cl_diary_handle *d = cl_diary_get_handle(srv->srv_diary);
    char const *s;
    size_t n, total_n = 0;
    void *state = NULL;

    cl_log(srv->srv_diary, CL_LEVEL_DETAIL, "REQUEST(%s,%llu,%llu).IN=",
           ses->ses_displayname, (unsigned long long)ses->ses_id,
           (unsigned long long)req->req_id);

    while (srv_request_text_next(req, &s, &n, &state) == 0) {
      if (total_n + n >= 4 * 1024 - sizeof(" [...]")) {
        n = 4 * 1024 - (total_n + sizeof(" [...]"));
        cl_diary_entry_add(d, s, 4 * 1024 - (total_n + sizeof(" [...]")));
        cl_diary_entry_add(d, " [...]", sizeof(" [...]") - 1);
        break;
      }

      cl_diary_entry_add(d, s, n);
    }
  }

  srv_session_schedule(ses);

  cl_leave(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
           "session %s request %p has been "
           "fully parsed",
           ses->ses_displayname, req);
}

void srv_request_done(srv_request *req, unsigned int flags) {
  unsigned int old_flags = req->req_done;

  if ((req->req_done & flags) == flags) return;

  req->req_ready &= ~flags;
  req->req_done |= flags;

  /* Count the request:
   */
  if (flags & (1 << SRV_OUTPUT)) {
    if (req->req_done & (1 << SRV_INPUT))
      req->req_session->ses_requests_out++;
    else
      req->req_session->ses_requests_made++;
  }
  if (flags & (1 << SRV_INPUT)) {
    if (req->req_done & (1 << SRV_OUTPUT))
      req->req_session->ses_replies_received++;
    else
      req->req_session->ses_requests_in++;
  }

  if (req->req_session != NULL)
    srv_session_change(req->req_session, true, "srv_request_done");

  cl_log(req->req_session->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
         "srv_request_done: %llu@%p:%s%s%s", req->req_id, (void *)req,
         old_flags & (1 << SRV_INPUT)
             ? "=input"
             : (flags & (1 << SRV_INPUT) ? "+input" : ""),
         old_flags & (1 << SRV_OUTPUT)
             ? "=output"
             : (flags & (1 << SRV_OUTPUT) ? "+output" : ""),
         old_flags & (1 << SRV_RUN) ? "=run"
                                    : (flags & (1 << SRV_RUN) ? "+run" : ""));
}

void srv_request_ready(srv_request *req, unsigned int flags) {
  cl_handle *const cl = req->req_session->ses_bc.bc_cl;
  unsigned int old_flags = req->req_ready;

  /*  You can't become ready for something that you were
   *  marked as "done" for.
   */
  if (req->req_done & flags)
    cl_notreached(cl,
                  "request %llu@%p: done %x, ready %x - can't "
                  "become ready for something I'm done for!",
                  req->req_id, (void *)req, (unsigned int)req->req_done,
                  (unsigned int)req->req_ready);

  req->req_ready |= flags;
  req->req_done &= ~flags;

  if (req->req_session != NULL)
    srv_session_change(req->req_session, true, "srv_request_ready");

  cl_log(cl, CL_LEVEL_VERBOSE, "srv_request_ready %x->%x: %llu@%p:%s%s%s",
         old_flags, flags, req->req_id, (void *)req,
         old_flags & (1 << SRV_INPUT)
             ? " input"
             : (flags & (1 << SRV_INPUT) ? "+input" : ""),
         old_flags & (1 << SRV_OUTPUT)
             ? " output"
             : (flags & (1 << SRV_OUTPUT) ? "+output" : ""),
         old_flags & (1 << SRV_RUN) ? " run"
                                    : (flags & (1 << SRV_RUN) ? "+run" : ""));
}

void srv_request_run_ready(srv_request *req) {
  srv_request_ready(req, 1 << SRV_RUN);
}

void srv_request_output_ready(srv_request *req) {
  srv_request_ready(req, 1 << SRV_OUTPUT);
}

void srv_request_input_ready(srv_request *req) {
  srv_request_ready(req, 1 << SRV_INPUT);
}

void srv_request_run_done(srv_request *req) {
  srv_request_done(req, 1 << SRV_RUN);
}

void srv_request_input_done(srv_request *req) {
  srv_request_done(req, 1 << SRV_INPUT);
}

void srv_request_output_done(srv_request *req) {
  srv_request_done(req, 1 << SRV_OUTPUT);
}

void srv_request_suspend(srv_request *req) {
  if (req->req_done & (1 << SRV_RUN)) {
    char buf[200];
    cl_notreached(req->req_session->ses_bc.bc_cl,
                  "request %s cannot suspend itself - it's "
                  "already done running!",
                  srv_request_to_string(req, buf, sizeof buf));
  }
  req->req_ready &= ~(1 << SRV_RUN);
}

/**
 * @brief A request has been sent out.
 * @param req	request that has been sent out
 *
 *        	  client   server
 *       	     |        |
 *      	     |------->|
 *   YOU ARE HERE--> X        |
 *                   |<-------|
 *                   |        |
 *
 */
void srv_request_sent(srv_request *req) {
  srv_request_done(req, 1 << SRV_OUTPUT);
  srv_request_ready(req, 1 << SRV_INPUT);

  req->req_session->ses_requests_made++;

  cl_log(req->req_session->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
         "srv_request_sent: session %s request %p has been "
         "sent out",
         req->req_session->ses_displayname, req);
}

/**
 * @brief A reply to an earlier request has come back.
 * @param req	request to which a reply has been sent.
 *
 *     		     client   server
 *     		       |        |
 *     		       |------->|
 *		       |        |
 * 	               |<-------|
 *		       |        |
 *     You are here -> X        |
 *		       |        |
 *
 */
void srv_request_reply_received(srv_request *req) {
  srv_session *ses = req->req_session;

  ses->ses_replies_received++;

  srv_request_done(req, 1 << SRV_INPUT);

  /*  If we were planning to run, now is when we can run.
   */
  if (!(req->req_done & (1 << SRV_RUN))) srv_request_ready(req, 1 << SRV_RUN);

  /*  If this request had priority, give it up now.
   */
  srv_request_priority_release(req);

  cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
         "srv_request_reply_received: session %s request %p has "
         "received a reply",
         ses->ses_displayname, req);
}

/**
 * @brief A reply to an earlier request has been sent.
 *
 *     		     client   server
 *     		       |        |
 *     		       |------->|
 *		       |        |
 * 	               |<-------|
 *		       |        X <--- YOU ARE HERE
 *		       |        |
 *
 */
void srv_request_reply_sent(srv_request *req) {
  srv_session *ses = req->req_session;

  srv_request_done(req, 1 << SRV_OUTPUT);

  /*  If this request had priority, give it up now.
   */
  srv_request_priority_release(req);

  cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
         "srv_request_reply_sent: session %s request %p has "
         "replied to a request",
         req->req_session->ses_displayname, req);
}

/**
 * @brief A request has completely finished processing.
 * @param req	request to which a reply has been sent.
 *
 *          client   server
 *            |        |
 *            |------->|
 *	      |        |
 *            |<-------|
 *            |        X <--- YOU ARE HERE
 *	      X<--- OR HERE
 *
 */
void srv_request_complete_loc(srv_request *req, char const *file, int line) {
  srv_session *ses = req->req_session;

  /*  We're done with everything.
   */
  if (req->req_done !=
      ((1 << SRV_INPUT) | (1 << SRV_OUTPUT) | (1 << SRV_RUN))) {
    srv_request_done(
        req, ~req->req_done &
                 ((1 << SRV_INPUT) | (1 << SRV_OUTPUT) | (1 << SRV_RUN)));
    req->req_ready = 0;

    srv_request_priority_release(req);
    srv_session_change(req->req_session, true, "srv_request_complete");

    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_DEBUG,
           "srv_request_complete: session %s request %llu@%p is done [%s:%d]",
           req->req_session->ses_displayname, req->req_id, (void *)req, file,
           line);
  } else {
    cl_log(ses->ses_bc.bc_cl, CL_LEVEL_VERBOSE,
           "srv_request_complete: session %s request %llu@%p is "
           "done [%s:%d, spurious]",
           req->req_session->ses_displayname, req->req_id, (void *)req, file,
           line);
  }
}

char const *srv_request_to_string(srv_request const *req, char *buf,
                                  size_t size) {
  char *w = buf;
  char const *e = buf + size;
  char const *sep = "";

  snprintf(buf, size, "%llu:%p ", req->req_id, (void *)req);
  w += strlen(buf);

  if (req->req_ready) {
    if (req->req_ready & (1 << SRV_INPUT)) {
      snprintf(w, e - w, "input");
      w += strlen(w);
      sep = ",";
    }
    if (req->req_ready & (1 << SRV_RUN)) {
      snprintf(w, e - w, "%srun", sep);
      w += strlen(w);
      sep = ",";
    }
    if (req->req_ready & (1 << SRV_OUTPUT)) {
      snprintf(w, e - w, "%soutput", sep);
      w += strlen(w);
    }
  } else {
    snprintf(w, e - w, "(suspended)");
    w += strlen(w);
  }

  if ((~req->req_done & ~req->req_ready) &
      ((1 << SRV_RUN) | (1 << SRV_OUTPUT) | (1 << SRV_INPUT))) {
    sep = "";
    snprintf(w, e - w, " (pending: ");
    w += strlen(w);

    if ((~req->req_done & ~req->req_ready) & (1 << SRV_INPUT)) {
      snprintf(w, e - w, "input");
      w += strlen(w);
      sep = ",";
    }
    if ((~req->req_done & ~req->req_ready) & (1 << SRV_RUN)) {
      snprintf(w, e - w, "%srun", sep);
      w += strlen(w);
      sep = ",";
    }
    if ((~req->req_done & ~req->req_ready) & (1 << SRV_OUTPUT)) {
      snprintf(w, e - w, "%soutput", sep);
      w += strlen(w);
    }
    snprintf(w, e - w, ")");
  }
  return buf;
}

bool srv_request_is_complete(srv_request const *req) {
  if (req->req_done ==
      ((1 << SRV_INPUT) | (1 << SRV_OUTPUT) | (1 << SRV_RUN))) {
    return true;
  }
  return false;
}

/*  Has an error occurred that will affect this request?
 */
bool srv_request_error(srv_request const *req) {
  return ((!(req->req_done & (1 << SRV_INPUT))) &&
          (req->req_session->ses_bc.bc_error & SRV_BCERR_READ)) ||
         ((!(req->req_done & (1 << SRV_OUTPUT)) &&
           (req->req_session->ses_bc.bc_error & SRV_BCERR_WRITE)));
}
