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
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>

#include "srvp.h"

#define SRV_GOOD_READ_SIZE 1024

#define STR(x) #x
#define CHANGE(ses, x, val) \
  ((x) == (val)             \
       ? false              \
       : ((x) = (val),      \
          srv_session_change((ses), true, STR(x) " := " STR(val)), true))

static char const *bc_error_to_string(unsigned int bc) {
  switch (bc) {
    case 0:
      return "";
    case SRV_BCERR_WRITE:
      return "ERROR ";
    case SRV_BCERR_READ:
      return "[EOF] ";
    case SRV_BCERR_SOCKET:
      return "[SOCKET ERROR] ";
  }
  return "[unexpected bc_error] ";
}

char const *srv_buffered_connection_to_string(srv_buffered_connection const *bc,
                                              char *buf, size_t size) {
  snprintf(buf, size, "%s%sIN:%s%s%s; OUT:%s%s%s",

           bc_error_to_string(bc->bc_error),
           bc->bc_have_priority ? " [PRIORITY]" : "",

           bc->bc_data_waiting_to_be_read ? "+wire" : "",
           bc->bc_input_buffer_capacity_available ? "+buffer" : "",
           bc->bc_input_waiting_to_be_parsed ? "+bytes" : "",

           bc->bc_write_capacity_available ? "+wire" : "",
           bc->bc_output_buffer_capacity_available ? "+buffer" : "",
           bc->bc_output_waiting_to_be_written ? "+bytes" : "");

  return buf;
}

void srv_buffered_connection_initialize(srv_buffered_connection *bc,
                                        cl_handle *cl, srv_buffer_pool *pool) {
  cl_assert(cl, pool != NULL);
  cl_assert(cl, bc != NULL);

  srv_buffer_queue_initialize(&bc->bc_output);
  srv_buffer_queue_initialize(&bc->bc_input);

  bc->bc_cl = cl;
  bc->bc_input_buffer_capacity_available = false;
  bc->bc_output_waiting_to_be_written = false;
  bc->bc_input_waiting_to_be_parsed = false;
  bc->bc_output_buffer_capacity_available = false;
  bc->bc_have_priority = false;
  bc->bc_error = 0;
  bc->bc_errno = 0;
  bc->bc_pool = pool;
}

void srv_buffered_connection_shutdown(srv_handle *srv,
                                      srv_buffered_connection *bc) {
  srv_buffer *buf;

  /* Release the buffers in the input and output queue
   * back into their pool.
   */
  while ((buf = srv_buffer_queue_remove(&bc->bc_output)) != NULL)
    srv_buffer_pool_free(srv, bc->bc_pool, buf);

  while ((buf = srv_buffer_queue_remove(&bc->bc_input)) != NULL)
    srv_buffer_pool_free(srv, bc->bc_pool, buf);
}

/**
 * @brief Allocate a buffer.
 *
 * @param bc the buffered connection this is happening for
 * @param priority
 *	2 if we could just generally use a buffer,
 *	1 if there's no other buffer to write into,
 *	0 if the connection really has priority and immediate use
 * @param what_kind documentation: "input" or "output"
 * @param line	documentation: caller's line
 *
 * @return 0 on success
 * @return ENOMEM if there's no buffer space currently available
 */
srv_buffer *srv_buffered_connection_policy_alloc(srv_buffered_connection *bc,
                                                 int priority,
                                                 char const *what_kind,
                                                 int line) {
  double pool_avail;
  srv_buffer *buf;

  cl_assert(bc->bc_cl, bc->bc_pool != NULL);

  /*  If we can easily afford it, or if it would be fair,
   *  or if this request has priority, allocate more storage.
   */
  pool_avail = srv_buffer_pool_available(bc->bc_pool);

  if (pool_avail >= SRV_BUFFER_POOL_MIN_GENEROUS ||
      (priority < 2 &&
       (pool_avail >= SRV_BUFFER_POOL_MIN_FAIR || priority < 1))) {
    if ((buf = srv_buffer_pool_alloc(bc->bc_pool)) == NULL) {
      cl_log(bc->bc_cl, CL_LEVEL_DEBUG,
             "bc: %s:%d buffer "
             "allocation rejected by system for %s:%d",
             what_kind, priority, __FILE__, line);
      return NULL;
    }

    cl_log(bc->bc_cl, CL_LEVEL_DEBUG,
           "bc: adding %s:%d buffer %p "
           "for %s:%d",
           what_kind, priority, (void *)buf, __FILE__, line);
    return buf;
  }
  cl_log(bc->bc_cl, CL_LEVEL_DEBUG,
         "bc: %s:%d buffer allocation "
         "rejected by policy (available: %lf, generous: %.0lf, "
         "fair: %.0lf) for %s:%d",
         what_kind, priority, pool_avail, (double)SRV_BUFFER_POOL_MIN_GENEROUS,
         (double)SRV_BUFFER_POOL_MIN_FAIR, __FILE__, line);

  return NULL;
}

void srv_buffered_connection_have_priority(srv_buffered_connection *bc,
                                           bool val) {
  bc->bc_have_priority = val;
}

bool srv_buffered_connection_input_waiting_to_be_parsed(
    srv_handle *srv, srv_buffered_connection *bc) {
  srv_buffer *buf;

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
  bc->bc_input_waiting_to_be_parsed = buf != NULL && buf->b_i < buf->b_n;

  return bc->bc_input_waiting_to_be_parsed;
}

/*  There's been an error. Throw away this session's unused input.
 */
void srv_buffered_connection_clear_unparsed_input(srv_handle *srv,
                                                  srv_buffered_connection *bc) {
  srv_buffer *buf;

  /* Drop emptied input buffers if
   *	- there is a buffer
   *  	- we've parsed everything that's in it  	(i >= n)
   * 	- for reading, there's already a next buffer 	(b_next)
   *	- or we've read so much into the existing one
   *		that we're almost full. 		(m - n < MIN)
   */
  while ((buf = bc->bc_input.q_head) != NULL) {
    if ((bc->bc_input.q_head = buf->b_next) == NULL)
      bc->bc_input.q_tail = &bc->bc_input.q_head;

    /*  Unlink the buffer from the session.  If pending
     *  requests used their contents, the requests still
     *  link to them, and they'll only be free'd once the
     *  request is serviced.
     */
    if (srv_buffer_unlink(buf)) srv_buffer_pool_free(srv, bc->bc_pool, buf);
  }

  cl_assert(bc->bc_cl, bc->bc_input.q_head == NULL);
  bc->bc_input_waiting_to_be_parsed = false;
}

static int srv_buffered_connection_write_call_pre_hook(
    srv_buffered_connection *bc, es_descriptor *ed, srv_buffer *buf, bool block,
    bool *any_out) {
  int err = 0;

  cl_assert(bc->bc_cl, bc->bc_pool != NULL);
  cl_assert(bc->bc_cl, ed != NULL);

  /* The callback may overwrite that.
   */
  *any_out = false;

  err = (*buf->b_pre_callback)(buf->b_pre_callback_data, block, any_out);

  cl_assert(bc->bc_cl, !block || (err != SRV_ERR_MORE));
  if (err == SRV_ERR_MORE) return SRV_ERR_MORE;

  /*  We did something - namely, got the callback over with.
   */
  *any_out = true;

  cm_free(buf->b_cm, buf->b_pre_callback_data);
  buf->b_pre_callback_data = NULL;
  buf->b_pre_callback = NULL;

  if (err != 0)
    cl_log(bc->bc_cl, CL_LEVEL_ERROR,
           "%s%sS: [shutting "
           "down because of pre-write hook error: %s]",
           ed->ed_displayname ? ed->ed_displayname : "",
           ed->ed_displayname ? ": " : "", strerror(err));
  return err;
}

/*  Get <bc> ready to write (to <ed>.)
 *
 * @param bc		The buffered connection
 * @param ed		The output descriptor
 * @param block		true if the call should block
 * @param any_out	out: Did we actually do anything?
 *
 * @return 0		if the descriptor is as ready
 *			as it'll ever be (including
 *			if it's empty)
 *
 * @return SRV_ERR_MORE	if block wasn't set, and the pre-hook
 *			flush is still in progress.
 *
 * @return other nonzero error codes on system error.
 *
 */
int srv_buffered_connection_write_ready(srv_buffered_connection *bc,
                                        es_descriptor *ed, bool *any_out) {
  srv_buffer *buf;
  int err = 0;

  *any_out = false;

  /*  Yes, we're ready to write (and discover
   *  that we do not have anything to write).
   */
  if ((buf = bc->bc_output.q_head) == NULL) return 0;

  if (buf->b_pre_callback == NULL) return 0;

  err = srv_buffered_connection_write_call_pre_hook(bc, ed, buf, true, any_out);

  if (err == SRV_ERR_MORE) return err;

  *any_out = true;

  if (err) {
    cl_log_errno(bc->bc_cl, CL_LEVEL_FAIL,
                 "srv_buffered_connection_write_call_pre_hook", err,
                 "pre-hook fails -> SRV_BCERR_WRITE");

    bc->bc_error |= SRV_BCERR_WRITE;
    bc->bc_errno = err;
  }
  return err;
}

int srv_buffered_connection_write(srv_handle *srv, srv_buffered_connection *bc,
                                  int fd, es_handle *es, es_descriptor *ed_out,
                                  bool *any_out) {
  bool first = true;
  srv_buffer *buf;

  cl_assert(bc->bc_cl, bc->bc_pool != NULL);
  cl_assert(bc->bc_cl, es != NULL);
  cl_assert(bc->bc_cl, ed_out != NULL);

  *any_out = false;

  /* Do we have anything to write?
   */
  while ((buf = bc->bc_output.q_head) != NULL) {
    if (buf->b_pre_callback != NULL) {
      int err = 0;
      bool write_any = false;

      /*  Call the pre-hook nonblockingly on later
       *  calls, blockingly on the first.
       *
       *  If you want to do an asynchronous flush,
       *  use srv_buffered_connection_write_ready()
       *  prior to srv_buffered_connection_write()
       *  to get that out of the way.
       */
      err = srv_buffered_connection_write_call_pre_hook(bc, ed_out, buf,
                                                        /* block? */ first,
                                                        &write_any);

      if (err == SRV_ERR_MORE) {
        bc->bc_write_capacity_available = 0;
        *any_out |= write_any;

        /*  This code path used to make some strange
         *  assertions and did nothing. It was reverted
         */
        return 0;
      }
      *any_out = true;

      if (err) {
        cl_log_errno(bc->bc_cl, CL_LEVEL_FAIL,
                     "srv_buffered_connection_write_"
                     "call_pre_hook",
                     err, "pre-hook fails -> SRV_BCERR_WRITE");

        bc->bc_error |= SRV_BCERR_WRITE;
        bc->bc_errno = err;

        break;
      }
    }

    if (buf->b_i < buf->b_n) {
      ssize_t cc;

      cc = write(fd, buf->b_s + buf->b_i, buf->b_n - buf->b_i);
      if (cc <= 0) {
        if (cc == 0 || errno == EAGAIN || errno == EINPROGRESS) {
          bc->bc_write_capacity_available = 0;
        } else {
          *any_out = true;

          bc->bc_errno = errno;

          cl_log_errno(bc->bc_cl, CL_LEVEL_FAIL, "write", errno,
                       "write error -> "
                       "SRV_BCERR_WRITE");

          bc->bc_error |= SRV_BCERR_WRITE;
        }
        break;
      }

      /*  Log outgoing buffer traffic.
       */
      bc->bc_total_bytes_out += cc;
      cl_log(bc->bc_cl, CL_LEVEL_DETAIL, "%s%sS: %.*s%s",
             ed_out->ed_displayname ? ed_out->ed_displayname : "",
             ed_out->ed_displayname ? ": " : "", (int)(cc > 200 ? 200 : cc),
             buf->b_s + buf->b_i, cc > 200 ? "..." : "");

      buf->b_i += cc;
      *any_out = true;
    }

    /* We wrote less than we could have.
     */
    if (buf->b_i < buf->b_n) {
      bc->bc_write_capacity_available = 0;

      cl_log(bc->bc_cl, CL_LEVEL_DETAIL, "%s%sstill %lu bytes left to write.",
             ed_out->ed_displayname ? ed_out->ed_displayname : "",
             ed_out->ed_displayname ? ": " : "",
             (unsigned long)buf->b_n - buf->b_i);

      break;
    }

    /*  All the data in this buffer has been written.
     */
    cl_assert(bc->bc_cl, buf->b_i == buf->b_n);

    /* Recycle a fully written buffer if it's no longer
     * used for formatting.
     *
     * We know it isn't used for formatting:
     * (a) if it has a trailer (buf->b_next) that is used instead
     * (b) if it is so full that it's not worth it.
     *	(fewer than SRV_MIN_BUFFER_SIZE bytes left.)
     */

    if (buf->b_next == NULL && buf->b_m - buf->b_n >= SRV_MIN_BUFFER_SIZE &&
        srv_buffer_pool_available(bc->bc_pool) >= SRV_BUFFER_POOL_MIN_FAIR)

      /* No, we're still using this one. */
      break;

    buf = srv_buffer_queue_remove(&bc->bc_output);
    cl_assert(bc->bc_cl, buf != NULL);

    srv_buffer_pool_free(srv, bc->bc_pool, buf);
    first = false;
  }
  return 0;
}

/*  Read input that is waiting on a buffered connection.
 *
 *  Returns true if something actually happened,
 *  false for a false alarm.
 */
bool srv_buffered_connection_read(srv_session *ses, int fd,
                                  es_descriptor *ed_in) {
  srv_buffered_connection *bc = &ses->ses_bc;
  ssize_t cc;
  srv_buffer *buf;

  srv_session_status(ses);
  /*
  srv_buffer_queue_check(bc->bc_cl, &bc->bc_input);
  */

  buf = srv_buffer_queue_tail(&bc->bc_input);
  if (buf == NULL || buf->b_n >= buf->b_m) {
    char sb[200];

    cl_assert(bc->bc_cl, bc->bc_input_buffer_capacity_available);
    cl_log(bc->bc_cl, CL_LEVEL_DEBUG,
           "srv_buffered_connection_read: out of input "
           "buffer space on %s",
           srv_session_to_string(ses, sb, sizeof sb));

    return CHANGE(ses, bc->bc_input_buffer_capacity_available, false);
  }

  cl_assert(bc->bc_cl, buf != NULL && buf->b_n < buf->b_m);
  while ((cc = read(fd, buf->b_s + buf->b_n, buf->b_m - buf->b_n)) > 0) {
    /*  Log the incoming data.
     */
    bc->bc_total_bytes_in += cc;
    cl_log(bc->bc_cl, CL_LEVEL_DETAIL, "%s%sC: %.*s%s",
           ed_in->ed_displayname ? ed_in->ed_displayname : "",
           ed_in->ed_displayname ? ": " : "", (int)(cc > 8000 ? 8000 : cc),
           buf->b_s + buf->b_n, cc > 8000 ? "..." : "");

    /* We read less than we could have?
     */
    if (cc < buf->b_m - buf->b_n)
      CHANGE(ses, bc->bc_data_waiting_to_be_read, false);

    buf->b_n += cc;
    bc->bc_input_waiting_to_be_parsed = 1;

    cl_log(bc->bc_cl, CL_LEVEL_DEBUG, "bc: %zu bytes ready to be parsed",
           buf->b_n - buf->b_i);

    srv_buffer_check(bc->bc_cl, buf);
    if (buf->b_n >= buf->b_m) {
      CHANGE(ses, bc->bc_input_buffer_capacity_available, false);
      break;
    }
  }
  if (cc < 0) {
    if (EAGAIN == errno || EINPROGRESS == errno)
      bc->bc_data_waiting_to_be_read = false;
    else {
      cl_loglevel ll = ECONNRESET == errno ? CL_LEVEL_FAIL : CL_LEVEL_ERROR;

      bc->bc_error |= SRV_BCERR_READ;
      bc->bc_errno = errno;
      cl_log_errno(bc->bc_cl, ll, "read", errno, "read( %d ) failed", fd);
    }
  } else if (cc == 0) {
    /* close/error */

    CHANGE(ses, bc->bc_error, bc->bc_error | SRV_BCERR_READ);
    CHANGE(ses, bc->bc_data_waiting_to_be_read, false);

    bc->bc_errno = 0;

    cl_log(bc->bc_cl, CL_LEVEL_DEBUG, "EOF event on fd %d", fd);
  }
  return true;
}

/**
 * @brief Look ahead into an input buffer
 * @param bc	Buffered connection we're curious about
 * @param s_out	out: beginning of a available data
 * @param e_out	out: end of a available data (would point to the '\\0' byte
 *	if there were one, but there isn't necessarily one)
 * @param b_out	out: buffer that the data is housed in.
 * @return 0 on success
 * @return SRV_ERR_NO if there is no data pending
 */
int srv_buffered_connection_input_lookahead(srv_buffered_connection *bc,
                                            char **s_out, char **e_out,
                                            srv_buffer **b_out) {
  srv_buffer *buf;

  buf = bc->bc_input.q_head;
  if (buf->b_i < buf->b_n) {
    /* srv_buffer_queue_check(bc->bc_cl, &bc->bc_input);
     */
    srv_buffer_check(bc->bc_cl, buf);

    *s_out = buf->b_s + buf->b_i;
    *e_out = buf->b_s + buf->b_n;
    *b_out = buf;

    cl_assert(bc->bc_cl, *e_out >= buf->b_s + buf->b_i);
    cl_assert(bc->bc_cl, *e_out <= buf->b_s + buf->b_n);

    return 0;
  }
  return SRV_ERR_NO;
}

void srv_buffered_connection_input_commit(srv_handle *srv,
                                          srv_buffered_connection *bc,
                                          char const *e) {
  srv_buffer *buf;

  buf = bc->bc_input.q_head;
  cl_assert(bc->bc_cl, buf != NULL);
  cl_assert(bc->bc_cl, e != NULL);

  if (e < buf->b_s + buf->b_i)
    cl_notreached(bc->bc_cl, "e %p, b_s %p, i %llu", (void *)e, buf->b_s,
                  (unsigned long long)buf->b_i);

  if (e > buf->b_s + buf->b_n)
    cl_notreached(bc->bc_cl, "e %p, b_s %p, i %llu, n %llu", (void *)e,
                  buf->b_s, (unsigned long long)buf->b_i,
                  (unsigned long long)buf->b_n);

  cl_assert(bc->bc_cl, e >= buf->b_s + buf->b_i);
  cl_assert(bc->bc_cl, e <= buf->b_s + buf->b_n);
  srv_buffer_check(bc->bc_cl, buf);

  buf->b_i = e - buf->b_s;
  srv_buffer_check(bc->bc_cl, buf);

  if (buf->b_i >= buf->b_n) {
    /* We've parsed everything in this buffer, and can recycle it
     * if it's not useful for reading input.
     *
     * We know it's not useful if
     * (a) it has a sibling that's already being used.
     * (b) it's pretty much full
     * (c) it's not linked to any requests.
     */

    if (buf->b_next || buf->b_m - buf->b_n < SRV_GOOD_READ_SIZE) {
      srv_buffer *bp;

      bp = srv_buffer_queue_remove(&bc->bc_input);

      cl_assert(bc->bc_cl, buf == bp);
      cl_assert(bc->bc_cl, buf->b_refcount >= 1);
      cl_assert(bc->bc_cl, bc->bc_pool != NULL);

      if (srv_buffer_unlink(buf)) srv_buffer_pool_free(srv, bc->bc_pool, buf);
    }
  }
}

/**
 * @brief Return a place to write formatted output to.
 *
 *  The caller wants to format output.
 *  They want at least min_size free bytes to write into.
 *
 *  If the call succeeds, the actual data written to must
 *  be declared with a call to srv_buffered_connection_output_commit().
 *
 *  This is one of the few places where an errno of ENOMEM
 *  actually may denote a safe policy decision, not a local resource
 *  catastrophe.
 *
 * @param bc the buffered connection this is happening for
 * @param min_size minimum # of bytes to bother with
 * @param s_out assign beginning of the formatting area to *s
 * @param e_out assign end of the formatting area to *e
 *
 * @return 0 on success
 * @return ENOMEM if there's no buffer space currently available
 */
int srv_buffered_connection_output_lookahead(srv_session *ses, size_t min_size,
                                             char **s_out, char **e_out) {
  srv_buffered_connection *bc = &ses->ses_bc;
  srv_buffer *buf;

  cl_assert(bc->bc_cl, min_size <= SRV_MIN_BUFFER_SIZE);
  cl_assert(bc->bc_cl, s_out);
  cl_assert(bc->bc_cl, e_out);

  if ((buf = srv_buffer_queue_tail(&bc->bc_output)) == NULL ||
      buf->b_m - buf->b_n < min_size) {
    buf = srv_buffered_connection_policy_alloc(
        bc, srv_session_output_priority(ses), "output", __LINE__);
    if (buf == NULL) {
      bc->bc_output_buffer_capacity_available = false;
      return ENOMEM;
    }
    cl_assert(bc->bc_cl, buf->b_m - buf->b_n >= min_size);

    srv_buffer_queue_append(&bc->bc_output, buf);
    bc->bc_output_buffer_capacity_available = true;
  }

  cl_assert(bc->bc_cl, buf->b_m - buf->b_n >= min_size);

  srv_buffer_check(bc->bc_cl, buf);

  *s_out = buf->b_s + buf->b_n;
  *e_out = buf->b_s + buf->b_m;

  return 0;
}

/*  Of the [*s_out...*e_out) the caller received at their
 *  most recent call to srv_buffered_connection_output_lookahead(),
 *  they've actually used up to, excluding, *e.
 */
void srv_buffered_connection_output_commit(srv_buffered_connection *bc,
                                           char const *e) {
  srv_buffer *buf;

  buf = srv_buffer_queue_tail(&bc->bc_output);

  cl_assert(bc->bc_cl, buf != NULL);
  cl_assert(bc->bc_cl, e >= buf->b_s + buf->b_n);
  cl_assert(bc->bc_cl, e <= buf->b_s + buf->b_m);

  buf->b_n = e - buf->b_s;
}

/*
 * @brief Associate a closure with an output buffer.
 *
 *  The caller has called srv_buffered_connection_output_lookahead,
 *  and has not yet called srv_buffered_connection_output_commit().
 *
 *  There is a current output buffer, and it's in the process of
 *  being appended to.
 *
 *  Before sending any of the output that's being written right now,
 *  libsrv must also invoke callback with callback_data.
 *  If that invocation fails, the connection will be broken,
 *  and the data will not be sent.
 *
 * @param bc			The buffered connection in charge
 * @param callback		Callback to invoke
 * @param callback_data_size	Size of the closure to allocate
 *
 * @return NULL on memory error, otherwise a per-buffer closure.
 * 	If the callback didn't have a closure, a new closure
 *	of the given size was allocated.
 */
void *srv_buffered_connection_output_alloc_pre_hook(srv_buffered_connection *bc,
                                                    srv_pre_callback *callback,
                                                    size_t callback_data_size) {
  srv_buffer *buf;
  void *mem;

  buf = srv_buffer_queue_tail(&bc->bc_output);

  /*  If you called srv_buffered_connection_output_lookahead
   *  and it returned 0, there has to be a buffer.
   */
  cl_assert(bc->bc_cl, buf != NULL);

  if (buf->b_pre_callback_data != NULL) {
    /*  If we need multiple callbacks, we can make this
     *  a list - but it's really just one.
     */
    cl_assert(bc->bc_cl, buf->b_pre_callback == callback);
    return buf->b_pre_callback_data;
  }

  if (callback_data_size == 0)
    mem = NULL;
  else {
    mem = cm_malloc(buf->b_cm, callback_data_size);
    if (mem == NULL) return NULL;
    memset(mem, 0, callback_data_size);
  }

  buf->b_pre_callback_data = mem;
  buf->b_pre_callback = callback;

  return mem;
}

void srv_buffered_connection_check(srv_buffered_connection *bc) {
  cl_handle *cl = bc->bc_cl;

  cl_assert(cl, bc->bc_pool != NULL);

  srv_buffer_queue_check(cl, &bc->bc_input);
  srv_buffer_queue_check(cl, &bc->bc_output);
}
