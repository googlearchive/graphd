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
#include "libgraphdb/graphdbp.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>

/*
 *  graphdb_request_send_buffer_req -- send a buffer or buffer chain
 *
 *  Parameters:
 *	graphdb		-- handle created with graphdb_create(),
 *			   must be connected.
 *	request_id_out	-- where to store a request id
 *	application_data -- opaque data strored with request
 *	buf		-- buffer
 */

int graphdb_request_send_buffer_req(graphdb_handle *graphdb,
                                    graphdb_request *req, graphdb_buffer *buf) {
  int queue_was_empty;

  if (!GRAPHDB_IS_HANDLE(graphdb)) return EINVAL;

  if (req == NULL) {
    graphdb_log(graphdb, CL_LEVEL_SPEW,
                "graphdb_request_send_buffer_req: NULL request");
    return EINVAL;
  }
  if (buf == NULL) {
    graphdb_log(graphdb, CL_LEVEL_SPEW,
                "graphdb_request_send_buffer_req: NULL buffer "
                "for request #%lu",
                (unsigned long)req->req_id);
    return EINVAL;
  }

  /*  Given that we know this is a whole request, we can
   *  compensate for some common syntax errors in the input,
   *  e.g. misplaced newlines.
   *
   *  I don't particularly like doing this in a low-level library,
   *  but at least this library knows what the syntax is, and can
   *  do things in C and in an already allocated buffer that would
   *  take a while longer in Python.
   */
  graphdb_buffer_format_dwim(graphdb, buf);

  graphdb_log(graphdb, CL_LEVEL_SPEW, "send request %p [slot id %lu]",
              (void *)req, (unsigned long)req->req_id);

  /* If the buffer is empty, we're done sending it.
   */
  while (buf->buf_data_i >= buf->buf_data_n) {
    graphdb_buffer *next = buf->buf_next;

    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_request_send_buffer_req: emtpy buffer %p?",
                (void *)buf);
    graphdb_buffer_free(graphdb, buf);

    if ((buf = next) == NULL) {
      graphdb_log(graphdb, CL_LEVEL_DEBUG,
                  "graphdb_request_send_buffer_req: "
                  "all were empty!");
      return 0;
    }
  }

  /*  Chain the buffer into the request.
   */
  req->req_out = req->req_out_unsent = buf;

  /*  Chain the request into the graphdb.
   */
  queue_was_empty = graphdb->graphdb_request_head == NULL;

  graphdb_request_chain_in(graphdb, req);
  if (graphdb->graphdb_connected && queue_was_empty)
    (void)graphdb_request_io(graphdb, 0);

  return 0;
}

static int graphdb_request_check_finish(graphdb_handle *graphdb,
                                        size_t *nparen_inout,
                                        bool *in_string_inout,
                                        bool *escaped_inout,
                                        bool *newline_inout) {
  if (*in_string_inout)
    graphdb->graphdb_syntax_error = "unmatched open \" at end of request";
  else if (*nparen_inout > 0)
    graphdb->graphdb_syntax_error = "unmatched open ( at end of request";
  else if (!*newline_inout)
    graphdb->graphdb_syntax_error = "request has no closing newline";
  else
    return 0;

  graphdb_log(graphdb, CL_LEVEL_FAIL, "graphdb_request_check: %s",
              graphdb->graphdb_syntax_error);
  return EINVAL;
}

static int graphdb_request_check_next(graphdb_handle *graphdb, char const *s,
                                      char const *e, size_t *nparen_inout,
                                      bool *in_string_inout,
                                      bool *escaped_inout,
                                      bool *newline_inout) {
  if (s < e && *newline_inout) {
    graphdb_log(graphdb, CL_LEVEL_FAIL, "graphdb_request_check: %s",
                graphdb->graphdb_syntax_error =
                    "request contains a newline outside "
                    "parentheses or strings, other than "
                    "at the very end.");
    return EINVAL;
  }

  for (; s < e; s++) {
    if (*s == '\n' && *nparen_inout == 0) {
      if (s == e - 1) {
        *newline_inout = true;
        return 0;
      }

      graphdb_log(graphdb, CL_LEVEL_FAIL, "graphdb_request_check: %s",
                  graphdb->graphdb_syntax_error =
                      "request contains a newline outside "
                      "parentheses or strings, other than "
                      "at the very end.");
      return EINVAL;
    } else if (*escaped_inout) {
      *escaped_inout = false;
    } else if (*s == '"') {
      *in_string_inout = !*in_string_inout;
    } else if (!*in_string_inout) {
      if (*s == '(')
        ++*nparen_inout;
      else if (*s == ')') {
        if (*nparen_inout > 0)
          --*nparen_inout;
        else {
          graphdb_log(graphdb, CL_LEVEL_FAIL, "graphdb_request_check: %s",
                      graphdb->graphdb_syntax_error =
                          "request contains a closing "
                          ") without matching (");
          return EINVAL;
        }
      }
    } else if (*s == '\\')
      *escaped_inout = true;
  }
  return 0;
}

static int graphdb_request_check_buffer(graphdb_handle *graphdb,
                                        graphdb_buffer *buf) {
  graphdb_buffer *b;
  size_t nparen = 0;
  bool in_string = false;
  bool escaped = false;
  bool newline = false;
  int err;

  for (b = buf->buf_head; b != NULL; b = b->buf_next) {
    err = graphdb_request_check_next(graphdb, b->buf_data,
                                     b->buf_data + b->buf_data_n, &nparen,
                                     &in_string, &escaped, &newline);
    if (err != 0) return err;
  }
  return graphdb_request_check_finish(graphdb, &nparen, &in_string, &escaped,
                                      &newline);
}

int graphdb_request_check(graphdb_handle *graphdb, char const *s,
                          char const *e) {
  size_t nparen = 0;
  bool in_string = false;
  bool escaped = false;
  bool newline = false;
  int err;

  err = graphdb_request_check_next(graphdb, s, e, &nparen, &in_string, &escaped,
                                   &newline);
  if (err) return err;

  return graphdb_request_check_finish(graphdb, &nparen, &in_string, &escaped,
                                      &newline);
}

/**
 * @brief Send a buffer as a request
 *
 *  The resulting request must not be free'd by request ID.
 *
 * @param graphdb	opaque graphdb handle, created with
 *			graphdb_create() and connected with grpahdb_connect().
 * @param request_id_out	NULL, or a pointer to assign the id
 *				of a successfully created request to.
 * @param application_data	associate this opaque application pointer
 *				with the request
 * @param buf			buffer to send.
 *
 * @return 0 on success, otherwise a nonzero error number.
 */
int graphdb_request_send_buffer(graphdb_handle *graphdb,
                                graphdb_request_id *request_id_out,
                                void *application_data, graphdb_buffer *buf) {
  graphdb_request *req;

  if (!GRAPHDB_IS_HANDLE(graphdb) || buf == NULL) return EINVAL;

  /*  Check whether the buffer contains exactly one request
   *  with no open parentheses or double quotes.
   */
  if (graphdb->graphdb_check_syntax) {
    int err = graphdb_request_check_buffer(graphdb, buf);
    if (err) return err;
  }

  /*  Allocate a new request.  Our code now holds
   *  one link onto the request.
   */
  if ((req = graphdb_request_alloc(graphdb)) == NULL) return ENOMEM;

  /*  The handling code now holds one reference to
   *  req; we must free it at the end.
   */

  req->req_application_data = application_data;
  req->req_out = req->req_out_unsent = buf;

  // See below.
  // int queue_was_empty;
  // queue_was_empty = graphdb->graphdb_request_head == NULL;

  /*  Now our infrastructure chain holds another link to the
   *  request.
   */
  graphdb_request_chain_in(graphdb, req);
  if (request_id_out != NULL) *request_id_out = req->req_id;

  /*  Free the code request link, leaving only the
   *  infrastructure link.
   */
  graphdb_request_unlink_req(graphdb, req);

  /*
  if (graphdb->graphdb_connected && queue_was_empty)
          (void) graphdb_request_io(graphdb, 0);
  */

  return 0;
}

/**
 * @brief Format and send a graph query, popper version.
 *
 *  The results are returned via graphdb_request_wait().
 *  For a more high-level synchronous query return mechanism, see
 *  graphdb_query_*().
 *
 * @param graphdb		handle created with graphdb_create() and
 *				connected to a server with graphdb_connect().
 * @param request_id_out	assign the request id to this address.
 * @param application_data	attach this application data to the request.
 * @param fmt			format string for request; followed by
 *			arguments for the format sequences in the string.
 * @param popper		the abstract argument list
 *
 * @par Formats
 * In the format string, literal % must be escaped
 * as %%.  Other than that, the following format
 * sequences have the following meaning:\n
 * @par
 * @b %%q \n
 * The argument, a char const *, is either NULL
 * or points to a '\\0'-terminated string.  NULL is sent as the word null.
 * Everything else is sent as a quoted strings, with contained \\ or "
 * or newline characters properly escaped.\n
 * @par
 * @b %%*q \n
 * Like %%q, but the arguments are a size_t
 * and a char const *, and the string need not be '\\0'-terminated.
 * A NULL string pointer is sent as the word null, regardless of the size_t.
 * @par
 * @b %%s \n
 * Arbitrary text, included literally.
 * The argument is a char const * pointing to a '\\0'-terminated string.
 * A NULL pointer is sent as the word null.
 * @par
 * @b %%*s \n
 * Like %%s, but the arguments are a size_t and a char const *,
 * and the string need not be '\\0'-terminated.
 * A NULL pointer with non-zero size is sent as the word null.
 * @par
 * @b %%g \n
 * A GUID.  The argument is NULL or a graph_guid const *.  Both
 * NULL and a NULL graph guid are sent as the word null.
 * @par
 * @b %%b \n
 * A boolean.  The argument is an int.	If it is zero, the word
 * false is sent; otherwise, the word true.
 * @par
 * @b %%d \n
 * A datatype.	The argument is an int.	 Its value is interpreted
 * as a graph_datatype and sent as a word.
 * @par
 * @b %%u \n
 * An unsigned number.	The argument is an unsigned long long; its
 * numeric value is sent as decimal digits.
 * @par
 * @b %%t \n
 * A time stamp.  The argument is a graph_timestamp_t.	A
 *	zero timestamp is sent as the word null.
 *
 * @returns 0 on success
 * @returns ENOMEM if running out of memory.
 * @returns EINVAL if invoked with a NULL handle or text pointer and
 *	non-NULL text size
 */
int graphdb_request_send_pprintf(graphdb_handle *graphdb,
                                 graphdb_request_id *request_id_out,
                                 void *application_data, char const *fmt,
                                 graphdb_arg_popper *popper) {
  graphdb_buffer *buf;
  graphdb_request *req;
  int err = 0;

  if (graphdb == NULL) return EINVAL;
  if (fmt == NULL) {
    graphdb_log(graphdb, CL_LEVEL_ERROR,
                "graphdb_request_send_vprintf: null format string");
    return EINVAL;
  }
  errno = 0;

  /* Allocate a request.	Our code holds one link to the request.
   */
  req = graphdb_request_alloc(graphdb);
  if (req == NULL) {
    err = errno ? errno : ENOMEM;
    graphdb_log(graphdb, CL_LEVEL_ERROR,
                "graphdb_request_send_vprintf: unexpected failure "
                "from graphdb_request_alloc: %s",
                strerror(err));
    return err;
  }

  if (request_id_out != NULL) *request_id_out = req->req_id;

  buf = graphdb_buffer_alloc_heap(graphdb, req->req_heap, 4 * 1024);
  if (buf == NULL) {
    err = errno ? errno : ENOMEM;
    graphdb_request_unlink_req(graphdb, req);
    graphdb_log(graphdb, CL_LEVEL_ERROR,
                "graphdb_request_send_vprintf: unexpected failure "
                "from graphdb_buffer_alloc_heap: %s",
                strerror(err));
    return err;
  }

  if ((err = graphdb_buffer_pformat(graphdb, buf, fmt, popper)) != 0) {
    graphdb_log(graphdb, CL_LEVEL_SPEW,
                "graphdb_request_send_vprintf: error "
                "in graphdb_buffer_pformat: %s; free request %p",
                strerror(err), (void *)req);

    graphdb_request_unlink_req(graphdb, req);
    return err;
  }
  if (graphdb->graphdb_check_syntax &&
      (err = graphdb_request_check_buffer(graphdb, buf)) != 0) {
    graphdb_request_unlink_req(graphdb, req);
    return err;
  }
  req->req_application_data = application_data;

  if ((err = graphdb_request_send_buffer_req(graphdb, req, buf)) != 0) {
    graphdb_log(graphdb, CL_LEVEL_SPEW,
                "graphdb_graphdb_request_send_vprintf: error from "
                "graphdb_request_send_buffer_req: %s; free request %p",
                strerror(err), (void *)req);
    graphdb_request_unlink_req(graphdb, req);
    return err;
  }

  /*  graphdb_request_send_buffer placed the request in the
   *  infrastructure (which holds a link to it now); we
   *  can unlink our pointer.
   */
  graphdb_request_unlink_req(graphdb, req);
  return 0;
}

/**
 * @brief Format and send a graph query, varargs version.
 *
 * @param graphdb		handle created with graphdb_create() and
 *				connected to a server with graphdb_connect().
 * @param request_id_out	assign the request id to this address.
 * @param application_data	attach this application data to the request.
 * @param fmt			format string for request; followed by
 *				arguments for the format sequences in the
 *				string.
 * @param ap			the va_list.
 */
int graphdb_request_send_vprintf(graphdb_handle *graphdb,
                                 graphdb_request_id *request_id_out,
                                 void *application_data, char const *fmt,
                                 va_list ap) {
  graphdb_va_arg_popper popper;
  int err;

  popper.ga_generic.ga_fns = &graphdb_va_arg_popper_fns;
  va_copy(popper.ga_ap, ap);

  err = graphdb_request_send_pprintf(graphdb, request_id_out, application_data,
                                     fmt, (graphdb_arg_popper *)&popper);
  va_end(popper.ga_ap);

  return err;
}

/**
 * @brief Format and send a graph query.
 *
 *  The results are returned via graphdb_request_wait().
 *  For a more high-level synchronous query return mechanism, see
 *  graphdb_query_*().
 *
 * @param graphdb		handle created with graphdb_create() and
 *				connected to a server with graphdb_connect().
 * @param request_id_out	assign the request id to this address.
 * @param application_data	attach this application data to the request.
 * @param fmt			format string for request; followed by
 *			arguments for the format sequences in the string.
 *
 * @par Formats
 * In the format string, literal % must be escaped
 * as %%.  Other than that, the following format
 * sequences have the following meaning:\n
 * @par
 * @b %%q \n
 * The argument, a char const *, is either NULL
 * or points to a '\\0'-terminated string.  NULL is sent as the word null.
 * Everything else is sent as a quoted strings, with contained \\ or "
 * or newline characters properly escaped.\n
 * @par
 * @b %%*q \n
 * Like %%q, but the arguments are a size_t
 * and a char const *, and the string need not be '\\0'-terminated.
 * A NULL string pointer is sent as the word null, regardless of the size_t.
 * @par
 * @b %%s \n
 * Arbitrary text, included literally.
 * The argument is a char const * pointing to a '\\0'-terminated string.
 * A NULL pointer is sent as the word null.
 * @par
 * @b %%*s \n
 * Like %%s, but the arguments are a size_t and a char const *,
 * and the string need not be '\\0'-terminated.
 * A NULL pointer with non-zero size is sent as the word null.
 * @par
 * @b %%g \n
 * A GUID.  The argument is NULL or a graph_guid const *.  Both
 * NULL and a NULL graph guid are sent as the word null.
 * @par
 * @b %%b \n
 * A boolean.  The argument is an int.	If it is zero, the word
 * false is sent; otherwise, the word true.
 * @par
 * @b %%d \n
 * A datatype.	The argument is an int.	 Its value is interpreted
 * as a graph_datatype and sent as a word.
 * @par
 * @b %%u \n
 * An unsigned number.	The argument is an unsigned long long; its
 * numeric value is sent as decimal digits.
 * @par
 * @b %%t \n
 * A time stamp.  The argument is a graph_timestamp_t.	A
 *	zero timestamp is sent as the word null.
 *
 * @returns 0 on success
 * @returns ENOMEM if running out of memory.
 * @returns EINVAL if invoked with a NULL handle or text pointer and
 *	non-NULL text size
 */
int graphdb_request_send_printf(graphdb_handle *graphdb,
                                graphdb_request_id *request_id_out,
                                void *application_data, char const *fmt, ...) {
  int err;
  graphdb_va_arg_popper popper;

  popper.ga_generic.ga_fns = &graphdb_va_arg_popper_fns;
  va_start(popper.ga_ap, fmt);
  err = graphdb_request_send_pprintf(graphdb, request_id_out, application_data,
                                     fmt, (graphdb_arg_popper *)&popper);
  va_end(popper.ga_ap);

  return err;
}

/**
 * @brief Send a request.
 *
 * This sends a command or other piece of text to a server.
 * If the text doesn't end in a newline, one is supplied by the call.
 * The result of sending the request can be obtained by calling
 * graphdb_request_wait().
 *
 * @param graphdb		handle created with graphdb_create() and
 *				connected to a server with graphdb_connect().
 * @param request_id_out	where to store a request id
 * @param application_data	opaque data strored with request
 * @param text			text to send
 * @param text_size		# of bytes pointed to by @b text.
 *
 * @returns 0 on success
 * @returns ENOMEM if running out of memory.
 * @returns EINVAL if invoked with a NULL handle or text pointer and
 *	non-NULL text size
 *
 * If the server is not connected, the request is stored for
 * later transmission.
 */

int graphdb_request_send(graphdb_handle *graphdb,
                         graphdb_request_id *request_id_out,
                         void *application_data, char const *text,
                         size_t text_size) {
  graphdb_request *req;
  graphdb_buffer *buf;
  int queue_was_empty;
  int err;

  if (!GRAPHDB_IS_HANDLE(graphdb) || (text_size > 0 && text == NULL))
    return EINVAL;

  /* Allocate a new request. */
  if ((req = graphdb_request_alloc(graphdb)) == NULL) return ENOMEM;

  /* Buffer the text */
  buf = graphdb_buffer_alloc_heap_text(graphdb, req->req_heap, text, text_size);
  if (buf == NULL) {
    graphdb_log(graphdb, CL_LEVEL_ERROR,
                "graphdb_request_send: failed to allocate "
                "heap text for %p",
                (void *)req);
    graphdb_request_unlink_req(graphdb, req);
    return ENOMEM;
  }

  if (graphdb->graphdb_check_syntax &&
      (err = graphdb_request_check(graphdb, buf->buf_data,
                                   buf->buf_data + buf->buf_data_n)) != 0) {
    graphdb_request_unlink_req(graphdb, req);
    return err;
  }

  req->req_application_data = application_data;
  req->req_out = req->req_out_unsent = buf;

  queue_was_empty = graphdb->graphdb_request_head == NULL;

  graphdb_request_chain_in(graphdb, req);
  *request_id_out = req->req_id;

  /*  graphdb_request_chain_in placed the request in the
   *  infrastructure (which holds a link to it now); we
   *  can unlink our pointer.
   */
  graphdb_request_unlink_req(graphdb, req);

  if (graphdb->graphdb_connected && queue_was_empty)
    (void)graphdb_request_io(graphdb, 0);

  return 0;
}

/**
 * @brief Expand the most recent graphdb_request_send() error code to text.
 *
 *	If the most recent call to graphdb_request_send() returned
 *	an error code, this function, rather than graphdb_strerror()
 *	or strerror(), should be used to get an error message that
 *	pinpoints what was wrong with the request.
 *
 * @param graphdb	opaque module handle
 * @param err		error code returned by graphdb_request_send
 * @param buf		buffer to use while formatting
 * @param buf_size	number of bytes pointed to by buf.
 *
 * @return either the result of graphdb_strerror, or a local syntax
 *	error if there was a syntactical problem with the request.
 */
char const *graphdb_request_send_error(graphdb_handle *graphdb, int err,
                                       char *buf, size_t buf_size) {
  if (err != EINVAL || graphdb == NULL || !graphdb->graphdb_syntax_error)
    return graphdb_strerror(err, buf, buf_size);

  return graphdb->graphdb_syntax_error;
}
