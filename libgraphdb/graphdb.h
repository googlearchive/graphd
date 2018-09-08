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
/**
 * @file graphdb.h
 * @brief Execute queries to a graph repository.
 *
 * The libgraphdb C client library lets applications connect to
 * a running graph repository server and execute queries against it.
 *
 * @par Connecting
 * To establish a connection with a server, create a handle
 * with graphdb_create(), then connect to the server with
 * graphdb_connect().
 * @code
 *   graphdb_handle * graphdb;
 *
 *   graphdb = graphdb_create();
 *   if (graphdb == NULL) error ...
 *   graphdb_set_loglevel(graphdb, GRAPHDB_LEVEL_DEBUG);
 *
 *   err = graphdb_connect(graphdb, GRAPHDB_INFINITY, NULL, 0);
 *   if (err != 0) error ...
 * @endcode
 * Between create and connect, the handle
 * can be parameterized in various ways; see graphdb_set_loglevel(),
 * graphdb_set_logging(), graphdb_set_memory(), and
 * graphdb_set_loglevel().
 *
 * @par Shutting down
 * Once the queries have been made, shut down the connection and
 * free the library resources.
 * @code
 *   graphdb_destroy(graphdb);
 * @endcode
 *
 * @par Making queries
 * The easiest way to execute actual requests is to
 * use graphdb_query() to execute a request, and
 * graphdb_query_next() to iterate over its results.
 * @code
 *   graphdb_iterator *it;
 *   err = graphdb_query(graphdb, &it, GRAPHDB_INFINITY,
 *		"read (type=%q result=((value)))", argv[1]);
 *   if (err != 0) error...
 *
 *   graphdb_iterator *elem_it;
 *   err = graphdb_query_next(graphdb, it, "ok (%...)", &elem_it);
 *   if (err != 0) error ...
 *
 *   size_t	     value_n;
 *   char const * value_s;
 *   while (!(err = graphdb_query_next(graphdb,
 *		elem_it, "(%b)", &value_s, &value_n)))
 *	if (value == NULL) puts("*null*");
 *	else printf("\"%.*s\"\n", (int)value_n, value_s);
 * @endcode
 * The query functions implement formatting sequences that
 * allow an application to inject user-supplied strings
 * into queries and to parse results; see the manual entries
 * on graphdb_query() and graphdb_query_next() for details.

 * @par Compiling and Linking
 * You'll need libgraphdb and libgraph.
 * Optionally, the library can be used with cm and cl.
 * @code
 *
 *  # Compile source code against libgraphdb includes:
 *  INCPATH=../..
 *  INCFLAGS=-I$(INCPATH)/libgraphdb	\
 *	-I$(INCPATH)/libgraph		\
 *	-I$(INCPATH)/libcm		\
 *	-I$(INCPATH)/libcl
 *  cc -c $(INCFLAGS) myprog.c
 *
 *  # Link object files against libgraph libraries:
 *  LIBPATH=../..
 *  LIBDIRS=-L$(LIBPATH)/libgraphdb	\
 *	-L$(LIBPATH)/libgraph		\
 *	-L$(LIBPATH)/libcm		\
 *	-L$(LIBPATH)/libcl
 *  LIBFLAGS=-lgraphdb -lgraph -lcm -lcl
 *  cc -o myprog $(LIBDIRS) myprog.o $(LIBFLAGS)
 * @endcode
 *
 */

#ifndef GRAPHDB_H

/**
 * @brief Guard against multiple includes.
 */
#define GRAPHDB_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>  /* size_t */
#include <stdarg.h>  /* va_list */
#include <stdbool.h> /* guess. */
#include "libgraph/graph.h"   /* graph_guid. */

/**
 * @brief Does this compiler support C9X-style preprocessor __VA_ARGS__?
 */
#ifndef GRAPHDB_HAVE_C9X_VA_ARGS
#if __STDC__ >= 199901L || defined(__GNUC__)
#define GRAPHDB_HAVE_C9X_VA_ARGS 1 /* Yes. */
#else
#define GRAPHDB_HAVE_C9X_VA_ARGS 0 /* No.	*/
#endif
#endif

/**
 * @brief CVS information of the most recently checked-in file of the library.
 */
extern char const graphdb_build_version[];

/**
 * @brief Opaque graphdb module state.
 * Allocated with graphdb_create(), free'ed with graphdb_destroy().
 */
typedef struct graphdb_handle graphdb_handle;

/**
 * @brief Opaque graphdb buffer state.
 * Used when efficiently formatting outgoing requests.
 */
typedef struct graphdb_buffer graphdb_buffer;

/**
 * @brief Opaque iterator state.
 * Used when parsing incoming replies.
 */
typedef struct graphdb_iterator graphdb_iterator;

/**
 * @brief Server-side error code (an integer)
 */
typedef unsigned long graphdb_server_error;

/**
 * @brief Internal error inside the graphdb library.
 */
#define GRAPHDB_SERVER_ERROR_INTERNAL ((graphdb_server_error)-1)

/**
 * @brief Server-side error: result set is empty.
 */
#define GRAPHDB_SERVER_ERROR_EMPTY \
  (((((((('e' << 5) ^ 'm') << 5) ^ 'p') << 5) ^ 't') << 5) ^ 'y')

/**
 * @brief Server-side error: trying to create a unique record that exists
 */
#define GRAPHDB_SERVER_ERROR_EXISTS \
  (((((((((('e' << 5) ^ 'x') << 5) ^ 'i') << 5) ^ 's') << 5) ^ 't') << 5) ^ 's')
/**
 * @brief Server-side error: setting replica mode on a non-replica server
 */
#define GRAPHDB_SERVER_ERROR_NOTREPLICA                                     \
  (((((((((((((((((('n' << 5) ^ 'o') << 5) ^ 't') << 5) ^ 'r') << 5) ^ 'e') \
            << 5) ^                                                         \
           'p')                                                             \
          << 5) ^                                                           \
         'l')                                                               \
        << 5) ^                                                             \
       'i')                                                                 \
      << 5) ^                                                               \
     'c')                                                                   \
    << 5) ^                                                                 \
   'a')

/**
 * @brief Default port for a graphd connection.
 */
#define GRAPHDB_DEFAULT_PORT 8100

struct cl_handle;
struct cm_handle;

/**
 * @brief Verbose loglevel.
 * Local syonym for CL_LEVEL_DEBUG from libcl.
 */
#define GRAPHDB_LEVEL_DEBUG 100

/**
 * @brief Normal loglevel.
 * Local syonym for CL_LEVEL_ERROR from libcl.
 */
#define GRAPHDB_LEVEL_ERROR 5

/**
 * @brief Recommended error buffer size.
 */
#define GRAPHDB_ERROR_SIZE 200

/**
 * @brief Recommended transaction ID buffer size.
 */
#define GRAPHDB_TRANSACTION_ID_SIZE 200

/**
 * @brief Recommended server buffer size.
 * Server names can be arbitrarily long (they're truncated to
 * fit into the buffer provided), but we figure that
 * after 200 bytes the reader will have lost interest.
 */
#define GRAPHDB_SERVER_NAME_SIZE 200

/**
 * @brief Input event on a file descriptor.
 */
#define GRAPHDB_INPUT 0x01

/**
 * @brief Output event on a file descriptor.
 */
#define GRAPHDB_OUTPUT 0x02

/**
 * @brief Error event on a file descriptor.
 */
#define GRAPHDB_ERROR 0x04

/**
 * @brief Primitive type: node.
 * Returned in response to the atom or string "node" in a query response
 * when matching %m.
 */
#define GRAPHDB_META_NODE 0 /* node */

/**
 * @brief Primitive type: outgoing link.
 * Returned in response to the atom or string "->" in a query response
 * when matching %m.  (Unless the link query specified incoming links,
 * this is how all links are sent.)
 */
#define GRAPHDB_META_LINK_FROM 1 /* -> */

/**
 * @brief Primitive type: incoming link.
 * Returned in response to the atom or string "<-" in a query response
 * when matching %m.
 */
#define GRAPHDB_META_LINK_TO 2 /* <- */

/**
 * @brief Match all requests.
 * Used with graphdb_request_wait() to wait for
 * any outstanding request.
 */
#define GRAPHDB_REQUEST_ANY ((graphdb_request_id)-1)

/**
 * @brief A timeout that means "no timeout, wait forever."
 * Use with graphdb_connect(), graphdb_request_wait(), graphdb_query(),
 * or any other function that consumes a millisecond timeout, to
 * indicate "no timeout."
 */
#define GRAPHDB_INFINITY (-1L)

/**
 * @brief Refer to a single request.
 * Assigned by the library when a request is sent; can be
 * used to wait for requests, free them, or cancel them.
 */
typedef long graphdb_request_id;

/**
 * @brief Be notified about a reply.
 * If the application registers a reply callback, the function is called
 * when the response to a request arrives.
 * @warning Untested.
 * @param	callback_data	opaque application pointer passed into the
 *				callback registration function
 * @param	graphdb		the graphdb module handle
 * @param	event		outcome of the transmission - 0 for success,
 *				otherwise an error number from errno.h
 * @param	request_data	opaque per-request data passed into
 *				one of the request sending functions
 * @param	reply_text	if @b event is 0, the text of the reply.
 * @param	reply_text_size if @b event is 0, the number of bytes
 *				pointed to by @b reply_text.
 */
typedef void graphdb_reply_callback(void *callback_data,
                                    graphdb_handle *graphdb, int event,
                                    void *request_data,
                                    graphdb_request_id request_id,
                                    char const *reply_text,
                                    size_t reply_text_size);

#include "libgraphdb/graphdb-args.h"

graphdb_handle *graphdb_create(void);
void graphdb_destroy(graphdb_handle *);
void graphdb_set_logging(graphdb_handle *, struct cl_handle *);
void graphdb_set_loglevel(graphdb_handle *, unsigned long);
void graphdb_set_memory(graphdb_handle *, struct cm_handle *);
char const *graphdb_server_name(graphdb_handle *, char *, size_t);
char const *graphdb_strerror(int, char *, size_t);
int graphdb_connect(graphdb_handle *_graphdb, long _timeout_milliseconds,
                    char const *const *_address_vector, int _flags);
void graphdb_set_reply_callback(graphdb_handle *_graphdb,
                                graphdb_reply_callback *_callback,
                                void *_callback_data);

/*  Requests
 */
void graphdb_request_free(graphdb_handle *, graphdb_request_id);
void graphdb_request_cancel(graphdb_handle *, graphdb_request_id);

int graphdb_request_send_pprintf(graphdb_handle *_graphdb,
                                 graphdb_request_id *_request_id_out,
                                 void *_application_data, char const *_fmt,
                                 graphdb_arg_popper *_popper);

int graphdb_request_send_vprintf(graphdb_handle *_graphdb,
                                 graphdb_request_id *_request_id_out,
                                 void *_application_data, char const *_fmt,
                                 va_list _ap);

int graphdb_request_send_printf(graphdb_handle *_graphdb,
                                graphdb_request_id *_request_id_out,
                                void *_application_data, char const *_fmt, ...);

int graphdb_request_send(graphdb_handle *_graphdb, graphdb_request_id *_id_out,
                         void *_application_data, char const *_text,
                         size_t _text_size);

char const *graphdb_request_send_error(graphdb_handle *_graphdb, int _err,
                                       char *_buf, size_t _buf_size);

int graphdb_request_wait(graphdb_handle *_graphdb,
                         graphdb_request_id *_request_id_inout,
                         long _timeout_milliseconds,
                         void **_application_data_out, char const **_text_out,
                         size_t *_text_size_out);

char const *graphdb_transaction_id(graphdb_handle *graphdb, char const *app,
                                   char **dom_buf, unsigned long sequence,
                                   char *buf, size_t size);

#ifdef DOCUMENTATION_GENERATOR_ONLY

/**
 * @brief Return the response to a request as an iterator.
 *
 *  This call can wait for any or a specific request, and returns
 *  its data results.
 *
 *  If request_id_inout is NULL, it waits for any request.
 *  If request_id_inout points to a graphdb_request_id with
 *  value GRAPHDB_REQUEST_ANY, it waits for any request (and returns
 *  that request's id in the pointed-to value.
 *  If request_id_inout points to any other value, the call waits
 *  for that request.  (Other requests are read and enqueued, but
 *  will not be returned.)
 *
 *  Three pieces of data can be returned for a request:
 *
 *  - its ID, which matches the one returned by graphdb_request_send()
 *  and its variants, and which can be used to destroy the request.
 *  This is internally used for managing the request.
 *
 *  - its application data closure, that is, the opaque pointer passed in
 *  with the call to graphdb_request_send().  This is the hook an
 *  application can use to manage its data in connection with the request.
 *
 *  - finally, its result, the actual data sent by the server.	That
 *  data is packaged into an iterator for convenient parsing and scanning.
 *  (If you just want the bytes, see graphdb_iterator_read().)
 *
 *  Any of the addresses used for these items can be NULL, in which
 *  case the item isn't assigned.
 *
 *  The request still exists in the internal data after
 *  graphdb_request_wait_iterator() returns.  It must be destroyed either
 *  by freeing the request itself - with a call to graphdb_request_free()
 *  with the rqeust ID - or by destroying the iterator with
 *  graphdb_iterator_free().
 *
 * @param	_graphdb			the graphdb module handle
 * @param	_request_id_inout	NULL, or the awaited request ID
 * @param	_timeout_millis		-1 or how long to wait in milliseconds
 * @param	_application_data_out	NULL, or where to assign the
 *						request data
 * @param	_it_out			NULL, or where to assign
 *						the request text
 * @return	0 on success
 * @return	ENOENT if there are no outstanding requests
 * @return	ETIMEDOUT if the wait timed out - in particular, if
 *		timeout_millis is 0 and there are no complete responses
 *		in the library's receive buffer.
 * @return	other nonzero errors on system error.
 */
int graphdb_request_wait_iterator(graphdb_handle *_graphdb,
                                  graphdb_request_id *_request_id_inout,
                                  long _timeout_millis,
                                  void **_application_data_out,
                                  graphdb_iterator **_it_out) {}

#else

#define graphdb_request_wait_iterator(a, b, c, d, e) \
  graphdb_request_wait_iterator_loc((a), (b), (c), (d), (e), __FILE__, __LINE__)

int graphdb_request_wait_iterator_loc(graphdb_handle *_graphdb,
                                      graphdb_request_id *_request_id_inout,
                                      long _timeout_millis,
                                      void **_application_data_out,
                                      graphdb_iterator **_it_out,
                                      char const *_file, int _line);

#endif

#ifdef DOCUMENTATION_GENERATOR_ONLY

/*  Calls that generate new iterator contexts and have been
 *  overloaded with macros to pass in __FILE__, __LINE__, in
 *  their unoverloaded forms.
 */

/**
 * @brief Format and send a graph query.
 *
 * This is the "explicit" varargs version of graphdb_query().
 *
 * @param graphdb		module handle
 * @param it_out		location to which an iterator is assigned.
 * @param timeout		timeout in milliseconds; #GRAPHDB_INFINITY
 *				means infinity.
 * @param fmt			format string for request.
 * @param ap			a va_start()ed list of arguments for the
 *				format sequences in @b fmt.
 * @returns 0 on success
 * @returns ENOMEM if running out of memory.
 * @returns EINVAL if invoked with a NULL handle or text pointer and
 *	non-NULL text size
 */
int graphdb_vquery(graphdb_handle *graphdb, graphdb_iterator **it_out,
                   long timeout, char const *fmt, va_list ap) {}

/**
 * @brief Scan results of a graph query.
 * This is the "explicit" varargs version of graphdb_query_next().
 *
 * @param graphdb		module handle
 * @param it			iterator returned by graphdb_query() or
 *				graphdb_vquery().
 * @param fmt			format string for scanning results.
 * @param ap			a va_start()ed list of arguments for the
 *				format sequences in @b fmt.
 *
 * @returns 0 on success
 * @returns ENOENT after encountering EOF at the beginning
 * @returns EILSEQ after a syntax error or EOF in the middle
 * @returns ENOMEM if running out of memory.
 * @returns EINVAL if invoked with a NULL handle or text pointer and
 *	non-NULL text size
 */

int graphdb_query_vnext(graphdb_handle *graphdb, graphdb_iterator *it,
                        char const *fmt, va_list ap) {}

/**
 * @brief Make duplicate of an iterator
 *
 * @param graphdb		module handle
 * @param it			iterator returned by graphdb_*query(),
 *				or graphdb_*query_next().
 * @returns a new query iterator on success, NULL on error.
 */
graphdb_iterator *graphdb_query_dup(graphdb_handle *graphdb,
                                    graphdb_iterator *it) {}

#else /* not DOCUMENTATION_GENERATOR_ONLY */

#define graphdb_pquery(a, b, c, d, e) \
  graphdb_pquery_loc(a, b, c, __FILE__, __LINE__, d, e)

int graphdb_pquery_loc(graphdb_handle *_handle, graphdb_iterator **_it_out,
                       long _timeout_milliseconds, char const *_file, int _line,
                       char const *_fmt, graphdb_arg_popper *_popper);

#define graphdb_vquery(a, b, c, d, e) \
  graphdb_vquery_loc(a, b, c, __FILE__, __LINE__, d, e)

int graphdb_vquery_loc(graphdb_handle *_handle, graphdb_iterator **_it_out,
                       long _timeout_milliseconds, char const *_file, int _line,
                       char const *_fmt, va_list _ap);

#if GRAPHDB_HAVE_C9X_VA_ARGS
#define graphdb_query(a, b, c, ...) \
  graphdb_query_loc(a, b, c, __FILE__, __LINE__, __VA_ARGS__)
#else
int graphdb_query(graphdb_handle *_handle, graphdb_iterator **_it_out,
                  long _timeout_milliseconds, char const *_fmt, ...);
#endif /* GRAPHDB_HAVE_C9X_VA_ARGS */

int graphdb_query_loc(graphdb_handle *_handle, graphdb_iterator **_it_out,
                      long _timeout_milliseconds, char const *_file, int _line,
                      char const *_fmt, ...);

#define graphdb_query_pnext(a, b, c, d) \
  graphdb_query_pnext_loc(a, b, c, d, __FILE__, __LINE__)

int graphdb_query_pnext_loc(graphdb_handle *_graphdb, graphdb_iterator *_it,
                            char const *_fmt, graphdb_arg_pusher *_pusher,
                            char const *_file, int _line);

#define graphdb_query_vnext(a, b, c, d) \
  graphdb_query_vnext_loc(a, b, c, d, __FILE__, __LINE__)

int graphdb_query_vnext_loc(graphdb_handle *_graphdb, graphdb_iterator *_it,
                            char const *_fmt, va_list _ap, char const *_file,
                            int _line);

#if GRAPHDB_HAVE_C9X_VA_ARGS
#define graphdb_query_next(a, b, ...) \
  graphdb_query_next_loc(a, b, __FILE__, __LINE__, __VA_ARGS__)
#else
int graphdb_query_next(graphdb_handle *_handle, graphdb_iterator *_it,
                       char const *_fmt, ...);
#endif /* GRAPHDB_HAVE_C9X_VA_ARGS */

int graphdb_query_next_loc(graphdb_handle *_handle, graphdb_iterator *_it,
                           char const *file, int line, char const *_fmt, ...);

#define graphdb_query_dup(a, b) graphdb_query_dup_loc(a, b, __FILE__, __LINE__)

graphdb_iterator *graphdb_query_dup_loc(graphdb_handle *_graphdb,
                                        graphdb_iterator *_it,
                                        char const *_file, int _line);

#endif /* DOCUMENTATION_GENERATOR_ONLY */

char const *graphdb_query_error(graphdb_handle *_handle, graphdb_iterator *_it,
                                int _err);

void graphdb_query_free(graphdb_handle *_graphdb, graphdb_iterator *_it);

int graphdb_query_read_bytes(graphdb_handle *_graphdb, graphdb_iterator *_it,
                             char const **_s_out, size_t *_n_out);

/*  Event-based interface
 */
int graphdb_descriptor(graphdb_handle *);
int graphdb_descriptor_io(graphdb_handle *, int);
int graphdb_descriptor_events(graphdb_handle *);

/* Iterator
 */

void graphdb_iterator_free(graphdb_handle *_graphdb, graphdb_iterator *_it);

bool graphdb_iterator_eof(graphdb_handle *_graphdb, graphdb_iterator *_it);

char const *graphdb_iterator_string(graphdb_handle *_graphdb,
                                    graphdb_iterator *_it);

int graphdb_iterator_bytes(graphdb_handle *_graphdb, graphdb_iterator *_it,
                           char const **_s_out, char const **_e_out);

int graphdb_iterator_guid(graphdb_handle *_graphdb, graphdb_iterator *_it,
                          graph_guid *_guid_out);

graphdb_iterator *graphdb_iterator_list(graphdb_handle *_graphdb,
                                        graphdb_iterator *_it);

int graphdb_iterator_list_or_null(graphdb_handle *_graphdb,
                                  graphdb_iterator *_it,
                                  graphdb_iterator **_it_out);

graphdb_server_error graphdb_iterator_server_error(graphdb_handle *_graphdb,
                                                   graphdb_iterator *_it);

char const *graphdb_iterator_server_error_string(graphdb_handle *_graphdb,
                                                 graphdb_iterator *_it);

int graphdb_iterator_read(graphdb_handle *_graphdb, graphdb_iterator *_it,
                          char const **_s_out, size_t *_n_out);

/* Server errors
 */

graphdb_server_error graphdb_server_error_hash_bytes(char const *_s,
                                                     char const *_e);

graphdb_server_error graphdb_server_error_hash_literal(char const *_s);

/* Working with buffers. */

#ifdef DOCUMENTATION_GENERATOR_ONLY

/**
 * @brief Allocate a buffer on a given heap.
 *
 * @param graphdb	opaque module pointer
 * @param heap		heap to allocate on
 * @param buffer_size	start size of the buffer
 *
 * @return NULL on error, a non-NULL buffer pointer otherwise.
 */
graphdb_buffer *graphdb_buffer_alloc_heap(graphdb_handle *graphdb,
                                          struct cm_handle *heap,
                                          size_t buffer_size) {}
#else

#define graphdb_buffer_alloc_heap(a, b, c) \
  graphdb_buffer_alloc_heap_loc(a, b, c, __FILE__, __LINE__)
graphdb_buffer *graphdb_buffer_alloc_heap_loc(graphdb_handle *_graphdb,
                                              struct cm_handle *_heap,
                                              size_t _buffer_size,
                                              char const *_file, int _line);
#endif /* DOCUMENTATION_GENERATOR_ONLY */

#ifdef DOCUMENTATION_GENERATOR_ONLY

/**
 * @brief Allocate a buffer for a given request.
 *
 * @param graphdb	opaque module pointer
 * @param request_id	ID of the request to allocate for.
 * @param buffer_size	start size of the buffer
 *
 * @return NULL on error, a non-NULL buffer pointer otherwise.
 */
graphdb_buffer *graphdb_buffer_alloc(graphdb_handle *graphdb,
                                     graphdb_request_id request_id,
                                     size_t buffer_size) {}
#else
#define graphdb_buffer_alloc(a, b, c) \
  graphdb_buffer_alloc_loc(a, b, c, __FILE__, __LINE__)
graphdb_buffer *graphdb_buffer_alloc_loc(graphdb_handle *_graphdb,
                                         graphdb_request_id _request_id,
                                         size_t _buffer_size, char const *_file,
                                         int _line);
#endif /* DOCUMENTATION_GENERATOR_ONLY */

#ifdef DOCUMENTATION_GENERATOR_ONLY

/**
 * @brief Allocate a buffer for a given request.
 *
 *  If the text doesn't end with a newline character,  one
 *  will be appended by the library.
 *
 * @param graphdb	opaque module pointer
 * @param heap		cm_handle to allocate through.
 * @param text		start of the text
 * @param text_n	number of bytes pointed to by text.
 *
 * @return NULL on error, a non-NULL buffer pointer otherwise.
 */
graphdb_buffer *graphdb_buffer_alloc_heap_text(graphdb_handle *graphdb,
                                               struct cm_handle *heap,
                                               char const *text,
                                               size_t text_n) {}

#else
#define graphdb_buffer_alloc_heap_text(a, b, c, d) \
  graphdb_buffer_alloc_heap_text_loc(a, b, c, d, __FILE__, __LINE__)
graphdb_buffer *graphdb_buffer_alloc_heap_text_loc(
    graphdb_handle *_graphdb, struct cm_handle *_heap, char const *_text,
    size_t _text_n, char const *_file, int _line);
#endif /* DOCUMENTATION_GENERATOR_ONLY */

int graphdb_buffer_pformat(graphdb_handle *_graphdb, graphdb_buffer *_buf,
                           char const *_fmt, graphdb_arg_popper *_popper);

int graphdb_buffer_vformat(graphdb_handle *_graphdb, graphdb_buffer *_buf,
                           char const *_fmt, va_list _ap);

int graphdb_buffer_format(graphdb_handle *_graphdb, graphdb_buffer *_buf,
                          char const *_fmt, ...);

int graphdb_request_send_buffer(graphdb_handle *_graphdb,
                                graphdb_request_id *_request_id_out,
                                void *_application_data,
                                graphdb_buffer *_buffer);

void graphdb_buffer_free(graphdb_handle *_graphdb, graphdb_buffer *_buf);

#ifdef __cplusplus
}
#endif

#endif /* GRAPHDB_H */
