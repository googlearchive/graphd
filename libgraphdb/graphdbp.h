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
#ifndef DOCUMENTATION_GENERATOR_ONLY

#ifndef GRAPHDBP_H
#define GRAPHDBP_H /* Guard against multiple includes */

#include <stdarg.h>
#include <stdbool.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

/*  Private graphdb headers; private to the C library implementation.
 *  These should not be installed, and you shouldn't have to know
 *  them to be able to use the graphdb library.
 */

#include "libgraphdb/graphdb.h"
#include "libcl/cl.h"
#include "libcm/cm.h"

#define GRAPHDB_MAGIC 0x4f2d3e4f
#define GRAPHDB_RECONNECT_WAIT_SECONDS (60)
#define GRAPHDB_REQUEST_RETRIES 1

/*  Tokenizer state
 */
typedef struct graphdb_tokenizer {
  int tok_state;
  unsigned char tok_char_class;

  char *tok_buf;
  size_t tok_buf_n, tok_buf_m;

  /* If tok_lookahead is set, we have a lookahead token in tok_s..tok_e.
   */
  int tok_lookahead;
  char const *tok_s;
  char const *tok_e;

} graphdb_tokenizer;

/*  Buffer for passing around data that doesn't need to be copied.
 */
struct graphdb_buffer {
  struct cm_handle *buf_heap;

  /* Next pointer in buffers that, together, make up a stream. */
  struct graphdb_buffer *buf_next;

  size_t buf_refcount;
  char *buf_data;

  /* Current read pointer.  Start reading here. */
  size_t buf_data_i;

  /* Write pointer.  Start writing here; finish reading here. */
  size_t buf_data_n;

  /*  Total number of allocated bytes pointed to by <buf_data.> */
  size_t buf_data_m;

  /*  If the buffer has grown into a chain, the chain is kept here.
   */
  struct graphdb_buffer *buf_head;
  struct graphdb_buffer **buf_tail;
};

typedef struct graphdb_request graphdb_request;
struct graphdb_request {
  /*  The handle must be first; it prevents us from
   *  confusing the free list with real requests.
   */
  void *req_handle;

  graphdb_request *req_next;
  graphdb_request *req_prev;

  struct cm_handle *req_heap;
  graphdb_request_id req_id;

  void *req_application_data;

  struct graphdb_buffer *req_out;
  struct graphdb_buffer *req_out_unsent;

  size_t req_in_head_i;
  struct graphdb_buffer *req_in_head;
  struct graphdb_buffer *req_in_tail;
  size_t req_in_tail_n;

  /*  If the application asked for the reply to a request,
   *  it is returned as one large string; this is where that
   *  string is buffered.  (It is also linked to req_heap.)
   */
  char *req_in_text;

  unsigned int req_answered : 1;
  unsigned int req_sent : 1;
  unsigned int req_cancelled : 1;
  unsigned int req_started : 1;
  unsigned int req_chained : 1;

  int req_errno;

  /*  Number of references to the request.   Typically
   *  between 1 and 2 -- one from the iterator, one from
   *  the I/O mechanism.
   */
  size_t req_refcount;

  /*  Number of resends we do if the server connection
   *  keeps crashing on us.
   */
  unsigned int req_retries;
};

/*  Iterator, marks position in a buffer.
 */
struct graphdb_iterator {
  graphdb_request *it_request;

  /* During scanning, the buffer and offset we're currently reading at.
   */
  graphdb_buffer *it_buffer;
  size_t it_offset;

  /*  During scanning, the tokenizer state.
   */
  graphdb_tokenizer it_tokenizer;

  /*  Iterators nest; the request gets free'ed when the
   *  last link on a parentless iterator is dropped.
   */
  struct graphdb_iterator *it_parent;
  unsigned int it_refcount;

  /*  How deeply nested are we?  Add one for each (, sub one for each ).
   *  Negative depth -> return EOF.
   */
  size_t it_depth;

  /*  Error number and -text for scanner errors.
   */
  int it_error_number;
  char const *it_error_text;
};

typedef struct graphdb_address {
  char *addr_display_name;
  struct graphdb_address *addr_next;
  enum {
    GRAPHDB_ADDRESS_UNSPECIFIED = 0,
    GRAPHDB_ADDRESS_TCP = 1,
    GRAPHDB_ADDRESS_LOCAL = 2
  } addr_type;
  union {
    struct sockaddr_in data_tcp_sockaddr_in;
    char const *data_local_path;
  } addr_data;

#define addr_tcp_sockaddr_in addr_data.data_tcp_sockaddr_in
#define addr_local_path addr_data.data_local_path

} graphdb_address;

struct graphdb_handle {
  unsigned long graphdb_magic;

  unsigned int graphdb_syslog_open : 1;
  cl_loglevel graphdb_loglevel;
  struct cl_handle *graphdb_cl;
  void (*graphdb_vlog)(cl_handle *, cl_loglevel, char const *, va_list);

  struct cm_handle *graphdb_cm;
  struct cm_handle *graphdb_heap;

  /*  The file descriptor we're connecting on, what the application
   *  thinks that file descriptor is, and whether we're connected.
   */
  int graphdb_fd;
  int graphdb_app_fd;
  unsigned int graphdb_connected : 1;

  /* While we're (re)connecting, save the most recent actual error.
   */
  int graphdb_connect_errno;

  /*  Singly linked list (via addr_next) of resolved server addresses.
   *  graphdb_address_current is the one we're either connected to
   *  or most recently tried to connect to; graphdb-connect.c
   *  updates it.
   */
  graphdb_address *graphdb_address_head;
  graphdb_address **graphdb_address_tail;
  graphdb_address const *graphdb_address_current;
  graphdb_address const *graphdb_address_last;

  /*  The number of resolved server addresses, and the
   *  number of retries left.
   */
  size_t graphdb_address_n;
  size_t graphdb_address_retries;

  unsigned int graphdb_input_state;
  graphdb_buffer *graphdb_input_buf;

  /*  graphdb_request points to graphdb_request_n used pointers
   *  within graphdb_request_m allocated pointers.  Within the
   *  first graphdb_request_n entries, free slots are linked via
   *  a singly linked list starting at graphdb_request_free;
   *  the <next> pointer is the slot value itself.
   *
   *  (The free slot values don't survive reallocation of
   *  graphdb_request, but if there are free slot values,
   *  graphdb_request doesn't *need* to be reallocated!)
   *
   *  The linked thing is just the slot (i.e., a request_id);
   *  the request data itself is free'd.
   */
  void **graphdb_request_free;
  void **graphdb_request;
  size_t graphdb_request_n;
  size_t graphdb_request_m;

  /*  Doubly linked list of active requests, connecting via
   *  req_prev, req_next.  If graphdb_request_head is NULL,
   *  there are no pending requests in the system.
   */
  graphdb_request *graphdb_request_head;
  graphdb_request *graphdb_request_tail;

  /*  Pointers into that list to first unanswered, first unsent.
   *
   *  An unanswered request may be partially answered.
   *  An unsent request is completely unsent - once the first
   *  byte has been sent, it is "started" and no longer unsent.
   */
  graphdb_request *graphdb_request_unanswered;
  graphdb_request *graphdb_request_unsent;

  /*  The application's optional asynchronous reply callback and data.
   */
  graphdb_reply_callback *graphdb_app_reply_callback;
  void *graphdb_app_reply_callback_data;

  /*  If  set, check the request syntax of outgoing whole
   *  requests to ensure we don't get hung up on a missing " or ).
   */
  unsigned int graphdb_check_syntax : 1;

  char const *graphdb_syntax_error;
};

#define GRAPHDB_IS_HANDLE(h) ((h) && (h)->graphdb_magic == GRAPHDB_MAGIC)

graphdb_request *graphdb_request_alloc(graphdb_handle *);
graphdb_request *graphdb_request_lookup(graphdb_handle *, graphdb_request_id);
void graphdb_request_chain_out(graphdb_handle *, graphdb_request *);
void graphdb_request_chain_in(graphdb_handle *, graphdb_request *);

/* graphdb-address.c */

int graphdb_address_connect(graphdb_handle *_graphdb,
                            graphdb_address const *_addr, int _fd);

int graphdb_address_set_nonblocking(graphdb_handle *_graphdb,
                                    graphdb_address const *_addr, int _fd);

int graphdb_address_set_nodelay(graphdb_handle *_graphdb,
                                graphdb_address const *_addr, int _fd);

int graphdb_address_socket(graphdb_handle *_graphdb,
                           graphdb_address const *_addr);

int graphdb_address_resolve(graphdb_handle *_graphdb, long long _deadline,
                            char const *_text, graphdb_address **_addr_out);

/* graphdb-heap.c */

struct cm_handle *graphdb_heap(struct cm_handle *);
#define graphdb_heap_destroy(h) \
  graphdb_heap_destroy_loc((h), __FILE__, __LINE__)
void graphdb_heap_destroy_loc(struct cm_handle *_handle, char const *_file,
                              int _line);

/* graphdb-log.c */

void graphdb_log(graphdb_handle *_graphdb, cl_loglevel _lev, char const *_fmt,
                 ...) __attribute__((format(printf, 3, 4)));

#define graphdb_assert_loc(graphdb, expr, file, line)                      \
  ((expr) ||                                                               \
   (graphdb_log(graphdb, CL_LEVEL_FATAL, "%s:%d: assertion fails: \"%s\"", \
                file, line, #expr),                                        \
    abort(), 0))

#define graphdb_assert(graphdb, expr)                                      \
  ((expr) ||                                                               \
   (graphdb_log(graphdb, CL_LEVEL_FATAL, "%s:%d: assertion fails: \"%s\"", \
                __FILE__, __LINE__, #expr),                                \
    abort(), 0))

#define graphdb_notreached(graphdb, ...)                                    \
  (graphdb_log(graphdb, CL_LEVEL_FATAL, __VA_ARGS__),                       \
   graphdb_log(graphdb, CL_LEVEL_FATAL, "%s:%d: unexpected state -- abort", \
               __FILE__, __LINE__),                                         \
   abort())

/* graphdb-buffer-alloc.c */

#define graphdb_buffer_check(a, b) \
  graphdb_buffer_check_loc(a, b, __FILE__, __LINE__)
void graphdb_buffer_check_loc(graphdb_handle *_graphdb, graphdb_buffer *_buf,
                              char const *_file, int _line);

/* graphdb-buffer-format.c */

void graphdb_buffer_format_dwim(graphdb_handle *_graphdb, graphdb_buffer *_buf);

/* graphdb-connect.c */

int graphdb_connect_reconnect(graphdb_handle *, long long);

/* graphdb-connection-drop.c */

void graphdb_connection_drop_reconnects(graphdb_handle *);
void graphdb_connection_drop(graphdb_handle *_graphdb, graphdb_request *_req,
                             char const *_why, int _why_errno);

/* graphdb-buffer-dup.c */

graphdb_buffer *graphdb_buffer_dup(graphdb_handle *_graphdb,
                                   graphdb_buffer *_buffer);

/* graphdb-initialize.c */

int graphdb_initialize(graphdb_handle *);

/* graphdb-iterator.c */

#define graphdb_iterator_alloc(req, parent) \
  graphdb_iterator_alloc_loc(req, parent, __FILE__, __LINE__)

graphdb_iterator *graphdb_iterator_alloc_loc(graphdb_request *_req,
                                             graphdb_iterator *_it_parent,
                                             char const *_file, int _line);

void graphdb_iterator_free(graphdb_handle *_graphdb, graphdb_iterator *_it);

int graphdb_iterator_token(graphdb_handle *_graphdb, graphdb_iterator *_it,
                           char const **_tok_s_out, char const **_tok_e_out);

int graphdb_iterator_token_skip(graphdb_handle *_graphdb,
                                graphdb_iterator *_it);

int graphdb_iterator_peek(graphdb_handle *_graphdb,
                          graphdb_iterator const *_it);

void graphdb_iterator_token_unget(graphdb_handle *_graphdb,
                                  graphdb_iterator *_it, int _tok,
                                  char const *_tok_s, char const *_tok_e);

void graphdb_iterator_error_set(graphdb_handle *_graphdb, graphdb_iterator *_it,
                                int _err, char const *_fmt, ...);

/* graphdb-reconnect-async.c */

void graphdb_reconnect_success(graphdb_handle *);
bool graphdb_reconnect_address(graphdb_handle *);
int graphdb_reconnect_async_io(graphdb_handle *_graphdb);
int graphdb_reconnect_async(graphdb_handle *_graphdb);

/* graphdb-request-free.c */

void graphdb_request_unlink_req(graphdb_handle *_graphdb,
                                graphdb_request *_req);

/* graphdb-request-io.c */

bool graphdb_request_io_want_input(graphdb_handle *);
int graphdb_request_io_write(graphdb_handle *);
int graphdb_request_io_read(graphdb_handle *);
int graphdb_request_io(graphdb_handle *, long long);

/* graphdb-request-lookup.c */

graphdb_request *graphdb_request_lookup(graphdb_handle *, graphdb_request_id);

/* graphdb-request-send.c */

int graphdb_request_send_buffer_req(graphdb_handle *_graphdb,
                                    graphdb_request *_req,
                                    graphdb_buffer *_buf);

int graphdb_request_check(graphdb_handle *_graphdb, char const *_s,
                          char const *_e);

/* graphdb-request-wait.c */

int graphdb_request_wait_req(graphdb_handle *_graphdb,
                             graphdb_request **_request_inout,
                             long long _deadline);

/* graphdb-time.c */

unsigned long long graphdb_time_millis(void);

/* graphdb-token.c */

void graphdb_token_initialize(graphdb_tokenizer *);

#define GRAPHDB_TOKENIZE_EOF -1
#define GRAPHDB_TOKENIZE_MORE -2
#define GRAPHDB_TOKENIZE_ERROR_MEMORY -3

int graphdb_token_is_atom(char const *, char const *);
int graphdb_token(graphdb_handle *_graphdb, graphdb_tokenizer *_state,
                  cm_handle *_heap, char const **_s, char const *_e,
                  char const **_tok_s_out, char const **_tok_e_out);

int graphdb_token_peek(graphdb_handle *_graphdb,
                       graphdb_tokenizer const *_state, char const *_s,
                       char const *_e);

void graphdb_token_unget(graphdb_handle *_graphdb, graphdb_tokenizer *_state,
                         int _tok, char const *_tok_s, char const *_tok_e);

char const *graphdb_token_atom_end(char const *);

int graphdb_token_skip(graphdb_handle *_graphdb, graphdb_tokenizer *_state,
                       char const **_s, char const *_e);

#endif /* GRAPHDBP_H */

#endif /* DOCUMENTATION_GENERATOR_ONLY */
