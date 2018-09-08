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
#include <string.h>

graphdb_buffer *graphdb_buffer_alloc_heap_loc(graphdb_handle *graphdb,
                                              cm_handle *heap,
                                              size_t payload_size,
                                              char const *file, int line) {
  graphdb_buffer *buf;

  /*  Allocate one larger than payload size, to allow
   *  us to later append a newline to the buffer contents
   *  without reallocating the buffer.
   */
  buf = (*heap->cm_realloc_loc)(heap, NULL, sizeof(*buf) + payload_size + 1,
                                file, line);
  if (buf == NULL) return NULL;

  memset(buf, 0, sizeof(*buf));

  buf->buf_heap = heap;

  buf->buf_data = (char *)(buf + 1);
  buf->buf_data_n = buf->buf_data_i = 0;
  buf->buf_data_m = payload_size; /* lie! */
  buf->buf_next = NULL;

  buf->buf_head = NULL;
  buf->buf_tail = &buf->buf_head;
  buf->buf_refcount = 1;

  return buf;
}

graphdb_buffer *graphdb_buffer_alloc_loc(graphdb_handle *graphdb,
                                         graphdb_request_id request_id,
                                         size_t buffer_size, char const *file,
                                         int line) {
  graphdb_request *req;
  int err;

  if ((err = graphdb_initialize(graphdb)) != 0) return NULL;

  req = graphdb_request_lookup(graphdb, request_id);
  if (req == NULL) {
    graphdb_log(graphdb, CL_LEVEL_ERROR,
                "graphdb_buffer_alloc (from %s:%d): unknown request "
                "%lu!",
                file, line, (unsigned long)request_id);
    return NULL;
  }

  return graphdb_buffer_alloc_heap_loc(graphdb, req->req_heap, buffer_size,
                                       file, line);
}

/*
 *  graphdb_buffer_alloc_heap_text_loc -- (Internal Utility)
 *	allocate and seed a buffer.
 *
 * 	When the application passes in a string, we use this
 *	buffer structure to \n-terminate (if needed) and cache
 * 	the string across retries.
 *
 *  Parameters:
 *	graphdb -- handle created with graphdb_create()
 *	heap	-- allocate buffer here
 *	string 	-- copy this string into it.
 *
 *  Returns:
 *	Pointer to the newly allocated buffer.
 */

graphdb_buffer *graphdb_buffer_alloc_heap_text_loc(graphdb_handle *graphdb,
                                                   cm_handle *heap,
                                                   char const *text,
                                                   size_t text_n,
                                                   char const *file, int line) {
  graphdb_buffer *buf;

  if (text == NULL || graphdb_initialize(graphdb)) return NULL;

  buf = graphdb_buffer_alloc_heap_loc(graphdb, heap, text_n + 2, file, line);
  if (buf == NULL) return buf;

  memcpy(buf->buf_data, text, text_n);
  if (text_n == 0 || text[text_n - 1] != '\n') buf->buf_data[text_n++] = '\n';
  buf->buf_data[text_n] = '\0';
  buf->buf_data_n = text_n;

  return buf;
}

/*
 * graphdb_buffer_check -- Internal Utility
 *
 * 	Check a buffer for consistency.
 *
 *  Parameters:
 *	graphdb -- handle created with graphdb_create()
 *	buf	-- buffer to be checked
 *	file    -- caller's __FILE__
 *	line    -- caller's __LINE__
 *
 *  Side Effects:
 *	Logs, fails assertion if buffer has problems.
 */

void graphdb_buffer_check_loc(graphdb_handle *graphdb, graphdb_buffer *buf,
                              char const *file, int line) {
  graphdb_assert_loc(graphdb, buf != NULL, file, line);
  graphdb_assert_loc(graphdb, buf->buf_data_n <= buf->buf_data_m, file, line);
  graphdb_assert_loc(graphdb, buf->buf_data_i <= buf->buf_data_n, file, line);
  graphdb_assert_loc(graphdb, buf->buf_tail != NULL, file, line);

  if (buf->buf_tail != &buf->buf_head)
    graphdb_assert_loc(graphdb, buf->buf_head != NULL, file, line);
}
