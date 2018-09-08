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

#include <stdio.h>

/*
 *  graphdb_request_unlink_req -- free resources associated with a request.
 *
 *  Parameters:
 *	graphdb  -- handle created with graphdb_create(),
 *	req      -- request pointer
 *
 */
void graphdb_request_unlink_req(graphdb_handle *graphdb, graphdb_request *req) {
  graphdb_buffer *buf, *next;

  if (req == NULL) return;

  if (graphdb != req->req_handle) {
    graphdb_log(graphdb, CL_LEVEL_FAIL,
                "graphdb_request_unlink_req: "
                "attempt to free %p, "
                "which isn't a valid request!",
                (void *)req);
    return;
  }

  if (req->req_refcount-- > 1) return;

  if (req->req_chained) {
    graphdb_log(graphdb, CL_LEVEL_FAIL,
                "graphdb_request_unlink_req: "
                "attempt to free %p [slot id %lu], "
                "which is still chained in!",
                (void *)req, (unsigned long)req->req_id);
    graphdb_assert(graphdb, !req->req_chained);

    return;
  }

  graphdb_log(graphdb, CL_LEVEL_SPEW,
              "graphdb_request_unlink_req: free %p [slot id %lu]", (void *)req,
              (unsigned long)req->req_id);

  /* Remove the request from the id lookup table. */
  graphdb_assert(graphdb, req->req_id < graphdb->graphdb_request_n);

  /*  The slot now points to the previous start of
   *  the free slot list.
   */
  graphdb->graphdb_request[req->req_id] = (void *)graphdb->graphdb_request_free;

  /*  And the free slot list points to the new slot.
   */
  graphdb->graphdb_request_free =
      (void *)(graphdb->graphdb_request + req->req_id);

  /*  Free the buffers.
   */
  next = req->req_in_head;
  while ((buf = next) != NULL) {
    next = (buf == req->req_in_tail) ? NULL : buf->buf_next;
    graphdb_buffer_free(graphdb, buf);
  }

  req->req_out_unsent = NULL;
  next = req->req_out;
  while ((buf = next) != NULL) {
    next = buf->buf_next;
    graphdb_buffer_free(graphdb, buf);
  }
  graphdb_heap_destroy(req->req_heap);
}

/**
 * @brief 	Free resources associated with a request.
 *
 * Once the application has finished using the reply to a request,
 * it must notify the library that the request is no longer needed
 * and that the memory used to hold the reply state can be released.
 *
 * @param graphdb 		handle created with graphdb_create(),
 * @param request_id	 	request identfier returned by
 *				graphdb_request_send().
 */
void graphdb_request_free(graphdb_handle *graphdb,
                          graphdb_request_id request_id) {
  graphdb_log(graphdb, CL_LEVEL_SPEW, "Application: free request #%lu",
              (unsigned long)request_id);

  if (GRAPHDB_IS_HANDLE(graphdb)) {
    graphdb_request *req;

    req = graphdb_request_lookup(graphdb, request_id);
    if (req == NULL) {
      graphdb_log(graphdb, CL_LEVEL_FAIL,
                  "graphdb_request_free: unknown request #%ld",
                  (long)request_id);
      return;
    }

    if (req->req_chained && req->req_refcount == 1) {
      graphdb_log(graphdb, CL_LEVEL_FAIL,
                  "graphdb_request_free: attempt to remove "
                  "request #%ld (%p) - request still chained in!",
                  (long)request_id, (void *)req);
      return;
    }
    graphdb_request_unlink_req(graphdb, req);
  } else {
    graphdb_log(graphdb, CL_LEVEL_SPEW, "That wasn't a handle (?)");
  }
}
