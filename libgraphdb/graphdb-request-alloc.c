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

/*
 *  graphdb_request_alloc -- (Utility) allocate a request structure
 *
 *  Parameters:
 *	graphdb 	-- handle created with graphdb_create()
 *	request_id_out  -- return the request id here
 * 	request_out	-- return the request record here.
 */
graphdb_request *graphdb_request_alloc(graphdb_handle *graphdb) {
  void **slot;
  graphdb_request *req;
  cm_handle *heap;

  if (!GRAPHDB_IS_HANDLE(graphdb)) return NULL;

  if ((heap = graphdb_heap(graphdb->graphdb_cm)) == NULL) return NULL;
  if ((req = cm_talloc(heap, graphdb_request, 1)) == NULL) {
    graphdb_heap_destroy(heap);
    return NULL;
  }
  memset(req, 0, sizeof(*req));

  req->req_handle = graphdb;
  req->req_heap = heap;
  req->req_next = NULL;
  req->req_prev = NULL;

  req->req_application_data = NULL;

  req->req_out_unsent = NULL;
  req->req_out = NULL;
  req->req_in_head = NULL;
  req->req_in_tail = NULL;
  req->req_in_text = NULL;
  req->req_refcount = 1;
  req->req_errno = 0;
  req->req_retries = GRAPHDB_REQUEST_RETRIES; /* ~3 */
  req->req_chained = false;

  /* Take a slot out of the free list.
   */
  if (graphdb->graphdb_request_free != NULL) {
    slot = graphdb->graphdb_request_free;
    graphdb->graphdb_request_free = *slot;
  } else {
    void **tmp;

    /* Grow the request array, if needed.
     */
    if (graphdb->graphdb_request_n >= graphdb->graphdb_request_m) {
      tmp = cm_trealloc(graphdb->graphdb_cm, void *, graphdb->graphdb_request,
                        graphdb->graphdb_request_m + 64);
      if (tmp == NULL) {
        graphdb_heap_destroy(heap);
        return NULL;
      }
      graphdb->graphdb_request = tmp;
      graphdb->graphdb_request_m += 64;
    }
    slot = graphdb->graphdb_request + graphdb->graphdb_request_n++;
  }

  req->req_id = slot - graphdb->graphdb_request;
  *slot = req;

  graphdb_log(graphdb, CL_LEVEL_SPEW, "new request %p [slot id %lu]",
              (void *)req, (unsigned long)req->req_id);

  return req;
}
