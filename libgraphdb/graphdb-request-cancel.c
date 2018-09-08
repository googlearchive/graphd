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
#include <stdio.h>

#include "libgraphdb/graphdbp.h"

/**
 * @brief Cancel a pending request.
 *
 * The application says it doesn't want to hear about a request.
 * If it hasn't been sent yet, don't send it, just throw it out;
 * but if it @em has been sent, or partially sent, remember to ignore
 * the response.
 *
 * An application must not call graphdb_request_free() after calling
 * graphdb_request_cancel() -- the cancel already implies disposing
 * of the resources allocated for the request, even though that disposal
 * can be delayed.
 *
 * @param graphdb 	handle created with graphdb_create()
 * @param request_id	which request to cancel
 */

void graphdb_request_cancel(graphdb_handle *graphdb,
                            graphdb_request_id request_id) {
  graphdb_request *req;

  if (!GRAPHDB_IS_HANDLE(graphdb)) return;
  if ((req = graphdb_request_lookup(graphdb, request_id)) == NULL) return;

  if (!req->req_started) {
    /* Remove the request from the internal queues. */
    graphdb_request_unlink_req(graphdb, req);

    /* Unlink the request from the application. */
    graphdb_request_unlink_req(graphdb, req);

    return;
  }

  req->req_cancelled = 1;
  graphdb_log(graphdb, CL_LEVEL_SPEW, "cancel request %p [slot id %lu]",
              (void *)req, (unsigned long)req->req_id);

  /* Unlink the request from the application. */
  graphdb_request_unlink_req(graphdb, req);
}
