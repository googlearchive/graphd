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

#include <stdlib.h>
#include <stdio.h>

static const cm_list_offsets graphdb_request_offsets =
    CM_LIST_OFFSET_INIT(graphdb_request, req_next, req_prev);

/*
 *  graphdb_request_chain_in -- (Utility) add request to internal
 * 	list of pending requests.
 *
 *  Parameters:
 *	graphdb -- handle created with graphdb_create(),
 *	req -- request
 */

void graphdb_request_chain_in(graphdb_handle *graphdb, graphdb_request *req) {
  if (req->req_handle != graphdb) {
    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_request_chain_in: attempt "
                "to chain in %p, which isn't a valid"
                " request!",
                (void *)req);
    return;
  }
  if (req->req_chained) {
    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_request_chain_in: attempt "
                "to chain in %p, which is already "
                "chained in!",
                (void *)req);
    return;
  }
  if (!req->req_refcount) {
    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_request_chain_in: attempt "
                "to chain in %p, which doesn't has"
                " any links!",
                (void *)req);
    return;
  }

  graphdb_log(graphdb, CL_LEVEL_SPEW,
              "chain in request %p [slot id %lu]; "
              "head %p, tail %p, unsent %p, unanswered %p",
              (void *)req, (unsigned long)req->req_id,
              (void *)graphdb->graphdb_request_head,
              (void *)graphdb->graphdb_request_tail,
              (void *)graphdb->graphdb_request_unsent,
              (void *)graphdb->graphdb_request_unanswered);

  req->req_refcount++;
  req->req_chained = true;

  cm_list_enqueue(graphdb_request, graphdb_request_offsets,
                  &graphdb->graphdb_request_head,
                  &graphdb->graphdb_request_tail, req);

  if (graphdb->graphdb_request_unanswered == NULL)
    graphdb->graphdb_request_unanswered = req;

  if (graphdb->graphdb_request_unsent == NULL)
    graphdb->graphdb_request_unsent = req;

  graphdb_assert(graphdb, graphdb->graphdb_request_unsent);
  graphdb_assert(graphdb, graphdb->graphdb_request_unanswered);
}

/*
 *  graphdb_request_chain_out -- (Utility) remove request from
 * 				our internal store
 *
 *  The request is removed from the internal queuing mechanism;
 *  but it stays in the ID lookup mechanism, and also keeps its
 *  handle.
 *
 *  Parameters:
 *	graphdb -- handle created with graphdb_create(),
 *	req -- request
 */

void graphdb_request_chain_out(graphdb_handle *graphdb, graphdb_request *req) {
  graphdb_log(graphdb, CL_LEVEL_VERBOSE,
              "graphdb_request_chain_out: req %p [slot id %lu]; "
              "unsent %p, unanswered %p, head %p, tail %p; %zu refs",

              req, (unsigned long)req->req_id, graphdb->graphdb_request_unsent,
              graphdb->graphdb_request_unanswered,
              graphdb->graphdb_request_head, graphdb->graphdb_request_tail,
              req->req_refcount);

  if (req->req_handle != graphdb) {
    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_request_chain_out: attempt "
                "to chain out %p, which isn't a valid"
                " request!",
                (void *)req);
    return;
  }
  if (!req->req_chained) {
    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_request_chain_out: attempt "
                "to chain out %p, which isn't chained"
                " in!",
                (void *)req);
    return;
  }
  if (!req->req_refcount) {
    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_request_chain_out: attempt "
                "to chain out %p, which doesn't have"
                " any links to it!",
                (void *)req);
    return;
  }

  if (req->req_next || req->req_prev || graphdb->graphdb_request_head == req) {
    graphdb_assert(graphdb, graphdb->graphdb_request_head != NULL);
    graphdb_assert(graphdb, graphdb->graphdb_request_tail != NULL);

    if (graphdb->graphdb_request_unanswered == req)
      graphdb->graphdb_request_unanswered = req->req_next;

    if (graphdb->graphdb_request_unsent == req)
      graphdb->graphdb_request_unsent = req->req_next;

    cm_list_remove(graphdb_request, graphdb_request_offsets,
                   &graphdb->graphdb_request_head,
                   &graphdb->graphdb_request_tail, req);
    req->req_chained = false;

    graphdb_request_unlink_req(graphdb, req);
  } else {
    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_request_chain_out: attempt "
                "to chain out %p, which isn't chained in!",
                (void *)req);
  }
}
