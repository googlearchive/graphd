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
#include <errno.h>

/**
 * @brief Shut down the graphdb module.
 * @param graphdb 	NULL or a handle created with graphdb_create().
 *
 * Shuts down the connection to a graph database (if any) and
 * frees all resources associated with the graph database.
 * The graphdb handle becomes invalid after the call.
 */

void graphdb_destroy(graphdb_handle *graphdb) {
  if (graphdb != NULL) {
    graphdb_address *addr, *addr_next;

    if (!GRAPHDB_IS_HANDLE(graphdb)) return;

    graphdb_log(graphdb, CL_LEVEL_SPEW,
                "graphdb_destroy: freeing request queue (head=%p)",
                (void *)graphdb->graphdb_request_head);

    /* Free all requests. */
    while (graphdb->graphdb_request_head != NULL) {
      graphdb_assert(graphdb,
                     graphdb->graphdb_request_head->req_handle == graphdb);
      graphdb_request_chain_out(graphdb, graphdb->graphdb_request_head);
    }
    graphdb_assert(graphdb, graphdb->graphdb_request_unanswered == NULL);
    graphdb_assert(graphdb, graphdb->graphdb_request_unsent == NULL);

    if (graphdb->graphdb_request_m > 0)
      cm_free(graphdb->graphdb_cm, graphdb->graphdb_request);

    graphdb_connection_drop(graphdb, NULL, "graphdb_destroy", ECANCELED);
    /* XXX free requests, connections, ... */

    if (graphdb->graphdb_input_buf != NULL) {
      graphdb_buffer_free(graphdb, graphdb->graphdb_input_buf);
      graphdb->graphdb_input_buf = NULL;
    }

    addr_next = graphdb->graphdb_address_head;
    while ((addr = addr_next) != NULL) {
      addr_next = addr->addr_next;
      cm_free(graphdb->graphdb_heap, addr);
    }

    graphdb_heap_destroy(graphdb->graphdb_heap);
    free(graphdb);
  }
}
