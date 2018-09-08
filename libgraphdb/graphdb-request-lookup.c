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

/*
 *  graphdb_request_lookup -- translate ID to request pointer
 *
 * 	This kind of "id" interface strikes me as just a buffer against
 *  	application errors, and I'm a bit ambivalent about using it at all.
 * 	Normally, one might just hand out pointers to abstract objects; or,
 *	as with the handle, use pointers with a magic tag that helps detect
 *	bad references early.
 *
 *  Parameters:
 *	graphdb -- handle created with graphdb_create()
 *	id -- request ID passed to application.
 *
 */
graphdb_request *graphdb_request_lookup(graphdb_handle *graphdb,
                                        graphdb_request_id id) {
  void *ptr;

  if (!GRAPHDB_IS_HANDLE(graphdb)) return NULL;
  if (id >= graphdb->graphdb_request_n) return NULL;

  /*  If the id is in the free list, the pointer at its beginning
   *  points to either NULL (at the end) or to the next slot
   *  in the free list.
   *
   *  If the id isn't in the free list -- that's true for the
   *  valid ids we're looking for here --, the pointer at its
   *  head points to the graphdb handle.
   */
  ptr = graphdb->graphdb_request[id];
  if (ptr != NULL && *(void **)ptr == graphdb) return ptr;

  return NULL;
}
