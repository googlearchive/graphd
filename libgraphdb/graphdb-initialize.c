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
#include <stdlib.h>
#include <string.h>

/*  grapdhb_initialize() -- (Internal utility) finish initialization once
 * 	the allocator has been installed.
 *
 *  Results:
 *	0 on success, ENOMEM on allocation error.
 *
 *  Timing:
 *	Invoked by anything that does allocations.
 */

int graphdb_initialize(graphdb_handle* graphdb) {
  if (!GRAPHDB_IS_HANDLE(graphdb)) return EINVAL;

  if (graphdb->graphdb_heap == NULL &&
      (graphdb->graphdb_heap = graphdb_heap(graphdb->graphdb_cm)) == NULL)
    return ENOMEM;

  return 0;
}
