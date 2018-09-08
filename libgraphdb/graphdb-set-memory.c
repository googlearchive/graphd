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

/**
 * @brief Install a libcm.a-style allocation interface.
 *
 *  This isn't necessary -- in fact, one doesn't have to link
 *  against libcm to link with libgraphdb -- but if you do happen
 *  to be using the libcm framework, this is how you connect to it.
 *
 * @param graphdb 	handle created with graphdb_create().
 * @param cm 		handle created with one of the cm_*() constructors.
 *
 * Once the call returns, libgraphdb will begin allocating all its
 * storage via cm.
 *
 * @warning
 *	This function must be called immediately after
 *	graphdb_create(), in order to not free objects via
 *	contexts that they weren't allocated in.
 *
 *  	If multiple threads are using the same graphdb
 * 	handle, it is up to the caller to make sure that
 * 	they don't interfere with each other.
 */
void graphdb_set_memory(graphdb_handle* graphdb, struct cm_handle* cm) {
  if (!GRAPHDB_IS_HANDLE(graphdb)) return;
  graphdb->graphdb_cm = cm;
}
