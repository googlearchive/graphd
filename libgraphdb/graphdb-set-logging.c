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
 *  @brief Install a libcl-style logging interface.
 *
 *  This isn't necessary -- in fact, one doesn't have to link
 *  against libcl to link with libgraphdb -- but if you do happen
 *  to be using the libcl framework, this is how you connect to it.
 *
 *  If you call this function, you need to link against libcl.a.
 *
 *  @param 	graphdb handle created with graphdb_create()
 *  @param	cl 	handle created with cl_create()
 *
 *  Once the call completes, libgraphdb stops logging via its
 *  builtin mechanism (or via any previously installed cl handle),
 *  and starts logging via the cl handle's vlog function.
 *
 *  The log vector can be changed at any time, as long as the
 *  handle is valid.
 *  If multiple threads are using the same graphdb
 *  handle, it is up to the caller to make sure that
 *  they don't interfere with each other.
 */
void graphdb_set_logging(graphdb_handle* graphdb, struct cl_handle* cl) {
  if (!GRAPHDB_IS_HANDLE(graphdb)) return;

  graphdb->graphdb_cl = cl;
  graphdb->graphdb_vlog = cl_vlog; /* this pulls in libcl.a */
}
