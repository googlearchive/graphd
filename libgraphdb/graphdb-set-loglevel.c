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
#include <syslog.h>

#include "libcl/cl.h"

/**
 * @brief Set the internal loglevel.
 *
 * After the call completes, internal log
 * calls (to the internal graphdb_log() and graphdb_assert() functions)
 * are executed if their log level is at least as important as
 * @b level.
 * (In keeping with tradition, the smaller the integer, the more important
 * the log call.)
 *
 *  @param 	graphdb handle created with graphdb_create()
 *  @param	level 	loglevel; can be #GRAPHDB_LEVEL_DEBUG,
 *			#GRAPHDB_LEVEL_ERROR, one of the
 *			CL_LEVEL_* levels.  Larger numbers are less important.
 *
 * @warning
 * 	If logging happens via a cl_handle, the loglevel in that handle must
 *	be set by the application separately.
 *	The internal loglevel of libgraphdb only controls what calls
 *	are passed on, not whether they get filtered elsewhere.
 */
void graphdb_set_loglevel(graphdb_handle* graphdb, unsigned long level) {
  if (GRAPHDB_IS_HANDLE(graphdb)) {
    graphdb->graphdb_loglevel = level;
  }
}
