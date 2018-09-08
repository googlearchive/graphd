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

/**
 * @brief Create a link to a buffer.
 *
 *  Each buffer has a reference count.  Calling this function
 *  increments that reference count as a side effect.
 *  Antonym: graphdb_buffer_free().
 *
 * @param graphd	opaque module handle created with graphdb_create()
 * @param buffer	buffer to add a link to.
 *
 * @return the unchanged second argument.
 */
graphdb_buffer* graphdb_buffer_dup(graphdb_handle* graphdb,
                                   graphdb_buffer* buffer) {
  if (buffer != NULL) buffer->buf_refcount++;
  return buffer;
}
