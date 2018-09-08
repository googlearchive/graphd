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
 * @brief Free a buffer.
 *
 *  Buffers are linkcounted.  This call decrements the linkcount
 *  by one and frees the buffer only if the link count either was
 *  0 or has just dropped to 0.
 *
 *  Buffers are created with a linkcount of 1; their linkcounts
 *  can explicitly be increased by calling graphdb_buffer_dup(),
 *  or implicitly by using the buffer with a request.
 *
 * @param graphdb	opaque module handle
 * @param buffer	buffer to be free'd.
 */
void graphdb_buffer_free(graphdb_handle* graphdb, graphdb_buffer* buffer) {
  if (buffer != NULL && buffer->buf_refcount-- <= 1)
    cm_free(buffer->buf_heap, buffer);
}
