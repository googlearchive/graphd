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
#include <stdlib.h>
#include <string.h>

static void *cm_c_realloc_loc(cm_handle *cm, void *ptr, size_t size,
                              char const *file, int line) {
  if (size > 0)
    return ptr ? realloc(ptr, size) : malloc(size);
  else if (ptr)
    free(ptr);
  return NULL;
}

static const cm_handle graphdb_default_allocator = {cm_c_realloc_loc};

/**
 *  @brief	allocate a new graphdb library handle
 *  @return 	NULL on allocation error,
 *		otherwise a disconnected graphdb handle good for
 *		use with the other graphdb functions.
 *		The handle must be destroyed with graphdb_destroy().
 *
 *  This is likely to be the first libgraphdb function
 *  a programmer calls.
 *
 *  One process can create an arbitrary number of graphdb_handles
 *  and use them in parallel or sequentially; they're all
 *  independent from each other.
 */
graphdb_handle *graphdb_create(void) {
  graphdb_handle *graphdb;

  if ((graphdb = malloc(sizeof(*graphdb))) == NULL) return NULL;

  memset(graphdb, 0, sizeof(*graphdb));
  graphdb->graphdb_magic = GRAPHDB_MAGIC;
  graphdb->graphdb_fd = -1;
  graphdb->graphdb_app_fd = -1;
  graphdb->graphdb_cm = (cm_handle *)&graphdb_default_allocator;
  graphdb->graphdb_heap = NULL;
  graphdb->graphdb_cl = NULL;
  graphdb->graphdb_vlog = NULL;

  graphdb->graphdb_loglevel = CL_LEVEL_ERROR;

  graphdb->graphdb_address_head = NULL;
  graphdb->graphdb_address_tail = &graphdb->graphdb_address_head;
  graphdb->graphdb_address_current = NULL;
  graphdb->graphdb_address_last = NULL;

  graphdb->graphdb_input_buf = NULL;

  graphdb->graphdb_request_free = NULL;
  graphdb->graphdb_request = NULL;
  graphdb->graphdb_request_head = NULL;
  graphdb->graphdb_request_tail = NULL;
  graphdb->graphdb_request_unanswered = NULL;
  graphdb->graphdb_request_unsent = NULL;
  graphdb->graphdb_check_syntax = true;

  return graphdb;
}
