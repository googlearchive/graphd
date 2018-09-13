/*
Copyright 2018 Google Inc. All rights reserved.
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

#ifndef GLD_H
#define GLD_H

#include <curses.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "libcl/cl.h"
#include "libcm/cm.h"
#include "libgraph/graph.h"
#include "libgraphdb/graphdb.h"

extern char const gld_build_version[];

typedef struct gld_request_data {
  graphdb_request_id d_most_recent_id;
  unsigned int d_can_be_empty : 1;
  unsigned int d_sent : 1;

} gld_request_data;

typedef struct gld_primitive {
  graph_guid pr_guid;
  struct gld_primitive *pr_next;
  struct gld_primitive *pr_head;
  struct gld_primitive **pr_tail;

} gld_primitive;

/* Overall system state.
 */
typedef struct gld_handle {
  cl_handle *gld_cl;
  cm_handle *gld_cm;
  graphdb_handle *gld_graphdb;

  /* of gld_request_data */
  cm_hashtable *gld_request;

  cm_hashtable *gld_var;
  long gld_timeout;

  size_t gld_outstanding;
  bool gld_print_answers;

  bool gld_passthrough;

} gld_handle;

/* gld-primitive.c */

void gld_primitive_free(gld_handle *, gld_primitive *);
void gld_primitive_free_contents(gld_handle *, gld_primitive *);
gld_primitive *gld_primitive_alloc(gld_handle *);
void gld_primitive_append(gld_handle *gld, gld_primitive *parent,
                          gld_primitive *child);
size_t gld_primitive_n(gld_handle *, gld_primitive *pr);
gld_primitive *gld_primitive_nth(gld_handle *gld, gld_primitive *pr, size_t n);
void gld_primitive_set_guid(gld_handle *gld, gld_primitive *pr,
                            graph_guid const *guid);
void gld_primitive_set_nil(gld_handle *, gld_primitive *);
int gld_primitive_is_list(gld_handle *, gld_primitive *);

/* gld-request.c */

gld_request_data *gld_request_alloc(gld_handle *_gld, char const *_name_s,
                                    char const *_name_e);

int gld_request_send(gld_handle *_gld, gld_request_data *_id,
                     char const *_request_s, char const *_request_e);

void gld_request_wait(gld_handle *_gld, char const *_name_s,
                      char const *_name_e);

void gld_request_wait_any(gld_handle *);
size_t gld_request_outstanding(gld_handle *);

/* gld-var.c */

gld_primitive *gld_var_create(gld_handle *gld, char const *name_s,
                              char const *name_e);

graph_guid const *gld_var_lookup(gld_handle *gld, char const *name_s,
                                 char const *name_e);

#endif /* GLD_H */
