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
#ifndef GRAPHD_WRITE_H
#define GRAPHD_WRITE_H 1

#include "graphd/graphd.h"

#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

/* graphd-write-annotate-anchor.c */

void graphd_write_annotate_anchor_push(graphd_request* greq,
                                       graphd_constraint* con, int* err_out);

/* graphd-write-annotate-keyed.c */

void graphd_write_annotate_keyed_push(graphd_request* _greq,
                                      graphd_constraint* _con, int* _err_out);

/* graphd-write-annotate-pointed.c */

bool graphd_write_is_keyed_parent_connection(graphd_constraint* _con);

void graphd_write_annotate_guid(graphd_constraint* _con,
                                graph_guid const* _guid);

int graphd_write_annotate_pointed(graphd_request* _greq,
                                  graphd_constraint* _con);

/* graphd-write-check-unique.c */

void graphd_write_check_unique_push(graphd_request* _greq,
                                    graphd_constraint* _con, int* _err_out);

#endif /* GRAPHD_WRITE_H */
