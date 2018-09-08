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
#ifndef GRAPHD_ITERATOR_ISA_H
#define GRAPHD_ITERATOR_ISA_H

#include "graphd/graphd.h"

#include <stdbool.h>
#include <stdlib.h>

typedef struct graphd_iterator_isa_storable graphd_iterator_isa_storable;

void graphd_iterator_isa_storable_range(graphd_iterator_isa_storable const* _is,
                                        pdb_range_estimate* _range,
                                        size_t _off);

bool graphd_iterator_isa_storable_offset_to_id(
    graphd_iterator_isa_storable* _iss, size_t _position, pdb_id* _id_out);

bool graphd_iterator_isa_storable_id_to_offset(
    graphd_iterator_isa_storable const* _is, pdb_id _id, size_t* _position_out);

size_t graphd_iterator_isa_storable_nelems(
    graphd_iterator_isa_storable const* _iss);

void graphd_iterator_isa_storable_free(graphd_iterator_isa_storable* _iss);

graphd_iterator_isa_storable* graphd_iterator_isa_storable_alloc(
    graphd_handle* _g);

bool graphd_iterator_isa_storable_complete(graphd_iterator_isa_storable* _is);

int graphd_iterator_isa_storable_add(graphd_handle* _g,
                                     graphd_iterator_isa_storable* _iss,
                                     size_t _position, pdb_id _id);

bool graphd_iterator_isa_storable_check(graphd_iterator_isa_storable const* is,
                                        pdb_id id);

graphd_iterator_isa_storable* graphd_iterator_isa_storable_thaw(
    graphd_handle* _g, char const** _s_ptr, char const* _e);

int graphd_iterator_isa_storable_run(graphd_handle* _g, pdb_iterator* _it,
                                     pdb_iterator* _sub, int _linkage,
                                     graphd_iterator_isa_storable* _is,
                                     pdb_budget* _budget_inout);

/* graphd-iterator-isa.c */
int graphd_iterator_isa_run_next(graphd_handle* _g, pdb_iterator* _it,
                                 pdb_iterator* _sub, int _linkage,
                                 size_t* _sub_trials, pdb_id* _id_out,
                                 pdb_budget* _budget_inout, bool _log_rxs);

#endif /* GRAPHD_ITERATOR_ISA_H */
