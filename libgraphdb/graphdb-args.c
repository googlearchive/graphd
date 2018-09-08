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
#include "libgraphdb/graphdb.h"

#include <errno.h>

#define DEFINE_POPPER(name, type)                                       \
  static int va_arg_pop_##name(graphdb_arg_popper *self, type *value) { \
    if (value == NULL) return EINVAL;                                   \
    *value = va_arg(((graphdb_va_arg_popper *)self)->ga_ap, type);      \
    return 0;                                                           \
  }

#define DEFINE_POPPER_BY_REF(name, type)                             \
  static int va_arg_pop_##name(graphdb_arg_popper *self, type **ptr, \
                               type *value) {                        \
    if (ptr == NULL) return EINVAL;                                  \
    *ptr = va_arg((((graphdb_va_arg_popper *)self)->ga_ap), type *); \
    return 0;                                                        \
  }

DEFINE_POPPER(int, int);
DEFINE_POPPER(size, size_t);
DEFINE_POPPER(string, char const *);
DEFINE_POPPER_BY_REF(guid, graph_guid);
DEFINE_POPPER(timestamp, graph_timestamp_t);
DEFINE_POPPER(ull, unsigned long long);
DEFINE_POPPER(datatype, graph_datatype);

graphdb_arg_popper_fns graphdb_va_arg_popper_fns = {
    .ga_pop_int = va_arg_pop_int,
    .ga_pop_size = va_arg_pop_size,
    .ga_pop_string = va_arg_pop_string,
    .ga_pop_guid = va_arg_pop_guid,
    .ga_pop_timestamp = va_arg_pop_timestamp,
    .ga_pop_ull = va_arg_pop_ull,
    .ga_pop_datatype = va_arg_pop_datatype,
};

#define DEFINE_PUSHER(name, type)                                       \
  static int va_arg_push_##name(graphdb_arg_pusher *self, type value) { \
    type *ptr = va_arg(((graphdb_va_arg_pusher *)self)->ga_ap, type *); \
    if (ptr == NULL) return EINVAL;                                     \
    *ptr = value;                                                       \
    return 0;                                                           \
  }

#define DEFINE_PUSHER_BY_REF(name, type)                                       \
  static int va_arg_push_##name(graphdb_arg_pusher *self, type const *value) { \
    type *ptr = va_arg(((graphdb_va_arg_pusher *)self)->ga_ap, type *);        \
    if (ptr == NULL) return EINVAL;                                            \
    *ptr = *value;                                                             \
    return 0;                                                                  \
  }

DEFINE_PUSHER(int, int);
DEFINE_PUSHER(size, size_t);
DEFINE_PUSHER(string, char const *);
DEFINE_PUSHER_BY_REF(guid, graph_guid);
DEFINE_PUSHER(timestamp, graph_timestamp_t);
DEFINE_PUSHER(ull, unsigned long long);
DEFINE_PUSHER(datatype, graph_datatype);
DEFINE_PUSHER(iterator, graphdb_iterator *);

graphdb_arg_pusher_fns graphdb_va_arg_pusher_fns = {
    .ga_push_int = va_arg_push_int,
    .ga_push_size = va_arg_push_size,
    .ga_push_string = va_arg_push_string,
    .ga_push_guid = va_arg_push_guid,
    .ga_push_timestamp = va_arg_push_timestamp,
    .ga_push_ull = va_arg_push_ull,
    .ga_push_datatype = va_arg_push_datatype,
    .ga_push_iterator = va_arg_push_iterator,
};
