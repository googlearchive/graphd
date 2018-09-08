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
#ifndef _GRAPHDB_ARGS_H
#define _GRAPHDB_ARGS_H

#include <stdarg.h>

typedef struct { struct _graphdb_arg_popper_fns *ga_fns; } graphdb_arg_popper;

typedef struct { struct _graphdb_arg_pusher_fns *ga_fns; } graphdb_arg_pusher;

typedef struct {
  graphdb_arg_popper ga_generic;
  va_list ga_ap;
} graphdb_va_arg_popper;

typedef struct {
  graphdb_arg_pusher ga_generic;
  va_list ga_ap;
} graphdb_va_arg_pusher;

/* There are some delicacies around pushing and popping guids since they are
 * structures that like to be passed around by reference instead of by value.
 *
 * Because a guid structure may not be available longer than for the current
 * stackframe, the push-by-ref push_guid() macro expects a guid structure
 * placeholder to which the ptr argument may be set after the stack-local
 * guid is copied into the placeholder argument by value. If the guid being
 * popped is the null guid, setting *ptr to NULL is good enough.
 */

typedef struct _graphdb_arg_popper_fns {
  int (*ga_pop_int)(graphdb_arg_popper *self, int *ptr);
  int (*ga_pop_size)(graphdb_arg_popper *self, size_t *ptr);
  int (*ga_pop_string)(graphdb_arg_popper *self, char const **ptr);
  int (*ga_pop_guid)(graphdb_arg_popper *self, graph_guid **ptr,
                     graph_guid *value);
  int (*ga_pop_timestamp)(graphdb_arg_popper *self, graph_timestamp_t *ptr);
  int (*ga_pop_ull)(graphdb_arg_popper *self, unsigned long long *ptr);
  int (*ga_pop_datatype)(graphdb_arg_popper *self, graph_datatype *ptr);
} graphdb_arg_popper_fns;

typedef struct _graphdb_arg_pusher_fns {
  int (*ga_push_int)(graphdb_arg_pusher *self, int value);
  int (*ga_push_size)(graphdb_arg_pusher *self, size_t value);
  int (*ga_push_string)(graphdb_arg_pusher *self, char const *value);
  int (*ga_push_guid)(graphdb_arg_pusher *self, graph_guid const *value);
  int (*ga_push_timestamp)(graphdb_arg_pusher *self, graph_timestamp_t value);
  int (*ga_push_ull)(graphdb_arg_pusher *self, unsigned long long value);
  int (*ga_push_datatype)(graphdb_arg_pusher *self, graph_datatype value);
  int (*ga_push_iterator)(graphdb_arg_pusher *self, graphdb_iterator *value);
} graphdb_arg_pusher_fns;

extern graphdb_arg_popper_fns graphdb_va_arg_popper_fns;
extern graphdb_arg_pusher_fns graphdb_va_arg_pusher_fns;

#define graphdb_pop_int(self, ptr) (self)->ga_fns->ga_pop_int((self), (ptr))
#define graphdb_pop_size(self, ptr) (self)->ga_fns->ga_pop_size((self), (ptr))
#define graphdb_pop_string(self, ptr) \
  (self)->ga_fns->ga_pop_string((self), (ptr))
#define graphdb_pop_guid(self, ptr, value) \
  (self)->ga_fns->ga_pop_guid((self), (ptr), (value))
#define graphdb_pop_timestamp(self, ptr) \
  (self)->ga_fns->ga_pop_timestamp((self), (ptr))
#define graphdb_pop_ull(self, ptr) (self)->ga_fns->ga_pop_ull((self), (ptr))
#define graphdb_pop_datatype(self, ptr) \
  (self)->ga_fns->ga_pop_datatype((self), (ptr))

#define graphdb_push_int(self, value) \
  (self)->ga_fns->ga_push_int((self), (value))
#define graphdb_push_size(self, value) \
  (self)->ga_fns->ga_push_size((self), (value))
#define graphdb_push_string(self, value) \
  (self)->ga_fns->ga_push_string((self), (value))
#define graphdb_push_guid(self, value) \
  (self)->ga_fns->ga_push_guid((self), &(value))
#define graphdb_push_timestamp(self, value) \
  (self)->ga_fns->ga_push_timestamp((self), (value))
#define graphdb_push_ull(self, value) \
  (self)->ga_fns->ga_push_ull((self), (value))
#define graphdb_push_datatype(self, value) \
  (self)->ga_fns->ga_push_datatype((self), (value))
#define graphdb_push_iterator(self, value) \
  (self)->ga_fns->ga_push_iterator((self), (value))

#endif /* _GRAPHDB_ARGS_H */
