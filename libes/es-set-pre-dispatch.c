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
#include "libes/esp.h"

/**
 * @brief Set pre-dispatch call.
 *
 * The pre-dispatch call is called just after the poll()
 * callback has been called, before the events are dispatched
 * to the application.
 *
 * @param es opaque module handle, created with es_create()
 * @param pre_dispatch callback to invoke before each round
 *	of dispatching I/O events.
 * @param pre_dispatch_data opaque application data pointer,
 *	passed to the dispatch callback as first argument.
 */

void es_set_pre_dispatch(es_handle* es, es_iteration_callback* pre_dispatch,
                         void* pre_dispatch_data) {
  es->es_pre_dispatch = pre_dispatch;
  es->es_pre_dispatch_data = pre_dispatch_data;

  cl_cover(es->es_cl);
}
