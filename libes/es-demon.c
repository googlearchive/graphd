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

#include <errno.h>
#include <stdlib.h>

/**
 * @brief Declare a descriptor to be a demon.
 *
 *  Being a demon means that the descriptor doesn't count
 *  when answering the question: "Are we still answering requests?"
 *
 * @param es opaque module handle, created with es_create()
 * @param ed descriptor structure
 * @param value true or false: is this a demon?
 */

void es_demon(es_handle* es, es_descriptor* ed, bool value) {
  if (es == NULL || ed == NULL || !ed->ed_demon == !value) return;

  ed->ed_demon = !!value;
  if (ed->ed_demon)
    es->es_demon_n++;
  else
    es->es_demon_n--;
}
