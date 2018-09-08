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
#include "libcm/cm.h"
#include <string.h>

void *cm_malcpy_loc(cm_handle *cm, void const *ptr, size_t size,
                    char const *file, int line) {
  void *dup_ptr;

  dup_ptr = (*cm->cm_realloc_loc)(cm, NULL, size + 1, file, line);
  if (dup_ptr != NULL) {
    if (size) memcpy(dup_ptr, ptr, size);
    ((char *)dup_ptr)[size] = '\0';
  }
  return dup_ptr;
}
