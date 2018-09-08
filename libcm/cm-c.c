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

#if __APPLE__

#include <malloc/malloc.h>
#define CM_PEEK_MEM_SIZE(ptr) malloc_size(ptr)

#elif __linux__

#include <malloc.h>
#define CM_PEEK_MEM_SIZE(ptr) malloc_usable_size(ptr)

#else
#define CM_PEEK_MEM_SIZE(ptr) (0)
#endif

static size_t cm_c_fragment_size(cm_handle *cm, void *ptr) {
  return CM_PEEK_MEM_SIZE(ptr);
}

static void cm_c_runtime_statistics_get(cm_handle *cm,
                                        cm_runtime_statistics *cmrts) {
  /* this allocator couldn't care less about stats */

  memset(cmrts, 0, sizeof(*cmrts));
}

static const cm_handle mem_c = {cm_c_realloc_loc, cm_c_fragment_size,
                                cm_c_runtime_statistics_get};

cm_handle *cm_c(void) { return (cm_handle *)&mem_c; }
