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

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "libcm/cm.h"

/*  grapdhb-heap.c -- utilities for minimal cm support
 * 		  without pulling in libcm.a at link-time.
 */

typedef struct cm_heap_fragment cm_heap_fragment;
typedef struct cm_heap_handle {
  cm_handle heap_handle;
  cm_handle *heap_source;
  cm_heap_fragment *heap_head;

} cm_heap_handle;

#define HEAP_HANDLE(cm) ((cm_heap_handle *)(cm))

/*  Eacn chunk of payload data is preceded by 16 bytes of header.
 */
struct cm_heap_fragment {
  cm_heap_fragment *frag_prev;
  cm_heap_fragment *frag_next;
};

static const cm_list_offsets cm_heap_fragment_offsets =
    CM_LIST_OFFSET_INIT(cm_heap_fragment, frag_next, frag_prev);

#define HEAP_FRAGMENT(base) \
  ((cm_heap_fragment *)((char *)(base) - sizeof(cm_heap_fragment)))
#define HEAP_PAYLOAD(frag) ((void *)((char *)(frag) + sizeof(cm_heap_fragment)))
#define HEAP_FRAMED_SIZE(size) ((size) + sizeof(cm_heap_fragment))

static void *cm_heap_alloc_chunk(cm_heap_handle *h, size_t size,
                                 char const *file, int line) {
  cm_heap_fragment *f;

  f = h->heap_source->cm_realloc_loc(h->heap_source, NULL,
                                     HEAP_FRAMED_SIZE(size), file, line);
  if (f != NULL) {
    f->frag_prev = NULL;
    f->frag_next = NULL;
  }
  return f;
}

/*  Allocate, reallocate, or free a fragment of memory.
 */
static void *graphdb_heap_realloc_loc(cm_handle *cm, void *ptr, size_t size,
                                      char const *file, int line) {
  cm_heap_handle *h = HEAP_HANDLE(cm);

  if (size == 0) {
    if (ptr != NULL) /* pure free */
    {
      cm_heap_fragment *f = HEAP_FRAGMENT(ptr);
      cm_list_remove(cm_heap_fragment, cm_heap_fragment_offsets, &h->heap_head,
                     0, f);
      h->heap_source->cm_realloc_loc(h->heap_source, f, 0, file, line);
    }
    ptr = NULL;
  } else if (ptr == NULL) /* pure malloc */
  {
    cm_heap_fragment *f;

    if ((f = cm_heap_alloc_chunk(h, size, file, line)) == NULL)
      ptr = NULL;
    else {
      cm_list_push(cm_heap_fragment, cm_heap_fragment_offsets, &h->heap_head,
                   NULL, f);
      ptr = HEAP_PAYLOAD(f);
    }
  } else {
    void *tmp;
    cm_heap_fragment *f = HEAP_FRAGMENT(ptr);

    cm_list_remove(cm_heap_fragment, cm_heap_fragment_offsets, &h->heap_head,
                   NULL, f);

    tmp = h->heap_source->cm_realloc_loc(h->heap_source, f,
                                         HEAP_FRAMED_SIZE(size), file, line);
    if (tmp == NULL) {
      ptr = NULL;
      cm_list_push(cm_heap_fragment, cm_heap_fragment_offsets, &h->heap_head,
                   NULL, f);
    } else {
      cm_list_push(cm_heap_fragment, cm_heap_fragment_offsets, &h->heap_head,
                   NULL, tmp);
      ptr = HEAP_PAYLOAD(tmp);
    }
  }
  return ptr;
}

/*  Create a heap memory context built on top of an arbitrary other
 *  memory context.
 */
cm_handle *graphdb_heap(cm_handle *source) {
  cm_heap_handle *h;

  h = cm_talloc(source, cm_heap_handle, 1);
  if (h == NULL) return NULL;
  memset(h, 0, sizeof(*h));

  h->heap_source = source;
  h->heap_handle.cm_realloc_loc = graphdb_heap_realloc_loc;
  h->heap_head = NULL;

  return &h->heap_handle;
}

/*  Free the heap data structure and all memory on it that's still unfree'd.
 */
void graphdb_heap_destroy_loc(cm_handle *cm, char const *file, int line) {
  if (cm != NULL) {
    cm_heap_handle *h = HEAP_HANDLE(cm);
    cm_heap_fragment *f;

    while ((f = h->heap_head) != NULL) {
      h->heap_head = f->frag_next;
      (*h->heap_source->cm_realloc_loc)(h->heap_source, f, 0, file, line);
    }
    (*h->heap_source->cm_realloc_loc)(h->heap_source, h, 0, file, line);
  }
}

/** @endcond */
