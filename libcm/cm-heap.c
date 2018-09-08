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

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


/*  cm-heap.c -- Keep a list of all allocated fragments;
 *		 fragments can be freed individually or as a whole.
 */

typedef struct cm_heap_fragment cm_heap_fragment;
typedef struct cm_heap_handle {
  cm_handle heap_handle;
  cm_handle *heap_source;
  cm_heap_fragment *heap_head;
  cm_runtime_statistics heap_cmrts;

} cm_heap_handle;

#define HEAP_HANDLE(cm) ((cm_heap_handle *)(cm))

/*  Eacn chunk of payload data is preceded by 16 bytes of header.
 */
struct cm_heap_fragment {
  cm_heap_fragment *frag_prev;
  cm_heap_fragment *frag_next;
};

/*  Offset table for cm_list..()
 */
static const cm_list_offsets cm_heap_fragment_offsets =
    CM_LIST_OFFSET_INIT(cm_heap_fragment, frag_next, frag_prev);

#define HEAP_FRAGMENT(base) \
  ((cm_heap_fragment *)((char *)(base) - sizeof(cm_heap_fragment)))
#define HEAP_FRAMED_SIZE(size) ((size) + sizeof(cm_heap_fragment))
#define HEAP_PAYLOAD(frag) ((void *)((char *)(frag) + sizeof(cm_heap_fragment)))

static void *cm_heap_alloc_chunk(cm_heap_handle *h, size_t size,
                                 char const *file, int line) {
  cm_heap_fragment *f;

  f = h->heap_source->cm_realloc_loc(h->heap_source, NULL,
                                     HEAP_FRAMED_SIZE(size), file, line);
  if (f != NULL) {
    unsigned long long f_size;

    f->frag_prev = NULL;
    f->frag_next = NULL;

    f_size = cm_fragment_size(h->heap_source, f);

    h->heap_cmrts.cmrts_num_fragments++;
    h->heap_cmrts.cmrts_total_allocs++;
    h->heap_cmrts.cmrts_size += f_size;
    h->heap_cmrts.cmrts_total_size += f_size;

    if (h->heap_cmrts.cmrts_num_fragments > h->heap_cmrts.cmrts_max_fragments)
      h->heap_cmrts.cmrts_max_fragments = h->heap_cmrts.cmrts_num_fragments;

    if (h->heap_cmrts.cmrts_size > h->heap_cmrts.cmrts_max_size)
      h->heap_cmrts.cmrts_max_size = h->heap_cmrts.cmrts_size;
  }
  return f;
}

/*  Allocate, reallocate, or free a fragment of memory.
 */
static void *cm_heap_realloc_loc(cm_handle *cm, void *ptr, size_t size,
                                 char const *file, int line) {
  cm_heap_handle *h = HEAP_HANDLE(cm);

  if (size == 0) {
    if (ptr != NULL) /* pure free */
    {
      cm_heap_fragment *f = HEAP_FRAGMENT(ptr);

      cm_list_remove(cm_heap_fragment, cm_heap_fragment_offsets, &h->heap_head,
                     0, f);

      h->heap_cmrts.cmrts_num_fragments--;
      h->heap_cmrts.cmrts_size -= cm_fragment_size(h->heap_source, f);

      h->heap_source->cm_realloc_loc(h->heap_source, f, 0, file, line);
    }
    ptr = NULL;
  } else if (ptr == NULL) /* pure malloc */
  {
    cm_heap_fragment *f;

    if (!(f = cm_heap_alloc_chunk(h, size, file, line)))
      ptr = NULL;
    else {
      cm_list_push(cm_heap_fragment, cm_heap_fragment_offsets, &h->heap_head, 0,
                   f);
      ptr = HEAP_PAYLOAD(f);
    }
  } else {
    void *tmp;
    unsigned long long f_size;
    cm_heap_fragment *f = HEAP_FRAGMENT(ptr);

    cm_list_remove(cm_heap_fragment, cm_heap_fragment_offsets, &h->heap_head, 0,
                   f);

    f_size = cm_fragment_size(h->heap_source, f);

    h->heap_cmrts.cmrts_size -= f_size;
    h->heap_cmrts.cmrts_total_size -= f_size;

    tmp = h->heap_source->cm_realloc_loc(h->heap_source, f,
                                         HEAP_FRAMED_SIZE(size), file, line);

    h->heap_cmrts.cmrts_total_allocs++;

    if (tmp == NULL) {
      ptr = NULL;
      cm_list_push(cm_heap_fragment, cm_heap_fragment_offsets, &h->heap_head,
                   NULL, f);

      h->heap_cmrts.cmrts_size += f_size;
      h->heap_cmrts.cmrts_total_size += f_size;
    } else {
      cm_list_push(cm_heap_fragment, cm_heap_fragment_offsets, &h->heap_head,
                   NULL, tmp);
      ptr = HEAP_PAYLOAD(tmp);

      f_size = cm_fragment_size(h->heap_source, tmp);

      h->heap_cmrts.cmrts_size += f_size;
      h->heap_cmrts.cmrts_total_size += f_size;

      if (h->heap_cmrts.cmrts_size > h->heap_cmrts.cmrts_max_size)
        h->heap_cmrts.cmrts_max_size = h->heap_cmrts.cmrts_size;
    }
  }

  return ptr;
}

/*  get size of a piece of memory.
 */
static size_t cm_heap_fragment_size(cm_handle *cm, void *ptr) {
  cm_heap_handle *h = HEAP_HANDLE(cm);

  return cm_fragment_size(h->heap_source, HEAP_FRAGMENT(ptr));
}

/*  Get the heap runtime statistics.
 */
static void cm_heap_runtime_statistics_get(cm_handle *cm,
                                           cm_runtime_statistics *cmrts) {
  cm_heap_handle *h = HEAP_HANDLE(cm);

  *cmrts = h->heap_cmrts;
}

/**
 * @brief Create a heap memory context.
 *
 * This context is built on top of an arbitrary other memory context.
 * It keeps its allocated pieces of memory in a chain, allowing
 * those not free()d individually to be free()d collectively when
 * the whole heap is destroyed.
 *
 * Heaps are a conceptually inexpensive way of making sure that all
 * resources allocated for a certain task (e.g. a request or a connection)
 * are being released when the task itself terminates.  It's almost
 * like having a garbage collector.
 *
 * @param source the underlying allocator that gets to do the real work.
 * @return NULL on error, otherwise a heap pointer.
 */
cm_handle *cm_heap(cm_handle *source) {
  cm_heap_handle *h;

  h = cm_talloc(source, cm_heap_handle, 1);
  if (h == NULL) return NULL;
  memset(h, 0, sizeof(*h));

  h->heap_source = source;
  h->heap_handle.cm_realloc_loc = cm_heap_realloc_loc;
  h->heap_handle.cm_fragment_size_ = cm_heap_fragment_size;
  h->heap_handle.cm_runtime_statistics_get_ = cm_heap_runtime_statistics_get;

  return &h->heap_handle;
}

/*  Free the heap data structure and all memory on it that's still unallocated.
 */
void cm_heap_destroy_loc(cm_handle *cm, char const *file, int line) {
  cm_heap_handle *h = HEAP_HANDLE(cm);
  cm_heap_fragment *f;

  /* don't bother managing the heap stats since the whole thing
   * is going away */

  for (f = h->heap_head; f != NULL;) {
    cm_heap_fragment *next = f->frag_next;
    h->heap_source->cm_realloc_loc(h->heap_source, f, 0, file, line);
    f = next;
  }
  h->heap_source->cm_realloc_loc(h->heap_source, h, 0, file, line);
}

/*  Given a heap, return its source.
 */
cm_handle *cm_heap_source(cm_handle const *cm) {
  cm_heap_handle *h = HEAP_HANDLE(cm);
  return h->heap_source;
}
