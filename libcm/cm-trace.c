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
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#include "libcm/cm.h"

/*  Keep a list of all allocated fragments; embed each piece of
 *  payload in sand traps.
 *
 *  Services built on top of that:
 *
 *	- Overall allocated memory tracking
 *	- Consistency/overrun checks
 *	- Allocation/deallocation logging
 */

typedef struct cm_trace_fragment cm_trace_fragment;
typedef struct cm_trace_handle {
  cm_handle mt_handle;
  cm_handle *mt_source;
  cm_trace_fragment *mt_head;

  void (*mt_log)(void *, int, char const *, ...);
  void *mt_log_data;

  cm_runtime_statistics mt_rts;

} cm_trace_handle;

#define MT_HANDLE(cm) ((cm_trace_handle *)(cm))

/*  Eacn chunk of payload data is preceded by 64 bytes of header,
 *  enough to hold the fragment header below and 4 bytes of sandtrap
 *  immediately preceeding the payload data.
 *
 */
struct cm_trace_fragment {
  char const *mtf_original_file;
  char const *mtf_recent_file;
  int mtf_original_line;
  int mtf_recent_line;
  size_t mtf_size;
  cm_trace_fragment *mtf_prev;
  cm_trace_fragment *mtf_next;

  /* placeholder -- this isn't actually where the header sandtrap goes. */
  unsigned char mtf__sandtrap[4];
};

static const cm_list_offsets cm_trace_fragment_offsets =
    CM_LIST_OFFSET_INIT(cm_trace_fragment, mtf_next, mtf_prev);

#define MT_FRAGMENT(base) \
  ((cm_trace_fragment *)((char *)(base) - sizeof(cm_trace_fragment)))
#define MT_FRAMED_SIZE(size) ((size) + sizeof(cm_trace_fragment) + 4)
#define MT_PAYLOAD(mtf) ((void *)((char *)(mtf) + sizeof(cm_trace_fragment)))
#define MT_HEAD_PADDING(mtf) ((unsigned char *)MT_PAYLOAD(mtf) - 4)
#define MT_TAIL_PADDING(mtf) \
  ((unsigned char *)MT_PAYLOAD(mtf) + (mtf)->mtf_size)

#define MT_TEST_4(base, ch)                \
  (((unsigned char *)(base))[0] == (ch) && \
   ((unsigned char *)(base))[1] == (ch) && \
   ((unsigned char *)(base))[2] == (ch) && \
   ((unsigned char *)(base))[3] == (ch))

#define MT_SET_4(base, ch)                                       \
  (((unsigned char *)(base))[0] = ((unsigned char *)(base))[1] = \
       ((unsigned char *)(base))[2] = ((unsigned char *)(base))[3] = (ch))

static void cm_trace_log_stderr(void *dummy, int level, char const *str, ...) {
  if (level != CM_LOG_ALLOC) {
    va_list ap;
    va_start(ap, str);

    vfprintf(stderr, str, ap);
    putc('\n', stderr);

    va_end(ap);
  }
}

static void *cm_trace_alloc_chunk(cm_trace_handle *h, size_t size,
                                  char const *file, int line) {
  cm_trace_fragment *f;

  f = h->mt_source->cm_realloc_loc(h->mt_source, NULL, MT_FRAMED_SIZE(size),
                                   file, line);
  if (f == NULL) return NULL;

  f->mtf_original_file = file;
  f->mtf_original_line = line;
  f->mtf_recent_file = NULL;
  f->mtf_recent_line = 0;
  f->mtf_size = size;
  f->mtf_prev = NULL;
  f->mtf_next = NULL;

  MT_SET_4(MT_HEAD_PADDING(f), 0x23);
  MT_SET_4(MT_TAIL_PADDING(f), 0xEF);

  if (size) memset((char *)(f + 1), 0xBB, size);
  return f;
}

static void cm_trace_log_fragment_data(cm_trace_handle const *h,
                                       cm_trace_fragment const *f,
                                       char const *fmt, ...) {
  char bigbuf[1024];
  va_list ap;

  va_start(ap, fmt);
  vsnprintf(bigbuf, sizeof(bigbuf), fmt, ap);
  va_end(ap);

  (*h->mt_log)(h->mt_log_data, CM_LOG_LIST, "%s", bigbuf);

  if (f->mtf_recent_file != NULL)
    h->mt_log(h->mt_log_data, CM_LOG_LIST,
              "\tfirst allocated \"%s\", line %d; "
              "most recent reallocation \"%s\", line %d.",
              f->mtf_original_file, f->mtf_original_line, f->mtf_recent_file,
              f->mtf_recent_line);
  else
    h->mt_log(h->mt_log_data, CM_LOG_LIST, "\tallocated \"%s\", line %d",
              f->mtf_original_file, f->mtf_original_line);
}

/*  Check the specified fragment <f> for under- and overrun and link errors.
 *  Log and return nonzero if any errors are found.
 */
static int cm_trace_check_chunk(cm_trace_handle const *h,
                                cm_trace_fragment const *f, char const *file,
                                int line) {
  int error = 0;

  if (!MT_TEST_4(MT_HEAD_PADDING(f), 0x23)) {
    cm_trace_fragment const *cc;

    error = 1;

    /* Differenciate between chunks that just aren't ours
     * at all, and those that have actually been corrupted.
     */
    for (cc = h->mt_head; cc != NULL; cc = cc->mtf_next)
      if (cc == f) break;

    if (cc == NULL)
      cm_trace_log_fragment_data(h, f, "\"%s\", line %d: never allocated %p!",
                                 file, line, MT_PAYLOAD(f));
    else
      cm_trace_log_fragment_data(h, f,
                                 "\"%s\", line %d: header overrun: "
                                 "%2.2x %2.2x %2.2x %2.2x (payload %p[%lu]",
                                 file, line, MT_HEAD_PADDING(f)[0],
                                 MT_HEAD_PADDING(f)[1], MT_HEAD_PADDING(f)[2],
                                 MT_HEAD_PADDING(f)[3], MT_PAYLOAD(f),
                                 (unsigned long)f->mtf_size);
  }

  if (!MT_TEST_4(MT_TAIL_PADDING(f), 0xEF)) {
    error = 1;
    cm_trace_log_fragment_data(h, f,
                               "\"%s\", line %d: trailer overrun: "
                               "%2.2x %2.2x %2.2x %2.2x (payload %p[%lu]",
                               file, line, MT_TAIL_PADDING(f)[0],
                               MT_TAIL_PADDING(f)[1], MT_TAIL_PADDING(f)[2],
                               MT_TAIL_PADDING(f)[3], MT_PAYLOAD(f),
                               (unsigned long)f->mtf_size);
  }

  if (f->mtf_prev != NULL) {
    if (f->mtf_prev->mtf_next != f) {
      error = 1;
      cm_trace_log_fragment_data(h, f,
                                 "\"%s\", line %d: chain corruption: "
                                 "mtf_prev %p -> mtf_next %p != "
                                 "fragment header %p",
                                 file, line, (void *)f->mtf_prev,
                                 (void *)f->mtf_prev->mtf_next, (void *)f);
    }
  } else if (h->mt_head != f) {
    error = 1;
    cm_trace_log_fragment_data(h, f,
                               "\"%s\", line %d: chain corruption: "
                               "mtf_prev of %p is NULL, yet head is %p",
                               file, line, (void *)f, (void *)h->mt_head);
  }

  if (f->mtf_next != NULL) {
    if (f->mtf_next->mtf_prev != f) {
      error = 1;
      cm_trace_log_fragment_data(h, f,
                                 "\"%s\", line %d: chain corruption: "
                                 "mtf_next %p -> mtf_prev %p != "
                                 "fragment header %p",
                                 file, line, (void *)f->mtf_next,
                                 (void *)f->mtf_next->mtf_prev, (void *)f);
    }
  }

  if (f->mtf_size > h->mt_rts.cmrts_size) {
    error = 1;
    cm_trace_log_fragment_data(
        h, f,
        "\"%s\", line %d: fragment %p (payload %p) size %lu "
        "exceeds total allocated size %llu",
        file, line, (void *)f, MT_PAYLOAD(f), (unsigned long)f->mtf_size,
        h->mt_rts.cmrts_size);
  }
  return error;
}

/*  Allocate, reallocate, or free a fragment of memory.
 */
static void *cm_trace_realloc_loc(cm_handle *cm, void *ptr, size_t size,
                                  char const *file, int line) {
  cm_trace_handle *h = MT_HANDLE(cm);

  if (size == 0) {
    if (ptr != NULL) /* pure free */
    {
      cm_trace_fragment *f = MT_FRAGMENT(ptr);

      if (h->mt_log != NULL) {
        (*h->mt_log)(h->mt_log_data, CM_LOG_ALLOC,
                     "cm_trace_realloc FREE cm=%p "
                     "ptr=%p [%s:%d; allocated %s:%d]",
                     (void *)cm, ptr, file, line, f->mtf_original_file,
                     f->mtf_original_line);
      }

      /* check the pointer. */
      if (cm_trace_check_chunk(h, f, file, line)) abort();

      cm_list_remove(cm_trace_fragment, cm_trace_fragment_offsets, &h->mt_head,
                     0, f);

      h->mt_rts.cmrts_size -= f->mtf_size;
      h->mt_rts.cmrts_num_fragments--;

      /* sabotage spurious references */
      memset(f, 0xCD, MT_FRAMED_SIZE(f->mtf_size));
      free(f);
    }
    ptr = NULL;
  } else if (ptr == NULL) /* pure malloc */
  {
    cm_trace_fragment *f;
    f = cm_trace_alloc_chunk(h, size, file, line);

    f->mtf_original_file = file;
    f->mtf_original_line = line;
    cm_list_push(cm_trace_fragment, cm_trace_fragment_offsets, &h->mt_head, 0,
                 f);

    h->mt_rts.cmrts_size += size;
    h->mt_rts.cmrts_total_size += size;
    f->mtf_size = size;
    h->mt_rts.cmrts_num_fragments++;
    h->mt_rts.cmrts_total_allocs++;

    ptr = MT_PAYLOAD(f);

    if (h->mt_log != NULL)
      (*h->mt_log)(h->mt_log_data, CM_LOG_ALLOC,
                   "cm_trace_realloc MALLOC cm=%p "
                   "ptr=%p size=%zu [%s:%d]",
                   (void *)cm, ptr, size, file, line);
  } else {
    void *old_ptr = ptr;
    cm_trace_fragment *old_f = MT_FRAGMENT(ptr);
    size_t old_size = old_f->mtf_size;
    cm_trace_fragment *new_f;

    /* check the original pointer */
    if (cm_trace_check_chunk(h, old_f, file, line)) abort();

    h->mt_rts.cmrts_total_allocs++;

    if (size <= old_f->mtf_size) /* realloc: shrink */
    {
      old_f->mtf_recent_file = file;
      old_f->mtf_recent_line = line;

      if (size == old_f->mtf_size) return ptr;

      h->mt_rts.cmrts_size -= old_f->mtf_size - size;
      h->mt_rts.cmrts_total_size -= old_f->mtf_size - size;
      old_f->mtf_size = size;
      new_f = old_f;

      /* rewrite (i.e. pull towards us) the old sand trap. */
      MT_SET_4(MT_TAIL_PADDING(old_f), 0xEF);
      ptr = MT_PAYLOAD(old_f);
    } else /* realloc: expand */
    {
      cm_list_remove(cm_trace_fragment, cm_trace_fragment_offsets, &h->mt_head,
                     0, old_f);
      new_f = h->mt_source->cm_realloc_loc(h->mt_source, (void *)old_f,
                                           MT_FRAMED_SIZE(size), file, line);
      if (new_f == NULL) {
        cm_list_push(cm_trace_fragment, cm_trace_fragment_offsets, &h->mt_head,
                     0, old_f);
        return NULL;
      }

      new_f->mtf_recent_file = file;
      new_f->mtf_recent_line = line;

      memset((char *)MT_PAYLOAD(new_f) + new_f->mtf_size, 0xBB,
             size - new_f->mtf_size);

      cm_list_push(cm_trace_fragment, cm_trace_fragment_offsets, &h->mt_head, 0,
                   new_f);

      h->mt_rts.cmrts_size += size - new_f->mtf_size;
      h->mt_rts.cmrts_total_size += size - new_f->mtf_size;
      new_f->mtf_size = size;
      MT_SET_4(MT_TAIL_PADDING(new_f), 0xEF);

      ptr = MT_PAYLOAD(new_f);
    }

    if (h->mt_log != NULL)
      (*h->mt_log)(h->mt_log_data, CM_LOG_ALLOC,
                   "cm_trace_realloc REALLOC cm=%p "
                   "old=%p[%zu] to %p[%zu] [%s:%d; allocated %s:%d]",
                   (void *)cm, old_ptr, old_size, ptr, size, file, line,
                   new_f->mtf_original_file, new_f->mtf_original_line);
  }

  if (h->mt_rts.cmrts_num_fragments > h->mt_rts.cmrts_max_fragments)
    h->mt_rts.cmrts_max_fragments = h->mt_rts.cmrts_num_fragments;
  if (h->mt_rts.cmrts_size > h->mt_rts.cmrts_max_size)
    h->mt_rts.cmrts_max_size = h->mt_rts.cmrts_size;

  return ptr;
}

/*  get size of a piece of memory.
 *   */
static size_t cm_trace_fragment_size(cm_handle *cm, void *ptr) {
  cm_trace_handle *h = MT_HANDLE(cm);

  return cm_fragment_size(h->mt_source, MT_FRAGMENT(ptr));
}

/*  Get the heap runtime statistics.
 */
static void cm_trace_runtime_statistics_get(cm_handle *cm,
                                            cm_runtime_statistics *cmrts) {
  cm_trace_handle *h = MT_HANDLE(cm);

  *cmrts = h->mt_rts;
}

/**
 * @brief Create a tracing memory context.
 *
 * The tracer context is built on top of an arbitrary other memory
 * context. It wraps a bookkeeping layer around the allocations, and keeps
 * track of what is allocated, where it was originally and most
 * recently allocated, and how much memory is in use overall.
 *
 * In spite of the name, the trace allocator does not print out
 * every single allocation; but it can be made to dump the list
 * of all allocated fragments, which should be as useful, perhaps more.
 *
 * @param source the underlying memory context that does the actual allocations.
 */
cm_handle *cm_trace(cm_handle *source) {
  cm_trace_handle *h;

  h = cm_talloc(source, cm_trace_handle, 1);
  if (h == NULL) return NULL;
  memset(h, 0, sizeof(*h));

  h->mt_source = source;
  h->mt_handle.cm_realloc_loc = cm_trace_realloc_loc;
  h->mt_handle.cm_fragment_size_ = cm_trace_fragment_size;
  h->mt_handle.cm_runtime_statistics_get_ = cm_trace_runtime_statistics_get;

  /* By default, log to stderr. */
  h->mt_log = cm_trace_log_stderr;
  h->mt_log_data = NULL;

  h->mt_head = NULL;

  return &h->mt_handle;
}

/*  Destroy a tracing memory context allocated with cm_trace()
 */
void cm_trace_destroy_loc(cm_handle *cm, char const *file, int line) {
  if (cm) {
    cm_trace_handle *h = MT_HANDLE(cm);
    h->mt_source->cm_realloc_loc(h->mt_source, h, 0, file, line);
  }
}

/*  Check consistency -- check all chunks for overruns,
 *  and make sure the total memory is accounted for.
 *  Log and exit if anything strange is going on.
 */
void cm_trace_check_loc(cm_handle const *cm, char const *file, int line) {
  unsigned long long my_total = 0;

  cm_trace_fragment const *mtf;
  cm_trace_handle *h = MT_HANDLE(cm);

  for (mtf = h->mt_head; mtf != NULL; mtf = mtf->mtf_next) {
    my_total += mtf->mtf_size;
    if (cm_trace_check_chunk(h, mtf, file, line)) abort();
  }

  if (my_total != h->mt_rts.cmrts_size) {
    if (h->mt_log != NULL)
      h->mt_log(h->mt_log_data, CM_LOG_ERROR,
                "\"%s\", line %d: total storage in inventory (%llu) "
                "disagrees with header (%llu)",
                file, line, (unsigned long long)my_total,
                (unsigned long long)h->mt_rts.cmrts_size);
    abort();
  }
}

/**
 * @brief Return the total number of payload bytes currently
 * 	allocated in a handle.
 * @param cm an allocator handle returned by cm_trace()
 */
unsigned long long cm_trace_total(cm_handle *cm) {
  cm_trace_handle *h = MT_HANDLE(cm);
  return h->mt_rts.cmrts_size;
}

/**
 * @brief Get the maximum heap size.
 *
 * This is the fictitious usable heap, as presented to the using
 * application; the actual program data usage will be a bit above that,
 * allowing for this allocator's data structures.
 *
 * @param cm an allocator handle returned by cm_trace()
 * @return the total number of payload bytes that were ever
 *  	allocated at one time through this handle.
 */
unsigned long long cm_trace_total_max(cm_handle *cm) {
  cm_trace_handle *h = MT_HANDLE(cm);
  return h->mt_rts.cmrts_max_size;
}

/**
 * @brief Return the total number of fragments currently allocated.
 * @param cm an allocator handle returned by cm_trace()
 * @return the number of individual fragments currently allocated
 *  	using this specific handle.
 */
unsigned long long cm_trace_n(cm_handle *cm) {
  cm_trace_handle *h = MT_HANDLE(cm);
  return h->mt_rts.cmrts_num_fragments;
}

/**
 * @brief Get the maximum fragment count.
 * @param cm an allocator handle returned by cm_trace()
 * @return the hightest total number of fragments that were ever
 *  	allocated at one time using this specified handle.
 */
unsigned long long cm_trace_n_max(cm_handle *cm) {
  cm_trace_handle *h = MT_HANDLE(cm);
  return h->mt_rts.cmrts_max_fragments;
}

/**
 * @brief Set the log callback and its opaque data pointer.
 *
 * Only one log callback can be active at any one time per handle;
 * if there was an old setting, it is overwritten by this call.
 *
 * @param cm 	an allocator handle returned by cm_trace()
 * @param callback  the desired callback
 * @param callback_data opaque application pointer that will
 *		be passed to the callback as its first argument.
 */
void cm_trace_set_log_callback(cm_handle *cm, cm_log_callback *callback,
                               void *callback_data) {
  cm_trace_handle *h = MT_HANDLE(cm);

  if (callback != NULL) {
    h->mt_log = callback;
    h->mt_log_data = callback_data;
  } else {
    h->mt_log = cm_trace_log_stderr;
    h->mt_log_data = NULL;
  }
}

/**
 * @brief Get the log callback and its opaque data pointer.
 *
 * @param cm 	an allocator handle returned by cm_trace()
 * @param callback_out  the desired callback
 * @param callback_data_out opaque application pointer that will
 *		be passed to the callback as its first argument.
 */
void cm_trace_get_log_callback(cm_handle *cm, cm_log_callback **callback,
                               void **callback_data) {
  cm_trace_handle *h = MT_HANDLE(cm);

  *callback = h->mt_log;
  *callback_data = h->mt_log_data;
}

/**
 * @brief List allocated fragments.
 *
 * Calls the log callback (installed with cm_trace_set_log_callback())
 * once for each allocated fragment that has never been free()d.
 *
 * @param cm 	an allocator handle returned by cm_trace()
 * @return 0 if there are no remaining fragments, 1 otherwise.
 */
int cm_trace_list(cm_handle const *cm) {
  cm_trace_handle const *h = MT_HANDLE(cm);
  cm_trace_fragment const *mtf;

  if (h->mt_log == NULL) return h->mt_head ? 1 : 0;

  for (mtf = h->mt_head; mtf != NULL; mtf = mtf->mtf_next) {
    if (mtf->mtf_recent_file != NULL)
      (*h->mt_log)(h->mt_log_data, CM_LOG_LIST,
                   "%p[%lu], created \"%s\", line %d; "
                   "most recent realloc \"%s\", line %d",
                   (void *)MT_PAYLOAD(mtf), (unsigned long)mtf->mtf_size,
                   mtf->mtf_original_file, mtf->mtf_original_line,
                   mtf->mtf_recent_file, mtf->mtf_recent_line);
    else
      (*h->mt_log)(h->mt_log_data, CM_LOG_LIST,
                   "%p[%lu], allocated \"%s\", line %d",
                   (void *)MT_PAYLOAD(mtf), (unsigned long)mtf->mtf_size,
                   mtf->mtf_original_file, mtf->mtf_original_line);
  }
  return h->mt_head ? 1 : 0;
}
