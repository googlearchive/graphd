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
#ifndef CM_H
#define CM_H

#include <stdbool.h> /* bool! */
#include <stddef.h>  /* offsetof */
#include <stdlib.h>  /* size_t */

struct cm_handle;

#define CM_FACILITY_MEMORY (1ul << 21)

/** @brief General-purpose text buffer.
 */
typedef struct cm_buffer {
  /** @brief Handle through which the buffer is extended.
   */
  struct cm_handle *buf_cm;

  /** @brief Text bytes.
   */
  char *buf_s;

  /** @brief Number of valid text bytes, not including a
   *  	terminating '\\0'.
   */
  size_t buf_n;

  /** @brief Number of total allocated bytes.
   */
  size_t buf_m;

} cm_buffer;

/** @brief allocator runtime statistics.
 */
typedef struct cm_runtime_statistics {
  unsigned long long cmrts_num_fragments;
  unsigned long long cmrts_max_fragments;
  unsigned long long cmrts_size;
  unsigned long long cmrts_max_size;
  unsigned long long cmrts_total_allocs;
  unsigned long long cmrts_total_size;

} cm_runtime_statistics;

/**
 * @brief Version string of the most recently modified source file.
 */
extern char const cm_build_version[];

/**
 * @brief Message passed to a #cm_log_callback to
 *	log the presence of a piece of memory.
 */
typedef enum {

  /* Listing (leftover?) pieces in response
   * to cm_trace_list
   */
  CM_LOG_LIST = 1,

  /* A serious error or assertion failure.
   */
  CM_LOG_ERROR = 2,

  /* Low-level logging of all calls to realloc,
   * malloc, or free.
   */
  CM_LOG_ALLOC = 3

} cm_log_message;

/**
 * @brief 	Log callback used by the tracing allocator.
 *
 * If memory is corrupted, or if a caller asked for a listing of all
 * allocated memory segments, the log callback is invoked with chunks
 * of text that describe the problem or the allocated pieces.
 *
 * @param data 	opaque application pointer
 * @param message one of #CM_LOG_LIST or #CM_LOG_ERROR
 * @param text	 details of the message, as human-readable text.
 */
typedef void cm_log_callback(void *data, int message, char const *fmt, ...);

/**
 * @brief Central allocator plugin callback.
 *
 * A single callback takes care of the three normal allocation functions
 * free (pointer is non-NULL and size is 0), malloc (pointer is NULL and
 * size is non-0), and realloc (pointer is non-NULL and size
 * is non-0).  If both the pointer is NULL and size is 0, nothing happens,
 * and the call returns NULL.
 *
 * @param cm	allocator module handle
 * @param pointer	NULL or data pointer to be free'ed or reallocated.
 * @param size		desired final size of the data block
 * @param file		__FILE__ of the calling code
 * @param line		__LINE__ of the calling code
 * @return NULL in case of allocation failure or if size was 0, otherwise
 * 	a pointer to @b size bytes of usable data.
 */
typedef void *cm_realloc_callback(struct cm_handle *cm, void *pointer,
                                  size_t size, char const *file, int line);

/**
 * @brief Fragment size plugin callback.
 *
 * Callback that, given a pointer to a piece of memory
 * allocated previously, will return the total size of the
 * container that piece of memory lives in.
 *
 * @param cm	allocator module handle
 * @param pointer	data pointer
 * @return size of memory block.
 */
typedef size_t cm_fragment_size_callback(struct cm_handle *cm, void *pointer);

/**
 * @brief Runtime statistics plugin callback.
 *
 * Callback to get the statistics regarding
 * number of fragments and memory size
 * as tracked by allocator.
 *
 * @param cm	allocator module handle
 * @param cmrts	output for statistics
 */
typedef void cm_runtime_statistics_get_callback(struct cm_handle *cm,
                                                cm_runtime_statistics *cmrts);

/**
 * @brief Common allocator state.
 *
 * The data in this struct is shared by all allocators.
 * After the single common member - the reallocator function - each
 * allocator's constructor may have other data stored in a larger
 * structure that contains the cm_handle as its first element, and
 * is passed around using a pointer to its first element in place of a
 * pointer to the whole.
 *
 * If this were C++, this would be the common superclass of all allocators.
 */
typedef struct cm_handle {
  /**
   * @brief The reallocator callback.
   */
  cm_realloc_callback *cm_realloc_loc;

  /**
   * @brief The fragment size callback.
   */
  cm_fragment_size_callback *cm_fragment_size_;

  /**
   * @brief The fragment size callback.
   */
  cm_runtime_statistics_get_callback *cm_runtime_statistics_get_;

} cm_handle;

#ifdef DOCUMENTATION_GENERATOR_ONLY
/**
 * @brief Free a pointer.
 *
 * If the pointer is NULL, nothing happens.
 * Otherwise, it must have been previously allocated with cm_malloc()
 * or cm_realloc() (or their implicitly typing forms, cm_talloc()
 * and cm_trealloc()) using the same handle cm.
 *
 * If the allocator supports release of individual memory blocks,
 * the allocated resources are returned to the C runtime pool
 * and may be reused in future calls.
 *
 * @param cm	the allocation module handle
 * @param ptr	pointer to free.
 */
void cm_free(cm_handle *cm, void *ptr) {}

/**
 * @brief Allocate memory of a given size.
 *
 * @param cm	the allocation module handle
 * @param size	number of bytes to allocate
 * @return NULL on allocation error or if @b size is zero,
 *	otherwise a pointer to at least that many bytes aligned
 *	for any use.
 */
void *cm_malloc(cm_handle *cm, size_t size) { return 0; }

/**
 * @brief Resize previously allocated memory.
 *
 * The pointer, if non-NULL, must have been previously
 * allocated with cm_malloc()
 * or cm_realloc() (or their implicitly typing forms, cm_talloc()
 * and cm_trealloc()) using the same handle cm.
 *
 * As with the original realloc, the storage up to the
 * size it previously had keeps its state.
 *
 * @param cm	the allocation module handle
 * @param ptr	NULL or pointer to existing memory.
 * @param size	number of bytes to allocate
 * @return NULL on allocation error or if @b size is zero,
 *	otherwise a pointer to at least that many bytes aligned
 *	for any use.
 */
void *cm_realloc(cm_handle *cm, void *ptr, size_t size) { return 0; }

/**
 * @brief Retrieve memory block size.
 *
 * The pointer must have been previously
 * allocated with cm_malloc()
 * or cm_realloc() (or their implicitly typing forms, cm_talloc()
 * and cm_trealloc()) using the same handle cm.
 *
 * @param cm	the allocation module handle
 * @param ptr	pointer to existing memory.
 * @return size of memory block.
 */
size_t cm_fragment_size(cm_handle *cm, void *ptr) { return 0; }

/**
 * @brief Get runtime statistics.
 *
 * @param cm	the allocation module handle
 * @param cmrts	place to store current statistics.
 */
void cm_runtime_statistics_get(cm_handle *cm, cm_runtime_statistics *cmrts) {}

#else

#define cm_free(handle, ptr) \
  ((*(handle)->cm_realloc_loc)(handle, ptr, 0, __FILE__, __LINE__))

#define cm_malloc(handle, size) \
  ((*(handle)->cm_realloc_loc)(handle, NULL, size, __FILE__, __LINE__))

#define cm_realloc(handle, ptr, size) \
  ((*(handle)->cm_realloc_loc)(handle, ptr, size, __FILE__, __LINE__))

#define cm_fragment_size(handle, ptr) \
  ((*(handle)->cm_fragment_size_)(handle, ptr))

#define cm_runtime_statistics_get(handle, cmrts) \
  ((*(handle)->cm_runtime_statistics_get_)(handle, cmrts))
#endif

/**
 * @brief Allocate a fixed number of objects of a specific type
 *
 * This macro wraps cm_malloc() with a cast and a multiplication,
 * making sure that the pointer type and element type have the right
 * relationship.  (Forgetting to remove the level of indirection from
 * the element type can yield bad, hard to debug results.)
 *
 * @param cm 	the allocator module handle.
 * @param type	the object type
 * @param n	number of such objects
 */
#define cm_talloc(cm, type, n) ((type *)cm_malloc((cm), (n) * sizeof(type)))

/**
 * @brief Re-allocate a fixed number of objects of a specific type
 *
 * The realloc version of cm_talloc().
 *
 * @param cm 	the allocator module handle.
 * @param type	the object type
 * @param ptr	NULL or a pointer to the base of an array of objects
 * @param n	desired number of objects
 */
#define cm_trealloc(cm, type, ptr, n) \
  ((type *)cm_realloc((cm), (ptr), (n) * sizeof(type)))

#ifndef DOCUMENTATION_GENERATOR_ONLY
#define cm_trealloc_loc(cm, type, ptr, n, file, line) \
  ((type *)(*(cm)->cm_realloc_loc)((cm), (ptr), (n) * sizeof(type), file, line))
#endif

/**
 * @brief Default C allocator.
 *
 * The static cm_handle pointer returned by this allocator
 * unceremoniously maps to calls to the regular C runtime library allocator
 * functions malloc(), realloc(), and free().
 * @return a handle to the default C allocator.
 *	Calls to cm_c() are guaranteed to never fail,
 *	and always return a non-NULL allocator pointer.
 */
cm_handle *cm_c(void);

/* cm-buffer.c */

unsigned long long cm_buffer_checksum(cm_buffer const *, int);
unsigned long long cm_buffer_checksum_text(char const *_s, char const *_e,
                                           int _bits);

#define cm_buffer_alloc(b, s) cm_buffer_alloc_loc(b, s, __FILE__, __LINE__)

int cm_buffer_alloc_loc(cm_buffer *buf, size_t size, char const *file,
                        int line);
void cm_buffer_truncate(cm_buffer *);
void cm_buffer_initialize(cm_buffer *, cm_handle *);
void cm_buffer_finish(cm_buffer *);
char const *cm_buffer_memory(cm_buffer const *);
char const *cm_buffer_memory_end(cm_buffer const *);

#define cm_buffer_alloc(b, s) cm_buffer_alloc_loc(b, s, __FILE__, __LINE__)
int cm_buffer_alloc_loc(cm_buffer *buf, size_t size, char const *file,
                        int line);
size_t cm_buffer_length(cm_buffer const *);
int cm_buffer_add_bytes_loc(cm_buffer *, char const *, size_t, char const *,
                            int);
#define cm_buffer_add_bytes(a, b, c) \
  cm_buffer_add_bytes_loc(a, b, c, __FILE__, __LINE__)
int cm_buffer_add_string(cm_buffer *, char const *);
int cm_buffer_sprintf(cm_buffer *, char const *, ...)
#if __GNUC__
    __attribute__((format(printf, 2, 3)))
#endif
    ;
int cm_buffer_sprintf_loc(char const *file, int line, cm_buffer *, char const *,
                          ...)
#if __GNUC__
    __attribute__((format(printf, 4, 5)))
#endif
    ;

#define cm_buffer_sprintf(b, ...) \
  cm_buffer_sprintf_loc(__FILE__, __LINE__, b, __VA_ARGS__)

/* cm-error.c */

cm_handle *cm_error(cm_handle *);
void cm_error_set_log_callback(cm_handle *, cm_log_callback *, void *);

#ifdef DOCUMENTATION_GENERATOR_ONLY

/**
 * @brief Destroy an error-reporting context.
 *
 * Frees up the resources allocated to hold the error-reporting
 * context.  This implicitly uses the source allocator stored
 * within the error-reporting allocator.  The source allocator
 * passed to cm_error() must still be valid at that point.
 *
 * @param cm	an error-reporting allocator handle created
 *		with cm_error()
 */
void cm_error_destroy(cm_handle *cm) {}
#else

void cm_error_destroy_loc(cm_handle *, char const *, int);
#define cm_error_destroy(cm) cm_error_destroy_loc((cm), __FILE__, __LINE__)
#endif

/* cm-hash.c */

/**
 * @brief Binary hashtable with variable-length keys and fixed-size values.
 *
 * @warning
 * The internal structure of this hash table is visible because (a)
 * it doesn't change much anymore, and (b) it's convenient to be
 * able to embed hashtables in other objects without paying the
 * complexity and fragmentation cost of allocating them separately.
 *
 * @par
 * If you're just using a hashtable, initialize it and manipulate its
 * contents using the cm_h* and cm_hash* functions; @em don't
 * access its components directly.
 *
 * This hashtable maps octet strings to fixed-size values.
 * All values have the same size;
 * the keys can have arbitrary sizes.  The values can be allocated
 * and retrieved, traversed in some arbitrary, but complete order.
 * Given the pointer to a value, its key and key size can be retrieved in
 * constant size.
 *
 * Value pointers, once allocated, do not move around.
 * There is only one underlying allocation per value.
 */
typedef struct cm_hashtable {
  /**
   * @brief Allocator used for hash table infrastructure and
   * 	 its elements.
   */
  cm_handle *h_cm;

  /**
   * @brief Mask for useable bits of the hash.
   */
  unsigned long h_mask;

  /**
   * @brief Total (maximum) number of allocated slots in the hashtable.
   */
  unsigned long h_m;

  /**
   * @brief Number of occupied slots.  At all times, h_n <= h_m.
   */
  unsigned long h_n;

  /**
   * @brief If h_n >= h_limit, the hashtable should grow.
   */
  unsigned long h_limit;

  /**
   * @brief The number of bytes per single hash value.
   */
  size_t h_value_size;

  /**
   * @brief Table of pointers to individual hash table slot arrays.
   */
  void **h_table;

} cm_hashtable;

cm_hashtable *cm_hashcreate(cm_handle *, size_t, int);
int cm_hashinit(cm_handle *, cm_hashtable *, size_t, int);
void *cm_hash(cm_hashtable *, void const *, size_t, int);
void *cm_hashnext(cm_hashtable const *, void const *);
void cm_hashfinish(cm_hashtable *);
void cm_hashdestroy(cm_hashtable *);
void cm_hashdelete(cm_hashtable *, void *);
void const *cm_hashmem(cm_hashtable const *, void const *);
size_t cm_hashsize(cm_hashtable const *, void const *);
cm_hashtable *cm_hashcopy(cm_hashtable const *h, cm_hashtable *out);

/**
 * @brief How many items does this hashtable contain?
 *
 *  Given a hashtable and the type of the application data,
 *  return the number of entries allocated in the hashtable.
 *
 * @param h hashtable
 * @return the number of elements in the hashtable.
 */
#define cm_hashnelems(h) ((h)->h_n)

/**
 * @brief Pass to cm_hash() to make it fail if an object doesn't exist.
 */
#define CM_HASH_READ_ONLY 0

/**
 * @brief Pass to cm_hash() to have it create an object if it doesn't exist.
 */
#define CM_HASH_READ_CREATE 1

/**
 * @brief Pass to cm_hash() to have it fail if an object exists.
 */
#define CM_HASH_CREATE_ONLY 2

#ifdef DOCUMENTATION_GENERATOR_ONLY
/**
 * @brief Allocate a new hashtable.  Type macro version of cm_hashcreate().
 *
 *  Given the type of the application data
 *  and the initial chunk size of the table m, allocate and
 *  initialize a cm_hashtable object.
 *
 * @param cm Memory manager to use
 * @param T Fixed-size type of a single element
 * @param page initial number of elements
 *
 * @return NULL on allocation failure, otherwise a pointer
 *  to the newly allocated hashtable.
 */
#define cm_hcreate(cm, T, page) cm_hashcreate(cm, sizeof(T), page)

/**
 * @brief How many items does this hashtable contain?
 *
 *  Given a hashtable and the type of the application data,
 *  return the number of entries allocated in the hashtable.
 *
 * @param h hashtable
 * @param T fixed-size type of a single element
 *
 * @return the number of elements in the hashtable.
 */
#define cm_hnelems(h, T) ((h)->h_n)

/**
 * @brief Destroy a hashtable. Type macro version of cm_hashdestroy().
 *
 * Destroying a NULL pointer is harmless and does nothing.
 * Otherwise, the hashtable and all its fixed-size elements
 * and keys are free'd.
 *
 * @param h NULL or hashtable created with cm_create
 * @param T fixed-size type of a single element
 */
#define cm_hdestroy(h, T) cm_hashdestroy(h)

/**
 * @brief Exclusively allocate a new value.
 * Fail if the value already exists in the hashtable.
 * @param h hashtable created with cm_hcreate()
 * @param T fixed-size type of one element
 * @param mem key
 * @param s number of bytes pointed to by @b mem
 * @return NULL on allocation failure, otherwise a pointer
 *  to the new element.  The element is initialized with '\\0' bytes.
 */
#define cm_hexcl(h, T, mem, s) ((T *)cm_hash(h, mem, s, CM_HASH_CREATE_ONLY))

/**
 * @brief Access or allocate a value.
 * If the value doesn't exist yet, it is created and initialized with zeroes.
 * @param h hashtable created with cm_hcreate()
 * @param T fixed-size type of one element
 * @param mem key
 * @param s number of bytes pointed to by @b mem
 * @return NULL on allocation failure, otherwise a pointer
 *  to the element.  If an element was newly allocated, it has been
 *  initialized with '\\0' bytes.
 */
#define cm_hnew(h, T, mem, s) ((T *)cm_hash(h, mem, s, CM_HASH_READ_CREATE))

/**
 * @brief Access an existing value.
 * @param h hashtable created with cm_hcreate()
 * @param T fixed-size type of one element
 * @param mem key
 * @param s number of bytes pointed to by @b mem
 * @return NULL if the key didn't exist in the table, otherwise a pointer
 *  to the element.
 */
#define cm_haccess(h, T, mem, s) ((T *)cm_hash(h, mem, s, CM_HASH_READ_ONLY))

/**
 * @brief Access next value.
 * @param h hashtable created with cm_hcreate()
 * @param T fixed-size type of one element
 * @param value NULL or previous value
 * @return NULL if the previous value was the last one,
 *  otherwise the value after the one passed in.
 */
#define cm_hnext(h, T, value) ((T *)cm_hashnext(h, value))

/**
 * @brief Get the key, given a value.  Type macro version of cm_hashmem().
 * @param h hashtable created with cm_hcreate()
 * @param T fixed-size type of one element
 * @param value a value
 * @return the first byte of a key.
 */
#define cm_hmem(h, T, value) (cm_hashmem(h, value))

/**
 * @brief Get the size of the key, given a value.
 *	Type macro version of cm_hashsize().
 * @param h hashtable created with cm_hcreate()
 * @param T fixed-size type of one element
 * @param value a value
 * @return the number of valid bytes pointed to by the key, not including
 * 	a trailing '\\0' appended at hash-ni.
 */
#define cm_hsize(h, T, value) (cm_hashsize(h, value))

/**
 * @brief Delete a hash value.  Type macro version of cm_hashdelete().
 * @param h hashtable created with cm_hcreate()
 * @param T fixed-size type of one element
 * @param value a value
 */
#define cm_hdelete(h, T, value) (cm_hashdelete(h, value))
#else
#define cm_htype(ob, T) ((void)(0 && cm_hashcreate(0, 0, (ob) == (T *)0)))
#define cm_hcreate(cm, T, page) cm_hashcreate(cm, sizeof(T), page)
#define cm_hdestroy(h, T) cm_hashdestroy(h)
#define cm_hnelems(h, T) ((h)->h_n)
#define cm_hexcl(h, T, mem, s) ((T *)cm_hash(h, mem, s, CM_HASH_CREATE_ONLY))
#define cm_hnew(h, T, mem, s) ((T *)cm_hash(h, mem, s, CM_HASH_READ_CREATE))
#define cm_haccess(h, T, mem, s) ((T *)cm_hash(h, mem, s, CM_HASH_READ_ONLY))
#define cm_hnext(h, T, ob) (cm_htype(ob, T), (T *)cm_hashnext(h, ob))
#define cm_hmem(h, T, ob) (cm_htype(ob, T), cm_hashmem(h, ob))
#define cm_hsize(h, T, ob) (cm_htype(ob, T), cm_hashsize(h, ob))
#define cm_hdelete(h, T, ob) (cm_htype(ob, T), cm_hashdelete(h, ob))
#endif

/* cm-heap.c */

cm_handle *cm_heap(cm_handle *);
cm_handle *cm_heap_source(cm_handle const *);

#ifdef DOCUMENTATION_GENERATOR_ONLY
/**
 * @brief Destroy a heap memory context.
 *
 * After the call, all memory allocated for the heap will
 * have been returned to the underying allocator.
 *
 * @param cm an allocator handle created with cm_heap().
 */
void cm_heap_destroy(cm_handle *cm) {}
#else
void cm_heap_destroy_loc(cm_handle *, char const *, int);
#define cm_heap_destroy(cm) cm_heap_destroy_loc((cm), __FILE__, __LINE__)
#endif

/* cm-resource.c */

/**
 * @brief  A single resource reference, for use in application data
 * 	structures.
 */
typedef struct cm_resource cm_resource;

/**
 * @brief  A resource reference, for use in the application data structures.
 */
typedef struct cm_resource_manager cm_resource_manager;

/**
 * @brief  A resource type, used to declare new resources
 */
typedef struct cm_resource_type {
  /**
   * @brief The name of the resource (for documentation only)
   */
  char const *rt_name;

  /**
   * @brief Free callback.
   */
  void (*rt_free)(void *_manager_data, void *_resource_data);

  /**
   * @brief List callback, for documentation.  The first
   *	argument is void here to avoid creating cross-library
   *	dependencies, but is a cl_handle in practice.
   *	(Cast it to cl_handle inside the callback.)
   */
  void (*rt_list)(void *_call_data, void *_manager_data, void *_resource_data);

} cm_resource_type;

/**
 * @brief  Resource handle, used to manage resources inside the
 *  	Application.
 *
 * 	This struct is public to allow embedding in other
 *	data structures.  Its contents should only be modified
 *	by calling cm_resource_... functions.
 */
struct cm_resource {
  /**
   * @brief Previous resource of the same manager.
   */
  cm_resource *r_prev;

  /**
   * @brief Next resource of the same manager.
   */
  cm_resource *r_next;

  /**
   * @brief Resource manager.  Keeps track of all
   *	resources in a doubly linked list.
   */
  cm_resource_manager *r_manager;

  /**
   * @brief Number of links to this resource.
   */
  unsigned int r_link;

  /**
   * @brief Opaque application data poitner.
   */
  void *r_data;

  /**
   * @brief Resource type with name and callbacks.
   */
  cm_resource_type const *r_type;
};

/**
 * @brief  A resource manager, used to manage resources inside the
 *  	application.
 */
struct cm_resource_manager {
  /**
   * @brief Head of doubly linked resource list.
   */
  cm_resource *rm_head;

  /**
   * @brief Tail of doubly linked resource list.
   */
  cm_resource *rm_tail;

  /**
   * @brief Application data of the resource manager, passed
   *  	to resource callbacks as first or second argument.
   */
  void *rm_data;
};

void cm_resource_initialize(cm_resource *);
void cm_resource_alloc(cm_resource_manager *_rm, cm_resource *_r,
                       cm_resource_type const *_rt, void *_data);
void cm_resource_free(cm_resource *);
void cm_resource_dup(cm_resource *);
void cm_resource_list(cm_resource *, void *);
cm_resource *cm_resource_top(cm_resource_manager *);
void cm_resource_manager_initialize(cm_resource_manager *_rm, void *_data);
void cm_resource_manager_finish(cm_resource_manager *);
void cm_resource_manager_list(cm_resource_manager *, void *);
void cm_resource_manager_map(cm_resource_manager *,
                             void (*)(void *, void *, void *), void *);

/* cm-trace.c */

cm_handle *cm_trace(cm_handle *);

#ifdef DOCUMENTATION_GENERATOR_ONLY
/**
 * @brief Check allocated memory for over- or underruns.
 *
 * Each piece of memory allocated by the cm_trace() allocator
 * has short prologue and epilogue byte arrays before and after the
 * caller-requested payload
 * that are seeded with a well-defined bit pattern.  This function
 * checks whether all those fragments still have the pattern that
 * was originally assigned to them, complaining about an error
 * if that's not the case.
 *
 * The error message includes the
 * filename and line of the call to cm_trace_check() that
 * discovered the corrupted memory, allowing an application
 * to narrow down the point of corruption.
 *
 * @param cm an allocator handle created by cm_trace().
 */
void cm_trace_check(cm_handle *cm) {}
#else
void cm_trace_check_loc(cm_handle const *, char const *, int);
#define cm_trace_check(cm) cm_trace_check_loc((cm), __FILE__, __LINE__)
#endif

#ifdef DOCUMENTATION_GENERATOR_ONLY
/**
 * @brief Destroy a trace memory context.
 *
 * This frees the data used to hold just the tracer memory handle.
 * If there is traced memory that hasn't been
 * free()d yet, it remains allocated.  (That's a bad thing.)
 *
 * @param cm an allocator handle created by cm_trace().
 */
cm_trace_destroy(cm_handle *cm) {}
#else
void cm_trace_destroy_loc(cm_handle *, char const *, int);
#define cm_trace_destroy(cm) cm_trace_destroy_loc((cm), __FILE__, __LINE__)
#endif

unsigned long long cm_trace_total(cm_handle *cm);
unsigned long long cm_trace_total_max(cm_handle *cm);
unsigned long long cm_trace_n(cm_handle *cm);
unsigned long long cm_trace_n_max(cm_handle *cm);
int cm_trace_list(cm_handle const *);
void cm_trace_set_log_callback(cm_handle *, cm_log_callback *, void *);
void cm_trace_get_log_callback(cm_handle *cm, cm_log_callback **callback,
                               void **callback_data);

/* cm-zalloc.c */

#ifdef DOCUMENTATION_GENERATOR_ONLY
/**
 * @brief Allocate zero-filled memory of a given size.
 *
 * The duplicate is allocated internally with cm_malloc()
 * and must, unless the allocator supports other means,
 * be free()d with cm_free() or cm_realloc().
 *
 * @param cm	the allocation module handle
 * @param size	number of bytes to allocate
 * @return NULL on allocation error or if @b size is zero,
 *	otherwise a pointer to at least that many bytes aligned
 *	for any use, filled with a zero byte pattern.
 */
void *cm_zalloc(cm_handle *cm, size_t size) { return 0; }
#else
void *cm_zalloc_loc(cm_handle *, size_t, char const *, int);
#define cm_zalloc(cm, size) cm_zalloc_loc((cm), (size), __FILE__, __LINE__)
#endif

/* cm-malcpy.c */

#ifdef DOCUMENTATION_GENERATOR_ONLY

/**
 * @brief Make a duplicate of an existing byte array
 *
 * The duplicate is allocated internally with cm_malloc()
 * and must, unless the allocator supports other means,
 * be free()d with cm_free() or cm_realloc().
 *
 * @param cm	the allocation module handle
 * @param ptr	memory that should be duplicated
 * @param size	number of bytes to duplicate
 * @return NULL on allocation error or if @b size is zero,
 *	otherwise a pointer to a byte-by-byte duplicate
 * 	of the @b size bytes pointed to by @b ptr.
 */
void *cm_malcpy(cm_handle *cm, void const *ptr, size_t size) { return 0; }

#else
void *cm_malcpy_loc(cm_handle *, void const *, size_t, char const *, int);
#define cm_malcpy(cm, ptr, size) \
  cm_malcpy_loc((cm), (ptr), (size), __FILE__, __LINE__)
#endif

/**
 * @brief duplicate a given number of typed values.
 *
 * The duplicate is allocated internally with cm_malloc()
 * and must, unless the allocator supports other means,
 * be free()d with cm_free() or cm_realloc().
 *
 * @param cm	the allocation module handle
 * @param type	the base type pointed to by ptr
 * @param ptr	memory that should be duplicated
 * @param nel	number of elements (of size sizeof(type)) to duplicate
 * @return NULL on allocation error or if @b nel is zero, otherwise
 * 	a pointer to a byte-by-byte duplicate of the @b nel elements
 *	pointed to by @b ptr.
 */
#define cm_tmalcpy(cm, type, ptr, nel) \
  ((ptr) ? ((type *)cm_malcpy((cm), (ptr), (nel) * sizeof(type))) : NULL)

/**
 * @brief duplicate a string
 * @param cm	the allocation module handle
 * @param ptr	NULL pointer to the beginning of a '\\0'-terminated string.
 * @return NULL on allocation error or if @b ptr is NULL, otherwise
 * 	a pointer to a duplicate of the string.
 */
#define cm_strmalcpy(cm, ptr)                                             \
  (((void *)0 != (ptr)) ? (char *)cm_malcpy((cm), (ptr), strlen(ptr) + 1) \
                        : NULL)

/* Same as cm_strmalcpy, but with a buffer pointer that cannot be NULL.
 * (To shut up gcc warnings.)
 */
#define cm_bufmalcpy(cm, ptr) ((char *)cm_malcpy((cm), (ptr), strlen(ptr) + 1))

/* cm-sprintf.c */

/**
 * @brief Return formatted data in dynamically allocated memory.
 *
 * The duplicate is allocated internally with cm_malloc()
 * and must, unless the allocator supports other means,
 * be free()d with cm_free() or cm_realloc().
 *
 * @warning
 * 	In spite of the dynamic interface, there's currently a fixed
 *	limit on the size of the returned string.  If you need truly
 * 	infinite output here, fix this function to dynamically reallocate
 *	its buffer when formatting fails with size-related errors.
 *
 * @param cm	allocation module handle to allocate in.
 * @param fmt	printf-style format string, followed by its arguments.
 * @return NULL on allocation error or if @b fmt is NULL, otherwise
 * 	a pointer to a duplicate of the string that resulted from
 *	printing the trailing arguments according to @b fmt.
 */
char *cm_sprintf(cm_handle *cm, char const *fmt, ...)
#if __GNUC__
    __attribute__((format(printf, 2, 3)))
#endif
    ;

/* cm-substr.c */

#ifdef DOCUMENTATION_GENERATOR_ONLY
/**
 * @brief Make a '\\0'-terminated duplicate of part of a string.
 *
 * The duplicate is allocated internally with cm_malloc()
 * and must, unless the allocator supports other means,
 * be free()d with cm_free() or cm_realloc().
 *
 * @param cm	allocation module handle to allocate in.
 * @param s	beginning of the substring to duplicate
 * @param e	end of the substring to duplicate
 * @return NULL on allocation error, otherwise
 * 	a pointer to a duplicate of the substring between
 *	s (inclusive) and e (exclusive), with an appended
 *	NUL terminating the substring.
 */
void *cm_substr(cm_handle *cm, char const *s, char const *e) { return 0; }
#else
void *cm_substr_loc(cm_handle *_cm, char const *_s, char const *_e,
                    char const *_file, int _line);

#define cm_substr(cm, s, e) (cm_substr_loc((cm), (s), (e), __FILE__, __LINE__))
#endif

/* cm-argv.c */

size_t cm_argvlen(char const *const *);
char **cm_argvarg(char const *const *, char const *);
int cm_argvpos(char const *const *, char const *);
char **cm_argvadd(cm_handle *, char **, char const *);
char **cm_argvdel(cm_handle *, char **, char const *);
void cm_argvfree(cm_handle *, char **);
char **cm_argvdup(cm_handle *, char const *const *);

/* cm-prefix.c */

/**
 * @brief A "prefix buffer".
 *
 * Supports the gradual recursive creation of hierarchical prefixes.
 */
typedef struct cm_prefix {
  /**
   * @brief The text itself.
   */
  char *pre_buffer;

  /**
   * @brief Number of bytes pointed to by pre_buffer.
   */
  size_t pre_size;

  /**
   * @brief Current write pointer for appending to the prefix.
   */
  size_t pre_offset;

} cm_prefix;

cm_prefix cm_prefix_initialize(char *buffer, size_t size);

cm_prefix cm_prefix_push(cm_prefix const *, char const *);
cm_prefix cm_prefix_pushf(cm_prefix const *, char const *, ...)
#if __GNUC__
    __attribute__((format(printf, 2, 3)))
#endif
    ;

char const *cm_prefix_end_bytes(cm_prefix const *, char const *, size_t);
char const *cm_prefix_end_string(cm_prefix const *, char const *);

/**
 * @brief A pointer just after the last byte of a prefix buffer
 */
#define cm_prefix_end(p, lit) cm_prefix_end_bytes((p), (lit), sizeof(lit) - 1)

/* cm-list.c */

/**
 * @brief Linked Lists
 *
 *	The offsets of the next and previous pointers are given
 *	by an instance (usually static) of cm_list_item_offsets.
 *	Indirecting in this manner allows clients to locate
 *	next and previous pointers anywhere in a structure and
 *	have them point at the structure instead of at the
 *	embedded list item.  Nice in the debugger.
 *
 *	{list,ring}_{push,pop} treat the head of a list like
 *      a stack.
 *	{list,ring}_{enqueue,dequeue} treat the tail of a list like
 *      a stack.
 *
 *	All list functions except "pop" are legal only on well-defined
 *	list states, and treat data errors and parameter errors
 *	as assertion failures.
 *
 *	Tail pointers are optional wherever it makes sense.
 *
 *	Example usage:
 *
 *	cm_list_offsets  my_offsets =
 *		CM_LIST_OFFSET_INIT( my_struct, ms_next, ms_prev );
 *
 * 	cm_list_remove(my_struct, my_offsets, &handle->head, NULL, item );
 */

typedef struct cm_list cm_list;

typedef struct cm_list_offsets {
  /** @brief offset of the next pointer in an element structure.
   */
  size_t lo_next;

  /** @brief offset of the previous pointer in an element structure.
   */
  size_t lo_prev;

} cm_list_offsets;

/**
 * @brief Initializer for a cm_list_offsets structure.
 *  Usage: static const cm_list_offsets my_offsets =
 *	CM_LIST_OFFSET_INIT(my_type, my_next, my_prev)
 * @param T__	the list element
 * @param N__	name of the next pointer
 * @param P__	name of the prev pointer
 */
#define CM_LIST_OFFSET_INIT(T__, N__, P__) \
  { offsetof(T__, N__), offsetof(T__, P__) }

/**
 * @brief Type-check a macro parameter
 *
 *  Macro used in macros below to have the compiler type check
 *  parameters against a type "TYPE" supplied in the parameter list.
 *
 * @param TYPE	the type the parameter should have
 * @param var	the parameter
 */
#define CM_LIST_TYPE(TYPE, var) ((void)(0 && (var) == (TYPE)0))

/**
 * @brief Insert an element at the head of a list.
 *
 * @param TYPE	the type of the list element, used for type checking in macro
 * @param o	a cm_list_offsets structure.
 * @param head	address of a pointer to the head element.
 * @param tail	address of a pointer to the tail element, or NULL
 * @param item	non-null item to insert.
 */
#define cm_list_push(TYPE, o, head, tail, item)                  \
  (CM_LIST_TYPE(TYPE **, (head)), CM_LIST_TYPE(TYPE **, (tail)), \
   CM_LIST_TYPE(TYPE *, (item)),                                 \
   cm_list_insert_after_i((o), (head), (tail), NULL, (cm_list *)(item)))

/**
 * @brief Remove an element from the beginning of a list.
 *
 * @param TYPE	the type of the list element, used for type checking in macro
 * @param o	a cm_list_offsets structure.
 * @param head	address of a pointer to the head element.
 * @param tail	address of a pointer to the tail element, or NULL
 * @return a pointer to the removed element, or NULL if the list is empty.
 */
#define cm_list_pop(TYPE, o, head, tail)                         \
  (CM_LIST_TYPE(TYPE **, (head)), CM_LIST_TYPE(TYPE **, (tail)), \
   (TYPE *)cm_list_pop_i((o), (head), (tail)))
cm_list *cm_list_pop_i(cm_list_offsets, void *, void *);

/**
 * @brief Append an element at the end of a list.
 *
 * @param TYPE	the type of the list element, used for type checking in macro
 * @param o	a cm_list_offsets structure.
 * @param head	address of a pointer to the head element.
 * @param tail	address of a pointer to the tail element, or NULL
 * @param item	non-null item to insert.
 */
#define cm_list_enqueue(TYPE, o, head, tail, item)               \
  (CM_LIST_TYPE(TYPE **, (head)), CM_LIST_TYPE(TYPE **, (tail)), \
   CM_LIST_TYPE(TYPE *, (item)),                                 \
   cm_list_enqueue_i((o), (head), (tail), (item)))
void cm_list_enqueue_i(cm_list_offsets, void *, void *, void *);

/**
 * @brief Remove an element from the end of a list.
 *
 * @param TYPE	the type of the list element, used for type checking in macro
 * @param o	a cm_list_offsets structure.
 * @param head	address of a pointer to the head element.
 * @param tail	address of a pointer to the tail element, or NULL
 */
#define cm_list_dequeue(TYPE, o, head, tail)                     \
  (CM_LIST_TYPE(TYPE **, (head)), CM_LIST_TYPE(TYPE **, (tail)), \
   (TYPE *)cm_list_dequeue_i((o), (head), (tail)))
cm_list *cm_list_dequeue_i(cm_list_offsets, void *, void *);

/**
 * @brief Insert an element before another one.
 *
 * @param TYPE	the type of the list element, used for type checking in macro
 * @param o	a cm_list_offsets structure.
 * @param head	address of a pointer to the head element.
 * @param tail	address of a pointer to the tail element, or NULL
 * @param here	element before which to insert the other, or NULL
 * @param item	element to insert
 */
#define cm_list_insert_before(TYPE, o, head, tail, here, item)   \
  (CM_LIST_TYPE(TYPE **, (head)), CM_LIST_TYPE(TYPE **, (tail)), \
   CM_LIST_TYPE(TYPE *, (here)), CM_LIST_TYPE(TYPE *, (item)),   \
   cm_list_insert_before_i((o), (head), (tail), (here), (item)))
void cm_list_insert_before_i(cm_list_offsets o, void *head, void *tail,
                             void *here, void *item);

/**
 * @brief Insert an element after another one.
 *
 * @param TYPE	the type of the list element, used for type checking in macro
 * @param o	a cm_list_offsets structure.
 * @param head	address of a pointer to the head element.
 * @param tail	address of a pointer to the tail element, or NULL
 * @param here	element before which to insert the other, or NULL
 * @param item	element to insert
 */
#define cm_list_insert_after(TYPE, o, head, tail, here, item)    \
  (CM_LIST_TYPE(TYPE **, (head)), CM_LIST_TYPE(TYPE **, (tail)), \
   CM_LIST_TYPE(TYPE *, (here)), CM_LIST_TYPE(TYPE *, (item)),   \
   cm_list_insert_after_i((o), (head), (tail), (here), (item)))
void cm_list_insert_after_i(cm_list_offsets, void *, void *, void *, void *);

/**
 * @brief Remove an element from anywhere in a doubly linked list.
 *
 * @param TYPE	the type of the list element, used for type checking in macro
 * @param o	a cm_list_offsets structure.
 * @param head	address of a pointer to the head element.
 * @param tail	address of a pointer to the tail element, or NULL
 * @param item	element to remove
 */
#define cm_list_remove(TYPE, o, head, tail, item)                \
  (CM_LIST_TYPE(TYPE **, (head)), CM_LIST_TYPE(TYPE **, (tail)), \
   CM_LIST_TYPE(TYPE *, (item)),                                 \
   cm_list_remove_i((o), (head), (tail), (item)))
void cm_list_remove_i(cm_list_offsets, void *, void *, void *);

/**
 * @brief Add an element at the head of a ring.
 *
 *  The element must not already be inserted into a list.
 *
 * @param TYPE	the type of the list element, used for type checking in macro
 * @param o	a cm_list_offsets structure.
 * @param head	address of a pointer to the head element.
 * @param item	element to insert
 */
#define cm_ring_push(TYPE, o, head, item)                       \
  (CM_LIST_TYPE(TYPE **, (head)), CM_LIST_TYPE(TYPE *, (item)), \
   cm_ring_push_i((o), (head), (item)))
void cm_ring_push_i(cm_list_offsets, void *, void *);

/**
 * @brief Add an element at the tail of a ring.
 *
 *  The element must not already be inserted into a list or ring.
 *
 * @param TYPE	the type of the list element, used for type checking in macro
 * @param o	a cm_list_offsets structure.
 * @param head	address of a pointer to the head element.
 * @param item	element to insert
 */
#define cm_ring_enqueue(TYPE, o, head, item)                    \
  (CM_LIST_TYPE(TYPE **, (head)), CM_LIST_TYPE(TYPE *, (item)), \
   cm_ring_enqueue_i((o), (head), (item)))
void cm_ring_enqueue_i(cm_list_offsets, void *, void *);

/**
 * @brief Remove an element from anywhere in a ring.
 *
 * @param TYPE	the type of the list element, used for type checking in macro
 * @param o	a cm_list_offsets structure.
 * @param head	address of a pointer to the head element.
 * @param item	element to remove
 */
#define cm_ring_remove(TYPE, o, head, item)                     \
  (CM_LIST_TYPE(TYPE **, (head)), CM_LIST_TYPE(TYPE *, (item)), \
   cm_ring_remove_i((o), (head), (item)))
void cm_ring_remove_i(cm_list_offsets, void *, void *);

/* cm-runtime-statistics.c */

void cm_runtime_statistics_max(cm_runtime_statistics *);

void cm_runtime_statistics_diff(cm_runtime_statistics const *_a,
                                cm_runtime_statistics const *_b,
                                cm_runtime_statistics *_c);

void cm_runtime_statistics_add(cm_runtime_statistics const *_a,
                               cm_runtime_statistics const *_b,
                               cm_runtime_statistics *_c);

bool cm_runtime_statistics_exceeds(cm_runtime_statistics const *_small,
                                   cm_runtime_statistics const *_large,
                                   cm_runtime_statistics *_report);

void cm_runtime_statistics_limit_below(
    cm_runtime_statistics const *_limit_below, cm_runtime_statistics *_large);

#endif /* CM_H */
