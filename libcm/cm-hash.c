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

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>


/* cm-hash.c -- A binary cm_hashtable.
 *
 * Maps octet strings to fixed-size objects.  All objects have the same size;
 * the keys can have arbitrary sizes.  The objects can be allocated
 * and retrieved, traversed in some random, but complete order;
 * given the pointer to an object, its key can be retrieved in
 * constant size.
 */

typedef struct slot {
  /* application data, possibly of size 0, sits here.
   */

  void *sl_next;         /* collision chain		   */
  unsigned long sl_hash; /* hash value of name		   */
  size_t sl_size;        /* size of application byte string */

  /* {size} bytes of hash value sit here.
   */
} slot;

/*  In the macros below, {base} points to the start of the
 *  (constant-, but run-time-sized) application data area;
 *  {h} is the cm_hashtable.
 */
#define INFO(h, base) ((slot *)((char *)(base) + (h)->h_value_size))
#define NEXT(h, base) (INFO(h, base)->sl_next)
#define SIZE(h, base) (INFO(h, base)->sl_size)
#define MEM(h, base) \
  ((void *)((char *)(base) + (h)->h_value_size + sizeof(slot)))

/*  Turn an array of {n} bytes into an unsigned long.
 */
static unsigned long hashf(unsigned char const *mem, size_t size) {
  register unsigned long i = 0;

  while (size--) i = i * 33 + *mem++;
  return i;
}

/*  The power of 2 that is greater or equal to {size}.
 */
static unsigned long hash_round(unsigned long size) {
  unsigned long i;

  if (size >= 1) size += size - 1;
  for (i = 1; i && i < size; i <<= 1)
    ;

  return i;
}

/**
 * @brief Initialize a hashtable-sized piece of storage.
 * Given the size of the fixed-size application data @b elsize
 * and the initial chunk size of the table @b m, initialize a
 * cm_hashtable object and allocate the first page that will
 * be given out later.
 *
 * @param cm allocator to allocate the hashtable contents in
 * @param h pointer to unitialized storage in the shape of a hashtable
 * @param elsize number of bytes in one fixed-size element
 * @param m initial element count.  If more elements are needed
 * 	later, the hashtable grows dynamically to adapt.
 *
 * @return 0 on success, otherwise a nonzero error code.
 * @return ENOMEM if the allocation failed.
 *
 *  This function is used by the cm_hashcreate() function,
 *  and by methods of objects that inherit from a cm_hashtable;
 *  plain users just use cm_hcreate() or cm_hashcreate().
 */
int cm_hashinit(cm_handle *cm, cm_hashtable *h, size_t elsize, int m) {
  h->h_cm = cm;

  /*  Pad the element size to multiples of sizeof(slot)
   */
  h->h_value_size =
      ((elsize + (sizeof(slot) - 1)) / sizeof(slot)) * sizeof(slot);

  /*  Round the chunk size until table selection and indexing become
   *  simple masking steps; initialize housekeeping information.
   */
  h->h_m = m = hash_round((unsigned long)m);
  h->h_mask = m - 1;      /* mask index in a table 	 */
  h->h_limit = m * 2 / 3; /* resize when two thidrs full	*/
  h->h_n = 0;             /* # of allocated elements.	 */

  /* Create the actual table of buckets.
   */
  {
    register void **c, **e;

    if (!(c = h->h_table = cm_talloc(h->h_cm, void *, m))) return -1;
    for (e = c + m; c < e; *c++ = (void *)0)
      ;
  }
  return 0;
}

/**
 * @brief Allocate a new hashtable.
 *
 *  Given the size of the fixed-size application data
 *  and the initial chunk size of the table m, allocate and
 *  initialize a cm_hashtable object.
 *
 * @param cm Memory manager to use
 * @param elsize Fixed size of a single element
 * @param m initial number of elements
 *
 * @return NULL on allocation failure, otherwise a pointer
 *  to the newly allocated hashtable.
 */
cm_hashtable *cm_hashcreate(cm_handle *cm, size_t elsize, int m) {
  cm_hashtable *h;

  if ((h = cm_talloc(cm, cm_hashtable, 1)) == NULL) return NULL;

  if (cm_hashinit(cm, h, elsize, m)) {
    cm_free(cm, h);
    return NULL;
  }
  return h;
}

/*  Double the number of slots available in a cm_hashtable.
 */
static int cm_hashgrow(cm_hashtable *h) {
  void **c;

  c = (void **)cm_realloc(h->h_cm, h->h_table, sizeof(void *) * h->h_m * 2);
  if (c == NULL) return ENOMEM;
  h->h_table = c;

  /*  Move entries into new chain slots if their hash value
   *  has the new bit set.
   */
  {
    register unsigned long const newbit = h->h_m;
    register void **o = (void **)h->h_table, **n = o + h->h_m,
                  **const N = n + h->h_m, **ne, **p;

    /* 	|--- old buckets ---|--- new buckets ---|
     *    o >>>               n >>>               N
     */
    for (; n < N; n++, o++) {
      ne = n; /*  ne points to the current end of the new chain */
      p = o;  /*  p  walks the old chain.			   */

      while (*p)
        if (INFO(h, *p)->sl_hash & newbit) {
          /* make *p skip an item; chain that item
           * into *ne instead; make the pointer to *p
           * point to next of *p.  (Pull on the rope.)
           */

          *ne = *p;
          ne = &NEXT(h, *ne);
          *p = *ne;
        } else
          p = &NEXT(h, *p); /* move along the rope */

      /*
       * Terminate the new chain.  (*p inherits the old terminator.)
       */
      *ne = (void *)0;
    }
  }

  h->h_mask |= h->h_m;
  h->h_m *= 2;
  h->h_limit = h->h_m * 2 / 3; /* recalculate limit	*/

  return 0;
}

/**
 * @brief Allocate a value in a hashtable
 *
 * Hash the key <mem, size> into the table.
 *  If it exists, return ...\n
 *  	... if alloc is #CM_HASH_CREATE_ONLY, a null pointer;\n
 * 	... otherwise a pointer to the key.\n
 *  if it doesn't exist, ...\n
 * 	... if alloc is #CM_HASH_READ_CREATE,
 *	allocate a new element and return its address;\n
 * 	... else, just return a null pointer.
 *
 *  The pointers returned point to the constant-size data storage, not
 *  to the hashed name; however, the name can be derived from the pointer
 *  to the storage in constant, short, time (using cm_hashmem(), cm_hashsize()).
 *
 * @param h the hashtable
 * @param mem pointer to the first byte of the key
 * @param size number of bytes pointed to by @b mem
 * @param alloc Allocation strategy:\n
 *	#CM_HASH_READ_ONLY: readonly; the key must exist.\n
 *	#CM_HASH_READ_CREATE: read, or create it (and fill with 0) if it doesn't
 *exist\n
 *	#CM_HASH_CREATE_ONLY: create; if the value already exists, return NULL.
 *
 * @return a pointer to the value data, or NULL if the call failed.
 */
void *cm_hash(cm_hashtable *h, void const *mem, size_t size, int alloc) {
  unsigned long i = hashf((unsigned char *)mem, size);
  void **s;
  void *e;
  slot *sl;

  for (s = h->h_table + (i & h->h_mask); *s; s = &sl->sl_next) {
    sl = INFO(h, *s);
    if (sl->sl_hash >= i) {
      if (sl->sl_hash > i) break;

      if (SIZE(h, *s) == size && !memcmp(MEM(h, *s), mem, SIZE(h, *s))) {
        if (alloc == CM_HASH_CREATE_ONLY) {
          errno = EEXIST;
          return NULL;
        }
        return *s;
      }
    }
  }
  if (alloc == CM_HASH_READ_ONLY) {
    errno = ENOENT;
    return NULL;
  }

  e = cm_malloc(h->h_cm, h->h_value_size + sizeof(slot) + size + 1);
  if (e == NULL) return NULL;

  memset(e, 0, h->h_value_size); /* user data 	*/
  memcpy(MEM(h, e), mem, size);
  ((char *)MEM(h, e))[size] = 0;
  sl = INFO(h, e);
  sl->sl_size = size;
  sl->sl_hash = i;

  /* chain in
   */
  sl->sl_next = *s;
  *s = e;

  /* count and do housekeeping if necessary.
   */
  h->h_n++;
  if (h->h_n >= h->h_limit) cm_hashgrow(h);

  return e;
}

/**
 * @brief Get next element from a hashtable, in arbitrary order.
 *
 * Repeated calls to cm_hashnext, using the previous result
 * as a second argument, step through all filled slots in the table.
 * (And can hence be used to simply dump the table).
 * @code
 *	char * p = 0; extern cm_hashtable * h;
 *	while (p = cm_hashnext(h, p)) ...
 * @endcode
 * To retrieve the first element, use a null pointer as the second argument.
 * When cm_hashnext() returns a null pointer, all elements have been visited
 * exactly once.
 *
 * @param h the hashtable
 * @param prev NULL or the previous value returned by a call.
 * @return NULL at the end, otherwise the element following @b prev
 *	(in some arbitrary, but fixed, order.)
 */
void *cm_hashnext(cm_hashtable const *h, void const *prev) {
  if (h == NULL) return NULL;
  if (prev && NEXT(h, prev))
    return NEXT(h, prev); /* same bucket chain */
  else {
    register void **s = h->h_table, **const e = s + h->h_m;

    /* linear scan for next bucket.
     */
    if (prev) s += (INFO(h, prev)->sl_hash & h->h_mask) + 1;
    while (s < e)
      if (*s++) return s[-1];
    return (void *)0;
  }
}

/**
 * @brief Destroy a cm_hashtable.
 *  This function is destined to be used by the cm_hashtable destroy
 *  function and by methods of objects that inherit from a cm_hashtable;
 *  plain users just call hdestroy() or hashdestroy().
 * @param h the hashtable
 */
void cm_hashfinish(cm_hashtable *h) {
  void **s;
  void **e;

  if (h == NULL || (s = h->h_table) == NULL) return;

  s = h->h_table;
  e = s + h->h_m;

  while (s < e) {
    register char *n = *s++, *p;
    while ((p = n) != NULL) {
      n = NEXT(h, p);
      cm_free(h->h_cm, p);
    }
  }
  cm_free(h->h_cm, h->h_table);
  h->h_table = NULL;
  h->h_m = 0;
}

/**
 * @brief Destroy a hashtable and free the hashtable object itself.
 *  If the hashtable is NULL, nothing happens.
 *
 *  Hash table elements that have not been deleted with hashdelete
 *  are free'd implicitly.
 *  If hash table elements contain pointers to other memory, those
 *  pointers are not free'd - the hash table doesn't know about the
 *  internal layout of the bytes it stores.
 *
 * @param h the hasthable, or NULL
 */
void cm_hashdestroy(cm_hashtable *h) {
  cm_handle *cm;

  if (h == NULL) return;

  cm = h->h_cm;
  cm_hashfinish(h);
  cm_free(cm, h);
}

/**
 * @brief Given a value pointer, return its key.
 * @param h the hashtable
 * @param value the value pointer
 */
void const *cm_hashmem(cm_hashtable const *h, void const *value) {
  if (value == NULL || h == NULL) return NULL;
  return MEM(h, value);
}

/**
 * @brief Given a value pointer, return the size of its key.
 * @param h the hashtable
 * @param value the value pointer
 */
size_t cm_hashsize(cm_hashtable const *h, void const *value) {
  if (value == NULL || h == NULL) return 0;
  return SIZE(h, value);
}

/**
 * @brief Delete a single entry from the hashtable.
 * @param h the hashtable to delete from
 * @param value pointer to a hash table value
 */
void cm_hashdelete(cm_hashtable *h, void *value) {
  void **s;

  if (h == NULL || value == NULL) return;

  s = h->h_table + (h->h_mask & INFO(h, value)->sl_hash);
  while (*s != value) s = &NEXT(h, *s);
  *s = NEXT(h, value);

  cm_free(h->h_cm, value);
  h->h_n--;
}

/**
 * @brief Create a deep copy of a hash table
 * @param h the hashtable to copy
 * @param out the hashtable to be copied into.
 *
 * Return out, if the copy was successful, null otherwise (ENOMEM)
 */
cm_hashtable *cm_hashcopy(cm_hashtable const *h, cm_hashtable *out) {
  if (!out) return 0;

  bool const out_initialized = out->h_cm && out->h_table && out->h_m &&
                               out->h_m == out->h_mask + 1 &&
                               out->h_limit == out->h_m * 2 / 3;

  if (out_initialized) cm_hashfinish(out);

  *out = *h;

  out->h_table = cm_talloc(out->h_cm, void *, out->h_m);
  memcpy(out->h_table, h->h_table, out->h_m * sizeof(void *));

  void **s = out->h_table;
  void **e = s + out->h_m;
  while (s < e) {
    void **p = s;
    void *hte = *s;

    while (hte) {
      *p = cm_malcpy(h->h_cm, hte,
                     h->h_value_size + sizeof(slot) + SIZE(h, hte) + 1);
      p = &NEXT(out, *p);
      hte = NEXT(out, hte);
    }
    s++;
  }

  return out;
}
