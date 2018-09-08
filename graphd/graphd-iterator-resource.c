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
#include "graphd/graphd.h"

#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

/*  resource/storable pointers
 *
 *  gir_next/gir_prev			doubly linked queue.  Deletions
 *					start at head.
 *
 *  gir_storable_next			singly linked hash chain.
 *
 *  g->g_iterator_resource		hashtable hash  -> gir record chain
 *  g->g_iterator_resource_stamp	hashtable stamp -> gir pointer
 *
 *  gir_stamp		 		resource -> stamp record
 */

static const cm_list_offsets graphd_iterator_resource_offsets =
    CM_LIST_OFFSET_INIT(graphd_iterator_resource, gir_next, gir_prev);

static void move_stamp(graphd_handle *g, graphd_iterator_resource *old,
                       graphd_iterator_resource *new) {
  if (old->gir_stamp != NULL) {
    cl_assert(g->g_cl, *(graphd_iterator_resource **)old->gir_stamp == old);
    cl_assert(g->g_cl, new->gir_stamp == NULL);

    *(graphd_iterator_resource **)old->gir_stamp = new;
    new->gir_stamp = old->gir_stamp;
    old->gir_stamp = NULL;
  }
}

static void move_chain(graphd_handle *g, graphd_iterator_resource *old,
                       graphd_iterator_resource *new) {
  graphd_iterator_resource *next = old->gir_next;

  /*  Chain out the old
   */
  cm_list_remove(graphd_iterator_resource, graphd_iterator_resource_offsets,
                 &g->g_iterator_resource_head, &g->g_iterator_resource_tail,
                 old);

  old->gir_prev = NULL;
  old->gir_next = NULL;

  /*  Chain in the new
   */
  cm_list_insert_before(
      graphd_iterator_resource, graphd_iterator_resource_offsets,
      &g->g_iterator_resource_head, &g->g_iterator_resource_tail, next, new);
}

static char const *renderchars(char const *s, char const *e, char *buf,
                               size_t bufsize) {
  char *w = buf, *buf_e = buf + bufsize;

  while (s < e && buf_e - w > 8) {
    if (isascii(*s) && isprint(*s))
      *w++ = *s++;
    else {
      snprintf(w, (size_t)(buf_e - w), "%%%2.2hx", (unsigned char)*s);
      w += strlen(w);
      s++;
    }
  }
  if (buf_e - w >= 8) {
    if (buf_e - w >= 4) {
      *w++ = '.';
      *w++ = '.';
      *w++ = '.';
    }
  }
  *w = '\0';
  return buf;
}

static size_t resource_size(graphd_handle *g,
                            graphd_iterator_resource const *gir) {
  return sizeof(*gir) + (gir->gir_storable
                             ? graphd_storable_size(gir->gir_storable)
                             : cm_hsize(&g->g_iterator_resource,
                                        graphd_iterator_resource, gir));
}

static void resource_flush(graphd_handle *g, graphd_iterator_resource *gir) {
  cl_handle *cl = g->g_cl;
  size_t size;
  char buf[1024 * 4];
  graphd_iterator_resource *gir2, **girp;
  graphd_storable *gs;
  unsigned long h;

  cl_assert(g->g_cl, g->g_iterator_resource_head != NULL);
  cl_assert(g->g_cl, gir->gir_stamp != NULL);
  cl_assert(g->g_cl, gir->gir_storable != NULL);
  cl_assert(g->g_cl, gir->gir_storable->gs_stored);
  cl_assert(g->g_cl, *(graphd_iterator_resource **)gir->gir_stamp == gir);

  cl_log(g->g_cl, CL_LEVEL_VERBOSE, "iterator-resource - %.*s -> \"%s\"",
         (int)cm_hsize(&g->g_iterator_resource_stamp,
                       graphd_iterator_resource *, gir->gir_stamp),
         (char const *)cm_hmem(&g->g_iterator_resource_stamp,
                               graphd_iterator_resource *, gir->gir_stamp),
         renderchars((char const *)cm_hmem(&g->g_iterator_resource,
                                           graphd_iterator_resource, gir),
                     (char const *)cm_hmem(&g->g_iterator_resource,
                                           graphd_iterator_resource, gir) +
                         cm_hsize(&g->g_iterator_resource,
                                  graphd_iterator_resource, gir),
                     buf, sizeof buf));

  /*  Remove the entry from the size accounting.
   */
  size = resource_size(g, gir);
  cl_assert(g->g_cl, size <= g->g_iterator_resource_size);
  g->g_iterator_resource_size -= size;

  /*  Remove the entry in the stamp hashtable that maps
   * the stamp we handed out to its resource pointer.
   */
  cm_hdelete(&g->g_iterator_resource_stamp, graphd_iterator_resource *,
             gir->gir_stamp);

  /*  Unlink the resource pointer from the reuse queue.
   */
  if (gir->gir_next == NULL && gir->gir_prev == NULL) {
    cl_assert(g->g_cl, g->g_iterator_resource_head == gir);
    cl_assert(g->g_cl, g->g_iterator_resource_tail == gir);
  }

  cm_list_remove(graphd_iterator_resource, graphd_iterator_resource_offsets,
                 &g->g_iterator_resource_head, &g->g_iterator_resource_tail,
                 gir);
  gir->gir_next = NULL;
  gir->gir_prev = NULL;

  /*  Unlink gir from the storable hash chain.
   */
  cl_assert(cl, gir->gir_storable != NULL);
  h = graphd_storable_hash(gir->gir_storable);
  gir2 = cm_haccess(&g->g_iterator_resource, graphd_iterator_resource, &h,
                    sizeof h);
  cl_assert(cl, gir2 != NULL);

  gs = gir->gir_storable;
  if (gir2 == gir) {
    if (gir->gir_storable_next) {
      /* Replace entry with successor;
       * free successor's memory
       */
      void *data = gir->gir_storable_next;

      move_stamp(g, data, gir);
      move_chain(g, data, gir);

      *gir = *gir->gir_storable_next;
      cm_free(g->g_cm, data);
    } else {
      /* Remove the record from
       * the hashtable; it's the only
       * one in its chain.
       */
      cm_hdelete(&g->g_iterator_resource, graphd_iterator_resource, gir);
    }
  } else {
    /*  Find gir's address in the hash chain.
     */
    girp = &gir2->gir_storable_next;
    while (*girp != NULL && *girp != gir) girp = &(*girp)->gir_storable_next;
    cl_assert(cl, *girp == gir);

    *girp = (*girp)->gir_next;
    cm_free(g->g_cm, gir);
  }

  gs->gs_stored = false;

  cl_log(cl, CL_LEVEL_VERBOSE, "resource_flush: unlink %p", (void *)gs);
  graphd_storable_unlink(gs);
}

static char const *resource_stamp(graphd_handle *g, char *buf, size_t bufsize) {
  snprintf(
      buf, bufsize, "%4.4lx%8.8lx%llu",
      (unsigned long)(g->g_predictable ? 0x0123 : getpid()),
      (unsigned long)(g->g_predictable ? 0x456789AB : srv_msclock(g->g_srv)),
      g->g_iterator_resource_id++);
  return buf;
}

graphd_iterator_resource *graphd_iterator_resource_storable_lookup(
    graphd_handle *g, graphd_storable const *gs) {
  graphd_iterator_resource *gir;
  unsigned long h = graphd_storable_hash(gs);

  /*  If we have that hash, walk its chain to see whether we
   *  have the whole object.
   *  If yes, that object is the storable name.
   *  Otherwise, make a new name.
   */
  gir = cm_haccess(&g->g_iterator_resource, graphd_iterator_resource, &h,
                   sizeof h);
  while (gir != NULL) {
    if (gir->gir_storable != NULL &&
        graphd_storable_equal(gir->gir_storable, gs))
      return gir;

    gir = gir->gir_storable_next;
  }
  return NULL;
}

static graphd_iterator_resource *graphd_iterator_resource_storable_allocate(
    graphd_handle *g, graphd_storable *gs) {
  graphd_iterator_resource *gir, **girp;
  unsigned long h = graphd_storable_hash(gs);

  /*  If we have that hash, walk its chain to see whether we
   *  have the whole object.
   *  If yes, return another link to it.
   *  Otherwise, make a new slot or hash chain entry, and
   *  return that.
   */
  gir = cm_haccess(&g->g_iterator_resource, graphd_iterator_resource, &h,
                   sizeof h);
  if (gir == NULL) {
    gir = cm_hnew(&g->g_iterator_resource, graphd_iterator_resource, &h,
                  sizeof h);
    if (gir == NULL) return NULL;

    return gir;
  }

  if (gir->gir_storable != NULL && graphd_storable_equal(gir->gir_storable, gs))
    return gir;

  girp = &gir->gir_next;
  while (*girp != NULL) {
    if ((*girp)->gir_storable != NULL &&
        graphd_storable_equal((*girp)->gir_storable, gs))
      return *girp;

    girp = &(*girp)->gir_storable_next;
  }

  /*  Allocate a new bucket.
   */
  *girp = cm_malloc(g->g_cm, sizeof(**girp));
  if (*girp != NULL) memset(*girp, 0, sizeof(**girp));
  return *girp;
}

int graphd_iterator_resource_initialize(graphd_handle *g) {
  cm_handle *cm = pdb_mem(g->g_pdb);
  int err;

  g->g_iterator_resource_id = 1;

  g->g_iterator_resource_head = NULL;
  g->g_iterator_resource_tail = NULL;
  g->g_iterator_resource_size = 0;
  g->g_iterator_resource_max = GRAPHD_ITERATOR_RESOURCE_MAX;

  err = cm_hashinit(cm, &g->g_iterator_resource,
                    sizeof(graphd_iterator_resource), 100);
  if (err != 0) return err;

  err = cm_hashinit(cm, &g->g_iterator_resource_stamp,
                    sizeof(graphd_iterator_resource *), 100);
  if (err != 0) {
    cm_hashfinish(&g->g_iterator_resource);
    return err;
  }

  return 0;
}

void graphd_iterator_resource_finish(graphd_handle *g) {
  while (g->g_iterator_resource_head != NULL)
    resource_flush(g, g->g_iterator_resource_head);

  cm_hashfinish(&g->g_iterator_resource);
  cm_hashfinish(&g->g_iterator_resource_stamp);
}

/**
 * @brief If you can find a place for the data, take a reference
 * 	 and store it.
 */
int graphd_iterator_resource_store(graphd_handle *g, graphd_storable *data,
                                   char *stamp_buf, size_t stamp_size) {
  graphd_storable *gs = data;
  graphd_iterator_resource *gir;
  char const *stamp;

  size_t size;

  /* Don't melt down. (And don't use more than 1/2 of ram for one resource)
   */
  size = graphd_storable_size(gs);
  if (size + sizeof(*gir) > g->g_iterator_resource_max / 2) {
    cl_log(g->g_cl, CL_LEVEL_FAIL,
           "graphd_iterator_resource_store: "
           "silently dropping %zu bytes -"
           " maximum acceptable is %llu",
           size, g->g_iterator_resource_max);

    stamp_buf[0] = 'x';
    stamp_buf[1] = '\0';

    return 0;
  }

  gir = graphd_iterator_resource_storable_allocate(g, gs);
  if (gir == NULL) return ENOMEM;

  if (gir->gir_stamp == NULL) {
    graphd_iterator_resource **gir_stamp;

    /*  Make a new claims ticket for this resource.
     */
    stamp = resource_stamp(g, stamp_buf, stamp_size);
    gir_stamp = cm_hexcl(&g->g_iterator_resource_stamp,
                         graphd_iterator_resource *, stamp, strlen(stamp));
    if (gir_stamp == NULL) return errno ? errno : ENOMEM;

    /*  Account for the claims ticket in the
     *  iterator size.
     */
    g->g_iterator_resource_size += size + sizeof(*gir);

    cl_log(g->g_cl, CL_LEVEL_DEBUG, "iterator-resource %p size=%zu, total %llu",
           (void *)gir, size + sizeof(*gir), g->g_iterator_resource_size);

    /*  Link from the ticket to the gir, and from the
     *  gir to the ticket.
     */
    *gir_stamp = gir;
    gir->gir_stamp = (void *)gir_stamp;

    gir->gir_next = NULL;
    gir->gir_prev = NULL;

    /*  Place the storable in the resource record.
     */
    gir->gir_storable = gs;
    graphd_storable_link(gs);
    gs->gs_stored = true;

    cl_assert(g->g_cl, gir->gir_storable == gs);
  } else {
    size_t size;

    /*  Since we already have that resource,
     *  just return its existing tag, and move
     *  it to the tail of the LRU chain.
     */
    size = cm_hsize(&g->g_iterator_resource_stamp, graphd_iterator_resource *,
                    gir->gir_stamp);
    if (size >= stamp_size) return GRAPHD_ERR_MORE;

    /*  Tell the caller about the existing
     *  claims ticket they're now sharing.
     */
    memcpy(stamp_buf, cm_hmem(&g->g_iterator_resource_stamp,
                              graphd_iterator_resource *, gir->gir_stamp),
           size);
    stamp_buf[size] = '\0';

    /*  Remove the resource from its chain.  We'll
     *  enqueue it again below.  This effectively
     *  moves it back to the end of the queue,
     *  delaying its deletion.
     */
    cm_list_remove(graphd_iterator_resource, graphd_iterator_resource_offsets,
                   &g->g_iterator_resource_head, &g->g_iterator_resource_tail,
                   gir);
  }

  /*  Append the resource at the end of the LRU queue.
   *  That's the best spot - we'll be deleting from the
   *  head once we fill up.
   */
  cm_list_enqueue(graphd_iterator_resource, graphd_iterator_resource_offsets,
                  &g->g_iterator_resource_head, &g->g_iterator_resource_tail,
                  gir);

  /*  If that put us over the allowed size, free some old records.
   */
  if (g->g_iterator_resource_size > g->g_iterator_resource_max) {
    size_t old = g->g_iterator_resource_size;
    while (g->g_iterator_resource_size > g->g_iterator_resource_max / 2) {
      cl_assert(g->g_cl, g->g_iterator_resource_head);
      resource_flush(g, g->g_iterator_resource_head);
    }
    cl_log(g->g_cl, CL_LEVEL_DEBUG,
           "graphd_iterator_resource_store: freed "
           "%llu bytes of iterator resources",
           (unsigned long long)(old - g->g_iterator_resource_size));
  }
  return 0;
}

/*  If a storable is found, return a shared link to it.
 */
static void *graphd_iterator_resource_lookup(graphd_handle *g,
                                             char const *stamp_s,
                                             char const *stamp_e) {
  graphd_iterator_resource **gir_stamp;
  cl_handle *cl = g->g_cl;

  if (stamp_s >= stamp_e || !isascii(*stamp_s) || !isalnum(*stamp_s)) {
    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_iterator_resource_lookup: "
           "bad stamp format \"%.*s\"",
           (int)(stamp_e - stamp_s), stamp_s);
    return NULL;
  }

  gir_stamp =
      cm_haccess(&g->g_iterator_resource_stamp, graphd_iterator_resource *,
                 stamp_s, stamp_e - stamp_s);
  if (gir_stamp == NULL) {
    /* Valid parse, but not found  */

    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_iterator_resource_lookup: MISS can't "
           "find \"%.*s\"",
           (int)(stamp_e - stamp_s), stamp_s);
    return NULL;
  }

  cl_assert(cl, (*gir_stamp)->gir_storable != NULL);
  (*gir_stamp)->gir_used = true;

  return (*gir_stamp)->gir_storable;
}

/**
 * @brief Return a fresh reference to the thawed resource
 *  	referenced by *s_ptr ... e, which must have the
 *	type <expected_type>.  NULL if anything goes wrong.
 */
void *graphd_iterator_resource_thaw(graphd_handle *g, char const **s_ptr,
                                    char const *e,
                                    graphd_storable_type const *expected_type) {
  char const *s;
  graphd_storable *gs;

  s = *s_ptr;
  while (s < e && isascii(*s) && (isxdigit(*s) || *s == 'x')) s++;

  gs = graphd_iterator_resource_lookup(g, *s_ptr, s);
  *s_ptr = s;

  if (gs == NULL) return NULL;

  if (expected_type != NULL && gs->gs_type != expected_type) return NULL;

  cl_log(g->g_cl, CL_LEVEL_VERBOSE,
         "graphd_iterator_resource_thaw: return %p[%zu]", (void *)gs,
         (size_t)gs->gs_linkcount);

  graphd_storable_link(gs);
  return gs;
}
