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
/* For O_NOATIME
 */
#define _GNU_SOURCE

#include "libaddb/addbp.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/*
 *  Printing the in-core status takes a lot of time,
 *  proportional to the amount of memory in core.
 *
 *  Doing it every ten seconds is more than we can
 *  afford.  Let's turn it off for now and worry about
 *  a more efficient method later.
 */
#undef SHOW_STATUS_IN_CORE

/* The stages of the index checkpoint process
 */
typedef enum addb_checkpoint_stage {
  ADDB_CKS_DONE,
  ADDB_CKS_FINISH_BACKUP,
  ADDB_CKS_SYNC_BACKUP,
  ADDB_CKS_START_WRITES1,
  ADDB_CKS_START_WRITES2,
  ADDB_CKS_FINISH_WRITES,
  ADDB_CKS_REMOVE_BACKUP,
  ADDB_CKS_N, /* number of checkpoint stages */

} addb_checkpoint_stage;

/*  Accessing a changing, possibly very large file,
 *  mapped into memory as needed.
 *
 *  Rules:
 *
 *      Tiles are a multiple of a fixed size (the "tile size"),
 *	and start at offsets that are multiples of that size.
 *
 *	If an area of memory is requested that is larger than a tile,
 * 	it lives alone in a slab whose size is a multiple of the tile size.
 * 	It need not start at the beginning of that slab, but it must be
 *	the only occupant.
 *
 *	Once an object of a certain size has been requested, it never
 *	changes size.  (It can change value.)
 */

typedef struct addb_mmap_slot {
  size_t mm_size;
  char const* mm_file;
  int mm_line;
} addb_mmap_slot;

/*  The tile structure is used to manage access to persistent data
 *
 *  Tiles live in zero or one of several lists:
 *
 *	tdp_free_head (tiled pool)
 *		The list of tiles which are not in use (refcount == 0),
 *		not dirty or pending, and hence available for re-use.
 *
 *	td_dirty_head (tiled file)
 *		All dirty tiles which are not scheduled.  They may
 *		or may not be in use.
 *
 *	td_scheduled_head (tiled file)
 *		All scheduled tiles whether in-use or not.  A tile
 *		which is both dirty and scheduled is kept on the
 *		scheduled list
 *
 *  A tile which is in use for reading and not dirty or pending will
 *  not be on any list.  These are rare because reads go direct to
 *  mapped memory whenever possible.
 *
 *  Tiles are reference-counted when in use.  A tile's memory is only
 *  usable when the reference count is non-zero.
 *
 *  Use functions to change the dirty bits, don't access them directly;
 *  tiles must be moved between queues as part of marking them.
 */

typedef struct addb_tile addb_tile;
struct addb_tile {
  void* tile_memory;
  void* tile_memory_disk;
  void* tile_memory_scheduled;
  addb_tile* tile_prev;
  addb_tile* tile_next;
  addb_tiled* tile_td; /* tiled file containing this tile */
  size_t tile_i;       /* index of this tile in td_tile */
  size_t tile_reference_count;

  unsigned short tile_dirty_bits;
  /*  dirty bit for each page in the tile */
  unsigned short tile_scheduled_bits;
};

#define ADDB_TILE_IS_DIRTY(T__) \
  ((T__) && ((T__)->tile_dirty_bits || (T__)->tile_scheduled_bits))

struct addb_tiled {
  int td_fd;
  char* td_path;

  /*  <td_tile> points to <td_tile_m> tile pointers.   The pointers
   *  can be NULL (for tiles that have not yet actually been mapped,
   *  or are back parts of larger slabs that span multiple tiles.)
   *  As the file grows, td_tile can be reallocated to also grow.
   */
  addb_tile** td_tile;
  size_t td_tile_m;

  /*  The initial mmap'd region
   */
  void* td_first_map;

  /*  A list for tiles that are dirty and need to
   *  be written to disk.
   *
   *  The list is circular; each element has both a
   *  prev and a next.
   */
  addb_tile* td_dirty_head;

  /*  The number of dirty tiles in the preceeding list.
   */
  size_t td_tile_dirty;

  /*  A circular list for tiles that are being written to disk
   */
  addb_tile* td_scheduled_head;

  /*  The total number of bytes that have been mapped into storage,
   *  and of those, the total number of bytes that are actually
   *  being referenced.  (The difference between td_total and
   *  td_total_linked is the amount of memory currently sitting in
   *  the free list.)
   */
  unsigned long long td_total;
  unsigned long long td_total_linked;

  /*  The size of the underlying file.
   */
  unsigned long long td_physical_file_size;

  /*  The size of the initial map
   */
  unsigned long long td_first_map_size;

  /* The common pool that this tiled file shares resources with.
   */
  struct addb_tiled_pool* td_pool;
  addb_tbk td_tbk; /* backup file information */

  /*  Where are we in the process of writing tiles to disk?
   */
  addb_checkpoint_stage td_checkpoint_stage;
  unsigned int td_locked : 1; /*  Are we locked in memory? */

  /*  Do we do backup in advance?  We try to, but if it fails,
   *  it's not the end of the world.
   */
  unsigned int td_advance_backup : 1;

  /* Have we started mmapping individual tiles?
   */
  unsigned int td_mmap_indv_tile : 1;
};

/* Compute the number of tiles in the initial mmap region
 */
unsigned long long addb_tiled_first_map(addb_tiled* td) {
  return td->td_first_map_size / ADDB_TILE_SIZE;
}

struct addb_tiled_pool {
  cl_handle* tdp_cl;
  cm_handle* tdp_cm;
  addb_handle* tdp_addb;

  /*  NULL for empty, otherwise the tile cache and first tile index
   *  of the free list, a doubly-linked ring via tile_prev, .._next.
   */
  addb_tile* tdp_free_head;

  /*  The total number of bytes that have been mapped into storage,
   *  and of those, the total number of bytes that are actually
   *  being referenced.  (The difference between td_total and
   *  td_total_linked is the amount of memory currently sitting in
   *  the free list.)
   *
   *  tdp_total is always >= tdp_total_linked.
   */
  unsigned long long tdp_total;
  unsigned long long tdp_total_linked;

  /* If there's more than this many bytes mapped, we'll
   * start unmapping tiles.
   */
  unsigned long long tdp_max;

  /* The number of times we've called tile_map
   */
  unsigned long long tdp_map_count;

  /* The number of times tile_map was able to
   * satisfy a request without mapping a new page.
   */
  unsigned long long tdp_map_cached;

  /*  Resource tracking -- keep track separately of
   *  who occupies what.
   */
  cm_hashtable tdp_mmaps;

  /* The number of bytes written to tiled files in this pool.
   */
  unsigned long long tdp_bytes_written;

  /* True if we have mmaped an individual tile.
   */
  bool tdp_have_mmapped_tile;
};

#define ADDB_TILED_TREF_MAKE_INITMAP(size) ((size_t)(-1L - (long)(size)))
#define ADDB_TILED_TREF_IS_INITMAP(ref) ((long)(ref) < -1)
#define ADDB_TILED_TREF_INITMAP_SIZE(ref) (-((long)(ref) + 1))

static inline void addb_physical_tile_link(cl_handle* cl, addb_tiled_pool* tdp,
                                           addb_tiled* td, addb_tile* tile,
                                           addb_tiled_reference const* tref) {
  cl_assert(cl, *tref != (size_t)-1);
  cl_assert(cl, !ADDB_TILED_TREF_IS_INITMAP(*tref));

  if (0 == tile->tile_reference_count++) {
    td->td_total_linked += ADDB_TILE_SIZE;
    tdp->tdp_total_linked += ADDB_TILE_SIZE;
    cl_assert(cl, tdp->tdp_total >= tdp->tdp_total_linked);
  }
}

/*  Passed to cm_list_.*(addb_tile, ...) calls
 */
static const cm_list_offsets addb_tile_offsets =
    CM_LIST_OFFSET_INIT(addb_tile, tile_next, tile_prev);

static void mmap_resource_add(addb_tiled_pool* tdp, void* ptr, size_t size,
                              char const* file, int line) {
  addb_mmap_slot* sl;
  cl_handle* cl = tdp->tdp_cl;

  sl = cm_hexcl(&tdp->tdp_mmaps, addb_mmap_slot, &ptr, sizeof(ptr));
  if (sl == NULL) {
    sl = cm_haccess(&tdp->tdp_mmaps, addb_mmap_slot, &ptr, sizeof(ptr));
    if (sl != NULL) {
      cl_wnotreached(cl,
                     "HEY! %s:%d: duplicate mmap for "
                     "pointer %p -- already mapped by %s:%d, "
                     "size %lu",
                     file, line, ptr, sl->mm_file, sl->mm_line,
                     (unsigned long)sl->mm_size);
    } else {
      cl_log(cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
             "mmap successful, but can't insert "
             "resource tracking record: %s",
             strerror(errno));
    }
  } else {
    sl->mm_size = size;
    sl->mm_file = file;
    sl->mm_line = line;
  }
}

static void mmap_resource_delete(addb_tiled_pool* tdp, void* ptr, size_t size,
                                 char const* file, int line) {
  addb_mmap_slot* sl;
  cl_handle* cl = tdp->tdp_cl;

  sl = cm_haccess(&tdp->tdp_mmaps, addb_mmap_slot, &ptr, sizeof(ptr));
  if (sl == NULL) {
    cl_notreached(cl,
                  "%s:%d: munmap for nonexistant "
                  "pointer %p",
                  file, line, ptr);
  } else {
    if (sl->mm_size != size) {
      cl_notreached(cl,
                    "%s:%d: mmap for "
                    "pointer %p with different size (%lu) from the "
                    "original %lu at %s:%d",
                    file, line, ptr, (unsigned long)size,
                    (unsigned long)sl->mm_size, sl->mm_file, sl->mm_line);
    }
    cm_hdelete(&tdp->tdp_mmaps, addb_mmap_slot, sl);
  }
}

/* Which list should a tile be on?
 */
static addb_tile** tile_which_list(addb_tile* tile) {
  if (tile->tile_scheduled_bits) return &tile->tile_td->td_scheduled_head;

  if (tile->tile_dirty_bits) {
    cl_assert(tile->tile_td->td_pool->tdp_cl, tile->tile_memory_disk);
    return &tile->tile_td->td_dirty_head;
  }

  if (0 == tile->tile_reference_count &&
      (tile->tile_i >= addb_tiled_first_map(tile->tile_td)))
    return &tile->tile_td->td_pool->tdp_free_head;

  return (addb_tile**)0;
}

/**
 * @brief Remove a tile from a ring.
 *
 *	We use a ring so we can know that a tile
 *	is on a list iff the next pointer is set
 */
static void tile_chain_out(addb_tile* tile) {
  cl_handle* const cl = tile->tile_td->td_pool->tdp_cl;
  addb_tile** const headp = tile_which_list(tile);

  if (!headp) {
    cl_assert(cl, !tile->tile_prev);
    cl_assert(cl, !tile->tile_next);

    return;
  }

  cm_ring_remove(addb_tile, addb_tile_offsets, headp, tile);
}

/* @brief Add a tile to a ring.
 */
static void tile_chain_in(addb_tile* tile) {
  cl_handle* const cl = tile->tile_td->td_pool->tdp_cl;
  addb_tile** const headp = tile_which_list(tile);

  cl_assert(cl, !tile->tile_prev);
  cl_assert(cl, !tile->tile_next);

  if (!headp) return; /* tile doesn't belong on any list */

  cm_ring_push(addb_tile, addb_tile_offsets, headp, tile);

  cl_assert(cl, tile->tile_prev);
  cl_assert(cl, tile->tile_next);
}

/**
 * @brief Allocate an internal tile management structure.
 *  This just does the malloc -- it doesn't map anything.
 */
static addb_tile* tile_alloc(addb_tiled* td, size_t tile_i) {
  addb_tile* const tile = cm_talloc(td->td_pool->tdp_cm, addb_tile, 1);

  if (!tile) {
    cl_log(td->td_pool->tdp_cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
           "addb: failed to allocate %lu bytes for tile [%s:%d]",
           (unsigned long)sizeof(addb_tile), __FILE__, __LINE__);
    return NULL;
  }

  tile->tile_memory = (void*)0;
  tile->tile_memory_disk = (void*)0;
  tile->tile_memory_scheduled = (void*)0;
  tile->tile_prev = (addb_tile*)0;
  tile->tile_next = (addb_tile*)0;
  tile->tile_i = tile_i;
  tile->tile_td = td;
  tile->tile_reference_count = 0;
  tile->tile_dirty_bits = 0;
  tile->tile_scheduled_bits = 0;

  td->td_tile[tile_i] = tile;

  return tile;
}

/* @brief Unmap a tile structure.
 *
 *  Unconditionally frees copy-on-write memory,
 *  and attempts to free the memory mapped storage
 *  underlying a tile.  That may fail; if it does,
 *  the call returns an error code.
 *
 *  The tile may still be in a free list.  (It'll be
 *  chained out only if the munmap works.)
 *
 * @param td 		The tile store
 * @param tile_i 	Index of the tile to free
 * @return 0 if the unmap succeeds, otherwise a nonzero
 *  	error code.
 */
static int tile_unmap(addb_tiled* td, size_t tile_i) {
  addb_tiled_pool* const tdp = td->td_pool;
  addb_tile* const tile = td->td_tile[tile_i];

  cl_assert(tdp->tdp_cl, tile_i < td->td_tile_m);
  cl_assert(tdp->tdp_cl, tile != NULL);
  cl_assert(tdp->tdp_cl, tile->tile_reference_count == 0);
  cl_assert(tdp->tdp_cl, tdp->tdp_total >= tdp->tdp_total_linked);

  if (tile->tile_memory_disk != NULL) {
    /* Throw away the modified copy without writing it.
     */
    cm_free(tdp->tdp_cm, tile->tile_memory);
    tile->tile_memory = tile->tile_memory_disk;
    tile->tile_memory_disk = NULL;
  }

  if (tile->tile_memory != NULL && tile_i >= addb_tiled_first_map(td)) {
    int err = addb_file_munmap(tdp->tdp_cl, td->td_path, tile->tile_memory,
                               ADDB_TILE_SIZE);
    if (err) return err;

    mmap_resource_delete(tdp, tile->tile_memory, ADDB_TILE_SIZE, __FILE__,
                         __LINE__);
    tile->tile_memory = NULL;
    cl_log(tdp->tdp_cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE,
           "tile: unmap %s.%lu", td->td_path, (unsigned long)tile_i);
  }

  cl_cover(tdp->tdp_cl);

  return 0;
}

/* @brief Free (and unmap, if it is still mapped) a tile structure.
 *
 *  It must not be in the free list, and must not have references.
 *  Contents of the tile do NOT automatically get flushed to disk.
 *  (They must have been flushed externally before calling this.)
 *
 * @param td 		The tile store
 * @param tile_i 	Index of the tile to free
 */
static void tile_free(addb_tile* tile) {
  addb_tiled* const td = tile->tile_td;
  addb_tiled_pool* const tdp = td->td_pool;
  bool const dirty = ADDB_TILE_IS_DIRTY(tile);

  cl_assert(tdp->tdp_cl, !tile->tile_prev);
  cl_assert(tdp->tdp_cl, !tile->tile_next);

  cl_assert(tdp->tdp_cl, tile->tile_reference_count == 0);
  cl_assert(tdp->tdp_cl, tdp->tdp_total >= tdp->tdp_total_linked);

  (void)tile_unmap(td, tile->tile_i);

  td->td_tile[tile->tile_i] = NULL;

  if (tile->tile_i >= addb_tiled_first_map(td)) {
    cl_assert(tdp->tdp_cl, tdp->tdp_total >= tdp->tdp_total_linked);

    td->td_total -= ADDB_TILE_SIZE;
    tdp->tdp_total -= ADDB_TILE_SIZE;

    cl_assert(tdp->tdp_cl, tdp->tdp_total >= tdp->tdp_total_linked);
  }

  cl_log(tdp->tdp_cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE,
         "tile: free %s.%lu %s", td->td_path, (unsigned long)tile->tile_i,
         dirty ? "dirty" : "clean");

  cm_free(tdp->tdp_cm, tile);
}

/*  To make space for new tiles to be allocated, release
 *  old tiles from the recycling chain.  (The tile data structure
 *  survives -- it's still linked into its partition -- but the file
 *  data it referred to is released.)
 *
 *  We evict tiles on a LRU basis by starting at the end of the
 *  free list and moving backwards towards the head.
 *
 *  Return 1 if anything actually got released, 0 otherwise.
 */

static bool tiled_pool_flush(addb_tiled_pool* tdp, unsigned long long need) {
  cl_handle* const cl = tdp->tdp_cl;
  addb_tile* tile;
  addb_tile* const last_tile = tdp->tdp_free_head;
  bool any = false;

  cl_enter(cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE, "(%llu bytes)", need);
  cl_assert(tdp->tdp_cl, tdp->tdp_total >= tdp->tdp_total_linked);

  if (!tdp->tdp_free_head) {
    cl_leave(cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE, "free list is empty!");

    return false;
  }
  tile = tdp->tdp_free_head->tile_prev;

  while (need > 0 && tile) {
    addb_tile* const next_tile = tile->tile_prev; /* moving backwards */

    cl_assert(cl, 0 == tile->tile_reference_count);
    cl_assert(cl, tile->tile_next);
    cl_assert(cl, tile->tile_prev);
    cl_assert(cl, !tile->tile_dirty_bits);

    if (ADDB_TILE_SIZE >= need)
      need = 0;
    else
      need -= ADDB_TILE_SIZE;

    tile_chain_out(tile);
    tile_free(tile);
    any = true;

    if (last_tile == tile)
      tile = 0;
    else
      tile = next_tile;
  }

  cl_leave(cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE, "need: %llu", need);

  return any;
}

/*  Make sure an existing tile is mapped.  It starts at tile offset
 *  tile_i*td->td_pool->tdp_size, and is td->td_pool->tdp_size large.
 *
 *  The tile data structure must exist, must not be in the free list,
 *  and must not have links.
 */
static int tile_map(addb_tiled* td, addb_tiled_pool* tdp, addb_tile* tile,
                    size_t tile_i) {
  cl_handle* cl = tdp->tdp_cl;
  int err = 0;

  cl_assert(cl, tile_i < td->td_tile_m);
  cl_assert(cl, tile_i + 1 <= td->td_tile_m);
  cl_assert(cl, tile != NULL);

  tdp->tdp_map_count++;
  if (tile->tile_memory) {
    td->td_pool->tdp_map_cached++;

    return 0;
  }

  if (tile_i < addb_tiled_first_map(td)) {
    /*  Use the large first map memory.
     */
    tile->tile_memory = td->td_first_map + (tile_i * ADDB_TILE_SIZE);
  } else {
    /*  If the allocated tile memory would have us exceed the
     *  policy maximum, try to flush out some old tiles before
     *  allocating the new ones.
     */
    if (tdp->tdp_total + ADDB_TILE_SIZE > tdp->tdp_max) {
      if (tdp->tdp_total >= tdp->tdp_max)
        (void)tiled_pool_flush(
            tdp, (ADDB_TILE_SIZE + (tdp->tdp_total - tdp->tdp_max)));
      else
        (void)tiled_pool_flush(
            tdp, (ADDB_TILE_SIZE - (tdp->tdp_max - tdp->tdp_total)));
      cl_cover(cl);
    }

    if (!tdp->tdp_have_mmapped_tile) {
      tdp->tdp_have_mmapped_tile = true;
      cl_log(cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
             "addb: mmapped individual tile in %s at %llu: ", td->td_path,
             (unsigned long long)((off_t)tile_i * ADDB_TILE_SIZE));
    }

    tile->tile_memory =
        mmap(0, ADDB_TILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, td->td_fd,
             (off_t)tile_i * ADDB_TILE_SIZE);
    /* offset into file*/

    if (MAP_FAILED == tile->tile_memory) {
      err = errno ? errno : -1;
      cl_log(cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
             "addb: failed to mmap tile at %llu: "
             "%s [%s:%d]",
             (unsigned long long)((off_t)tile_i * ADDB_TILE_SIZE),
             strerror(errno), __FILE__, __LINE__);

      tile->tile_memory = NULL;

      return err;
    }
    cl_log(cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE, "tile: %p mapped",
           (void*)tile->tile_memory);

    /*  Debug: insert resource tracking record.
     */
    mmap_resource_add(tdp, tile->tile_memory, ADDB_TILE_SIZE, __FILE__,
                      __LINE__);
  }
  td->td_total += ADDB_TILE_SIZE;
  tdp->tdp_total += ADDB_TILE_SIZE;

  cl_assert(cl, tdp->tdp_total >= tdp->tdp_total_linked);
  cl_assert(cl, tile->tile_memory != NULL);
  cl_assert(cl, !tile->tile_prev);
  cl_assert(cl, !tile->tile_next);

  cl_cover(cl);

  return 0;
}

/**
 * @brief Make sure a tile structure is ready for use.
 *
 *  Make sure the tile has a slot and a small tile
 *  structure, and isn't in the free list.
 *  This doesn't yet map or link anything.
 *
 *  The returned slot must either be placed in the
 *  free list or linkcounted.
 *
 * @param td	tiled partition to allocate in
 * @param tile_i	where to allocate
 *
 * @return 0 if a tile structure has been placed at the slot.
 */
static int tiled_grow(addb_tiled* td, size_t tile_i) {
  addb_tiled_pool* const tdp = td->td_pool;
  cl_handle* const cl = tdp->tdp_cl;
  addb_tile* tile;

  if (tile_i + 1 > td->td_tile_m) {
    addb_tile **tmp, **e;
    size_t m = tile_i + 1024;

    tmp = cm_trealloc(tdp->tdp_cm, addb_tile*, td->td_tile, m);
    if (tmp == NULL) {
      cl_log(cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
             "addb: failed to allocate %lu bytes "
             "for tile cache",
             (unsigned long)(sizeof(addb_tile*) * m));
      return errno ? errno : ENOMEM;
    }

    td->td_tile = tmp;
    e = tmp + m;
    tmp += td->td_tile_m;
    td->td_tile_m = m;

    /* Initialize the new tile slots we just allocated */
    while (tmp < e) *tmp++ = NULL;
  }

  cl_assert(cl, tile_i + 1 <= td->td_tile_m);

  tile = td->td_tile[tile_i];
  if (tile) {
    /*  We have a tile already.
     *  If it was in the free ring, chain it out.
     */
    if (tile->tile_prev && tile_which_list(tile) == &tdp->tdp_free_head)
      tile_chain_out(tile);
  } else {
    /* No tile at this location, allocate a new one.
     */
    cl_log(cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE, "tile: allocate %s.%lu",
           td->td_path, (unsigned long)tile_i);

    tile = tile_alloc(td, tile_i);
    if (!tile) return ENOMEM;
  }

  return 0;
}

/*  Increment the reference count for a tile.
 */
void addb_tiled_link_loc(addb_tiled* td, addb_tiled_reference const* tref,
                         char const* file, int line) {
  addb_tiled_pool* tdp = td->td_pool;
  addb_tile* tile;

  if ((size_t)-1 == *tref)
    return;

  else if (ADDB_TILED_TREF_IS_INITMAP(*tref)) {
    long size = ADDB_TILED_TREF_INITMAP_SIZE(*tref);

    td->td_total_linked += size;
    td->td_pool->tdp_total_linked += size;

    td->td_total += size;
    td->td_pool->tdp_total += size;

    cl_assert(tdp->tdp_cl, tdp->tdp_total >= tdp->tdp_total_linked);
  } else {
    tile = td->td_tile[*tref];
    cl_assert(tdp->tdp_cl, tile != NULL);
    addb_physical_tile_link(tdp->tdp_cl, tdp, td, tile, tref);
  }
}

/**
 * @brief  Look at N bytes from a tiled file.  Very Quickly.
 *
 * @return NULL if we couldn't do this quickly.  The caller
 *	should retry with tiled_get/tiled_free.
 */

unsigned char const* addb_tiled_peek(addb_tiled* td, unsigned long long offset,
                                     size_t len) {
  size_t tile_i;
  addb_tile* tile;

  if (offset > td->td_physical_file_size - len)
    return NULL; /* value not in physical file */

  tile_i = offset / ADDB_TILE_SIZE;
  tile = tile_i < td->td_tile_m ? td->td_tile[tile_i] : NULL;

  if (tile) {
    size_t tile_offset = offset % ADDB_TILE_SIZE;

    if (tile_offset > ADDB_TILE_SIZE - len)
      return NULL; /* value crosses tiles, punt */

    return tile->tile_memory + tile_offset;
  } else {
    if (offset + len > td->td_first_map_size)
      return NULL; /* value not in initial mmap */

    /* But what about values which start in storage without a tile
     * and overlap a tile (which might be written?)  If the
     * overlapping value had been written, both tiles would be
     * present. The fact that the starting tile isn't present
     * indicates that the value in question hasn't been written.
     */

    return td->td_first_map + offset;
  }
}

/**
 * @brief Backup pages within a tile.
 *
 *  If a write error occurs, the backup file is removed
 *  prior to returning.
 *
 * @param td		opaque tile manager handle
 * @param tile		tile to save pages from
 * @param write_mask	mask with 1 &lt;&lt; x set if we want to backup page x.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int addb_tiled_page_backup(addb_tiled* const td,
                                  addb_tile const* const tile,
                                  unsigned int const write_mask) {
  addb_handle* addb = td->td_pool->tdp_addb;
  cl_handle* cl = td->td_pool->tdp_cl;
  int err = 0;
  size_t const page_size = getpagesize();
  size_t const pages_per_tile = ADDB_TILE_SIZE / page_size;
  unsigned long long pages_written = 0;
  size_t page_i;

  cl_assert(cl, td->td_tbk.tbk_do_backup);

  for (page_i = 0; page_i < pages_per_tile; page_i++)
    if (write_mask & (1 << page_i)) {
      unsigned long long offset_in_tile = page_i * page_size;
      unsigned long long total_offset =
          tile->tile_i * ADDB_TILE_SIZE + offset_in_tile;

      err = addb_backup_write(addb, &td->td_tbk, total_offset,
                              tile->tile_memory + offset_in_tile, page_size);

      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "addb_backup_write", err,
                     "offset=%llu, pages written so "
                     "far=%llu (at %lu bytes each)",
                     total_offset, pages_written, (unsigned long)page_size);

        /* Close and discard the backup file.
         */
        addb_backup_punt(&td->td_tbk);
        return err;
      }
      pages_written++;
    }

  return 0;
}

/**
 * @brief The caller intends to modify a tile.  Back it up.
 *
 * @param td	The tiled store
 * @param tref	Reference to the specific tile.
 * @param offset_s offset of the first byte in the area to be written
 * @param offset_e offset of the last byte in the area to be written
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int addb_tiled_modify_start(addb_tiled* const td, addb_tile* const tile,
                                   unsigned long long offset_s,
                                   unsigned long long offset_e) {
  cl_handle* const cl = td->td_pool->tdp_cl;
  size_t const ps = getpagesize();
  unsigned short new_dirty_bits;

  cl_assert(cl, tile != NULL);
  cl_assert(cl, tile->tile_memory != NULL);

  offset_s /= ps;
  offset_e /= ps;
  new_dirty_bits = ((1 << (offset_e - offset_s + 1)) - 1) << offset_s;
  cl_assert(cl, new_dirty_bits);

  if (tile->tile_dirty_bits != (tile->tile_dirty_bits | new_dirty_bits)) {
    if (td->td_tbk.tbk_do_backup && td->td_advance_backup) {
      int err = addb_tiled_page_backup(
          td, tile, (tile->tile_dirty_bits ^ new_dirty_bits) & new_dirty_bits);

      /*  Continue.  We'll try to do the backup prior to the
       *  coming write.  Hopefully we'll have some space then.
       */
      if (err) td->td_advance_backup = false;
    }
    if (!tile->tile_dirty_bits) {
      void* mem;

      /*  We're dirtying a page that is either clean
       *  or scheduled.  Do a copy-on-write from the state
       *  that either is on disk right now, or will hit
       *  disk once the scheduled writes are done.
       */

      if (tile->tile_memory_disk) {
        /*  The tile has scheduled memory and should
         *  be on the scheduled list.
         */
        cl_assert(cl, tile->tile_memory_scheduled);
        cl_assert(cl, tile->tile_scheduled_bits);
        cl_assert(cl, tile->tile_next);
        cl_assert(cl, tile->tile_prev);
      } else {
        /* Tile does not have scheduled memory.
         * If it is on the scheduled list, the scheduled
         * bits are set.
         */
        cl_assert(cl, !tile->tile_memory_scheduled);
        cl_assert(cl, !tile->tile_scheduled_bits == !tile->tile_next);
        cl_assert(cl, !tile->tile_scheduled_bits == !tile->tile_prev);
      }
      mem = cm_malcpy(td->td_pool->tdp_cm, tile->tile_memory, ADDB_TILE_SIZE);
      if (!mem) return ENOMEM;

      if (!tile->tile_memory_disk) tile->tile_memory_disk = tile->tile_memory;
      tile->tile_memory = mem;

      td->td_tile_dirty++;
    }
    tile->tile_dirty_bits |= new_dirty_bits;
  } else
    /* Something better be dirty */
    cl_assert(cl, td->td_dirty_head || td->td_scheduled_head);

  cl_assert(cl, tile->tile_dirty_bits);

  /* If this tile is not on any list,
   * put it where it belongs.
   */
  if (!tile->tile_next)
    tile_chain_in(tile);
  else
    cl_assert(cl, tile->tile_dirty_bits || tile->tile_scheduled_bits);

  return 0;
}

/**
 * @brief Get a piece of memory.
 *
 *  Get the chunk of memory corresponding to [s...e), or NULL on error.
 *  It's an error if the underlying file isn't large enough.
 *
 *  If the tile manager uses backups, and the access involves ADDB_MODE_WRITE,
 *  the tile is copied into malloc'ed memory the first time before it is
 *  accessed for writing.
 *
 *  If the requested area is larger than the tile size, all accesses into
 *  the requested area must ask for a chunk of memory that's the same number
 *  of tiles large, and a tile larger than the tile size will be created.
 *
 * @param td	tile module handle
 * @param s	start of the region we want
 * @param e	end of (pointer just after) the region we want
 * @param mode	contains ADDB_MODE_WRITE if the region will be written to
 * @param ref_out assign a reference to the region's tile to this.
 * @param file	source file name of invoking code
 * @param line	line of invoking code
 *
 * @return NULL on error (and errno is set), otherwise a pointer to tile memory
 */
void* addb_tiled_get_loc(addb_tiled* const td, unsigned long long s,
                         unsigned long long e, int mode,
                         addb_tiled_reference* ref_out, char const* file,
                         int line) {
  addb_tile* tile;
  addb_tiled_pool* const tdp = td->td_pool;
  cl_handle* const cl = tdp->tdp_cl;
  size_t const tile_min = s / ADDB_TILE_SIZE;
  size_t const tile_max = (e - 1) / ADDB_TILE_SIZE;
  int err;
  unsigned long long phys_size;

  cl_assert(cl, e - s <= ADDB_TILE_SIZE);
  phys_size = (unsigned long long)(tile_max + 1) * ADDB_TILE_SIZE;

  if (phys_size > td->td_physical_file_size) {
    cl_log(cl, CL_LEVEL_VERBOSE | ADDB_FACILITY_TILE,
           "tile: %s: requested physical size "
           "%llu >= physical file "
           "size %llu [for %s:%d]",
           td->td_path, (unsigned long long)phys_size,
           (unsigned long long)td->td_physical_file_size, file, line);

    errno = E2BIG;
    *ref_out = (size_t)-1;

    cl_assert(cl, tdp->tdp_total >= td->td_pool->tdp_total_linked);
    return NULL;
  }

  /* Use the initial mmap if:
   *
   *	(a) we are not writing transactionally, and
   *	(b) the region is the initial mmap, and
   *	(c) there is no existing tile structure for the region
   */
  if ((!(mode & ADDB_MODE_WRITE) || !tdp->tdp_addb->addb_transactional) &&
      tile_min < addb_tiled_first_map(td) &&
      (tile_min >= td->td_tile_m || !td->td_tile[tile_min])) {
    size_t requested_size = e - s;

    *ref_out = ADDB_TILED_TREF_MAKE_INITMAP(requested_size);

    td->td_total += requested_size;
    td->td_total_linked += requested_size;

    tdp->tdp_total += requested_size;
    tdp->tdp_total_linked += requested_size;

    return (unsigned char*)td->td_first_map + s;
  }

  err = tiled_grow(td, tile_min);
  if (err != 0) {
    cl_log(cl, CL_LEVEL_FAIL | ADDB_FACILITY_TILE,
           "tile: %s: tiled_grow to %lu fails: %s [for %s:%d]", td->td_path,
           (unsigned long)tile_min, addb_xstrerror(err), file, line);
    *ref_out = (size_t)-1;
    errno = err;
    return NULL;
  }

  cl_assert(cl, tdp->tdp_total >= tdp->tdp_total_linked);

  *ref_out = tile_min;
  tile = td->td_tile[tile_min];
  cl_assert(cl, tile != NULL);

  err = tile_map(td, tdp, tile, tile_min);
  if (err != 0) {
    cl_log(cl, CL_LEVEL_FAIL | ADDB_FACILITY_TILE,
           "tile: %s: tile_map (%lu) fails: %s [%s:%d]", td->td_path,
           (unsigned long)tile_min, addb_xstrerror(err), file, line);

    /* tiled_grow may have chained out a tile -- throw it back. */
    if (0 == td->td_tile[tile_min]->tile_reference_count &&
        !td->td_tile[tile_min]->tile_next)
      tile_chain_in(td->td_tile[tile_min]);

    cl_assert(cl, tdp->tdp_total >= tdp->tdp_total_linked);
    *ref_out = (size_t)-1;
    errno = err;
    return NULL;
  }

  /*  Create a link to the new tile.
   */
  addb_physical_tile_link(cl, tdp, td, tile, &tile_min);

  /* If we need to make a backup copy, do so.  Istore writes are
   * not backed up and go directly to disk.  Recovery for istores
   * is via the next id stored separately istore marker file.
   */
  if ((mode & ADDB_MODE_WRITE) && tdp->tdp_addb->addb_transactional &&
      td->td_tbk.tbk_do_backup) {
    err = addb_tiled_modify_start(td, tile, s % ADDB_TILE_SIZE,
                                  (e - 1) % ADDB_TILE_SIZE);
    if (err) {
      errno = err;
      cl_log_errno(cl, CL_LEVEL_FAIL | ADDB_FACILITY_TILE,
                   "addb_tiled_modify_start", err, "path=%s(%zu) [%s:%d]",
                   td->td_path, *ref_out, file, line);
      *ref_out = (size_t)-1;
      cl_assert(cl, tdp->tdp_total >= tdp->tdp_total_linked);
      return NULL;
    }
  }

  return (unsigned char*)tile->tile_memory + (s - (tile_min * ADDB_TILE_SIZE));
}

static void addb_cease_locking(addb_handle* addb) {
  addb->addb_bytes_locked = -1;

#ifdef _POSIX_MEMLOCK_RANGE
  if (munlockall())
    cl_log(addb->addb_cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
           "munlockall() failed, (%d, %s)", errno, strerror(errno));
#endif
}

/* Lock a tiled file in memory
 */

void addb_tiled_mlock(addb_tiled* td) {
#ifdef _POSIX_MEMLOCK_RANGE
  size_t const map_len = td->td_first_map_size;
  size_t len = td->td_physical_file_size;

  if (len > map_len) len = map_len;

  if (!td->td_first_map) return; /* can't mlock individual tiles */

  if (td->td_pool->tdp_addb->addb_bytes_locked < 0)
    return; /* no longer locking (too big) */

  if (td->td_locked) return; /* already locked */

  if (len > 0) {
    if ((td->td_pool->tdp_addb->addb_bytes_locked + len) >
        (td->td_pool->tdp_addb->addb_mlock_max)) {
      cl_log(
          td->td_pool->tdp_cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
          "Ceased locking at %llu bytes, good luck",
          (unsigned long long)(td->td_pool->tdp_addb->addb_bytes_locked + len));
      addb_cease_locking(td->td_pool->tdp_addb);
      return;
    }

    if (mlock(td->td_first_map, len)) {
      cl_log(td->td_pool->tdp_cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
             "mlock( %llu ) failed, (%d, %s), for %s", (unsigned long long)len,
             errno, strerror(errno), td->td_path);
      if ((ENOMEM == errno) || (EPERM == errno))
        addb_cease_locking(td->td_pool->tdp_addb);
      return;
    }
    td->td_locked = 1;
    td->td_pool->tdp_addb->addb_bytes_locked += len;
  }
#else
  cl_log(td->td_pool->tdp_cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
         "_POSIX_MEMLOCK_RANGE undefined, no mlock");
  addb_cease_locking(td->td_pool->tdp_addb);
#endif
}

/* Unlock a tiled file (allow paging)
 */

void addb_tiled_munlock(addb_tiled* td) {
#ifdef _POSIX_MEMLOCK_RANGE
  size_t const map_len = td->td_first_map_size;
  size_t len = td->td_physical_file_size;

  if (len > map_len) len = map_len;

  if (!td->td_first_map) return; /* can't mlock individual tiles */

  if (!td->td_locked) return; /*  not locked */

  if (len > 0) {
    if (munlock(td->td_first_map, len)) {
      cl_log(td->td_pool->tdp_cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
             "mulock( %llu ) failed, (%s), for %s", (unsigned long long)len,
             strerror(errno), td->td_path);
    }
    td->td_locked = false;
    td->td_pool->tdp_addb->addb_bytes_locked -= len;
  }
#else
  cl_log(td->td_pool->tdp_cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
         "_POSIX_MEMLOCK_RANGE undefined, no munlock");
  addb_cease_locking(td->td_pool->tdp_addb);
#endif

  return;
}

/*
 * Return the number of references to a tile
 */
size_t addb_tiled_total_linked(addb_tiled* td) { return td->td_total_linked; }

/**
 * @brief Get the chunk of data corresponding to s...e, or NULL on error.
 *
 *  If it doesn't yet exist, it's allocated in the underlying
 *  file.
 *
 * @param td		tiled manager
 * @param s		start here
 * @param e		end just before here
 * @param ref_out	return a reference here
 * @param file		calling code's source file name, filled in by macro
 * @param line		calling code's line, filled in by macro
 *
 * @return NULL on error (and errno is set)
 * @return a pointer to the tile memory on success
 */
void* addb_tiled_alloc_loc(addb_tiled* td, unsigned long long s,
                           unsigned long long e, addb_tiled_reference* ref_out,
                           char const* file, int line) {
  size_t tile_max;
  unsigned long long phys_size;

  *ref_out = (size_t)-1;
  cl_assert(td->td_pool->tdp_cl, s < e);

  /*  Grow the file if it isn't large enough.
   */
  tile_max = (e - 1) / ADDB_TILE_SIZE;
  phys_size = ((unsigned long long)tile_max + 1) * ADDB_TILE_SIZE;

  if (phys_size > td->td_physical_file_size) {
    bool locked = td->td_locked;
    int err;

    err =
        addb_file_grow(td->td_pool->tdp_cl, td->td_fd, td->td_path, phys_size);
    if (err != 0) {
      cl_log_errno(td->td_pool->tdp_cl, CL_LEVEL_FAIL, "addb_file_grow", err,
                   "for %s:%d, growing to phys_size=%llu", file, line,
                   phys_size);
      errno = err;
      return NULL;
    }

    if (locked) addb_tiled_munlock(td);

    td->td_physical_file_size = phys_size;

    if (locked) addb_tiled_mlock(td);

    cl_log(td->td_pool->tdp_cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE,
           "tile: grow file to %llu (while allocating a "
           "%llu-byte-chunk)",
           (unsigned long long)phys_size, (unsigned long long)(e - s));
  }
  return addb_tiled_get_loc(td, s, e, ADDB_MODE_WRITE, ref_out, file, line);
}

/*  Align boundaries of a chunk of memory to fit into a tiled grid.
 *  Return 1 if the values changed, 0 otherwise.
 */
int addb_tiled_align(addb_tiled* td, off_t* s, off_t* e) {
  unsigned long long off;

  cl_assert(td->td_pool->tdp_cl, *s <= *e);

  if (*e - *s <= ADDB_TILE_SIZE) {
    /*  Pieces up to the size of the tile must be aligned such
     *  that they don't cross tile boundaries.
     */
    if (*s / ADDB_TILE_SIZE == (*e - 1) / ADDB_TILE_SIZE) return 0;
  } else {
    /*  Pieces larger than a tile must be aligned to start on
     *  tile boundaries.
     */
    if (*s % ADDB_TILE_SIZE == 0) return 0;
  }

  /* Shift s..e to start at the next tile boundary. */

  off = ADDB_TILE_SIZE - (*s % ADDB_TILE_SIZE);
  *s += off;
  *e += off;

  return 1;
}

/*  Free a previously allocated tile reference.
 */
void addb_tiled_free_loc(addb_tiled* td, addb_tiled_reference* tref,
                         char const* file, int line) {
  long size;
  addb_tiled_pool* const tdp = td->td_pool;
  cl_handle* cl = tdp->tdp_cl;

  cl_assert(cl, tref != NULL);

  if (*tref == (size_t)-1) return;

  cl_assert(cl, tdp->tdp_total >= tdp->tdp_total_linked);

  if (ADDB_TILED_TREF_IS_INITMAP(*tref)) {
    size = ADDB_TILED_TREF_INITMAP_SIZE(*tref);

    cl_assert(cl, td->td_total >= size);
    cl_assert(cl, tdp->tdp_total >= size);
    cl_assert(cl, td->td_total_linked >= size);
    cl_assert(cl, tdp->tdp_total_linked >= size);

    /*  Normal tiles are deducted from the "total"
     *  only when they're completely free'd.
     *
     *  But these non-tiled slivers have no
     *  existence beyond their allocation
     *  and deallocation by the application--
     *  so we're deducting them here.
     *
     *  The linked sizes are deducted in common with
     *  the free'd tiles, below.
     */
    td->td_total -= size;
    td->td_pool->tdp_total -= size;

    td->td_total_linked -= size;
    tdp->tdp_total_linked -= size;
  } else {
    addb_tile* const tile = td->td_tile[*tref];

    cl_assert(tdp->tdp_cl, tile != NULL);
    cl_assert(tdp->tdp_cl, tile->tile_reference_count > 0);

    if (tile->tile_reference_count-- > 1) {
      *tref = (size_t)-1;
      return;
    }

    cl_assert(tdp->tdp_cl, td->td_total_linked >= ADDB_TILE_SIZE);
    cl_assert(tdp->tdp_cl, tdp->tdp_total_linked >= ADDB_TILE_SIZE);

    td->td_total_linked -= ADDB_TILE_SIZE;
    tdp->tdp_total_linked -= ADDB_TILE_SIZE;

    /* Place the tile in a free list.  Don't flush here
     * unless we're really in trouble.
     */
    if (!tile->tile_next)
      tile_chain_in(tile);
    else
      cl_assert(tdp->tdp_cl,
                tile->tile_dirty_bits || tile->tile_scheduled_bits);

    if (tdp->tdp_total > tdp->tdp_max)
      (void)tiled_pool_flush(tdp, tdp->tdp_total - tdp->tdp_max);
  }

  *tref = (size_t)-1;

  cl_assert(tdp->tdp_cl, tdp->tdp_total >= tdp->tdp_total_linked);
}

static void addb_tiled_revert_tile(cm_handle* cm, addb_tile* tile) {
  if (tile->tile_memory_scheduled &&
      tile->tile_memory != tile->tile_memory_scheduled)
    cm_free(cm, tile->tile_memory_scheduled);
  cm_free(cm, tile->tile_memory);
  tile->tile_memory = tile->tile_memory_disk;
  tile->tile_memory_disk = (void*)0;
  tile->tile_memory_scheduled = (void*)0;
  tile->tile_dirty_bits = 0;
  tile->tile_scheduled_bits = 0;
  tile->tile_next = (addb_tile*)0;
  tile->tile_prev = (addb_tile*)0;
  tile_chain_in(tile);
}

/**
 * @brief Remove the backup information associated with current changes.
 *
 *  Get rid of dirty tiles and the associated unpublished
 *  backup file.
 *
 *  The normal progression is: (1) make changes in memory, (2) backup,
 *  then (3) copy the changes to disk and (4) remove the backup.
 *
 *  This function both throws away the changes in memory *and*
 *  (if present) removes the backup files that anticipated the
 *  changes to disk.  (In other words, phase 3 and 4 have not
 *  yet happened.)  The resulting database is a fully
 *  consistent earlier state.
 *
 * @param td	tiled handle
 * @return 0 on success, a nonzero error code otherwise
 */

static int addb_tiled_backup_abort(addb_tiled* td) {
  cl_handle* const cl = td->td_pool->tdp_cl;

  cl_assert(cl, td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);

  td->td_checkpoint_stage = ADDB_CKS_DONE;

  /* The tiles in the "dirty" chain are not scheduled -
   * they're not participating in the backup, and are
   * therefore safe to reset.
   */
  if (td->td_dirty_head) {
    addb_tile* tile = td->td_dirty_head;
    addb_tile* const last_tile = tile;

    while (tile) {
      addb_tile* const next_tile = tile->tile_next;

      cl_assert(cl, tile->tile_dirty_bits);
      cl_assert(cl, tile->tile_memory_disk);
      cl_assert(cl, !tile->tile_memory_scheduled);
      cl_assert(cl, !tile->tile_scheduled_bits);

      addb_tiled_revert_tile(td->td_pool->tdp_cm, tile);

      tile = next_tile;
      if (last_tile == tile) tile = 0;
    }
    td->td_dirty_head = (addb_tile*)0;
  }
  td->td_tile_dirty = 0;

  if (td->td_tbk.tbk_do_backup) {
    char const* filename = td->td_tbk.tbk_a.path;

    int err = addb_backup_abort(td->td_pool->tdp_addb, &td->td_tbk);
    if (err == ENOENT) {
      cl_log(td->td_pool->tdp_cl, CL_LEVEL_SPEW,
             "addb_tiled_backup_abort \"%s\" not found "
             "(ignore)",
             filename);
      err = 0;
    } else if (err != 0)
      cl_log_errno(td->td_pool->tdp_cl, CL_LEVEL_ERROR, "addb_backup_abort",
                   err, "unexpected error while aborting backup to \"%s\"",
                   filename);

    /*  The tiles in the "scheduled" chain are safe to remove,
     *  now that we've aborted the pending write from them.
     */
    if (td->td_scheduled_head) {
      addb_tile* tile = td->td_scheduled_head;
      addb_tile* const last_tile = tile;

      while (tile) {
        addb_tile* const next_tile = tile->tile_next;

        cl_assert(cl, tile->tile_scheduled_bits);
        cl_assert(cl, tile->tile_memory_disk);
        cl_assert(cl, !tile->tile_dirty_bits ==
                          (tile->tile_memory_scheduled == tile->tile_memory));

        addb_tiled_revert_tile(td->td_pool->tdp_cm, tile);

        tile = next_tile;
        if (last_tile == tile) tile = 0;
      }
      td->td_scheduled_head = (addb_tile*)0;
    }

    return err;
  }

  return 0;
}

/**
 * @brief Free a tiled file handle.
 * @param td	NULL or a tiled descriptor to destroy.
 * @return 0 on success, otherwise a nonzero error code.
 */
int addb_tiled_destroy(addb_tiled* td) {
  addb_tiled_pool* const tdp = td->td_pool;
  size_t i;
  int result = 0, err;

  if (td == NULL) return 0;
  if (addb_tiled_is_dirty(td))
    cl_log(tdp->tdp_cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
           "addb_tiled_destroy %s losing uncommited changes", td->td_path);

  err = addb_tiled_backup_abort(td);
  if (!result) result = err;
  if (td->td_first_map_size &&
      munmap(td->td_first_map, td->td_first_map_size) < 0) {
    err = errno;
    cl_log_errno(tdp->tdp_cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE, "munmap",
                 err,
                 "unexpected error while unmapping initial"
                 "%llu bytes from database file \"%s\", leaking %p",
                 (unsigned long long)td->td_first_map_size, td->td_path,
                 (void*)td->td_first_map);
    if (!result) result = err;
  }

  if (td->td_tile_m > 0) {
    for (i = 0; i < td->td_tile_m; i++) {
      addb_tile* const tile = td->td_tile[i];

      if (tile) {
        if (tile->tile_dirty_bits || tile->tile_scheduled_bits) {
          cl_log(tdp->tdp_cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
                 "addb_tiled_destroy: tile %s.%llu has "
                 "dirty or scheduled bits at termination",
                 td->td_path, (unsigned long long)i);

          if (!result) result = ETXTBSY;
        }
        if (tile->tile_reference_count != 0) {
          cl_log(tdp->tdp_cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
                 "addb_tiled_destroy: tile %s.%llu has non-zero "
                 "reference count %lu at termination",
                 td->td_path, (unsigned long long)i,
                 (unsigned long)tile->tile_reference_count);
          if (!result) result = ETXTBSY;
        }
        if (tile->tile_prev) tile_chain_out(tile);
        tile_free(tile);
      }
    }
    cm_free(tdp->tdp_cm, td->td_tile);
  }

  if (close(td->td_fd) < 0) {
    err = errno;
    cl_log_errno(tdp->tdp_cl, CL_LEVEL_ERROR, "close", err,
                 "unexpected error while closing "
                 "database file \"%s\"",
                 td->td_path);
    if (!result) result = err;
  }

  cl_log(tdp->tdp_cl, CL_LEVEL_SPEW, "addb_tiled_destroy: \"%s\" %s",
         td->td_path, result ? "FAILED" : "successful");

  cl_assert(tdp->tdp_cl, tdp->tdp_total >= tdp->tdp_total_linked);
  cm_free(tdp->tdp_cm, td);
  return result;
}

/** @brief Create a tiled file handle.
 *
 *  If the file doesn't exist, it is created;
 *  if that doesn't work, the call fails.
 *
 * @param cm allocate memory via this handle.
 * @param cl opaque handle to log through
 * @param granularity each tile is a multiple of this large
 *
 * @result NULL on error, otherwise a pointer to a tile
 * 	manager for a file.
 */
addb_tiled_pool* addb_tiled_pool_create(addb_handle* addb) {
  cl_handle* const cl = addb->addb_cl;
  cm_handle* const cm = addb->addb_cm;
  addb_tiled_pool* tdp = cm_malloc(cm, sizeof *tdp);
  int err;

  if (!tdp) {
    cl_log(cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
           "addb: failed to "
           "allocate %lu bytes for tiled pool: %s [%s:%d]",
           (unsigned long)sizeof(*tdp), strerror(errno), __FILE__, __LINE__);
    return NULL;
  }
  memset(tdp, 0, sizeof *tdp);

  tdp->tdp_addb = addb;
  tdp->tdp_cl = cl;
  tdp->tdp_cm = cm;
  tdp->tdp_free_head = (addb_tile*)0;
  tdp->tdp_have_mmapped_tile = false;

  err = cm_hashinit(cm, &tdp->tdp_mmaps, sizeof(addb_mmap_slot), 64 * 2 * 1024);
  if (err != 0) {
    cl_log(cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE,
           "addb: failed to allocate hashtable with %lu "
           "slots for resource tracking: %s [%s:%d]",
           64ul * 2 * 1024, strerror(errno), __FILE__, __LINE__);
    cm_free(cm, tdp);
    return NULL;
  }
  cl_assert(cl, tdp->tdp_total >= tdp->tdp_total_linked);
  return tdp;
}

void addb_tiled_pool_destroy(addb_tiled_pool* tdp) {
  if (tdp != NULL) {
    cl_assert(tdp->tdp_cl, tdp->tdp_total >= tdp->tdp_total_linked);
    cm_hashfinish(&tdp->tdp_mmaps);
    cm_free(tdp->tdp_cm, tdp);
  }
}

int addb_tiled_apply_backup_record(addb_tiled* td, unsigned long long offset,
                                   char* mem, unsigned long long size) {
  addb_tiled_reference tref;
  void* target;
  int err = 0;

  cl_assert(td->td_pool->tdp_cl, getpagesize() == size);
  cl_assert(td->td_pool->tdp_cl, 0 == offset % getpagesize());

  cl_assert(td->td_pool->tdp_cl,
            td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);

  target = addb_tiled_get(td, offset, offset + size, ADDB_MODE_BACKUP, &tref);
  if (!target) {
    err = errno;
    cl_log_errno(td->td_pool->tdp_cl, CL_LEVEL_ERROR, "addb_tiled_get", err,
                 "Unable to get %llu", offset);
    return err;
  }
  memcpy(target, mem, size);
  addb_tiled_free(td, &tref);

  return 0;
}

/** @brief Create a tiled file handle.
 *
 *  If the file doesn't exist, it is created;
 *  if that doesn't work, the call fails.
 *
 * @param tdp 		tiled pool to share
 * @param path	 	allocate or map this file.
 * @param mode		mode to open underlying file, O_RDWR or O_RDONLY
 *
 * @result NULL on error, otherwise a pointer to a tile manager for a file.
 */
addb_tiled* addb_tiled_create(addb_tiled_pool* tdp, char* path, int mode,
                              unsigned long long init_map_size) {
  cl_handle* const cl = tdp->tdp_cl;
  addb_tiled* td;
  size_t path_n = strlen(path) + 1;
  size_t a_path_n = path_n + 5;
  size_t v_path_n = path_n + 4;
  int fd;
  void* first_map;
  bool const read_only = O_RDONLY == mode;
  unsigned long long file_size = 0;
  unsigned long long first_map_size = 0;
  int err;

  if (0 == mode) mode = O_RDWR;

  cl_assert(cl, ADDB_TILE_SIZE > 0);
  cl_assert(cl, O_RDWR == mode || O_RDONLY == mode);

#ifdef O_NOATIME
  mode |= O_NOATIME;
#endif

  fd = open(path, mode | O_CREAT, 0666);
  if (-1 == fd) {
    cl_log_errno(cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE, "open", errno,
                 "addb: failed to open \"%s\"", path);

    return NULL;
  }

  (void)addb_file_advise_random(cl, fd, path);

  {
    struct stat st;

    err = addb_file_fstat(cl, fd, path, &st);
    if (err != 0) {
      (void)close(fd);
      errno = err;

      return NULL;
    }
    file_size = st.st_size;
  }

  if (file_size % ADDB_TILE_SIZE) {
    off_t const new_size = (file_size / ADDB_TILE_SIZE + 1) * ADDB_TILE_SIZE;

    cl_log(cl, CL_LEVEL_ERROR,
           "addb_tiled_create( %s ) file size, %llu, "
           "not a multiple of tile size",
           path, file_size);
    (void)addb_file_grow(cl, fd, path, new_size);
  }

  {
    size_t const sz = sizeof *td + path_n + /* storage for tiled path */
                      a_path_n + /* storage for active backup "...0.clx" */
                      a_path_n + /* storage for active backup "...1.clx" */
                      v_path_n;  /* storage for published backup */

    td = cm_malloc(tdp->tdp_cm, sz);
    if (!td) {
      int err = errno;
      cl_log_errno(cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE, "cm_malloc", err,
                   "failed to allocate %zu bytes for "
                   "tiled file \"%s\"",
                   sz, path);
      close(fd);
      return NULL;
    }
  }
  memset(td, 0, sizeof *td);
  td->td_path = (char*)(td + 1);
  memcpy(td->td_path, path, path_n);

  {
    char* const a0_path = td->td_path + path_n;
    char* const a1_path = a0_path + a_path_n;
    char* const v_path = a1_path + a_path_n;

    cl_assert(cl,
              a_path_n > snprintf(a0_path, a_path_n, "%s0.clx", td->td_path));
    cl_assert(cl,
              a_path_n > snprintf(a1_path, a_path_n, "%s1.clx", td->td_path));
    cl_assert(cl, v_path_n > snprintf(v_path, v_path_n, "%s.cln", td->td_path));

    if (addb_backup_init(tdp->tdp_addb, &td->td_tbk, a0_path, a1_path, v_path))
      return NULL;
  }

  /* Map the initial portion of the file
   *
   * On 64-bit systems, ensure that files are always completely
   * covered by the initial map.
   */
  if (init_map_size > 0) {
    first_map_size =
        sizeof(void*) > 4
            ? (file_size > init_map_size ? file_size : init_map_size)
            : init_map_size;

    if (sizeof(void*) > 4 && !read_only && first_map_size == file_size) {
      size_t n_tiles = file_size / ADDB_TILE_SIZE;
      size_t n_extra = n_tiles / 10;

      /* Add some room to grow
       */

      if (n_extra > 0)
        first_map_size += n_extra * ADDB_TILE_SIZE;
      else
        first_map_size += ADDB_TILE_SIZE;
    }

    /* Round up to a whole number of tiles */
    first_map_size = 1ull + (first_map_size - 1ull) / ADDB_TILE_SIZE;
    first_map_size *= ADDB_TILE_SIZE;

    first_map =
        mmap(0, first_map_size, PROT_READ | (read_only ? 0 : PROT_WRITE),
             MAP_SHARED, fd, 0);
    if (MAP_FAILED == first_map) {
      int err = errno;
      cl_log_errno(cl, CL_LEVEL_ERROR | ADDB_FACILITY_TILE, "mmap", err,
                   "addb: failed to mmap %llu bytes for %s. "
                   "Do you need to set "
                   "{istore,gmap}-init-map-tiles?",
                   first_map_size, path);
      first_map = 0;
    } else
      cl_log(cl, CL_LEVEL_DEBUG | ADDB_FACILITY_TILE,
             "mmap: %llu bytes for %s. "
             "(current size: %llu)",
             first_map_size, path, file_size);
  } else {
    cl_log(cl, CL_LEVEL_INFO | ADDB_FACILITY_TILE, "No initmap for %s", path);
    first_map = 0;
  }

  td->td_fd = fd;
  td->td_pool = tdp;
  td->td_physical_file_size = file_size;
  td->td_first_map_size = first_map_size;
  td->td_tile = NULL;
  td->td_tile_m = 0;
  td->td_first_map = first_map;
  td->td_checkpoint_stage = ADDB_CKS_DONE;
  td->td_advance_backup = true;

  cl_assert(cl, td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);

  return td;
}

/**
 * @brief Turn backup on or off for a tiled file.
 * @param td  	tiled handle
 * @param on	true to turn on backup, false to turn it off (the default).
 */

int addb_tiled_backup(addb_tiled* td, bool on) {
  cl_assert(td->td_pool->tdp_cl,
            td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);
  if (td->td_tbk.tbk_do_backup && !on) {
    int err = 0;
    int e;

    /* We're turning backup off, usually just before deleting
     * the file in question.  Abort changes and remove any published
     * backup files.
     */
    e = addb_tiled_backup_abort(td);
    if (e) err = e;
    e = addb_backup_unpublish(td->td_pool->tdp_addb, &td->td_tbk);
    if (e) err = e;

    if (err) return err;
  }

  td->td_tbk.tbk_do_backup = on;

  return 0;
}

/** @brief Try reading a backup.
 *
 * @param td		tile manager handle
 * @param horizon	We *know* (externally) that we're sync'ed up to
 * 			this point.  Ignore backups that go before this
 *			state.
 *
 * @result 0 on success, otherwise a nonzero error code.
 */
int addb_tiled_read_backup(addb_tiled* td, unsigned long long horizon) {
  addb_tiled_pool* const tdp = td->td_pool;
  int err;

  cl_assert(tdp->tdp_cl, td->td_tbk.tbk_do_backup);
  cl_assert(tdp->tdp_cl, td->td_tbk.tbk_v_path);
  cl_assert(tdp->tdp_cl, tdp->tdp_total >= tdp->tdp_total_linked);

  /* Abort any planned modifications, we're going back into the past.
   */
  err = addb_tiled_backup_abort(td);
  if (err) return err;

  /*  If we have a published backup file, use it.
   */
  cl_log(tdp->tdp_addb->addb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE,
         "[%llu] %s: reading backup %s, rolling back to %llu.",
         (unsigned long long)addb_msclock(tdp->tdp_addb), td->td_path,
         td->td_tbk.tbk_v_path, horizon);

  err = addb_backup_read(tdp->tdp_addb, td, &td->td_tbk, horizon);
  if (err == ENOENT || err == ADDB_ERR_NO) {
    /* Either there was no backup file, or the
     * backup file was outdated.  That's okay.
     */
    err = 0;
  } else if (err)
    return err;
  else {
    /*  We used a backup to restore state.
     *  Sync the database partition to disk to ensure
     *  that our state is well-defined; then remove
     *  the backup.
     */
    err = addb_file_sync(tdp->tdp_addb, td->td_fd, td->td_path);
    if (err) return err;

    err = addb_file_unlink(tdp->tdp_addb, td->td_tbk.tbk_v_path);
    if (err) return err;
  }

  cl_log(tdp->tdp_addb->addb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE,
         "[%llu] %s: done reading backup.",
         (unsigned long long)addb_msclock(tdp->tdp_addb), td->td_path);

  return err;
}

#ifdef SHOW_STATUS_IN_CORE

static double addb_tiled_percent_in_core(addb_tiled* td) {
  size_t const l = td->td_physical_file_size > td->td_first_map_size
                       ? td->td_first_map_size
                       : td->td_physical_file_size;
  size_t n_pages = l / getpagesize();
  unsigned char* v = cm_talloc(td->td_pool->tdp_cm, unsigned char, n_pages);
  int pages_in_core = 0;
  int i;

  if (!v) return 111.0;
  if (n_pages == 0) return 666.6;

  if (mincore(td->td_first_map, l, v)) {
    cm_free(td->td_pool->tdp_cm, v);
    return 111.0;
  }

  for (i = 0; i < n_pages; i++) pages_in_core += v[i] & 1;

  cm_free(td->td_pool->tdp_cm, v);

  return ((double)pages_in_core / (double)n_pages) * 100.0;
}

#endif

/*  Pass up a series of name, value pairs that report
 *  operating parameters of the tile cache.
 */
int addb_tiled_status(addb_tiled* td, cm_prefix const* prefix,
                      addb_status_callback* cb, void* cb_data) {
  int err;
  char num_buf[42];
  cm_prefix tile_pre = cm_prefix_push(prefix, "tile");

  /*  Memory in the cache and the linked list.
   */
  snprintf(num_buf, sizeof num_buf, "%llu", td->td_total_linked);
  err = (*cb)(cb_data, cm_prefix_end(&tile_pre, "bytes-in-use"), num_buf);
  if (err) return err;

  snprintf(num_buf, sizeof num_buf, "%llu", td->td_total - td->td_total_linked);
  err = (*cb)(cb_data, cm_prefix_end(&tile_pre, "bytes-free"), num_buf);
  if (err) return err;

  /*  The size of the underlying file.
   */
  snprintf(num_buf, sizeof num_buf, "%llu", td->td_physical_file_size);
  err = (*cb)(cb_data, cm_prefix_end(&tile_pre, "file-size"), num_buf);
  if (err) return err;

  /*  The number of dirty tiles
   */
  snprintf(num_buf, sizeof num_buf, "%lu", (unsigned long)td->td_tile_dirty);
  err = (*cb)(cb_data, cm_prefix_end(&tile_pre, "dirty"), num_buf);
  if (err) return err;

  /*
   * The size of the init map
   */

  snprintf(num_buf, sizeof num_buf, "%lu",
           (unsigned long)td->td_first_map_size);
  err = (*cb)(cb_data, cm_prefix_end(&tile_pre, "init-map"), num_buf);
  if (err) return err;

#ifdef SHOW_STATUS_IN_CORE

  /*  The percentage of this file in core
   */
  snprintf(num_buf, sizeof num_buf, "%.2f", addb_tiled_percent_in_core(td));
  err = (*cb)(cb_data, cm_prefix_end(&tile_pre, "in-core"), num_buf);
  if (err) return err;
#endif

  return 0;
}

/**
 * @brief Compose the  "status (tiles)" reply for a specific tile cache.
 *
 * @param td	  tiled partition the caller is asking about
 * @param prefix  prefix for the reported values
 * @param cb	  pass name/value pairs to this callback
 * @param cb_data together with this opaque pointer
 *
 * @return 0 on success, a nonzero error code on error.
 */
int addb_tiled_status_tiles(addb_tiled* td, cm_prefix const* prefix,
                            addb_status_callback* cb, void* cb_data) {
  int err;
  cm_prefix tile_pre = cm_prefix_push(prefix, "tile");
  size_t i;
  char *buf, *w;
  char const *e, *r, *last_written = NULL;
  char tile_buf[128];
  unsigned long last_repeat = 0;
  cl_handle* cl = td->td_pool->tdp_cl;

  if ((w = buf = malloc(td->td_tile_m * 9 + 1)) == NULL) return ENOMEM;
  e = buf + td->td_tile_m * 9;

  tile_buf[0] = '\0';
  for (i = 0; i < td->td_tile_m; i++) {
    addb_tile* tile = td->td_tile[i];

    /*  Format a tile value, null-terminated, into tile_buf.
     */
    if (tile == NULL) {
      strcpy(tile_buf, ".");

      if (last_written != NULL && *last_written == '.') {
        /*  Skip tiles until the next tile, if any,
         *  is not NULL.
         */
        while (i + 1 < td->td_tile_m && td->td_tile[i + 1] == NULL) {
          last_repeat++;
          i++;
        }
      }
    } else {
      snprintf(tile_buf, sizeof tile_buf, "%hx:%hx+%s%x", tile->tile_dirty_bits,
               tile->tile_scheduled_bits,
               tile->tile_reference_count > 0xff ? "?" : "",
               tile->tile_reference_count > 0xff
                   ? (unsigned int)tile->tile_reference_count % 0xf
                   : (unsigned int)tile->tile_reference_count);
    }

    /*  If that's what we just wrote, just increment a counter;
     *  otherwise, format the last counter, and append the
     *  new value.
     */

    if (last_written != NULL) {
      if (strcmp(last_written, tile_buf) == 0) {
        last_repeat++;
        continue;
      }

      if (last_repeat > 1) {
        snprintf(w, e - w, "*%lu", last_repeat);
        w += strlen(w);
      }
      *w++ = ' ';
    }

    last_written = w;
    r = tile_buf;
    while ((*w = *r++) != '\0') w++;

    last_repeat = 1;
  }

  if (last_written != NULL && last_repeat > 1) {
    snprintf(w, e - w, "*%lu", last_repeat);
    w += strlen(w);
  }
  cl_assert(cl, w <= e);
  *w = '\0';

  err = (*cb)(cb_data, cm_prefix_end(&tile_pre, "map"), buf);
  free(buf);
  return err;
}

/* Move the list of dirty tiles to list of scheduled tiles
 * in preparation for writing the changes back to disk
 */

static void addb_schedule_dirty_tiles(cl_handle* cl, addb_tiled* td) {
  addb_tile* tile;
  addb_tile* last_tile;

  cl_assert(cl, td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);
  cl_assert(cl, td->td_dirty_head);
  cl_assert(cl, !td->td_scheduled_head);

  td->td_scheduled_head = td->td_dirty_head;
  td->td_dirty_head = (addb_tile*)0;

  tile = td->td_scheduled_head;
  last_tile = tile;
  while (tile) {
    cl_assert(cl, tile->tile_dirty_bits);
    cl_assert(cl, tile->tile_memory);
    cl_assert(cl, !tile->tile_memory_scheduled);
    cl_assert(cl, !tile->tile_scheduled_bits);

    tile->tile_memory_scheduled = tile->tile_memory;
    tile->tile_scheduled_bits = tile->tile_dirty_bits;
    tile->tile_dirty_bits = 0;

    tile = tile->tile_next;
    if (last_tile == tile) tile = 0;
  }
  td->td_tile_dirty = 0;
}

/**
 * @brief Catch up on a backup that failed earlier.
 *
 *  When we began to modify a tile (in memory) earlier, we tried to back
 *  it up on disk.  That failed.  (For example, we might have been out of
 *  disk space.)  We got rid of the back-up file.
 *
 *  Now we're going to actually write our modifications to disk.
 *  Before that starts, we *have* to have a backup file.  Try to create
 *  one again.
 *
 * @param td		opaque tile manager handle
 * @return 0 on success, other nonzero error codes on error.
 */
static int addb_tiled_checkpoint_start_backup(addb_tiled* td) {
  cl_handle* cl = td->td_pool->tdp_cl;
  int err = 0;
  addb_tile* tile;
  addb_tile* last_tile;

  cl_assert(cl, td->td_tbk.tbk_do_backup);
  cl_assert(cl, td->td_tile_dirty);
  cl_assert(cl, td->td_tbk.tbk_a.fd == -1);

  tile = td->td_dirty_head;
  last_tile = tile;
  while (tile) {
    /*  Backup the dirty tiles.
     */
    cl_assert(cl, tile->tile_dirty_bits);

    err = addb_tiled_page_backup(td, tile, tile->tile_dirty_bits);
    if (err != 0) return err;

    tile = tile->tile_next;
    if (last_tile == tile) tile = 0;
  }

  /* The scheduled tiles may or may not have gotten dirty
   * since they were scheduled; they, too, need to be backed
   * up now.
   */
  tile = td->td_scheduled_head;
  last_tile = tile;
  while (tile) {
    if (tile->tile_dirty_bits) {
      err = addb_tiled_page_backup(td, tile, tile->tile_dirty_bits);
      if (err != 0) return err;
    }
    tile = tile->tile_next;
    if (last_tile == tile) tile = 0;
  }

  td->td_advance_backup = true;
  return 0;
}

/**
 * @brief Phase 1 of a disk flush.
 *
 *  Back up the originals of copied-on-write tiles to the backup file,
 *  and sync that file to disk.
 *
 *  After this function completes, we can modify the mmap'ed disk
 *  database file, safe in the knowledge that we can always back up
 *  to the previous horizon.
 *
 *  The tile database must have had backup turned on previously with
 *  a call to addb_tiled_backup().
 *
 * @param td		opaque tile manager handle
 * @param horizon	the state that the gmap would revert to if we used
 *			the backup we're about to write.
 * @param hard_sync	wait for writes to hit disk?
 * @param block		(unused)
 *
 * @return 0 on success, other nonzero error codes on error.
 * @return ADDB_ERR_ALREADY if there was nothing to do.
 */
int addb_tiled_checkpoint_finish_backup(addb_tiled* td,
                                        unsigned long long horizon,
                                        bool hard_sync, bool block) {
  addb_handle* addb = td->td_pool->tdp_addb;
  cl_handle* cl = td->td_pool->tdp_cl;
  int err = 0;

  cl_assert(cl, td->td_pool);
  cl_assert(cl, td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);

  /* We better be doing backups
   */
  cl_assert(cl, td->td_tbk.tbk_do_backup);

  /* Iff there are dirty tiles then we have an open backup file.
   */
  if (td->td_tile_dirty > 0 && td->td_tbk.tbk_a.fd == -1) {
    /*  ... Unless we tried that earlier and it didn't work,
     *  in which case we're retrying it now, and fail if
     *  that fails.
     */
    err = addb_tiled_checkpoint_start_backup(td);
    if (err) return err;
  }
  cl_assert(cl, (td->td_tile_dirty == 0) == (td->td_tbk.tbk_a.fd == -1));

  /* Iff there are no dirty tiles,
   * there should be no tiles on the dirty list
   */
  cl_assert(cl, !td->td_tile_dirty == !td->td_dirty_head);

  /* There should be no scheduled tiles, we're going to schedule some
   */
  cl_assert(cl, !td->td_scheduled_head);

  if (!td->td_tile_dirty) {
    td->td_checkpoint_stage = ADDB_CKS_DONE;
    return 0;
  }

  cl_assert(cl, addb->addb_transactional);
  cl_assert(cl, td->td_tbk.tbk_a.fd != -1);
  td->td_checkpoint_stage = ADDB_CKS_FINISH_BACKUP;

  /* All we do here is rewrite the horizon to indicate that
   * the backup file is valid and sync it to disk.
   */
  err = addb_backup_finish(addb, &td->td_tbk, horizon);
  if (err) {
    td->td_checkpoint_stage = ADDB_CKS_DONE;
    return err;
  }

  if (hard_sync) {
    /*   Hard_sync, you make my heart sing!
     *   You make everything -- groovy!  Hard_sync!
     */
    err = addb_backup_sync_start(addb, &td->td_tbk);
    if (err) {
      td->td_checkpoint_stage = ADDB_CKS_DONE;
      return err;
    }
  }

  /* Move all dirty tiles to the scheduled list and clear dirty bits
   * Future writing will be backed up in the next backup file.
   */

  addb_schedule_dirty_tiles(cl, td);

  td->td_checkpoint_stage++;

  cl_log(addb->addb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE,
         "%s: checkpoint (1): backup file written; horizon=%llu", td->td_path,
         horizon);

  return 0;
}

/* Wait for the backup file to be synchronized.
 *
 * It is tempting to roll this function into the following
 * tiled_checkpoint_start_writes until you realize that
 * tiled_checkpoint_start_writes
 * is used as the checkpoint entry by the istore which is not backed up.
 */

int addb_tiled_checkpoint_sync_backup(addb_tiled* td,
                                      unsigned long long horizon,
                                      bool hard_sync, bool block) {
  addb_handle* const addb = td->td_pool->tdp_addb;
  cl_handle* const cl = addb->addb_cl;
  unsigned long long bytes_written = 0;
  int err = 0;

  cl_assert(cl, td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);

  /* If we're not doing anything or this stage is already
   * done, then we're done.
   */
  if (ADDB_CKS_DONE == td->td_checkpoint_stage ||
      ADDB_CKS_SYNC_BACKUP + 1 == td->td_checkpoint_stage)
    return 0;

  cl_assert(cl, ADDB_CKS_SYNC_BACKUP == td->td_checkpoint_stage);
  cl_assert(cl, addb->addb_transactional);

  if (hard_sync) {
    err = addb_backup_sync_finish(addb, &td->td_tbk, block);
    if (err) {
      if (ADDB_ERR_MORE == err)
        cl_log(cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE,
               "%s: checkpoint (2): ADDB_ERR_MORE", td->td_path);
      return err;
    }
  }

  err = addb_backup_close(addb, &td->td_tbk, &bytes_written);
  if (err) return err;
  td->td_pool->tdp_bytes_written += bytes_written;

  err = addb_backup_publish(addb, &td->td_tbk);
  if (err) return err;

  td->td_checkpoint_stage++;

  cl_log(addb->addb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE,
         "%s: checkpoint (2): backup file sync'ed.", td->td_path);

  return 0;
}

/**
 * @brief Phase 3 of a disk flush.
 *
 *  Copy the data of all modified tiles into the disk file.
 *
 *  If we are doing backups then ...checkpoint_backup
 *  and ...checkpoint_written will have moved the dirty
 *  list to the scheduled list, otherwise we move dirty
 *  tiles ourself.
 *
 * @param td	opaque tile manager handle
 *
 * @return 0 on success, other nonzero error codes on error.
 * @return ADDB_ERR_ALREADY if there was nothing to do.
 */
int addb_tiled_checkpoint_start_writes(addb_tiled* td,
                                       unsigned long long horizon,
                                       bool hard_sync, bool block) {
  addb_handle* const addb = td->td_pool->tdp_addb;
  cl_handle* const cl = addb->addb_cl;
  size_t const ps = getpagesize();
  size_t const pages_per_tile = ADDB_TILE_SIZE / ps;
  addb_tile* tile;
  addb_tile* last_tile;
  size_t n_scheduled = 0;
  int err = 0;

  cl_assert(td->td_pool->tdp_cl,
            td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);

  /* If we're not doing anything or this stage is already
   * done, then we're done.
   */
  if (ADDB_CKS_DONE == td->td_checkpoint_stage ||
      ADDB_CKS_START_WRITES1 + 2 == td->td_checkpoint_stage)
    return 0;

  switch (td->td_checkpoint_stage) {
    case ADDB_CKS_START_WRITES1:
      cl_assert(cl, td->td_tbk.tbk_do_backup);
      cl_assert(cl, td->td_scheduled_head);

      /* Copy modified tile contents into the memory mapped file
       * Note that the tile may have been written again which
       * would cause tile_memory to be different from
       * tile_memory_scheduled.
       */

      tile = td->td_scheduled_head;
      last_tile = tile;
      while (tile) {
        size_t page_i;

        cl_assert(cl, tile->tile_memory_disk);
        cl_assert(cl, tile->tile_memory_scheduled);
        cl_assert(cl, tile->tile_scheduled_bits);

        for (page_i = 0; page_i < pages_per_tile; page_i++)
          if ((1 << page_i) & tile->tile_scheduled_bits)
            memcpy(tile->tile_memory_disk + (page_i * ps),
                   tile->tile_memory_scheduled + (page_i * ps), ps);

        cm_free(td->td_pool->tdp_cm, tile->tile_memory_scheduled);
        if (tile->tile_memory == tile->tile_memory_scheduled) {
          cl_assert(cl, !tile->tile_dirty_bits);

          tile->tile_memory = tile->tile_memory_disk;
          tile->tile_memory_disk = (addb_tile*)0;
        }
        tile->tile_memory_scheduled = (addb_tile*)0;

        tile = tile->tile_next;
        if (last_tile == tile) tile = 0;
        n_scheduled++;
      }

      /*  Return scheduled tiles to the dirty- or free-lists as appropriate
       */
      if (td->td_scheduled_head) {
        addb_tile* tile = td->td_scheduled_head;
        addb_tile* const last_tile = tile;

        while (tile) {
          addb_tile* const next_tile = tile->tile_next;

          tile->tile_scheduled_bits = 0;
          tile->tile_next = (addb_tile*)0;
          tile->tile_prev = (addb_tile*)0;
          tile_chain_in(tile);

          tile = next_tile;
          if (last_tile == tile) tile = 0;
        }
        td->td_scheduled_head = (addb_tile*)0;
      }

      td->td_checkpoint_stage++;
      /* falls through */

    case ADDB_CKS_START_WRITES2:
      if (!addb->addb_transactional) {
        td->td_checkpoint_stage = ADDB_CKS_DONE;
        return 0;
      }

      if (hard_sync) {
        err = addb_file_sync_start(cl, td->td_fd, &td->td_tbk.tbk_fsc,
                                   td->td_path, false);
        if (err) return err; /* enclosing switch allows this to be retried */
      }
      td->td_checkpoint_stage++;
      break;

    default:
      cl_notreached(cl, "Checkpoint stage %d unexpected",
                    td->td_checkpoint_stage);
  }

  cl_log(cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE,
         "%s: checkpoint (3): %zu tile writes scheduled.", td->td_path,
         n_scheduled);

  return err;
}

/**
 * @brief Phase 4 of a disk flush.
 *
 * flush mapped file I/O to disk.
 *
 * @param td		opaque tile manager handle
 * @param hard_sync	if true, data must have hit the disk before
 *			a successful return.
 * @param block		wait for completion before returning.
 *
 * @return 0 on success, other nonzero error codes on error.
 * @return ADDB_ERR_ALREADY if there was nothing to do.
 */
int addb_tiled_checkpoint_finish_writes(addb_tiled* td,
                                        unsigned long long horizon,
                                        bool hard_sync, bool block) {
  addb_handle* const addb = td->td_pool->tdp_addb;
  cl_handle* const cl = addb->addb_cl;
  int err = 0;

  cl_assert(cl, td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);

  /* If we're not doing anything or this stage is already
   * done, then we're done.
   */
  if (ADDB_CKS_DONE == td->td_checkpoint_stage ||
      ADDB_CKS_FINISH_WRITES + 1 == td->td_checkpoint_stage)
    return 0;

  cl_assert(cl, ADDB_CKS_FINISH_WRITES == td->td_checkpoint_stage);
  cl_assert(cl, !td->td_scheduled_head);
  cl_assert(cl, addb->addb_transactional);

  if (hard_sync) {
    err = addb_file_sync_finish(cl, &td->td_tbk.tbk_fsc, block, td->td_path);
    if (err) {
      if (ADDB_ERR_MORE == err)
        cl_log(cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE,
               "%s: checkpoint (4): ADDB_ERR_MORE", td->td_path);
      return err;
    }
  }

  td->td_checkpoint_stage++;

  cl_log(cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE,
         "%s: checkpoint (4): tiles sync'ed to disk.", td->td_path);

  return 0;
}

/**
 * @brief Phase 5 of the disk flush.
 *
 *  It's no longer necessary to be able to roll back to a
 *  previous horizon.  Remove the undo file.
 *
 *  This phase is called only after the entire system has
 *  collectively made it to phase 3.
 *
 * @param addb	database handle
 * @param td	opaque tile manager handle
 *
 * @return 0 on success, other nonzero error codes on error.
 * @return ADDB_ERR_ALREADY if there was nothing to do.
 */

int addb_tiled_checkpoint_remove_backup(addb_tiled* td,
                                        unsigned long long horizon,
                                        bool hard_sync, bool block) {
  addb_handle* const addb = td->td_pool->tdp_addb;
  cl_handle* const cl = addb->addb_cl;
  int err = 0;

  cl_assert(cl, td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);

  /* If we're not doing anything or this stage is already
   * done, then we're done.
   */
  if (ADDB_CKS_DONE == td->td_checkpoint_stage ||
      ADDB_CKS_REMOVE_BACKUP + 1 == td->td_checkpoint_stage)
    return 0;

  cl_assert(cl, ADDB_CKS_REMOVE_BACKUP == td->td_checkpoint_stage);
  cl_assert(cl, addb->addb_transactional);

  if (!td->td_tbk.tbk_published) return 0;

  /*  OK, at this point we know we're consistent, and we know
   *  that we know.  Remove the undo information; it won't be needed.
   */
  err = addb_backup_unpublish(addb, &td->td_tbk);
  if (err) return err;

  td->td_checkpoint_stage = ADDB_CKS_DONE;

  cl_log(cl, CL_LEVEL_SPEW | ADDB_FACILITY_TILE,
         "%s: checkpoint (5): backup removed.", td->td_path);

  return 0;
}

/* Write all modified tiles.
 */
int addb_tiled_checkpoint_write(addb_tiled* td, bool sync, bool block) {
  int err = 0;
  addb_handle* const addb = td->td_pool->tdp_addb;
  cl_handle* const cl = addb->addb_cl;

  /* We better not be doing backups
   */
  cl_assert(td->td_pool->tdp_cl, !td->td_tbk.tbk_do_backup);
  cl_assert(td->td_pool->tdp_cl,
            td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);

  /* It doesn't make sense to block if you don't wish to sync
   */
  block = block && sync;

  /* If we're syncing (the expected case), start syncing.
   * to sync td_fd.
   */
  if (sync) {
    err = addb_file_sync_start(cl, td->td_fd, &td->td_tbk.tbk_fsc, td->td_path,
                               false);
    if (err) return err;
  }

  /* We need to do this regardless of the sync parameter: sync may
   * have been on in the past and calling addb_file_sync_finish will
   * reap any threads that may still exist.
   */
  err = addb_file_sync_finish(cl, &td->td_tbk.tbk_fsc, block, td->td_path);

  cl_assert(cl, !block || err != ADDB_ERR_MORE);
  if (!sync && err == ADDB_ERR_MORE) err = 0;

  return err;
}

int addb_tiled_checkpoint_linear_start(addb_tiled* td, bool hard_sync,
                                       bool block) {
  int err;

  cl_assert(td->td_pool->tdp_cl, !td->td_tbk.tbk_do_backup);
  cl_assert(td->td_pool->tdp_cl,
            td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);
  td->td_checkpoint_stage = ADDB_CKS_START_WRITES2;

  err = addb_tiled_checkpoint_start_writes(td, 0, hard_sync, block);
  if (err == ADDB_ERR_ALREADY) return 0;

  return err;
}

int addb_tiled_checkpoint_linear_finish(addb_tiled* td, bool hard_sync,
                                        bool block) {
  int err;

  cl_assert(td->td_pool->tdp_cl, !td->td_tbk.tbk_do_backup);
  cl_assert(td->td_pool->tdp_cl,
            td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);
  err = addb_tiled_checkpoint_finish_writes(td, 0, hard_sync, block);
  if (err && err != ADDB_ERR_ALREADY) return err;

  td->td_checkpoint_stage = ADDB_CKS_DONE;

  return 0;
}

/**
 * @brief Has this tiled file been modified since its last save to disk?
 *
 * @param addb	database handle
 * @param td	opaque tile manager handle
 */
bool addb_tiled_is_dirty(addb_tiled* td) {
  cl_assert(td->td_pool->tdp_cl,
            td->td_pool->tdp_total >= td->td_pool->tdp_total_linked);

  return td->td_dirty_head || td->td_scheduled_head;
}

/**
 * @brief Is someone using this tiled file?
 *
 *  This is used by the largefile code to check whether a large file
 *  descriptor can be closed.
 *
 * @param addb	database handle
 * @param td	opaque tile manager handle
 */
bool addb_tiled_is_in_use(addb_tiled* td) {
  return addb_tiled_is_dirty(td) || td->td_total_linked ||
         td->td_checkpoint_stage != ADDB_CKS_DONE;
}

void addb_tiled_pool_set_max(addb_tiled_pool* tdp, unsigned long long m) {
  if ((tdp->tdp_max = m) < tdp->tdp_total) {
    (void)tiled_pool_flush(tdp, tdp->tdp_total - tdp->tdp_max);
    cl_cover(tdp->tdp_cl);
  }
}

void addb_tiled_set_mlock(addb_tiled* td, bool lock) {
  if (lock != td->td_locked) {
    if (lock)
      addb_tiled_mlock(td);
    else
      addb_tiled_munlock(td);
  }
}

int addb_tiled_pool_status(addb_tiled_pool* tdp, cm_prefix const* prefix,
                           addb_status_callback* cb, void* cb_data) {
  char buf[80];
  int err;
  cm_prefix pool;

  pool = cm_prefix_push(prefix, "pool");

  snprintf(buf, sizeof buf, "%llu", tdp->tdp_max);
  err = (*cb)(cb_data, cm_prefix_end(&pool, "bytes-max"), buf);
  if (err) return err;

  snprintf(buf, sizeof buf, "%llu", tdp->tdp_total - tdp->tdp_total_linked);
  err = (*cb)(cb_data, cm_prefix_end(&pool, "bytes-free"), buf);
  if (err) return err;

  snprintf(buf, sizeof buf, "%llu", tdp->tdp_total_linked);
  err = (*cb)(cb_data, cm_prefix_end(&pool, "bytes-in-use"), buf);
  if (err) return err;

  snprintf(buf, sizeof buf, "%llu", tdp->tdp_map_count);
  err = (*cb)(cb_data, cm_prefix_end(&pool, "map-count"), buf);
  if (err) return err;

  snprintf(buf, sizeof buf, "%llu", tdp->tdp_map_cached);
  err = (*cb)(cb_data, cm_prefix_end(&pool, "map-cached"), buf);
  if (err) return err;

  snprintf(buf, sizeof buf, "%llu", tdp->tdp_bytes_written);
  err = (*cb)(cb_data, cm_prefix_end(&pool, "bytes-written"), buf);
  if (err) return err;

  return 0;
}

/* Read the largest possible array of bytes from
 * an append-only datastructure in a tiled file
 */
void* addb_tiled_read_array_loc(addb_tiled* const td, unsigned long long s,
                                unsigned long long e, unsigned long long* e_out,
                                addb_tiled_reference* ref_out, char const* file,
                                int line) {
  addb_tiled_pool* const tdp = td->td_pool;
  long long const tile_min = s / ADDB_TILE_SIZE;
  long long const tile_max = (e - 1) / ADDB_TILE_SIZE;
  long long non_tile_max = tile_max;
  size_t const n_first_map_tiles = addb_tiled_first_map(td);

  if (s >= td->td_first_map_size) goto use_tiles;

  /* If we're doing backups and there's a tile
   * for the start of the array, we've just
   * grown the array; we'll have to use tiles.
   */
  if (td->td_tbk.tbk_do_backup && td->td_tile && tile_min < td->td_tile_m &&
      td->td_tile[tile_min])
    goto use_tiles;

  if (non_tile_max >= n_first_map_tiles) non_tile_max = n_first_map_tiles - 1;

  /* Modified tiles are only at the end of the array;
   * work backwards until we find an unmodified tile.
   * If the tile array isn't long enough, we couldn't
   * have written anything.
   */
  if (td->td_tile && non_tile_max < td->td_tile_m)
    while (non_tile_max >= tile_min &&
           td->td_tile[non_tile_max] /* there is a tile... */
           && (td->td_tile[non_tile_max]->tile_dirty_bits ||
               td->td_tile[non_tile_max]
                   ->tile_scheduled_bits)) /*... which is dirty */
      non_tile_max--;

  if (non_tile_max < tile_min) goto use_tiles;

  if (non_tile_max != tile_max) e = (non_tile_max + 1) * ADDB_TILE_SIZE;

  *e_out = e;

  {
    size_t const sz = (size_t)(e - s);

    td->td_total += sz;
    td->td_total_linked += sz;
    tdp->tdp_total += sz;
    tdp->tdp_total_linked += sz;
    *ref_out = ADDB_TILED_TREF_MAKE_INITMAP(sz);
  }

  return (char*)td->td_first_map + s;

use_tiles: /* just return the first tile */
  if (tile_min != tile_max) e = (tile_min + 1) * ADDB_TILE_SIZE;
  *e_out = e;

  return addb_tiled_get_loc(td, s, e, ADDB_MODE_READ, ref_out, file, line);
}

cl_handle* addb_tiled_cl(addb_tiled* td) { return td->td_pool->tdp_cl; }

unsigned long long addb_tiled_physical_file_size(addb_tiled* td) {
  return td->td_physical_file_size;
}

/*
 * Update tiled data structures for a file that may have changed on disk.
 * This extends the tiled limit counters and remaps data as required.
 */
int addb_tiled_stretch(addb_tiled* td) {
  struct stat sb;
  cl_handle* cl = td->td_pool->tdp_cl;

  int err;

  err = addb_file_fstat(cl, td->td_fd, td->td_path, &sb);
  if (err) {
    cl_log(cl, CL_LEVEL_ERROR, "addb_tiled_stretch: cannot fstat %s (%i)",
           td->td_path, td->td_fd);
    return err;
  }

  cl_log(cl, CL_LEVEL_VERBOSE, "addb_tiled_stretch: updating %s", td->td_path);

  /*
   * File has grown since we last touched it
   */
  if (td->td_physical_file_size < sb.st_size) {
    td->td_physical_file_size = sb.st_size;
  } else if (td->td_physical_file_size > sb.st_size) {
    cl_notreached(cl,
                  "addb_tiled_stretch: file %s has shrunk"
                  "from %llu bytes to %llu bytes",
                  td->td_path, (unsigned long long)td->td_physical_file_size,
                  (unsigned long long)sb.st_size);
  } else {
    cl_log(cl, CL_LEVEL_VERBOSE, "addb_tiled_stretch: file %s has not changed",
           td->td_path);
    return 0;
  }

  /*
   * If the file no longer fits inside the initmap, try to remap it.
   * This is safe as long as addb_tiled_stretch never gets called with
   * on a file with any read references]
   */
  if ((td->td_first_map_size > 0) &&
      td->td_first_map_size < td->td_physical_file_size) {
    size_t tiles;
    size_t bytes;
    void* m;

    tiles = (td->td_physical_file_size + ADDB_TILE_SIZE - 1) / ADDB_TILE_SIZE;

    /*  Leave ten percent extra space.
     */
    tiles = tiles + tiles / 10;
    bytes = tiles * ADDB_TILE_SIZE;

    /*
     * Assert read-onlyness
     */
    m = mmap(0, bytes, PROT_READ, MAP_SHARED, td->td_fd, 0);

    if (m == MAP_FAILED) {
      err = errno;

      switch (errno) {
        case EINVAL:
        case ENFILE:
        case ENOMEM:
        case EFAULT:

          /*
           * These errors imply that we may simply be
           * mmap'ing a segment thats too big for our
           * OS or VM space. Revert to tiles.
           */
          cl_log_errno(cl, CL_LEVEL_FAIL, "mmap", err,
                       "Unable to remap %llu bytes of initmap for %s. "
                       "Reverting to tiles",
                       (unsigned long long)bytes, td->td_path);
        default:
          cl_log_errno(
              cl, CL_LEVEL_ERROR, "mmap", err,
              "Unable to remap %llu bytes of initmap for %s. Fatal error",
              (unsigned long long)bytes, td->td_path);
          return err;
      }

    } else {
      int rv;
      cl_log(cl, CL_LEVEL_VERBOSE, "addb_tiled_stretch: remaped file %s at %p",
             td->td_path, m);
      rv = munmap(td->td_first_map, td->td_first_map_size);
      err = errno;
      if (rv < 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "munmap", err,
                     "Cannot unmap %llu bytes at %p", td->td_first_map_size,
                     td->td_first_map);
      }

      td->td_first_map = m;
      td->td_first_map_size = bytes;
    }
  } else {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "addb_tiled_stretch: grew file %s to %llu bytes"
           " (no remap required)",
           td->td_path, td->td_physical_file_size);
  }

  return 0;
}
