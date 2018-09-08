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
#include "libaddb/addb-hmap-file.h"
#include "libaddb/addb-hmap.h"
#include "libaddb/addbp.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "libcl/cl.h"

#define ADDB_HMAP_GM_SUBDIR "gm"
#define ADDB_HMAP_FILE "h-00.addb"

/** Initialize a bucket page.
*/

static void addb_hmap_bkp_init(addb_hmap_bucket_page* bkp, size_t bkp_size) {
  ADDB_BKP_FREE_OFFSET_SET(bkp, bkp_size - 1);
}

/** Return a pointer to a bucket's key storage
*/

static char* addb_hmap_hmb_key(addb_hmap_bucket_page* bkp,
                               addb_hmap_bucket* b) {
  return ADDB_HMAP_HMB_KEY(bkp, b);
}

/** Compare an indirect key
*
*	Return (cmp_value) semantics match memcmp.
*/

static int addb_hmap_iky_compare(addb_hmap* hm, char const* key, size_t key_len,
                                 addb_hmap_indirect_key* iky,
                                 int case_insensitive, int* cmp_value) {
  cl_handle* const cl = hm->hmap_addb->addb_cl;
  unsigned long long offset = ADDB_IKY_OFFSET(iky) * hm->hmap_bucket_page_size;

  cl_assert(cl, key);
  cl_assert(cl, iky);
  cl_assert(cl, cmp_value);
  cl_assert(cl, key_len > 0);
  cl_assert(cl, offset >= (hm->hmap_n_slots + 1) * hm->hmap_bucket_page_size);

  *cmp_value = 0;

  do {
    addb_tiled_reference the_tile;
    addb_hmap_iky_page* ikp = (addb_hmap_iky_page*)addb_tiled_get(
        hm->hmap_td, offset, offset + hm->hmap_bucket_page_size, ADDB_MODE_READ,
        &the_tile);
    if (!ikp) {
      cl_log_errno(hm->hmap_addb->addb_cl, CL_LEVEL_ERROR, "addb_tiled_get",
                   errno, "no bucket page for %llu",
                   offset / (unsigned long long)hm->hmap_bucket_page_size);
      return errno;
    }

    {
      size_t const len = ADDB_IKP_LENGTH(ikp);
      size_t const min_len = len < key_len ? len : key_len;

      if (case_insensitive)
        *cmp_value = strncasecmp(key, (char const*)ikp->ikp_key, min_len);
      else
        *cmp_value = memcmp(key, ikp->ikp_key, min_len);

      key_len -= min_len;
      key += min_len;
    }

    offset = ADDB_IKP_NEXT(ikp) * hm->hmap_bucket_page_size;
    addb_tiled_free(hm->hmap_td, &the_tile);

    if (*cmp_value)
      return 0; /* compared as not equal, we can stop */
    else if (0 == key_len) {
      /* ... but we're out of key
       */
      if (offset) {
        *cmp_value = -1; /* shorter comes first */
        return 0;
      }
      /* *cmp_value must be 0, just fall through */
    }

    cl_assert(cl, !(key_len > 0) == !offset);
  } while (offset);

  return 0;
}

/** Compare two buckets, a and b
 *
 *	Actually, it compares a bucket-to-be, a, against existing bucket b.
 *	Analogous to memcmp.
 *
 * 	Return -1, 0, or 1 if a < b, a == b, or a > b, respectively.
 */

static int addb_hmap_bucket_cmp(char const* a_key, size_t a_key_len,
                                addb_hmap_type a_type, addb_hmap* hm,
                                addb_hmap_bucket_page* bkp, addb_hmap_bucket* b,
                                int* cmp_value) {
  addb_hmap_type const b_type = ADDB_HMB_TYPE(b);

  if (a_type == b_type) {
    size_t const b_key_len = ADDB_HMB_KEY_LEN(b);
    size_t const min_len = (a_key_len < b_key_len) ? a_key_len : b_key_len;

    if (b_key_len <= ADDB_HMAP_BKP_MAX_KEY_LEN) {
      char const* b_key = addb_hmap_hmb_key(bkp, b);

      if (ADDB_HMAP_TYPE_KEY_IS_STRING(a_type))
        *cmp_value = strncasecmp(a_key, b_key, min_len);
      else
        *cmp_value = memcmp(a_key, b_key, min_len);
    } else {
      addb_hmap_indirect_key* iky =
          (addb_hmap_indirect_key*)addb_hmap_hmb_key(bkp, b);
      int err = addb_hmap_iky_compare(hm, a_key, a_key_len, iky,
                                      ADDB_HMAP_TYPE_KEY_IS_STRING(a_type),
                                      cmp_value);
      if (err) return err;
    }

    if (!*cmp_value && a_key_len != b_key_len) {
      if (a_key_len < b_key_len) /* shorter comes first */
        *cmp_value = 1;
      else
        *cmp_value = -1;
    }
  } else if (a_type < b_type)
    *cmp_value = -1;
  else
    *cmp_value = 1;

  return 0;
}

/** Binary search for a bucket on a bucket page
 *
 *  If we find a bucket, return a pointer to it.  If not, return
 *  the index to insert into.
 */

static int addb_hmap_bkp_bsearch_bucket(addb_hmap* hm,
                                        addb_hmap_bucket_page* bkp,
                                        char const* key, size_t key_len,
                                        addb_hmap_type type,
                                        addb_hmap_bucket** bucket_found,
                                        unsigned short* insert_at) {
  cl_handle* const cl = hm->hmap_addb->addb_cl;
  unsigned short const nb = ADDB_BKP_N_BUCKETS(bkp);

  *bucket_found = (addb_hmap_bucket*)0;
  *insert_at = 0;

  if (0 == nb)
    return 0;
  else {
    int start = 0;
    int end = (int)nb - 1;
    int cmp = 0;

    while (start <= end) {
      unsigned short middle = start + (end - start) / 2;
      addb_hmap_bucket* bkt = &bkp->bkp_buckets[middle];
      int err = addb_hmap_bucket_cmp(key, key_len, type, hm, bkp, bkt, &cmp);
      if (err) return err;

      if (cmp > 0)
        start = middle + 1;
      else if (0 == cmp) {
        *bucket_found = bkt;
        *insert_at = bkt - &bkp->bkp_buckets[0];

        return 0;
      } else if (cmp < 0)
        end = middle - 1;
      else
        cl_assert(cl, 0);
    }

    cl_assert(cl, cmp);

    *insert_at = start;
  }

  return 0;
}

#if 0
/** Find a bucket on a bucket page
*
*	For now, this is a simple linear probe.
*/

static int
addb_hmap_bkp_find_bucket(
	addb_hmap*  hm,
	addb_hmap_bucket_page*  bkp,
	char const*  key,
	size_t  key_len,
	addb_hmap_type  type,
	addb_hmap_bucket**  bucket_found )
{
	cl_handle* const  cl = hm->hmap_addb->addb_cl;
	unsigned short const  nb = ADDB_BKP_N_BUCKETS( bkp );
	addb_hmap_bucket  x;
	int  i;

	cl_assert( cl, nb <= ADDB_HMAP_BKP_MAX_N_BUCKETS );

	ADDB_HMB_KEY_LEN_SET( &x, key_len );
	ADDB_HMB_TYPE_SET( &x, type );
	*bucket_found = (addb_hmap_bucket*)0;


	for( i = 0; i < nb; i++)
	{
		addb_hmap_bucket*  b = &bkp->bkp_buckets[i];
		int  cmp_value;

		if (x.hmb_type != b->hmb_type)
			continue;  /* types don't agree */
		if ((x.hmb_key_len[0] != b->hmb_key_len[0]) ||
			(x.hmb_key_len[1] != b->hmb_key_len[1]))
			continue;  /* key lengths don't agree */
		if (0 == key_len)
			cmp_value = 0;
		else if (key_len <= ADDB_HMAP_BKP_MAX_KEY_LEN)
		{
			if (ADDB_HMAP_TYPE_KEY_IS_STRING( type ))
				cmp_value = strncasecmp(
					key,
					addb_hmap_hmb_key( bkp, b ), key_len );
			else
				cmp_value = memcmp( key, addb_hmap_hmb_key( bkp, b ), key_len );
		}
		else
		{
			addb_hmap_indirect_key*  iky =
				(addb_hmap_indirect_key*)addb_hmap_hmb_key( bkp, b );
			int  err =
				addb_hmap_iky_compare(
					hm,
					key,
					key_len,
					iky,
					ADDB_HMAP_TYPE_KEY_IS_STRING( type ),
					&cmp_value );
			if (err)
				return err;
		}
		if (!cmp_value)
		{
			*bucket_found = b;
			return 0;
		}
	}

	return 0;
}
#endif

/**
 * @brief Retrieve the header information of an HMAP file.
 *
 * @param hm		The hmap to retreive in
 * @param mode		ADDB_MODE_READ, ADDB_MODE_WRITE, or both.
 * @param ref_out	assign the reference to this.
 *
 * @return NULL on error (details in errno)
 * @return otherwise, a pointer to the hmap header.
 */

static addb_hmap_header* addb_hmh(addb_hmap* hm, int mode,
                                  addb_tiled_reference* ref_out) {
  cl_assert(hm->hmap_addb->addb_cl, hm->hmap_td);

  return (addb_hmap_header*)addb_tiled_get(
      hm->hmap_td, 0, 0 + hm->hmap_bucket_page_size, mode, ref_out);
}

/**
 * @brief Allocate a new page in an HMAP
 *
 * @param hm		The hmap to allocate in
 * @param offset_out	store the offset (in pages) here
 * @param ref_out	return a reference here
 *
 * @return NULL on error, a non-NULL page pointer on success.
 */

static void* addb_hmap_new_page(
    addb_hmap* hm, unsigned long long* offset_out, /* offset in pages */
    addb_tiled_reference* ref_out) {
  int err;
  addb_tiled_reference hmh_tile;
  addb_hmap_header* hmh = addb_hmh(hm, ADDB_MODE_READ_WRITE, &hmh_tile);

  if (!hmh)
    return (void*)0;
  else {
    unsigned long long o = ADDB_HMH_LAST_BKP_OFFSET(hmh);
    unsigned long long s = (o + 1) * hm->hmap_bucket_page_size;
    unsigned long long e = s + hm->hmap_bucket_page_size - 1;
    void* new_page = addb_tiled_alloc(hm->hmap_td, s, e, ref_out);

    if (!new_page) goto free_hmh;

    *offset_out = o + 1;
    ADDB_HMH_LAST_BKP_OFFSET_SET(hmh, *offset_out);

    addb_tiled_free(hm->hmap_td, &hmh_tile);

    return new_page;
  }

free_hmh:
  /*  Save and restore errno around the call to free(), just in case.
   */
  err = errno;
  addb_tiled_free(hm->hmap_td, &hmh_tile);
  errno = err;
  return (void*)0;
}

static addb_hmap_bucket_page* addb_hmap_new_bkp(addb_hmap* hm,
                                                addb_hmap_bucket_page* prev_bkp,
                                                addb_tiled_reference* ref_out) {
  unsigned long long new_bkp_off;
  addb_hmap_bucket_page* new_bkp =
      addb_hmap_new_page(hm, &new_bkp_off, ref_out);

  if (!new_bkp) return (addb_hmap_bucket_page*)0;

  addb_hmap_bkp_init(new_bkp, hm->hmap_bucket_page_size);
  ADDB_BKP_NEXT_OFFSET_SET(prev_bkp, new_bkp_off);

  return new_bkp;
}

static size_t addb_hmap_bkp_key_storage_remaining(addb_hmap_bucket_page* bkp) {
  return ADDB_HMAP_KEY_STORAGE_REMAINING(bkp);
}

/** Compute key storage needed, taking indirect keys into account
*/

static size_t addb_hmap_bkp_key_storage_needed(size_t key_len) {
  return (key_len <= ADDB_HMAP_BKP_MAX_KEY_LEN)
             ? key_len
             : sizeof(addb_hmap_indirect_key);
}

/** Write an indirect key
*/

static int addb_hmap_iky_write(addb_hmap* hm, char const* key, size_t key_len,
                               unsigned long long* offset_out) {
  int err = 0;
  addb_tiled_reference the_tile;
  addb_hmap_iky_page* ikp = addb_hmap_new_page(hm, offset_out, &the_tile);

  if (!ikp) return ENOMEM;

  {
    size_t l = (key_len <= ADDB_IKP_MAX_LENGTH) ? key_len : ADDB_IKP_MAX_LENGTH;

    ADDB_IKP_LENGTH_SET(ikp, l);
    memcpy(ikp->ikp_key, key, l);

    key += l;
    key_len -= l;
  }

  if (key_len) {
    unsigned long long next_off;
    err = addb_hmap_iky_write(hm, key, key_len, &next_off);

    if (!err) ADDB_IKP_NEXT_SET(ikp, next_off);
  }

  addb_tiled_free(hm->hmap_td, &the_tile);

  return err;
}

/** Add a bucket to a bucket page
*
*	The caller is responsible for making sure that the bucket does
*	not already exist, that adequate key storage does and that
*	the current bucket page is locked for writing.
*
*	A zero return indicates ENOMEM
*/

static addb_hmap_bucket* addb_hmap_bkp_add_bucket(
    addb_hmap* hm, addb_hmap_bucket_page* bkp, unsigned short insert_at,
    char const* key, size_t key_len, addb_hmap_type type) {
  addb_hmap_bucket* b;

  if (key_len <= ADDB_HMAP_BKP_MAX_KEY_LEN) {
    unsigned short nb = ADDB_BKP_N_BUCKETS(bkp);
    unsigned short fo = ADDB_BKP_FREE_OFFSET(bkp);
    unsigned short const ko = key_len > 0 ? fo - key_len + 1 : 0;
    size_t const ksr = addb_hmap_bkp_key_storage_remaining(bkp);

    cl_assert(hm->hmap_addb->addb_cl, ksr >= key_len);

    b = &bkp->bkp_buckets[insert_at];
    if (nb > 0) {
      addb_hmap_bucket* e = &bkp->bkp_buckets[nb - 1];

      while (e >= b) {
        *(e + 1) = *e; /* slide buckets down 1 */
        e--;
      }
    }

    ADDB_HMB_KEY_OFFSET_SET(b, ko);
    ADDB_HMB_KEY_LEN_SET(b, key_len);
    ADDB_HMB_TYPE_SET(b, type);
    if (key_len > 0) memcpy(addb_hmap_hmb_key(bkp, b), key, key_len);

    fo -= key_len;
    ADDB_BKP_FREE_OFFSET_SET(bkp, fo);
    nb++;
    ADDB_BKP_N_BUCKETS_SET(bkp, nb);
  } else {
    addb_hmap_indirect_key iky;
    unsigned long long o;

    cl_log(hm->hmap_addb->addb_cl, CL_LEVEL_SPEW,
           "addb_hmap_add_bucket: indirect key, len=%d type=%d", (int)key_len,
           type);

    if (addb_hmap_iky_write(hm, key, key_len, &o))
      return (addb_hmap_bucket*)0; /* ENOMEM */

    ADDB_IKY_OFFSET_SET(&iky, o);

    b = addb_hmap_bkp_add_bucket(hm, bkp, insert_at, (char*)&iky, sizeof iky,
                                 type);

    if (b) ADDB_HMB_KEY_LEN_SET(b, key_len); /* indicates indirect key */
  }

  return b;
}

/** A write address tells where to write a key
*
*	There are three possibilities:
*
*	1. if wa_page_fitting is set, it is the offset of a bucket page
*	which will fit the key.  The bucket offset for insert is given
*	in wa_bucket
*
*	2. if wa_last_page is set, it is the offset of the last bucket
*	page in the chain.  The chain will have to be extended to
*	fit the key
*
*	3. if wa_page is set, it is the offset of the page containing
*	the bucket for the key.  The bucket offset is given in wa_bucket
*/

typedef struct addb_hmap_write_address {
  unsigned long long wa_page_fitting; /* offset of page fitting key */
  unsigned long long wa_last_page;    /* last page in chain */
  unsigned long long wa_page;         /* page offset */
  unsigned short wa_bucket;           /* bucket offset (in wa_page{_fitting}) */
} addb_hmap_write_address;

/**
 * @brief Search a chain of bucket pages for a bucket matching the key
 *
 *  Bucket page chains are expected to be very short, usually of length 1.
 *  This method is read-only.  If the caller is interested in writing,
 *  a write address must be passed and the necessary tile locking handled
 *  separately.
 */

static int addb_hmap_find(addb_hmap* hm, addb_hmap_bucket_page* bkp,
                          unsigned long long bkp_off, char const* key,
                          size_t key_len, addb_hmap_type type,
                          addb_gmap_id* val_out, addb_hmap_write_address* wa) {
  addb_hmap_bucket* bk;
  unsigned short insert_at;
  int err = 0;
  bool tref_set = false;
  addb_tiled_reference tref;
  unsigned long long next_bkp_off;

  /*  Break out of this loop if we find something,
   *  or find the place where it goes.
   */
  for (;;) {
    err = addb_hmap_bkp_bsearch_bucket(hm, bkp, key, key_len, type, &bk,
                                       &insert_at);
    if (err != 0) break;

    cl_assert(hm->hmap_addb->addb_cl, val_out);

    /*  Did we find a bucket?
     */
    if (bk) {
      if (wa) {
        wa->wa_page_fitting = 0;
        wa->wa_page = bkp_off;
        wa->wa_bucket = bk - &bkp->bkp_buckets[0];
      }

      *val_out = ADDB_HMB_VALUE(bk);
      err = 0;
      break;
    }

    /*  We want to append, and there's room?
     */
    /* XXX why >, why not = */

    /*  If we're looking for a place to write, remember
     *  the first good slot.
     */
    if (wa != NULL && !wa->wa_page_fitting &&
        addb_hmap_bkp_key_storage_remaining(bkp) >
            addb_hmap_bkp_key_storage_needed(key_len)) {
      wa->wa_page_fitting = bkp_off;
      wa->wa_bucket = insert_at;
    }

    /* Check remainder of bucket page chain
    */
    next_bkp_off = ADDB_BKP_NEXT_OFFSET(bkp) * hm->hmap_bucket_page_size;
    if (!next_bkp_off) {
      if (wa && !wa->wa_page_fitting)
        wa->wa_last_page = bkp_off; /* end of the chain */
      err = ADDB_ERR_NO;
      break;
    }

    if (tref_set) {
      addb_tiled_free(hm->hmap_td, &tref);
      tref_set = false;
    }

    bkp = (addb_hmap_bucket_page*)addb_tiled_get(
        hm->hmap_td, next_bkp_off, next_bkp_off + hm->hmap_bucket_page_size,
        ADDB_MODE_READ, &tref);
    if (bkp == NULL) {
      cl_log_errno(
          hm->hmap_addb->addb_cl, CL_LEVEL_ERROR, "addb_tiled_get", errno,
          "no bucket page for %llu",
          next_bkp_off / (unsigned long long)hm->hmap_bucket_page_size);
      err = errno;
      break;
    }
    tref_set = true;
    bkp_off = next_bkp_off;
  }
  if (tref_set) addb_tiled_free(hm->hmap_td, &tref);

  return err;
}

/** given an offset, locate (and initialize) the bucket page.
*
*	If we're writing, uninitialized pages will be initialized
*	and returned.  If we're reading an uninitialized page will
*	cause a ADDB_ERR_NO return indicating that the value in question
*	wasn't found.
*/

static int addb_hmap_bucket_page_from_offset(addb_hmap* hm,
                                             unsigned long long offset,
                                             int mode,
                                             addb_tiled_reference* tile_out,
                                             addb_hmap_bucket_page** bkp_out) {
  cl_handle* const cl = hm->hmap_addb->addb_cl;

  cl_assert(cl, !(offset & (hm->hmap_bucket_page_size -
                            1))); /* offset is page aligned */

  *bkp_out = (addb_hmap_bucket_page*)addb_tiled_get(
      hm->hmap_td, offset, offset + hm->hmap_bucket_page_size, mode, tile_out);

  if (!*bkp_out) {
    cl_log_errno(hm->hmap_addb->addb_cl, CL_LEVEL_ERROR, "addb_tiled_get",
                 errno, "no bucket page for %llu",
                 offset / (unsigned long long)hm->hmap_bucket_page_size);
    return errno;
  }

  if (!addb_hmap_bkp_initialized(*bkp_out)) {
    if (mode & ADDB_MODE_WRITE)
      addb_hmap_bkp_init(*bkp_out, hm->hmap_bucket_page_size);
    else {
      *bkp_out = (addb_hmap_bucket_page*)0;
      addb_tiled_free(hm->hmap_td, tile_out);
      return ADDB_ERR_NO;
    }
  }

  return 0;
}

/** Locate the Ith slot (bucket page) in the table and return it.
*/

static int addb_hmap_slot(addb_hmap* hm, unsigned long long i, int mode,
                          addb_tiled_reference* tile_out,
                          addb_hmap_bucket_page** bkp_out,
                          unsigned long long* bkp_off_out) {
  cl_handle* const cl = hm->hmap_addb->addb_cl;
  unsigned long long slot_offset =
      hm->hmap_bucket_page_size * i + hm->hmap_first_slot_offset;

  cl_assert(cl, i < hm->hmap_n_slots);

  *bkp_off_out = slot_offset;

  return addb_hmap_bucket_page_from_offset(hm, slot_offset, mode, tile_out,
                                           bkp_out);
}

/** Retrieve the value (gmap id or singleton) based on the key
*/

static int addb_hmap_value(addb_hmap* hm, unsigned long long hash_of_key,
                           char const* key, size_t key_len, addb_hmap_type type,
                           addb_gmap_id* val_out,
                           addb_hmap_write_address* wa_out) {
  unsigned long long i = hash_of_key % hm->hmap_n_slots;
  addb_tiled_reference the_tile;
  unsigned long long bkp_off;
  addb_hmap_bucket_page* bkp;
  int err;

  cl_assert(hm->hmap_addb->addb_cl, key_len >= 0);

  if (val_out) *val_out = 0;
  if (wa_out) {
    wa_out->wa_page_fitting = 0;
    wa_out->wa_last_page = 0;
    wa_out->wa_page = 0;
    wa_out->wa_bucket = 0;
  }

  err = addb_hmap_slot(hm, i, ADDB_MODE_READ, &the_tile, &bkp, &bkp_off);
  if (ADDB_ERR_NO == err) {
    if (wa_out) wa_out->wa_page_fitting = bkp_off;
    return err;
  } else if (err)
    return err;

  cl_assert(hm->hmap_addb->addb_cl, bkp);
  cl_assert(hm->hmap_addb->addb_cl, bkp_off);

  err = addb_hmap_find(hm, bkp, bkp_off, key, key_len, type, val_out, wa_out);

  addb_tiled_free(hm->hmap_td, &the_tile);

  return err;
}

int addb_hmap_read_value(addb_hmap* hm, unsigned long long hash_of_key,
                         char const* key, size_t key_len, addb_hmap_type type,
                         addb_gmap_id* val_out) {
  return addb_hmap_value(hm, hash_of_key, key, key_len, type, val_out,
                         (addb_hmap_write_address*)0);
}

static int addb_hmap_add_single_value(addb_hmap* hm, addb_hmap_bucket_page* bkp,
                                      unsigned short insert_at, char const* key,
                                      size_t key_len, addb_hmap_type type,
                                      addb_gmap_id id) {
  addb_hmap_bucket* b =
      addb_hmap_bkp_add_bucket(hm, bkp, insert_at, key, key_len, type);

  if (!b) return ENOMEM;

  ADDB_HMB_VALUE_SET(b, ADDB_GMAP_IVAL_MAKE_SINGLE(id));

  return 0;
}

int addb_hmap_add(addb_hmap* hm, unsigned long long hash_of_key,
                  char const* key, size_t key_len, addb_hmap_type type,
                  addb_gmap_id id) {
  addb_hmap_write_address wa;
  addb_gmap_id val;
  unsigned long long pg_off = 0;
  int err;

  cl_log(
      hm->hmap_addb->addb_cl, CL_LEVEL_SPEW,
      "addb_hmap_add: \"%.*s\" h=%llu len=%d type=%d -> %llu",
      ADDB_HMAP_TYPE_KEY_IS_STRING(type) ? (int)key_len : (int)sizeof "[bits]",
      ADDB_HMAP_TYPE_KEY_IS_STRING(type) ? key : "[bits]", hash_of_key,
      (int)key_len, type, (unsigned long long)id);

  err = addb_hmap_value(hm, hash_of_key, key, key_len, type, &val, &wa);

  if (!err) {
    cl_assert(hm->hmap_addb->addb_cl, !wa.wa_page_fitting);
    cl_assert(hm->hmap_addb->addb_cl, !wa.wa_last_page);
    cl_assert(hm->hmap_addb->addb_cl, wa.wa_page);

    if (ADDB_GMAP_IVAL_IS_SINGLE(val)) {
      if (ADDB_GMAP_IVAL_SINGLE(val) >= id) return ADDB_ERR_EXISTS;

      pg_off = wa.wa_page;
    } else
      return addb_gmap_add(hm->hmap_gm, val, id, true);
  } else if (ADDB_ERR_NO == err) {
    /* Didn't find anything so we better have located either a page
    *  that will fit this key, or the last page so that we can chain
    *  a new page on.
    */
    if (wa.wa_page_fitting)
      pg_off = wa.wa_page_fitting;
    else if (wa.wa_last_page)
      pg_off = wa.wa_last_page;
    else
      cl_assert(hm->hmap_addb->addb_cl, 0); /* no place to write */
  } else
    return err;

  cl_assert(hm->hmap_addb->addb_cl, pg_off);

  /* At this point we know we have to write something and we have the page
  *  offset of the page we're going to start with.
  */

  {
    addb_tiled_reference bkp_tile;
    addb_hmap_bucket_page* bkp;

    err = addb_hmap_bucket_page_from_offset(hm, pg_off, ADDB_MODE_WRITE,
                                            &bkp_tile, &bkp);
    if (err) return err;

    if (wa.wa_page_fitting)
      /* Add a new bucket/value to an existing page
      */
      err = addb_hmap_add_single_value(hm, bkp, wa.wa_bucket, key, key_len,
                                       type, id);
    else if (wa.wa_last_page)
    /* Add a new bucket/value to a new page.
    */
    {
      addb_tiled_reference new_tile;
      addb_hmap_bucket_page* new_bkp = addb_hmap_new_bkp(hm, bkp, &new_tile);
      if (!new_bkp)
        err = ENOMEM;
      else {
        err =
            addb_hmap_add_single_value(hm, new_bkp, 0, key, key_len, type, id);

        addb_tiled_free(hm->hmap_td, &new_tile);
      }
    } else if (wa.wa_page)
    /* We have a single entry and we're adding a second id to it.
    *  Allocate a new gmap entry, add both ids to it and store
    *  the gmap entry in the bucket.
    */
    {
      addb_tiled_reference hmh_tile;
      addb_hmap_bucket* b = &bkp->bkp_buckets[wa.wa_bucket];
      addb_hmap_header* hmh = (addb_hmap_header*)addb_tiled_get(
          hm->hmap_td, 0, 0 + sizeof *hmh, ADDB_MODE_WRITE, &hmh_tile);
      addb_gmap_id next_id = ADDB_HMH_NEXT_ENTRY(hmh);

      if (!hmh) {
        cl_log_errno(hm->hmap_addb->addb_cl, CL_LEVEL_ERROR, "addb_tiled_get",
                     errno, "no bucket page for %llu", (unsigned long long)0);
        err = errno;
      } else {
        cl_assert(hm->hmap_addb->addb_cl, ADDB_GMAP_IVAL_IS_SINGLE(val));

        ADDB_HMH_NEXT_ENTRY_SET(hmh, next_id + 1);

        err = addb_gmap_add(hm->hmap_gm, next_id, ADDB_GMAP_IVAL_SINGLE(val),
                            true);
        if (err) goto free_hmh_tile;

        err = addb_gmap_add(hm->hmap_gm, next_id, id, true);
        if (err) goto free_hmh_tile;

        ADDB_HMB_VALUE_SET(b, next_id);

      free_hmh_tile:
        addb_tiled_free(hm->hmap_td, &hmh_tile);
      }
    } else
      cl_assert(hm->hmap_addb->addb_cl, 0); /* invalid write address */

    addb_tiled_free(hm->hmap_td, &bkp_tile);
  }

  return err;
}

int addb_hmap_open(addb_handle* addb, char const* path, int mode,
                   unsigned long long estimated_size,
                   unsigned long long horizon, addb_hmap_configuration* hcf,
                   addb_gmap_configuration* gcf, addb_hmap** hm_out) {
  unsigned long long n_slots;
  cl_handle* const cl = addb->addb_cl;
  char gm_dir[] = ADDB_HMAP_GM_SUBDIR;
  char hmap_file[] = ADDB_HMAP_FILE;
  size_t path_n;
  int new_file;
  addb_hmap* hm;
  union {
    addb_hmap_header h;
    char bits[ADDB_HMAP_HEADER_SIZE];
  } hmh;
  struct stat st;
  int fd;
  int err = 0;
  char* dir_path;
  char* file_path;
  addb_tiled* td;
  size_t bucket_page_size = ADDB_HMAP_BUCKET_PAGE_SIZE;
  unsigned long long file_size;
  struct addb_tiled_pool* tiled_pool;
  char* gm_path;

  *hm_out = 0;

  /* If the directory doesn't yet exist, try to create it.
   */
  if ((mode & ADDB_MODE_WRITE) && mkdir(path, 0755)) {
    if (errno != EEXIST) {
      err = errno;
      cl_log_errno(cl, CL_LEVEL_ERROR, "mkdir", err,
                   "failed to create hmap database directory \"%s\"", path);
      return err;
    }
  }
  if (stat(path, &st)) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "stat", err,
                 "can't stat hmap database directory \"%s\"", path);
    return err;
  }
  if (!S_ISDIR(st.st_mode)) {
    cl_log(cl, CL_LEVEL_ERROR,
           "[%s:%d] addb: \"%s\" exists, but is not a directory", __FILE__,
           __LINE__, path);
    return ENOTDIR;
  }

  path_n = strlen(path);
  if (path_n > 0 && '/' == path[path_n - 1])
    path_n--; /* get rid of trailing slash */

  {
    size_t hmap_f_sz =
        path_n + 1 + sizeof hmap_file; /* the hmap file, / replaces one null */
    size_t gmap_d_sz = path_n + 1 + sizeof gm_dir; /* the gmap directory */
    size_t extra_bytes = path_n + 1 + hmap_f_sz + gmap_d_sz;

    hm = (addb_hmap*)cm_zalloc(addb->addb_cm, sizeof *hm + extra_bytes);
    if (!hm) {
      err = errno;
      cl_log_errno(cl, CL_LEVEL_ERROR, "cm_zalloc", err,
                   "addb: failed to allocate %lu bytes for "
                   "hmap database structure for \"%s\"",
                   (unsigned long)(sizeof *hm + extra_bytes), path);
      return err;
    }

    hm->hmap_gm = NULL;

    dir_path = (char*)(hm + 1);
    memcpy(dir_path, path, path_n);
    dir_path[path_n] = 0;

    file_path = dir_path + path_n + 1;
    memcpy(file_path, path, path_n);
    file_path[path_n] = '/';
    strcpy(file_path + path_n + 1, hmap_file);

    gm_path = file_path + hmap_f_sz;
    memcpy(gm_path, path, path_n);
    gm_path[path_n] = '/';
    strcpy(gm_path + path_n + 1, gm_dir);

    cl_assert(cl, gm_path + strlen(gm_path) + 1 ==
                      (char*)hm + sizeof *hm + extra_bytes);
  }

  fd = open(file_path,
            mode == ADDB_MODE_READ_ONLY ? O_RDONLY : (O_RDWR | O_CREAT), 0666);
  if (-1 == fd) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "open", err, "addb: open \"%s\" fails",
                 file_path);
    goto free_hm;
  }

  hm->hmap_dir_fd = open(path, O_RDONLY);
  if (hm->hmap_dir_fd < 0) {
    err = errno;
    cl_log_errno(cl, CL_LEVEL_ERROR, "open", err, "Can't open path: %s", path);
    goto close_fd;
  }

  err = addb_file_fstat(cl, fd, file_path, &st);
  if (err != 0) goto close_fd;

  file_size = st.st_size;

  new_file = file_size < bucket_page_size;
  if (new_file) {
    unsigned long long phys_size;

    n_slots = estimated_size / ADDB_HMAP_BUCKET_PAGE_SIZE;
    if (n_slots <= 1) n_slots = 2;

    phys_size = ADDB_HMAP_HEADER_SIZE + n_slots * bucket_page_size;
    cl_assert(cl, sizeof(addb_hmap_header) < ADDB_HMAP_HEADER_SIZE);

    if (!(mode & ADDB_MODE_WRITE)) {
      err = EINVAL;
      goto close_fd;
    }

    if (file_size >= sizeof hmh.h) {
      cl_log(cl, CL_LEVEL_ERROR,
             "addb_hmap_open: \"%s\" appears to have been truncated",
             file_path);
    }

    memset(hmh.bits, 0, sizeof hmh);

    cl_assert(cl, sizeof(hmh.h.hmh_magic) == sizeof ADDB_HMAP_MAGIC - 1);

    memcpy(hmh.h.hmh_magic, ADDB_HMAP_MAGIC, sizeof hmh.h.hmh_magic);
    ADDB_HMH_BKP_SIZE_SET(&hmh.h, bucket_page_size);

    ADDB_HMH_N_SLOTS_SET(&hmh.h, n_slots);
    ADDB_HMH_LAST_BKP_OFFSET_SET(&hmh.h, n_slots);

    err = addb_file_write(addb, fd, file_path, (char const*)hmh.bits,
                          sizeof hmh.bits);
    if (err) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "addb_file_write", err,
                   "addb_file_write \"%s\" fails", file_path);
      goto close_fd;
    }

    file_size = addb_round_up(phys_size, ADDB_TILE_SIZE);
    err = addb_file_truncate(addb, fd, file_path, file_size);
    if (err) goto close_fd;
  } else /* an existing file */
  {
    unsigned long long last_bkp_offset;

    if (addb_round_up(file_size, ADDB_TILE_SIZE) != file_size)
      cl_log(cl, CL_LEVEL_ERROR, "%s: non-tile size in HMAP %llu", file_path,
             (unsigned long long)file_size);

    err = addb_file_read(addb, fd, file_path, (char*)hmh.bits, sizeof hmh.bits,
                         false); /* don't expect EOF */
    if (err) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "addb_file_read", err,
                   "%s: can't read header", file_path);
      goto close_fd;
    }

    if (memcmp(&hmh.h.hmh_magic, ADDB_HMAP_MAGIC, sizeof hmh.h.hmh_magic)) {
      cl_log(cl, CL_LEVEL_ERROR,
             "%s: invalid magic number in "
             "HMAP file (want: %s, got %.4s)",
             file_path, ADDB_HMAP_MAGIC, hmh.h.hmh_magic);
      err = EINVAL;
      goto close_fd;
    }

    bucket_page_size = ADDB_HMH_BKP_SIZE(&hmh.h);
    if (ADDB_HMAP_BUCKET_PAGE_SIZE != bucket_page_size) {
      cl_log(cl, CL_LEVEL_OVERVIEW, "%s: non-standard bucket page size: %llu",
             file_path, (unsigned long long)bucket_page_size);
    }
    n_slots = ADDB_HMH_N_SLOTS(&hmh.h);
    last_bkp_offset = ADDB_HMH_LAST_BKP_OFFSET(&hmh.h);
    {
      unsigned long long o = last_bkp_offset * bucket_page_size;
      if (o > file_size)
        cl_log(cl, CL_LEVEL_ERROR,
               "%s: stat doesn't agree with internal offset (%llu > %llu)",
               file_path, o, file_size);
    }
    if (n_slots > last_bkp_offset) {
      cl_log(cl, CL_LEVEL_ERROR,
             "%s: n slots (%llu) > last offset (%llu) corrupt HMAP?", file_path,
             n_slots, last_bkp_offset);
      err = EINVAL;
      goto close_fd;
    }
  }

  err = addb_file_advise_random(cl, fd, file_path);
  if (err != 0) goto close_fd;

  tiled_pool = addb->addb_master_tiled_pool;
  cl_assert(cl, tiled_pool != NULL);

  td = addb_tiled_create(tiled_pool, file_path, O_RDWR, hcf->hcf_init_map);
  if (!td) {
    err = ENOMEM;
    goto close_fd;
  }

  addb_tiled_set_mlock(td, hm->hmap_cf.hcf_mlock);
  addb_tiled_backup(td, 1);
  err = addb_tiled_read_backup(td, horizon);
  if (err) {
    cl_log(cl, CL_LEVEL_ERROR, "%s: cannot initialize backup", file_path);
    goto free_td;
  }

  /* Create the underlying gmap.
   */
  hm->hmap_gm = addb_gmap_open(addb, gm_path, mode, horizon, gcf);
  if (!hm->hmap_gm) {
    err = ENOMEM;
    goto free_td;
  }
  hm->hmap_addb = addb;
  hm->hmap_dir_path = dir_path;
  hm->hmap_file_path = file_path;
  hm->hmap_td = td;
  hm->hmap_horizon = horizon;
  hm->hmap_backup = 0;
  hm->hmap_bucket_page_size = bucket_page_size;
  hm->hmap_n_slots = n_slots;
  hm->hmap_first_slot_offset = bucket_page_size;
  hm->hmap_tiled_pool = tiled_pool;
  hm->hmap_gm_path = gm_path;
  addb_file_sync_initialize(addb, &hm->hmap_dir_fsync_ctx);

  err = addb_file_close(addb, fd, file_path);
  if (err != 0) goto free_td;

  *hm_out = hm;
  addb_hmap_configure(hm, hcf, gcf);

  /*
   * Override gm_bitmap because we never want to use that
   * for hmaps rightnow
   */
  hm->hmap_gm->gm_bitmap = false;
  return 0;

/* Error handling/cleanup
*/
free_td:
  if (td) (void)addb_tiled_destroy(td);

close_fd:
  (void)addb_file_close(addb, fd, file_path);

free_hm:
  if (hm) {
    if (hm->hmap_gm != NULL) addb_gmap_close(hm->hmap_gm);
    cm_free(addb->addb_cm, hm);
  }

  if (!err) {
    cl_log(cl, CL_LEVEL_ERROR, "addb_hmap_open failed but errno == 0 [%s:%d]",
           __FILE__, __LINE__);
    err = -1;
  }

  return err;
}

int addb_hmap_close(addb_hmap* hm) {
  addb_handle* addb;
  int err = 0;
  int e;

  if (!hm) return 0;

  addb = hm->hmap_addb;

  if (hm->hmap_td) {
    e = addb_tiled_destroy(hm->hmap_td);
    if (e && !err) err = e;
    hm->hmap_td = (addb_tiled*)0;
  }
  if (hm->hmap_tiled_pool) {
    hm->hmap_tiled_pool = (addb_tiled_pool*)0;
  }

  if (hm->hmap_gm) {
    e = addb_gmap_close(hm->hmap_gm);
    if (e && !err) err = e;
    hm->hmap_gm = (addb_gmap*)0;
  }

  close(hm->hmap_dir_fd);
  cm_free(addb->addb_cm, hm);
  return err;
}

int addb_hmap_remove(addb_handle* addb, char const* path) {
  char gm_dir[] = ADDB_HMAP_GM_SUBDIR;
  char hmap_file[] = ADDB_HMAP_FILE;
  size_t path_n = strlen(path);
  size_t pb_size =
      path_n + 1 +
      (sizeof gm_dir > sizeof hmap_file ? sizeof gm_dir : sizeof hmap_file);
  char* pb = cm_malloc(addb->addb_cm, pb_size);
  struct stat st;
  int err = 0;

  if (!pb) {
    err = errno;
    cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "cm_malloc", err,
                 "addb: failed to allocate %lu bytes for hmap "
                 "file name path buffer",
                 (unsigned long)pb_size);
    return err;
  }

  memcpy(pb, path, path_n);
  if ('/' != pb[path_n - 1]) {
    pb[path_n] = '/';
    path_n++;
  }

  strcpy(pb + path_n, gm_dir);
  if (-1 == stat(pb, &st)) {
    err = errno;
    if (err != ENOENT) {
      cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "stat", err,
                   "stat( %s ) fails", pb);
      goto free_pb;
    }
  } else {
    err = addb_gmap_remove(addb, pb);
    if (err) {
      cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "addb_gmap_remove", err,
                   "addb: can't remove gmap \"%s\"", pb);
      goto free_pb;
    }
  }

  strcpy(pb + path_n, hmap_file);
  if (unlink(pb)) {
    err = errno;
    if (err == ENOENT)
      err = 0;
    else {
      cl_log_errno(addb->addb_cl, CL_LEVEL_ERROR, "unlink", err,
                   "addb: can't remove hmap \"%s\"", pb);
      goto free_pb;
    }
  }

  cm_free(addb->addb_cm, pb);

  if (rmdir(path)) {
    err = errno;
    cl_log_errno(addb->addb_cl,
                 err == ENOTEMPTY ? CL_LEVEL_FAIL : CL_LEVEL_ERROR, "rmdir",
                 err, "addb: can't remove hmap directory \"%s\"", path);
    return err;
  }

  return 0;

free_pb:
  cl_assert(addb->addb_cl, err);
  cm_free(addb->addb_cm, pb);

  return err;
}

int addb_hmap_truncate(addb_hmap* hm, char const* path) {
  if (hm) {
    addb_handle* const addb = hm->hmap_addb;
    int err = 0;
    int e;

    e = addb_gmap_truncate(hm->hmap_gm, hm->hmap_gm_path);
    if (e) err = e;
    hm->hmap_gm = (addb_gmap*)0;

    e = addb_tiled_backup(hm->hmap_td, 0);
    if (e) err = e;

    e = addb_hmap_close(hm);
    if (e) err = e;

    e = addb_hmap_remove(addb, path);

    return err;
  }

  return 0;
}

int addb_hmap_array_n_bounded(addb_hmap* hm, unsigned long long hash_of_key,
                              char const* key, size_t key_len,
                              addb_hmap_type type,
                              unsigned long long upper_bound,
                              unsigned long long* n_out) {
  addb_hmap_id val;
  int err;

  *n_out = 0;

  err = addb_hmap_value(hm, hash_of_key, key, key_len, type, &val,
                        (addb_hmap_write_address*)0);
  if (err) return err;

  if (ADDB_GMAP_IVAL_IS_SINGLE(val)) {
    *n_out = 1;

    return 0;
  }

  return addb_gmap_array_n_bounded(hm->hmap_gm, val, upper_bound, n_out);
}

int addb_hmap_last(addb_hmap* hm, unsigned long long hash_of_key,
                   char const* key, size_t key_len, addb_hmap_type type,
                   addb_gmap_id* val_out) {
  int err = addb_hmap_value(hm, hash_of_key, key, key_len, type, val_out,
                            (addb_hmap_write_address*)0);
  if (err) return err;

  if (ADDB_GMAP_IVAL_IS_SINGLE(*val_out)) {
    *val_out = ADDB_GMAP_IVAL_SINGLE(*val_out);
    return 0;
  }

  return addb_gmap_array_last(hm->hmap_gm, *val_out, val_out);
}

int addb_hmap_array_n(addb_hmap* hm, unsigned long long hash_of_key,
                      char const* key, size_t key_len, addb_hmap_type type,
                      unsigned long long* n_out) {
  return addb_hmap_array_n_bounded(hm, hash_of_key, key, key_len, type, -1ull,
                                   n_out);
}

int addb_hmap_array_nth(addb_hmap* hm, unsigned long long hash_of_key,
                        char const* key, size_t key_len, addb_hmap_type type,
                        unsigned long long i, addb_gmap_id* id_out) {
  addb_hmap_id val;
  int err = addb_hmap_value(hm, hash_of_key, key, key_len, type, &val,
                            (addb_hmap_write_address*)0);
  if (err) return err;

  if (ADDB_GMAP_IVAL_IS_SINGLE(val)) {
    if (0 == i) {
      *id_out = ADDB_GMAP_IVAL_SINGLE(val);
      return 0;
    } else
      return ADDB_ERR_NO;
  }

  return addb_gmap_array_nth(hm->hmap_gm, val, i, id_out);
}

/** Dump a (chain) of bucket pages
*/

static void addb_hmap_bkp_dump(addb_hmap* hm, addb_hmap_bucket_page* bkp,
                               addb_tiled_reference* the_tile, FILE* f) {
  unsigned long long next_bkp_off =
      ADDB_BKP_NEXT_OFFSET(bkp) * hm->hmap_bucket_page_size;
  unsigned short const nb = ADDB_BKP_N_BUCKETS(bkp);
  size_t ksr = addb_hmap_bkp_key_storage_remaining(bkp);
  int i;
  int err;

  cl_assert(hm->hmap_addb->addb_cl, nb <= ADDB_HMAP_BKP_MAX_N_BUCKETS);

  if (!f) f = stdout;

  fputs("{\n", f);
  fprintf(f, "\tksr=%lu\n", (unsigned long)ksr);
  for (i = 0; i < nb; i++) {
    addb_hmap_bucket* b = &bkp->bkp_buckets[i];
    unsigned short bko = ADDB_HMB_KEY_OFFSET(b);
    unsigned short bkl = ADDB_HMB_KEY_LEN(b);
    addb_hmap_type bt = ADDB_HMB_TYPE(b);
    unsigned long long bv = ADDB_HMB_VALUE(b);
    char* bk = addb_hmap_hmb_key(bkp, b);
    char key_buf[20];
    char const* single = "";

    if (ADDB_GMAP_IVAL_IS_SINGLE(bv)) {
      bv = ADDB_GMAP_IVAL_SINGLE(bv);
      single = "(s)";
    }

    if (bkl <= ADDB_HMAP_BKP_MAX_KEY_LEN) {
      int ki;
      for (ki = 0; ki < sizeof key_buf - 1; ki++) {
        if (ki >= bkl) break;
        if (isprint((int)bk[ki]))
          key_buf[ki] = bk[ki];
        else
          key_buf[ki] = '.';
      }
      key_buf[ki] = 0;
    } else {
      addb_hmap_indirect_key* iky = (addb_hmap_indirect_key*)bk;

      snprintf(key_buf, sizeof key_buf, "iky(%lu)", ADDB_IKY_OFFSET(iky));
    }

    fprintf(f, "\t%d [o=%d kl=%d, t=%d, v=%llu%s] %s\n", i, (int)bko, (int)bkl,
            (int)bt, bv, single, key_buf);
  }
  fprintf(f, "}%s\n", next_bkp_off ? "-->" : "");

  addb_tiled_free(hm->hmap_td, the_tile);

  if (next_bkp_off) {
    err = addb_hmap_bucket_page_from_offset(hm, next_bkp_off, ADDB_MODE_READ,
                                            the_tile, &bkp);
    if (err)
      fprintf(f, "Unable to follow bucket page chain %llu, errno=%d\n",
              next_bkp_off, err);
    else
      addb_hmap_bkp_dump(hm, bkp, the_tile, f);
  }
}

void addb_hmap_slot_dump(addb_hmap* hm, unsigned long long hash_of_key,
                         FILE* f) {
  unsigned long long i = hash_of_key % hm->hmap_n_slots;
  addb_tiled_reference the_tile;
  addb_hmap_bucket_page* bkp;
  unsigned long long bkp_off;
  int err;

  if (!f) f = stdout;

  err = addb_hmap_slot(hm, i, ADDB_MODE_READ, &the_tile, &bkp, &bkp_off);
  if (ADDB_ERR_NO == err)
    fputs("{empty}", f);
  else if (err)
    fprintf(f, "Unable to open slot %llu, errno=%d\n", i, err);

  cl_assert(hm->hmap_addb->addb_cl, bkp);

  addb_hmap_bkp_dump(hm, bkp, &the_tile, f);
}

void addb_hmap_configure(addb_hmap* hm, addb_hmap_configuration* hcf,
                         addb_gmap_configuration* gcf) {
  addb_gmap_configure(hm->hmap_gm, gcf);

  if (!hm) return;

  hm->hmap_cf = *hcf;

  if (hm->hmap_tiled_pool) {
    if (hm->hmap_td) addb_tiled_set_mlock(hm->hmap_td, hm->hmap_cf.hcf_mlock);
  }
}

unsigned long long addb_hmap_horizon(addb_hmap* hm) {
  if (!hm) return 0;

  return hm->hmap_horizon;
}

void addb_hmap_horizon_set(addb_hmap* hm, unsigned long long horizon) {
  addb_gmap_horizon_set(hm->hmap_gm, horizon);
  hm->hmap_horizon = horizon;
}

int addb_hmap_status(addb_hmap* hm, cm_prefix const* prefix,
                     addb_status_callback* callback, void* callback_data) {
  char buf[80];
  int err;
  cm_prefix hmap_pre;

  if (!hm) return EINVAL;

  hmap_pre = cm_prefix_push(prefix, "hmap");

  snprintf(buf, sizeof buf, "%llu", hm->hmap_n_slots);
  err = (*callback)(callback_data, cm_prefix_end(&hmap_pre, "n-slots"), buf);
  if (err) return err;

  if (hm->hmap_td) {
    err = addb_tiled_status(hm->hmap_td, &hmap_pre, callback, callback_data);
    if (err) return err;
  }

  if (hm->hmap_gm) {
    err = addb_gmap_status(hm->hmap_gm, &hmap_pre, callback, callback_data);
    if (err) return err;
  }

  return 0;
}

int addb_hmap_status_tiles(addb_hmap* hm, cm_prefix const* prefix,
                           addb_status_callback* callback,
                           void* callback_data) {
  int err;
  cm_prefix hmap_pre;

  if (!hm) return EINVAL;

  hmap_pre = cm_prefix_push(prefix, "hmap");
  if (hm->hmap_td) {
    err = addb_tiled_status_tiles(hm->hmap_td, &hmap_pre, callback,
                                  callback_data);
    if (err) return err;
  }

  if (hm->hmap_gm) {
    err =
        addb_gmap_status_tiles(hm->hmap_gm, &hmap_pre, callback, callback_data);
    if (err) return err;
  }
  return 0;
}

/* Sparse HMAP API
*
*	When we use HMAPS as a sparse arrays, we use the source id as the hash
*	with the source id bytes forming the key.
*
* @return ADDB_ERR_EXISTS	if the entry already exists.
*/

int addb_hmap_sparse_add(addb_hmap* hm, addb_gmap_id source,
                         addb_hmap_type type, addb_gmap_id id) {
  addb_u5 source_key;

  ADDB_PUT_U5(source_key, source);
  return addb_hmap_add(hm, source, (char const*)&source_key, sizeof source_key,
                       type, id);
}

int addb_hmap_sparse_array_n_bounded(addb_hmap* hm, addb_gmap_id source,
                                     addb_hmap_type type,
                                     unsigned long long upper_bound,
                                     unsigned long long* n_out) {
  addb_u5 source_key;

  ADDB_PUT_U5(source_key, source);
  return addb_hmap_array_n_bounded(hm, source, (char const*)&source_key,
                                   sizeof source_key, type, upper_bound, n_out);
}

int addb_hmap_sparse_array_n(addb_hmap* hm, addb_gmap_id source,
                             addb_hmap_type type, unsigned long long* n_out) {
  return addb_hmap_sparse_array_n_bounded(hm, source, type, -1ull, n_out);
}

int addb_hmap_sparse_last(addb_hmap* hm, addb_gmap_id source,
                          addb_hmap_type type, addb_gmap_id* val_out) {
  addb_u5 source_key;

  ADDB_PUT_U5(source_key, source);
  return addb_hmap_last(hm, source, (char const*)&source_key, sizeof source_key,
                        type, val_out);
}

int addb_hmap_sparse_array_nth(addb_hmap* hm, addb_gmap_id source,
                               addb_hmap_type type, unsigned long long i,
                               addb_gmap_id* id_out) {
  addb_u5 source_key;

  ADDB_PUT_U5(source_key, source);
  return addb_hmap_array_nth(hm, source, (char const*)&source_key,
                             sizeof source_key, type, i, id_out);
}

#if 0
void
addb_hmap_validate( addb_hmap*  hm )
{
	cl_handle*  cl = hm->hmap_addb->addb_cl;
	addb_tiled_reference  the_tile;
	addb_hmap_header*  hmh =
		(addb_hmap_header*)addb_tiled_get(
			hm->hmap_td,
			0,
			0 + sizeof *hmh,
			ADDB_MODE_READ,
			&the_tile );
	unsigned long long  n_slots = ADDB_HMH_N_SLOTS( hmh );
	unsigned long  last_bkp_offset = ADDB_HMH_LAST_BKP_OFFSET( hmh );
	unsigned long long  i;
	int  err;

	addb_tiled_free( hm->hmap_td, &the_tile );

	for (i = 0; i < n_slots; i++)
	{
		addb_hmap_bucket_page*  bkp;
		unsigned long long  bkp_off;
		err =
			addb_hmap_slot(
				hm,
				i,
				ADDB_MODE_READ,
				&the_tile,
				&bkp,
				&bkp_off );
		cl_assert( cl, 0 == err );
		cl_assert( cl, ADDB_BKP_NEXT_OFFSET( bkp ) <= last_bkp_offset );
		cl_assert( cl, ADDB_BKP_N_BUCKETS( bkp ) <= ADDB_HMAP_BKP_MAX_N_BUCKETS );

		addb_tiled_free( hm->hmap_td, &the_tile );
	}
}
#endif

/**
 * @brief Create an accessor based on an HMAP entry.
 *
 * @param hm 		opaque HMAP handle
 * @param hash_of_key	the source of the results
 * @param key		key
 * @param key_len	number of bytes pointed to by key
 * @param type		type of key
 * @param ida		out: assign to this array
 *
 * @return 0 on success, a nonzero error code on error.
 * @return ADDB_ERR_NO if the source doesn't have a mapping
 * @return ADDB_ERR_BITMAP if it's really a bitmap
 */
int addb_hmap_idarray(addb_hmap* hm, unsigned long long hash_of_key,
                      char const* key, size_t key_len, addb_hmap_type type,
                      addb_idarray* ida) {
  int err;
  addb_id val;

  /*  Look the key up in the hmap.  The result is either
   *  a singleton result value (which return as such) or
   *  a pointer to the slot in the hmap's background gmap
   *  where the results are stored.
   */
  err = addb_hmap_value(hm, hash_of_key, key, key_len, type, &val, NULL);
  if (err != 0) return err;

  if (ADDB_GMAP_IVAL_IS_SINGLE(val)) {
    addb_idarray_single(hm->hmap_addb->addb_cl, ida, val);
    return 0;
  }

  /*  Good-bye, HMAP; now that we know where to look,
   *  it's only a GMAP.
   */
  val = ADDB_GMAP_LOW_34(val);
  return addb_gmap_idarray(hm->hmap_gm, val, ida);
}

/**
 * @brief Create an accessor based on an HMAP entry, sparse version
 *
 * @param hm 		opaque HMAP database handle
 * @param source	the source of the results
 * @param type		type of array
 * @param ida		out: assign to this array
 *
 * @return 0 on success, a nonzero error code on error.
 * @return ADDB_ERR_NO if the source doesn't have a mapping
 */
int addb_hmap_sparse_idarray(addb_hmap* hm, addb_gmap_id source,
                             addb_hmap_type type, addb_idarray* ida) {
  addb_u5 source_key;

  ADDB_PUT_U5(source_key, source);
  return addb_hmap_idarray(hm, source, (char const*)&source_key,
                           sizeof source_key, type, ida);
}

/*
 * Inform the hmap that things may have changed on disk.
 */
int addb_hmap_refresh(addb_hmap* hm, unsigned long long n) {
  int err;
  cl_handle* cl = hm->hmap_addb->addb_cl;

  /*
   * stretch the hmap file itsself
   */

  cl_log(cl, CL_LEVEL_VERBOSE, "addb_hmap_refresh: trying to stretch");

  err = addb_tiled_stretch(hm->hmap_td);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_hmap_refresh", err,
                 "Cannot stretch hmap");
    return err;
  }

  /*
   * Now stretch the gmap that backs the hmap.
   * Since we've updated the hmap correctly, the next step is to
   * use that data to tell us how far into the gmap we need to update
   */

  addb_tiled_reference hmh_tile;
  addb_hmap_header* hmh = (addb_hmap_header*)addb_tiled_get(
      hm->hmap_td, 0, 0 + sizeof *hmh, ADDB_MODE_WRITE, &hmh_tile);
  addb_gmap_id next_id = ADDB_HMH_NEXT_ENTRY(hmh);
  err = addb_gmap_refresh(hm->hmap_gm, (unsigned long long)next_id);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "addb_gmap_refresh", err,
                 "Cannot refresh the hmap's gmap");
    return err;
  }

  return 0;
}
