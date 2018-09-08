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
/*
*	addb-hmap-file.h
*
*	HMAP internal (file) structure definitions
*
*	An HMAP is an array of "bucket pages" designed for
*	access with via a hashed key.  The first page is the
*	file header.  The next N pages are the hash table and
*	pages N+1 and greater are used for additional bucket
*	pages and segmented storage for large keys.
*/

#ifndef ADDB_HMAP_FILE_H
#define ADDB_HMAP_FILE_H

#include "libaddb/addb-scalar.h"

#define ADDB_HMAP_MAGIC "ah2p" /* Addb HMAP Partition, version 2 */
#define ADDB_HMAP_BUCKET_PAGE_SIZE (1024 * 4)

/** The offset (in pages) of a page in the HMAP file
*/

typedef addb_u4 addb_hmap_file_offset;

/** The offset (in bytes) from the start of a bucket page to a location in that
 * page
*/

typedef addb_u2 addb_hmap_bucket_offset;

/** A bucket holds all values having the same key
*
*	The values themselves (an array of local ids) are stored
*	in the underlying GMAP at hmb_value.
*
*	Using a base-relative offset for key storage allows buckets
*	to be sorted in the bucket page array should we want to do that.
*/

typedef struct addb_hmap_bucket {
  addb_hmap_bucket_offset hmb_key_offset; /* offset to the start of the key */
  addb_u2 hmb_key_len;                    /* length of key */
  addb_u1 hmb_type;                       /* type component of the key */
  addb_u5 hmb_value;                      /* index into GMAP */
} addb_hmap_bucket;

#define ADDB_HMB_KEY_OFFSET(B__) ADDB_GET_U2((B__)->hmb_key_offset)
#define ADDB_HMB_KEY_OFFSET_SET(B__, V__) \
  ADDB_PUT_U2((B__)->hmb_key_offset, (V__))

#define ADDB_HMB_KEY_LEN(B__) ADDB_GET_U2((B__)->hmb_key_len)
#define ADDB_HMB_KEY_LEN_SET(B__, V__) ADDB_PUT_U2((B__)->hmb_key_len, (V__))

#define ADDB_HMB_TYPE(B__) (B__)->hmb_type
#define ADDB_HMB_TYPE_SET(B__, V__) (B__)->hmb_type = (V__)

#define ADDB_HMB_VALUE(B__) ADDB_GET_U5((B__)->hmb_value)
#define ADDB_HMB_VALUE_SET(B__, V__) ADDB_PUT_U5((B__)->hmb_value, (V__))

/** An indirect key holds the offset of the indirect key page
*/

typedef struct addb_hmap_indirect_key {
  addb_hmap_file_offset iky_offset; /* offset of first key page */
} addb_hmap_indirect_key;

#define ADDB_IKY_OFFSET(I__) ADDB_GET_U4((I__)->iky_offset)
#define ADDB_IKY_OFFSET_SET(I__, V__) ADDB_PUT_U4((I__)->iky_offset, (V__))

/** An indirect key page
*
*	Indirect (long) keys are stored as a segmented array
*/

typedef struct addb_hmap_iky_page {
  addb_hmap_file_offset ikp_next;
  addb_u2 ikp_length;
  addb_u1 ikp_key[1];
} addb_hmap_iky_page;

#define ADDB_IKP_NEXT(I__) ADDB_GET_U4((I__)->ikp_next)
#define ADDB_IKP_NEXT_SET(I__, V__) ADDB_PUT_U4((I__)->ikp_next, (V__))

#define ADDB_IKP_LENGTH(I__) ADDB_GET_U2((I__)->ikp_length)
#define ADDB_IKP_LENGTH_SET(I__, V__) ADDB_PUT_U2((I__)->ikp_length, (V__))

/** The maximum length of a key segment in an indirect key page
*/

#define ADDB_IKP_MAX_LENGTH \
  (ADDB_HMAP_BUCKET_PAGE_SIZE - (sizeof(addb_hmap_iky_page) + sizeof(addb_u1)))

/** A bucket page holds an array of buckets and their associated keys.
*
*	Bucket pages serve as "slots" in an HMAP hash table
*
*	Buckets are allocated from the front of the page and keys
*	are allocated from the end.
*/

typedef struct addb_hmap_bucket_page {
  addb_hmap_file_offset
      bkp_next_offset; /* offset of next bucket page in *bucket pages* */
  addb_hmap_bucket_offset
      bkp_free_offset;   /* offset of first free byte of key storage */
  addb_u2 bkp_n_buckets; /* number of buckets */
  addb_hmap_bucket bkp_buckets[1];
} addb_hmap_bucket_page;

#define ADDB_BKP_NEXT_OFFSET(B__) ADDB_GET_U4((B__)->bkp_next_offset)
#define ADDB_BKP_NEXT_OFFSET_SET(B__, V__) \
  ADDB_PUT_U4((B__)->bkp_next_offset, (V__))

#define ADDB_BKP_FREE_OFFSET(B__) ADDB_GET_U2((B__)->bkp_free_offset)
#define ADDB_BKP_FREE_OFFSET_SET(B__, V__) \
  ADDB_PUT_U2((B__)->bkp_free_offset, (V__))

#define ADDB_BKP_N_BUCKETS(B__) ADDB_GET_U2((B__)->bkp_n_buckets)
#define ADDB_BKP_N_BUCKETS_SET(B__, V__) \
  ADDB_PUT_U2((B__)->bkp_n_buckets, (V__))

/** The largest possible key that could be stored in a bucket page.
*/
#define ADDB_HMAP_BKP_MAX_KEY_LEN \
  (ADDB_HMAP_BUCKET_PAGE_SIZE - sizeof(addb_hmap_bucket_page))

/** The maximum number of buckets that could be stored in a bucket page
*/

#define ADDB_HMAP_BKP_MAX_N_BUCKETS \
  (ADDB_HMAP_BUCKET_PAGE_SIZE / (sizeof(addb_hmap_bucket) + 1))

/** Is a bucket page initialized?
*
*	Bucket pages must be zero-filled when allocated.  Usually the OS
*	guarantees this.  A non-zero free_offset indicates that the bucket
*	page has been initialized.
*/

#define addb_hmap_bkp_initialized(B__) \
  ((B__)->bkp_free_offset[0] || (B__)->bkp_free_offset[1])

/** Compute the amount of key storage remaining in a bucket page.
*
*	Since key storage is allocated from the end of the page, the
*	free offset + 1 less the storage used for buckets (remember the one
*	we're adding!) gives the amount of storage remaining.
*/

#define ADDB_HMAP_KEY_STORAGE_REMAINING(B__)                         \
  (((char*)(B__) + (ADDB_BKP_FREE_OFFSET((B__)) + 1) >               \
    (char*)&(B__)->bkp_buckets[ADDB_BKP_N_BUCKETS((B__)) + 1])       \
       ? ((char*)(B__) + (ADDB_BKP_FREE_OFFSET((B__)) + 1) -         \
          (char*)&(B__)->bkp_buckets[ADDB_BKP_N_BUCKETS((B__)) + 1]) \
       : 0)

/** Get a pointer to the key bytes given a bucket page and a bucket
*/

#define ADDB_HMAP_HMB_KEY(P__, B__) ((char*)(P__) + ADDB_HMB_KEY_OFFSET((B__)))

/** The HMAP header
*
*	The hmap header occupies (er, mostly wastes) the first "bucket page" of
*	an HMAP file.  Following the header is the array of slots (bucket pages)
*	indexed by hash.  Following the hash table are bucket pages used
*	to handle overflow or for storing long keys.
*/

typedef struct addb_hmap_header {
  addb_u4 hmh_magic;
  addb_u5 hmh_next_entry; /* next free entry in associated gmap */
  addb_u4 hmh_bkp_size;   /* size of a bucket page */
  addb_u8 hmh_n_slots;    /* number of slots (bucket pages) in the hash table */
  addb_hmap_file_offset hmh_last_bkp_offset; /* offset of last page in file */
} addb_hmap_header;

#define ADDB_HMH_NEXT_ENTRY(B__) ADDB_GET_U5((B__)->hmh_next_entry)
#define ADDB_HMH_NEXT_ENTRY_SET(B__, V__) \
  ADDB_PUT_U5((B__)->hmh_next_entry, (V__))

#define ADDB_HMH_BKP_SIZE(B__) ADDB_GET_U4((B__)->hmh_bkp_size)
#define ADDB_HMH_BKP_SIZE_SET(B__, V__) ADDB_PUT_U4((B__)->hmh_bkp_size, (V__))

#define ADDB_HMH_N_SLOTS(B__) ADDB_GET_U8((B__)->hmh_n_slots)
#define ADDB_HMH_N_SLOTS_SET(B__, V__) ADDB_PUT_U8((B__)->hmh_n_slots, (V__))

#define ADDB_HMH_LAST_BKP_OFFSET(B__) ADDB_GET_U4((B__)->hmh_last_bkp_offset)
#define ADDB_HMH_LAST_BKP_OFFSET_SET(B__, V__) \
  ADDB_PUT_U4((B__)->hmh_last_bkp_offset, (V__))

#define ADDB_HMAP_HEADER_SIZE ADDB_HMAP_BUCKET_PAGE_SIZE

#endif
