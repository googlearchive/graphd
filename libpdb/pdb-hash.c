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
#include "libpdb/pdbp.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>


char const* pdb_hash_type_to_string(int t) {
  switch (t) {
    case addb_hmt_name:
      return "name";
    case addb_hmt_value:
      return "value";
    case addb_hmt_word:
      return "word";
    case addb_hmt_bin:
      return "bin";
    case addb_hmt_reserved2:
      return "reserved2";
    case addb_hmt_reserved3:
      return "reserved3";
    case addb_hmt_reserved4:
      return "reserved4";
    case addb_hmt_typeguid:
      return "typeguid";
    case addb_hmt_scope:
      return "scope";
    case addb_hmt_vip:
      return "vip";
    case addb_hmt_key:
      return "key";
    case addb_hmt_gen:
      return "gen";
    case addb_hmt_reserved5:
      return "reserved5";
    default:
      break;
  }
  return "unexpected hash type";
}

static unsigned long long pdb_case_insensitive_hash(char const* key,
                                                    size_t key_len) {
  char const* e = key + key_len;
  unsigned long long h = 0;

  while (key < e) {
    unsigned char c = *key;

    if (isascii(c)) c = tolower(c);

    h *= 33;
    h += c;
    key++;
  }

  return h & ((1ull << 34) - 1);
}

int pdb_hash_add(pdb_handle* pdb, addb_hmap_type t, char const* key,
                 size_t key_len, pdb_id id) {
  cl_assert(pdb->pdb_cl, key);
  cl_assert(pdb->pdb_cl, ADDB_HMAP_TYPE_VALID(t));

  pdb->pdb_runtime_statistics.rts_index_elements_written++;
  return addb_hmap_add(pdb->pdb_hmap, pdb_case_insensitive_hash(key, key_len),
                       key, key_len, t, id);
}

/**
 * @brief How many entries are there for this value and type?
 *  Return the number of entries hashed with a given value and type.
 *
 * @param pdb Opaque database pointer, creatd with pdb_create()
 * @param type One of #PDB_HASH_TYPE, #PDB_HASH_VALUE, or #PDB_HASH_TYPE.
 * @param s beginning of the token that should match type, value, or name
 * @param e pointer just past the end of the token.
 * @param low  end of the local ID range, first ID included
 * @param high end of the local ID range, first ID not included
 * @param upper_bound if it's more than this many, we don't care to be exact.
 *
 * @return the number of entries that an iterator for this
 * 	set would return, or 0 if no such set exists.
 */

int pdb_hash_count(pdb_handle* pdb, addb_hmap_type t, char const* key,
                   size_t key_len, pdb_id low, pdb_id high,
                   unsigned long long upper_bound, unsigned long long* n_out) {
  return pdb_count_hmap(pdb, pdb->pdb_hmap,
                        pdb_case_insensitive_hash(key, key_len), key, key_len,
                        t, low, high, upper_bound, n_out);
}

/*
 *
 * Get the HASH iterator for the number n.
 */
int pdb_hash_number_iterator(pdb_handle* pdb, const graph_number* n, pdb_id low,
                             pdb_id high, bool forward, pdb_iterator** it_out) {
  char* key;
  unsigned long h;
  int err;

  key = pdb_number_to_string(pdb->pdb_cm, n);

  if (!key) return ENOMEM;

  h = pdb_case_insensitive_hash(key, strlen(key));

  cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
         "pdb_hash_number_iterator: value hash for %s (at %lu)", key, h);

  err = pdb_iterator_hmap_create(pdb, pdb->pdb_hmap, h, key, strlen(key),
                                 addb_hmt_value, low, high, forward, false,
                                 it_out);

  cm_free(pdb->pdb_cm, key);
  return err;
}

int pdb_hash_iterator(pdb_handle* pdb, addb_hmap_type t, char const* key,
                      size_t key_len, pdb_id low, pdb_id high, bool forward,
                      pdb_iterator** it_out) {
  unsigned long long h = 0;
  int err;

  if (t == addb_hmt_value) {
    const char* norm_s;
    const char* norm_e;
    char* buf;
    err = pdb_hmap_value_normalize(pdb, key, key + key_len, &norm_s, &norm_e,
                                   &buf);

    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_word_normalize", err,
                   "Can't normalize string %.*s for hmap"
                   " lookup",
                   (int)key_len, key);
      return err;
    }
    h = pdb_case_insensitive_hash(norm_s, norm_e - norm_s);

    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_hash_iterator: %s key=\"%.*s\"[%ld] -->\"%.*s\""
           " hash=%llx (%llu)",
           pdb_hash_type_to_string(t), (int)key_len, key, (long)key_len,
           (int)(norm_e - norm_s), norm_s, h, h);

    err = pdb_iterator_hmap_create(pdb, pdb->pdb_hmap, h, norm_s,
                                   norm_e - norm_s, t, low, high, forward,
                                   /* error-if-null */ false, it_out);
    cm_free(pdb->pdb_cm, buf);
    return err;

  } else {
    h = pdb_case_insensitive_hash(key, key_len);

    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
           "pdb_hash_iterator: %s key=\"%.*s\"[%ld] hash=%llx (%llu)",
           pdb_hash_type_to_string(t), (int)key_len, key, (long)key_len, h, h);
    return pdb_iterator_hmap_create(pdb, pdb->pdb_hmap, h, key, key_len, t, low,
                                    high, forward, /* error-if-null */ false,
                                    it_out);
  }
}

/**
 * @brief Synchronize against existing, but unrecorded, records.
 *
 *  The record <pr> is known to the system with local id <id>.
 *  It may or may not be in the hash tables.
 *
 * @param pdb   opaque pdb module handle
 * @param id    local ID of the passed-in record
 * @param pr    passed-in record
 */
int pdb_hash_synchronize(pdb_handle* pdb, pdb_id id, pdb_primitive const* pr) {
  char const* s;
  size_t sz;
  int err;

  cl_assert(pdb->pdb_cl, pdb->pdb_hmap);
  sz = pdb_primitive_name_get_size(pr);
  if (sz > 0) {
    s = pdb_primitive_name_get_memory(pr);
    err = pdb_hash_add(pdb, addb_hmt_name, s, sz - 1, id);
    if (err) {
      int len = (int)sz - 1 > 80 ? 80 : (int)sz - 1;

      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_hash_add", err,
                   "error hashing name \"%.*s\"", len, s);

      return err;
    }
  }

  sz = pdb_primitive_value_get_size(pr);
  if (sz > 0) {
    const char *norm_s, *norm_e;
    char* norm_buf;

    s = pdb_primitive_value_get_memory(pr);
    err = pdb_hmap_value_normalize(pdb, s, s + (sz - 1), &norm_s, &norm_e,
                                   &norm_buf);

    if (err != 0) {
      int len = (int)sz - 1 > 80 ? 80 : (int)sz - 1;
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_word_normalize", err,
                   "error normalizing value \"%.*s\"", len, s);

      return err;
    }

    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
           "pdb_hash_synchronize: normalized '%.*s' to '%.*s'", (int)(sz), s,
           (int)(norm_e - norm_s), norm_s);

    err = pdb_hash_add(pdb, addb_hmt_value, norm_s, norm_e - norm_s, id);

    if (err) {
      int len = (int)sz - 1 > 80 ? 80 : (int)sz - 1;

      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_hash_add", err,
                   "error hashing value \"%.*s\"", len, s);
      if (norm_buf != NULL) cm_free(pdb->pdb_cm, norm_buf);
      return err;
    }

    if (norm_buf != NULL) cm_free(pdb->pdb_cm, norm_buf);

    err = pdb_word_synchronize(pdb, id, pr);
    if (err != 0) {
      int len = (int)sz - 1 > 80 ? 80 : (int)sz - 1;

      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_word_synchronize", err,
                   "error adding words in \"%.*s\"", len, s);

      return err;
    }
  }

  return 0;
}
