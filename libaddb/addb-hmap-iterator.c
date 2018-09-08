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
#include "libaddb/addb-hmap.h"
#include "libaddb/addbp.h"

#include <errno.h>

void addb_hmap_iterator_initialize(addb_hmap_iterator* iter) {
  memset(iter, 0, sizeof *iter);
  iter->hmit_forward = true;
  addb_gmap_iterator_initialize(&iter->hmit_gmap_iter);
}

int addb_hmap_iterator_finish(addb_hmap_iterator* iter) {
  addb_hmap_iterator_initialize(iter);

  return 0;
}

/**
 * @brief Set up the iterator.
 *
 * @param hm		hmap this all takes place in.
 * @param hash_of_key	look up this hash
 * @param key		of this key
 * @param key_len	number of bytes pointed to by key
 * @param type		another part of the key
 * @param iter		iterator to be set up
 *
 * @return 0 on success, a nonzero error code on error.
 * @return ADDB_ERR_NO if the iterator results are empty.
 */

static int addb_hmap_iterator_start(addb_hmap* hm, addb_hmap_id hash_of_key,
                                    char const* const key, size_t key_len,
                                    addb_hmap_type type,
                                    addb_hmap_iterator* iter) {
  addb_hmap_id v;
  int err = 0;

  cl_assert(hm->hmap_addb->addb_cl, iter);
  cl_assert(hm->hmap_addb->addb_cl, !iter->hmit_hmap);

  err = addb_hmap_read_value(hm, hash_of_key, key, key_len, type, &v);
  if (err) {
    if (err != ADDB_ERR_NO)
      cl_log_errno(hm->hmap_addb->addb_cl, CL_LEVEL_VERBOSE,
                   "addb_hmap_read_value", err, "map=%p, hash_of_key=%llx",
                   (void*)hm, (unsigned long long)hash_of_key);
    return err;
  }

  if (ADDB_GMAP_IVAL_IS_SINGLE(v)) {
    iter->hmit_single = ADDB_GMAP_IVAL_SINGLE(v);
    iter->hmit_unread_singleton = true;
  } else {
    addb_gmap_iterator_initialize(&iter->hmit_gmap_iter);
    addb_gmap_iterator_set_forward(hm->hmap_gm, &iter->hmit_gmap_iter,
                                   iter->hmit_forward);
    iter->hmit_gmap_source = v;
    iter->hmit_see_gmap = 1;
  }
  iter->hmit_hmap = hm;

  return 0;
}

int addb_hmap_iterator_next_loc(addb_hmap* hm, addb_hmap_id hash_of_key,
                                char const* const key, size_t key_len,
                                addb_hmap_type type, addb_hmap_iterator* iter,
                                addb_hmap_id* out, char const* file, int line) {
  int err = 0;

  cl_assert(hm->hmap_addb->addb_cl, iter);

  if (!iter->hmit_hmap) {
    err = addb_hmap_iterator_start(hm, hash_of_key, key, key_len, type, iter);
    if (err) {
      return err;
    }
  }

  if (!iter->hmit_see_gmap) /* single item iterator. */
  {
    if (iter->hmit_unread_singleton) {
      *out = iter->hmit_single;
      iter->hmit_unread_singleton = 0;

      return 0;
    } else
      return ADDB_ERR_NO;
  }

  cl_assert(hm->hmap_addb->addb_cl, iter->hmit_see_gmap);

  return addb_gmap_iterator_next_loc(hm->hmap_gm, iter->hmit_gmap_source,
                                     &iter->hmit_gmap_iter, out, file, line);
}

int addb_hmap_iterator_set_offset(addb_hmap* hm, addb_hmap_id hash_of_key,
                                  char const* const key, size_t key_len,
                                  addb_hmap_type type, addb_hmap_iterator* iter,
                                  unsigned long long i) {
  int err = 0;

  cl_assert(hm->hmap_addb->addb_cl, iter);

  if (!iter->hmit_hmap) {
    if (0 == i) return 0;

    err = addb_hmap_iterator_start(hm, hash_of_key, key, key_len, type, iter);
    if (err) return err;

    cl_assert(hm->hmap_addb->addb_cl, iter->hmit_see_gmap);
  }

  if (!iter->hmit_see_gmap) /* singleton */
  {
    if (i > 1) return ADDB_ERR_NO;

    iter->hmit_unread_singleton = (i == 0);
    return 0;
  }

  cl_assert(hm->hmap_addb->addb_cl, iter->hmit_see_gmap);

  return addb_gmap_iterator_set_offset(hm->hmap_gm, iter->hmit_gmap_source,
                                       &iter->hmit_gmap_iter, i);
}

int addb_hmap_iterator_n(addb_hmap* hm, addb_hmap_id hash_of_key,
                         char const* const key, size_t key_len,
                         addb_hmap_type type, addb_hmap_iterator* iter,
                         unsigned long long* n_out) {
  int err = 0;

  cl_assert(hm->hmap_addb->addb_cl, iter);

  if (!iter->hmit_hmap) {
    err = addb_hmap_iterator_start(hm, hash_of_key, key, key_len, type, iter);
    if (err != 0) {
      if (err != ADDB_ERR_NO) return err;

      *n_out = 0;
      return 0;
    }
  }

  if (!iter->hmit_see_gmap) /* singleton */
  {
    *n_out = iter->hmit_unread_singleton;
    return 0;
  }

  return addb_gmap_iterator_n(hm->hmap_gm, iter->hmit_gmap_source,
                              &iter->hmit_gmap_iter, n_out);
}

int addb_hmap_iterator_find_loc(addb_hmap* hm, addb_hmap_id hash_of_key,
                                char const* const key, size_t key_len,
                                addb_hmap_type type, addb_hmap_iterator* iter,
                                addb_hmap_id* id_in_out, bool* changed_out,
                                char const* file, int line) {
  int err = 0;

  cl_assert(hm->hmap_addb->addb_cl, iter);

  if (!iter->hmit_hmap) {
    err = addb_hmap_iterator_start(hm, hash_of_key, key, key_len, type, iter);
    if (err != 0) return err;
  }

  if (!iter->hmit_see_gmap) /* singleton */
  {
    iter->hmit_unread_singleton = 0;

    if (iter->hmit_forward ? *id_in_out <= iter->hmit_single
                           : *id_in_out >= iter->hmit_single) {
      *changed_out = (*id_in_out != iter->hmit_single);
      if (*changed_out) *id_in_out = iter->hmit_single;
      return 0;
    }
    return ADDB_ERR_NO;
  }

  cl_assert(hm->hmap_addb->addb_cl, iter->hmit_see_gmap);

  return addb_gmap_iterator_find_loc(hm->hmap_gm, iter->hmit_gmap_source,
                                     &iter->hmit_gmap_iter, id_in_out,
                                     changed_out, file, line);
}

void addb_hmap_iterator_unget(addb_hmap* hm, addb_hmap_iterator* iter) {
  cl_assert(hm->hmap_addb->addb_cl, iter);

  if (!iter->hmit_hmap) return;

  if (!iter->hmit_see_gmap) /* singleton */
  {
    iter->hmit_unread_singleton = true;
    return;
  }

  cl_assert(hm->hmap_addb->addb_cl, iter->hmit_see_gmap);

  addb_gmap_iterator_unget(hm->hmap_gm, iter->hmit_gmap_source,
                           &iter->hmit_gmap_iter, 0);
}

void addb_hmap_iterator_get_position(addb_hmap* hm, addb_hmap_iterator* iter,
                                     addb_hmap_iterator_position* pos_out) {
  cl_assert(hm->hmap_addb->addb_cl, iter);
  cl_assert(hm->hmap_addb->addb_cl, pos_out);

  if (!iter->hmit_hmap)
    *pos_out = ADDB_HMAP_POSITION_START;

  else if (!iter->hmit_see_gmap) /* singleton */
    *pos_out = (iter->hmit_unread_singleton ? ADDB_HMAP_POSITION_START
                                            : ADDB_GMAP_POSITION_END);
  else
    addb_gmap_iterator_get_position(hm->hmap_gm, iter->hmit_gmap_source,
                                    &iter->hmit_gmap_iter, pos_out);
}

int addb_hmap_iterator_set_position(addb_hmap* hm, addb_hmap_id hash_of_key,
                                    char const* const key, size_t key_len,
                                    addb_hmap_type type,
                                    addb_hmap_iterator* iter,
                                    addb_hmap_iterator_position const* pos) {
  cl_handle* cl = hm->hmap_addb->addb_cl;
  unsigned long long i;
  int err;

  cl_assert(cl, pos);
  cl_log(cl, CL_LEVEL_SPEW, "addb_hmap_iterator_set_position (pos=%llu)",
         (unsigned long long)*pos);

  if (ADDB_GMAP_POSITION_START == *pos)
    i = 0;
  else if (ADDB_GMAP_POSITION_END == *pos) {
    err = addb_hmap_iterator_n(hm, hash_of_key, key, key_len, type, iter, &i);
    if (err) return err;
    if (i > 0) i--;
  } else
    i = *pos;

  return addb_hmap_iterator_set_offset(hm, hash_of_key, key, key_len, type,
                                       iter, i);
}

char const* addb_hmap_iterator_to_string(addb_hmap* hm,
                                         addb_hmap_id hash_of_key,
                                         char const* const key, size_t key_len,
                                         addb_hmap_type type,
                                         addb_hmap_iterator* iter, char* buf,
                                         size_t size) {
  if (!iter->hmit_hmap)
    snprintf(buf, size, "%s%s %llu[unopened]", iter->hmit_forward ? "" : "~",
             hm->hmap_dir_path, (unsigned long long)hash_of_key);
  else
    snprintf(buf, size, "%s%s.%llu", iter->hmit_forward ? "" : "~",
             hm->hmap_dir_path, (unsigned long long)hash_of_key);

  return buf;
}

/* Sparse HMAP API
*/

int addb_hmap_sparse_iterator_next_loc(addb_hmap* hm, addb_hmap_id source,
                                       addb_hmap_type type,
                                       addb_hmap_iterator* iter,
                                       addb_hmap_id* out, char const* file,
                                       int line) {
  addb_u5 source_key;

  ADDB_PUT_U5(source_key, source);
  return addb_hmap_iterator_next_loc(hm, source, (char const*)&source_key,
                                     sizeof source_key, type, iter, out, file,
                                     line);
}

int addb_hmap_sparse_iterator_set_offset(addb_hmap* hm, addb_hmap_id source,
                                         addb_hmap_type type,
                                         addb_hmap_iterator* iter,
                                         unsigned long long i) {
  addb_u5 source_key;

  ADDB_PUT_U5(source_key, source);
  return addb_hmap_iterator_set_offset(hm, source, (char const*)&source_key,
                                       sizeof source_key, type, iter, i);
}

int addb_hmap_sparse_iterator_n(addb_hmap* hm, addb_hmap_id source,
                                addb_hmap_type type, addb_hmap_iterator* iter,
                                unsigned long long* n_out) {
  addb_u5 source_key;

  ADDB_PUT_U5(source_key, source);
  return addb_hmap_iterator_n(hm, source, (char const*)&source_key,
                              sizeof source_key, type, iter, n_out);
}

int addb_hmap_sparse_iterator_find_loc(addb_hmap* hm, addb_hmap_id source,
                                       addb_hmap_type type,
                                       addb_hmap_iterator* iter,
                                       addb_hmap_id* id_in_out,
                                       bool* changed_out, char const* file,
                                       int line) {
  addb_u5 source_key;

  ADDB_PUT_U5(source_key, source);
  return addb_hmap_iterator_find_loc(hm, source, (char const*)&source_key,
                                     sizeof source_key, type, iter, id_in_out,
                                     changed_out, file, line);
}

int addb_hmap_sparse_iterator_set_position(
    addb_hmap* hm, addb_hmap_id source, addb_hmap_type type,
    addb_hmap_iterator* iter, addb_hmap_iterator_position const* pos) {
  addb_u5 source_key;

  ADDB_PUT_U5(source_key, source);
  return addb_hmap_iterator_set_position(hm, source, (char const*)&source_key,
                                         sizeof source_key, type, iter, pos);
}

/**
 * @brief Set the iterator's direction.
 *
 *   This should be called before any accesses to the iterator.
 *   Effects of changing direction in mid-flow are not defined.
 *
 * @param gm 		opaque GMAP database handle
 * @param iter		an iterator
 * @param forward 	true: go from low to high; false: high to low.
 */
void addb_hmap_iterator_set_forward(addb_hmap* hm, addb_hmap_iterator* iter,
                                    bool forward) {
  cl_assert(hm->hmap_addb->addb_cl, iter != NULL);
  iter->hmit_forward = forward;
}
