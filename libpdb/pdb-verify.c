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
#include "libpdb/pdb.h"
#include "libpdb/pdbp.h"

#include <errno.h>
#include <stdio.h>

#include "libcl/cl.h"

/*
 *
 * 	X can't read the primitive or corruption in the primitive
 * 	L entry in left gmap missing
 * 	R entry in right gmap missing
 * 	T entry in typeguid gmap missing
 * 	S entry in scope gmap missing
 * 	N entry in name hash missing
 * 	P entry in prefix tree missing
 * 	V entry in value hash missing
 * 	W entry in word hash missing
 * 	G generation missing
 * 	Il left VIP missing
 * 	Ir right VIP missing
 * 	D  bit in bmap/versioned wrong
 * 	B bins wrong
 *
 */

/*
 * Convert a the bitmap from pdb_verify_id into a string.
 */
int pdb_verify_render_error(char *output, size_t len, unsigned long error) {
  const char *error_table[] = {"T",  "L",  "R", "S", "X", "N", "V", "P",
                               "Il", "Ir", "W", "G", "D", "B", NULL};

  int i;

  /* Need one byte for \0 at the end */
  len--;

  for (i = 0; error_table[i]; i++) {
    if (error & (1ul << i)) {
      if (len < strlen(error_table[i])) {
        return ENOSPC;
      }
      strcpy(output, error_table[i]);

      output += strlen(error_table[i]);
      len -= strlen(error_table[i]);
    }
  }

  return 0;
}

/*
 * Verify that the primitive, id, is in the correct VIP table for
 * linkage.
 */
static int pdb_verify_vip(pdb_handle *pdb, pdb_id id, pdb_primitive *pr,
                          int linkage, pdb_budget *budget, const char *name) {
  int err;
  bool isvip;
  graph_guid source, type;
  pdb_iterator *it_v;

  if (!pdb_primitive_has_linkage(pr, linkage)) {
    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW, "%llx does not have a %s. no VIP check",
           (unsigned long long)id, name);

    return 0;
  }

  pdb_primitive_linkage_get(pr, linkage, source);

  err = pdb_vip_id(pdb, GRAPH_GUID_SERIAL(source), linkage, &isvip);

  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_INFO, "pdb_vip_id", err,
                 "Can't even tell if %llx is supposed to have a VIP"
                 " entry",
                 (unsigned long long)id);
    return err;
  }

  if (!isvip) return 0;

  /*
   * You need to have a typeguid to ride this ride.
   */
  if (pdb_primitive_has_linkage(pr, PDB_LINKAGE_TYPEGUID))
    pdb_primitive_linkage_get(pr, PDB_LINKAGE_TYPEGUID, type);
  else
    return 0;

  err = pdb_vip_id_iterator(pdb, GRAPH_GUID_SERIAL(source), linkage, &type, 0,
                            PDB_ITERATOR_HIGH_ANY, true,
                            /* error-if-null */ false, &it_v);

  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_INFO, "pdb_vip_id_iterator", err,
                 "Can't get the VIP iterator that %llx should be in",
                 (unsigned long long)id);
    return err;
  }

  err = pdb_iterator_check(pdb, it_v, id, budget);

  pdb_iterator_destroy(pdb, &it_v);

  if (err == 0) {
    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW, "Found id %llx in vip %llx:%s:%llx",
           (unsigned long long)id, (unsigned long long)GRAPH_GUID_SERIAL(type),
           name, (unsigned long long)GRAPH_GUID_SERIAL(source));
    return 0;
  } else {
    cl_log(pdb->pdb_cl, CL_LEVEL_INFO,
           "Did not find id %llx in vip %llx:%s:%llx", (unsigned long long)id,
           (unsigned long long)GRAPH_GUID_SERIAL(type), name,
           (unsigned long long)GRAPH_GUID_SERIAL(source));

    return PDB_ERR_NO;
  }
}

/*
 * Check that id is in the word hash for  [start..end) and that
 * [start..end) is in the prefix tree.
 */
static int pdb_verify_word_callback(void *data, pdb_handle *pdb, pdb_id id,
                                    const char *start, const char *end) {
  int err;
  unsigned long *error_code = (unsigned long *)data;
  pdb_iterator *it_w;
  unsigned long word_hash;
  unsigned long m;
  bool bit;
  bool fail = false;

  /*
   * Step 1: Make sure the word is in the word hash.
   */
  err = pdb_iterator_word_create(pdb, start, end, 0, PDB_ITERATOR_HIGH_ANY,
                                 true, false, &it_w);

  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_INFO, "pdb_iterator_word_create", err,
                 "Cannot create a pdb word iterator for %.*s",
                 (int)(end - start > 5 ? 5 : end - start), start);

    *error_code |= PDB_VERIFY_WORD;
    fail = true;
  } else {
    err = pdb_iterator_check_nonstep(pdb, it_w, id);
    pdb_iterator_destroy(pdb, &it_w);

    if (err == 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
             "Found id %llx in word hmap for '%.*s'", (unsigned long long)id,
             (int)(end - start > 5 ? 5 : end - start), start);
    } else {
      graph_guid g;
      char gbuf[GRAPH_GUID_SIZE];

      graph_guid_from_db_serial(&g, pdb->pdb_database_id, id);
      cl_log(pdb->pdb_cl, CL_LEVEL_INFO,
             "%s: did not find id %llx in word hmap for '%.*s'",
             graph_guid_to_string(&g, gbuf, sizeof gbuf),
             (unsigned long long)id, (int)(end - start > 5 ? 5 : end - start),
             start);

      *error_code |= PDB_VERIFY_WORD;
      fail = true;
    }
  }

  /*
   * Step 2: make sure that each prefix of the word is in the
   * prefix tree.
   */

  word_hash = pdb_word_hash(pdb, start, end);

  /*
   * Check the entire word (or first 5 characters)
   */
  err = addb_bmap_check(pdb->pdb_prefix, word_hash, &bit);

  if (err) {
    *error_code |= PDB_VERIFY_PREFIX;
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_INFO, "addb_bmap_check", err,
                 "Could not check the prefix bitmap at position %lx",
                 word_hash);
    return err;
  } else {
    if (bit) {
      cl_log(pdb->pdb_cl, CL_LEVEL_SPEW, "Found id %llx for prefix %lx",
             (unsigned long long)id, word_hash);
    } else {
      cl_log(pdb->pdb_cl, CL_LEVEL_INFO, "Did not find id %llx for prefix %lx",
             (unsigned long long)id, word_hash);
      *error_code |= PDB_VERIFY_PREFIX;
      return PDB_ERR_DATABASE;
    }
  }

  /*
   * Check each prefix.
   */
  for (m = 0x1F; m < (1 << (5 * 4)); m |= (m << 5)) {
    unsigned long wh = word_hash | m;
    cl_assert(pdb->pdb_cl, (word_hash & m) != wh);

    err = addb_bmap_check(pdb->pdb_prefix, wh, &bit);

    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_INFO, "addb_bmap_check", err,
                   "bitmap check failed at %lx", wh);
      *error_code |= PDB_VERIFY_PREFIX;
    } else {
      if (bit) {
        cl_log(pdb->pdb_cl, CL_LEVEL_SPEW, "Found id %llx for prefix %lx",
               (unsigned long long)id, wh);
      } else {
        cl_log(pdb->pdb_cl, CL_LEVEL_INFO,
               "Did not find id %llx for prefix %lx", (unsigned long long)id,
               wh);
        *error_code |= PDB_VERIFY_PREFIX;
        return PDB_ERR_DATABASE;
      }
    }
  }
  return fail ? PDB_ERR_DATABASE : 0;
}

/*
 * Verify that a data field (either name or value) is present
 * in the right HMAP.
 */
static int pdb_verify_data(pdb_handle *pdb, size_t size, char *data,
                           addb_hmap_type map, const char *name, pdb_id id,
                           pdb_budget *budget) {
  int err;
  pdb_iterator *it_h;

  /* zero size? We don't have any data here. Success */

  if (!size) return 0;

  size--;

  err = pdb_hash_iterator(pdb, map, data, size, 0, PDB_ITERATOR_HIGH_ANY, true,
                          &it_h);
  if (err) return err;

  err = pdb_iterator_check(pdb, it_h, id, budget);

  pdb_iterator_destroy(pdb, &it_h);
  if (err == 0) {
    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW, "Found id %llx in %s hmap for '%.*s'",
           (unsigned long long)id, name, (int)size, data);
    return 0;
  } else {
    cl_log(pdb->pdb_cl, CL_LEVEL_INFO,
           "Did not find id %llx in %s hmap for '%.*s'", (unsigned long long)id,
           name, (int)size, data);
    return PDB_ERR_NO;
  }
}
static int pdb_verify_bin(pdb_handle *pdb, pdb_id id, pdb_primitive *pr) {
  graph_number num;
  pdb_iterator *it;
  int err;
  pdb_budget b = 10000;
  int bin;
  bool exact;
  const char *s, *e;
  if (pdb_primitive_value_get_size(pr) == 0) return 0;

  s = pdb_primitive_value_get_memory(pr);
  e = s + pdb_primitive_value_get_size(pr) - 1;

  bin = pdb_bin_lookup(pdb, PDB_BINSET_STRINGS, s, e, NULL);

  err = pdb_bin_to_iterator(pdb, bin, id, id + 1, true, true, &it);

  if (err) {
    cl_log(pdb->pdb_cl, CL_LEVEL_INFO,
           "VERIFY %llx: bin for %s does not exist: %s", (unsigned long long)id,
           pdb_primitive_value_get_memory(pr), pdb_xstrerror(err));

    return err;
  }

  err = pdb_iterator_check(pdb, it, id, &b);

  pdb_iterator_destroy(pdb, &it);

  if (err) {
    cl_log(pdb->pdb_cl, CL_LEVEL_INFO,
           "VERIFY %llx: primitive is not in bin for %s: %s",
           (unsigned long long)id, pdb_primitive_value_get_memory(pr),
           pdb_xstrerror(err));
    return err;
  }

  err = graph_decode_number(
      pdb_primitive_value_get_memory(pr),
      pdb_primitive_value_get_memory(pr) + pdb_primitive_value_get_size(pr) - 1,
      &num, true);

  /*
   * Not a number? Not a problem.
   */
  if (err) return 0;

  bin = pdb_bin_lookup(pdb, PDB_BINSET_NUMBERS, &num, &num + 1, &exact);

  if (!exact) {
    err = pdb_bin_to_iterator(pdb, bin, id, id + 1, true, true, &it);

    if (err) {
      cl_log(pdb->pdb_cl, CL_LEVEL_INFO,
             "VERIFY: %llx: bin for '%s' (number) does not exist: %s",
             (unsigned long long)id, pdb_primitive_value_get_memory(pr),
             pdb_xstrerror(err));
      return err;
    }

    b = 1000000;
    err = pdb_iterator_check(pdb, it, id, &b);

    pdb_iterator_destroy(pdb, &it);

    if (err) {
      cl_log(pdb->pdb_cl, CL_LEVEL_INFO,
             "VERIFY: %llx: primitive is not in (number) bin for %s: %s",
             (unsigned long long)id, pdb_primitive_value_get_memory(pr),
             pdb_xstrerror(err));
      return err;
    }
  }

  return 0;
}

static int pdb_verify_versioned(pdb_handle *pdb, graph_guid *g,
                                pdb_primitive *pr) {
  pdb_id id;
  unsigned long long n_out;
  pdb_id lineage;
  int err;

  id = GRAPH_GUID_SERIAL(*g);

  bool b;
  err = pdb_is_versioned(pdb, id, &b);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_INFO, "pdb_versioned_check", err,
                 "Can't check liveness for %llx", (unsigned long long)id);
    return PDB_ERR_NO;
  }
  if (pdb_primitive_has_generation(pr))
    lineage = pdb_primitive_lineage_get(pr);
  else
    lineage = id;

  if (b) {
    err =
        addb_hmap_sparse_array_n(pdb->pdb_hmap, lineage, addb_hmt_gen, &n_out);

    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_INFO, "addb_hmap_sparse_array_n", err,
                   "Couldn't find generation for %llx which has "
                   "its versioned bit set",
                   (unsigned long long)id);

      return PDB_ERR_NO;
    }
    return 0;
  } else {
    err = addb_hmap_sparse_array_n(pdb->pdb_hmap, id, addb_hmt_gen, &n_out);

    if (err == ADDB_ERR_NO)
      return 0;
    else if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_INFO, "addb_hmap_sparse_array_n", err,
                   "unexpected error");
      return PDB_ERR_NO;
    }

    if (n_out != pdb_primitive_generation_get(pr)) {
      cl_log(pdb->pdb_cl, CL_LEVEL_INFO,
             "primitive %llx is marked as live but "
             "is not the most recent generation: "
             "n: %llu g:%llu",
             (unsigned long long)id, (unsigned long long)n_out,
             (unsigned long long)pdb_primitive_generation_get(pr));
      return PDB_ERR_NO;
    }

    return 0;
  }
}

/*
 * Verify that pr (with guid g) is in the right place in its
 * generation table.
 */
static int pdb_verify_generation(pdb_handle *pdb, graph_guid *g,
                                 pdb_primitive *pr) {
  int err;
  pdb_id idout;
  pdb_id id;
  pdb_id gen;
  pdb_id lineage;

  /*
   * Never versioned. Done.
   */
  if (!pdb_primitive_has_generation(pr)) {
    return 0;
  }

  id = GRAPH_GUID_SERIAL(*g);

  err = pdb_generation_guid_to_lineage(pdb, g, &lineage, &gen);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_INFO, "pdb_generation_guid_diarray", err,
                 "Can't get generation idarray for %llx",
                 (unsigned long long)id);

    return err;
  }

  /*
   * If we look at the addb_htm_gen hmap for lineage,
   * we aught to find id at the genth position.
   */

  err = addb_hmap_sparse_array_nth(pdb->pdb_hmap, lineage, addb_hmt_gen, gen,
                                   &idout);

  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_INFO, "addb_hmap_sparse_array_nth", err,
                 "Can't search lineage idarray %llu for %llx",
                 (unsigned long long)lineage, (unsigned long long)id);

    return err;
  }

  if (id == idout) {
    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW, "Found %llx in lineage %llu",
           (unsigned long long)id, (unsigned long long)lineage);
    return 0;
  }
  cl_log(pdb->pdb_cl, CL_LEVEL_INFO, "Cannot find %llx in lineage %llu",
         (unsigned long long)id, (unsigned long long)lineage);

  return PDB_ERR_NO;
}

int pdb_verify_id(pdb_handle *pdb, pdb_id id, unsigned long *error_code) {
  int err;
  int i;
  bool fail = false;
  pdb_primitive pr;
  graph_guid g;

  /*
   * Give ourselves a huge budget to pass to iterators.
   * If we actually run out of budget and get PDB_ERR_MORE
   * back, something must be seriously wrong.
   */
  pdb_budget budget = 10000000;
  *error_code = 0;

  if (pdb->pdb_primitive == NULL) {
    if ((err = pdb_initialize(pdb)) != 0) return err;
    err = pdb_initialize_checkpoint(pdb);
    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_initialize_checkpoint",
                   err, "Unable to re-initialize checkpointing");
      return err;
    }
  }

  graph_guid_from_db_serial(&g, pdb->pdb_database_id, id);

  err = pdb_primitive_read(pdb, &g, &pr);
  if (err) {
    *error_code |= PDB_VERIFY_PRIMITIVE;
    return PDB_ERR_DATABASE;
  }

  /*
   * Step 1: verify linkage tables
   * If anything goes wrong, we mark the linkage as broken
   * and continue on our marry way.
   */

  for (i = 0; i < PDB_LINKAGE_N; i++) {
    addb_gmap *gmap_l;
    pdb_iterator *it_l;
    graph_guid guid_l;
    pdb_id id_l;

    if (!pdb_primitive_has_linkage(&pr, i)) continue;

    gmap_l = pdb_linkage_to_gmap(pdb, i);

    pdb_primitive_linkage_get(&pr, i, guid_l);

    err = pdb_id_from_guid(pdb, &id_l, &guid_l);

    if (err) {
      /* the linkage guid doesn't point to anything that
       * could possibly be in the database. The primitive
       * must be corrupt.
       */
      *error_code |= PDB_VERIFY_PRIMITIVE;
      fail = true;
      continue;
    }

    err = pdb_iterator_gmap_create(pdb, gmap_l, i, id_l, id, id + 1, true,
                                   /* error if null? */ false, &it_l);
    if (err) {
      /* Table does exist at all? */
      *error_code |= 1ul << i;
      fail = true;
      continue;
    }

    /*  We're using a custom bypass here to _not_
     *  just read the primitive and look there!
     */
    err = pdb_iterator_gmap_verify_check(pdb, it_l, id, &budget);

    if (err == 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
             "Found id %llx in %s gmap for id %llx.", (unsigned long long)id,
             pdb_linkage_to_string(i), (unsigned long long)id_l);
    } else {
      /* The primitive, id, isn't in the table
       * for guid_l.
       */
      cl_log(pdb->pdb_cl, CL_LEVEL_INFO,
             "Did not find %llx in %s gmap for id %llx.",
             (unsigned long long)id, pdb_linkage_to_string(i),
             (unsigned long long)id_l);
      *error_code |= 1ull << i;
      fail = true;
    }
    pdb_iterator_destroy(pdb, &it_l);
  }

  /*
   * verify name
   */

  err = pdb_verify_data(pdb, pdb_primitive_name_get_size(&pr),
                        pdb_primitive_name_get_memory(&pr), addb_hmt_name,
                        "name", id, &budget);
  if (err) {
    *error_code |= PDB_VERIFY_NAME;
    fail = true;
  }

  /*
   * verify value
   */

  err = pdb_verify_data(pdb, pdb_primitive_value_get_size(&pr),
                        pdb_primitive_value_get_memory(&pr), addb_hmt_value,
                        "value", id, &budget);
  if (err) {
    *error_code |= PDB_VERIFY_VALUE;
    fail = true;
  }

  err = pdb_verify_bin(pdb, id, &pr);
  if (err) {
    *error_code |= PDB_VERIFY_BIN;
    fail = true;
  }

  /*
   * Verify the word hash and prefix tree
   */
  if (pdb_primitive_value_get_size(&pr)) {
    if (pdb_word_chop(error_code, pdb, id, pdb_primitive_value_get_memory(&pr),
                      pdb_primitive_value_get_memory(&pr) +
                          pdb_primitive_value_get_size(&pr) - 1,
                      pdb_verify_word_callback)) {
      fail = true;
    }
  }

  /*
   * Verify the VIP tables for left and right.
   */
  err = pdb_verify_vip(pdb, id, &pr, PDB_LINKAGE_LEFT, &budget, "left");
  if (err) {
    fail = true;
    *error_code |= PDB_VERIFY_VIPL;
  }

  err = pdb_verify_vip(pdb, id, &pr, PDB_LINKAGE_RIGHT, &budget, "right");
  if (err) {
    fail = true;
    *error_code |= PDB_VERIFY_VIPR;
  }

  /*
   * Verify the generation table
   */
  err = pdb_verify_generation(pdb, &g, &pr);
  if (err) {
    fail = true;
    *error_code |= PDB_VERIFY_GENERATION;
  }

  err = pdb_verify_versioned(pdb, &g, &pr);

  if (err) {
    fail = true;
    *error_code |= PDB_VERIFY_DEAD;
  }

  pdb_primitive_finish(pdb, &pr);

  return fail ? PDB_ERR_DATABASE : 0;
}

int pdb_verify_range(pdb_handle *pdb, pdb_id low, pdb_id high, int *count) {
  pdb_id id;
  int e = 0, err = 0;
  unsigned long error_code;
  char error_str[20];

  cl_enter(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_verify_range %lld..%lld (exclusive)", (long long)low,
           (long long)high);

  if (count) *count = 0;
  for (id = low; id < high; id++) {
    if ((e = pdb_verify_id(pdb, id, &error_code)) != 0) {
      if (err == 0) err = e;

      pdb_verify_render_error(error_str, 20, error_code);

      cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
             "pdb_verify_range: error verifying id %llx: verify_error_str:%s",
             (long long)id, error_str);
    }
    if (error_code && count) (*count)++;
  }

  cl_leave(pdb->pdb_cl, CL_LEVEL_VERBOSE, "%s",
           err ? pdb_xstrerror(err) : "ok");
  return err;
}
