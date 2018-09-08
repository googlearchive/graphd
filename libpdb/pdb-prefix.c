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

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "libaddb/addb-bmap.h"

#define LAST_N_CHARS_MASKED(x) ((1ul << ((x)*5)) - 1ul)
#define HASH_IS_TEST(x) (((x)&0x1F) == 0x1F)

/*
 *  Given the previous test results (if any) and the previous hash,
 *  produce a new hash.
 *
 *  If the hash ends in 1F, this is a new test.
 *  Otherwise, it's a specific lookup; if it finds something,
 *  return the found indices.
 */
static int pdb_prefix_next_hash(bool yes, unsigned long hash_in,
                                unsigned long* hash_out) {
  int n = 0;

  if ((hash_in & 0x1F) == 0) {
    /*  ...00 -> ...## */

    for (n = 0; n < 5; n++) {
      if ((hash_in & (0x1Ful << (5 * n))) == 0)
        hash_in |= 0x1Ful << (5 * n);
      else
        break;
    }
    *hash_out = hash_in;
    return 0;
  } else if ((hash_in & 0x1F) == 0x1F) {
    /*  ...## */

    if ((hash_in & LAST_N_CHARS_MASKED(2)) != LAST_N_CHARS_MASKED(2))

      n = 1; /* ....# */

    else if ((hash_in & LAST_N_CHARS_MASKED(3)) != LAST_N_CHARS_MASKED(3))

      n = 2; /* ...## */

    else if ((hash_in & LAST_N_CHARS_MASKED(4)) != LAST_N_CHARS_MASKED(4))

      n = 3; /* ..### */
    else
      n = 4; /* .#### */

    if (yes) {
      /* ...## -> ...10 */

      *hash_out = (hash_in & ~LAST_N_CHARS_MASKED(n)) | (1ul << ((n - 1) * 5));
      return 0;
    }
  }

  /* Carry */
  for (; n < 5; n++) {
    if ((0x1F & (hash_in >> (5 * n))) < 0x1E) {
      /* ..299 -> ..300 */

      *hash_out = (hash_in + (0x1ul << (5 * n))) & ~LAST_N_CHARS_MASKED(n);
      return 0;
    }
  }
  return PDB_ERR_NO;
}

/**
 * @brief Return to the next iterator for a context
 *
 * @param pdb		opaque module handle, created with pdb_create()
 * @param ppc		context, initialized with pdb_prefix_initialize()
 * @param low		first value of included range
 * @param high		last value of incldued range
 * @param forward	sort low-to-high?
 * @param it_out	assign result iterator to here.
 *
 * @return 0 on success, a nonzero error code on error
 */
int pdb_prefix_next(pdb_prefix_context* ppc, pdb_id low, pdb_id high,
                    bool forward, pdb_iterator** it_out) {
  pdb_handle* pdb = ppc->ppc_pdb;
  cl_handle* cl = pdb->pdb_cl;
  int err = 0;
  char key[4];
  bool bit = 0;

  for (;;) {
    /* Get the next prefix hash. */

    if (ppc->ppc_first)
      ppc->ppc_first = false;
    else {
      err = pdb_prefix_next_hash(bit, ppc->ppc_hash_current,
                                 &ppc->ppc_hash_current);

      if (err != 0 ||
          (ppc->ppc_hash_current & ppc->ppc_hash_mask) !=
              ppc->ppc_hash_original)
        break;
    }

    /*  Is the bit corresponding to our current hash code set?
     */
    err = addb_bmap_check(pdb->pdb_prefix, ppc->ppc_hash_current, &bit);

    /*  No - there are no values with this
     *  prefix / no such words.  Go to try the next prefix.
     */
    if (err == PDB_ERR_NO || !bit) continue;

    /*  Something else is wrong?
     */
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "addb_bmap_check", err, "(for %lx in %s)",
                   (unsigned long)ppc->ppc_hash_current, ppc->ppc_title);
      return err;
    }

    /*  Yes, we have entries here.
     *
     *  If this hash code corresponds to the potential completions,
     *  set a flag that makes the next test recurse further into
     *  those potential completions.
     */
    if (HASH_IS_TEST(ppc->ppc_hash_current)) continue;

    /*  The hash code corresponds to a possible full word.
     *  See if we have hash table entries for this hash.
     */
    pdb_word_key(ppc->ppc_hash_current, key);
    err = pdb_iterator_hmap_create(pdb, pdb->pdb_hmap, ppc->ppc_hash_current,
                                   key, sizeof key, addb_hmt_word, low, high,
                                   forward, /* error-if-null */ true, it_out);
    if (err != 0) {
      if (err != PDB_ERR_NO) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_hmap_create", err,
                     "(for %lx in %s)", (unsigned long)ppc->ppc_hash_current,
                     ppc->ppc_title);
        return err;
      }
    } else if (pdb_iterator_null_is_instance(pdb, *it_out))
      pdb_iterator_destroy(pdb, it_out);
    else
      return 0;
  }
  return PDB_ERR_NO;
}

/**
 * @brief initialize a prefix context.
 *
 *  The prefix context enumerates words
 *  or word prefixes that start with the
 *  prefix supplied at its creation.

 * @param pdb		hosting PDB module handle
 * @param s		beginning of the prefix
 * @param e		end of the prefix (where '\0' would be)
 * @param ppc		context to initialize
 */
void pdb_prefix_initialize(pdb_handle* pdb, char const* s, char const* e,
                           pdb_prefix_context* ppc) {
  size_t len;
  cl_handle* cl = pdb->pdb_cl;

  len = pdb_word_utf8len(pdb, s, e);
  cl_assert(cl, len > 0 && len < 5);

  snprintf(ppc->ppc_title, sizeof(ppc->ppc_title), "%.*s", (int)(e - s), s);

  ppc->ppc_hash_current = ppc->ppc_hash_original = pdb_word_hash(pdb, s, e);
  ppc->ppc_hash_mask = LAST_N_CHARS_MASKED(len) << (5 * (5 - len));
  ppc->ppc_len = len;
  ppc->ppc_pdb = pdb;

  ppc->ppc_first = true;
}

static pdb_prefix_statistics* pdb_prefix_statistics_slot(pdb_handle* pdb,
                                                         char const* s,
                                                         char const* e) {
  size_t len;
  unsigned short h;
  cl_handle* cl = pdb->pdb_cl;

  len = pdb_word_utf8len(pdb, s, e);
  if (len <= 0 || len > 2) return NULL;

  h = pdb_word_hash(pdb, s, e) >> ((5 - 2) * 5);
  cl_assert(cl, h < sizeof(pdb->pdb_prefix_statistics) /
                        sizeof(*pdb->pdb_prefix_statistics));

  return pdb->pdb_prefix_statistics + h;
}

int pdb_prefix_statistics_load(pdb_handle* pdb, pdb_iterator* it, char const* s,
                               char const* e) {
  cl_handle* cl = pdb->pdb_cl;
  pdb_prefix_statistics const* pps;
  char buf[200];

  /*  Did we cache statistics for this prefix?
   */
  pps = pdb_prefix_statistics_slot(pdb, s, e);
  if (pps == NULL || pps->pps_next_cost == 0) {
    cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
           "pdb_prefix_statistics_load: %.*s: %s", (int)(e - s), s,
           pps ? "null next cost" : "no slot found");
    return PDB_ERR_MORE;
  }

  pdb_iterator_n_set(pdb, it, pps->pps_n);
  pdb_iterator_sorted_set(pdb, it, true);
  pdb_iterator_next_cost_set(pdb, it, pps->pps_next_cost);
  pdb_iterator_find_cost_set(pdb, it, pps->pps_find_cost);
  pdb_iterator_check_cost_set(pdb, it, PDB_COST_PRIMITIVE + 10);
  pdb_iterator_statistics_done_set(pdb, it);

  cl_log(cl, CL_LEVEL_VERBOSE | PDB_FACILITY_ITERATOR,
         "PDB STAT for %s (cached): n=%llu cc=%llu; "
         "nc=%llu fc=%llu; sorted",
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         (unsigned long long)pdb_iterator_n(pdb, it),
         (unsigned long long)pdb_iterator_check_cost(pdb, it),
         (unsigned long long)pdb_iterator_next_cost(pdb, it),
         (unsigned long long)pdb_iterator_find_cost(pdb, it));
  return 0;
}

void pdb_prefix_statistics_store(pdb_handle* pdb, pdb_iterator const* it,
                                 char const* s, char const* e) {
  pdb_prefix_statistics* pps;
  pdb_budget x;

  pps = pdb_prefix_statistics_slot(pdb, s, e);
  if (pps == NULL) return;

  pps->pps_n = pdb_iterator_n(pdb, it);

  if ((x = pdb_iterator_next_cost(pdb, it)) > USHRT_MAX) x = USHRT_MAX;
  pps->pps_next_cost = x;

  if ((x = pdb_iterator_find_cost(pdb, it)) > USHRT_MAX) x = USHRT_MAX;
  pps->pps_find_cost = x;
}

/**
 * @brief Invalidate the prefix statistics cache for a prefix.
 *
 *  In other words, a value that starts with s...e is being
 *  added to the system; adjust the cache to reflect that.
 *
 *  Actually, the statistics cache only invalidates if this
 *  happens a few times over - we can handle a little bit
 *  of drift.
 *
 * @param pdb	pdb handle
 * @param s		beginning of added value.
 * @param e 		end of added value.
 */

#define PDB_PREFIX_DRIFT_MAX 0.05

static void pps_drift(pdb_prefix_statistics* pps) {
  if (pps->pps_next_cost > 0) {
    pps->pps_drift++;
    if (pps->pps_drift >= pps->pps_n * PDB_PREFIX_DRIFT_MAX) {
      pps->pps_drift = 0;
      pps->pps_n = 0;
      pps->pps_next_cost = 0;
      pps->pps_find_cost = 0;
    }
  }
}

void pdb_prefix_statistics_drift(pdb_handle* pdb, char const* s,
                                 char const* e) {
  unsigned short h;
  size_t len;

  if ((len = pdb_word_utf8len(pdb, s, e)) <= 0) return;
  h = pdb_word_hash(pdb, s, e) >> ((5 - 2) * 5);

  /* Words that start with the first letter...
   */
  if (len > 1) pps_drift(pdb->pdb_prefix_statistics + (h | 0x1F));

  /* Words that start with the beginning...
   */
  pps_drift(pdb->pdb_prefix_statistics + h);
}
