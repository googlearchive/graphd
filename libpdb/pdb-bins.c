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

#include "libaddb/addb-hmap.h"

typedef struct pdb_binset {
  char const *binset_name;
  void *binset_table;
  size_t *binset_n; /* a horrible, horrible kludge */
  int binset_offset;
  size_t binset_elsz;
  int (*binset_comparator)(pdb_handle *pdb, void const *table_s,
                           void const *table_e, void const *value_s,
                           void const *value_e);
} pdb_binset;

#define STRING_END(s) ((s) ? ((s) + strlen(s)) : NULL)

/*
 * Render e..s into buf_out, while octal escaping anything that isn't
 * in [A-z0-9].
 */
char *pdb_render_unsafe_text(pdb_handle *pdb,

                             char *buf_out, size_t size, size_t *used_out,
                             const char *s, const char *e) {
  size_t out = 0;
  // int err = 0;

  cl_assert(pdb->pdb_cl, size != 0);
  cl_assert(pdb->pdb_cl, buf_out != NULL);
  if (s == NULL) {
    buf_out[0] = 0;
    used_out = 0;
    return buf_out;
  }

  while ((out < (size - 1)) && (s < e)) {
    if (isascii(*s) && isalnum(*s)) {
      buf_out[out] = *s;
      out++;
    } else {
      if ((size - out) > 5) {
        out += snprintf(buf_out + out, 5, "\\%o", (unsigned int)*s);
      } else {
        // err = ENOSPC;
        goto done;
      }
    }
    s++;
  }
done:
  buf_out[out] = 0;
  if (used_out) *used_out = out;

  return buf_out;
}

static int pdb_number_compare(pdb_handle *pdb, const void *a_s, const void *a_e,
                              const void *b_s, const void *b_e) {
  const graph_number *an, *bn;

  cl_assert(pdb->pdb_cl, a_s || b_s);
  cl_assert(pdb->pdb_cl, !a_s || ((a_e - a_s) == (sizeof(graph_number))));
  cl_assert(pdb->pdb_cl, !b_s || ((b_e - b_s) == (sizeof(graph_number))));

  if (a_s == NULL) return 1;
  if (b_s == NULL) return -1;

  an = a_s;
  bn = b_s;

  return graph_number_compare(an, bn);
}

/*
 * This is the comparison function used to sort bins.
 * Its equality to graphd_strcasecmp is only coincidental and is likely
 * to change.
 */
static int pdb_bin_strcasecmp(pdb_handle *pdb, const void *a_s, const void *a_e,
                              const void *b_s, const void *b_e)

{
  const char *ts, *te;
  const char *vs, *ve;

  vs = b_s;
  ve = b_e;

  ts = *((const char **)a_s);
  if (!ts)
    te = NULL;
  else
    te = ts + strlen(ts);

  return graph_strcasecmp(ts, te, vs, ve);
}

pdb_binset pdb_binset_numbers = {.binset_name = "numeric",
                                 .binset_table = pdb_bins_number_table,
                                 .binset_n = &pdb_bins_number_size,
                                 .binset_offset = 20000,
                                 .binset_comparator = pdb_number_compare,
                                 .binset_elsz = sizeof(graph_number)};

pdb_binset pdb_binset_strings = {
    .binset_name = "strings",
    .binset_table = pdb_bins_string_table,
    .binset_n = &pdb_bins_string_size,
    .binset_offset = 0, /* Code in graphd assumes this is 0 */
    .binset_comparator = pdb_bin_strcasecmp,
    .binset_elsz = sizeof(char *)};

pdb_binset *pdb_binset_numbers_ptr = &pdb_binset_numbers;
pdb_binset *pdb_binset_strings_ptr = &pdb_binset_strings;

/*
 * Lookup the bin number that a particular string should be in
 *
 * table	an array of sorted strings specifying boundaries
 * n		the number of elements in the table
 * string	the string to search for
 */
static size_t pdb_bin_bsearch(pdb_handle *pdb, pdb_binset *binset,
                              const char *string_s, const char *string_e,
                              bool *exact) {
  int r;
  size_t start, end, middle;

  size_t n = *(binset->binset_n);

  start = 0;
  end = n;
  do {
    middle = start + (end - start) / 2;

    r = binset->binset_comparator(
        pdb, binset->binset_table + binset->binset_elsz * middle,
        binset->binset_table + binset->binset_elsz * middle +
            binset->binset_elsz,
        string_s, string_e);

    if (r == 0) {
      if (exact) *exact = true;
      return middle;
    } else if (r > 0) {
      end = middle;
    } else if (r < 0) {
      start = middle;
    }
    cl_assert(pdb->pdb_cl, (end - start) > 0);
  } while ((end - start) != 1);

  cl_assert(pdb->pdb_cl,
            binset->binset_comparator(
                pdb, binset->binset_table + binset->binset_elsz * start,
                binset->binset_table + binset->binset_elsz * start +
                    binset->binset_elsz,
                string_s, string_e) <= 0);

  cl_assert(pdb->pdb_cl,
            binset->binset_comparator(
                pdb, binset->binset_table + binset->binset_elsz * end,
                binset->binset_table + binset->binset_elsz * end +
                    binset->binset_elsz,
                string_s, string_e) >= 0);

  if (exact) {
    r = binset->binset_comparator(
        pdb, binset->binset_table + binset->binset_elsz * start,
        binset->binset_table + binset->binset_elsz * start +
            binset->binset_elsz,
        string_s, string_e);

    *exact = !r;
  }

  return start;
}

/*
 * Write this primitive into the appropriate value bin, if it has a value
 */

int pdb_value_bin_synchronize(pdb_handle *pdb, pdb_id id,
                              pdb_primitive const *pr) {
  int bin;
  unsigned char key[4];
  int err;
  /*
   * No value means nothing to do.
   */

  if (!pdb_primitive_value_get_size(pr)) return 0;

  bin = (int)pdb_bin_bsearch(pdb, &pdb_binset_strings,
                             pdb_primitive_value_get_memory(pr),
                             (pdb_primitive_value_get_memory(pr) +
                              pdb_primitive_value_get_size(pr) - 1),
                             NULL);

  ADDB_PUT_U4(key, bin);

  err = addb_hmap_add(pdb->pdb_hmap, (unsigned long long)bin, (char *)key,
                      sizeof key, addb_hmt_bin, id);

  if (err) {
    char buff[100];
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "addb_hmap_add", err,
                 "Can't add id %llx (value: %s) to bin %i",
                 (unsigned long long)id,
                 pdb_render_unsafe_text(
                     pdb, buff, sizeof(buff), NULL,
                     (const char *)pdb_primitive_value_get_memory(pr),
                     (const char *)(pdb_primitive_value_get_memory(pr) +
                                    pdb_primitive_value_get_size(pr))),
                 (int)bin);

    return err;
  } else {
    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
           "pdb_value_bin_synchronize_added id %llx to bin %i",
           (unsigned long long)id, (int)bin);
  }

  /*
   * Now do it for numbers
   */
  {
    graph_number num;
    bool exact;

    err = graph_decode_number(pdb_primitive_value_get_memory(pr),
                              pdb_primitive_value_get_memory(pr) +
                                  pdb_primitive_value_get_size(pr) - 1,
                              &num, true);

    /*
* Not a number? Not a problem.
*/
    if (err) {
      cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
             "pdb_bins_synchronizing: not indexing %s"
             " because it is not a number",
             pdb_primitive_value_get_memory(pr));
      return 0;
    }

    bin = pdb_bin_bsearch(pdb, &pdb_binset_numbers, (char *)&num,
                          (char *)(&num + 1), &exact);
    /*
     * The number is exactly equal to the 'first' number
     * in the bin. Don't index because we can find it from
     * the hmap bin instead.
     */
    if (exact) {
      cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
             "pdb_bins_synchronize: not indexing %s"
             " because it its equal to the bin name",
             pdb_primitive_value_get_memory(pr));

      return 0;
    }

    bin += pdb_binset_numbers.binset_offset;

    ADDB_PUT_U4(key, bin);

    err = addb_hmap_add(pdb->pdb_hmap, (unsigned long long)bin, (char *)key,
                        sizeof key, addb_hmt_bin, id);

    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "addb_hmap_add", err,
                   "Can't add id %llx to bin %i", (unsigned long long)id, bin);
      return err;
    } else
      cl_log(pdb->pdb_cl, CL_LEVEL_SPEW, "adding id %llx to bin %i",
             (unsigned long long)id, bin);
  }
  return 0;
}

/*
 * Create an HMAP iterator that iterates over a single
 * bin
 */
int pdb_bin_to_iterator(pdb_handle *pdb, int bin, pdb_id low, pdb_id high,
                        bool forward, bool error_if_null, pdb_iterator **it) {
  int err;
  unsigned char key[4];

  ADDB_PUT_U4(key, bin);

  err = pdb_iterator_hmap_create(pdb, pdb->pdb_hmap, bin, (char *)key,
                                 sizeof key, addb_hmt_bin, low, high, forward,
                                 error_if_null, it);

  /*
   * ADDB_ERR_NO is a perfectly fine error. It simply means that
   * this bin doesn't have any IDs in it.
   */
  if (err && (err != ADDB_ERR_NO)) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_iterator_hmap_create", err,
                 "Can't grab the binned iterator for (bin %i)", bin);
  } else if (err == ADDB_ERR_NO) {
    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW, "pdb_bin_to_iterator: bin %i is empty",
           bin);
  } else {
    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
           "pdb_bin_to_iterator: created hmap iterator %p for"
           " bin %i",
           *it, bin);
  }
  return err;
}

/*
 * Create a single (hmap) iterator that iterates over whatever bin has
 * 'start' in it.
 * This is only used by pdb_verify
 */
int pdb_bin_one_iterator(pdb_handle *pdb, const char *start, bool forward,
                         pdb_iterator **it) {
  size_t bin;

  bin =
      pdb_bin_bsearch(pdb, &pdb_binset_strings, start, STRING_END(start), NULL);

  return pdb_bin_to_iterator(pdb, bin, 0, PDB_ITERATOR_HIGH_ANY, forward,
                             /* return an error if bin is empty? */ false, it);
}

int pdb_bin_lookup(pdb_handle *pdb, pdb_binset *binset, const void *s,
                   const void *e, bool *exact) {
  int b;

  b = pdb_bin_bsearch(pdb, binset, s, e, exact);

  return b + binset->binset_offset;
}

size_t pdb_bin_end(pdb_handle *pdb, pdb_binset *binset) {
  return (*(binset->binset_n) + binset->binset_offset);
}

size_t pdb_bin_start(pdb_handle *pdb, pdb_binset *binset) {
  return (binset->binset_offset);
}

void pdb_bin_value(pdb_handle *pdb, pdb_binset *binset, int bin, void **out_s) {
  bin -= binset->binset_offset;

  cl_assert(pdb->pdb_cl, bin < *(binset->binset_n));

  *out_s = binset->binset_table + bin * binset->binset_elsz;
}
