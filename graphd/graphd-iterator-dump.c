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
#include <limits.h> /* DBL_MAX */
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

/* graphd-iterator-dump.c -- dump an iterator as a graphd_value
 */

/*  Turn val into a list of statistics values about the iterator it.
 */
static int iterator_statistics(graphd_request *greq, pdb_iterator *it,
                               graphd_value *val) {
  /*  Return a structure that describes this iterator.
   */
  graphd_value *el;
  size_t i;
  int err;
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *pdb = graphd->g_pdb;
  cm_handle *cm = greq->greq_req.req_cm;
  cl_handle *cl = graphd_request_cl(greq);

  (void)pdb;

  err = graphd_value_list_alloc(graphd, cm, cl, val, 7);
  if (err != 0) return err;

  /* Six pairs. */
  for (i = 0; i < 6; i++) {
    err =
        graphd_value_list_alloc(graphd, cm, cl, val->val_list_contents + i, 2);
    if (err != 0) return err;
  }

  /* 1 */
  el = val->val_list_contents;
  graphd_value_text_set_cm(el->val_list_contents, GRAPHD_VALUE_STRING,
                           (char *)"n", 1, NULL);
  if (pdb_iterator_n_valid(pdb, it))
    graphd_value_number_set(el->val_list_contents + 1, pdb_iterator_n(pdb, it));
  else
    graphd_value_null_set(el->val_list_contents + 1);
  el++;

  /* 2 */
  graphd_value_text_set_cm(el->val_list_contents, GRAPHD_VALUE_STRING,
                           (char *)"check-cost", sizeof("check-cost") - 1,
                           NULL);
  if (pdb_iterator_check_cost_valid(pdb, it))
    graphd_value_number_set(el->val_list_contents + 1,
                            pdb_iterator_check_cost(pdb, it));
  else
    graphd_value_null_set(el->val_list_contents + 1);
  el++;

  /* 3 */
  graphd_value_text_set_cm(el->val_list_contents, GRAPHD_VALUE_STRING,
                           (char *)"next-cost", sizeof("next-cost") - 1, NULL);
  if (pdb_iterator_next_cost_valid(pdb, it))
    graphd_value_number_set(el->val_list_contents + 1,
                            pdb_iterator_next_cost(pdb, it));
  else
    graphd_value_null_set(el->val_list_contents + 1);
  el++;

  /* 4 */
  graphd_value_text_set_cm(el->val_list_contents, GRAPHD_VALUE_STRING,
                           (char *)"find-cost", sizeof("find-cost") - 1, NULL);
  if (pdb_iterator_find_cost_valid(pdb, it))
    graphd_value_number_set(el->val_list_contents + 1,
                            pdb_iterator_find_cost(pdb, it));
  else
    graphd_value_null_set(el->val_list_contents + 1);
  el++;

  /* 5 */
  graphd_value_text_set_cm(el->val_list_contents, GRAPHD_VALUE_STRING,
                           (char *)"low", sizeof("low") - 1, NULL);
  graphd_value_number_set(el->val_list_contents + 1, it->it_low);
  el++;

  /* 6 */
  graphd_value_text_set_cm(el->val_list_contents, GRAPHD_VALUE_STRING,
                           (char *)"high", sizeof("high") - 1, NULL);
  if (it->it_high == PDB_ITERATOR_HIGH_ANY)
    graphd_value_null_set(el->val_list_contents + 1);
  else
    graphd_value_number_set(el->val_list_contents + 1, it->it_high);
  el++;

  /* 7, not preallocated. */
  {
    char const *ord;

    if (!pdb_iterator_sorted_valid(pdb, it))
      graphd_value_null_set(el);
    else {
      if (pdb_iterator_sorted(pdb, it))
        ord = pdb_iterator_forward(pdb, it) ? "forward" : "backward";
      else
        ord = "unsorted";
      graphd_value_text_set_cm(el, GRAPHD_VALUE_STRING, (char *)ord,
                               strlen(ord), NULL);
    }
  }
  return 0;
}

/*  Turn val into a list of statistics values about the iterator it.
 */
static int iterator_type(graphd_request *greq, pdb_iterator *it,
                         graphd_value *val) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *pdb = graphd->g_pdb;
  cm_handle *cm = graphd_request_cm(greq);
  cl_handle *cl = graphd_request_cl(greq);
  char buf[500];
  char const *key_s, *key_e;
  char const *str;
  int err;
  pdb_id *fixed_id;
  size_t n, producer;
  char const *name;
  unsigned long long hash;
  pdb_iterator *sub;
  int linkage;

  PDB_IS_ITERATOR(cl, it);

  if (pdb_iterator_null_is_instance(pdb, it)) {
    graphd_value_null_set(val);
    return 0;
  }
  if (pdb_iterator_all_is_instance(pdb, it)) {
    err = graphd_value_list_alloc(graphd, cm, cl, val, 3);
    if (err != 0) return err;

    graphd_value_text_set_cm(val->val_list_contents, GRAPHD_VALUE_STRING,
                             (char *)"all", sizeof("all") - 1, NULL);
    graphd_value_number_set(val->val_list_contents + 1, it->it_low);
    graphd_value_number_set(val->val_list_contents + 2, it->it_high - 1);
    return 0;
  }
  if (pdb_iterator_gmap_is_instance(pdb, it, PDB_LINKAGE_ANY)) {
    pdb_id source_id;
    int linkage;

    err = graphd_value_list_alloc(graphd, cm, cl, val, 3);
    if (err != 0) return err;

    pdb_iterator_gmap_source_id(pdb, it, &source_id);
    pdb_iterator_gmap_linkage(pdb, it, &linkage);

    name = pdb_linkage_to_string(linkage);
    if (name == NULL) name = "???";

    graphd_value_text_set_cm(val->val_list_contents, GRAPHD_VALUE_STRING,
                             (char *)"gmap", 4, NULL);

    graphd_value_text_set_cm(val->val_list_contents + 1, GRAPHD_VALUE_STRING,
                             (char *)name, strlen(name), NULL);
    graphd_value_number_set(val->val_list_contents + 2, source_id);
    return 0;
  }
  if (pdb_iterator_bgmap_is_instance(pdb, it, PDB_LINKAGE_ANY)) {
    pdb_id source_id;

    err = graphd_value_list_alloc(graphd, cm, cl, val, 3);
    if (err != 0) return err;

    pdb_iterator_gmap_source_id(pdb, it, &source_id);
    name = pdb_iterator_bgmap_name(pdb, it);
    if (name == NULL) name = "???";

    graphd_value_text_set_cm(val->val_list_contents, GRAPHD_VALUE_STRING,
                             (char *)"bgmap", 4, NULL);

    graphd_value_text_set_cm(val->val_list_contents + 1, GRAPHD_VALUE_STRING,
                             (char *)name, strlen(name), NULL);
    graphd_value_number_set(val->val_list_contents + 2, source_id);
    return 0;
  }
  if (pdb_iterator_hmap_is_instance(pdb, it, &name, &hash, &key_s, &key_e)) {
    size_t i;
    char *w;

    err = graphd_value_list_alloc(graphd, cm, cl, val, 4);
    if (err != 0) return err;

    graphd_value_text_set_cm(val->val_list_contents, GRAPHD_VALUE_STRING,
                             (char *)"hmap", 4, NULL);
    graphd_value_text_strdup(cm, val->val_list_contents + 1,
                             GRAPHD_VALUE_STRING, (char *)name,
                             name + strlen(name));
    cl_assert(cl, key_e - key_s <= sizeof(buf) / 3 - 1);

    w = buf;
    for (i = 0; i < key_e - key_s; i++) {
      if (isascii(key_s[i]) && isalnum(key_s[i]))
        *w++ = key_s[i];
      else {
        snprintf(w, (buf + sizeof(buf)) - w, "\\%2.2hx",
                 (unsigned char)key_s[i]);
        if (w < buf + sizeof(buf)) w += strlen(w);
      }
    }
    *w = '\0';

    graphd_value_text_strdup(cm, val->val_list_contents + 2,
                             GRAPHD_VALUE_STRING, buf, buf + strlen(buf));
    graphd_value_number_set(val->val_list_contents + 3, hash);

    return 0;
  }

  if (graphd_iterator_prefix_is_instance(pdb, it, &key_s, &key_e)) {
    err = graphd_value_list_alloc(graphd, cm, cl, val, 3);
    if (err != 0) return err;

    graphd_value_text_set_cm(val->val_list_contents, GRAPHD_VALUE_STRING,
                             (char *)"prefix-or", sizeof("prefix-or") - 1,
                             NULL);
    graphd_value_text_strdup(cm, val->val_list_contents + 1,
                             GRAPHD_VALUE_STRING, key_s, key_e);

    graphd_iterator_prefix_or(pdb, it, &sub);
    if (sub == NULL)
      graphd_value_null_set(val->val_list_contents + 2);
    else {
      err = graphd_value_list_alloc(graphd, cm, cl, val->val_list_contents + 2,
                                    2);
      if (err != 0) return err;

      err =
          iterator_type(greq, sub, val->val_list_contents[2].val_list_contents);
      if (err != 0) return err;

      err = iterator_statistics(
          greq, sub, val->val_list_contents[2].val_list_contents + 1);
      if (err != 0) return err;
    }
    return 0;
  }

  if (graphd_iterator_fixed_is_instance(pdb, it, &fixed_id, &n)) {
    size_t i;

    err = graphd_value_list_alloc(graphd, cm, cl, val, n + 1);
    if (err != 0) return err;

    graphd_value_text_set_cm(val->val_list_contents, GRAPHD_VALUE_STRING,
                             (char *)"fixed", 5, NULL);
    for (i = 0; i < n; i++)
      graphd_value_number_set(val->val_list_contents + 1 + i, fixed_id[i]);
    return 0;
  }

  if (graphd_iterator_isa_is_instance(pdb, it, &linkage, &sub)) {
    err = graphd_value_list_alloc(graphd, cm, cl, val, 3);
    if (err != 0) return err;

    graphd_value_text_set_cm(val->val_list_contents, GRAPHD_VALUE_STRING,
                             (char *)"is-a", 4, NULL);

    name = pdb_linkage_to_string(linkage);
    graphd_value_text_set_cm(val->val_list_contents + 1, GRAPHD_VALUE_STRING,
                             (char *)name, strlen(name), NULL);

    err =
        graphd_value_list_alloc(graphd, cm, cl, val->val_list_contents + 2, 2);
    if (err != 0) return err;

    err = iterator_type(greq, sub, val->val_list_contents[2].val_list_contents);
    if (err != 0) return err;

    err = iterator_statistics(greq, sub,
                              val->val_list_contents[2].val_list_contents + 1);
    if (err != 0) return err;
    return 0;
  }

  if (graphd_iterator_linksto_is_instance(pdb, it, &linkage, &sub)) {
    cl_assert(cl, sub != NULL);
    PDB_IS_ITERATOR(cl, sub);

    err = graphd_value_list_alloc(graphd, cm, cl, val, 3);
    if (err != 0) return err;

    graphd_value_text_set_cm(val->val_list_contents, GRAPHD_VALUE_STRING,
                             (char *)"links-to", 8, NULL);

    name = pdb_linkage_to_string(linkage);
    graphd_value_text_set_cm(val->val_list_contents + 1, GRAPHD_VALUE_STRING,
                             (char *)name, strlen(name), NULL);

    err =
        graphd_value_list_alloc(graphd, cm, cl, val->val_list_contents + 2, 2);
    if (err != 0) return err;

    err = iterator_type(greq, sub, val->val_list_contents[2].val_list_contents);
    if (err != 0) return err;

    err = iterator_statistics(greq, sub,
                              val->val_list_contents[2].val_list_contents + 1);
    if (err != 0) return err;
    return 0;
  }

  if (graphd_iterator_and_is_instance(pdb, it, &n, &producer)) {
    size_t i;

    err = graphd_value_list_alloc(graphd, cm, cl, val, n + 1);
    if (err != 0) return err;

    graphd_value_text_set_cm(val->val_list_contents, GRAPHD_VALUE_STRING,
                             (char *)"and", 3, NULL);

    for (i = 0; i < n; i++) {
      err = graphd_iterator_and_get_subconstraint(pdb, it, i, &sub);
      cl_assert(cl, err == 0);

      err = graphd_value_list_alloc(graphd, cm, cl,
                                    val->val_list_contents + 1 + i,
                                    i == producer ? 3 : 2);
      if (err != 0) return err;

      err = iterator_type(greq, sub,
                          val->val_list_contents[1 + i].val_list_contents);
      if (err != 0) return err;

      err = iterator_statistics(
          greq, sub, val->val_list_contents[1 + i].val_list_contents + 1);
      if (err != 0) return err;

      if (i == producer)
        graphd_value_text_set_cm(
            val->val_list_contents[1 + i].val_list_contents + 2,
            GRAPHD_VALUE_STRING, (char *)"producer", sizeof("producer") - 1,
            NULL);
    }
    return 0;
  }

  if (graphd_iterator_or_is_instance(pdb, it, &n)) {
    size_t i;

    err = graphd_value_list_alloc(graphd, cm, cl, val, n + 1);
    if (err != 0) return err;

    graphd_value_text_set_cm(val->val_list_contents, GRAPHD_VALUE_STRING,
                             (char *)"or", 2, NULL);

    for (i = 0; i < n; i++) {
      err = graphd_iterator_or_get_subconstraint(pdb, it, i, &sub);
      cl_assert(cl, err == 0);

      err = graphd_value_list_alloc(graphd, cm, cl,
                                    val->val_list_contents + 1 + i, 2);
      if (err != 0) return err;

      err = iterator_type(greq, sub,
                          val->val_list_contents[1 + i].val_list_contents);
      if (err != 0) return err;

      err = iterator_statistics(
          greq, sub, val->val_list_contents[1 + i].val_list_contents + 1);
      if (err != 0) return err;
    }
    return 0;
  }

  if (graphd_iterator_vip_is_instance(pdb, it)) {
    err = graphd_value_list_alloc(graphd, cm, cl, val, 4);
    if (err != 0) return err;

    graphd_value_text_set_cm(val->val_list_contents, GRAPHD_VALUE_STRING,
                             (char *)"vip", 3, NULL);

    name = pdb_linkage_to_string(graphd_iterator_vip_linkage(pdb, it));
    graphd_value_text_set_cm(val->val_list_contents + 1, GRAPHD_VALUE_STRING,
                             (char *)name, strlen(name), NULL);

    graphd_value_number_set(val->val_list_contents + 2,
                            graphd_iterator_vip_source_id(pdb, it));

    graphd_value_number_set(val->val_list_contents + 3,
                            graphd_iterator_vip_type_id(pdb, it));
    return 0;
  }

  str = pdb_iterator_to_string(pdb, it, buf, sizeof buf);
  return graphd_value_text_strdup(cm, val, GRAPHD_VALUE_STRING, str,
                                  str + strlen(str));
}

/**
 * @brief ITERATOR II: Print the iterator.
 *
 *  Compiling statistics means that the iterator figures out
 *  internally how to actually get us its values.  Only after
 *  statistics have taken place do we know, e.g., whether the
 *  iterator is sorted, and in what direction (if any).
 *
 * @param stack		execution stack in the request
 * @param stack_context	current read constraint context
 *
 * @return 0 on successful, possibly partial, execution
 * @return other nonzero errors on system/resource error.
 */
int graphd_iterator_dump(graphd_request *greq, pdb_iterator *it,
                         graphd_value *val) {
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = graphd_request_cm(greq);
  graphd_handle *graphd = graphd_request_graphd(greq);
  int err = 0;

  PDB_IS_ITERATOR(cl, it);

  /*  Return a structure that describes this iterator.
   */
  if ((err = graphd_value_list_alloc(graphd, cm, cl, val, 2)) != 0 ||
      (err = iterator_type(greq, it, val->val_list_contents)) != 0)
    return err;

  return iterator_statistics(greq, it, val->val_list_contents + 1);
}
