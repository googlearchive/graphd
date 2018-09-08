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
#include "graphd/graphd-read.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

char const* graphd_pattern_frame_to_string(graphd_pattern_frame const* pf,
                                           char* buf, size_t size) {
  char pbuf[200];

  if (pf == NULL) return "pf_null";

  if (pf->pf_set == NULL) {
    if (pf->pf_one == NULL) return "{pf:NULL/NULL}";

    snprintf(buf, size, "pf_one{%s, offset=%zu}",
             graphd_pattern_dump(pf->pf_one, pbuf, sizeof pbuf),
             pf->pf_one_offset);
    return buf;
  }
  if (pf->pf_one == NULL) {
    snprintf(buf, size, "pf_set{%s}",
             graphd_pattern_dump(pf->pf_set, pbuf, sizeof pbuf));
    return buf;
  }
  snprintf(buf, size, "pf{%s[one: %zu]}",
           graphd_pattern_dump(pf->pf_set, pbuf, sizeof pbuf),
           pf->pf_one_offset);
  return buf;
}

static void graphd_pattern_to_pattern_frame(cl_handle* cl, graphd_pattern* pat,
                                            graphd_pattern_frame* pf) {
  char b1[200], b2[200], b3[200];

  if ((pf->pf_set = pat) != NULL && pat->pat_type == GRAPHD_PATTERN_UNSPECIFIED)
    pf->pf_set = NULL;

  pf->pf_one_offset = 0;

  if (pat == NULL || pat->pat_type != GRAPHD_PATTERN_LIST)
    pf->pf_one = NULL;
  else {
    pat = pat->pat_list_head;
    while (pat != NULL && pat->pat_type != GRAPHD_PATTERN_LIST) {
      pf->pf_one_offset++;
      pat = pat->pat_next;
    }
    pf->pf_one = pat;
  }

  cl_log(
      cl, CL_LEVEL_VERBOSE,
      "graphd_pattern_to_pattern_frame: in: %s; set: %s one@%zu: %s",
      graphd_pattern_dump(pat, b1, sizeof b1),
      (pf->pf_set ? graphd_pattern_dump(pf->pf_set, b2, sizeof b2) : "null"),
      pf->pf_one_offset,
      (pf->pf_one ? graphd_pattern_dump(pf->pf_one, b3, sizeof b3) : "null"));
}

/*  We want to sample values of <sample>.  Find them
 *  somewhere in the per-single-id result set.
 *  If none exist, create one in the unnamed last result frame.
 *
 *  If "sort_only" is 1, newly created pattern frames are
 *  tagged as sort-only.  If it is 0, used patterns have
 *  that flag set to 0.  (This isn't symmetrical -- if sort_only
 *  is both 1 and 0 in different invocations, it needs to
 *  come out 0 in the end.)
 */
static int locate_sample_atom(graphd_request* greq, graphd_constraint* con,
                              graphd_pattern* sample, bool sort_only) {
  size_t i;
  cl_handle* const cl = graphd_request_cl(greq);
  graphd_pattern_frame* pf;
  graphd_pattern* pat;
  char buf[200];

  /*  Find the value we're sampling somewhere in the
   *  existing result values.
   */
  cl_log(cl, CL_LEVEL_VERBOSE, "locate_sample_atom %s",
         graphd_pattern_dump(sample, buf, sizeof buf));
  cl_assert(cl, sample != NULL);
  cl_assert(cl, sample->pat_type != GRAPHD_PATTERN_LIST);

  cl_assert(cl, !graphd_pattern_is_set_dependent(cl, con, sample));

  for (i = 0, pf = con->con_pframe; i < con->con_pframe_n; i++, pf++) {
    graphd_pattern* pat = pf->pf_one;
    size_t j = 0;

    if (pat == NULL) continue;
    cl_assert(cl, pat->pat_type == GRAPHD_PATTERN_LIST);

    for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next) {
      if (graphd_pattern_equal_value(cl, con, pat, con, sample)) {
        char buf[200];
        cl_log(cl, CL_LEVEL_VERBOSE,
               "locate_sample_"
               "atom: %s at result=%zu elem=%zu",
               graphd_pattern_dump(pat, buf, sizeof buf), i, j);
        sample->pat_result_offset = i;
        sample->pat_element_offset = j;

        pat->pat_sort_only &= sort_only;

        return 0;
      }
      j++;
    }
  }

  /*  We couldn't find the pattern.  Add an overflow pframe
   *  (if we don't have one already), and add the pattern
   *  into that overflow pframe.
   */
  cl_assert(cl, con->con_pframe_n >= con->con_assignment_n);
  cl_assert(cl, con->con_pframe_n <= con->con_assignment_n + 2);

  if (con->con_pframe_temporary == (size_t)-1) {
    cl_assert(cl, con->con_pframe_n < con->con_assignment_n + 2);

    con->con_pframe_temporary = con->con_pframe_n++;
    pf = con->con_pframe + con->con_pframe_temporary;
    pf->pf_one = NULL;
    pf->pf_set = NULL;
  } else {
    pf = con->con_pframe + con->con_pframe_temporary;
  }

  if (pf->pf_one == NULL) {
    graphd_pattern* parent = NULL;

    /*  We need to allocate both "pf_set" and "pf_one" if we need
     *  to collect temporaries from all matching records,
     *  then sort them, then sample.
     *
     *  We allocate only "pf_one" if we need temporaries
     *  returned from the single-record matches, but not
     *  collected (i.e. we merely sample).
     */
    if (con->con_sort != NULL && con->con_sort_valid) {
      pf->pf_set = parent =
          graphd_pattern_alloc(greq, NULL, GRAPHD_PATTERN_LIST);
      if (pf->pf_set == NULL) return ENOMEM;
    }
    pf->pf_one = graphd_pattern_alloc(greq, parent, GRAPHD_PATTERN_LIST);
    if (pf->pf_one == NULL) return ENOMEM;
    pf->pf_one_offset = 0;
  }
  cl_assert(cl, pf->pf_one != NULL);
  cl_assert(cl, pf->pf_one->pat_type == GRAPHD_PATTERN_LIST);

  /* Add a duplicate of the pattern we're looking for to the list.
   */
  pat = graphd_pattern_dup(greq, pf->pf_one, sample);
  if (pat == NULL) return ENOMEM;

  sample->pat_result_offset = con->con_pframe_temporary;
  sample->pat_element_offset = pf->pf_one->pat_list_n - 1;

  cl_log(cl, CL_LEVEL_VERBOSE,
         "locate_sample_atom: added %s at "
         "result=%zu elem=%hu",
         graphd_pattern_dump(pat, buf, sizeof buf), sample->pat_result_offset,
         sample->pat_element_offset);

  pat->pat_sort_only = sort_only;
  return 0;
}

static int locate_samples(graphd_request* greq, graphd_constraint* con,
                          graphd_pattern* pat, bool sort_only) {
  int err;
  cl_handle* const cl = graphd_request_cl(greq);
  char buf[200];

  cl_log(cl, CL_LEVEL_VERBOSE, "locate_samples %s sort_only=%s",
         graphd_pattern_dump(pat, buf, sizeof buf),
         sort_only ? "true" : "false");

  cl_assert(cl, pat != NULL);

  if (pat->pat_type != GRAPHD_PATTERN_LIST) {
    if (graphd_pattern_is_primitive_dependent(cl, con, pat)) {
      err = locate_sample_atom(greq, con, pat, sort_only);
      if (err != 0) return err;
    }
    return 0;
  }
  for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next) {
    if (pat->pat_type == GRAPHD_PATTERN_LIST) continue;

    if (graphd_pattern_is_primitive_dependent(cl, con, pat)) {
      err = locate_sample_atom(greq, con, pat, sort_only);
      if (err != 0) return err;
    }
  }
  return 0;
}

bool graphd_pattern_frame_uses_per_primitive_data(graphd_request* greq,
                                                  graphd_constraint* con) {
  size_t i;

  (void)greq;

  for (i = 0; i < con->con_pframe_n; i++)
    if (con->con_pframe[i].pf_one != NULL) return true;

  return false;
}

void graphd_pattern_frame_spectrum(graphd_request* greq, graphd_constraint* con,
                                   unsigned long long* set_out,
                                   unsigned long long* one_out) {
  size_t i;

  (void)greq;

  for (i = 0; i < con->con_pframe_n; i++) {
    if (con->con_pframe[i].pf_one != NULL)
      *one_out |= graphd_pattern_spectrum(con->con_pframe[i].pf_one);
    if (con->con_pframe[i].pf_set != NULL)
      *one_out |= graphd_pattern_spectrum(con->con_pframe[i].pf_set);
  }
}

/**
 * @brief Compute the pattern frames for a constraint.
 *
 *  The pattern frame determines which values are harvested
 *  into which slot for a context, both per individual matched id
 *  ("one") and for all of them ("set").
 */

int graphd_pattern_frame_create(graphd_request* greq, graphd_constraint* con) {
  size_t n;
  graphd_pattern_frame* pf;
  cm_handle* cm = greq->greq_req.req_cm;
  cl_handle* const cl = graphd_request_cl(greq);
  graphd_assignment* a;
  size_t i;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  /*  One pattern frame for the result, if there is one.
   *  One for anything assigned (implicit and explicit).
   *  One, potentially, for sorting and sampling.
   */
  n = con->con_assignment_n + 2;

  /* Allocate space for the pattern frames.
   */
  con->con_pframe = pf = cm_zalloc(cm, sizeof(*con->con_pframe) * n);
  if (con->con_pframe == NULL) {
    cl_leave(cl, CL_LEVEL_VERBOSE, "malloc fails");
    return ENOMEM;
  }

  a = con->con_assignment_head;
  for (n = 0; n < con->con_assignment_n; n++, a = a->a_next)
    graphd_pattern_to_pattern_frame(cl, a->a_result, pf++);

  if (con->con_result != NULL)
    graphd_pattern_to_pattern_frame(cl, con->con_result, pf++);

  con->con_pframe_n = pf - con->con_pframe;

  if (con->con_sort != NULL && con->con_sort_valid)
    locate_samples(greq, con, con->con_sort, true);

  /* Now that we have the declared pframes, make sure all
   * samples (non-list elements of pf_set) are locatable (exist
   * as list-elements of pf_one somewhere).
   *
   * This may expand pf into the (con->con_assignment_n+1)th slot
   * (if locating the sort elements didn't do that.)
   */
  pf = con->con_pframe;

  for (i = 0; i < con->con_pframe_n; i++, pf++)
    if (pf->pf_set != NULL) {
      int err = locate_samples(greq, con, pf->pf_set, false);
      if (err != 0) {
        cl_leave(cl, CL_LEVEL_VERBOSE, "error from locate_samples");
        return err;
      }
    }

  /*  Mark whether we want cursors, counts, and per-element data.
   *
   *  Promote sort-only flags (set by locate_samples() above)
   *  from all list elements to the lists themselves.
   *
   *  (If all elements in a list are sort-only, the list itself
   *  becomes sort-only, unless it was empty to begin with.)
   */
  pf = con->con_pframe;
  for (i = 0; i < con->con_pframe_n; i++, pf++) {
    /*  If pf->pf_one is present, but pf->pf_set isn't, this
     *  is just a frame of sampling data that we don't need
     *  once the sampling is over.
     */
    if (pf->pf_one != NULL && pf->pf_set != NULL)
      con->con_pframe_want_data = true;

    if (graphd_pattern_lookup(pf->pf_set, GRAPHD_PATTERN_CURSOR) != NULL)
      con->con_pframe_want_cursor = true;

    if (graphd_pattern_lookup(pf->pf_set, GRAPHD_PATTERN_COUNT) != NULL)
      con->con_pframe_want_count = true;

    if (pf->pf_one != NULL && pf->pf_one->pat_list_n != 0) {
      graphd_pattern* pat = pf->pf_one;
      cl_assert(cl, pat->pat_type == GRAPHD_PATTERN_LIST);

      for (pat = pat->pat_list_head; pat != NULL; pat = pat->pat_next) {
        if (!pat->pat_sort_only) break;
      }

      if (pat == NULL) pf->pf_one->pat_sort_only = true;
    }
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "%zu frame%s", con->con_pframe_n,
           con->con_pframe_n == 1 ? "" : "s");
  return 0;
}
