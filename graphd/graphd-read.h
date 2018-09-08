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
#ifndef GRAPHD_READ_H
#define GRAPHD_READ_H 1

#include "graphd/graphd.h"

#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

typedef struct graphd_read_base graphd_read_base;
typedef struct graphd_read_context {
  graphd_stack_context grc_sc;

  struct graphd_read_context *grc_parent;
  struct graphd_read_base *grc_base;
  graphd_constraint *grc_con;

  /*  If the ID is not PDB_ID_NONE, then either the GRC
   *  context ist frozen, or grc_alt_pr is the loaded primitive.
   *  Keep the two in sync - when finishing grc_alt_pr, also
   *  set grc_alt_id to PDB_ID_NONE.
   */
  pdb_id grc_alt_id;
  pdb_primitive grc_alt_pr;
  graphd_value grc_alt_contents;

  /*  grc_count_wanted -- we want to count how many matching elements
   *  	there are.  Keep scanning, even after a page fills up.
   */
  unsigned int grc_count_wanted : 1;

  /*  grc_data_wanted -- we are using the contents or element data
   * 	somewhere, we don't just want to know whether something
   *	matches or how many there are.
   */
  unsigned int grc_data_wanted : 1;

  /*  grc_sample_wanted -- even though the page may have filled
   *	up, there are still unassigned variables that wait for
   *	something in the iteration to assign them.
   */
  unsigned int grc_sample_wanted : 1;

  /*  grc_cursor_wanted -- are we looking for a cursor?
   *	If yes, we need to go for one further than the pagesize,
   * 	to be able to return NULL if we run out by the end of the page.
   */
  unsigned int grc_cursor_wanted : 1;

  /*  grc_count -- the number of returned elements.
   *
   *  grc_count_total -- mostly (unsigned long long)-1 (invalid);
   *	when set to anything else, a credible estimate of the total
   *	count to use when filling in outer-level "count" requests,
   *	sometimes short-circuiting iterations.
   */
  unsigned long long grc_count;
  unsigned long long grc_count_total;

  graph_guid grc_parent_guid_buf;
  graph_guid const *grc_parent_guid;
  graph_guid const *grc_guid;

  graphd_sort_context *grc_sort;

  pdb_iterator *grc_it;

  /*  Variable offsets assigned by subconstraints
   *
   *  While evaluating subconstraints, the subconstraints track which
   *  parent variables they're assigning to, so that those assignments
   *  can be undone if the containing constraints turn out not to
   *  match after all.
   */
  size_t *grc_sub_assigned;
  size_t grc_sub_assigned_n;

  /* local state for read_next_alternative (RNA)
   */

  size_t grc_state_rna_loc;
  unsigned int grc_state_rna_count_primitive : 1;
  unsigned int grc_state_rna_store_primitive : 1;

  int grc_sub_error;
  graphd_constraint *grc_sub_con;

  /*  If non-NULL, append the result to this sequence.
   */
  graphd_value *grc_contents_out;

  /*  If non-NULL, assign an error code to this location.
   */
  int *grc_err_out;

  /*  Error to pass to subevaluations.
   */
  int grc_err;

} graphd_read_context;

typedef struct graphd_read_parent {
  pdb_id rp_id;
  graph_guid rp_guid;
  pdb_primitive rp_pr;

  unsigned int rp_id_valid : 1;
  unsigned int rp_guid_valid : 1;
  unsigned int rp_pr_valid : 1;

} graphd_read_parent;

typedef void graphd_read_one_callback(void *data, int err, pdb_id id,
                                      graphd_constraint const *con,
                                      graphd_value *res);

struct graphd_read_set_context;
typedef struct graphd_read_one_context {
  graphd_stack_context groc_sc;
  struct graphd_read_base *groc_base;

  unsigned int groc_link;

  /*  NULL or the containing graphd_read_set context.
   *  The container is linkcounted through the dependent.
   */
  struct graphd_read_set_context *groc_parent;

  /*  The constraint that is being evaluated.
   */
  graphd_constraint *groc_con;

  /*  The single ID/primitive it is being evaluated against.
   */
  graphd_primitive_cache groc_pc;

  /*  When checking for subconstraint matches, which
   *  subconstraint are we matching right now?
   */
  graphd_constraint *groc_sub;
  size_t groc_sub_i;

  /*  Once we're done, call this callback.
   */
  graphd_read_one_callback *groc_callback;
  void *groc_callback_data;

  /*  The results.  One value pair sample/one.
   */
  graphd_value *groc_result;

  /*  Variable assignments returned by subconstraints.
   */
  graphd_value *groc_local;

  /*  Contents returned by subconstraints.
   *
   *  This is a sequence with one item each for each subconstraint;
   *  each sequence element is the main return value for the
   *  subconstraint set evaluation.
   */
  graphd_value groc_contents;

  /*  Unexpected errors.
   */
  int groc_err;

} graphd_read_one_context;

typedef void graphd_read_set_callback(void *data, int err,
                                      graphd_constraint const *con,
                                      graphd_value *res);

/* The "read-or-map" is a sequence of read-or-slots.
 * Each slot records the match state of the containing branch.
 */
typedef struct graphd_read_or_slot {
  /*  We have made a decision about whether or not this OR
   *  branch matches?
   */
  enum {
    GRAPHD_READ_OR_INITIAL = 0,

    /*  A mismatch somewhere along the line.
     */
    GRAPHD_READ_OR_FALSE,

    /*  The intrinsics of this constraint match
     *  (including its "or" subconditions).  We don't
     *  know about subconstraints, if any, yet.
     */
    GRAPHD_READ_OR_INTRINSICS_MATCH,

    /*  The intrinsics of this constraint match,
     *  and the contained subconstraints match, if any, too.
     */
    GRAPHD_READ_OR_TRUE
  } ros_state;
  graphd_constraint *ros_con;

} graphd_read_or_slot;

struct graphd_read_or_map {
  graphd_read_or_slot rom_buf[1];
  graphd_read_or_slot *rom_slot;
  size_t rom_n;
};

typedef struct graphd_read_set_context {
  graphd_stack_context grsc_sc;
  struct graphd_read_base *grsc_base;

  /*  The context that is being evaluated.
   */
  graphd_constraint *grsc_con;

  /*  The single ID/primitive we're currently looking at.
   */
  graphd_primitive_cache grsc_pc;

  /*  The iterator that generated it.
   */
  pdb_iterator *grsc_it;

  /*  For the purposes of checking our parent
   *  relationship - who is our parent in the constraint?
   */
  pdb_id grsc_parent_id;
  graph_guid grsc_parent_guid;

  /*  The location of the current ID on the returned page
   *  or in the sort page.
   */
  size_t grsc_page_location;

  /*  Temporary state for an ongoing sort.
   */
  graphd_sort_context *grsc_sort;

  /*  If any, this context's entry in the pdb_iterator_base.
   */
  void **grsc_pib_entry;

  /*  Temporary state for result count, including a count
   *  generated by an abstract pre-cached estimate.
   */
  unsigned long long grsc_count;
  unsigned long long grsc_count_total;

  /*  Outcome of the whole operation,
   *  grsc->grsc_con->con_pframe_n records of graphd_value.
   */
  graphd_value *grsc_result;

  /*  Track the progress in matching "or" subclauses here.
   */
  graphd_read_or_map grsc_rom;

  int grsc_err;

  unsigned int grsc_link;

  /*  If set, the grsc evaluation can return deferred values;
   *  its job is just to verify whether or not the constraint
   *  is met at all, not to actually produce values.
   */
  unsigned int grsc_verify : 1;

  /*  If set, the values returned do not redirect to continued
   *  evaluation of grsc.  (They can redirect to other contexts.)
   */
  unsigned int grsc_evaluated : 1;

  /*  While filling in samples, we've encountered one that
   *  we couldn't fill.
   */
  unsigned int grsc_sampling : 1;

  /*  While filling in samples, we've encountered one that
   *  is a deferred value.  We need to evaluate these deferred
   *  values on the spot - otherwise we can't tell whether we're
   *  still sampling or not...
   */
  unsigned int grsc_deferred_samples : 1;
  size_t grsc_deferred_samples_i;

  /*  Deliver results to this callback.
   */
  graphd_read_set_callback *grsc_callback;
  void *grsc_callback_data;

} graphd_read_set_context;

struct graphd_read_base {
  /*  Stack context, hooks this into the request stack.
   */
  graphd_stack_context grb_sc;

  /*  Request we're running for; we use it to get log handles,
   *  heap allocators, and the graphd context.
   */
  graphd_request *grb_greq;

  /*  Constraint to read.
   */
  graphd_constraint *grb_con;

  /*  Where to store the final result.
   */
  graphd_value *grb_val_out;
  int *grb_err_out;

  /*  Result for assignment by pushed graphd_read_contexts
   *  above this one.
   */
  graphd_value grb_val;
  int grb_err;

  /*  When this drops to zero, the base is free'd.
   */
  int grb_link;

  /*  Hook into the request's resource manager with this.
   */
  cm_resource grb_request_resource;

  /*  Something has been deferred in the course of answering
   *  this request.
   */
  unsigned int grb_deferred : 1;
};

/* graphd-read-base.c */

int graphd_read_base_push(graphd_request *greq, graphd_constraint *con,
                          graphd_value *val_out, int *err_out,
                          graphd_read_base **grb_out);

/* graphd-read-base-evaluate.c */

int graphd_read_base_evaluate_push(graphd_request *greq, graphd_value *val);

/* graphd-read-one.c */

void graphd_read_one_context_link(graphd_read_one_context *groc);
void graphd_read_one_context_free(graphd_read_one_context *groc);
void graphd_read_one_push(graphd_read_base *_grb,
                          graphd_read_set_context *_grsc, pdb_id _id,
                          pdb_primitive *_pr, graphd_constraint *_con,
                          graphd_read_one_callback *_callback,
                          void *_callback_data);

/* graphd-read-or.c */

int graphd_read_or_state(graphd_request const *_greq,
                         graphd_constraint const *_con,
                         graphd_read_or_map const *_rom);

void graphd_read_or_finish(graphd_request *_greq, graphd_read_or_map *_rom);

int graphd_read_or_initialize(graphd_request *_greq, graphd_constraint *_con,
                              graphd_read_or_map *_rom);

/* graphd-read-set.c */

int graphd_read_set_path(graphd_request *_greq, graphd_constraint *_con,
                         pdb_id _id, cm_buffer *_buf);
void graphd_read_set_context_link(graphd_read_set_context *);
int graphd_read_set_context_suspend(graphd_read_set_context *);
int graphd_read_set_context_unsuspend(graphd_read_set_context *);
void graphd_read_set_free(graphd_read_set_context *);
void graphd_read_set_push(graphd_read_base *_grb, graphd_constraint *_con,
                          pdb_id _parent_id, pdb_primitive const *_parent_pr,
                          graphd_read_set_callback *_callback,
                          void *_callback_data);

void graphd_read_set_resume(graphd_read_set_context *grsc,
                            graphd_read_set_callback *callback,
                            void *callback_data);

/* graphd-read-set-boundary.c */

int graphd_read_set_boundary_check(graphd_read_set_context *_grsc,
                                   bool *_accept_out, bool *_eof_out);

/* graphd-read-set-cursor.c */

int graphd_read_set_cursor_get_value(graphd_read_set_context *grsc,
                                     graphd_value *val);

int graphd_read_set_cursor_get_atom(graphd_constraint *con,
                                    graphd_request *greq, char const *prefix,
                                    pdb_iterator *it, graphd_value *val_out);

int graphd_read_set_cursor_get(graphd_read_set_context *_grsc,
                               graphd_pattern_frame const *_pf,
                               graphd_value *_val);

void graphd_read_set_cursor_clear(graphd_read_set_context *_grsc,
                                  graphd_pattern_frame const *_pf,
                                  graphd_value *_val);

/* graphd-read-set-count.c */

void graphd_read_set_count_get_atom(graphd_read_set_context *gsrc,
                                    graphd_value *val);

int graphd_read_set_count_fast(graphd_read_set_context *_grsc,
                               unsigned long long *_count_out);

/* graphd-read-set-defer.c */

int graphd_read_set_defer_results(graphd_read_set_context *_grsc,
                                  graphd_value **_res_out);

/* graphd-read-set-estimate.c */

int graphd_read_set_estimate_get(graphd_request *_greq, pdb_iterator *_it,
                                 graphd_value *_val_out);

#endif /* GRAPHD_READ_H */
