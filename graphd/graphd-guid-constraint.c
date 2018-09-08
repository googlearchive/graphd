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
#include "graphd/graphd-hash.h"

#include <limits.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

/**
 * @brief Initialize an empty constraint
 * @param guidcon 	constraint to initialize
 */
void graphd_guid_constraint_initialize(graphd_guid_constraint* guidcon) {
  memset(guidcon, 0, sizeof(*guidcon));
}

/*  Translate the GUID constraint next=.... or next~=.... into
 *  constraints on the GUID= set of ids.
 */
static int guidcon_convert_next(graphd_request* greq, graphd_constraint* con,
                                bool is_read) {
  cl_handle* cl = graphd_request_cl(greq);
  pdb_handle* pdb = graphd_request_graphd(greq)->g_pdb;
  graphd_guid_constraint* const vn = &con->con_version_next;
  graphd_guid_set* in = &vn->guidcon_include;
  size_t i;
  int err = 0;

  if (!vn->guidcon_match_valid && !vn->guidcon_include_valid &&
      !vn->guidcon_exclude_valid)
    return 0;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  if (vn->guidcon_match_valid) {
    graphd_guid_set* ma = &vn->guidcon_match;

    /*  Convert the matches into includes.
     */
    do /* for all match set records. */
    {
      if (ma->gs_n == 0) {
        /* For write, that's always true.
         */
        if (is_read) {
          /*  next=null or next ~= null, same
           *  as newest=0 (and, generally, the
           *  default) -- this entry has not
           *  been replaced by another.
           */
          if (!con->con_newest.gencon_valid ||
              con->con_newest.gencon_min == 0) {
            con->con_newest.gencon_valid = true;
            con->con_newest.gencon_min = con->con_newest.gencon_max = 0;
          } else {
            con->con_false = true;
            cl_log(cl, CL_LEVEL_DEBUG, "FALSE: [%s:%d] GUID is newest",
                   __FILE__, __LINE__);
          }
        }
      } else if (!is_read) {
        char buf[GRAPH_GUID_SIZE];

        graphd_request_errprintf(
            greq, 0,
            "SEMANTICS "
            "can't use NEXT~=%s in a write request!",
            graph_guid_to_string(ma->gs_guid, buf, sizeof buf));
        return GRAPHD_ERR_SEMANTICS;
      } else {
        graphd_guid_set gs;

        graphd_guid_set_initialize(&gs);
        for (i = 0; i < ma->gs_n; i++) {
          pdb_id last, n;

          /* How many generations are there
           * of this?
           */
          err = pdb_generation_last_n(pdb, greq->greq_asof, ma->gs_guid + i,
                                      &last, &n);
          if (err == GRAPHD_ERR_NO) continue;
          if (err != 0) goto err;
          if (n <= 1) continue;

          /*  Add all but the first - the fist
           *  one can't be anyone's next!
           */
          err = graphd_guid_set_add_generations(greq, ma->gs_guid + i, 1, n - 1,
                                                &gs);
          if (err != 0) goto err;
        }
        err = graphd_guid_constraint_merge(greq, con, vn, GRAPHD_OP_EQ, &gs);
        if (err != 0) goto err;
      }

    } while ((ma = ma->gs_next) != NULL);

    vn->guidcon_match_valid = false;
  }

  /*  Play off exclude against include.
   */
  if (vn->guidcon_include_valid && vn->guidcon_exclude_valid) {
    if (!graphd_guid_set_subtract(greq, in, &vn->guidcon_exclude)) {
      cl_log(cl, CL_LEVEL_DEBUG,
             "FALSE: [%s:%d]: =/!= subtraction "
             "reduces to null",
             __FILE__, __LINE__);
      con->con_false = true;
    }
    vn->guidcon_exclude_valid = false;
  }

  /*  Turn include into GUIDs.
   */
  if (vn->guidcon_include_valid) {
    if (in->gs_n == 0) {
      /* Same case as with  match.
       */
      if (is_read) {
        /*  next=null or next ~= null, same as newest=0
         *  (and, generally, the default) -- this entry
         *  has not been replaced by another.
         */
        if (!con->con_newest.gencon_valid || con->con_newest.gencon_min == 0) {
          con->con_newest.gencon_valid = true;
          con->con_newest.gencon_min = con->con_newest.gencon_max = 0;
        } else {
          con->con_false = true;
          cl_log(cl, CL_LEVEL_DEBUG, "FALSE: [%s:%d] GUID is newest", __FILE__,
                 __LINE__);
        }
      }
    }

    else if (!is_read) {
      char buf[GRAPH_GUID_SIZE];
      graphd_request_errprintf(
          greq, 0,
          "SEMANTICS "
          "can't use NEXT=%s in a write request!",
          graph_guid_to_string(in->gs_guid, buf, sizeof buf));
      return GRAPHD_ERR_SEMANTICS;
    }

    /*  Translate these into a set of candidate GUIDs.
     *
     *  The caller specifies the NEXT GUID; consequently,
     *  the candidate GUID is the PREVIOUS (gen - 1) of the
     *  specified GUID.
     */
    else {
      graphd_guid_set gs;
      graphd_guid_set_initialize(&gs);

      if (graphd_guid_set_contains_null(in))
        graphd_guid_set_add(greq, &gs, NULL);

      for (i = 0; i < in->gs_n; i++) {
        pdb_id id, gen;
        graph_guid g;

        err = pdb_generation_guid_to_lineage(pdb, in->gs_guid + i, &id, &gen);
        if (err != 0) goto err;

        /*  If the caller-supplied value is the first
         *  generation, there's no result - it has
         *  no predecessor.
         */
        if (gen <= 0) continue;

        err = pdb_generation_nth(pdb, greq->greq_asof, in->gs_guid + i,
                                 false, /* is_newest? No */
                                 gen - 1, NULL, &g);
        if (err != 0) goto err;

        err = graphd_guid_set_add(greq, &gs, &g);
        if (err != 0) goto err;
      }
      if (i >= in->gs_n) {
        err = graphd_guid_constraint_merge(greq, con, &con->con_guid,
                                           GRAPHD_OP_EQ, &gs);
        if (err != 0) goto err;
        vn->guidcon_include_valid = false;
      }
    }
  }

  /* If no "newest" constraint has been specified,
   * set it to newest > 0.
   */
  if (is_read && !con->con_newest.gencon_valid) {
    con->con_newest.gencon_valid = true;
    con->con_newest.gencon_min = 1;
    con->con_newest.gencon_max = (unsigned long)-1;
  }

err:
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", err ? graphd_strerror(err) : "ok");
  return err;
}

/*  Translate the GUID constraint PREVIOUS=.... or PREVIOUS~=.... into
 *  constraints on the GUID= set of ids.
 */
static int guidcon_convert_previous(graphd_request* greq,
                                    graphd_constraint* con, bool is_read) {
  cl_handle* cl = graphd_request_cl(greq);
  pdb_handle* pdb = graphd_request_graphd(greq)->g_pdb;
  graphd_guid_constraint* vp = &con->con_version_previous;
  graphd_guid_set* in = &vp->guidcon_include;
  size_t i;
  int err = 0;

  if (!vp->guidcon_match_valid && !vp->guidcon_include_valid &&
      !vp->guidcon_exclude_valid)
    return 0;

  cl_enter(cl, CL_LEVEL_VERBOSE, "enter");

  if (vp->guidcon_match_valid) {
    graphd_guid_set* ma = &vp->guidcon_match;

    do {
      if (vp->guidcon_include_valid && in->gs_n == 0) {
        /* null include -> does it survive eliminiation?
         */
        if (!graphd_guid_set_contains_null(ma)) {
          con->con_false = true;
          cl_log(cl, CL_LEVEL_DEBUG,
                 "FALSE: [%s:%d] ~= null, "
                 "but =(!null)",
                 __FILE__, __LINE__);
        }
      } else if (ma->gs_n == 0) {
        /*  prev~=null is the same as prev=null
         *  -- this entry must be the original.
         */
        if (!vp->guidcon_include_valid) {
          vp->guidcon_include_valid = true;
          in->gs_n = 0;
        } else if (in->gs_n > 0) {
          if (!graphd_guid_set_contains_null(in)) {
            con->con_false = true;
            cl_log(cl, CL_LEVEL_DEBUG,
                   "FALSE: [%s:%d] ~= null"
                   ", but = (!null)",
                   __FILE__, __LINE__);
          } else {
            in->gs_n = 0;
            in->gs_null = true;
          }
        }
      } else {
        graphd_guid_set gs;
        graphd_guid_set_initialize(&gs);

        /* null expands to null
         */
        if (graphd_guid_set_contains_null(ma))
          (void)graphd_guid_set_add(greq, &gs, NULL);

        /* Expand the MATCH GUIDs into gs.
         */
        for (i = 0; i < ma->gs_n; i++) {
          pdb_id last, n;

          /* How many generations are there?
           */
          err = pdb_generation_last_n(pdb, greq->greq_asof, ma->gs_guid + i,
                                      &last, &n);
          if (err == GRAPHD_ERR_NO) continue;
          if (err != 0) goto err;
          {
            char buf[200];
            cl_log(cl, CL_LEVEL_VERBOSE,
                   "guidcon_convert_previous: got %lu generations for %s",
                   (unsigned long)n,
                   graph_guid_to_string(ma->gs_guid + i, buf, sizeof buf));
          }
          if (is_read && n <= 1) continue;

          err = (is_read ? graphd_guid_set_add_generations(
                               greq, ma->gs_guid + i, 0, n - 1, &gs)
                         : graphd_guid_set_add_generations(
                               greq, ma->gs_guid + i, n - 1, 1, &gs));
          if (err != 0) goto err;
        }
        err = graphd_guid_constraint_merge(greq, con, vp, GRAPHD_OP_EQ, &gs);
        if (err != 0) goto err;
      }
    } while ((ma = ma->gs_next) != NULL);

    vp->guidcon_match_valid = false;
  }

  /*  By now, match has been converted to eq.
   */
  cl_assert(cl, !vp->guidcon_match_valid);

  /*  Play exclude off against include.
   */
  if (vp->guidcon_include_valid && vp->guidcon_exclude_valid) {
    if (!graphd_guid_set_subtract(greq, in, &vp->guidcon_exclude)) {
      cl_log(cl, CL_LEVEL_DEBUG,
             "FALSE: [%s:%d] graphd_guid_constraint_merge: "
             "=/!= subtraction reduces to null",
             __FILE__, __LINE__);
      con->con_false = true;
    }
    vp->guidcon_exclude_valid = false;
  }

  /*  If there is an include set, convert it to a GUID constraint.
   */
  if (vp->guidcon_include_valid) {
    graphd_guid_set gs;

    graphd_guid_set_initialize(&gs);

    if (in->gs_n == 0) {
      /*  previous=null or previous~=null
       *  -- this entry is the original.
       */
      if (!con->con_oldest.gencon_valid || con->con_oldest.gencon_min == 0) {
        con->con_oldest.gencon_valid = true;
        con->con_oldest.gencon_min = con->con_oldest.gencon_max = 0;
      } else {
        con->con_false = true;
        cl_log(cl, CL_LEVEL_DEBUG, "FALSE: [%s:%d] GUID is newest", __FILE__,
               __LINE__);
      }
      return 0;
    }

    if (graphd_guid_set_contains_null(in))
      (void)graphd_guid_set_add(greq, in, NULL);

    /*  Translate these into a set of candidate GUIDs.
     *  Stop if you hit "null" - a prev that includes "null" is
     *  pretty much useless for purposes of restricting the
     *  result set.
     */
    for (i = 0; i < in->gs_n; i++) {
      pdb_id id, gen;
      graph_guid candidate_guid;

      /*  Write case: take the GUID as written.
       */
      if (!is_read) {
        err = graphd_guid_set_add(greq, &gs, vp->guidcon_include.gs_guid + i);
        if (err != 0) goto err;
        continue;
      }

      /*  Read case: Caller tells us the predecessor;
       *  what's that predecessor's successor?
       */
      err = pdb_generation_guid_to_lineage(pdb, in->gs_guid + i, &id, &gen);
      if (err == GRAPHD_ERR_NO) continue;
      if (err != 0) goto err;

      err = pdb_generation_nth(pdb, greq->greq_asof, in->gs_guid + i,
                               false, /* is_newest? No, count from oldest */
                               gen + 1, NULL, &candidate_guid);

      if (err == GRAPHD_ERR_NO) continue;
      if (err != 0) goto err;

      err = graphd_guid_set_add(greq, &gs, &candidate_guid);
      if (err != 0) goto err;
    }

    /*  We've converted everything.  Behave as if someone
     *  had specified that as a GUID constraint.
     */
    if (i >= in->gs_n) {
      err = graphd_guid_constraint_merge(greq, con, &con->con_guid,
                                         GRAPHD_OP_EQ, &gs);
      if (err != 0) goto err;

      vp->guidcon_include_valid = false;
    }

    /*  Otherwise, we *can't* merge into the GUID
     *  constraint, and will simply match.
     */
  }

err:
  cl_leave(cl, CL_LEVEL_VERBOSE, "%s", err ? graphd_strerror(err) : "ok");
  return err;
}

/**
 *  @brief Convert "match" look-ups to "eq"
 *
 *  This prepwork needs to happen without intervening "write"
 *  calls (or otherwise the results may no longer be valid by
 *  the time the request executes!)
 *
 *  @param greq	Request for which the conversion takes place
 *  @param con 	Constraint that's being converted
 *  @param is_read true if this is for a read request
 *
 *  @return 0 on success, otherwise a nonzero error code.
 */
int graphd_guid_constraint_convert(graphd_request* greq, graphd_constraint* con,
                                   bool is_read) {
  int err = 0, lin;
  graphd_constraint* sub;
  graphd_constraint_or* cor;

  /*  Translate next, previous into =()
   */
  if (((con->con_version_previous.guidcon_include_valid ||
        con->con_version_previous.guidcon_match_valid ||
        con->con_version_previous.guidcon_exclude_valid) &&
       (err = guidcon_convert_previous(greq, con, is_read)) != 0) ||
      ((con->con_version_next.guidcon_include_valid ||
        con->con_version_next.guidcon_match_valid ||
        con->con_version_next.guidcon_exclude_valid) &&
       (err = guidcon_convert_next(greq, con, is_read))))

    return err;

  if (!is_read && con->con_guid.guidcon_include_valid &&
      con->con_guid.guidcon_include.gs_n > 1) {
    graphd_request_errprintf(greq, 0,
                             "SEMANTICS "
                             "can't version more than one GUID at once!");
    return GRAPHD_ERR_SEMANTICS;
  }

  /*  Translate ~= into =.
   */
  if (con->con_guid.guidcon_match_valid) {
    graphd_guid_set* gs;

    for (gs = &con->con_guid.guidcon_match; gs != NULL; gs = gs->gs_next) {
      /* Convert the generations in place */
      err = graphd_guid_set_convert_generations(greq, con, /* is_guid? */ true,
                                                gs);
      if (err != 0) return err;

      /* Merge the converted generations into the GUID. */
      err = graphd_guid_constraint_merge(greq, con, &con->con_guid,
                                         GRAPHD_OP_EQ, gs);
      if (err != 0) return err;
    }
    con->con_guid.guidcon_match_valid = false;
  }

  for (lin = 0; lin < PDB_LINKAGE_N; lin++) {
    graphd_guid_set* gs;

    if (!con->con_linkcon[lin].guidcon_match_valid) continue;

    for (gs = &con->con_linkcon[lin].guidcon_match; gs != NULL;
         gs = gs->gs_next) {
      /* Convert the generations in place */
      err = graphd_guid_set_convert_generations(greq, con, /* is_guid? */ false,
                                                gs);
      if (err != 0) return err;

      /* Merge the converted generations into the GUID. */
      err = graphd_guid_constraint_merge(greq, con, con->con_linkcon + lin,
                                         GRAPHD_OP_EQ, gs);
      if (err != 0) return err;
    }
    con->con_linkcon[lin].guidcon_match_valid = false;
  }

  if (is_read) {
    /*  Complete generational constraints.
     *
     *  Before this call, !valid means "default me".
     *  After this call, !valid means "no restrictions."
     */
    if (!con->con_newest.gencon_valid && !con->con_oldest.gencon_valid) {
      cl_cover(graphd_request_cl(greq));

      /* Just the newest. */
      con->con_newest.gencon_valid = 1;
      con->con_newest.gencon_min = 0;
      con->con_newest.gencon_max = 0;
    }

    if (con->con_newest.gencon_valid && con->con_newest.gencon_min == 0 &&
        con->con_newest.gencon_max == ULONG_MAX) {
      /* newest >= 0, ie. don't check */
      con->con_newest.gencon_valid = false;
    }
    if (con->con_oldest.gencon_valid && con->con_oldest.gencon_min == 0 &&
        con->con_oldest.gencon_max == ULONG_MAX) {
      /* oldest >= 0, ie. don't check */
      con->con_oldest.gencon_valid = false;
    }
  }

  /*  Recursively transform "or" branches and subconstraints
   *  in this manner.
   */
  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    err = graphd_guid_constraint_convert(greq, &cor->or_head, is_read);
    if (err != 0) return err;
    if (cor->or_tail != NULL) {
      err = graphd_guid_constraint_convert(greq, cor->or_tail, is_read);
      if (err != 0) return err;
    }
  }

  for (sub = con->con_head; sub != NULL; sub = sub->con_next) {
    /*  If this subconstraint is part of an or-branch, skip it;
     *  we already visited it while visiting the or branch.
     */
    if (sub->con_parent != con) continue;

    err = graphd_guid_constraint_convert(greq, sub, is_read);
    if (err != 0) break;
  }
  con->con_title = NULL;

  return err;
}

/**
 * @brief Are these two guid constraints equal?
 *
 *  False negatives are okay.  In particular,
 *  GUID constraints with the same GUID in the wrong order
 *  are misclassified as unequal.
 *
 * @param cl 	Log through here
 * @param a 	guid constraint
 * @param b 	another guid constraint
 *
 * @return true if they're equal, false otherwise.
 */
bool graphd_guid_constraint_equal(cl_handle* cl,
                                  graphd_guid_constraint const* a,
                                  graphd_guid_constraint const* b) {
  cl_assert(cl, a != NULL);
  cl_assert(cl, b != NULL);

  if (a->guidcon_include_valid != b->guidcon_include_valid ||
      a->guidcon_exclude_valid != b->guidcon_exclude_valid ||
      a->guidcon_match_valid != b->guidcon_match_valid)
    return false;

  if (a->guidcon_include_valid) {
    bool res =
        graphd_guid_set_equal(cl, &a->guidcon_include, &b->guidcon_include);
    if (!res) return res;
  }
  if (a->guidcon_exclude_valid) {
    bool res =
        graphd_guid_set_equal(cl, &a->guidcon_exclude, &b->guidcon_exclude);
    if (!res) return res;
  }
  if (a->guidcon_match_valid) {
    bool res = graphd_guid_set_equal(cl, &a->guidcon_match, &b->guidcon_match);
    if (!res) return res;
  }
  return true;
}

/**
 * @brief Hash a guid constraint
 *
 * @param cl 		Log through here
 * @param guidcon 	guid constraint
 * @param hash_inout 	hash accumulator
 */
void graphd_guid_constraint_hash(cl_handle* const cl,
                                 graphd_guid_constraint const* const guidcon,
                                 unsigned long* const hash_inout) {
  cl_assert(cl, guidcon != NULL);
  cl_assert(cl, hash_inout != NULL);

  GRAPHD_HASH_VALUE(*hash_inout, (guidcon->guidcon_include_valid << 2) |
                                     (guidcon->guidcon_exclude_valid << 1) |
                                     guidcon->guidcon_match_valid);

  if (guidcon->guidcon_include_valid)
    graphd_guid_set_hash(cl, &guidcon->guidcon_include, hash_inout);
  if (guidcon->guidcon_exclude_valid)
    graphd_guid_set_hash(cl, &guidcon->guidcon_exclude, hash_inout);
  if (guidcon->guidcon_match_valid)
    graphd_guid_set_hash(cl, &guidcon->guidcon_match, hash_inout);
}

/**
 * @brief Are these two generational constraints equal?
 *
 * @param cl 	Log through here
 * @param a 	generational constraint
 * @param b 	another generational constraint
 *
 * @return true if they're equal, false otherwise.
 */
bool graphd_guid_constraint_generational_equal(
    cl_handle* const cl, graphd_generational_constraint const* a,
    graphd_generational_constraint const* b) {
  if (a->gencon_valid != b->gencon_valid) return false;

  if (!a->gencon_valid) return true;

  return a->gencon_min == b->gencon_min && a->gencon_max == b->gencon_max;
}

/**
 * @brief Hash a generational constraint.
 *
 * @param cl 		Log through here
 * @param gencon 	generational constraint
 *
 * @return true if they're equal, false otherwise.
 */
void graphd_guid_constraint_generational_hash(
    cl_handle* const cl, graphd_generational_constraint const* const gencon,
    unsigned long* const hash_inout) {
  GRAPHD_HASH_BIT(*hash_inout, gencon->gencon_valid);
  if (gencon->gencon_valid) {
    GRAPHD_HASH_VALUE(*hash_inout, gencon->gencon_min);
    GRAPHD_HASH_VALUE(*hash_inout, gencon->gencon_max);
  }
}

/**
 *  @brief Merge a new GUID constraint into an existing one.
 *
 *   The constraint operators must be EQ, NE, MATCH, or UNSPECIFIED.
 *   Both constraints are allocated on the request heap; not freeing
 *   is okay.
 *
 *   This function can be called with op=GRAPHD_OP_UNSPECIFIED
 *   and gs=NULL to simply merge the exclude and include parts
 *   of the accu together, if the include part has been normalized
 *   to something with GRAPHD_OP_EQ.
 *
 *  @param greq	Request for which all this happens
 *  @param con 	Containing constraint (for con_false marking)
 *  @param accu constraint to merge into
 *  @param op   GRAPHD_OP_UNSPECIFIED or operator of incoming guid set
 *  @param gs   NULL or incoming guidset to merge
 *
 *  @return 0 on success, otherwise a nonzero error code.
 */
int graphd_guid_constraint_merge(graphd_request* greq, graphd_constraint* con,
                                 graphd_guid_constraint* accu,
                                 graphd_operator op, graphd_guid_set* gs) {
  cl_handle* cl = graphd_request_cl(greq);
  int err = 0;

  cl_assert(cl, accu != NULL);

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_guid_constraint_merge: "
         "i=%d(%zu)%s/x=%d(%zu)%s/m=%d(%zu)%s vs %d(%zu)%s",
         accu->guidcon_include_valid, accu->guidcon_include.gs_n,
         accu->guidcon_include.gs_null ? "+null" : "",
         accu->guidcon_exclude_valid, accu->guidcon_exclude.gs_n,
         accu->guidcon_exclude.gs_null ? "+null" : "",
         accu->guidcon_match_valid, accu->guidcon_match.gs_n,
         accu->guidcon_match.gs_null ? "+null" : "", op, gs ? gs->gs_n : 0,
         gs && gs->gs_null ? "+null" : "");

  switch (op) {
    case GRAPHD_OP_MATCH:
      cl_assert(cl, gs != NULL);

      if (accu->guidcon_match_valid) {
        err = graphd_guid_set_intersect(greq, con,
                                        /* postpone resolution? */ true,
                                        &accu->guidcon_match, gs);
        if (err != 0) return err;
      } else {
        graphd_guid_set_move(&accu->guidcon_match, gs);
        accu->guidcon_match_valid = true;
      }
      break;

    case GRAPHD_OP_EQ:
      cl_assert(cl, gs != NULL);
      if (accu->guidcon_include_valid) {
        err = graphd_guid_set_intersect(greq, con,
                                        /* postpone? */ false,
                                        &accu->guidcon_include, gs);
        if (err != 0) return err;
      } else {
        graphd_guid_set_move(&accu->guidcon_include, gs);
        accu->guidcon_include_valid = true;
      }
      break;

    case GRAPHD_OP_NE:
      cl_assert(cl, gs != NULL);

      if (accu->guidcon_include_valid) {
        /*  Just filter them against the include right now --
         *  no need to keep a separate set.
         */
        if (!graphd_guid_set_subtract(greq, &accu->guidcon_include, gs)) {
          cl_log(cl, CL_LEVEL_DEBUG,
                 "FALSE: [%s:%d] "
                 "graphd_guid_constraint_merge: "
                 "=/!= subtraction reduces to null",
                 __FILE__, __LINE__);
          con->con_false = true;
        }
        break;
      }
      if (accu->guidcon_exclude_valid) {
        err = graphd_guid_set_union(greq, &accu->guidcon_exclude, gs);
        if (err != 0) return err;
      } else {
        graphd_guid_set_move(&accu->guidcon_exclude, gs);
        accu->guidcon_exclude_valid = true;
      }
      break;

    case GRAPHD_OP_UNSPECIFIED:
      break;

    default:
      cl_notreached(cl, "unexpected op %d", op);
  }

  /*  EQ(A) & NE(B) = EQ( A - B )
   */
  if (accu->guidcon_include_valid && accu->guidcon_exclude_valid) {
    if (!graphd_guid_set_subtract(greq, &accu->guidcon_include,
                                  &accu->guidcon_exclude)) {
      cl_log(cl, CL_LEVEL_DEBUG,
             "FALSE: [%s:%d] graphd_guid_constraint_merge: "
             "=/!= subtraction reduces to null",
             __FILE__, __LINE__);
      con->con_false = true;
    } else {
      graphd_guid_set_initialize(&accu->guidcon_exclude);
      accu->guidcon_exclude_valid = false;
    }
  }
  return 0;
}

/*  Does this constraint have a single, non-NULL GUID at <linkage>?
 */
bool graphd_guid_constraint_single_linkage(graphd_constraint const* con,
                                           int linkage, graph_guid* guid_out) {
  graphd_guid_constraint const* guidcon = con->con_linkcon + linkage;

  if (guidcon->guidcon_include_valid &&
      guidcon->guidcon_include.gs_next == NULL &&
      guidcon->guidcon_include.gs_n == 1 &&
      !GRAPH_GUID_IS_NULL(*guidcon->guidcon_include.gs_guid) &&
      !guidcon->guidcon_exclude_valid && !guidcon->guidcon_match_valid) {
    if (guid_out != NULL) *guid_out = *guidcon->guidcon_include.gs_guid;

    return true;
  }
  return false;
}

/*  Merge <guid> into the guidcon for <linkage>.  If it wasn't
 *  contained in it to begin with, mark the constraint as false.
 */
int graphd_guid_constraint_intersect_with_guid(graphd_request* greq,
                                               graphd_constraint* con,
                                               graphd_guid_constraint* guidcon,
                                               graph_guid const* guid) {
  int err;
  graphd_guid_set tmp;

  graphd_guid_set_initialize(&tmp);
  err = graphd_guid_set_add(greq, &tmp, guid);
  if (err != 0) return err;

  if (guidcon->guidcon_include_valid) {
    err = graphd_guid_set_intersect(greq, con,
                                    /* do not postpone */ false,
                                    &guidcon->guidcon_include, &tmp);
    if (err != 0) return err;
  } else {
    guidcon->guidcon_include_valid = true;
    guidcon->guidcon_include_annotated = true;
    graphd_guid_set_move(&guidcon->guidcon_include, &tmp);
  }
  return 0;
}
