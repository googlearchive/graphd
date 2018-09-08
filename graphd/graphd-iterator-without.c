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

#include <errno.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

GRAPHD_SABOTAGE_DECL;

/*  WITHOUT -- an iterator that subtracts contents of one iterator
 *	from another iterator.
 */

#define GRAPHD_WO_MAGIC 0x08316558
#define GRAPHD_IS_WITHOUT(cl, wo) \
  cl_assert(cl, (wo)->wo_magic == GRAPHD_WO_MAGIC)

#define IS_LIT(s, e, lit)                     \
  ((s) != NULL && e - s == sizeof(lit) - 1 && \
   !strncasecmp(s, lit, sizeof(lit) - 1))

static const pdb_iterator_type without_type;

/**
 * @brief Internal state for an is-a operator.
 */
typedef struct graphd_iterator_without {
  unsigned long wo_magic;

  /**
   * @brief Containing grpahd.
   */
  graphd_handle *wo_graphd;

  /**
   * @brief graphd's pdb
   */
  pdb_handle *wo_pdb;

  /**
   * @brief pdb's cm_handle.  Allocate and free through this.
   */
  cm_handle *wo_cm;

  /**
   * @brief pdb's cl_handle.  Log through this.
   */
  cl_handle *wo_cl;

  /* @brief Cached ID to work on during subiterator calls
   *	in find.
   */
  pdb_id wo_call_id;

  /**
   * @brief Producer and checker.
   */
  pdb_iterator *wo_producer;
  pdb_iterator *wo_checker;
  int (*wo_builtin_check)(struct graphd_iterator_without *, pdb_id id,
                          pdb_budget *inout);

} graphd_iterator_without;

#define cl_leave_suspend(cl, st)                                        \
  cl_leave(cl, CL_LEVEL_VERBOSE, "suspend [%s:%d; state=%d]", __FILE__, \
           __LINE__, (int)(st))

typedef int graphd_iterator_without_builtin_check(graphd_iterator_without *_wo,
                                                  pdb_id _id,
                                                  pdb_budget *_budget_inout);

/*  Return 0 if the primitive has some value, GRAPHD_ERR_NO if it doesn't.
 */
static int without_builtin_any_value(graphd_iterator_without *wo, pdb_id id,
                                     pdb_budget *budget_inout) {
  pdb_primitive pr;
  int err;

  err = pdb_id_read(wo->wo_pdb, id, &pr);
  if (err != 0) {
    cl_log_errno(wo->wo_cl, CL_LEVEL_FAIL, "pdb_id_read", err, "id=%llx",
                 (unsigned long long)id);
    return err;
  }

  err = PDB_PRIMITIVE_HAS_VALUE(&pr) ? 0 : GRAPHD_ERR_NO;
  pdb_primitive_finish(wo->wo_pdb, &pr);

  *budget_inout -= PDB_COST_PRIMITIVE;
  return err;
}

static int without_builtin_from_string(
    graphd_iterator_without_builtin_check **builtin_out, char const *s,
    char const *e) {
  *builtin_out = NULL;

  if (IS_LIT(s, e, "any-value")) {
    *builtin_out = without_builtin_any_value;
    return 0;
  }
  return GRAPHD_ERR_NO;
}

static char const *without_builtin_to_string(
    graphd_iterator_without_builtin_check *builtin) {
  if (builtin == without_builtin_any_value) return "any-value";

  return NULL;
}

static int without_find_loc(pdb_handle *pdb, pdb_iterator *it, pdb_id id_in,
                            pdb_id *id_out, pdb_budget *budget_inout,
                            char const *file, int line) {
  graphd_iterator_without *wo = it->it_theory;
  pdb_budget budget_in = *budget_inout;
  int err;
  char buf[200];

  pdb_rxs_push(pdb, "FIND %p without %llx state=%d [%s:%d]", (void *)it,
               (unsigned long long)id_in, (int)it->it_call_state, file, line);

  if ((err = pdb_iterator_refresh(pdb, it)) != PDB_ERR_ALREADY) {
    if (err == 0) {
      pdb_rxs_push(pdb, "FIND %p without %llx state=%d [%s:%d] redirect",
                   (void *)it, (unsigned long long)id_in,
                   (int)it->it_call_state, file, line);
      pdb_iterator_account_charge_budget(pdb, it, find);

      return pdb_iterator_find_loc(pdb, it, id_in, id_out, budget_inout, file,
                                   line);
    }
    goto return_err;
  }

  switch (it->it_call_state) {
    case 0:
      wo->wo_call_id = id_in;

    /* Fall through */

    case 1:
      err = pdb_iterator_find(pdb, wo->wo_producer, wo->wo_call_id,
                              &wo->wo_call_id, budget_inout);
      if (err != 0) {
        if (err == PDB_ERR_MORE) it->it_call_state = 1;

        goto return_err;
      }

    /* Fall through. */

    case 3:
      while (*budget_inout > 0) {
        err = wo->wo_checker != NULL
                  ? pdb_iterator_check(pdb, wo->wo_checker, wo->wo_call_id,
                                       budget_inout)
                  : (*wo->wo_builtin_check)(wo, wo->wo_call_id, budget_inout);
        if (err != 0) {
          if (err == PDB_ERR_MORE) {
            it->it_call_state = 3;
            goto return_err;
          }
          it->it_call_state = 0;

          /*  Checker says "no" -> we say "yes"
           */
          if (err == GRAPHD_ERR_NO) {
            *id_out = wo->wo_call_id;

            /*  Just so we're not saving
             *  spurious data.
             */
            wo->wo_call_id = PDB_ID_NONE;
            err = 0;
            goto err;
          }

          /*  Unexpected error.
           */
          cl_log_errno(
              wo->wo_cl, CL_LEVEL_FAIL, "check", err, "check=%s id=%llx",
              wo->wo_checker == NULL ? "builtin" : pdb_iterator_to_string(
                                                       pdb, wo->wo_checker, buf,
                                                       sizeof buf),
              (unsigned long long)id_in);
          goto return_err;
        }

        case 2:
          err = pdb_iterator_next(pdb, wo->wo_producer, &wo->wo_call_id,
                                  budget_inout);
          if (err != 0) {
            if (err == PDB_ERR_MORE) it->it_call_state = 2;
            goto return_err;
          }
      }

      /*  When we return, continue at the top of the "while" loop.
       */
      it->it_call_state = 3;
  }
  err = PDB_ERR_MORE;

return_err:
  cl_assert(wo->wo_cl, err != 0);
  if (err != PDB_ERR_MORE) {
    it->it_call_state = 0;
    wo->wo_call_id = PDB_ID_NONE;
  }

err:
  if (err == 0)
    pdb_rxs_pop(pdb, "FIND %p without %llx -> %llx ($%lld)", (void *)it,
                (unsigned long long)id_in, (unsigned long long)*id_out,
                (long long)(budget_in - *budget_inout));
  else if (err == PDB_ERR_MORE)
    pdb_rxs_pop(pdb, "FIND %p without %llx suspend state=%d ($%lld)",
                (void *)it, (unsigned long long)id_in, (int)it->it_call_state,
                (long long)(budget_in - *budget_inout));
  else
    pdb_rxs_pop(pdb, "FIND %p without %llx %s ($%lld)", (void *)it,
                (unsigned long long)id_in,
                (err == PDB_ERR_NO ? "eof" : graphd_strerror(err)),
                (long long)(budget_in - *budget_inout));

  pdb_iterator_account_charge_budget(pdb, it, find);
  return err;
}

static int without_reset(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_without *wo = it->it_theory;
  int err;

  it->it_call_state = 0;

  if (wo->wo_checker != NULL) {
    err = pdb_iterator_reset(pdb, wo->wo_checker);
    if (err != 0) return err;
  }
  return pdb_iterator_reset(pdb, wo->wo_producer);
}

static int without_statistics(pdb_handle *pdb, pdb_iterator *it,
                              pdb_budget *budget_inout) {
  graphd_iterator_without *wo = it->it_theory;
  cl_handle *cl = wo->wo_cl;
  char buf[200];
  int err;
  pdb_budget cc;

  err = pdb_iterator_statistics(pdb, wo->wo_producer, budget_inout);
  if (err != 0) return err;

  if (wo->wo_checker != NULL &&
      pdb_iterator_check_cost_valid(pdb, wo->wo_checker)) {
    err = pdb_iterator_statistics(pdb, wo->wo_checker, budget_inout);
    if (err != 0) return err;
  }

  pdb_iterator_statistics_copy(pdb, it, wo->wo_producer);

  if (wo->wo_checker != NULL)
    cc = pdb_iterator_check_cost(pdb, wo->wo_checker);
  else
    cc = PDB_COST_PRIMITIVE;

  pdb_iterator_check_cost_set(
      pdb, it, pdb_iterator_check_cost(pdb, wo->wo_producer) + cc);

  pdb_iterator_next_cost_set(pdb, it,
                             pdb_iterator_next_cost(pdb, wo->wo_producer) + cc);

  pdb_iterator_find_cost_set(pdb, it,
                             pdb_iterator_find_cost(pdb, wo->wo_producer) + cc);

  cl_log(cl, CL_LEVEL_VERBOSE | PDB_FACILITY_ITERATOR,
         "PDB STAT for %s: n=%llu cc=%llu, "
         "nc=%llu; fc=%llu",
         pdb_iterator_to_string(pdb, it, buf, sizeof buf),
         (unsigned long long)pdb_iterator_n(pdb, it),
         (unsigned long long)pdb_iterator_check_cost(pdb, it),
         (unsigned long long)pdb_iterator_next_cost(pdb, it),
         (unsigned long long)pdb_iterator_find_cost(pdb, it));

  return 0;
}

static int without_check(pdb_handle *pdb, pdb_iterator *it, pdb_id check_id,
                         pdb_budget *budget_inout) {
  graphd_iterator_without *const wo = it->it_theory;
  cl_handle *const cl = wo->wo_cl;
  pdb_budget const budget_in = *budget_inout;
  char buf[200];
  int err = 0;

  pdb_rxs_push(pdb, "CHECK %p without %llx state=%d", (void *)it,
               (unsigned long long)check_id, (int)it->it_call_state);
  if (it->it_call_state == 0) {
    err =
        (wo->wo_checker == NULL
             ? (*wo->wo_builtin_check)(wo, check_id, budget_inout)
             : pdb_iterator_check(pdb, wo->wo_checker, check_id, budget_inout));
    if (err == 0) {
      err = GRAPHD_ERR_NO;
      goto err;
    }

    if (err != GRAPHD_ERR_NO) {
      if (err != PDB_ERR_MORE)
        cl_log_errno(
            cl, CL_LEVEL_FAIL, "pdb_iterator_check", err,
            "wo_check=%s; id=%llx",
            wo->wo_checker == NULL
                ? "builtin"
                : pdb_iterator_to_string(pdb, wo->wo_checker, buf, sizeof buf),
            (unsigned long long)check_id);
      goto err;
    }
  }

  err = pdb_iterator_check(pdb, wo->wo_producer, check_id, budget_inout);
  if (err == PDB_ERR_MORE)
    it->it_call_state = 1;
  else
    it->it_call_state = 0;

err:
  if (err != PDB_ERR_MORE)
    pdb_rxs_pop(
        pdb, "CHECK %p without %llx %s ($%lld)", (void *)it,
        (unsigned long long)check_id,
        (err == PDB_ERR_NO ? "eof" : (err ? "ok" : graphd_strerror(err))),
        (long long)(budget_in - *budget_inout));
  else
    pdb_rxs_pop(pdb, "CHECK %p without %llx suspended state=%d ($%lld)",
                (void *)it, (unsigned long long)check_id,
                (int)it->it_call_state, (long long)(budget_in - *budget_inout));

  pdb_iterator_account_charge_budget(pdb, it, check);
  return err;
}

static int without_next_loc(pdb_handle *pdb, pdb_iterator *it, pdb_id *id_out,
                            pdb_budget *budget_inout, char const *file,
                            int line) {
  int err = 0;
  graphd_iterator_without *const wo = it->it_theory;
  pdb_budget budget_in = *budget_inout;

  pdb_rxs_push(pdb, "NEXT %p without state=%d", (void *)it,
               (int)it->it_call_state);
  for (;;) {
    if (it->it_call_state == 0) {
      err = pdb_iterator_next(pdb, wo->wo_producer, &wo->wo_call_id,
                              budget_inout);

      if (err != 0) break;

      it->it_call_state = 1;
    }

    if (wo->wo_checker != NULL)
      err =
          pdb_iterator_check(pdb, wo->wo_checker, wo->wo_call_id, budget_inout);
    else
      err = (*wo->wo_builtin_check)(wo, wo->wo_call_id, budget_inout);

    if (err == PDB_ERR_MORE) {
      it->it_call_state = 1;
      break;
    }
    it->it_call_state = 0;
    if (err != 0) {
      if (err == GRAPHD_ERR_NO) {
        /*  Check says no, we say yes.
         */
        *id_out = wo->wo_call_id;
        wo->wo_call_id = PDB_ID_NONE;

        err = 0;
        pdb_rxs_log(pdb, "NEXT %p without %llx ($%lld)", (void *)it,
                    (unsigned long long)*id_out,
                    (long long)(budget_in - *budget_inout));
        goto err;
      }

      /*  Unexpected error.
       */
      cl_log_errno(wo->wo_cl, CL_LEVEL_FAIL, "wo->wo_builtin_check", err,
                   "id=%llx [%s:%d]", (unsigned long long)wo->wo_call_id, file,
                   line);
      break;
    }
    if (GRAPHD_SABOTAGE(wo->wo_graphd, *budget_inout < 0)) {
      err = PDB_ERR_MORE;
      break;
    }
  }

  cl_assert(wo->wo_cl, err != 0);
  if (err != PDB_ERR_MORE) {
    wo->wo_call_id = PDB_ID_NONE;
    it->it_call_state = 0;

    pdb_rxs_pop(pdb, "NEXT %p without %s ($%lld)", (void *)it,
                (err == PDB_ERR_NO ? "eof" : graphd_strerror(err)),
                (long long)(budget_in - *budget_inout));
  } else
    pdb_rxs_pop(pdb, "NEXT %p without suspended state=%d ($%lld)", (void *)it,
                (int)it->it_call_state, (long long)(budget_in - *budget_inout));

err:
  pdb_iterator_account_charge_budget(pdb, it, next);
  return err;
}

/*
 * wo:[~]LOW[-HIGH]:LINKAGE[+TYPE]<-(SUBSET)
 *	/ RESUMEID SOURCEID / [STATISTICS]:SUBSTATE
 */
static int without_freeze(pdb_handle *pdb, pdb_iterator *it, unsigned int flags,
                          cm_buffer *buf) {
  graphd_iterator_without *wo = it->it_theory;
  int err = 0;
  char const *sep = "";

  if (flags & PDB_ITERATOR_FREEZE_SET) {
    err = cm_buffer_add_string(buf, "without:");
    if (err != 0) return err;

    err = graphd_iterator_util_freeze_subiterator(pdb, wo->wo_producer,
                                                  PDB_ITERATOR_FREEZE_SET, buf);
    if (err != 0) return err;

    if (wo->wo_checker != NULL)
      err = graphd_iterator_util_freeze_subiterator(
          pdb, wo->wo_checker, PDB_ITERATOR_FREEZE_SET, buf);
    else
      err = cm_buffer_sprintf(buf, "#(%s)",
                              without_builtin_to_string(wo->wo_builtin_check));
    if (err != 0) return err;

    err = pdb_iterator_freeze_account(pdb, buf, it);
    if (err != 0) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_POSITION) {
    err = cm_buffer_sprintf(buf, "%s", sep);
    if (err != 0) return err;

    err = pdb_iterator_freeze(pdb, wo->wo_producer,
                              PDB_ITERATOR_FREEZE_POSITION, buf);
    if (err != 0) return err;

    sep = "/";
  }
  if (flags & PDB_ITERATOR_FREEZE_STATE) {
    char idbuf[200];

    err = cm_buffer_sprintf(
        buf, "%s%d:%s:", sep, it->it_call_state,
        pdb_id_to_string(pdb, wo->wo_call_id, idbuf, sizeof idbuf));
    if (err != 0) return err;

    err = graphd_iterator_util_freeze_subiterator(
        pdb, wo->wo_producer, PDB_ITERATOR_FREEZE_STATE, buf);
    if (err != 0) return err;

    err = graphd_iterator_util_freeze_subiterator(
        pdb, wo->wo_checker /* sic */,
        PDB_ITERATOR_FREEZE_POSITION | PDB_ITERATOR_FREEZE_STATE, buf);
    if (err != 0) return err;
  }
  return err;
}

static int without_clone(pdb_handle *pdb, pdb_iterator *it,
                         pdb_iterator **it_out) {
  pdb_iterator *it_orig = it->it_original;
  graphd_iterator_without *wo = it->it_theory;
  cm_handle *cm = wo->wo_cm;
  graphd_iterator_without *wo_out;
  pdb_iterator *checker_clone, *producer_clone;
  int err;

  PDB_IS_ITERATOR(wo->wo_cl, it);
  GRAPHD_IS_WITHOUT(pdb_log(pdb), wo);

  if (wo->wo_checker == NULL)
    checker_clone = NULL;
  else {
    err = pdb_iterator_clone(pdb, wo->wo_checker, &checker_clone);
    if (err != 0) return err;
  }

  err = pdb_iterator_clone(pdb, wo->wo_producer, &producer_clone);
  if (err != 0) return err;

  *it_out = NULL;
  if ((wo_out = cm_malcpy(cm, wo, sizeof(*wo))) == NULL) {
    return errno ? errno : ENOMEM;
  }

  wo_out->wo_checker = checker_clone;
  wo_out->wo_producer = producer_clone;

  if ((err = pdb_iterator_make_clone(pdb, it_orig, it_out)) != 0) {
    pdb_iterator_destroy(pdb, &wo_out->wo_checker);
    pdb_iterator_destroy(pdb, &wo_out->wo_producer);
    cm_free(wo->wo_cm, wo_out);

    return err;
  }
  (*it_out)->it_theory = wo_out;
  if (!pdb_iterator_has_position(pdb, it)) {
    if ((err = pdb_iterator_reset(pdb, *it_out)) != 0) {
      pdb_iterator_destroy(pdb, it_out);
      return err;
    }
  }
  return 0;
}

static void without_finish(pdb_handle *pdb, pdb_iterator *it) {
  graphd_iterator_without *wo = it->it_theory;

  if (wo != NULL) {
    cl_cover(wo->wo_cl);

    pdb_iterator_destroy(pdb, &wo->wo_checker);
    pdb_iterator_destroy(pdb, &wo->wo_producer);

    cm_free(wo->wo_cm, it->it_displayname);
    it->it_displayname = NULL;

    cm_free(wo->wo_cm, wo);
    it->it_theory = NULL;
  }
  it->it_type = NULL;
  it->it_magic = 0;
}

static char const *without_to_string(pdb_handle *pdb, pdb_iterator *it,
                                     char *buf, size_t size) {
  graphd_iterator_without *wo = it->it_theory;
  char prod[200];
  char check[200];

  snprintf(buf, size, "%s without %s",
           pdb_iterator_to_string(pdb, wo->wo_producer, prod, sizeof prod),
           (wo->wo_checker != NULL ? pdb_iterator_to_string(pdb, wo->wo_checker,
                                                            check, sizeof check)
                                   : "builtin"));
  return buf;
}

/**
 * @brief Will this iterator ever return a value beyond this one?
 *
 * @param graphd	module handle
 * @param it		iterator we're asking about
 * @param s		start of comparison value
 * @param e		end of comparison value
 * @param beyond_out	out: true if we're safely beyond this value.
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int without_beyond(pdb_handle *pdb, pdb_iterator *it, char const *s,
                          char const *e, bool *beyond_out) {
  graphd_iterator_without *wo = it->it_theory;
  return pdb_iterator_beyond(pdb, wo->wo_producer, s, e, beyond_out);
}

static int without_primitive_summary(pdb_handle *pdb, pdb_iterator *it,
                                     pdb_primitive_summary *psum_out) {
  int err;
  graphd_iterator_without *wo = it->it_theory;

  /*  Defer to the original.  It may have a different type.
   */
  err = pdb_iterator_primitive_summary(pdb, wo->wo_producer, psum_out);
  if (err != 0) return err;

  psum_out->psum_complete = false;
  return 0;
}

static const pdb_iterator_type without_type = {
    "without",
    without_finish,
    without_reset,
    without_clone,
    without_freeze,
    without_to_string,

    without_next_loc,
    without_find_loc,
    without_check,
    without_statistics,

    NULL,                      /* idarry */
    without_primitive_summary, /* primitive-summary */
    without_beyond,
    NULL, /* range estimate */
    NULL, /* restrict */

    NULL, /* suspend */
    NULL  /* unsuspend */
};

/**
 * @brief Assemble a "without" iterator structure.
 *
 * @param graphd	server for whom we're doing this
 * @param linkage	linkage that the subiterator results point with.
 * @param sub		pointer to subiterator.  A successful call
 *			zeroes out the pointer and takes possession of
 *			the pointed-to iterator.
 * @param low		low limit of the results (included), or
 *			PDB_ITERATOR_LOW_ANY
 * @param high		high limit of the results (wo included),
 *			or PDB_ITERATOR_HIGH_ANY
 * @param forward	go from low to high?
 * @param optimize	should I try to optimize this?
 * @param it_out	Assign the new construct to this.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
static int without_make(graphd_request *greq, pdb_iterator **producer,
                        pdb_iterator **checker,
                        int (*builtin_check)(graphd_iterator_without *, pdb_id,
                                             pdb_budget *),
                        pdb_iterator **it_out) {
  graphd_handle *graphd = graphd_request_graphd(greq);
  pdb_handle *pdb = graphd->g_pdb;
  cl_handle *cl = pdb_log(pdb);
  cm_handle *cm = pdb_mem(pdb);
  graphd_iterator_without *wo;

  if (pdb_iterator_null_is_instance(pdb, *producer) ||
      (checker != NULL && *checker != NULL &&
       pdb_iterator_null_is_instance(pdb, *checker))) {
    cl_log(cl, CL_LEVEL_VERBOSE, "without_make: shortcut: null");

    *it_out = *producer;
    *producer = NULL;

    return 0;
  }

  if ((wo = cm_zalloc(cm, sizeof(*wo))) == NULL ||
      (*it_out = cm_malloc(cm, sizeof(**it_out))) == NULL) {
    int err = errno ? errno : ENOMEM;
    if (wo != NULL) cm_free(cm, wo);

    cl_log_errno(cl, CL_LEVEL_VERBOSE, "cm_malloc", err,
                 "failed to allocate wo-iterator");
    return err;
  }
  wo->wo_magic = GRAPHD_WO_MAGIC;
  wo->wo_graphd = graphd;
  wo->wo_pdb = graphd->g_pdb;
  wo->wo_cl = cl;
  wo->wo_cm = cm;
  wo->wo_builtin_check = builtin_check;

  if (checker != NULL) {
    wo->wo_checker = *checker;
    *checker = NULL;
  } else {
    wo->wo_checker = NULL;
  }

  wo->wo_producer = *producer;
  *producer = NULL;

  pdb_iterator_make(graphd->g_pdb, *it_out, wo->wo_producer->it_low,
                    wo->wo_producer->it_high,
                    pdb_iterator_forward(pdb, wo->wo_producer));

  pdb_iterator_statistics_copy(graphd->g_pdb, *it_out, wo->wo_producer);

  if (pdb_iterator_statistics_done(pdb, wo->wo_producer)) {
    if (wo->wo_checker == NULL ||
        pdb_iterator_check_cost_valid(pdb, wo->wo_checker)) {
      pdb_budget cc = wo->wo_checker != NULL
                          ? pdb_iterator_check_cost(pdb, wo->wo_checker)
                          : PDB_COST_PRIMITIVE;
      (*it_out)->it_check_cost += cc;
      (*it_out)->it_next_cost += cc;
      (*it_out)->it_find_cost += cc;
    } else {
      (*it_out)->it_check_cost_valid = false;
      (*it_out)->it_next_cost_valid = false;
      (*it_out)->it_find_cost_valid = false;
    }
  }
  (*it_out)->it_theory = wo;
  (*it_out)->it_type = &without_type;

  GRAPHD_IS_WITHOUT(cl, wo);
  return 0;
}

/**
 * @brief Create a "without" iterator structure.
 *
 * @param graphd	server for whom we're doing this
 * @param linkage	linkage that the subiterator results point with.
 * @param sub		pointer to subiterator.  A successful call
 *			zeroes out the pointer and takes possession of
 *			the pointed-to iterator.
 * @param low		low limit of the results (included), or
 *			PDB_ITERATOR_LOW_ANY
 * @param high		high limit of the results (wo included),
 *			or PDB_ITERATOR_HIGH_ANY
 * @param forward	sort from low to high, if sorting should happen?
 * @param it_out	Assign the new construct to this.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
int graphd_iterator_without_create(graphd_request *greq,
                                   pdb_iterator **producer,
                                   pdb_iterator **checker,
                                   pdb_iterator **it_out) {
  return without_make(greq, producer, checker, NULL, it_out);
}

/**
 * @brief Reconstitute a frozen without-iterator
 *
 * @param graphd	module handle
 * @param s		beginning of stored form
 * @param e		pointer just past the end of stored form
 * @param forward	no ~ before the name?
 * @param it_out	rebuild the iterator here.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_iterator_without_thaw(graphd_handle *graphd,
                                 pdb_iterator_text const *pit,
                                 pdb_iterator_base *pib,
                                 graphd_iterator_hint hint,
                                 cl_loglevel loglevel, pdb_iterator **it_out) {
  pdb_handle *pdb = graphd->g_pdb;
  pdb_iterator *checker = NULL, *producer = NULL;
  cl_handle *cl = pdb_log(pdb);
  int err, call_state = 0;
  pdb_id call_id = PDB_ID_NONE;
  bool call_unchanged = true;
  char const *e, *s;
  char const *state_s, *state_e;
  pdb_iterator_text subpit1, subpit2;
  graphd_request *greq;
  graphd_iterator_without *wo;
  pdb_iterator_account *acc;
  int (*builtin_check)(graphd_iterator_without *, pdb_id, pdb_budget *) = NULL;

  /*
   * SET      := (SET1)(SET2) or (SET1)#(BUILTIN)
   * POSITION := POS1
   * STATE    := call-state:id:(STATE1)(STATE2)
   *		or call-state:id:(STATE1)-
   */

  greq = pdb_iterator_base_lookup(graphd->g_pdb, pib, "graphd.request");
  if (greq == NULL) {
    err = errno ? errno : EINVAL;
    cl_log_errno(cl, loglevel, "graphd_iterator_thaw", err,
                 "failed to look up request context");
    goto err;
  }

  memset(&subpit1, 0, sizeof subpit1);
  memset(&subpit2, 0, sizeof subpit2);

  /*  SET
   */
  s = pit->pit_set_s;
  e = pit->pit_set_e;
  cl_assert(cl, s != NULL && e != NULL);

  err = pdb_iterator_util_thaw(pdb, &s, e, "%{(bytes)}", &subpit1.pit_set_s,
                               &subpit1.pit_set_e);
  if (err != 0) {
    cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                 "could not thaw set");
    return err;
  }

  if (*s == '#') {
    char const *builtin_s, *builtin_e;
    s++;
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{(bytes)}", &builtin_s,
                                 &builtin_e);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                   "could not thaw builtin name");
      return err;
    }
    err = without_builtin_from_string(&builtin_check, builtin_s, builtin_e);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "without_builtin_from_string", err,
                   "could not scan builtin");
      return err;
    }
  } else {
    err = pdb_iterator_util_thaw(pdb, &s, e, "%{(bytes)}", &subpit2.pit_set_s,
                                 &subpit2.pit_set_e);
    if (err != 0) {
      cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                   "could not thaw subpit2 set");
      return err;
    }
  }
  err = pdb_iterator_util_thaw(pdb, &s, e, "%{account}%{extensions}", pib, &acc,
                               (pdb_iterator_property *)NULL);
  if (err != 0) {
    cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                 "could not thaw set");
    return err;
  }

  /* POSITION
   */
  subpit1.pit_position_s = pit->pit_position_s;
  subpit1.pit_position_e = pit->pit_position_e;

  /* STATE - CALL-STATE:ID:[OPT]PRODUCER_STATE CHECKER_POS/STATE
   *     or  CALL-STATE:ID:[OPT]PRODUCER_STATE -
   *     (for built-in checkers).
   */
  state_s = pit->pit_state_s;
  state_e = pit->pit_state_e;
  if (state_s != NULL && state_s < state_e) {
    err = pdb_iterator_util_thaw(
        pdb, &state_s, state_e, "%d:%{forward}%{id}:%{extensions}%{(bytes)}",
        &call_state, &call_unchanged, &call_id, (pdb_iterator_property *)NULL,
        &subpit1.pit_state_s, &subpit1.pit_state_e);
    if (err != 0) {
      /*  Allow for errors during decode; this may
       *  be a cursor from the previous release.
       */
      cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                   "could not thaw state");

      call_state = 0;
      call_id = PDB_ID_NONE;
      call_unchanged = true;
      subpit1.pit_state_s = subpit1.pit_state_e = NULL;
    } else if (builtin_check == NULL) {
      err = pdb_iterator_util_thaw(pdb, &state_s, state_e,
                                   "%{(position/state)}", &subpit2);
      if (err != 0) {
        cl_log_errno(cl, loglevel, "pdb_iterator_util_thaw", err,
                     "could not thaw "
                     "checker position/state");
        return err;
      }
    } else {
      if (state_s < state_e && *state_s == '-') state_s++;
    }
  }

  err =
      graphd_iterator_thaw(graphd, &subpit1, pib, 0, loglevel, &producer, NULL);
  if (err != 0) return err;

  if (builtin_check == NULL) {
    err = graphd_iterator_thaw(graphd, &subpit2, pib, 0, loglevel, &checker,
                               NULL);
    if (err != 0) {
      pdb_iterator_destroy(pdb, &producer);
      return err;
    }
  }
  err = without_make(greq, &producer, &checker, builtin_check, it_out);

  pdb_iterator_destroy(graphd->g_pdb, &producer);
  pdb_iterator_destroy(graphd->g_pdb, &checker);

  if (err != 0) {
    cl_log_errno(cl, loglevel, "without_make", err, "unexpected error");
    goto err;
  }

  pdb_iterator_account_set(pdb, *it_out, acc);

  /* Restore the local state.  If we didn't get it,
   * it has the default values.
   */
  (*it_out)->it_call_state = call_state;
  wo = (graphd_iterator_without *)(*it_out)->it_theory;
  wo->wo_call_id = call_id;

  return 0;

err:
  pdb_iterator_destroy(pdb, it_out);
  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_iterator_without_thaw: error %s",
         graphd_strerror(err));
  return err;
}

/**
 * @brief Is this a without-iterator?
 *
 * @param pdb		module handle
 * @param it		iterator the caller is asking about
 * @param linkage_out	what's the connection to the subiterator?
 * @param sub_out	what's the subiterator?
 *
 * @return true if this is a without iterator.
 */
bool graphd_iterator_without_is_instance(pdb_handle *pdb, pdb_iterator *it) {
  return it->it_type == &without_type;
}

/**
 * @brief Create a "without" iterator structure.
 *
 * @param greq		request for whom we're doing this
 * @param producer	iterator that produces candidates
 * @param it_out	Assign the new construct to this.
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
int graphd_iterator_without_any_value_create(graphd_request *greq,
                                             pdb_iterator **producer,
                                             pdb_iterator **it_out) {
  return without_make(greq, producer, NULL, without_builtin_any_value, it_out);
}
