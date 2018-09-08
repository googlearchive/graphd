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
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sysexits.h>

#include "libgdp/gdp.h"

/**
 * @brief Utility: implement a restore request.
 *
 * 	This is used for both restore and replica requests -
 * 	any sort of incoming record data.
 */
int graphd_restore_create_primitives(graphd_request* greq) {
  cl_handle* cl = graphd_request_cl(greq);
  graphd_handle* g = graphd_request_graphd(greq);
  pdb_primitive pr;
  int err = 0;

  char const *name_s = NULL, *type_s = NULL, *value_s = NULL;

  size_t name_n = 0, type_n = 0, value_n = 0;

  graph_guid guid;
  graph_guid const* prev_guid = NULL;

  unsigned long long i, n;
  unsigned long long third;
  char errbuf[200], buf[200];

  gdp_record* v5;

  cl_log(
      cl, CL_LEVEL_VERBOSE,
      "graphd_restore_create_primitives: session %s, type %d",
      graphd_session_to_string(graphd_request_session(greq), buf, sizeof buf),
      graphd_request_session(greq)->gses_type);

  /*  Special case: starting from zero in a situation other than
   *  an import - throw away the old, existing database, and assume
   *  the character of the incoming stream.
   */
  if (greq->greq_start == 0) {
    graphd_type_initialize(g);

    /*  Starting from zero, and it's not a restore?
     */
    if (greq->greq_end > greq->greq_start) {
      v5 = greq->greq_restore_base;

      if (greq->greq_restore_version == 1) {
        /*  Move the database identifier space
         *  to be different - we'll need a type system
         *  to handle the incoming format.
         */
        err = pdb_restore_avoid_database_id(g->g_pdb, &v5->r_v5_guid);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_restore_avoid_database_id", err,
                       "cannot avoid PDB database ID");
          return err;
        }

        /*  Bootstrap the initial type system.
         */
        err = graphd_type_bootstrap(greq);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_type_bootstrap", err,
                       "error while bootstrapping "
                       "the initial type system");
          return err;
        }
      } else {
        /*  Adopt the incoming database ID as
         *  insertion and compression ID.
         */
        char buf[GRAPH_GUID_SIZE];

        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_restore_create_primitive: "
               "adopting database id %s",
               graph_guid_to_string(&v5->r_v5_guid, buf, sizeof buf));

        /*  Move the database identifier space
         *  to be the same.
         */
        err = pdb_restore_adopt_database_id(g->g_pdb, &v5->r_v5_guid);
        if (err != 0) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_restore_adopt_database_id", err,
                       "error while attempting to "
                       "adopt new database ID");
          return err;
        }
      }
    }
  }

  n = greq->greq_restore_n;
  if (n > 1024 * 64)
    third = n / 3;
  else
    third = 0;
  for (v5 = greq->greq_restore_base, i = 0; i < n; i++, v5++) {
    unsigned char bits = 0;
    unsigned long long serial;

    /*  How much space are we going to need?
     *
     *  There are three dynamic-size elements: (1) name,
     *  (2) value,
     *  and (3) up to four links.
     */

    prev_guid = GRAPH_GUID_IS_NULL(v5->r_v5_prev) ? NULL : &v5->r_v5_prev;

    bits = (v5->r_v5_archival ? PDB_PRIMITIVE_BIT_ARCHIVAL : 0) |
           (v5->r_v5_live ? PDB_PRIMITIVE_BIT_LIVE : 0) |
           (v5->r_v6_txstart ? PDB_PRIMITIVE_BIT_TXSTART : 0) |
           (prev_guid != NULL ? PDB_PRIMITIVE_BIT_PREVIOUS : 0);

    /* Name.
     */
    if ((name_s = v5->r_v5_name.tkn_start) == NULL)
      name_n = 0;
    else {
      cl_assert(cl, name_s <= v5->r_v5_name.tkn_end);
      name_n = (v5->r_v5_name.tkn_end - name_s) + 1;
    }

    /* Type.
     */
    switch (greq->greq_restore_version) {
      case 1:
        /*  If this is version 1, we have a string type,
         *  and need to convert it into the GUID of a
         *  type node, similar to what happens when
         *  writing a record with a string type.
         */
        if ((type_s = v5->r_v1_type.tkn_start) == NULL ||
            (type_n = (v5->r_v1_type.tkn_end - v5->r_v1_type.tkn_start)) == 0)
          GRAPH_GUID_MAKE_NULL(v5->r_v5_typeguid);
        else {
          err = graphd_type_make_name(greq, type_s, type_n, &v5->r_v5_typeguid);
          if (err != 0) {
            cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_type_make_name", err,
                         "can't create type "
                         "\"%.*s\"",
                         (int)type_n, (char const*)type_s);
            return err;
          }
        }

      /* FALL THROUGH */

      case 2:
      case 5:
      case 6:
        break;
    }

    /* Value.
     */
    if ((value_s = v5->r_v5_value.tkn_start) == NULL)
      value_n = 0;
    else {
      cl_assert(cl, value_s <= v5->r_v5_value.tkn_end);
      value_n = (v5->r_v5_value.tkn_end - value_s) + 1;
    }

    serial = GRAPH_GUID_SERIAL(v5->r_v5_guid);
    if (greq->greq_restore_version > 1 && serial < pdb_primitive_n(g->g_pdb)) {
      /* Verify that the primitive being "restored"
       *  matches what is in the database.
       */

      cl_log(cl, CL_LEVEL_SPEW, "We already know this primitive...");

      char const* difference = NULL;
      graph_guid pointer;

      err = pdb_primitive_read(g->g_pdb, &v5->r_v5_guid, &pr);
      if (err) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_primitive_read", err,
                     "Unable to read primitive %llx "
                     "for comparison",
                     GRAPH_GUID_SERIAL(v5->r_v5_guid));
        return err;
      }

      /* guid is implicitly compared by primitive_read */

      if (v5->r_v5_timestamp != pdb_primitive_timestamp_get(&pr))

        difference = "timestamps";

      else if (v5->r_v5_datatype != pdb_primitive_valuetype_get(&pr))

        difference = "valuetypes";

      else if (bits !=
               (pdb_primitive_bits_get(&pr) &
                (PDB_PRIMITIVE_BIT_LIVE | PDB_PRIMITIVE_BIT_ARCHIVAL |
                 PDB_PRIMITIVE_BIT_TXSTART | PDB_PRIMITIVE_BIT_PREVIOUS))) {
        cl_log(cl, CL_LEVEL_FAIL, "bits: %2.2x, primitive: %2.2x", (int)bits,
               (int)pdb_primitive_bits_get(&pr));
        difference = "bits";
      } else if ((name_n != pdb_primitive_name_get_size(&pr)) ||
                 (name_n > 1 &&
                  memcmp(name_s, pdb_primitive_name_get_memory(&pr),
                         name_n - 1)))

        difference = "names";

      else if (value_n != pdb_primitive_value_get_size(&pr) ||
               (value_n && memcmp(value_s, pdb_primitive_value_get_memory(&pr),
                                  value_n - 1)))

        difference = "values";

      else if (!GRAPH_GUID_IS_NULL(v5->r_v5_typeguid) &&
               (pdb_primitive_typeguid_get(&pr, pointer),
                graph_guid_compare(&pointer, &v5->r_v5_typeguid)))

        difference = "typeguids";

      else if (!GRAPH_GUID_IS_NULL(v5->r_v5_scope) &&
               (pdb_primitive_scope_get(&pr, pointer),
                graph_guid_compare(&pointer, &v5->r_v5_scope)))

        difference = "scopes";

      else if (!GRAPH_GUID_IS_NULL(v5->r_v5_left) &&
               (pdb_primitive_left_get(&pr, pointer),
                graph_guid_compare(&pointer, &v5->r_v5_left)))

        difference = "left GUIDs";

      else if (!GRAPH_GUID_IS_NULL(v5->r_v5_right) &&
               (pdb_primitive_right_get(&pr, pointer),
                graph_guid_compare(&pointer, &v5->r_v5_right)))

        difference = "right GUIDs";

      pdb_primitive_finish(g->g_pdb, &pr);

      if (difference != NULL) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_restore_create_primitive primitive "
               "%llx: %s are different",
               GRAPH_GUID_SERIAL(v5->r_v5_guid), difference);
        return GRAPHD_ERR_RESTORE_MISMATCH;
      }
    } else {
      /* Allocate the primitive
       */
      graphd_dateline_expire(g);

      err = pdb_primitive_alloc(
          g->g_pdb, g->g_now, prev_guid, &pr, &guid, v5->r_v5_timestamp,
          v5->r_v5_datatype,
          (v5->r_v5_archival ? PDB_PRIMITIVE_BIT_ARCHIVAL : 0) |
              (v5->r_v5_live ? PDB_PRIMITIVE_BIT_LIVE : 0) |
              (v5->r_v6_txstart ? PDB_PRIMITIVE_BIT_TXSTART : 0),
          name_n, value_n, name_s, value_s, &v5->r_v5_typeguid, &v5->r_v5_right,
          &v5->r_v5_left, &v5->r_v5_scope, &v5->r_v5_guid, errbuf,
          sizeof(errbuf));

      if (err)
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_primitive_alloc", err, "errbuf=%s",
                     errbuf);
      else {
        err = pdb_primitive_alloc_commit(g->g_pdb, prev_guid, &guid, &pr,
                                         errbuf, sizeof errbuf);
        if (err) {
          char buf[GRAPH_GUID_SIZE];

          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_primitive_alloc_commit", err,
                       "unable to index primitive %s",
                       pdb_primitive_to_string(&pr, buf, sizeof buf));
        }
      }

      if (err) {
        char buf[GRAPH_GUID_SIZE];

        graphd_request_errprintf(
            greq, 0,
            "SEMANTICS cannot allocate "
            "primitive %s: %s%s%s",
            graph_guid_to_string(&guid, buf, sizeof buf),
            err == GRAPHD_ERR_NO ? "not found" : graphd_strerror(err),
            *errbuf ? ": " : "", errbuf);
        return err;
      }

      /* Make sure the system timestamp is always ahead
       * of the inserted timestamp.
       */
      if (g->g_now <= v5->r_v5_timestamp) g->g_now = v5->r_v5_timestamp + 1;
    }

    /*  For large restores, do some leftover checkpoint work
     *  every so often.
     */
    if (third > 0 && 0 == (n + 1) % third) {
      err = graphd_checkpoint_work(g);
      if (err && PDB_ERR_MORE != err) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_checkpoint_work", err,
                     "Unable to save indices");
        return err;
      }
    }
  }
  return err;
}

int graphd_restore_checkpoint(cl_handle* cl, graphd_handle* g,
                              graphd_session* gses) {
  int err = pdb_checkpoint_mandatory(g->g_pdb, true);
  if (err && err != PDB_ERR_ALREADY) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_checkpoint_mandatory", err,
                 "Unable to save primitives");
    return err;
  }

  if (!pdb_transactional(g->g_pdb)) {
    /* Call the much-shorter pdb_checkpoint_optional to
     * update the marker file
     */
    pdb_checkpoint_optional(g->g_pdb, 0);
  }

  /* Make sure pdb's indices will eventually get flushed to disk.
   */
  if ((err = graphd_idle_install_checkpoint(g)) != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_idle_install_checkpoint", err,
                 "Unable to request idle callback");
    return err;
  }
  return 0;
}

static int graphd_restore_check(graphd_request* greq) {
  cl_handle* const cl = graphd_request_cl(greq);
  graphd_handle* const g = graphd_request_graphd(greq);
  srv_handle* srv = graphd_request_srv(greq);
  int err = 0;

  if (greq->greq_start > greq->greq_end) {
    graphd_request_errprintf(greq, 0,
                             "SEMANTICS \"restore\" start "
                             "offset %llx exceeds end offset %llx",
                             (unsigned long long)greq->greq_start,
                             (unsigned long long)greq->greq_end);
    cl_log(cl, CL_LEVEL_FAIL, "graphd_restore_check: start %llx > end %llx",
           (unsigned long long)greq->greq_start,
           (unsigned long long)greq->greq_end);
    return GRAPHD_ERR_SEMANTICS;
  }

  if (greq->greq_start > pdb_primitive_n(g->g_pdb)) {
    graphd_request_errprintf(
        greq, 0,
        "SEMANTICS restored records must be contiguous - "
        "cannot restore records above %llu (attempted: %llu)",
        pdb_primitive_n(g->g_pdb), greq->greq_start);
    return GRAPHD_ERR_SEMANTICS;
  }

  if (greq->greq_start == 0) {
    /* We are blowing away the database -- make sure we've no
     * fingers in the istore */
    if (srv_any_sessions_ready_for(srv, (1 << SRV_OUTPUT)))
      return GRAPHD_ERR_MORE;
  }

  return err;
}

/* Prepare to execute a restore command
 */
static int graphd_restore_prepare(graphd_request* greq) {
  cl_handle* cl = graphd_request_cl(greq);
  graphd_handle* g = graphd_request_graphd(greq);
  int err = 0;

  cl_assert(cl, greq->greq_start <= greq->greq_end);

  err = pdb_restore_prepare(g->g_pdb, greq->greq_start);
  if (err)
    cl_log_errno(cl, CL_LEVEL_ERROR, "pdb_restore_prepare", err, "start=%llx",
                 greq->greq_start);
  return err;
}

/* Execute a restore or replica write, and clean up in the
 * (unlikely) event of failure.
 */
static int graphd_restore_execute(graphd_request* greq) {
  graphd_session* const gses = graphd_request_session(greq);
  cl_handle* const cl = gses->gses_cl;
  graphd_handle* const g = gses->gses_graphd;
  srv_handle* const srv = g->g_srv;
  pdb_handle* const pdb = g->g_pdb;
  unsigned long long const horizon = pdb_primitive_n(pdb);
  char const* fn = "";
  int err;

  /*  a crash here is fatal for non-transactional databases
   */
  if (!pdb_transactional(pdb)) srv_shared_set_safe(srv, false);

  err = graphd_restore_create_primitives(greq);
  if (err) {
    if (greq->greq_error_message)
      fn = NULL;
    else
      fn = "graphd_restore_create_primitives";
    goto rollback;
  }

  err = graphd_restore_checkpoint(cl, g, gses);
  if (err) {
    fn = "graphd_restore_checkpoint";
    goto rollback;
  }

  goto safe;

rollback:
  cl_assert(cl, err);
  if (fn)
    cl_log_errno(cl, CL_LEVEL_OPERATOR_ERROR, fn, err,
                 "Unable to execute restore");

  int rollback_err = graphd_checkpoint_rollback(g, horizon);
  if (rollback_err) {
    char bigbuf[1024 * 8];
    char const* req_s;
    int req_n;
    bool incomplete;

    graphd_request_as_string(greq, bigbuf, sizeof bigbuf, &req_s, &req_n,
                             &incomplete);

    cl_log_errno(cl, CL_LEVEL_FATAL, "graphd_checkpoint_rollback", rollback_err,
                 "failed to roll back to horizon=%llx", horizon);

    srv_epitaph_print(gses->gses_ses.ses_srv, EX_GRAPHD_DATABASE,
                      "graphd: failed to roll back changes after "
                      "a restore error: "
                      "session=%s (SID=%lu, RID=%lu), "
                      "error=\"%s\" (%d), "
                      "rollback error=\"%s\" (%d), "
                      "request: %.*s%s",
                      gses->gses_ses.ses_displayname, gses->gses_ses.ses_id,
                      greq->greq_req.req_id, graphd_strerror(err), err,
                      graphd_strerror(rollback_err), rollback_err, (int)req_n,
                      req_s, incomplete ? "..." : "");

    exit(EX_GRAPHD_DATABASE);
  }

safe:
  if (!pdb_transactional(pdb)) srv_shared_set_safe(srv, true);

  return err;
}

/**
 * @brief Execute a "restore" request, once it's been parsed.
 *
 *  The restore may happen in the course of a fresh replica connection,
 *  in the course of a fresh import connection, or via something like gbackup.
 *
 * @param greq  restore request
 * @return 0 on success, a nonzero error code on error.
 */

int graphd_restore(graphd_request* greq) {
  cl_handle* cl = graphd_request_cl(greq);
  graphd_handle* g = graphd_request_graphd(greq);
  graphd_session* gses = graphd_request_session(greq);
  int err;
  pdb_id start, end;

  if ((err = graphd_smp_pause_for_write(greq))) return err;

  err = graphd_defer_write(greq);
  if (err) {
    cl_leave(cl, CL_LEVEL_VERBOSE,
             "refusing to write while "
             "no disk is available: %s",
             strerror(err));
    return err;
  }

  cl_assert(cl, pdb_disk_is_available(g->g_pdb));

  /*  If we're in the middle of delayed index updates, or if
   *  the indices are "too far" behind the istore,
   *  try and get some checkpointing work done.
   */

  if (g->g_checkpoint_state != GRAPHD_CHECKPOINT_CURRENT ||
      pdb_checkpoint_urgent(g->g_pdb)) {
    err = graphd_checkpoint_optional(g);
    if (err && err != PDB_ERR_MORE) {
      cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_checkpoint_optional", err,
                   "graphd_restore: refusing to restore while "
                   "the checkpoint system is broken");

      return err;
    }
  }

  cl_log(cl, CL_LEVEL_DEBUG, "+++ graphd_restore +++");

  /* Check whether the insertions are legitimate.
   */
  err = graphd_restore_check(greq);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_restore_check", err, " ");
    goto err;
  }

  if (g->g_rep_master == gses) {
    /* Replicas are not allowed to replicate from 0 if
     * that would mean throwing out existing data.
     * Too dangerous.
     */
    if (0 == greq->greq_start && 0 != pdb_primitive_n(g->g_pdb)) {
      cl_log(cl, CL_LEVEL_OPERATOR_ERROR,
             "Attempt to replicate an empty master on "
             "a non-empty replica.  Delete database files "
             "manually if this is really your intention");
      err = EFAULT;
      goto err;
    }

    cl_log(cl, CL_LEVEL_INFO, "Replication restore start: %llx end: %llx",
           greq->greq_start, greq->greq_end);
  }

  /* Execute them.
   */

  err = graphd_restore_prepare(greq);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_restore_prepare", err, " ");
    goto err;
  }

  start = pdb_primitive_n(g->g_pdb);
  err = graphd_restore_execute(greq);
  if (err) goto err;
  end = pdb_primitive_n(g->g_pdb);

  err = graphd_replicate_restore(g, start, end);
  if (err) {
    cl_log_errno(cl, CL_LEVEL_ERROR, "graphd_replicate_restore", err,
                 "One or more replica restores failed.");
    err = 0;
  }

err:
  cl_log(cl, CL_LEVEL_DEBUG, "--- graphd_restore%s%s ---", err ? ": " : "",
         err ? graphd_strerror(err) : "");

  return err;
}
