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

#include "libaddb/addb.h"

/* (One hundred thousand.)  When we're this many or more
 *  behind, start talking about it.
 */
#define PDB_CHECKPOINT_URGENT_DEFICIT_MIN 100000

/* (Five hundred thousand.)  When we're more than this many
 *  behind, do something.
 */
#define PDB_CHECKPOINT_URGENT_DEFICIT_MAX 500000

static char const* const pdb_checkpoint_stage_names[] = {
    "0-START",         "1-FINISH_BACKUP", "2-SYNC_BACKUP",  "3-SYNC_DIRECTORY",
    "4-START_WRITES",  "5-FINISH_WRITES", "6-START_MARKER", "7-FINISH_MARKER",
    "8-REMOVE_BACKUP", "9-DONE"};

pdb_id pdb_checkpoint_id_on_disk(pdb_handle* p) {
  if (p == NULL) return 0;

  return p->pdb_id_on_disk;
}

/**
 * @brief Checkpoint the internal database state.
 *
 *  This call is invoked before replying "OK" to a write request.
 *  It transitions the essential disk state from "none of the written
 *  primitives are present" to "all of the written primitives are present".
 *
 *  The "essential" disk state is the state that's difficult to
 *  recover if lost - the primitives themselves and their generational
 *  information.
 *
 * @param p	opaque module handle
 * @return 0 on success, otherwise a nonzero error code.
 *
 * We need to call addb_istore_checkpoint regardless of the
 * state of pcf_sync for two reasons:
 * 1. It updates the marker file and that's kinda important
 * 2. We may have been running with pcb_sync on at some point
 * in the past and we still need to reap (pthread_join)
 * any threads we may have created.
 */
int pdb_checkpoint_mandatory(pdb_handle* p, bool block) {
  int err = 0;

  /*  During an emergency shutdown, this may be
   *  called without an initialized primitive database;
   *  in that case, do nothing.
   */
  if (!p->pdb_primitive) return 0;

  cl_enter(p->pdb_cl, CL_LEVEL_SPEW, "%s, %s", block ? "block" : "non-blocking",
           p->pdb_cf.pcf_sync ? "syncing" : "non-syncing");

  block = block && p->pdb_cf.pcf_sync;
  err = addb_istore_checkpoint(p->pdb_primitive, p->pdb_cf.pcf_sync, block);

  cl_assert(p->pdb_cl, !block || (err != ADDB_ERR_MORE));

  /*  Remember that this ID made it to disk, so
   *  we can avoid redundant flushes later.
   */
  if (err == 0) p->pdb_id_on_disk = addb_istore_marker_next(p->pdb_primitive);

  cl_leave(p->pdb_cl, CL_LEVEL_SPEW, "%s", err ? strerror(err) : "ok");
  return err;
}

/**
 * @brief Checkpoint non-essential index data.
 *
 *  Every once in a while, the indices need to be flushed to disk.
 *  Since the flushing process is non-blocking, we flush all
 *  indices simultaneously.  Work will continue while the disk
 *  is working.
 *
 *  Checkpoints always start with the primitive store is in
 *  a well-defined state.  When the checkpoint is finished,
 *  that well defined state will become the new rollback horizon
 *  for the index.
 *
 * @param p		opaque database handle
 * @param deadline	0 or deadline after which to stop running.
 *
 * @return 0 on success, a nonzero error code on error.
 * @return PDB_ERR_MORE if no more work can be done without waiting for IO
 */

int pdb_checkpoint_optional(pdb_handle* pdb, pdb_msclock_t deadline) {
  pdb_checkpoint_stage const start_stage = pdb->pdb_indices[0].ii_stage;
  pdb_checkpoint_stage stage = start_stage;
  bool wouldblock = false;
  unsigned long long deficit;
  int err;
  int i;

  /*  In an emergency, this function may be called without
   *  a fully loaded database.
   *  If we didn't get a database, don't run.
   */
  if (pdb->pdb_primitive == NULL) return 0;

  deficit = pdb_checkpoint_deficit(pdb);
  if (0 == deficit && PDB_CKS_START == stage) return 0;

  cl_enter(pdb->pdb_cl, CL_LEVEL_DEBUG, "deadline=%llu, deficit=%llu%s",
           (unsigned long long)deadline, (unsigned long long)deficit,
           deadline ? "" : " (BLOCK)");

  if (pdb_checkpoint_urgent(pdb)) {
    if (deficit > PDB_CHECKPOINT_URGENT_DEFICIT_MAX) {
      cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
             "pdb_checkpoint_optional: deficit=%llu exceeds"
             " %llu primitives, ignoring deadline",
             deficit, (unsigned long long)PDB_CHECKPOINT_URGENT_DEFICIT_MAX);
      deadline = 0;
    } else if (!pdb->pdb_deficit_exceeded) {
      pdb->pdb_deficit_exceeded = true;
      cl_log(pdb->pdb_cl, CL_LEVEL_INFO,
             "pdb_checkpoint_optional: deficit=%llu exceeds"
             " %llu primitives",
             deficit, (unsigned long long)PDB_CHECKPOINT_URGENT_DEFICIT_MIN);
    }
  }

  /*  Not our first time?
   */
  if (stage != PDB_CKS_START) {
    time_t const now = time(0);
    time_t const delta_t = now - pdb->pdb_started_checkpoint;

    if (delta_t > 60) {
      if (delta_t > 600) {
        cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
               "pdb_checkpoint_optional: STALLED checkpoint "
               "delta_t=%ld, stage=%s horizon=%llx",
               (long)delta_t, pdb_checkpoint_stage_names[stage],
               (unsigned long long)addb_istore_horizon(pdb->pdb_primitive));
      } else {
        cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
               "pdb_checkpoint_optional: slow checkpoint "
               "delta_t=%ld, stage=%s horizon=%llx",
               (long)delta_t, pdb_checkpoint_stage_names[stage],
               (unsigned long long)addb_istore_horizon(pdb->pdb_primitive));
      }
    }
  }

  for (; stage < PDB_CKS_N; stage++) {
    switch (stage) {
      case PDB_CKS_START: {
        addb_istore_id const old_horizon =
            addb_istore_horizon(pdb->pdb_primitive);
        addb_istore_id const new_horizon =
            addb_istore_next_id(pdb->pdb_primitive);

        if (old_horizon == new_horizon) {
          cl_leave(pdb->pdb_cl, CL_LEVEL_DEBUG, "already up to date");
          return 0; /* nothing to do */
        }

        /* We're starting a checkpoint.  Make sure that everyone is
         * on the bus.  The next id will become the horizon that we're
         * working on synchronizing the indices to.
         */

        pdb->pdb_started_checkpoint = time(0);
        pdb->pdb_active_checkpoint_sync = pdb->pdb_cf.pcf_sync;

        for (i = 0; i < PDB_INDEX_N; i++)
          cl_assert(pdb->pdb_cl, PDB_CKS_START == pdb->pdb_indices[i].ii_stage);

        pdb->pdb_new_index_horizon = new_horizon;

        cl_log(
            pdb->pdb_cl, CL_LEVEL_DEBUG,
            "pdb_checkpoint_optional: starting, new horizon=%llx deadline=%llu",
            pdb->pdb_new_index_horizon, deadline);
        if (!pdb_transactional(pdb) &&
            pdb->pdb_active_checkpoint_sync == false) {
          stage = PDB_CKS_START_MARKER - 1;
          for (i = 0; i < PDB_INDEX_N; i++)
            pdb->pdb_indices[i].ii_stage = stage;
        }
      } break;

      case PDB_CKS_FINISH_BACKUP:
      case PDB_CKS_SYNC_BACKUP:
      case PDB_CKS_SYNC_DIRECTORY:
      case PDB_CKS_START_WRITES:
      case PDB_CKS_FINISH_WRITES:
      case PDB_CKS_REMOVE_BACKUP:
      call_index_callbacks:

        /*  Call the appropriate checkpointing stage handler
         *  for each index.
         */

        /* number of checkpoint stages */
        for (i = 0; i < PDB_INDEX_N; i++) {
          pdb_index_instance* const ii = &pdb->pdb_indices[i];

          if (ii->ii_stage == stage) continue;

          cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
                 "pdb_checkpoint_optional: index %s trying %s -> %s",
                 ii->ii_path, pdb_checkpoint_stage_names[ii->ii_stage],
                 pdb_checkpoint_stage_names[stage]);

          err = pdb_index_do_checkpoint_stage(pdb, &pdb->pdb_indices[i], stage,
                                              pdb->pdb_active_checkpoint_sync,
                                              0 == deadline);
          if (err) {
            char const* const sn = pdb_checkpoint_stage_names[stage];

            switch (err) {
              case PDB_ERR_MORE:
                wouldblock = true;
                cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
                       "pdb_checkpoint_optional: %s %s: PDB_ERR_MORE",
                       ii->ii_path, sn);
                break;

              case PDB_ERR_ALREADY:
                cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
                       "pdb_checkpoint_optional: %s %s: ALREADY", ii->ii_path,
                       sn);
                break;

              default:
                pdb_disk_set_available(pdb, false);
                cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR,
                             "pdb_index_do_checkpoint_stage", err,
                             "Unable to checkpoint %s %s", ii->ii_path, sn);
                cl_leave(pdb->pdb_cl, CL_LEVEL_DEBUG, "leave");
                return err;
            }
          }
        }
        break;

      case PDB_CKS_START_MARKER:

        if (pdb->pdb_new_index_horizon ==
            addb_istore_marker_horizon(pdb->pdb_primitive))
          err = PDB_ERR_ALREADY;
        else {
          addb_istore_horizon_set(pdb->pdb_primitive,
                                  pdb->pdb_new_index_horizon);
          err = addb_istore_marker_horizon_write_start(
              pdb->pdb_primitive, pdb->pdb_active_checkpoint_sync);
        }
        if (err != PDB_ERR_ALREADY && err != 0) {
          cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR,
                       "addb_istore_marker_checkpoint_start", err,
                       "Unable to save new horizon %llx",
                       (unsigned long long)pdb->pdb_new_index_horizon);
          cl_leave(pdb->pdb_cl, CL_LEVEL_DEBUG, "lost horizon");
          return err;
        }
        goto call_index_callbacks;

      case PDB_CKS_FINISH_MARKER:
        err = addb_istore_marker_horizon_write_finish(
            pdb->pdb_primitive,
            /* blocking: */ deadline == 0);
        if (err == ADDB_ERR_MORE) {
          wouldblock = true;
          cl_log(pdb->pdb_cl, CL_LEVEL_VERBOSE,
                 "pdb_checkpoint_optional: "
                 "addb_istore_marker_checkpoint_finish: WOULDBLOCK");
        } else if (err && err != PDB_ERR_ALREADY) {
          cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR,
                       "addb_istore_marker_checkpoint_finish", err,
                       "error while saving horizon %llx",
                       (unsigned long long)pdb->pdb_new_index_horizon);
          cl_leave(pdb->pdb_cl, CL_LEVEL_DEBUG, "lost horizon");
          return err;
        }
        goto call_index_callbacks;

      default:
        cl_notreached(pdb->pdb_cl, "unexpected stage %d", stage);
    }

    if (wouldblock) {
      cl_leave(pdb->pdb_cl, CL_LEVEL_DEBUG, "blocking");

      return PDB_ERR_MORE; /* call us again */
    }

    if (deadline != 0) {
      addb_msclock_t const now = addb_msclock(pdb->pdb_addb);

      if (ADDB_PAST_DEADLINE(now, deadline)) {
        cl_leave(pdb->pdb_cl, CL_LEVEL_DEBUG, "past deadline");

        return PDB_ERR_MORE;
      }
    }
  }

  /* We're done, reset all index stages for the next checkpoint.
   */
  for (i = 0; i < PDB_INDEX_N; i++) {
    pdb_index_instance* const ii = &pdb->pdb_indices[i];

    ii->ii_stage = PDB_CKS_START;
    (*ii->ii_type->ixt_advance_horizon)(pdb, ii, pdb->pdb_new_index_horizon);
  }
  cl_leave(pdb->pdb_cl, CL_LEVEL_DEBUG, "done, new horizon=%llx",
           pdb->pdb_new_index_horizon);

  pdb_disk_set_available(pdb, true);
  pdb->pdb_new_index_horizon = 0;

  deficit = pdb_checkpoint_deficit(pdb);
  if (deficit <= PDB_CHECKPOINT_URGENT_DEFICIT_MIN &&
      pdb->pdb_deficit_exceeded) {
    cl_log(pdb->pdb_cl, CL_LEVEL_INFO,
           "pdb_checkpoint_optional: deficit=%llu now less than "
           " 100,000 primitives",
           deficit);
    pdb->pdb_deficit_exceeded = false;
  }

  return 0;
}

/**
 * @brief Make sure that the local database is in a consistent state.
 *
 *  Call this after pdb_initialize() and pdb_configure().
 *
 * @param pdb	A database handle.
 * @return 0 on success, a nonzero error code on error.
 */
int pdb_checkpoint_synchronize(pdb_handle* pdb) {
  addb_istore_id const horizon = addb_istore_horizon(pdb->pdb_primitive);
  addb_istore_id const next_id = addb_istore_next_id(pdb->pdb_primitive);
  int err = 0;
  addb_istore_id id;

  if (!pdb) return EINVAL;

  cl_enter(pdb->pdb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY, "%s",
           pdb->pdb_path);

  if (next_id == horizon) {
    cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY,
             "next id and horizon are %llx", (unsigned long long)horizon);
    return 0;
  }

  /*  Add primitives to the indices
   */
  for (id = horizon; id < next_id; id++) {
    pdb_primitive pr;

    err = pdb_id_read(pdb, id, &pr);
    if (err == PDB_ERR_NO) {
      err = 0;
      continue;
    } else if (err) {
      cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY,
               "unexpected error from pdb_id_read: %s", strerror(err));
      return err;
    }
    if (!pdb_transactional(pdb)) {
      /* If we're non-transactional then well, we MAY be okay.
       * Then again, there are no guarantees.
       * So verify each primitive above the horizon and only *then*
       * reindex. (We know we must anyway if we're transactional)
       */
      unsigned long error_code;
      err = pdb_verify_id(pdb, id, &error_code);
      if (!err) {
        /* This ID is fine. Check the next */
        continue;
      }
    }

    /* Reindex the primitive */

    err = pdb_index_new_primitive(pdb, id, &pr);
    if (err) {
      cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY,
               "unexpected error from "
               "pdb_index_new_primitive: %s",
               strerror(err));
      return err;
    }
    pdb_primitive_finish(pdb, &pr);
  }

  /*  The internal memory state of the indices is now consistent with
   *  the istore.  Flush indices to disk.
   */
  err = pdb_checkpoint_optional(pdb, 0);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_checkpoint_optional", err,
                 "Unable to synchronize indices");

    if (!pdb_disk_is_available(pdb))

      /* This is a well-defined error case that
       * we know how to deal with in the calling code.
       */
      err = 0;
  } else {
    cl_log(pdb->pdb_cl, CL_LEVEL_DEBUG,
           "pdb_checkpoint_synchronize: "
           "synchronization completed.");
  }
  cl_leave(pdb->pdb_cl, CL_LEVEL_VERBOSE | ADDB_FACILITY_RECOVERY, "%s",
           err ? strerror(err)
               : pdb_disk_is_available(pdb) ? "done" : "out of disk space");
  return err;
}

/**
 * @brief Roll back to a previous state.
 *
 *  A write operation somewhere went horribly wrong -- something ran
 *  out of memory, or disk space, something like that.  (We try to
 *  head off all avoidable accidents in advance.  But sometimes you
 *  don't know that you'll run out until you run out.)
 *
 *  If we just crashed at this point, we'd be consistent.  But, well,
 *  we'd like to keep running.
 *
 *  Use our existing resources to go back to a well-defined state
 *  before the accident.
 *
 * @param p		opaque database handle
 * @param horizon	state to go back to.
 *
 * @return 0 on success, a nonzero error code on error.  If this
 *	call fails, the best course of action is to crash the
 * 	server and hope for a restart.
 */
int pdb_checkpoint_rollback(pdb_handle* pdb, unsigned long long horizon) {
  int err = 0;
  int i;

  if (!pdb_transactional(pdb))
    cl_notreached(pdb->pdb_cl,
                  "Tried to execute pdb_checkpoint_rollback but"
                  " this database is transactionless.");

  /* Make sure that we're not trying to roll back to before
   * the horizon committed by the indicies.
   */
  for (i = 0; i < PDB_INDEX_N; i++) {
    pdb_index_instance* const ii = &pdb->pdb_indices[i];
    if (ii->ii_impl.any)
      cl_assert(pdb->pdb_cl, horizon >= (*ii->ii_type->ixt_horizon)(pdb, ii));
  }

  cl_enter(pdb->pdb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY, "%s to %llx",
           pdb->pdb_path, horizon);

  err = addb_istore_checkpoint_rollback(pdb->pdb_primitive, horizon);
  if (err != 0) return err;

  /* Roll the indices back to their last defined checkpoint (the
   * horizon stored in the marker file)
   */
  for (i = 0; i < PDB_INDEX_N; i++) {
    pdb_index_instance* const ii = &pdb->pdb_indices[i];

    err = (*ii->ii_type->ixt_rollback)(pdb, ii);
    if (err) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "ixt_rollback", err,
                   "Unable to rollback %s", ii->ii_path);
      cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY,
               "%s to %llx FAILED", pdb->pdb_path, horizon);
      return err;
    }
  }

  /* Now move them forward again, from the stored horizon
   * to our disk location.
   */
  err = pdb_checkpoint_synchronize(pdb);

  cl_leave(pdb->pdb_cl, CL_LEVEL_SPEW | ADDB_FACILITY_RECOVERY, "%s",
           err ? strerror(err) : "done");

  return err;
}

/* How many primitives have been committed to the istore but not to
 * the indices?
 */
unsigned long long pdb_checkpoint_deficit(pdb_handle* pdb) {
  if (pdb->pdb_primitive == NULL) return 0;
  return addb_istore_next_id(pdb->pdb_primitive) -
         addb_istore_horizon(pdb->pdb_primitive);
}

/* Do we urgently need to do a checkpoint?
 */
bool pdb_checkpoint_urgent(pdb_handle* pdb) {
  return pdb_checkpoint_deficit(pdb) >= PDB_CHECKPOINT_URGENT_DEFICIT_MIN;
}

addb_istore_id pdb_checkpoint_horizon(pdb_handle* pdb) {
  return addb_istore_horizon(pdb->pdb_primitive);
}
