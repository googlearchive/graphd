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

/* Assign ``or'' frames to slots in node-first order.
 */
static size_t graphd_read_or_assign(graphd_constraint *con,
                                    graphd_read_or_slot *ros, size_t n) {
  graphd_constraint_or *cor;

  if (con == NULL) return 0;

  ros[n].ros_con = con;
  ros[n].ros_state = GRAPHD_READ_OR_INITIAL;
  con->con_or_index = n++;

  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    n = graphd_read_or_assign(&cor->or_head, ros, n);
    if (cor->or_tail != NULL) n = graphd_read_or_assign(cor->or_tail, ros, n);
  }
  return n;
}

static size_t graphd_read_or_n(graphd_constraint *con) {
  graphd_constraint_or *cor;
  size_t n = 1;

  if (con == NULL) return 0;

  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    n += graphd_read_or_n(&cor->or_head);
    if (cor->or_tail != NULL) n += graphd_read_or_n(cor->or_tail);
  }
  return n;
}

/*  Free resources allocated for a read-or map.
 */
void graphd_read_or_finish(graphd_request *greq, graphd_read_or_map *rom) {
  if (rom->rom_slot != NULL && rom->rom_slot != rom->rom_buf)
    cm_free(greq->greq_req.req_cm, rom->rom_slot);
  rom->rom_slot = NULL;
}

/*  Initialize or re-initialize the "read-or-map" that,
 *  for a given ID, tracks which of the OR branches
 *  in the ID's constraint evaluate to true.
 */
int graphd_read_or_initialize(graphd_request *greq, graphd_constraint *con,
                              graphd_read_or_map *rom) {
  size_t n;

  if (rom->rom_slot == NULL) {
    size_t n2;

    /* Allocate the state vector.
     */

    /* Most common case: no "or".
     */
    if ((n = graphd_read_or_n(con)) <= 1) {
      rom->rom_slot = rom->rom_buf;
      rom->rom_n = n;
      con->con_or_index = 0;

      return 0;
    }

    rom->rom_slot =
        cm_zalloc(greq->greq_req.req_cm, sizeof(*rom->rom_slot) * n);
    if (rom->rom_slot == NULL) return ENOMEM;

    n2 = graphd_read_or_assign(con, rom->rom_slot, 0);
    cl_assert(graphd_request_cl(greq), n2 == n);

    rom->rom_n = n2;
  } else {
    /* Reset the state to "unmatched".
     */
    for (n = 0; n < rom->rom_n; n++)
      rom->rom_slot[n].ros_state = GRAPHD_READ_OR_INITIAL;
  }

  return 0;
}

bool graphd_read_or_check(graphd_request *greq, size_t i,
                          graphd_read_or_map const *rom) {
  return rom != NULL && rom->rom_slot[i].ros_state != GRAPHD_READ_OR_FALSE;
}

int graphd_read_or_state(graphd_request const *greq,
                         graphd_constraint const *con,
                         graphd_read_or_map const *rom) {
  cl_handle *cl = graphd_request_cl(greq);
  cl_assert(cl, con->con_or_index < rom->rom_n);

  return rom->rom_slot[con->con_or_index].ros_state;
}

void graphd_read_or_fail(graphd_request *greq, graphd_constraint *con,
                         graphd_read_or_map *rom) {
  graphd_constraint_or *cor;
  cl_handle *cl = graphd_request_cl(greq);

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_read_or_fail "
         "con=%s [rom: %p; %zu; or-index %zu]",
         graphd_constraint_to_string(con), (void *)rom, rom ? rom->rom_n : 0,
         con->con_or_index);

  cl_assert(cl, con->con_or_index < rom->rom_n);
  if (rom->rom_slot[con->con_or_index].ros_state == GRAPHD_READ_OR_FALSE)
    return;

  rom->rom_slot[con->con_or_index].ros_state = GRAPHD_READ_OR_FALSE;

  /*  Mark all alternatives inside the branch con as false, too.
   *  We do that so that we don't even begin evaluating
   *  subconstraints that are in those alternatives.
   */
  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    graphd_read_or_fail(greq, &cor->or_head, rom);
    if (cor->or_tail != NULL) graphd_read_or_fail(greq, cor->or_tail, rom);
  }

  /*  If we have a sibling branch, and that sibling branch
   *  is also false, mark our prototype as false.
   */
  if ((cor = con->con_or) != NULL) {
    if ((con == &cor->or_head && cor->or_tail != NULL &&
         rom->rom_slot[cor->or_tail->con_or_index].ros_state ==
             GRAPHD_READ_OR_FALSE)) {
      cl_log(cl, CL_LEVEL_VERBOSE,
             "graphd_read_or_fail: tail is non-NULL, "
             "and false");
      graphd_read_or_fail(greq, cor->or_prototype, rom);
    } else if ((con == cor->or_tail &&
                rom->rom_slot[cor->or_head.con_or_index].ros_state ==
                    GRAPHD_READ_OR_FALSE)) {
      cl_log(cl, CL_LEVEL_VERBOSE, "graphd_read_or_fail: head is false, too");
      graphd_read_or_fail(greq, cor->or_prototype, rom);
    }
  }
}

/*  Record that all direct or enclosed subconstraints of
 *  "con" have matched.
 */
void graphd_read_or_match_subconstraints(graphd_request *greq,
                                         graphd_constraint *con,
                                         graphd_read_or_map *rom) {
  cl_handle *cl = graphd_request_cl(greq);

  if (rom->rom_slot[con->con_or_index].ros_state !=
      GRAPHD_READ_OR_INTRINSICS_MATCH) {
    cl_log(cl, CL_LEVEL_VERBOSE,
           "graphd_read_or_match_subconstraints: "
           "state of rom[%zu] isn't INTRINSICS_MATCH, it's %d",
           con->con_or_index, rom->rom_slot[con->con_or_index].ros_state);
    return;
  }

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_read_or_match_subconstraints: "
         "rom[%zu] := GRAPHD_READ_OR_TRUE (rom=%p, con=%s)",
         con->con_or_index, (void *)rom, graphd_constraint_to_string(con));

  rom->rom_slot[con->con_or_index].ros_state = GRAPHD_READ_OR_TRUE;

  /*  If there is an alternative to <con>, we don't
   *  need to examine it anymore.  Obviate it and its
   *  branches.
   */
  if (con->con_or != NULL && con == &con->con_or->or_head &&
      con->con_or->or_tail != NULL)

    graphd_read_or_fail(greq, con->con_or->or_tail, rom);
}

/*  Record that the intrinsics of "con" (and its
 *  sub-alternatives, if any) have matched.
 */
void graphd_read_or_match_intrinsics(graphd_request *greq,
                                     graphd_constraint *con,
                                     graphd_read_or_map *rom) {
  cl_handle *cl = graphd_request_cl(greq);

  if (rom->rom_slot[con->con_or_index].ros_state != GRAPHD_READ_OR_INITIAL)
    return;

  rom->rom_slot[con->con_or_index].ros_state =
      (con->con_tail == &con->con_head ? GRAPHD_READ_OR_TRUE
                                       : GRAPHD_READ_OR_INTRINSICS_MATCH);

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_read_or_match_intrinsics: "
         "rom[%zu] := GRAPHD_READ_OR_%s (rom=%p, con=%s)",
         con->con_or_index,
         rom->rom_slot[con->con_or_index].ros_state == GRAPHD_READ_OR_TRUE
             ? "TRUE"
             : "INTRINSICS_MATCH",
         (void *)rom, graphd_constraint_to_string(con));

  if (con->con_head == NULL && con->con_or != NULL &&
      con == &con->con_or->or_head && con->con_or->or_tail != NULL)

    graphd_read_or_fail(greq, con->con_or->or_tail, rom);
}
