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

#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <ctype.h>

static char const graphd_dump_version[] = "6";

/**
 * @param Dump parts of the graph.
 *
 *  Most of the actual work happens in format() - we're just
 *  setting up a description of the results to be returned.
 *
 * @param g		opaque graph module handle
 * @param greq		request within the session
 * @param deadline	if going on past this, return
 *			GRAPHD_ERR_MORE and get called back later.
 *
 * @return 0 on success, a nonzer error code on error.
 */

static int graphd_dump_run(graphd_request* greq, srv_msclock_t deadline) {
  int err = 0;
  unsigned long long pagesize;
  pdb_id start, end;
  graphd_value* val;
  cl_handle* cl = graphd_request_cl(greq);
  graphd_handle* const g = graphd_request_graphd(greq);

  /* Figure out start, end, and pagesize.
   */
  start = 0;
  if (greq->greq_start != PDB_ID_NONE) start = greq->greq_start;

  end = pdb_primitive_n(g->g_pdb);
  if (greq->greq_end != PDB_ID_NONE && greq->greq_end < end)
    end = greq->greq_end;

  pagesize = 0;
  if (start < end) pagesize = end - start;
  if (greq->greq_pagesize != 0) pagesize = greq->greq_pagesize;

  if (start + pagesize < end) end = start + pagesize;

  cl_log(cl, CL_LEVEL_DEBUG, "graphd_dump(start=%llu, end=%llu, pagesize=%llu)",
         (unsigned long long)start, (unsigned long long)end,
         (unsigned long long)pagesize);

  if (start > end) {
    cl_log(cl, CL_LEVEL_FAIL, "graphd_dump: start %llu > end %llu",
           (unsigned long long)start, (unsigned long long)end);
    graphd_request_errprintf(greq, 0, "SEMANTICS start %llu exceeds end %llu",
                             (unsigned long long)start,
                             (unsigned long long)end);
    return 0;
  }

  /*  Build the array of results.  The last one, a value of type
   *  "records", is a placeholder for the actual payload data;
   *  we'll start reading records once we're actually formatting.
   */
  err = graphd_value_list_alloc(g, greq->greq_req.req_cm, cl, &greq->greq_reply,
                                4);
  if (err != 0) return err;

  val = greq->greq_reply.val_list_contents;
  graphd_value_text_set(val++, GRAPHD_VALUE_STRING, graphd_dump_version,
                        graphd_dump_version + sizeof graphd_dump_version - 1,
                        NULL);
  graphd_value_number_set(val++, start);
  graphd_value_number_set(val++, end);
  graphd_value_records_set(val, g->g_pdb, start, end - start);

  graphd_request_served(greq);
  return 0;
}

static graphd_request_type graphd_dump_request = {
    "dump",
    /* input-arrived */ NULL,
    /* output-sent 	 */ NULL,
    graphd_dump_run,
    /* graphd_write_cancel */ NULL,
    /* graphd_set_free */ NULL};

void graphd_dump_initialize(graphd_request* greq) {
  greq->greq_request = GRAPHD_REQUEST_DUMP;
  greq->greq_type = &graphd_dump_request;
}
