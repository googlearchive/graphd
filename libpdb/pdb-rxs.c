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
#include <stdarg.h>

#include "libcl/cl.h"

void pdb_rxs_set(pdb_handle* pdb, size_t depth) { pdb->pdb_rxs_depth = depth; }

size_t pdb_rxs_get(pdb_handle* pdb) { return pdb->pdb_rxs_depth; }

void pdb_rxs_push(pdb_handle* pdb, char const* fmt, ...) {
  if (pdb->pdb_cl == NULL || cl_is_logged(pdb->pdb_cl, CL_LEVEL_VERBOSE)) {
    char bigbuf[4 * 1024];

    va_list ap;
    va_start(ap, fmt);

    snprintf(bigbuf, sizeof bigbuf, "{ RXS:%*s%s", /*}*/
             (int)(pdb->pdb_rxs_depth > 80 ? 80 : pdb->pdb_rxs_depth), "", fmt);

    cl_vlog(pdb->pdb_cl, CL_LEVEL_VERBOSE, bigbuf, ap);
    cl_indent(pdb->pdb_cl, CL_LEVEL_VERBOSE, 1);

    va_end(ap);
  }
  pdb->pdb_rxs_depth++;
}

void pdb_rxs_pop(pdb_handle* pdb, char const* fmt, ...) {
  pdb->pdb_rxs_depth--;
  if (pdb->pdb_cl == NULL || cl_is_logged(pdb->pdb_cl, CL_LEVEL_VERBOSE)) {
    char bigbuf[4 * 1024];

    va_list ap;
    va_start(ap, fmt);

    snprintf(bigbuf, sizeof bigbuf,
             /*{*/ "} RXS:%*s%s",
             (int)(pdb->pdb_rxs_depth > 80 ? 80 : pdb->pdb_rxs_depth), "", fmt);

    cl_indent(pdb->pdb_cl, CL_LEVEL_VERBOSE, -1);
    cl_vlog(pdb->pdb_cl, CL_LEVEL_VERBOSE, bigbuf, ap);

    va_end(ap);
  }
}

void pdb_rxs_pop_id(pdb_handle* pdb, int err, pdb_id id, pdb_budget cost,
                    char const* fmt, ...) {
  pdb->pdb_rxs_depth--;
  if (pdb->pdb_cl == NULL || cl_is_logged(pdb->pdb_cl, CL_LEVEL_VERBOSE)) {
    char bigbuf[4 * 1024];
    char idbuf[200];

    va_list ap;
    va_start(ap, fmt);

    if (err == 0)
      snprintf(bigbuf, sizeof bigbuf,
               /*{*/ "} RXS:%*s%s %s ($%lld)",
               (int)(pdb->pdb_rxs_depth > 80 ? 80 : pdb->pdb_rxs_depth), "",
               fmt, pdb_id_to_string(pdb, id, idbuf, sizeof idbuf),
               (long long)cost);
    else
      snprintf(bigbuf, sizeof bigbuf,
               /*{*/ "} RXS:%*s%s %s ($%lld)",
               (int)(pdb->pdb_rxs_depth > 80 ? 80 : pdb->pdb_rxs_depth), "",
               fmt, err == PDB_ERR_NO
                        ? "done"
                        : (err == PDB_ERR_MORE ? "suspend" : strerror(err)),
               (long long)cost);

    cl_indent(pdb->pdb_cl, CL_LEVEL_VERBOSE, -1);
    cl_vlog(pdb->pdb_cl, CL_LEVEL_VERBOSE, bigbuf, ap);

    va_end(ap);
  }
}

void pdb_rxs_pop_test(pdb_handle* pdb, int err, pdb_budget cost,
                      char const* fmt, ...) {
  pdb->pdb_rxs_depth--;
  if (pdb->pdb_cl == NULL || cl_is_logged(pdb->pdb_cl, CL_LEVEL_VERBOSE)) {
    char bigbuf[4 * 1024];

    va_list ap;
    va_start(ap, fmt);

    snprintf(
        bigbuf, sizeof bigbuf,
        /*{*/ "} RXS:%*s%s %s ($%lld)",
        (int)(pdb->pdb_rxs_depth > 80 ? 80 : pdb->pdb_rxs_depth), "", fmt,
        err == 0 ? "yes" : (err == PDB_ERR_NO ? "no" : (err == PDB_ERR_MORE
                                                            ? "suspend"
                                                            : strerror(err))),
        (long long)cost);

    cl_indent(pdb->pdb_cl, CL_LEVEL_VERBOSE, -1);
    cl_vlog(pdb->pdb_cl, CL_LEVEL_VERBOSE, bigbuf, ap);

    va_end(ap);
  }
}

void pdb_rxs_log(pdb_handle* pdb, char const* fmt, ...) {
  if (pdb->pdb_cl == NULL || cl_is_logged(pdb->pdb_cl, CL_LEVEL_VERBOSE)) {
    char bigbuf[4 * 1024];

    va_list ap;
    va_start(ap, fmt);

    snprintf(bigbuf, sizeof bigbuf, "RXS:  %*s%s",
             (int)(pdb->pdb_rxs_depth > 80 ? 80 : pdb->pdb_rxs_depth), "", fmt);
    cl_vlog(pdb->pdb_cl, CL_LEVEL_VERBOSE, bigbuf, ap);
    va_end(ap);
  }
}
