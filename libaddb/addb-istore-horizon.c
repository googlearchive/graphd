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
#include "libaddb/addb-istore.h"
#include "libaddb/addbp.h"

#include <errno.h>


/**
 *  The horizon of a pdb database is one more than the highest ID that
 *  has been committed into any of the index files.
 *
 *  When recovering, we can roll back to the horizon, but not further
 *  into the past.  (We can't go before that ID because we don't know
 *  how to roll back a committed index file.)
 *
 *  When a horizon is set, the whole database needs to be in a
 *  well-defined state.
 *
 *  The horizon is a PDB concept.  It happens to be stored in the
 *  istore, but it applies to the whole cluster of primitive + index
 *  databases.  To the istore itself, it's pretty much opaque and just
 *  a number that needs to be stored and loaded.
 */

/**
 * @brief Return the horizon.
 *
 * @param is an opaque istore handle opened with addb_istore_open()
 * @return the first ID that may not have been indexed yet.
 */
addb_istore_id addb_istore_horizon(addb_istore const* is) {
  cl_cover(is->is_addb->addb_cl);
  return is->is_horizon.ism_memory_value;
}

/**
 * @brief Set the horizon.
 *
 * @param is an opaque istore handle opened with addb_istore_open()
 * @param horizon Horizon to move to.
 */
void addb_istore_horizon_set(addb_istore* is, addb_istore_id horizon) {
  if (is->is_horizon.ism_memory_value != horizon) {
    cl_cover(is->is_addb->addb_cl);
    cl_log(is->is_addb->addb_cl, CL_LEVEL_SPEW, "%s: istore horizon := %llu",
           is->is_path, (unsigned long long)horizon);

    is->is_horizon.ism_memory_value = horizon;
  }
}
