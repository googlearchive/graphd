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
 * @brief Return the first unallocated id in istore.
 *
 * This is for informational purposes only (it's one more than
 * a count of objects in the store) - using applications don't
 * normally need to call this.
 *
 * @param is an opaque istore handle opened with addb_istore_open()
 * @return the id that the next call to addb_istore_alloc() will return.
 */
addb_istore_id addb_istore_next_id(addb_istore const* is) {
  cl_cover(is->is_addb->addb_cl);
  return is->is_next.ism_memory_value;
}
