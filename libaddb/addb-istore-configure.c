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
#include "libaddb/addbp.h"

/**
 * @brief Configure an istore database
 * @param is	opaque istore database handle
 * @param icf	configuration structure, parsed from user input
 */

void addb_istore_configure(addb_istore* is, addb_istore_configuration* icf) {
  if (!is) return;

  is->is_cf = *icf;

  if (is->is_tiled_pool) {
    size_t const n_parts = sizeof is->is_partition / sizeof is->is_partition[0];
    size_t i;

    for (i = 0; i < n_parts; i++)
      if (is->is_partition[i].ipart_td)
        addb_tiled_set_mlock(is->is_partition[i].ipart_td, is->is_cf.icf_mlock);
  }
}
