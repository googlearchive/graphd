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
#include "libaddb/addb.h"
#include "libaddb/addbp.h"

int addb_status(addb_handle* addb, cm_prefix* prefix, addb_status_callback* cb,
                void* cb_data) {
  if (addb != NULL) {
    cl_assert(addb->addb_cl, addb->addb_master_tiled_pool != NULL);
    return addb_tiled_pool_status(addb->addb_master_tiled_pool, prefix, cb,
                                  cb_data);
  }
  return 0;
}
