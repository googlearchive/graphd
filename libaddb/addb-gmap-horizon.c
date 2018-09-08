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
#include "libaddb/addb-bgmap.h"

#include <stdio.h>

#include "libcm/cm.h"

/**
 * @brief Return the horizon of a GMAP.
 * @param gm an addb_gmap object created with addb_gmap_open()
 * @return the state that the GMAP could roll back to if the
 *  	process crashed right now.
 */
unsigned long long addb_gmap_horizon(addb_gmap* gm) {
  if (gm == NULL) return 0;
  return gm->gm_horizon;
}

void addb_gmap_horizon_set(addb_gmap* gm, unsigned long long horizon) {
  gm->gm_horizon = horizon;
  addb_bgmap_horizon_set(gm, horizon);
}
