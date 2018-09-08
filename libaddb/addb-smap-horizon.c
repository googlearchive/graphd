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
#include "libaddb/addb-smap.h"

#include <stdio.h>

/**
 * @brief Return the horizon of a SMAP.
 * @param gm an addb_gmap object created with addb_gmap_open()
 * @return the state that the SMAP could roll back to if the
 *  	process crashed right now.
 */
unsigned long long addb_smap_horizon(addb_smap* sm) {
  if (sm == NULL) return 0;
  return sm->sm_horizon;
}

void addb_smap_horizon_set(addb_smap* sm, unsigned long long horizon) {
  sm->sm_horizon = horizon;
}
