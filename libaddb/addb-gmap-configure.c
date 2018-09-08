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
 * @brief Configure a GMAP database
 *
 *  Sets the stored configuration inside the GMAP.
 *  If the GMAP arleady has a tiled pool, the maximum cache size,
 *  tile size, and initial map size for that pool are also
 *  set (possibly in vein - initial map and tile sizes for a pool
 *  cannot be changed after the pool is first loaded).
 *
 * @param gm	Database to configure
 * @param gcf	Parameters to set.
 */
void addb_gmap_configure(addb_gmap* gm, addb_gmap_configuration* gcf) {
  if (!gm) return;

  gm->gm_cf = *gcf;

  if (gm->gm_tiled_pool != NULL) {
    size_t const n_parts = sizeof gm->gm_partition / sizeof gm->gm_partition[0];
    size_t i;

    for (i = 0; i < n_parts; i++)
      if (gm->gm_partition[i].part_td)
        addb_tiled_set_mlock(gm->gm_partition[i].part_td, gm->gm_cf.gcf_mlock);
  }

  addb_largefile_set_maxlf(gm->gm_lfhandle, gcf->gcf_max_lf);
  gm->gm_bitmap = gcf->gcf_allow_bgmaps;
  if (!gm->gm_bitmap) {
    cl_log(gm->gm_addb->addb_cl, CL_LEVEL_INFO,
           "Disableing bgmaps. Any bgmaps already on disk will be"
           " used. No new bgmaps wil be created");
  }
}
