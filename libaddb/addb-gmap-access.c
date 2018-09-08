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
#include "libaddb/addb-bgmap.h"
#include "libaddb/addb-gmap-access.h"
#include "libaddb/addb-gmap.h"
#include "libaddb/addb.h"
#include "libaddb/addb-largefile-file.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>


/**
 * @brief setup an addb_gmap_accessor struct for a gmap/id
 *
 * @param gm 	the gmap to use
 * @param id 	the id to lookup
 * @param ac 	the accessor to set
 *
 * @return 0 on success, or error on failure.
 * @return ADDB_ERR_NO if the array would have zero elements
 */

int addb_gmap_accessor_set(addb_gmap* gm, addb_gmap_id id,
                           addb_gmap_accessor* ac) {
  int err;

  ac->gac_part = addb_gmap_partition_by_id(gm, id);
  if (ac->gac_part == NULL) {
    /* This isn't terribly bad. It means you've asked for a GMAP
     * that doesn't exist but that should be in a partition that
     * also doesn't exist.
     */
    err = errno;
    cl_log_errno(gm->gm_addb->addb_cl, CL_LEVEL_VERBOSE,
                 "addb_gmap_partition_by_id", err,
                 "Unable to locate partition for %llu", (unsigned long long)id);

    return ADDB_ERR_NO;
  }
  err = addb_gmap_partition_data(ac->gac_part, id, &ac->gac_offset,
                                 &ac->gac_length, &ac->gac_index);
  if (err) {
    if (ADDB_ERR_NO != err) /* ADDB_ERR_NO means "null array" */
      cl_log_errno(gm->gm_addb->addb_cl, CL_LEVEL_FAIL,
                   "addb_gmap_partition_data", err,
                   "Unable to retrieve offset and length for "
                   "%llu in \"%s\"",
                   (unsigned long long)id, ac->gac_part->part_path);

    return err;
  }
  ac->gac_lf = NULL;
  ac->gac_bgmap = NULL;
  if (ac->gac_length == 1) {
    if (ADDB_GMAP_IVAL_IS_FILE(ac->gac_index)) {
      cl_log(gm->gm_addb->addb_cl, CL_LEVEL_SPEW,
             "addb_gmap_accessor_set: access: %llu lf", (unsigned long long)id);
      err = addb_largefile_get(gm->gm_lfhandle, id, &ac->gac_lf);
      if (err) {
        cl_log(gm->gm_addb->addb_cl, CL_LEVEL_FAIL,
               "addb_gmap_accessor_set: "
               "addb_largefile_get failed: %s",
               addb_xstrerror(err));

        return err;
      }
      ac->gac_offset = ADDB_LARGE_HEADER;
      ac->gac_length = (ac->gac_lf->lf_size - ADDB_LARGE_HEADER) / 5;

      cl_assert(gm->gm_addb->addb_cl, ac->gac_length != 1);

      return 0;
    }

    if (ADDB_GMAP_IVAL_IS_BGMAP(ac->gac_index)) {
      cl_log(gm->gm_addb->addb_cl, CL_LEVEL_SPEW,
             "addb_gmap_accessor_set: access: %llu bgmap",
             (unsigned long long)id);
      err = addb_gmap_bgmap_read_size(gm, id, &(ac->gac_length));
      if (err) {
        cl_log(gm->gm_addb->addb_cl, CL_LEVEL_FAIL, "Can't get bmap size");
        return err;
      }

      err = addb_bgmap_lookup(gm, id, &(ac->gac_bgmap));

      /* LOGGING! What does enoent mean here */
      if (err) {
        cl_log(gm->gm_addb->addb_cl, CL_LEVEL_FAIL, "can't get gmap");
        return err;
      }

    } else
      cl_log(gm->gm_addb->addb_cl, CL_LEVEL_SPEW,
             "addb_gmap_accessor_set: access: %llu single",
             (unsigned long long)id);

  } else
    cl_log(gm->gm_addb->addb_cl, CL_LEVEL_SPEW,
           "addb_gmap_accessor_set: access: %llu multi",
           (unsigned long long)id);
  return 0;
}

int addb_gmap_accessor_clear(addb_gmap_accessor* ac) {
  ac->gac_part = NULL;
  ac->gac_lf = NULL;
  ac->gac_bgmap = NULL;
  return 0;
}

const char* addb_gmap_accessor_display_name_i(addb_gmap_accessor const* ac) {
  char* dn;

  if (!ac) return NULL;

  if (!ac->gac_lf && !ac->gac_bgmap) {
    return ac->gac_part->part_path;
  }

  if (ac->gac_bgmap) {
    return addb_bgmap_name(ac->gac_bgmap);
  }

  if (ac->gac_lf->lf_display_name) {
    return ac->gac_lf->lf_display_name;
  }

  dn = cm_malloc(ac->gac_part->part_gm->gm_addb->addb_cm, 1024);
  if (!dn) return NULL;

  snprintf(dn, 1024, "lf:%s:%llu", ac->gac_part->part_path, ac->gac_lf->lf_id);
  ac->gac_lf->lf_display_name = dn;
  return dn;
}
