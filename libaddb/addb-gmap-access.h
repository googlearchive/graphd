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
#ifndef _ADDB_ACCESS_H
#define _ADDB_ACCESS_H

#include "libaddb/addb.h"

/*
 * addb_gmap_accessors are a fast, mostly syntactic abstraction layer that
 * moves partitions, largefiles, partition offsets and the machinery to
 * interface to them into a single abstraction.
 */

/*
 * Get the data living at offset p (byte offset from the begining of the
 * array) and copy it into ptr
 */
#define addb_gmap_accessor_get(ac, p, ptr)                                 \
  ((ac)->gac_lf                                                            \
       ? addb_largefile_read5((ac)->gac_lf, (ac)->gac_offset + (p), (ptr)) \
       : ((ac)->gac_length == 1                                            \
              ? (ac)->gac_index & 0x3ffffffffull                           \
              : addb_gmap_partition_get((ac)->gac_part,                    \
                                        (ac)->gac_offset + (p), (ptr))))

/* Get the length (in ids, not bytes) for this array
 */
#define addb_gmap_accessor_n(ac) ((ac)->gac_length)

/*
 * Does this accessor access anything?
 */
#define addb_gmap_accessor_is_set(ac) ((ac)->gac_part != NULL)

/*
 * Grab a sensible display name for generating error messages.
 * This is guaranteed to return a string (which might be "" if
 * something went wrong) and must not change or free the string
 */
#define addb_gmap_accessor_display_name(ac)    \
  (addb_gmap_accessor_display_name_i(ac)       \
       ? addb_gmap_accessor_display_name_i(ac) \
       : "")

int addb_gmap_accessor_set(struct addb_gmap *gm, addb_gmap_id id,
                           addb_gmap_accessor *ac);

int addb_gmap_accessor_clear(addb_gmap_accessor *ac);

const char *addb_gmap_accessor_display_name_i(addb_gmap_accessor const *ac);

#endif
