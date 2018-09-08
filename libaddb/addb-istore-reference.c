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

/**
 * @brief Initialize a reference
 *
 *  Once this call completes, a later call to addb_istore_reference_free()
 *  is harmless, even if the data has not been written to by
 *  a call to addb_istore_read() or addb_istore_alloc().
 *
 * @param iref 	pointer to undefined data
 */
void addb_istore_reference_initialize(addb_istore_reference* iref) {
  if (iref != NULL) {
    iref->iref_td = NULL;
    ADDB_TILED_REFERENCE_INITIALIZE(iref->iref_tref);
  }
}

/**
 * @brief Release a reference.
 * @param iref 	part of a data reference created by an earlier
 * 		call to addb_istore_alloc() or addb_istore_read(),
 *		or initialized with addb_istore_reference_initialize().
 * @param file __FILE__ of calling context, filled in by
 * 	addb_istore_reference_free macro.
 * @param line __LINE__ of calling context, filled in by
 * 	addb_istore_reference_free macro.
 */
void addb_istore_reference_free_loc(addb_istore_reference* iref,
                                    char const* file, int line) {
  if (iref != NULL && iref->iref_td != NULL) {
    addb_tiled_free_loc(iref->iref_td, &iref->iref_tref, file, line);
    ADDB_TILED_REFERENCE_INITIALIZE(iref->iref_tref);
  }
}

/**
 * @brief Make a duplicate of a reference.
 * @param iref 	part of a data reference created by an earlier
 * 		call to addb_istore_alloc() or addb_istore_read(),
 *		or initialized with addb_istore_reference_initialize().
 * @param file	__FILE__ of calling code (inserted by
 *		addb_istore_reference_dup macro.)
 * @param line	__LINE__ of calling code (inserted by
 *		addb_istore_reference_dup macro.)
 */
void addb_istore_reference_dup_loc(addb_istore_reference* iref,
                                   char const* file, int line) {
  if (iref != NULL && iref->iref_td != NULL)
    addb_tiled_link_loc(iref->iref_td, &iref->iref_tref, file, line);
}

/**
 * @brief Create a reference from a piece of data.
 * @param iref 	reference to create
 * @param data 	data to create it from
 * @param file	__FILE__ of calling code (inserted by
 *		addb_istore_reference_from_data macro.)
 * @param line	__LINE__ of calling code (inserted by
 *		addb_istore_reference_from_data macro.)
 */
void addb_istore_reference_from_data_loc(addb_istore_reference* iref,
                                         addb_data const* data,
                                         char const* file, int line) {
  if (data == NULL || data->data_type != ADDB_DATA_ISTORE)
    addb_istore_reference_initialize(iref);
  else {
    addb_tiled_link_loc(data->data_iref.iref_td, &data->data_iref.iref_tref,
                        file, line);
    *iref = data->data_iref;
  }
}
