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
#include "libpdb/pdbp.h"

#if 0 /* macros in pdb.h */

void pdb_primitive_reference_initialize(pdb_primitive_reference * pr)
{
	addb_istore_reference_initialize(pr);
}

void pdb_primitive_reference_free_loc(
	pdb_primitive_reference * pr,
	char const		* file,
	int			  line)
{
	addb_istore_reference_free_loc(pr, file, line);
}
#endif

/**
 * @brief Create a reference to a primitive.
 *
 * @param pref	The uninitialized reference buffer
 * @param pr	NULL, or a pointer to a primitive
 * @param file	__FILE__ of calling context (inserted by
 *		the pdb_primitive_reference_from_primitive macro)
 * @param line	__LINE__ of calling context (inserted by
 *		the pdb_primitive_reference_from_primitive macro)
 */
void pdb_primitive_reference_from_primitive_loc(pdb_primitive_reference* pref,
                                                pdb_primitive const* pr,
                                                char const* file, int line) {
  if (pr != NULL)
    addb_istore_reference_from_data_loc(pref, &pr->pr_data, file, line);
  else
    addb_istore_reference_initialize(pref);
}

void pdb_primitive_reference_dup_loc(pdb_primitive_reference* pref,
                                     char const* file, int line) {
  addb_istore_reference_dup_loc(pref, file, line);
}
