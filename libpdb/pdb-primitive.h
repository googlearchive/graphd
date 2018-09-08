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
#ifndef PDB_PRIMITIVE_H
#define PDB_PRIMITIVE_H

#include "libaddb/addb-scalar.h"

/*
 * Version two primitive tuple in the database
 * 0..5		timestamp (6)
 * 6		bits and links:
 * 			live 			0x1
 * 			archival		0x2
 * 			txstart			0x4
 * 			prev-data		0x8
 * 			has-value		0x10
 * 			has-name		0x20
 * 7..9		lengths
 * 			bits 0-3	typegiud length (TL)
 * 			bits 4-7	right link length (RL)
 * 			bits 8-11	left link length (LL)
 * 			bits 12-15	scope link length (SL)
 * 			bits 16-19	myguid link length (GL)
 *
 * 10			value type
 * if has-name		<--- PDB_PRIMITIVE_NAME_OFFSET
 * 	11..12 offset of first byte after name data (NO)
 * 	13..NO name data
 * else NO=11
 * if has-value		<---- PDB_PRIMITIVE_VALUE_OFFSET
 * 	NO..NO+2 offset of first byte after value data (VO)
 * 	NO+3...VO
 * else VO=NO
 *			<---- PDB_PRIMITIVE_LINKS_OFFSET
 * VO..(VO+TL-1)	compressed typeguid bits
 * $$..(+RL-1)		compressed left guid bits
 * $$..(+LL-1)		compressed right guid bits
 * $$..(+SL-1)		compressed scope guid bits
 * $$..(+GL-1)    	compressed external guid bits
 * (VO+TL+RL+LL+SL)..+4	prev_sequence
 * $$+1..$$+5		generation number
 *
 * see pdb-primitive-compress.c for information on GUID compression.
 */


/*
 * ADDB glue
 */

#define pdb_get8(ptr) ADDB_GET_U8(ptr)
#define pdb_set8(ptr, val) ADDB_PUT_U8(ptr, val)

#define pdb_get6(ptr) ADDB_GET_U6(ptr)
#define pdb_set6(ptr, val) ADDB_PUT_U6(ptr, val)

#define pdb_get5(ptr) ADDB_GET_U5(ptr)
#define pdb_set5(ptr, val) ADDB_PUT_U5(ptr, val)

#define pdb_get4(ptr) ADDB_GET_U4(ptr)
#define pdb_set4(ptr, val) ADDB_PUT_U4(ptr, val)

#define pdb_get3(ptr) ADDB_GET_U3(ptr)
#define pdb_set3(ptr, val) ADDB_PUT_U3(ptr, val)

#define pdb_get2(ptr) ADDB_GET_U2(ptr)
#define pdb_set2(ptr, val) ADDB_PUT_U2(ptr, val)

/*
 * There are PDB_LINKAGE_N links numbers from 0 to PDB_LINKAGE_N - 1.
 * We store the primitive GUID as if it were the PDB_LINKAGE_Nth link.
 */
#define PDB_LINKAGE_GUID PDB_LINKAGE_N
#define PDB_LINKAGE_ALL (PDB_LINKAGE_N + 1)

/* Pointer to the start of a primitive in ram */
#define PDB_PTR(pr) ((pr)->pr_data.data_memory)

/* Size of all the fixed fields in a primitive */
#define PDB_PRIMITIVE_TIMESTAMP_SIZE 6
#define PDB_PRIMITIVE_LINKAGE_BITS_SIZE 3
#define PDB_PRIMITIVE_BITS_SIZE 1
#define PDB_PRIMITIVE_VALUETYPE_SIZE 1
#define PDB_PRIMITIVE_VALUELEN_SIZE 3
#define PDB_PRIMITIVE_NAMELEN_SIZE 2

#define PDB_PRIMITIVE_BITS_PER_LINK 4
#define PDB_PRIMITIVE_BITS_PER_LINK_MASK 0xf

/* Location of all the fixed fields in a primitive */
#define PDB_PRIMITIVE_TIMESTAMP_OFFSET 0
#define PDB_PRIMITIVE_BITS_OFFSET 6
#define PDB_PRIMITIVE_LINKAGE_BITS_OFFSET 7
#define PDB_PRIMITIVE_VALUETYPE_OFFSET 10

/*
 * Offsets and lengths for the name field _if_ it exists
 */

#define PDB_PRIMITIVE_NAMELEN_OFFSET 11
#define PDB_PRIMITIVE_NAME_OFFSET 13
#define PDB_PRIMITIVE_NAME_LENGTH(pr) \
  pdb_get2(PDB_PTR(pr) + PDB_PRIMITIVE_NAMELEN_OFFSET)

/* What is the maximum length of a compressed GUID */
#define PDB_PRIMITIVE_GUID_MAXLEN 16

/*
 * Offsets and lengths for the value field, _if_ it exists
 */

#define PDB_PRIMITIVE_VALUELEN_OFFSET(pr)                            \
  (PDB_PRIMITIVE_HAS_NAME(pr)                                        \
       ? (PDB_PRIMITIVE_NAME_OFFSET + PDB_PRIMITIVE_NAME_LENGTH(pr)) \
       : PDB_PRIMITIVE_NAMELEN_OFFSET)

#define PDB_PRIMITIVE_VALUE_OFFSET(pr) \
  (PDB_PRIMITIVE_VALUELEN_OFFSET(pr) + PDB_PRIMITIVE_VALUELEN_SIZE)

#define PDB_PRIMITIVE_VALUE_LENGTH(pr) \
  pdb_get3(PDB_PTR(pr) + PDB_PRIMITIVE_VALUELEN_OFFSET(pr))

/*
 * Do we have a name or value?
 */
#define PDB_PRIMITIVE_HAS_NAME(pr) \
  (pdb_primitive_bits_get(pr) & PDB_PRIMITIVE_BIT_HAS_NAME)

#define PDB_PRIMITIVE_HAS_VALUE(pr) \
  (pdb_primitive_bits_get(pr) & PDB_PRIMITIVE_BIT_HAS_VALUE)

/* Where do the links start */
#define PDB_PRIMITIVE_LINK_OFFSET(pr)                                      \
  (PDB_PRIMITIVE_HAS_VALUE(pr)                                             \
       ? (PDB_PRIMITIVE_VALUE_OFFSET(pr) + PDB_PRIMITIVE_VALUE_LENGTH(pr)) \
       : PDB_PRIMITIVE_VALUELEN_OFFSET(pr))

/* How many bytes does a particular link use? 0 means it doesn't exist */
#define PDB_PRIMITIVE_LINK_LENGTH(pr, link) \
  PDB_PRIMITIVE_LENGTH_COOK(PDB_PRIMITIVE_RAW_LINK_LENGTH(pr, link))

#define PDB_PRIMITIVE_RAW_LINK_LENGTH(pr, link)                  \
  ((pdb_get3(PDB_PTR(pr) + PDB_PRIMITIVE_LINKAGE_BITS_OFFSET) >> \
    (link * PDB_PRIMITIVE_BITS_PER_LINK)) &                      \
   PDB_PRIMITIVE_BITS_PER_LINK_MASK)

#define PDB_PRIMITIVE_LENGTH_COOK(l) (((l) == 0) ? 0 : (l) + 1)

#define PDB_PRIMITIVE_LENGTH_FREEZE(l) (((l) == 0) ? 0 : (l)-1)

/* Flags in the bits field */
#define PDB_PRIMITIVE_BIT_LIVE 0x1
#define PDB_PRIMITIVE_BIT_ARCHIVAL 0x2
#define PDB_PRIMITIVE_BIT_TXSTART 0x4
#define PDB_PRIMITIVE_BIT_PREVIOUS 0x8
#define PDB_PRIMITIVE_BIT_HAS_VALUE 0x10
#define PDB_PRIMITIVE_BIT_HAS_NAME 0x20

/* What is the smallest size of a primitive */
#define PDB_PRIMITIVE_SIZE_MIN PDB_PRIMITIVE_NAMELEN_OFFSET

/* Macros for accessing the bits field */
#define pdb_primitive_bits_get(pr) \
  (*(unsigned char *)(PDB_PTR(pr) + PDB_PRIMITIVE_BITS_OFFSET))

#define pdb_primitive_bits_set(pr, val) \
  ((*(unsigned char *)(PDB_PTR(pr) + PDB_PRIMITIVE_BITS_OFFSET)) = val)

#define pdb_primitive_bits_pointer(pr) (PDB_PTR(pr) + PDB_PRIMITIVE_BITS_OFFSET)

#define pdb_primitive_is_archival(pr) \
  (!!(pdb_primitive_bits_get(pr) & PDB_PRIMITIVE_BIT_ARCHIVAL))

#define pdb_primitive_is_live(pr) \
  (!!(pdb_primitive_bits_get(pr) & PDB_PRIMITIVE_BIT_LIVE))

#define pdb_primitive_is_txstart(pr) \
  (!!(pdb_primitive_bits_get(pr) & PDB_PRIMITIVE_BIT_TXSTART))

/* macros for accessing the timestamp field */
#define pdb_primitive_timestamp_get(pr) \
  pdb_get6(PDB_PTR(pr) + PDB_PRIMITIVE_TIMESTAMP_OFFSET)

#define pdb_primitive_timestamp_set(pr, val) \
  pdb_set6(PDB_PTR(pr) + PDB_PRIMITIVE_TIMESTAMP_OFFSET, val)

/* Macros for accessing the valuetype field */
#define pdb_primitive_valuetype_get(pr) \
  (*(unsigned char *)(PDB_PTR(pr) + PDB_PRIMITIVE_VALUETYPE_OFFSET))

#define pdb_primitive_valuetype_set(pr, val) \
  ((*(PDB_PTR(pr) + PDB_PRIMITIVE_VALUETYPE_OFFSET)) = val)

/*
 * Macros for grabbing the name of a primitive. These are only
 * valid if PDB_PRIMITIVE_HAS_NAME(pr) is true.
 */

#define pdb_primitive_name_pointer(pr) (PDB_PTR(pr) + PDB_PRIMITIVE_NAME_OFFSET)

#define pdb_primitive_name_get_size(pr) \
  ((PDB_PRIMITIVE_HAS_NAME(pr)) ? (PDB_PRIMITIVE_NAME_LENGTH(pr)) : 0)

#define pdb_primitive_name_get_memory(pr) pdb_primitive_name_pointer(pr)

/*
 * Macros for grabbing the value of a primitive. These are only
 * valid if PDB_PRIMITIVE_HAS_VALUE(pr) is true.
 */

#define pdb_primitive_value_pointer(pr) \
  (PDB_PTR(pr) + PDB_PRIMITIVE_VALUE_OFFSET(pr))

#define pdb_primitive_value_get_size(pr) \
  ((PDB_PRIMITIVE_HAS_VALUE(pr)) ? PDB_PRIMITIVE_VALUE_LENGTH(pr) : 0)

#define pdb_primitive_value_get_memory(pr) pdb_primitive_value_pointer(pr)

/*
 * Test or get linkages based on linkage number
 */
#define pdb_primitive_has_linkage(pr, lg) \
  (PDB_PRIMITIVE_LINK_LENGTH(pr, lg) != 0)

#define pdb_primitive_linkage_get(pr, linkage, g) \
  pdb_primitive_linkage_get_ptr(pr, linkage, &(g))

/*
 * macros for all the basic linkage operations:
 * pdb_primitive{has,get,set,eq}{guid,left,right,typeguid,scope}
 */

/*
 * These macros are used internally for primitives which
 * have a guid that doesn't match our database ID.
 */
#define pdb_primitive_has_external_guid(pr) \
  pdb_primitive_has_linkage(pr, PDB_LINKAGE_GUID)

#define pdb_primitive_get_external_guid(pr, val) \
  pdb_primitive_linkage_get_ptr(pr, PDB_LINKAGE_GUID, &(val))

/*
 * GUID operations
 */
#define pdb_primitive_guid_get(pr, val) ((val) = (pr)->pr_guid)

#define pdb_primitive_has_guid(x) (true)

#define pdb_primitive_guid_eq(pr, tmp, val) \
  (pdb_primitive_has_guid(pr) &&            \
   (pdb_primitive_guid_get(pr, (tmp)), GRAPH_GUID_EQ(val, tmp)))

/*typeguid operations */
#define pdb_primitive_has_typeguid(pr) \
  pdb_primitive_has_linkage(pr, PDB_LINKAGE_TYPEGUID)

#define pdb_primitive_typeguid_get(pr, val) \
  pdb_primitive_linkage_get_ptr(pr, PDB_LINKAGE_TYPEGUID, &(val))

#define pdb_primitive_typeguid_eq(pr, tmp, val) \
  (pdb_primitive_has_typeguid(pr) &&            \
   (pdb_primitive_typeguid_get(pr, (tmp)), GRAPH_GUID_EQ(val, tmp)))

/* left operations */
#define pdb_primitive_has_left(pr) \
  pdb_primitive_has_linkage(pr, PDB_LINKAGE_LEFT)

#define pdb_primitive_left_get(pr, val) \
  pdb_primitive_linkage_get_ptr(pr, PDB_LINKAGE_LEFT, &(val))

#define pdb_primitive_left_eq(pr, tmp, val) \
  (pdb_primitive_has_left(pr) &&            \
   (pdb_primitive_left_get(pr, (tmp)), GRAPH_GUID_EQ(val, tmp)))

/*
 * right operations */
#define pdb_primitive_has_right(pr) \
  pdb_primitive_has_linkage(pr, PDB_LINKAGE_RIGHT)

#define pdb_primitive_right_get(pr, val) \
  pdb_primitive_linkage_get_ptr(pr, PDB_LINKAGE_RIGHT, &(val))

#define pdb_primitive_right_eq(pr, tmp, val) \
  (pdb_primitive_has_right(pr) &&            \
   (pdb_primitive_right_get(pr, (tmp)), GRAPH_GUID_EQ(val, tmp)))

/* scope operations */
#define pdb_primitive_has_scope(pr) \
  pdb_primitive_has_linkage(pr, PDB_LINKAGE_SCOPE)

#define pdb_primitive_scope_get(pr, val) \
  pdb_primitive_linkage_get_ptr(pr, PDB_LINKAGE_SCOPE, &(val))

#define pdb_primitive_scope_eq(pr, tmp, val) \
  (pdb_primitive_has_scope(pr) &&            \
   (pdb_primitive_scope_get(pr, (tmp)), GRAPH_GUID_EQ(val, tmp)))

/*
 * Operations on generation and linkage
 */

/* Calculate pointers to the linkeage and generation fields, assuming they
 * exist.
 */
#define PDB_PRIMITIVE_LINEAGE_POINTER(pr) \
  (pdb_primitive_guid_offset(pr, PDB_LINKAGE_ALL) + PDB_PTR(pr))

#define PDB_PRIMITIVE_GENERATION_POINTER(pr) \
  (pdb_primitive_guid_offset(pr, PDB_LINKAGE_ALL) + PDB_PTR(pr) + 5)

#define pdb_primitive_has_generation(pr) \
  (!!(pdb_primitive_bits_get(pr) & PDB_PRIMITIVE_BIT_PREVIOUS))

#define pdb_primitive_set_generation_bit(pr) \
  (pdb_primitive_bits_set(                   \
      pr, pdb_primitive_bits_get(pr) | PDB_PRIMITIVE_BIT_PREVIOUS))

#define pdb_primitive_has_previous(pr) pdb_primitive_has_generation(pr)

#define pdb_primitive_lineage_get(pr) \
  pdb_get5(PDB_PRIMITIVE_LINEAGE_POINTER(pr))

#define pdb_primitive_lineage_set(pr, val) \
  pdb_set5(PDB_PRIMITIVE_LINEAGE_POINTER(pr), val)

#define pdb_primitive_generation_get(pr) \
  pdb_get5(PDB_PRIMITIVE_GENERATION_POINTER(pr))

#define pdb_primitive_generation_set(pr, val) \
  pdb_set5(PDB_PRIMITIVE_GENERATION_POINTER(pr), val)

/* random stuff */

#define pdb_primitive_is_node(pr) \
  (!pdb_primitive_has_left(pr) && !pdb_primitive_has_right(pr))

#define pdb_primitive_is_link(pr) (!pdb_primitive_is_node(pr))

/* Calculate the real length of this primitive */

#endif
