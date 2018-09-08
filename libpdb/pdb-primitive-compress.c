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
#include "libpdb/pdb.h"

/*
 * Compression
 * The database has a master database guid formed by using the
 * database ID (which never changes once the first primitive is written)
 * and the local id of zero.
 *
 * Thus the database guid for graphd is "boring mode" is:
 * 00000012400034568000000000000000
 *
 * To compress a GUID being inserted into any of the linkage fields we XOR
 * that guid with the master database guid and drop all the leading zeros.
 * Compressed GUIDs must always be padded to at least two bytes in length
 * For example:
 *
 * 00000012400034568000000000000000 xor 0000001240003456800000000000001d
 * =
 * 0000000000000000000000000000001d  --> 00 1d
 *
 * We store the length in 4 bits-per-linkage in the length field of the
 * primitive.
 * If the length is zero, the link doesn't exist and takes up no space.
 * Otherwise, the link exists and takes up (n+1) bytes.  Hence links may use
 * anywhere from 2 to 16 bytes of storage.  (The worst case compression might
 * require 16 bytes)
 *
 * In the above example, we would store the value 1 in the appropriate link
 * length nibble.
 *
 */

#define BASE_GUID_A(pr) (((pr)->pr_database_guid)->guid_a)
#define BASE_GUID_B(pr) (((pr)->pr_database_guid)->guid_b)

/*
 * Get the offset of the begining of a specific compressed guid
 */

size_t pdb_primitive_guid_offset(const pdb_primitive *pr, int linkage) {
  size_t offset;
  offset = PDB_PRIMITIVE_LINK_OFFSET(pr);
  switch (linkage) {
    case PDB_LINKAGE_ALL:
      offset += PDB_PRIMITIVE_LINK_LENGTH(pr, PDB_LINKAGE_GUID);
    case PDB_LINKAGE_GUID:
      offset += PDB_PRIMITIVE_LINK_LENGTH(pr, PDB_LINKAGE_SCOPE);
    case PDB_LINKAGE_SCOPE:
      offset += PDB_PRIMITIVE_LINK_LENGTH(pr, PDB_LINKAGE_LEFT);
    case PDB_LINKAGE_LEFT:
      offset += PDB_PRIMITIVE_LINK_LENGTH(pr, PDB_LINKAGE_RIGHT);
    case PDB_LINKAGE_RIGHT:
      offset += PDB_PRIMITIVE_LINK_LENGTH(pr, PDB_LINKAGE_TYPEGUID);
    case PDB_LINKAGE_TYPEGUID:;
  }
  return (offset);
}

/*
 * Uncompress a guid from a primitive
 */
void pdb_primitive_linkage_get_ptr(const pdb_primitive *pr, int link,
                                   graph_guid *g) {
  int len;
  size_t offset = pdb_primitive_guid_offset(pr, link);

  len = PDB_PRIMITIVE_LINK_LENGTH(pr, link) - 1;

  g->guid_a = 0;
  g->guid_b = 0;

  for (; len >= 8; len--) {
    g->guid_a <<= 8;
    g->guid_a |= *(unsigned char *)(PDB_PTR(pr) + offset);
    offset++;
  }
  for (; len >= 0; len--) {
    g->guid_b <<= 8;
    g->guid_b |= *(unsigned char *)(PDB_PTR(pr) + offset);
    offset++;
  }

  g->guid_a ^= BASE_GUID_A(pr);
  g->guid_b ^= BASE_GUID_B(pr);
}

/*
 * Compress and insert a guid into a primitive.
 * buffer must be at least 16 bytes long, we return the number of
 * bytes actually used.
 */
int pdb_primitive_linkage_set_ptr(pdb_primitive *pr, const graph_guid *g,
                                  unsigned char *buffer) {
  graph_guid newg;
  int i;
  int offset = 0;

  newg.guid_a = g->guid_a ^ BASE_GUID_A(pr);
  newg.guid_b = g->guid_b ^ BASE_GUID_B(pr);

  for (i = 15; i >= 8; i--) {
    if (newg.guid_a & (0xffull << ((i - 8) * 8))) goto have_count;
  }
  for (; i >= 0; i--) {
    if (newg.guid_b & (0xffull << (i * 8))) goto have_count;
  }

have_count:
  if (i < 1) i = 1;

  for (; i >= 8; i--) {
    buffer[offset] = (newg.guid_a >> ((i - 8) * 8)) & 0xff;
    offset++;
  }

  for (; i >= 0; i--) {
    buffer[offset] = (newg.guid_b >> (i * 8)) & 0xff;
    offset++;
  }
  return offset;
}

size_t pdb_primitive_len(const pdb_primitive *pr) {
  size_t o;
  o = pdb_primitive_guid_offset(pr, PDB_LINKAGE_ALL);
  o += pdb_primitive_has_generation(pr) ? 10 : 0;

  return o;
}

/*
 * Zero fields of a primitive that might get read before the first
 * time they are written.
 */
void pdb_primitive_zero(pdb_primitive *pr) {
  pdb_primitive_bits_set(pr, 0);
  pdb_set3(PDB_PTR(pr) + PDB_PRIMITIVE_LINKAGE_BITS_OFFSET, 0);
}

unsigned long pdb_primitive_link_bitmask(pdb_primitive *pr) {
  int i;
  unsigned long o = 0;

  for (i = 0; i < PDB_LINKAGE_N; i++) {
    if (pdb_primitive_has_linkage(pr, i)) o |= 1 << i;
  }
  return o;
}
