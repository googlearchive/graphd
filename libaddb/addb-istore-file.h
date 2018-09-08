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
#ifndef ADDB_ISTORE_FILE_H
#define ADDB_ISTORE_FILE_H

/*  Offsets, magic numbers, and sizes within an istore partition file
 *
 *  +----------------+-----------------+
 *  | magic number   | first free slot |                                header
 *  +----------------+-----------------+
 *  +----------+----------+----------+--- .... --+-------------------+  fixed-
 *  | offset 0 | offset 1 | offset 2 |           | offset 16,777,215 |  size
 *  +-------+--+-------+--+--------+-+--- .... --+-------------------+  index
 * padding  `--------. |           |                                   (64 MB)
 * to multiple of   /  `-----,     `-----------------,
 * page size       v        v                       v
 *  +--------------+--------+------------ .... -----+-- ------+------+
 *  |  d a t a  0  | data 1 |   d   a   t   a   2   |  data 3 | ...  |  data
 *  +--------------+--------+------------ .... -----+---------+------+  (up to
 *  |  d             a             t             a               N   |  34 GB)
 *  +----------------------------------------------------------------+
 *  :     |      |         physical file expands         |       |   :
 *  :     V      V               as needed               V       V   :
 *  +----------------------------------------------------------------+
 *
 *
 *  In addition to the up to 1024 partitions, there is also a special
 *  marker file that remembers the highest local ID.  The marker file
 *  is updated only after all the data has successfully been flushed
 *  to disk.  If partitions and marker file disagree, the partitions
 *  can be safely rolled back to the marker file's state.
 *
 *  +-------------------+
 *  | magic number (4)  |
 *  +-------------------+-------------+
 *  | highest offset in partition (8) |
 *  +-----------------------+---------+
 *  | next_id (5)           |
 *  +-----------------------+
 *  | horizon (5)           |
 *  +-----------------------+
 */

/*  The first four bytes of each ADDB file are a "magic number" that is
 *  unique for each file type and tells tools and system administrators
 *  what they're dealing with.
 */

#define ADDB_ISTORE_MAGIC "ai3p"         /* Addb Istore v3 Partition */
#define ADDB_ISTORE_NEXT_MAGIC "ai1n"    /* Addb Istore v1 next Marker    */
#define ADDB_ISTORE_HORIZON_MAGIC "ai1h" /* Addb Istore v1 horizon Marker */

/*  Bytes 4..7 of the istore partition header are the partition-local
 *  index of the first unallocated slot ("next"), as a big-endian
 *  4-byte binary number.
 *
 *  This index runs from 0 (the partition is completely empty) to
 *  16,777,216 (the partition is completely full).  (16m is 2^24,
 *  so that fits conveniently into our 32-bit integer.)
 */

#define ADDB_ISTORE_NEXT_OFFSET 4
#define ADDB_ISTORE_NEXT_SIZE 4
#define ADDB_ISTORE_NEXT_POINTER(mem) \
  ((unsigned char *)(mem) + ADDB_ISTORE_NEXT_OFFSET)

#define ADDB_ISTORE_HEADER_SIZE \
  (ADDB_ISTORE_NEXT_OFFSET + ADDB_ISTORE_NEXT_SIZE)

/*  After the header is a large index table of offsets into the
 *  remaining file, divided by 8, relative to the start of the
 *  data segment - 8.
 *
 *  (Tiles are involved when mapping the file into memory;
 *  here, all that's interesting is that the index base is
 *  actually set back into the index table a little bit, so that
 *  "0" is never a valid index and can serve as a redundant
 *  placeholder.)
 */

#define ADDB_ISTORE_INDEX_N (16ull * 1024 * 1024)
#define ADDB_ISTORE_INDEX_SIZE 4
#define ADDB_ISTORE_INDEX_OFFSET_BASE ADDB_ISTORE_HEADER_SIZE
#define ADDB_ISTORE_INDEX_OFFSET(id) \
  (ADDB_ISTORE_INDEX_OFFSET_BASE +   \
   ((id) % ADDB_ISTORE_INDEX_N) * ADDB_ISTORE_INDEX_SIZE)

/*  If the istore partition is too large to be mapped into memory,
 *  only the index table is mapped, and the data storage further down
 *  in the file is accessed using 64k+ tiles.
 *
 *  They're managed in addb-tiled.c, and addressed using a
 *  "tile index".  Getting from an IXUNIT offset to a tile index
 *  is easy -- just divide by 2^13  (2^16 is 64k, 2^3 is already
 *  in the implicit *8 factor for the IX units.)
 *
 *  Trade-offs for the tile size are similar to file system block
 *  size tradeoffs:
 *
 *  - must be a multiple of getpagesize(), usually 32k
 *
 *  - small objects don't span tile boundaries, so we lose
 *    small-object-size/2 on average at the end.
 *
 *  - large objects are stored in multiples of the tile-size, so we
 *    lose ADDB_ISTORE_TILE_SIZE/2 on average per large object.
 */
#define ADDB_ISTORE_TILE_SIZE (32ul * 1024)

#define ADDB_ISTORE_DATA_OFFSET_0 \
  (ADDB_ISTORE_TILE_SIZE + (ADDB_ISTORE_INDEX_N * ADDB_ISTORE_INDEX_SIZE))

/*  The base for calculating the byte offsets is recessed 8 bytes into
 *  the empty space behind the index, so that "0" is never a valid offset.
 */
#define ADDB_ISTORE_DATA_OFFSET_BASE (ADDB_ISTORE_DATA_OFFSET_0 - 8)

/*  The things in the index table are called IXOFFSETs.  Here are
 *  some conversion macros between IXOFFSETs and byte offsets:
 */

#define ADDB_ISTORE_IXOFFSET_TO_BYTES(off) \
  (ADDB_ISTORE_DATA_OFFSET_BASE + (unsigned long long)(off)*8)

#define ADDB_ISTORE_IXOFFSET_FROM_BYTES(bytes) \
  (((bytes)-ADDB_ISTORE_DATA_OFFSET_BASE) / 8)

#endif /* ADDBP_ISTORE_FILE_H */
