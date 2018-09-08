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
#ifndef ADDB_GMAP_FILE_H
#define ADDB_GMAP_FILE_H

#include "libaddb/addb-scalar.h"

/*  Offsets, magic numbers, and sizes within a gmap partition file
 *
 *  GMAP partition file:
 *  0
 *  +----------------------+--------------------------+
 *  | 4 byte magic number  | 8 byte virtual file size |
 *  +----------------------+--------------------------+
 *  | 20 bytes reserved, initialized to zero.         |
 *  +---------------+----........----+-----------+----+-----+--------+---+
 *  | per-size freelist heads  (1..34)   #32     |    #33   |    #34 |
 *  +----------------+-----+-----+-----+---------+----------+--------+  fixed-
 *  | offset 0 | offset 1 | offset 2 |           | offset 16,777,215 |  size
 *  +-------+--+----------+-------+--+--- .... --+-------------------+  index
 *  ,-------'                ,----'                                 (5*16=80 MB)
 *  V                        V
 *  +-----------------------+-------+---- .... -----+-- ------+------+ overflow
 *  | data0                 | data2 |               | data123 | ...  |  slots
 *  +-----------------------+-------+---- .... -----+---------+------+
 *  :                                                                :
 *  |     |      |         physical file expands         |       |   |
 *  |     V      V               as needed               V       V   |
 *  +----------------------------------------------------------------+
 *
 *  INDEX TABLE
 *  ===========
 *
 *  In the 80 MB index table, slots are either single-valued solutions
 *                      or pointers to multi-element
 *     +-----------+    tables ("multis") elsewhere     +------------+
 *     |0=1        |    in the file.                    |0=*         |
 *     |1=1        |                                    |1=* S I Z E |
 *     |2=1        |    For sizes >= 2, the size is     |2=*  A   S  |
 *     |3=1        |    stored as a base-2 exponent     |3=* E X P 2 |
 *     |4=1        |    in the index pointer.           |4=*  1..34  |
 *     |5=1        |                                    |5=*         |
 *     +-----------+    For size=1, the size isn't the  +------------+
 *     |6          |    base-2 exponent (i.e. 0) but    |6           |
 *     :  D A T A  :    63; 0 is reserved for empty     :   D A T A  |
 *     |39         |    slots.                          |39          |
 *     +-----------+                                    +------------+
 *     Single           Unused slots are recycled in     Multi reference
 *                      per-size free lists.
 *
 *
 *     +-----------+
 *     |0=0        |
 *     |1=1        |
 *     |2=1        |
 *     |3=1        |
 *     |4=1        |
 *     |5=1        |
 *     +-----------+
 *     |6          |
 *     :  X  X  X  :
 *     |39         |
 *     +-----------+
 *     External reference
 *
 *  The offsets in multi references are stored as multiples of 10 bytes,
 *  or two pairs, since the arrays are always at least two pairs in size
 *  and come only in sizes that are powers of two.
 *
 *  That means that one partition table can address 160 GB of
 *  linear storage behind 16M indices, or 2048 result indices on
 *  average per index.
 *
 *  The index table records live in tile slabs of 5*64k bytes each.
 *
 *  An external reference means that the data for that array has been
 *  moved out of the gmap into its own file.  By convention the file
 *  will be in large/<id>.glf.  The file will be "almost" flat. It stores
 *  a 80 byte header followed by arbitrerily many 5 byte datums. See
 *  addb-bigfile.c fore more details.
 *
 *  MULTI TABLE
 *  ===========
 *
 *  The multis are dynamically sized arrays of 34-bit indices, each padded
 *  to 40 bits (5 bytes).
 *
 *  Control information about the size of the array is encoded in the
 *  top 6 bits of its index pointer.
 *
 *  Control information about the current array size is encoded in the
 *  top bits and value of the last possible entry.
 *
 *   +------------+-...-+------------+     +------------+-...-+------------+
 *   |0=0         |     |0=1         |     |0=0         |     |0=0         |
 *   |1=0         |     |1=0         |     |0=0         |     |1=0         |
 *   |2=0         |     |2=0         |     |0=0         |     |2=0         |
 *   |3=0         |     |3=0         |     |3=0         |     |3=0         |
 *   |4=0         |     |4=0         |     |4=0         |     |4=0         |
 *   |5=0         |     |5=0         |     |5=0         |     |5=0         |
 *   +------------+     +------------+     +------------+     +------------+
 *   |6           |     |6  L A S T  |     |6           |     |6           |
 *   : D A T A    :     :   ELEMENT  |     |  D A T A   |     :  D A T A   :
 *   |39          |     |39 I N D E X|     |39          |     |39          |
 *   +------------+-...-+------------+     +------------+-...-+------------+
 *    partially filled multi-table                 full multi-table
 *
 *  If an array is in the freelist, the free list "next" pointer (same
 *  as the index pointer) is kept in the first element of the array.
 *
 */

/*  The first four bytes of each ADDB file are a "magic number" that is
 *  unique for each file type and tells tools and confused system
 *  administrators what they're dealing with.
 */

#define ADDB_GMAP_MAGIC "ag4p" /* Addb GMAP v4 Partition */

/*  Bytes 4..7 of the gmap partition header are the partition-local
 *  index of the first unallocated slot ("next"), as a big-endian
 *  4-byte binary number.
 *
 *  This index runs from 0 (the partition is completely empty) to
 *  16,777,216 (the partition is completely full).
 */

#define ADDB_GMAP_ENTRY_SIZE 5

#define ADDB_GMAP_VSIZE_OFFSET 4
#define ADDB_GMAP_VSIZE_SIZE 8
#define ADDB_GMAP_RESERVED_OFFSET \
  (ADDB_GMAP_VSIZE_OFFSET + ADDB_GMAP_VSIZE_SIZE)

#define ADDB_GMAP_RESERVED_SIZE (32 - ADDB_GMAP_RESERVED_OFFSET)

#define ADDB_GMAP_FREE_BASE 32
#define ADDB_GMAP_FREE_ENTRY_SIZE ADDB_GMAP_ENTRY_SIZE
#define ADDB_GMAP_FREE_ENTRY_N 34
#define ADDB_GMAP_FREE_SIZE (ADDB_GMAP_FREE_ENTRY_SIZE * ADDB_GMAP_FREE_ENTRY_N)

#define ADDB_GMAP_FREE_OFFSET(e) \
  (ADDB_GMAP_FREE_BASE + ((e)-1) * ADDB_GMAP_FREE_ENTRY_SIZE)

#define ADDB_GMAP_FREE_POINTER(b, e)            \
  ((unsigned char *)(b) + ADDB_GMAP_FREE_BASE + \
   ((e)-1) * ADDB_GMAP_FREE_ENTRY_SIZE)

#define ADDB_GMAP_HEADER_SIZE (ADDB_GMAP_FREE_BASE + ADDB_GMAP_FREE_SIZE)

#define ADDB_GMAP_SINGLE_OFFSET ADDB_GMAP_HEADER_SIZE
#define ADDB_GMAP_SINGLE_ENTRY_OFFSET(i) \
  (ADDB_GMAP_SINGLE_OFFSET + ADDB_GMAP_ENTRY_SIZE * (i))
#define ADDB_GMAP_SINGLE_ENTRY_N (16ull * 1024 * 1024)
#define ADDB_GMAP_SINGLE_SIZE (ADDB_GMAP_ENTRY_SIZE * ADDB_GMAP_SINGLE_ENTRY_N)

#define ADDB_GMAP_MULTI_OFFSET (ADDB_GMAP_SINGLE_OFFSET + ADDB_GMAP_SINGLE_SIZE)
#define ADDB_GMAP_MULTI_FACTOR (2 * ADDB_GMAP_ENTRY_SIZE)
#define ADDB_GMAP_MULTI_ENTRY_OFFSET(ival) \
  (ADDB_GMAP_MULTI_OFFSET +                \
   ((ival) & ((1ull << 34) - 1)) * ADDB_GMAP_MULTI_FACTOR)

#endif /* ADDBP_GMAP_FILE_H */
