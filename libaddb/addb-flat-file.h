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
#ifndef ADDB_FLAT_FILE_H
#define ADDB_FLAT_FILE_H

/*  Offsets, magic numbers, and sizes within a "flat" file
 *
 *  +----------------+
 *  | magic number   | header
 *  +----------------+
 *  +----------------------------------+
 *  |     d     a     t    a           |
 *  +----------------------------------+
 */

/*  The first four bytes of each ADDB file are a "magic number" that is
 *  unique for each file type and tells tools and confused system
 *  administrators what they're dealing with.
 */

#define ADDB_FLAT_MAGIC "afl2" /* Addb Flat v1 */

#define ADDB_FLAT_HEADER_SIZE 4
#define ADDB_FLAT_DATA_OFFSET ADDB_FLAT_HEADER_SIZE

#endif /* ADDBP_FLAT_FILE_H */
