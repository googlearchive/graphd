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
/**
 * @brief The version of the on-disk format
 *
 *  If you are running a graphd with format version N and
 *  you wish to upgrade to a new graphd there are two possibilities:
 *
 *  1. the format version of the new graphd is N
 *
 *	The new graphd can run with the database files of the old.
 *
 *  2. or the format version of the new graphd is different from N
 *
 *	The new graphd is not binary compatible with the old.
 *	You must dump data from the old graphd and restore it into
 *	the new.
 *
 * To obtain the format version, invoke graphd as "graphd -w"
 *
 * Version History
 *
 *	2	gd-dev-15	Initial (because starting at 1 is bad luck)
 *	3	gd-dev-16	hmap buckets sorted
 *	4			hmap used for vip
 *	5			new GUIDs
 *	6			largefiles
 *	7	gd-dev-27	hash function change
 *	8	gd-dev-33	istore bug fix, largefile length in gmap,
 *transaction bit
 *	9	gd-dev-34	istore compression
 *	10	gd-dev-35	BMAPs. Use them for prefix hashing.
 *	11	gd-dev-36	Removed redundant "next offset" from marker file
 *	12	gd-dev-38	change to the prefix hash function
 *	13	gd-dev-43	append-style marker file, better numerical
 *hashing
 *	14	gd-dev-44	split marker file into separate horizon/nextID
 *files
 * 	15			bgmaps
 * 	16			more istore compression
 * 	17	         	bitmap versioned primitives
 * 				write the version number in the header
 * 				value bins
 * 	18	gd-dev-47	fix hmap normalization
 * 	19			fix liveness bitmap
 * 	20	gd-dev-49	Add numerical binning support
 */

/*
 * The GRAPHS_FORMAT_VERSION string must be an integer
 */
#define GRAPHD_FORMAT_VERSION "21"
