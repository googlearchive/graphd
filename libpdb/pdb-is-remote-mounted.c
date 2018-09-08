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

#include <stdbool.h>
#include <stdio.h>

#ifndef PDB_HAS_STATFS
#if linux
#define PDB_HAS_STATFS 1
#endif
#endif

#ifdef PDB_HAS_STATFS
#include <sys/vfs.h>

/*  I don't want this to depend on linux, but I can't
 *  seem to find a place for this magic number outside
 *  of /usr/include/linux/.  Help?
 */
#ifndef NFS_SUPER_MAGIC
#define NFS_SUPER_MAGIC 0x6969
#endif

#endif

/**
 * @brief Is a file remote mounted?
 *
 *  	This is an optional guard against accidentally running
 * 	graphd on an NFS-mounted database.
 *
 * @param  name	Name of a file or directory of the
 *			graphd database
 * @return true if the file is remote mounted (and shouldn't
 *	be used as the database), false if it's local or we can't tell.
 */

bool pdb_is_remote_mounted(char const* pathname) {
#if PDB_HAS_STATFS

  struct statfs fs;

  if (statfs(pathname, &fs) != 0) return false;

  /*  Add magic numbers of other file systems you
   *  don't want to run on here.
   */
  return fs.f_type == NFS_SUPER_MAGIC;

#endif
  return false;
}
