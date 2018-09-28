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

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

#define PDB_ISTORE_TILE_SIZE_DEFAULT (32 * 1024ul)
#define PDB_GMAP_TILE_SIZE_DEFAULT (32 * 1024ul)

#ifndef PDB_AVAILABLE_MEMORY
#define PDB_AVAILABLE_MEMORY (1024ull * 1024 * 1024)
#endif

#ifndef PDB_MEMORY_MAP_SLOTS
#define PDB_MEMORY_MAP_SLOTS (64 * 1024ull)
#endif

#if __APPLE__ || __FreeBSD__

#ifndef USE_SYSCTL
#define USE_SYSCTL 1
#endif

#else

#ifndef USE_SYSINFO
#define USE_SYSINFO 1
#endif

#ifndef USE_SLASH_PROC
#define USE_SLASH_PROC 1
#endif

#endif

#if USE_SYSCTL

#include <sys/types.h>
#include <sys/sysctl.h>

static unsigned long long pdb_available_memory(cl_handle *cl) {
  int name[2];
  size_t memsize;
  size_t memsize_n;

  name[0] = CTL_HW;
  name[1] = HW_USERMEM;

  memsize = 0;
  memsize_n = sizeof(memsize);

  if (sysctl(name, 2, (void *)&memsize, &memsize_n, NULL, 0)) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "pdb: sysctl fails: %s - guessing "
           "at %llu bytes available memory.",
           strerror(errno), PDB_AVAILABLE_MEMORY);
    return PDB_AVAILABLE_MEMORY;
  }

  cl_log(cl, CL_LEVEL_DEBUG,
         "sysctl reports: %llu bytes of main memory for user processes",
         (unsigned long long)memsize);
  return memsize;
}

#elif USE_SYSINFO

#include <sys/sysinfo.h>

static unsigned long long pdb_available_memory(cl_handle *cl) {
  struct sysinfo sy;

  if (sysinfo(&sy)) {
    cl_log(cl, CL_LEVEL_DEBUG,
           "pdb: sysinfo fails: %s - guessing "
           "at %llu bytes available memory.",
           strerror(errno), PDB_AVAILABLE_MEMORY);
    return PDB_AVAILABLE_MEMORY;
  }

  cl_log(cl, CL_LEVEL_DEBUG,
         "sysinfo reports: %llu bytes of usable main memory",
         (unsigned long long)sy.totalram * (unsigned long long)sy.mem_unit);
  return sy.totalram * (unsigned long long)sy.mem_unit;
}

#else /* neither USE_SYSINFO nor USE_SYSCTL */

static unsigned long long pdb_available_memory(cl_handle *cl) {
  cl_log(cl, CL_LEVEL_DEBUG,
         "pdb: neither sysctl nor sysinfo enabled - guessing "
         "at %llu bytes available memory.",
         PDB_AVAILABLE_MEMORY);
  return PDB_AVAILABLE_MEMORY;
}

#endif /* USE_SYSCTL or USE_SYSINFO  */

void pdb_configure(pdb_handle *pdb, pdb_configuration const *cf) {
  if (pdb != NULL && &pdb->pdb_cf != cf) {
    pdb->pdb_cf = *cf;
    pdb->pdb_total_mem = pdb->pdb_cf.pcf_total_memory;

    if (pdb->pdb_total_mem > 0) {
      cl_log(pdb->pdb_cl, CL_LEVEL_DEBUG,
             "user specified: %llu bytes of main memory for user processes",
             (unsigned long long)pdb->pdb_total_mem);
    } else {
      pdb->pdb_total_mem = pdb_available_memory(pdb->pdb_cl);
    }
  }
}

#if 0
bool pdb_sync(pdb_handle * pdb)
{
	return pdb->pdb_cf.pcf_sync;
}
#endif

bool pdb_transactional(pdb_handle *pdb) {
  return pdb->pdb_cf.pcf_transactional;
}

/**
 * Return the configuration for the given handle.
 */
pdb_configuration *pdb_config(pdb_handle *pdb) { return &pdb->pdb_cf; }
