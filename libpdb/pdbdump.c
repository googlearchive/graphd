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

#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sysexits.h>
#include <sys/stat.h>
#include <unistd.h>

int status = 0;

static void usage(char const *progname) {
  fprintf(stderr, "Usage: %s directories...\n", progname);
  exit(EX_USAGE);
}

unsigned long long n_prim = 0;
unsigned long long n_guids = 0;

#if 0
static void savings(pdb_primitive const * pr)
{
	n_prim++;
	if (pdb_primitive_has_left( pr ))
		n_guids++;
	if (pdb_primitive_has_right( pr ))
		n_guids++;
	if (pdb_primitive_has_typeguid(pr))
		n_guids++;
	if (pdb_primitive_has_scope(pr))
		n_guids++;
}
#endif

static void dump(pdb_primitive const *pr) {
  char buf[200];
  graph_guid guid;
  int bits, dt, sz;
  char const *ptr;

  pdb_primitive_guid_get(pr, guid);
  printf("\nGUID: %s\n", graph_guid_to_string(&guid, buf, sizeof buf));

  pdb_primitive_scope_get(pr, guid);
  printf("SCOPE: %s\n", graph_guid_to_string(&guid, buf, sizeof(buf)));

  printf("TIMESTAMP: %s\n",
         graph_timestamp_to_string(pdb_primitive_timestamp_get(pr), buf,
                                   sizeof(buf)));
  bits = pdb_primitive_bits_get(pr);
  printf("BITS: %x", bits);
  if (bits & PDB_PRIMITIVE_BIT_ARCHIVAL) fputs(" archival", stdout);
  if (bits & PDB_PRIMITIVE_BIT_LIVE) fputs(" live", stdout);
  putchar('\n');

  dt = pdb_primitive_valuetype_get(pr);
  if (!(ptr = graph_datatype_to_string(dt))) {
    ptr = buf;
    snprintf(buf, sizeof buf, "%hu", dt);
  }
  printf("VALUETYPE: %s\n", ptr);

  sz = pdb_primitive_name_get_size(pr);
  if (sz == 0)
    printf("NAME: null\n");
  else
    printf("NAME: %.*s [%d]\n", sz - 1, pdb_primitive_name_get_memory(pr), sz);

  sz = pdb_primitive_value_get_size(pr);
  if (sz == 0)
    printf("VALUE: null\n");
  else
    printf("VALUE: %.*s [%d]\n", sz - 1, pdb_primitive_value_get_memory(pr),
           sz);

  if (pdb_primitive_has_left(pr)) {
    pdb_primitive_left_get(pr, guid);
    printf("LEFT: %s\n", graph_guid_to_string(&guid, buf, sizeof(buf)));
  }
  if (pdb_primitive_has_right(pr)) {
    pdb_primitive_right_get(pr, guid);
    printf("RIGHT: %s\n", graph_guid_to_string(&guid, buf, sizeof(buf)));
  }
  if (pdb_primitive_has_typeguid(pr)) {
    pdb_primitive_typeguid_get(pr, guid);
    printf("TYPE: %s\n", graph_guid_to_string(&guid, buf, sizeof(buf)));
  }
  if (pdb_primitive_has_scope(pr)) {
    pdb_primitive_scope_get(pr, guid);
    printf("SCOPE: %s\n", graph_guid_to_string(&guid, buf, sizeof(buf)));
  }
  if (pdb_primitive_has_previous(pr)) {
    pdb_id prev;
    unsigned long long gen;

    prev = pdb_primitive_lineage_get(pr);
    gen = pdb_primitive_generation_get(pr);

    printf("PREV: %llx (generation %llu)\n", (unsigned long long)prev,
           (unsigned long long)gen);
  }
}

#if 0
static pdb_configuration const* default_config( void )
{
	static pdb_configuration  cf;
	static int  initialized = 0;

	if (initialized)
		return &cf;

	cf.pcf_tile_cache_vmem = 70;
	cf.pcf_tile_cache_primitive = 10;
	cf.pcf_tile_cache_hash = 50;
	cf.pcf_tile_cache_from = 10;
	cf.pcf_tile_cache_to = 10;
	cf.pcf_tile_cache_key = 5;
	cf.pcf_tile_cache_generation = 10;
	cf.pcf_tile_cache_scope = 10;
	cf.pcf_tile_cache_type = 10;
	cf.pcf_sync = 1;
	cf.pcf_istore_init_map_tiles = sizeof( void* ) > 4 ? 32768 : 64;
	cf.pcf_gmap_init_map_tiles = sizeof( void* ) > 4 ? 32768 : 64;

	initialized = 1;

	return &cf;
}
#endif

static void process(char const *progname, char const *dirname) {
  pdb_handle *pdb;
  cl_handle *cl = cl_create();
  cm_handle *cm = cm_c();
  pdb_iterator *it;
  pdb_primitive pr;
  int err;
  pdb_id id;

  if ((pdb = pdb_create(cm, cl, 0)) == NULL) {
    fprintf(stderr, "%s: failed to create PDB state for %s\n", progname,
            dirname);
    return;
  }

  if ((err = pdb_set_path(pdb, dirname)) != 0) {
    fprintf(stderr,
            "%s: failed to set database directory "
            "name to \"%s\": %s\n",
            progname, dirname, strerror(err));
    pdb_destroy(pdb);
    return;
  }

  if ((err = pdb_initialize(pdb)) != 0) {
    fprintf(stderr, "%s: failed to initialize \"%s\"\n", progname, dirname);
    pdb_destroy(pdb);
    return;
  }
  if ((err = pdb_initialize_checkpoint(pdb)) != 0) {
    fprintf(stderr, "%s: failed to initialize checkpoint \"%s\"\n", progname,
            dirname);
    pdb_destroy(pdb);
    return;
  }

  pdb_iterator_all_create(pdb, PDB_ITERATOR_LOW_ANY, PDB_ITERATOR_HIGH_ANY,
                          true, &it);

  while ((err = pdb_iterator_next_nonstep(pdb, it, &id)) == 0) {
    if ((err = pdb_id_read(pdb, id, &pr)) == 0)
      dump(&pr);
    else
      fprintf(stderr, "%s: cannot read primitive %llx: %s\n", progname,
              (unsigned long long)id, strerror(err));
  }
  pdb_iterator_destroy(pdb, &it);
  printf("n_prim=%llu, n_guids=%llu, bytes to save=%llu\n", n_prim, n_guids,
         n_guids * 11);

  if (err != ADDB_ERR_NO) {
    fprintf(stderr, "%s: error in iteration over \"%s\": %s\n", progname,
            dirname, strerror(err));
    pdb_destroy(pdb);
    return;
  }
  pdb_destroy(pdb);
}

int main(int argc, char **argv) {
  int opt;
  char const *progname;
  struct stat st;

  if ((progname = strrchr(argv[0], '/')) != NULL)
    progname++;
  else
    progname = argv[0];

  while ((opt = getopt(argc, argv, "dih")) != EOF) switch (opt) {
      default:
        usage(progname);
        /* NOTREACHED */
        break;
    }

  if (optind >= argc) usage(progname);

  for (; optind < argc; optind++) {
    if (stat(argv[optind], &st) != 0) {
      fprintf(stderr, "%s: \"%s\" -- %s\n", progname, argv[optind],
              strerror(errno));
      exit(EX_NOINPUT);
    }

    if (!S_ISDIR(st.st_mode)) {
      fprintf(stderr, "%s: \"%s\" is not a directory.\n", progname,
              argv[optind]);
      exit(EX_NOINPUT);
    }
    process(progname, argv[optind]);
  }
  return status;
}
