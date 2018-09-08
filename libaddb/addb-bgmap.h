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
#ifndef _ADDB_GMAP_H
#define _ADDB_GMAP_H

#include "libaddb/addbp.h"

struct addb_bgmap;
struct addb_bgmap_handle;
typedef struct addb_bgmap addb_bgmap;
typedef struct addb_bgmap_handle addb_bgmap_handle;

addb_bgmap_handle *addb_bgmap_create(addb_handle *addb, const char *path);

int addb_bgmap_lookup(struct addb_gmap *gm, addb_gmap_id s,
                      struct addb_bgmap **out);

void addb_bgmap_handle_destroy(struct addb_bgmap_handle *ah);

int addb_bgmap_check(struct addb_gmap *gm, struct addb_bgmap *bg,
                     addb_gmap_id s, bool *out);

int addb_bgmap_append(struct addb_gmap *gm, struct addb_bgmap *bg,
                      addb_gmap_id s);

int addb_bgmap_next(struct addb_gmap *gm, struct addb_bgmap *bm,
                    addb_gmap_id *start, addb_gmap_id low, addb_gmap_id high,
                    bool direction);

int addb_bgmap_checkpoint(addb_gmap *gm, unsigned long long horizon,
                          bool hard_sync, bool block,
                          addb_tiled_checkpoint_fn *cpfn);

const char *addb_bgmap_name(struct addb_bgmap *bm);

int addb_bgmap_horizon_set(addb_gmap *gm, unsigned long long horizon);

int addb_bgmap_guess_size(addb_gmap *gm, addb_bgmap *bg, unsigned long long max,
                          unsigned long long *out);

int addb_bgmap_truncate(addb_bgmap_handle *ah);

int addb_bgmap_estimate(addb_gmap *gm, addb_gmap_id source, addb_gmap_id low,
                        addb_gmap_id high, unsigned long long *resuilt);

int addb_bgmap_refresh(addb_gmap *gm, unsigned long long max);
#endif
