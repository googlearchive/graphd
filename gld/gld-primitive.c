/*
Copyright 2018 Google Inc. All rights reserved.
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

#include "gld.h"

void gld_primitive_set_guid(gld_handle* gld, gld_primitive* pr,
                            graph_guid const* guid) {
  cl_assert(gld->gld_cl, pr->pr_head == NULL);
  pr->pr_tail = NULL;
  pr->pr_guid = *guid;
}

void gld_primitive_set_nil(gld_handle* gld, gld_primitive* pr) {
  pr->pr_head = NULL;
  pr->pr_tail = &pr->pr_head;
  pr->pr_next = NULL;
  GRAPH_GUID_MAKE_NULL(pr->pr_guid);
}

void gld_primitive_free_contents(gld_handle* gld, gld_primitive* pr) {
  gld_primitive *sub, *next;

  next = pr->pr_head;
  while ((sub = next) != NULL) {
    next = sub->pr_next;
    gld_primitive_free(gld, sub);
  }
  gld_primitive_set_nil(gld, pr);
}

void gld_primitive_free(gld_handle* gld, gld_primitive* pr) {
  gld_primitive_free_contents(gld, pr);
  cm_free(gld->gld_cm, pr);
}

gld_primitive* gld_primitive_alloc(gld_handle* gld) {
  gld_primitive* pr;

  if ((pr = cm_talloc(gld->gld_cm, gld_primitive, 1)) == NULL) return pr;
  gld_primitive_set_nil(gld, pr);

  return pr;
}

void gld_primitive_append(gld_handle* gld, gld_primitive* parent,
                          gld_primitive* child) {
  cl_assert(gld->gld_cl, parent != NULL);
  cl_assert(gld->gld_cl, child != NULL);
  cl_assert(gld->gld_cl, parent->pr_tail != NULL);

  *parent->pr_tail = child;
  parent->pr_tail = &child->pr_next;
}

int gld_primitive_is_list(gld_handle* gld, gld_primitive* pr) {
  return pr->pr_tail != NULL;
}

size_t gld_primitive_n(gld_handle* gld, gld_primitive* pr) {
  gld_primitive* child;
  size_t n = 0;

  cl_assert(gld->gld_cl, pr->pr_tail != NULL);
  cl_assert(gld->gld_cl, pr != NULL);

  for (child = pr->pr_head; child != NULL; child = child->pr_next) n++;
  return n;
}

gld_primitive* gld_primitive_nth(gld_handle* gld, gld_primitive* pr, size_t n) {
  gld_primitive* child;

  cl_assert(gld->gld_cl, pr != NULL);
  cl_assert(gld->gld_cl, pr->pr_tail != NULL);

  for (child = pr->pr_head; child != NULL && n-- > 0; child = child->pr_next)
    ;

  return child;
}
