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
#include "libcm/cm.h"

static const cm_list_offsets cm_resource_offsets =
    CM_LIST_OFFSET_INIT(cm_resource, r_next, r_prev);

/**
 * @brief Initialize a resource pointer.
 *
 *  Once a resource pointer has been initialized,
 *  it is safe to pass it to cm_resource_free(), with
 *  any number of intervening calls to cm_resource_alloc()
 *  or cm_resource_free().
 *
 * @param r	fat resource pointer to initialize.
 */
void cm_resource_initialize(cm_resource* r) { r->r_manager = NULL; }

/**
 * @brief Manage a resource within a resource pointer.
 *
 *  Note that the memory for the resource is actually mananged
 *  separately (needs to be free'd separately), perhaps as part
 *  of a surrounding, larger structure that contains application
 *  data.
 *
 *  When the resource manager is free'd, all resources that
 *  were allocated but not free'd with intervening calls to
 *  cm_resource_free() are free'd using their resource type's
 *  "free" method.
 *
 * @param rm	resource manager
 * @param r	resource pointer to initialize.
 * @param rt	type of the resource
 * @param data	opaque application data to manage
 */
void cm_resource_alloc(cm_resource_manager* rm, cm_resource* r,
                       cm_resource_type const* rt, void* data) {
  r->r_data = data;
  r->r_type = rt;
  r->r_link = 1;
  r->r_manager = rm;

  cm_list_push(cm_resource, cm_resource_offsets, &rm->rm_head, &rm->rm_tail, r);
}

static void resource_free(cm_resource* r) {
  cm_resource_manager* const rm = r->r_manager;
  void* const manager_data = rm->rm_data;

  cm_list_remove(cm_resource, cm_resource_offsets, &rm->rm_head, &rm->rm_tail,
                 r);

  /*  Call the application's free callback.
   *
   *  Set the resource's manager pointer
   *  to NULL before calling the callback
   *  to avoid infinite recursion if the
   *  callback, in turn, frees the resource.
   */
  r->r_manager = NULL;
  (*r->r_type->rt_free)(manager_data, r->r_data);
}

/**
 * @brief Free a resource.
 *
 *  Chain out the resource, and call the "free" method of
 *  its type pointer.
 *
 *  It is safe, and does nothing, to call this function with
 *  a NULL pointer, or with a pointer that has been cleared
 *  with a call to cm_resource_initialize().
 *
 * @param r	resource to free.
 */
void cm_resource_free(cm_resource* r) {
  if (r == NULL || r->r_manager == NULL) return;

  if (r->r_link-- > 1) return;

  resource_free(r);
}

/**
 * @brief Duplicate a resource.
 *
 *  For each call to cm_resource_dup, a corresponding call
 *  to cm_resource_free() does nothing but decrement a
 *  link counter.
 *
 * @param r	resource to duplicate.
 */
void cm_resource_dup(cm_resource* r) {
  if (r == NULL || r->r_manager == NULL) return;

  r->r_link++;
}

/**
 * @brief List a single resource
 *
 *  By convention, lists are written to a cl_handle
 *  at loglevel CL_LEVEL_VERBOSE.
 *
 * @param r		resource to list
 * @param cl_data	the cl_handle to write to
 */
void cm_resource_list(cm_resource* r, void* cl_data) {
  if (r != NULL && r->r_type->rt_list != NULL)
    (*r->r_type->rt_list)(cl_data, r->r_manager->rm_data, r->r_data);
}

/**
 * @brief Initialize a resource manager
 *
 * @param rm	resource manager to initialize
 * @param data	opaque per-manager application pointer
 */
void cm_resource_manager_initialize(cm_resource_manager* rm, void* data) {
  rm->rm_head = NULL;
  rm->rm_tail = NULL;
  rm->rm_data = data;
}

/**
 * @brief Free resource controlled by a resource manager
 *
 * @param rm	resource manager to free
 */
void cm_resource_manager_finish(cm_resource_manager* rm) {
  if (rm == NULL) return;

  /*  Free tail-first, so that older resources
   *  are freed before newer resources, minimizing
   *  the chance of freeing a contained element
   *  before its manager.
   */
  while (rm->rm_tail != NULL) resource_free(rm->rm_tail);
}

/**
 * @brief List resources held by a resource manager
 * @param rm	resource manager to list
 * @param data	opaque callback data for resource type list calls
 *	(by convention, a cl_handle.)
 */
void cm_resource_manager_list(cm_resource_manager* rm, void* data) {
  cm_resource* r;

  if (rm == NULL) return;

  for (r = rm->rm_head; r != NULL; r = r->r_next)
    if (r->r_type->rt_list != NULL)
      (*r->r_type->rt_list)(data, rm->rm_data, r->r_data);
}

/**
 * @brief Traverse resources held by a resource manager
 * @param rm	resource manager to traverse
 * @param callback callback to call.
 * @param callback_data	opaque callback data for traversal
 */
void cm_resource_manager_map(cm_resource_manager* rm,
                             void (*callback)(void*, void*, void*),
                             void* data) {
  cm_resource* r;

  if (rm == NULL) return;

  for (r = rm->rm_head; r != NULL; r = r->r_next)
    (*callback)(data, rm->rm_data, r->r_data);
}

/**
 * @brief Return the most recently added resource
 *
 *  The resource manager is really intended to just be
 *  a bag - but we've got enough scaffolding to work as
 *  a stack, so why not do that.
 *
 * @param rm 	resource manager
 * @return The resource most recently added to this
 * 	resource manager.
 */
cm_resource* cm_resource_top(cm_resource_manager* rm) { return rm->rm_head; }
