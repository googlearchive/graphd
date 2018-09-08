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
#include "libgraphdb/graphdbp.h"

/**
 * @brief Install an asynchronous reply callback.
 *
 * If installed, replies are not returned by graphdb_request_wait(),
 * but instead delivered asynchronously to the callback.
 *
 * This just changes the result delivery mechanism;
 * the application still needs to @em run graphdb_request_wait()
 * in order to give libgraphdb a chance to do processing.
 *
 * @param graphdb 	handle created with graphdb_create()
 * @param callback 	the reply callback
 * @param callback_data pointer passed to each invocation of @b callback.
 */
void graphdb_set_reply_callback(graphdb_handle* graphdb,
                                graphdb_reply_callback* callback,
                                void* callback_data) {
  if (!GRAPHDB_IS_HANDLE(graphdb)) return;

  graphdb->graphdb_app_reply_callback = callback;
  graphdb->graphdb_app_reply_callback_data = callback_data;
}
