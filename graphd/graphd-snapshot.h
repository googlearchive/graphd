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
#ifndef GRAPHD_SNAPSHOT_H
#define GRAPHD_SNAPSHOT_H 1

#include "graphd/graphd.h"

int graphd_snapshot_restore(graphd_handle* g, srv_handle* srv,
                            graphd_database_config const* dcf);

#endif
