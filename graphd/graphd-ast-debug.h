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
#ifndef GRAPHD_AST_DEBUG_H
#define GRAPHD_AST_DEBUG_H

#include "graphd/graphd.h"

//#ifdef DEBUG_PARSER

extern void graphd_ast_debug_reading(graphd_request const* greq);

extern void graphd_ast_debug_received(graphd_request const* greq, bool eof);

extern void graphd_ast_debug_parsed(graphd_request const* greq,
                                    bool has_errors);

extern void graphd_ast_debug_serving(graphd_request const* greq);

extern void graphd_ast_debug_finished(graphd_request const* greq);

//#else  // ifndef DEBUG_PARSER
//
//#define graphd_ast_debug_reading(greq) \
//  do {                                 \
//  } while (0)
//
//#define graphd_ast_debug_received(greq, eof) \
//  do {                                       \
//  } while (0)
//
//#define graphd_ast_debug_parsed(greq, has_errors) \
//  do {                                            \
//  } while (0)
//
//#define graphd_ast_debug_serving(greq) \
//  do {                                 \
//  } while (0)
//
//#define graphd_ast_debug_finished(greq) \
//  do {                                  \
//  } while (0)
//
//#endif  // ifndef DEBUG_PARSER

#endif
