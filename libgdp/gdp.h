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
#ifndef __GDP_H__
#define __GDP_H__

#include <sys/types.h>

#include "libcl/cl.h"
#include "libcm/cm.h"
#include "libsrv/srv.h"

/** Parser */
typedef struct gdp gdp;

/** Input specs */
typedef struct gdp_input gdp_input;

/** Output specs */
typedef struct gdp_output gdp_output;

#include "libgdp/gdp-error.h"
#include "libgdp/gdp-private.h"

/**
 * Initialize parser.
 *
 * @param parser
 *	The object to be initialized.
 * @param cm
 *	Memory allocator.
 * @param cl
 *	Log handle.
 * @return
 *	Zero.
 */
extern int gdp_init(gdp* parser, cm_handle* cm, cl_handle* cl);

/**
 * Initialize input as a plain buffer.
 *
 * @param input
 *	The stream object.
 * @param buf
 *	The input data, as a plain char buffer.
 * @param size
 *	Size of the buffer.
 * @param cm
 *	Memory allocator.
 * @param cl
 *	Log handle.
 * @return
 *	Zero on success, otherwise an error code.
 */
extern int gdp_input_init_plain(gdp_input* input, char const* buf, size_t size,
                                cm_handle* cm, cl_handle* cl);

/**
 * Initialize input as a chain of buffers.
 *
 * @param input
 *	The stream object.
 * @param chain
 *	The chain of buffers.
 * @param cm
 *	Memory allocator.
 * @param cl
 *	Log handle.
 * @return
 *	Zero on success, otherwise an error code.
 */
extern int gdp_input_init_chain(gdp_input* input, srv_buffer* chain,
                                cm_handle* cm, cl_handle* cl);

/**
 * Initialize input as a partial chain of buffers.
 *
 * @param input
 *	The stream object.
 * @param first
 *	First buffer in the chain.
 * @param first_offs
 *	Offset in the first buffer.
 * @param last
 *	Last buffer in the chain.
 * @param last_n
 *	Number of bytes to consider in the last buffer.
 * @param cm
 *	Memory allocator.
 * @param cl
 *	Log handle.
 * @return
 *	Zero on success, otherwise an error code.
 */
extern int gdp_input_init_chain_part(gdp_input* in, srv_buffer* first,
                                     size_t first_offs, srv_buffer* last,
                                     size_t last_n, cm_handle* cm,
                                     cl_handle* cl);

/**
 * Parse a request.
 *
 * @param parser
 *	The parser object.
 * @param input
 *	The input to the parser. Must be initialized using gdp_input_init() or
 *	gdp_input_init_plain().
 * @param output
 *	The output produced by the parser. Must be initialized using an
 *	appropriate implementation-specific function.
 * @return
 *	Zero on success, otherwise an error code.
 */
extern int gdp_parse(gdp* parser, gdp_input* input, gdp_output* output);

/**
 * Parse the reply to a specfic command.
 *
 * @param parser
 *	The parser object.
 * @param cmd
 *	The type of request we want a reply to.
 * @param input
 *	The input to the parser. Must be initialized using gdp_input_init() or
 *	gdp_input_init_plain().
 * @param output
 *	The output produced by the parser. Must be initialized using an
 *	appropriate implementation-specific function.
 * @return
 *	Zero on success, otherwise an error code.
 */
int gdp_parse_reply(gdp* parser, graphd_command cmd, gdp_input* input,
                    gdp_output* output);

/**
 * Return an error message
 *
 * @param err
 *	An error code returned by GDP
 * @return
 *	A string pointer to a gdp- or strerror error message.
 */
char const* gdp_strerror(int err);

#endif
