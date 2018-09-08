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
#ifndef __GDP_INPUT_H__
#define __GDP_INPUT_H__

#include "libcl/cl.h"
#include "libcm/cm.h"
#include "libsrv/srv.h"

/** The constant that identifies an end-of-file character */
#define GDP_EOF_CHAR EOF

/**
 * Support structure for buffer queue input kinds (see #gdp_input).
 */
struct gdp_input_queue {
  srv_buffer *iq_curr;  ///< Current buffer
  size_t iq_curr_i;     ///< Current offset in @c buf
  srv_buffer *iq_prev;  ///< Previous buffer
  srv_buffer *iq_tail;  ///< Last buffer
  size_t iq_tail_n;     ///< Number of interesting bytes in @c tail
  srv_buffer *iq_mark;  ///< Buffer where the current token begins
  size_t iq_mark_i;     ///< Where the token begins in @c mark
  size_t iq_mark_len;   ///< Length of the token
  bool iq_eof;          ///< End of input reached
};

typedef struct gdp_input_queue gdp_input_queue;  ///< See #gdp_input_queue

/**
 * Input to the gdp_parse() function.
 */
struct gdp_input {
  gdp_input_queue in_queue;  ///< Linked list of buffers
  cm_handle *in_cm;          ///< Heap
  cl_handle *in_cl;          ///< Log
  int in_row;                ///< Line number
  int in_col;                ///< Column number
};

/**
 * Fetch a character from the input.
 *
 * If the end of file is reached, `ch' is set to GDP_EOF_CHAR_EOF.
 */
extern int gdp_input_getch(struct gdp_input *in, int *ch);

/**
 * Put character back in the input.
 */
extern int gdp_input_putch(struct gdp_input *in, int ch);

/**
 * Mark the beginning of a token.
 */
extern void gdp_input_tokbegin(struct gdp_input *in);

/**
 * Mark the end of a token.
 *
 * @param in
 *	The input specs.
 * @param alloc
 *	Move the token's image to a malloc'ed memory buffer. Notice that this
 *	is done anyway if the token is split between different input buffers.
 * @param [out] start
 *	The beginning of the token.
 * @param [out] end
 *	The end of the token.
 * @return
 *	Zero on success, otherwise an error code.
 */
extern int gdp_input_tokend(struct gdp_input *in, bool alloc, char **start,
                            char **end);

#endif
