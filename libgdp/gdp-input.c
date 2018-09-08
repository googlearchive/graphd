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
#include "libgdp/gdp.h"

#include <errno.h>

int gdp_input_getch(gdp_input *in, int *ch) {
  gdp_input_queue *q = &in->in_queue;

  if
    unlikely(q->iq_eof) return EIO;
  /* end of input? */
  else if
    unlikely(q->iq_curr_i == q->iq_tail_n && q->iq_curr == q->iq_tail) {
      *ch = GDP_EOF_CHAR;
      q->iq_eof = true;
      return 0;
    }
  /* end of current buffer? */
  else if
    unlikely(q->iq_curr_i == q->iq_curr->b_n) {
      q->iq_prev = q->iq_curr;
      q->iq_curr = q->iq_curr->b_next;
      q->iq_curr_i = 0;
      // NOTE: We assume that the buffer contains data!
      cl_assert(in->in_cl, q->iq_curr && q->iq_curr->b_n);
    }

  /* advance one posiiton */
  *ch = q->iq_curr->b_s[q->iq_curr_i++];
  /* update the token's length */
  q->iq_mark_len++;

  return 0;
}

int gdp_input_putch(gdp_input *in, int ch) {
  gdp_input_queue *q = &in->in_queue;

  /* end of input? */
  if
    unlikely(ch == GDP_EOF_CHAR) {
      q->iq_eof = false;
      return 0;
    }
  /* beginning of a buffer? */
  else if
    unlikely(q->iq_curr_i == 0) {
      cl_assert(in->in_cl, q->iq_prev != NULL);
      q->iq_curr = q->iq_prev;
      q->iq_curr_i = q->iq_curr->b_n;
      q->iq_prev = NULL;
    }

  q->iq_curr_i--;
  /* update the token's length */
  q->iq_mark_len--;

  return 0;
}

void gdp_input_tokbegin(gdp_input *in) {
  gdp_input_queue *q = &in->in_queue;
  q->iq_mark = q->iq_curr;
  q->iq_mark_i = q->iq_curr_i;
  q->iq_mark_len = 0;
}

int gdp_input_tokend(gdp_input *in, bool alloc, char **start, char **end) {
  gdp_input_queue *q = &in->in_queue;
  srv_buffer *sbuf, *ebuf;
  size_t six, eix;

  cl_assert(in->in_cl, q->iq_mark != NULL);

  /* end-of-file token, or empty token */
  if
    unlikely(q->iq_eof || (q->iq_mark_len == 0)) {
      *start = *end = NULL;
      return 0;
    }

  /* determine start buffer and offset of the token */
  sbuf = q->iq_mark;
  six = q->iq_mark_i;

  /* determine end buffer and offset of the token */
  if
    likely(q->iq_curr_i > 0) {
      ebuf = q->iq_curr;
      eix = q->iq_curr_i;
    }
  else {
    cl_assert(in->in_cl, q->iq_prev != NULL);
    ebuf = q->iq_prev;
    eix = q->iq_prev->b_n;
  }

  /*
   *       sbuf                                  ebuf
   *  (Start Buffer)     (More buffers)      (End Buffer)
   * +--------------+    +---/    /---+    +--------------+
   * |//////////"Bla| -> |bla  ..  bla| -> |blabla"///////|
   * +----------^---+    +---/    /---+    +-------^------+
   *            ^                                  ^
   *     sbuf->b_s[six]                     ebuf->b_s[eix]
   */

  /* is the token entirely within a single buffer? */
  if
    likely(sbuf == ebuf) {
      *start = sbuf->b_s + six;
      *end = sbuf->b_s + eix;
      /* move image to a malloc'ed buffer if requested */
      if (unlikely(alloc)) {
        size_t len;
        void *ptr;
        len = q->iq_mark_len;
        if ((ptr = cm_malloc(in->in_cm, len)) == NULL) {
          cl_log(in->in_cl, CL_LEVEL_ERROR, "insufficient memory");
          return ENOMEM;
        }
        memcpy(ptr, *start, len);
        *start = ptr;
        *end = ptr + len;
      }
    }
  /* nope... then we have to move the token's image to a contiguous
   * memory area; keep in mind that the token may be split between more
   * than just two buffers */
  else {
    const srv_buffer *b;
    size_t len;
    void *ptr;
    /* allocate the memory area */
    if ((ptr = cm_malloc(in->in_cm, q->iq_mark_len)) == NULL) {
      cl_log(in->in_cl, CL_LEVEL_ERROR, "insufficient memory");
      return ENOMEM;
    }
    *start = ptr;
    /* copy token fragment from the head buffer */
    len = sbuf->b_n - six;
    memcpy(ptr, sbuf->b_s + six, len);
    ptr += len;
    /* copy token fragments from the intermediate buffers */
    for (b = sbuf->b_next; b != ebuf; b = b->b_next) {
      len = b->b_n;
      memcpy(ptr, b->b_s, len);
      ptr += len;
    }
    /* copy token fragment from the tail buffer */
    memcpy(ptr, ebuf->b_s, eix);
    *end = ptr + eix;
  }

  return 0;
}

int gdp_input_init_plain(gdp_input *in, char const *buf, size_t size,
                         cm_handle *cm, cl_handle *cl) {
  srv_buffer *b;

  if ((b = cm_malloc(cm, sizeof(srv_buffer))) == NULL) return ENOMEM;

  *b = (srv_buffer){
      .b_s = (char *)buf,  // (ignore `const')
      .b_n = size,
  };

  *in = (gdp_input){
      .in_queue =
          (gdp_input_queue){
              .iq_curr = b, .iq_curr_i = 0, .iq_tail = b, .iq_tail_n = size,
          },
      .in_cm = cm,
      .in_cl = cl,
      .in_row = 1,
      .in_col = 1,
  };

  return 0;
}

int gdp_input_init_chain(gdp_input *in, srv_buffer *chain, cm_handle *cm,
                         cl_handle *cl) {
  srv_buffer *b;

  /* find end of the buffer chain */
  gdp_assert(cl, chain);
  for (b = chain; b->b_next; b = b->b_next)
    ;

  *in = (gdp_input){
      .in_queue =
          (gdp_input_queue){
              .iq_curr = chain,
              .iq_curr_i = 0,
              .iq_tail = b,
              .iq_tail_n = b->b_n,
          },
      .in_cm = cm,
      .in_cl = cl,
      .in_row = 1,
      .in_col = 1,
  };

  return 0;
}

int gdp_input_init_chain_part(gdp_input *in, srv_buffer *first,
                              size_t first_offs, srv_buffer *last,
                              size_t last_n, cm_handle *cm, cl_handle *cl) {
  *in = (gdp_input){
      .in_queue =
          (gdp_input_queue){
              .iq_curr = first,
              .iq_curr_i = first_offs,
              .iq_tail = last,
              .iq_tail_n = last_n,
          },
      .in_cm = cm,
      .in_cl = cl,
      .in_row = 1,
      .in_col = 1,
  };

  return 0;
}
