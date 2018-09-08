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
#include "graphd/graphd.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "libpdb/pdb.h"

int graphd_constraint_cursor_scan_prefix(graphd_request* greq,
                                         graphd_constraint* con,
                                         char const** s_ptr, char const* e) {
  cl_handle* const cl = graphd_request_cl(greq);
  char const* s = *s_ptr;

  if (s == NULL) return 0;

  while (s < e && *s == '[') {
    char const* r = graphd_unparenthesized_textchr(s + 1, e, ']');
    if (r == NULL) break;

    if (r - s > 3 && strncasecmp(s, "[o:", 3) == 0) {
      unsigned long long ull;
      int err;

      s += 3;
      err = graphd_bytes_to_ull(&s, r + 1, &ull);
      if (err != 0) return err;

      if (s != r + 1) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_semantic_constraint_scan_cursor: "
               "trailing garbage \"%.*s\"",
               (int)((r - 1) - s), s);
        return GRAPHD_ERR_LEXICAL;
      }
      con->con_cursor_offset = ull;
      if (!con->con_count.countcon_min_valid)
        con->con_count.countcon_min = ull + 1;

      /*  If there is no "start" offset,
       *  default that to be the cursor offset.
       */
    } else if (r - s > 3 && strncasecmp(s, "[n:", 3) == 0) {
      unsigned long long ull;
      int err;

      s += 3;
      err = graphd_bytes_to_ull(&s, r + 1, &ull);
      if (err != 0) return err;

      if (s != r + 1) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_semantic_constraint_scan_cursor: "
               "trailing garbage \"%.*s\"",
               (int)((r - 1) - s), s);
        return GRAPHD_ERR_LEXICAL;
      }
      if (con->con_high > ull) con->con_high = ull;
    }

    /* Ignore what we don't know. */
    s = r + 1;
  }

  /* Rewrite the cursor to start behind the last adjacent
   * leading [].
   */
  *s_ptr = s;
  return 0;
}

int graphd_constraint_cursor_thaw(graphd_request* greq, graphd_constraint* con,
                                  pdb_iterator** it_out) {
  unsigned short check, scanned_check;
  char buf[20];
  cm_buffer sig;
  graphd_handle* g = graphd_request_graphd(greq);
  char const *p, *e, *cursor_s, *cursor_e;
  int err;

  cursor_s = con->con_cursor_s;
  cursor_e = con->con_cursor_e;

  if (cursor_e - cursor_s < sizeof("cursor:NNNN:") - 1 ||
      strncasecmp(cursor_s, "cursor:", sizeof("cursor:") - 1) != 0) {
    /*  Grandfather in pre-signature cursors.
     */
    err = graphd_constraint_cursor_scan_prefix(greq, con, &cursor_s, cursor_e);
    if (err != 0) goto syntax;

    err = graphd_iterator_thaw_bytes(greq, cursor_s, cursor_e, 0, CL_LEVEL_FAIL,
                                     it_out);
    if (err != 0) {
    thaw_error:
      if (err == GRAPHD_ERR_SYNTAX || err == PDB_ERR_SYNTAX ||
          err == GRAPHD_ERR_LEXICAL) {
        if (memchr(cursor_s, '/', cursor_e - cursor_s) == NULL) {
          graphd_request_errprintf(
              greq, 0,
              "BADCURSOR cannot parse old-style "
              "cursor \"%.*s%s\"",
              con->con_cursor_e - con->con_cursor_s > 1027
                  ? 1024
                  : (int)(con->con_cursor_e - con->con_cursor_s),
              con->con_cursor_s,
              con->con_cursor_e - con->con_cursor_s > 1027 ? "..." : "");
        } else
          graphd_request_errprintf(
              greq, 0,
              "BADCURSOR cannot resume at "
              "\"%.*s%s\"",
              con->con_cursor_e - con->con_cursor_s > 1027
                  ? 1024
                  : (int)(con->con_cursor_e - con->con_cursor_s),
              con->con_cursor_s,
              con->con_cursor_e - con->con_cursor_s > 1027 ? "..." : "");
      } else
      system:
      graphd_request_errprintf(
          greq, 0,
          "SYSTEM unexpected error while trying "
          "to resume at \"%.*s%s\"",
          con->con_cursor_e - con->con_cursor_s > 1027
              ? 1024
              : (int)(con->con_cursor_e - con->con_cursor_s),
          con->con_cursor_s,
          con->con_cursor_e - con->con_cursor_s > 1027 ? "..." : "");
    }
    return err;
  }

  p = memchr(cursor_s, ':', cursor_e - cursor_s);
  cl_assert(graphd_request_cl(greq), p != NULL);
  p++;

  e = p + 4;
  if (*e != ':') {
  syntax:
    graphd_request_errprintf(
        greq, 0, "BADCURSOR \"%.*s%s\" is not a valid cursor",
        con->con_cursor_e - con->con_cursor_s > 1027
            ? 1024
            : (int)(con->con_cursor_e - con->con_cursor_s),
        con->con_cursor_s,
        con->con_cursor_e - con->con_cursor_s > 1027 ? "..." : "");
    return GRAPHD_ERR_LEXICAL;
  }

  /* Scan the hash sum from the incoming cursor.
   */
  memcpy(buf, p, e - p);
  buf[e - p] = '\0';

  if (sscanf(buf, "%hx", &scanned_check) != 1) goto syntax;

  /* Get the hash for the constraint signature.
   */
  err = graphd_constraint_signature(g, con, GRAPHD_SIGNATURE_OMIT_CURSOR, &sig);
  if (err != 0) {
    cm_buffer_finish(&sig);
    goto system;
  }
  check = cm_buffer_checksum(&sig, 16) ^
          cm_buffer_checksum_text(e + 1, cursor_e, 16);
  cm_buffer_finish(&sig);

  if (check != scanned_check) {
    graphd_request_errprintf(
        greq, 0,
        "BADCURSOR cursor \"%.*s%s\" "
        "and checksum %hx don't match",
        cursor_e - cursor_s > 1027 ? 1024 : (int)(cursor_e - cursor_s),
        cursor_s, cursor_e - cursor_s > 1027 ? "..." : "", check);
    return GRAPHD_ERR_LEXICAL;
  }

  e++;

  err = graphd_constraint_cursor_scan_prefix(greq, con, &e, cursor_e);
  if (err != 0) goto syntax;

  /* Unpack the cursor.
   */
  err = graphd_iterator_thaw_bytes(greq, e, cursor_e, 0, CL_LEVEL_FAIL, it_out);
  if (err != 0) goto thaw_error;
  return 0;
}

/**
 * @brief Where are we?  Record that as a string.
 *
 * @param con		constraint the cursor is for.
 * @param greq		request we're working for
 * @param it 		The iterator underlying the cursor object
 * @param val_out	Assign the string value to this.
 *
 * @return 0 on success, an error code on resource failure.
 */
int graphd_constraint_cursor_from_iterator(graphd_request* greq,
                                           graphd_constraint* con,
                                           char const* prefix, pdb_iterator* it,
                                           graphd_value* val_out) {
  int err;
  graphd_handle* g = graphd_request_graphd(greq);
  cl_handle* cl = graphd_request_cl(greq);
  cm_handle* cm = greq->greq_req.req_cm;
  cm_buffer buf, total;
  unsigned short check_constraint, check_cursor;

  cl_assert(cl, val_out != NULL);
  cl_assert(cl, it != NULL);

  /* Zero out the buffers so they'll be safe to free in case of error.
   */
  cm_buffer_initialize(&buf, cm);
  cm_buffer_initialize(&total, cm);

  /*  Calculate a check sum for this constraint signature.
   */
  err = graphd_constraint_signature(g, con, GRAPHD_SIGNATURE_OMIT_CURSOR, &buf);
  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_constraint_signature", err,
                 "con=%s", graphd_constraint_to_string(con));
    goto err;
  }
  check_constraint = cm_buffer_checksum(&buf, 16);
  cm_buffer_finish(&buf);

  /*  Calculate the unsigned cursor.
   */
  cm_buffer_initialize(&buf, cm);
  if (prefix != NULL) cm_buffer_add_string(&buf, prefix);
  if ((err = graphd_iterator_freeze(g, it, &buf)) != 0) {
    char cbuf[200];
    cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_iterator_freeze", err, "it=%s",
                 pdb_iterator_to_string(g->g_pdb, it, cbuf, sizeof cbuf));
    goto err;
  }

  /*  Calculate a checksum for the unsigned cursor.
   */
  check_cursor = cm_buffer_checksum(&buf, 16);

  /*  Combine the two checksums and the cursor in a string.
   *  This is the cursor:XXXX:... string the outside world sees.
   */
  err = cm_buffer_sprintf(&total, "cursor:%4.4hx:%.*s",
                          check_cursor ^ check_constraint,
                          (int)cm_buffer_length(&buf), cm_buffer_memory(&buf));
  cm_buffer_finish(&buf);

  if (err != 0) {
    cl_log_errno(cl, CL_LEVEL_FAIL, "cm_buffer_sprintf", err,
                 "unexpected error");
    goto err;
  }

  /*  Make the buffer contents our result value.
   */
  graphd_value_text_set_cm(val_out, GRAPHD_VALUE_STRING, total.buf_s,
                           total.buf_n, total.buf_cm);
  cl_cover(cl);
  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_read_set_cursor_get: %.*s",
         (int)total.buf_n, total.buf_s);
  return 0;

err:
  cm_buffer_finish(&buf);
  cm_buffer_finish(&total);
  return err;
}

static bool constraint_patterns_involve_type(graphd_constraint* con, int type) {
  graphd_assignment* a;

  if (con->con_result != NULL && graphd_pattern_lookup(con->con_result, type))
    return true;

  if (con->con_sort != NULL && graphd_pattern_lookup(con->con_sort, type))
    return true;

  for (a = con->con_assignment_head; a != NULL; a = a->a_next)
    if (a->a_result != NULL && graphd_pattern_lookup(a->a_result, type))
      return true;

  return false;
}

static bool constraint_cursor_usable(graphd_request* greq,
                                     graphd_constraint* con) {
  graphd_constraint_or* cor;

  /*  A constraint is usable if
   *  - its result or assignment sets involve a cursor
   *  - it is the non-optional child of a parent that
   *    involves a cursor.
   */
  if (constraint_patterns_involve_type(con, GRAPHD_PATTERN_CURSOR))
    return true;

  else if (con->con_parent != NULL && con->con_parent->con_cursor_usable &&
           GRAPHD_CONSTRAINT_IS_MANDATORY(con))
    return true;

  /* Our "or" branches - if they contain a cursor, their prototype does.
   */
  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    if (constraint_cursor_usable(greq, &cor->or_head) ||
        (cor->or_tail != NULL && constraint_cursor_usable(greq, cor->or_tail)))

      return true;
  }
  return false;
}

/*  Called recursively in preorder, as part of
 *  graphd_semantic_constraint_complete_subtree().
 */
void graphd_constraint_cursor_mark_usable(graphd_request* greq,
                                          graphd_constraint* con) {
  /*  A constraint is usable if
   *  - its result or assignment sets involve a cursor
   *  - it is the non-optional child of a parent that
   *    involves a cursor.
   */
  con->con_cursor_usable = constraint_cursor_usable(greq, con);
  if (con->con_cursor_usable) {
    graphd_constraint_or* cor;

    /* Mark our "or" branches.
     */
    for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
      cor->or_head.con_cursor_usable = true;
      if (cor->or_tail != NULL) cor->or_tail->con_cursor_usable = true;
    }
  }
}
