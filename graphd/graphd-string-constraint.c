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
#include "graphd/graphd-hash.h"

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

static char const *op_to_string(int op, char *buf, size_t size) {
  switch (op) {
    case GRAPHD_OP_UNSPECIFIED:
      return "unspecified";
    case GRAPHD_OP_LT:
      return "<";
    case GRAPHD_OP_LE:
      return "<=";
    case GRAPHD_OP_EQ:
      return "=";
    case GRAPHD_OP_GE:
      return ">=";
    case GRAPHD_OP_GT:
      return ">";
    case GRAPHD_OP_NE:
      return "!=";
    case GRAPHD_OP_MATCH:
      return "~=";
    default:
      break;
  }
  snprintf(buf, size, "unexpected operator %x", op);
  return buf;
}

/**
 * @brief Return a string representation of a string constraint.
 *
 * @param greq		Request we're working for
 * @param con		constraint strcon is part of
 * @param strcon	string constraint to pick from
 * @param which		-1: lowest, 1: highest.
 *
 * @return the lowest/highest element, or NULL if there are none.
 */
char const *graphd_string_constraint_to_string(
    graphd_string_constraint const *strcon, char *buf, size_t size) {
  graphd_string_constraint_element *strcel;
  char obuf[10];
  char const *optr;
  char const *e;
  char *s;

  if (strcon == NULL) return "null";

  optr = op_to_string(strcon->strcon_op, obuf, sizeof obuf);
  if ((strcel = strcon->strcon_head) == NULL) {
    /* Empty constraint.
     */
    snprintf(buf, size, "%snull", optr);
    return buf;
  } else if (strcel->strcel_next == NULL) {
    /*  Single-elment constraint.
     */
    if (strcel->strcel_s == NULL)
      snprintf(buf, size, "%s(null)", optr);
    else
      snprintf(buf, size, "%s\"%.*s\"", optr,
               (int)(strcel->strcel_e - strcel->strcel_s), strcel->strcel_s);
    return buf;
  } else {
    /*  Parenthesized list
     */
    if (size < 10) return "(...)";

    snprintf(buf, size, "%s(", optr);
    s = buf + strlen(buf);
    e = buf + size;

    while (strcel != NULL && e - s >= 20) {
      if (strcel->strcel_s == NULL) {
        memcpy(s, "null", 4);
        s += 4;
      } else {
        snprintf(s, (size_t)(e - s) - 5, "\"%.*s\"",
                 (int)(strcel->strcel_e - strcel->strcel_s), strcel->strcel_s);
        s += strlen(s);
      }
      *s++ = " )"[(strcel = strcel->strcel_next) == NULL];
      *s = '\0';
    }
    if (strcel != NULL && e - s > 4) {
      *s++ = '.';
      *s++ = '.';
      *s++ = ')';
      *s = '\0';
    } else {
      s[-1] = ')';
    }
  }
  return buf;
}

/**
 * @brief Return a string representation of a string constraint.
 *
 * @param greq		Request we're working for
 * @param con		constraint strcon is part of
 * @param strcon	string constraint to pick from
 * @param which		-1: lowest, 1: highest.
 *
 * @return the lowest/highest element, or NULL if there are none.
 */
int graphd_string_constraint_to_signature(
    graphd_string_constraint const *strcon, cm_buffer *sig, bool write_values) {
  graphd_string_constraint_element *strcel;
  char obuf[10];
  char const *optr;

  if (strcon == NULL) return 0;

  optr = op_to_string(strcon->strcon_op, obuf, sizeof obuf);
  if ((strcel = strcon->strcon_head) == NULL)
    /* Empty constraint.
     */
    return cm_buffer_sprintf(sig, "%snull", optr);

  else if (strcel->strcel_next == NULL || !write_values) {
    /*  Single-elment constraint, or we don't care about text--
     */
    if (strcel->strcel_s == NULL)
      return cm_buffer_sprintf(sig, "%s(null)", optr);
    else if (write_values)
      return cm_buffer_sprintf(sig, "%s\"%.*s\"", optr,
                               (int)(strcel->strcel_e - strcel->strcel_s),
                               strcel->strcel_s);
    else
      return cm_buffer_sprintf(sig, "%s\"...\"", optr);
  } else {
    int err;

    /*  Parenthesized list
     */
    err = cm_buffer_sprintf(sig, "%s(", optr);
    if (err != 0) return err;

    while (strcel != NULL) {
      if (strcel->strcel_s == NULL)
        err = cm_buffer_sprintf(sig, "null");
      else
        err = cm_buffer_sprintf(sig, "\"%.*s\"",
                                (int)(strcel->strcel_e - strcel->strcel_s),
                                strcel->strcel_s);

      if ((strcel = strcel->strcel_next) == NULL)
        err = cm_buffer_add_string(sig, ")");
      else
        err = cm_buffer_add_string(sig, " ");
    }
  }
  return 0;
}

/**
 * @brief Pick the lowest/highest from a list of string constraint elements.
 *
 * @param greq		Request we're working for
 * @param con		constraint strcon is part of
 * @param strcon	string constraint to pick from
 * @param which		-1: lowest, 1: highest.
 *
 * @return the lowest/highest element, or NULL if there are none.
 */
graphd_string_constraint_element *graphd_string_constraint_pick(
    graphd_request *greq, graphd_constraint *con,
    graphd_string_constraint *strcon, int which) {
  graphd_string_constraint_element *this, *best = NULL;
  graphd_comparator const *cmp;

  if ((cmp = con->con_value_comparator) == NULL)
    cmp = graphd_comparator_unspecified;

  for (this = strcon->strcon_head; this != NULL; this = this->strcel_next) {
    if (best == NULL ||
        ((*cmp->cmp_sort_compare)(greq, this->strcel_s, this->strcel_e,
                                  best->strcel_s, best->strcel_e) < 0) ==
            (which < 0))
      best = this;
  }
  return best;
}

/**
 * @brief Allocate a string constraint element for a request.
 *
 *   The string constraint element is allocated on the
 *   request's heap.  The string it points to is not copied;
 *   this just sets the pointers.
 *
 * @param greq	Request to allocate for
 * @param s	beginning of the string
 * @param e	end of the string
 *
 * @return NULL on allocation error, otherwise the new element.
 */
graphd_string_constraint_element *graphd_string_constraint_element_alloc(
    graphd_request *greq, char const *s, char const *e) {
  graphd_string_constraint_element *cel;

  cel = cm_malloc(greq->greq_req.req_cm, sizeof(*cel));
  if (cel == NULL) return cel;

  cel->strcel_s = s;
  cel->strcel_e = e;
  cel->strcel_next = NULL;

  return cel;
}

/**
 * @brief Append a string constraint element to a string constraint.
 *
 * @param strcon	constraint to append to
 * @param cel		element to append
 */
void graphd_string_constraint_add_element(
    graphd_string_constraint *strcon, graphd_string_constraint_element *cel) {
  cel->strcel_next = NULL;
  *strcon->strcon_tail = cel;
  strcon->strcon_tail = &cel->strcel_next;
}

/*  Is this string in this string constraint?
 */
int graphd_string_constraint_member(graphd_request *greq,
                                    graphd_comparator const *cmp,
                                    graphd_string_constraint const *strcon,
                                    char const *s, char const *e) {
  graphd_string_constraint_element const *cel;

  if (strcon->strcon_head == NULL)
    return cmp->cmp_sort_compare(greq, NULL, NULL, s, e) == 0;

  for (cel = strcon->strcon_head; cel != NULL; cel = cel->strcel_next)
    if ((cmp->cmp_sort_compare)(greq, cel->strcel_s, cel->strcel_e, s, e) == 0)

      return true;
  return false;
}

/*  What string constraint did we just add?
 */
graphd_string_constraint *graphd_string_constraint_last(
    graphd_string_constraint_queue const *q) {
  if (q->strqueue_head == NULL) return NULL;

  return (graphd_string_constraint *)((char *)q->strqueue_tail -
                                      offsetof(graphd_string_constraint,
                                               strcon_next));
}

/*  What string constraint did we just add?
 */
int graphd_string_constraint_element_last(
    graphd_string_constraint_queue const *q, char const **out_s,
    char const **out_e) {
  graphd_string_constraint const *last;
  graphd_string_constraint_element const *cel;

  if (q->strqueue_head == NULL) return GRAPHD_ERR_NO;

  last = (graphd_string_constraint const *)((char *)q->strqueue_tail -
                                            offsetof(graphd_string_constraint,
                                                     strcon_next));
  if (last->strcon_head == NULL) return GRAPHD_ERR_NO;

  cel = (graphd_string_constraint_element const
             *)((char *)last->strcon_tail -
                offsetof(graphd_string_constraint_element, strcel_next));
  *out_s = cel->strcel_s;
  *out_e = cel->strcel_e;

  return 0;
}

graphd_string_constraint *graphd_string_constraint_alloc(
    graphd_request *greq, graphd_constraint *con,
    graphd_string_constraint_queue *q, int op) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_string_constraint *strcon;

  cl_assert(cl, q != NULL);
  cl_assert(cl, q->strqueue_tail != NULL);

  if (con->con_strcon_n <
      sizeof(con->con_strcon_buf) / sizeof(*con->con_strcon_buf)) {
    cl_cover(cl);
    strcon = con->con_strcon_buf + con->con_strcon_n++;
  } else {
    cl_cover(cl);
    strcon = cm_talloc(greq->greq_req.req_cm, graphd_string_constraint, 1);
  }

  if (strcon == NULL) return strcon;

  strcon->strcon_next = NULL;
  strcon->strcon_op = op;

  *q->strqueue_tail = strcon;
  q->strqueue_tail = &strcon->strcon_next;

  strcon->strcon_head = NULL;
  strcon->strcon_tail = &strcon->strcon_head;

  cl_assert(cl, q->strqueue_head != NULL);
  cl_cover(cl);

  return strcon;
}

/**
 * @brief Compare two string constraint queues for equality.
 *
 *   Together with graphd_string_constraint_hash(), below, this
 *   is part of a generic framework for identifying identical
 *   graphd constraints - nothing to do with actually matching
 *   strings.
 *
 * @param cl 	Log through here
 * @param a 	constraint queue
 * @param b 	another constraint queue
 *
 * @return true if they're equal, false otherwise.
 */
static bool graphd_string_constraint_equal(
    cl_handle *cl, graphd_string_constraint const *a_strcon,
    graphd_string_constraint const *b_strcon) {
  graphd_string_constraint_element const *a, *b;

  cl_assert(cl, a_strcon != NULL);
  cl_assert(cl, b_strcon != NULL);

  for (a = a_strcon->strcon_head, b = b_strcon->strcon_head;
       a != NULL && b != NULL; a = a->strcel_next, b = b->strcel_next) {
    if (a->strcel_s == NULL && b->strcel_s == NULL) continue;

    if (a->strcel_s == NULL || b->strcel_s == NULL) return false;

    if (a->strcel_e - a->strcel_s != b->strcel_e - b->strcel_s) return false;

    /*  Case-sensitive, even though most of the time,
     *  case-insensitive would suffice.
     */
    if (memcmp(a->strcel_s, b->strcel_s, b->strcel_e - b->strcel_s) != 0)
      return false;
  }
  return a == NULL && b == NULL;
}

/**
 * @brief Compare two string constraint queues for equality.
 *
 *   Together with graphd_string_constraint_hash(), below, this
 *   is part of a generic framework for identifying identical
 *   graphd constraints - nothing to do with actually matching
 *   strings.
 *
 * @param cl 	Log through here
 * @param a 	constraint queue
 * @param b 	another constraint queue
 *
 * @return true if they're equal, false otherwise.
 */
bool graphd_string_constraint_queue_equal(
    cl_handle *cl, graphd_string_constraint_queue const *a_queue,
    graphd_string_constraint_queue const *b_queue) {
  graphd_string_constraint const *a, *b;

  cl_assert(cl, a_queue != NULL);
  cl_assert(cl, b_queue != NULL);

  for (a = a_queue->strqueue_head, b = b_queue->strqueue_head;
       a != NULL && b != NULL; a = a->strcon_next, b = b->strcon_next) {
    if (a->strcon_op != b->strcon_op) return false;
    cl_assert(cl, a->strcon_op != GRAPHD_OP_UNSPECIFIED);
    if (!graphd_string_constraint_equal(cl, a, b)) return false;
  }
  return a == NULL && b == NULL;
}

/**
 * @brief Hash a string constraint.
 *
 *  The hash computed identifies the constraint that contains
 *  it -- this is about finding identical subconstraints in a
 *  nested tree, not about matching strings.
 *
 * @param cl 		Log through here
 * @param q 		string constraint queue
 * @param hash_inout 	add the constraint hashes to this accumulator.
 */
void graphd_string_constraint_hash(cl_handle *cl,
                                   graphd_string_constraint_queue const *q,
                                   unsigned long *hash_inout) {
  graphd_string_constraint const *c;

  cl_assert(cl, q != NULL);

  for (c = q->strqueue_head; c != NULL; c = c->strcon_next) {
    graphd_string_constraint_element const *cel;
    cl_assert(cl, c->strcon_op != GRAPHD_OP_UNSPECIFIED);
    GRAPHD_HASH_VALUE(*hash_inout, c->strcon_op);

    for (cel = c->strcon_head; cel != NULL; cel = cel->strcel_next)
      if (cel->strcel_s != NULL)
        GRAPHD_HASH_BYTES(*hash_inout, cel->strcel_s, cel->strcel_e);
  }
}

/**
 * @brief Does this string constraint contain anything that clashes with s..e?
 *
 *  If yes, return an ascii rendering of the offending constraint.
 *
 * @param cl 		Log through here
 * @param strcon 	start of string constraint queue
 * @param s 		string to conflict with
 * @param e 		end of string to conflict with
 * @param s_out 	out: assign conflicting string to here
 * @param e_out 	out: end of conflicting string
 *
 * @return true if there is a conflict, false otherwise.
 */
bool graphd_string_constraint_contradiction(
    cl_handle *cl, graphd_string_constraint const *strcon, char const *s,
    char const *e, char const **s_out, char const **e_out) {
  graphd_string_constraint const *sc;

  for (sc = strcon; sc != NULL; sc = sc->strcon_next) {
    graphd_string_constraint_element const *cel;

    cl_assert(cl, sc->strcon_op == GRAPHD_OP_EQ);
    if (sc->strcon_head == NULL) {
      if (s != NULL) {
      conflict_with_null:
        *s_out = "null";
        *e_out = *s_out + 4;

        return true;
      }
    }
    for (cel = sc->strcon_head; cel != NULL; cel = cel->strcel_next) {
      if (s == NULL && cel->strcel_s == NULL) continue;

      if (cel->strcel_s == NULL) goto conflict_with_null;

      if (e - s != cel->strcel_e - cel->strcel_s ||
          memcmp(s, cel->strcel_s, e - s) != 0) {
        *s_out = cel->strcel_s;
        *e_out = cel->strcel_e;

        return true;
      }
    }
  }

  /* No conflicts found.
   */
  return false;
}
