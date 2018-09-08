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

#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <limits.h>
#include <ctype.h>

/**
 * @file graphd-value.c
 * @brief Utilities that handle graph query result nodes.
 *
 *  Note that these values are different from the "value" primitive component.
 *
 *  The values this module is about are the dynamically-typed building
 *  blocks of graphd responses, encoded as the graphd_value union.
 */

/**
 * @brief Initialize a value to a safe, empty value.
 *
 *  After calling graphd_value_initialize, it is legal to either
 *  call graphd_value_finish() (which will do nothing) or *not*
 *  call graphd_value_finish().  That is, no resources are allocated,
 *  and the underlying datastructure knows that no resources are
 *  allocated.
 *
 * @param val	pointer to the undefined struct to initialize
 */
void graphd_value_initialize(graphd_value *val) {
  val->val_type = GRAPHD_VALUE_UNSPECIFIED;
}

/**
 * @brief Free resources associated with a value.
 *
 *   It is safe (and does nothing) to call graphd_value_finish
 *   multiple times on a value.  After the call, the value
 *   has a well-defined, resource-free, "undefined" value.
 *
 * @param cl	Log through this if anything unusual happens
 * @param val	Value to free.
 */
void graphd_value_finish(cl_handle *cl, graphd_value *val) {
  graphd_value *s, *e;

  /*
          char		 buf[200];
          cl_log(cl, CL_LEVEL_DEBUG, "graphd_value_finish(%s)",
                  graphd_value_to_string(val, buf, sizeof buf));
  */

  switch (val->val_type) {
    case GRAPHD_VALUE_LIST:
    case GRAPHD_VALUE_SEQUENCE:

      /* Free array elements. */
      if (val->val_array_m > 0) {
        s = val->val_array_contents;
        e = s + val->val_array_n;
        while (s < e) graphd_value_finish(cl, s++);

        if (val->val_array_cm != NULL)
          cm_free(val->val_array_cm, val->val_array_contents);
      }
      break;

    case GRAPHD_VALUE_RECORDS:
      break;

    case GRAPHD_VALUE_STRING:
    case GRAPHD_VALUE_ATOM:

      /*  The text might be constant, might come from a mapped-in
       *  primitive, or might have been dynamically allocated.
       *  Free the dynamic allocation, or release the lock on the
       *  primitive.  (If there is no lock, it's still safe to
       *  call pdb_primitive_reference_free.)
       */
      if (val->val_text_cm != NULL)
        cm_free(val->val_text_cm, (char *)val->val_text_s);
      else
        pdb_primitive_reference_free(&val->val_text_ref);

      break;

    case GRAPHD_VALUE_DEFERRED: {
      graphd_value tmp;

      /* Avoid circular frees. */
      tmp = *val;
      val->val_type = GRAPHD_VALUE_UNSPECIFIED;

      (*tmp.val_deferred_base->db_type->dt_finish)(&tmp);
    } break;

    default:
      break;
  }

  val->val_type = GRAPHD_VALUE_UNSPECIFIED;
}

/**
 * @brief Allocate a list with room for a certain number of elements.
 *
 *  The n list slots (but not the list itself) are counted in
 *  g's value allocation counter.
 *
 *  After a successful call, the n graphd_value structures
 *  pointed to by val-&gt;val_list_contents have well-defined
 *  GRAPHD_VALUE_UNSPECIFIED values and are available for assignment.
 *
 * @param g	Opaque module handle
 * @param cm	Allocate on this heap.
 * @param cl	Log through this if anything unusual happens
 * @param val	Out: the list value is created in these bytes
 * @param n	number of slots to allocate.
 *
 * @return 0 on success
 * @return ENOMEM on allocation error
 */
int graphd_value_list_alloc(graphd_handle *g, cm_handle *cm, cl_handle *cl,
                            graphd_value *val, size_t n) {
  cl_assert(cl, val != NULL);

  val->val_type = GRAPHD_VALUE_LIST;
  val->val_array_cm = cm;

  if ((val->val_array_n = n) == 0) {
    val->val_array_m = 0;
    val->val_array_contents = NULL;
    cl_cover(cl);
  } else {
    val->val_array_contents = cm_talloc(cm, graphd_value, n);
    if (val->val_array_contents == NULL) {
      val->val_type = GRAPHD_VALUE_UNSPECIFIED;
      cl_log(cl, CL_LEVEL_ERROR,
             "graphd_value_list_alloc: "
             "failed to allocate %llu slots in "
             "result token array",
             (unsigned long long)n);
      return ENOMEM;
    }
    g->g_rts_values_allocated += n;
    memset(val->val_array_contents, 0, sizeof(graphd_value) * n);
    val->val_array_m = n;
    cl_cover(cl);
  }
  return 0;
}

/**
 * @brief Initialize a graphd_value as null
 *  Null values read and print as "null".
 * @param val	Value to assign to.
 */
void graphd_value_null_set(graphd_value *val) {
  val->val_type = GRAPHD_VALUE_NULL;
}

/**
 * @brief Set a graphd_value to have a datatype value.
 *
 *  A "datatype" is a built-in enumerated value that
 *  describes possible types of a primitive's "value"
 *  field (which is different from the graphd_value that
 *  this source module is about).  At the graphd interface
 *  language level, datatypes are atoms like "string",
 *  "timestamp"; libgraph's graph_datatype type and functions
 *  deal with datatypes in more detail.
 *
 *  This function turns an unspecified graphd_value-shaped
 *  chunk of memory into a value that holds a datatype of a
 *  certain value.
 *
 * @param cl	Handle to log through.
 * @param val	Value to assign to.
 * @param dt	Desired datatype enum value.  Must be
 * 		a valid datatype, as defined by GRAPH_IS_DATATYPE().
 */
void graphd_value_datatype_set(cl_handle *cl, graphd_value *val, int dt) {
  cl_cover(cl);

  val->val_type = GRAPHD_VALUE_DATATYPE;
  val->val_datatype = dt;
}

/**
 * @brief Set a graphd_value to have a boolean value.
 *
 *  0 is false, nonzero is true; all nonzero valeus
 *  are mapped to 1 by the assignment.
 *
 * @param val	Value to assign to.
 * @param b	Boolean value.
 */
void graphd_value_boolean_set(graphd_value *val, int b) {
  val->val_type = GRAPHD_VALUE_BOOLEAN;
  val->val_boolean = !!b;
}

/**
 * @brief Set a graphd_value to have a specific "records" value.
 *
 *  "Records" are a value type used by the "dump" command.  They're
 *  a way of passing around a set of sequential primitives by simply
 *  describing its boundaries - the primitives aren't actually
 *  handled or copied, and only the latest stage - the formatter -
 *  will access the underlying primitive records and print their contents.
 *
 *  It is up to the caller to make sure that the primitives in
 *  question actually exist.
 *
 * @param val	Value to assign to.
 * @param pdb	Source of the record set
 * @param i	Local ID of the first primitive
 * @param n	number of primitives
 */
void graphd_value_records_set(graphd_value *val, pdb_handle *pdb, pdb_id i,
                              unsigned long long n) {
  val->val_type = GRAPHD_VALUE_RECORDS;
  val->val_records_pdb = pdb;
  val->val_records_i = i;
  val->val_records_n = n;
}

/**
 * @brief Set a graphd_value to have a specific "deferred" value.
 *
 *  "Deferred" values are evaluated at a later time by evaluating
 *  their context con for the parent ID id, and then extracting
 *  the pattern PAT from that result (or simply the contents, if
 *  no pattern is specified.)
 *
 * @param val		Value to assign to.
 * @param ind		Index into the deferred base
 * @param db		Deferred base.
 */
void graphd_value_deferred_set(graphd_value *val, size_t ind,
                               graphd_deferred_base *db) {
  val->val_type = GRAPHD_VALUE_DEFERRED;
  val->val_deferred_base = db;
  val->val_deferred_index = ind;

  db->db_link++;
}

/**
 * @brief Set a graphd_value to a fixed string without quotes.
 *
 *  The value will be printed without quotes (that's what
 *  I mean by "atom"), and the underlying string will not
 *  be freed when the graphd_value is freed (that's what
 *  I mean by "constant").
 *
 *  This is used mostly in deprecated situations (for the
 *  "meta" value of a primitive) and, for some reason,
 *  with cursors, where atoms and strings are treated
 *  identically.
 *
 * @param val	Value to assign to.
 * @param lit	constant string value
 * @param n	number of characters pointed to by lit,
 *		not including a trailing '\\0', if any.
 */
void graphd_value_atom_set_constant(graphd_value *val, char const *lit,
                                    size_t n) {
  val->val_type = GRAPHD_VALUE_ATOM;

  val->val_text_s = lit;
  val->val_text_e = lit + n;
  val->val_text_cm = NULL;

  pdb_primitive_reference_initialize(&val->val_text_ref);
}

/**
 * @brief Set a graphd_value to a piece of text.
 *
 *  If pr is non-NULL, this call will create and track a tile reference
 *  to the text it contains.  The value must be freed with
 *  graphd_value_finish().  (All values should be freed that way,
 *  but with this one, we really mean it.)
 *
 * @param val	Value to assign to.
 * @param type	type of the resulting graphd_value, typically
 *		GRAPHD_VALUE_STRING or GRAPHD_VALUE_ATOM.
 * @param s	pointer to the start of the string value
 * @param e	pointer to the first byte after the string value (if the
 *		string had a '\\0', e would point to it - but it doesn't
 *		have to have a '\\0').
 * @param pr	NULL or the primitive that is a source of that value.
 *		If the primitive is NULL, the text lives somewhere else
 *		and will not be freed when the graphd_value is freed.
 *		If the primitive is non-NULL, the text is part of the
 *		primitive body.  The called code acquires a reference
 *		to the primitive and will release the reference once
 *		the value is freed.
 * @param file	name of the calling source file, usually inserted by a macro
 * @param line	line of the calling source file, usually inserted by a macro
 */
void graphd_value_text_set_loc(graphd_value *val, int type, char const *s,
                               char const *e, pdb_primitive const *pr,
                               char const *file, int line) {
  val->val_type = type;
  val->val_text_s = s;
  val->val_text_e = e;
  val->val_text_cm = NULL;

  if (pr != NULL)
    pdb_primitive_reference_from_primitive_loc(&val->val_text_ref, pr, file,
                                               line);
  else
    pdb_primitive_reference_initialize(&val->val_text_ref);
}

/**
 * @brief Set a graphd_value to a piece of text.
 *
 *  The text has been allocated on a heap, and will be freed
 *  against that heap once the value is freed.
 *
 * @param val	Value to assign to.
 * @param type	type of the resulting graphd_value, typically
 *		GRAPHD_VALUE_STRING or GRAPHD_VALUE_ATOM.
 * @param s	pointer to the start of the string value.
 * @param n	total number of bytes in the string value, not
 *		including a trailing '\\0' if any.  (There doesn't
 *		actually have to be a trailing \\0.)
 * @param cm	Heap that s[..n] was allocated on and will be returned to,
 *		or NULL.
 */
void graphd_value_text_set_cm(graphd_value *val, int type, char *s, size_t n,
                              cm_handle *cm) {
  val->val_type = type;
  val->val_text_s = s;
  val->val_text_e = s + n;
  val->val_text_cm = cm;

  pdb_primitive_reference_initialize(&val->val_text_ref);
}

/**
 * @brief Allocate an uninitialized piece of text.
 *
 *  Use this if you'd normally construct the result text
 *  into a fixed-length buffer to avoid the extra copy
 *  from the buffer into a graphd_value instance.
 *
 * @param cm 	Heap to allocate the text on.
 * @param val	Value to assign to.
 * @param type	type of the resulting graphd_value, typically
 *		GRAPHD_VALUE_STRING or GRAPHD_VALUE_ATOM.
 * @param n	total number of bytes in the string value,
 *		not including a '\\0', if any.
 *
 * @return 0 on success
 * @return ENOMEM or another system error on allocation error.
 */
int graphd_value_text_alloc(cm_handle *cm, graphd_value *val, int type,
                            size_t n) {
  char *str;

  str = cm_malloc(cm, n + 1);
  if (str == NULL) return errno ? errno : ENOMEM;

  val->val_type = type;
  val->val_text_cm = cm;
  val->val_text_s = str;
  val->val_text_e = str + n;
  str[n] = '\0';
  pdb_primitive_reference_initialize(&val->val_text_ref);
  return 0;
}

/**
 * @brief Duplicate a piece of text into an independent heap.
 *
 *  Use this to create values that refer to text with a
 *  duration less than the value you're creating.
 *
 * @param cm 	Heap to allocate the text on.
 * @param val	Value to assign to.
 * @param type	type of the resulting graphd_value, typically
 *		GRAPHD_VALUE_STRING or GRAPHD_VALUE_ATOM.
 * @param s	pointer to the first byte of text.
 * @param e	pointer just beyond the last byte of text, not
 *		including a '\\0'.  (The text doesn't need to
 *		be '\\0'-terminated, but if it were, e would
 *		point to the '\\0'.)
 *
 * @return 0 on success
 * @return ENOMEM or another system error on allocation error.
 */
int graphd_value_text_strdup(cm_handle *cm, graphd_value *val, int type,
                             char const *s, char const *e) {
  char *str_dup;

  str_dup = cm_substr(cm, s, e);
  if (str_dup == NULL) return ENOMEM;

  val->val_type = type;
  val->val_text_cm = cm;
  val->val_text_s = str_dup;
  val->val_text_e = str_dup + (e - s);

  pdb_primitive_reference_initialize(&val->val_text_ref);

  return 0;
}

/**
 * @brief Turn an uninitialized graphd_value into a number.
 *
 * @param val	Value to assign to.
 * @param num	Desired number value.
 */
void graphd_value_number_set(graphd_value *val, unsigned long long num) {
  val->val_type = GRAPHD_VALUE_NUMBER;
  val->val_data.data_number = num;
}

/**
 * @brief Turn an uninitialized graphd_value into a timestamp.
 *
 *  More functions for dealing with timestamps are in libgraph.
 *
 * @param val	Value to assign to.
 * @param ts	Desired timestamp value.
 * @param id	ID that goes with that timestamp, or PDB_ID_NONE.
 */
void graphd_value_timestamp_set(graphd_value *val, graph_timestamp_t ts,
                                pdb_id id) {
  val->val_type = GRAPHD_VALUE_TIMESTAMP;
  val->val_data.data_timestamp.gdt_timestamp = ts;
  val->val_data.data_timestamp.gdt_id = id;
}

/**
 * @brief Make a GUID-typed value.
 * @param val	value to assign to
 * @param guid 	NULL (for a null GUID) or the GUID to assign.
 */
void graphd_value_guid_set(graphd_value *val, graph_guid const *guid) {
  val->val_type = GRAPHD_VALUE_GUID;
  if (guid != NULL)
    val->val_data.data_guid = *guid;
  else
    GRAPH_GUID_MAKE_NULL(val->val_data.data_guid);
}

/**
 * @brief Initialize a sequence result node.
 *
 *  Sequences and lists have the same underlying representation,
 *  a dynamic array.  Sequences are displayed without () and usually
 *  grow dynamically; lists are displayed with () and are usually
 *  allocated with a fixed size.
 *
 * @param cm 	Heap for the element storage (typically, the request's heap)
 * @param arval	Storage to initialize.
 */
void graphd_value_sequence_set(cm_handle *cm, graphd_value *arval) {
  arval->val_type = GRAPHD_VALUE_SEQUENCE;

  arval->val_array_n = 0;
  arval->val_array_m = 0;
  arval->val_array_contents = NULL;
  arval->val_array_cm = cm;
}

/**
 * @brief Utility: enlarge a list or sequence.
 *
 *  This is a wrapper aound graphd_value_array_grow_loc that
 *  automatically inserts the filename and line number of
 *  the calling code.
 *
 * @param g 	global module handle
 * @param cl 	log through this
 * @param ar	sequence or list to resize
 * @param n	add this many value slots
 *
 * @return 0 on success.
 * @return ENOMEM or another nonzero error code on allocation failure.
 */
#define graphd_value_array_grow(g, cl, ar, n) \
  graphd_value_array_grow_loc(g, cl, ar, n, __FILE__, __LINE__)

/**
 * @brief Utility: enlarge a list or sequence.
 *
 *  The n new list or sequence slots are counted in
 *  g's value allocation counter.
 *
 *  After a successful call, the n graphd_value structures
 *  pointed to by val-&gt;val_list_contents have well-defined
 *  GRAPHD_VALUE_UNSPECIFIED values and are available for
 *  assignment.
 *
 *  The new slots are not yet counted in arval's element count;
 *  it is up to the caller to increment arval-&gt;val_array_n
 *  (or their list- or sequence-equivalent.)
 *
 * @param g 	global module handle
 * @param cl 	log through this
 * @param arval	sequence or list to resize
 * @param n	add this many value slots
 * @param file	calling source file name, usually added by a macro
 * @param line	calling source file line, usually added by a macro
 *
 * @return 0 on success.
 * @return ENOMEM or another nonzero error code on allocation failure.
 */
static int graphd_value_array_grow_loc(graphd_handle *g, cl_handle *cl,
                                       graphd_value *arval, size_t n,
                                       char const *file, int line) {
  cl_assert(cl, arval != NULL);
  cl_assert(cl, GRAPHD_VALUE_IS_ARRAY(*arval));
  cl_assert(cl, arval->val_array_n <= arval->val_array_m);

  if (n == 0) return 0;

  if ((arval->val_array_n + n) > arval->val_array_m) {
    graphd_value *tmp;

    cl_assert(cl, arval->val_array_cm != NULL);
    tmp = cm_trealloc_loc(arval->val_array_cm, graphd_value,
                          arval->val_data.data_array.array_contents,
                          arval->val_data.data_array.array_m + n, file, line);
    if (tmp == NULL) return errno ? errno : ENOMEM;

    g->g_rts_values_allocated += n;
    arval->val_array_m += n;
    arval->val_array_contents = tmp;

    cl_cover(cl);
  }

  cl_assert(cl, arval != NULL);
  cl_assert(cl, (long long)n > 0);
  cl_assert(cl, arval->val_array_n <= arval->val_array_m);
  cl_assert(cl, arval->val_array_n + n <= arval->val_array_m);

  return 0;
}

/**
 * @brief Enlarge a list or sequence.
 *
 *  Make sure that the array elements val-&gt;val_array_n through
 *  val-&gt;val_array_n + (n - 1) exist and have storage initialized
 *  to undefined values.
 *
 *  The new slots are not yet counted in arval's element count;
 *  it is up to the caller to call graphd_value_array_alloc_commit()
 *  with the number of elements actually populated.
 *
 * @param g 	global module handle
 * @param cl 	log through this
 * @param arval	sequence or list to resize
 * @param n	add this many value slots
 * @param file	calling source file name, usually added by a macro
 * @param line	calling source file line, usually added by a macro
 *
 * @return 0 on success.
 * @return ENOMEM or another nonzero error code on allocation failure.
 */
graphd_value *graphd_value_array_alloc_loc(graphd_handle *g, cl_handle *cl,
                                           graphd_value *arval, size_t n,
                                           char const *file, int line) {
  int err;

  cl_assert(cl, arval != NULL);
  cl_assert(cl, (long long)n > 0);
  cl_assert(cl, GRAPHD_VALUE_IS_ARRAY(*arval));

  if (arval->val_array_n + n >= arval->val_array_m &&
      (err = graphd_value_array_grow_loc(g, cl, arval, n, file, line))) {
    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_value_array_alloc: "
           "graphd_value_array_grow(%llu) fails: %s [for %s:%d]",
           (unsigned long long)n, graphd_strerror(err), file, line);
    return NULL;
  }

  cl_assert(cl, arval->val_array_n + n <= arval->val_array_m);
  memset(arval->val_array_contents + arval->val_array_n, 0,
         sizeof(*arval->val_array_contents) * n);
  cl_cover(cl);

  return arval->val_array_contents + arval->val_array_n;
}

/**
 * @brief Commit to having actually filled in a number of added elements
 *
 *  When adding values to an array, first, new slots are allocated
 *  using graphd_value_array_alloc().  Then those slots are directly
 *  accessed and populated; and after each has been filled, a call
 *  to graphd_value_array_alloc_commit() increments the actual element
 *  counter to cover the new slot.
 *
 * @param cl 	log through this
 * @param arval	sequence or list to resize
 * @param n	add this many slots
 */
void graphd_value_array_alloc_commit(cl_handle *cl, graphd_value *arval,
                                     size_t n) {
  cl_assert(cl, arval != NULL);
  cl_assert(cl, GRAPHD_VALUE_IS_ARRAY(*arval));
  cl_assert(cl, (long long)n > 0);
  cl_assert(cl, arval->val_array_n + n <= arval->val_array_m);
  cl_cover(cl);

  arval->val_array_n += n;
}

/**
 * @brief Append to a sequence, destryoing the original value.
 *
 *  If src is a sequence, the move takes the contents
 *  of src; otherwise, src itself is appended to dst.
 *
 *  The source value of this operation is destroyed by it.
 *  (It may be freed afterwards, but that doesn't do anything.)
 *
 * @param g 	global module handle
 * @param cl 	Log and assert through this
 * @param dst	Append to this list or sequence.
 * @param src	Use this value.  The value is destroyed by the call.
 *
 * @return 0 on success
 * @return ENOMEM or another nonzero error value on allocation error.
 */
int graphd_value_sequence_append(graphd_handle *g, cl_handle *cl,
                                 graphd_value *dst, graphd_value *src) {
  int err = 0;

  cl_assert(cl, dst != NULL);
  cl_assert(cl, GRAPHD_VALUE_IS_ARRAY(*dst));

  if (src->val_type != GRAPHD_VALUE_SEQUENCE) {
    int err;

    if (dst->val_array_n >= dst->val_array_m) {
      err = graphd_value_array_grow(
          g, cl, dst, dst->val_array_n >= 6 ? 64 : 1 << dst->val_array_n);
      if (err) return err;
      cl_cover(cl);
    }
    cl_assert(cl, dst->val_array_n + 1 <= dst->val_array_m);
    dst->val_array_contents[dst->val_array_n++] = *src;
    cl_assert(cl, dst != NULL);
    cl_assert(cl, dst->val_array_n <= dst->val_array_m);
  } else {
    graphd_value *t_dst;
    size_t n;

    n = src->val_data.data_array.array_n;
    if (n > 0) {
      t_dst = graphd_value_array_alloc(g, cl, dst, n);
      if (t_dst == NULL) {
        /* XXX error */
        return ENOMEM;
      }
      memcpy(t_dst, src->val_array_contents, n * sizeof(*t_dst));
      graphd_value_array_alloc_commit(cl, dst, n);
      cl_cover(cl);
    }
    if (src->val_data.data_array.array_m > 0)
      cm_free(src->val_data.data_array.array_cm,
              src->val_data.data_array.array_contents);
  }
  src->val_type = GRAPHD_VALUE_UNSPECIFIED;

  return err;
}

/**
 * @brief Truncate a list or sequence, freeing the removed values.
 *
 *  If the array is shorter than the length it is being
 *  truncated to, nothing happens.
 *
 * @param cl 	Log and assert through this
 * @param arval	Truncate this array.
 * @param len	Truncate to this length.
 */
void graphd_value_array_truncate(cl_handle *cl, graphd_value *arval,
                                 size_t len) {
  cl_assert(cl, arval != NULL);
  cl_assert(cl, GRAPHD_VALUE_IS_ARRAY(*arval));

  if (arval->val_array_n > len) {
    graphd_value *el = arval->val_array_contents + len;
    graphd_value *end = arval->val_array_contents + arval->val_array_n;

    for (; el < end; el++) {
      graphd_value_finish(cl, el);
      cl_cover(cl);
    }
    arval->val_array_n = len;
  }
}

/**
 * @brief Remove items from a list or sequence, freeing the removed values.
 *
 *  If the offset is too large, nothing happens.  If the number
 *  of elements is too large, it is truncated to fit.
 *
 * @param cl 		Log and assert through this
 * @param arval		Remove from this array.
 * @param offset	Start removing here (first item is 0)
 * @param nelems	Remove this many elements.
 */
void graphd_value_array_delete_range(cl_handle *cl, graphd_value *arval,
                                     size_t offset, size_t nelems) {
  graphd_value *el, *end;

  cl_assert(cl, arval != NULL);
  cl_assert(cl, GRAPHD_VALUE_IS_ARRAY(*arval));

  if (offset >= arval->val_array_n) return;

  if (offset + nelems > arval->val_array_n)
    nelems = arval->val_array_n - offset;

  if (nelems == 0) return;

  /*  Free the contents of the elements to be removed.
   */
  el = arval->val_array_contents + offset;
  for (end = el + nelems; el < end; el++) graphd_value_finish(cl, el);

  /* Move the trailing elements up to close the gap.
   */
  memmove(
      arval->val_array_contents + offset, end,
      (char *)(arval->val_array_contents + arval->val_array_n) - (char *)end);

  /* Truncate the array to omit the deleted elements.
  */
  arval->val_array_n -= nelems;
}

/**
 * @brief Assign a value to a slot in an array.
 *
 *   Move el to arval[i].  Free the previous arval[i].
 *  The old *el is destroyed in the process.
 *
 * @param g	global module handle
 * @param cl	log through this
 * @param arval	modify this array
 * @param i	index of the element that will be set
 * @param el	new value of the element, will be destroyed after the call.
 *
 * @return 0 	on success
 * @return ENOMEM or some other nonzero error value on allocation failure.
 */
int graphd_value_array_set(graphd_handle *g, cl_handle *cl, graphd_value *arval,
                           size_t i, graphd_value *el) {
  cl_assert(cl, arval != NULL);
  cl_assert(cl, GRAPHD_VALUE_IS_ARRAY(*arval));

  if (i >= arval->val_array_n) {
    graphd_value *val;

    val = graphd_value_array_alloc(g, cl, arval, (i + 1) - arval->val_array_n);
    if (val == NULL) {
      int err = errno;
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_value_array_set: "
             "failed to allocate slot %lu",
             (unsigned long)((i + 1) - arval->val_array_n));
      return err ? err : ENOMEM;
    }
    graphd_value_array_alloc_commit(cl, arval, (i + 1) - arval->val_array_n);
    cl_cover(cl);
  }

  graphd_value_finish(cl, arval->val_array_contents + i);
  arval->val_array_contents[i] = *el;
  graphd_value_initialize(el);

  return 0;
}

/**
 * @brief Print a value for debugging
 *
 * @param t	value to print
 * @param buf	use these bytes for formatting (optionally)
 * @param size	number of usable bytes pointed to by buf.
 *
 * @return a '\\0'-terminated string.
 */
char const *graphd_value_to_string(graphd_value const *t, char *buf,
                                   size_t size) {
  size_t n, len;
  char const *el;
  char elbuf[200], idbuf[200];
  char const *q;
  char *b0 = buf;

  if (t == NULL) return "null";

  switch (t->val_type) {
    case GRAPHD_VALUE_UNSPECIFIED:
      return "unspecified";

    case GRAPHD_VALUE_ATOM:
    case GRAPHD_VALUE_STRING:
      n = t->val_data.data_text.text_e - t->val_data.data_text.text_s;
      if (n > 60)
        n = 60, el = "...";
      else
        el = "";

      if (size <= 10)
        return (t->val_type == GRAPHD_VALUE_STRING) ? "\"...\"" : "...";

      q = (t->val_type == GRAPHD_VALUE_STRING) ? "\"" : "'";
      if (n >= 60)
        snprintf(buf, size, "%s%.*s%s%s[%llu]", q, (int)n,
                 t->val_data.data_text.text_s, el, q,
                 (unsigned long long)(t->val_data.data_text.text_e -
                                      t->val_data.data_text.text_s));
      else
        snprintf(buf, size, "%s%.*s%s%s", q, (int)n,
                 t->val_data.data_text.text_s, el, q);
      return buf;

    case GRAPHD_VALUE_DEFERRED:
      snprintf(buf, size, "<%s %zu %p>", t->val_deferred_base->db_type->dt_name,
               t->val_deferred_index, t->val_deferred_base->db_data);
      return buf;

    case GRAPHD_VALUE_RECORDS:
      snprintf(buf, size, "<records %llu[%llu]>",
               (unsigned long long)t->val_records_i,
               (unsigned long long)t->val_records_n);
      return buf;

    case GRAPHD_VALUE_BOOLEAN:
      return t->val_boolean ? "true" : "false";

    case GRAPHD_VALUE_DATATYPE:
      q = graph_datatype_to_string(t->val_datatype);
      if (q == NULL) {
        snprintf(buf, size, "%hu", t->val_datatype);
        q = buf;
      }
      return q;

    case GRAPHD_VALUE_NUMBER:
      snprintf(buf, size, "%llu", (unsigned long long)t->val_data.data_number);
      return buf;

    case GRAPHD_VALUE_TIMESTAMP:
      if (t->val_timestamp_id == PDB_ID_NONE)
        strcpy(idbuf, "-");
      else
        snprintf(idbuf, sizeof elbuf, "%llu",
                 (unsigned long long)t->val_timestamp_id);

      if (GRAPH_TIMESTAMP_TIME(t->val_timestamp) == 0)
        snprintf(buf, size, "<timestamp +%lu/%s>",
                 (unsigned long)GRAPH_TIMESTAMP_SERIAL(t->val_timestamp),
                 idbuf);
      else
        snprintf(buf, size, "%s/%s", graph_timestamp_to_string(
                                         t->val_timestamp, elbuf, sizeof elbuf),
                 idbuf);
      return buf;

    case GRAPHD_VALUE_GUID:
      return graph_guid_to_string(&t->val_data.data_guid, buf, size);

    case GRAPHD_VALUE_LIST:
    case GRAPHD_VALUE_SEQUENCE:

      if (size <= 10)
        return t->val_type == GRAPHD_VALUE_LIST ? "(...)" : "[...]";

      /* save space for trailing q[1] + '\0'
       */
      size -= 2;
      q = (t->val_type == GRAPHD_VALUE_LIST ? "()" : "[]");

      snprintf(b0 = buf, size, "[%lu]%c", (unsigned long)t->val_array_n, q[0]);
      len = strlen(buf);
      size -= len;
      buf += len;
      for (n = 0; n < t->val_array_n && size >= 20; n++) {
        size_t part_n;
        char const *part = graphd_value_to_string(t->val_array_contents + n,
                                                  elbuf, sizeof elbuf);

        if (part == NULL) continue;
        part_n = strlen(part);
        if (part_n + 5 < size) {
          memcpy(buf, part, part_n);
          buf += part_n;
          size -= part_n;

          if (n + 1 < t->val_array_n) {
            *buf++ = ' ';
            size--;
          } else {
            *buf++ = q[1];
            *buf = '\0';

            return b0;
          }
          continue;
        }
        if (size > 20)
          snprintf(buf, (int)size, "%.*s..%c", (int)size - 5, part, q[1]);
        else
          snprintf(buf, (int)size, "..%c", q[1]);

        len = strlen(buf);
        buf += len;
        size -= len;
      }
      *buf++ = q[1];
      *buf++ = '\0';
      return b0;

    case GRAPHD_VALUE_NULL:
      return "null";

    default:
      break;
  }
  snprintf(buf, size, "<unexpected value type %d (%x)>", (int)t->val_type,
           (int)t->val_type);
  return buf;
}

/**
 * @brief Encode a value as bytes
 *
 *  The difference between this and graphd_value_to_string()
 *  is that it's type-presserving, but not very human readable.
 *
 * @param cl	log through this
 * @param val	value to serialize
 * @param buf	Append the bytes to this buffer.
 *
 * @return 0 on success
 * @return ENOMEM on allocation failure
 */
int graphd_value_serialize(cl_handle *cl, graphd_value const *val,
                           cm_buffer *buf) {
  char txt[200];
  size_t i;
  int err;

  if (val == NULL) {
    cl_cover(cl);
    return cm_buffer_add_string(buf, "0");
  }

  switch (val->val_type) {
    case GRAPHD_VALUE_STRING:
    case GRAPHD_VALUE_ATOM:
      snprintf(txt, sizeof txt, "%c%llu:",
               val->val_type == GRAPHD_VALUE_STRING ? 's' : 'a',
               (unsigned long long)(val->val_text_e - val->val_text_s));
      err = cm_buffer_add_string(buf, txt);
      if (err) return err;
      cl_cover(cl);
      return cm_buffer_add_bytes(buf, val->val_text_s,
                                 (size_t)(val->val_text_e - val->val_text_s));

    case GRAPHD_VALUE_NUMBER:
      cl_cover(cl);
      snprintf(txt, sizeof txt, "#%llu.", val->val_number);
      return cm_buffer_add_string(buf, txt);

    case GRAPHD_VALUE_GUID:
      if (GRAPH_GUID_IS_NULL(val->val_guid))
        return cm_buffer_add_string(buf, "-");
      err = cm_buffer_add_string(buf, "g");
      if (err) return err;

      cl_cover(cl);
      return cm_buffer_add_string(
          buf, graph_guid_to_string(&val->val_guid, txt, sizeof txt));

    case GRAPHD_VALUE_LIST:
    case GRAPHD_VALUE_SEQUENCE:
      snprintf(txt, sizeof txt, "%c%llu:",
               val->val_type == GRAPHD_VALUE_SEQUENCE ? '_' : 'l',
               (unsigned long long)val->val_array_n);
      err = cm_buffer_add_string(buf, txt);
      for (i = 0; i < val->val_array_n; i++) {
        err = graphd_value_serialize(cl, val->val_array_contents + i, buf);
        if (err) return err;
        cl_cover(cl);
      }
      return 0;

    case GRAPHD_VALUE_TIMESTAMP:
      cl_cover(cl);
      err = cm_buffer_add_string(buf, "t");
      if (err) return err;
      return cm_buffer_add_string(
          buf, graph_timestamp_to_string(val->val_timestamp, txt, sizeof txt));

    case GRAPHD_VALUE_BOOLEAN:
      cl_cover(cl);
      return cm_buffer_add_string(buf, val->val_boolean ? "b1" : "b0");

    case GRAPHD_VALUE_DATATYPE:
      cl_cover(cl);
      snprintf(txt, sizeof txt, "d%d.", (int)val->val_datatype);
      return cm_buffer_add_string(buf, txt);

    case GRAPHD_VALUE_NULL:
      cl_cover(cl);
      return cm_buffer_add_string(buf, "n");

    case GRAPHD_VALUE_DEFERRED:
      cl_notreached(cl, "attempt to serialize deferred value?");

    case GRAPHD_VALUE_RECORDS:
      cl_notreached(cl, "attempt to serialize records?");

    case GRAPHD_VALUE_UNSPECIFIED:
    default:
      break;
  }

  cl_notreached(cl, "unexpected val->val_type %d", val->val_type);
  /* NOTREACHED */
  return EINVAL;
}

/**
 * @brief Utility: scan a signed integer.
 *
 *  Deserializes a number and advances *s past the terminating
 *  punctuation character.
 *
 * @param s	in: the beginning of the available bytes; out: a pointer
 * 		just past the terminating (arbitrary) punctuation character.
 * @param e 	end of input.
 * @param n_out	out: the integer
 * @return 0 on success
 * @return GRAPHD_ERR_LEXICAL on syntax error
 * @return ERANGE on overflow
 */
static int graphd_value_deserialize_int(char const **s, char const *e,
                                        int *n_out) {
  char const *r = *s;
  unsigned int n = 0, neg = 0;

  if ((neg = (r < e && *r == '-')) != 0) r++;

  while (r < e && isascii((unsigned char)*r) && isdigit(*r)) {
    if (n > (UINT_MAX - (*r - '0')) / 10) return ERANGE;

    n = n * 10;
    n += *r - '0';

    r++;
  }
  if (r >= e) return GRAPHD_ERR_LEXICAL;
  if (neg) {
    if (n > (unsigned int)INT_MAX + 1) return ERANGE;
    *n_out = (int)-n; /* sic */
  } else {
    if (n > INT_MAX) return ERANGE;
    *n_out = n;
  }
  *s = r + 1;
  return 0;
}

/**
 * @brief Turn a string into a value.
 *
 *  This is the opposite counterpart to deserialize_ull().
 *  It is not an error for the string to be too long; *s is simply
 *  advanced to point just after the last consumed byte.
 *
 * @param g		module handle
 * @param cm		allocate resources for the deserialized value here.
 * @param cl		log through this
 * @param val_out	make a new value in this space
 * @param s		Beginning of the text to deserialize, advanced.
 * @param e		End of the text to deserialize.
 *
 * @return 0 on success
 * @return ENOMEM on allocation failure
 * @return GRAPHD_ERR_LEXICAL on syntax error
 * @return ERANGE on integer overflow
 * @return GRAPHD_ERR_NO if the string is empty
 */
int graphd_value_deserialize(graphd_handle *g, cm_handle *cm, cl_handle *cl,
                             graphd_value *val_out, char const **s,
                             char const *e) {
  int err, tp;
  unsigned long long ull, i;
  int d;
  char const *ep;

  if (*s >= e) {
    cl_log(cl, CL_LEVEL_FAIL, "graphd_value_deserialize: null string");
    return GRAPHD_ERR_NO;
  }

  switch (tp = *(*s)++) {
    case 'u':
      graphd_value_initialize(val_out);
      cl_cover(cl);
      return 0;

    case 's':
    case 'a':
      if ((err = graphd_bytes_to_ull(s, e, &ull)) != 0) {
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_value_deserialize: "
               "deserialize_ull fails: %s",
               strerror(err));
        return err;
      }
      if (e - *s < ull) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_value_deserialize: claimed string "
               "length of %llu, but only %llu bytes in the "
               "request?",
               ull, (unsigned long long)(e - *s));
        cl_cover(cl);
        return GRAPHD_ERR_LEXICAL;
      }
      err = graphd_value_text_alloc(
          cm, val_out, tp == 's' ? GRAPHD_VALUE_STRING : GRAPHD_VALUE_ATOM,
          (size_t)ull);
      if (err) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_value_deserialize: "
               "failed to allocate text value: %s",
               strerror(err));
        return err;
      }
      memcpy((char *)val_out->val_text_s, *s, ull);
      val_out->val_text_e = val_out->val_text_s + ull;
      *s += ull;
      cl_cover(cl);
      return 0;

    case '#':
      val_out->val_type = GRAPHD_VALUE_NUMBER;
      if ((err = graphd_bytes_to_ull(s, e, &val_out->val_number)) != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "graphd_bytes_to_ull", err, "\"%.*s\"",
                     (int)(e - *s), *s);
        cl_cover(cl);
        return err;
      }
      cl_cover(cl);
      return 0;

    case '-':
      val_out->val_type = GRAPHD_VALUE_GUID;
      GRAPH_GUID_MAKE_NULL(val_out->val_guid);
      cl_cover(cl);
      return 0;

    case 'g':
      val_out->val_type = GRAPHD_VALUE_GUID;
      for (ep = *s; ep < e && ep < *s + GRAPH_GUID_SIZE - 1 && isascii(*ep) &&
                    isalnum(*ep);
           ep++)
        ;
      err = graph_guid_from_string(&val_out->val_guid, *s, ep);
      if (err != 0) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_value_deserialize: "
               "graph_guid_from_string(%.*s) fails: %s",
               (int)(ep - *s), *s, strerror(err));
        cl_cover(cl);
        return err;
      }
      *s = ep;
      cl_cover(cl);
      return 0;

    case 'l':
    case '_':
      if ((err = graphd_bytes_to_ull(s, e, &ull)) != 0) {
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_value_deserialize: "
               "graphd_bytes_to_ull fails: %s",
               strerror(err));
        return err;
      }
      if (e - *s < ull) {
        cl_cover(cl);
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_value_deserialize: claimed "
               "list length of %llu, but only %llu bytes "
               "in the request?",
               ull, (unsigned long long)(e - *s));
        return GRAPHD_ERR_LEXICAL;
      }
      err = graphd_value_list_alloc(g, cm, cl, val_out, (size_t)ull);
      if (err != 0) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_value_deserialize: "
               "graphd_value_list_alloc(%llu) fails: %s",
               ull, strerror(err));
        return err;
      }
      for (i = 0; i < ull; i++) {
        cl_cover(cl);
        err = graphd_value_deserialize(g, cm, cl,
                                       val_out->val_array_contents + i, s, e);
        if (err) {
          graphd_value_finish(cl, val_out);
          cl_log(cl, CL_LEVEL_FAIL,
                 "graphd_value_deserialize: "
                 "nested call fails: %s",
                 strerror(err));
          return err;
        }
      }
      if (tp == '_') {
        val_out->val_type = GRAPHD_VALUE_SEQUENCE;
        cl_cover(cl);
      }
      return 0;

    case 't':
      for (ep = *s; ep < e && isascii(*ep) &&
                    (isdigit(*ep) || *ep == ':' || *ep == 'T' || *ep == 'Z' ||
                     *ep == '.' || *ep == '-');
           ep++)
        ;
      err = graph_timestamp_from_string(&val_out->val_timestamp, *s, ep);
      if (err != 0) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_value_deserialize: timestamp "
               "format, but don't understand value \"%.*s\"",
               (int)(ep - *s), *s);
        cl_cover(cl);
        return err;
      }
      val_out->val_type = GRAPHD_VALUE_TIMESTAMP;
      *s = ep;
      cl_cover(cl);
      return 0;

    case 'b':
      if (*s >= e || (**s != '0' && **s != '1')) {
        cl_log(cl, CL_LEVEL_FAIL,
               "graphd_value_deserialize: unexpected "
               "boolean value %.*s",
               *s >= e ? 3 : 1, *s >= e ? "EOF" : *s);
        return GRAPHD_ERR_LEXICAL;
      }
      graphd_value_boolean_set(val_out, *(*s)++ != '0');
      return 0;

    case 'd':
      cl_cover(cl);

      err = graphd_value_deserialize_int(s, e, &d);
      if (err) {
        cl_cover(cl);
        return err;
      }
      graphd_value_datatype_set(cl, val_out, d);
      return 0;

    case 'n':
      cl_cover(cl);
      graphd_value_null_set(val_out);
      return 0;

    case 'r':
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_value_deserialize: can't "
             "deserialize records!");
      return GRAPHD_ERR_LEXICAL;

    default:
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_value_deserialize: unexpected "
             "type %x (%c)",
             tp, tp);
      return GRAPHD_ERR_LEXICAL;
  }
}

/**
 * @brief Make an independent copy of a value.
 *
 *  After a successful call, the result must be freed
 *  wtih graphd_value_finish().
 *
 * @param g	module handle
 * @param cm	allocate resources for the new value here.
 * @param cl	log through this
 * @param dst	make a new value in this space
 * @param src	create a duplicate of this, without destroying it.
 *
 * @return 0 on success
 * @return ENOMEM on allocation error.
 */
int graphd_value_copy(graphd_handle *g, cm_handle *cm, cl_handle *cl,
                      graphd_value *dst, graphd_value const *src) {
  size_t i;
  int err;
  graphd_value const *src_child;
  graphd_value *dst_child;

  cl_assert(cl, src != NULL);
  cl_assert(cl, dst != NULL);

  switch (src->val_type) {
    case GRAPHD_VALUE_STRING:
    case GRAPHD_VALUE_ATOM:
      if (src->val_text_cm != NULL)
        return graphd_value_text_strdup(cm, dst, src->val_type, src->val_text_s,
                                        src->val_text_e);

      *dst = *src;
      pdb_primitive_reference_dup(&dst->val_text_ref);
      cl_cover(cl);
      return 0;

    case GRAPHD_VALUE_NUMBER:
    case GRAPHD_VALUE_GUID:
    case GRAPHD_VALUE_TIMESTAMP:
    case GRAPHD_VALUE_BOOLEAN:
    case GRAPHD_VALUE_DATATYPE:
    case GRAPHD_VALUE_NULL:
    case GRAPHD_VALUE_UNSPECIFIED:
    case GRAPHD_VALUE_RECORDS:
      *dst = *src;
      cl_cover(cl);
      return 0;

    case GRAPHD_VALUE_DEFERRED:
      *dst = *src;
      dst->val_deferred_base->db_link++;
      cl_cover(cl);
      return 0;

    case GRAPHD_VALUE_LIST:
    case GRAPHD_VALUE_SEQUENCE:
      dst->val_type = src->val_type;
      dst->val_array_n = 0;
      dst->val_array_m = 0;
      dst->val_array_contents = NULL;
      dst->val_array_cm = cm;

      err = graphd_value_array_grow(g, cl, dst, src->val_array_n);
      if (err != 0) return err;

      i = 0;
      src_child = src->val_array_contents;
      dst_child = dst->val_array_contents;

      for (i = 0; i < src->val_array_n; i++) {
        err = graphd_value_copy(g, cm, cl, dst_child, src_child);
        if (err != 0) {
          while (i-- > 0) graphd_value_finish(cl, --dst_child);
          graphd_value_finish(cl, dst);
          graphd_value_initialize(dst);

          return err;
        }
        src_child++;
        dst_child++;
        cl_cover(cl);
      }
      dst->val_array_n = i;
      return 0;

    default:
      break;
  }

  cl_notreached(cl, "unexpected src->val_type %d", src->val_type);
  /* NOTREACHED */
  return EINVAL;
}

static bool graphd_value_compare_extract_text(graphd_value const *val,
                                              char const **s_out,
                                              char const **e_out) {
  if (val == NULL || (val->val_type != GRAPHD_VALUE_STRING &&
                      val->val_type != GRAPHD_VALUE_ATOM) ||
      (val->val_type == GRAPHD_VALUE_ATOM &&
       val->val_text_e - val->val_text_s == sizeof("null") - 1 &&
       strncasecmp(val->val_text_s, "null", 4) == 0)) {
    *s_out = *e_out = "";
    return false;
  }

  *s_out = val->val_text_s;
  *e_out = val->val_text_e;

  return true;
}

/**
 * @brief Compare two values.
 *
 * @param cl	log through this
 * @param a	first value to compare
 * @param b	second value to compare
 *
 * @return A value less than, greater than, or equal to 0 depending
 * 	on whether a is greater than, less than, or equal to b.
 */
int graphd_value_compare(graphd_request *greq, graphd_comparator const *cmp,
                         graphd_value const *a, graphd_value const *b) {
  cl_handle *cl = graphd_request_cl(greq);

  char b1[200], b2[200];
  cl_log(cl, CL_LEVEL_VERBOSE, "graphd_value_compare %s(%s, %s)",
         cmp ? cmp->cmp_name : "(null)",
         graphd_value_to_string(a, b1, sizeof b1),
         graphd_value_to_string(b, b2, sizeof b2));

  /* Null sorts greater than (above) everything. */

  if (a == NULL || a->val_type == GRAPHD_VALUE_NULL ||
      a->val_type == GRAPHD_VALUE_UNSPECIFIED)
    return b != NULL && b->val_type != GRAPHD_VALUE_NULL &&
           b->val_type != GRAPHD_VALUE_UNSPECIFIED;

  if (b == NULL || b->val_type == GRAPHD_VALUE_NULL ||
      b->val_type == GRAPHD_VALUE_UNSPECIFIED)
    return -1;

  if (a->val_type == b->val_type) switch (a->val_type) {
      case GRAPHD_VALUE_STRING:
      case GRAPHD_VALUE_ATOM:
        goto have_text;

      case GRAPHD_VALUE_NUMBER:
        if (a->val_number < b->val_number) return -1;
        return a->val_number > b->val_number;

      case GRAPHD_VALUE_GUID:
        return graph_guid_compare(&a->val_guid, &b->val_guid);

      case GRAPHD_VALUE_LIST:
      case GRAPHD_VALUE_SEQUENCE:
        goto have_array;

      case GRAPHD_VALUE_TIMESTAMP:
        if (a->val_timestamp < b->val_timestamp) return -1;
        return a->val_timestamp > b->val_timestamp;

      case GRAPHD_VALUE_BOOLEAN:
        /* True > False. */
        if (!a->val_boolean != b->val_boolean) return -!!a->val_boolean;
        return 0;

      case GRAPHD_VALUE_DATATYPE:
        if (a->val_datatype < b->val_datatype) return -1;
        return a->val_datatype > b->val_datatype;

      default:
        cl_notreached(cl, "unexpected value type %d", a->val_type);
        /* NOTREACHED */
    }

  if ((a->val_type == GRAPHD_VALUE_LIST ||
       a->val_type == GRAPHD_VALUE_SEQUENCE) &&
      (b->val_type == GRAPHD_VALUE_LIST ||
       b->val_type == GRAPHD_VALUE_SEQUENCE)) {
    size_t i;

  have_array:
    for (i = 0; i < a->val_array_n; i++) {
      int res;

      if (i >= b->val_array_n) return 1;

      res = graphd_value_compare(greq, cmp, a->val_array_contents + i,
                                 b->val_array_contents + i);
      if (res != 0) return res;
    }
    return -(i < b->val_array_n);
  }

  if ((a->val_type == GRAPHD_VALUE_ATOM ||
       a->val_type == GRAPHD_VALUE_STRING) &&
      (b->val_type == GRAPHD_VALUE_ATOM ||
       b->val_type == GRAPHD_VALUE_STRING)) {
    char const *as, *ae, *bs, *be;

  have_text:
    if (graphd_value_compare_extract_text(a, &as, &ae)) {
      if (graphd_value_compare_extract_text(b, &bs, &be)) {
        return (*cmp->cmp_sort_compare)(greq, as, ae, bs, be);
      }
      return -1;
    } else {
      if (graphd_value_compare_extract_text(b, &bs, &be)) return 1;
      return 0;
    }
  }

  /*  Type mismatch.
   *
   *  - When comparing a singleton against a list,
   *    treat the singleton like a one-element list.
   */

  if (a->val_type == GRAPHD_VALUE_LIST ||
      a->val_type == GRAPHD_VALUE_SEQUENCE) {
    int res;

    /* An empty list < an element.
     */
    if (a->val_array_n == 0) return -1;

    res = graphd_value_compare(greq, cmp, a->val_array_contents, b);
    if (res != 0) return res;

    /* A longer list > a shorter list.
     */
    if (a->val_array_n > 1) return 1;

    return 0;
  }
  if (b->val_type == GRAPHD_VALUE_LIST || b->val_type == GRAPHD_VALUE_SEQUENCE)
    return -graphd_value_compare(greq, cmp, b, a);

  return a->val_type - b->val_type;
}

/**
 * @brief Freeze a string or atom.
 *
 *  Move anything that refers to the database into
 *  allocated storage.
 *
 * @param cm	allocate here
 * @param cl	log through this
 * @param dst	make a new value in this space
 *
 * @return 0 on success
 * @return ENOMEM on allocation error.
 */
static int freeze_string(cm_handle *cm, graphd_value *val) {
  if (val->val_text_cm == NULL) {
    char *tmp;
    size_t n = val->val_text_e - val->val_text_s;
    if ((tmp = cm_malloc(cm, 1 + n)) == NULL) return ENOMEM;
    memcpy(tmp, val->val_text_s, n);
    tmp[n] = '\0';
    val->val_text_cm = cm;
    val->val_text_s = tmp;
    val->val_text_e = tmp + n;

    pdb_primitive_reference_free(&val->val_text_ref);
    pdb_primitive_reference_initialize(&val->val_text_ref);
  }
  return 0;
}

/**
 * @brief Freeze a list.
 *
 *  Move anything that refers to the database into
 *  allocated storage.
 *
 * @param cm	allocate here
 * @param cl	log through this
 * @param dst	make a new value in this space
 *
 * @return 0 on success
 * @return ENOMEM on allocation error.
 */
static int freeze_array(cm_handle *cm, cl_handle *cl, graphd_value *val) {
  int err;
  graphd_value *v, *v_e;
  static unsigned int const needs_freezing =
      (1 << GRAPHD_VALUE_STRING) | (1 << GRAPHD_VALUE_ATOM) |
      (1 << GRAPHD_VALUE_DEFERRED) | (1 << GRAPHD_VALUE_LIST) |
      (1 << GRAPHD_VALUE_SEQUENCE);

  if (val->val_array_n == 0) return 0;

  v = val->val_array_contents;
  v_e = v + val->val_array_n;

  for (; v < v_e; v++) {
    if (!(needs_freezing & (1 << v->val_type))) continue;

    if (v->val_type == GRAPHD_VALUE_SEQUENCE ||
        v->val_type == GRAPHD_VALUE_LIST) {
      err = freeze_array(cm, cl, v);
      if (err != 0) return err;
    } else if (v->val_type == GRAPHD_VALUE_DEFERRED) {
      err = (*v->val_deferred_base->db_type->dt_suspend)(cm, cl, v);
      if (err != 0) return err;
    } else if (v->val_text_cm == NULL &&
               !pdb_primitive_reference_is_empty(&v->val_text_ref)) {
      err = freeze_string(cm, v);
      if (err != 0) return err;
    }
  }
  return 0;
}

/**
 * @brief Freeze a value.
 *
 *  Move anything that refers to the database into
 *  allocated storage.
 *
 * @param cm	allocate here
 * @param cl	log through this
 * @param dst	make a new value in this space
 *
 * @return 0 on success
 * @return ENOMEM on allocation error.
 */
int graphd_value_suspend(cm_handle *cm, cl_handle *cl, graphd_value *val) {
  if (val == NULL) return 0;

  switch (val->val_type) {
    case GRAPHD_VALUE_STRING:
    case GRAPHD_VALUE_ATOM:
      return freeze_string(cm, val);

    case GRAPHD_VALUE_NUMBER:
    case GRAPHD_VALUE_GUID:
    case GRAPHD_VALUE_TIMESTAMP:
    case GRAPHD_VALUE_BOOLEAN:
    case GRAPHD_VALUE_DATATYPE:
    case GRAPHD_VALUE_NULL:
    case GRAPHD_VALUE_UNSPECIFIED:
    case GRAPHD_VALUE_RECORDS:
      return 0;

    case GRAPHD_VALUE_LIST:
    case GRAPHD_VALUE_SEQUENCE:
      return freeze_array(cm, cl, val);

    case GRAPHD_VALUE_DEFERRED:
      return (*val->val_deferred_base->db_type->dt_suspend)(cm, cl, val);

    default:
      break;
  }

  cl_notreached(cl,
                "graphd_value_suspend: unexpected "
                "val->val_type %d",
                val->val_type);

  /* NOTREACHED */
  return EINVAL;
}

/**
 * @brief Thaw a list.
 *
 * @param cm	allocate here
 * @param dst	thaw this.
 *
 * @return 0 on success
 * @return ENOMEM on allocation error.
 */
static int thaw_array(cm_handle *cm, cl_handle *cl, graphd_value *val) {
  int err;
  graphd_value *v, *v_e;

  static unsigned int const needs_thawing = (1 << GRAPHD_VALUE_DEFERRED) |
                                            (1 << GRAPHD_VALUE_LIST) |
                                            (1 << GRAPHD_VALUE_SEQUENCE);

  if (val->val_array_n == 0) return 0;

  v = val->val_array_contents;
  v_e = v + val->val_array_n;

  for (; v < v_e; v++) {
    if (!(needs_thawing & (1 << v->val_type))) continue;

    if (v->val_type == GRAPHD_VALUE_SEQUENCE ||
        v->val_type == GRAPHD_VALUE_LIST) {
      err = thaw_array(cm, cl, v);
      if (err != 0) return err;
    } else {
      err = (*v->val_deferred_base->db_type->dt_unsuspend)(cm, cl, v);
      if (err != 0) return err;
    }
  }
  return 0;
}

/**
 * @brief Thaw a value previously frozen with graphd_value_thaw().
 *
 *  Currently, this doesn't need to do anything - a frozen value
 *  is a perfectly good value.
 *
 * @param cm	allocate here
 * @param cl	log through this
 * @param dst	make a new value in this space
 *
 * @return 0 on success
 * @return ENOMEM on allocation error.
 */
int graphd_value_unsuspend(cm_handle *cm, cl_handle *cl, graphd_value *val) {
  if (val == NULL) return 0;

  switch (val->val_type) {
    default:
      break;

    case GRAPHD_VALUE_LIST:
    case GRAPHD_VALUE_SEQUENCE:
      return thaw_array(cm, cl, val);

    case GRAPHD_VALUE_DEFERRED:
      return (*val->val_deferred_base->db_type->dt_unsuspend)(cm, cl, val);
  }
  return 0;
}

int graphd_value_deferred_push(graphd_request *greq, graphd_value *val) {
  if (val->val_type != GRAPHD_VALUE_DEFERRED) return 0;

  return (*val->val_deferred_base->db_type->dt_push)(greq, val);
}

graphd_value *graphd_value_locate(graphd_value const *val, int type) {
  size_t i;

  if (val->val_type == type) return (graphd_value *)val;

  if (!GRAPHD_VALUE_IS_ARRAY(*val)) return NULL;

  for (i = 0; i < val->val_array_n; i++) {
    if (val->val_array_contents[i].val_type == type)
      return val->val_array_contents + i;

    if (GRAPHD_VALUE_IS_ARRAY(val->val_array_contents[i])) {
      graphd_value *v = graphd_value_locate(val->val_array_contents + i, type);
      if (v != NULL) return v;
    }
  }
  return NULL;
}
