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
#include <errno.h>
#include <ctype.h>
#include <stdio.h>

#include "libgraph/graphp.h"

/**
 * @file graph-dateline.c
 * @brief Create and manipulate dateline objects.
 *
 *  A dateline is a partial state that specifies how "up-to-date"
 *  a set of servers is.
 */

/**
 * @brief Create a dateline object.
 *
 *  If the call is successful, the new object must be free'd
 *  either by freeing cm's heap as a whole or with a call
 *  to graph_dateline_destroy().
 *
 * @param cm	allocate resources here.
 * @return NULL if the allocation failed, otherwise
 *  	a new dateline object.
 */

/**
 * @brief An opaque set of dbid/primitive-count pairs.
 */
struct graph_dateline {
  /*  Linkcount.
   */
  size_t dl_link;

  /**
   * @brief Allocator handle
   */
  cm_handle *dl_cm;

  /**
   * @brief Hashtable, maps DBID to primitive count.
   */
  cm_hashtable dl_hash;

  /**
   * @brief instance_id of the database as a whole
   */
  char dl_instance_id[GRAPH_INSTANCE_ID_SIZE + 1];
};

/**
 * @brief Create a dateline object.
 *
 *  If successful, the result must be free'd with
 *  graph_dateline_destroy() (or be deleted as part
 *  of a global heap destroy).
 *
 * @param cm		allocate the copy here
 * @return NULL on on error, otherwise a dateline object.
 */

graph_dateline *graph_dateline_create(struct cm_handle *cm) {
  graph_dateline *dl;

  dl = cm_malloc(cm, sizeof(*dl));
  if (dl == NULL) return NULL;

  memset(dl, 0, sizeof(*dl));
  dl->dl_link = 1;
  dl->dl_cm = cm;
  cm_hashinit(cm, &dl->dl_hash, sizeof(unsigned long long), 8);

  return dl;
}

/**
 * @brief Make a deep copy of an existing dateline.
 *
 * @param cm		allocate the copy here
 * @param original 	NULL or a pointer to a dateline
 *		as returned by graph_dateline_create.
 * @return a copy of the original.
 */

graph_dateline *graph_dateline_copy(struct cm_handle *cm,
                                    graph_dateline const *original) {
  graph_dateline *copy;

  if (original == NULL) return NULL;
  if ((copy = graph_dateline_create(cm)) == NULL) return copy;
  if (graph_dateline_merge(&copy, original) != 0) {
    graph_dateline_destroy(copy);
    return NULL;
  }
  return copy;
}

/**
 * @brief Add a link to a datelie.
 *
 * @param original 	NULL or a pointer to a dateline
 * @return the original.
 */

graph_dateline *graph_dateline_dup(graph_dateline *original) {
  if (original == NULL) return NULL;
  original->dl_link++;
  return original;
}

/**
 * @brief Release resources associated with a dateline object.
 *
 *  It is safe, and does nothing, to pass in a null pointer here.
 *
 * @param dl	NULL or a pointer to a dateline object,
 *		as returned by graph_dateline_create.
 */

void graph_dateline_destroy(graph_dateline *dl) {
  if (dl == NULL) return;

  if (dl->dl_link > 1) {
    dl->dl_link--;
    return;
  }

  cm_hashfinish(&dl->dl_hash);
  cm_free(dl->dl_cm, dl);
}

/**
 * @brief Return the number of elements in a dateline server set.
 *
 * @param dl	NULL or a pointer to a dateline object,
 *		as returned by graph_dateline_create.
 *
 * @return the number of servers in a dateline.
 */

size_t graph_dateline_n(graph_dateline *dl) {
  if (dl == NULL) return 0;

  return dl->dl_hash.h_n;
}

/**
 * @brief Make sure a dateline only has one link to it.
 * @param dl		Pointer to a dateline pointer, as
 *			allocated with graph_dateline_create()
 *
 * @return 0 on success, otherwise a nonzero error code.
 */
int graph_dateline_split(graph_dateline **dl) {
  if (dl == NULL || *dl == NULL) return EINVAL;

  if ((*dl)->dl_link > 1) {
    graph_dateline *new_dl;

    new_dl = graph_dateline_copy((*dl)->dl_cm, *dl);
    if (new_dl == NULL) return errno ? errno : ENOMEM;

    (*dl)->dl_link--;
    *dl = new_dl;
  }
  return 0;
}

/**
 * @brief Add a dbid/count to an existing, consistent dateline object.
 *
 *  If the dbid was previously absent, it is added with the
 *  specified dateline.
 *
 *  If the dbid was previously present with an earlier dateline,
 *  it is upgraded to the later dateline.
 *
 *  If the dbid was previously present with a later dateline,
 *  it remains unchanged.
 *
 *  If there is more than one link to that dateline object, the
 *  object is split off and a new object is returned.
 *
 * @param dl		A dateline, as allocated with graph_dateline_create()
 * @param dbid		Database ID.  E.g., from GRAPH_GUID_DB(guid).
 * @param count		The odometer reading from that database.
 * @param instance_id	The instance ID.  Must be NULL, new, or match.
 *
 * @return NULL on allocation error, otherwise the new dateline object.
 */
int graph_dateline_add(graph_dateline **dl, unsigned long long dbid,
                       unsigned long long count, char const *instance_id) {
  unsigned long long *slot;
  int err;
  bool first = false;

  if (dl == NULL || *dl == NULL) return EINVAL;

  if (dbid >= (1ull << 48) || count >= (1ull << 34)) return GRAPH_ERR_SEMANTICS;

  if (instance_id) {
    if ((*dl)->dl_hash.h_n == 0 && (*dl)->dl_instance_id[0] == '\0')
      first = true;

    else if (strcmp(instance_id, (*dl)->dl_instance_id) != 0)
      return GRAPH_ERR_INSTANCE_ID_MISMATCH;
  }
  if ((err = graph_dateline_split(dl)) != 0) return err;

  slot = cm_hnew(&(*dl)->dl_hash, unsigned long long, &dbid, sizeof(dbid));
  if (slot == NULL) return errno ? errno : ENOMEM;

  if (*slot < count) *slot = count;

  if (first)
    snprintf((*dl)->dl_instance_id, sizeof((*dl)->dl_instance_id), "%s",
             instance_id);

  return 0;
}

/**
 * @brief Add a dbid/count to an existing, consistent dateline object.
 *
 *  If the dbid was previously absent, it is added with the
 *  specified dateline.
 *
 *  If the dbid was previously present with a later dateline,
 *  it is upgraded to the earlier dateline.
 *
 *  If the dbid was previously present with an earlier dateline,
 *  it remains unchanged.
 *
 * @param dl		A dateline, as allocated with graph_dateline_create()
 * @param dbid		Database ID.  E.g., from GRAPH_GUID_DB(guid).
 * @param count		The odometer reading from that database.
 * @param instance_id	The instance ID.  Must be NULL, new, or match.
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graph_dateline_add_minimum(graph_dateline **dl, unsigned long long dbid,
                               unsigned long long count,
                               char const *instance_id) {
  unsigned long long *slot;
  int err;
  bool first = false;

  if (dl == NULL || *dl == NULL) return EINVAL;

  if (dbid >= (1ull << 48) || count >= (1ull << 34)) return GRAPH_ERR_SEMANTICS;

  if (instance_id) {
    if ((*dl)->dl_hash.h_n == 0 && (*dl)->dl_instance_id[0] == '\0')
      first = true;

    else if (strcmp(instance_id, (*dl)->dl_instance_id) != 0)
      return GRAPH_ERR_INSTANCE_ID_MISMATCH;
  }
  if ((err = graph_dateline_split(dl)) != 0) return err;

  slot = cm_haccess(&(*dl)->dl_hash, unsigned long long, &dbid, sizeof(dbid));
  if (slot == NULL) {
    slot = cm_hnew(&(*dl)->dl_hash, unsigned long long, &dbid, sizeof(dbid));
    if (slot == NULL) return errno ? errno : ENOMEM;
    *slot = count;
  } else {
    if (*slot > count) *slot = count;
  }

  if (first)
    snprintf((*dl)->dl_instance_id, sizeof((*dl)->dl_instance_id), "%s",
             instance_id);
  return 0;
}

/**
 * @brief Get the instance-id of a dateline.
 *
 * @param dl	dateline
 * @return NULL if the dateline is NULL or wasn't tagged with an
 *		instance ID; otherwise a pointer to the instance ID string.
 */
char const *graph_dateline_instance_id(graph_dateline const *a) {
  return a && a->dl_instance_id[0] ? a->dl_instance_id : NULL;
}

/**
 * @brief Merge a dateline into an existing one according to
 *  	minimum rules.
 *
 *  Where two datelines overlap, the resulting dateline is the
 *  minimum of the two ingredients.
 *
 * @param a		Accumulator dateline.
 * @param b		dateline to merge into the accumulator
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graph_dateline_merge_minimum(graph_dateline **a, graph_dateline const *b) {
  unsigned long long dbid, n;
  void *state;
  int err;

  if (a == NULL || *a == NULL) return EINVAL;

  if (b == NULL) return 0;

  state = NULL;
  while ((err = graph_dateline_next(b, &dbid, &n, &state)) == 0) {
    err = graph_dateline_add_minimum(a, dbid, n, graph_dateline_instance_id(b));
    if (err != 0) return err;
  }
  return err == GRAPH_ERR_NO ? 0 : err;
}

/**
 * @brief Merge a dateline into an existing one.
 *
 *  Where two datelines overlap, the resulting dateline is the
 *  maximum of the two ingredients.
 *
 * @param a		Accumulator dateline.
 * @param b		dateline to merge into the accumulator
 *
 * @return 0 on success, a nonzero error code on error.
 */
int graph_dateline_merge(graph_dateline **a, graph_dateline const *b) {
  unsigned long long dbid, n;
  void *state;
  int err;

  if (a == NULL || *a == NULL) return EINVAL;

  if (b == NULL) return 0;

  state = NULL;
  while ((err = graph_dateline_next(b, &dbid, &n, &state)) == 0) {
    err = graph_dateline_add(a, dbid, n, graph_dateline_instance_id(b));
    if (err != 0) return err;
  }
  return err == GRAPH_ERR_NO ? 0 : err;
}

/**
 * @brief Is a server in the set?  If yes, what is its local counter?
 *
 * @param dl		A dateline, as allocated with graph_dateline_create()
 * @param dbid	A server ID, or a GUID from the server (that isn't
 *			an external import or a version).
 * @param count_out	Store the odometer reading for the server here.
 *
 * @return 0 on success, a nonzero error code on error.
 * @return GRAPH_ERR_NO if the server is not mentioned in the dateline.
 */
int graph_dateline_get(graph_dateline const *dl, unsigned long long dbid,
                       unsigned long long *count_out) {
  unsigned long long const *slot;

  if (dl == NULL) return EINVAL;

  slot = cm_haccess((cm_hashtable *)&dl->dl_hash, unsigned long long, &dbid,
                    sizeof(dbid));
  if (slot == NULL) return GRAPH_ERR_NO;

  *count_out = *slot;
  return 0;
}

/**
 * @brief Get the next element in an iteration over all servers in the set.
 *
 * @param dl		A dateline, as allocated with graph_dateline_create()
 * @param dbid_out	Out: database ID
 * @param count_out	Out: database's odometer reading
 * @param state_inout	In/Out: iteration state, initially 0.
 *
 * @return 0 on success, a nonzero error code on error.
 * @return GRAPH_ERR_NO if the server is not mentioned in the dateline.
 */
int graph_dateline_next(graph_dateline const *dl, unsigned long long *dbid_out,
                        unsigned long long *count_out, void **state_inout) {
  unsigned long long const *slot;

  if (dl == NULL || state_inout == NULL) return EINVAL;

  slot = cm_hnext(&dl->dl_hash, unsigned long long, *state_inout);
  if (slot == NULL) return GRAPH_ERR_NO;

  if (count_out != NULL) *count_out = *slot;
  if (dbid_out != NULL)
    memcpy(dbid_out, cm_hmem(&dl->dl_hash, unsigned long long, slot),
           sizeof(*dbid_out));

  *state_inout = (void *)slot;
  return 0;
}

static int dateline_next_fragment(char const **s, char const *e,
                                  char const **frag_s_out,
                                  char const **frag_e_out) {
  char const *r = *s;

  while (r < e && isascii(*r) && isspace(*r)) r++;
  if (r >= e) return GRAPH_ERR_NO;

  *frag_s_out = r;
  while (r < e && *r != '/' && *r != ',') r++;
  *frag_e_out = r;
  if (r < e) r++;
  *s = r;

  return 0;
}

/**
 * @brief Parse a string as a dateline.
 *
 *   Datelines look like "/"-separated GUIDs.  The DB's of those
 *   GUIDs are the dbids; the sequence numbers are the odometer readings
 *   (One larger than the most recently allocated GUID; 0 initially.)
 *
 *  If the call fails, the value of dl is defined, but indeterminate.
 *  (It must still be destroyed, but don't trust it to have the same value
 *  it had before.)
 *
 * @param dl	Dateline
 * @param s	Pointer to first byte of string
 * @param e	NULL or pointer just after last byte of string; if
 *		NULL, e defaults to s + strlen(s)
 *
 * @return 0 	on success
 * @return GRAPH_ERR_LEXICAL on scanner error
 * @return other nonzero errors on system error.
 */
int graph_dateline_from_string(graph_dateline **dl, char const *s,
                               char const *e) {
  int err;
  char const *frag_s, *frag_e, *comma;

  if (s == NULL || dl == NULL) return GRAPH_ERR_LEXICAL;

  if (e == NULL) e = s + strlen(s);

  if ((err = graph_dateline_split(dl)) != 0) return err;

  if ((comma = memchr(s, ',', e - s)) != NULL &&
      comma - s <= GRAPH_INSTANCE_ID_SIZE) {
    if ((*dl)->dl_instance_id[0] == '\0') {
      memcpy((*dl)->dl_instance_id, s, comma - s);
      (*dl)->dl_instance_id[comma - s] = '\0';
    } else if (strncasecmp((*dl)->dl_instance_id, s, comma - s) != 0 ||
               (*dl)->dl_instance_id[comma - s] != '\0')
      return GRAPH_ERR_INSTANCE_ID_MISMATCH;

    s = comma + 1;
  }

  while ((err = dateline_next_fragment(&s, e, &frag_s, &frag_e)) == 0) {
    char const *dot;
    unsigned long long dbid;
    unsigned long long serial;

    /*  The format used to be serial.guidcount.
     *  now it's simply GUIDs.
     */
    dot = memchr(frag_s, '.', frag_e - frag_s);
    if (dot == NULL) {
      graph_guid g;

      err = graph_guid_from_string(&g, frag_s, frag_e);
      if (err != 0) return err;

      dbid = GRAPH_GUID_DB(g);
      serial = GRAPH_GUID_SERIAL(g);
    } else {
      if ((err = graph_ull_from_hexstring(&dbid, frag_s, dot)) != 0 ||
          (err = graph_ull_from_hexstring(&serial, dot + 1, frag_e)) != 0)
        return err;
    }
    err = graph_dateline_add(dl, dbid, serial, NULL);
    if (err != 0) return err;
  }
  if (err != GRAPH_ERR_NO) return err;

  return 0;
}

/**
 * @brief Format a dateline as a string for logging.
 *
 * @param dl	Dateline
 * @param s	place output here
 * @param n	Number of bytes pointed to by s that can be used.
 *
 * @return a pointer to a string representation of a dateline.
 */
char const *graph_dateline_to_string(graph_dateline const *dl, char *s,
                                     size_t n) {
  void *state;
  unsigned long long dbid;
  unsigned long long serial;
  char const *s0 = s;
  char *w = s;
  char const *e = s + n;
  char const *filler = "";

  if (dl == NULL) return "null";

  if (n == 0) return "[dateline]";

  if (dl->dl_instance_id[0] != '\0') {
    snprintf(w, (size_t)(e - w), "%s,", dl->dl_instance_id);
    w += strlen(w);

    n -= (w - s);
    s = w;
  }

  state = NULL;
  while (graph_dateline_next(dl, &dbid, &serial, &state) == 0) {
    graph_guid g;

    graph_guid_from_db_serial(&g, dbid, serial);
    if (n >= strlen(filler) + GRAPH_GUID_SIZE) {
      strcpy(w, filler);

      n -= strlen(filler);
      w += strlen(w);

      graph_guid_to_string(&g, w, (size_t)(e - w));

      n -= strlen(w);
      w += strlen(w);

      filler = "/";
    } else {
      if (e - w > 1) *w++ = '.';
      if (e - w > 1) *w++ = '.';
      if (e - w > 1) *w++ = '.';

      break;
    }
  }
  if (w == s) {
    snprintf(w, e - w, "null");
    w += strlen(w);
  }
  if (w < e)
    *w = '\0';
  else
    w[-1] = '\0';

  return s0;
}

/**
 * @brief Format a dateline as a string for transmission.
 *
 * @param dl	Dateline
 * @param s	place output here
 * @param e	end of space available
 * @param state in/out iterator state; initially NULL
 * @param offset in/out offset.
 *
 * @return 0 		  on success
 * @return GRAPH_ERR_DONE on completion
 */
int graph_dateline_format(graph_dateline const *dl, char **s, char *e,
                          void **state, size_t *offset) {
  int err;
  void *prev_state;
  unsigned long long dbid;
  unsigned long long serial;
  char *w = *s;
  char const *filler = *state ? "/" : "";
  size_t off_base = 0;

  if (dl == NULL) {
    if (*offset >= 4) return GRAPH_ERR_DONE;
    while (w < e && *offset < 4) *w++ = "null"[(*offset)++];
    *s = w;
    return 0;
  }

  if (dl->dl_instance_id[0]) {
    /*  The dateline starts with the database ID and a comma.
     */
    off_base = strlen(dl->dl_instance_id) + 1;
    if (*offset < off_base) {
      while (w < e && dl->dl_instance_id[*offset] != '\0')
        *w++ = dl->dl_instance_id[(*offset)++];
      if (w >= e) {
        *s = w;
        return 0;
      }

      *w++ = ',';
      ++*offset;

      if (w >= e) {
        *s = w;
        return 0;
      }
    }
  }

  if (dl->dl_hash.h_n == 0) {
    if (*offset >= 4 + off_base) {
      *s = w;
      return GRAPH_ERR_DONE;
    }

    while (w < e && *offset < off_base + 4)
      *w++ = "null"[(*offset)++ - off_base];
    *s = w;
    return 0;
  }

  prev_state = *state;
  while ((err = graph_dateline_next(dl, &dbid, &serial, state)) == 0) {
    size_t buf_n;
    char const *buf_s;
    char guidbuf[GRAPH_GUID_SIZE], bigbuf[GRAPH_GUID_SIZE + 10];
    char const *guidtext;
    graph_guid guid;

    graph_guid_from_db_serial(&guid, dbid, serial);
    guidtext = graph_guid_to_string(&guid, guidbuf, sizeof guidbuf);
    snprintf(bigbuf, sizeof bigbuf, "%s%s", filler, guidtext);

    buf_n = strlen(bigbuf);
    buf_s = bigbuf;

    /*  Resume copying from the buffer.  We've already
     *  copied the first *offset characters.
     */
    if (*offset - off_base < buf_n) {
      buf_n -= (*offset - off_base);
      buf_s += (*offset - off_base);
    }

    /* Not enough space?  Copy what we can.
     */
    if (e - w < buf_n) {
      memcpy(w, buf_s, e - w);
      *s = e;

      /*  e - w          -- we just copied those bytes
       *  buf_s - bigbuf -- started at this offset.
       */

      *offset = off_base + (buf_s - bigbuf) + (e - w);
      *state = prev_state;

      return 0;
    }

    /* Copy everything, move to the next.
     */
    memcpy(w, buf_s, buf_n);
    w += buf_n;

    filler = "/";

    prev_state = *state;
    *offset = off_base;
  }
  if (w == *s) return GRAPH_ERR_DONE;
  *s = w;
  return 0;
}

/**
 * @brief Compare two datelines for equality
 *
 *  False negatives are OK.
 *
 * @param a		one dateline
 * @param b		another dateline
 *
 * @return true if they're equal, false if they may or may not be equal.
 */

bool graph_dateline_equal(graph_dateline const *a, graph_dateline const *b) {
  int a_err, b_err;
  unsigned long long a_dbid, b_dbid;
  unsigned long long a_n = 0, b_n = 0;
  void *a_state, *b_state;

  if (a == NULL && b == NULL) return true;
  if (a == NULL || b == NULL) return false;

  if (strcmp(a->dl_instance_id, b->dl_instance_id)) return false;

  a_state = b_state = NULL;
  for (;;) {
    a_err = graph_dateline_next(a, &a_dbid, &a_n, &a_state);
    b_err = graph_dateline_next(b, &b_dbid, &b_n, &b_state);

    if (a_err != 0 || b_err != 0) break;

    if (a_dbid != b_dbid || a_n != b_n) return false;
  }
  return a_err != 0 && b_err != 0;
}

/**
 * @brief Hash a dateline into a value.
 *
 * @param d		a dateline
 * @return a hash such that datelines that compare equal
 *	always have equal hashes.
 */

unsigned long long graph_dateline_hash(graph_dateline const *const d) {
  int err;
  unsigned long long dbid, n;
  void *state = NULL;
  unsigned long long h = 0;

  if (d == NULL) return 0;

  while ((err = graph_dateline_next(d, &dbid, &n, &state)) == 0) {
    h = (h << 7) | (h >> (sizeof(h) - 7));
    h ^= dbid ^ n;
  }
  return h;
}

/*
 * Make sure that s..e contains only characters that are valid for use
 * in a dateline instance identifier
 */
bool graph_dateline_instance_verify(const char *s, const char *e) {
  if (s == e || e - s > GRAPH_INSTANCE_ID_SIZE) return false;

  while (s < e) {
    if (!isalnum(*s)) return false;
    s++;
  }
  return true;
}
