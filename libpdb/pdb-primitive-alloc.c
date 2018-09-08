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
#include "libpdb/pdbp.h"

#include <errno.h>
#include <stdio.h>


/**
 * @brief Allocate primitive data of a certain size; return it and a GUID.
 *
 *  This allocates an ID and fills it in with all the fields expected
 *  in a primitive.  The caller should then call pdb_primitive_alloc_commit
 *  to update the indicies.
 *
 * @param pdb		opaque database module handle
 * @param now		current timestamp
 * @param prev_guid	NULL or a pointer to the graph_guid of a previous
 * @param pr		adjust this structure to point to allocate bytes
 * @param guid_out	assign new GUID to this pointer
 * @param timestamp	timestamp to store, must be ascending
 *			 or bad things will happen.
 * @param valuetype	valuetype to store
 * @param bits		bit flags, mut match the rest
 * @param name_size	strlen(name) + 1 (includes a '\0')
 * @param value_size	strlen(value) + 1 (includes a '\0')
 * @param name		NULL or pointer to first byte of name
 * @param value		NULL or pointer to first byte of value
 * @param type		NULL or type guid
 * @param right		NULL or right guid
 * @param left		NULL or left guid
 * @param scope		NULL or scope guid
 * @param myguid	NULL (normally) or desired assigned guid
 * @param errbuf	error message is placed here on error
 * @param errbuf_size	# of bytes pointed to by errbuf
 *
 * @return 0 on success, a nonzero error code on error.
 */

int pdb_primitive_alloc(pdb_handle* pdb, graph_timestamp_t now,
                        graph_guid const* prev_guid, pdb_primitive* const pr,
                        graph_guid* guid_out, unsigned long long timestamp,
                        unsigned char valuetype, unsigned bits,
                        size_t name_size, size_t value_size, const char* name,
                        const char* value, const graph_guid* type,
                        const graph_guid* right, const graph_guid* left,
                        const graph_guid* scope, const graph_guid* myguid,
                        char* errbuf, size_t errbuf_size) {
  int err;
  addb_istore_id id;
  addb_istore_id real_id;
  char buf1[GRAPH_GUID_SIZE], buf2[GRAPH_GUID_SIZE];
  size_t pr_len;

  /*
   * Temporary storage for compressed GUIDs before they are
   * copied into the new primitive.
   */
  unsigned char guidspace[PDB_PRIMITIVE_GUID_MAXLEN * PDB_LINKAGE_ALL];
  size_t guidlen[PDB_LINKAGE_ALL];
  int i;
  unsigned long lengthbits = 0;

  if (pdb->pdb_primitive == NULL) {
    if ((err = pdb_initialize(pdb)) != 0) return err;
    if ((err = pdb_initialize_checkpoint(pdb)) != 0) return err;
  }

  pr->pr_database_guid = &pdb->pdb_database_guid;

  /* Check the name size for overflow.
   *
   * The name is in front of the value.  The value offset
   * is two bytes; the name can't be larger than the
   * distance between the largest number the name offset
   * can represent and the value offset.
   */
  if (name_size >= (1ul << (8 * PDB_PRIMITIVE_NAMELEN_SIZE))) {
    snprintf(errbuf, errbuf_size, "name too long");
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_primitive_alloc: attempt to "
           "allocate a primitive with a "
           "%zu-byte-name (allowed: %lu)",
           name_size, (1ul << (8 * PDB_PRIMITIVE_NAMELEN_SIZE)) -
                          PDB_PRIMITIVE_NAME_OFFSET);
    return PDB_ERR_PRIMITIVE_TOO_LARGE;
  }

  /* Check the value size for overflow.
   *
   * The link offset is three bytes, the value offset is
   * two bytes.  The value is between the value offset
   * and the link offset.
   */
  if (value_size >= (1ul << (8 * PDB_PRIMITIVE_VALUELEN_SIZE)) -
                        (PDB_PRIMITIVE_NAME_OFFSET + name_size)) {
    snprintf(errbuf, errbuf_size, "value too long");
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_primitive_alloc: attempt to "
           "allocate a primitive with a "
           "%zu-byte value (allowed: %lu)",
           name_size, (1ul << (8 * PDB_PRIMITIVE_VALUELEN_SIZE)) -
                          PDB_PRIMITIVE_NAME_OFFSET + name_size);
    return PDB_ERR_PRIMITIVE_TOO_LARGE;
  }

  memset(guidlen, 0, sizeof(guidlen));

  /*
   * Compress each guid. Store the compressed guids into the
   * guidspace array and the lengths of the guids into the guidlen
   * array.
   */

  if (type && !GRAPH_GUID_IS_NULL(*type)) {
    guidlen[0] = pdb_primitive_linkage_set_ptr(pr, type, guidspace);
  }
  if (right && !GRAPH_GUID_IS_NULL(*right)) {
    guidlen[1] = pdb_primitive_linkage_set_ptr(
        pr, right, guidspace + PDB_PRIMITIVE_GUID_MAXLEN);
  }
  if (left && !GRAPH_GUID_IS_NULL(*left)) {
    guidlen[2] = pdb_primitive_linkage_set_ptr(
        pr, left, guidspace + PDB_PRIMITIVE_GUID_MAXLEN * 2);
  }
  if (scope && !GRAPH_GUID_IS_NULL(*scope)) {
    guidlen[3] = pdb_primitive_linkage_set_ptr(
        pr, scope, guidspace + PDB_PRIMITIVE_GUID_MAXLEN * 3);
  }

  id = addb_istore_next_id(pdb->pdb_primitive);
  graph_guid_from_db_serial(guid_out, pdb->pdb_database_id, id);

  /*
   * If myguid it set, it means we're importing a primitive
   * from a different database.  We'll need to store the GUID
   * for this primitive.
   *
   * Normally, we don't bother to store the GUID of a primitive
   * because its GUID is the database ID + its serial number.
   */

  if (myguid && !GRAPH_GUID_IS_NULL(*myguid)) {
    guidlen[4] = pdb_primitive_linkage_set_ptr(
        pr, myguid, guidspace + PDB_PRIMITIVE_GUID_MAXLEN * 4);
    pr->pr_guid = *myguid;
  } else {
    pr->pr_guid = *guid_out;
  }

  /*
   * Calculate the length of the primitive.
   * Add the basic length the length of the value and name fields
   * and the length of the compressed GUIDs
   */
  pr_len = PDB_PRIMITIVE_SIZE_MIN + (!!prev_guid) * 10 +
           (name_size ? name_size + PDB_PRIMITIVE_NAMELEN_SIZE : 0) +
           (value_size ? value_size + PDB_PRIMITIVE_VALUELEN_SIZE : 0);

  for (i = 0; i < PDB_LINKAGE_ALL; i++) pr_len += guidlen[i];

  /*  We can't build a primitive that's larger than
   *  addb's tile size.
   */
  if (pr_len > ADDB_TILE_SIZE) {
    snprintf(errbuf, errbuf_size, "primitive too big");
    cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
           "pdb_primitive_alloc: attempt to "
           "allocate a primitive that will be "
           "at least %zu bytes large (allowed: %zu)",
           pr_len, (size_t)ADDB_TILE_SIZE);
    return PDB_ERR_PRIMITIVE_TOO_LARGE;
  }

  /*  OK.  We will now modify the database.  If there are
   *  iterators around that still point to database things,
   *  they need to suspend themselves.
   */
  if (pdb->pdb_iterator_n_unsuspended > 0) {
    err = pdb_iterator_suspend_all(pdb);
    if (err != 0) {
      cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_iterator_suspend_all", err,
                   "guid=%s",
                   graph_guid_to_string(guid_out, buf1, sizeof buf1));
      snprintf(errbuf, errbuf_size, "internal error: %s", pdb_xstrerror(err));
      return err;
    }
  }

  cl_assert(pdb->pdb_cl, pdb->pdb_iterator_n_unsuspended == 0);
  err = addb_istore_alloc(pdb->pdb_primitive, pr_len, &pr->pr_data, &real_id);

  if (err != 0) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "addb_istore_alloc", err,
                 "Can't allocate %zu length primitive", pr_len);
    return err;
  }

  cl_assert(pdb->pdb_cl, real_id == id);

  /*
   * Now go and build up the real primitive.
   */
  pdb_primitive_zero(pr);

  pdb_primitive_timestamp_set(pr, timestamp);
  pdb_primitive_valuetype_set(pr, valuetype);

  /*
   * Ordering here is very important.
   * We need to set the bits field so that the pdb_primitive_xxx_pointer
   * macros are correct. THen we need to set the name and value field
   * (in that order as the value macros use the name length).
   * Then, we write the GUIDs
   */
  pdb_primitive_bits_set(pr,
                         bits | (name_size ? PDB_PRIMITIVE_BIT_HAS_NAME : 0) |
                             (value_size ? PDB_PRIMITIVE_BIT_HAS_VALUE : 0));

  if (name_size) {
    memcpy(pdb_primitive_name_pointer(pr), name, name_size);
    pdb_primitive_name_pointer(pr)[name_size - 1] = 0;
    pdb_set2(PDB_PTR(pr) + PDB_PRIMITIVE_NAMELEN_OFFSET, name_size);
  }

  if (value_size) {
    memcpy(pdb_primitive_value_pointer(pr), value, value_size - 1);
    pdb_primitive_value_pointer(pr)[value_size - 1] = 0;
    pdb_set3(PDB_PTR(pr) + PDB_PRIMITIVE_VALUELEN_OFFSET(pr), value_size);
  }

  /*
   * Calculate the link-length fields of the primitive.
   */
  for (i = 0; i < PDB_LINKAGE_ALL; i++) {
    if (guidlen[i])
      lengthbits |= PDB_PRIMITIVE_LENGTH_FREEZE(guidlen[i])
                    << (i * PDB_PRIMITIVE_BITS_PER_LINK);
  }
  pdb_set3(PDB_PTR(pr) + PDB_PRIMITIVE_LINKAGE_BITS_OFFSET, lengthbits);

  /*
   * Now copy the previously compressed GUIDs into the primitive.
   * This depends on the fields set in the primitive above.
   */

  for (i = 0; i < PDB_LINKAGE_ALL; i++) {
    if (guidlen[i])
      memcpy(PDB_PTR(pr) + pdb_primitive_guid_offset(pr, i),
             guidspace + PDB_PRIMITIVE_GUID_MAXLEN * i,
             PDB_PRIMITIVE_LINK_LENGTH(pr, i));
  }

  /* If we're versioning a primitive, compute a generation
   * and lineage for it.
   */
  if (prev_guid) {
    cl_log(pdb->pdb_cl, CL_LEVEL_SPEW,
           "pdb_primitive_alloc: "
           "prev_guid == %s ",
           graph_guid_to_string(prev_guid, buf1, sizeof buf1));

    pdb_id lineage_id;      /* first primitive in our lineage */
    unsigned long long gen; /* our index in the generation */

    /*  Look up the lineage ID in the predecessor.
     */
    err = pdb_generation_guid_to_lineage(pdb, prev_guid, &lineage_id, NULL);
    if (err) {
      char const* const g_string =
          graph_guid_to_string(prev_guid, buf1, sizeof buf1);

      cl_log_errno(pdb->pdb_cl, CL_LEVEL_FAIL, "pdb_generation_guid_to_lineage",
                   err, "can't read predecessor %s", g_string);
      snprintf(errbuf, errbuf_size, "cannot read predecessor record %s",
               g_string);
      err = PDB_ERR_NO;
      goto err;
    }

    err = pdb_generation_lineage_n(pdb, lineage_id, &gen);
    if (err) goto err;

    /*  If the ID of the lineage we're versioning is >= our
     *  own, something is very wrong - reject the data.
     */
    if (lineage_id >= id) {
      cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
             "pdb_primitive_alloc_commit: continuity "
             "error: rejecting attempt to version "
             "GUID %s (id %llx) into %s (id %llx)",
             graph_guid_to_string(prev_guid, buf1, sizeof buf1),
             (unsigned long long)lineage_id,
             graph_guid_to_string(guid_out, buf2, sizeof buf2),
             (unsigned long long)id);
      snprintf(errbuf, errbuf_size,
               "Are you telling me you built a time "
               "machine... out of a DeLorean? (GUID %s"
               " doesn't exist in this database - yet)",
               graph_guid_to_string(prev_guid, buf1, sizeof buf1));
      err = PDB_ERR_NO;
      goto err;
    }

    /*  Overwrite the previous values: set the bit, assign the
     *  generation and "previous" lineage index.
     */
    pdb_primitive_set_generation_bit(pr);
    pdb_primitive_generation_set(pr, gen);
    pdb_primitive_lineage_set(pr, lineage_id);
  }

  pdb->pdb_runtime_statistics.rts_primitives_written++;

  cl_log(pdb->pdb_cl, CL_LEVEL_DEBUG,
         "pdb_primitive_alloc(now=%llu, %s%s%sguid=%s, dbid=%llu)",
         (unsigned long long)now, prev_guid == NULL ? "" : "pred=",
         prev_guid == NULL ? ""
                           : graph_guid_to_string(prev_guid, buf1, sizeof buf1),
         prev_guid == NULL ? "" : ", ",
         graph_guid_to_string(guid_out, buf2, sizeof buf2),
         pdb->pdb_database_id);

  return 0;

err:
  addb_istore_free(pdb->pdb_primitive, &pr->pr_data);
  return err;
}

/**
 * @brief Commit a previously allocated primitive to the database.
 *
 *  The primitive becomes visible to the indices.  Its storage
 *  stops being changeable.
 *
 * @param pdb		database handle
 * @param prev_guid	NULL or the GUID of an ancestor
 * @param my_guid	the GUID allocated in a previous call to
 *			pdb_primitive_alloc.
 * @param pr		primitive data
 * @param errbuf	on error return, English error message detail
 * @param errbuf_size	# of bytes pointed to by errbuf
 *
 * @return 0 on success, otherwise a nonzero error number.
 *
 */
int pdb_primitive_alloc_commit(pdb_handle* pdb, pdb_guid const* prev_guid,
                               graph_guid const* my_guid, pdb_primitive* pr,
                               char* errbuf, size_t errbuf_size) {
  int err = 0;
  int error_line = 0;
  int linkage;
  pdb_id id;
  graph_guid guid;
  char buf1[GRAPH_GUID_SIZE];
  char buf2[GRAPH_GUID_SIZE];
  char buf3[200];
  char const* errstr;

  cl_enter(pdb->pdb_cl, CL_LEVEL_DEBUG, "(%s%s%sguid=%s): %s",
           prev_guid == NULL ? "" : "pred=",
           prev_guid == NULL ? "" : graph_guid_to_string(prev_guid, buf1,
                                                         sizeof buf1),
           prev_guid == NULL ? "" : ", ",
           graph_guid_to_string(my_guid, buf2, sizeof buf2),
           pdb_primitive_to_string(pr, buf3, sizeof buf3));
#undef except_throw
#define except_throw(e)    \
  do {                     \
    error_line = __LINE__; \
    err = (e);             \
    goto err;              \
  } while (0)

  *errbuf = '\0';

  /*  Every primitive has a GUID.
   *  This may not be the same GUID as in the GUID field
   *  inside the primitive, but we know both must be non-NULL.
   */
  cl_assert(pdb->pdb_cl, !GRAPH_GUID_IS_NULL(*my_guid));

  errstr = pdb_primitive_check(pr, buf3, sizeof buf3);
  if (errstr != NULL) {
    snprintf(errbuf, errbuf_size,
             "graphd programmer error - internally "
             "inconsistent record: %s",
             errstr);
    cl_log(pdb->pdb_cl, CL_LEVEL_ERROR,
           "pdb_primitive_alloc_commit: attempt "
           "to write internally inconsistent record: %s",
           errstr);
    except_throw(err);
  }

  pdb_primitive_guid_get(pr, guid);
  cl_assert(pdb->pdb_cl, !GRAPH_GUID_IS_NULL(guid));

  /*  If the two primitives are different,
   *  they must be different in database ID.
   */
  if (GRAPH_GUID_SERIAL(*my_guid) != GRAPH_GUID_SERIAL(guid)) {
    if (GRAPH_GUID_DB(*my_guid) == GRAPH_GUID_DB(guid)) {
      snprintf(errbuf, errbuf_size,
               "cannot allocate imported record with GUID "
               "%s - database ID already exists",
               graph_guid_to_string(my_guid, buf2, sizeof buf2));

      cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
             "pdb_primitive_alloc_commit: cannot "
             "allocate an imported record with GUID \"%s\" "
             "on top of internal record with GUID \"%s\" --"
             " their database IDs are both %llx",
             graph_guid_to_string(my_guid, buf2, sizeof buf2),
             graph_guid_to_string(&guid, buf3, sizeof buf3),
             (unsigned long long)GRAPH_GUID_DB(guid));
      except_throw(err);
    }

    /*  We're importing.  Make sure that the
     *  incoming GUID doesn't yet exist.
     */
    err = pdb_id_from_guid(pdb, &id, &guid);
    if (err == 0) {
      cl_assert(pdb->pdb_cl, id == GRAPH_GUID_SERIAL(*my_guid));

      /*  pdb_primitive_n() should be the current id
       *  plus one. If the new GUID maps to that, that's
       *  ok.  Otherwise, we have a problem.
       */
      if (id != pdb_primitive_n(pdb) - 1) {
        char buf[200];
        cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
               "pdb_primitive_alloc_commit: "
               "guid %s maps to %llu; expcted %llu",
               graph_guid_to_string(&guid, buf, sizeof buf),
               (unsigned long long)id,
               (unsigned long long)pdb_primitive_n(pdb) - 1);
        err = PDB_ERR_EXISTS;
      }
    } else if (err == PDB_ERR_NO)
      err = 0;

    if (err) {
      snprintf(errbuf, errbuf_size,
               "cannot allocate a record with GUID "
               "%s - local primitive ID exists",
               graph_guid_to_string(my_guid, buf1, sizeof buf1));
      cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
             "pdb_primitive_alloc_commit: cannot "
             "allocate a record with GUID \"%s\": %s",
             graph_guid_to_string(&guid, buf3, sizeof buf3), pdb_strerror(err));
      except_throw(err);
    }
  }

  /* Validate the primitive's linkage guids.
   */
  for (linkage = 0; linkage < PDB_LINKAGE_N; linkage++)
    if (pdb_primitive_has_linkage(pr, linkage)) {
      graph_guid g;
      pdb_id linkage_id;

      pdb_primitive_linkage_get(pr, linkage, g);
      cl_assert(pdb->pdb_cl, !GRAPH_GUID_IS_NULL(g));
      err = pdb_id_from_guid(pdb, &linkage_id, &g);
      if (err) {
        snprintf(errbuf, errbuf_size, "%s=%s does not exist",
                 pdb_linkage_to_string(linkage),
                 graph_guid_to_string(&g, buf1, sizeof buf1));
        cl_log(pdb->pdb_cl, CL_LEVEL_FAIL,
               "pdb_primitive_alloc_commit: cannot resolve "
               "%s.%s=%s to a local id: %s (%d)",
               graph_guid_to_string(&guid, buf1, sizeof buf1),
               pdb_linkage_to_string(linkage),
               graph_guid_to_string(&g, buf2, sizeof buf2), pdb_xstrerror(err),
               err);
        except_throw(err);
      }
    }

  /*  If this GUID contains a non-local DB-ID, internalize it.
   */
  id = GRAPH_GUID_SERIAL(*my_guid);
  if (!PDB_GUID_IS_LOCAL(pdb, guid)) {
    /*  Connect our local id (the serial number part of the
     *  GUID we generated) to the incoming global id (as
     *  encoded in the primitive body.)
     */
    err = pdb_id_add_guid(pdb, id, &guid);
    if (err != 0) {
      snprintf(errbuf, errbuf_size, "cannot import %s",
               graph_guid_to_string(&guid, buf1, sizeof buf1));
      except_throw(err);
    }
  }

  /* Our primitive is complete.  Index it.
   */
  err = pdb_index_new_primitive(pdb, id, pr);
  if (err) {
    cl_log_errno(pdb->pdb_cl, CL_LEVEL_ERROR, "pdb_index_new_primitive", err,
                 "Unable to index %s",
                 graph_guid_to_string(my_guid, buf1, sizeof buf1));
    snprintf(errbuf, errbuf_size, "Unable to index %s",
             graph_guid_to_string(my_guid, buf1, sizeof buf1));

    /*  Some of the internal errors PDB will return here
     *  are indistinguishable from legitimate errors.  Make
     *  sure that they map to something sufficiently scary -
     *  this really shouldn't happen, and should translate
     *  to a SYSTEM error message in the caller.
     */
    if (err == PDB_ERR_EXISTS || err == PDB_ERR_ALREADY || err == PDB_ERR_NO)
      err = PDB_ERR_DATABASE;

    except_throw(err);
  }

  addb_istore_free(pdb->pdb_primitive, &pr->pr_data);
  cl_leave(pdb->pdb_cl, CL_LEVEL_DEBUG, "done");

  return err;

err:
  addb_istore_free(pdb->pdb_primitive, &pr->pr_data);
  cl_leave(pdb->pdb_cl, CL_LEVEL_DEBUG, "error: %s [%s:%d]", pdb_strerror(err),
           __FILE__, error_line);
  return err;
}
