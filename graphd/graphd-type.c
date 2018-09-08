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
#include <stdbool.h>
#include <stdio.h>

/*
 *  This module collects utilities that convert between
 *  typeguids and strings.
 */

static int primitive_guid_is_latest(graphd_handle* g,
                                    graph_dateline const* asof,
                                    graph_guid const* guid, bool* latest_out) {
  pdb_id last_id, my_id;
  int err;

  err = pdb_id_from_guid(g->g_pdb, &my_id, guid);
  if (err != 0) return err;

  err = pdb_generation_last_n(g->g_pdb, asof, guid, &last_id, NULL);
  if (err != 0 && err != GRAPHD_ERR_NO) return err;

  *latest_out = (err == GRAPHD_ERR_NO || (err == 0 && last_id == my_id));
  return 0;
}

static int primitive_is_latest(graphd_handle* g, graph_dateline const* asof,
                               pdb_primitive* pr, bool* latest_out) {
  graph_guid tmp;

  pdb_primitive_guid_get(pr, tmp);
  return primitive_guid_is_latest(g, asof, &tmp, latest_out);
}

/*  Utility - make an efficient iterator for links.
 */
static int read_iterator(graphd_handle* g, graph_guid const* left,
                         graph_guid const* right, graph_guid const* typeguid,
                         char const* value_s, size_t value_n,
                         pdb_iterator** it_out) {
  unsigned long long right_count, left_count, value_count;
  pdb_id right_id, left_id;
  pdb_handle* pdb = g->g_pdb;
  cl_handle* cl = pdb_log(pdb);
  int err;

  right_count = left_count = value_count = (unsigned long long)-1;

  cl_assert(cl, left != NULL || right != NULL);
  cl_assert(cl, typeguid != NULL);

  if (right != NULL && pdb_id_from_guid(g->g_pdb, &right_id, right) == 0) {
    err = pdb_vip_linkage_id_count(pdb, right_id, PDB_LINKAGE_RIGHT, typeguid,
                                   PDB_ITERATOR_LOW_ANY, PDB_ITERATOR_HIGH_ANY,
                                   PDB_COUNT_UNBOUNDED, &right_count);
    if (err != 0) return err;
    if (right_count == 0) return GRAPHD_ERR_NO;
  }

  if (left != NULL && pdb_id_from_guid(g->g_pdb, &left_id, left) == 0) {
    err = pdb_vip_linkage_id_count(pdb, left_id, PDB_LINKAGE_LEFT, typeguid,
                                   PDB_ITERATOR_LOW_ANY, PDB_ITERATOR_HIGH_ANY,
                                   PDB_COUNT_UNBOUNDED, &left_count);
    if (err != 0) return err;
    if (left_count == 0) return GRAPHD_ERR_NO;
  }

  if (value_s != NULL) {
    int err = pdb_hash_count(pdb, PDB_HASH_VALUE, value_s, value_n,
                             PDB_ITERATOR_LOW_ANY, PDB_ITERATOR_HIGH_ANY,
                             PDB_COUNT_UNBOUNDED, &value_count);
    if (err) return err;
    if (value_count == 0) return GRAPHD_ERR_NO;
  }

  if (left_count <= right_count) {
    if (left_count <= value_count) {
      return pdb_vip_linkage_iterator(pdb, left, PDB_LINKAGE_LEFT, typeguid,
                                      PDB_ITERATOR_LOW_ANY,
                                      PDB_ITERATOR_HIGH_ANY,
                                      /* forward */ true,
                                      /* error-if-null */ false, it_out, NULL);
    }
  } else {
    if (right_count <= value_count)
      return pdb_vip_linkage_iterator(pdb, right, PDB_LINKAGE_RIGHT, typeguid,
                                      PDB_ITERATOR_LOW_ANY,
                                      PDB_ITERATOR_HIGH_ANY,
                                      /* forward */ true,
                                      /* error-if-null */ false, it_out, NULL);
  }

  cl_assert(cl, value_s != NULL);
  pdb_hash_iterator(pdb, PDB_HASH_VALUE, value_s, value_n, PDB_ITERATOR_LOW_ANY,
                    PDB_ITERATOR_HIGH_ANY, true, it_out);
  return 0;
}

/**
 * @brief Given a GUID of a type, convert the type's name to a graphd_value.
 *
 * @param graphd	database handle
 * @param asof 		NULL or pretend-current dateline
 * @param guid		primitive to look up, or NULL.
 * @param value_out	store the result value here.
 *
 * @return 0 on success, nonzero on weird system or data
 * 	inconsistency errors.
 * @return GRAPHD_ERR_NO if we couldn't identify the type name.
 */
int graphd_type_value_from_guid(graphd_handle* g, graph_dateline const* asof,
                                graph_guid const* guid, graphd_value* val_out) {
  pdb_primitive pr;
  pdb_iterator* it;
  size_t size;
  cl_handle* cl = pdb_log(g->g_pdb);
  char buf[GRAPH_GUID_SIZE];
  char* adhoc;
  cm_handle* cm;
  int err;

  pdb_primitive_initialize(&pr);

  if (guid == NULL || GRAPH_GUID_IS_NULL(*guid)) {
    graphd_value_null_set(val_out);
    return 0;
  }

  /*  If we don't yet know what a "name" attribute or
   *  what the global namespace is, try finding out.
   */
  if (GRAPH_GUID_IS_NULL(g->g_attribute_has_key) ||
      GRAPH_GUID_IS_NULL(g->g_namespace_root)) {
    if ((err = graphd_type_bootstrap_read(g)) != 0) {
      graphd_value_null_set(val_out);
      cl_log(cl, CL_LEVEL_SPEW,
             "graphd_type_value_from_guid: can't"
             " resolve %s: can't read bootstrap code: %s",
             graph_guid_to_string(guid, buf, sizeof buf), graphd_strerror(err));
      goto err;
    }
    if (GRAPH_GUID_IS_NULL(g->g_attribute_has_key)) {
      graphd_value_null_set(val_out);
      cl_log(cl, CL_LEVEL_SPEW,
             "graphd_type_value_from_guid: can't"
             " resolve %s: no \"has_key\" attribute",
             graph_guid_to_string(guid, buf, sizeof buf));
      goto err;
    }
    if (GRAPH_GUID_IS_NULL(g->g_namespace_root)) {
      graphd_value_null_set(val_out);
      cl_log(cl, CL_LEVEL_SPEW,
             "graphd_type_value_from_guid: can't"
             " resolve %s: no global type namespace",
             graph_guid_to_string(guid, buf, sizeof buf));
      goto err;
    }
  }

  /*  Wanted: primitive with the following features:
   *
   *	- meta: is a link with left and right side.
   *	- right: guid
   *	- left: bootstrap namespace
   *	- live: true
   *	- type: has_key
   *	- most recent generation of its lineage
   */
  err = read_iterator(g,
                      /* left	    */ &g->g_namespace_root,
                      /* right    */ guid,
                      /* typeguid */ &g->g_attribute_has_key, NULL, 0, &it);
  if (err == 0) {
    pdb_id id;

    while ((err = pdb_iterator_next_nonstep(g->g_pdb, it, &id)) == 0) {
      bool latest = false;
      int err;
      graph_guid tmp;

      if ((err = pdb_id_read(g->g_pdb, id, &pr)) != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err,
                     "graphd_type_value_from_guid: "
                     "unexpected error reading primitive "
                     "for id %llx",
                     (unsigned long long)id);
        continue;
      }

      if (!pdb_primitive_is_live(&pr) ||
          !pdb_primitive_typeguid_eq(&pr, tmp, g->g_attribute_has_key) ||
          !pdb_primitive_right_eq(&pr, tmp, *guid) ||
          !pdb_primitive_left_eq(&pr, tmp, g->g_namespace_root)) {
        char prbuf[200];
        cl_log(cl, CL_LEVEL_SPEW,
               "graphd_type_value_from_guid: "
               "skipping %s",
               pdb_primitive_to_string(&pr, prbuf, sizeof prbuf));

        pdb_primitive_finish(g->g_pdb, &pr);
        continue;
      }

      /*  OK, this is a name attribute.
       *
       *  Is it the newest one of its lineage, or has it
       *  been versioned?
       */
      err = primitive_is_latest(g, asof, &pr, &latest);
      if (err != 0) {
        pdb_primitive_finish(g->g_pdb, &pr);
        pdb_iterator_destroy(g->g_pdb, &it);

        cl_log(cl, CL_LEVEL_SPEW,
               "graphd_type_value_from_guid: "
               "is-latest fails: %s",
               graphd_strerror(err));
        goto err;
      }
      if (latest) {
        size = pdb_primitive_value_get_size(&pr);
        if (size == 0)
          graphd_value_null_set(val_out);
        else {
          char const* mem;
          mem = pdb_primitive_value_get_memory(&pr);
          graphd_value_text_set(val_out, GRAPHD_VALUE_STRING, mem,
                                mem + size - 1, &pr);
        }
        pdb_primitive_finish(g->g_pdb, &pr);
        pdb_iterator_destroy(g->g_pdb, &it);

        return 0;
      }
      pdb_primitive_finish(g->g_pdb, &pr);
    }

    pdb_iterator_destroy(g->g_pdb, &it);
    if (err != GRAPHD_ERR_NO) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next_nonstep", err,
                   "graphd_type_value_from_guid: "
                   "error while resolving %s",
                   graph_guid_to_string(guid, buf, sizeof buf));
      return err;
    }

    /* Didn't match anything! */
    cl_log(cl, CL_LEVEL_DEBUG,
           "graphd_type_value_from_guid: "
           "no links from type %s in the root namespace",
           graph_guid_to_string(guid, buf, sizeof buf));
  }

  /*  We didn't find anything in the global type namespace.
   *
   *  But maybe there's a name hanging off the type pointing
   *  to some other namespace?
   *
   *	- meta: is a link with left and right side.
   *	- right: guid
   *	- type: has_key
   *	- live: true
   *	- most recent generation of its lineage
   */
  pdb_primitive_initialize(&pr);
  err = read_iterator(g, NULL, guid, &g->g_attribute_has_key, NULL, 0, &it);
  if (err == 0) {
    for (;;) {
      pdb_id id;
      bool latest = false;
      int err;
      graph_guid tmp;

      err = pdb_iterator_next_nonstep(g->g_pdb, it, &id);
      if (err != 0) {
        /* Didn't match anything! */

        pdb_iterator_destroy(g->g_pdb, &it);

        if (err != GRAPHD_ERR_NO) {
          cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next_nonstep", err,
                       "graphd_type_value_from_guid: "
                       "error while resolving %s",
                       graph_guid_to_string(guid, buf, sizeof buf));
          return err;
        }
        goto err;
      }
      err = pdb_id_read(g->g_pdb, id, &pr);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err,
                     "graphd_type_value_from_guid: "
                     "unexpected error reading primitive "
                     "for id %llx",
                     (unsigned long long)id);
        continue;
      }
      if (!(pdb_primitive_has_right(&pr) && pdb_primitive_has_left(&pr) &&
            pdb_primitive_has_typeguid(&pr) && pdb_primitive_is_live(&pr)) ||
          !pdb_primitive_typeguid_eq(&pr, tmp, g->g_attribute_has_key) ||
          !pdb_primitive_right_eq(&pr, tmp, *guid)) {
        char prbuf[200];
        cl_log(cl, CL_LEVEL_SPEW,
               "graphd_type_value_from_guid: "
               "skipping %s",
               pdb_primitive_to_string(&pr, prbuf, sizeof prbuf));

        pdb_primitive_finish(g->g_pdb, &pr);
        continue;
      }

      /*  OK, this is a name attribute.
       *
       *  Is it the newest one of its lineage, or has it
       *  been versioned?
       */
      err = primitive_is_latest(g, asof, &pr, &latest);
      if (err != 0) {
        pdb_primitive_finish(g->g_pdb, &pr);
        pdb_iterator_destroy(g->g_pdb, &it);

        cl_log(cl, CL_LEVEL_SPEW,
               "graphd_type_value_from_guid: "
               "is-latest fails: %s",
               graphd_strerror(err));
        goto err;
      }
      if (latest) break;

      pdb_primitive_finish(g->g_pdb, &pr);
    }

    /*  If we arrive here, pr has been loaded and is valid (so far).
     */
    if ((size = pdb_primitive_value_get_size(&pr)) == 0)
      graphd_value_null_set(val_out);
    else {
      char const* mem = pdb_primitive_value_get_memory(&pr);
      graphd_value_text_set(val_out, GRAPHD_VALUE_STRING, mem, mem + size - 1,
                            &pr);
    }
    pdb_primitive_finish(g->g_pdb, &pr);
    pdb_iterator_destroy(g->g_pdb, &it);

    return 0;
  }

err: /*  Make up a name, given the typeguid.
      *
      *  This doesn't leak because the string is free'd
      *  and cm_c() returns a singleton.
      */
  cm = cm_c();
  if ((adhoc = cm_malloc(cm, GRAPH_GUID_SIZE + 1)) == NULL) {
    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_type_value_from_guid:"
           " malloc fails: %s",
           graphd_strerror(errno));
    return ENOMEM;
  }

  snprintf(adhoc, GRAPH_GUID_SIZE + 1, "#%s",
           graph_guid_to_string(guid, buf, sizeof buf));
  graphd_value_text_set_cm(val_out, GRAPHD_VALUE_STRING, adhoc, strlen(adhoc),
                           cm);
  return 0;
}

/**
 * @brief Given the string name of a type, look up the GUID of the type node.
 *
 * @param graphd	database handle
 * @param asof		NULL or as-of dateline
 * @param name_s	name of the type
 * @param name_n	# of bytes pointed to by name_s, not including a \0
 * @param guid_out	out: GUID of the type object.
 *
 * @return 0 if *guid_out has been assigned the GUID, or if
 *	the name was NULL and *guid_out has been assigned a null GUID.
 * @return GRAPHD_ERR_NO if the specified type does not exist.
 * @return other nonzero error codes on system errors.
 */
static int graphd_type_guid_from_name_in_namespace(
    graphd_handle* g, graph_dateline const* asof, char const* name_s,
    size_t name_n, graph_guid const* namespace, graph_guid* guid_out) {
  pdb_primitive pr;
  pdb_iterator* it;
  int err;
  cl_handle* cl = pdb_log(g->g_pdb);
  char b1[GRAPH_GUID_SIZE];
  char b2[GRAPH_GUID_SIZE];

  cl_enter(cl, CL_LEVEL_SPEW, "(\"%.*s\", %s)", name_s ? (int)name_n : 4,
           name_s ? name_s : "null",
           graph_guid_to_string(namespace, b1, sizeof b1));

  if (name_s == NULL) {
    GRAPH_GUID_MAKE_NULL(*guid_out);
    cl_leave(cl, CL_LEVEL_SPEW, "NULL name; null");
    return 0;
  }

  /*  Wanted: primitive with the following features:
   *	- value is name_s...name_n
   * 	- meta: is a link with left and right side.
   *	- left: the namespace
   *	- live: true
   *	- type: has_key
   *	- most recent generation of its lineage
   */

  err = read_iterator(g, namespace, NULL, &g->g_attribute_has_key, name_s,
                      name_n, &it);
  if (err != 0) {
    cl_leave(cl, CL_LEVEL_SPEW, "can't resolve %.*s: %s", (int)name_n, name_s,
             graphd_strerror(err));
    return err;
  }
  for (;;) {
    bool latest = false;
    int err;
    graph_guid tmp;
    void* mem;
    pdb_id id;

    if ((err = pdb_iterator_next_nonstep(g->g_pdb, it, &id)) != 0) {
      pdb_iterator_destroy(g->g_pdb, &it);

      if (err != GRAPHD_ERR_NO)
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_iterator_next_nonstep", err,
                     "unexpected error while resolving %.*s", (int)name_n,
                     name_s);
      cl_leave(cl, CL_LEVEL_SPEW, "can't resolve %.*s: %s", (int)name_n, name_s,
               graphd_strerror(err));
      return err;
    }
    err = pdb_id_read(g->g_pdb, id, &pr);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err,
                   "can't read primitive for \"%.*s\"", (int)name_n, name_s);
      pdb_iterator_destroy(g->g_pdb, &it);
      return err;
    }
    if (!(pdb_primitive_has_right(&pr) && pdb_primitive_has_left(&pr) &&
          pdb_primitive_has_typeguid(&pr) && pdb_primitive_is_live(&pr)) ||
        !pdb_primitive_typeguid_eq(&pr, tmp, g->g_attribute_has_key) ||
        !pdb_primitive_left_eq(&pr, tmp, *namespace) ||
        pdb_primitive_value_get_size(&pr) != name_n + 1 ||
        (mem = pdb_primitive_value_get_memory(&pr)) == NULL ||
        strncasecmp(mem, name_s, name_n) != 0) {
      pdb_primitive_finish(g->g_pdb, &pr);
      continue;
    }

    /*  OK, this is the link we're looking for.
     *
     *  Is it the newest one of its lineage, or has it
     *  been versioned?
     */
    err = primitive_is_latest(g, asof, &pr, &latest);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "primitive_is_latest", err,
                   "error while checking primitive for \"%.*s\"", (int)name_n,
                   name_s);
      pdb_primitive_finish(g->g_pdb, &pr);
      pdb_iterator_destroy(g->g_pdb, &it);

      cl_leave(cl, CL_LEVEL_SPEW, "can't resolve %.*s: %s", (int)name_n, name_s,
               graphd_strerror(err));
      return err;
    }
    if (latest) break;

    pdb_primitive_finish(g->g_pdb, &pr);
  }
  /*  If we arrive here, we've found a good candidate;
   *  it's in <pr>.
   */
  pdb_primitive_right_get(&pr, *guid_out);

  pdb_primitive_finish(g->g_pdb, &pr);
  pdb_iterator_destroy(g->g_pdb, &it);

  cl_leave(cl, CL_LEVEL_SPEW, "\"%.*s\" in %s -> %s", (int)name_n, name_s,
           graph_guid_to_string(namespace, b1, sizeof b1),
           graph_guid_to_string(guid_out, b2, sizeof b2));
  return 0;
}

/**
 * @brief Given the string name of a type, look up the GUID of the type node.
 *
 * @param graphd	database handle
 * @param asof		NULL or as-of dateline
 * @param name_s	name of the type
 * @param name_n	# of bytes pointed to by name_s, not including a \0
 * @param guid_out	out: GUID of the type object.
 *
 * @return 0 if *guid_out has been assigned the GUID, or if
 *	the name was NULL and *guid_out has been assigned a null GUID.
 * @return GRAPHD_ERR_NO if the specified type does not exist.
 * @return other nonzero error codes on system errors.
 */
int graphd_type_guid_from_name(graphd_handle* g, graph_dateline const* asof,
                               char const* name_s, size_t name_n,
                               graph_guid* guid_out) {
  int err;
  cl_handle* cl = pdb_log(g->g_pdb);
  char buf[GRAPH_GUID_SIZE];

  cl_enter(cl, CL_LEVEL_SPEW, "(\"%.*s\")", name_s ? (int)name_n : 4,
           name_s ? name_s : "null");

  if (name_s == NULL) {
    GRAPH_GUID_MAKE_NULL(*guid_out);
    cl_leave(cl, CL_LEVEL_SPEW, "NULL name; null");
    return 0;
  }

  /*  If we haven't yet figured out what a "name" attribute or
   *  what the global namespace is, this won't find anything.
   */
  /*  If we don't yet know what a "name" attribute or
   *  what the global namespace is, try finding out.
   */
  if (GRAPH_GUID_IS_NULL(g->g_attribute_has_key) ||
      GRAPH_GUID_IS_NULL(g->g_namespace_root) ||
      GRAPH_GUID_IS_NULL(g->g_namespace_bootstrap)) {
    if ((err = graphd_type_bootstrap_read(g)) != 0) {
      cl_leave(cl, CL_LEVEL_SPEW, "can't resolve %.*s: bootstrap fails: %s",
               (int)name_n, name_s, graphd_strerror(err));
      return err;
    }
    if (GRAPH_GUID_IS_NULL(g->g_attribute_has_key)) {
      cl_leave(cl, CL_LEVEL_SPEW,
               "can't resolve %.*s: no \"has_key\" attribute", (int)name_n,
               name_s);
      return GRAPHD_ERR_NO;
    }
    if (GRAPH_GUID_IS_NULL(g->g_namespace_root)) {
      cl_leave(cl, CL_LEVEL_SPEW,
               "can't resolve %.*s: no global type namespace", (int)name_n,
               name_s);
      return GRAPHD_ERR_NO;
    }
    if (GRAPH_GUID_IS_NULL(g->g_namespace_bootstrap)) {
      cl_leave(cl, CL_LEVEL_SPEW, "can't resolve %.*s: no bootstrap namespace",
               (int)name_n, name_s);
      return GRAPHD_ERR_NO;
    }
  }

  err = graphd_type_guid_from_name_in_namespace(
      g, asof, name_s, name_n, &g->g_namespace_bootstrap, guid_out);
  if (err == GRAPHD_ERR_NO)
    err = graphd_type_guid_from_name_in_namespace(
        g, asof, name_s, name_n, &g->g_namespace_root, guid_out);

  cl_leave(cl, CL_LEVEL_SPEW, "\"%.*s\" -> %s", (int)name_n, name_s,
           err ? graphd_strerror(err)
               : graph_guid_to_string(guid_out, buf, sizeof buf));
  return err;
}

static int write_primitive(graphd_request* greq, graph_guid* guid_out,
                           graph_guid const* guid_left,
                           graph_guid const* guid_right,
                           graph_guid const* guid_type,
                           graph_guid const* guid_scope, char const* name,
                           size_t name_n, /* 0 or strlen(name) + 1 */
                           char const* value,
                           size_t value_n) /* 0 or strlen(value) + 1 */
{
  graphd_handle* g = graphd_request_graphd(greq);
  pdb_primitive pr;
  int err;
  cl_handle* cl = graphd_request_cl(greq);
  char errbuf[200];

  cl_log(cl, CL_LEVEL_SPEW, "write_primitive(value=%.*s)",
         (int)(value_n ? value_n - 1 : 4), value_n ? value : "null");

  graphd_dateline_expire(g);
  err =
      pdb_primitive_alloc(g->g_pdb, g->g_now, NULL, &pr, guid_out, g->g_now,
                          value == NULL ? GRAPH_DATA_NULL : GRAPH_DATA_STRING,
                          PDB_PRIMITIVE_BIT_LIVE | PDB_PRIMITIVE_BIT_ARCHIVAL |
                              (greq->greq_data.gd_write.gdw_txstart_written
                                   ? 0
                                   : PDB_PRIMITIVE_BIT_TXSTART),
                          name_n, value_n, name, value, guid_type, guid_right,
                          guid_left, guid_scope, NULL, errbuf, sizeof errbuf);

  if (err != 0)
    cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_primitive_alloc", err, "errbuf=%s",
                 errbuf);

  else {
    err = pdb_primitive_alloc_commit(g->g_pdb, NULL, guid_out, &pr, errbuf,
                                     sizeof errbuf);
    if (err != 0)
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_primitive_alloc_commit", err,
                   "errbuf=%s", errbuf);
  }

  /* Error anywhere along the line?
   */
  if (err != 0) {
    if (greq->greq_error_message == NULL) {
      if (err == PDB_ERR_PRIMITIVE_TOO_LARGE)
        graphd_request_errprintf(greq, 0, "TOOBIG %s",
                                 *errbuf ? errbuf : "primitive too big");
      else
        graphd_request_errprintf(
            greq, 0, "SEMANTICS %s%s%s",
            err == PDB_ERR_NO ? "not found" : graphd_strerror(err),
            *errbuf ? ": " : "", errbuf);
    }
    return err;
  }

  greq->greq_data.gd_write.gdw_txstart_written = 1;
  graph_timestamp_next(&g->g_now);

  return 0;
}

/**
 * @brief Given a namespace and a name, find or make the node with
 *  that name in that namespace.
 *
 *  The namespace points to the node with a "has_key" link.
 *
 * @param g		opaque graphd handle
 * @param namespace 	look in this namespace
 * @param name_s	for something labelled with this name
 * @param name_n	# of bytes pointed to by name_s, not including
 *			a \0, if any.
 * @param guid_out 	write result to this
 *
 * @return 0 on success, a nonzero error code on error.
 */
static int make_node_has_key(graphd_request* greq, graph_guid const* namespace,
                             char const* name_s, size_t name_n,
                             graph_guid* guid_out) {
  graphd_handle* g = graphd_request_graphd(greq);
  pdb_primitive pr;
  int err;
  cl_handle* cl = pdb_log(g->g_pdb);
  char* name = NULL;
  pdb_iterator* it;
  graph_guid guid_tmp;

  cl_assert(cl, name_s != NULL);
  cl_enter(cl, CL_LEVEL_SPEW, "enter");

  /*  Wanted: a primitive with the following features:
   *
   *	- typeguid = has_key
   *	- left     = namespace parameter
   *	- value    = name parameter
   *	- live     = true
   *
   *  If we find one of those, we want its right side in *guid_out.
   *
   *  We can go via the typeguid and left, or via the value.
   *  I guess we'll go via the value.
   */

  pdb_primitive_initialize(&pr);
  err = read_iterator(g, namespace, NULL, &g->g_attribute_has_key, name_s,
                      name_n, &it);
  if (err == 0) {
    pdb_id id;

    while ((err = pdb_iterator_next_nonstep(g->g_pdb, it, &id)) == 0) {
      bool latest = false;
      unsigned long sz;
      char const* mem;

      err = pdb_id_read(g->g_pdb, id, &pr);
      if (err != 0) {
        cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err,
                     "make_node_has_key: unexpected "
                     "error reading primitive for id %llx",
                     (unsigned long long)id);
        continue;
      }
      if (!(pdb_primitive_has_right(&pr) && pdb_primitive_has_left(&pr) &&
            pdb_primitive_is_live(&pr)) ||
          (sz = pdb_primitive_value_get_size(&pr)) != name_n + 1 ||
          (mem = pdb_primitive_value_get_memory(&pr)) == NULL ||
          strncasecmp(mem, name_s, name_n) != 0) {
        pdb_primitive_finish(g->g_pdb, &pr);
        continue;
      }

      /*  OK, this is the value we want.
       *
       *  Is it the newest one of its lineage, or has it
       *  been versioned?
       */
      if ((err = primitive_is_latest(g, /* asof */ NULL, &pr, &latest)) != 0) {
        pdb_primitive_finish(g->g_pdb, &pr);
        pdb_iterator_destroy(g->g_pdb, &it);

        return err;
      }
      if (!latest) {
        pdb_primitive_finish(g->g_pdb, &pr);
        continue;
      }

      /*  OK, it's the value we want and the latest instance.
       *  But does it point to the namespace we want?
       */
      if (!pdb_primitive_left_eq(&pr, guid_tmp, *namespace)) {
        graph_guid left;
        char left_buf[GRAPH_GUID_SIZE];

        /*  It's probably better to just pick it up,
         *  even if its value _isn't_ in the root
         *  namespace.
         *
         *  (If the user knew what they were doing,
         *  they'd have used typeguids to begin with.)
         */
        pdb_primitive_left_get(&pr, left);
        cl_log(cl, CL_LEVEL_DEBUG,
               "make_node_has_key(%.*s): %s isn't "
               "the namespace I was looking for, "
               "but I'll take it.",
               (int)name_n, name_s,
               graph_guid_to_string(&left, left_buf, sizeof left_buf));
      }
      pdb_primitive_right_get(&pr, *guid_out);

      pdb_primitive_finish(g->g_pdb, &pr);
      pdb_iterator_destroy(g->g_pdb, &it);

      cl_leave(cl, CL_LEVEL_SPEW, "(%.*s): found", (int)name_n, name_s);

      return 0;
    }
    pdb_iterator_destroy(g->g_pdb, &it);
  }

  /*  We didn't find anything in our iteration (if we even looked).
   *  Create the opaque node that the name will name.
   */
  err = write_primitive(greq, guid_out, NULL, NULL, NULL, &g->g_core_scope,
                        NULL, 0, NULL, 0);
  if (err == 0) {
    /*  Convert from programmer strings to graphd strings
     *  (which include a closing \0 in the count).
     */
    if (name_s == NULL)
      name = NULL;
    else {
      name = malloc(name_n + 1);
      if (name == NULL) return ENOMEM;
      memcpy(name, name_s, name_n);
      name[name_n] = '\0';
    }

    /*  Connect the node to the namespace with its name.
     */
    err = write_primitive(greq, &guid_tmp,
                          /* left    */ namespace,
                          /* right   */ guid_out,
                          /* typeguid */ &g->g_attribute_has_key,
                          /* scope */ &g->g_core_scope, NULL, 0, name,
                          name_n + 1);

    if (name != NULL) free(name);
  }
  cl_leave(cl, CL_LEVEL_SPEW, "(%.*s): created %s", (int)name_n, name_s,
           err ? graphd_strerror(err) : "ok");
  return err;
}

static int read_named(graphd_handle* g, char const* name, int link_bits,
                      pdb_primitive* pr_out) {
  unsigned long long num_named = 0;
  size_t name_n = 0;
  int err;
  pdb_iterator* it;
  char const* mem;
  cl_handle* cl = pdb_log(g->g_pdb);
  pdb_id id;

  /*  Wanted: primitive with the following features:
   *	- name is <name>
   * 	- link_bits are all set
   * 	- newest version
   */

  cl_assert(cl, name);

  name_n = strlen(name);
  err = pdb_hash_count(g->g_pdb, PDB_HASH_NAME, name, name_n,
                       PDB_ITERATOR_LOW_ANY, PDB_ITERATOR_HIGH_ANY,
                       PDB_COUNT_UNBOUNDED, &num_named);
  if (err) return err;
  if (num_named <= 0) return GRAPHD_ERR_NO;

  pdb_primitive_initialize(pr_out);
  pdb_hash_iterator(g->g_pdb, PDB_HASH_NAME, name, name_n, PDB_ITERATOR_LOW_ANY,
                    PDB_ITERATOR_HIGH_ANY, true, &it);

  while ((err = pdb_iterator_next_nonstep(g->g_pdb, it, &id)) == 0) {
    bool latest = false;
    unsigned long sz;

    err = pdb_id_read(g->g_pdb, id, pr_out);
    if (err != 0) {
      cl_log_errno(cl, CL_LEVEL_FAIL, "pdb_id_read", err,
                   "read_named: unexpected "
                   "error reading primitive for id %llx",
                   (unsigned long long)id);
      continue;
    }

    if (!pdb_primitive_is_live(pr_out) ||
        (pdb_primitive_link_bitmask(pr_out) & link_bits) != link_bits ||
        (sz = pdb_primitive_name_get_size(pr_out)) != name_n + 1 ||
        (mem = pdb_primitive_name_get_memory(pr_out)) == NULL ||
        strncasecmp(mem, name, name_n) != 0) {
      pdb_primitive_finish(g->g_pdb, pr_out);
      continue;
    }

    /*  OK, this is a name attribute with the value we want.
     *
     *  Is it the newest one of its lineage, or has it
     *  been versioned?
     */
    if ((err = primitive_is_latest(g, NULL /* asof */, pr_out, &latest)) != 0) {
      pdb_iterator_destroy(g->g_pdb, &it);
      pdb_primitive_finish(g->g_pdb, pr_out);
      return err;
    }
    if (latest) {
      /* Found something with the desired name.
       */
      pdb_iterator_destroy(g->g_pdb, &it);
      return 0;
    }

    pdb_primitive_finish(g->g_pdb, pr_out);
  }
  pdb_iterator_destroy(g->g_pdb, &it);
  return err;
}

/**
 * @brief Read the core type system, if there is one.
 * @param graphd	database handle
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_type_bootstrap_read(graphd_handle* g) {
  pdb_primitive pr;
  int err;

  pdb_primitive_initialize(&pr);
  err = read_named(g, "ROOT_NAMESPACE", 1 << PDB_LINKAGE_SCOPE, &pr);
  if (err != 0) return err;
  pdb_primitive_guid_get(&pr, g->g_namespace_root);
  pdb_primitive_scope_get(&pr, g->g_core_scope);
  pdb_primitive_finish(g->g_pdb, &pr);

  err = read_named(g, "Metaweb_Bootstrap_Anchor", 1 << PDB_LINKAGE_SCOPE, &pr);
  if (err != 0) return err;
  pdb_primitive_left_get(&pr, g->g_namespace_bootstrap);
  pdb_primitive_right_get(&pr, g->g_attribute_has_key);
  pdb_primitive_finish(g->g_pdb, &pr);

  return 0;
}

/**
 * @brief Create the core type system.
 * @param graphd	database handle
 * @return 0 on success, a nonzero error code on error.
 */
int graphd_type_bootstrap(graphd_request* greq) {
  graphd_handle* g = graphd_request_graphd(greq);
  cl_handle* cl = graphd_request_cl(greq);
  int err;
  pdb_primitive pr;

  /*  We're operating as part of a write call; there are no
   *  other reads or writes going on in the server in parallel.
   */

  /*             METAWEB_BOOTSTRAP_ANCHOR
   *  AT-has_key   ----has_key--->	NS:bootstrap-namespace
   */
  pdb_primitive_initialize(&pr);
  err = read_named(g, "Metaweb_Bootstrap_Anchor",
                   1 << PDB_LINKAGE_RIGHT | 1 << PDB_LINKAGE_LEFT |
                       1 << PDB_LINKAGE_TYPEGUID | 1 << PDB_LINKAGE_SCOPE,
                   &pr);
  if (err != 0 && err != GRAPHD_ERR_NO) {
    cl_log(cl, CL_LEVEL_FAIL,
           "graphd_type_bootstrap: unexpected error while "
           "looking for Metaweb_Bootstrap_Anchor: %s",
           graphd_strerror(err));
    return err;
  }
  if (err == 0) {
    pdb_primitive_finish(g->g_pdb, &pr);
    err = graphd_type_bootstrap_read(g);
  } else {
    graph_guid dummy;

    /* 0: name=CORE_SCOPE */
    err = write_primitive(greq, &g->g_core_scope, NULL, NULL, NULL, NULL,
                          "CORE_SCOPE", sizeof("CORE_SCOPE"), NULL, 0);
    if (err != 0) {
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_type_bootstrap: unexpected error while "
             "inserting CORE_SCOPE: %s",
             graphd_strerror(err));
      goto clean_up_bootstrap;
    }

    /* 1: name=ROOT_NAMESPACE scope=0 */
    err = write_primitive(greq, &g->g_namespace_root, NULL, NULL, NULL,
                          &g->g_core_scope, "ROOT_NAMESPACE",
                          sizeof("ROOT_NAMESPACE"), NULL, 0);
    if (err != 0) {
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_type_bootstrap: unexpected error while "
             "inserting CORE_SCOPE: %s",
             graphd_strerror(err));
      goto clean_up_bootstrap;
    }

    /* 2: name=HAS_KEY scope=0 */
    err = write_primitive(greq, &g->g_attribute_has_key, NULL, NULL, NULL,
                          &g->g_core_scope, "HAS_KEY", sizeof("HAS_KEY"), NULL,
                          0);
    if (err != 0) {
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_type_bootstrap: unexpected error while "
             "writing Metaweb_Bootstrap_Anchor has_key "
             "primitive: %s",
             graphd_strerror(err));
      goto clean_up_bootstrap;
    }

    /* 3: scope=0  [bootstrap namespace] */
    err = write_primitive(greq, &g->g_namespace_bootstrap, NULL, NULL, NULL,
                          &g->g_core_scope, NULL, 0, NULL, 0);
    if (err != 0) {
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_type_bootstrap: unexpected error while "
             "inserting CORE_SCOPE: %s",
             graphd_strerror(err));
      goto clean_up_bootstrap;
    }

    /* 4: In the root namespace, the boostrap namespace is
     *    called "/boot"
     */
    err = write_primitive(greq, &dummy,
                          /* left */ &g->g_namespace_root,
                          /* right */ &g->g_namespace_bootstrap,
                          /* typeguid */ &g->g_attribute_has_key,
                          /* scope */ &g->g_core_scope,
                          /* name */ NULL, 0,
                          /* value */ "boot", sizeof("boot"));
    if (err != 0) {
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_type_bootstrap: unexpected error while "
             "writing /boot has_key primitive: %s",
             graphd_strerror(err));
      goto clean_up_bootstrap;
    }

    /* 5: In the bootstrap namespace, the root namespace is
     *    called "/root_namespace".
     */
    err =
        write_primitive(greq, &dummy,
                        /* left */ &g->g_namespace_bootstrap,
                        /* right */ &g->g_namespace_root,
                        /* typeguid */ &g->g_attribute_has_key,
                        /* scope */ &g->g_core_scope,
                        /* name */ NULL, 0,
                        /* value */ "root_namespace", sizeof("root_namespace"));
    if (err != 0) {
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_type_bootstrap: unexpected error while "
             "writing /root_namespace has_key primitive: %s",
             graphd_strerror(err));
      goto clean_up_bootstrap;
    }

    /* 6: name=Metaweb_Bootstrap_Anchor scope=0 value="has_key"
     * 	type=#HAS_KEY right=#HAS_KEY, left={bootstrap}
     */
    err = write_primitive(
        greq, &dummy, &g->g_namespace_bootstrap, &g->g_attribute_has_key,
        &g->g_attribute_has_key, &g->g_core_scope, "Metaweb_Bootstrap_Anchor",
        sizeof("Metaweb_Bootstrap_Anchor"), "has_key", sizeof("has_key"));
    if (err != 0) {
      cl_log(cl, CL_LEVEL_FAIL,
             "graphd_type_bootstrap: unexpected error while "
             "inserting Metaweb_Bootstrap_Anchor namespace: "
             "%s",
             graphd_strerror(err));
      goto clean_up_bootstrap;
    }
  }
  return 0;

clean_up_bootstrap:
  cl_assert(cl, err);

  return err;
}

/**
 * @brief Given the string name of a type, look up or create
 * 	the GUID of its type object.
 *
 * @param g		database handle
 * @param name_s	name of the type
 * @param name_n	# of bytes pointed to by name_s, not including a \0
 * @param guid_out	out: GUID of the type object.
 *
 * @return 0 if *guid_out has been assigned the GUID, or if
 *	the name was NULL and *guid_out has been assigned a null GUID.
 * @return other nonzero error codes on system errors.
 */
int graphd_type_make_name(graphd_request* greq, char const* name_s,
                          size_t name_n, graph_guid* guid_out) {
  graphd_handle* g = graphd_request_graphd(greq);
  int err;
  cl_handle* cl = pdb_log(g->g_pdb);
  char guid_buf[GRAPH_GUID_SIZE];

  cl_enter(cl, CL_LEVEL_SPEW, "enter");
  if (name_s == NULL) {
    GRAPH_GUID_MAKE_NULL(*guid_out);
    cl_leave(cl, CL_LEVEL_SPEW, "null");
    return 0;
  }

  /*  If we don't yet know what a "name" attribute or
   *  what the global namespace is, we should now go create them.
   */
  if (GRAPH_GUID_IS_NULL(g->g_attribute_has_key) ||
      GRAPH_GUID_IS_NULL(g->g_namespace_root)) {
    if ((err = graphd_type_bootstrap(greq)) != 0) {
      cl_leave(cl, CL_LEVEL_SPEW, "graphd_type_bootstrap fails: %s",
               graphd_strerror(err));
      return err;
    }
  }

  err = make_node_has_key(greq, &g->g_namespace_root, name_s, name_n, guid_out);

  cl_leave(cl, CL_LEVEL_SPEW, "(%.*s): %s", (int)name_n, name_s,
           err ? graphd_strerror(err)
               : graph_guid_to_string(guid_out, guid_buf, sizeof(guid_buf)));
  return err;
}

/**
 * @brief Reset the cached type system, e.g. prior to a restore.
 * @param g	database handle
 */
void graphd_type_initialize(graphd_handle* g) {
  GRAPH_GUID_MAKE_NULL(g->g_namespace_bootstrap);
  GRAPH_GUID_MAKE_NULL(g->g_namespace_root);
  GRAPH_GUID_MAKE_NULL(g->g_attribute_has_key);
  GRAPH_GUID_MAKE_NULL(g->g_core_scope);
}
