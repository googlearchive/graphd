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
#include <limits.h>
#include <stdio.h>
#include <string.h>

graphd_constraint *graphd_constraint_or_prototype_root(
    graphd_constraint const *con) {
  while (con != NULL && con->con_or != NULL &&
         con->con_or->or_prototype != NULL)
    con = con->con_or->or_prototype;

  return (graphd_constraint *)con;
}

graphd_constraint_or *graphd_constraint_or_create(graphd_request *greq,
                                                  graphd_constraint *prototype,
                                                  bool short_circuit) {
  graphd_constraint_or *o;

  o = cm_malloc(greq->greq_req.req_cm, sizeof(*o));
  if (o == NULL) return NULL;

  memset(o, 0, sizeof *o);
  o->or_prototype = prototype;
  graphd_constraint_initialize(graphd_request_graphd(greq), &o->or_head);
  if (prototype != NULL) o->or_head.con_parent = prototype->con_parent;
  return o;
}

/*  Append a set of alternatives to the "prototype" constraint
 *  that all those alternatives have in common.
 */
void graphd_constraint_or_append_to_prototype(graphd_constraint *prototype,
                                              graphd_constraint_or *new_or) {
  *prototype->con_or_tail = new_or;
  prototype->con_or_tail = &new_or->or_next;
  new_or->or_next = NULL;
}

/*  Return the "or" above "sub" that's directly below
 *  "prototype".
 */
graphd_constraint_or *graphd_constraint_or_below(
    graphd_constraint const *prototype, graphd_constraint const *sub) {
  graphd_constraint_or *cor;
  graphd_constraint *proto;

  if (sub == NULL || sub == prototype) return NULL;

  for (cor = sub->con_or; cor != NULL; cor = proto->con_or) {
    proto = cor->or_prototype;

    if (proto == NULL || proto == prototype) break;
  }
  return cor;
}

/*  A single { ... } has just finished parsing.
 */
int graphd_constraint_or_complete_parse(graphd_request *greq,
                                        graphd_constraint *prototype,
                                        graphd_constraint *sub) {
  size_t i;
  cl_handle *cl = graphd_request_cl(greq);
  graphd_constraint_or *cor;

  cl_log(cl, CL_LEVEL_VERBOSE,
         "graphd_constraint_or_complete_parse (proto=%p,sub=%p)",
         (void *)prototype, (void *)sub);

  sub->con_parent = prototype->con_parent;
  sub->con_next = NULL;

  /*  Certain things can't be set in the "or"
   *  subconstraint.
   */
  if (sub->con_result != NULL) {
    graphd_request_errprintf(
        greq, 0, "SEMANTICS can't change result=... in an or-branch");
    return GRAPHD_ERR_SEMANTICS;
  }

  if (sub->con_linkage != 0) {
    graphd_request_errprintf(greq, 0,
                             "SEMANTICS can't change linkage in an or-branch");
    return GRAPHD_ERR_SEMANTICS;
  }

  if (sub->con_sort != NULL && sub->con_sort_valid) {
    graphd_request_errprintf(
        greq, 0, "SEMANTICS can't change sort order in an or-branch");
    return GRAPHD_ERR_SEMANTICS;
  }

  if (sub->con_sort_comparators.gcl_used) {
    graphd_request_errprintf(
        greq, 0, "SEMANTICS can't change comparator list in an or-branch");
    return GRAPHD_ERR_SEMANTICS;
  }
  if (sub->con_pagesize_valid) {
    graphd_request_errprintf(greq, 0,
                             "SEMANTIC can't change pagesize in an or-branch");
    return GRAPHD_ERR_SEMANTICS;
  }
  if (sub->con_resultpagesize_parsed_valid) {
    graphd_request_errprintf(
        greq, 0, "SEMANTICS can't change resultpagesize in an or-branch");
    return GRAPHD_ERR_SEMANTICS;
  }
  if (sub->con_countlimit_valid) {
    graphd_request_errprintf(
        greq, 0, "SEMANTICS can't change countlimit in an or-branch");
    return GRAPHD_ERR_SEMANTICS;
  }
  if (sub->con_cursor_s != NULL) {
    graphd_request_errprintf(greq, 0,
                             "SEMANTICS can't use a cursor in an or-branch");
    return GRAPHD_ERR_SEMANTICS;
  }

  if (prototype->con_linkage != 0) {
    if (sub->con_linkage != 0 && sub->con_linkage != prototype->con_linkage) {
      graphd_request_errprintf(greq, 0,
                               "SEMANTICS conflicting linkage inside "
                               "and outside or-constraint");
      return GRAPHD_ERR_SEMANTICS;
    }
    sub->con_linkage = prototype->con_linkage;
  }

  sub->con_parent = prototype->con_parent;
  sub->con_next = NULL;

  /*  For anything that's unset, copy a default
   *  from the prototype.
   */

  sub->con_forward = prototype->con_forward;

  if (prototype->con_false) sub->con_false = true;

  if (sub->con_live == GRAPHD_FLAG_UNSPECIFIED)
    sub->con_live = prototype->con_live;

  if (sub->con_archival == GRAPHD_FLAG_UNSPECIFIED)
    sub->con_archival = prototype->con_archival;

  /* Generational constraints. */

  if (!sub->con_newest.gencon_valid) sub->con_newest = prototype->con_newest;

  if (!sub->con_oldest.gencon_valid) sub->con_oldest = prototype->con_oldest;

  if (!sub->con_timestamp_valid) {
    sub->con_timestamp_valid = prototype->con_timestamp_valid;
    sub->con_timestamp_min = prototype->con_timestamp_min;
    sub->con_timestamp_max = prototype->con_timestamp_max;
  }

  if (sub->con_meta == GRAPHD_META_UNSPECIFIED)
    sub->con_meta = prototype->con_meta;

  if (prototype->con_linkage != 0) {
    if (sub->con_linkage != 0 && sub->con_linkage != prototype->con_linkage) {
      graphd_request_errprintf(greq, 0,
                               "SEMANTICS conflicting linkage inside "
                               "and outside or-constraint");
      return GRAPHD_ERR_SEMANTICS;
    }
    sub->con_linkage = prototype->con_linkage;
  }

  for (i = 0; i < PDB_LINKAGE_N; i++) {
    if (!GRAPH_GUID_IS_NULL(prototype->con_linkguid[i])) {
      if (GRAPH_GUID_IS_NULL(sub->con_linkguid[i]))
        sub->con_linkguid[i] = prototype->con_linkguid[i];
      else if (!GRAPH_GUID_EQ(sub->con_linkguid[i], prototype->con_linkguid[i]))
        prototype->con_false = true;
    }
  }

  if (sub->con_valuetype == 0) sub->con_valuetype = prototype->con_valuetype;

  if (sub->con_comparator == NULL ||
      sub->con_comparator == graphd_comparator_default)
    sub->con_comparator = prototype->con_comparator;

  if (sub->con_value_comparator == NULL ||
      sub->con_value_comparator == graphd_comparator_default)
    sub->con_value_comparator = prototype->con_value_comparator;

#if 0
	graphd_string_constraint_queue	  con_type;
	graphd_string_constraint_queue	  con_name;
	graphd_string_constraint_queue	  con_value;

	graphd_string_constraint    	  con_strcon_buf[1];
	size_t			    	  con_strcon_n;

	graphd_guid_constraint	    	  con_guid;
	graphd_guid_constraint		  con_linkcon[PDB_LINKAGE_N];

	/* The next two are pseudoconstraints that eventually
	 * translate into GUID restrictions.
	 */
	graphd_guid_constraint		  con_version_previous;
	graphd_guid_constraint		  con_version_next;

	graphd_count_constraint		  con_count;

	/* This expression uses "contents" somewhere -- in a sort,
	 * assignment, or return (possibly an implicit return).
	 */
	unsigned int		    	  con_uses_contents:	1;

	int				  con_unique;
	int				  con_key;

	/*  There are up to assignment_n + 2 pframes in total.
	 *
	 *  pframe 0..n-1 are the first assignment_n pframes.
	 *  pframe N is the returned result.
	 *  pframe N + 1 is the unnamed temporary.
	 */
	graphd_pattern_frame		* con_pframe;
	size_t				  con_pframe_n;
	cm_hashtable			  con_pframe_by_name;

	size_t				  con_pframe_temporary;
	unsigned int			  con_pframe_want_count:  1;
	unsigned int			  con_pframe_want_cursor: 1;
	unsigned int			  con_pframe_want_data: 1;

	graphd_sort_root		  con_sort_root;

	/* The estimated maximum number of matches for this
	 * constraint, given any fixed parent.
	 */
	unsigned long long		  con_setsize;

	size_t				  con_start;

	/*  Some IDs that don't match this.
	 */
	graphd_bad_cache		  con_bad_cache;

	graphd_dateline_constraint	  con_dateline;

	/* The constraint title.  Used to
	 * identify this constraint in log entries.
	 */
	char				* con_title;
	char                              con_title_buf[256];

	/*  (READ)
	 *
	 *  A counted chain that keeps track of the variables assigned
	 *  to via $tag=value statements.
	 */
	graphd_assignment    	       * con_assignment_head;
	graphd_assignment   	      ** con_assignment_tail;
	size_t				 con_assignment_n;

	/*  How many variables in this context are assigned to in
	 *  subconstraints.
	 */
	size_t				  con_local_n;

	/*  Map the variable name to an index into the temporary
	 *  local results.
	 */
	cm_hashtable			  con_local_hash;

	/*  (READ)  Variables declared (i.e., used) in this constraint.
	 *
	 *  The hashtable hashes their name to a
	 *  graphd_variable_declaration record.
	 */
	cm_hashtable			  con_variable_declaration;
	unsigned int			  con_variable_declaration_valid: 1;

	/*  (READ)  An iterator that produces candidates for this
	 *  constraint (not taking into account the parent).
	 */
	graphd_freezable_iterator	  con_frit;

	/*  (READ) low, high boundaries
	 */
	unsigned long long		  con_low;
	unsigned long long		  con_high;
#endif

  /*  If I have little ORs below me, let those copy from me.
   */
  for (cor = sub->con_or_head; cor != NULL; cor = cor->or_next) {
    graphd_constraint_or_complete_parse(greq, sub, &cor->or_head);
    if (cor->or_tail != NULL)
      graphd_constraint_or_complete_parse(greq, sub, cor->or_tail);
  }
  return 0;
}

/*  Initialize or re-initialize the "read-or-map" that,
 *  for a given ID, tracks which of the OR branches
 *  in the ID's constraint evaluate to true.
 */
size_t graphd_constraint_or_index(graphd_request *greq, graphd_constraint *con,
                                  size_t n) {
  graphd_constraint_or *cor;

  con->con_or_index = n++;
  for (cor = con->con_or_head; cor != NULL; cor = cor->or_next) {
    n = graphd_constraint_or_index(greq, &cor->or_head, n);
    if (cor->or_tail != NULL)
      n = graphd_constraint_or_index(greq, cor->or_tail, n);
  }
  return n;
}

/*  Declare the variable <name_s..name_e> in the constraint
 *  <orcon>.
 *
 *  This actually declares <name_s..name_e[con_or_index]> instead,
 *  and adds a __pick__ entry in <name_s..name_e> in the arch prototype.
 */
int graphd_constraint_or_declare(graphd_request *greq, graphd_constraint *orcon,
                                 char const *name_s, char const *name_e,
                                 graphd_variable_declaration **lhs_vdecl_out,
                                 graphd_variable_declaration **new_vdecl_out) {
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;

  graphd_variable_declaration *arch_vdecl, *indexed_vdecl;
  graphd_assignment *arch_a;
  char *tmp_name;
  char const *tmp_name_e;
  graphd_pattern *pick;
  graphd_constraint *arch;
  size_t need;

  *new_vdecl_out = NULL;

  cl_assert(cl, orcon->con_or_index > 0);

  arch = graphd_constraint_or_prototype_root(orcon);
  cl_assert(cl, arch != NULL);
  cl_assert(cl, arch->con_or_index == 0);

  /*  Transform into a local assignment to a temporary.
   *
   *  	orcon[con_or_index]: $var = ((pattern))
   *  ->
   * 	orcon[0]: $var[con_or_index] = ((pattern))
   *  	orcon[0]: $var = __pick__($var[con_or_index], ...)
   */

  /*  Get, or make, the declaration of the protovariable
   *  in the arch constraint.
   */
  arch_vdecl = graphd_variable_declaration_by_name(arch, name_s, name_e);
  if (arch_vdecl == NULL) {
    arch_vdecl = graphd_variable_declaration_add(cm, cl, arch, name_s, name_e);
    if (arch_vdecl == NULL) return ENOMEM;

    /*  This newly created declaration in the arch
     *  constraint is what the caller is interested in as
     *  a new vdecl (which may require further promotion).
     */
    *new_vdecl_out = arch_vdecl;
  }

  /*  Create a renamed or-specific version of the variable name.
   */
  need = (name_e - name_s) + 42 + 2;
  if ((tmp_name = cm_malloc(greq->greq_req.req_cm, need)) == NULL)
    return ENOMEM;
  snprintf(tmp_name, need, "%.*s [or#%zu]", (int)(name_e - name_s), name_s,
           orcon->con_or_index);
  tmp_name_e = tmp_name + strlen(tmp_name);

  indexed_vdecl =
      graphd_variable_declaration_add(cm, cl, arch, tmp_name, tmp_name_e);
  if (indexed_vdecl == NULL) return ENOMEM;

  *lhs_vdecl_out = indexed_vdecl;

  /*  Share or create an assignment to the protovariable in the
   *  arch constraint.
   */
  arch_a = graphd_assignment_by_declaration(arch, arch_vdecl);
  if (arch_a == NULL) {
    arch_a = graphd_assignment_alloc_declaration(greq, arch, arch_vdecl);
    if (arch_a == NULL) return ENOMEM;
  }

  /*  Pick-ify, if necessary, the prototype assignment.
   */
  if (arch_a->a_result == NULL) {
    arch_a->a_result = graphd_pattern_alloc(greq, NULL, GRAPHD_PATTERN_PICK);
    if (arch_a->a_result == NULL) goto mem;
  }
  if (arch_a->a_result->pat_type != GRAPHD_PATTERN_PICK) {
    graphd_pattern *tmp;

    tmp = graphd_pattern_alloc(greq, NULL, GRAPHD_PATTERN_PICK);
    if (tmp == NULL) goto mem;

    graphd_pattern_append(greq, tmp, arch_a->a_result);
    arch_a->a_result = tmp;
  }

  cl_assert(cl, arch_a->a_result != NULL);
  cl_assert(cl, arch_a->a_result->pat_type == GRAPHD_PATTERN_PICK);

  /*  Append the new $tmp to the pick.
   */
  pick = graphd_pattern_alloc_variable(greq,
                                       /* parent result */ arch_a->a_result,
                                       indexed_vdecl);
  if (pick == NULL) goto mem;
  pick->pat_or_index = orcon->con_or_index;

  /*  tmp_name is now pointed to by the variable; we
   *  mustn't free it.
   *  It will be free()d as part of the request heap.
   */
  return 0;

mem:
  if (tmp_name != NULL) cm_free(cm, tmp_name);

  return ENOMEM;
}

int graphd_constraint_or_compile_declaration(
    graphd_request *greq, graphd_constraint *arch,
    graphd_variable_declaration *old_vdecl,
    graphd_variable_declaration **new_vdecl_out) {
  cl_handle *cl = graphd_request_cl(greq);
  cm_handle *cm = greq->greq_req.req_cm;
  graphd_constraint *con = old_vdecl->vdecl_constraint;

  graphd_variable_declaration *arch_vdecl, *indexed_vdecl;
  graphd_assignment *arch_a;
  char *tmp_name;
  char const *tmp_name_e;
  graphd_pattern *pick;
  size_t need;
  char const *name_s, *name_e;

  *new_vdecl_out = NULL;

  cl_assert(cl, con->con_or_index > 0);
  cl_assert(cl, arch->con_or_index == 0);

  /*  Transform into a local assignment to a temporary.
   *
   *  	con[con_or_index]: $var = ((pattern))
   *  ->
   * 	con[0]: $var[con_or_index] = ((pattern))
   *  	con[0]: $var = __pick__($var[con_or_index], ...)
   */

  /*  Get the name of the protovariable.
   */
  graphd_variable_declaration_name(old_vdecl, &name_s, &name_e);

  /*  Get, or make, the declaration of the protovariable
   *  in the arch constraint.
   */
  arch_vdecl = graphd_variable_declaration_by_name(arch, name_s, name_e);
  if (arch_vdecl == NULL) {
    arch_vdecl = graphd_variable_declaration_add(cm, cl, arch, name_s, name_e);
    if (arch_vdecl == NULL) return ENOMEM;

    /*  This newly created declaration in the arch
     *  constraint is what the caller is interested in as
     *  a new vdecl (which may require further promotion).
     */
    *new_vdecl_out = arch_vdecl;
  }

  /*  Create a renamed or-specific version of the variable name.
   */
  need = (name_e - name_s) + 42 + 2;
  if ((tmp_name = cm_malloc(greq->greq_req.req_cm, need)) == NULL)
    return ENOMEM;
  snprintf(tmp_name, need, "%.*s [or#%zu]", (int)(name_e - name_s), name_s,
           con->con_or_index);
  tmp_name_e = tmp_name + strlen(tmp_name);

  indexed_vdecl =
      graphd_variable_declaration_add(cm, cl, arch, tmp_name, tmp_name_e);
  if (indexed_vdecl == NULL) return ENOMEM;

  /* Rename the variable to the new temporary in the "or" and below.
   */
  graphd_variable_rename(greq, con, old_vdecl, indexed_vdecl);

  /*  Share or create an assignment to the protovariable in the
   *  arch constraint.
   */
  arch_a = graphd_assignment_by_declaration(arch, arch_vdecl);
  if (arch_a == NULL) {
    arch_a = graphd_assignment_alloc_declaration(greq, arch, arch_vdecl);
    if (arch_a == NULL) return ENOMEM;
  }

  /*  Pick-ify, if necessary, the prototype assignment.
   */
  if (arch_a->a_result == NULL) {
    arch_a->a_result = graphd_pattern_alloc(greq, NULL, GRAPHD_PATTERN_PICK);
    if (arch_a->a_result == NULL) goto mem;
  }
  if (arch_a->a_result->pat_type != GRAPHD_PATTERN_PICK) {
    graphd_pattern *tmp;

    tmp = graphd_pattern_alloc(greq, NULL, GRAPHD_PATTERN_PICK);
    if (tmp == NULL) goto mem;

    graphd_pattern_append(greq, tmp, arch_a->a_result);
    arch_a->a_result = tmp;
  }

  cl_assert(cl, arch_a->a_result != NULL);
  cl_assert(cl, arch_a->a_result->pat_type == GRAPHD_PATTERN_PICK);

  /*  Append the new $tmp to the pick.
   */
  pick = graphd_pattern_alloc_variable(greq,
                                       /* parent result */ arch_a->a_result,
                                       indexed_vdecl);
  if (pick == NULL) goto mem;
  pick->pat_or_index = con->con_or_index;

  /*  tmp_name is now pointed to by the variable; we
   *  mustn't free it.
   *  It will be free()d as part of the request heap.
   */
  return 0;

mem:
  if (tmp_name != NULL) cm_free(cm, tmp_name);

  return ENOMEM;
}

void graphd_constraint_or_move_assignments(graphd_request *greq,
                                           graphd_constraint *arch,
                                           graphd_constraint *con) {
  (void)greq;
  /*  If this is inside an "or" branch ...
   */
  if (con->con_or == NULL) return;

  /*  Move the assignments to the now or-indexed variables
   *  into the arch constraint.
   */
  *arch->con_assignment_tail = con->con_assignment_head;
  if (*arch->con_assignment_tail != NULL)
    arch->con_assignment_tail = con->con_assignment_tail;
  arch->con_assignment_n += con->con_assignment_n;

  con->con_assignment_n = 0;
  con->con_assignment_head = NULL;
  con->con_assignment_tail = &con->con_assignment_head;
}

int graphd_constraint_or_move_declarations(graphd_request *greq,
                                           graphd_constraint *arch,
                                           graphd_constraint *con) {
  graphd_variable_declaration *vdecl, *arch_vdecl;

  (void)greq;
  /*  If this is inside an "or" branch ...
   */
  if (con->con_or == NULL) return 0;

  /*  Move any declarations that weren't indexed as part
   *  of an OR-dependent assignment into the arch constraint.
   */
  vdecl = NULL;
  while ((vdecl = cm_hnext(&con->con_variable_declaration,
                           graphd_variable_declaration, vdecl)) != NULL) {
    char const *name_s, *name_e;

    /* Get the variable's name.
     */
    graphd_variable_declaration_name(vdecl, &name_s, &name_e);

    /*  Get, or make, the declaration of the protovariable
     *  in the arch constraint.
     */
    arch_vdecl = graphd_variable_declaration_by_name(arch, name_s, name_e);
    if (arch_vdecl == NULL) {
      arch_vdecl = graphd_variable_declaration_add(
          greq->greq_req.req_cm, graphd_request_cl(greq), arch, name_s, name_e);
      if (arch_vdecl == NULL) return ENOMEM;
    }
    graphd_variable_rename(greq, con, vdecl, arch_vdecl);
  }
  return 0;
}
