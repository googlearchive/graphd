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
#ifndef __GDP_OUTPUT_H__
#define __GDP_OUTPUT_H__

#include "graphd/graphd.h"
#include "libcl/cl.h"
#include "libcm/cm.h"
#include "libgraph/graph.h"

/**
 * A generic request object.
 */
typedef void gdp_request_t;

//
// Abstract syntax trees.
//
// These trees are specific to a particular request type.
//

/**
 * Constraint list, for "read" and "write" requests.
 */
typedef void gdp_conlist_t;

/**
 * Record list, for "restore" requests.
 */
typedef void gdp_recordlist_t;

/**
 * Subject list, for "status" requests.
 */
typedef void gdp_statlist_t;

/**
 * Property list, for "set" requests.
 */
typedef void gdp_proplist_t;

//
// Collections of values.
//

/**
 * A collection of request modifiers.
 */
typedef void gdp_modlist_t;

/**
 * A collection of string values.
 */
typedef void gdp_strset_t;

/**
 * A collection of GUID values.
 */
typedef void gdp_guidset_t;

/**
 * A value for SMP command types
 */
typedef void gdp_smpcmd_t;

//
// Special objects.
//

/**
 * A pattern.
 */
typedef void gdp_pattern_t;

/**
 * A property, used for "set" requests.
 */
typedef void gdp_property_t;

/**
 * A record structure, used for "restore" requests.
 */
struct gdp_record {
  graph_guid r_v5_guid;              ///< Node GUID
  graph_guid r_v5_left;              ///< Left node
  graph_guid r_v5_right;             ///< Right node
  graph_guid r_v5_prev;              ///< Previous version
  graph_guid r_v5_scope;             ///< Scope node
  graph_guid r_v5_typeguid;          ///< Type node
  gdp_token r_v1_type;               ///< Type name (version 1)
  gdp_token r_v5_name;               ///< Name
  gdp_token r_v5_value;              ///< Value
  graph_datatype r_v5_datatype;      ///< Data type
  graph_timestamp_t r_v5_timestamp;  ///< Timestamp
  bool r_v5_archival;                ///< Archival
  bool r_v5_live;                    ///< Live
  bool r_v6_txstart;                 ///< Txstart
};

typedef struct gdp_record gdp_record;  ///< See #gdp_record structure

/**
 * The meta constraints.
 */
typedef enum {
  GDP_META_UNK = 0,
  GDP_META_TO = 2,
  GDP_META_FROM = 4,
} gdp_meta;

/**
 * GUID constraint kind.
 *
 * Used by the gdp_ast_ops::conlist_add_guid() callback in #gdp_ast_ops.
 */
typedef enum {
  GDP_GUIDCON_THIS = 0,  ///< The GUID of the current node
  GDP_GUIDCON_NEXT = 1,  ///< The GUID of the next version
  GDP_GUIDCON_PREV = 2,  ///< The GUID of the previous version
} gdp_guidcon_kind;

/**
 * Generation constraint kind.
 *
 * Used by the gdp_ast_ops::conlist_add_gen() callback in #gdp_ast_ops.
 */
typedef enum {
  GDP_GENCON_NEWEST = 0,  ///< For the "newest" constraint
  GDP_GENCON_OLDEST = 1,  ///< For the "oldest" constraint
} gdp_gencon_kind;

/**
 * Abstract syntax tree operations.
 */
struct gdp_ast_ops {
  // ===================================================================
  // REQUESTS
  // ===================================================================

  /**
   * Initialize an empty request.
   *
   * If non-NULL, this is called before request_new(), and
   * before parsing any request modifiers or constraints.
   *
   * The new request can be stored in the output structure.
   *
   * @param out
   *	Output specs.
   * @param kind
   *	Request kind.
   * @return
   *	Zero on success, otherwise @c ENOMEM.
   */
  int (*request_initialize)(gdp_output* out, graphd_command cmd);

  /**
   * Create a "read", "write", or "iterate" request.
   *
   * The new request is stored in the output structure.
   *
   * @param out
   *	Output specs.
   * @param kind
   *	Request kind.
   * @param modlist
   *	List of request modifiers.
   * @param conlist
   *	Constraint list.
   * @return
   *	Zero on success, otherwise @c ENOMEM.
   */
  int (*request_new)(gdp_output* out, graphd_command cmd,
                     gdp_modlist_t* modlist, gdp_conlist_t* conlist);

  /**
   * Create a "dump" request.
   */
  int (*request_new_dump)(gdp_output* out, gdp_modlist_t* modlist,
                          unsigned long long start, unsigned long long end,
                          unsigned long long pagesize);

  /**
   * Create a parse error request.
   */
  void (*request_new_error)(gdp_output* out, gdp_modlist_t* modlist, int err,
                            char const* msg);

  /**
   * Create a "replica" request.
   */
  int (*request_new_replica)(gdp_output* out, gdp_modlist_t* modlist,
                             unsigned long long start_id,
                             unsigned long long version, bool check_master);

  /**
   * Create a "replica-write" request.
   */
  int (*request_new_replica_write)(gdp_output* out, gdp_modlist_t* modlist,
                                   gdp_recordlist_t* records, size_t n,
                                   unsigned long long start,
                                   unsigned long long end);

  /**
   * Create an "error" or "ok" request.
   */
  int (*request_new_response)(gdp_output* out, gdp_modlist_t* modlist, bool ok);

  /**
   * Create a @e restore request.
   */
  int (*request_new_restore)(gdp_output* out, gdp_modlist_t* modlist,
                             gdp_recordlist_t* records, size_t n,
                             unsigned char version, unsigned long long start,
                             unsigned long long end);

  /**
   * Create a "rok" request.
   */
  int (*request_new_rok)(gdp_output* out, gdp_modlist_t* modlist,
                         unsigned int version, gdp_token const* address);

  /**
   * Create a "set" request.
   */
  int (*request_new_set)(gdp_output* out, gdp_modlist_t* modlist,
                         gdp_proplist_t* props);

  /**
   * Create a "smp" request.
   */
  int (*request_new_smp)(gdp_output* out, gdp_modlist_t* modlist,
                         gdp_smpcmd_t* smpcmd);

  /**
   * Create a "status" request.
   */
  int (*request_new_status)(gdp_output* out, gdp_modlist_t* modlist,
                            gdp_statlist_t* statlist);

  /**
   * Create a "verify" request.
   */
  int (*request_new_verify)(gdp_output* out, gdp_modlist_t* modlist,
                            graph_guid const* low, graph_guid const* high,
                            unsigned long long pagesize);

  // ===================================================================
  // REQUEST MODIFIERS
  // ===================================================================

  /**
   * Allocate an empty list of request modifiers.
   */
  int (*modlist_new)(gdp_output* out, gdp_modlist_t** modlist);

  /**
   * Create an "asof" request modifier.
   *
   * @param out
   *	Output specs.
   * @param modlist
   *	List of request modifiers.
   * @param value
   *	The value of the "asof" modifier.
   * @return
   *	Zero on success, otherwise an error code, in particular @c
   *	EINVAL if the given value is invalid.
   */
  int (*modlist_add_asof)(gdp_output* out, gdp_modlist_t* modlist,
                          gdp_token const* tok);

  /**
   * Create a "cost" request modifier.
   *
   * @param out
   *	Output specs.
   * @param op
   *	Operator (= or ~=).
   * @param value
   *	The value of the "cost" modifier.
   * @return
   *	Zero on success, otherwise an error code, in particular @c
   *	EINVAL if the given value is invalid.
   */
  int (*modlist_add_cost)(gdp_output* out, gdp_modlist_t* modlist,
                          graphd_operator op, gdp_token const* tok);

  /**
   * Create a "dateline" request modifier.
   */
  int (*modlist_add_dateline)(gdp_output* out, gdp_modlist_t* modlist,
                              gdp_token const* tok);

  /**
   * Create an "id" request modifier.
   */
  int (*modlist_add_id)(gdp_output* out, gdp_modlist_t* modlist,
                        gdp_token const* value);

  /**
   * Create a "heatmap" request modifier.
   */
  int (*modlist_add_heatmap)(gdp_output* out, gdp_modlist_t* modlist,
                             gdp_token const* value);

  /**
   * Create a "loglevel" request modifier.
   *
   * This function may be invoked multiple times to enable multiple log
   * levels.
   *
   * @return
   *	Zero on success, otherwise @c EINVAL if the given log level
   *	does not exist.
   */
  int (*modlist_add_loglevel)(gdp_output* out, gdp_modlist_t* modlist,
                              gdp_token const* tok);

  /**
   * Create a "timeout" request modifier.
   */
  int (*modlist_add_timeout)(gdp_output* out, gdp_modlist_t* modlist,
                             unsigned long long timeout);

  // ===================================================================
  // CONSTRAINT LIST
  // ===================================================================

  /**
   * Create an empty constraint list.
   *
   * @param out
   *	The output specs.
   * @param [out] conlist
   *	The new constraint list.
   * @return
   *	Zero on success, otherwise @c ENOMEM.
   */
  int (*conlist_new)(gdp_output* out, gdp_conlist_t** conlist);

  /**
   * Add a "comparator" constraint.
   *
   * @return
   *	Zero on success, otherwise @c EINVAL if the given comparator
   *	does not exist, or @c EBUSY if a comparator has already been
   *	specified for the given constraint.
   */
  int (*conlist_add_comparator)(gdp_output* out, gdp_conlist_t* where,
                                gdp_token const* name);

  int (*conlist_new_sortcomparator)(gdp_output* out, gdp_conlist_t* wehre);

  int (*conlist_add_sortcomparator)(gdp_output* out, gdp_conlist_t* where,
                                    gdp_token* comp);

  int (*conlist_add_valuecomparator)(gdp_output* out, gdp_conlist_t* where,
                                     gdp_token const* name);

  /**
   * Add a "count" constraint.
   */
  int (*conlist_add_count)(gdp_output* out, gdp_conlist_t* where,
                           graphd_operator op, unsigned long long value);

  /**
   * Create a "cursor" constraint.
   *
   * @return
   *	Zero on success, otherwise @c EINVAL if the cursor is invalid.
   */
  int (*conlist_add_cursor)(gdp_output* out, gdp_conlist_t* where,
                            gdp_token const* value);

  /**
   * Add a "dateline" constraint.
   *
   * @return
   *	Zero on success, otherwise @c EINVAL if @a value is invalid.
   */
  int (*conlist_add_dateline)(gdp_output* out, gdp_conlist_t* where,
                              graphd_operator op, gdp_token const* value);

  /**
   * Add "false" constraint.
   */
  int (*conlist_add_false)(gdp_output* out, gdp_conlist_t* where);

  /**
   * Add a "live" or "archival" flag constraint.
   *
   * @param out
   *	Output specs.
   * @param where
   *	Parent constraint list.
   * @param name
   *	The constraint name: "live" or "archival".
   * @param flag
   *	Flag value.
   * @return
   *	Zero on success, otherwise @c GDP_ERR_SEMANTICS if the constraint has
   *	already been specified.
   */
  int (*conlist_add_flag)(gdp_output* out, gdp_conlist_t* where,
                          gdp_token const* name, graphd_flag_constraint flag);

  /**
   * Add a "newest" or "oldest" constraint.
   *
   * @param out
   *	Output specs.
   * @param where
   *	Parent constraint list.
   * @param kind
   *	#GDP_GENCON_NEWEST for the "newest" constraint, or
   *	#GDP_GENCON_OLDEST for the "oldest" constraint.
   * @param op
   *	Operator (<, <=, =, >=, or >).
   * @param value
   *	Constraint value.
   */
  int (*conlist_add_gen)(gdp_output* out, gdp_conlist_t* where,
                         gdp_gencon_kind kind, graphd_operator op,
                         unsigned long long value);

  /**
   * Create a GUID constraint.
   *
   * @param out
   *	Outputs specs.
   * @param where
   *	The list receiving the constraint.
   * @param kind
   *	Specifies one the following: guid, next, or previous.
   * @param op
   *	One of the following: @c GRAPHD_OP_EQ, @c GRAPHD_OP_NE, or @c
   *	GRAPHD_OP_MATCH.
   * @param guidset
   *	The constraint values.
   *
   * @see guidset_new()
   *	to create a GUID set.
   * @see guidset_add()
   *	to add entries to a GUID set.
   */
  int (*conlist_add_guid)(gdp_output* out, gdp_conlist_t* where,
                          gdp_guidcon_kind kind, graphd_operator op,
                          gdp_guidset_t* guidset);

  /**
   * Add a "key" constraint.
   *
   * @return
   *	Zero on success, otherwise @c EINVAL if @a pattern is invalid.
   */
  int (*conlist_add_key)(gdp_output* out, gdp_conlist_t* where,
                         gdp_pattern_t* pattern);

  /**
   * Create a linkage constraint.
   *
   * @param out
   *	Outputs specs.
   * @param where
   *	The list receiving the constraint.
   * @param linkage
   *	A @c PBD_LINKAGE_xxxx value, specifying whether the constraint
   *	applies to the left, right, scope, or type linkage.
   * @param op
   *	Operator (=, !=, ~=).
   * @param guidset
   *	The GUID set of the constraint.
   *
   * @see guidset_new()
   *	to create a GUID set.
   * @see guidset_add()
   *	to add entries to a GUID set.
   */
  int (*conlist_add_linkage)(gdp_output* out, gdp_conlist_t* where,
                             unsigned int linkage, graphd_operator op,
                             gdp_guidset_t* guidset);

  /**
   * Add an "or" constraint.
   */
  int (*conlist_add_or)(gdp_output* out, gdp_conlist_t* where,
                        gdp_conlist_t* rhs, bool short_circuit);

  /**
   * Add a "pagesize" constraint.
   */
  int (*conlist_add_pagesize)(gdp_output* out, gdp_conlist_t* where,
                              size_t size);

  /**
   * Add a "countlimit" constraint.
   */
  int (*conlist_add_countlimit)(gdp_output* out, gdp_conlist_t* where,
                                size_t size);

  /**
   * Add a "resultpagesize" constraint.
   */
  int (*conlist_add_resultpagesize)(gdp_output* out, gdp_conlist_t* where,
                                    size_t size);

  /**
   * Add a "result" constraint.
   */
  int (*conlist_add_result)(gdp_output* out, gdp_conlist_t* where,
                            gdp_pattern_t* pat);

  /**
   * Add a "sequence" constraint.
   */
  int (*conlist_add_sequence)(gdp_output* out, gdp_conlist_t* where,
                              gdp_conlist_t* sub);

  /**
   * Add a "sort" constraint.
   */
  int (*conlist_add_sort)(gdp_output* out, gdp_conlist_t* where,
                          gdp_pattern_t* pat);

  /**
   * Add a "start" constraint.
   */
  int (*conlist_add_start)(gdp_output* out, gdp_conlist_t* where, size_t size);

  /**
   * Add a string constraint.
   *
   * @param out
   *	The output specs.
   * @param where
   *	The list where to insert the new constraint.
   * @param name
   *	The token containing the name of the string constraint
   *	("value", "type", or "name").
   * @param op
   *	The constraint operator.
   * @param values
   *	The constraint values.
   * @param allow_multi
   *	Allow multiple values for the constraint.
   * @return
   *	Zero on success, otherwise @c GDP_ERR_SEMANTICS if the constraint has
   *	already been defined but multiple values are not allowed (i.e.
   *	@a allow_multi is @c false).
   */
  int (*conlist_add_string)(gdp_output* out, gdp_conlist_t* where,
                            gdp_token* name, graphd_operator op,
                            gdp_strset_t* values, bool allow_multi);

  /**
   * Add a sub-list of constraints.
   */
  int (*conlist_add_sublist)(gdp_output* out, gdp_conlist_t* where,
                             gdp_conlist_t* sublist);

  /**
   * Add a "timestamp" constraint.
   */
  int (*conlist_add_timestamp)(gdp_output* out, gdp_conlist_t* where,
                               graphd_operator op, graph_timestamp_t ts);

  /**
   * Add a "unique" constraint.
   *
   * @return
   *	Zero on success, otherwise @c EINVAL if @a pattern is not an
   *	appropriate value for a @c "unique" constraint.
   */
  int (*conlist_add_unique)(gdp_output* out, gdp_conlist_t* where,
                            gdp_pattern_t* pattern);

  /**
   * Add a "valuetype" constraint.
   *
   * @return
   *	Zero on success, otherwise @c GDP_ERR_SEMANTICS if a "valuetype"
   *	constraint has already been specified.
   */
  int (*conlist_add_valuetype)(gdp_output* out, gdp_conlist_t* where,
                               graph_datatype type);

  /**
   * Add a variable constraint.
   *
   * A variable constraint has the form "$var=pattern...".
   */
  int (*conlist_add_variable)(gdp_output* out, gdp_conlist_t* where,
                              gdp_token const* var, gdp_pattern_t* pat);

  /**
   * Check whether a meta attribute exists.
   */
  bool (*conlist_has_meta)(gdp_output* out, gdp_conlist_t* list);

  /**
   * Return value of the meta attribute.
   */
  gdp_meta (*conlist_get_meta)(gdp_output* out, gdp_conlist_t const* list);

  /**
   * Set meta attribute.
   */
  int (*conlist_set_meta)(gdp_output* out, gdp_conlist_t* list, gdp_meta meta);

  /**
   * Check whether a linkage exists.
   */
  bool (*conlist_has_linkage)(gdp_output* out, gdp_conlist_t* list);

  /**
   * Set linkage.
   */
  int (*conlist_set_linkage)(gdp_output* out, gdp_conlist_t* list,
                             unsigned int linkage);

  // ===================================================================
  // RECORDS
  // ===================================================================

  /**
   * Allocate a collection of records.
   *
   * @param out
   *	The output specs.
   * @param n
   *	Number of records to be allocated.
   * @param [out] records
   *	The new record set.
   * @return
   *	Zero on success, otherwise @c ENOMEM.
   */
  int (*recordlist_new)(gdp_output* out, size_t n, gdp_recordlist_t** records);

  /**
   * Set record value.
   *
   * @param out
   *	The output specs.
   * @param version
   *	Record version number
   * @param records
   *	Record set.
   * @param index
   *	Which record.
   * @param value
   *	Record value.
   * @return
   *	Zero on success, otherwise a non-zero error code.
   *
   * @see recordlist_new()
   *	to create a record set.
   * @see request_new_restore()
   *	to create a restore request.
   */
  int (*recordlist_set)(gdp_output* out, unsigned int version,
                        gdp_recordlist_t* records, unsigned int index,
                        gdp_record const* value);

  // ===================================================================
  // PROPERTIES
  // ===================================================================

  /**
   * Create an empty property list.
   */
  int (*proplist_new)(gdp_output* out, gdp_proplist_t** props);

  int (*proplist_add)(gdp_output* out, gdp_proplist_t* props,
                      const char* name_s, const char* name_e,
                      const char* value_s, const char* value_e);

  // ===================================================================
  // STATUS
  // ===================================================================

  /**
   * Create a list of "status" subjects.
   */
  int (*statlist_new)(gdp_output* out, gdp_statlist_t** statlist);

  /**
   * Add status subject.
   */
  int (*statlist_add)(gdp_output* out, gdp_statlist_t* statlist,
                      gdp_token const* tok, unsigned long long num);

  // ===================================================================
  // SMP
  // ===================================================================

  /**
   * Create the smp command structure.
   */
  int (*smpcmd_new)(gdp_output* out, gdp_smpcmd_t** smpcmd,
                    unsigned long long** smppid);

  /**
   * Set the smp command
   */
  int (*smpcmd_set)(gdp_output* out, gdp_smpcmd_t* smpcmd,
                    gdp_token const* tok);

  // ===================================================================
  // GUID SETS
  // ===================================================================

  /**
   * Create an empty set of GUID values.
   *
   * @param out
   *	The output specs.
   * @param [out] set
   *	The new set.
   * @return
   *	Zero on success, otherwise @c ENOMEM.
   *
   * @see guidset_add()
   *	to add entries to the set.
   * @see addlncon()
   *	to create a linkage constraint.
   */
  int (*guidset_new)(gdp_output* out, gdp_guidset_t** set);

  /**
   * Add a value to a GUID set.
   *
   * @param out
   *	The output specs.
   * @param set
   *	The GUID set.
   * @param guid
   *	The value to be added.
   * @return
   *	Zero on success, otherwise an error code.
   *
   * @see guidset_new()
   *	to create a GUID set.
   * @see addlncon()
   *	to create a linkage constraint.
   */
  int (*guidset_add)(gdp_output* out, gdp_guidset_t* set,
                     graph_guid const* guid);

  // ===================================================================
  // STRING SET
  // ===================================================================

  /**
   * Create an empty list of strings.
   *
   * @param out
   *	The output specs.
   * @param [out] values
   *	A new list of string values.
   * @return
   *	Zero on success, otherwise @c ENOMEM.
   */
  int (*strset_new)(gdp_output* out, gdp_strset_t** values);

  /**
   * Add string to set.
   *
   * @param out
   *	The output specs.
   * @param values
   *	The set where to add the new string value.
   * @param tok
   *	A token, whose image string will be added to the list. Valid
   *	token kinds are #TOK_STR and #TOK_NULL.
   * @return
   *	Zero on success, otherwise @c ENOMEM.
   */
  int (*strset_add)(gdp_output* out, gdp_strset_t* values, gdp_token* tok);

  // ===================================================================
  // PATTERN
  // ===================================================================

  /**
   * Create a pattern.
   *
   * @param out
   *	The output specs.
   * @param scope
   *	A constraint list defining the scope in which a variable is
   *	declared. This is meaningful only when @a kind is @c
   *	GRAPHD_PATTERN_VARIABLE. May be @c NULL in all other cases.
   * @param tok
   *	The token associated with the new pattern. This is meaningful
   *	only when @a kind is @c GRAPHD_PATTERN_VARIABLE or @c
   *	GRAPHD_PATTERN_LITERAL. May be @c NULL in all other cases.
   * @param kind
   *	One of the @c GRAPHD_PATTERN_xxx constants defining the pattern
   *	kind.
   * @param forward
   *	Sort direction, meaningful only for "sort" constraints.
   * @param parent_pat
   *	The parent pattern, or @c NULL if none.
   * @param [out]
   *	The new pattern.
   * @return
   *	Zero on success, otherwise @c ENOMEM.
   *
   * @see conlist_add_result()
   *	to add the newly created pattern to a constraint list.
   */
  int (*pattern_new)(gdp_output* out, gdp_conlist_t* scope,
                     gdp_token const* tok, graphd_pattern_type kind,
                     bool forward, gdp_pattern_t* parent, gdp_pattern_t** pat);
};

typedef struct gdp_ast_ops gdp_ast_ops;

/**
 * Output of the gdp_parse() function.
 */
struct gdp_output {
  void* out_private;    ///< Implementation-specific data
  cm_handle* out_cm;    ///< Heap
  cl_handle* out_cl;    ///< Log
  gdp_ast_ops out_ops;  ///< Abstract syntax tree operations
};

#endif
