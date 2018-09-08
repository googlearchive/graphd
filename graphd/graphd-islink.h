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
#ifndef GRAPHD_ISLINK_H

#include "graphd/graphd.h"

#define GRAPHD_ISLINK_INTERESTING_MIN 100
#define GRAPHD_ISLINK_INTERESTING_MAX (1000 * 1000)

typedef enum graphd_islink_side_index {
  GRAPHD_ISLINK_RIGHT = 0,
  GRAPHD_ISLINK_LEFT = 1,
  GRAPHD_ISLINK_SIDE_N = 2

} graphd_islink_side_index;

typedef struct graphd_islink_side_count {
  unsigned long sc_count;
  graph_idset *sc_idset;

} graphd_islink_side_count;

#define GRAPHD_ISLINK_GROUPID_NONE ((unsigned long)-1)
#define GRAPHD_ISLINK_INTERSECT(a, b)                  \
  ((a) > (b) ? (((unsigned long long)(b) << 24) | (a)) \
             : (((unsigned long long)(a) << 24) | (b)))

#define GRAPHD_ISLINK_CLOCK (1024)

#define GRAPHD_ISLINK_ID_HIGH (1024ull * 1024ull * 100ull)
#define GRAPHD_ISLINK_ID_LOW (1024ull * 1024ull * 60ull)

#define GRAPHD_ISLINK_SLOT_HIGH (1024 * 4)
#define GRAPHD_ISLINK_SLOT_LOW (1024 * 2)

#define GRAPHD_ISLINK_VALUE_MAX (1024ull * 1024ull)

typedef struct graphd_islink_job graphd_islink_job;

/*  ISLINK job callback
 *
 *  Job callbacks create iterators and walk all instances
 *  of some type or relationship, creating sets as needed.
 *
 * @param	_job		execution context
 * @param  	_g		graphd handle
 * @param 	_budget_inout	execute until this drops to 0.
 *
 * @return 0 on success, including on termination or
 *	when running out of budget.
 * @return a nonzero error code on unexpected error *only*
 */
typedef int graphd_islink_job_callback(graphd_islink_job *_job,
                                       graphd_handle *_g,
                                       pdb_budget *_budget_inout);

/* One side (GRAPHD_ISLINK_LEFT or -RIGHT) of a type
 * relationship.  This is where the system keeps its temporary
 * data while deciding whether a type's endpoints are worth
 * recording.
 */
typedef struct graphd_islink_side {
  /*  Whenever an ID shows up more than once on this side
   *  of a relationship, an entry in side_count is created
   *  for it, and used to count the occurrences of that
   *  particular ID in that particular side.  If more than
   *  50 instances of it occur, the endpoint grows an
   *  idset; all *other*-side-IDs are saved in that particular
   *  idset, and later become a group.
   */
  cm_hashtable side_count;

  /*  Every ID that occurs on this side of the type
   *  gets an entry in this idset.
   */
  graph_idset *side_idset;

  /*  If "vast" is set, there are too many IDs on this
   *  side of the relationship to keep track of them
   *  all.
   */
  unsigned int side_vast : 1;

  /*  If a side has been converted into a group, the
   *  side_idset is a link-counted pointer to the group's
   *  idset.  Before that, the side_idset resides only
   *  in the type.
   */
  unsigned int side_group : 1;

} graphd_islink_side;

/*  A statistics structure about the type.
 */
typedef struct graphd_islink_type {
  unsigned long long tp_n;
  graphd_islink_side tp_side[GRAPHD_ISLINK_SIDE_N];
  unsigned int tp_initialized : 1;

} graphd_islink_type;

/*  An asynchronous process that analyses all instances of a particular
 *  type, or of a type/endpoint pair.  (What the job is for is recorded
 *  in the job's specification, i.e. its hashtable key.)
 */
struct graphd_islink_job {
  graphd_islink_job *job_next;
  graphd_islink_job_callback *job_run;

  /*  The number of primitives that have so far matched the job's
   *  specification.
   */
  unsigned long long job_n;

  /*  The budget expended on this job so far, for documentation
   *  only.
   */
  unsigned long long job_budget;

  /*  The first unread entry in the (ascending) ordered list of
   *  IDs corresponding to the job specification.  When the job
   *  runs, it starts by reading the first ID at job_low or above.
   */
  pdb_id job_low;

  /*  For a specific job (started by an ISLINK command), the list
   *  of IDs that it found.
   */
  graph_idset *job_idset;
};

/*  A group.  A group is a precomputed ordered set of IDs, typically
 *  endpoints of a relationship, that is usually saved forever, and
 *  whose intersections with other IDs are precomputed.
 */
typedef struct graphd_islink_group {
  graph_idset *group_idset;
  unsigned long group_id;

} graphd_islink_group;

/*  Toplevel management structure for all things islink.
 */
struct graphd_islink_handle {
  /*  Hashtable graphd_islink_key -> graphd_islink_group
   */
  cm_hashtable ih_group;

  /*  Information about typeguids and the sets at
   *  their left and right sides, if they're large enough
   *  to be interesting.
   *
   *  Hashtable type_id -> graphd_islink_type
   */
  cm_hashtable ih_type;

  /*  Hashtable graphd_islink_key -> graphd_islink_job.
   */
  cm_hashtable ih_job;

  /*  Hashtable unsigned long long -> graphd_islink_intersect.
   */
  cm_hashtable ih_intersect;

  /*  Are we getting primitive add notifications?
   */
  unsigned int ih_subscribed : 1;
};

typedef enum graphd_islink_intersect_type {
  /* The intersection is the ids in ii_include.ii_idset.
   */
  GRAPHD_ISLINK_INTERSECT_SET,

  /*  The intersection contains the number of ids
   *  listed in ii_include.ii_count.
   */
  GRAPHD_ISLINK_INTERSECT_COUNTED,

} graphd_islink_intersect_type;

/*  A NULL included set is everything.
 *  A 0 included count is nothing.
 */
typedef struct {
  union {
    graph_idset *ii_idset;
    unsigned long long ii_count;
  } ii_include;
  unsigned int ii_include_set : 1;

  union {
    graph_idset *ii_idset;
    unsigned long long ii_count;
  } ii_exclude;
  unsigned int ii_exclude_set : 1;

} graphd_islink_intersect;

/* graphd-islink.c */

int graphd_islink_subscribe(graphd_handle *);
void graphd_islink_panic(graphd_handle *);

/* graphd-islink-group.c */

graphd_islink_group *graphd_islink_group_lookup(graphd_handle *g,
                                                graphd_islink_key const *key);

int graphd_islink_group_create(graphd_handle *g, graphd_islink_key const *key,
                               graph_idset *idset);

graph_idset *graphd_islink_group_idset(graphd_handle *g,
                                       graphd_islink_key const *key);

int graphd_islink_group_check(graphd_handle *g, graphd_islink_key const *key,
                              pdb_id id);

int graphd_islink_group_update(graphd_handle *g, pdb_id result_id,
                               int result_linkage, pdb_id type_id,
                               pdb_id endpoint_id);

int graphd_islink_group_job_make(graphd_handle *g, int result_linkage,
                                 pdb_id type_id, pdb_id endpoint_id);

void graphd_islink_group_finish(graphd_handle *g, graphd_islink_group *group);

/* graphd-islink-intersect.c */

void graphd_islink_intersect_finish(graphd_handle *g,
                                    graphd_islink_intersect *ii);

int graphd_islink_intersect_make(graphd_handle *g, graphd_islink_group *g1,
                                 graphd_islink_group *g2,
                                 graphd_islink_intersect *ii);

int graphd_islink_intersect_add(graphd_handle *g, graphd_islink_intersect *ii,
                                pdb_id id, bool included);

int graphd_islink_intersect_lookup(graphd_handle *g,
                                   graphd_islink_key const *key1,
                                   graphd_islink_key const *key2,
                                   graphd_islink_intersect *ii_out);
/* graphd-islink-job.c */

graphd_islink_job *graphd_islink_job_lookup(graphd_handle *g,
                                            graphd_islink_key const *key);

graphd_islink_job *graphd_islink_job_alloc(graphd_handle *g,
                                           graphd_islink_key const *key);

void graphd_islink_job_finish(graphd_handle *g, graphd_islink_job *job);

void graphd_islink_job_free(graphd_handle *g, graphd_islink_job *job);

int graphd_islink_job_run(graphd_handle *g, graphd_islink_key const *key,
                          pdb_budget *budget_inout);

/* graphd-islink-key.c */

char const *graphd_islink_key_to_string(graphd_islink_key const *_key,
                                        char *_buf, size_t _size);

graphd_islink_key *graphd_islink_key_make(graphd_handle *_g,
                                          int _result_linkage, pdb_id _type_id,
                                          pdb_id _endpoint_id,
                                          graphd_islink_key *_buf);

int graphd_islink_key_endpoint_linkage(graphd_islink_key const *);

int graphd_islink_key_psum(graphd_handle *g, graphd_islink_key const *key,
                           pdb_primitive_summary *psum);

/* graphd-islink-type.c */

pdb_id graphd_islink_type_id(graphd_handle *g, graphd_islink_type const *tp);

void graphd_islink_type_finish(graphd_handle *g, graphd_islink_type *tp);

graphd_islink_type *graphd_islink_type_lookup(graphd_handle *g, pdb_id type_id);

graphd_islink_job *graphd_islink_type_job_lookup(graphd_handle *g,
                                                 pdb_id type_id);

int graphd_islink_type_add_id(graphd_handle *g, pdb_id type_id);

int graphd_islink_type_add_guid(graphd_handle *g, graph_guid const *type_guid);

void graphd_islink_type_finish_all(graphd_handle *);

/* graphd-islink-side.c */

void graphd_islink_side_finish(graphd_handle *_g, graphd_islink_side *_side,
                               int _result_linkage, pdb_id _type_id);

int graphd_islink_side_initialize(graphd_handle *_g, graphd_islink_side *_side);

pdb_id graphd_islink_side_count_id(graphd_islink_side const *,
                                   graphd_islink_side_count const *);

int graphd_islink_side_add(graphd_handle *_g, graphd_islink_side *_side,
                           int _linkage, pdb_id _side_id, pdb_id _type_id,
                           pdb_id _other_id, pdb_id _pr_id);

int graphd_islink_side_complete(graphd_handle *_g, graphd_islink_side *_side,
                                int _result_linkage, pdb_id _type_id);

#endif /* GRAPHD_ISLINK_H */
