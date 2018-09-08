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
#include <string.h>

/**
 * @brief Assign locations to the fields of a sort instruction.
 *
 *  Sort instructions are specified by content, not by location -
 *  clients ask for the "name" or  the "value", not for "the second
 *  field of the result".  This function maps the user-specified
 *  names to their locations in the result and variable assignment
 *  expressions of a constraint.
 *
 *  If a field doesn't occur, a new entry is created in a variable
 *  espression with a null name that holds the sort criteria that
 *  aren't otherwise covered.
 *
 *  Entries are also created in that entry for sample expressions
 *  in the sorted constraint that are not twice nested, yet correspond
 *  to per-constraint values. (For example, a result=value.)
 *  These sample expressions are resampled from the sorted values
 *  in sort order, once the sort is complete.
 *
 *  In all cases, the result instructions of the sort expression
 *  are annotated with result- and field index that selects one
 *  of the returned tuples and the position within the repeated
 *  alternative-expression of the tuple.
 *
 * @param greq	request we're doing this for
 * @param con	constraint to compile
 *
 * @return 0 on success
 * @return a nonzero error code on error
 * @return ENOMEM on allocation failure
 */
int graphd_sort_compile(graphd_request *greq, graphd_constraint *con) {
  cl_handle *cl = graphd_request_cl(greq);
  graphd_pattern *pat, *head;
  char buf[200];

  if (!con->con_sort_valid || (head = (graphd_pattern *)con->con_sort) == NULL)
    return 0;

  cl_enter(cl, CL_LEVEL_VERBOSE, "pat=%s",
           graphd_pattern_dump(head, buf, sizeof buf));

  /*  If the sort instruction is a single element, and
   *  the element's type is not "by GUID", turn the single
   *  element into a list -- we'll append a "by GUID" to
   *  cap everything off.
   *
   * xyz -> (xyz)
   */
  if (head->pat_type != GRAPHD_PATTERN_LIST &&
      head->pat_type != GRAPHD_PATTERN_GUID) {
    head = graphd_pattern_wrap(greq, head);
    if (head == NULL) {
      cl_leave_err(cl, CL_LEVEL_FAIL, ENOMEM, "couldn't wrap pattern");
      return ENOMEM;
    }

    cl_assert(cl, head->pat_type == GRAPHD_PATTERN_LIST);
    cl_assert(cl, head->pat_list_n == 1);
    cl_assert(cl, head->pat_list_head != NULL);

    con->con_sort = head;
    cl_cover(cl);
  }

  /* () -> (GUID)
   */
  if (head != NULL && head->pat_type == GRAPHD_PATTERN_LIST) {
    if (head->pat_list_head == NULL) {
      if (graphd_pattern_alloc(greq, head, GRAPHD_PATTERN_GUID) == NULL) {
        cl_leave_err(cl, CL_LEVEL_FAIL, ENOMEM,
                     "couldn't allocate trailing GUID "
                     "pattern");
        return ENOMEM;
      }
      cl_cover(cl);
    }
    cl_assert(cl, head->pat_list_head != NULL);
    head = head->pat_list_head;
    cl_cover(cl);
  }

  /*  <head> either points to a single sorting criterion
   *  that's GUID; or to a list that may or may not contain
   *  GUID as a criterion.
   */
  cl_assert(cl, head != NULL);
  for (pat = head; pat != NULL; pat = pat->pat_next) {
    if (pat->pat_type == GRAPHD_PATTERN_GUID) {
      /*  GUIDs are unique -- the sort ends here,
       *  no matter what else the user specified.
       *
       *  Don't worry too much about memory management;
       *  these are allocated either in the constraint
       *  or in the request heap, and will be free'ed
       *  automatically when the request ends.
       */
      if (pat->pat_parent != NULL) {
        pat->pat_parent->pat_list_tail = &pat->pat_next;
        cl_cover(cl);
      }
      pat->pat_next = NULL;
      break;
    }

    /*  Conversely, if the sort ends without having compared GUIDs,
     *  throw in a free comparison, so that all sorts are decisive.
     *  Otherwise, cursors for sorted lists run the risk of
     *  cutting off too much or too little.
     */
    if (pat->pat_next == NULL) {
      cl_assert(cl, pat->pat_parent != NULL);
      cl_assert(cl, pat->pat_parent->pat_list_tail == &pat->pat_next);

      if (graphd_pattern_alloc(greq, pat->pat_parent, GRAPHD_PATTERN_GUID) ==
          NULL) {
        cl_leave_err(cl, CL_LEVEL_FAIL, ENOMEM, "graphd_pattern_alloc fails");
        return ENOMEM;
      }

      cl_assert(cl, pat->pat_next != NULL);
      cl_cover(cl);
    }
  }
  cl_leave(cl, CL_LEVEL_VERBOSE, "sort=%s",
           graphd_pattern_dump(con->con_sort, buf, sizeof buf));
  return 0;
}
