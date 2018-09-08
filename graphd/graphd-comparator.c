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

#include <stdio.h>
#include <ctype.h>
#include <errno.h>

#define TOLOWER(a) (isascii(a) ? tolower(a) : (a))
#define ISALNUM(a) (isascii(a) ? isalnum(a) : false)

const char* graphd_value_lo = "";

const char* graphd_value_hi = NULL;

static graphd_comparator const* const graphd_comparators[] = {
    graphd_comparator_unspecified,
    graphd_comparator_default,
    graphd_comparator_octet,
    graphd_comparator_case,
    graphd_comparator_number,
    graphd_comparator_datetime,
    NULL};

static bool graphd_comparator_match_alias(graphd_comparator const* cmp,
                                          char const* s, char const* e) {
  char const* const* al;

  /*  It matches the name ..
   */
  if (TOLOWER(*s) == TOLOWER(cmp->cmp_name[0]) &&
      strncasecmp(s, cmp->cmp_name, (size_t)(e - s)) == 0 &&
      cmp->cmp_name[e - s] == '\0')
    return true;

  /*  ... or, if there is an alias list, any one of the aliases.
   */
  if (cmp->cmp_alias == NULL) return false;

  for (al = cmp->cmp_alias; *al != NULL; al++) {
    if (TOLOWER(*s) == TOLOWER(**al) &&
        strncasecmp(s, *al, (size_t)(e - s)) == 0 && (*al)[e - s] == '\0')

      return true;
  }
  return false;
}

/**
 * @brief Return the best comparator match for a name.
 *
 *  The names have the syntax
 *
 *	[locale-prefix ";"] name
 *
 *  Locale prefixes are optional.   Iterators who have
 *  a locale prefix only match names that include that
 *  domain prefix or more.
 *
 * @param s	NULL or first byte of the name
 * @param e	NULL or pointer just past the end of the name
 *
 * @return NULL if there is no comparator matching the name at all;
 *	otherwise a pointer to a comparator structure.
 */
graphd_comparator const* graphd_comparator_from_string(char const* s,
                                                       char const* e) {
  char const *locale_s = s, *locale_e;
  char const *name_s, *name_e = e;
  char const* prefix;
  graphd_comparator const* const* cmp;

  /*
   * XXX shouldn't this be graphd_comparator_unspecified?
   */
  if (s == NULL || e == NULL) return graphd_comparator_default;

  if ((prefix = memchr(s, ';', e - s)) == NULL)
    locale_e = name_s = locale_s;
  else {
    locale_e = prefix;
    name_s = prefix + 1;
  }

  for (cmp = graphd_comparators; *cmp != NULL; cmp++) {
    /*  The name matches completely ....
     */
    if (graphd_comparator_match_alias(*cmp, name_s, name_e)

        /*  ... and if the iterator has a prefix, the user's
         *  locale prefix contains the iterator's.
         */
        &&
        (*(*cmp)->cmp_locale == '\0' ||
         (locale_s < locale_e &&
          TOLOWER(*locale_s) == TOLOWER((*cmp)->cmp_locale[0]) &&
          strncasecmp(locale_s, (*cmp)->cmp_locale, locale_e - locale_s) == 0 &&
          (*cmp)->cmp_locale[locale_e - locale_s] == '\0')))

      return *cmp;
  }
  return NULL;
}

static bool op_match(cl_handle* cl, int relationship, int operation) {
  cl_assert(cl, operation != GRAPHD_OP_MATCH);

  switch (operation) {
    case (GRAPHD_OP_NE):
      return relationship != 0;
    case (GRAPHD_OP_EQ):
      return relationship == 0;
    case (GRAPHD_OP_LE):
      return relationship <= 0;
    case (GRAPHD_OP_LT):
      return relationship < 0;
    case (GRAPHD_OP_GE):
      return relationship >= 0;
    case (GRAPHD_OP_GT):
      return relationship > 0;

    default:
      cl_notreached(cl, "%i is not a valid graphd_op", operation);
      return 0;
  }
}

char const* graphd_comparator_to_string(graphd_comparator const* comparator) {
  if (comparator == NULL) return "unspecified";

  return comparator->cmp_name;
}

int graphd_comparator_value_match(graphd_request* greq,
                                  graphd_string_constraint const* strcon,
                                  const char* s, const char* e,
                                  graphd_comparator const* cmp) {
  int eq;
  cl_handle* const cl = graphd_request_cl(greq);

  graphd_string_constraint_element const* strcel;

  if (strcon->strcon_head == NULL) {
    if (strcon->strcon_op == GRAPHD_OP_MATCH)
      return s == NULL ? 0 : GRAPHD_ERR_NO;
    else {
      eq = cmp->cmp_sort_compare(greq, s, e, NULL, NULL);
      return op_match(cl, eq, strcon->strcon_op) ? 0 : GRAPHD_ERR_NO;
    }
  }

  if (strcon->strcon_op == GRAPHD_OP_NE) {
    for (strcel = strcon->strcon_head; strcel != NULL;
         strcel = strcel->strcel_next) {
      if (0 ==
          cmp->cmp_sort_compare(greq, s, e, strcel->strcel_s, strcel->strcel_e))
        return GRAPHD_ERR_NO;
    }
    return 0;
  }

  for (strcel = strcon->strcon_head; strcel != NULL;
       strcel = strcel->strcel_next) {
    if (strcon->strcon_op == GRAPHD_OP_MATCH) {
      if (strcel->strcel_s == NULL) {
        if (s == NULL) return 0;
        continue;
      }
      if (s == NULL) continue;

      if (cmp->cmp_glob(greq, strcel->strcel_s, strcel->strcel_e, s, e))
        return 0;
    } else {
      cl_assert(cl, strcon->strcon_op != GRAPHD_OP_NE);
      eq =
          cmp->cmp_sort_compare(greq, s, e, strcel->strcel_s, strcel->strcel_e);

      if (op_match(cl, eq, strcon->strcon_op)) return 0;
    }
  }

  return GRAPHD_ERR_NO;
}
