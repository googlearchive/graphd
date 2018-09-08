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

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define IS_LITERAL(lit, fmt) (strncasecmp((lit), (fmt), sizeof(lit) - 1) == 0)

int pdb_iterator_util_statistics_none(pdb_handle *pdb, pdb_iterator *it,
                                      pdb_budget *budget) {
  char buf[1024];
  cl_notreached(pdb->pdb_cl,
                "unexpected pdb_util_statistics_none() on iterator %s",
                pdb_iterator_to_string(pdb, it, buf, sizeof buf));
  return 0;
}

void pdb_iterator_util_finish(pdb_handle *pdb, pdb_iterator *it) {
  cl_cover(pdb->pdb_cl);
  PDB_IS_FINISHING_ITERATOR(pdb->pdb_cl, it);

  if (it->it_displayname != NULL) {
    cm_free(pdb->pdb_cm, it->it_displayname);
    it->it_displayname = NULL;
  }
  it->it_type = NULL;
  it->it_magic = 0;
}

/*  Linear table look up of a name in the list of supported properties.
 */
static pdb_iterator_property *pdb_iterator_util_property(
    pdb_iterator_property *pip, char const *name_s, char const *name_e) {
  if (pip == NULL) return NULL;

  for (; pip->pip_name != NULL; pip++)
    if (strncasecmp(pip->pip_name, name_s, name_e - name_s) == 0 &&
        pip->pip_name[name_e - name_s] == '\0')

      return pip;
  return NULL;
}

int pdb_iterator_util_thaw(pdb_handle *pdb, char const **s_ptr, char const *e,
                           char const *fmt, ...) {
  va_list ap;
  cl_handle *cl = pdb->pdb_cl;
  char const *r = *s_ptr;
  int err = 0;
  char const *fmt0 = fmt;
  char const *s0 = *s_ptr;

  va_start(ap, fmt);
  for (;;) {
    *s_ptr = r;
    if (*fmt == '\0') break;
    if (r == NULL) {
      if (IS_LITERAL("%{end}", fmt) || IS_LITERAL("%$", fmt)) {
        va_end(ap);
        return 0;
      }
      cl_log(cl, CL_LEVEL_VERBOSE,
             "pdb_iterator_util_thaw: "
             "null argument; fmt=\"%s\"",
             fmt0);
      va_end(ap);
      return PDB_ERR_SYNTAX;
    }

    if (*fmt != '%' || *++fmt == '%') {
      if (*r != *fmt) {
        err = PDB_ERR_SYNTAX;
        goto err;
      }
      r++;
      fmt++;

      continue;
    }

    if (fmt[0] == '{') /*}*/
    {
      char const *fmt_end = strchr(fmt, /*{*/ '}');
      cl_assert(cl, fmt_end != NULL);

      /* New-style explicit name */

      if (IS_LITERAL("{low[-high]}", fmt)) {
        unsigned long long *arg_low;
        unsigned long long *arg_high;

        arg_low = va_arg(ap, unsigned long long *);
        arg_high = va_arg(ap, unsigned long long *);

        if ((err = pdb_scan_ull(&r, e, arg_low)) != 0) goto err;

        if (r < e && *r == '-') {
          r++;
          err = pdb_scan_ull(&r, e, arg_high);
          if (err != 0) goto err;
        } else
          *arg_high = PDB_ITERATOR_HIGH_ANY;

      } else if (IS_LITERAL("{next[+find]}", fmt)) {
        pdb_budget *arg_nc;
        pdb_budget *arg_fc;
        unsigned long long ull;

        arg_nc = va_arg(ap, pdb_budget *);
        arg_fc = va_arg(ap, pdb_budget *);

        if ((err = pdb_scan_ull(&r, e, &ull)) != 0) goto err;

        *arg_nc = ull;

        if (r < e && *r == '+') {
          r++;
          if ((err = pdb_scan_ull(&r, e, &ull)) != 0) goto err;
          *arg_fc = ull;
        } else
          *arg_fc = 0;
      } else if (IS_LITERAL("{linkage[+guid]}", fmt)) {
        int *arg_linkage;
        graph_guid *arg_guid;
        char const *ptr;

        arg_linkage = va_arg(ap, int *);
        arg_guid = va_arg(ap, graph_guid *);

        ptr = r;
        while (ptr < e && isascii(*ptr) && isalpha(*ptr)) ptr++;

        *arg_linkage = pdb_linkage_from_string(r, ptr);
        if (*arg_linkage == PDB_LINKAGE_N) {
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "pdb_iterator_util_thaw: "
                 "mismatch: %.*s vs. %%lg (l)",
                 (int)(e - *s_ptr), *s_ptr);
          err = PDB_ERR_SYNTAX;
          goto err;
        }

        r = ptr;
        if (ptr < e && *ptr == '+') {
          r++;
          ptr++;
          while (ptr < e && isalnum(*ptr) && ptr - r <= 32) ptr++;

          err = graph_guid_from_string(arg_guid, r, ptr);
          if (err != 0) goto err;
          r = ptr;
        } else {
          GRAPH_GUID_MAKE_NULL(*arg_guid);
        }
      } else if (IS_LITERAL("{linkage}", fmt)) {
        int *arg_linkage;
        char const *ptr;

        arg_linkage = va_arg(ap, int *);

        ptr = r;
        while (ptr < e && isascii(*ptr) && isalpha(*ptr)) ptr++;

        *arg_linkage = pdb_linkage_from_string(r, ptr);
        if (*arg_linkage == PDB_LINKAGE_N) {
          err = PDB_ERR_SYNTAX;
          goto err;
        }
        r = ptr;
      } else if (IS_LITERAL("{eof/id}", fmt)) /* EOF flag and ID */
      {
        pdb_id *id_arg;
        bool *eof_arg;

        eof_arg = va_arg(ap, bool *);
        id_arg = va_arg(ap, pdb_id *);

        if (r < e && *r == '$') {
          *eof_arg = true;
          r++;
        } else {
          *eof_arg = false;
          err = pdb_id_from_string(pdb, id_arg, &r, e);
          if (err != 0) goto err;
        }
      } else if (IS_LITERAL("{id}", fmt)) /* id */
      {
        pdb_id *arg;

        arg = va_arg(ap, pdb_id *);
        err = pdb_id_from_string(pdb, arg, &r, e);
        if (err != 0) goto err;
      } else if (IS_LITERAL("{guid}", fmt)) /* guid */
      {
        graph_guid *arg;
        char const *ptr;

        arg = va_arg(ap, graph_guid *);
        for (ptr = r;
             ptr < e && ptr < r + 32 && isascii(*ptr) && isxdigit(*ptr); ptr++)
          ;
        err = graph_guid_from_string(arg, r, ptr);
        if (err != 0) goto err;
        r = ptr;
      } else if (IS_LITERAL("{budget}", fmt)) /* budget */
      {
        unsigned long long ull;
        pdb_budget *arg;
        pdb_budget neg = 1;

        arg = va_arg(ap, pdb_budget *);
        if (r < e && *r == '-') {
          neg = -1;
          r++;
        }
        err = pdb_scan_ull(&r, e, &ull);
        if (err != 0) goto err;

        *arg = neg * (pdb_budget)ull;
      } else if (IS_LITERAL("{bytes}", fmt)) {
        char const **arg_start;
        char const **arg_end;
        char const *ptr;

        arg_start = va_arg(ap, char const **);
        arg_end = va_arg(ap, char const **);

        *arg_start = r;

        cl_assert(cl, fmt_end != NULL);

        /* At the end of the format string? */
        if (fmt_end[1] == '\0')
          ptr = e;
        else {
          ptr = pdb_unparenthesized(r, e, fmt_end[1]);
          if (ptr == NULL) ptr = e;
        }
        if (arg_end != NULL) *arg_end = ptr;
        r = ptr;
      } else if (IS_LITERAL("{(bytes)}", fmt) || IS_LITERAL("{[bytes]}", fmt)) {
        char const **arg_start;
        char const **arg_end;
        char const *ptr;
        unsigned int nparen = 0;
        bool in_string = false, escaped = false;

        arg_start = va_arg(ap, char const **);
        arg_end = va_arg(ap, char const **);

        if (r >= e || *r != fmt[1]) {
          err = PDB_ERR_SYNTAX;
          goto err;
        }
        ptr = r;
        while (ptr < e && (*ptr != fmt_end[-1] || nparen != 1 || in_string)) {
          if (in_string) {
            if (escaped)
              escaped = false;
            else if (*ptr == '"')
              in_string = false;
            else
              escaped = (*ptr == '\\');
            ptr++;
            continue;
          } else
            switch (*ptr++) {
              case '(':
              case '[':
                nparen++;
                break;
              case ')':
              case ']':
                nparen--;
                break;
              case '"':
                in_string = true;
                break;
              default:
                break;
            }
        }

        if (ptr >= e) {
          cl_log(cl, CL_LEVEL_FAIL, "expected %c...%c, got \"%.*s\"", fmt[1],
                 fmt_end[-1], (int)(e - r), r);
          err = PDB_ERR_SYNTAX;
          goto err;
        }
        *arg_start = r + 1;
        *arg_end = ptr;

        r = ptr + 1;
      } else if (IS_LITERAL("{position/state}",
                            fmt)) /* position/state 	*/
      {
        pdb_iterator_text *arg_pit;

        arg_pit = va_arg(ap, pdb_iterator_text *);

        arg_pit->pit_position_s = r;
        arg_pit->pit_state_e = r = pdb_unparenthesized(r, e, ')');
        arg_pit->pit_position_e = pdb_unparenthesized(
            arg_pit->pit_position_s, arg_pit->pit_state_e, '/');
        arg_pit->pit_state_s = arg_pit->pit_position_e +
                               (arg_pit->pit_position_e < arg_pit->pit_state_e);
      } else if (IS_LITERAL("{(position/state)}", fmt)) {
        pdb_iterator_text *arg_pit;

        arg_pit = va_arg(ap, pdb_iterator_text *);

        err = pdb_iterator_util_thaw(
            pdb, &r, e, "%()", &arg_pit->pit_position_s, &arg_pit->pit_state_e);
        if (err) goto err;

        arg_pit->pit_position_e = pdb_unparenthesized(
            arg_pit->pit_position_s, arg_pit->pit_state_e, '/');
        arg_pit->pit_state_s = arg_pit->pit_position_e +
                               (arg_pit->pit_position_e < arg_pit->pit_state_e);
      } else if (IS_LITERAL("{forward}", fmt)) /* boolean forward */
      {
        bool *arg;

        arg = va_arg(ap, bool *);
        *arg = (r < e && *r == '~') ? r++, false : true;
      } else if (IS_LITERAL("{end}", fmt)) /* end */
      {
        if (r < e) {
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "pdb_iterator_util_thaw: "
                 "unexpected trailing data \"%.*s\"",
                 (int)(e - r), r);
          err = PDB_ERR_SYNTAX;
          goto err;
        }
      } else if (IS_LITERAL("{account}", fmt)) /* account */
      {
        pdb_iterator_base *pib;
        pdb_iterator_account **acc;

        pib = va_arg(ap, pdb_iterator_base *);
        acc = va_arg(ap, pdb_iterator_account **);

        if (r == NULL || e - r < 3 || strncasecmp(r, "[a:", 3) != 0)
          *acc = NULL;
        else {
          char const *a_s, *a_e;
          unsigned long long ull;

          a_s = r + 3;
          a_e = pdb_unparenthesized(a_s, e, ']');
          if (a_e == NULL) {
            err = PDB_ERR_SYNTAX;
            *acc = NULL;
            goto err;
          }
          r = a_e + 1;

          /*  Scan the string as a number.
           */
          err = pdb_scan_ull(&a_s, a_e, &ull);
          if (err != 0) goto err;

          *acc = pdb_iterator_base_account_lookup(pdb, pib, (size_t)ull);
        }
      } else if (IS_LITERAL("{orderingbytes}", fmt)) /* ordering, as bytes */
      {
        char const **ord_s;
        char const **ord_e;

        ord_s = va_arg(ap, char const **);
        ord_e = va_arg(ap, char const **);

        cl_assert(cl, ord_s != NULL);
        cl_assert(cl, ord_e != NULL);

        if (r == NULL || e - r < 3 || strncasecmp(r, "[o:", 3) != 0) {
          *ord_s = NULL;
          *ord_e = NULL;
        } else {
          *ord_s = r + 3;
          *ord_e = pdb_unparenthesized(*ord_s, e, ']');
          if (*ord_e == NULL) {
            err = PDB_ERR_SYNTAX;
            *ord_s = NULL;
            goto err;
          }
          r = *ord_e + 1;
        }
      } else if (IS_LITERAL("{ordering}", fmt)) /* ordering */
      {
        pdb_iterator_base *pib;
        char const **ord;

        pib = va_arg(ap, pdb_iterator_base *);
        ord = va_arg(ap, char const **);

        if (r == NULL || e - r < 3 || strncasecmp(r, "[o:", 3) != 0)
          *ord = NULL;
        else {
          char *str;
          char const *o_s, *o_e;

          o_s = r + 3;
          o_e = pdb_unparenthesized(o_s, e, ']');
          if (o_e == NULL) {
            err = PDB_ERR_SYNTAX;
            *ord = NULL;
            goto err;
          }
          r = o_e + 1;

          /*  Duplicate the string onto the heap in
           *  the pdb_iterator_base.
           */
          str = cm_malloc(pib->pib_cm, 1 + (o_e - o_s));
          if (str == NULL) return errno ? errno : ENOMEM;

          memcpy(str, o_s, o_e - o_s);
          str[o_e - o_s] = '\0';

          *ord = str;
        }
      } else if (IS_LITERAL("{extensions}", fmt)) {
        pdb_iterator_property *pip, *p2;
        char const *col, *clo;

        pip = va_arg(ap, pdb_iterator_property *);
        while (r < e && *r == '[') {
          clo = pdb_unparenthesized(r + 1, e, ']');
          if (clo == NULL || clo == e) {
            cl_log(cl, CL_LEVEL_VERBOSE,
                   "expected [...], "
                   "got \"%.*s\"",
                   (int)(e - r), r);
            err = PDB_ERR_SYNTAX;
            goto err;
          }
          col = pdb_unparenthesized(r + 1, clo, ':');
          if (col == NULL) {
            cl_log(cl, CL_LEVEL_VERBOSE,
                   "expected [name:..], "
                   "got \"%.*s\"",
                   (int)(clo - r), r);
            err = PDB_ERR_SYNTAX;
            goto err;
          }

          p2 = pdb_iterator_util_property(pip, r + 1, col);

          /* Must exist? */
          if (col + 1 < clo && col[1] == ':') {
            if (p2 == NULL) {
              cl_log(cl, CL_LEVEL_VERBOSE,
                     "unsupported required"
                     "property %.*s::",
                     (int)(col - (r + 1)), r + 1);
              err = PDB_ERR_SYNTAX;
              goto err;
            }

            p2->pip_s = (col + 2);
            p2->pip_e = clo;
          } else if (p2 != NULL) {
            p2->pip_s = col + 1;
            p2->pip_e = clo;
          }
          r = clo + 1;
        }
      } else
        cl_notreached(cl, "unexpected format sequence %s in \"%s\"", fmt, fmt0);

      fmt = fmt_end + 1;
    } else {
      /* Old-style scanf imitation */
      if (fmt[0] == 'l' /* unsigned long long */
          && fmt[1] == 'l' && fmt[2] == 'u') {
        unsigned long long *arg;

        arg = va_arg(ap, unsigned long long *);
        err = pdb_scan_ull(&r, e, arg);
        if (err != 0) goto err;

        fmt += 3;
      } else if (fmt[0] == 'l' /* unsigned long */
                 && fmt[1] == 'u') {
        unsigned long long ull;
        unsigned long *arg;

        arg = va_arg(ap, unsigned long *);
        err = pdb_scan_ull(&r, e, &ull);
        if (err != 0) goto err;

        if (ull > ULONG_MAX) {
          err = ERANGE;
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "pdb_iterator_util_thaw: "
                 "value out of range: %llu vs. "
                 "%lu",
                 ull, ULONG_MAX);
          goto err;
        }

        *arg = ull;
        fmt += 2;
      } else if (fmt[0] == 'l' /* long long */
                 && fmt[1] == 'l' && fmt[2] == 'd') {
        unsigned long long ull;
        long long *arg;
        long long neg = 1;

        arg = va_arg(ap, long long *);

        if (r < e && *r == '-') {
          neg = -1;
          r++;
        }
        if ((err = pdb_scan_ull(&r, e, &ull)) != 0) goto err;
        *arg = ull * neg;
        fmt += 3;
      } else if (fmt[0] == 'd') {
        unsigned long long ull;
        int *arg;
        int neg = 1;

        arg = va_arg(ap, int *);

        if (r < e && *r == '-') {
          neg = -1;
          r++;
        }
        if ((err = pdb_scan_ull(&r, e, &ull)) != 0) goto err;

        *arg = (int)((long long)ull * neg);
        fmt++;
      } else if (fmt[0] == 'z' /* size_t */
                 && fmt[1] == 'u') {
        unsigned long long ull;
        size_t *arg;

        arg = va_arg(ap, size_t *);
        err = pdb_scan_ull(&r, e, &ull);
        if (err != 0) goto err;

        if (ull > (size_t)-1) {
          err = ERANGE;
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "pdb_iterator_util_thaw: "
                 "out of range:  %llu vs. %zu",
                 ull, (size_t)-1);
          goto err;
        }
        *arg = ull;
        fmt += 2;
      } else if (fmt[0] == 'l' /* LOW[-HIGH] */
                 && fmt[1] == 'h') {
        unsigned long long *arg_low;
        unsigned long long *arg_high;

        arg_low = va_arg(ap, unsigned long long *);
        arg_high = va_arg(ap, unsigned long long *);

        if ((err = pdb_scan_ull(&r, e, arg_low)) != 0) goto err;

        if (r < e && *r == '-') {
          r++;
          if ((err = pdb_scan_ull(&r, e, arg_high)) != 0) goto err;
        } else
          *arg_high = PDB_ITERATOR_HIGH_ANY;

        fmt += 2;
      } else if (fmt[0] == 'n' /* NEXT[+FIND] */
                 && fmt[1] == 'f') {
        pdb_budget *arg_nc;
        pdb_budget *arg_fc;
        unsigned long long ull;

        arg_nc = va_arg(ap, pdb_budget *);
        arg_fc = va_arg(ap, pdb_budget *);

        if ((err = pdb_scan_ull(&r, e, &ull)) != 0) goto err;

        *arg_nc = ull;

        if (r < e && *r == '+') {
          r++;
          if ((err = pdb_scan_ull(&r, e, &ull)) != 0) goto err;
          *arg_fc = ull;
        } else
          *arg_fc = 0;
        fmt += 2;
      } else if (fmt[0] == 'l' /* LINKAGE[+GUID] */
                 && fmt[1] == 'g') {
        int *arg_linkage;
        graph_guid *arg_guid;
        char const *ptr;

        arg_linkage = va_arg(ap, int *);
        arg_guid = va_arg(ap, graph_guid *);

        ptr = r;
        while (ptr < e && isascii(*ptr) && isalpha(*ptr)) ptr++;

        *arg_linkage = pdb_linkage_from_string(r, ptr);
        if (*arg_linkage == PDB_LINKAGE_N) {
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "pdb_iterator_util_thaw: "
                 "mismatch: %.*s vs. %%lg (l)",
                 (int)(e - *s_ptr), *s_ptr);
          err = PDB_ERR_SYNTAX;
          goto err;
        }

        r = ptr;
        if (ptr < e && *ptr == '+') {
          r++;
          ptr++;
          while (ptr < e && isalnum(*ptr) && ptr - r <= 32) ptr++;

          err = graph_guid_from_string(arg_guid, r, ptr);
          if (err != 0) goto err;
          r = ptr;
        } else {
          GRAPH_GUID_MAKE_NULL(*arg_guid);
        }
        fmt += 2;
      } else if (fmt[0] == 'l' /* LINKAGE */
                 && fmt[1] != 'l' && fmt[1] != 'd' && fmt[1] != 'g') {
        int *arg_linkage;
        char const *ptr;

        arg_linkage = va_arg(ap, int *);

        ptr = r;
        while (ptr < e && isascii(*ptr) && isalpha(*ptr)) ptr++;

        *arg_linkage = pdb_linkage_from_string(r, ptr);
        if (*arg_linkage == PDB_LINKAGE_N) {
          err = PDB_ERR_SYNTAX;
          goto err;
        }
        r = ptr;
        fmt++;
      } else if (fmt[0] == 'e' /* EOF flag and ID */
                 && fmt[1] == 'i') {
        pdb_id *id_arg;
        bool *eof_arg;

        eof_arg = va_arg(ap, bool *);
        id_arg = va_arg(ap, pdb_id *);

        if (r < e && *r == '$') {
          *eof_arg = true;
          r++;
        } else {
          *eof_arg = false;
          err = pdb_id_from_string(pdb, id_arg, &r, e);
          if (err != 0) goto err;
        }
        fmt += 2;
      } else if (*fmt == 'i') /* id */
      {
        pdb_id *arg;

        arg = va_arg(ap, pdb_id *);
        err = pdb_id_from_string(pdb, arg, &r, e);
        if (err != 0) goto err;

        fmt++;
      } else if (*fmt == 'g') /* guid */
      {
        graph_guid *arg;
        char const *ptr;

        arg = va_arg(ap, graph_guid *);
        for (ptr = r;
             ptr < e && ptr < r + 32 && isascii(*ptr) && isxdigit(*ptr); ptr++)
          ;
        err = graph_guid_from_string(arg, r, ptr);
        if (err != 0) goto err;
        r = ptr;
        fmt++;
      } else if (*fmt == 'b') /* budget */
      {
        unsigned long long ull;
        pdb_budget *arg;
        pdb_budget neg = 1;

        arg = va_arg(ap, pdb_budget *);
        if (r < e && *r == '-') {
          neg = -1;
          r++;
        }
        err = pdb_scan_ull(&r, e, &ull);
        if (err != 0) goto err;

        *arg = neg * (pdb_budget)ull;
        fmt++;
      } else if (*fmt == 'c') /* single char */
      {
        char *arg;

        arg = va_arg(ap, char *);
        if (r < e)
          *arg = *r++;
        else
          goto err;
        fmt++;
      } else if (*fmt == 's') /* bytes, start and end pointer */
      {
        char const **arg_start;
        char const **arg_end;
        char const *ptr;

        arg_start = va_arg(ap, char const **);
        arg_end = va_arg(ap, char const **);

        *arg_start = r;
        if (fmt[1] == '\0')
          ptr = e;
        else {
          ptr = memchr(r, fmt[1], e - r);
          if (ptr == NULL) ptr = e;
        }
        if (arg_end != NULL) *arg_end = ptr;
        r = ptr;
        fmt++;
      } else if ((fmt[0] == '(' && fmt[1] == ')')     /* list contents */
                 || (fmt[0] == '[' && fmt[1] == ']')) /* grouping */
      {
        char const **arg_start;
        char const **arg_end;
        char const *ptr;
        unsigned int nparen = 0;
        bool in_string = false, escaped = false;

        arg_start = va_arg(ap, char const **);
        arg_end = va_arg(ap, char const **);

        if (r >= e || *r != fmt[0]) {
          err = PDB_ERR_SYNTAX;
          goto err;
        }
        ptr = r;
        while (ptr < e && (*ptr != fmt[1] || nparen != 1 || in_string)) {
          if (in_string) {
            if (escaped)
              escaped = false;
            else if (*ptr == '"')
              in_string = false;
            else
              escaped = (*ptr == '\\');
            ptr++;
            continue;
          } else
            switch (*ptr++) {
              case '(':
              case '[':
                nparen++;
                break;
              case ')':
              case ']':
                nparen--;
                break;
              case '"':
                in_string = true;
                break;
              default:
                break;
            }
        }

        if (ptr >= e) {
          cl_log(cl, CL_LEVEL_FAIL, "expected %c...%c, got \"%.*s\"", fmt[0],
                 fmt[1], (int)(e - r), r);
          err = PDB_ERR_SYNTAX;
          goto err;
        }
        *arg_start = r + 1;
        *arg_end = ptr;

        r = ptr + 1;
        fmt += 2;
      } else if (fmt[0] == 'p'     /* position/state 	*/
                 && fmt[1] == 's') /* without leading ()   */
      {
        pdb_iterator_text *arg_pit;

        arg_pit = va_arg(ap, pdb_iterator_text *);

        arg_pit->pit_position_s = r;
        arg_pit->pit_state_e = r = pdb_unparenthesized(r, e, ')');
        arg_pit->pit_position_e = pdb_unparenthesized(
            arg_pit->pit_position_s, arg_pit->pit_state_e, '/');
        arg_pit->pit_state_s = arg_pit->pit_position_e +
                               (arg_pit->pit_position_e < arg_pit->pit_state_e);

        fmt += 2;
      } else if (fmt[0] == '(' && fmt[1] == 'p' /* (position/state) */
                 && fmt[2] == 's' && fmt[3] == ')') {
        pdb_iterator_text *arg_pit;

        arg_pit = va_arg(ap, pdb_iterator_text *);

        err = pdb_iterator_util_thaw(
            pdb, &r, e, "%()", &arg_pit->pit_position_s, &arg_pit->pit_state_e);
        if (err) goto err;

        arg_pit->pit_position_e = pdb_unparenthesized(
            arg_pit->pit_position_s, arg_pit->pit_state_e, '/');
        arg_pit->pit_state_s = arg_pit->pit_position_e +
                               (arg_pit->pit_position_e < arg_pit->pit_state_e);

        fmt += 4;
      } else if (*fmt == '~') /* boolean forward */
      {
        bool *arg;

        arg = va_arg(ap, bool *);

        *arg = (r < e && *r == '~') ? r++, false : true;
        fmt++;
      } else if (*fmt == '$') /* end */
      {
        if (r < e) {
          cl_log(cl, CL_LEVEL_VERBOSE,
                 "pdb_iterator_util_thaw: "
                 "unexpected trailing data \"%.*s\"",
                 (int)(e - r), r);
          err = PDB_ERR_SYNTAX;
          goto err;
        }
        fmt++;
      } else if (*fmt == '?' && fmt[1] == 'O') /* ordering, as bytes */
      {
        char const **ord_s;
        char const **ord_e;

        ord_s = va_arg(ap, char const **);
        ord_e = va_arg(ap, char const **);

        cl_assert(cl, ord_s != NULL);
        cl_assert(cl, ord_e != NULL);

        if (r == NULL || e - r < 3 || strncasecmp(r, "[o:", 3) != 0) {
          *ord_s = NULL;
          *ord_e = NULL;
        } else {
          *ord_s = r + 3;
          *ord_e = pdb_unparenthesized(*ord_s, e, ']');
          if (*ord_e == NULL) {
            err = PDB_ERR_SYNTAX;
            *ord_s = NULL;
            goto err;
          }
          r = *ord_e + 1;
        }
        fmt += 2;
      } else if (*fmt == '?' && fmt[1] == 'o') /* ordering */
      {
        pdb_iterator_base *pib;
        char const **ord;

        pib = va_arg(ap, pdb_iterator_base *);
        ord = va_arg(ap, char const **);

        if (r == NULL || e - r < 3 || strncasecmp(r, "[o:", 3) != 0)
          *ord = NULL;
        else {
          char *str;
          char const *o_s, *o_e;

          o_s = r + 3;
          o_e = pdb_unparenthesized(o_s, e, ']');
          if (o_e == NULL) {
            err = PDB_ERR_SYNTAX;
            *ord = NULL;
            goto err;
          }
          r = o_e + 1;

          /*  Duplicate the string onto the heap in
           *  the pdb_iterator_base.
           */
          str = cm_malloc(pib->pib_cm, 1 + (o_e - o_s));
          if (str == NULL) return errno ? errno : ENOMEM;

          memcpy(str, o_s, o_e - o_s);
          str[o_e - o_s] = '\0';

          *ord = str;
        }
        fmt += 2;
      } else if (*fmt == '?') /* optional/extensible parameters */
      {
        pdb_iterator_property *pip, *p2;
        char const *col, *clo;

        pip = va_arg(ap, pdb_iterator_property *);
        while (r < e && *r == '[') {
          clo = pdb_unparenthesized(r + 1, e, ']');
          if (clo == NULL || clo == e) {
            cl_log(cl, CL_LEVEL_VERBOSE, "expected [...], got \"%.*s\"",
                   (int)(e - r), r);
            err = PDB_ERR_SYNTAX;
            goto err;
          }
          col = pdb_unparenthesized(r + 1, clo, ':');
          if (col == NULL) {
            cl_log(cl, CL_LEVEL_VERBOSE,
                   "expected [name:..], "
                   "got \"%.*s\"",
                   (int)(clo - r), r);
            err = PDB_ERR_SYNTAX;
            goto err;
          }

          p2 = pdb_iterator_util_property(pip, r + 1, col);

          /* Must exist? */
          if (col + 1 < clo && col[1] == ':') {
            if (p2 == NULL) {
              cl_log(cl, CL_LEVEL_VERBOSE,
                     "unsupported required"
                     "property %.*s::",
                     (int)(col - (r + 1)), r + 1);
              err = PDB_ERR_SYNTAX;
              goto err;
            }

            p2->pip_s = (col + 2);
            p2->pip_e = clo;
          } else if (p2 != NULL) {
            p2->pip_s = col + 1;
            p2->pip_e = clo;
          }
          r = clo + 1;
        }
        fmt++;
      } else
        cl_notreached(cl, "unexpected format sequence %%%c in \"%s\"", *fmt,
                      fmt0);
    }
  }
err:
  if (err != 0)
    cl_log(cl, CL_LEVEL_VERBOSE,
           "pdb_iterator_util_thaw: "
           "mismatch at '%c' in fmt=\"%.*s^%s\", "
           "str=\"%.*s^%.*s\": %s",
           *fmt, (int)(fmt - fmt0), fmt0, fmt, (int)(r - s0), s0, (int)(e - r),
           r, pdb_xstrerror(err));
  va_end(ap);
  return err;
}
