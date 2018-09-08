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
#include "libcm/cm.h"

#include <string.h>

/*
 *  cm-argv.c -- manipulating a malloc'ed argv vector.
 */

/**
 * @brief determine the length of an argv vector
 *
 * The length takes O(n) time to compute.  This data structure
 * is not suited to large vectors.
 *
 * @param argv 	the vector whose length we're measuring.
 * @return the length of the vector, not including the terminal NULL.
 */
size_t cm_argvlen(char const *const *argv) {
  size_t i = 0;
  while (*argv++) i++;
  return i;
}

/**
 * @brief determine the position of an element in an argv vector
 *
 * The position takes O(n) time to compute.  This data structure
 * is not suited to large vectors.
 * The string is compared by value, not by pointer.
 *
 * @param argv 	the vector in which we're searching.
 * @param arg 	the string we're searching for.
 * @return a pointer to the first occurrence of @b arg
 * in @b argv, compared as '\\0'-terminated, case-sensitive
 * strings.
 */
/* like strchr */
char **cm_argvarg(char const *const *argv, char const *arg) {
  if (!argv) return 0;
  if (!arg) {
    while (*argv) argv++;
    return (char **)argv;
  }
  while (*argv) {
    if (**argv == *arg && !strcmp(*argv, arg))
      return (char **)argv;
    else
      argv++;
  }
  return (char **)0;
}

/**
 * @brief determine the offset of an element in an argv vector
 *
 * The position takes O(n) time to compute.  This data structure
 * is not suited to large vectors.
 * The string is compared by value, not by pointer.
 *
 * @param argv 	the vector in which we're searching.
 * @param arg 	the string we're searching for.
 * @return the offset of the first occurrence of @b arg
 * in @b argv, when compared as '\\0'-terminated, case-sensitive
 * strings.
 */
int cm_argvpos(char const *const *argv, char const *arg) {
  char **a = cm_argvarg(argv, arg);
  if (a)
    return a - (char **)argv;
  else
    return -1;
}

/**
 * @brief add an element to an argv vector.
 *
 * The element is added only if it doesn't appear in the structure
 * yet (when compared as case-sensitive strings.)
 *
 * This operation takes O(n) time to execute.  This data structure
 * is not suited to large vectors.
 *
 * @param cm 	allocator module handle
 * @param argv 	NULL or the array to which we're adding.  The
 *	array becomes invalid after the call.  (Replace it with
 * 	the call result.)
 * @param arg 	the string we're adding.
 * @return an array that now contains an independent copy of the
 * 	new argument string's content, or NULL on allocation error.
 */
char **cm_argvadd(cm_handle *cm, char **argv, char const *arg) {
  if (!argv) {
    argv = cm_talloc(cm, char *, 1 + !!arg);
    if (!arg)
      argv[0] = (char *)0;
    else if (!(argv[0] = cm_strmalcpy(cm, arg))) {
      cm_free(cm, argv);
      return NULL;
    } else
      argv[1] = NULL;
  } else if (cm_argvarg((char const *const *)argv, arg))
    return argv;
  else {
    size_t len = cm_argvlen((char const *const *)argv);
    argv = cm_trealloc(cm, char *, argv, len + 2);
    if (!argv || !(argv[len] = cm_strmalcpy(cm, arg))) return NULL;
    argv[len + 1] = NULL;
  }
  return argv;
}

/**
 * @brief delete an element from an argv vector.
 *
 * This operation takes O(n) time to execute.  This data structure
 * is not suited to large vectors.
 *
 * @param cm 	allocator module handle
 * @param argv 	NULL or the vector from which an element should be deleted.
 *	The array becomes invalid after the call.  (Replace it with the
 * 	call result.)
 * @param arg 	the string that is deleted.
 * @return the resulting array.
 */
char **cm_argvdel(cm_handle *cm, char **argv, char const *arg) {
  char **a = cm_argvarg((char const *const *)argv, arg);
  if (a && arg) {
    cm_free(cm, *a);
    while ((a[0] = a[1]) != NULL) a++;
  }
  return argv;
}

/**
 * @brief Free an argv vector and all its elements.
 * @param cm 	allocator module handle
 * @param argv vector to free
 */
void cm_argvfree(cm_handle *cm, char **argv) {
  char **a;

  if (argv) {
    for (a = argv; *a; a++) cm_free(cm, *a);
    cm_free(cm, argv);
  }
}

/**
 * @brief Make a deep duplicate of an argv vector and all its elements.
 *
 * Note that the source and the destination need not be allocated
 * using the same cm_handle.
 *
 * @param cm 	allocator handle in which to allocate the duplicate.
 * @param argv 	vector to duplicate.
 * @return A deep duplicate of the argument.  (Meaning that the
 * 	source and destination do not share the indivdiual, pointed-to
 *	argument strings.)
 */
char **cm_argvdup(cm_handle *cm, char const *const *argv) {
  if (!argv)
    return NULL;
  else {
    size_t n = cm_argvlen(argv);
    char **a = cm_talloc(cm, char *, n + 1);

    if (a == NULL) return NULL;
    a[n] = (char *)0;
    while (n--)
      if (!(a[n] = cm_strmalcpy(cm, argv[n]))) {
        while (a[++n]) cm_free(cm, a[n]);
        cm_free(cm, a);
        return NULL;
      }
    return a;
  }
}
