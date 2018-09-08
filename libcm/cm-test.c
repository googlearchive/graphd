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
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include "libcm/cm.h"

#define except_throw(e) goto e
#define except_catch(e) \
  while (0) e:

#define TEST(expr)                          \
  do {                                      \
    if (!(expr)) {                          \
      fprintf(stderr,                       \
              "test \"%s\", line %d: test " \
              "failed: %s\n",               \
              __FILE__, __LINE__, #expr);   \
      except_throw(err);                    \
    }                                       \
  } while (0)

int const numbers[4] = {1, 2, 3, 0};

static int test_malcpy(cm_handle *cm) {
  char *tmp;
  int *nums;
  size_t i;
  int result = 0;

  tmp = cm_malcpy(cm, "Hello, World!", sizeof("Hello, World!"));
  TEST(!strcmp(tmp, "Hello, World!"));
  cm_free(cm, tmp);

  tmp = cm_strmalcpy(cm, "Hello, World!");
  TEST(!strcmp(tmp, "Hello, World!"));
  cm_free(cm, tmp);

  nums = cm_tmalcpy(cm, int, numbers, 4);
  for (i = 0; i < 4; i++) TEST(nums[i] == numbers[i]);
  cm_free(cm, nums);

  except_catch(err) result = 1;

  return result;
}

static int test_zero_fill(cm_handle *cm, size_t total) {
  char *tmp;
  size_t i;

  tmp = cm_malloc(cm, total);
  memset(tmp, 0xFF, total);
  cm_free(cm, tmp);

  tmp = cm_zalloc(cm, total);
  for (i = 0; i < total; i++) {
    if (tmp[i] != '\0') {
      fprintf(stderr,
              "test_zero_fill: expected "
              "zero-filled allocated data, "
              "got [%d] = %2.2x!\n",
              (int)i, (int)(unsigned char)tmp[i]);
      return 1;
    }
  }
  return 0;
}

static int malloc_realloc_free(cm_handle *cm, char const *file, int line) {
  char *tmp;

  tmp = cm_malloc(cm, sizeof("Hello,"));
  TEST(tmp != NULL);
  strcpy(tmp, "Hello,");
  tmp = cm_realloc(cm, tmp, sizeof("Hello, World!"));
  TEST(tmp != NULL);
  TEST(!strcmp(tmp, "Hello,"));
  strcat(tmp, " World!");
  TEST(!strcmp(tmp, "Hello, World!"));
  cm_free(cm, tmp);

  except_catch(err) {
    fprintf(stderr, "\t[from \"%s\", line %d]\n", file, line);
    return 1;
  }

  return 0;
}

static int mallocing_sprintf(cm_handle *cm, char const *file, int line) {
  char *tmp;
  size_t n;

  tmp = cm_sprintf(cm, "%s, %s!", "Hello", "World");
  TEST(tmp != NULL);
  TEST(!strcmp(tmp, "Hello, World!"));
  cm_free(cm, tmp);

  tmp = cm_sprintf(cm, "%s", "");
  TEST(tmp != NULL);
  TEST(!strcmp(tmp, ""));
  cm_free(cm, tmp);

  tmp = cm_sprintf(cm, "%100000s", "");
  TEST(tmp != NULL);
  n = strlen(tmp);
  TEST(strlen(tmp) == 100000);
  cm_free(cm, tmp);

  except_catch(err) {
    fprintf(stderr, "\t[from \"%s\", line %d]\n", file, line);
    return 1;
  }

  return 0;
}

static int hashtable(cm_handle *cm, char const *file, int line) {
  cm_hashtable *h;
  char **elem;

  h = cm_hcreate(cm, char *, 1024);
  TEST(h != NULL);

  elem = cm_hnew(h, char *, "blue", 5);
  TEST(elem != NULL);
  TEST(*elem == NULL);
  *elem = "fish";

  elem = cm_hexcl(h, char *, "blue", 5);
  TEST(elem == NULL);
  TEST(errno == EEXIST);

  elem = cm_haccess(h, char *, "blue", 5);
  TEST(elem != NULL);
  TEST(strcmp(*elem, "fish"));

  elem = cm_hexcl(h, char *, "red", 5);
  TEST(elem != NULL);
  TEST(*elem == NULL);

  elem = cm_haccess(h, char *, "yellow", 5);
  TEST(elem == NULL);
  TEST(errno == ENOENT);

  cm_hdestroy(h, char *);

  except_catch(err) {
    fprintf(stderr, "\t[from \"%s\", line %d]\n", file, line);
    return 1;
  }
  return 0;
}

int main(int ac, char **av) {
  cm_handle *h_c, *cm;
  int result = 0;

  /* A round with the C library. */

  cm = cm_c();
  result |= malloc_realloc_free(cm, __FILE__, __LINE__);
  h_c = cm;

  /* A round with the trace library. */

  cm = cm_trace(h_c);
  result |= malloc_realloc_free(cm, __FILE__, __LINE__);
  result |= mallocing_sprintf(cm, __FILE__, __LINE__);
  result |= test_malcpy(cm);
  cm_trace_destroy(cm);

  /* A round with the error library. */

  cm = cm_error(h_c);
  result |= malloc_realloc_free(cm, __FILE__, __LINE__);
  cm_error_destroy(cm);

  /* A round with the heap library. */

  cm = cm_heap(h_c);
  result |= malloc_realloc_free(cm, __FILE__, __LINE__);

  /* cm_zalloc -- allocate zero-filled */
  if (test_zero_fill(cm, 1024)) result = 1;

  /* free the heap. */
  cm_heap_destroy(cm);

  /* Normal data isn't zero filled. */
  return result;
}
