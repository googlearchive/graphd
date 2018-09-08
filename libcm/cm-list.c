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

#include <stddef.h>
#include <stdio.h>
#include <syslog.h>


#define NEXT(O__, I__) *(cm_list **)((char *)(I__) + (O__).lo_next)
#define PREV(O__, I__) *(cm_list **)((char *)(I__) + (O__).lo_prev)

#define cm_list_assert(expr) \
  ((expr) || (cm_list_assert_i(#expr, __FILE__, __LINE__), 0))

/*  Make a tail pointer, given a list element and a buffer.
 */
static cm_list **cm_list_long_tail(cm_list_offsets o, void *elem,
                                   cm_list **tail_buf) {
  if (elem != NULL) {
    cm_list *initial = elem;
    cm_list *next;

    while ((next = NEXT(o, elem)) != NULL && next != initial) elem = next;
  }
  *tail_buf = elem;
  return tail_buf;
}

static void cm_list_assert_i(char const *expr, char const *file, int line) {
  FILE *fp;

  if ((fp = fopen("/dev/tty", "a")) != NULL) {
    fprintf(fp, "%s, line %d: assertion fails: \"%s\"\n", file, line, expr);
    fclose(fp);
  }
  syslog(LOG_ERR | LOG_USER, "ERROR: %s, line %d: assertion fails: \"%s\"",
         file, line, expr);
  abort();
  exit(1);
}

/**
 * @brief Insert an item before another.
 *
 * @param o	offset descriptor
 * @param head	address of the head pointer
 * @param tail	NULL address of the tail pointer -- faster if present
 * @param here	NULL or pointer to the position before which to insert
 * @param item	the new item.
 */
void cm_list_insert_before_i(cm_list_offsets o, void *head_ptr, void *tail_ptr,
                             void *here_ptr, void *item_ptr) {
  cm_list **head = head_ptr;
  cm_list **tail = tail_ptr;
  cm_list *here = here_ptr;
  cm_list *item = item_ptr;
  cm_list *h_prev, *tail_buf;

  cm_list_assert(item != NULL);
  cm_list_assert(head != NULL);

  if (here != NULL)
    h_prev = PREV(o, here);
  else {
    if (tail == NULL) tail = cm_list_long_tail(o, *head, &tail_buf);
    h_prev = *tail;
  }

  NEXT(o, item) = here;
  PREV(o, item) = h_prev;

  if (here != NULL)
    PREV(o, here) = item;
  else {
    /* If we didn't have a tail on function entry,
     * we made one in the first "else".
     */
    cm_list_assert(tail != NULL);
    *tail = item;
  }

  if (*head == here) *head = item;
  if (h_prev != NULL) NEXT(o, h_prev) = item;
}

/**
 * @brief Insert an item after another
 *
 * @param o 	link description
 * @param head	head pointer
 * @param tail	tail pointer
 * @param here	NULL or item after which to insert
 * @param item	item to insert
 */
void cm_list_insert_after_i(cm_list_offsets o, void *head, void *tail,
                            void *here, void *item) {
  cm_list_assert(head != NULL);
  cm_list_assert(item != NULL);

  if (tail != NULL) {
    cm_list_offsets shazaam = {o.lo_prev, o.lo_next};
    cm_list_insert_before_i(shazaam, tail, head, here, item);
  } else if (here != NULL)
    cm_list_insert_before_i(o, head, tail, NEXT(o, here), item);

  else
    cm_list_insert_before_i(o, head, tail, *(cm_list **)head, item);
}

/**
 * @brief Add an item to the end of a list
 *
 * @param o	descriptor structure for the list
 * @param head	address of the list head pointer
 * @param tail	NULL or address of the list tail pointer - slower if NULL
 * @param item	item to be appended.
 */
void cm_list_enqueue_i(cm_list_offsets o, void *head, void *tail, void *item) {
  cm_list *tail_buf;
  cm_list_offsets shazaam = {o.lo_prev, o.lo_next};

  if (tail == NULL) tail = cm_list_long_tail(o, *(cm_list **)head, &tail_buf);
  cm_list_insert_before_i(shazaam, tail, head, *(cm_list **)tail, item);
}

/**
 * @brief Remove an item anywhere in a list
 *
 *  This is used both for the doubly linked list and the
 *  ring; calls for the ring use a NULL tail pointer.
 *
 *  If the item does not occur in the list, it must have
 *  NULL prev and next pointers; otherwise, it will be
 *  unlinked out of whatever other structure it may be
 *  part of, and chaos will ensue.
 *
 *  After a removal, an item's PREV and NEXT pointers are
 *  always set to NULL.
 *
 * @param o	list description
 * @param head	address of head pointer
 * @param tail	NULL or address of tail pointer
 * @param item	item to be unlinked.
 */

void cm_list_remove_i(cm_list_offsets o, void *head_ptr, void *tail_ptr,
                      void *item_ptr) {
  cm_list *i_next;
  cm_list *i_prev;
  cm_list **head = head_ptr;
  cm_list **tail = tail_ptr;
  cm_list *item = item_ptr;

  cm_list_assert(item != NULL);
  cm_list_assert(head != NULL);
  cm_list_assert(*head != NULL);

  i_next = NEXT(o, item);
  i_prev = PREV(o, item);

  if (i_next != NULL) {
    cm_list_assert(item == PREV(o, i_next));
    PREV(o, i_next) = i_prev;
  }
  if (i_prev != NULL) {
    cm_list_assert(item == NEXT(o, i_prev));
    NEXT(o, i_prev) = i_next;
  }

  /*  The next two "ifs" are usually written as "else"
   *  clauses to the next/prev tests above; but if they
   *  are made dependent on the head/tail match instead,
   *  they work both for doubly linked lists and for rings.
   */
  if (*head == item) *head = i_next;

  if (tail != NULL && *tail == item) *tail = i_prev;

  NEXT(o, item) = NULL;
  PREV(o, item) = NULL;
}

/**
 * @brief Remove an item from the front of a list
 *
 * @param o	list description
 * @param head	pointer to the list's head pointer.
 * @param tail	NULL or a pointer to the list's tail pointer.
 *
 * @return NULL if the list is empty, otherwise the popped item.
 */
cm_list *cm_list_pop_i(cm_list_offsets o, void *head_ptr, void *tail_ptr) {
  void *removed;
  cm_list **head = head_ptr;
  cm_list **tail = tail_ptr;

  cm_list_assert(head != NULL);

  if (*head == NULL) {
    cm_list_assert(tail == NULL || *tail == NULL);
    return NULL;
  }
  removed = *head;
  cm_list_remove_i(o, head, tail, removed);

  return removed;
}

/**
 * @brief Remove an item from the end of a list
 *
 * @param o		list description
 * @param head_ptr	pointer to the list's head pointer.
 * @param tail_ptr	NULL or a pointer to the list's tail pointer.
 *
 * @return NULL if the list is empty, otherwise the unqueued item.
 */
cm_list *cm_list_dequeue_i(cm_list_offsets o, void *head_ptr, void *tail_ptr) {
  cm_list **head = head_ptr;
  cm_list **tail = tail_ptr;
  void *removed;
  cm_list *tail_buf;

  cm_list_assert(head != NULL);

  if (*head == NULL) {
    cm_list_assert(tail == NULL || *tail == NULL);
    return NULL;
  }
  if (tail == NULL) tail = cm_list_long_tail(o, *head, &tail_buf);
  removed = *tail;
  cm_list_remove_i(o, head, tail, removed);

  return removed;
}

/**
 * @brief Add an item to a ring
 *
 *  We push it onto the head of the list and then
 *  add the circularity if this was the first item.
 *
 * @param o	list description
 * @param head	pointer to the list's head pointer.
 * @param item	item to insert at the ring's head.
 */
void cm_ring_push_i(cm_list_offsets o, void *head_ptr, void *item_ptr) {
  cm_list *item = item_ptr;
  cm_list **head = head_ptr;
  cm_list *tail;

  cm_list_assert(head != NULL);
  cm_list_assert(item != NULL);
  cm_list_assert(*head == NULL || (PREV(o, *head) && NEXT(o, *head)));

  tail = *head ? PREV(o, *head) : NULL;
  cm_list_insert_before_i(o, head, &tail, *head, item);

  if (NEXT(o, item) == NULL && PREV(o, item) == NULL) {
    NEXT(o, item) = item;
    PREV(o, item) = item;
  }
}

/**
 * @brief Enqueue an item in a ring
 *
 *  The logic is identical to ring_push except that we
 *  only want to update head if the ring is empty.
 *
 *  By not updating head, the item is effectively appended
 *  at the end.
 *
 * @param o	list description
 * @param head	pointer to the list's head pointer.
 * @param item	item to insert at the ring's head.
 */
void cm_ring_enqueue_i(cm_list_offsets o, void *head_ptr, void *item_ptr) {
  cm_list **head = head_ptr;
  cm_list *item = item_ptr;
  cm_list *fiddle_with_this = *head;

  cm_list_assert(head != NULL);
  cm_list_assert(item != NULL);
  cm_list_assert(*head == NULL || (PREV(o, *head) && NEXT(o, *head)));

  cm_ring_push_i(o, *head ? &fiddle_with_this : head, item);
}

/**
 * @brief Remove an item from a ring.
 *
 * We check for the singular-item case, then remove the item
 *
 * @param o	list descriptor structure
 * @param head	pointer to the list head
 * @param item	list item to remove.
 */
void cm_ring_remove_i(cm_list_offsets o, void *head_ptr, void *item_ptr) {
  cm_list *item = item_ptr;
  cm_list **head = head_ptr;
  cm_list *i_next;
  cm_list *i_prev;

  cm_list_assert(item != NULL);
  cm_list_assert(head != NULL);
  cm_list_assert(*head == NULL || (PREV(o, *head) && NEXT(o, *head)));

  i_next = NEXT(o, item);
  i_prev = PREV(o, item);

  if (item == i_next && i_next == i_prev) {
    NEXT(o, item) = NULL;
    PREV(o, item) = NULL;
  }
  return cm_list_remove_i(o, head, NULL, (cm_list *)item);
}
