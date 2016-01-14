#ifndef __LOCK_T_
#define __LOCK_T_

#include "trx_t.h"
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>

/* HOLDING, RELEASE, WATING is state of lock 
   HOLDING means lock is holding so enter critical section
   RELEASE means lock is released
   SENTINEL means sentinel node of list
   WAITING means waiting another lock, waiting is implemented
   using sleep/wake up. */
#define HOLDING  0   
#define WAITING  1   
#define NONE     2  // state is not decided yet
#define SENTINEL 3  // list head or tail 

/* There are two mode of lock, shared or exclusive */
#define SHARED 5
#define EXCLUSIVE 6

struct lock_t{
    unsigned long table_id;  // database table id
    unsigned long record_id; // database record id
  
    struct trx_t *trx;       // backpointer to the lock holder
    int lock_state;          // marker to represent the state of a lock
    int lock_mode;           // shared or exclusive

    struct lock_t *next;     // next lock pointer in a list
    struct lock_t *prev;     // previous lock pointer in a list
};

/* This structure is used for implement list.
   Hash bucket use this list structure to implement lock list. */

   struct lock_t_list{
    struct lock_t head;      // sentinel node, first node of a list
    struct lock_t tail;      // sentinel node, last node of a list
};

void init_lock_t(lock_t *lock, int table_id, int record_id, 
        int lock_mode, struct trx_t *trx);
void init_lock_t_list(struct lock_t_list *list);
void lock_t_list_append(struct lock_t_list *list, struct lock_t *node);
void lock_t_list_remove(struct lock_t_list *list, struct lock_t *node);


/* initialize lock */
void init_lock_t(lock_t *lock, int table_id, int record_id, 
        int lock_mode, struct trx_t* trx)
{
    assert(lock != NULL);
    
    /* initialize the new_lock */
    lock->table_id   = table_id;
    lock->record_id  = record_id;
    lock->lock_mode  = lock_mode ;
    lock->lock_state = NONE;
    lock->trx        = trx;
    lock->next       = NULL;
    lock->prev       = NULL;
}


/* initialize lock_t_list 
   head's next pointer points tail and 
   tail's previous pointer points head */
void init_lock_t_list(struct lock_t_list *list)
{
    assert(list != NULL);
  
    list->head.table_id   = 0;
    list->head.record_id  = 0;
    list->head.trx        = NULL;
    list->head.lock_state = SENTINEL;
    list->head.lock_mode  = 0;

    list->head.prev = NULL;         // list head's prev is NULL
    list->head.next = &(list->tail);// list head's next points tail 

    list->tail.table_id   = 0;
    list->tail.record_id  = 0;
    list->tail.trx        = NULL;
    list->tail.lock_state = SENTINEL;
    list->tail.lock_mode  = 0;

    list->tail.next = NULL;         // list tail's next is NULL
    list->tail.prev = &(list->head);// list tail's prev points head
}

/* insert node into list in front of tail */
void lock_t_list_append(struct lock_t_list *list, struct lock_t *node)
{
    assert(list != NULL);
    assert(node != NULL);
    /* insert node into list */
    node->next = &(list->tail);
    node->prev = list->tail.prev;

    list->tail.prev->next = node;
    list->tail.prev = node;
}

/* remove node from list */
void lock_t_list_remove(struct lock_t_list *list, struct lock_t *node)
{
    assert(list != NULL);
    assert(node != NULL);
    assert(node->next != NULL);
    assert(node->prev != NULL);
    
    node->prev->next = node->next;
    node->next->prev = node->prev;   
}

#endif
