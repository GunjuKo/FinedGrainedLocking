#ifndef __TRX_T_
#define __TRX_T_

#include "lock_t.h"
#include <vector>
#include <pthread.h>
#include <semaphore.h>
#include <assert.h>

using namespace std;

enum transaction_state {RUN, WAIT, ABORT, IDLE};

struct trx_t
{
    unsigned long trx_id;               // current trasaction id assigned
                                        // from the global count

    vector<struct lock_t *> trx_locks;  // A list of database locks this
                                        // trx is holding

    enum transaction_state trx_state;   // RUNNING, WAITING, IDLE
    
    pthread_mutex_t trx_mutex;          // mutex protecting trx struct
    sem_t sem;                          // This is needed to sleep/wakeup
    
    lock_t *wait_lock;                  // This is the lock object that
                                        // this transaction is waiting
};

void init_trx(struct trx_t *t, unsigned long trx_id)
{
    assert(t != NULL);
    
    /* initialize variable */
    t->trx_id    = trx_id;
    t->trx_state = RUN;
    t->wait_lock = NULL;
    t->trx_locks.clear();
    pthread_mutex_init(&(t->trx_mutex), NULL);
    sem_init(&(t->sem), 0, 0);  // semaphore's value is 0
}

#endif
