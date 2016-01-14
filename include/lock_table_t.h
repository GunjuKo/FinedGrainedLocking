#ifndef __LOCK_TABLE_
#define __LOCK_TABLE_

#include "lock_t.h"
#include "assert.h"
#include <pthread.h>
#include <unistd.h>

/* LockTable class is hash table whose entry points
   to a list of database lock objects hashed to bucket 
   before you access to entry, you must hold entry lock */
class LockTable
{
    private:
        lock_t_list *table;             // Lock table, each entry points
                                        // to a list of lock objects
        pthread_mutex_t *entry_locks;   // protect each entry
        pthread_rwlock_t table_lock;
        int table_size;                 // size of lock table
        
        void lock_x_release(lock_t *lock);  // release exclusive lock
        void lock_s_release(lock_t *lock);  // release shared lock 
    public:
        /* initialize Lock table */
        LockTable(int size)
        {
            table = (lock_t_list *)malloc(sizeof(lock_t_list) * size);
            entry_locks = (pthread_mutex_t *)malloc(
                    sizeof(pthread_mutex_t) * size);

            assert(table != NULL);
            assert(entry_locks != NULL);

            for(int i = 0; i < size; i++){
                init_lock_t_list(&table[i]);                    
                pthread_mutex_init(&entry_locks[i], NULL);
            }
            pthread_rwlock_init(&table_lock, NULL);
            table_size = size;
        }
        int hash_key(int table_id, int record_id);
        bool lock_s_acquire(int table_id, int record_id,
                struct trx_t *trx);
        bool lock_x_acquire(int table_id, int record_id,
                struct trx_t *trx);
        void lock_release(lock_t *lock);
        bool check_dead_lock(lock_t *lock, struct trx_t *me);
};

int LockTable::hash_key(int table_id, int record_id)
{
    int A = (table_id)*(table_size/2);
    int B = (record_id)%table_size;

    return ((A+B)%table_size);
}

/* hold lock in shared mode. if there are any exclusive
   lock in front of lock object, check dead lock and sleep 
   if deadlock detected return false, so trasaction will be aborted 
   if lock is acquired return true */
bool LockTable::lock_s_acquire(int table_id, int record_id,
        struct trx_t *trx)
{
    assert(trx != NULL);
   
    /* create lock object and initialize */
    lock_t *lock = (lock_t *)malloc(sizeof(lock_t));
    init_lock_t(lock, table_id, record_id, SHARED, trx);

    int key = hash_key(table_id, record_id);    // calculate hasy key
    assert(key >= 0 && key < table_size);

    pthread_mutex_lock(&entry_locks[key]);  
    pthread_rwlock_rdlock(&table_lock);   
    
    lock_t_list_append(&table[key], lock);  // insert lock object into 
                                            // list   
    lock_t *prev = lock->prev;
    assert(prev != NULL);

    struct trx_t *transaction = lock->trx;
    assert(transaction == trx);
    
    while(prev != NULL){
        if(prev->lock_state == SENTINEL){  

            // if there is no conflict lock, acquire lock 
            lock->lock_state = HOLDING;             // change lock state
            transaction->trx_locks.push_back(lock); // insert lock into
                                                    //transaction's trx lock
            pthread_rwlock_unlock(&table_lock);
            pthread_mutex_unlock(&entry_locks[key]);
            return true;
        }else if(prev->lock_mode == EXCLUSIVE &&
                    prev->table_id == lock->table_id &&
                    prev->record_id == lock->record_id){
            
            /* if there are exclusive lock in fron of me,
               check dead lock and if dead lock
               is not detected, sleep until wake me up */         
            lock->lock_state = WAITING;
            transaction->wait_lock  = lock;
            transaction->trx_state  = WAIT;
            pthread_rwlock_unlock(&table_lock);
            /* check dead lock */
            bool deadlock = check_dead_lock(lock, transaction);     
            if(deadlock == true){   // deadlock is detected
                pthread_rwlock_rdlock(&table_lock);
                /* remove lock from list and abort transaction */
                lock_t_list_remove(&table[key], lock);
                transaction->trx_state = ABORT;
                transaction->wait_lock = NULL;
                free(lock);
                
                pthread_rwlock_unlock(&table_lock);
                pthread_mutex_unlock(&entry_locks[key]);
                return false;
            }else{                  // deadlock is not detected
                pthread_mutex_unlock(&entry_locks[key]);
                /* wait until wake up */
                sem_wait(&transaction->sem);
                /* wake up that means acquire a lock */
                __sync_synchronize();
                transaction->trx_locks.push_back(lock);
                return true;
            }
        }else{
            prev = prev->prev;
        }
       
    }
}

/* hold lock in exclusive mode. if there are any lock in front of
   lock object, check dead lock and sleep 
   if deadlock detected return false, so transaction will be aborted
   if lock is acquired return true */
bool LockTable::lock_x_acquire(int table_id, int record_id, 
        struct trx_t *trx)
{
    assert(trx != NULL);
    /* create lock object and initialize */
    lock_t *lock = (lock_t *)malloc(sizeof(lock_t));
    init_lock_t(lock, table_id, record_id, EXCLUSIVE, trx);

    int key = hash_key(table_id, record_id);    // calculate hash_key
    assert(key >= 0 && key < table_size);

    pthread_mutex_lock(&entry_locks[key]);
    pthread_rwlock_rdlock(&table_lock);
   
    lock_t_list_append(&table[key], lock);  // insert lock into list

    lock_t *prev = lock->prev;
    assert(prev != NULL);
    
    struct trx_t *transaction = lock->trx;
    assert(transaction == trx);

    while(prev != NULL){
        if(prev->lock_state == SENTINEL){
            /* if there in no lock in front of me, 
             acquire lock */
            lock->lock_state = HOLDING;
            transaction->trx_locks.push_back(lock);
            
            pthread_rwlock_unlock(&table_lock);
            pthread_mutex_unlock(&entry_locks[key]);
            return true;
        }else if((prev->lock_state == HOLDING |
                    prev->lock_state == WAITING) &&
                prev->record_id == lock->record_id &&
                prev->table_id  == lock->table_id){
            // if there are any lock in front of me, conflict!
            // check dead lock and if dead lock is not detected
            // sleep
            lock->lock_state       = WAITING;
            transaction->wait_lock = lock;
            transaction->trx_state = WAIT;  
            
            pthread_rwlock_unlock(&table_lock);
            
            /* check dead lock */
            bool deadlock = check_dead_lock(lock, transaction);

            if(deadlock == true){   // deadlock is detected
                pthread_rwlock_rdlock(&table_lock);
               
                /* remove lock from list */
                lock_t_list_remove(&table[key], lock);
                /* abort transaction */
                transaction->trx_state = ABORT;
                transaction->wait_lock = NULL;
                free(lock);
                
                pthread_rwlock_unlock(&table_lock);
                pthread_mutex_unlock(&entry_locks[key]);
                return false;
            }else{                  // no deadlock
                pthread_mutex_unlock(&entry_locks[key]);
                /* sleep until wake me up */
                sem_wait(&transaction->sem);
                /* wake up that means aquire a lock */
                __sync_synchronize();
                transaction->trx_locks.push_back(lock);
                return true;
            }
        }else{
            prev = prev->prev;
        }
    }
}

/* remove lock object from list. and check there are locks 
   which are waiting for lock release */
void LockTable::lock_release(lock_t *lock)
{
    assert(lock != NULL);
    assert(lock->lock_mode == SHARED || lock->lock_mode == EXCLUSIVE);
    if(lock->lock_mode == SHARED){
        // shared lock release
        lock_s_release(lock);       
    }else{
        // exclusive lock release
        lock_x_release(lock);
    }
}
/* release shared lock */
void LockTable::lock_s_release(lock_t *lock)
{
    int key = hash_key(lock->table_id, lock->record_id);
    assert(key >= 0 && key < table_size);

    pthread_mutex_lock(&entry_locks[key]);
    pthread_rwlock_rdlock(&table_lock);

    lock_t *previous = lock->prev;
    lock_t *pNext    = lock->next;
    /* remove lock from list */
    lock_t_list_remove(&table[key], lock); 
    assert(pNext != NULL);
    assert(previous != NULL);
    while(pNext->lock_state != SENTINEL){
        if(pNext->lock_mode == EXCLUSIVE 
                && pNext->table_id == lock->table_id
                && pNext->record_id == lock->record_id){
            /* if there are any shared lock in front of lock object
               do not wake up exclusive lock */
            assert(pNext->lock_state == WAITING);

            bool s = false; // if there are any shared lock before
                            // lock object, don't wake up any lock
            while(previous->lock_state != SENTINEL){
                if(previous->lock_mode == SHARED &&
                        previous->table_id == lock->table_id &&
                        previous->record_id == lock->record_id){
                    s = true;
                    break;
                }else{
                    previous = previous->prev;
                }
            }
            if(!s){
                // if there isn't any shared lock object, wake up
                // lock object
                struct trx_t *transaction = pNext->trx;
                assert(transaction != NULL);
                // wake up exclusive lock object
                pNext->lock_state = HOLDING;
                transaction->trx_state = RUN;
                transaction->wait_lock = NULL;
                __sync_synchronize();           
                sem_post(&(transaction->sem));   // wake up 
                break;
            }else{
                // if there is shared lock object in front of me
                // not wake up...
                break;
            }
        }else if(pNext->lock_mode == SHARED
                &&pNext->table_id == lock->table_id
                &&pNext->record_id == lock->record_id){
            // if there are another shared lock, don't wake up any lock
            // object
            assert(pNext->lock_state == HOLDING);
            break;
        }else{
            pNext = pNext->next;
        }
    }
    free(lock);     // free lock object
    pthread_rwlock_unlock(&table_lock);
    pthread_mutex_unlock(&entry_locks[key]);
}
/* release exclusive lock */
void LockTable::lock_x_release(lock_t *lock)
{
    int key = hash_key(lock->table_id, lock->record_id);
    assert(key >= 0 && key < table_size);

    pthread_mutex_lock(&entry_locks[key]);
    pthread_rwlock_rdlock(&table_lock);
    
    lock_t *pNext    = lock->next;
    lock_t *previous = lock->prev;
    /* remove lock from list */
    lock_t_list_remove(&table[key], lock);
    
    assert(previous != NULL);
    assert(pNext != NULL);
    
    while(pNext->lock_state != SENTINEL){
        if(pNext->lock_mode == SHARED
                && pNext->table_id  == lock->table_id
                && pNext->record_id == lock->record_id){
            
            // if there are shared lock objects, wake up
            // wake up all shared lock until exclusive lock.
            while(pNext->lock_state != SENTINEL){
                
                if(pNext->lock_mode == SHARED
                        && pNext->table_id == lock->table_id
                        && pNext->record_id == lock->record_id){
                
                    struct trx_t *transaction = pNext->trx;
                    assert(pNext->lock_state == WAITING);
                    assert(transaction != NULL);
                    /* wake up, shared lock */
                    pNext->lock_state = HOLDING;
                    transaction->trx_state = RUN;
                    transaction->wait_lock = NULL;
                    __sync_synchronize();
                    sem_post(&(transaction->sem)); // wake up
                    /* goto next lock */
                    pNext = pNext->next;
                }else if(pNext->lock_mode == EXCLUSIVE
                        && pNext->table_id == lock->table_id
                        && pNext->record_id == lock->record_id){
                    assert(pNext->lock_state == WAITING);
                    // don't wake up and finish while loop
                    break;
                }else{
                    pNext = pNext->next;
                }               
            }
            break;  // finish loop     
        }else if(pNext->lock_mode == EXCLUSIVE
                && pNext->table_id == lock->table_id
                && pNext->record_id == lock->record_id){
            /* if next lock is exclusive lock, wake up */
            assert(pNext->lock_state == WAITING);
           
            struct trx_t *transaction = pNext->trx;
            assert(transaction != NULL);       
           
            /* wake up exclusive lock */
            pNext->lock_state = HOLDING;
            transaction->trx_state = RUN;
            transaction->wait_lock = NULL;
            
            __sync_synchronize();
            sem_post(&(transaction->sem));  // wake up 
            break;
        }else{
            pNext = pNext->next;
        }
    }
    __sync_synchronize();
    free(lock);
    pthread_rwlock_unlock(&table_lock);
    pthread_mutex_unlock(&entry_locks[key]);
}
/* check dead lock is stop the world function
 while check dead lock, any thread can't access lock table */
bool LockTable::check_dead_lock(lock_t* lock, struct trx_t* me)
{
    /* acquire table_lock in exclusive mode */
    pthread_rwlock_wrlock(&table_lock);
    assert(lock != NULL);
    assert(lock->lock_state != NONE && lock->lock_state != SENTINEL);

    lock_t *waiting_lock = lock;
    lock_t *p            = waiting_lock;
    struct trx_t *transaction;
    while(waiting_lock->lock_state != SENTINEL){
        if(waiting_lock->lock_state == HOLDING
                &&waiting_lock->table_id == p->table_id
                &&waiting_lock->record_id == p->record_id){
            
            // check transaction which holding the lock is RUNNING
            transaction = waiting_lock->trx;
            if(transaction == me){
                // dead lock detected
                pthread_rwlock_unlock(&table_lock);
                return true;
            }
            
            if(transaction->trx_state == WAITING){
                // transaction is WAITING 
                // retry
                waiting_lock = transaction->wait_lock;
                p            = waiting_lock;
                continue;
            }else{
                // transacion is RUNNING or ABORT
                pthread_rwlock_unlock(&table_lock);
                return false;
            }
        }else{
            // go previous node to find HOLDING state lock 
            waiting_lock = waiting_lock->prev;
            assert(waiting_lock->lock_state != SENTINEL);
        }
    }
}

#endif
