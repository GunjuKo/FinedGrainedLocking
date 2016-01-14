#include <getopt.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <boost/atomic.hpp>
#include <vector>
#include <pthread.h>
#include "lock_t.h"
#include "trx_t.h"
#include "lock_table_t.h"

#define LOCK_TABLE_SIZE 1000        // size of Lock table
#define RECORD_INIT_VALUE 100000    // record's initial value

using namespace std;

/* record */
struct record{
    long int transaction_id;    // record's last update transaction id
    long int value;             // record's value
};

LockTable *lockTable;

unsigned int num_thread = sysconf(_SC_NPROCESSORS_ONLN);     
unsigned int duration   = 30;       // 30 second
unsigned int table_size = 10000;    
unsigned int read_num   = 10;

struct record *table_0; // record_table
struct record *table_1; // record_table


bool running            = true;

boost::atomic_int transaction_id(1);
boost::atomic_int read_throughput(0);
boost::atomic_int update_throughput(0);
boost::atomic_int abort_transaction(0);

void *transaction_func(void *data)
{
    int read = read_num;
    int update = 10 - read_num;
    bool succ;  // if acquire lock, succ is true
                // if fail to acquire lock, succ is false
    while(running){
        int trx_id = transaction_id++;
        struct trx_t transaction;
        init_trx(&transaction, trx_id); // initialize transaction 
    
        int record_k = (rand() % table_size) - 10;  
        if(record_k < 0){
            record_k = 0;
        }
        
        int summation = 0;

        for(int i = 0; i < read; i++){
            succ = lockTable->lock_s_acquire(0, record_k + i, &transaction);
            if(!succ){
                // dead lock detected, abort transaction
                abort_transaction++;
                goto release;
            }else{
                // success lock in shared mode, read value
                summation += table_0[record_k + i].value;               
            }
           
            succ = lockTable->lock_s_acquire(1, record_k + i, &transaction);
            if(!succ){
                // dead lock detected, abort transaction
                abort_transaction++;
                goto release;
            }else{
                // success lock in shared mode
                summation += table_1[record_k + i].value;
            }
            read_throughput++;  // increase read_throughput
        }

        for(int i = 0; i < update; i++){
            succ = lockTable->lock_x_acquire(0, record_k + read + i,
                    &transaction);

            if(!succ){
                // dead lock detected, abort transaction
                abort_transaction++;
                goto release;
            }

            succ = lockTable->lock_x_acquire(1, record_k + read + i,
                    &transaction);
            if(!succ){
                // dead lock detected, abort transaction
                abort_transaction++;
                goto release;
            }
            
            // if acquire lock table0 and table1 in exclusive mode,
            // update value
            if(record_k % 2 == 0){

                // if acquire lock table1 and table0 in exclusive mode,
                // update value
                table_0[record_k + read + i].value = 
                    table_0[record_k + read +i].value + 10;
                
                table_1[record_k + read + i].value = 
                    table_1[record_k + read + i].value - 10;
            }else{

                /* transfer value from table_0 to table_1 */
                table_0[record_k + read + i].value =
                    table_0[record_k + read + i].value - 10;
                
                table_1[record_k + read + i].value = 
                    table_1[record_k + read + i].value + 10;
            }
            /* update record's transaction_id */
            table_0[record_k + read + i].transaction_id = trx_id;
            table_1[record_k + read + i].transaction_id = trx_id;
            update_throughput++;
        }

        /* release all locks(the lock shrinking phase) */
release: 
        int size = transaction.trx_locks.size();
        for(int i = 0; i < size; i++){
            lock_t *release_lock = transaction.trx_locks[i];
            lockTable->lock_release(release_lock);
        }
    }
    
}


int main(int argc, char* argv[])
{
    int opt;
    int index;

    srand(time(NULL));
    
    lockTable = new LockTable(LOCK_TABLE_SIZE);     // create lock table

    struct option options[]={
        {"num_thread", 1, 0, 0},    // option 0
        {"duration"  , 1, 0, 0},    // option 1
        {"table_size", 1, 0, 0},    // option 2
        {"read_num"  , 1, 0, 0}     // option 3
    };

    while(1){
        opt = getopt_long(argc, argv, "n:d:t:r:", options, &index);

        if(opt == -1){
            break;
        }

        switch(opt){
            case 0:
                switch(index){
                    case 0:     // num_thread
                        num_thread = (unsigned int)atoi(optarg);
                        break;
                    case 1:     // duration
                        duration   = (unsigned int)atoi(optarg);
                        break;
                    case 2:     // table_size
                        table_size = (unsigned int)atoi(optarg);
                        break;
                    case 3:     // read_num
                        read_num   = (unsigned int)atoi(optarg);
                        break;
                }
                break;
            case 'n':           // num_thread
                num_thread = (unsigned int)atoi(optarg);
                break;
            case 'd':           // duration
                duration   = (unsigned int)atoi(optarg);
                break;
            case 't':           // table_size
                table_size = (unsigned int)atoi(optarg);
                break;
            case 'r':           // read_num
                read_num   = (unsigned int)atoi(optarg);
                break;
        }
    }
    
    /* create record table */
    table_0 = (struct record *)malloc(sizeof(struct record) * table_size);
    table_1 = (struct record *)malloc(sizeof(struct record) * table_size);

    for(int i = 0; i < table_size; i++){
        /* intialize record table */
        table_0[i].value = RECORD_INIT_VALUE;
        table_1[i].value = RECORD_INIT_VALUE;
    }

    pthread_t* threads = (pthread_t*)malloc(sizeof(pthread_t)
            * num_thread);
    
    assert(threads != NULL);
    
    for(int i = 0; i < num_thread; i++){
        pthread_create(&threads[i], NULL, transaction_func, NULL);       
    }
    
    assert(duration > 0);
    /* sleep */
    sleep(duration);
    __sync_synchronize();
    
    running = false;    // finish all transaction thread

    for(int i = 0; i < num_thread; i++){
        pthread_join(threads[i], NULL);
    }
    int read = read_throughput;
    int update = update_throughput;
    int transaction = transaction_id;
    int abort_time = abort_transaction;

    printf("READ throughput: %d READS and %d READS/sec\n",
            read, read/duration);
    
    printf("UDATE throughput: %d UPDATES and %d UPDATE/sec\n",
            update, update/duration);

    printf("Transaction thoughput: %d trx and %d trx/sec\n",
            transaction, transaction/duration);

    printf("Aborted transaction: %d aborts and %d aborts/sec\n",
            abort_time, abort_time/duration);

    long int sum = 0;
    /* check data race is occured */
    for(int i = 0; i < table_size; i++){
        sum = sum + table_0[i].value + table_1[i].value;
    }
    if(sum == (table_size * RECORD_INIT_VALUE * 2)){
        printf("Correct!\n");
    }else{
        printf("Data race occur...\n");
    }
    return 0;
}

