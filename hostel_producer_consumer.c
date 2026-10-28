/*
 * Hostel Room Cleaning Request Simulator
 * Producer-Consumer (Bounded Buffer) using POSIX threads and semaphores
 *
 * Producers = students who post cleaning requests on a notice board
 * Consumers = housekeeping staff who pick requests and clean rooms
 * Buffer    = notice board with BOARD_SIZE slots (circular queue)
 *
 * Compile: gcc hostel_producer_consumer.c -o hostel -pthread
 * Run    : ./hostel
 */
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>

#define BOARD_SIZE   5      /* capacity of the notice board          */
#define NUM_STUDENTS 3      /* producer threads                      */
#define NUM_STAFF    2      /* consumer threads                      */
#define REQ_PER_STUDENT 4   /* requests each student posts           */
/* Total requests must divide evenly among staff so every thread ends */
#define TOTAL_REQUESTS (NUM_STUDENTS * REQ_PER_STUDENT)
#define REQ_PER_STAFF  (TOTAL_REQUESTS / NUM_STAFF)

typedef struct {
    int id;
    int room;
    const char *task;
} Request;

static const char *TASKS[] = {"Sweep", "Mop floor", "Clean bathroom",
                              "Empty bin", "Dust shelves", "Change linen"};
static const char *STUDENT_NAMES[] = {"Aarav", "Diya", "Rohan", "Meera", "Kabir"};
static const char *STAFF_NAMES[]   = {"Raju", "Sunita", "Ganesh", "Lata", "Imran"};

Request board[BOARD_SIZE];   /* shared buffer */
int in = 0, out = 0;         /* circular queue pointers */
int next_id = 1;             /* request id counter (protected by mutex) */

sem_t empty_slots;           /* counts free slots on the board    */
sem_t full_slots;            /* counts requests waiting on board  */
sem_t mutex;                 /* mutual exclusion on board access  */

int count_on_board(void) {   /* only called inside the critical section */
    int n;
    sem_getvalue(&full_slots, &n);
    return n;
}

void *student(void *arg) {
    int sid = (int)(long)arg;
    for (int i = 0; i < REQ_PER_STUDENT; i++) {
        usleep((rand() % 600 + 200) * 1000);          /* writing the request */

        Request r;
        r.room = (rand() % 4 + 1) * 100 + rand() % 24 + 1;
        r.task = TASKS[rand() % 6];

        sem_wait(&empty_slots);                       /* wait for a free slot */
        sem_wait(&mutex);                             /* lock the board       */

        r.id = next_id++;
        board[in] = r;
        printf("[STUDENT %-6s] posted  R%-2d : %-14s room %d  (slot %d)\n",
               STUDENT_NAMES[sid], r.id, r.task, r.room, in);
        in = (in + 1) % BOARD_SIZE;

        sem_post(&mutex);                             /* unlock the board     */
        sem_post(&full_slots);                        /* one more request     */
        fflush(stdout);
    }
    return NULL;
}

void *staff(void *arg) {
    int sid = (int)(long)arg;
    for (int i = 0; i < REQ_PER_STAFF; i++) {
        sem_wait(&full_slots);                        /* wait for a request   */
        sem_wait(&mutex);                             /* lock the board       */

        Request r = board[out];
        printf("[STAFF   %-6s] took    R%-2d : %-14s room %d  (slot %d)\n",
               STAFF_NAMES[sid], r.id, r.task, r.room, out);
        out = (out + 1) % BOARD_SIZE;

        sem_post(&mutex);                             /* unlock the board     */
        sem_post(&empty_slots);                       /* one slot freed       */
        fflush(stdout);

        usleep((rand() % 800 + 400) * 1000);          /* cleaning the room    */
        printf("[STAFF   %-6s] cleaned R%-2d : room %d done\n",
               STAFF_NAMES[sid], r.id, r.room);
        fflush(stdout);
    }
    return NULL;
}

int main(void) {
    pthread_t students[NUM_STUDENTS], crew[NUM_STAFF];
    srand(42);

    sem_init(&empty_slots, 0, BOARD_SIZE);   /* empty = N */
    sem_init(&full_slots,  0, 0);            /* full  = 0 */
    sem_init(&mutex,       0, 1);            /* mutex = 1 */

    printf("=== Hostel Cleaning Desk: %d students, %d staff, board size %d ===\n\n",
           NUM_STUDENTS, NUM_STAFF, BOARD_SIZE);

    for (long i = 0; i < NUM_STUDENTS; i++)
        pthread_create(&students[i], NULL, student, (void *)i);
    for (long i = 0; i < NUM_STAFF; i++)
        pthread_create(&crew[i], NULL, staff, (void *)i);

    for (int i = 0; i < NUM_STUDENTS; i++) pthread_join(students[i], NULL);
    for (int i = 0; i < NUM_STAFF; i++)    pthread_join(crew[i], NULL);

    sem_destroy(&empty_slots);
    sem_destroy(&full_slots);
    sem_destroy(&mutex);

    printf("\n=== All %d requests posted and cleaned. ===\n", TOTAL_REQUESTS);
    return 0;
}
