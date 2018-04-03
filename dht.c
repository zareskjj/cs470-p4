/**
 * dht.c
 *
 * CS 470 Project 4
 *
 * Implementation for distributed hash table (DHT).
 *
 * Name: Jason Zareski 
 */

#include <mpi.h>
#include <pthread.h>
#include "dht.h"

const int PUT = 1;
const int GET = 2;
const int GET_RETURN_VALUE = 3;
const int CONFIRM = 4;
const int TERM = 5;

/*
 * Private module variable: current process ID
 */
static int pid;
int rank;
int mpi_size;
int lsize;

int confirmed;
int val_received;
long get_val;

pthread_mutex_t confirm_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t confirm_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t get_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t get_cond = PTHREAD_COND_INITIALIZER;
pthread_t server_thread;


typedef struct dht_msg msg_t;
struct dht_msg {
    char s[MAX_KEYLEN];
    long value;
};

msg_t blank_msg() {
    msg_t msg;
    memset(&msg, 0, sizeof(msg_t));
    return msg;
}

void *server(void *ptr) {
  while(1) {
    MPI_Status status;
    msg_t msg = blank_msg();
    printf("R%d: Server Thread waiting on messge.\n", rank);
    MPI_Recv(&msg, sizeof(msg_t), MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    printf("R%d: Received message with tag (%d), from R%d\n", rank, status.MPI_TAG, status.MPI_SOURCE);
    if (status.MPI_TAG == PUT)
    {
        //Put into local table
        local_put(msg.s, msg.value);
        
        msg_t ret = blank_msg();
        ret.value = CONFIRM; //Not really using message here

        MPI_Send(&ret, sizeof(msg_t), MPI_BYTE, status.MPI_SOURCE, CONFIRM, MPI_COMM_WORLD);
        printf("R%d: Confirmation sent!\n", rank);
    } 
    else if (status.MPI_TAG == GET)
    {
        msg_t send_get_value = blank_msg();
        long val = local_get(msg.s);
        send_get_value.value = val;
        MPI_Send(&send_get_value, sizeof(msg_t), MPI_BYTE, status.MPI_SOURCE, GET_RETURN_VALUE, MPI_COMM_WORLD);
    }
    else if (status.MPI_TAG == GET_RETURN_VALUE)
    {
        pthread_mutex_lock(&get_lock);
        val_received = 1;
        get_val = msg.value;
        pthread_mutex_unlock(&get_lock);
        pthread_cond_signal(&get_cond);
    }
    else if (status.MPI_TAG == CONFIRM)
    {
        confirmed = 1;
        pthread_mutex_unlock(&confirm_lock);
        printf("R%d: PUT CONFIRMED, LOCK RELEASED\n", rank);
        pthread_cond_signal(&confirm_cond);
    }
    else if (status.MPI_TAG == TERM)
    {
        printf("Exiting thread on rank %d\n", rank);
        pthread_exit(NULL);
    }
    else
    {
        printf("Status source: %d\n", status.MPI_SOURCE);
        printf("Status tag: %d\n", status.MPI_TAG);
        printf("Error on server thread receiving message... Incorrect message parameters.\n");
        exit(EXIT_FAILURE);
    }
  }
}

void put_send(int dest, const char *key, long value) {
  msg_t msg = blank_msg(); 
  strcpy(msg.s, key); // Do I need to make secure for buffer overflow?
  msg.value = value;
  pthread_mutex_lock(&confirm_lock);
  printf("R%d: SEND PUT REQ LOCK ACQUIRED\n", rank);
  MPI_Send(&msg, sizeof(msg_t), MPI_BYTE, dest, PUT, MPI_COMM_WORLD);
  return;
}

void get_send(int src_node, const char *key) {
  msg_t msg = blank_msg();
  strcpy(msg.s, key);
  MPI_Send(&msg, sizeof(msg_t), MPI_BYTE, src_node, GET, MPI_COMM_WORLD);
  return;
}

int hash(const char *s) 
{
  unsigned hash = 5381;
  while (*s != '\0')
  {
    hash = ((hash << 5) + hash) + (unsigned)(*s++);
  }
  return hash % mpi_size;
}

int dht_init()
{
    int p;
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &p);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

    /* ERRORS RETURN FROM MPI CALLS */
    MPI_Errhandler_set(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

    confirmed = 0;
    val_received = 0;

    if (p != MPI_THREAD_MULTIPLE) 
    {
      printf("ERROR: Cannot initialize MPI in THREAD_MULTIPLE mode.\n");
      exit(EXIT_FAILURE);
    }
    local_init();
    pid = rank;

    pthread_create(&server_thread, NULL, server, NULL);
    // do I need to pass a param to the server here (last param)?
    return pid;
}

void dht_put(const char *key, long value)
{
  int dest_node = hash(key);
  put_send(dest_node, key, value);
  printf("R%d: PUT {%s:%ld} ON %d.\n", rank, key, value, dest_node);
  pthread_mutex_lock(&confirm_lock);
  printf("R%d: LOCK ACQUIRED\n", rank);
  while (!confirmed)
  {
    pthread_cond_wait(&confirm_cond,&confirm_lock);
  }
  confirmed = 0;
  printf("R%d: PUT {%s:%ld} confirmed.\n", rank, key, value);
  pthread_mutex_unlock(&confirm_lock);
  return;
}

long dht_get(const char *key)
{
    int src_node = hash(key);
    long value = 0;
    //Tell the node holding the value to send it (get_send)
    get_send(src_node, key);
    pthread_mutex_lock(&get_lock);
    while (!val_received)
    {
        pthread_cond_wait(&get_cond, &get_lock);
    }
    value = get_val;
    val_received = 0;
    pthread_mutex_unlock(&get_lock);
    //Receive the value from the node src_node
    return value;//local_get(key);
}

size_t dht_size()
{
    return local_size();
}

void dht_sync()
{
    // nothing to do in the serial version
}

void dht_destroy(FILE *output)
{
    local_destroy(output);
    pthread_join(server_thread, NULL);
    MPI_Finalize();
}

