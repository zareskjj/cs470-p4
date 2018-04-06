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

/* Server message tags */
const int PUT = 1;
const int GET = 2;
const int CONFIRM = 3;
const int SIZE = 5;
const int TERM = 6;

/* Client message tags */
const int GET_RETURN_VALUE = 10;
const int TOTALSZ = 6;

/*
 * Private module variable: current process ID
 */
static int pid;
int rank;
int mpi_size;
int lsize;

int confirmed;

/**
 * Pthread mutual exclusion utilities for put confirmations
 */
pthread_mutex_t confirm_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t confirm_cond = PTHREAD_COND_INITIALIZER;
pthread_t server_thread;


/**
 * Message struct used for the messages on the client and server threads.
 */
typedef struct dht_msg msg_t;
struct dht_msg
{
    char s[MAX_KEYLEN];
    long value;
};


/**
 *  Helper function for creating the message struct and zeroing out the message before use.
 */
msg_t blank_msg()
{
    msg_t msg;
    memset(&msg, 0, sizeof(msg_t));
    return msg;
}

/**
 * MPI Recv wrapper function, receives from any source a message with the given tag.
 */
void tag_recv(msg_t *msg, int tag, MPI_Status *recv_st)
{
    MPI_Recv(msg, sizeof(msg_t), MPI_BYTE, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, recv_st);
}

/**
 * Handles all messages with server tags
 *
 * MPI Probe is like a peek on a queue, and will get the message status, which includes
 * the pending message's source and tag.
 *
 * Using the source and tag, the server decides whether or not to accept the pending message
 * in an MPI_Recv call
 */
void *server(void *ptr)
{
    /* Loop until thread receives a termination message */
    while(1) {
        MPI_Status status;
        MPI_Status recv_st;
        msg_t msg = blank_msg();
        
        // Peeks at the queue of messages for status, containing the source, tag, and other message information
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        /**
         * SERVER WILL ONLY ACCEPT MESSAGES WITH TAGS:
         *  PUT
         *  GET
         *  CONFIRM
         *  TERM
         *  SIZE
         * All other tags will be processed by the client thread.
         */
        if (status.MPI_TAG == PUT)
        {
            /**
             * Pending message has a PUT tag, put the message's key and value pair into local table
             *
             * One server thread per node, only one message per processing.  The local_put will not have
             * conflicts placing this key value pair into the local table.
             */

            // Use helper method to receive the message with the PUT tag, saves the status
            tag_recv(&msg, PUT, &recv_st);
            MPI_Request req;
            local_put(msg.s, msg.value);
            
            // Dummy message and value
            msg_t ret = blank_msg();
            ret.value = CONFIRM; 
            
            // Sending to own process was sometimes blocking, this stopped that
            if (status.MPI_SOURCE != rank) 
            {
                MPI_Send(&ret, sizeof(msg_t), MPI_BYTE, status.MPI_SOURCE, CONFIRM, MPI_COMM_WORLD);
            }
            else 
            {
                MPI_Isend(&ret, sizeof(msg_t), MPI_BYTE, status.MPI_SOURCE, CONFIRM, MPI_COMM_WORLD, &req);
            }
        } 
        else if (status.MPI_TAG == GET)
        {
            tag_recv(&msg, GET, &recv_st);
            MPI_Request req;

            // Process the get message, and prepare new message with value received by local table
            msg_t send_get_value = blank_msg();
            long val = local_get(msg.s);
            send_get_value.value = val;

            // Stopped blocking when sending to self
            if (status.MPI_SOURCE != rank)
            {
                MPI_Send(&send_get_value, sizeof(msg_t), MPI_BYTE, status.MPI_SOURCE, GET_RETURN_VALUE, MPI_COMM_WORLD);
            }
            else
            {
                MPI_Isend(&send_get_value, sizeof(msg_t), MPI_BYTE, status.MPI_SOURCE, GET_RETURN_VALUE, MPI_COMM_WORLD, &req);
            }
        }
        else if (status.MPI_TAG == CONFIRM)
        {
            // Receive confirmation message from put, signal client thread
            tag_recv(&msg, CONFIRM, &recv_st);
            confirmed = 1;
            pthread_mutex_unlock(&confirm_lock);
            pthread_cond_signal(&confirm_cond);
        }
        else if (status.MPI_TAG == TERM)
        {
            // Received the terminate message from the client, shut down the server thread.
            // MPI messages are processed in order, terminate will be the last message server processes.
            tag_recv(&msg, TERM, &recv_st);
            pthread_exit(NULL);
        }
        else if (status.MPI_TAG == SIZE)
        {
            // Received a request for the size of the DHT, all server threads will come to the MPI_Reduce call and give the root process the total
            // Root process sends result to client thread
            tag_recv(&msg, SIZE, &recv_st);
            long localsz = 0;
            localsz = local_size();
            long totalsz = 0;
            MPI_Reduce(&localsz, &totalsz, 1, MPI_LONG, MPI_SUM, recv_st.MPI_SOURCE, MPI_COMM_WORLD);
            //Only have root process send result to self for result
            if (rank == recv_st.MPI_SOURCE)
            {
                MPI_Send(&totalsz, 1, MPI_LONG, recv_st.MPI_SOURCE, TOTALSZ, MPI_COMM_WORLD);
            }
        } 
    }
}

void put_send(int dest, const char *key, long value)
{
    // Initialize a blank message for new key and value pair
    msg_t msg = blank_msg(); 
    // Copy the new key and value into the struct
    strcpy(msg.s, key);
    msg.value = value;
    
    // Send new key and value pair to the destination's server thread
    MPI_Send(&msg, sizeof(msg_t), MPI_BYTE, dest, PUT, MPI_COMM_WORLD);
}

long get_send(int src_node, const char *key)
{
    MPI_Status status;

    // Initialize blank message structs for sending and receiving
    msg_t msg = blank_msg();
    msg_t ret = blank_msg();
    // Put the key into the message struct for the src node to use for a local
    // get command
    strcpy(msg.s, key);

    // Send request for value from get command key to the node
    // the hash function determined would store the key and value
    MPI_Sendrecv(&msg, sizeof(msg_t), MPI_BYTE, src_node, GET,
                 &ret, sizeof(msg_t), MPI_BYTE, src_node, GET_RETURN_VALUE, MPI_COMM_WORLD, &status);
    return ret.value;
}

/**
 * Given hash function for this project
 */
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
    // Initialize MPI with multithreading capabilities
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &p);
    
    // Rank and Size set to globals
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

    // Flag for confirmation of put command (0=unconfirmed, 1=confirmed)
    confirmed = 0;

    // Make sure MPI initialized in an environment for multithreading support
    if (p != MPI_THREAD_MULTIPLE) 
    {
      printf("ERROR: Cannot initialize MPI in THREAD_MULTIPLE mode.\n");
      exit(EXIT_FAILURE);
    }
    // Initialize local data structures
    local_init();
    // Process ID is the process's MPI rank
    pid = rank;

    // Start server thread (infinite loop, until termination message received)
    pthread_create(&server_thread, NULL, server, NULL);
    return pid;
}

void dht_put(const char *key, long value)
{
    // determines which node this k,v pair will placed on
    int dest_node = hash(key);

    // Use helper method to send the k,v pair onto the correct node
    put_send(dest_node, key, value);

    // Using a pthread conditional wait to process when the send completes
    // Node X (this node) sends to Y (dest_node),
    // Y server puts new k,v into table,
    // Y server sends confirmation message to X's server thread,
    // X server thread signals condition on X client
    pthread_mutex_lock(&confirm_lock);
    // double check signaling
    while (!confirmed)
    {
        // release lock until condition is signaled, then take lock again
        // loop until lock is grabbed and put is confirmed by server thread
        pthread_cond_wait(&confirm_cond,&confirm_lock);
    }
    // Set flag back to unconfirmed.
    confirmed = 0;
    // Release lock for more put commands
    pthread_mutex_unlock(&confirm_lock);
}

long dht_get(const char *key)
{
    // Find the node that this key will be on if exists
    int src_node = hash(key);
    // Use helper method to send this key in the message to the node
    long value = get_send(src_node, key);

    // Will be the value found at that key or "Key not found"
    return value;
}

size_t dht_size()
{
    int total = 0;
    // Prepare requests for wait
    MPI_Request reqs[mpi_size];
    // Dummy message for simplicity
    msg_t m = blank_msg();
    // Send to all server threads a request to calculate their size, and reduce on this rank as root
    for (int i = 0; i < mpi_size; i++) 
    {
        MPI_Isend(&m, sizeof(msg_t), MPI_BYTE, i, SIZE, MPI_COMM_WORLD, &reqs[i]);
    }
    // Wait for all sends to compelete before waiting on receive
    MPI_Waitall(mpi_size, reqs, MPI_STATUSES_IGNORE);
    
    // Receive the total from won process's server thread
    MPI_Status status;
    MPI_Recv(&total, 1, MPI_LONG, rank, TOTALSZ, MPI_COMM_WORLD, &status);
    return total;
}

void dht_sync()
{
    // Forces clients to freeze until all other client's call their implicit sync.
    MPI_Barrier(MPI_COMM_WORLD);
}

void dht_destroy(FILE *output)
{
    //Wait for all to reach destroy before initiating
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Request req;
    // Send output
    local_destroy(output);
    // Send a dummy message value
    msg_t m = blank_msg();
    // Send non-blocking message indicating server thread ending
    // This message wil not cancel any pending messages, as messages sent to a proces
    // are processed in order.
    MPI_Isend(&m, sizeof(msg_t), MPI_BYTE, rank, TERM, MPI_COMM_WORLD, &req);
    pthread_join(server_thread, NULL);
    // Clean up MPI processes
    MPI_Finalize();
}

