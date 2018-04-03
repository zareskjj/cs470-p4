/**
 * dht.c
 *
 * CS 470 Project 4
 *
 * Implementation for distributed hash table (DHT).
 *
 * Name: 
 *
 */

#include <mpi.h>
#include <pthread.h>

#include "dht.h"

/*
 * Private module variable: current process ID
 */
static int pid;

int dht_init()
{
    local_init();

    pid = 0;
    return pid;
}

void dht_put(const char *key, long value)
{
    local_put(key, value);
}

long dht_get(const char *key)
{
    return local_get(key);
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
}

