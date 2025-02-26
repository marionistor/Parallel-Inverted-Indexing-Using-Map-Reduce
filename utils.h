#ifndef _UTILS_H_
#define _UTILS_H_

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <bits/stdc++.h>

using namespace std;

#define DIE(assertion, call_description)                                       \
  do {                                                                         \
    if (assertion) {                                                           \
      fprintf(stderr, "(%s, %d): ", __FILE__, __LINE__);                       \
      perror(call_description);                                                \
      exit(EXIT_FAILURE);                                                      \
    }                                                                          \
  } while (0)

/* struct for the barrier that is used by both
   mapper and reducer threads and for the mutex */
struct common_elements {
    pthread_mutex_t mutex;
    pthread_barrier_t *barrier;
};

// struct for mapper threads
struct mapper {
    unordered_map<string, set<int>> partial_list;
    common_elements *common;
    queue<pair<int, string>> *files;
};

// struct for reducer threads
struct reducer {
    vector<mapper> *mapper_structs;
    common_elements *common;
    queue<char> *letters;
};

#endif