#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <vector>
#include <algorithm>


using namespace std;

struct node {
  int data;
  node *next;
};

int lock = 0;

int main(int argc, char **argv) {
  int SIZE = 100;
  int ITS = 100000;
  vector<int> indices;
  node *nodes = new node[SIZE];
  for (int i = 0; i < SIZE; i++) {
    indices.push_back(i);
    nodes[i].data = i;
  }
  random_shuffle(indices.begin(), indices.end());
  int prev = indices[0];
  nodes[prev].next = NULL;
  for (int i = 1; i < SIZE; i++) {
    nodes[prev].next = &nodes[indices[i]]; 
    prev = indices[i];
    nodes[prev].next = NULL;
  }

  struct timeval start, end, diff;
  gettimeofday(&start, 0);
  for (int i = 0; i < ITS; i++) { 
    for (int j = 0; j < SIZE; j++) {
      nodes[indices[j]].data++;
    }
  }
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("NO_ATOMIC %d: %ld.%06ld\n", 0, (long)diff.tv_sec, (long)diff.tv_usec);

  gettimeofday(&start, 0);
  for (int i = 0; i < ITS; i++) { 
    for (int j = 0; j < SIZE; j++) {
      // __sync_fetch_and_add(&nodes[indices[j]].data, 1);
      while(__sync_fetch_and_add(&lock, 1) != 0);
      nodes[indices[j]].data++;
      lock = 0;
    }
  }
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("ATOMIC %d: %ld.%06ld\n", 0, (long)diff.tv_sec, (long)diff.tv_usec);

  return 0;
}
