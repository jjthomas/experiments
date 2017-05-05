#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <vector>
#include <algorithm>


using namespace std;

#define OP(x) (x)

int main(int argc, char **argv) {
  int SIZE = atoi(argv[1]);
  vector<int> incremented;
  int *data = new int[SIZE];
  int *data2 = new int[SIZE];
  for (int i = 0; i < SIZE; i++) {
    data[i] = i;
    data2[i] = 0;
  }

  int sum = 0;
  struct timeval start, end, diff;
  gettimeofday(&start, 0);
  for (int i = 0; i < SIZE; i++) {
    sum += (int)OP(data[i] + 1);
  }
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("FUSED %d: %ld.%06ld\n", sum, (long)diff.tv_sec, (long)diff.tv_usec);

  sum = 0;
  gettimeofday(&start, 0);
  for (int i = 0; i < SIZE; i++) {
    incremented.push_back(data[i] + 1);
  }
  for (int i = 0; i < SIZE; i++) {
    sum += (int)OP(incremented[i]);
  } 
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("SPLIT (VECTOR) %d: %ld.%06ld\n", sum, (long)diff.tv_sec, (long)diff.tv_usec);

  sum = 0;
  gettimeofday(&start, 0);
  for (int i = 0; i < SIZE; i++) {
    data2[i] = data[i] + 1;
  }
  for (int i = 0; i < SIZE; i++) {
    sum += (int)OP(data2[i]);
  } 
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("SPLIT (ARRAY) %d: %ld.%06ld\n", sum, (long)diff.tv_sec, (long)diff.tv_usec);

  return 0;
}
