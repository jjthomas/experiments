// clang++ -std=c++11 bw.cpp
#include <sys/time.h>
#include <fstream>
#include <vector>
#include <limits.h>

using namespace std;

typedef struct {
  int first;
  int second;
  int i;
} p;

int compare(const void *a, const void *b) {
  p *a_p = (p *)a;
  p *b_p = (p *)b;
  if (a_p->first != b_p->first) {
    return a_p->first - b_p->first;
  } else {
    return a_p->second - b_p->second;
  }
};

bool verify_sorted(p *data, int size) {
  for (int i = 1; i < size; i++) {
    if (compare(data + i, data + i - 1) < 0) {
      return false;
    }
  }
  return true;
}

int main(int argc, char **argv) {
  int CHARS = atoi(argv[1]);
  
  ifstream infile("/Users/joseph/streaming-benchmarks/data/kafka-json.txt");
  string line;

  int chars = 0;
  char *buf = new char[CHARS];
  while (getline(infile, line)) {
    if (chars + line.length() > CHARS) {
      break;
    }
    memcpy(buf + chars, line.c_str(), line.length());
    chars += line.length();
  }

  p *data1 = (p *)malloc(sizeof(p) * chars);
  p *data2 = (p *)malloc(sizeof(p) * chars);
  p *data3 = (p *)malloc(sizeof(p) * chars);
  for (int i = 0; i < chars; i++) {
    data1[i].first = buf[i];
    // assumption here is that alphabet size is <= chars
    data1[i].second = (i == chars - 1) ? chars : buf[i + 1];
    data1[i].i = i;
  }
  int *counts = (int *)malloc(sizeof(int) * (chars + 1));
  int gap = 2;
 
  struct timeval start, end, diff;
  gettimeofday(&start, 0);
  while (true) {
    // radix pass 1
    for (int i = 0; i < chars + 1; i++) {
      counts[i] = 0;
    }
    for (int i = 0; i < chars; i++) {
      counts[data1[i].second]++;
    }
    int sum = 0;
    for (int i = 0; i < chars + 1; i++) {
      int new_sum = sum + counts[i];
      counts[i] = sum;
      sum = new_sum;
    }
    for (int i = 0; i < chars; i++) {
      data2[counts[data1[i].second]++] = data1[i];
    }
    // radix pass 2
    for (int i = 0; i < chars + 1; i++) {
      counts[i] = 0;
    }
    for (int i = 0; i < chars; i++) {
      counts[data2[i].first]++;
    }
    sum = 0;
    for (int i = 0; i < chars + 1; i++) {
      int new_sum = sum + counts[i];
      counts[i] = sum;
      sum = new_sum;
    }
    for (int i = 0; i < chars; i++) {
      data3[counts[data2[i].first]++] = data2[i];
    }
    // sort
    memcpy(data3, data1, sizeof(p) * chars);
    qsort(data3, chars, sizeof(p), compare);
    /*
    if (!verify_sorted(data3, chars)) {
      printf("not sorted at %d\n", gap);
    }
    */

    bool dups = false;
    int cur_char = 0;
    for (int i = 0; i < chars; i++) {
      if (i > 0) {
	if (data3[i].first == data3[i - 1].first && data3[i].second == data3[i - 1].second) {
          dups = true;
	} else {
          cur_char++;
	}
      }
      data1[data3[i].i].first = cur_char;
    }
    if (!dups) {
      break;
    }
    for (int i = 0; i < chars; i++) {
      data1[i].i = i;
      data1[i].second = (i < chars - gap) ? data1[i + gap].first : chars;
    }
    gap *= 2;
  }
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("%ld.%06ld\n", (long)diff.tv_sec, (long)diff.tv_usec);
  printf("%d\n", data1[0].first);
  printf("%d\n", gap);
  return 0;
}
