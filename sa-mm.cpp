// http://stackoverflow.com/questions/17761704/suffix-array-algorithm
// clang++ -std=c++11 bw.cpp
#include <sys/time.h>
#include <fstream>
#include <vector>
#include <limits.h>

static inline uint64_t
rdtsc(void)
{
	uint32_t eax = 0, edx;

	__asm__ __volatile__("cpuid;"
			     "rdtsc;"
				: "+a" (eax), "=d" (edx)
				:
				: "%rcx", "%rbx", "memory");

	__asm__ __volatile__("xorl %%eax, %%eax;"
			     "cpuid;"
				:
				:
				: "%rax", "%rbx", "%rcx", "%rdx", "memory");

	return (((uint64_t)edx << 32) | eax) / 1000;
}

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
    // increment buf contents by 1 so we can use 0 as the special '$' char
    data1[i].first = buf[i] + 1;
    // assumption here is that alphabet size is <= chars
    data1[i].second = (i == chars - 1) ? 0 : buf[i + 1] + 1;
    data1[i].i = i;
  }
  int *counts = (int *)malloc(sizeof(int) * (chars + 1));
  int gap = 2;
 
  struct timeval start, end, diff;
  gettimeofday(&start, 0);
  uint64_t top_start = rdtsc();
  uint64_t radix_time = 0;
  uint64_t rank_time = 0;
  while (true) {
    uint64_t radix_start = rdtsc();
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
    radix_time += (rdtsc() - radix_start);
    /*
    // sort
    memcpy(data3, data1, sizeof(p) * chars);
    qsort(data3, chars, sizeof(p), compare);
    */
    /*
    if (!verify_sorted(data3, chars)) {
      printf("not sorted at %d\n", gap);
    }
    */

    uint64_t rank_start = rdtsc();
    bool dups = false;
    int cur_char = 1;
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
    rank_time += rdtsc() - rank_start;
    if (!dups) {
      break;
    }
    for (int i = 0; i < chars; i++) {
      data1[i].i = i;
      data1[i].second = (i < chars - gap) ? data1[i + gap].first : 0;
    }
    gap *= 2;
  }
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("gettimeofday: %ld.%06ld\n", (long)diff.tv_sec, (long)diff.tv_usec);
  printf("rdtsc: %lld\n", rdtsc() - top_start);
  printf("rdtsc-radix: %lld\n", radix_time);
  printf("rdtsc-rank: %lld\n", rank_time);
  printf("%d\n", data1[0].first);
  printf("%d\n", gap);
  return 0;
}
