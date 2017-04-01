// http://stackoverflow.com/questions/17761704/suffix-array-algorithm
// clang++ -std=c++11 bw.cpp
#include <sys/time.h>
#include <fstream>
#include <vector>
#include <limits.h>
#include <assert.h>
#include <ipps_l.h>

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

typedef struct __attribute__((packed)) {
  unsigned int: 1;
  unsigned int i: 21;
  unsigned int second: 21;
  unsigned int first: 21;
} p;

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

  p test[0];
  uint64_t *u = (uint64_t *)test;
  test[0].i = 0;
  test[0].second = 0;
  test[0].first = 1;
  uint64_t f_test = *u;
  test[0].i = 0;
  test[0].second = 1;
  test[0].first = 0;
  uint64_t s_test = *u;
  test[0].i = 1;
  test[0].second = 0;
  test[0].first = 0;
  uint64_t i_test = *u;
  assert(f_test > s_test);
  assert(s_test > i_test);

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

  IppSizeL buf_size;
  ippsSortRadixGetBufferSize_L(chars, ipp64u, &buf_size);
  Ipp8u *w_buf = (Ipp8u *)malloc(buf_size * sizeof(Ipp8u));

  struct timeval start, end, diff;
  gettimeofday(&start, 0);
  uint64_t top_start = rdtsc();
  uint64_t radix_time = 0;
  uint64_t rank_time = 0;
  while (true) {
    memcpy(data3, data1, sizeof(p) * chars);
    uint64_t radix_start = rdtsc();
    ippsSortRadixAscend_64u_I_L((Ipp64u *)data3, chars, w_buf);
    radix_time += (rdtsc() - radix_start);

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
