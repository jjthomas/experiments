// http://stackoverflow.com/questions/17761704/suffix-array-algorithm
// clang++ -std=c++11 bw.cpp
#include <sys/time.h>
#include <fstream>
#include <vector>
#include <limits.h>
#include <algorithm>

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
  unsigned int: 13;
  unsigned int i: 17;
  unsigned int second: 17;
  unsigned int first: 17;
} p;

p *data1;
p *merge_buf;
p *sort_buf;
int buf_size;
int num_lists;

#define OFF_MASK 0x7
#define LOOKUP(arr, l, e) ((arr) + (((l) >> 3) << 12) + ((l) & OFF_MASK) + ((e) << 3))

#define GLOB_OFF_MASK 0x1FF
#define LOOKUP_GLOB(arr, e) (LOOKUP(arr, (e) >> 9, (e) & GLOB_OFF_MASK))  

void do_full_sort() {
  for (int i = 0; i < num_lists; i++) { 
    for (int j = 0; j < 512; j++) {
      sort_buf[j] = *LOOKUP(data1, i, j);
    }
    std::sort((uint64_t *)sort_buf, (uint64_t *)(sort_buf + 512));
    for (int j = 0; j < 512; j++) {
      *LOOKUP(data1, i, j) = sort_buf[j];
    }
  }
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

  buf_size = ((chars - 1) / 4096 + 1) * 4096;
  num_lists = buf_size / 512;
  data1 = (p *)malloc(sizeof(p) * buf_size);
  memset(data1, 0xFF, sizeof(p) * buf_size);
  int *ranks = (int *)malloc(sizeof(int) * chars);
  merge_buf = (p *)malloc(sizeof(p) * buf_size);
  // only for CPU-only version
  sort_buf = (p *)malloc(sizeof(p) * 512);
  for (int i = 0; i < chars; i++) {
    data1[i].first = 0;
    data1[i].second = buf[i + 1];
    data1[i].i = i;
  }

  int gap = 2;
 
  while (true) {
    do_full_sort();
    int el_to_check = 512;
    while (el_to_check < buf_size) {
      p *prev = LOOKUP_GLOB(data1, el_to_check - 1);
      p *next = LOOKUP_GLOB(data1, el_to_check);
      if (prev->first == next->first) {
	int lower_bound = el_to_check - 1;
	int upper_bound = el_to_check;
        while (lower_bound >= 0 && LOOKUP_GLOB(data1, lower_bound)->first == prev->first) {
          lower_bound--;
	}
	lower_bound++;
        while (upper_bound < buf_size && LOOKUP_GLOB(data1, upper_bound)->first == prev->first) {
          upper_bound++;
	}
        merge_lists(lower_bound, upper_bound);
	el_to_check = ((upper_bound >> 9) + 1) << 9;
      } else {
        el_to_check += 512;
      }
    }

    bool dups = false;
    int cur_char = 1;
    for (int i = 0; i < chars; i++) {
      if (i > 0) {
	if (data1[i].first == data1[i - 1].first && data1[i].second == data1[i - 1].second) {
          dups = true;
	} else {
          cur_char++;
	}
      }
      ranks[data1[i].i] = cur_char;
    }
    if (!dups) {
      break;
    }
    for (int i = 0; i < chars; i++) {
      data1[i].first = ranks[data1[i].i];
      data1[i].second = (data1[i].i < chars - gap) ? ranks[data1[i].i + gap] : 0;
    }
    gap *= 2;
  }
  printf("%d\n", data1[0].first);
  printf("%d\n", gap);
  return 0;
}
