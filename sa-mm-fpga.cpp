// http://stackoverflow.com/questions/17761704/suffix-array-algorithm
// clang++ -std=c++11 bw.cpp
#include <sys/time.h>
#include <fstream>
#include <vector>
#include <limits.h>
#include <algorithm>
#include <cassert>

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
  unsigned int: 4;
  unsigned int i: 20;
  unsigned int second: 20;
  unsigned int first: 20;
} p;

p *data1;
p *merge_buf;
p *sort_buf;
int buf_size;
int num_lists;

#define LIST_BITS 9
#define LIST_SIZE (1 << LIST_BITS)
#define LG_LISTS_PER_BLOCK (12 - LIST_BITS)

#define OFF_MASK ((1 << LG_LISTS_PER_BLOCK) - 1)
#define LOOKUP(l, e) (data1 + (((l) >> LG_LISTS_PER_BLOCK) << 12) + ((l) & OFF_MASK) + ((e) << LG_LISTS_PER_BLOCK))

#define GLOB_OFF_MASK (LIST_SIZE - 1)
#define LOOKUP_GLOB(e) LOOKUP((e) >> LIST_BITS, (e) & GLOB_OFF_MASK)

/*
#define LOOKUP(l, e) (data1 + (l << LIST_BITS) + e)
#define LOOKUP_GLOB(e) (data1 + e)
*/

int compare(const void *a, const void *b) {
  p *a_p = (p *)a;
  p *b_p = (p *)b;
  if (a_p->first != b_p->first) {
    return a_p->first - b_p->first;
  } else {
    return a_p->second - b_p->second;
  }
};

bool verify_sorted(int lower, int upper) {
  for (int i = lower + 1; i < upper; i++) {
    if (compare(LOOKUP_GLOB(i), LOOKUP_GLOB(i - 1)) < 0) {
      return false;
    }
  }
  return true;
}

void do_full_sort() {
  for (int i = 0; i < num_lists; i++) { 
    for (int j = 0; j < LIST_SIZE; j++) {
      sort_buf[j] = *LOOKUP(i, j);
    }
    qsort(sort_buf, LIST_SIZE, sizeof(p), compare);
    for (int j = 0; j < LIST_SIZE; j++) {
      *LOOKUP(i, j) = sort_buf[j];
    }
  }
}

void merge_lists(int lower, int upper) {
  int lower_list = lower >> LIST_BITS;
  int upper_list = (upper - 1) >> LIST_BITS;
  if (lower_list == upper_list) {
    return;
  } else {
    // two or more lists
    int first_ptr = lower;
    int first_bound = upper_list << LIST_BITS;
    int second_ptr = first_bound;
    if (upper_list - lower_list > 1) {
      int mid_list = (lower_list + upper_list) / 2;
      first_bound = mid_list << LIST_BITS;
      second_ptr = first_bound;
      merge_lists(lower, first_bound);
      merge_lists(first_bound, upper);
    }
    int merge_ptr = 0;
    while (first_ptr < first_bound && second_ptr < upper) {
      p *first_el = LOOKUP_GLOB(first_ptr);
      p *second_el = LOOKUP_GLOB(second_ptr);
      if (first_el->second < second_el->second) {
        merge_buf[merge_ptr++] = *first_el;
        first_ptr++;
      } else {
        merge_buf[merge_ptr++] = *second_el;
        second_ptr++;
      }
    }
    while (first_ptr < first_bound) {
      merge_buf[merge_ptr++] = *LOOKUP_GLOB(first_ptr);
      first_ptr++;
    }
    while (second_ptr < upper) {
      merge_buf[merge_ptr++] = *LOOKUP_GLOB(second_ptr);
      second_ptr++;
    }
    for (int i = lower; i < upper; i++) {
      *LOOKUP_GLOB(i) = merge_buf[i - lower];
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
  num_lists = buf_size / LIST_SIZE;
  data1 = (p *)malloc(sizeof(p) * buf_size);
  int *ranks = (int *)malloc(sizeof(int) * chars);
  merge_buf = (p *)malloc(sizeof(p) * buf_size);
  // only for CPU-only version
  sort_buf = (p *)malloc(sizeof(p) * LIST_SIZE);
  for (int i = 0; i < chars; i++) {
    // TODO we can save an iteration and set gap back to 2 if we set both first and second here
    p *cur = LOOKUP_GLOB(i);
    cur->first = 0;
    cur->second = buf[i];
    cur->i = i;
  }
  for (int i = chars; i < buf_size; i++) {
    p *cur = LOOKUP_GLOB(i);
    cur->first = 1048575;
    cur->second = 1048575;
    cur->i = 1048575;
  }

  int gap = 1;

  struct timeval start, end, diff;
  gettimeofday(&start, 0);
  uint64_t total_start = rdtsc();
  uint64_t sort_time = 0;
  while (true) {
    uint64_t sort_start = rdtsc();
    do_full_sort();
    sort_time += rdtsc() - sort_start;
    int el_to_check = LIST_SIZE;
    while (el_to_check < buf_size) {
      p *prev = LOOKUP_GLOB(el_to_check - 1);
      p *next = LOOKUP_GLOB(el_to_check);
      if (prev->first == next->first) {
	int lower_bound = el_to_check - 1;
	int upper_bound = el_to_check;
        while (lower_bound >= 0 && LOOKUP_GLOB(lower_bound)->first == prev->first) {
          lower_bound--;
	}
	lower_bound++;
        while (upper_bound < buf_size && LOOKUP_GLOB(upper_bound)->first == prev->first) {
          upper_bound++;
	}
	// printf("%d %d %d %d\n", upper_bound, buf_size, LOOKUP_GLOB(upper_bound)->first, prev->first);
	// printf("%d %d\n", lower_bound, upper_bound);
        merge_lists(lower_bound, upper_bound);
	// TODO remove
	// assert(verify_sorted(lower_bound, upper_bound));
	el_to_check = ((upper_bound >> LIST_BITS) + 1) << LIST_BITS;
      } else {
        el_to_check += LIST_SIZE;
      }
    }

    bool dups = false;
    int cur_char = 1;
    for (int i = 0; i < chars; i++) {
      p *cur = LOOKUP_GLOB(i);
      if (i > 0) {
	p *prev = LOOKUP_GLOB(i - 1);
	if (cur->first == prev->first && cur->second == prev->second) {
          dups = true;
	} else {
          cur_char++;
	}
      }
      ranks[cur->i] = cur_char;
    }
    if (!dups) {
      break;
    }
    for (int i = 0; i < chars; i++) {
      p *cur = LOOKUP_GLOB(i);
      cur->first = ranks[cur->i];
      cur->second = (cur->i < chars - gap) ? ranks[cur->i + gap] : 0;
    }
    gap *= 2;
    // printf("new gap %d\n", gap);
  }
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("gettimeofday: %ld.%06ld\n", (long)diff.tv_sec, (long)diff.tv_usec);
  printf("%lld/%lld\n", sort_time, rdtsc() - total_start);
  printf("%d\n", ranks[0]);
  printf("%d\n", gap);
  return 0;
}
