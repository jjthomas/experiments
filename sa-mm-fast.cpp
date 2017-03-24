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
  int c;
  int i;
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

  int *data1 = (int *)malloc(sizeof(int) * chars);
  p *data2 = (p *)malloc(sizeof(p) * chars);
  p *data3 = (p *)malloc(sizeof(p) * chars);

  for (int i = 0; i < chars; i++) {
    data1[i] = buf[i];
    data2[i].c = 0;
    data2[i].i = i;
  }

  int *counts1 = (int *)malloc(sizeof(int) * (chars + 1));
  int *counts2 = (int *)malloc(sizeof(int) * (chars + 1));
  int gap = 0;
 
  struct timeval start, end, diff;
  gettimeofday(&start, 0);
  while (true) {
    int idx = 0;
    int reps = 0;
    bool change = false;
    while (true) {
      while (idx < chars) {
        if (reps != 0) {
          if (data2[idx].c != data2[idx - 1].c) {
            break;
          } else {
            reps++;
          }
        } else {
          if (idx > 0 && data2[idx].c == data2[idx - 1].c) {
            reps = 1;
	  }
	}
        idx++;
      }
      if (reps == 0) {
        break;
      }
      idx = idx - 1 - reps;
      change = true;
      // printf("id: %d %d %d\n", idx, reps, gap);
      // printf("%d %d %d %d %d %d %d\n", data2[idx].c, data2[idx + 1].c, data2[idx + 2].c, data2[idx + 3].c, data2[idx + 4].c, data2[idx + 5].c, data2[idx + 6].c);
      // if (idx == 43) exit(0);

      for (int i = 0; i < chars + 1; i++) {
        counts1[i] = 0;
        counts2[i] = 0;
      }
      for (int i = 0; i <= reps; i++) {
	int next = (data2[idx + i].i + gap >= chars) ? 0 : data1[data2[idx + i].i + gap];
        counts1[next]++;
        counts2[next]++;
      }
      int sum1 = 0;
      int sum2 = 0;
      for (int i = 0; i < chars + 1; i++) {
        int new_sum1 = sum1 + counts1[i];
        int new_sum2 = sum2 + counts2[i];
        counts1[i] = sum1;
        counts2[i] = sum2;
        sum1 = new_sum1;
        sum2 = new_sum2;
      }
      for (int i = 0; i <= reps; i++) {
	int next = (data2[idx + i].i + gap >= chars) ? 0 : data1[data2[idx + i].i + gap];
        // increment by 1 so we can use 0 as the special '$' char
        data3[counts1[next]].c = idx + counts2[next] + 1;
        data3[counts1[next]].i = data2[idx + i].i;
	counts1[next]++;
      }
      for (int i = 0; i <= reps; i++) {
        data2[idx + i] = data3[i];
	data1[data3[i].i] = data3[i].c;
      }

      idx += reps + 1;
      reps = 0;
    }
    if (!change) {
      break;
    }
    gap = (gap == 0) ? 1 : gap * 2;
  }
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("gettimeofday: %ld.%06ld\n", (long)diff.tv_sec, (long)diff.tv_usec);
  printf("%d\n", data1[0]);
  printf("%d\n", gap);
  return 0;
}
