// clang++ -std=c++11 bw.cpp
#include <sys/time.h>
#include <fstream>

using namespace std;

int chars;
char *buf;

int compare(const void *a, const void *b) {
  int count = 0;
  int a_ptr = *(int*)a;
  int b_ptr = *(int*)b;
  while (count < chars) { 
    if (a_ptr == chars) a_ptr = 0; 
    if (b_ptr == chars) b_ptr = 0;
    if (buf[a_ptr] != buf[b_ptr]) return buf[a_ptr] - buf[b_ptr];
    a_ptr++;
    b_ptr++;
    count++;
  }
  return 0;
};

int main(int argc, char **argv) {
  int CHARS = atoi(argv[1]);
  
  ifstream infile("/Users/joseph/streaming-benchmarks/data/kafka-json.txt");
  string line;

  chars = 0;
  buf = new char[CHARS];
  while (getline(infile, line)) {
    if (chars + line.length() > CHARS) {
      break;
    }
    memcpy(buf + chars, line.c_str(), line.length());
    chars += line.length();
  }

  printf("chars: %d\n", chars);

  int *rots = new int[chars];
  for (int i = 0; i < chars; i++) {
    rots[i] = i;
  }

  struct timeval start, end, diff;
  gettimeofday(&start, 0);
  qsort(rots, chars, sizeof(int), compare);
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("%ld.%06ld\n", (long)diff.tv_sec, (long)diff.tv_usec);

  return 0;
}
