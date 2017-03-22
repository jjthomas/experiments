// clang++ -std=c++11 bw.cpp
#include <sys/time.h>
#include <fstream>
#include <vector>

using namespace std;

int chars;
char *buf;

int compare(const void *a, const void *b) {
  int count = 0;
  int a_ptr = *(int*)a + 2;
  int b_ptr = *(int*)b + 2;
  while (count < chars) { 
    if (a_ptr >= chars) a_ptr -= chars; 
    if (b_ptr >= chars) b_ptr -= chars;
    if (buf[a_ptr] != buf[b_ptr]) return buf[a_ptr] - buf[b_ptr];
    a_ptr++;
    b_ptr++;
    count++;
  }
  return 0;
};

static inline int char_at(int rot, int d) {
  int pos = rot + d;
  return (pos >= chars) ? -1 : buf[pos];  
}

void sort(vector<int> &rots, int lo, int hi, int d) { 
  if (hi <= lo) return;
  int lt = lo, gt = hi;
  int v = char_at(rots[lo], d);
  int i = lo + 1;
  while (i <= gt) {
    int t = char_at(rots[i], d);
    if      (t < v) swap(rots[lt++], rots[i++]);
    else if (t > v) swap(rots[i], rots[gt--]);
    else            i++;
  }
  sort(rots, lo, lt-1, d);
  sort(rots, lt, gt, d+1);
  sort(rots, gt+1, hi, d);
}

void sort_top(vector<int> &rots) {
  sort(rots, 0, rots.size() - 1, 0);
}

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

  int BUCKETS = 256 * 256;
  vector<int>* buckets = new vector<int>[BUCKETS];
  vector<int> rots;

  struct timeval start, end, diff;
  gettimeofday(&start, 0);
  for (int i = 0; i < chars; i++) {
    int second = (i == chars - 1) ? 0 : i + 1;
    buckets[buf[i] * 256 + buf[second]].push_back(i);
  }
  for (int i = 0; i < BUCKETS; i++) {
    // qsort(&buckets[i][0], buckets[i].size(), sizeof(int), compare);
    sort_top(buckets[i]);
  }
  /*
  for (int i = 0; i < chars; i++) {
    rots.push_back(i);
  }
  sort_top(rots);
  */
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("%ld.%06ld\n", (long)diff.tv_sec, (long)diff.tv_usec);
  int pos = 0;
  while (buckets[pos].empty()) {
    pos++;
  }
  printf("%d\n", buckets[pos][0]);
  // printf("%d\n", rots[0]);

  return 0;
}
