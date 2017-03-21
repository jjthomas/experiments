// clang++ -std=c++11 -I/Users/joseph/rapidjson/include rj-test.cpp
// https://github.com/mloskot/json_benchmark
#include <sys/time.h>
#include <fstream>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

using namespace std;
using namespace rapidjson;

int main(int argc, char **argv) {
  int EVENTS = 40000;
  string *raw = new string[EVENTS];
  long *times = new long[EVENTS];
  
  ifstream infile("/Users/joseph/streaming-benchmarks/data/kafka-json.txt");
  string line;
  int count = 0;
  int chars = 0;
  while (getline(infile, line)) {
    raw[count++] = line; 
    chars += line.length();
  }

  struct timeval start, end, diff;
  gettimeofday(&start, 0);
  for (int i = 0; i < EVENTS; i++) {
    Document d;
    d.ParseInsitu(raw[i].c_str());
    Value &v = d["event_time"];
    times[i] = v.GetInt64();
  }
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("%ld.%06ld\n", (long)diff.tv_sec, (long)diff.tv_usec);

  int num_longs = chars / 8;
  long *dummy = new long[num_longs];
  long sum = 0;
  gettimeofday(&start, 0);
  for (int i = 0; i < num_longs; i++) {
    sum += dummy[i];
  }
  gettimeofday(&end, 0);
  timersub(&end, &start, &diff);
  printf("%d: %ld.%06ld\n", chars, (long)diff.tv_sec, (long)diff.tv_usec);

  for (int i = 0; i < EVENTS; i++) {
    sum += times[i];
  }
  printf("%ld\n", sum);
  return 0;
}
