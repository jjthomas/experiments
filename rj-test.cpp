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
  char *ad_id = new char[EVENTS * 250];
  int ad_counter = 0;
  char *page_id = new char[EVENTS * 250];
  int page_counter = 0;
  char *user_id = new char[EVENTS * 250];
  int user_counter = 0;
  char *event_type = new char[EVENTS * 250];
  int event_counter = 0;
  char *ad_type = new char[EVENTS * 250];
  int ad_type_counter = 0;
  char *ip = new char[EVENTS * 250];
  int ip_counter = 0;
  
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
    d.ParseInsitu(const_cast<char *>(raw[i].c_str()));
    // Value &v = d["event_time"];
    times[i] = d["event_time"].GetInt64();
    memcpy(ad_id + ad_counter, d["ad_id"].GetString(), strlen(d["ad_id"].GetString()));
    ad_counter += strlen(d["ad_id"].GetString());
    memcpy(page_id + page_counter, d["page_id"].GetString(), strlen(d["page_id"].GetString()));
    page_counter += strlen(d["page_id"].GetString());
    memcpy(user_id + user_counter, d["user_id"].GetString(), strlen(d["user_id"].GetString()));
    user_counter += strlen(d["user_id"].GetString());
    memcpy(event_type + event_counter, d["event_type"].GetString(), strlen(d["event_type"].GetString()));
    event_counter += strlen(d["event_type"].GetString());
    memcpy(ad_type + ad_type_counter, d["ad_type"].GetString(), strlen(d["ad_type"].GetString()));
    ad_type_counter += strlen(d["ad_type"].GetString());
    memcpy(ip + ip_counter, d["ip_address"].GetString(), strlen(d["ip_address"].GetString()));
    ip_counter += strlen(d["ip_address"].GetString());
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
  printf("%ld ... %c %c %c %c %c %c\n", sum, ad_id[sum % (EVENTS * 250)], page_id[sum % (EVENTS * 250)], user_id[sum % (EVENTS * 250)],
         event_type[sum % (EVENTS * 250)], ad_type[sum % (EVENTS * 250)], ip[sum % (EVENTS * 250)]);
  return 0;
}
