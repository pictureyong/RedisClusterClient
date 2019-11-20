// Author : lzy
// Email  : pictureyong@163.com
// Date   : 2019-11-19 15:05
// Description: 

#ifndef _UTIL_H__
#define _UTIL_H__

#include <time.h>
#include <sys/time.h>
#include <iostream>
#include <string>
#include <vector>

#define VLOG(FLAG) std::cout << "\n" << #FLAG << ":"

typedef long long int64;

void SplitString(const std::string& str, char ch, std::vector<std::string>& dest);

inline int64 GetTimeInSecond() {
  struct timeval now;
  gettimeofday(&now, NULL);
  return static_cast<int64>(now.tv_sec);
}

inline int64 GetTimeInMs() {
  struct timeval now;
  gettimeofday(&now, NULL);
  return static_cast<int64>(now.tv_sec * 1000 + now.tv_usec / 1000);
}

#endif
