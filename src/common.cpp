#include <sstream>

#include "common.h"

// 用于读取csv文件
// 将一个CSV格式的字符串解析成多个字段，并将这些字段存储在一个向量中
int csv_tokenize(const std::string &s, string_vec *tokens)
{
  std::istringstream ss(s);
  std::string token;
  while (std::getline(ss, token, ','))
  {
    tokens->push_back(token);
  }
  return 0;
}
