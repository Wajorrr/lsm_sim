#include <experimental/optional>
#include <cinttypes>
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <cassert>

#ifndef COMMON_H
#define COMMON_H

template <class T>
using optional = std::experimental::optional<T>; // std::experimental::optional是一个模板类，用于表示一个可能不存在的值

constexpr auto nullopt = std::experimental::nullopt;

typedef std::vector<std::string> string_vec;

// breaks a CSV string into a vector of tokens
// 将一个CSV格式的字符串解析成多个字段，并将这些字段存储在一个向量中
int csv_tokenize(const std::string &s, string_vec *tokens);

// 将数值转换为具有指定精度的字符串
// 接受一个任意类型的值a_value和一个整数n，默认值为2，表示小数点后的位数
template <typename T>
std::string to_string_with_precision(const T a_value, const int n = 2)
{
  std::ostringstream out{};
  out << std::setprecision(n) << a_value;
  return out.str();
}

#endif
