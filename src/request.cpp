#include <iostream>
#include <string>
#include <stdexcept>
#include <sstream>

#include "common.h"
#include "request.h"

// #include "openssl/sha.h"
#include <openssl/evp.h>

// 请求结构体，支持读取csv并解析，以及生成哈希值
Request::Request(const std::string &s)
    : time{}, key_sz{}, val_sz{}, frag_sz{}, kid{}, appid{}, type{}, hit{}
{
  parse(s);
}

// Populate a Request \a r from a single CSV string \a s.  Logs errors on lines
// that cannot be parsed.
void Request::parse(const std::string &s)
{
  std::string::size_type sz;
  string_vec tokens;
  csv_tokenize(s, &tokens);

  try
  {
    time = std::stod(tokens.at(0), &sz);
    appid = std::stoi(tokens.at(1), &sz);
    type = req_type(std::stoi(tokens.at(2), &sz));
    key_sz = std::stoi(tokens.at(3), &sz);
    val_sz = std::stoi(tokens.at(4), &sz);
    kid = std::stoi(tokens.at(5), &sz);
  }
  catch (const std::out_of_range &e)
  {
    std::cerr << "! Malformed line couldn't be parsed: " << e.what() << ". "
              << s << std::endl;
  }
}

// debug/check
void Request::dump() const
{
  std::cerr << "*** Request ***" << std::endl
            << "time: " << time << std::endl
            << "app id: " << appid << std::endl
            << "req type: " << type << std::endl
            << "key size: " << key_sz << std::endl
            << "val size: " << val_sz << std::endl
            << "kid: " << kid << std::endl
            << "hit:" << (hit == 1 ? "yes" : "no") << std::endl;
}

int32_t Request::size() const
{
  return key_sz + val_sz;
}

int32_t Request::get_frag() const
{
  return frag_sz;
}

bool Request::operator<(const Request &other)
{
  return time < other.time;
}

// 生成一个基于请求的键(kid)的哈希值，以便将请求均匀地分布在桶、分区等中。
size_t Request::hash_key(const size_t modulus) const
{
  // 用于将一个uint32_t类型的整数转换为一个无符号字符数组(unsigned char[])
  // 这在进行SHA-1哈希计算时非常有用，因为SHA-1函数需要字节序列作为输入
  union Int_to_unsigned_char
  {
    uint32_t int_value;
    unsigned char char_value[sizeof(uint32_t)];
  };

  // 用于将SHA-1哈希计算的输出（一个20字节的无符号字符数组）
  // 转换为一个size_t类型的值，这样就可以对其进行模运算
  union Unsigned_char_to_size_t
  {
    // unsigned char char_value[SHA_DIGEST_LENGTH]; // 20 bytes.
    unsigned char char_value[20];
    size_t size_t_value; // 8 bytes on x86.
  };

  Int_to_unsigned_char input;
  input.int_value = this->kid;
  Unsigned_char_to_size_t output;

  // SHA_CTX context;
  // SHA1_Init(&context);
  // SHA1_Update(&context, input.char_value, sizeof(uint32_t));
  // SHA1_Final(output.char_value, &context);

  // 创建一个EVP_MD_CTX类型的上下文(context)，然后初始化这个上下文以使用SHA-1算法
  EVP_MD_CTX *context = EVP_MD_CTX_new();
  EVP_DigestInit_ex(context, EVP_sha1(), NULL);

  // 使用EVP_DigestUpdate函数将input中的数据（即请求的键）作为哈希计算的输入
  EVP_DigestUpdate(context, input.char_value, sizeof(uint32_t));

  // 完成哈希计算后，使用EVP_DigestFinal_ex函数将计算结果存储到output中
  EVP_DigestFinal_ex(context, output.char_value, NULL);

  // 释放上下文所占用的资源
  EVP_MD_CTX_free(context);

  // 对output中的size_t_value进行模运算，得到最终的哈希值
  return output.size_t_value % modulus;
}
