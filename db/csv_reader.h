#pragma once 

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <string.h>
#include <unordered_map>

//#include "thirdparty/zlib-1.2.8/include/zlib.h"
#include <zlib.h>

typedef std::unordered_map<std::string, std::string> CsvRow;

class CsvReader {
 public:
  explicit CsvReader(const std::string& filename);
  ~CsvReader();

  bool Init();
  void InitFromString(const std::string& context);

  void ReadHeader();
  bool HasNextRow();
  bool NextRow(CsvRow* csv_row);

  template<typename T>
  static inline bool GetColumnValue(
      const CsvRow& row,
      const std::string& column,
      T* value);

  static inline bool GetColumnValue(
      const CsvRow& row,
      const std::string& column,
      char* value);

  static inline bool GetColumnValue(
      const CsvRow& row,
      const std::string& column,
      std::string* value);

  template<typename T>
  static T DataFromString(const std::string& str);

  const std::vector<std::string>& columns() const {
    return column_list_;
  }

 private:
  int NextChar(std::streambuf* sb);
  void ReadFile(uint32_t read_size);
  static const uint32_t kBufferMaxSize = 204800;
  static const uint32_t kBufferLoadSize = 102400;
  static const uint32_t kBufferThreshold = kBufferMaxSize - kBufferLoadSize;
  char buffer_[kBufferMaxSize];
  uint32_t curr_pos_;
  uint32_t curr_size_;
  bool is_compressed_flag_;
  bool is_from_string_flag_;
  gzFile gz_file_;
  std::ifstream input_file_;

  bool IsCompressedFile(const std::string& filename) const;
  std::istream& safegetline(std::istream& is, std::string& t);

  std::string filename_;
  std::stringstream content_stream_;

  std::vector<std::string> column_list_;
};

template<typename T>
T CsvReader::DataFromString(const std::string& str) {
  T output;
  std::istringstream is(str);
  is >> output;
  return output;
}

template<typename T>
bool CsvReader::GetColumnValue(
    const CsvRow& row,
    const std::string& column,
    T* value) {
  auto citer = row.find(column);
  if (citer == row.end()) {
    return false;
  }
  *value = DataFromString<T>(citer->second);
  return true;
}

bool CsvReader::GetColumnValue(
    const CsvRow& row,
    const std::string& column,
    char* value) {
  auto citer = row.find(column);
  if (citer == row.end()) {
    return false;
  }
  *value = citer->second[0];
  return true;
}

bool CsvReader::GetColumnValue(
    const CsvRow& row,
    const std::string& column,
    std::string* value) {
  auto citer = row.find(column);
  if (citer == row.end()) {
    return false;
  }
  *value = citer->second;
  return true;
}


#endif  // SRC_UTIL_CSV_READER_H_
