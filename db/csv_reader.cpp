#include "csv_reader.h"

#include <assert.h>

const std::string kCompressSuffix = ".gz";

CsvReader::CsvReader(const std::string& filename)
    : curr_pos_(0),
      curr_size_(0),
      is_compressed_flag_(false),
      is_from_string_flag_(false),
      filename_(filename) {
}

CsvReader::~CsvReader() {
}

bool CsvReader::Init() {
  if (access(filename_.c_str(), 0) != 0) {
    //LOG(ERROR) << "CsvReader::Init failed. File not existed: " << filename_;
    return false;
  }
  if (IsCompressedFile(filename_)) {
    is_compressed_flag_ = true;
    gz_file_ = gzopen(filename_.c_str(), "rb");
  } else {
    input_file_.open(filename_.c_str());
  }

  ReadFile(kBufferMaxSize);
  ReadHeader();

  return true;
}

void CsvReader::InitFromString(const std::string& context) {
  content_stream_ << context;
  is_from_string_flag_ = true;
  curr_size_ = content_stream_.str().size();
  ReadHeader();
}

void CsvReader::ReadHeader() {
  std::string header;
  safegetline(content_stream_, header);

  std::istringstream ss(header);
  std::string cell;
  while(getline(ss, cell, ',')) {
    column_list_.push_back(cell);
  }

}

bool CsvReader::HasNextRow() {
  content_stream_.peek();
  return !content_stream_.eof();
}

bool CsvReader::NextRow(CsvRow* csv_row) {
  std::string line;
  safegetline(content_stream_, line);
  //std::cout << "line:" << line << std::endl;
  std::istringstream ss(line);

  std::string cell;
  uint32_t index = 0;
  while (getline(ss, cell,',')) {
    if (index >= column_list_.size()) {
        std::cout << "*** ERROR*** More columns than header. Filename: " << filename_
                 << "Line index: " << index << std::endl;
      return false;
    }
    //std::cout << "key:" << column_list_.at(index) << " value:" << cell << std::endl;
    (*csv_row)[column_list_.at(index)] = cell;
    ++index;
  }
  if (index < column_list_.size()) {
      std::cout << "*** ERROR*** Less columns than header. Filename: " << filename_
               << "Line index: " << index << std::endl;
    return false;
  }
  return true;
}

bool CsvReader::IsCompressedFile(const std::string& filename) const {
  std::string suffix = filename.substr(
      filename.size() - kCompressSuffix.size(),
      kCompressSuffix.size());
  return suffix == kCompressSuffix ? true : false;
}

int CsvReader::NextChar(std::streambuf* sb) {
  int ret = sb->sbumpc();
  if (EOF != ret) {
    ++curr_pos_;
  }
  assert(curr_pos_ <= curr_size_);
  //CHECK(curr_pos_ <= curr_size_)
  //    << "Curr Pos: " << curr_pos_
  //    << ", Curr Size: " << curr_size_;
  return ret;
}

void CsvReader::ReadFile(uint32_t read_size) {
  int left_size = curr_size_ - curr_pos_;
  memmove(buffer_, buffer_ + curr_pos_, left_size);
  if (!is_compressed_flag_) {
    curr_size_ = input_file_.rdbuf()->sgetn(buffer_ + left_size, read_size);
  } else {
    curr_size_ = gzread(gz_file_, buffer_ + left_size, read_size);
  }
  curr_size_ += left_size;
  content_stream_.rdbuf()->pubsetbuf(buffer_, curr_size_);
  curr_pos_ = 0;
}

std::istream& CsvReader::safegetline(std::istream& is, std::string& t) {
  t.clear();

  std::istream::sentry se(is, true);
  std::streambuf* sb = is.rdbuf();

  while(true)
  {
    int c = NextChar(sb);
    if (c == '\n') {
      break;
    } else if (c == '\r') {
      if(sb->sgetc() == '\n')
        NextChar(sb);
      break;
    } else if (c == EOF) {
      is.setstate(std::ios::eofbit);
      std::cout << "EOF, line end" << std::endl;
      break;
    } else {
      t += (char)c;
    }
  }

  if (!is_from_string_flag_) {
    uint32_t left_size = curr_size_ - curr_pos_;
    if (left_size < kBufferThreshold) {
      ReadFile(kBufferLoadSize);
    }
  }

  return is;
}

