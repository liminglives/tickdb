#pragma once 

#include <string>
#include <chrono>
#include <vector>
#include <iostream>
#include <unordered_map>

#include "tickdb/defines.h"

#define Throw(x) do { throw ::TickDB::TickDBException(__FILE__,__LINE__,__FUNCTION__,(x));} while (0)

#define Log(x) do { ::TickDB::Print(__FILE__,__LINE__,__FUNCTION__,(x));} while (0)

namespace TickDB {

void Print(const std::string& file, int line, const std::string& func, const std::string& info="");

class TickDBException {
public:
    TickDBException(const std::string& file, int line, const std::string& func, const std::string& info="") :
	       _file(file), _line(line), _func(func), _info(info) {} std::string info() {
		std::string ret;
		ret.append(_file);
		ret.append(":");
		ret.append(std::to_string(_line));
		ret.append(" ");
		ret.append(_func + "():");
		ret.append(_info);
		return ret;
	}
private:
    std::string _file;
    int _line;
    std::string _func;
    std::string _info;
};

namespace Util {

const int BinarySearchLEFlag = -1;
const int BinarySearchGEFlag = 1;
const int BinarySearchEqualFlag = 0;
const int BinarySearchLTFlag = -2;
const int BinarySearchGTFlag = 2;



void split(const std::string& src, const std::string& separator, std::vector<std::string>& dest);

std::string& trim(std::string &s);

std::time_t getTimeStamp();

std::string to_date(uint64_t timestamp);

bool file_exists(const std::string& path);

uint64_t get_file_size(const std::string& fname);

int binary_search(const std::vector<uint64_t>& ts_vec, uint64_t ts, int flag, int start = 0, int end = -1); 

void sort_insert(std::vector<uint64_t>& ts_vec, uint64_t ts);


bool to_kv(const std::string& str, std::string& k, std::string& v);
bool to_kv_map(const std::string& str, std::unordered_map<std::string, std::string>& kv_map);
bool to_kv_vec(const std::string& str, std::vector<std::pair<std::string, std::string>>& kv_vec);

template <class T> 
void serialize(const T& val, std::string& str) {
    str.assign(static_cast<char*>(static_cast<void*>(const_cast<T *>(&val))), sizeof(T));
}

void serialize(const char* val, std::string& str); 

uint64_t hashcode(const std::string& str); 

std::vector<std::string> to_lines(const std::string& data); 

template<typename T>
int binary_search(const std::vector<T>& vec, const T& target, int flag, int start = 0, int end = -1) {
    if (end == -1) {
        end = vec.size() - 1;
    }
    if (end < start) {
        return -1;
    }
    while (end >= start) {
        int middle = (start + end) / 2;
        if (vec[middle] == target) {
            if (flag == BinarySearchLTFlag) {
                return middle - 1;
            } else if (flag == BinarySearchGTFlag) {
                return middle + 1;
            } 
            return middle;
        } else if (vec[middle] > target) {
            end = middle - 1;
        } else {
            start = middle + 1;
        }
    }
    switch (flag) {
        case BinarySearchEqualFlag:
            return -1;
        case BinarySearchLEFlag:
            return end;
        case BinarySearchGEFlag:
            return start;
        case BinarySearchLTFlag:
            return end;
        case BinarySearchGTFlag:
            return start;
        default:
            Log("illegal flag:" + std::to_string(flag));
            return -1;
    }
}

 
template <typename T>
void sort_insert2(std::vector<T>& vec, const T& item) {
    if (vec.empty() || !(vec.back() > item)) {
        vec.push_back(item);
    } else {
        int pos = binary_search(vec, item, BinarySearchGEFlag);
        vec.insert(vec.begin() + pos, item);
    }
}

} // namespace Util
} // namespace TickDB
