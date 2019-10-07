#include "util.h"

#include <chrono>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <functional>

namespace TickDB {

void Print(const std::string& file, int line, const std::string& func, const std::string& info) {
    auto pos = file.find_last_of("/");
    std::cout << file.substr(pos + 1) << ":" << line << " " << func << "(): " << info << std::endl;
}

namespace Util {

void split(const std::string& src, const std::string& separator, std::vector<std::string>& dest) {
	//dest.clear();
    using namespace std;
    if (src.empty()) {
        return;
    }
    string str = src;
    string substring;
    string::size_type start = 0, index;

    do {
        index = str.find_first_of(separator,start);
        if (index != string::npos) {    
            substring = str.substr(start,index-start);
            dest.push_back(trim(substring));
			start = index + separator.size();
            //start = str.find_first_not_of(separator,index);
            if (start == string::npos) return;
        }
    } while (index != string::npos);
    
    //the last token
    substring = str.substr(start);
    dest.push_back(trim(substring));
}

std::string& trim(std::string &s) {  
    if (s.empty()) {  
        return s;  
    }  
    s.erase(0,s.find_first_not_of(" "));  
    s.erase(s.find_last_not_of(" ") + 1);  
    return s;  
} 

std::time_t getTimeStamp() {
    std::chrono::time_point<std::chrono::system_clock,std::chrono::milliseconds> tp = 
        std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
    auto tmp=std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch());
    std::time_t timestamp=tmp.count();
    //std::time_t timestamp=std::chrono::system_clock::to_time_t(tp);
    return timestamp;
}

bool file_exists(const std::string& path) {
    return access(path.c_str(), F_OK) == 0;
}

uint64_t get_file_size(const std::string& fname) {
    struct stat info;
    stat(fname.c_str(), &info);
    return info.st_size;
}

int binary_search(const std::vector<uint64_t>& ts_vec, uint64_t ts, int flag, int start, int end) {
    if (end == -1) {
        end = ts_vec.size() - 1;
    }
    if (end < start) {
        return -1;
    }
    while (end >= start) {
        int middle = (start + end) / 2;
        if (ts_vec[middle] == ts) {
            if (flag == BinarySearchLTFlag) {
                return middle - 1;
            } else if (flag == BinarySearchGTFlag) {
                return middle + 1;
            } 
            return middle;
        } else if (ts_vec[middle] > ts) {
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

void sort_insert(std::vector<uint64_t>& ts_vec, uint64_t ts) {
    if (ts_vec.empty() || ts_vec.back() <= ts) {
        ts_vec.push_back(ts);
    } else {
        int pos = binary_search(ts_vec, ts, BinarySearchGEFlag);
        ts_vec.insert(ts_vec.begin() + pos, ts);
    }
}

bool to_kv(const std::string& str, std::string& k, std::string& v) {
    std::string::size_type pos = str.find_first_of('=');
    if (pos == std::string::npos) {
        return false;
    }

    k = str.substr(0, pos - 0);
    k = trim(k);
    v = str.substr(pos + 1);
    v = trim(v);

    return true;
}

bool to_kv_vec(const std::string& str, std::vector<std::pair<std::string, std::string>>& kv_vec) {
    std::vector<std::string> items;
    split(str, ",", items);
    for (auto& item : items) {
        std::string k;
        std::string v;
        if (!to_kv(item, k, v)) {
            return false;
        }
        kv_vec.emplace_back(k, v);
    }
    return true;
}

bool to_kv_map(const std::string& str, std::unordered_map<std::string, std::string>& kv_map) {
    std::vector<std::pair<std::string, std::string>> kv_vec;
    if (!to_kv_vec(str, kv_vec)) {
        return false;
    }
    for (auto& it : kv_vec) {
        kv_map[it.first] = it.second;
    }
    return true;
}

template <> 
void serialize<std::string>(const std::string& val, std::string& str) {
    str = val;
}

//template <> 
//void serialize<char*>(const char*& val, std::string& str) {
//    str.assgin(val);
//}
//
void serialize(const char* val, std::string& str) {
    str.assign(val);
}

static std::hash<std::string> s_hash_str;

uint64_t hashcode(const std::string& str) {
    return static_cast<uint64_t>(s_hash_str(str));
}

std::vector<std::string> to_lines(const std::string& data) {
    size_t start = 0;
    std::vector<std::string> ret;
    for (size_t i = 0; i < data.size(); ++i) {
        if (data[i] == '\n') {
            std::string line(data, start, i - start);
            ret.push_back(line);
            start = i + 1;
        }
    }
    if (start < data.size()) {
        ret.emplace_back(data, start, data.size() - start);
    }
    return std::move(ret);
}


} // namespace Util
} // namespace TickDB
