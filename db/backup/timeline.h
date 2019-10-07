#pragma once

#include <string>
#include <vector>
#include <unordered_map>

#include "defines.h"
#include "util.h"

namespace TickDB {

template <typename T>
class Timeline {
public:
    Timeline();
    ~Timeline();

    void insert(const T& ts, Slice* row);

    Slice* get(const T& ts);
    Slice* at(int index);

    Slice* gt(const T& ts); // great than: >
    Slice* ge(const T& ts); // great or equal: >=
    Slice* lt(const T& ts); // less than: <
    Slice* le(const T& ts); // less or equal: <=

    bool index_range(int start, int end, std::vector<Slice*>& rows); // [start, end]
    bool time_range(const T& start, uint64_t end, std::vector<Slice*>& rows); // [start, end]

    bool shift_left(const T& ts, int n, std::vector<Slice*>& rows);
    bool shift_right(const T& ts, int n, std::vector<Slice*>& rows);

    int get_index_by_time(constT& ts, QueryOP op);

    size_t size() {
        return _ts_index.size();
    }

    void del(const T& ts);
    std::vector<Slice*> del(const T& start, const T& end);

    // for test
    void print() {
        for (auto ts : _ts_index) {
            std::cout << ts << ",";
        }
        std::cout << std::endl;
    }

private:
    std::unordered_map<T, Slice*> _ts_data_map;
    std::vector<T> _ts_index;
};

} // namespace TickDB
