#pragma once

#include <string>
#include <vector>
#include <unordered_map>

#include "tickdb/defines.h"
#include "tickdb/slice.h"
#include "db/util.h"

namespace TickDB {

class Timeline {
public:
    Timeline();
    ~Timeline();

    void insert(uint64_t ts, const Slice& row);

    const Slice* get(uint64_t ts);
    const Slice* at(int index);

    const Slice* gt(uint64_t ts); // great than: >
    const Slice* ge(uint64_t ts); // great or equal: >=
    const Slice* lt(uint64_t ts); // less than: <
    const Slice* le(uint64_t ts); // less or equal: <=

    bool index_range(int start, int end, std::vector<const Slice*>& rows); // [start, end]
    bool time_range(uint64_t start, uint64_t end, std::vector<const Slice*>& rows); // [start, end]

    bool shift_left(uint64_t ts, int n, std::vector<const Slice*>& rows);
    bool shift_right(uint64_t ts, int n, std::vector<const Slice*>& rows);

    int get_index_by_time(uint64_t ts, QueryOP op);

    size_t size() {
        return _ts_index.size();
    }

    void del(uint64_t ts);
    std::vector<const Slice*> del(uint64_t start, uint64_t end);

    // for test
    void print() {
        for (auto ts : _ts_index) {
            std::cout << ts << ",";
        }
        std::cout << std::endl;
    }

private:
    std::unordered_map<uint64_t, Slice> _ts_data_map;
    std::vector<uint64_t> _ts_index;
};

} // namespace TickDB
