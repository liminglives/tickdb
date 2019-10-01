#pragma once

namespace TickDB {

struct BlockInfo {
    Block* block;
    uint64_t ts_start;
    uint64_t ts_end;

    uint64_t ts_avai_start;
    uint64_t ts_avai_end;

    std::string date;
};

struct TableDataHeader {
    uint64_t data_len;
    uint64_t capacity;
    int16_t status;
};

class Table {
public:
    Table();
    ~Table();

    void append(const Slice& value);

    // [ts_start, ts_end]
    void del(const Slice& key, uint64_t ts_start, uint64_t ts_end);
    // [date_start, date_end]
    void del(const std::string& date_start, const std::string& date_end);

    void get_index();

private:
    std::string _table_name;
    FlowBufferMeta _flowbuffer_meta;

    std::vector<BlockInfo*> _block_info_vec;
    TableHeader


    Index* _index = nullptr;
    std::string _key_column_name;
    std::string _ts_column_name;

};

} // namespace TickDB
