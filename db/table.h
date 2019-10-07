#pragma once

#include "tickdb/flowbuffer.h"
#include "tickdb/data_index.h"
#include "tickdb/slice.h"
#include "tickdb/defines.h"
#include "tickdb/option.h"
#include "db/data_parser.h"
#include "db/block.h"
#include "db/memory_allocator.h"
#include "db/util.h"

namespace TickDB {

struct BlockInfo {
    Block* block = nullptr;

    uint64_t ts_start = 0;
    uint64_t ts_end = 0;

    uint64_t ts_avai_start = 0;
    uint64_t ts_avai_end = 0;

    std::string date;

    ~BlockInfo() {
        if (block != nullptr) {
            delete block;
            block = nullptr;
        }
    }
};

struct TableDataHeader {
    uint64_t data_len;
    uint64_t capacity;
    int16_t status;
};

class RowParser { 
public:
    static RowHeader* row_header(const Slice& row) {
        return (RowHeader*)(row.data());
    }

    static const char* row_data(const Slice& row) {
        return row.data() + sizeof(RowHeader);
    }

    static uint32_t row_data_len(const Slice& row) {
        return row_header(row)->len;
    }

    static EnumRowType row_type(const Slice& row) {
        return static_cast<EnumRowType>(row_header(row)->type);
    }

};

class Table {
public:
    Table() = default;
    ~Table() {
        if (_data_parser) {
            delete _data_parser;
        }
        if (_data_index) {
            delete _data_index;
        }

        for(auto item : _block_info_vec) {
            delete item;
        }

        //delete [] _block_info_vec;
    }

    bool init(const TableOption& opt) {
        _table_name = opt.table_name;
        _default_block_size = opt.block_size;
        _table_type = opt.table_type;

        if (!parse_header(opt.table_header)) {
            return false;
        }

        if (_table_type == TableType_Shared) {
            Slice data(opt.table_header);
            append(data, RowType_TableHeader, nullptr);
        }

        return true;
    }

    bool append(const Slice& data, EnumRowType row_type = RowType_Insert, Slice* output = nullptr) {
        RowHeader row_header;
        row_header.len = data.size();
        row_header.type = row_type;
        Slice header((char*)(&row_header), sizeof(RowHeader));
        BlockInfo* block_info = get_latest_block_info();
        const char* data_addr = block_info->block->append(header, data);
        if (data_addr == nullptr) {
            uint64_t block_size = data.size() > (_default_block_size << 1) ? _default_block_size >> 1 : _default_block_size;
            BlockInfo* block_info = new_block_info(block_size);
            data_addr = block_info->block->append(header, data);
        }
        if (output) {
            output->init(data_addr + sizeof(RowHeader), row_header.len);
        }
        Slice row(data_addr, sizeof(RowHeader) + row_header.len);
        // TODO: leak
        process(row);
        return true;
    }

    void process(Slice& row) {
        RowHeader* row_header = (RowHeader*)(row.data());
        Slice data(row.data() + sizeof(RowHeader), row_header->len);
        switch (RowParser::row_type(row)) {
            case RowType_Insert: 
                insert_to_index(row);
                break;
            case RowType_Del: 
                del_from_index(row);
                break;
            case RowType_TableHeader: 
                if (_table_type == TableType_Watcher) {
                    parse_header(data.to_string());
                }
                break;
            default:
                Throw("unknown row type:" + std::to_string(RowParser::row_type(row)));
        }
    }

    void insert_to_index(Slice& hrow) {
        if (_data_index != nullptr) {
            Slice row(hrow);
            row.remove_prefix(sizeof(RowHeader));
            _data_index->insert(_data_parser->key(RowParser::row_data(hrow)), 
                    _data_parser->index(RowParser::row_data(hrow)), row);
        }
    }

    void del_from_index(Slice& row) {
    }

    // [ts_start, ts_end]
    void del(const Slice& key, uint64_t ts_start, uint64_t ts_end);
    // [date_start, date_end]
    void del(const std::string& date_start, const std::string& date_end);


    IDataIndex* get_index() {
        return _data_index;
    }

    const ::FlowBuffer::FlowBufferMeta* get_flowbuffer_meta() {
        return &_flowbuffer_meta;
    }

private:
    BlockInfo* new_block_info(uint64_t block_size) {
        Block* block = new Block();
        MemoryBlock* memory_block = MemoryAllocator::create_memory_block(block_size);
        block->init(memory_block);

        BlockInfo* block_info = new BlockInfo;
        block_info->block = block;
        _block_info_vec.push_back(block_info);

        return block_info;
    }

    BlockInfo* get_latest_block_info() {
        if (_block_info_vec.empty()) {
            new_block_info(_default_block_size);
        }

        return _block_info_vec.back();
    }

    bool parse_header(const std::string& header) {
        std::vector<std::string> header_lines = Util::to_lines(header);
        for (size_t i = 0; i < header_lines.size(); ++i) {
            Log(_table_name + ":" + header_lines[i]);
            switch (i) {
                case 0: {
                    // data_type = 
                    std::unordered_map<std::string, std::string> kv_map;
                    if (!Util::to_kv_map(header_lines[i], kv_map)) {
                        Log("error meta line:" + header_lines[i]);
                        return false;
                    }
                    _data_type = kv_map.find("data_type") != kv_map.end() ? 
                        static_cast<EnumDataType>(std::stoi(kv_map["data_type"])) : DataType_Flowbuffer;
                    break;
                } // case 0
                case 1: {
                    // schema
                    std::vector<std::pair<std::string, std::string>> schema;
                    if (!Util::to_kv_vec(header_lines[i], schema)) {
                        Log("error shema line:" + header_lines[i]);
                        return false;
                    }
                    if (_data_type == DataType_Flowbuffer) {
                        if (!_flowbuffer_meta.init(schema)) {
                            Log("error shema:" + header_lines[i]);
                            return false;
                        }
                    }
                    break;
                } // case 1
                case 2: {
                    // index_type=,key_column=,index_column=
                    std::unordered_map<std::string, std::string> index_kv_map;
                    if (!Util::to_kv_map(header_lines[i], index_kv_map)) {
                        Log("error index line:" + header_lines[i]);
                        return false;
                    }
                    _index_type = index_kv_map.find("index_type") != index_kv_map.end() ? 
                        static_cast<EnumIndexType>(std::stoi(index_kv_map["index_type"])) : IndexType_TimeSeries;
                    _key_column_name = index_kv_map.find("key_column") != index_kv_map.end() ?
                        index_kv_map["key_column"] : "";
                    _index_column_name = index_kv_map.find("index_column") != index_kv_map.end() ? 
                        index_kv_map["index_column"] : "";

                    switch (_index_type) {
                        case IndexType_None:
                            break;
                        case IndexType_TimeSeries:
                            if (_key_column_name.size() == 0 || _index_column_name.size() == 0) {
                                Log("TimeSeries error, key_column_name or index_column_name is empty");
                                return false;
                            }
                            _data_index = new TimeSeriesDataIndex();
                            break;
                        case IndexType_KV:
                            if (_key_column_name.size() == 0) {
                                Log("KV error, key_column_name is empty");
                                return false;
                            }
                            _data_index = new KVDataIndex();
                            break;
                        default:
                            Log("unknown index:" + std::to_string(_index_type));
                            break;
                    }
                    break;
                } // case 2

                default:
                    Log("unexpected line:" + header_lines[i]);
                    break;
            }
        }

        // only support flowbuffer for now
        if (_data_type == DataType_Flowbuffer) {
            _data_parser = new FlowBufferDataParser(_key_column_name, _index_column_name, &_flowbuffer_meta);
        } else {
            Log("only support flowbuffer for now");
            return false;
        }
        return true;
    }

private:
    std::string _table_name;
    EnumTableType _table_type;
    EnumIndexType _index_type;
    EnumDataType _data_type;
    
    ::FlowBuffer::FlowBufferMeta _flowbuffer_meta;

    std::vector<BlockInfo*> _block_info_vec;

    IDataParser* _data_parser = nullptr;
    IDataIndex* _data_index = nullptr;
    std::string _key_column_name;
    std::string _index_column_name;

    uint64_t _default_block_size = 1024 * 1024;

};

} // namespace TickDB
