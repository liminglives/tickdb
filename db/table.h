#pragma once

#include <limits>
#include <unordered_set>

#include "tickdb/flowbuffer.h"
#include "tickdb/data_index.h"
#include "tickdb/slice.h"
#include "tickdb/defines.h"
#include "tickdb/option.h"

#include "db/data_parser.h"
#include "db/block.h"
#include "db/memory_allocator.h"
#include "db/util.h"
#include "db/common.h"

namespace TickDB {

struct BlockInfo {
    Block* block = nullptr;

    uint64_t ts_del_start = 0;
    uint64_t ts_del_end = 0;

    std::string date;

    ~BlockInfo() {
        if (block != nullptr) {
            delete block;
            block = nullptr;
        }
    }
};

struct TableScale {
    uint64_t data_len = 0;
    int16_t status = 0;
};

class TableSHMMeta {
public:
    TableSHMMeta(Slice& meta) : _shm_meta(meta), _raw_shm_meta(meta) {}
    ~TableSHMMeta() = default;

    bool next(std::string& shm_name) {
        if (_shm_meta.size() < sizeof(uint32_t)) {
            Log("shm reach limits, it has no more next table block");
            return false;
        }
        uint32_t val_len = _shm_meta.get<uint32_t>();
        if (val_len == 0) {
            Log("shm has no next table block yet");
            return false;
        }

        _shm_meta.remove_prefix(sizeof(val_len));
        if (_shm_meta.size() < val_len) {
            Log("shm has error next table block");
            return false;
        }
        Slice next_block_name(_shm_meta.data(), val_len);
        _shm_meta.remove_prefix(val_len);

        shm_name = next_block_name.to_string();

        return true;
    }

private:
    Slice _shm_meta;
    Slice _raw_shm_meta;
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
        Log("Release Table " + _table_name);
        if (_data_parser) {
            delete _data_parser;
        }
        if (_data_index) {
            delete _data_index;
        }

        for(auto item : _block_info_vec) {
            delete item;
        }

        if (_table_shm_meta) {
            delete _table_shm_meta;
        }

        if (_cur_block_reader) {
            delete _cur_block_reader;
        }

    }

    bool init(const TableOption& opt) {
        _table_name = opt.table_name;
        _default_block_size = opt.block_size;
        _table_type = opt.table_type;

        Log("init table, table_name:" + opt.table_name + " table_type:" + std::to_string(opt.table_type));

        if (_table_type == TableType_Watcher) {
            _first_shm_name = opt.shm_name;
            if (_first_shm_name.empty()) {
                Log("missing shm name");
                return false;
            }

            update();
            //// parse first shm block
            //if (!parse_shm(opt.shm_name)) {
            //    Log("parse first shm block failed");
            //    return false;
            //}
            //// update shm block according to first shm block
            //update_shm();

        } else { // TableType_Normal, TableType_Shared

            if (!parse_header(opt.table_header)) {
                return false;
            }

            if (_table_type == TableType_Shared) {
                Slice data(opt.table_header);
                append(data, RowType_TableHeader, nullptr);

                TableScale table_scale;
                Slice scale_data(&(table_scale), sizeof(TableScale));
                append(scale_data, RowType_TableScale);

                char* shm_meta = (char*)malloc(opt.shm_meta_size);
                memset(shm_meta, 0, opt.shm_meta_size);
                append(Slice(shm_meta, opt.shm_meta_size), RowType_TableSHMMeta);
                delete shm_meta;
            }
        }

        return true;
    }

    //void update_shm() {
    //    if (_table_shm_meta == nullptr) {
    //        return;
    //    }

    //    std::string shm_name;
    //    while (_table_shm_meta->next(shm_name)) {
    //        parse_shm(shm_name);
    //    }
    //}

    BlockInfo* next_shm_block() {
        std::string shm_name;
        if (!_first_shm_loaded) {
            shm_name = _first_shm_name;
            _first_shm_loaded = true;
        } else {
            if (!_table_shm_meta->next(shm_name)) {
                return nullptr;
            }
        }
        if (shm_name.empty()) {
            Throw("watch shm block name is empty");
        }
        
        if (_loaded_shm_name_set.find(shm_name) != _loaded_shm_name_set.end()) {
            Log("had loaded shm block:" + shm_name);
            return nullptr;
        }

        _loaded_shm_name_set.insert(shm_name);
        Log("loading shm block:" + shm_name);

        BlockInfo* block_info = new_block_info(0, shm_name);
        if (block_info == nullptr) {
            Throw("watch shm block failed, shm:" + shm_name);
        }

        return block_info;
    }

    void update() {
        while (true) {
            if (_cur_block_reader == nullptr) {
                BlockInfo* block_info = next_shm_block();
                if (block_info == nullptr) {
                    break;
                }
                _cur_block_reader = new BlockReader(block_info->block);
            } 

            Slice row;
            while(_cur_block_reader->next(row)) {
                process(row);
            }

            if (_cur_block_reader->end()) {
                delete _cur_block_reader;
                _cur_block_reader = nullptr;
            } else {
                break;
            }
        }
    }

    //bool parse_shm(const std::string& shm_name) {
    //    if (_loaded_shm_name_set.find(shm_name) != _loaded_shm_name_set.end()) {
    //        Log("had loaded shm block:" + shm_name);
    //        return false;
    //    }
    //    BlockInfo* block_info = new_block_info(0, shm_name);
    //    if (block_info == nullptr) {
    //        Throw("watch shm block failed, shm:" + shm_name);
    //    }

    //    read_block(block_info->block);
    //    _loaded_shm_name_set.insert(shm_name);
    //    Log("loaded shm block:" + shm_name);
    //    return true;
    //} 

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
        if (_data_index != nullptr) {
            process(row);
        }
        if (_table_scale) {
            _table_scale->data_len += row.size();
        }
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
            case RowType_TableScale: 
                _table_scale = data.get_ptr<TableScale>();
                break;
            case RowType_TableSHMMeta: 
                _table_shm_meta = new TableSHMMeta(data);
                break;
            default:
                Throw("unknown row type:" + std::to_string(RowParser::row_type(row)));
        }
    }

    void insert_to_index(Slice& hrow) {
        Slice row(hrow);
        row.remove_prefix(sizeof(RowHeader));
        Slice index = _data_parser->index(RowParser::row_data(hrow));
        _data_index->insert(_data_parser->key(RowParser::row_data(hrow)), 
                   index, row);
        //if (_index_type == IndexType_TimeSeries) {
        //    BlockInfo* block_info = get_latest_block_info();
        //    uint64_t int_ts = index.get<uint64_t>();
        //    block_info->ts_start = std::min(int_ts, block_info->ts_start);
        //    block_info->ts_end = std::max(int_ts, block_info->ts_end);

        //    block_info->ts_start = std::min(int_ts, block_info->ts_start);
        //    block_info->ts_end = std::max(int_ts, block_info->ts_end);
        //}
    }

    void del_from_index(Slice& hrow) {
        _data_index->del(hrow);
    }

    // [ts_start, ts_end]
    bool del(const Slice& key, uint64_t ts_start, uint64_t ts_end) {
        std::string data;
        data = Common::pack_time_series_del_params(key, ts_start, ts_end, IndexType_TimeSeries);
        return append(Slice(data), RowType_Del);
    }

    // [date_start, date_end]
    void del(const std::string& date_start, const std::string& date_end);


    IDataIndex* get_index() {
        return _data_index;
    }

    const ::FlowBuffer::FlowBufferMeta* get_flowbuffer_meta() {
        return &_flowbuffer_meta;
    }

    size_t size(const Slice& key) {
        return _data_index->size(key);
    }

    std::vector<Slice> keys() {
        return _data_index->keys();
    }

private:

    std::string gen_block_name() {
        return _table_name + "_block_" + std::to_string(_block_info_vec.size());
    }

    MemoryBlock* new_memory_block(uint64_t block_size, const std::string& block_name = "") {
        MemoryBlock* ret = nullptr;
        switch (_table_type) {
            case TableType_Normal:
                ret = MemoryAllocator::create_memory_block(block_size);
                break;
            case TableType_Shared:
                ret = MemoryAllocator::create_share_memory_block(gen_block_name(), block_size);
                break;
            case TableType_Watcher:
                ret = MemoryAllocator::watch_share_memory_block(block_name);
                break;
            default:
                Throw("unknown table type:" + std::to_string(_table_type));
        }
        if (ret == nullptr) {
            Throw("new memory block failed, block_size:" + std::to_string(block_size));
        }
        return ret;
    }

    BlockInfo* new_block_info(uint64_t block_size, const std::string& block_name = "") {
        MemoryBlock* memory_block = new_memory_block(block_size, block_name);
        if (memory_block == nullptr) {
            return nullptr;
        }

        Block* block = new Block();
        if (_table_type == TableType_Watcher) {
            block->watch(memory_block);
        } else {
            block->init(memory_block);
        }

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

    void read_block(Block* block) {
        BlockReader reader(block);
        Slice row;
        while(reader.next(row)) {
            process(row);
        }
    }

private:
    std::string _table_name;
    EnumTableType _table_type;
    EnumIndexType _index_type;
    EnumDataType _data_type;

    TableScale* _table_scale = nullptr;
    TableSHMMeta* _table_shm_meta = nullptr;
    std::unordered_set<std::string> _loaded_shm_name_set;

    ::FlowBuffer::FlowBufferMeta _flowbuffer_meta;

    std::vector<BlockInfo*> _block_info_vec;

    IDataParser* _data_parser = nullptr;
    IDataIndex* _data_index = nullptr;
    std::string _key_column_name;
    std::string _index_column_name;

    uint64_t _default_block_size = 1024 * 1024;

    // for watcher
    BlockReader* _cur_block_reader = nullptr;
    std::string _first_shm_name;
    bool _first_shm_loaded = false;

};

} // namespace TickDB
