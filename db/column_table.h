#pragma once 

class ColumnTable {
public:
    ColumnTable() {}
    ~ColumnTable() {
        for (auto it : _column_map) {
            delete it.second;
        }
    }

    bool init(const TableOption& opt) {
        _table_name = opt.table_name;

        std::vector<std::pair<std::string, std::string>> schema;
        if (!Util::to_kv_vec(opt.table_header, schema)) {
            Log("error, table header:" + opt.table_header);
            return false;
        }
        if (!_table_meta.init(schema)) {
            Log("error, init follower meta:" + opt.table_header);
            return false;
        }

        auto col_metas = _table_meta.get_col_metas();
        for (auto col_meta : col_metas) {
            Column* col = new Column(col_meta.name, col_meta.enum_type);
            if (!col->init()) {
                delete col;
                return false;
            }
            _column_map[col_meta.name] = col;
        }

        return true;
    }

    void append(const std::unordered_map<std::string, Slice>& columns_data) {
        for (auto it : columns_data) {
            append(it.first, it.second);
        }
    }

    void append(const std::string& column, const Slice& column_data) {
        Column* col = get_column(column);
        if (col == nullptr) {
            return;
        }
        col->push_back(column_data);
    }

    bool range(const std::vector<Slice>& key_vec, 
            const std::vector<uint64_t>& time_vec, 
            std::unordered_map<std::string, Slice>& res);

    Column* get_column(const std::string& col) {
        auto it = _column_map.find(col);
        return it == _column_map.end() ? nullptr : it->second;
    }

private:
    std::string _table_name;

    std::string _key_column_name;
    std::string _index_column_name;

    // reuse FollowerBufferMeta
    ::FlowBuffer::FlowBufferMeta _table_schema;

    std::unordered_map<std::string, Column*> _column_map;
};

