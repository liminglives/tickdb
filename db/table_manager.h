#pragma once

#include "db/table.h"

namespace TickDB {

class TableManager {
public:
    TableManager() = default;
    ~TableManager() {
        for (auto it : _table_map) {
            delete it.second;
        }
    }

    bool add_table(const std::string& table_name, Table* table) {
        if (_table_map.find(table_name) != _table_map.end()) {
            return false;
        }
        _table_map[table_name] = table;
        return true;
    }

    Table* get_table(const std::string& table_name) {
        auto it = _table_map.find(table_name);
        return it == _table_map.end() ? nullptr : it->second;
    }


    std::vector<std::string> get_all_table_name() {
        std::vector<std::string> ret;
        for (auto it : _table_map) {
            ret.push_back(it.first);
        }
        return std::move(ret);
    }

private:
    std::unordered_map<std::string, Table*> _table_map;
};



} // namespace TickDB
