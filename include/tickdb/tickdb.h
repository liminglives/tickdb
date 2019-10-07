#pragma once

#include "db/table_manager.h"
#include "tickdb/option.h"

namespace TickDB {

class TickDB {
public:
    TickDB() = default;
    ~TickDB() = default;

    bool init(const Option& opt) {
        return true;
    }

    bool create_table(const TableOption& table_opt) {
        const std::string& table_name = table_opt.table_name;
        if (get_table(table_name)) {
            Log(table_name + " already exists");
            return false;
        }

        Table* table = new Table();
        if (!table->init(table_opt)) {
            Log(table_name + " init failed");
            return false;
        }

        return _table_manager.add_table(table_name, table);
    }

    bool insert(const std::string& table_name, const char* data, uint64_t len, Slice* output = nullptr) {
        return insert(table_name, Slice(data, len));
    }

    bool insert(const std::string& table_name, const Slice& data, Slice* output = nullptr) {
        Table* table = get_table(table_name);
        return  table == nullptr ? false : table->append(data, RowType_Insert, output);

        //if (table == nullptr) {
        //    Throw("insert failed, " + table_name + " do not exist");
        //}
        //return table->insert(data, RowType_Insert, output);
    }

    void del(const std::string& table_name, const Slice& key, uint64_t ts_start, uint64_t ts_end);
    void del(const std::string& table_name, const Slice& key, const std::string& date_start, const std::string& date_end);

    Table* get_table(const std::string& table_name) {
        return _table_manager.get_table(table_name);
    }

    std::vector<std::string> get_all_table_name() {
        return _table_manager.get_all_table_name();
    }

private:
    TableManager _table_manager;
};

} // namespace TickDB
