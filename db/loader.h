#pragma once

#include "db/table.h"
#include "db/csv_reader.h"

namespace TickDB {

void serialize_row(std::unordered_map<std::string, std::string>& row, FlowBuffer::FlowBufferBuilder& builder) {
    for (auto it : row) {
        builder.auto_set(it.first, it.second);
    }
}

bool read_csv(Table* table, const std::string& csvfile_path) {
    CsvReader reader(csvfile_path);
    if (!reader.init()) {
        Log("csv reader failed, file:" << csvfile_path);
        return false;
    }

    // TODO: check columns of table and csvfile 
    //const std::vector<std::string>& file_columns = reader.columns() 


    FlowBuffer::FlowBufferBuilder builder(table->get_flowbuffer_meta());
    try {
        while (reader.HasNextRow()) {
            builder.reset();
            CsvRow row;
            reader.NextRow(&row);
            serialize_row(row, builder);
            table->append(Slice(builder.data(), builder.size()));
        }
    } catch (TickDBException& e) {
        Log("TickDBExcetion: " + e.info());
        return false;
    } catch (FlowBufferException& e) {
        Log("FlowBufferExcetion: " + e.info());
        return false;
    }

    return true;
}

} // namespace TickDB
