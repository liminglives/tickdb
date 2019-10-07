#pragma once 

#include <string>
#include <vector>

#include "defines.h"

namespace TickDB {

struct TableOption {
    std::string table_name;
    std::string table_header;
    uint64_t block_size = 1024 * 1024;
    EnumTableType table_type = TableType_Normal;

    std::vector<int> shm_key_vec;
};

struct Option {
    std::string db_output;
};

} // namespace TickDB
