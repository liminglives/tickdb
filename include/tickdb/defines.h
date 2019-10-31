#pragma once

namespace TickDB {

struct RowHeader {
    uint32_t len = 0;
    uint8_t type = 0;
    uint8_t proto = 0; // only flowbuffer
    uint8_t status = 0; // 0: available, 1: deleted 
    uint8_t reserved = 0; 
};

enum EnumRowType {
    RowType_Insert = 0,
    RowType_Del = 1,
    RowType_TableHeader = 2,
    RowType_TableScale = 3,
    RowType_TableSHMMeta = 4,
};

enum EnumIndexType {
    IndexType_None = 0,
    IndexType_TimeSeries = 1,
    IndexType_KV = 2,
};

enum EnumTableType {
    TableType_Normal = 0,
    TableType_Shared = 1,
    TableType_Watcher = 2,
};
enum EnumDataType {
    DataType_Flowbuffer = 0,
    DataType_Struct = 1,
};

enum class QueryOP : int {
    EQ = 1,
    GT,
    GE,
    LT,
    LE,
    INDEX,
};



} // namespace TickDB
