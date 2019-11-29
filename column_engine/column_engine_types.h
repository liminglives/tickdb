#pragma once

#include <iostream>
#include <string>
#include <vector>


namespace TickDb
{
namespace column_engine
{

struct EngineConfig
{

};

struct TableConfig
{

};

struct ColumnMeta
{
    uint32_t id;
    std::string name;
    Type type;
}

struct TableSchema
{
    std::string tableName;
    std::vector<ColumnMeta> columns;
    uint32_t oneRowLength;
    uint32_t rowNumber;
    uint32_t keyColumnId;
    std::string keyColumn;
    uint32_t indexColumnId;
    std::string indexColumn;
};

struct SessionConfig
{

};

struct SessionInfo
{
    uint32_t id;
    std::string tableName;
    uint64_t startEpochUsecond;
    uint64_t endEpochUsecond;
};

struct RowData
{
    std::vector<char*> rows;
};

struct ColumnData
{
    std::vector<char*> columns;
    int rowNumber;
};

struct QueryRequest
{
    uint64_t startEpochUsecond;
    uint64_t endEpochUsecond;
    std::vector<std::string> projections;
    std::string key;
};

struct QueryResponse
{
    std::vector<ColumnMeta> columns;
    std::vector<char*> rawData;
    int rowNumber;
};

using QueryResponseCallback = std::function<void(bool, const QueryResponse&)>;


}
}
