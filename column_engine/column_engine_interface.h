#pragma once

#include <iostream>
#include "engine_types.h"

namespace TickDB
{
namespace column_engine
{

class WriteSession
{
public:
    WriteSession(){};
    virtual ~WriteSession(){};

    // 1. check memory enough
    //    if not, create 2x memory
    // 2. check key valid (new or in writing)
    // 3. check ts valid (in interval and not decrease by key)
    // 4. write data
    // 5. update index
    bool WriteRowData(const RowData& rowData);
    
    bool WriteColumnData(const ColumnData& columnData);

    bool GetSessionInfo(SessionInfo& info);

    // 1. check key valid
    // 2. check ts valid
    // 3. find index range
    // 4. get request columns
    bool Query(QueryResult& result, const QueryRequest& request);
    
};

class ColumnEngineInterface
{
public:
    ColumnEngineInterface(){};
    virtual ~ColumnEngineInterface(){};
    
    bool Init(const EngineConfig& config);

    // store schema
    bool CreateTable(const std::string& tableName, const TableConfig& config, const TableSchema& schema);
    
    bool GetTables(std::map<std::string, TableSchema>& tables);

    // create memory in session time interval
    bool CreateWriteSession(WriteSessionPtr& session, const SessionConfig& config, const SessionInfo& info);
    
    bool GetWriteSessions(map<SessionInfo, WriteSessionPtr>& sessions, const SessionInfo& info);

    // choose write sessions to query
    bool Query(QueryResponse& response, const std::string& tableName, const QueryRequest& request);
    
    void Query(QueryResponseCallback& response, const std::string& tableName, const QueryRequest& request);
};
}
}
