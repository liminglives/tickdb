#pragma once

#include "tickdb/flowbuffer.h"
#include "tickdb/slice.h"
#include "db/util.h"

namespace TickDB {

class IDataParser {
public:
    IDataParser() = default;
    virtual ~IDataParser() {};

    virtual Slice key(const char* data) = 0; 
    virtual Slice index(const char* data) = 0;
};

class FlowBufferDataParser : public IDataParser {
public:
    FlowBufferDataParser(const std::string& key_column_name, 
            const std::string& index_column_name, 
            ::FlowBuffer::FlowBufferMeta* meta) : 
        _key_column_name(key_column_name), 
        _index_column_name(index_column_name), 
        _meta(meta) {}

    ~FlowBufferDataParser() {};

    Slice key(const char* data) override {
        char* str = nullptr;
        size_t size = 0;
        if (::FlowBuffer::to_cstring(_meta, const_cast<char*>(data), _key_column_name, str, size) != ::FlowBuffer::RET_OK) {
            Throw("data has no key");
        }
        return Slice(str, size);
    }

    Slice index(const char* data) override {
        char* str = nullptr;
        size_t size = 0;
        if (::FlowBuffer::to_cstring(_meta, const_cast<char*>(data), _index_column_name, str, size) != ::FlowBuffer::RET_OK) {
            Throw("data has no index");
        }

        return Slice(str, size);
    }

private:
    // TODO: to index
    std::string _key_column_name;
    std::string _index_column_name;
    ::FlowBuffer::FlowBufferMeta* _meta;
};


} // namespace TickDB
