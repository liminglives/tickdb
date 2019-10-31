#pragma once

#include <string.h>

#include "tickdb/defines.h"
#include "db/memory_allocator.h"

namespace TickDB {

struct BlockHeader {
    uint64_t len;
    uint64_t capacity;
};

class Block {
public:
    Block() = default;
    ~Block() {
        if (_memory_block != nullptr) {
            delete _memory_block;
        }
    }

    // init a new block 
    bool init(MemoryBlock* memory_block) {
        _memory_block = memory_block;
        _buf = memory_block->data();
        uint64_t capacity = memory_block->size();

        if (capacity < sizeof(BlockHeader)) {
            Log("error, block capacity is less than sizeof BlockHeader");
            return false;
        }

        _header = static_cast<BlockHeader*>(static_cast<void*>(_buf));
        _header->len = sizeof(BlockHeader);
        _header->capacity = capacity;

        return true;
    }

    // watch an exist block
    bool watch(MemoryBlock* memory_block) {
        _memory_block = memory_block;
        _buf = memory_block->data();

        _header = static_cast<BlockHeader*>(static_cast<void*>(_buf));

        return true;
    }

    const char* append(const Slice& row_header, const Slice& row) {
        const uint64_t data_size = row_header.size() + row.size();
        if ((data_size) > (_header->capacity - _header->len)) {
            return nullptr;
        }

        char* start = _buf + _header->len;
        memcpy(start, row_header.data(), row_header.size());
        memcpy(start + row_header.size(), row.data(), row.size());

        _header->len += data_size;

        return start;
    }

    const MemoryBlock* memory_block() {
        return _memory_block;
    }

    const BlockHeader* block_header() {
        return _header;
    }

    uint64_t size() {
        return _header->len;
    }

    const char* data() {
        return _memory_block->data();
    }

private:
    MemoryBlock* _memory_block = nullptr;

    char* _buf = nullptr;
    BlockHeader* _header = nullptr;
    
};

class BlockReader {
public:
    BlockReader(Block* block) {
        _pos = sizeof(BlockHeader);
        _block_start = const_cast<char*>(block->data());
        _size = block->size();
    }
    ~BlockReader() = default;

    bool next(Slice& row) {
        if (_pos >= _size) {
            return false;
        }

        char* cur = _block_start + _pos;

        RowHeader* row_header = static_cast<RowHeader*>(static_cast<void*>(cur));
        row.init(cur, row_header->len);

        _pos += sizeof(RowHeader) + row_header->len;

        return true;
    }

private:
    char* _block_start;
    uint64_t _pos = 0;
    uint64_t _size = 0;
};



} // namespace TickDB
