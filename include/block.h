#pragma once

namespace TickDB {

struct BlockHeader {
    uint64_t len;
    uint64_t capacity;
};

class Block {
public:
    Block();
    ~Block() {
        if (_owner && _buf != nullptr) {
            free(nullptr);
            _buf = nullptr;
        }
    }

    // init a new block 
    bool init(char* buf, uint64_t capacity) {
        _owner = true;
        _buf = buf;

        if (capacity < sizeof(BlockHeader)) {
            LOG("error, block capacity is less than sizeof BlockHeader")
            return false;
        }

        _header = static_cast<BlockHeader*>(static_cast<void*>(_buf));
        _header->len = sizeof(BlockHeader);
        _header->capacity = capacity;

        return true;
    }

    // init from exist block
    bool init(char* buf) {
        _owner = false;
        _buf = buf;

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

    const char* append_line(const Slic)

private:
    char* _buf = nullptr;
    BlockHeader* _header = nullptr;

    bool _owner = false;
    
};

} // namespace TickDB
