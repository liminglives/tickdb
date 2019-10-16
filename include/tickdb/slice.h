#pragma once

#include <string>
#include <string.h>
#include <assert.h>

namespace TickDB {

class Slice {
public:
    Slice() : _data(nullptr), _size(0) {};
    Slice(const char* data, const size_t size) : _data(data), _size(size) {};
    Slice(const void* data, const size_t size) : _data(static_cast<const char*>(data)), _size(size) {};
    Slice(const std::string& str) : _data(str.c_str()), _size(str.size()) {};

    Slice(const Slice& v) = default;
    Slice& operator=(const Slice& v) = default;
    Slice(Slice&& v) = default;
    Slice& operator=(Slice&& v) = default;

    void copy(const Slice& s) {
        _data = s.data();
        _size = s.size();
    };

    void init(const char* data, const size_t size) {
        _data = data;
        _size = size;
    };

    const char* data() const {
        return _data;
    }

    size_t size() const {
        return _size;
    }

    bool empty() const {
        return _size == 0;
    }

    char operator[](size_t n) const {
        assert(n < _size);
        return _data[n];
    }

    void clear() {
        _data = nullptr;
        _size = 0;
    }

    void remove_prefix(size_t n) {
        assert(n < _size);
        _data += n;
        _size -= n;
    }

    std::string to_string() const {
        return std::move(std::string(_data, _size));
    }

    int compare(const Slice& s) const;

    template <typename T> 
    T get() const {
        return *(static_cast<T*>(static_cast<void*>(const_cast<char*>(_data))));
    }

    template <typename T> 
    const T* get_ptr() const {
        return static_cast<T*>(static_cast<void*>(const_cast<char*>(_data)));
    }

private:
    const char* _data;
    size_t _size;
};

inline bool operator==(const Slice& a, const Slice& b) {
    return (a.size() == b.size()) && (memcmp(a.data(), b.data(), a.size()) == 0);
}

inline bool operator!=(const Slice& a, const Slice& b) {
    return !(a == b);
}

inline int Slice::compare(const Slice& b) const {
    const size_t min = (size() < b.size()) ? size() : b.size();
    int ret = memcmp(data(), b.data(), min);
    if (ret == 0) {
        if (size() < b.size()) ret = -1;
        else if (size() > b.size()) ret = 1;
    }
    return ret;
}

inline bool operator<(const Slice& a, const Slice& b) {
    return a.compare(b) < 0;
}

inline bool operator>(const Slice& a, const Slice& b) {
    return a.compare(b) > 0;
}

inline bool operator<=(const Slice& a, const Slice& b) {
    return !(a.compare(b) > 0);
}

inline bool operator>=(const Slice& a, const Slice& b) {
    return !(a.compare(b) < 0);
}

} // namespace TickDB
