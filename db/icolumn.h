#pragma once

#include <string.h>
#include <stdlib.h>

#include <vector>
#include <string>

#include "tickdb/slice.h"

namespace TickDB {


class IColumn {
public:
    IColumn() = default;
    virtual ~IColumn() {};

    IColumn(const IColumn&) = delete;
    IColumn& operator= (const IColumn&) = delete;

    virtual void append(const Slice& val) = 0; 

    virtual Slice at(const size_t pos) = 0; 

    virtual Slice range(const size_t pos_start, const size_t pos_end) = 0; 

};

template <typename T>
class DataArray {
pulic:
    static size_t byte_size(const size_t num_of_value) {
        return num_of_value * sizeof(T);
    }

public:
    DataArray() {}

    DataArray(size_t n) {
        alloc(n);
    }

    DataArray(size_t n, const T& v) {
        alloc(n);
        for (int i = 0; i < n; ++i) {
            memcpy(_buf + byte_size(i), reinterpret_cast<void*>(&v), sizeof(T));
        }
    }

    ~DataArray() {
        dealloc();
    }

    size_t len() {
        return _len;
    }

    size_t capacity() {
        return _capacity;
    }

    Slice at(const size_t pos) {
        return Slice(_buf[pos * sizeof(T)], sizeof(T));
    }

    // [pos_start, pos_end]
    Slice range(const size_t pos_start, const size_t pos_end) {
        if (pos_end >= len()) {
            pos_end = len() - 1;
        }
        if (pos_end < pos_start) {
            return Slice();
        }

        return Slice(_buf[pos_start * sizeof(T)], (pos_end - pos_start + 1) * sizeof(T));
    }

    void push_back(const Slice& val) {
        if (_len + val.size() > _capacity) { // unlikely
            realloc();
        }
        memcpy(_buf + _len, val.data(), val.size());
    }


private:
    void alloc(size_t n) {
        _capacity = byte_size(n);
        _buf = static_cast<char*>(malloc(_capacity)); // TODO: optimize memory alloc
        memset(_buf, 0, _capacity);
    }

    void realloc(size_t bytes) {
        _capacity = _capacity == 0 ? 2 : _capacity * 2;
        //_capacity = _capacity * 2;
        _buf = static_cast<char*>(realloc(_buf, _capacity));
    }

    void dealloc() {
        if (_buf != nullptr) {
            free(_buf);
        }
    }
private:
    char* _buf = nullptr;
    size_t _len = 0;
    size_t _capacity = 0;
};

template <typename T>
class ColumnVector : public IColumn {
public:
    //using Container = std::vector<T>;
    using Container = DataArray<T>;

public:
    ColumnVector() {};
    ColumnVector(const size_t n) : data(n) {};
    ColumnVector(const size_t n, const T& x) :data(n, x) {};
    ~ColumnVector();

    void append(const Slice& val) override {
        data.push_back(val);
    }

    Slice at(const size_t n)  override {
        return std::move(data.at(n));
    }

private:
    Container data;
};



} // namespce TickDB
