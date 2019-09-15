#pragma once

#include <vector>

namespace TickDB {

enum ValueType {
    UINT8 = 1,
    UINT16 = 2,
    UINT32 = 3,
    UINT64 = 4,
    INT8 = 5,
    INT16 = 6,
    INT32 = 7,
    INT64 = 8,
    FLOAT32 = 9,
    FLOAT64 = 9,
    STRING = ,
};


class Value {
public:
    Value() : _data(nullptr), _size(0) {};
    Value(const char* data, const size_t size) : _data(data), _size(size) {};
    Value(const std::string& str) : _data(str.c_str()), _size(data.size()) {};

    const char* data() {
        return _data
    }

    size_t size() {
        return _size;
    }

    template <typename T> 
    T get() {
        return *(static_cast<T*>(static_cast<void*>(_data)));
    }

    template <typename T> 
    const T* get_ptr() {
        return static_cast<T*>(static_cast<void*>(_data));
    }


private:
    char* _data;
    size_t _size;
}

class IColumn {
public:
    IColumn() = default;
    virtual ~IColumn() {};

    IColumn(const IColumn&) = delete;
    IColumn& operator= (const IColumn&) = delete;

    virtual void insert(const Value& val) = 0; 

    virtual Value at(size_t n) = 0; 

};

template <typename T>
class ColumnVector : public IColumn {
public:
    using Container = std::vector<T>;

public:
    ColumnVector() {};
    ColumnVector(const size_t n) : data(n, T()) {};
    ColumnVector(const size_t n, const T& x) :data(n, x) {};
    ~ColumnVector();

    void insert(const Value& val) override {
        data.push_back(val);
    }

    Value at(cosnt size_t n) {
        return std::move(data.at(n));
    }

private:
    Container data;
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

    ~DataArray() {
        dealloc();
    }

    size_t len() {
        return _len;
    }

    size_t capacity() {
        return _capacity;
    }

    Value at(const size_t n) {
        return std::move(Value(_buf[n * sizeof(T)], sizeof(T)));
    }

    void push_back(const Value& val) {
        if (_len + val.size() > _capacity) { // unlikely
            realloc()
        }
        memcpy(_buf + _len, val.data(), val.size());
    }


private:
    void alloc(size_t n) {
        _capacity = byte_size(n);
        _buf = static_cast<char*>(malloc(_capacity)); // TODO: optimize memory alloc
    }

    void realloc(size_t bytes) {
        _capacity = _capacity * 2;
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



} // namespce TickDB
