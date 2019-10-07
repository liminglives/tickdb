#pragma once 

#include <iostream>
#include <string.h>

#include <string>
#include <unordered_map>
#include <vector>

#define FlowBufferThrow(x) do { throw ::FlowBuffer::FlowBufferException(__FILE__,__LINE__,__FUNCTION__,(x));} while (0)

namespace FlowBuffer {

using FlowBufferHeader = std::vector<std::pair<std::string, std::string> >;

class FlowBufferException {
public:
    FlowBufferException(const std::string& file, int line, const std::string& func, const std::string& info="") :
	       _file(file), _line(line), _func(func), _info(info) {} std::string info() {
		std::string ret;
		ret.append(_file);
		ret.append(":");
		ret.append(std::to_string(_line));
		ret.append(" ");
		ret.append(_func + "():");
		ret.append(_info);
		return ret;
	}
private:
    std::string _file;
    int _line;
    std::string _func;
    std::string _info;
};



enum {
    RET_OK = 0,
    RET_ERROR = 1,
    RET_NONE = 2,
};

struct String {
    uint32_t offset;
    uint32_t len;
};

using INT8 = int8_t;
using INT16 = int16_t;
using INT32 = int32_t;
using INT64 = int64_t;
using UINT8 = uint8_t;
using UINT16 = uint16_t;
using UINT32 = uint32_t;
using UINT64 = uint64_t;
using FLOAT = float;
using DOUBLE = double;
using LONGDOUBLE = long double;
using STRING = String;
using STDSTRING = std::string;

static uint32_t ExtendDefaultLength = 32;

enum DataType {
    DT_START = 0, // must be the first one

    DT_INT8 = 1,	 // size 1
    DT_INT16 = 2,	 // size 2
    DT_INT32 = 3,	// size 4
    DT_INT64 = 4,	// size 8 
    DT_UINT8 = 5,  // 
    DT_UINT16 = 6,  // 
    DT_UINT32 = 7,  // 
    DT_UINT64 = 8, // 
    DT_FLOAT = 9, //  size 4
    DT_DOUBLE = 10, // size 8
    DT_LD = 11, // long double, size 16. not use 
    DT_STRING = 12,

	DT_END // must be the last one
};

static std::unordered_map<std::string, DataType> DataTypeMap = {
    {"INT8", DT_INT8},
    {"INT16", DT_INT16},
    {"INT32", DT_INT32},
    {"INT64", DT_INT64},
    {"UINT8", DT_UINT8},
    {"UINT16", DT_UINT16},
    {"UINT32", DT_UINT32},
    {"UINT64", DT_UINT64},
    {"FLOAT", DT_FLOAT},
    {"DOUBLE", DT_DOUBLE},
    {"STRING", DT_STRING},
};

static std::unordered_map<std::string, uint32_t> DataTypeSizeMap = {
    {"INT8", sizeof(INT8)},
    {"INT16", sizeof(INT16)},
    {"INT32", sizeof(INT32)},
    {"INT64", sizeof(INT64)},
    {"UINT8", sizeof(UINT8)},
    {"UINT16", sizeof(UINT16)},
    {"UINT32", sizeof(UINT32)},
    {"UINT64", sizeof(UINT64)},
    {"FLOAT", sizeof(FLOAT)},
    {"DOUBLE", sizeof(DOUBLE)},
    {"STRING", sizeof(STRING)},
};

struct ColMeta {
    std::string name;
    std::string type;
    int enum_type;
    uint32_t size;
    uint32_t offset;
    uint32_t none_offset;
    int index;
};



class FlowBufferMeta {
public:
    FlowBufferMeta() {}
    ~FlowBufferMeta() {}

    bool init(const FlowBufferHeader& header) {
        uint32_t offset = sizeof(uint32_t); // for data len
        int index = 0;
        for (const auto& col : header) {
            ColMeta col_meta;
            col_meta.name = col.first;
            col_meta.type = col.second;
            if (DataTypeSizeMap.find(col.second) == DataTypeSizeMap.end()) {
                std::cout << ("error data type: " + col.second) << std::endl;
                return false;
            }
            col_meta.size = DataTypeSizeMap[col.second];

            col_meta.enum_type = DataTypeMap[col.second];

            col_meta.offset = offset;
            offset += col_meta.size;

            col_meta.index = index;
            ++index;
            _col_metas.push_back(col_meta);

            if (_col_name_index_map.find(col_meta.name) != _col_name_index_map.end()) {
                std::cout << "repeated col_name:" + col_meta.name << std::endl;
                return false;
            }
            _col_name_index_map[col_meta.name] = col_meta.index;
        }
        _col_num = _col_metas.size();

        _none_offset = offset;

        //_none_size = _col_num / 8;
        //_none_size = _col_num % 8 == 0 ? _none_size : _none_size;
        _none_size = _col_num;
        offset += _none_size;

        for (uint32_t i = 0; i < _col_num; ++i) {
            _col_metas[i].none_offset = _none_offset + i;
        }

        _size = offset;
        _extend_offset = offset;

        return true;
    }

    const std::vector<ColMeta>& get_col_metas() const {
        return _col_metas;
    }

    uint32_t get_col_num() const {
        return _col_num;
    }

    const std::unordered_map<std::string, int>& get_col_name_index_map() const {
        return _col_name_index_map;
    }

    uint32_t get_base_len() const {
        return _size;
    }

    const ColMeta& get_col_meta(const std::string& col_name) const {
        auto it = _col_name_index_map.find(col_name);
        if (it == _col_name_index_map.end()) {
            FlowBufferThrow("unknown col_name:" + col_name);
        }
        return _col_metas[it->second];
    }

    const ColMeta& get_col_meta(int index) const {
        return _col_metas[index];
    }

    int get_index(const std::string& col_name) const {
        auto it = _col_name_index_map.find(col_name);
        if (it == _col_name_index_map.end()) {
            FlowBufferThrow("unknown col_name:" + col_name);
        }
        return it->second;
    }

    uint32_t get_none_offset() const {
        return _none_offset;
    }

    uint32_t get_col_offset(const std::string& col_name) const {
        return get_col_meta(col_name).offset;
    }

    std::vector<std::string> header() const {
        std::vector<std::string> ret;
        for (const auto& col_meta : _col_metas) {
            ret.push_back(col_meta.name);
        }
        return std::move(ret);
    }

    void print() const {
        for (const auto& col_meta : _col_metas) {
            std::cout << col_meta.name << ":" << col_meta.type << ",";
        }
        std::cout << std::endl;
    }

private:
    std::vector<ColMeta> _col_metas;
    uint32_t _col_num;
    std::unordered_map<std::string, int> _col_name_index_map;
    uint32_t _none_offset;
    uint32_t _none_size;
    uint32_t _extend_offset;
    uint32_t _size;
};


class FlowBufferBuilder {
public:
    FlowBufferBuilder(const FlowBufferMeta* river_meta) : _river_meta(river_meta) {
        _extend_len = ExtendDefaultLength;
        _base_len = _river_meta->get_base_len();
        _data_len = _base_len;
        _buf_len = _data_len + _extend_len;
        _buf = (char*)malloc(_buf_len);
        memset(_buf, 0, _buf_len);
        _extend_start = _base_len;

        _extend_offset = 0;

        _none_offset = _river_meta->get_none_offset();

        update_data_len();
    } 

    ~FlowBufferBuilder() {
        free(_buf);
    }

    void reset() {
        memset(_buf, 0, _buf_len);
        _data_len = _base_len;
        _extend_offset = 0;
        update_data_len();
    }

    template <class T> void set(const std::string& col_name, const T& val) {
       const ColMeta& col_meta = _river_meta->get_col_meta(col_name);
       if (sizeof(T) != col_meta.size) {
           FlowBufferThrow("illegal val size:" + std::to_string(sizeof(T)) + " col size:" + std::to_string(col_meta.size));
       }
       uint32_t offset = col_meta.offset;
       memcpy(_buf + offset, static_cast<void*>(const_cast<T *>(&val)), sizeof(T));
       set_none(col_meta.none_offset);
    }

    void* data() {
        return (void*)_buf;
    }

    uint32_t size() {
        return _data_len;
    }

private:
    void update_data_len() {
        _data_len = _extend_offset + _extend_start;
        *((uint32_t*)((void*)_buf)) = _data_len;
    }

    void set_none(uint32_t offset) {
        *((uint8_t*)((void*)(_buf + offset))) = 1;
    }

    //void set_none2(int index) {
    //    int num = index / 8;
    //    int offset = index % 8;
    //    int8_t b = 0x01 << (8 - offset);
    //    *((int8_t*)((void*)(_buf + _none_offset + num))) &= b;
    //}

    //void set_none3(int index) {
    //    *((uint8_t*)((void*)(_buf + _none_offset + index))) = 1;
    //}


    void resize(uint32_t len) {
        uint32_t align_size = ((uint32_t)(len / 8)) * 8;
        _extend_len = align_size * 2;
        _buf_len = _extend_start + _extend_len;
        char* new_buf = (char*)malloc(_buf_len);
        memset(new_buf, 0, _buf_len);
        memcpy(new_buf, _buf, _extend_start + _extend_offset);
        free(_buf);
        _buf = new_buf;
    }

private:
    const FlowBufferMeta* _river_meta;
    char* _buf;
    uint32_t _buf_len;

    uint32_t _data_len;

    uint32_t _base_len;
    uint32_t _extend_len;

    uint32_t _extend_start;
    uint32_t _extend_offset;

    uint32_t _none_offset;

};

template <> inline void FlowBufferBuilder::set<std::string>(const std::string& col_name, const std::string& val) {
    const ColMeta& col_meta = _river_meta->get_col_meta(col_name);
    if (_extend_offset + val.size() > _extend_len) {
        resize(_extend_offset + val.size());
    }

    String str;
    str.offset = _extend_start + _extend_offset;
    str.len = val.size();
    //std::cout << "offset:" << str.offset << " len:" << str.len << std::endl;

    memcpy(_buf + col_meta.offset, (void*)(&(str)), sizeof(String));

    memcpy(_buf + str.offset, val.data(), str.len);
    set_none(col_meta.none_offset);

    _extend_offset += val.size();
    update_data_len();
}

template <class T> int get(const FlowBufferMeta* flow_meta, char* data, int index, T* val) {
    const ColMeta& col_meta = flow_meta->get_col_meta(index); 
    if (*((uint8_t*)((void*)(data + col_meta.none_offset))) != 1) {
        std::cout << "null " << index << std::endl;
        return RET_NONE;
    }  
    *val = *(static_cast<T *>(static_cast<void *>(data + col_meta.offset)));
    return RET_OK;

}

template <> inline int get<std::string>(const FlowBufferMeta* flow_meta, char* data, int index, std::string* val) {
    const ColMeta& col_meta = flow_meta->get_col_meta(index); 
    if (*((uint8_t*)((void*)(data + col_meta.none_offset))) != 1) {
        return RET_NONE;
    }  

    String* str = (static_cast<String *>(static_cast<void *>(data + col_meta.offset)));
    //std::cout << "get offset:" << str->offset << " len:" << str->len << std::endl;
    val->assign(data + str->offset, str->len);
    return RET_OK;
}


template <class T> int get(const FlowBufferMeta* flow_meta, char* data, const std::string& name, T* val) {
    return get<T>(flow_meta, data, flow_meta->get_index(name), val);
}

static int to_string(const FlowBufferMeta* flow_meta, char* data, const std::string& name, std::string* val) {
    const ColMeta& col_meta = flow_meta->get_col_meta(name); 
    if (*((uint8_t*)((void*)(data + col_meta.none_offset))) != 1) {
        return RET_NONE;
    }  
    if (col_meta.enum_type == DT_STRING) {
        String* str = (static_cast<String *>(static_cast<void *>(data + col_meta.offset)));
        val->assign(data + str->offset, str->len);
    } else {
        val->assign(data + col_meta.offset, col_meta.size);
    }
    return RET_OK;
}

static int to_cstring(const FlowBufferMeta* flow_meta, char* data, const std::string& name, char*& cstr, size_t& size) {
    const ColMeta& col_meta = flow_meta->get_col_meta(name); 
    if (*((uint8_t*)((void*)(data + col_meta.none_offset))) != 1) {
        return RET_NONE;
    }  
    if (col_meta.enum_type == DT_STRING) {
        String* str = (static_cast<String *>(static_cast<void *>(data + col_meta.offset)));
        cstr = data + str->offset;
        size = str->len;
    } else {
        cstr = data + col_meta.offset;
        size = col_meta.size;
    }
    return RET_OK;
}




} // namespace FlowBuffer
