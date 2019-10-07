#pragma once 

#include <string>

#include "tickdb/defines.h"
#include "tickdb/slice.h"

namespace TickDB {

class Common {
public:

    static std::string pack_time_series_del_params1(const std::string& key, 
            uint64_t ts_start, uint64_t ts_end, uint32_t type) {
        uint32_t key_len = key.size();
        uint32_t data_len = sizeof(uint32_t) + key_len + sizeof(uint64_t) * 2;
        //char* data = new char[data_len];
        //uint32_t cursor = 0;
        //memcpy(data + cursor, (void*)(&key_len), sizeof(key_len));
        //cursor += sizeof(key_len);
        //memcpy(data + cursor, key.data(), key.size());
        //cursor += key.size();
        std::string data;
        data.reserve(data_len);
        data.append(static_cast<char*>(static_cast<void*>(&key_len)), sizeof(key_len));
        data.append(key);
        data.append(static_cast<char*>(static_cast<void*>(&ts_start)), sizeof(ts_start));
        data.append(static_cast<char*>(static_cast<void*>(&ts_end)), sizeof(ts_end));
    
        return std::move(data);
    }
    
    static bool unpack_time_series_del_params1(char* data, uint32_t data_len, 
            std::string& key, uint64_t& ts_start, uint64_t& ts_end) {
        if (data_len < sizeof(uint32_t)) {
            return false;
        }
        uint32_t key_len = *(static_cast<uint32_t*>(static_cast<void *>(data)));
        uint32_t cursor = sizeof(uint32_t);
    
        if (data_len < cursor + key_len) {
            return false;
        }
        key.assign(data + cursor, key_len);
        cursor += key_len;
    
        if (data_len < cursor + 2 * sizeof(uint64_t)) {
            return false;
        }
        ts_start = *(static_cast<uint64_t*>(static_cast<void*>(data + cursor))); 
        cursor += sizeof(uint64_t);
        ts_end = *(static_cast<uint64_t*>(static_cast<void*>(data + cursor))); 
        cursor += sizeof(uint64_t);
    
        return true;
    }

    static std::string pack_time_series_del_params2(const std::string& key, 
            uint64_t ts_start, uint64_t ts_end, uint32_t type) {
        uint32_t key_len = key.size();
        uint32_t data_len = sizeof(uint32_t) + key_len + sizeof(uint64_t) * 2;
        //char* data = new char[data_len];
        //uint32_t cursor = 0;
        //memcpy(data + cursor, (void*)(&key_len), sizeof(key_len));
        //cursor += sizeof(key_len);
        //memcpy(data + cursor, key.data(), key.size());
        //cursor += key.size();
        std::string data;
        data.reserve(data_len);
        data.append(static_cast<char*>(static_cast<void*>(&key_len)), sizeof(key_len));
        data.append(key);
        data.append(static_cast<char*>(static_cast<void*>(&ts_start)), sizeof(ts_start));
        data.append(static_cast<char*>(static_cast<void*>(&ts_end)), sizeof(ts_end));
        data.append(static_cast<char*>(static_cast<void*>(&type)), sizeof(type));
    
        return std::move(data);
    }

    static std::string pack_time_series_del_params(const Slice& key, 
            uint64_t ts_start, uint64_t ts_end, uint32_t type) {
        uint32_t key_len = key.size();
        uint32_t data_len = sizeof(uint32_t) + key_len + sizeof(uint64_t) * 2;

        std::string data;
        data.reserve(data_len);
        data.append(static_cast<char*>(static_cast<void*>(&key_len)), sizeof(key_len));
        data.append(key.data(), key_len);
        data.append(static_cast<char*>(static_cast<void*>(&ts_start)), sizeof(ts_start));
        data.append(static_cast<char*>(static_cast<void*>(&ts_end)), sizeof(ts_end));
        data.append(static_cast<char*>(static_cast<void*>(&type)), sizeof(type));
    
        return std::move(data);
    }
 
 
    static bool unpack_time_series_del_params(char* data, uint32_t data_len, 
            std::string& key, uint64_t& ts_start, uint64_t& ts_end, uint32_t& type) {
        if (data_len < sizeof(uint32_t)) {
            return false;
        }
        uint32_t key_len = *(static_cast<uint32_t*>(static_cast<void *>(data)));
        uint32_t cursor = sizeof(uint32_t);
    
        if (data_len < cursor + key_len) {
            return false;
        }
        key.assign(data + cursor, key_len);
        cursor += key_len;
    
        if (data_len < cursor + 2 * sizeof(uint64_t) + sizeof(uint32_t)) {
            return false;
        }
        ts_start = *(static_cast<uint64_t*>(static_cast<void*>(data + cursor))); 
        cursor += sizeof(uint64_t);
        ts_end = *(static_cast<uint64_t*>(static_cast<void*>(data + cursor))); 
        cursor += sizeof(uint64_t);

        type = *(static_cast<uint32_t*>(static_cast<void*>(data + cursor))); 
        cursor += sizeof(uint32_t);
    
        return true;
    }

    static std::string pack_kv_del_params(const std::string& key) {
        uint32_t key_len = key.size();
        uint32_t data_len = sizeof(uint32_t) + key_len;
        std::string data;
        data.reserve(data_len);
        data.append(static_cast<char*>(static_cast<void*>(&key_len)), sizeof(key_len));
        data.append(key);
        return std::move(data);
    }

 
    static bool unpack_kv_del_params(char* data, uint32_t data_len, std::string& key) {
        if (data_len < sizeof(uint32_t)) {
            return false;
        }
        uint32_t key_len = *(static_cast<uint32_t*>(static_cast<void *>(data)));
        uint32_t cursor = sizeof(uint32_t);
    
        if (data_len < cursor + key_len) {
            return false;
        }
        key.assign(data + cursor, key_len);
        cursor += key_len;

        return true;
    }

    static const Slice* to_data(Slice* row) {
        row->remove_prefix(sizeof(RowHeader));
        return row;
    }

}; 

} // namespace TickDB 
