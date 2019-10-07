#pragma once 

#include <unordered_map>

#include "tickdb/slice.h"
#include "tickdb/defines.h"

#include "db/timeline.h"

namespace TickDB {

class IDataIndex {
public:
    IDataIndex(EnumIndexType type) : _type(type) {}
    virtual ~IDataIndex() {}

    virtual void insert(const Slice& key, const Slice& val, const Slice& row) = 0;
    virtual void del(const Slice& row) = 0;

    EnumIndexType type() {
        return _type;
    }

protected:
    EnumIndexType _type;
};

class TimeSeriesDataIndex : public IDataIndex {
public:
    TimeSeriesDataIndex() : IDataIndex(IndexType_TimeSeries) {}
    ~TimeSeriesDataIndex() {
        for (auto it : _timeline_map) {
            delete it.second;
        }
    }

    Timeline* get_timeline(const std::string& key, bool is_create = false) {
        auto it = _timeline_map.find(key);
        if (it == _timeline_map.end()) {
            if (!is_create) {

                // for debugging
                //std::string log;
                //for (auto it : _timeline_map) {
                //    log += it.first + ", ";
                //}
                //Log("not found timeline key:" + key + " exists keys:" + log);

                return NULL;
            }
            auto timeline = new Timeline;
            _timeline_map[key] = timeline;
            return timeline;
        } else {
            return it->second;
        }
    }

    void insert(const Slice& key, const Slice& index, const Slice& row) override {
        if (index.size() != sizeof(uint64_t)) {
            Throw("error index value for timeseries");
        }
        uint64_t int_ts = index.get<uint64_t>();
        std::string str_key = key.to_string();
        insert(str_key, int_ts, row);
    }

    void del(const Slice& row) override {
        Slice data_row(row);
        data_row.remove_prefix(sizeof(RowHeader));
        std::string key;
        uint64_t ts_start = 0;
        uint64_t ts_end = 0;
        uint32_t type = 0;

        if (!Common::unpack_time_series_del_params(const_cast<char*>(data_row.data()), data_row.size(), key, ts_start, ts_end, type)) {
            Throw("parse del params failed");
        }

        //assert(static_cast<EnumIndexType>(type) == IndexType_TimeSeries);

        del(key, ts_start, ts_end);
    }

    void insert(const std::string& key, uint64_t ts, const Slice& row) {
        //Log("time insert key:" + key + ", ts:" + std::to_string(ts));
        (get_timeline(key, true))->insert(ts, row);
    }

    std::vector<const Slice*> del(const std::string& key, uint64_t start, uint64_t end) {
        auto tl = get_timeline(key, false);
        if (tl) { 
            return std::move(tl->del(start, end));
        }
        std::vector<const Slice*> tmp;
        return tmp;
    }

    void del(const std::string& key, uint64_t ts) {
        auto tl = get_timeline(key, false);
        if (tl) { 
            tl->del(ts);
        }
    }

    const Slice* get(const std::string& key, uint64_t ts) {
        Timeline* tl = get_timeline(key);
        return tl == NULL ? NULL : tl->get(ts);
    }

    const Slice* at(const std::string& key, int index) {
        Timeline* tl = get_timeline(key);
        if (tl == NULL) Log("timeline is null, key:" + key + ", key_size:" + std::to_string(key.size()));
        return tl == NULL ? NULL : tl->at(index);
    }

    // [time_start, time_end]
    bool time_range(const std::string& key, uint64_t time_start, uint64_t time_end, std::vector<const Slice*>& rows) {
        Timeline* tl = get_timeline(key);
        return tl == NULL ? false : tl->time_range(time_start, time_end, rows);
    }

    // [start, end]
    bool index_range(const std::string& key, int start, int end, std::vector<const Slice*>& rows) {
        Timeline* tl = get_timeline(key);
        return tl == NULL ? false : tl->index_range(start, end, rows);
    }

    const Slice* gt(const std::string& key, uint64_t ts) { // great than: >
        Timeline* tl = get_timeline(key);
        return tl == NULL ? NULL : tl->gt(ts);
    }
    const Slice* ge(const std::string& key, uint64_t ts) { // great or equal: >=
        Timeline* tl = get_timeline(key);
        return tl == NULL ? NULL : tl->ge(ts);
    }
    const Slice* lt(const std::string& key, uint64_t ts) { // less than: <
        Timeline* tl = get_timeline(key);
        return tl == NULL ? NULL : tl->lt(ts);
    }
    const Slice* le(const std::string& key, uint64_t ts) { // less or equal: <=
        Timeline* tl = get_timeline(key);
        return tl == NULL ? NULL : tl->le(ts);
    }
    int get_index_by_time(const std::string& key, uint64_t ts, QueryOP op) {
        Timeline* tl = get_timeline(key);
        return tl == NULL ? -1 : tl->get_index_by_time(ts, op);
    }
    bool shift_left(const std::string& key, uint64_t ts, int n, std::vector<const Slice*>& rows) {
        Timeline* tl = get_timeline(key);
        return tl == NULL ? false : tl->shift_left(ts, n, rows);
    }
    bool shift_right(const std::string& key, uint64_t ts, int n, std::vector<const Slice*>& rows) {
        Timeline* tl = get_timeline(key);
        return tl == NULL ? false : tl->shift_right(ts, n, rows);
    }

    size_t size(const std::string& key) {
        Timeline* tl = get_timeline(key);
        return tl == NULL ? 0 : tl->size();
    }
    
private:
    std::unordered_map<std::string, Timeline*> _timeline_map;
    //IData* _idata = NULL;
};

class KVDataIndex : public IDataIndex {
public:
    KVDataIndex() : IDataIndex(IndexType_KV) {}
    ~KVDataIndex() {}

    void insert(const Slice& key, const Slice& val, const Slice& row) override {
    }

    void del(const Slice& row) override {
    }

    void insert(const std::string& key, const Slice& row) {
        _data_map[key] = row;
    } 

    void del(const std::string& key) {
        _data_map.erase(key);
    }


    const Slice* get(const std::string& key) {
        auto it = _data_map.find(key);
        if (it == _data_map.end()) {
            return NULL;
        }
        return &(it->second);
    }

    std::vector<std::string> keys() {
        std::vector<std::string> ret;
        for (auto it : _data_map) {
            ret.push_back(it.first);
        }
        return std::move(ret);
    }

private:
    std::unordered_map<std::string, Slice> _data_map;
    //IData* _idata = NULL;
};


/*
class KListDataIndex : public IDataIndex {
public:
    KListDataIndex() : IDataIndex(IndexType_KList) {}
    ~KListDataIndex() {}

    void insert(const Slice& key, const Slice* row) {
        _data_map[key].push_back(row);
    } 


    void del(const std::string& key) {
        _data_map.erase(key);
    }

    std::vector<const Slice*> get(const std::string& key) {
        std::vector<const Slice*> ret;
        auto it = _data_map.find(key);
        if (it == _data_map.end()) {
            return std::move(ret);
        }
        for (auto item : it->second) {
            ret.push_back(item);
        }
        return std::move(ret);
    }

    const Slice* get(const std::string& key, int index) {
        auto it = _data_map.find(key);
        if (it == _data_map.end()) {
            return NULL;
        }
        int len = it->second.size();
        if (index >= len) {
            return NULL;
        } else if (index < 0) {
            index += len;
            if (index < 0) {
                return NULL;
            }
        }
            
        return (it->second)[index];
    }

    uint64_t len(const std::string& key) {
        auto it = _data_map.find(key);
        if (it == _data_map.end()) {
            return 0;
        }
        return it->second.size();
    }

    std::vector<std::string> keys() {
        std::vector<std::string> ret;
        for (auto it : _data_map) {
            ret.push_back(it.first);
        }
        return std::move(ret);
    }

private:
    std::unordered_map<std::string, std::vector<const Slice*> > _data_map;
    //IData* _idata = NULL;
};
*/

} // namespace TickDB


