#pragma once 

#include <unordered_map>

#include "timeline.h"
#include "defines.h"
#include "row.h"
#include "common.h"

namespace TickDB {

class IDataIndex {
public:
    IDataIndex(EnumDataIndexType type) : _type(type) {}
    virtual ~IDataIndex() {}

    virtual bool insert(Row* row) = 0;
    virtual bool del(Row* row) = 0;

    virtual void insert(const Slice& key, const Slice& index, const Slice* row);
    virtual void del(const Slice& key, const Slice& start, const Slice& end);

    EnumDataIndexType type() {
        return _type;
    }

protected:
    EnumDataIndexType _type;
};

class TimeSeriesDataIndex : public IDataIndex {
public:
    TimeSeriesDataIndex() : IDataIndex(DataIndexType_TimeSeries) {}
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

    bool del(Row* row) override {
        char* data = const_cast<char*>(row->data());
        uint32_t data_len = row->len();

        std::string key;
        uint64_t ts_start = 0;
        uint64_t ts_end = 0;
        uint32_t type = 0;

        if (!Common::unpack_time_series_del_params(data, data_len, key, ts_start, ts_end, type)) {
            Log("parse del params failed");
            return false;
        }

        //Log("time del, key:" + key + " start:" + std::to_string(ts_start) + " end:" + std::to_string(ts_end));

        std::vector<Row*> deleted_rows = del(key, ts_start, ts_end);
        for (auto& del_row : deleted_rows) {
            del_row->free_data();
        }
        return true;
    }

    void insert(const std::string& key, uint64_t ts, Row* row) {
        //Log("time insert key:" + key + ", ts:" + std::to_string(ts));
        (get_timeline(key, true))->insert(ts, row);
    }

    std::vector<Row*> del(const std::string& key, uint64_t start, uint64_t end) {
        auto tl = get_timeline(key, false);
        if (tl) { 
            return std::move(tl->del(start, end));
        }
        std::vector<Row*> tmp;
        return tmp;
    }

    void del(const std::string& key, uint64_t ts) {
        auto tl = get_timeline(key, false);
        if (tl) { 
            tl->del(ts);
        }
    }

    Row* get(const std::string& key, uint64_t ts) {
        Timeline* tl = get_timeline(key);
        return tl == NULL ? NULL : tl->get(ts);
    }

    Row* at(const std::string& key, int index) {
        Timeline* tl = get_timeline(key);
        // if (tl == NULL) Log("timeline is null, key:" + key + ", key_size:" + std::to_string(key.size()));
        return tl == NULL ? NULL : tl->at(index);
    }

    // [time_start, time_end]
    bool time_range(const std::string& key, uint64_t time_start, uint64_t time_end, std::vector<Row*>& rows) {
        Timeline* tl = get_timeline(key);
        return tl == NULL ? false : tl->time_range(time_start, time_end, rows);
    }

    // [start, end]
    bool index_range(const std::string& key, int start, int end, std::vector<Row*>& rows) {
        Timeline* tl = get_timeline(key);
        return tl == NULL ? false : tl->index_range(start, end, rows);
    }

    Row* gt(const std::string& key, uint64_t ts) { // great than: >
        Timeline* tl = get_timeline(key);
        return tl == NULL ? NULL : tl->gt(ts);
    }
    Row* ge(const std::string& key, uint64_t ts) { // great or equal: >=
        Timeline* tl = get_timeline(key);
        return tl == NULL ? NULL : tl->ge(ts);
    }
    Row* lt(const std::string& key, uint64_t ts) { // less than: <
        Timeline* tl = get_timeline(key);
        return tl == NULL ? NULL : tl->lt(ts);
    }
    Row* le(const std::string& key, uint64_t ts) { // less or equal: <=
        Timeline* tl = get_timeline(key);
        return tl == NULL ? NULL : tl->le(ts);
    }
    int get_index_by_time(const std::string& key, uint64_t ts, QueryOP op) {
        Timeline* tl = get_timeline(key);
        return tl == NULL ? -1 : tl->get_index_by_time(ts, op);
    }
    bool shift_left(const std::string& key, uint64_t ts, int n, std::vector<Row*>& rows) {
        Timeline* tl = get_timeline(key);
        return tl == NULL ? false : tl->shift_left(ts, n, rows);
    }
    bool shift_right(const std::string& key, uint64_t ts, int n, std::vector<Row*>& rows) {
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
    KVDataIndex() : IDataIndex(DataIndexType_KV) {}
    ~KVDataIndex() {}

    bool init(DataInterfaceManager* data_interface_manager) override {
        _data_interface_manager = data_interface_manager;
        return true;
    }

    bool insert(Row* row) override {
        IData* idata = _data_interface_manager->new_and_init_data_interface(row);
        if (!idata) {
            return false;
        }
        insert(idata->key(), row);
        delete idata;
        return true;
    }

    void insert(const std::string& key, Row* row) {
        _data_map[key] = row;
    } 

    bool del(Row* row) override {
        char* data = const_cast<char*>(row->data());
        uint32_t data_len = row->len();

        std::string key;

        if (!Common::unpack_kv_del_params(data, data_len, key)) {
            Log("parse kv del params failed");
            return false;
        }

        //Log("kv del, key:" + key);

        del(key);

        return true;
    }

    void del(const std::string& key) {
        Row* del_row = get(key);
        if (del_row) {
            del_row->free_data();
        }
        _data_map.erase(key);
    }


    Row* get(const std::string& key) {
        auto it = _data_map.find(key);
        if (it == _data_map.end()) {
            return NULL;
        }
        return it->second;
    }

    std::vector<std::string> keys() {
        std::vector<std::string> ret;
        for (auto it : _data_map) {
            ret.push_back(it.first);
        }
        return std::move(ret);
    }

private:
    std::unordered_map<std::string, Row*> _data_map;
    //IData* _idata = NULL;
};


class SnapshotTimeSeriesDataIndex : public IDataIndex {
public:
    SnapshotTimeSeriesDataIndex() : IDataIndex(DataIndexType_SnapshotTimeSeries) {}
    ~SnapshotTimeSeriesDataIndex() {
        if (_trade_index) {
            delete _trade_index;
        }
        if (_epoch_index) {
            delete _epoch_index;
        }
    }

    bool init(DataInterfaceManager* data_interface_manager) override {
        Log("create SnapshotTimeSeriesDataIndex");
        _data_interface_manager = data_interface_manager;
        _trade_index = new TimeSeriesDataIndex();
        _trade_index->init(_data_interface_manager);
        _epoch_index = new TimeSeriesDataIndex();
        _epoch_index->init(_data_interface_manager);
        return true;
    }

    bool insert(Row* row) override {
        IData* idata = _data_interface_manager->new_and_init_data_interface(row);
        if (!idata) {
            return false;
        }
        _trade_index->insert(idata->key(), idata->get_uint64("MicroSecondSinceTrade"), row); 
        _epoch_index->insert(idata->key(), idata->get_uint64("MicroSecondSinceEpoch"), row);
        return true;
    }

    void del(TimeSeriesDataIndex* t1, TimeSeriesDataIndex* t2, const std::string& col, 
            const std::string& key, uint64_t start, uint64_t end) {

        std::vector<Row*> rows = t1->del(key, start, end);
        for (auto r : rows) {
            IData* idata = _data_interface_manager->new_and_init_data_interface(r);
            idata->init(r);
            t2->del(key, idata->get_uint64(col));
        }

    }

    TimeSeriesDataIndex* get_timeseries(int type) {
        if (type == 10) {
            return _trade_index;
        } else if (type == 11) {
            return _epoch_index;
        } else {
            Throw("unknown snapshot timeseries type:" + std::to_string(type));
        }
        return NULL;
    }

    TimeSeriesDataIndex* get_trade_timeseries() {
        return _trade_index;
    }

    TimeSeriesDataIndex* get_epoch_timeseries() {
        return _epoch_index;
    }

    void del(const std::string& key, uint64_t ts_start, uint64_t ts_end, uint32_t type) {
        if (type == 10) {
            del(_trade_index, _epoch_index, "MicroSecondSinceEpoch", key, ts_start, ts_end);
        } else if (type == 11) {
            del(_epoch_index, _trade_index, "MicroSecondSinceTrade", key, ts_start, ts_end);
        } else {
            Throw("unknown snapshot timeseries type:" + std::to_string(type));
        }
    }

    bool del(Row* row) override {
        char* data = const_cast<char*>(row->data());
        uint32_t data_len = row->len();

        std::string key;
        uint64_t ts_start = 0;
        uint64_t ts_end = 0;
        uint32_t type = -1;

        if (!Common::unpack_time_series_del_params(data, data_len, key, ts_start, ts_end, type)) {
            Throw("parse del params failed");
            return false;
        }

        //Log("time del, key:" + key + " start:" + std::to_string(ts_start) + 
        //        " end:" + std::to_string(ts_end) +
        //        " type:" + std::to_string(type));

        del(key, ts_start, ts_end, type);

        return true;
    }
private:
    TimeSeriesDataIndex* _trade_index = NULL;
    TimeSeriesDataIndex* _epoch_index = NULL;

};

class KListDataIndex : public IDataIndex {
public:
    KListDataIndex() : IDataIndex(DataIndexType_KList) {}
    ~KListDataIndex() {}

    bool init(DataInterfaceManager* data_interface_manager) override {
        _data_interface_manager = data_interface_manager;
        return true;
    }

    bool insert(Row* row) override {
        IData* idata = _data_interface_manager->new_and_init_data_interface(row);
        if (!idata) {
            return false;
        }
        insert(idata->key(), row);
        delete idata;
        return true;
    }

    void insert(const std::string& key, Row* row) {
        _data_map[key].push_back(row);
    } 

    bool del(Row* row) override {
        char* data = const_cast<char*>(row->data());
        uint32_t data_len = row->len();

        std::string key;

        if (!Common::unpack_kv_del_params(data, data_len, key)) {
            Log("parse kv del params failed");
            return false;
        }

        //Log("kv del, key:" + key);

        del(key);

        return true;
    }

    void del(const std::string& key) {
        _data_map.erase(key);
    }



    std::vector<Row*> get(const std::string& key) {
        std::vector<Row*> ret;
        auto it = _data_map.find(key);
        if (it == _data_map.end()) {
            return std::move(ret);
        }
        for (auto item : it->second) {
            ret.push_back(item);
        }
        return std::move(ret);
    }

    Row* get(const std::string& key, int index) {
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
    std::unordered_map<std::string, std::vector<Row*> > _data_map;
    //IData* _idata = NULL;
};


} // namespace TickDB


