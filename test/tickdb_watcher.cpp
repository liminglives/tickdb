#include <iostream>

#include <signal.h>

#include "tickdb/tickdb.h"

static bool g_running = true;

void signal_handler(int signal_no) {
    std::cout << "recv signal " << signal_no << std::endl;
    g_running = false;
}

void watch_tickdb() {
    TickDB::TickDB db;
    TickDB::Option opt;
    if (!db.init(opt)) {
        std::cout << "init tickdb failed" << std::endl;
        return;
    }

    std::string table_name = "test";

    TickDB::TableOption table_opt;
    table_opt.table_name = table_name;
    table_opt.table_type = TickDB::TableType_Watcher;
    table_opt.shm_name = "test_block_0";

    try {
        if (!db.create_table(table_opt)) {
            std::cout << "watch table failed, " << table_name << std::endl;
            return;
        }
    } catch (TickDB::TickDBException& e) {
        Log("create table failed, e:" + e.info());
        return; 
    }

    TickDB::Table* table = db.get_table(table_name);

    auto fb_meta = table->get_flowbuffer_meta();
    fb_meta->print();

    int64_t int_key = 100;
    std::string key((char*)&int_key, sizeof(int_key));
    auto index = table->get_index();
    auto ts_index = static_cast<TickDB::TimeSeriesDataIndex*>(index);


    signal(SIGINT, signal_handler);
    uint64_t last_ts = 0;
    while (g_running) {
        table->update();

        const TickDB::Slice* row = ts_index->at(key, -1);

        int64_t id;
        FlowBuffer::get(fb_meta, const_cast<char*>(row->data()), "id", &id);
        uint64_t ts;
        FlowBuffer::get(fb_meta, const_cast<char*>(row->data()), "ts", &ts); 
        if (ts != last_ts) {
            std::cout << "id:" << id << " ts:" << ts << std::endl;
            last_ts = ts;
        }

        sleep(3);
    }
 
    std::cout << "stoped" << std::endl;
}

int main(int argc, char** argv) {

    watch_tickdb(); 
        
}
