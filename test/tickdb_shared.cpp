#include <iostream>

#include <signal.h>

#include "tickdb/tickdb.h"

const std::string TestTableHeader = "data_type=0\n"
                           "id=INT64,ts=UINT64,price=FLOAT,size=DOUBLE\n"
                           "index_type=1,key_column=id,index_column=ts\n";
static bool g_running = true;

void signal_handler(int signal_no) {
    std::cout << "recv signal " << signal_no << std::endl;
    g_running = false;
}

void create_shared_tickdb() {
    TickDB::TickDB db;
    TickDB::Option opt;
    if (!db.init(opt)) {
        std::cout << "init tickdb failed" << std::endl;
        return;
    }

    std::string table_name = "test";
    TickDB::TableOption table_opt;
    table_opt.table_name = table_name;
    table_opt.table_header = TestTableHeader;
    table_opt.table_type = TickDB::TableType_Shared;
    
    if (!db.create_table(table_opt)) {
        std::cout << "create table failed, " << table_name << std::endl;
        return;
    }

    TickDB::Table* table = db.get_table(table_name);

    auto flowbuffer_meta = table->get_flowbuffer_meta();
    flowbuffer_meta->print();


    signal(SIGINT, signal_handler);
    uint64_t ts = 100000;
    while (g_running) {

        FlowBuffer::FlowBufferBuilder builder(flowbuffer_meta);
        builder.set<int64_t>("id", 100);
        builder.set<uint64_t>("ts", ts);
        builder.set<float>("price", 10.20);
        builder.set<double>("size", 100000.20);
        ts += 1;
    
        std::cout << "insert " << 100 << " " << ts << std::endl;
        db.insert(table_name, TickDB::Slice(builder.data(), builder.size()));


        sleep(5);
    }
 
    std::cout << "stoped" << std::endl;

}

int main(int argc, char** argv) {
    
    create_shared_tickdb();
}
