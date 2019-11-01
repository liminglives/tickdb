#include "gtest/gtest.h"

#include "tickdb/tickdb.h"
#include "tickdb/flowbuffer.h"


class FlowDBTest : public testing::Test {
protected:
    virtual void SetUp() {}
};

const std::string TestTableHeader = "data_type=0\n"
                           "id=INT64,ts=UINT64,price=FLOAT,size=DOUBLE\n"
                           "index_type=1,key_column=id,index_column=ts\n";


TEST_F(FlowDBTest, TestInit) {
    TickDB::TickDB db; 
    TickDB::Option opt;
    ASSERT_TRUE(db.init(opt));
    std::string table_name = "test";

    TickDB::TableOption table_opt;
    table_opt.table_name = table_name;
    table_opt.table_header = TestTableHeader;
    
    ASSERT_TRUE(db.create_table(table_opt));

    TickDB::Table* table = db.get_table(table_name);

    ASSERT_TRUE(table);

    auto flowbuffer_meta = table->get_flowbuffer_meta();
    flowbuffer_meta->print();

    FlowBuffer::FlowBufferBuilder builder(flowbuffer_meta);
    builder.set<int64_t>("id", 100);
    builder.set<uint64_t>("ts", 100000);
    builder.set<float>("price", 10.20);
    builder.set<double>("size", 100000.20);

    ASSERT_TRUE(db.insert(table_name, TickDB::Slice(builder.data(), builder.size())));
    //std::cout << "size " << builder.size() << std::endl;

    builder.reset();
    builder.set<int64_t>("id", 100);
    builder.set<uint64_t>("ts", 100001);
    builder.set<float>("price", 10.25);
    builder.set<double>("size", 100000.25);

    ASSERT_TRUE(table->append(TickDB::Slice(builder.data(), builder.size())));

    int64_t int_key = 100;
    std::string key((char*)&int_key, sizeof(int_key));
    auto index = table->get_index();
    auto ts_index = static_cast<TickDB::TimeSeriesDataIndex*>(index);
    const TickDB::Slice* row = ts_index->at(key, -1);
    ASSERT_TRUE(row);
    //std::cout << "ret size " << row->size() << std::endl;

    {
        int64_t id;
        ASSERT_TRUE(FlowBuffer::get(flowbuffer_meta, const_cast<char*>(row->data()), "id", &id) == FlowBuffer::RET_OK);
        ASSERT_TRUE(id == 100);

        uint64_t ts;
        ASSERT_TRUE(FlowBuffer::get(flowbuffer_meta, const_cast<char*>(row->data()), "ts", &ts) == FlowBuffer::RET_OK);
        ASSERT_TRUE(ts == 100001);

        float price;
        ASSERT_TRUE(FlowBuffer::get(flowbuffer_meta, const_cast<char*>(row->data()), "price", &price) == FlowBuffer::RET_OK);
        //std::cout << "price " << price << std::endl;
        ASSERT_TRUE(price == 10.25);

        double size;
        ASSERT_TRUE(FlowBuffer::get(flowbuffer_meta, const_cast<char*>(row->data()), "size", &size) == FlowBuffer::RET_OK);
        ASSERT_TRUE(size == 100000.25);
    }

    ASSERT_TRUE(table->size(TickDB::Slice(key)) == 2);
    ASSERT_TRUE(table->del(TickDB::Slice(key), 100000, 100001));
    ASSERT_TRUE(table->size(TickDB::Slice(key)) == 0);

}

TEST_F(FlowDBTest, TestCreateShared) {
    try {
        TickDB::TickDB db; 
        TickDB::Option opt;
        ASSERT_TRUE(db.init(opt));
        std::string table_name = "test";

        TickDB::TableOption table_opt;
        table_opt.table_name = table_name;
        table_opt.table_header = TestTableHeader;
        table_opt.table_type = TickDB::TableType_Shared;
        
        ASSERT_TRUE(db.create_table(table_opt));

        TickDB::Table* table = db.get_table(table_name);

        ASSERT_TRUE(table);

        auto flowbuffer_meta = table->get_flowbuffer_meta();
        flowbuffer_meta->print();

        FlowBuffer::FlowBufferBuilder builder(flowbuffer_meta);
        builder.set<int64_t>("id", 100);
        builder.set<uint64_t>("ts", 100000);
        builder.set<float>("price", 10.20);
        builder.set<double>("size", 100000.20);

        ASSERT_TRUE(db.insert(table_name, TickDB::Slice(builder.data(), builder.size())));
        //std::cout << "size " << builder.size() << std::endl;

        builder.reset();
        builder.set<int64_t>("id", 100);
        builder.set<uint64_t>("ts", 100001);
        builder.set<float>("price", 10.25);
        builder.set<double>("size", 100000.25);

        ASSERT_TRUE(table->append(TickDB::Slice(builder.data(), builder.size())));

        int64_t int_key = 100;
        std::string key((char*)&int_key, sizeof(int_key));
        auto index = table->get_index();
        auto ts_index = static_cast<TickDB::TimeSeriesDataIndex*>(index);
        const TickDB::Slice* row = ts_index->at(key, -1);
        ASSERT_TRUE(row);
        //std::cout << "ret size " << row->size() << std::endl;

        {
            int64_t id;
            ASSERT_TRUE(FlowBuffer::get(flowbuffer_meta, const_cast<char*>(row->data()), "id", &id) == FlowBuffer::RET_OK);
            ASSERT_TRUE(id == 100);

            uint64_t ts;
            ASSERT_TRUE(FlowBuffer::get(flowbuffer_meta, const_cast<char*>(row->data()), "ts", &ts) == FlowBuffer::RET_OK);
            ASSERT_TRUE(ts == 100001);

            float price;
            ASSERT_TRUE(FlowBuffer::get(flowbuffer_meta, const_cast<char*>(row->data()), "price", &price) == FlowBuffer::RET_OK);
            //std::cout << "price " << price << std::endl;
            ASSERT_TRUE(price == 10.25);

            double size;
            ASSERT_TRUE(FlowBuffer::get(flowbuffer_meta, const_cast<char*>(row->data()), "size", &size) == FlowBuffer::RET_OK);
            ASSERT_TRUE(size == 100000.25);
        }

        ASSERT_TRUE(table->size(TickDB::Slice(key)) == 2);
        ASSERT_TRUE(table->del(TickDB::Slice(key), 100000, 100001));
        ASSERT_TRUE(table->size(TickDB::Slice(key)) == 0);
    } catch (TickDB::TickDBException& e) {
        Log("TestCreateShared failed, e:" + e.info());
        ASSERT_TRUE(false);
    }

}



int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
