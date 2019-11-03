#pragma once

static IColumn* create_column(int data_type) {
    IColumn* col = nullptr;
    switch (data_type) {
        case DT_INT8:
            col = new INT8Column();
            break;
        case DT_INT16:
            col = new INT16Column();
            break;
        case DT_INT32:
            col = new INT32Column();
            break;
        case DT_INT64:
            col = new INT64Column();
            break;
        case DT_UINT8:
            col = new UINT8Column();
            break;
        case DT_UINT16:
            col = new UINT16Column();
            break;
        case DT_UINT32:
            col = new UINT32Column();
            break;
        case DT_UINT64:
            col = new UINT64Column();
            break;
        case DT_FLOAT:
            col = new FLOATColumn();
            break;
        case DT_DOUBLE:
            col = new DOUBLEColumn();
            break;
        case DT_STRING:
            col = new STRINGColumn();
            break;
        default:
            Throw("unknown data type:" + std::to_string(data_type));
    }
    return col;
}


class Column {
public:
    Column(const std::string& name, int data_type) : _name(name), _data_type(data_type) {}
    ~Column() {
        if (_col) {
            delete _col;
        }
    }

    bool init() {
        _col = create_column(_data_type);
        if (_col == nullptr) {
            return false;
        }
        return true;
    }

    void push_back(const Slice& data) {
        _col->push_back(data);
    }

private:
    std::string _name;
    int _data_type;
    IColumn* _col = nullptr;
};

