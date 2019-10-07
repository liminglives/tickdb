class TableMeta {
};

class Table {
public:
    Table() {}
    ~Table() {}

private:
    TableMeta _meta;

    std::unordered_map<std::string, IColumn> _column_map;
};
