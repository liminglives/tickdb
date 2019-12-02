#include "column_engine_types.h"

namespace TickDB
{
namespace column_engine
{

template <class T>
class StructWrapper
{
public:
    static bool FromStruct(StructWrapper<T> w, const T& s);

    static bool ToStruct(T s, int& length, const char*);
    
    int32_t GetLength()
    {
        return mLength;
    }
    char* GetData()
    {
        return mData;
    }
    virtual ~StructWrapper(){}
private:
    StructWrapper(int length, char* data):mLength(length),mData(data){}
    int mLength;
    char* mData;
};
    
using StructWrapper<TableSchema> TableSchemaWrapper;
using StructWrapper<SessionInfo> SessionInfoWrapper;

}
}
