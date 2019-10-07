#pragma once

#include "tickdb/flowbuffer.h"
#include "tickdb/slice.h"
namespace TickDB {

static int to_slice(const FlowBufferMeta* flow_meta, char* data, const std::string& name, TickDB::Slice** val) {
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


} // namespace TickDB
