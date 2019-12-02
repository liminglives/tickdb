#pragma once

#ifndef _COMMON_TYPE
#define _COMMON_TYPE

#include <iostream>
#include <string>

namespace TickDB
{
namespace common
{

class Type
{
public:

    enum Code
    {
        Unknown = -1,
        Int,
        UInt,
        Float,
        Bool,
        String
    };
    
    virtual Code getCode() = 0;
    virtual int32_t getByte() = 0;
};

class TypeImpl: public Type
{
public:
    TypeImpl(int32_t byte, Code code): mByte(byte),mCode(code){}
    virtual ~TypeImpl(){}
    
    Code getCode()
    {
        return mCode;
    }
    
    int32_t getByte()
    {
        return mByte;
    }

private:
    int32_t mByte;
    Code mCode;
};

class Int: public TypeImpl
{
public:
    Int(int32_t byte): TypeImpl(byte, Code::Int){}
};

class UInt: public TypeImpl
{
public:
    UInt(int32_t byte): TypeImpl(byte, Code::UInt){}
};

class Float: public TypeImpl
{
public:
    Float(int32_t byte): TypeImpl(byte, Code::Float){}
};

class Bool: public TypeImpl
{
public:
    Bool(): TypeImpl(1, Code::Bool){}
};

class String: public TypeImpl
{
public:
    String(int32_t byte): TypeImpl(byte, Code::String){}
};


}
}

#endif
