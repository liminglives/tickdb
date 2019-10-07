#pragma once

#include <fcntl.h>
#include <sys/mman.h>

#include "db/util.h"

namespace TickDB {

class MemoryBlock {
public:
    MemoryBlock() = default;
    virtual ~MemoryBlock() {
        if (_data) {
            free(_data);
        }
    }
    bool create(size_t size) {
        _data = (char*)malloc(size);
        _size = size;
        return true;
    }

    virtual std::string name() const {
        return "";
    }

    virtual char* data() const {
        return _data;
    }

    virtual size_t size() const {
        return _size;
    }
    
private:
    char* _data = nullptr;
    size_t _size = 0;
};

class ShareMemoryBlock : public MemoryBlock{
public:
    ShareMemoryBlock() = default;
    ~ShareMemoryBlock() {
        if (_shmid != -1) {
            close(_shmid);
        }
        if (_is_create) {
            shm_unlink(_name.c_str());
            munmap(_data, _size);
        }
    }

    bool create(const std::string& name, uint64_t size) {
        _name = name;
        _shmid = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0666);

        if (_shmid == -1) {
            Log("shm_open failed, " + name);
            return false;
        }
        if (ftruncate(_shmid, size) == -1) {
            Log("ftruncate failed, " + name);
            return false;
        }

        _data = (char*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, _shmid, 0);
        if (_data == MAP_FAILED) {
            Log("mmap failed, " + name);
            return false;
        }
        Log("create share memory " + name + " success, size:" + std::to_string(size));
        _is_create = true;
        return true;
    }

    bool watch(const std::string& name) {
        _name = name;
        _shmid = shm_open(name.c_str(), O_RDONLY, 0);
        if (_shmid == -1) {
            Log("shm_open failed, " + name);
            return false;
        }

        struct stat sb;
        if (fstat(_shmid, &sb) == -1) {
            Log("fstat failed, " + name);
            return false;
        }

        _data = (char*)mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, _shmid, 0);
        if (_data == MAP_FAILED) {
            Log("mmap failed, " + name);
            return false;
        }

        Log("watch share memory " + name + " sucess, mmap size:" + std::to_string(sb.st_size));

        _is_create = false;
        return true;
    }

    std::string name() const override {
        return _name;
    }

    char* data() const override {
        return _data;
    }

    size_t size() const override {
        return _size;
    }

private:
    bool _is_create = false;
    int _shmid = -1;
    size_t _size = 0;
    char* _data = nullptr;
    std::string _name;
};

class MemoryAllocator {
public:
    static char* alloc(uint64_t size) {
        return static_cast<char*>(malloc(size));
    }

    static MemoryBlock* create_memory_block(size_t size) {
        MemoryBlock* mb = new MemoryBlock;
        mb->create(size);
        return mb;
    }

    static ShareMemoryBlock* create_share_memory_block(const std::string& name, size_t size) {
        ShareMemoryBlock* sm = new ShareMemoryBlock();
        return sm->create(name, size) ? sm : nullptr;
    }

    static ShareMemoryBlock* watch_share_memory_block(const std::string& name) {
        ShareMemoryBlock* sm = new ShareMemoryBlock();
        return sm->watch(name) ? sm : nullptr;
    }

};

} // namespace TickDB
