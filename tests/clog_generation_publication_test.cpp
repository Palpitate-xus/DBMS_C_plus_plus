// test_sources: src/storage/CommitLog.cpp
#include "storage/CommitLog.h"
#include <cassert>
#include <dlfcn.h>
#include <filesystem>
#include <iostream>
#include <sys/file.h>
#include <unistd.h>

namespace { dbms::CommitLog* publishAtUnlock=nullptr; bool published=false; }
extern "C" int flock(int fd,int operation) {
    static const auto original=reinterpret_cast<int(*)(int,int)>(dlsym(RTLD_NEXT,"flock"));
    assert(original);
    const int result=original(fd,operation);
    if(result==0 && operation==LOCK_UN && publishAtUnlock) {
        auto* peer=publishAtUnlock;
        publishAtUnlock=nullptr;
        peer->setStatus(13,dbms::CommitLog::Status::Committed);
        assert(peer->flush());
        published=true;
    }
    return result;
}
int main() {
    using Log=dbms::CommitLog;
    char pattern[]="/tmp/dbms-clog-generation-XXXXXX";
    assert(mkdtemp(pattern));
    {
        Log writer(pattern),peer(pattern);
        assert(peer.getStatus(13)==Log::Status::InProgress);
        writer.setStatus(12,Log::Status::Committed);
        publishAtUnlock=&peer;
        assert(writer.flush());
        assert(published);
        // The peer publishes after the writer's actual file lock is released,
        // before flush() returns. The writer must not cache the peer's inode
        // with its own older bytes, even when the filesystem clock is coarse.
        assert(writer.getStatus(12)==Log::Status::Committed);
        assert(writer.getStatus(13)==Log::Status::Committed);
        Log reopened(pattern);
        assert(reopened.getStatus(12)==Log::Status::Committed);
        assert(reopened.getStatus(13)==Log::Status::Committed);
    }
    std::filesystem::remove_all(pattern);
    std::cout<<"[CLOG GENERATION PUBLICATION] passed\n";
}
