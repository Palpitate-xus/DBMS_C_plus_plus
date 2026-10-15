#include "access/IndexFileUtil.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <mutex>
#include <set>
#include <signal.h>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

extern dbms::StorageEngine g_engine;
namespace fs=std::filesystem;

static std::string record(uint64_t next) {
    const uint32_t magic=0x31444954u,version=1;
    std::string bytes;
    bytes.append(reinterpret_cast<const char*>(&magic),4);
    bytes.append(reinterpret_cast<const char*>(&version),4);
    bytes.append(reinterpret_cast<const char*>(&next),8);
    uint64_t checksum=1469598103934665603ULL;
    for(unsigned char byte:bytes) {checksum^=byte;checksum*=1099511628211ULL;}
    bytes.append(reinterpret_cast<const char*>(&checksum),8);
    return bytes;
}

static std::string readBytes(const fs::path& path) {
    std::ifstream input(path,std::ios::binary);
    assert(input);
    return {std::istreambuf_iterator<char>(input),{}};
}

static uint64_t floorOf(const fs::path& path) {
    const auto bytes=readBytes(path);
    assert(bytes.size()==24);
    uint64_t floor=0;
    std::memcpy(&floor,bytes.data()+8,8);
    assert(bytes==record(floor));
    return floor;
}

static void overwriteInPlace(const fs::path& path,const std::string& bytes) {
    std::ofstream output(path,std::ios::binary|std::ios::trunc);
    assert(output);
    output.write(bytes.data(),static_cast<std::streamsize>(bytes.size()));
    output.close();
    assert(output);
}

// The child has a fresh executable image. Kill it after the actual allocator
// returns, before any StorageEngine/TLS destructor can run.
static uint64_t killedAllocator(const std::string& database,const std::string& table) {
    int channel[2];assert(::pipe(channel)==0);
    const std::string descriptor=std::to_string(channel[1]);
    const pid_t child=::fork();assert(child>=0);
    if(child==0) {
        (void)::close(channel[0]);
        ::execl("/proc/self/exe","toast_id_reservation_test","--allocate-hold",
            database.c_str(),table.c_str(),descriptor.c_str(),static_cast<char*>(nullptr));
        ::_exit(127);
    }
    (void)::close(channel[1]);
    uint64_t allocated=0;
    size_t received=0;
    while(received<sizeof(allocated)) {
        const auto count=::read(channel[0],reinterpret_cast<char*>(&allocated)+received,
            sizeof(allocated)-received);
        if(count<0 && errno==EINTR)continue;
        if(count<=0)break;
        received+=static_cast<size_t>(count);
    }
    (void)::close(channel[0]);
    const bool killed=::kill(child,SIGKILL)==0;
    int status=0;pid_t waited;
    do {waited=::waitpid(child,&status,0);}while(waited<0 && errno==EINTR);
    assert(received==sizeof(allocated) && allocated && killed && waited==child &&
        WIFSIGNALED(status) && WTERMSIG(status)==SIGKILL);
    return allocated;
}

int main(int argc,char** argv) {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    if(argc==5 && std::string(argv[1])=="--allocate-hold") {
        const auto allocated=g_engine.allocToastId(argv[2],argv[3]);
        const int descriptor=std::stoi(argv[4]);
        if(!allocated || ::write(descriptor,&allocated,sizeof(allocated))!=sizeof(allocated))
            ::_exit(3);
        (void)::close(descriptor);
        for(;;)::pause();
    }
    const auto database=testDbPath("toast_id_reservation");
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    for(const auto* name:{"identity","fault","parallel","legacy","overflow"}) {
        TableSchema table;table.tablename=name;
        table.append(makeTextColumn("payload",true));
        assert(g_engine.createTable(database,table)==DBStatus::OK);
    }
    const auto meta=[&](const std::string& name){return fs::path(database)/(name+".toastmeta");};
    const auto allocate=[&](const std::string& name) {
        const auto id=g_engine.allocToastId(database,name);
        if(id)assert(id<floorOf(meta(name)));
        return id;
    };
    size_t groups=0;

    assert(allocate("identity")==1);
    const auto identical=readBytes(meta("identity"));
    const auto identicalFloor=floorOf(meta("identity"));
    assert(index_file::writeAtomically(meta("identity"),identical));
    // Identical highwater bytes but a different inode must invalidate a range.
    assert(allocate("identity")>=identicalFloor);
    ++groups;

    const auto saved=readBytes(meta("identity"));
    const auto savedFloor=floorOf(meta("identity"));
    std::string corrupt=saved;corrupt.back()^=1;
    overwriteInPlace(meta("identity"),corrupt);
    assert(allocate("identity")==0);
    overwriteInPlace(meta("identity"),saved.substr(0,7));
    assert(allocate("identity")==0);
    overwriteInPlace(meta("identity"),saved);
    assert(allocate("identity")>=savedFloor);
    ++groups;

    // Exercise the unchanged publication's actual post-rename failure seam.
    index_file::failNextDirectorySyncForTesting();
    assert(allocate("fault")==0);
    assert(index_file::directorySyncFailureCountForTesting.load()==0);
    const auto unconfirmedFloor=floorOf(meta("fault"));
    assert(allocate("fault")>=unconfirmedFloor);
    ++groups;

    // Other live threads own distinct reservations, under the real file lock.
    std::set<uint64_t> concurrent;
    std::mutex resultMutex;
    std::atomic<unsigned> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> workers;
    for(size_t worker=0;worker<4;++worker)workers.emplace_back([&] {
        ready.fetch_add(1,std::memory_order_release);
        while(!start.load(std::memory_order_acquire))std::this_thread::yield();
        for(size_t i=0;i<16;++i) {
            const auto id=allocate("parallel");assert(id);
            std::lock_guard<std::mutex> lock(resultMutex);
            assert(concurrent.insert(id).second);
        }
    });
    while(ready.load(std::memory_order_acquire)!=4)std::this_thread::yield();
    start.store(true,std::memory_order_release);
    for(auto& worker:workers)worker.join();
    assert(concurrent.size()==64);
    ++groups;

    assert(allocate("identity"));
    const auto beforeChild=floorOf(meta("identity"));
    const auto killedId=killedAllocator(database,"identity");
    assert(killedId>=beforeChild);
    const auto afterChild=floorOf(meta("identity"));
    // A live parent's cached range cannot survive the child's publication.
    const auto afterKill=allocate("identity");
    assert(afterKill>=afterChild && afterKill!=killedId);
    const auto beforeRestart=floorOf(meta("identity"));
    const auto restartedId=killedAllocator(database,"identity");
    assert(restartedId>=beforeRestart && restartedId!=killedId && restartedId!=afterKill);
    ++groups;

    assert(g_engine.truncateTable(database,"identity")==DBStatus::OK);
    assert(floorOf(meta("identity"))==1 && allocate("identity")==1);
    ++groups;

    const uint64_t legacyFloor=345;
    std::string legacy(reinterpret_cast<const char*>(&legacyFloor),sizeof(legacyFloor));
    assert(index_file::writeAtomically(meta("legacy"),legacy));
    assert(allocate("legacy")==legacyFloor && floorOf(meta("legacy"))>legacyFloor);
    const auto replacedLegacyFloor=floorOf(meta("legacy"));
    legacy.assign(reinterpret_cast<const char*>(&replacedLegacyFloor),sizeof(replacedLegacyFloor));
    overwriteInPlace(meta("legacy"),legacy); // Same inode, different format.
    assert(allocate("legacy")==replacedLegacyFloor);
    ++groups;

    const auto maximum=std::numeric_limits<uint64_t>::max();
    assert(index_file::writeAtomically(meta("overflow"),record(maximum-3)));
    for(uint64_t offset=3;offset>0;--offset)assert(allocate("overflow")==maximum-offset);
    assert(floorOf(meta("overflow"))==maximum && allocate("overflow")==0);
    assert(index_file::writeAtomically(meta("overflow"),record(0)));
    assert(allocate("overflow")==0);
    ++groups;

    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    std::cout<<"TOAST_ID_RESERVATION_COMPLETE groups="<<groups
             <<" concurrent_ids="<<concurrent.size()<<" actual_SIGKILL_children=2\n";
}
