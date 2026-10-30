#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "test_utils.h"
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <cstdio>
#include <iostream>
#include <dlfcn.h>
#include <filesystem>
#include <mutex>
#include <thread>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
namespace {
std::mutex gate;
std::condition_variable changed;
std::string target;
bool armed=false, entered=false, released=false, fail=false;
size_t markerWrites=0;

void arm(const std::string& database,bool failure) {
    std::lock_guard<std::mutex> lock(gate);
    target=(std::filesystem::absolute(database).lexically_normal()/"checkpoint.tmp.").string();
    armed=true;entered=false;released=false;fail=failure;markerWrites=0;
}
void awaitMarker() {
    std::unique_lock<std::mutex> lock(gate);
    assert(changed.wait_for(lock,std::chrono::seconds(10),[]{return entered;}));
}
void releaseMarker() {
    {std::lock_guard<std::mutex> lock(gate);released=true;}
    changed.notify_all();
}
void disarm() {std::lock_guard<std::mutex> lock(gate);armed=false;}
}

extern "C" int fsync(int descriptor) {
    using Function=int(*)(int);
    static const auto call=reinterpret_cast<Function>(::dlsym(RTLD_NEXT,"fsync"));
    if(!call)::_exit(93);
    char link[64],path[4096];
    const int length=std::snprintf(link,sizeof(link),"/proc/self/fd/%d",descriptor);
    const auto size=length>0?::readlink(link,path,sizeof(path)-1):-1;
    if(size>=0) {
        path[size]='\0';
        std::unique_lock<std::mutex> lock(gate);
        if(!target.empty() && std::strstr(path,target.c_str())) {
            ++markerWrites;
            if(armed && !entered) {
                entered=true;changed.notify_all();
                changed.wait(lock,[]{return released;});
                if(fail){errno=EIO;return -1;}
            }
        }
    }
    return call(descriptor);
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    g_engine.setBackgroundIntervals(60000,600000);
    StorageEngine observer;
    observer.setBackgroundIntervals(60000,600000);
    for(const bool failure:{false,true}) {
        const auto database=testDbPath(failure?"checkpoint_coalescing_failure":"checkpoint_coalescing_success");
        assert(g_engine.createDatabase(database)==DBStatus::OK);
        TableSchema table;table.tablename="rows";table.append(makeIntColumn("id",false,4,true));
        assert(g_engine.createTable(database,table)==DBStatus::OK);
        assert(g_engine.insertRow(database,"rows",{{"id","1"}})==DBStatus::OK);
        arm(database,failure);
        bool first=false,second=false;
        std::atomic<bool> secondDone{false};
        std::thread leader([&]{first=g_engine.checkpoint(database);});
        awaitMarker();
        std::thread follower([&]{second=g_engine.checkpoint(database);secondDone=true;});
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        assert(!secondDone.load());
        releaseMarker();leader.join();follower.join();disarm();
        assert(first==!failure && second==!failure);
        {std::lock_guard<std::mutex> lock(gate);assert(markerWrites==1);}
        // A real fsync error is shared as failure; a later request retries.
        assert(g_engine.checkpoint(database));
        {std::lock_guard<std::mutex> lock(gate);assert(markerWrites==2);}
        assert(g_engine.query(database,"rows",{}, {"id"})==std::vector<std::string>{"1 "});

        assert(g_engine.beginTransaction(database)==DBStatus::OK);
        assert(g_engine.insertRow(database,"rows",{{"id","2"}})==DBStatus::OK);
        assert(!g_engine.checkpoint(database));
        bool peer=true;
        std::thread activePeer([&]{peer=observer.checkpoint(database);});activePeer.join();
        assert(!peer);
        assert(g_engine.rollbackTransaction()==DBStatus::OK);

        // Independent engines have independent dirty caches. They wait for
        // each other, then perform their own checkpoint rather than reusing
        // another engine's durability result.
        arm(database,false);first=false;peer=false;
        std::thread owner([&]{first=g_engine.checkpoint(database);});awaitMarker();
        std::thread independent([&]{peer=observer.checkpoint(database);});
        releaseMarker();owner.join();independent.join();disarm();
        assert(first && peer);
        {std::lock_guard<std::mutex> lock(gate);assert(markerWrites==2);target.clear();}
        assert(g_engine.dropDatabase(database)==DBStatus::OK);
    }
    std::cout<<"[CHECKPOINT COALESCING] same-owner success/failure, retry, active transaction and independent-owner checks passed\n";
}
