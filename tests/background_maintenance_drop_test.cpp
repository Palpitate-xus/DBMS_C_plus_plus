#include "commands/TableManage.h"
#include "storage/PageAllocator.h"
#include "catalog/type_registry.h"
#include "test_utils.h"
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <thread>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
namespace {
std::mutex mutex;
std::condition_variable changed;
std::string target;
bool armed=false,entered=false,released=false,fail=false;
}
extern "C" int fsync(int descriptor) {
    static const auto call=reinterpret_cast<int(*)(int)>(::dlsym(RTLD_NEXT,"fsync"));
    if(!call)::_exit(94);
    char link[64],path[4096];
    std::snprintf(link,sizeof(link),"/proc/self/fd/%d",descriptor);
    const auto size=::readlink(link,path,sizeof(path)-1);
    if(size>=0) {
        path[size]='\0';
        std::unique_lock<std::mutex> lock(mutex);
        if(armed && !entered && target==path) {
            entered=true;changed.notify_all();
            changed.wait(lock,[]{return released;});
            if(fail){errno=EIO;return -1;}
        }
    }
    return call(descriptor);
}

static void run(bool drop,bool writebackFailure) {
    using namespace dbms;
    const auto database=testDbPath(drop?"drop_background_writeback":writebackFailure?
        "checkpoint_background_failure":"checkpoint_background_writeback");
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    TableSchema table;table.tablename="rows";table.append(makeIntColumn("id",false,4,true));
    assert(g_engine.createTable(database,table)==DBStatus::OK);
    assert(g_engine.insertRow(database,"rows",{{"id","1"}})==DBStatus::OK);
    assert(g_engine.checkpoint(database));
    auto* allocator=g_engine.getPageAllocator(database,"rows");
    assert(allocator && allocator->fetchPage(1));
    {
        std::lock_guard<std::mutex> lock(mutex);
        target=(std::filesystem::absolute(database).lexically_normal()/"rows.dt").string();
        armed=true;entered=false;released=false;fail=writebackFailure;
    }
    // No tuple change: force the background writeback path to synchronize a
    // valid page, while a pin prevents it from racing ahead of the gate.
    allocator->markDirty(1);allocator->unpinPage(1);
    {
        std::unique_lock<std::mutex> lock(mutex);
        assert(changed.wait_for(lock,std::chrono::seconds(10),[]{return entered;}));
    }
    std::atomic<bool> done{false};
    bool checkpoint=false;
    DBStatus dropped=DBStatus::INTERNAL_ERROR;
    std::thread foreground([&] {
        if(drop)dropped=g_engine.dropDatabase(database);
        else checkpoint=g_engine.checkpoint(database);
        done=true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    assert(!done.load());
    {std::lock_guard<std::mutex> lock(mutex);released=true;}
    changed.notify_all();foreground.join();
    {std::lock_guard<std::mutex> lock(mutex);armed=false;target.clear();}
    if(drop) {
        assert(dropped==DBStatus::OK && !g_engine.databaseExists(database));
    } else {
        assert(checkpoint);
        assert(g_engine.query(database,"rows",{}, {"id"})==std::vector<std::string>{"1 "});
        assert(g_engine.beginTransaction(database)==DBStatus::OK);
        assert(g_engine.dropDatabase(database)==DBStatus::DATABASE_IN_USE);
        assert(!g_engine.checkpoint(database));
        assert(g_engine.rollbackTransaction()==DBStatus::OK);
        assert(g_engine.dropDatabase(database)==DBStatus::OK);
    }
}
int main() {
    dbms::TypeRegistry::instance().bootstrap();
    g_engine.setBackgroundIntervals(20,600000);
    run(true,false);run(false,false);run(false,true);
    std::cout<<"[BACKGROUND MAINTENANCE] DROP/CHECKPOINT wait for writeback, retry IO and reject active user transactions\n";
}
