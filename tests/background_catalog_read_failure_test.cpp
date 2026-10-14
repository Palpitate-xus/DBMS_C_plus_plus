#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "storage/PageAllocator.h"
#include "test_utils.h"
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fcntl.h>
#include <iostream>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <thread>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
namespace {
enum Phase { Idle, Armed, Injected, Released };
std::atomic<int> phase{Idle};
std::atomic<unsigned> failures{0};
std::atomic<bool> blocked{false}, synced{false};
std::thread::id foreground;
std::string namespaceImage, heapImage;
std::mutex eventMutex;
std::condition_variable event;

bool samePath(const char* path,const std::string& expected) {
    return std::filesystem::absolute(path).lexically_normal().string()==expected;
}
}

// One actual filesystem failure in the writeback thread only. Foreground
// catalog reads, other databases and real file contents are never modified.
extern "C" int lstat(const char* path,struct stat* status) noexcept {
    int current=phase.load(std::memory_order_acquire);
    if(current!=Idle && std::this_thread::get_id()!=foreground && samePath(path,namespaceImage)) {
        if(current==Armed && phase.compare_exchange_strong(current,Injected)) {
            static constexpr char message[]="[BACKGROUND CATALOG] injected actual lstat EIO\n";
            (void)::syscall(SYS_write,STDERR_FILENO,message,sizeof(message)-1);
            ++failures;errno=EIO;return -1;
        }
        if(phase.load()==Injected) {
            blocked.store(true);event.notify_all();
            std::unique_lock<std::mutex> lock(eventMutex);
            assert(event.wait_for(lock,std::chrono::seconds(5),[]{return phase.load()==Released;}));
        }
    }
    return static_cast<int>(::syscall(SYS_newfstatat,AT_FDCWD,path,status,AT_SYMLINK_NOFOLLOW));
}

extern "C" int fsync(int fd) {
    const int result=static_cast<int>(::syscall(SYS_fsync,fd));
    if(result==0 && phase.load()==Released && std::this_thread::get_id()!=foreground) {
        char path[4096];
        const auto size=::readlink(("/proc/self/fd/"+std::to_string(fd)).c_str(),path,sizeof(path));
        if(size>0 && std::string(path,static_cast<size_t>(size))==heapImage) {
            synced.store(true);event.notify_all();
        }
    }
    return result;
}

int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();foreground=std::this_thread::get_id();
    const std::string name="background_catalog_read_failure",db=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="testuser";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore { Session* value;~Restore(){setCurrentSession(value);} } restore{previous};
    DdlExecutor ddl;assert(!ddl.executeSql("CREATE TABLE source_rows(id INT)",session));
    assert(g_engine.insert(db,"source_rows",{{"id","17"}})==DBStatus::OK);
    auto* allocator=g_engine.getPageAllocator(db,"source_rows");assert(allocator);
    auto* page=allocator->fetchPage(1);assert(page);allocator->markDirty(1);
    namespaceImage=std::filesystem::absolute(g_engine.dbPath(db)/"pg_catalog/pg_namespace.cat").lexically_normal().string();
    heapImage=std::filesystem::absolute(g_engine.dbPath(db)/"source_rows.dt").lexically_normal().string();
    g_engine.catalogService().evict(db);
    g_engine.setBackgroundIntervals(10,300000);
    phase.store(Armed,std::memory_order_release);g_engine.wakeBackgroundWorker();
    {
        std::unique_lock<std::mutex> lock(eventMutex);
        assert(event.wait_for(lock,std::chrono::seconds(5),[]{return blocked.load();}));
    }
    assert(failures.load()==1 && !synced.load());
    bool retained=false;
    for(const auto& frame:allocator->bufferPool()->getFrameInfo())
        retained=retained || (frame.pageId==1 && frame.valid && frame.dirty && frame.pinCount==1);
    assert(retained);
    allocator->unpinPage(1);
    phase.store(Released);event.notify_all();
    {
        std::unique_lock<std::mutex> lock(eventMutex);
        assert(event.wait_for(lock,std::chrono::seconds(5),[]{return synced.load();}));
    }
    phase.store(Idle);
    assert(g_engine.query(db,"source_rows",{},{"id"})==std::vector<std::string>{"17 "});
    assert(!g_engine.inTransaction());
    // The isolated native runner owns these files. Let the engine close its
    // worker/caches before process cleanup, rather than racing DROP DATABASE
    // with the independently user-deferred cache-lock investigation.
    std::cout<<"[BACKGROUND CATALOG READ] actual EIO, live worker, retained dirty page and healthy writeback retry passed\n";
}
