// test_sources: src/storage/BufferPool.cpp src/storage/PageAllocator.cpp
// POSIX semaphore gates control the real public fetchPage call's lock gap.
// Interposition forwards libc calls unchanged unless this test thread arms it.
#include "storage/BufferPool.h"
#include "storage/PageAllocator.h"

#include <atomic>
#include <cerrno>
#include <dlfcn.h>
#include <future>
#include <iostream>
#include <semaphore.h>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace {
thread_local bool gateMutex = false, countReads = false, failRead = false, gateRead = false;
thread_local int mutexCalls = 0, readCalls = 0;
thread_local pthread_mutex_t* targetMutex = nullptr;
sem_t entered, released;

void releaseGate() { if (::sem_post(&released) != 0) ::_exit(87); }
void waitRelease() {
    int result;
    do { result=::sem_wait(&released); } while(result!=0 && errno==EINTR);
    if(result!=0) ::_exit(88);
}
bool waitEntered() {
    timespec deadline{};
    if(::clock_gettime(CLOCK_REALTIME,&deadline)!=0) return false;
    deadline.tv_sec+=5;
    int result;
    do { result=::sem_timedwait(&entered,&deadline); } while(result!=0 && errno==EINTR);
    return result==0;
}
void arm(bool fail, bool gate) {
    targetMutex=nullptr;mutexCalls=0;readCalls=0;
    gateMutex=gate;countReads=true;failRead=fail;gateRead=false;
}
void disarm() { gateMutex=countReads=failRead=gateRead=false; }
void require(bool good,const char* what) { if(!good) throw std::runtime_error(what); }
int pins(const dbms::BufferPool& pool,uint32_t page) {
    for(const auto& info:pool.getFrameInfo()) if(info.pageId==page) return info.pinCount;
    return 0;
}
}

extern "C" int pthread_mutex_lock(pthread_mutex_t* mutex) noexcept {
    using Original=int(*)(pthread_mutex_t*);
    static const auto original=reinterpret_cast<Original>(dlsym(RTLD_NEXT,"pthread_mutex_lock"));
    if(!original) ::_exit(86);
    if(gateMutex) {
        if(!targetMutex) targetMutex=mutex;
        if(targetMutex==mutex && ++mutexCalls==2) {
            gateMutex=false;
            if(::sem_post(&entered)!=0) ::_exit(87);
            waitRelease();
        }
    }
    return original(mutex);
}

extern "C" ssize_t pread(int fd,void* data,size_t size,off_t offset) {
    using Original=ssize_t(*)(int,void*,size_t,off_t);
    static const auto original=reinterpret_cast<Original>(dlsym(RTLD_NEXT,"pread"));
    if(!original) ::_exit(86);
    if(countReads) {
        ++readCalls;
        if(gateRead) {
            gateRead=false;
            if(::sem_post(&entered)!=0) ::_exit(87);
            waitRelease();
        }
        if(failRead) { errno=EIO;return -1; }
    }
    return original(fd,data,size,offset);
}

static bool racedCache(size_t frames,bool keepPin,bool dirty,bool fail,size_t index) {
    const std::string path="fetch-race-"+std::to_string(index)+".data";
    dbms::BufferPool pool(path,frames,128);
    require(pool.open(),"open race pool");
    char* seed=pool.fetchPage(1);require(seed,"seed race page");
    seed[0]='A';pool.markDirty(1);pool.unpinPage(1);
    require(pool.flush(),"persist race seed");pool.invalidatePage(1);
    int barriers=0;pool.setWritebackBarrier([&]{++barriers;return true;});
    char* result=nullptr;int reads=-1,locks=-1;
    std::thread worker([&]{arm(fail,true);result=pool.fetchPage(1);reads=readCalls;locks=mutexCalls;disarm();});
    const bool reached=waitEntered();
    char* cached=nullptr;
    if(reached) {
        cached=pool.fetchPage(1);require(cached,"peer caches race page");
        if(dirty) { cached[0]='B';pool.markDirty(1); }
        if(!keepPin) pool.unpinPage(1);
    }
    releaseGate();worker.join();
    const bool good=reached && locks==2 && result && result==cached &&
        result[0]==(dirty?'B':'A') && reads==0 && barriers==0 &&
        pins(pool,1)==(keepPin?2:1);
    std::cout<<"BUFFER_FETCH_RACE "<<frames<<' '<<keepPin<<' '<<dirty<<' '<<fail
             <<" gate="<<reached<<" result="<<(result!=nullptr)<<" reads="<<reads
             <<" barriers="<<barriers<<" pins="<<pins(pool,1)<<'\n';
    if(result) pool.unpinPage(1);
    if(cached && keepPin) pool.unpinPage(1);
    pool.setWritebackBarrier({});require(pool.flush(),"flush race pool");
    return good;
}

static bool allocatorRace() {
    dbms::PageAllocator allocator("fetch-allocator.data",32);
    require(allocator.open() && allocator.allocPage()==1 && allocator.flush(),"allocator seed");
    auto* pool=allocator.bufferPool();pool->invalidatePage(1);
    char* result=nullptr;int reads=-1;
    std::thread worker([&]{arm(true,true);result=allocator.fetchPage(1);reads=readCalls;disarm();});
    const bool reached=waitEntered();
    char* cached=reached?allocator.fetchPage(1):nullptr;
    releaseGate();worker.join();
    const bool good=reached && cached && result==cached && reads==0 && pins(*pool,1)==2;
    if(result) allocator.unpinPage(1);
    if(cached) allocator.unpinPage(1);
    return good;
}

static bool coldWaiter(bool failed) {
    dbms::BufferPool pool(failed?"cold-failed.data":"cold-success.data",3,128);
    require(pool.open(),"cold pool open");
    char* first=nullptr;int firstReads=-1;
    std::thread loader([&]{arm(failed,false);gateRead=true;
        first=pool.fetchPage(1);firstReads=readCalls;disarm();});
    const bool reached=waitEntered();
    auto waiter=std::async(std::launch::async,[&]{arm(false,false);
        char* page=pool.fetchPage(1);const int reads=readCalls;disarm();return std::make_pair(page,reads);});
    // A blocked read of page 1 must not hold the metadata lock or prevent
    // another page's ordinary load from completing.
    char* other=pool.fetchPage(2);
    releaseGate();loader.join();const auto second=waiter.get();
    const bool good=reached && other && second.first && firstReads==1 &&
        (failed ? first==nullptr && second.second==1 && pins(pool,1)==1
                : first==second.first && second.second==0 && pins(pool,1)==2);
    if(first) pool.unpinPage(1);
    if(second.first) pool.unpinPage(1);
    if(other) pool.unpinPage(2);
    return good;
}

static int closedChild(int mode) {
    dbms::BufferPool pool(mode==1?"missing-parent/fetch.data":"closed.data",1,128);
    if(mode==0) return pool.fetchPage(1)==nullptr?0:1;
    if(mode==1) return !pool.open() && pool.fetchPage(1)==nullptr?0:1;
    if(!pool.open()) return 2;
    char* page=pool.fetchPage(1);if(!page) return 3;
    page[0]='P';pool.markDirty(1);pool.unpinPage(1);
    if(!pool.flush()) return 4;
    pool.close(mode!=3);
    if(mode==4) pool.close();
    if(pool.fetchPage(1)!=nullptr) return 5;
    if(mode==5) {
        if(!pool.open()) return 6;
        page=pool.fetchPage(1);
        if(!page || page[0]!='P') return 7;
        pool.unpinPage(1);
    }
    return 0;
}

int main(int argc,char** argv) {
    if(argc==3 && std::string(argv[1])=="--closed") return closedChild(std::stoi(argv[2]));
    require(::sem_init(&entered,0,0)==0 && ::sem_init(&released,0,0)==0,"semaphore init");
    size_t controls=0,failures=0;
    const auto check=[&](bool good,const std::string& name) {
        ++controls;if(!good){++failures;std::cerr<<"BUFFER_FETCH_ADMISSION_FAIL "<<name<<'\n';}
    };
    for(size_t frames:{1U,2U,4U}) for(bool keep:{false,true})
        for(bool dirty:{false,true}) for(bool fail:{false,true})
            check(racedCache(frames,keep,dirty,fail,controls),"raced cache "+std::to_string(controls));
    check(allocatorRace(),"actual PageAllocator cache race");
    check(coldWaiter(false),"one cold loader/cache waiter");
    check(coldWaiter(true),"failed cold loader/waiter retries");
    for(uint32_t page:{dbms::kOrphanedPage,UINT32_MAX}) {
        dbms::BufferPool pool("reserved-"+std::to_string(page)+".data",1,128);
        require(pool.open(),"reserved pool open");
        arm(false,false);char* result=pool.fetchPage(page);const int reads=readCalls;disarm();
        check(!result && reads==0 && pool.getFrameInfo().empty(),"internal frame sentinel");
        if(result) pool.unpinPage(page);
    }
    for(int mode=0;mode<6;++mode) {
        const pid_t child=::fork();require(child>=0,"fork closed check");
        if(child==0){const auto value=std::to_string(mode);::execl(argv[0],argv[0],"--closed",value.c_str(),nullptr);::_exit(127);}
        int status=0;require(::waitpid(child,&status,0)==child,"wait closed check");
        check(WIFEXITED(status) && WEXITSTATUS(status)==0,"closed lifecycle "+std::to_string(mode));
    }
    ::sem_destroy(&entered);::sem_destroy(&released);
    if(controls!=35){++failures;std::cerr<<"BUFFER_FETCH_ADMISSION_BAD_INVENTORY "<<controls<<'\n';}
    std::cout<<"BUFFER_FETCH_ADMISSION_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
