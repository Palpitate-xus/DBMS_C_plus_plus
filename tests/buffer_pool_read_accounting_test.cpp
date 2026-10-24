// test_sources: src/storage/BufferPool.cpp src/storage/PageCrypto.cpp
// Count actual page-load attempts, not slow-path admission or sidecar IO.
#include "storage/BufferPool.h"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <new>
#include <semaphore.h>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>

namespace {
thread_local bool gateLock=false,gateRead=false,waiterRole=false,traceReads=false,failWrite=false;
thread_local bool traceAllocations=false;
thread_local size_t allocationCount=0,failOrdinal=0;
thread_local int lockCalls=0,readCalls=0,readFault=0;
thread_local pthread_mutex_t* targetMutex=nullptr;
sem_t enteredLock,releaseLock,enteredRead,releaseRead,enteredWait;
void require(bool good,const char* message){if(!good)throw std::runtime_error(message);}
void post(sem_t& sem){if(::sem_post(&sem))::_exit(87);}
void wait(sem_t& sem){int r;do{r=::sem_wait(&sem);}while(r && errno==EINTR);if(r)::_exit(88);}
bool timedWait(sem_t& sem){timespec limit{};require(!::clock_gettime(CLOCK_REALTIME,&limit),"clock");limit.tv_sec+=5;
    int r;do{r=::sem_timedwait(&sem,&limit);}while(r && errno==EINTR);return r==0;}
void arm(bool lock=false){gateLock=lock;lockCalls=readCalls=readFault=0;targetMutex=nullptr;traceReads=true;}
void disarm(){gateLock=gateRead=waiterRole=traceReads=failWrite=traceAllocations=false;readFault=0;failOrdinal=0;}
void prepare(const std::string& path){
    char bytes[384];std::memset(bytes,'H',128);std::memset(bytes+128,'L',128);std::memset(bytes+256,'M',128);
    const int fd=::open(path.c_str(),O_RDWR|O_CREAT|O_TRUNC,0600);require(fd>=0,"prepare open");
    require(::pwrite(fd,bytes,sizeof(bytes),0)==static_cast<ssize_t>(sizeof(bytes)),"prepare write");
    require(::pread(fd,bytes,1,0)==1 && !::close(fd),"prepare warm IO");
}
bool stats(dbms::BufferPool& pool,size_t hits,size_t reads,const std::string& label){
    const auto actual=pool.statistics();const bool good=actual.hits==hits && actual.misses==reads;
    std::cout<<"BUFFER_READ_COUNTER "<<label<<" hits="<<actual.hits<<" reads="<<actual.misses
             <<" expected="<<hits<<','<<reads<<'\n';return good;
}
int pins(dbms::BufferPool& pool,uint32_t page){for(const auto& row:pool.getFrameInfo())if(row.pageId==page)return row.pinCount;return 0;}
}

void* operator new(std::size_t size){
    if(traceAllocations && ++allocationCount==failOrdinal)throw std::bad_alloc();
    if(void* value=std::malloc(size?size:1))return value;
    throw std::bad_alloc();
}
void operator delete(void* value) noexcept{std::free(value);}
void operator delete(void* value,std::size_t) noexcept{std::free(value);}
void* operator new[](std::size_t size){return ::operator new(size);}
void operator delete[](void* value) noexcept{std::free(value);}
void operator delete[](void* value,std::size_t) noexcept{std::free(value);}

extern "C" int pthread_mutex_lock(pthread_mutex_t* mutex) noexcept{
    using Original=int(*)(pthread_mutex_t*);
    static const auto original=reinterpret_cast<Original>(dlsym(RTLD_NEXT,"pthread_mutex_lock"));if(!original)::_exit(86);
    if(gateLock){if(!targetMutex)targetMutex=mutex;
        if(targetMutex==mutex && ++lockCalls==2){gateLock=false;post(enteredLock);wait(releaseLock);}}
    return original(mutex);
}
extern "C" int pthread_cond_wait(pthread_cond_t* condition,pthread_mutex_t* mutex){
    using Original=int(*)(pthread_cond_t*,pthread_mutex_t*);
    static const auto original=reinterpret_cast<Original>(dlsym(RTLD_NEXT,"pthread_cond_wait"));if(!original)::_exit(86);
    if(waiterRole){waiterRole=false;post(enteredWait);}return original(condition,mutex);
}
extern "C" ssize_t pread(int fd,void* bytes,size_t size,off_t offset){
    using Original=ssize_t(*)(int,void*,size_t,off_t);
    static const auto original=reinterpret_cast<Original>(dlsym(RTLD_NEXT,"pread"));if(!original)::_exit(86);
    if(traceReads){++readCalls;if(gateRead){gateRead=false;post(enteredRead);wait(releaseRead);}
        if(readFault==1){readFault=0;errno=EIO;return -1;}
        if(readFault==2){readFault=0;throw std::runtime_error("accounting-read-exception");}}
    return original(fd,bytes,size,offset);
}
extern "C" ssize_t pwrite(int fd,const void* bytes,size_t size,off_t offset){
    using Original=ssize_t(*)(int,const void*,size_t,off_t);
    static const auto original=reinterpret_cast<Original>(dlsym(RTLD_NEXT,"pwrite"));if(!original)::_exit(86);
    if(failWrite){errno=EIO;return -1;}return original(fd,bytes,size,offset);
}

static bool racedCache(size_t frames,bool keepPin,bool dirty,bool fail,size_t index){
    const std::string path="accounting-race-"+std::to_string(index)+".data";prepare(path);
    dbms::BufferPool pool(path,frames,128);require(pool.open(),"race open");
    char* result=nullptr;int reads=-1,locks=-1;
    std::thread worker([&]{arm(true);readFault=fail?1:0;result=pool.fetchPage(1);reads=readCalls;locks=lockCalls;disarm();});
    const bool entered=timedWait(enteredLock);char* peer=nullptr;
    if(entered){peer=pool.fetchPage(1);require(peer && peer[0]=='L',"race peer");
        if(dirty){peer[0]='B';pool.markDirty(1);}if(!keepPin)pool.unpinPage(1);}
    post(releaseLock);worker.join();
    const bool counters=stats(pool,1,1,"peer-cache-"+std::to_string(index));
    const bool good=entered && locks==2 && result && result==peer && reads==0 &&
        result[0]==(dirty?'B':'L') && pins(pool,1)==(keepPin?2:1) && counters;
    if(result)pool.unpinPage(1);
    if(peer && keepPin)pool.unpinPage(1);
    return good;
}

static bool lifecycle(int mode){
    const std::string path="accounting-closed-"+std::to_string(mode)+".data";prepare(path);
    dbms::BufferPool pool(mode==1?"missing-parent/accounting.data":path,1,128);
    bool setup=true;
    if(mode==1)setup=!pool.open();
    if(mode>=2){require(pool.open(),"lifecycle open");char* page=pool.fetchPage(1);require(page && page[0]=='L',"lifecycle seed");pool.unpinPage(1);
        pool.close(mode!=3);if(mode==4)pool.close();}
    pool.resetStats();arm();char* page=pool.fetchPage(1);const int reads=readCalls;disarm();
    const bool good=setup && !page && reads==0 && stats(pool,0,0,"closed-"+std::to_string(mode));
    if(mode==5){require(pool.open(),"reopen");page=pool.fetchPage(1);const bool restored=page && page[0]=='L' && stats(pool,0,1,"reopened");
        if(page)pool.unpinPage(1);
        return good && restored;
    }
    return good;
}

static bool rejectedAdmission(int mode){
    const std::string path="accounting-no-frame-"+std::to_string(mode)+".data";prepare(path);
    dbms::BufferPool pool(path,1,128);require(pool.open(),"admission open");
    char* original=pool.fetchPage(1);require(original && original[0]=='L',"admission seed");
    if(mode!=0){original[0]='D';pool.markDirty(1);pool.unpinPage(1);}
    if(mode==1)pool.setWritebackBarrier([]{return false;});
    if(mode==3)pool.setWriteLastPage(1);
    pool.resetStats();arm();failWrite=mode==2;char* page=pool.fetchPage(2);const int reads=readCalls;disarm();
    const bool good=!page && reads==0 && original[0]==(mode?'D':'L') && stats(pool,0,0,"no-admission-"+std::to_string(mode));
    if(mode==0)pool.unpinPage(1);
    pool.setWritebackBarrier({});require(pool.flush(),"admission cleanup");return good;
}

static bool readAttempt(int mode){
    const std::string path="accounting-io-"+std::to_string(mode)+".data";prepare(path);
    dbms::BufferPool pool(path,1,128);require(pool.open(),"IO open");
    if(mode==2)pool.setPageValidator([](uint32_t,const char*){return false;});
    if(mode==3)pool.setPageValidator([](uint32_t,const char*)->bool{throw std::runtime_error("accounting-validator-exception");});
    arm();readFault=mode==1?1:mode==4?2:0;char* page=nullptr;bool caught=false;
    try{page=pool.fetchPage(1);}catch(const std::runtime_error& error){caught=std::string(error.what())==(mode==3?"accounting-validator-exception":"accounting-read-exception");}
    const int reads=readCalls;disarm();
    const bool value=mode==0?page && page[0]=='L':!page;
    const bool good=value && caught==(mode>=3) && reads==1 && stats(pool,0,1,"attempt-"+std::to_string(mode));
    if(page)pool.unpinPage(1);
    return good;
}

static bool waiter(bool slow,bool failed){
    const std::string path="accounting-waiter-"+std::to_string(slow)+std::to_string(failed)+".data";prepare(path);
    dbms::BufferPool pool(path,1,128);require(pool.open(),"waiter open");
    char* first=nullptr;char* second=nullptr;int firstReads=-1,secondReads=-1;
    const auto observer=[&]{arm(slow);waiterRole=true;second=pool.fetchPage(1);secondReads=readCalls;disarm();};
    std::thread secondThread;bool gap=true;
    if(slow){secondThread=std::thread(observer);gap=timedWait(enteredLock);}
    std::thread loader([&]{arm();gateRead=true;readFault=failed?1:0;first=pool.fetchPage(1);firstReads=readCalls;disarm();});
    const bool reading=timedWait(enteredRead);
    if(slow)post(releaseLock);else secondThread=std::thread(observer);
    const bool registered=timedWait(enteredWait);post(releaseRead);loader.join();secondThread.join();
    const bool counters=stats(pool,failed?0:1,failed?2:1,"waiter-"+std::to_string(slow)+std::to_string(failed));
    const bool good=gap && reading && registered && second && second[0]=='L' && firstReads==1 &&
        (failed?!first && secondReads==1 && pins(pool,1)==1:first==second && secondReads==0 && pins(pool,1)==2) && counters;
    if(first)pool.unpinPage(1);
    if(second)pool.unpinPage(1);
    return good;
}

static size_t allocationTrace(){
    prepare("accounting-trace.data");dbms::BufferPool pool("accounting-trace.data",1,128);require(pool.open(),"trace open");
    allocationCount=0;failOrdinal=0;traceAllocations=true;char* page=pool.fetchPage(1);traceAllocations=false;
    require(page && page[0]=='L' && allocationCount>0 && allocationCount<=32,"trace complete");
    pool.unpinPage(1);std::cout<<"BUFFER_READ_ALLOCATION_COUNT "<<allocationCount<<'\n';return allocationCount;
}
static bool allocationFailure(size_t ordinal){
    const std::string path="accounting-allocation-"+std::to_string(ordinal)+".data";prepare(path);
    dbms::BufferPool pool(path,1,128);require(pool.open(),"allocation open");arm();
    allocationCount=0;failOrdinal=ordinal;traceAllocations=true;bool caught=false;
    try{(void)pool.fetchPage(1);}catch(const std::bad_alloc&){caught=true;}
    const int reads=readCalls;const size_t observed=allocationCount;disarm();
    const bool good=caught && observed==ordinal && reads<=1 && pool.getFrameInfo().empty() &&
        stats(pool,0,static_cast<size_t>(reads),"allocation-"+std::to_string(ordinal));
    char* retry=pool.fetchPage(1);const bool recovered=retry && retry[0]=='L';if(retry)pool.unpinPage(1);return good && recovered;
}

int main(){
    try{
        for(auto* sem:{&enteredLock,&releaseLock,&enteredRead,&releaseRead,&enteredWait})require(!::sem_init(sem,0,0),"semaphore init");
        size_t controls=0,failures=0;const auto check=[&](bool good,const std::string& label){++controls;if(!good){++failures;std::cerr<<"BUFFER_READ_ACCOUNTING_FAIL "<<label<<'\n';}};
        for(size_t frames:{1U,2U,4U})for(bool keep:{false,true})for(bool dirty:{false,true})for(bool fail:{false,true})
            check(racedCache(frames,keep,dirty,fail,controls),"peer cached page");
        for(int mode=0;mode<6;++mode)check(lifecycle(mode),"closed/failed-open/reopened lifecycle");
        for(uint32_t page:{dbms::kOrphanedPage,UINT32_MAX}){
            dbms::BufferPool pool("accounting-sentinel-"+std::to_string(page)+".data",1,128);require(pool.open(),"sentinel open");
            arm();char* value=pool.fetchPage(page);const int reads=readCalls;disarm();
            check(!value && reads==0 && stats(pool,0,0,"sentinel"),"internal sentinel");}
        for(int mode=0;mode<4;++mode)check(rejectedAdmission(mode),"no IO after rejected admission");
        for(int mode=0;mode<5;++mode)check(readAttempt(mode),"actual IO attempt including errors");
        for(bool slow:{false,true})for(bool failed:{false,true})check(waiter(slow,failed),"registered waiter");
        const size_t allocations=allocationTrace();for(size_t i=1;i<=allocations;++i)check(allocationFailure(i),"observed allocation failure");
        {
            prepare("accounting-basic.data");dbms::BufferPool pool("accounting-basic.data",1,128);require(pool.open(),"basic open");
            char* value=pool.fetchPage(1);require(value,"basic cold");pool.unpinPage(1);check(stats(pool,0,1,"basic cold"),"basic read");
            value=pool.fetchPage(1);require(value,"basic warm");pool.unpinPage(1);check(stats(pool,1,1,"basic warm"),"basic hit");
            pool.resetStats();value=pool.fetchPage(1);require(value,"basic reset");pool.unpinPage(1);check(stats(pool,1,0,"basic reset"),"reset retains cache");
        }
        require(controls==48+allocations,"complete accounting inventory");
        for(auto* sem:{&enteredLock,&releaseLock,&enteredRead,&releaseRead,&enteredWait})::sem_destroy(sem);
        std::cout<<"BUFFER_READ_ACCOUNTING_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<" ALLOCATIONS "<<allocations<<'\n';return failures?1:0;
    }catch(const std::exception& error){disarm();std::cerr<<"BUFFER_READ_ACCOUNTING_DRIVER_EXCEPTION "<<error.what()<<'\n';return 120;}
}
