// test_sources: src/storage/BufferPool.cpp src/storage/PageCrypto.cpp
// Exhaust every observed cold-load allocation, plus callback/IO exceptions.
#include "storage/BufferPool.h"
#include "common/DbError.h"
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <new>
#include <semaphore.h>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace {
thread_local bool allocations=false,didRead=false,holdIO=false,waiterRole=false;
thread_local size_t allocationCount=0,failOrdinal=0;
thread_local size_t allocationSizes[32]{};
thread_local bool allocationAfterRead[32]{};
thread_local int fault=0;
sem_t ioEntered,releaseIO,waiterEntered;
struct Marker { int value; };
void require(bool good,const char* message){if(!good)throw std::runtime_error(message);}
void post(sem_t& sem){if(::sem_post(&sem))::_exit(88);}
void wait(sem_t& sem){int r;do{r=::sem_wait(&sem);}while(r && errno==EINTR);if(r)::_exit(88);}
bool timedWait(sem_t& sem){timespec limit{};::clock_gettime(CLOCK_REALTIME,&limit);limit.tv_sec+=2;
    int r;do{r=::sem_timedwait(&sem,&limit);}while(r && errno==EINTR);return r==0;}
void deadline(int){const char text[]="BUFFER_LOAD_CLEANUP_DEADLINE\n";(void)::write(2,text,sizeof(text)-1);::_exit(91);}
void prepare(const std::string& path){
    char bytes[384];std::memset(bytes,'H',128);std::memset(bytes+128,'L',128);std::memset(bytes+256,'M',128);
    const int fd=::open(path.c_str(),O_RDWR|O_CREAT|O_TRUNC,0600);require(fd>=0,"fixture open");
    require(::pwrite(fd,bytes,sizeof(bytes),0)==static_cast<ssize_t>(sizeof(bytes)),"fixture write");
    // Resolve the real libc pread before the allocation trace is armed.
    require(::pread(fd,bytes,1,0)==1 && ::close(fd)==0,"fixture warm libc");
}
bool expectedException(const std::string& kind,const dbms::DbError& error){
    return (kind=="validator-db" || kind=="io-db") && error.sqlState()=="58030" && error.message()=="load-injected-diagnostic";
}
}

void* operator new(std::size_t size){
    if(allocations){const size_t index=allocationCount++;
        if(index<32){allocationSizes[index]=size;allocationAfterRead[index]=didRead;}
        if(failOrdinal && allocationCount==failOrdinal)throw std::bad_alloc();}
    if(void* value=std::malloc(size?size:1))return value;
    throw std::bad_alloc();
}
void operator delete(void* value) noexcept{std::free(value);}
void operator delete(void* value,std::size_t) noexcept{std::free(value);}
void* operator new[](std::size_t size){return ::operator new(size);}
void operator delete[](void* value) noexcept{std::free(value);}
void operator delete[](void* value,std::size_t) noexcept{std::free(value);}

extern "C" ssize_t pread(int fd,void* bytes,size_t size,off_t offset){
    using Original=ssize_t(*)(int,void*,size_t,off_t);
    static const auto original=reinterpret_cast<Original>(dlsym(RTLD_NEXT,"pread"));
    if(!original)::_exit(86);
    if(holdIO){holdIO=false;post(ioEntered);wait(releaseIO);}
    if(fault==4){fault=0;throw std::runtime_error("load-io-exception");}
    if(fault==5){fault=0;throw dbms::DbError("58030","load-injected-diagnostic");}
    if(fault==6){fault=0;errno=EIO;return -1;}
    const auto result=original(fd,bytes,size,offset);if(allocations)didRead=true;return result;
}
extern "C" int pthread_cond_wait(pthread_cond_t* condition,pthread_mutex_t* mutex){
    using Original=int(*)(pthread_cond_t*,pthread_mutex_t*);
    static const auto original=reinterpret_cast<Original>(dlsym(RTLD_NEXT,"pthread_cond_wait"));
    if(!original)::_exit(86);
    if(waiterRole){waiterRole=false;post(waiterEntered);}
    return original(condition,mutex);
}

static void validator(dbms::BufferPool& pool){
    pool.setPageValidator([](uint32_t,const char*){
        const int selected=fault;
        if(selected>=1 && selected<=3){fault=0;
            if(selected==1)throw std::runtime_error("load-validator-exception");
            if(selected==2)throw dbms::DbError("58030","load-injected-diagnostic");
            throw Marker{73};}
        if(selected==7){fault=0;return false;}
        return true;
    });
}
static int modeFor(const std::string& kind){
    if(kind=="validator-runtime")return 1;if(kind=="validator-db")return 2;
    if(kind=="validator-marker")return 3;if(kind=="io-runtime")return 4;
    if(kind=="io-db")return 5;if(kind=="io-error")return 6;
    if(kind=="validator-false")return 7;return 0;
}
static int countAllocations(){
    prepare("count.data");dbms::BufferPool pool("count.data",1,128);validator(pool);require(pool.open(),"count pool");
    allocationCount=0;didRead=false;failOrdinal=0;allocations=true;
    char* page=pool.fetchPage(1);allocations=false;require(page && page[0]=='L',"count data");
    std::cout<<"BUFFER_LOAD_ALLOCATION_TRACE "<<allocationCount;
    for(size_t i=0;i<allocationCount && i<32;++i)std::cout<<' '<<allocationSizes[i]<<':'<<allocationAfterRead[i];
    std::cout<<'\n';pool.unpinPage(1);require(pool.flush(),"count flush");
    require(allocationCount>0 && allocationCount<=32,"allocation bound");return static_cast<int>(allocationCount);
}
static int failureCase(const std::string& kind,size_t ordinal){
    const std::string path=kind+"-"+std::to_string(ordinal)+".data";prepare(path);
    dbms::BufferPool pool(path,1,128);validator(pool);require(pool.open(),"failure pool");
    fault=modeFor(kind);allocationCount=0;failOrdinal=ordinal;didRead=false;allocations=kind=="allocation";
    bool caught=false;char* first=nullptr;
    try{first=pool.fetchPage(1);}
    catch(const std::bad_alloc&){caught=kind=="allocation";}
    catch(const dbms::DbError& error){caught=expectedException(kind,error);}
    catch(const std::runtime_error& error){caught=(kind=="io-runtime" && std::string(error.what())=="load-io-exception") ||
        (kind=="validator-runtime" && std::string(error.what())=="load-validator-exception");}
    catch(const Marker& marker){caught=kind=="validator-marker" && marker.value==73;}
    catch(...){caught=false;}
    allocations=false;fault=0;failOrdinal=0;
    const bool normal=kind=="ordinary",returnedFailure=kind=="io-error" || kind=="validator-false";
    const bool expected=normal?first && first[0]=='L':returnedFailure?!first && !caught:caught && !first;
    if(first)pool.unpinPage(1);
    const bool clean=normal || pool.getFrameInfo().empty();
    std::cout<<"BUFFER_LOAD_AFTER_FAILURE "<<kind<<' '<<ordinal<<" original_exception="<<caught<<" clean="<<clean<<std::endl;
    char* retry=pool.fetchPage(1);const bool retried=retry && retry[0]=='L';if(retry)pool.unpinPage(1);
    char* other=pool.fetchPage(2);const bool reused=other && other[0]=='M';if(other)pool.unpinPage(2);
    bool unpinned=true;for(const auto& info:pool.getFrameInfo())unpinned=unpinned && info.pinCount==0;
    require(pool.flush(),"failure pool flush");pool.close();
    require(pool.open(),"reopen failure pool");retry=pool.fetchPage(1);const bool reopened=retry && retry[0]=='L';if(retry)pool.unpinPage(1);
    const bool good=expected && clean && retried && reused && unpinned && reopened;
    std::cout<<"BUFFER_LOAD_RECOVERY "<<kind<<' '<<ordinal<<" good="<<good<<std::endl;return good?0:1;
}
static int waiterCase(const std::string& kind){
    prepare(kind+".data");dbms::BufferPool pool(kind+".data",1,128);validator(pool);require(pool.open(),"waiter pool");
    require(::sem_init(&ioEntered,0,0)==0 && ::sem_init(&releaseIO,0,0)==0 && ::sem_init(&waiterEntered,0,0)==0,"waiter semaphores");
    bool caught=false;char* second=nullptr;
    std::thread loader([&]{fault=kind=="waiter-io"?5:2;holdIO=true;
        try{(void)pool.fetchPage(1);}catch(const dbms::DbError& error){caught=error.sqlState()=="58030" && error.message()=="load-injected-diagnostic";}
        fault=0;std::cout<<"BUFFER_LOAD_LOADER_CAUGHT "<<caught<<std::endl;});
    const bool entered=timedWait(ioEntered);
    std::thread waiter([&]{waiterRole=true;second=pool.fetchPage(1);waiterRole=false;});
    const bool registered=timedWait(waiterEntered);
    std::cout<<"BUFFER_LOAD_WAITER_REGISTERED "<<entered<<' '<<registered<<std::endl;
    post(releaseIO);loader.join();waiter.join();
    const bool loaded=second && second[0]=='L';if(second)pool.unpinPage(1);
    bool clean=true;for(const auto& info:pool.getFrameInfo())clean=clean && info.pinCount==0;
    pool.close();::sem_destroy(&ioEntered);::sem_destroy(&releaseIO);::sem_destroy(&waiterEntered);
    return entered && registered && caught && loaded && clean?0:1;
}
static int child(const char* executable,const std::string& kind,size_t ordinal=0){
    std::cout<<"BUFFER_LOAD_CHILD "<<kind<<' '<<ordinal<<std::endl;
    const pid_t pid=::fork();require(pid>=0,"fork cleanup case");
    if(pid==0){const auto number=std::to_string(ordinal);::execl(executable,executable,kind.c_str(),number.c_str(),nullptr);::_exit(127);}
    int status=0;require(::waitpid(pid,&status,0)==pid,"wait cleanup case");
    return WIFEXITED(status)?WEXITSTATUS(status):128+(WIFSIGNALED(status)?WTERMSIG(status):0);
}
int main(int argc,char** argv){
    try{
        if(argc==3){const std::string kind=argv[1];::signal(SIGALRM,deadline);::alarm(kind.rfind("waiter-",0)==0?8:3);
            if(kind=="count")return countAllocations();
            if(kind.rfind("waiter-",0)==0)return waiterCase(kind);
            return failureCase(kind,static_cast<size_t>(std::stoul(argv[2])));}
        const int count=child(argv[0],"count");require(count>0 && count<=32,"cold allocation trace failed");
        size_t controls=0,failures=0;
        const auto check=[&](const std::string& kind,size_t number=0){++controls;const int status=child(argv[0],kind,number);
            if(status){++failures;std::cerr<<"BUFFER_LOAD_CLEANUP_FAIL "<<kind<<' '<<number<<" exit="<<status<<'\n';}};
        for(int i=1;i<=count;++i)check("allocation",static_cast<size_t>(i));
        for(const auto* kind:{"validator-runtime","validator-db","validator-marker","io-runtime","io-db", "waiter-io","waiter-validator","io-error","validator-false","ordinary"})check(kind);
        require(controls==static_cast<size_t>(count)+10,"complete load exception inventory");
        std::cout<<"BUFFER_LOAD_CLEANUP_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<" ALLOCATIONS "<<count<<'\n';return failures?1:0;
    }catch(const std::exception& error){allocations=false;std::cerr<<"BUFFER_LOAD_CLEANUP_DRIVER_EXCEPTION "<<error.what()<<'\n';return 120;}
}
