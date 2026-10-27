// test_sources: src/storage/PageAllocator.cpp src/storage/BufferPool.cpp src/storage/PageCrypto.cpp src/storage/PgPage.cpp src/storage/PageWrapper.cpp
// Observe every C++ allocation in open(), then fail each real site separately.
#include "storage/PageAllocator.h"
#include "common/DbError.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <csignal>
#include <cstdarg>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace {
// This executable has an explicit supported small-cache configuration.
// Set it before the common test-stub engine can cache the environment.
[[gnu::constructor(101)]] void configureFixtureFrames(){
    if(::setenv("DBMS_BUFFER_FRAMES","16",1)!=0)::_exit(88);
}
std::mutex outputMutex;
using namespace dbms;
namespace fs=std::filesystem;
constexpr size_t noFailure=std::numeric_limits<size_t>::max();
constexpr const char* target="storage_open_failure_heap_abcdefghijklmnopqrstuvwxyz_0123456789.dt";
struct Point {size_t bytes=0;int phase=0;uint32_t page=0;};
struct Fault {
    bool armed=false,mainLive=false,readSeen=false;
    size_t calls=0,selected=noFailure,thrown=0,writes=0;
    std::array<Point,1024> points{};
    Point failed{};
    uint32_t lastPage=0,ioPage=0;
    int ioKind=0,ioTriggered=0;
    dev_t mainDevice=0,sideDevice=0;
    ino_t mainInode=0,sideInode=0;
} ;
thread_local Fault fault;
struct Marker {int code;};
void require(bool good,const char* message){if(!good)throw std::runtime_error(message);}
bool identity(int fd,dev_t device,ino_t inode){
    struct stat status{};
    return inode!=0 && ::fstat(fd,&status)==0 && status.st_dev==device && status.st_ino==inode;
}
void beforeAllocation(size_t bytes){
    if(!fault.armed)return;
    if(fault.calls==fault.points.size())::_exit(86);
    const Point point{bytes,!fault.mainLive?0:!fault.readSeen?1:2,fault.lastPage};
    const size_t ordinal=fault.calls++;
    fault.points[ordinal]=point;
    if(ordinal==fault.selected){fault.selected=noFailure;fault.failed=point;++fault.thrown;throw std::bad_alloc();}
}
[[gnu::noinline]] void* allocate(size_t bytes,size_t alignment=0){
    beforeAllocation(bytes);
    void* result=nullptr;
    if(alignment){if(::posix_memalign(&result,alignment,std::max<size_t>(bytes,1))!=0)result=nullptr;}
    else result=std::malloc(std::max<size_t>(bytes,1));
    if(!result)throw std::bad_alloc();return result;
}
[[gnu::noinline]] void deallocate(void* pointer)noexcept{std::free(pointer);}
int trackedOpen(const char* path,int fd){
    if(fault.armed && fd>=0){
        const bool main=std::strcmp(path,target)==0;
        const bool side=std::strcmp(path,"storage_open_failure_heap_abcdefghijklmnopqrstuvwxyz_0123456789.dt.tde")==0;
        if(main||side){struct stat status{};if(::fstat(fd,&status)!=0)::_exit(87);
            if(main){fault.mainLive=true;fault.mainDevice=status.st_dev;fault.mainInode=status.st_ino;}
            else{fault.sideDevice=status.st_dev;fault.sideInode=status.st_ino;}}
    }
    return fd;
}
}

void* operator new(size_t bytes){return allocate(bytes);}
void* operator new[](size_t bytes){return allocate(bytes);}
void* operator new(size_t bytes,const std::nothrow_t&)noexcept{try{return allocate(bytes);}catch(...){return nullptr;}}
void* operator new[](size_t bytes,const std::nothrow_t&)noexcept{try{return allocate(bytes);}catch(...){return nullptr;}}
void* operator new(size_t bytes,std::align_val_t alignment){return allocate(bytes,static_cast<size_t>(alignment));}
void* operator new[](size_t bytes,std::align_val_t alignment){return allocate(bytes,static_cast<size_t>(alignment));}
void* operator new(size_t bytes,std::align_val_t alignment,const std::nothrow_t&)noexcept{try{return allocate(bytes,static_cast<size_t>(alignment));}catch(...){return nullptr;}}
void* operator new[](size_t bytes,std::align_val_t alignment,const std::nothrow_t&)noexcept{try{return allocate(bytes,static_cast<size_t>(alignment));}catch(...){return nullptr;}}
void operator delete(void* p)noexcept{deallocate(p);}
void operator delete[](void* p)noexcept{deallocate(p);}
void operator delete(void* p,size_t)noexcept{deallocate(p);}
void operator delete[](void* p,size_t)noexcept{deallocate(p);}
void operator delete(void* p,const std::nothrow_t&)noexcept{deallocate(p);}
void operator delete[](void* p,const std::nothrow_t&)noexcept{deallocate(p);}
void operator delete(void* p,std::align_val_t)noexcept{deallocate(p);}
void operator delete[](void* p,std::align_val_t)noexcept{deallocate(p);}
void operator delete(void* p,size_t,std::align_val_t)noexcept{deallocate(p);}
void operator delete[](void* p,size_t,std::align_val_t)noexcept{deallocate(p);}
void operator delete(void* p,std::align_val_t,const std::nothrow_t&)noexcept{deallocate(p);}
void operator delete[](void* p,std::align_val_t,const std::nothrow_t&)noexcept{deallocate(p);}

extern "C" int open(const char* path,int flags,...){
    using Original=int(*)(const char*,int,...);static const auto original=reinterpret_cast<Original>(::dlsym(RTLD_NEXT,"open"));
    if(!original)::_exit(87);mode_t mode=0;
    if((flags&O_CREAT)||(flags&O_TMPFILE)==O_TMPFILE){va_list args;va_start(args,flags);mode=va_arg(args,mode_t);va_end(args);}
    return trackedOpen(path,original(path,flags,mode));
}
extern "C" int open64(const char* path,int flags,...){
    using Original=int(*)(const char*,int,...);static const auto original=reinterpret_cast<Original>(::dlsym(RTLD_NEXT,"open64"));
    if(!original)::_exit(87);mode_t mode=0;
    if((flags&O_CREAT)||(flags&O_TMPFILE)==O_TMPFILE){va_list args;va_start(args,flags);mode=va_arg(args,mode_t);va_end(args);}
    return trackedOpen(path,original(path,flags,mode));
}
extern "C" int __open_2(const char* path,int flags){
    using Original=int(*)(const char*,int);static const auto original=reinterpret_cast<Original>(::dlsym(RTLD_NEXT,"__open_2"));
    if(!original)::_exit(87);return trackedOpen(path,original(path,flags));
}
extern "C" int __open64_2(const char* path,int flags){
    using Original=int(*)(const char*,int);static const auto original=reinterpret_cast<Original>(::dlsym(RTLD_NEXT,"__open64_2"));
    if(!original)::_exit(87);return trackedOpen(path,original(path,flags));
}
extern "C" int close(int fd){
    using Original=int(*)(int);static const auto original=reinterpret_cast<Original>(::dlsym(RTLD_NEXT,"close"));
    if(!original)::_exit(87);
    const bool main=fault.armed && identity(fd,fault.mainDevice,fault.mainInode);
    const int result=original(fd);if(main && result==0)fault.mainLive=false;return result;
}
extern "C" ssize_t pread(int fd,void* bytes,size_t size,off_t offset){
    using Original=ssize_t(*)(int,void*,size_t,off_t);static const auto original=reinterpret_cast<Original>(::dlsym(RTLD_NEXT,"pread"));
    if(!original)::_exit(87);
    const bool main=fault.armed && identity(fd,fault.mainDevice,fault.mainInode);
    if(main && fault.ioKind && offset==static_cast<off_t>(fault.ioPage)*PgPage::PAGE_SIZE){
        const int kind=fault.ioKind;fault.ioKind=0;++fault.ioTriggered;
        if(kind==1){errno=EIO;return -1;}
        if(kind==2)throw std::runtime_error("allocator-open-read-fault");
        if(kind==3)throw DbError("58030","allocator-open-read-fault");
        throw Marker{37};
    }
    const auto result=original(fd,bytes,size,offset);
    if(main && result>=0){fault.readSeen=true;fault.lastPage=static_cast<uint32_t>(offset/PgPage::PAGE_SIZE);}
    return result;
}
extern "C" ssize_t pwrite(int fd,const void* bytes,size_t size,off_t offset){
    using Original=ssize_t(*)(int,const void*,size_t,off_t);static const auto original=reinterpret_cast<Original>(::dlsym(RTLD_NEXT,"pwrite"));
    if(!original)::_exit(87);
    if(fault.armed && (identity(fd,fault.mainDevice,fault.mainInode)||identity(fd,fault.sideDevice,fault.sideInode)))++fault.writes;
    return original(fd,bytes,size,offset);
}

namespace {
struct Snapshot{bool exists=false;std::vector<char> bytes;};
Snapshot snapshot(const std::string& path){
    Snapshot result;result.exists=fs::exists(path);if(!result.exists)return result;
    std::ifstream input(path,std::ios::binary|std::ios::ate);require(static_cast<bool>(input),"snapshot open");
    const auto size=input.tellg();require(size>=0,"snapshot size");result.bytes.resize(static_cast<size_t>(size));
    input.seekg(0);if(!result.bytes.empty())input.read(result.bytes.data(),static_cast<std::streamsize>(result.bytes.size()));
    require(static_cast<bool>(input),"snapshot bytes");return result;
}
void restore(const std::string& path,const Snapshot& original){
    if(!original.exists){std::error_code error;fs::remove(path,error);require(!error,"restore absent private fixture");return;}
    std::ofstream output(path,std::ios::binary|std::ios::trunc);
    if(!original.bytes.empty())output.write(original.bytes.data(),static_cast<std::streamsize>(original.bytes.size()));
    output.close();require(static_cast<bool>(output),"restore private fixture");
}
bool unchanged(const std::string& path,const Snapshot& before){
    const auto after=snapshot(path);
    // O_CREAT may leave a zero-byte new file; no data publication is allowed.
    return before.exists?after.exists && after.bytes==before.bytes:!after.exists || after.bytes.empty();
}
size_t targetDescriptors(){
    size_t count=0;
    for(const auto& entry:fs::directory_iterator("/proc/self/fd")){
        const auto name=entry.path().filename().string();char* end=nullptr;
        const long fd=std::strtol(name.c_str(),&end,10);
        if(end && *end=='\0' && fd>=0 && (identity(static_cast<int>(fd),fault.mainDevice,fault.mainInode)||identity(static_cast<int>(fd),fault.sideDevice,fault.sideInode)))++count;
    }return count;
}
bool put(PageAllocator& allocator,uint32_t id,unsigned char marker){
    char* data=allocator.fetchPage(id);if(!data)return false;
    PgPage page(data);std::array<char,64> bytes{};bytes.fill(static_cast<char>(marker));OffsetNumber slot=0;
    const bool good=page.insert(bytes.data(),bytes.size(),slot) && slot==1;
    allocator.markDirty(id);allocator.unpinPage(id);return good;
}
bool value(PageAllocator& allocator,uint32_t id,unsigned char marker){
    char* data=allocator.fetchPage(id);if(!data)return false;
    PgPage page(data);const char* bytes=nullptr;size_t size=0;
    bool good=page.isValid()&&(!page.hasBoundPageId()||page.isValid(id))&&page.get(1,bytes,size)&&size==64&&page.liveCount()==1;
    if(good)good=std::all_of(bytes,bytes+size,[&](char v){return static_cast<unsigned char>(v)==marker;});
    allocator.unpinPage(id);return good;
}
struct Seed{unsigned flow=0,freeCount=0,pages=0;std::map<uint32_t,unsigned char> rows;Snapshot main,side;};
Seed seed(unsigned flow){
    Seed result;result.flow=flow;
    if(flow==1){std::ofstream out(target,std::ios::binary);out.close();require(static_cast<bool>(out),"empty seed");}
    if(flow>=2){
        result.freeCount=(flow==4?1U:(flow==5 || flow==7)?19U:0U);
        result.pages=flow==2?0:result.freeCount?result.freeCount+1:3;
        PageAllocator allocator(target,32);require(allocator.open(),"seed open");
        for(uint32_t id=1;id<=result.pages;++id){require(allocator.allocPage()==id,"seed allocate");require(put(allocator,id,static_cast<unsigned char>(id+31)),"seed tuple");result.rows[id]=static_cast<unsigned char>(id+31);}
        for(uint32_t id=1;id<=result.freeCount;++id){allocator.freePage(id);result.rows.erase(id);}
        require(allocator.flush(),"seed flush");allocator.close();
        if(flow>=6){
            auto bytes=snapshot(target).bytes;
            for(uint32_t id=1;id<=result.pages;++id){PgPage page(bytes.data()+id*PgPage::PAGE_SIZE);
                page.header()->pd_flags &= ~PgPage::PD_PAGE_ID_BOUND;
                page.header()->pd_pagesize_version=static_cast<uint16_t>((PgPage::PAGE_SIZE/512<<8)|PgPage::LEGACY_PAGE_LAYOUT_VERSION);
                page.writeChecksum();require(page.isValid(),"legacy seed");}
            restore(target,Snapshot{true,std::move(bytes)});
        }
    }
    result.main=snapshot(target);result.side=snapshot(std::string(target)+".tde");return result;
}
bool healthy(PageAllocator& allocator,const Seed& original){
    if(!allocator.open() || allocator.numPages()!=original.pages+1)return false;
    for(const auto& [id,marker]:original.rows)if(!value(allocator,id,marker))return false;
    std::map<uint32_t,unsigned char> rows=original.rows;std::set<uint32_t> allocated;
    const size_t extra=heapBufferFrameCount()+3;
    for(size_t i=0;i<original.freeCount+extra;++i){
        const uint32_t expected=i<original.freeCount?original.freeCount-static_cast<uint32_t>(i):original.pages+1+static_cast<uint32_t>(i-original.freeCount);
        const uint32_t id=allocator.allocPage();
        if(id!=expected || !allocated.insert(id).second)return false;
        const auto marker=static_cast<unsigned char>(97+id%100);
        if(!put(allocator,id,marker))return false;rows[id]=marker;
    }
    for(const auto& [id,marker]:rows)if(!value(allocator,id,marker))return false;
    if(!allocator.flush())return false;allocator.close();
    if(!allocator.open() || allocator.numPages()!=original.pages+1+extra)return false;
    for(const auto& [id,marker]:rows)if(!value(allocator,id,marker))return false;
    return true;
}
bool healthy(BufferPool& pool,const Seed& original,bool existingOnly){
    const bool opened=existingOnly?pool.openExisting():pool.open();
    if(existingOnly && !original.main.exists)return !opened && !pool.isOpen() && !fs::exists(target);
    if(!opened)return false;
    const size_t count=std::max<size_t>(original.pages+1,heapBufferFrameCount()+3)+1;
    for(uint32_t id=0;id<count;++id){
        char* data=pool.fetchPage(id);if(!data)return false;
        const size_t offset=static_cast<size_t>(id)*PgPage::PAGE_SIZE;
        const bool stored=offset<original.main.bytes.size();
        const bool good=stored?std::memcmp(data,original.main.bytes.data()+offset,PgPage::PAGE_SIZE)==0:
            std::all_of(data,data+PgPage::PAGE_SIZE,[](char byte){return byte==0;});
        pool.unpinPage(id);if(!good)return false;
    }
    pool.close();if(!(existingOnly?pool.openExisting():pool.open()))return false;
    return true;
}
void deadline(int){const char message[]="STORAGE_OPEN_CHILD_DEADLINE\n";(void)::write(2,message,sizeof(message)-1);::_exit(91);}
void observeFatal(int signal){
    const char segv[]="STORAGE_OPEN_OBSERVED_SIGNAL 11\n";
    const char bus[]="STORAGE_OPEN_OBSERVED_SIGNAL 7\n";
    if(signal==SIGBUS)(void)::write(1,bus,sizeof(bus)-1);
    else(void)::write(1,segv,sizeof(segv)-1);
    // Still a failed case, with its real signal recorded. Avoid starting the
    // system-wide crash reporter for every deliberately observed old failure.
    ::_exit(128+signal);
}
int child(unsigned api,unsigned flow,bool reopen,const std::string& mode,size_t ordinal,Point expected,int ioKind,uint32_t ioPage){
    require(heapBufferFrameCount()==16,"explicit supported 16-frame fixture configuration");
    const int warmFd=::open("/dev/null",O_RDONLY);require(warmFd>=0,"libc warm");char byte=0;(void)::pread(warmFd,&byte,1,0);require(::close(warmFd)==0,"libc warm close");
    Seed original=seed(flow);
    // Warm the filename registry without opening or changing the target.
    PageAllocator registryWarm(target,32);registryWarm.close();
    std::unique_ptr<PageAllocator> allocator;std::unique_ptr<BufferPool> pool;
    if(api==0)allocator=std::make_unique<PageAllocator>(target,32);
    else pool=std::make_unique<BufferPool>(target,16,PgPage::PAGE_SIZE);
    const auto openTarget=[&]{return allocator?allocator->open():api==2?pool->openExisting():pool->open();};
    const bool expectedOpen=api!=2 || original.main.exists;
    if(reopen){require(openTarget()==expectedOpen,"pre-open for reused instance");if(allocator)allocator->close();else pool->close();
        restore(target,original.main);restore(std::string(target)+".tde",original.side);}
    fault=Fault{};fault.selected=mode=="allocation"?ordinal:noFailure;fault.ioKind=ioKind;fault.ioPage=ioPage;
    bool opened=false,caught=false,unexpected=false;
    ::signal(SIGALRM,deadline);::alarm(5);fault.armed=true;
    try{opened=openTarget();}
    catch(const std::bad_alloc&){caught=mode=="allocation";unexpected=!caught;}
    catch(const DbError& error){caught=ioKind==3 && error.sqlState()=="58030" && error.message()=="allocator-open-read-fault";unexpected=!caught;}
    catch(const std::runtime_error& error){caught=ioKind==2 && std::string(error.what())=="allocator-open-read-fault";unexpected=!caught;}
    catch(const Marker& marker){caught=ioKind==4 && marker.code==37;unexpected=!caught;}
    catch(...){unexpected=true;}
    fault.armed=false;
    if(mode=="trace"){
        require(!unexpected && !caught && opened==expectedOpen,"healthy trace open");
        std::cout<<"OPEN_TRACE "<<fault.calls;
        for(size_t i=0;i<fault.calls;++i)std::cout<<' '<<fault.points[i].bytes<<':'<<fault.points[i].phase<<':'<<fault.points[i].page;
        std::cout<<std::endl;
    }else{
        if(mode=="allocation")require(fault.thrown==1 && fault.failed.bytes==expected.bytes && fault.failed.phase==expected.phase && fault.failed.page==expected.page,"selected real allocation site matched healthy trace");
        else require(fault.ioTriggered==1,"selected real read fault reached");
        const bool closed=allocator?!allocator->isOpen() && allocator->numPages()==0:!pool->isOpen();
        bool pins=true;
        for(const auto& info:(allocator?allocator->bufferPool():pool.get())->getFrameInfo())pins=pins && info.pinCount==0;
        const size_t descriptors=targetDescriptors();
        const bool bytes=unchanged(target,original.main)&&unchanged(std::string(target)+".tde",original.side);
        const bool recovered=mode=="allocation" && opened && !caught && !unexpected;
        // Filesystem error_code overloads may recover an allocation internally
        // or return false. Success must still be complete; failure must unwind.
        const bool failure=!opened && !unexpected && (mode=="allocation" || (ioKind==1?!caught:caught));
        const bool ready=recovered && (allocator?allocator->isOpen() && allocator->numPages()==original.pages+1:pool->isOpen());
        const bool beforeRetry=(failure && closed && descriptors==0 || ready && descriptors==2) && pins && bytes && fault.writes==0;
        std::cout<<"OPEN_AFTER_FAULT api="<<api<<" flow="<<flow<<" reopen="<<reopen<<" mode="<<mode<<" ordinal="<<ordinal
                 <<" caught="<<caught<<" recovered="<<recovered<<" closed="<<closed<<" pins="<<pins<<" descriptors="<<descriptors<<" bytes="<<bytes<<" writes="<<fault.writes<<std::endl;
        const bool retried=allocator?healthy(*allocator,original):healthy(*pool,original,api==2);
        std::cout<<"OPEN_SAME_INSTANCE_RETRY "<<retried<<std::endl;
        ::alarm(0);return beforeRetry && retried?0:1;
    }
    const bool retried=allocator?healthy(*allocator,original):healthy(*pool,original,api==2);
    require(retried,"healthy trace and full-frame/value reuse");::alarm(0);return 0;
}
struct Run{int code=0,signal=0,waitSignal=0;std::string output;};
Run launch(const std::string& executable,unsigned api,unsigned flow,bool reopen,const std::string& mode,size_t ordinal=0,Point expected={},int ioKind=0,uint32_t ioPage=0){
    char pattern[]="open_case_XXXXXX";const char* made=::mkdtemp(pattern);require(made,"private child directory");
    const std::string directory=fs::absolute(made).string();
    const std::array<std::string,10> args={executable,"--child",std::to_string(api),std::to_string(flow),std::to_string(reopen),mode,
        std::to_string(ordinal),std::to_string(expected.bytes)+":"+std::to_string(expected.phase)+":"+std::to_string(expected.page),std::to_string(ioKind),std::to_string(ioPage)};
    std::array<char*,11> pointers{};for(size_t i=0;i<args.size();++i)pointers[i]=const_cast<char*>(args[i].c_str());
    int descriptors[2];require(::pipe2(descriptors,O_CLOEXEC)==0,"child stdout pipe");
    {std::lock_guard<std::mutex> lock(outputMutex);
        std::cout<<"OPEN_CHILD api="<<api<<" flow="<<flow<<" reopen="<<reopen<<" mode="<<mode<<" ordinal="<<ordinal<<" dir="<<directory<<std::endl;}
    const pid_t pid=::fork();require(pid>=0,"fork open case");
    if(pid==0){::close(descriptors[0]);if(::dup2(descriptors[1],STDOUT_FILENO)<0)::_exit(127);::close(descriptors[1]);
        if(::chdir(directory.c_str())!=0)::_exit(127);::execv(executable.c_str(),pointers.data());::_exit(127);}
    ::close(descriptors[1]);Run result;std::array<char,8192> buffer{};
    for(;;){const auto count=::read(descriptors[0],buffer.data(),buffer.size());if(count<0 && errno==EINTR)continue;
        require(count>=0,"read child stdout");if(!count)break;result.output.append(buffer.data(),static_cast<size_t>(count));}
    ::close(descriptors[0]);int status=0;pid_t waited;do{waited=::waitpid(pid,&status,0);}while(waited<0 && errno==EINTR);require(waited==pid,"wait child");
    result.waitSignal=WIFSIGNALED(status)?WTERMSIG(status):0;
    result.signal=result.waitSignal;
    if(result.output.find("STORAGE_OPEN_OBSERVED_SIGNAL 11\n")!=std::string::npos)result.signal=SIGSEGV;
    if(result.output.find("STORAGE_OPEN_OBSERVED_SIGNAL 7\n")!=std::string::npos)result.signal=SIGBUS;
    result.code=WIFEXITED(status)?WEXITSTATUS(status):128+result.waitSignal;
    {std::lock_guard<std::mutex> lock(outputMutex);
        std::cout<<result.output<<"OPEN_CHILD_RESULT api="<<api<<" flow="<<flow<<" reopen="<<reopen<<" ordinal="<<ordinal<<" mode="<<mode<<" exit="<<result.code<<" observed_signal="<<result.signal<<" wait_signal="<<result.waitSignal<<std::endl;}
    return result;
}
Point parsePoint(const std::string& text){Point result;char first=0,second=0;std::istringstream input(text);input>>result.bytes>>first>>result.phase>>second>>result.page;
    require(static_cast<bool>(input)&&first==':'&&second==':',"trace point parse");return result;}
}

int main(int argc,char** argv){
    const rlimit core{0,0};(void)::setrlimit(RLIMIT_CORE,&core);
    try{
        if(argc==10 && std::strcmp(argv[1],"--child")==0){
            ::signal(SIGSEGV,observeFatal);::signal(SIGBUS,observeFatal);
            const int result=child(static_cast<unsigned>(std::stoul(argv[2])),static_cast<unsigned>(std::stoul(argv[3])),std::stoul(argv[4])!=0,
                argv[5],std::stoull(argv[6]),parsePoint(argv[7]),std::stoi(argv[8]),static_cast<uint32_t>(std::stoul(argv[9])));
            // child() has already destroyed ALL local tested allocators and
            // pools. Verify their descriptors before skipping only unrelated
            // test-stub global-engine teardown in this exec worker.
            struct stat status{};
            if(::stat(target,&status)==0){fault.mainDevice=status.st_dev;fault.mainInode=status.st_ino;}
            if(::stat((std::string(target)+".tde").c_str(),&status)==0){fault.sideDevice=status.st_dev;fault.sideInode=status.st_ino;}
            const size_t remaining=targetDescriptors();
            std::cout<<"OPEN_LOCAL_DESTRUCTION_DESCRIPTORS "<<remaining<<std::endl;
            ::_exit(result==0 && remaining!=0?1:result);
        }
        require(argc==1,"driver arguments");require(heapBufferFrameCount()==16,"driver 16 frames");
        // Resolve the interposed close before any multithreaded fork. Child
        // pre-exec code then uses only already-resolved POSIX operations.
        const int warm=::open("/dev/null",O_RDONLY);require(warm>=0,"driver libc warm");require(::close(warm)==0,"driver close warm");
        const std::string executable=fs::absolute(argv[0]).string();size_t controls=0,failures=0,setupErrors=0,signals=0,sites=0,traces=0,ioCases=0;
        const auto outcome=[&](const Run& result){++controls;if(result.code){++failures;if(result.code==120 || result.code==127 || result.code==86 || result.code==87)++setupErrors;}if(result.signal)++signals;};
        for(unsigned api=0;api<3;++api){
            const std::vector<unsigned> flows=api==0?std::vector<unsigned>{0,1,2,3,4,5,6,7}:std::vector<unsigned>{3,5};
            for(unsigned flow:flows)for(bool reopen:{false,true}){
                const auto trace=launch(executable,api,flow,reopen,"trace");outcome(trace);require(trace.code==0,"trace child passed");++traces;
                const size_t offset=trace.output.find("OPEN_TRACE ");require(offset!=std::string::npos,"trace footer");
                std::istringstream input(trace.output.substr(offset+11));size_t count=0;input>>count;require(count>0 && count<1024,"trace allocation bound");
                std::vector<Point> points;for(size_t i=0;i<count;++i){std::string value;input>>value;points.push_back(parsePoint(value));}
                sites+=count;
                for(size_t first=0;first<count;first+=4){
                    std::vector<std::future<Run>> active;
                    for(size_t ordinal=first;ordinal<std::min(first+4,count);++ordinal)
                        active.push_back(std::async(std::launch::async,[&,api,flow,reopen,ordinal]{return launch(executable,api,flow,reopen,"allocation",ordinal,points[ordinal]);}));
                    for(auto& result:active)outcome(result.get());
                }
                if(api==0){
                    std::set<uint32_t> pages{0};if(flow==4)pages.insert(1);if(flow==5 || flow==7){pages.insert(19);pages.insert(1);}
                    for(uint32_t page:pages)for(int kind=1;kind<=4;++kind){outcome(launch(executable,api,flow,reopen,"io",0,{},kind,page));++ioCases;}
                }
            }
        }
        require(traces==24 && ioCases==104 && controls==sites+traces+ioCases,"complete declared case inventory");
        std::cout<<"STORAGE_OPEN_FAILURE_COMPLETE "<<controls<<" FAILURES "<<failures<<" ALLOCATION_SITES "<<sites
                 <<" TRACES "<<traces<<" IO_CASES "<<ioCases<<" SIGNALS "<<signals<<" SETUP_ERRORS "<<setupErrors<<std::endl;
        return failures||setupErrors?1:0;
    }catch(const std::exception& error){fault.armed=false;std::cerr<<"STORAGE_OPEN_CONTROL_ERROR "<<error.what()<<std::endl;return 120;}
}
