// test_sources: src/storage/WAL.cpp
// Preserve a valid WAL prefix across short transfers, errors and stale writers.
#include "storage/WAL.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
namespace fs=std::filesystem;
using dbms::Lsn;
using dbms::WALManager;
enum class Mode { Normal, Eintr, TwiceEintr, Short, AllShort, Mixed, Eio, NoSpace,
                  ShortEio, ShortNoSpace, Zero, SeekError, NextEio, NextZero,
                  NextOpenError, OpenEintr, OpenError, OpenEmfile,
                  LastEio, LastZero, LastOpenError };
enum class Cleanup { None, TruncateEintr, TruncateError, SyncEintr, SyncError,
                     UnlinkError, DirSyncEintr, DirSyncError };
bool armed=false,positions=true;
Mode selected=Mode::Normal;
Cleanup cleanup=Cleanup::None;
std::string activeDirectory;
Lsn beginLsn=0;
size_t recordLength=0,writes=0,accepted=0,shorts=0,interrupts=0,hard=0,zeroes=0,openFaults=0,seekFaults=0,cleanupFaults=0;
void require(bool good,const char* message){if(!good)throw std::runtime_error(message);}
bool segmentName(const char* name,uint32_t& segment){
    if(std::strlen(name)!=24)return false;
    segment=0;
    for(size_t i=0;i<24;++i){const char ch=name[i];const int digit=ch>='0'&&ch<='9'?ch-'0':ch>='A'&&ch<='F'?ch-'A'+10:-1;
        if(digit<0)return false;
        if(i>=16)segment=(segment<<4)|static_cast<uint32_t>(digit);}
    return true;
}
bool pathSegment(const char* path,uint32_t& segment){
    const size_t size=activeDirectory.size();
    return armed && std::strncmp(path,activeDirectory.c_str(),size)==0 && path[size]=='/' && segmentName(path+size+1,segment);
}
bool descriptorSegment(int fd,uint32_t& segment){
    char name[64],path[1024];std::snprintf(name,sizeof(name),"/proc/self/fd/%d",fd);
    const auto size=::readlink(name,path,sizeof(path)-1);if(size<0)return false;path[size]='\0';
    if(size>=10 && std::strcmp(path+size-10," (deleted)")==0)path[size-10]='\0';
    struct stat status{};
    return ::fstat(fd,&status)==0 && S_ISREG(status.st_mode) && pathSegment(path,segment);
}
bool descriptorDirectory(int fd){struct stat opened{},named{};return armed && ::fstat(fd,&opened)==0 &&
    S_ISDIR(opened.st_mode) && ::stat(activeDirectory.c_str(),&named)==0 && opened.st_dev==named.st_dev && opened.st_ino==named.st_ino;}
void arm(Mode mode,Cleanup fault=Cleanup::None){selected=mode;cleanup=fault;writes=accepted=shorts=interrupts=hard=zeroes=openFaults=seekFaults=cleanupFaults=0;positions=true;armed=true;}
bool successMode(Mode mode){return mode==Mode::Normal||mode==Mode::Eintr||mode==Mode::TwiceEintr||mode==Mode::Short||
    mode==Mode::AllShort||mode==Mode::Mixed||mode==Mode::OpenEintr;}
bool injected(Mode mode){switch(mode){
    case Mode::Normal:return writes>0;
    case Mode::Eintr:return interrupts==1;
    case Mode::TwiceEintr:return interrupts==2;
    case Mode::Short:return shorts==1;
    case Mode::AllShort:return shorts>1;
    case Mode::Mixed:return interrupts==1 && shorts>1;
    case Mode::Eio:case Mode::NoSpace:case Mode::NextEio:case Mode::LastEio:return hard==1;
    case Mode::ShortEio:case Mode::ShortNoSpace:return shorts==1 && hard==1;
    case Mode::Zero:case Mode::NextZero:case Mode::LastZero:return zeroes==1;
    case Mode::SeekError:return seekFaults==1;
    case Mode::NextOpenError:case Mode::LastOpenError:case Mode::OpenEintr:case Mode::OpenError:case Mode::OpenEmfile:return openFaults==1;
    }return false;
}
int dispatchOpen(const char* symbol,const char* path,int flags,mode_t permissions){
    using Original=int(*)(const char*,int,...);
    const auto original=reinterpret_cast<Original>(::dlsym(RTLD_NEXT,symbol));if(!original)::_exit(86);
    uint32_t segment=0;
    if(pathSegment(path,segment) && (flags&O_ACCMODE)!=O_RDONLY && !openFaults &&
       (selected==Mode::OpenEintr||selected==Mode::OpenError||selected==Mode::OpenEmfile||
        (selected==Mode::NextOpenError && segment>beginLsn/WALManager::kSegmentSize)||
        (selected==Mode::LastOpenError && segment==(beginLsn+recordLength-1)/WALManager::kSegmentSize))){
        ++openFaults;errno=selected==Mode::OpenEintr?EINTR:selected==Mode::OpenEmfile?EMFILE:EIO;return -1;}
    return original(path,flags,permissions);
}
}

extern "C" int open(const char* path,int flags,...){mode_t permissions=0;if((flags&O_CREAT)||(flags&O_TMPFILE)==O_TMPFILE){
    va_list args;va_start(args,flags);permissions=va_arg(args,mode_t);va_end(args);}return dispatchOpen("open",path,flags,permissions);}
extern "C" int open64(const char* path,int flags,...){mode_t permissions=0;if((flags&O_CREAT)||(flags&O_TMPFILE)==O_TMPFILE){
    va_list args;va_start(args,flags);permissions=va_arg(args,mode_t);va_end(args);}return dispatchOpen("open64",path,flags,permissions);}
extern "C" off_t lseek(int fd,off_t offset,int whence) noexcept {
    using Original=off_t(*)(int,off_t,int);static const auto original=reinterpret_cast<Original>(::dlsym(RTLD_NEXT,"lseek"));if(!original)::_exit(86);
    uint32_t segment=0;if(whence==SEEK_SET && descriptorSegment(fd,segment) && selected==Mode::SeekError && !seekFaults){++seekFaults;errno=EIO;return -1;}
    return original(fd,offset,whence);
}
extern "C" ssize_t write(int fd,const void* bytes,size_t size){
    using Original=ssize_t(*)(int,const void*,size_t);static const auto original=reinterpret_cast<Original>(::dlsym(RTLD_NEXT,"write"));if(!original)::_exit(86);
    uint32_t segment=0;if(!descriptorSegment(fd,segment))return original(fd,bytes,size);
    ++writes;if(writes>100000)::_exit(87);
    const off_t offset=::lseek(fd,0,SEEK_CUR);const Lsn logical=segment*WALManager::kSegmentSize+offset;
    positions=positions && offset>=0 && logical==beginLsn+accepted && accepted<=recordLength &&
        size==std::min<uint64_t>(recordLength-accepted,WALManager::kSegmentSize-static_cast<uint64_t>(offset));
    const size_t count=selected==Mode::TwiceEintr?2:selected==Mode::Eintr||selected==Mode::Mixed?1:0;
    if(interrupts<count){++interrupts;errno=EINTR;return -1;}
    const bool later=segment>beginLsn/WALManager::kSegmentSize;
    const bool last=segment==(beginLsn+recordLength-1)/WALManager::kSegmentSize;
    if(!hard && (selected==Mode::Eio||selected==Mode::NoSpace||
       ((selected==Mode::ShortEio||selected==Mode::ShortNoSpace)&&shorts)|| (selected==Mode::NextEio&&later)|| (selected==Mode::LastEio&&last))){
        ++hard;errno=selected==Mode::NoSpace||selected==Mode::ShortNoSpace?ENOSPC:EIO;return -1;}
    if(!zeroes && (selected==Mode::Zero||(selected==Mode::NextZero&&later)||(selected==Mode::LastZero&&last))){++zeroes;return 0;}
    size_t transfer=size;
    if(!shorts && (selected==Mode::Short||selected==Mode::ShortEio||selected==Mode::ShortNoSpace))transfer=std::min<size_t>(size,7);
    if(selected==Mode::AllShort||selected==Mode::Mixed)transfer=std::min<size_t>(size,size>8192?4093:13);
    const auto n=original(fd,bytes,transfer);
    if(n>0){accepted+=static_cast<size_t>(n);if(static_cast<size_t>(n)<size)++shorts;
        positions=positions && ::lseek(fd,0,SEEK_CUR)==offset+n;}
    return n;
}
extern "C" int ftruncate(int fd,off_t length) noexcept {
    using Original=int(*)(int,off_t);static const auto original=reinterpret_cast<Original>(::dlsym(RTLD_NEXT,"ftruncate"));if(!original)::_exit(86);
    uint32_t segment=0;if(hard && descriptorSegment(fd,segment) && !cleanupFaults &&
       (cleanup==Cleanup::TruncateEintr||cleanup==Cleanup::TruncateError)){
        ++cleanupFaults;errno=cleanup==Cleanup::TruncateEintr?EINTR:EIO;return -1;}
    return original(fd,length);
}
extern "C" int fsync(int fd){
    using Original=int(*)(int);static const auto original=reinterpret_cast<Original>(::dlsym(RTLD_NEXT,"fsync"));if(!original)::_exit(86);
    uint32_t segment=0;const bool data=descriptorSegment(fd,segment),directory=descriptorDirectory(fd);
    if(hard && !cleanupFaults && ((data && (cleanup==Cleanup::SyncEintr||cleanup==Cleanup::SyncError))||
       (directory && (cleanup==Cleanup::DirSyncEintr||cleanup==Cleanup::DirSyncError)))){
        ++cleanupFaults;errno=cleanup==Cleanup::SyncEintr||cleanup==Cleanup::DirSyncEintr?EINTR:EIO;return -1;}
    return original(fd);
}
extern "C" int unlink(const char* path) noexcept {
    using Original=int(*)(const char*);static const auto original=reinterpret_cast<Original>(::dlsym(RTLD_NEXT,"unlink"));if(!original)::_exit(86);
    uint32_t segment=0;if(hard && pathSegment(path,segment) && !cleanupFaults && cleanup==Cleanup::UnlinkError){++cleanupFaults;errno=EIO;return -1;}
    return original(path);
}

namespace {
using Image=std::map<std::string,std::vector<char>>;
std::vector<char> fileBytes(const fs::path& path){
    const int fd=::open(path.c_str(),O_RDONLY);require(fd>=0,"snapshot open");
    struct stat info{};require(!::fstat(fd,&info)&&info.st_size>=0,"snapshot stat");std::vector<char> result(static_cast<size_t>(info.st_size));
    size_t read=0;while(read<result.size()){const auto n=::pread(fd,result.data()+read,result.size()-read,read);require(n>0,"snapshot read");read+=static_cast<size_t>(n);}
    require(!::close(fd),"snapshot close");return result;
}
Image image(const fs::path& directory){Image result;for(const auto& entry:fs::directory_iterator(directory)){
    uint32_t segment=0;const auto name=entry.path().filename().string();if(segmentName(name.c_str(),segment))result[name]=fileBytes(entry.path());}return result;}
bool prefixPreserved(const Image& before,const Image& after){for(const auto& [name,bytes]:before){const auto found=after.find(name);
    if(found==after.end()||found->second.size()<bytes.size()||!std::equal(bytes.begin(),bytes.end(),found->second.begin()))return false;}return true;}
size_t descriptors(){const bool wasArmed=armed;armed=true;size_t count=0;for(const auto& entry:fs::directory_iterator("/proc/self/fd")){
    const auto name=entry.path().filename().string();char* end=nullptr;const long fd=std::strtol(name.c_str(),&end,10);uint32_t segment=0;
    if(end && !*end && fd>=0 && descriptorSegment(static_cast<int>(fd),segment))++count;}armed=wasArmed;return count;}
bool recordEquals(WALManager& wal,Lsn position,Lsn previous,const std::vector<char>& payload,uint64_t xid){
    const auto record=wal.ReadRecord(position);if(!record||record->rmid()!=dbms::RM_HEAP_ID||record->info()!=dbms::XLOG_HEAP_INSERT||
        record->header.xl_prev!=previous||record->header.xl_xid!=xid||record->data.size()<payload.size()||
        !std::equal(payload.begin(),payload.end(),record->data.begin()))return false;
    return std::all_of(record->data.begin()+payload.size(),record->data.end(),[](char c){return c==0;});
}
struct Layout {Lsn start;size_t payload;};
const std::array<Layout,6> layouts{{{0,0},{64,89},{WALManager::kSegmentSize-16,89},
    {WALManager::kSegmentSize-64,89},{WALManager::kSegmentSize,89},{WALManager::kSegmentSize-16,static_cast<size_t>(WALManager::kSegmentSize+89)}}};
struct Template {
    fs::path directory;Layout layout;
    Template(const fs::path& path,Layout config):directory(path),layout(config){WALManager wal(path);require(wal.ensureOpen(),"template open");
        if(config.start){const Lsn length=config.start-32;std::vector<char> prefix(static_cast<size_t>(length-sizeof(dbms::XLogRecHeader)),'P');
            require(wal.XLogInsert(dbms::RM_HEAP_ID,dbms::XLOG_HEAP_INSERT,17,prefix)==0,"template prefix");require(wal.XLogFlush(wal.currentWriteLsn()),"template flush");}}
};
std::unique_ptr<WALManager> writer(const Template& source,const fs::path& directory,bool cold){
    fs::copy(source.directory,directory,fs::copy_options::recursive);auto wal=std::make_unique<WALManager>(directory);require(wal->ensureOpen(false),"writer open");
    if(source.layout.start){require(wal->XLogInsert(dbms::RM_HEAP_ID,dbms::XLOG_HEAP_INSERT,18,{})==source.layout.start-32,"warm tail");
        require(wal->XLogFlush(wal->currentWriteLsn()),"warm flush");}
    if(cold){wal.reset();wal=std::make_unique<WALManager>(directory);require(wal->ensureOpen(false),"cold open");}
    require(wal->currentWriteLsn()==source.layout.start,"writer exact start");return wal;
}
bool runCase(const Template& source,const fs::path& directory,bool cold,Mode mode,Cleanup fault,size_t layout){
    auto wal=writer(source,directory,cold);activeDirectory=directory.string();beginLsn=source.layout.start;
    std::vector<char> payload(source.layout.payload);for(size_t i=0;i<payload.size();++i)payload[i]=static_cast<char>(i*37);
    recordLength=(sizeof(dbms::XLogRecHeader)+payload.size()+7)&~size_t(7);
    const Image before=image(directory);const auto archive=wal->pendingArchiveSegments();
    std::unique_ptr<WALManager> observer;
    if(fault==Cleanup::TruncateError||fault==Cleanup::UnlinkError){observer=std::make_unique<WALManager>(directory);require(observer->ensureOpen(false),"cached observer");}
    arm(mode,fault);const auto result=wal->XLogInsert(dbms::RM_HEAP_ID,dbms::XLOG_HEAP_INSERT,73,payload);armed=false;
    const bool cleanFailure=fault!=Cleanup::TruncateError && fault!=Cleanup::UnlinkError;
    const bool expectedSuccess=successMode(mode);
    bool good=(expectedSuccess?result==beginLsn:result==dbms::INVALID_LSN) && injected(mode) && positions &&
        (!expectedSuccess||accepted==recordLength) && (fault==Cleanup::None||cleanupFaults==1) && descriptors()<=1;
    const auto end=wal->currentWriteLsn();const auto after=image(directory);
    good=good && prefixPreserved(before,after) && end==beginLsn+(expectedSuccess?recordLength:0);
    if(!expectedSuccess && cleanFailure)good=good && after==before && wal->pendingArchiveSegments()==archive;
    if(expectedSuccess)good=good && recordEquals(*wal,beginLsn,beginLsn?beginLsn-32:0,payload,73);
    const size_t observedWrites=writes,observedShorts=shorts,observedIntr=interrupts,observedAccepted=accepted,observedCleanup=cleanupFaults;
    const auto retry=wal->XLogInsert(dbms::RM_HEAP_ID,dbms::XLOG_HEAP_INSERT,74,{'r'});
    if(cleanFailure){const Lsn retryAt=beginLsn+(expectedSuccess?recordLength:0);const Lsn previous=expectedSuccess?beginLsn:(beginLsn?beginLsn-32:0);
        good=good && retry==retryAt && recordEquals(*wal,retryAt,previous,{'r'},74) && wal->XLogFlush(wal->currentWriteLsn());
        WALManager reader(directory);good=good && reader.ensureOpen(false) && recordEquals(reader,retryAt,previous,{'r'},74);
        if(expectedSuccess)good=good && recordEquals(reader,beginLsn,beginLsn?beginLsn-32:0,payload,73);
    }else{WALManager reader(directory);good=good && retry==dbms::INVALID_LSN && !reader.ensureOpen(false);
        const auto observed=observer->XLogInsert(dbms::RM_HEAP_ID,dbms::XLOG_HEAP_INSERT,75,{'O'});
        good=good && observed==dbms::INVALID_LSN && image(directory)==after;observer.reset();}
    wal.reset();good=good && descriptors()==0;
    if(!cleanFailure){
        // Explicit fixture repair only AFTER observing fail-closed cleanup.
        WALManager names(directory);for(uint32_t segment=0;segment<3;++segment){const auto path=names.segmentPath(segment);const auto found=before.find(path.filename().string());
            if(found==before.end()){fs::remove(path);continue;}
            const int fd=::open(path.c_str(),O_WRONLY|O_CREAT|O_TRUNC,0600);require(fd>=0,"manual repair open");size_t count=0;
            while(count<found->second.size()){const auto n=::write(fd,found->second.data()+count,found->second.size()-count);require(n>0,"manual repair write");count+=static_cast<size_t>(n);}require(!::close(fd),"manual repair close");}
        WALManager repaired(directory);good=good && repaired.ensureOpen(false) && repaired.currentWriteLsn()==beginLsn;
    }
    fs::remove_all(directory);
    std::cout<<"WAL_APPEND_CASE layout="<<layout<<" cold="<<cold<<" mode="<<static_cast<int>(mode)<<" cleanup="<<static_cast<int>(fault)
        <<" valid="<<(result!=dbms::INVALID_LSN)<<" writes="<<observedWrites<<" short="<<observedShorts<<" intr="<<observedIntr
        <<" bytes="<<observedAccepted<<" cleanupFaults="<<observedCleanup<<" retry="<<(retry!=dbms::INVALID_LSN)<<" good="<<good<<'\n';
    return good;
}
bool peerCase(const Template& source,const fs::path& directory,bool cold,bool process){
    auto stale=writer(source,directory,cold);const auto prefix=image(directory);const Lsn original=stale->currentWriteLsn();Lsn peer=dbms::INVALID_LSN;
    if(process){int channel[2];require(!::pipe(channel),"peer pipe");const pid_t child=::fork();require(child>=0,"peer fork");
        if(!child){::close(channel[0]);WALManager wal(directory);Lsn location=dbms::INVALID_LSN;if(wal.ensureOpen(false)){
            location=wal.XLogInsert(dbms::RM_HEAP_ID,dbms::XLOG_HEAP_INSERT,90,{'P'});if(!wal.XLogFlush(wal.currentWriteLsn()))location=dbms::INVALID_LSN;}
            const auto sent=::write(channel[1],&location,sizeof(location));::_exit(sent==sizeof(location)?0:121);}
        ::close(channel[1]);const auto got=::read(channel[0],&peer,sizeof(peer));::close(channel[0]);int status=0;require(::waitpid(child,&status,0)==child&&WIFEXITED(status)&&WEXITSTATUS(status)==0&&got==sizeof(peer),"peer result");
    }else{WALManager wal(directory);require(wal.ensureOpen(false),"peer open");peer=wal.XLogInsert(dbms::RM_HEAP_ID,dbms::XLOG_HEAP_INSERT,90,{'P'});require(wal.XLogFlush(wal.currentWriteLsn()),"peer flush");}
    require(peer==original,"peer start");const Image withPeer=image(directory);
    const auto own=stale->XLogInsert(dbms::RM_HEAP_ID,dbms::XLOG_HEAP_INSERT,91,{'S'});WALManager reader(directory);
    const bool good=own==peer+32 && prefixPreserved(prefix,image(directory)) && prefixPreserved(withPeer,image(directory)) &&
        reader.ensureOpen(false) && recordEquals(reader,peer,peer?peer-32:0,{'P'},90) && recordEquals(reader,own,peer,{'S'},91);
    stale.reset();fs::remove_all(directory);std::cout<<"WAL_APPEND_PEER start="<<original<<" cold="<<cold<<" process="<<process<<" own="<<own<<" good="<<good<<'\n';return good;
}
bool foreignTail(const Template& source,const fs::path& directory,bool cold){
    auto wal=writer(source,directory,cold);const auto path=wal->segmentPath(static_cast<uint32_t>(source.layout.start/WALManager::kSegmentSize));
    const int fd=::open(path.c_str(),O_WRONLY|O_CREAT|O_APPEND,0600);require(fd>=0,"foreign open");require(::write(fd,"X",1)==1&&!::close(fd),"foreign append");
    const auto before=image(directory);const auto result=wal->XLogInsert(dbms::RM_HEAP_ID,dbms::XLOG_HEAP_INSERT,92,{'F'});
    const bool good=result==dbms::INVALID_LSN && image(directory)==before;wal.reset();fs::remove_all(directory);return good;
}
}
int main(){char pattern[]="/tmp/dbms-wal-append-failure.XXXXXX";const char* made=::mkdtemp(pattern);if(!made)return 120;const fs::path root(made);
    try{size_t controls=0,failures=0;const auto check=[&](bool good,const char* label){++controls;if(!good){++failures;std::cerr<<"WAL_APPEND_FAIL "<<label<<'\n';}};
        const auto path=[&]{return root/("case-"+std::to_string(controls));};
        for(size_t layout=0;layout<layouts.size();++layout){Template source(root/("seed-"+std::to_string(layout)),layouts[layout]);
            for(bool cold:{false,true})for(Mode mode:{Mode::Normal,Mode::Eintr,Mode::TwiceEintr,Mode::Short,Mode::AllShort,Mode::Mixed,
                    Mode::Eio,Mode::NoSpace,Mode::ShortEio,Mode::ShortNoSpace,Mode::Zero,Mode::SeekError})
                check(runCase(source,path(),cold,mode,Cleanup::None,layout),"transfer/failure");
            for(Mode mode:{Mode::OpenEintr,Mode::OpenError,Mode::OpenEmfile})check(runCase(source,path(),true,mode,Cleanup::None,layout),"cold open");
            if(layout==2||layout==3||layout==5){
                for(bool cold:{false,true})for(Mode mode:{Mode::NextEio,Mode::NextZero,Mode::NextOpenError})
                    check(runCase(source,path(),cold,mode,Cleanup::None,layout),"later segment failure");
                for(bool cold:{false,true})for(Cleanup fault:{Cleanup::TruncateEintr,Cleanup::TruncateError,Cleanup::SyncEintr,Cleanup::SyncError,
                        Cleanup::UnlinkError,Cleanup::DirSyncEintr,Cleanup::DirSyncError})
                    check(runCase(source,path(),cold,layout==5?Mode::LastEio:Mode::NextEio,fault,layout),"rollback IO boundary");
            }
            if(layout==5)for(bool cold:{false,true})for(Mode mode:{Mode::LastEio,Mode::LastZero,Mode::LastOpenError})
                check(runCase(source,path(),cold,mode,Cleanup::None,layout),"third segment failure");
            if(layout==0||layout==1||layout==4){for(bool cold:{false,true}){
                for(bool process:{false,true})check(peerCase(source,path(),cold,process),"stale writer preserves peer");
                check(foreignTail(source,path(),cold),"foreign malformed tail not silently removed");}}
        }
        require(controls==246,"complete append inventory");fs::remove_all(root);std::cout<<"WAL_APPEND_FAILURE_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';return failures?1:0;
    }catch(const std::exception& error){armed=false;std::cerr<<"WAL_APPEND_DRIVER_EXCEPTION "<<error.what()<<'\n';fs::remove_all(root);return 120;}
}
