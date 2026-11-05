#pragma once

// Read-only evidence readers for caller-identified restored database files.
// No allocator, MVCC visibility decision, page upgrade, mutation or cache owner.
#include "storage/DataFileHeader.h"
#include "storage/HeapTupleHeader.h"
#include "storage/PgPage.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <exception>
#include <limits>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace dbms { namespace restore_horizon_detail {
namespace reader_detail {
constexpr uint64_t maxXid=std::numeric_limits<uint32_t>::max();
constexpr uint64_t xidsPerClogSegment=1024u*1024u;
constexpr uint64_t clogSegmentBytes=xidsPerClogSegment/4u;
enum class Open { Missing, Ready, Failed };
struct File {
    int fd=-1;
    struct stat initial{};
    ~File(){if(fd>=0)(void)::close(fd);}
    File()=default;
    File(const File&)=delete;
    File& operator=(const File&)=delete;
    bool finish(){
        struct stat after{};
        const bool stable=::fstat(fd,&after)==0 && after.st_dev==initial.st_dev &&
            after.st_ino==initial.st_ino && after.st_size==initial.st_size &&
            after.st_mtim.tv_sec==initial.st_mtim.tv_sec &&
            after.st_mtim.tv_nsec==initial.st_mtim.tv_nsec &&
            after.st_ctim.tv_sec==initial.st_ctim.tv_sec &&
            after.st_ctim.tv_nsec==initial.st_ctim.tv_nsec;
        const int old=fd;fd=-1;
        return ::close(old)==0 && stable;
    }
};
inline Open open(const std::filesystem::path& path,File& file){
    do {file.fd=::open(path.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW);}
    while(file.fd<0 && errno==EINTR);
    if(file.fd<0)return errno==ENOENT?Open::Missing:Open::Failed;
    if(::fstat(file.fd,&file.initial)!=0 || !S_ISREG(file.initial.st_mode) ||
       file.initial.st_size<0)return Open::Failed;
    return Open::Ready;
}
inline bool read(const File& file,void* output,size_t count,uint64_t offset){
    if(offset>static_cast<uint64_t>(std::numeric_limits<off_t>::max()) ||
       count>static_cast<uint64_t>(std::numeric_limits<off_t>::max())-offset)return false;
    size_t done=0;
    while(done<count){
        const auto n=::pread(file.fd,static_cast<char*>(output)+done,count-done,
            static_cast<off_t>(offset+done));
        if(n<0 && errno==EINTR)continue;
        if(n<=0)return false;
        done+=static_cast<size_t>(n);
    }
    return true;
}
inline bool eof(const File& file,uint64_t offset){
    if(offset>static_cast<uint64_t>(std::numeric_limits<off_t>::max()))return false;
    char extra=0;ssize_t n;
    do {n=::pread(file.fd,&extra,1,static_cast<off_t>(offset));}while(n<0&&errno==EINTR);
    return n==0;
}
inline bool heapPage(const File& file,uint32_t id,std::array<char,PgPage::PAGE_SIZE>& bytes){
    if(!read(file,bytes.data(),bytes.size(),uint64_t(id)*bytes.size()))return false;
    PgPage page(bytes.data());
    // Preserve readable layout4 without upgrade; layout5 authenticates its block.
    return page.isValid() && (!page.hasBoundPageId() || page.header()->pd_page_id==id) &&
        page.header()->pd_special==PgPage::PAGE_SIZE-sizeof(uint32_t);
}
inline bool tuple(const char* bytes,size_t size,uint64_t& high){
    if(size<sizeof(HeapTupleHeaderData))return false;
    HeapTupleHeaderData header{};std::memcpy(&header,bytes,sizeof(header));
    const size_t attributes=header.t_infomask2&HEAP_NATTS_MASK;
    const size_t minimum=sizeof(header)+((header.t_infomask&HEAP_HASNULL)?(attributes+7)/8:0);
    if(header.t_hoff<minimum || header.t_hoff>size || header.t_hoff%PgPage::MAXALIGN!=0)return false;
    // Even aborted/invalid/lock-only xmax can name an allocated identifier.
    high=std::max(high,uint64_t(header.t_fields.t_xmin));
    high=std::max(high,uint64_t(header.t_fields.t_xmax));
    return true;
}
} // namespace reader_detail

// Only MAIN/init heap forks identified from real schema may be passed here.
// TOAST chunk files have a different tuple representation and are not inputs.
// Missing optional forks preserve H; a present malformed file fails without
// changing H. Complete unpublished tail pages beyond header.numPages are not
// part of the durable logical heap extent and are deliberately not inspected.
inline bool readHeap(const std::filesystem::path& path,uint64_t& H){
    using namespace reader_detail;
    try {
        File file;const auto opened=open(path,file);
        if(opened==Open::Missing)return true;
        if(opened!=Open::Ready)return false;
        const uint64_t physical=static_cast<uint64_t>(file.initial.st_size);
        if(physical<PgPage::PAGE_SIZE || physical%PgPage::PAGE_SIZE!=0)return false;
        std::array<char,PgPage::PAGE_SIZE> bytes{};
        if(!read(file,bytes.data(),bytes.size(),0))return false;
        DataFileHeader header{};std::memcpy(&header,bytes.data(),sizeof(header));
        if(header.magic!=DATA_FILE_MAGIC || header.formatVersion!=DATA_FILE_FORMAT_VERSION ||
           header.numPages==0 || header.freeListHead>=header.numPages ||
           header.headerChecksum!=computeDataFileHeaderChecksum(header) ||
           uint64_t(header.numPages)*PgPage::PAGE_SIZE>physical)return false;
        std::set<uint32_t> free;
        for(uint32_t id=header.freeListHead;id!=0;){
            if(id>=header.numPages || !free.insert(id).second || !heapPage(file,id,bytes))return false;
            PgPage page(bytes.data());
            if(page.numLinePointers()!=0)return false;
            id=page.nextPage();
        }
        uint64_t observed=0;
        for(uint64_t id=1;id<header.numPages;++id){
            if(free.count(static_cast<uint32_t>(id)))continue;
            if(!heapPage(file,static_cast<uint32_t>(id),bytes))return false;
            PgPage page(bytes.data());
            std::vector<std::pair<size_t,size_t>> ranges;
            for(uint16_t slot=1;slot<=page.numLinePointers();++slot){
                const auto* item=page.itemId(slot);
                const auto flags=item->lp_len>>PgPage::LP_FLAG_SHIFT;
                const size_t length=item->lp_len&PgPage::LP_LEN_MASK;
                const size_t offset=item->lp_off&PgPage::LP_OFF_MASK;
                if(flags==PgPage::LP_REDIRECT || flags==PgPage::LP_DEAD ||
                   (flags==PgPage::LP_UNUSED && length==0))continue;
                // LP_UNUSED with retained location/length is rollback/dead
                // tuple evidence, not a nonexistent row. Include both XIDs.
                if((flags!=PgPage::LP_NORMAL && flags!=PgPage::LP_UNUSED) ||
                   !tuple(bytes.data()+offset,length,observed))return false;
                ranges.emplace_back(offset,offset+length);
            }
            std::sort(ranges.begin(),ranges.end());
            for(size_t i=1;i<ranges.size();++i)if(ranges[i].first<ranges[i-1].second)return false;
        }
        if(!eof(file,physical) || !file.finish())return false;
        H=std::max(H,observed);return true;
    }catch(const std::exception&){return false;}
}

// dir is the actual pg_xact directory, not its parent. The canonical current
// writer uses unpadded lowercase hex segment numbers ("0", "1", ...).
inline bool readClog(const std::filesystem::path& dir,uint64_t& H){
    using namespace reader_detail;
    try {
        std::error_code ec;const auto status=std::filesystem::symlink_status(dir,ec);
        if(ec==std::errc::no_such_file_or_directory || (!ec && status.type()==std::filesystem::file_type::not_found))return true;
        if(ec || !std::filesystem::is_directory(status))return false;
        uint64_t observed=0;std::filesystem::directory_iterator it(dir,ec),end;
        if(ec)return false;
        for(;it!=end;it.increment(ec)){
            if(ec)return false;
            const auto entryStatus=it->symlink_status(ec);
            if(ec || !std::filesystem::is_regular_file(entryStatus))return false;
            const std::string name=it->path().filename().string();
            if(name==".clog.lock")continue;
            if(name.empty() || (name.size()>1 && name[0]=='0') ||
               name.find_first_not_of("0123456789abcdef")!=std::string::npos)return false;
            uint64_t segment=0;
            const auto parsed=std::from_chars(name.data(),name.data()+name.size(),segment,16);
            if(parsed.ec!=std::errc{} || parsed.ptr!=name.data()+name.size() ||
               segment>maxXid/xidsPerClogSegment)return false;
            File file;if(open(it->path(),file)!=Open::Ready)return false;
            const uint64_t size=static_cast<uint64_t>(file.initial.st_size);
            if(size>clogSegmentBytes)return false;
            std::array<unsigned char,8192> bytes{};
            for(uint64_t offset=0;offset<size;){
                const size_t count=static_cast<size_t>(std::min<uint64_t>(bytes.size(),size-offset));
                if(!read(file,bytes.data(),count,offset))return false;
                for(size_t i=0;i<count;++i)if(bytes[i]){
                    for(unsigned bit=0;bit<4;++bit)if((bytes[i]>>(bit*2))&3u){
                        const uint64_t xid=segment*xidsPerClogSegment+(offset+i)*4+bit;
                        if(xid>maxXid)return false;
                        observed=std::max(observed,xid);
                    }
                }
                offset+=count;
            }
            // Short segments retain the original zero/InProgress EOF tail.
            if(!eof(file,size) || !file.finish())return false;
        }
        if(ec)return false;
        H=std::max(H,observed);return true;
    }catch(const std::exception&){return false;}
}

// Existing native-endian checkpoint: uint64 timestamp, max-allocated, WAL LSN.
// Timestamp and LSN are opaque here; the existing restore/WAL validators own
// their meaning. This helper does not invent a clock or transaction floor.
inline bool readCheckpoint(const std::filesystem::path& path,uint64_t& H){
    using namespace reader_detail;
    File file;const auto opened=open(path,file);
    if(opened==Open::Missing)return true;
    if(opened!=Open::Ready || file.initial.st_size!=24)return false;
    std::array<uint64_t,3> words{};
    if(!read(file,words.data(),sizeof(words),0) || words[1]>maxXid ||
       !eof(file,sizeof(words)) || !file.finish())return false;
    H=std::max(H,words[1]);return true;
}

}} // namespace dbms::restore_horizon_detail
