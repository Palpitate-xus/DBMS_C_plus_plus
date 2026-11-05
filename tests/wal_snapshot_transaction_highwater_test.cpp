// Public read-only WAL snapshot regression. Original 20 and multi-root 12
// independently executed controls/expectations are retained unchanged below.
// No stdin, CLI argument, external artifact, test ordering or Python is needed.
// The regular repository runner may link its usual full core and real stubs.
#include "storage/WAL.h"
#include "common/sha256.h"
#include <array>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <cstring>
#include <sys/stat.h>
#include <cstdlib>
#include <system_error>
#include <unistd.h>

namespace original_snapshot20 {
using namespace dbms;
namespace fs=std::filesystem;
static void need(bool v,const std::string& m){if(!v)throw std::runtime_error(m);}
static std::string digest(const fs::path&p){std::ifstream f(p,std::ios::binary);need(bool(f),"hash open");SHA256 h;std::array<char,65536>b{};while(f){f.read(b.data(),b.size());h.append(b.data(),size_t(f.gcount()));}need(f.eof(),"hash read");return h.finishHex();}
static std::map<std::string,std::string> inventory(const fs::path&p){std::map<std::string,std::string>m;if(!fs::exists(p))return m;for(auto&e:fs::recursive_directory_iterator(p)){const auto n=e.path().lexically_relative(p).string();if(e.is_regular_file())m[n]="F:"+std::to_string(e.file_size())+":"+digest(e.path());else if(e.is_directory())m[n]="D";else m[n]="OTHER";}return m;}
static size_t controls=0;
static void check(const std::string&name,const fs::path&root,bool expected,uint64_t high){auto before=inventory(root);uint64_t actual=0xabcddcbaULL;const bool ok=WALManager::readSnapshotTransactionHighWater(root/"pg_wal",actual);need(ok==expected,name+" success flag");need(actual==(expected?high:0xabcddcbaULL),name+" highwater/output on failure");need(inventory(root)==before,name+" source mutated");for(const auto&[file,hash]:before)std::cout<<"WAL_SNAPSHOT_FILE case="<<name<<" file="<<file<<" inventory="<<hash<<'\n';++controls;std::cout<<"WAL_SNAPSHOT_CASE name="<<name<<" success="<<ok<<" high="<<actual<<" immutable=1 passed=1\n";}
static std::vector<char> cpdata(uint64_t next){std::array<uint64_t,2>d{next,12345};return std::vector<char>(reinterpret_cast<const char*>(d.data()),reinterpret_cast<const char*>(d.data())+sizeof(d));}
static void meta(const fs::path&root,Lsn anchor){std::array<uint64_t,3>a{1,1,anchor};std::ofstream f(root/"checkpoint",std::ios::binary);f.write(reinterpret_cast<const char*>(a.data()),sizeof(a));need(bool(f),"checkpoint metadata write");}
static void flip(const fs::path&p,uint64_t offset){std::fstream f(p,std::ios::binary|std::ios::in|std::ios::out);f.seekg(offset);char c=0;f.read(&c,1);need(bool(f),"flip read");c^=1;f.seekp(offset);f.write(&c,1);need(bool(f),"flip write");}
static void clone(const fs::path&a,const fs::path&b){fs::copy(a,b,fs::copy_options::recursive);}
int run(const fs::path& ownedCases){try{const fs::path base=ownedCases;need(!fs::exists(base),"fresh owned output");fs::create_directories(base);
 {auto r=base/"empty";fs::create_directories(r/"pg_wal");check("empty",r,true,0);}
 {auto r=base/"missing";fs::create_directories(r);check("missing",r,false,0);}
 fs::path normal=base/"normal";{WALManager w(normal/"pg_wal");need(w.ensureOpen(),"normal writer");need(w.XLogInsert(RM_HEAP_ID,XLOG_HEAP_INSERT,17,{'a'})!=INVALID_LSN,"insert");need(w.XLogInsert(RM_XACT_ID,XLOG_XACT_ABORT,41,{'b'})!=INVALID_LSN,"abort");need(w.XLogFlush(w.currentWriteLsn()),"normal flush");}check("normal",normal,true,41);
 {auto r=base/"without_timeline";clone(normal,r);fs::remove(r/"pg_wal/timeline");check("infer_timeline_no_creation",r,true,41);}
 {auto r=base/"checkpoint";{WALManager w(r/"pg_wal");need(w.ensureOpen(),"cp writer");need(w.XLogInsert(RM_CHECKPOINT_ID,XLOG_CHECKPOINT_ONLINE,0,cpdata(151))!=INVALID_LSN,"cp record");need(w.XLogFlush(w.currentWriteLsn()),"cp flush");}check("checkpoint_exclusive",r,true,150);}
 {auto r=base/"checkpoint_zero";{WALManager w(r/"pg_wal");need(w.ensureOpen(),"cp zero writer");need(w.XLogInsert(RM_CHECKPOINT_ID,0,0,cpdata(0))!=INVALID_LSN,"cp zero record");need(w.XLogFlush(w.currentWriteLsn()),"cp zero flush");}check("checkpoint_zero",r,false,0);}
 {auto r=base/"checkpoint_short";{WALManager w(r/"pg_wal");need(w.ensureOpen(),"cp short writer");need(w.XLogInsert(RM_CHECKPOINT_ID,0,0,{'x'})!=INVALID_LSN,"cp short record");need(w.XLogFlush(w.currentWriteLsn()),"cp short flush");}check("checkpoint_short",r,false,0);}
 fs::path padded=base/"padded";{WALManager w(padded/"pg_wal");need(w.ensureOpen(),"pad writer");need(w.XLogInsert(RM_HEAP_ID,0,9,{'a'})!=INVALID_LSN,"pad first");need(w.switchWal()!=INVALID_LSN,"pad switch1");need(w.XLogInsert(RM_HEAP_ID,0,201,{'b'})!=INVALID_LSN,"pad second");need(w.switchWal()!=INVALID_LSN,"pad switch2");need(w.XLogInsert(RM_CHECKPOINT_ID,0,0,cpdata(304))!=INVALID_LSN,"pad cp");need(w.XLogFlush(w.currentWriteLsn()),"pad flush");}check("padding_continues_two_switches",padded,true,303);
 {auto r=base/"padding_corruption";clone(padded,r);WALManager names(r/"pg_wal");flip(names.segmentPath(0),WALManager::kSegmentSize-1);check("padding_nonzero_suffix_rejected",r,false,0);}
 {auto r=base/"interior_missing";clone(padded,r);WALManager names(r/"pg_wal");fs::remove(names.segmentPath(1));check("interior_missing_segment",r,false,0);}
 {auto r=base/"crc";clone(normal,r);WALManager names(r/"pg_wal");flip(names.segmentPath(0),sizeof(XLogRecHeader));check("crc_corruption",r,false,0);}
 {auto r=base/"truncated";clone(normal,r);WALManager names(r/"pg_wal");fs::resize_file(names.segmentPath(0),fs::file_size(names.segmentPath(0))-1);check("truncated_record",r,false,0);}
 {auto r=base/"oversize";clone(normal,r);WALManager names(r/"pg_wal");fs::resize_file(names.segmentPath(0),WALManager::kSegmentSize+1);check("oversize_segment",r,false,0);}
 {auto r=base/"fork";{WALManager w(r/"pg_wal");need(w.ensureOpen(),"fork writer");need(w.XLogInsert(RM_HEAP_ID,0,907,{'a'})!=INVALID_LSN,"old timeline");need(w.XLogFlush(w.currentWriteLsn()),"old fork flush");need(w.setTimeline(2),"real timeline fork");need(w.XLogInsert(RM_HEAP_ID,0,501,{'b'})!=INVALID_LSN,"new timeline");need(w.XLogFlush(w.currentWriteLsn()),"new fork flush");}check("all_timelines_not_only_selected",r,true,907);}
 {auto r=base/"crossing";{WALManager w(r/"pg_wal");need(w.ensureOpen(),"cross writer");need(w.XLogInsert(RM_HEAP_ID,0,601,std::vector<char>(WALManager::kSegmentSize+123,'q'))==0,"cross record");need(w.XLogInsert(RM_HEAP_ID,0,602,{'r'})!=INVALID_LSN,"cross tail");need(w.XLogFlush(w.currentWriteLsn()),"cross flush");}check("record_spans_segments",r,true,602);}
 fs::path retained=base/"retained";Lsn first=0,cp=0,last=0;{WALManager w(retained/"pg_wal");need(w.ensureOpen(),"retained writer");const size_t initial=WALManager::kSegmentSize-256;need(w.XLogInsert(RM_HEAP_ID,0,11,std::vector<char>(initial-sizeof(XLogRecHeader),'a'))==0,"retained prefix");need(w.XLogInsert(RM_HEAP_ID,0,12,std::vector<char>(512,'b'))==initial,"retained crossing");first=w.XLogInsert(RM_HEAP_ID,0,703,{'c'});need(first!=INVALID_LSN,"retained first");cp=w.XLogInsert(RM_CHECKPOINT_ID,0,0,cpdata(801));need(cp!=INVALID_LSN,"retained cp");last=w.XLogInsert(RM_HEAP_ID,0,704,{'d'});need(last!=INVALID_LSN,"retained tail");need(w.XLogFlush(w.currentWriteLsn()),"retained flush");meta(retained,cp);need(w.markSegmentsReadyBefore(cp),"retained ready");need(w.archivePendingSegments(retained/"archive"),"retained archive");need(w.truncateBefore(cp),"actual retained truncate");}check("retained_prefix_checked_anchor",retained,true,800);
 {auto r=base/"retained_no_anchor";clone(retained,r);fs::remove(r/"checkpoint");check("retained_prefix_missing_anchor",r,false,0);}
 {auto r=base/"retained_wrong_anchor";clone(retained,r);meta(r,first);check("retained_prefix_noncheckpoint_anchor",r,false,0);}
 {auto r=base/"retained_bad_crc";clone(retained,r);WALManager names(r/"pg_wal");flip(names.segmentPath(first/WALManager::kSegmentSize),first%WALManager::kSegmentSize+sizeof(XLogRecHeader));check("retained_prefix_CRC_failure",r,false,0);}
 {auto r=base/"readonly_permissions";clone(normal,r);for(auto&e:fs::recursive_directory_iterator(r))fs::permissions(e.path(),e.is_directory()?fs::perms::owner_read|fs::perms::owner_exec:fs::perms::owner_read);check("source_files_readonly",r,true,41);}
 std::cout<<"WAL_SNAPSHOT_COMPLETE controls="<<controls<<" failed=0\n";return 0;
 }catch(const std::exception&e){std::cerr<<"WAL_SNAPSHOT_FAILURE completed="<<controls<<" error="<<e.what()<<'\n';return 1;}}

} // namespace original_snapshot20

namespace multiroot_snapshot12 {
using namespace dbms;namespace fs=std::filesystem;
static void need(bool v,const char*m){if(!v)throw std::runtime_error(m);}
static std::string digest(const fs::path&p){std::ifstream f(p,std::ios::binary);need(bool(f),"hash open");SHA256 h;std::array<char,65536>b{};while(f){f.read(b.data(),b.size());h.append(b.data(),size_t(f.gcount()));}need(f.eof(),"hash read");return h.finishHex();}
static std::map<std::string,std::string> inventory(const fs::path&p){std::map<std::string,std::string>m;for(auto&e:fs::recursive_directory_iterator(p)){auto n=e.path().lexically_relative(p).string();m[n]=e.is_regular_file()?"F:"+std::to_string(e.file_size())+":"+digest(e.path()):"D";}return m;}
static size_t count=0;static fs::path base;
static void check(const char*label,std::vector<fs::path>roots,const fs::path&cp,bool expected,uint64_t want){const auto before=inventory(base);uint64_t h=1234567;bool ok=WALManager::readSnapshotTransactionHighWater(roots,cp,h);need(ok==expected,"success flag");need(h==(expected?want:1234567),"H/output failure");need(inventory(base)==before,"input mutation");++count;std::cout<<"MULTIROOT_CASE name="<<label<<" success="<<ok<<" high="<<h<<" immutable=1 passed=1\n";}
static void flip(const fs::path&p,size_t off){std::fstream f(p,std::ios::binary|std::ios::in|std::ios::out);f.seekg(off);char b=0;f.read(&b,1);need(bool(f),"flip read");b^=1;f.seekp(off);f.write(&b,1);need(bool(f),"flip write");}
int run(const fs::path& ownedCases,const fs::path& originalFixtures){try{base=ownedCases;need(!fs::exists(base),"fresh base");fs::create_directories(base);const fs::path original=originalFixtures;const auto cp=base/"absent-checkpoint";
 fs::create_directories(base/"empty1");fs::create_directories(base/"empty2");check("empty_roots",{base/"empty1",base/"empty2"},cp,true,0);check("no_roots",{},cp,false,0);
 auto a=base/"a",b=base/"b";fs::copy(original/"normal/pg_wal",a,fs::copy_options::recursive);fs::copy(a,b,fs::copy_options::recursive);check("duplicate_equal",{a,b},cp,true,41);
 WALManager names(a);auto filename=names.segmentPath(0).filename();fs::resize_file(b/filename,20);check("duplicate_short_prefix",{a,b},cp,true,41);check("duplicate_reverse_root_order",{b,a},cp,true,41);flip(b/filename,4);check("duplicate_conflict",{a,b},cp,false,0);
 const auto cross=original/"crossing/pg_wal";fs::create_directories(base/"cross1");fs::create_directories(base/"cross2");WALManager crossnames(cross);fs::copy_file(crossnames.segmentPath(0),base/"cross1"/crossnames.segmentPath(0).filename());fs::copy_file(crossnames.segmentPath(1),base/"cross2"/crossnames.segmentPath(1).filename());check("record_crosses_roots",{base/"cross1",base/"cross2"},cp,true,602);check("incomplete_archive_only",{base/"cross1"},cp,false,0);
 const auto pad=original/"padded/pg_wal";fs::create_directories(base/"pad1");fs::create_directories(base/"pad2");WALManager padnames(pad);for(unsigned n:{0u,1u,2u})fs::copy_file(padnames.segmentPath(n),base/(n?"pad2":"pad1")/padnames.segmentPath(n).filename());check("padding_across_roots",{base/"pad1",base/"pad2"},cp,true,303);
 auto r=base/"retained-noparent-anchor";fs::copy(original/"retained/pg_wal",r,fs::copy_options::recursive);check("explicit_retained_anchor",{r},original/"retained/checkpoint",true,800);check("missing_explicit_anchor",{r},cp,false,0);
 fs::copy(original/"fork/pg_wal",base/"fork",fs::copy_options::recursive);check("all_fork_timelines",{base/"fork",base/"empty1"},cp,true,907);
 std::cout<<"MULTIROOT_COMPLETE controls="<<count<<" failed=0\n";return 0;}catch(const std::exception&e){std::cerr<<"MULTIROOT_FAILURE completed="<<count<<" error="<<e.what()<<'\n';return 1;}}

} // namespace multiroot_snapshot12

namespace {
// Fixtures are genuine writer-created WAL in a unique owned temporary root.
// The readonly-input control intentionally removes write bits; restore only
// these owned directory permissions after all immutability checks to clean up.
class OwnedTemporaryRoot {
public:
    OwnedTemporaryRoot() {
        std::string name = (std::filesystem::temp_directory_path() /
            "dbms-wal-snapshot-XXXXXX").string();
        std::vector<char> pattern(name.begin(), name.end());
        pattern.push_back('\0');
        const char* created = ::mkdtemp(pattern.data());
        if (!created) throw std::runtime_error("owned temporary root");
        path = created;
    }
    ~OwnedTemporaryRoot() {
        std::error_code error;
        for (std::filesystem::recursive_directory_iterator it(path, error), end;
             !error && it != end; it.increment(error)) {
            std::error_code permissionError;
            if (it->is_directory(permissionError) && !permissionError)
                std::filesystem::permissions(it->path(),
                    std::filesystem::perms::owner_all,
                    std::filesystem::perm_options::add, permissionError);
        }
        std::filesystem::remove_all(path, error);
    }
    std::filesystem::path path;
    OwnedTemporaryRoot(const OwnedTemporaryRoot&) = delete;
    OwnedTemporaryRoot& operator=(const OwnedTemporaryRoot&) = delete;
};
} // namespace

int main() {
    try {
        OwnedTemporaryRoot owned;
        const auto original = owned.path / "original20";
        const auto multiple = owned.path / "multiroot12";
        if (original_snapshot20::run(original) != 0) return 1;
        if (multiroot_snapshot12::run(multiple, original) != 0) return 1;
        std::cout << "Tests run: 32\nTests failed: 0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "WAL_SNAPSHOT_FIXTURE_FAILURE " << error.what() << '\n';
        return 1;
    }
}
