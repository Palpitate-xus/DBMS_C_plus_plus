// test_sources: src/process/RuntimeStats.cpp src/process/SqlStats.cpp src/process/StatsPersistence.cpp
#include "process/RuntimeStats.h"
#include "process/SqlStats.h"
#include "process/StatsPersistence.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>
#include <vector>

namespace {
namespace fs=std::filesystem;
using Database=dbms::RuntimeDatabaseStats;
using Table=dbms::RuntimeTableStats;
using Sql=dbms::SqlStatEntry;
struct Runtime { Database database;std::vector<Table> tables; };
constexpr size_t noFailure=std::numeric_limits<size_t>::max();
constexpr size_t allocationCases=1024;
enum class Io { None,FileSync,DirectorySync,Rename,Write,ShortWriteError,InterruptedWrite,ShortWrite };
struct Fault {
    bool armed=false,published=false;
    size_t calls=0,selected=noFailure,thrown=0,afterRenameAllocations=0;
    Io io=Io::None;
    int ioTriggered=0,writeStage=0;
    const char* destination=nullptr;
    dev_t directoryDevice=0;ino_t directoryInode=0;
};
thread_local Fault fault;
size_t controls=0,failures=0,allocationTriggered=0,ioTriggered=0;
void check(bool value,const std::string& label) {
    ++controls;if (!value) { ++failures;std::cerr<<"FAIL "<<label<<'\n'; }
}
void require(bool value,const char* label) { if (!value) throw std::runtime_error(label); }
void beforeAllocation() {
    if (!fault.armed) return;
    if (fault.published) ++fault.afterRenameAllocations;
    const size_t selected=fault.calls++;
    if (selected==fault.selected) { fault.selected=noFailure;++fault.thrown;throw std::bad_alloc(); }
}
[[gnu::noinline]] void* allocate(size_t n,size_t alignment=0) {
    beforeAllocation();void* result=nullptr;
    if (alignment) { if (::posix_memalign(&result,alignment,std::max<size_t>(1,n))!=0) result=nullptr; }
    else result=std::malloc(std::max<size_t>(1,n));
    if (!result) throw std::bad_alloc();
    return result;
}
[[gnu::noinline]] void deallocate(void* p) noexcept { std::free(p); }
bool isTemporary(int fd) {
    if (!fault.armed||!fault.destination) return false;
    char descriptor[64],path[4096];
    const int length=std::snprintf(descriptor,sizeof(descriptor),"/proc/self/fd/%d",fd);
    if (length<=0||static_cast<size_t>(length)>=sizeof(descriptor)) return false;
    const ssize_t count=::readlink(descriptor,path,sizeof(path)-1);
    if (count<0) return false;
    path[count]=0;const size_t prefix=std::strlen(fault.destination);
    return std::strncmp(path,fault.destination,prefix)==0&&std::strncmp(path+prefix,".tmp.",5)==0;
}
template<typename T> void scalar(std::string& out,T value) { out.append(reinterpret_cast<const char*>(&value),sizeof(value)); }
void text(std::string& out,const std::string& value) { scalar(out,static_cast<uint32_t>(value.size()));out+=value; }
void dbBytes(std::string& out,const Database& d) {
    text(out,d.dbname);scalar(out,d.queries);scalar(out,d.failedQueries);scalar(out,d.xactCommit);
    scalar(out,d.xactRollback);scalar(out,d.tupReturned);scalar(out,d.tupFetched);scalar(out,d.totalQueryTimeMs);
}
void tableBytes(std::string& out,const Table& t) {
    text(out,t.dbname);text(out,t.relname);scalar(out,t.seqScan);scalar(out,t.seqTupRead);scalar(out,t.idxScan);
    scalar(out,t.idxTupFetch);scalar(out,t.nTupIns);scalar(out,t.nTupUpd);scalar(out,t.nTupDel);
    scalar(out,t.nLiveTup);scalar(out,static_cast<uint8_t>(t.liveTupEstimateValid));
}
void sqlBytes(std::string& out,const Sql& s) {
    text(out,s.dbname);text(out,s.sql);scalar(out,s.calls);scalar(out,s.totalTimeMs);
    scalar(out,s.minTimeMs);scalar(out,s.maxTimeMs);scalar(out,s.meanTimeMs);
}
std::string encode(Runtime r) {
    std::sort(r.tables.begin(),r.tables.end(),[](const Table& a,const Table& b){return a.relname<b.relname;});
    std::string out;text(out,"DBMS_RUNTIME_STATS_V1");scalar(out,uint32_t{1});dbBytes(out,r.database);
    scalar(out,static_cast<uint64_t>(r.tables.size()));for (const auto& row:r.tables) tableBytes(out,row);return out;
}
std::string encodeSql(const std::string& db,std::vector<Sql> rows) {
    std::sort(rows.begin(),rows.end(),[](const Sql& a,const Sql& b){return a.sql<b.sql;});
    std::string out;text(out,"DBMS_SQL_STATS_V1");scalar(out,uint32_t{1});text(out,db);
    scalar(out,static_cast<uint64_t>(rows.size()));for (const auto& row:rows) sqlBytes(out,row);return out;
}
std::string runtimeState(std::vector<Database> databases,std::vector<Table> tables) {
    std::sort(databases.begin(),databases.end(),[](const Database& a,const Database& b){return a.dbname<b.dbname;});
    std::sort(tables.begin(),tables.end(),[](const Table& a,const Table& b){return a.dbname==b.dbname?a.relname<b.relname:a.dbname<b.dbname;});
    std::string out;scalar(out,static_cast<uint64_t>(databases.size()));for (const auto& row:databases) dbBytes(out,row);
    scalar(out,static_cast<uint64_t>(tables.size()));for (const auto& row:tables) tableBytes(out,row);return out;
}
std::string runtimeState(const Runtime& target,const Runtime& other) {
    auto rows=target.tables;rows.insert(rows.end(),other.tables.begin(),other.tables.end());
    return runtimeState({target.database,other.database},rows);
}
std::string sqlState(std::vector<Sql> rows) {
    std::sort(rows.begin(),rows.end(),[](const Sql& a,const Sql& b){return a.dbname==b.dbname?a.sql<b.sql:a.dbname<b.dbname;});
    std::string out;scalar(out,static_cast<uint64_t>(rows.size()));for (const auto& row:rows) sqlBytes(out,row);return out;
}
std::string currentRuntime() { return runtimeState(dbms::getRuntimeDatabaseStats(),dbms::getRuntimeTableStats()); }
std::string currentSql() { return sqlState(dbms::getSqlStats()); }
void writeFile(const fs::path& path,const std::string& data) {
    std::ofstream out(path,std::ios::binary|std::ios::trunc);out.write(data.data(),static_cast<std::streamsize>(data.size()));
    out.close();require(!out.fail(),"fixture write");
}
std::string readFile(const fs::path& path) {
    std::ifstream in(path,std::ios::binary);require(in.good(),"fixture read");
    std::string out{std::istreambuf_iterator<char>(in),std::istreambuf_iterator<char>()};require(!in.bad(),"fixture read error");return out;
}
size_t descriptors() { size_t n=0;for (const auto& row:fs::directory_iterator("/proc/self/fd")) {(void)row;++n;}return n; }
Database makeDb(const std::string& name,uint64_t seed) {
    Database d;d.dbname=name;d.queries=seed+100;d.failedQueries=seed+1;d.xactCommit=seed+2;d.xactRollback=seed+3;
    d.tupReturned=seed+4;d.tupFetched=seed+5;d.totalQueryTimeMs=static_cast<double>(seed)+0.5;return d;
}
Table makeTable(const std::string& db,const std::string& name,uint64_t seed) {
    Table t;t.dbname=db;t.relname=name;t.seqScan=seed+1;t.seqTupRead=seed+2;t.idxScan=seed+3;t.idxTupFetch=seed+4;
    t.nTupIns=seed+5;t.nTupUpd=seed+6;t.nTupDel=seed+7;t.nLiveTup=static_cast<int64_t>(seed+80);t.liveTupEstimateValid=true;return t;
}
Sql makeSql(const std::string& db,const std::string& query,uint64_t calls,double total,double min,double max) {
    Sql row;row.dbname=db;row.sql=query;row.calls=calls;row.totalTimeMs=total;row.minTimeMs=min;row.maxTimeMs=max;
    row.meanTimeMs=total/static_cast<double>(calls);return row;
}
struct Scenario {
    bool sql;
    fs::path path,otherPath;
    std::string target="target_database_abcdefghijklmnopqrstuvwxyz",other="foreign_database_abcdefghijklmnopqrstuvwxyz";
    std::string baseQuery="SELECT value FROM alpha_table_abcdefghijklmnopqrstuvwxyz WHERE id = 1";
    std::string localQuery="SELECT value FROM beta_table_abcdefghijklmnopqrstuvwxyz WHERE id = 1";
    std::string peerQuery="SELECT value FROM gamma_table_abcdefghijklmnopqrstuvwxyz WHERE id = 1";
    Runtime initial,pending,peer,merged,foreignInitial,foreignPending;
    std::vector<Sql> initialSql,pendingSql,peerSql,mergedSql,foreignSql;
    std::string diskBefore,before,expected,foreignBefore;
    explicit Scenario(const fs::path& root,bool useSql):sql(useSql) {
        path=root/(sql?"sql_snapshot_abcdefghijklmnopqrstuvwxyz.bin":"runtime_snapshot_abcdefghijklmnopqrstuvwxyz.bin");
        otherPath=root/(sql?"other_sql.bin":"other_runtime.bin");
        initial.database=makeDb(target,10);initial.tables={makeTable(target,"alpha_resident_abcdefghijklmnopqrstuvwxyz",12),makeTable(target,"deleted_table_abcdefghijklmnopqrstuvwxyz",13)};
        pending=initial;++pending.database.queries;++pending.database.xactCommit;pending.database.tupReturned+=3;
        pending.database.tupFetched+=3;pending.database.totalQueryTimeMs+=2.5;pending.tables.pop_back();
        pending.tables[0].nTupIns+=4;pending.tables[0].nLiveTup+=4;
        Table local;local.dbname=target;local.relname="beta_local_abcdefghijklmnopqrstuvwxyz";
        local.seqScan=1;local.seqTupRead=37;local.nLiveTup=37;local.liveTupEstimateValid=true;pending.tables.push_back(local);
        peer=initial;peer.database.queries+=2;peer.database.xactCommit+=2;peer.database.tupReturned+=7;
        peer.database.tupFetched+=7;peer.database.totalQueryTimeMs+=8;peer.tables[0].seqScan+=3;peer.tables[0].seqTupRead+=8;
        peer.tables.push_back(makeTable(target,"gamma_peer_abcdefghijklmnopqrstuvwxyz",38));
        merged=pending;merged.database.queries+=2;merged.database.xactCommit+=2;merged.database.tupReturned+=7;
        merged.database.tupFetched+=7;merged.database.totalQueryTimeMs+=8;merged.tables[0].seqScan+=3;merged.tables[0].seqTupRead+=8;
        merged.tables.push_back(peer.tables.back());
        foreignInitial.database=makeDb(other,30);foreignInitial.tables={makeTable(other,"other_keep_abcdefghijklmnopqrstuvwxyz",31),makeTable(other,"other_deleted_abcdefghijklmnopqrstuvwxyz",32)};
        foreignPending=foreignInitial;foreignPending.tables.pop_back();
        initialSql={makeSql(target,baseQuery,2,4,2,2)};
        pendingSql={makeSql(target,baseQuery,3,8,2,4),makeSql(target,localQuery,1,1.25,1.25,1.25)};
        peerSql={makeSql(target,baseQuery,3,12,2,8),makeSql(target,peerQuery,1,5.5,5.5,5.5)};
        mergedSql={makeSql(target,baseQuery,4,16,2,8),pendingSql[1],peerSql[1]};
        foreignSql={makeSql(other,baseQuery,5,15,3,3)};
    }
    void seed() {
        dbms::resetRuntimeStats();dbms::resetSqlStats();require(dbms::setSqlStatsMaxEntries(5000),"SQL bound");
        if (sql) {
            writeFile(path,encodeSql(target,initialSql));writeFile(otherPath,encodeSql(other,foreignSql));
            require(dbms::loadSqlStats(target,path)&&dbms::loadSqlStats(other,otherPath),"SQL seed");
            dbms::recordSqlStat(baseQuery,4,target);dbms::recordSqlStat(localQuery,1.25,target);
            auto expectedRows=pendingSql;expectedRows.insert(expectedRows.end(),foreignSql.begin(),foreignSql.end());
            require(currentSql()==sqlState(expectedRows),"independent SQL seed model");
            auto mergedRows=mergedSql;mergedRows.insert(mergedRows.end(),foreignSql.begin(),foreignSql.end());expected=sqlState(mergedRows);
            writeFile(path,encodeSql(target,peerSql));before=currentSql();foreignBefore=currentRuntime();
        } else {
            writeFile(path,encode(initial));writeFile(otherPath,encode(foreignInitial));
            require(dbms::loadRuntimeStats(target,path)&&dbms::loadRuntimeStats(other,otherPath),"runtime seed");
            dbms::recordQueryExecution("COMMIT",2.5,target,true,3);
            dbms::recordTableMutation(target,initial.tables[0].relname,dbms::TableMutation::Insert,4);
            dbms::recordTableScan(target,pending.tables[1].relname,37,false,true);
            dbms::resetRuntimeTableStats(target,initial.tables[1].relname);
            dbms::resetRuntimeTableStats(other,foreignInitial.tables[1].relname);
            require(currentRuntime()==runtimeState(pending,foreignPending),"independent runtime seed model");
            expected=runtimeState(merged,foreignPending);writeFile(path,encode(peer));before=currentRuntime();foreignBefore=currentSql();
        }
        diskBefore=readFile(path);
    }
    bool persist() const { return sql?dbms::persistSqlStats(target,path):dbms::persistRuntimeStats(target,path); }
    std::string state() const { return sql?currentSql():currentRuntime(); }
    std::string expectedFile() const { return sql?encodeSql(target,mergedSql):encode(merged); }
    void arm(size_t position,Io io) const {
        struct stat status{};require(::stat(path.parent_path().c_str(),&status)==0,"directory identity");
        fault={true,false,0,position,0,0,io,0,0,path.c_str(),status.st_dev,status.st_ino};
    }
    void checkAfter(bool returned,bool threw,bool unexpected,size_t position,size_t trace,Io io,const std::string& label) const {
        const bool published=fault.published;const auto thrown=fault.thrown;
        check(!unexpected,label+" supported outcome");
        check(!published||!threw,label+" no throwing allocation after rename");
        check(!returned||published,label+" success published");
        if (io==Io::None) check(thrown==(position<trace?1u:0u),label+" selected real allocation observed");
        else {
            check(fault.ioTriggered>0,label+" real target IO fault observed");
            if (io==Io::InterruptedWrite||io==Io::ShortWrite)
                check(returned&&!threw&&published,label+" short/EINTR completes normally");
            if (io==Io::FileSync||io==Io::DirectorySync||io==Io::Rename||io==Io::Write||io==Io::ShortWriteError)
                check(!returned&&!threw,label+" hard IO failure reported");
        }
        check(readFile(path)==(published?expectedFile():diskBefore),label+" whole on-disk image");
        check(state()==(published?expected:before),label+" memory matches publication outcome");
        check((sql?currentRuntime():currentSql())==foreignBefore,label+" other statistics subsystem unchanged");
        check(!fs::exists(path.string()+".tmp."+std::to_string(static_cast<long long>(::getpid()))),label+" temporary cleaned");
        check(persist(),label+" healthy retry succeeds");
        check(readFile(path)==expectedFile()&&state()==expected,label+" retry applies delta exactly once");
        check(persist(),label+" repeated persist succeeds");
        check(readFile(path)==expectedFile()&&state()==expected,label+" repeated persist idempotent");
        const bool otherSaved=sql?dbms::persistSqlStats(other,otherPath):dbms::persistRuntimeStats(other,otherPath);
        check(otherSaved,label+" other database persist succeeds");
        check(readFile(otherPath)==(sql?encodeSql(other,foreignSql):encode(foreignPending)),label+" other database baseline/reset intact");
    }
};
}

extern "C" int rename(const char* from,const char* to) {
    const bool target=fault.armed&&fault.destination&&std::strcmp(to,fault.destination)==0;
    if (target&&fault.io==Io::Rename&&fault.ioTriggered==0) { ++fault.ioTriggered;errno=EIO;return -1; }
    const int result=static_cast<int>(::syscall(SYS_rename,from,to));
    if (target&&result==0) fault.published=true;
    return result;
}
extern "C" int fsync(int fd) {
    if (fault.armed&&fault.ioTriggered==0) {
        struct stat status{};const bool directory=::fstat(fd,&status)==0&&S_ISDIR(status.st_mode)&&
            status.st_dev==fault.directoryDevice&&status.st_ino==fault.directoryInode;
        if ((fault.io==Io::FileSync&&isTemporary(fd))||(fault.io==Io::DirectorySync&&directory)) {
            ++fault.ioTriggered;errno=EIO;return -1;
        }
    }
    return static_cast<int>(::syscall(SYS_fsync,fd));
}
extern "C" ssize_t write(int fd,const void* data,size_t size) {
    if (isTemporary(fd)) {
        if (fault.io==Io::Write&&fault.ioTriggered==0) { ++fault.ioTriggered;errno=EIO;return -1; }
        if (fault.io==Io::ShortWriteError) {
            if (fault.writeStage++==0) { ++fault.ioTriggered;return ::syscall(SYS_write,fd,data,std::min<size_t>(17,size)); }
            ++fault.ioTriggered;errno=EIO;return -1;
        }
        if (fault.io==Io::InterruptedWrite&&fault.ioTriggered==0) { ++fault.ioTriggered;errno=EINTR;return -1; }
        if (fault.io==Io::ShortWrite) { ++fault.ioTriggered;return ::syscall(SYS_write,fd,data,std::min<size_t>(17,size)); }
    }
    return ::syscall(SYS_write,fd,data,size);
}
extern "C" ssize_t writev(int fd,const struct iovec* vectors,int count) {
    if (isTemporary(fd)&&(fault.io==Io::Write||fault.io==Io::ShortWriteError||fault.io==Io::InterruptedWrite||fault.io==Io::ShortWrite)) {
        if (count<=0) return 0;
        return write(fd,vectors[0].iov_base,vectors[0].iov_len);
    }
    return ::syscall(SYS_writev,fd,vectors,count);
}

void* operator new(size_t n) { return allocate(n); }
void* operator new[](size_t n) { return allocate(n); }
void* operator new(size_t n,const std::nothrow_t&) noexcept { try{return allocate(n);}catch(...){return nullptr;} }
void* operator new[](size_t n,const std::nothrow_t&) noexcept { try{return allocate(n);}catch(...){return nullptr;} }
void* operator new(size_t n,std::align_val_t a) { return allocate(n,static_cast<size_t>(a)); }
void* operator new[](size_t n,std::align_val_t a) { return allocate(n,static_cast<size_t>(a)); }
void* operator new(size_t n,std::align_val_t a,const std::nothrow_t&) noexcept { try{return allocate(n,static_cast<size_t>(a));}catch(...){return nullptr;} }
void* operator new[](size_t n,std::align_val_t a,const std::nothrow_t&) noexcept { try{return allocate(n,static_cast<size_t>(a));}catch(...){return nullptr;} }
void operator delete(void* p) noexcept { deallocate(p); }
void operator delete[](void* p) noexcept { deallocate(p); }
void operator delete(void* p,size_t) noexcept { deallocate(p); }
void operator delete[](void* p,size_t) noexcept { deallocate(p); }
void operator delete(void* p,const std::nothrow_t&) noexcept { deallocate(p); }
void operator delete[](void* p,const std::nothrow_t&) noexcept { deallocate(p); }
void operator delete(void* p,std::align_val_t) noexcept { deallocate(p); }
void operator delete[](void* p,std::align_val_t) noexcept { deallocate(p); }
void operator delete(void* p,size_t,std::align_val_t) noexcept { deallocate(p); }
void operator delete[](void* p,size_t,std::align_val_t) noexcept { deallocate(p); }
void operator delete(void* p,std::align_val_t,const std::nothrow_t&) noexcept { deallocate(p); }
void operator delete[](void* p,std::align_val_t,const std::nothrow_t&) noexcept { deallocate(p); }


namespace {
struct WriterMarker { int value; };
void helperAndEmptyCases(const fs::path& root) {
    const auto path=root/"direct_helper_abcdefghijklmnopqrstuvwxyz.bin";
    writeFile(path,"original");bool threw=false;
    try {
        (void)dbms::publishStatsSnapshot(path,[&](const fs::path& temporary)->bool {
            writeFile(temporary,"partial");throw WriterMarker{91};
        });
    }catch (const WriterMarker& marker) { threw=marker.value==91; }
    check(threw,"writer exception propagates");
    check(readFile(path)=="original","writer exception retains original");
    const auto temporary=path.string()+".tmp."+std::to_string(static_cast<long long>(::getpid()));
    check(!fs::exists(temporary),"writer exception cleans temporary");
    check(!dbms::publishStatsSnapshot(path,[&](const fs::path& name){writeFile(name,"partial");return false;}),"writer false propagates");
    check(readFile(path)=="original"&&!fs::exists(temporary),"writer false cleans and retains original");
    check(dbms::publishStatsSnapshot(path,[&](const fs::path& name){writeFile(name,"complete");return true;}),"writer healthy retry");
    check(readFile(path)=="complete"&&!fs::exists(temporary),"writer healthy image");
    dbms::resetRuntimeStats();dbms::resetSqlStats();
    const std::string db="empty_database_abcdefghijklmnopqrstuvwxyz";
    const auto runtime=root/"empty_runtime.bin",sql=root/"empty_sql.bin";
    check(!dbms::persistRuntimeStats("",runtime)&&!dbms::persistSqlStats("",sql),"empty database rejects both APIs");
    check(!fs::exists(runtime)&&!fs::exists(sql),"empty database publishes no file");
    Runtime expected;expected.database.dbname=db;
    check(dbms::persistRuntimeStats(db,runtime),"new empty runtime persists");
    check(readFile(runtime)==encode(expected),"new empty runtime image");
    check(dbms::persistSqlStats(db,sql),"new empty SQL persists");
    check(readFile(sql)==encodeSql(db,{}),"new empty SQL image");
    check(dbms::persistRuntimeStats(db,runtime)&&dbms::persistSqlStats(db,sql),"empty repeated save succeeds");
    check(readFile(runtime)==encode(expected)&&readFile(sql)==encodeSql(db,{}),"empty repeated save idempotent");
    for (bool useSql:{false,true}) {
        Scenario data(root,useSql);
        for (int kind=0;kind<3;++kind) {
            data.seed();const std::string damaged=kind==0?"":kind==1?data.diskBefore.substr(0,data.diskBefore.size()-1):data.diskBefore+"tail";
            writeFile(data.path,damaged);
            check(!data.persist(),"damaged existing snapshot rejects");
            check(readFile(data.path)==damaged&&data.state()==data.before,"damaged snapshot rejection preserves bytes/state");
            writeFile(data.path,data.diskBefore);
            check(data.persist(),"damaged snapshot repair healthy retry");
            check(readFile(data.path)==data.expectedFile()&&data.state()==data.expected,"repair counts once");
        }
    }
}
}
int main() {
    char pattern[]="/tmp/dbms-stats-publication.XXXXXX";const char* created=::mkdtemp(pattern);
    if (!created) { std::cerr<<"SETUP_ERROR mkdtemp\n";return 2; }
    const fs::path root(created);
    try {
        helperAndEmptyCases(root);
        for (bool sql:{false,true}) {
            Scenario data(root,sql);data.seed();data.arm(noFailure,Io::None);bool traceSuccess=false;
            try { traceSuccess=data.persist(); } catch (...) { fault.armed=false;throw; }
            fault.armed=false;const size_t trace=fault.calls;
            check(traceSuccess,"trace persists");check(trace>0&&trace<=allocationCases,"all sites fit fixed matrix");
            check(fault.afterRenameAllocations==0,"no allocation after successful rename");
            std::cout<<"STATS_PUBLICATION_ALLOCATIONS "<<(sql?"sql":"runtime")<<' '<<trace
                     <<" AFTER_RENAME "<<fault.afterRenameAllocations<<'\n';
            for (size_t point=0;point<allocationCases;++point) {
                data.seed();const auto fds=descriptors();data.arm(point,Io::None);
                bool returned=false,threw=false,unexpected=false;
                try { returned=data.persist(); }catch (const std::bad_alloc&) { threw=true; }catch (...) { unexpected=true; }
                fault.armed=false;allocationTriggered+=fault.thrown;
                const std::string label=(sql?"SQL allocation ":"runtime allocation ")+std::to_string(point);
                check(descriptors()==fds,label+" descriptors balanced");
                data.checkAfter(returned,threw,unexpected,point,trace,Io::None,label);
            }
            for (Io io:{Io::FileSync,Io::DirectorySync,Io::Rename,Io::Write,Io::ShortWriteError,Io::InterruptedWrite,Io::ShortWrite}) {
                data.seed();const auto fds=descriptors();data.arm(noFailure,io);
                bool returned=false,threw=false,unexpected=false;
                try { returned=data.persist(); }catch (const std::bad_alloc&) { threw=true; }catch (...) { unexpected=true; }
                fault.armed=false;ioTriggered+=static_cast<size_t>(fault.ioTriggered);
                const std::string label=(sql?"SQL IO ":"runtime IO ")+std::to_string(static_cast<int>(io));
                check(descriptors()==fds,label+" descriptors balanced");
                data.checkAfter(returned,threw,unexpected,noFailure,trace,io,label);
            }
        }
    }catch (const std::exception& error) { fault.armed=false;++failures;std::cerr<<"SETUP_ERROR "<<error.what()<<'\n'; }
    dbms::resetRuntimeStats();dbms::resetSqlStats();std::error_code cleanup;fs::remove_all(root,cleanup);
    check(!cleanup,"private fixture cleanup");
    std::cout<<"STATS_PUBLICATION_COMPLETE "<<controls<<" FAILURES "<<failures<<" ALLOCATION_CASES "<<2*allocationCases
             <<" TRIGGERED "<<allocationTriggered<<" IO_TRIGGERED "<<ioTriggered<<'\n';
    return failures?1:0;
}
