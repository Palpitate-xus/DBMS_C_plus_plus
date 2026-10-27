// test_sources: src/process/RuntimeStats.cpp src/process/StatsPersistence.cpp
#include "process/RuntimeStats.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

namespace {
namespace fs = std::filesystem;
using Database = dbms::RuntimeDatabaseStats;
using Table = dbms::RuntimeTableStats;
struct Snapshot { Database database; std::vector<Table> tables; };
constexpr size_t noFailure = std::numeric_limits<size_t>::max();
constexpr size_t allocationCases = 512;
struct Fault { bool armed=false; size_t calls=0, selected=noFailure, thrown=0; };
thread_local Fault fault;
void beforeAllocation() {
    if (!fault.armed) return;
    const size_t ordinal=fault.calls++;
    if (ordinal==fault.selected) {
        fault.selected=noFailure;
        ++fault.thrown;
        throw std::bad_alloc();
    }
}
[[gnu::noinline]] void* allocate(size_t bytes,size_t alignment=0) {
    beforeAllocation();
    void* result=nullptr;
    if (alignment) {
        if (::posix_memalign(&result,alignment,std::max<size_t>(bytes,1))!=0) result=nullptr;
    } else result=std::malloc(std::max<size_t>(bytes,1));
    if (!result) throw std::bad_alloc();
    return result;
}
[[gnu::noinline]] void deallocate(void* pointer) noexcept { std::free(pointer); }
size_t controls=0, failures=0, triggeredSites=0;
void check(bool good,const std::string& label) {
    ++controls;
    if (!good) { ++failures; std::cerr<<"FAIL "<<label<<'\n'; }
}
void require(bool good,const char* message) { if (!good) throw std::runtime_error(message); }
template<typename T> void scalar(std::string& out,T value) {
    out.append(reinterpret_cast<const char*>(&value),sizeof(value));
}
void text(std::string& out,const std::string& value) {
    scalar(out,static_cast<uint32_t>(value.size())); out+=value;
}
void databaseBytes(std::string& out,const Database& value) {
    text(out,value.dbname);scalar(out,value.queries);scalar(out,value.failedQueries);
    scalar(out,value.xactCommit);scalar(out,value.xactRollback);scalar(out,value.tupReturned);
    scalar(out,value.tupFetched);scalar(out,value.totalQueryTimeMs);
}
void tableBytes(std::string& out,const Table& value) {
    text(out,value.dbname);text(out,value.relname);scalar(out,value.seqScan);scalar(out,value.seqTupRead);
    scalar(out,value.idxScan);scalar(out,value.idxTupFetch);scalar(out,value.nTupIns);scalar(out,value.nTupUpd);
    scalar(out,value.nTupDel);scalar(out,value.nLiveTup);scalar(out,static_cast<uint8_t>(value.liveTupEstimateValid));
}
std::string encode(Snapshot value) {
    std::sort(value.tables.begin(),value.tables.end(),[](const Table& a,const Table& b){return a.relname<b.relname;});
    std::string out;text(out,"DBMS_RUNTIME_STATS_V1");scalar(out,uint32_t{1});databaseBytes(out,value.database);
    scalar(out,static_cast<uint64_t>(value.tables.size()));for (const auto& row:value.tables) tableBytes(out,row);
    return out;
}
std::string fingerprint(std::vector<Database> databases,std::vector<Table> tables) {
    std::sort(databases.begin(),databases.end(),[](const Database& a,const Database& b){return a.dbname<b.dbname;});
    std::sort(tables.begin(),tables.end(),[](const Table& a,const Table& b){
        return a.dbname==b.dbname?a.relname<b.relname:a.dbname<b.dbname;
    });
    std::string out;scalar(out,static_cast<uint64_t>(databases.size()));
    for (const auto& row:databases) databaseBytes(out,row);
    scalar(out,static_cast<uint64_t>(tables.size()));for (const auto& row:tables) tableBytes(out,row);
    return out;
}
std::string fingerprint(const Snapshot& target,const Snapshot& foreign) {
    auto rows=target.tables;rows.insert(rows.end(),foreign.tables.begin(),foreign.tables.end());
    return fingerprint({target.database,foreign.database},rows);
}
std::string current() { return fingerprint(dbms::getRuntimeDatabaseStats(),dbms::getRuntimeTableStats()); }
void writeFile(const fs::path& path,const std::string& data) {
    std::ofstream out(path,std::ios::binary|std::ios::trunc);
    out.write(data.data(),static_cast<std::streamsize>(data.size()));out.close();require(!out.fail(),"fixture write");
}
std::string readFile(const fs::path& path) {
    std::ifstream in(path,std::ios::binary);require(in.good(),"fixture read");
    std::string result{std::istreambuf_iterator<char>(in),std::istreambuf_iterator<char>()};
    require(!in.bad(),"fixture read error");return result;
}
size_t descriptors() {
    size_t count=0;for (const auto& item:fs::directory_iterator("/proc/self/fd")) { (void)item;++count; }
    return count;
}
Database makeDatabase(const std::string& name,uint64_t seed) {
    Database value;value.dbname=name;value.queries=seed+100;value.failedQueries=seed+1;
    value.xactCommit=seed+2;value.xactRollback=seed+3;value.tupReturned=seed+4;
    value.tupFetched=seed+5;value.totalQueryTimeMs=static_cast<double>(seed)+0.5;return value;
}
Table makeTable(const std::string& database,const std::string& name,uint64_t seed) {
    Table value;value.dbname=database;value.relname=name;value.seqScan=seed+1;value.seqTupRead=seed+2;
    value.idxScan=seed+3;value.idxTupFetch=seed+4;value.nTupIns=seed+5;value.nTupUpd=seed+6;
    value.nTupDel=seed+7;value.nLiveTup=static_cast<int64_t>(seed+80);value.liveTupEstimateValid=true;return value;
}
struct Fixture {
    fs::path base,foreignPath,incomingPath;
    Snapshot initial,pending,foreignInitial,foreignPending,incoming;
    std::string retired="retired_before_reload_absent_from_snapshot_abcdefghijklmnopqrstuvwxyz";
    std::string sourceBytes;
    explicit Fixture(const fs::path& root,int profile=0) {
        base=root/"baseline.bin";foreignPath=root/"foreign.bin";incomingPath=root/"incoming.bin";
        const std::string db="Target_database_abcdefghijklmnopqrstuvwxyz_"+std::to_string(profile);
        initial.database=makeDatabase(db,10);
        initial.tables={makeTable(db,"resident_long_table_abcdefghijklmnopqrstuvwxyz",12),
                        makeTable(db,"stale_disk_table_abcdefghijklmnopqrstuvwxyz",13),
                        makeTable(db,"removed_existing_table_abcdefghijklmnopqrstuvwxyz",14)};
        foreignInitial.database=makeDatabase("Other_database_abcdefghijklmnopqrstuvwxyz",30);
        foreignInitial.tables={makeTable(foreignInitial.database.dbname,"keep_foreign_table_abcdefghijklmnopqrstuvwxyz",31),
                               makeTable(foreignInitial.database.dbname,"drop_foreign_table_abcdefghijklmnopqrstuvwxyz",32)};
        pending=initial;++pending.database.queries;++pending.database.xactCommit;
        pending.database.tupReturned+=3;pending.database.tupFetched+=3;pending.database.totalQueryTimeMs+=2.5;
        pending.tables.pop_back();pending.tables[0].nTupIns+=4;pending.tables[0].nLiveTup+=4;
        Table local;local.dbname=db;local.relname="local_only_table_abcdefghijklmnopqrstuvwxyz";
        local.seqScan=1;local.seqTupRead=37;local.nLiveTup=37;local.liveTupEstimateValid=true;
        pending.tables.push_back(local);
        foreignPending=foreignInitial;foreignPending.tables.pop_back();
        incoming.database=makeDatabase(db,100);
        incoming.tables={makeTable(db,"resident_long_table_abcdefghijklmnopqrstuvwxyz",101),
                         makeTable(db,"new_table_one_abcdefghijklmnopqrstuvwxyz",102),
                         makeTable(db,"new_table_two_中文_abcdefghijklmnopqrstuvwxyz",103),
                         makeTable(db,"removed_existing_table_abcdefghijklmnopqrstuvwxyz",104)};
        if (profile==1) incoming.tables.clear();
        if (profile==2) incoming.tables={makeTable(db,"disjoint_新表_abcdefghijklmnopqrstuvwxyz",201)};
        if (profile==3) { incoming.tables[1].liveTupEstimateValid=false;incoming.tables[1].nLiveTup=0; }
        sourceBytes=encode(incoming);
    }
    void seed() const {
        dbms::resetRuntimeStats();writeFile(base,encode(initial));writeFile(foreignPath,encode(foreignInitial));
        writeFile(incomingPath,sourceBytes);
        require(dbms::loadRuntimeStats(initial.database.dbname,base),"initial load");
        require(dbms::loadRuntimeStats(foreignInitial.database.dbname,foreignPath),"foreign load");
        dbms::recordQueryExecution("COMMIT",2.5,initial.database.dbname,true,3);
        dbms::recordTableMutation(initial.database.dbname,initial.tables[0].relname,dbms::TableMutation::Insert,4);
        dbms::recordTableScan(initial.database.dbname,"local_only_table_abcdefghijklmnopqrstuvwxyz",37,false,true);
        dbms::resetRuntimeTableStats(initial.database.dbname,initial.tables[2].relname);
        dbms::resetRuntimeTableStats(initial.database.dbname,retired);
        dbms::resetRuntimeTableStats(foreignInitial.database.dbname,foreignInitial.tables[1].relname);
        require(current()==fingerprint(pending,foreignPending),"independent seed model");
    }
    void checkpoints(bool loaded,const std::string& label) const {
        const auto& target=loaded?incoming:pending;
        const auto path=loaded?incomingPath:base;
        const bool targetSaved=dbms::persistRuntimeStats(initial.database.dbname,path);
        check(targetSaved,label+" target checkpoint succeeds");
        check(readFile(path)==encode(target),label+" target private baseline and complete counters");
        const bool foreignSaved=dbms::persistRuntimeStats(foreignInitial.database.dbname,foreignPath);
        check(foreignSaved,label+" foreign checkpoint succeeds");
        check(readFile(foreignPath)==encode(foreignPending),label+" foreign baseline and reset marker retained");
        check(current()==fingerprint(target,foreignPending),label+" checkpoint public state exact");
    }
};
void healthyProfiles(const fs::path& root) {
    for (int profile=0;profile<4;++profile) {
        Fixture data(root,profile);data.seed();const std::string label="profile "+std::to_string(profile);
        check(dbms::loadRuntimeStats(data.initial.database.dbname,data.incomingPath),label+" load succeeds");
        check(current()==fingerprint(data.incoming,data.foreignPending),label+" replaces full target only");
        check(dbms::loadRuntimeStats(data.initial.database.dbname,data.incomingPath),label+" repeated load succeeds");
        check(current()==fingerprint(data.incoming,data.foreignPending),label+" repeated load idempotent");
        check(readFile(data.incomingPath)==data.sourceBytes,label+" load does not rewrite source");
        uint64_t rows=0;
        check(!dbms::getRuntimeLiveRowEstimate(data.initial.database.dbname,data.initial.tables[1].relname,rows),label+" no stale planner estimate");
        data.checkpoints(true,label);
        auto peer=data.incoming;peer.tables.push_back(makeTable(peer.database.dbname,data.retired,300));
        writeFile(data.incomingPath,encode(peer));
        check(dbms::persistRuntimeStats(peer.database.dbname,data.incomingPath),label+" peer-only table persist");
        check(readFile(data.incomingPath)==encode(peer),label+" old absent-table tombstone discarded");
        dbms::resetRuntimeStats();
        check(dbms::loadRuntimeStats(peer.database.dbname,data.incomingPath),label+" memory-reset reread");
        check(current()==fingerprint({peer.database},peer.tables),label+" memory-reset full values");
    }
}
void rejectedInputs(const fs::path& root) {
    Fixture data(root);
    std::vector<std::string> contents={"",data.sourceBytes.substr(0,8),data.sourceBytes.substr(0,data.sourceBytes.size()-1),data.sourceBytes+"tail"};
    auto wrong=data.incoming;wrong.database.dbname="wrong_database";contents.push_back(encode(wrong));
    for (size_t i=0;i<contents.size();++i) {
        data.seed();writeFile(data.incomingPath,contents[i]);const auto before=current();
        const std::string label="rejected "+std::to_string(i);
        check(!dbms::loadRuntimeStats(data.initial.database.dbname,data.incomingPath),label+" false");
        check(current()==before,label+" complete state unchanged");
        check(readFile(data.incomingPath)==contents[i],label+" source unchanged");
        data.checkpoints(false,label);
    }
    for (int kind=0;kind<5;++kind) {
        data.seed();fs::path path=root/("unavailable_"+std::to_string(kind));
        if (kind==0) fs::create_symlink(path.filename(),path);
        if (kind==1) path=root/std::string(300,'x');
        if (kind==2) fs::create_directory(path.string()+".lock");
        if (kind==2) writeFile(path,data.sourceBytes);
        if (kind==3) fs::create_directory(path);
        const auto before=current();const auto fdBefore=descriptors();
        std::error_code ec;(void)fs::exists(path,ec);
        if (kind<2) check(static_cast<bool>(ec),"actual OS status error");
        const bool loaded=dbms::loadRuntimeStats(data.initial.database.dbname,path);
        check(loaded==(kind==4),"unavailable path contract "+std::to_string(kind));
        check(current()==before,"unavailable path unchanged "+std::to_string(kind));
        check(descriptors()==fdBefore,"unavailable path descriptors "+std::to_string(kind));
        data.checkpoints(false,"unavailable "+std::to_string(kind));
    }
    data.seed();const auto before=current();
    check(!dbms::loadRuntimeStats("",data.incomingPath),"empty database rejects");
    check(current()==before,"empty database preserves all state");
}
void allocationMatrix(const fs::path& root) {
    Fixture data(root);data.seed();fault={true,0,noFailure,0};
    bool traced=false;
    try { traced=dbms::loadRuntimeStats(data.initial.database.dbname,data.incomingPath); }
    catch (...) { fault.armed=false;throw; }
    fault.armed=false;const size_t tracedSites=fault.calls;
    check(traced,"allocation trace load succeeds");
    check(tracedSites>0&&tracedSites<=allocationCases,"all observed allocations fit fixed exhaustive matrix");
    std::cout<<"RUNTIME_STATS_LOAD_ALLOCATION_SITES "<<tracedSites<<'\n';
    for (size_t selected=0;selected<allocationCases;++selected) {
        data.seed();const auto before=current();const auto fdBefore=descriptors();
        bool loaded=false,threw=false,unexpected=false;
        fault={true,0,selected,0};
        try { loaded=dbms::loadRuntimeStats(data.initial.database.dbname,data.incomingPath); }
        catch (const std::bad_alloc&) { threw=true; }
        catch (...) { unexpected=true; }
        fault.armed=false;triggeredSites+=fault.thrown;
        const std::string label="allocation "+std::to_string(selected);
        check(!unexpected&&!(loaded&&threw),label+" result contract");
        check(fault.thrown==(selected<tracedSites?1u:0u),label+" actual selected allocation observed");
        check(current()==(loaded?fingerprint(data.incoming,data.foreignPending):before),label+" all-or-nothing full state");
        check(readFile(data.incomingPath)==data.sourceBytes,label+" input unchanged");
        check(descriptors()==fdBefore,label+" no descriptor leak");
        data.checkpoints(loaded,label);
        check(dbms::loadRuntimeStats(data.initial.database.dbname,data.incomingPath),label+" healthy retry succeeds");
        check(current()==fingerprint(data.incoming,data.foreignPending),label+" healthy retry complete");
    }
    check(triggeredSites==tracedSites,"every actual traced allocation failed exactly once");
}
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

int main() {
    char pattern[]="/tmp/dbms-runtime-stats-load.XXXXXX";const char* created=::mkdtemp(pattern);
    if (!created) { std::cerr<<"SETUP_ERROR mkdtemp\n";return 2; }
    const fs::path root(created);
    try { healthyProfiles(root);rejectedInputs(root);allocationMatrix(root); }
    catch (const std::exception& error) { fault.armed=false;++failures;std::cerr<<"SETUP_ERROR "<<error.what()<<'\n'; }
    dbms::resetRuntimeStats();std::error_code cleanupError;fs::remove_all(root,cleanupError);
    check(!cleanupError,"private fixture cleanup");
    std::cout<<"RUNTIME_STATS_LOAD_COMPLETE "<<controls<<" FAILURES "<<failures
             <<" ALLOCATION_CASES "<<allocationCases<<" TRIGGERED "<<triggeredSites<<'\n';
    return failures?1:0;
}
