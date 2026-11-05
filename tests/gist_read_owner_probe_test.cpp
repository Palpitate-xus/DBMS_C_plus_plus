// test_link_flags: -Wl,--wrap=fstat -Wl,--wrap=_ZN4dbms13PageAllocator9fetchPageEj -Wl,--wrap=_ZNK4dbms14TxnIdGenerator16maxCommittedTxIdEv
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "commands/StoredRowNull.h"
#include "storage/HeapTupleHeader.h"
#include "transaction/TxnIdGenerator.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <fstream>
#include <future>
#include <iostream>
#include <iomanip>
#include <map>
#include <set>
#include <sys/stat.h>
#include <thread>

extern dbms::StorageEngine g_engine;
namespace {
thread_local bool watching=false,injectAllocation=false;
thread_local dev_t heapDevice=0;
thread_local ino_t heapInode=0;
thread_local size_t heapChecks=0,heapPages=0,fenceCalls=0;
thread_local dbms::PageAllocator* watchedAllocator=nullptr;
thread_local uint64_t allocatedGap=0;
size_t checks=0;
void need(bool pass,const std::string& label){++checks;if(!pass)throw std::runtime_error(label);}
using Cells=std::map<int,std::pair<std::optional<std::string>,std::optional<std::string>>>;
std::string bytes(const std::filesystem::path& path){std::ifstream in(path,std::ios::binary);need(bool(in),"owned image exists");return{std::istreambuf_iterator<char>(in),{}};}
void write(const std::filesystem::path& path,const std::string& value){std::ofstream out(path,std::ios::binary|std::ios::trunc);out.write(value.data(),value.size());out.close();need(!out.fail(),"complete owned image replacement");}
} // namespace

extern "C" int __real_fstat(int,struct stat*);
extern "C" int __wrap_fstat(int fd,struct stat* value) {
    const int result=__real_fstat(fd,value);
    if(watching&&result==0&&value->st_dev==heapDevice&&value->st_ino==heapInode)++heapChecks;
    return result;
}
extern "C" char* realFetch(dbms::PageAllocator*,uint32_t)
    asm("__real__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator*,uint32_t)
    asm("__wrap__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator* allocator,uint32_t page) {
    if(watching&&allocator==watchedAllocator)++heapPages;
    return realFetch(allocator,page);
}
extern "C" uint64_t realMax(const dbms::TxnIdGenerator*)
    asm("__real__ZNK4dbms14TxnIdGenerator16maxCommittedTxIdEv");
extern "C" uint64_t wrapMax(const dbms::TxnIdGenerator* generator)
    asm("__wrap__ZNK4dbms14TxnIdGenerator16maxCommittedTxIdEv");
extern "C" uint64_t wrapMax(const dbms::TxnIdGenerator* generator) {
    if(watching&&++fenceCalls==2&&injectAllocation) {
        // A real durable allocation, never a counter rewind or fake epoch.
        // The gap must invalidate the already selected current-read fence.
        allocatedGap=dbms::TxnIdGenerator::instance().nextTxId();
    }
    return realMax(generator);
}

int main() try {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();Session session;session.username="testuser";session.permission=1;
    session.currentDB=testDbPath("gist_read_owner_probe");auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* previous;~Restore(){watching=false;if(g_engine.inTransaction())g_engine.rollbackTransaction();setCurrentSession(previous);}} restore{previous};
    const auto db=session.currentDB;const std::string table="gist_probe";
    need(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"actual database");DdlExecutor ddl;
    need(!ddl.executeSql("CREATE TABLE gist_probe(id integer PRIMARY KEY,k text,pad text)",session),"actual typed heap");
    Cells population;
    need(g_engine.beginTransaction(db)==DBStatus::OK,"real seed transaction");
    for(int id=1;id<=1024;++id) {
        std::optional<std::string> key;
        if(id==1)key="";
        else if(id!=2&&id%29!=0)key=id==3?"NULL":id==1024?"needle_界":"bulk\n|;,"+std::to_string(id);
        std::optional<std::string> pad=id%17?std::optional<std::string>("payload 界\n"+std::to_string(id)):std::nullopt;
        population.emplace(id,std::make_pair(key,pad));
        need(g_engine.insertRow(db,table,StorageEngine::SqlRow{{"id",std::to_string(id)},{"k",key},{"pad",pad}})==DBStatus::OK,"complete real row insert");
    }
    need(g_engine.commitTransaction()==DBStatus::OK,"seed commit");
    need(!ddl.executeSql("CREATE INDEX gist_probe_index ON gist_probe USING gist(k)",session),"actual current GiST sidecar");
    const auto schema=g_engine.getTableSchema(db,table);
    const auto heap=g_engine.dbPath(db)/(table+".dt"),index=g_engine.dbPath(db)/(table+"_k.gist");
    const auto originalHeap=bytes(heap),originalIndex=bytes(index);
    struct stat identity{};need(::stat(heap.c_str(),&identity)==0,"real heap inode");heapDevice=identity.st_dev;heapInode=identity.st_ino;
    const auto rows=[&](const std::vector<int64_t>& rids) {
        Cells actual;
        for(const auto rid:rids) {
            std::string row;bool failed=false;
            const bool found=g_engine.readIndexedRowByRid(db,table,rid,row,schema,nullptr,&failed);
            need(!failed&&found,"every candidate is a visible complete tuple");
            const auto bitmap=detail::storedRowNullBitmap(g_engine,db,table,schema,rid);
            need(bitmap&&bitmap->size()==schema.len,"actual physical bitmap belongs to candidate RID");
            const int id=std::stoi(g_engine.extractColumnValue(row,schema,0,db));
            const auto cell=[&](size_t column)->std::optional<std::string> {
                if((*bitmap)[column])return {};
                return g_engine.extractColumnValue(row,schema,column,db);
            };
            need(actual.emplace(id,std::make_pair(cell(1),cell(2))).second,"each physical candidate once");
        }
        return actual;
    };
    const auto probe=[&](bool contained,const std::string& low,const std::string& high,
                         const Cells& wanted,const std::string& label,bool inject=false) {
        watchedAllocator=g_engine.getPageAllocator(db,table);need(watchedAllocator!=nullptr,"actual heap owner for physical probe observation");
        heapChecks=0;heapPages=0;fenceCalls=0;allocatedGap=0;injectAllocation=inject;watching=true;
        std::vector<int64_t> result;
        try { result=contained?g_engine.giSTSearchContainedBy(db,table,"k",low,high):
                              g_engine.giSTSearchOverlap(db,table,"k",low,high); }
        catch(...){watching=false;injectAllocation=false;throw;}
        watching=false;injectAllocation=false;const auto count=heapPages;
        std::cout<<"GIST_PHYSICAL_PROBE "<<label<<" heap_fd_stat_checks="<<heapChecks<<" heap_page_fetches="<<heapPages
            <<" fence_reads="<<fenceCalls<<" durable_gap="<<allocatedGap<<'\n';
        const auto actual=rows(result);
        if(actual!=wanted) {
            for(const auto& value:actual)std::cerr<<"ACTUAL id="<<value.first<<" key="<<std::quoted(value.second.first.value_or("<NULL>"))<<" pad="<<std::quoted(value.second.second.value_or("<NULL>"))<<'\n';
            for(const auto& value:wanted)std::cerr<<"WANTED id="<<value.first<<" key="<<std::quoted(value.second.first.value_or("<NULL>"))<<" pad="<<std::quoted(value.second.second.value_or("<NULL>"))<<'\n';
        }
        need(actual==wanted,"complete values/NULL/visible rows: "+label);
        return count;
    };
    const Cells needle{{1024,population.at(1024)}};
    const auto begin=[&](IsolationLevel isolation) {
        need(g_engine.setIsolationLevel(isolation),"select isolation");
        need(g_engine.beginTransaction(db)==DBStatus::OK,"actual read owner");
        need(g_engine.beginSqlCommand(),"actual command owner");g_engine.noteQuerySnapshot();
    };
    const auto end=[&] {need(g_engine.finishSqlCommand(),"finish command");need(g_engine.commitTransaction()==DBStatus::OK,"finish owner");};
    const auto noOwner=probe(false,"needle","needlf",needle,"no-owner overlap");
    begin(IsolationLevel::READ_COMMITTED);
    const auto current=probe(false,"needle","needlf",needle,"fresh RC overlap");
    need(current<=noOwner,"fresh indexed probe avoids whole-heap page fetches");
    need(probe(true,"needle","needlf",needle,"fresh RC contained")<=noOwner,"contained probe avoids full heap");end();
    begin(IsolationLevel::SERIALIZABLE);
    need(probe(false,"needle","needlf",needle,"serializable matching predicate")>noOwner,
         "serializable reader retains existing heap SIREAD registration");
    need(probe(true,"absent","absenu",Cells{},"serializable empty predicate")>noOwner,
         "empty serializable answer still retains relation SIREAD");
    end();
    begin(IsolationLevel::REPEATABLE_READ);
    need(probe(false,"needle","needlf",needle,"initial RR current")<=noOwner,"initial isolated RR is a current snapshot");
    need(g_engine.finishSqlCommand(),"finish initial RR command");
    std::exception_ptr writerError;
    std::thread writer([&]{try {
        need(g_engine.beginTransaction(db)==DBStatus::OK,"peer real transaction");
        need(g_engine.update(db,table,{{"k","new_界"}},{"=id 1024"})==DBStatus::OK,"peer changes actual indexed key");
        need(g_engine.updateRows(db,table,StorageEngine::SqlRow{{"k",std::nullopt}},{"=id 1"})==DBStatus::OK,"peer changes old empty to SQL NULL");
        need(g_engine.updateRows(db,table,StorageEngine::SqlRow{{"k",std::string{}}},{"=id 2"})==DBStatus::OK,"peer changes old SQL NULL to empty");
        need(g_engine.commitTransaction()==DBStatus::OK,"peer actual commit");
    }catch(...){writerError=std::current_exception();}});writer.join();if(writerError)std::rethrow_exception(writerError);
    need(g_engine.beginSqlCommand(),"next RR command");
    need(probe(false,"needle","needlf",needle,"historical RR")>noOwner,"historical snapshot retains heap fallback");
    probe(false,"","\x01",Cells{{1,population.at(1)}},"historical visible empty/NULL bitmap");end();
    population[1].first=std::nullopt;population[2].first=std::string{};
    population[1024].first="new_界";const Cells changed{{1024,population.at(1024)}};
    need(probe(false,"new_","new`",changed,"post-commit current")<=noOwner,"current index has newly committed key");
    begin(IsolationLevel::READ_COMMITTED);
    need(g_engine.update(db,table,{{"k","own_界"}},{"=id 1024"})==DBStatus::OK,"own real indexed update");
    need(g_engine.finishSqlCommand()&&g_engine.beginSqlCommand(),"own-write next command");
    auto own=changed;own[1024].first="own_界";
    need(probe(false,"own_","own`",own,"own writes")>noOwner,"own-write path retains heap");
    need(g_engine.finishSqlCommand()&&g_engine.rollbackTransaction()==DBStatus::OK,"own-write rollback");
    begin(IsolationLevel::READ_COMMITTED);
    need(probe(false,"new_","new`",changed,"allocation changed during probe",true)>noOwner,"allocation race discards current-index completeness");
    need(allocatedGap!=0,"real durable allocation seam reached");end();
    // Another backend, even a read-only owner, keeps the conservative path.
    // Always release and join it before propagating a main-thread failure.
    std::promise<void> active, release;
    auto released=release.get_future();
    std::exception_ptr overlapError, mainError;
    std::thread overlapping([&] {
        try {
            need(g_engine.beginTransaction(db)==DBStatus::OK,"other backend actual owner");
            active.set_value();
            released.wait();
            need(g_engine.rollbackTransaction()==DBStatus::OK,"other backend rollback");
        } catch(...) {
            overlapError=std::current_exception();
            try { active.set_exception(overlapError); } catch(...) {}
        }
    });
    try {
        active.get_future().get();
        begin(IsolationLevel::READ_COMMITTED);
        need(probe(false,"new_","new`",changed,"other active backend")>noOwner,
             "overlapping owner retains visible-heap path");
        end();
    } catch(...) { mainError=std::current_exception(); }
    release.set_value();overlapping.join();
    if(mainError)std::rethrow_exception(mainError);
    if(overlapError)std::rethrow_exception(overlapError);
    const auto committedIndex=bytes(index);
    auto corrupt=committedIndex;corrupt.back()^=1;write(index,corrupt);
    need(probe(false,"new_","new`",changed,"bad checksum")>noOwner,"corrupt index retains heap recovery");write(index,committedIndex);
    std::filesystem::remove(index);
    need(probe(false,"new_","new`",changed,"missing index")>noOwner,"missing file cannot prove an empty answer");write(index,committedIndex);
    Cells empty{{2,population.at(2)}};
    need(probe(false,"","\x01",empty,"empty text overlap")>noOwner,"V2 omitted empty keys require visible heap");
    need(probe(true,"","\x01",empty,"empty text contained")>noOwner,"contained query keeps empty distinct from NULL");
    // Healthy index reads must preserve every real physical byte. Updates
    // above legitimately changed the heap/index; freeze the current image.
    const auto frozenHeap=bytes(heap),frozenIndex=bytes(index);
    for(int i=0;i<8;++i)probe(false,"new_","new`",changed,"repeated current");
    need(bytes(heap)==frozenHeap&&bytes(index)==frozenIndex,"read probes preserve whole heap and index bytes");
    need(!originalHeap.empty()&&!originalIndex.empty(),"original real images retained during controls");
    std::cout<<"GIST_READ_OWNER checks="<<checks<<" failures=0\n";return 0;
}catch(const std::exception& error) {
    watching=false;if(g_engine.inTransaction())g_engine.rollbackTransaction();
    std::cerr<<"GIST_READ_OWNER_FAIL checks="<<checks<<' '<<error.what()<<'\n';return 1;
}
