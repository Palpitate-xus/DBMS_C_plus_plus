// Index builders borrow only their own live tuple bitmap.
// test_link_flags: -Wl,--wrap=fstat -Wl,--wrap=_ZN4dbms13PageAllocator9fetchPageEj
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "commands/StoredRowNull.h"
#include "access/BPTree.h"
#include "access/HashIndex.h"
#include "access/BloomIndex.h"
#include "common/Config.h"
#include "common/DbError.h"
#include "storage/HeapTupleHeader.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <algorithm>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <sys/stat.h>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
using namespace dbms;
size_t checks=0,heapChecks=0,heapFetches=0;
bool watching=false;
dev_t device=0;ino_t inode=0;
PageAllocator* allocator=nullptr;
void need(bool pass,const std::string& label){++checks;if(!pass)throw std::runtime_error(label);}
using Cells=std::map<int,StorageEngine::SqlRow>;
const std::string db=testDbPath("index_build_scan_owner"),table="items";
Cells model;
std::map<int,int64_t> inspect() {
    const auto schema=g_engine.getTableSchema(db,table);
    std::map<int,int64_t> result;
    need(g_engine.forEachRow(db,table,[&](uint32_t page,uint16_t slot,const char* data,size_t size){
        const std::string row(data,size);const auto rid=StorageEngine::encodeRid(page,slot);
        const int id=std::stoi(g_engine.extractColumnValue(row,schema,0,db));
        need(result.emplace(id,rid).second&&model.count(id),"every logical row once");
        const auto nulls=detail::storedRowNullBitmap(g_engine,db,table,schema,rid);
        need(nulls&&nulls->size()==schema.len,"full physical bitmap");
        for(size_t i=0;i<schema.len;++i) {
            const auto& wanted=model.at(id).at(schema.cols[i].dataName);
            need((*nulls)[i]==!wanted.has_value(),"complete independent SQL NULL map");
            bool isNull=false;auto value=g_engine.extractColumnValue(row,schema,i,db,true,&isNull);
            need(isNull==!wanted.has_value(),"decoder SQL NULL flag");
            if(wanted&&schema.cols[i].dataName=="location") {
                value.erase(std::remove_if(value.begin(),value.end(),[](char c){return c=='('||c==')'||c==' ';}),value.end());
                const auto comma=value.find(',');need(comma!=std::string::npos&&std::stod(value.substr(0,comma))==id&&std::stod(value.substr(comma+1))==id*2,"full point coordinates");
            } else need(value==wanted.value_or(""),"complete original bytes, empty text and literal NULL");
        }
    }),"whole real heap scan");
    need(result.size()==model.size(),"no omitted logical row");return result;
}
void probes(const std::map<int,int64_t>& rids) {
    const int id=19;const auto rid=rids.at(id);
    auto* hash=g_engine.getHashIndex(db,table,"body");auto* bloom=g_engine.getBloomIndex(db,table,"body");
    need(hash&&bloom,"actual persisted Hash/Bloom loaders");
    need(hash->search(*model.at(id).at("body"))==std::vector<int64_t>{rid},"Hash exact key/RID");
    need(bloom->search(*model.at(id).at("body"))==std::vector<int64_t>{rid},"Bloom exact key/RID");
    need(hash->search("")==std::vector<int64_t>{rids.at(2)},"Hash includes genuine empty text");
    need(g_engine.ginSearch(db,table,"body","tok19")==std::vector<int64_t>{rid},"GIN exact token/RID");
    need(g_engine.fullTextSearch(db,table,"body","tok19")==std::vector<int64_t>{rid},"full text exact token/RID");
    need(g_engine.giSTSearchOverlap(db,table,"score","190","190")==std::vector<int64_t>{rid},"GiST exact scalar/RID");
    need(g_engine.spGiSTSearch(db,table,"location","=","19,38")==std::vector<int64_t>{rid},"SP-GiST exact point/RID");
    need(g_engine.getSecondaryIndex(db,table,"body")->searchMulti(*model.at(id).at("body"))==std::vector<int64_t>{rid},"secondary Btree exact key/RID");
}
void measured(const std::string& name,const std::function<DBStatus()>& operation) {
    allocator=g_engine.getPageAllocator(db,table);need(allocator!=nullptr,"owned heap allocator");
    struct stat statbuf{};const auto path=g_engine.dbPath(db)/(table+".dt");
    need(::stat(path.c_str(),&statbuf)==0,"actual heap identity");device=statbuf.st_dev;inode=statbuf.st_ino;
    const auto pages=allocator->numPages();heapChecks=0;heapFetches=0;watching=true;
    DBStatus status;
    try{status=operation();}catch(...){watching=false;throw;}
    watching=false;
    std::cout<<"BUILD_OWNER_PROBE "<<name<<" rows="<<model.size()<<" physical_pages="<<pages<<" heap_fd_stat_checks="<<heapChecks<<" heap_page_fetches="<<heapFetches<<'\n';
    need(status==DBStatus::OK,"actual builder status: "+name);
    need(heapFetches<=pages*2+8,"builder avoids a per-column heap lookup: "+name);
    need(heapChecks<=pages*2+16,"builder avoids a per-column file identity lookup: "+name);
}
} // namespace
extern "C" int __real_fstat(int,struct stat*);
extern "C" int __wrap_fstat(int fd,struct stat* value) {
    const int code=__real_fstat(fd,value);
    if(watching&&code==0&&value->st_dev==device&&value->st_ino==inode)++heapChecks;
    return code;
}
extern "C" char* realFetch(dbms::PageAllocator*,uint32_t) asm("__real__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator*,uint32_t) asm("__wrap__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator* value,uint32_t page) {
    if(watching&&value==allocator)++heapFetches;
    return realFetch(value,page);
}
int main() try {
    TypeRegistry::instance().bootstrap();g_config.autoAnalyzeEnabled=false;g_config.autoVacuumEnabled=false;
    g_engine.setBackgroundIntervals(60000,60000);
    need(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"private real database");
    TableSchema schema;schema.tablename=table;schema.formatVersion=DATA_FILE_FORMAT_VERSION;
    schema.append(makeIntColumn("id",false,4,true));schema.append(makeVarCharColumn("body",true,128,false));
    schema.append(makeIntColumn("score",true,4,false));schema.append(makePointColumn("location",true,false));
    schema.append(makeVarCharColumn("pad",true,128,false));
    need(g_engine.createTable(db,schema)==DBStatus::OK,"current physical heap format");
    need(g_engine.beginTransaction(db)==DBStatus::OK,"real seed transaction");
    for(int id=1;id<=192;++id) {
        StorageEngine::SqlRow row{{"id",std::to_string(id)},
            {"body",id==1?std::nullopt:std::optional<std::string>(id==2?"":id==3?"NULL":"tok"+std::to_string(id)+" 界\n|;,")},
            {"score",id%11?std::optional<std::string>(std::to_string(id*10)):std::nullopt},
            {"location",id%13?std::optional<std::string>(std::to_string(id)+","+std::to_string(id*2)):std::nullopt},
            {"pad",id%7?std::optional<std::string>("pad 界\n"+std::to_string(id)):std::nullopt}};
        need(g_engine.insertRow(db,table,row)==DBStatus::OK,"actual nullable row");model.emplace(id,std::move(row));
    }
    need(g_engine.commitTransaction()==DBStatus::OK,"durable seed");inspect();
    measured("hash",[]{return g_engine.createHashIndex(db,table,"body");});
    measured("bloom",[]{return g_engine.createBloomIndex(db,table,"body");});
    measured("gin",[]{return g_engine.createGinIndex(db,table,"body");});
    measured("gist",[]{return g_engine.createGiSTIndex(db,table,"score");});
    measured("spgist",[]{return g_engine.createSPGiSTIndex(db,table,"location");});
    measured("brin",[]{return g_engine.createBrinIndex(db,table,"score",1);});
    measured("fulltext",[]{return g_engine.createFullTextIndex(db,table,"body");});
    measured("btree",[]{return g_engine.createIndex(db,table,"body");});
    measured("composite",[]{return g_engine.createCompositeIndex(db,table,{"score","body"},"score_body");});
    measured("reindex",[]{return g_engine.reindex(db,table);});probes(inspect());
    need(g_engine.beginTransaction(db)==DBStatus::OK,"real owned write");
    need(g_engine.updateRows(db,table,StorageEngine::SqlRow{{"body",std::string("temporary")}},{"=id 19"})==DBStatus::OK,"indexed update");
    need(g_engine.reindex(db,table)==DBStatus::OK,"physical maintenance view retains own update");
    need(g_engine.rollbackTransaction()==DBStatus::OK,"rollback restores old complete rows");probes(inspect());
    // With no live RID binding BRIN must still use this pinned tuple's bitmap.
    TableSchema nulls;nulls.tablename="all_null";nulls.formatVersion=DATA_FILE_FORMAT_VERSION;
    nulls.append(makeIntColumn("id",false,4,true));nulls.append(makeIntColumn("n",true,4,false));
    need(g_engine.createTable(db,nulls)==DBStatus::OK,"all-NULL fixture");
    for(int id=1;id<=3;++id)need(g_engine.insertRow(db,"all_null",{{"id",std::to_string(id)},{"n",std::nullopt}})==DBStatus::OK,"actual SQL NULL integer");
    need(g_engine.createBrinIndex(db,"all_null","n",1)==DBStatus::OK,"all-NULL BRIN summary build");
    need(g_engine.brinSearchRange(db,"all_null","n","=","0").empty(),"all-NULL integer is not a zero-valued range");
    need(g_engine.brinSearchRange(db,"all_null","n","=","").empty(),"all-NULL integer is not an empty-text range");
    TableSchema empty;empty.tablename="empty_text";empty.formatVersion=DATA_FILE_FORMAT_VERSION;
    empty.append(makeIntColumn("id",false,4,true));empty.append(makeVarCharColumn("s",true,32,false));
    need(g_engine.createTable(db,empty)==DBStatus::OK,"NULL/empty text fixture");
    need(g_engine.insertRow(db,"empty_text",{{"id",std::string("1")},{"s",std::nullopt}})==DBStatus::OK,"actual text NULL");
    need(g_engine.insertRow(db,"empty_text",{{"id",std::string("2")},{"s",std::string()}})==DBStatus::OK,"actual empty text");
    need(g_engine.createBrinIndex(db,"empty_text","s",1)==DBStatus::OK,"empty text BRIN summary");
    need(!g_engine.brinSearchRange(db,"empty_text","s","=","").empty(),"genuine empty text still has a summary");
    Session session;session.username="testuser";session.permission=1;session.currentDB=db;
    DdlExecutor ddl;
    need(!ddl.executeSql("CREATE TABLE virtual_value(id integer PRIMARY KEY,k integer,v integer GENERATED ALWAYS AS(k+1) VIRTUAL)",session),"real virtual declaration");
    need(g_engine.insertRow(db,"virtual_value",{{"id",std::string("1")},{"k",std::nullopt}})==DBStatus::OK,"NULL virtual source");
    need(g_engine.insertRow(db,"virtual_value",{{"id",std::string("2")},{"k",std::string("41")}})==DBStatus::OK,"non-NULL virtual source");
    int64_t virtualRid=-1;
    need(g_engine.getPKIndex(db,"virtual_value")->search("2",virtualRid),"actual virtual row RID");
    need(g_engine.createGiSTIndex(db,"virtual_value","v")==DBStatus::OK,"virtual builder ignores stored placeholder NULL");
    need(g_engine.giSTSearchOverlap(db,"virtual_value","v","42","42")==std::vector<int64_t>{virtualRid},"actual generated value and RID");
    need(g_engine.createBrinIndex(db,"virtual_value","v",1)==DBStatus::OK,"BRIN computes virtual semantic NULL");
    need(g_engine.brinSearchRange(db,"virtual_value","v","=","0").empty(),"NULL virtual result is not zero");
    need(!g_engine.brinSearchRange(db,"virtual_value","v","=","42").empty(),"non-NULL virtual value summarized");
    need(!ddl.executeSql("CREATE TABLE virtual_error(id integer PRIMARY KEY,v integer GENERATED ALWAYS AS(1/(id-id)) VIRTUAL)",session),"real error declaration");
    need(g_engine.insertRow(db,"virtual_error",{{"id",std::string("1")}})==DBStatus::OK,"unused virtual error is not demanded by insertion");
    bool primaryError=false;
    try{(void)g_engine.createGiSTIndex(db,"virtual_error","v");}
    catch(const DbError& error){primaryError=error.sqlState()=="22012";}
    need(primaryError,"index demand preserves original division SQLSTATE while unwinding bitmap owner");
    // A later unrelated builder/decoder must not inherit the previous bitmap.
    need(g_engine.createGinIndex(db,table,"body")==DBStatus::OK,"subsequent builder after NULL-only scan");probes(inspect());
    std::cout<<"INDEX_BUILD_SCAN_OWNER checks="<<checks<<" failures=0\n";return 0;
} catch(const std::exception& error) {
    watching=false;std::cerr<<"INDEX_BUILD_SCAN_OWNER_FAIL checks="<<checks<<' '<<error.what()<<'\n';return 1;
}
