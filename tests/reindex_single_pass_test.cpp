// Genuine REINDEX input/order, bounded pool fanout and original fallback.
// test_link_flags: -Wl,--wrap=open,--wrap=close
#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "Session.h"
#include "test_utils.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cctype>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

extern dbms::StorageEngine g_engine;
extern "C" int __real_open(const char*,int,...);
extern "C" int __real_close(int);

namespace {
using namespace dbms;
constexpr const char* databaseName="reindex_single_pass";
struct OpenObservation {
    bool active=false;
    size_t attempts=0,opened=0,live=0,maximum=0,failAttempt=0;
    std::array<int,16> dataFds{};
    OpenObservation(){dataFds.fill(-1);}
} observed;
bool ownTemporary(const char* path) {
    return path && std::strstr(path,"__t_reindex_single_pass") && std::strstr(path,".reindex.tmp.");
}
bool endsWith(const char* text,const char* suffix) {
    const auto a=std::strlen(text),b=std::strlen(suffix);return a>=b && std::strcmp(text+a-b,suffix)==0;
}
void require(bool good,const std::string& message){if(!good)throw std::runtime_error(message);}
size_t temporaryFds() {
    size_t result=0;
    for(const auto& entry:std::filesystem::directory_iterator("/proc/self/fd")) {
        char target[4096];const auto n=::readlink(entry.path().c_str(),target,sizeof(target)-1);
        if(n<0)continue;
        target[n]='\0';result+=ownTemporary(target);
    }
    return result;
}
bool noArtifacts(const std::string& db) {
    if(temporaryFds())return false;
    for(const auto& entry:std::filesystem::recursive_directory_iterator(db)) {
        const auto name=entry.path().filename().string();
        if(name.find(".reindex.tmp.")!=std::string::npos || name.find(".reindex.old.")!=std::string::npos ||
           name.find(".reindex_swap")!=std::string::npos)return false;
    }
    return true;
}
std::vector<int64_t> sorted(std::vector<int64_t> values){std::sort(values.begin(),values.end());return values;}
struct Row {int id;std::optional<std::string> tag;std::string part,amount;bool active;};
Row makeRow(int id) {
    std::optional<std::string> tag;
    if(id%11)tag=id%13?"tag"+std::to_string(id%4):std::string();
    const std::string amount=id%3==0?"0.00":id%3==1?"-1.500":"1.50";
    return {id,tag,"part"+std::to_string(id%5),amount,id%2!=0};
}
struct Index {
    std::string name;bool composite=false;std::string column;
    std::function<bool(const Row&)> included=[](const Row&){return true;};
    std::function<std::string(const Row&)> key;
};
struct Table {
    std::string db,name;bool primary;size_t ordinaryCount=0;
    std::vector<Row> model;std::vector<Index> indexes;
    Table(const std::string& database,const std::string& table,bool pk=true,size_t count=64,
          const std::string& generated="") :db(database),name(table),primary(pk) {
        Session session;session.username="admin";session.permission=1;session.currentDB=db;
        std::string ddl="CREATE TABLE "+name+"(id integer"+(pk?" PRIMARY KEY":"")+
            ",tag text COLLATE \"C\",part varchar(32),amount numeric,active boolean";
        for(int i=0;i<8;++i)ddl+=",c"+std::to_string(i)+" integer";
        if(!generated.empty())ddl+=",doubled integer GENERATED ALWAYS AS (id*2) "+generated;
        ddl+=")";DdlExecutor executor;require(!executor.executeSql(ddl,session),"create genuine "+name);
        require(g_engine.beginTransaction(db)==DBStatus::OK,"fixture transaction");
        for(size_t id=1;id<=count;++id){auto row=makeRow(int(id));insert(row);model.push_back(row);}
        require(g_engine.commitTransaction()==DBStatus::OK,"fixture commit");
    }
    void insert(const Row& row) {
        StorageEngine::SqlRow input={{"id",std::to_string(row.id)},{"tag",row.tag},{"part",row.part},
            {"amount",row.amount},{"active",row.active?"true":"false"}};
        for(int i=0;i<8;++i)input["c"+std::to_string(i)]=std::to_string(row.id%(i+3));
        require(g_engine.insertRow(db,name,input)==DBStatus::OK,"insert actual "+name);
    }
    void auxiliary(const std::string& column,const std::string& predicate="",const std::string& expression="") {
        require(g_engine.createIndex(db,name,column,true,{},predicate,expression)==DBStatus::OK,"create real auxiliary index");
        Index index;index.name=expression.empty()?column:expression;index.column=column;
        if(!predicate.empty())index.included=[](const Row& row){return row.active;};
        if(column=="tag")index.key=[expression](const Row& row) {
            auto key=row.tag.value_or("");if(!expression.empty())for(char& ch:key)ch=char(std::toupper(static_cast<unsigned char>(ch)));return key;};
        else if(column=="amount")index.key=[](const Row& row){return row.id%3==0?"0":row.id%3==1?"-1.5":"1.5";};
        else if(column=="doubled")index.key=[](const Row& row){return std::to_string(row.id*2);};
        else {const int col=std::stoi(column.substr(1));index.key=[col](const Row& row){return std::to_string(row.id%(col+3));};}
        indexes.push_back(std::move(index));++ordinaryCount;
    }
    void composite() {
        require(g_engine.createCompositeIndex(db,name,{"tag","part"},"tag_part")==DBStatus::OK,"create actual composite");
        Index index;index.name="tag_part";index.composite=true;
        // Preserve the existing composite encoding: an empty first component
        // contributes no separator. This test does not redefine that policy.
        index.key=[](const Row& row){const auto tag=row.tag.value_or("");
            return tag.empty()?row.part:tag+std::string(1,'\x01')+row.part;};indexes.push_back(std::move(index));
    }
    BPTree* tree(StorageEngine& engine,const Index& index) const {
        return index.composite?engine.getCompositeIndexTree(db,name,index.name):engine.getSecondaryIndex(db,name,index.name);
    }
    std::map<int,int64_t> actualRids(StorageEngine& engine) const {
        const auto schema=engine.getTableSchema(db,name);std::map<int,int64_t> result;
        require(engine.forEachRow(db,name,[&](uint32_t page,uint16_t slot,const char* bytes,size_t size) {
            const std::string row(bytes,size);const int id=std::stoi(engine.extractColumnValue(row,schema,0,db));
            require(result.emplace(id,StorageEngine::encodeRid(page,slot)).second,"one real heap occurrence per id");
        }),"genuine heap RID scan");return result;
    }
    void verify(StorageEngine& engine) const {
        const auto rids=actualRids(engine);require(rids.size()==model.size(),"all actual rows preserved");
        std::vector<int64_t> expectedPrimary;
        for(const auto& row:model) {
            const auto found=rids.find(row.id);require(found!=rids.end(),"every model id has actual heap RID");
            if(primary){auto* index=engine.getPKIndex(db,name);int64_t indexed=-1;
                require(index && index->search(std::to_string(row.id),indexed) && indexed==found->second,"actual primary RID mapping");
                expectedPrimary.push_back(indexed);}
        }
        if(primary){std::vector<int64_t> all;require(engine.getPKIndex(db,name)->allValuesChecked(all) &&
            sorted(all)==sorted(expectedPrimary),"complete primary membership");}
        for(const auto& index:indexes) {
            auto* btree=tree(engine,index);require(btree && btree->isOpen(),"actual auxiliary tree open");
            std::map<std::string,std::vector<int64_t>> expected;
            for(const auto& row:model)if(index.included(row)) {
                const auto key=index.key(row);
                if(!key.empty())expected[key].push_back(rids.at(row.id));
            }
            std::vector<int64_t> expectedAll;
            for(const auto& entry:expected) {
                std::vector<int64_t> actual;require(btree->searchMultiChecked(entry.first,actual),"checked actual key read");
                require(sorted(actual)==sorted(entry.second),"all duplicate/NULL/canonical key memberships: "+index.name);
                expectedAll.insert(expectedAll.end(),entry.second.begin(),entry.second.end());
            }
            std::vector<int64_t> all;require(btree->allValuesChecked(all) && sorted(all)==sorted(expectedAll),"no extra/missing auxiliary RID");
        }
        const auto nulls=engine.query(db,name,{"isnull tag"},{"id"});
        require(nulls.size()==size_t(std::count_if(model.begin(),model.end(),[](const Row& r){return !r.tag;})),"stored NULL count");
    }
    std::vector<std::vector<int64_t>> indexOrder() const {
        std::vector<std::vector<int64_t>> result;
        if(primary){result.emplace_back();require(g_engine.getPKIndex(db,name)->allValuesChecked(result.back()),"primary order snapshot");}
        for(const auto& index:indexes){result.emplace_back();require(tree(g_engine,index)->allValuesChecked(result.back()),"auxiliary order snapshot");}
        return result;
    }
};
DBStatus rebuild(Table& table,size_t expectedPeak,size_t failAt=0) {
    require(temporaryFds()==0,"no preexisting owned temporary descriptor");
    observed=OpenObservation{};observed.failAttempt=failAt;observed.active=true;
    DBStatus status;
    try{status=g_engine.reindex(table.db,table.name);}catch(...){observed.active=false;throw;}
    observed.active=false;
    require(observed.live==0 && noArtifacts(table.db),"all private file/descriptor cleanup");
    if(!failAt)require(observed.maximum==expectedPeak,"actual data-file pool fanout matches selected branch");
    return status;
}
void standard(Table& table,size_t peak) {
    table.verify(g_engine);const auto order=table.indexOrder();
    require(rebuild(table,peak)==DBStatus::OK,"REINDEX success");table.verify(g_engine);
    require(table.indexOrder()==order,"per-key/per-tree RID order unchanged");
}
struct Rollback {~Rollback(){if(g_engine.inTransaction())g_engine.rollbackTransaction();}};
}

extern "C" int __wrap_open(const char* path,int flags,...) {
    mode_t mode=0;const bool modeNeeded=(flags&O_CREAT)!=0;
    if(modeNeeded){va_list args;va_start(args,flags);mode=mode_t(va_arg(args,int));va_end(args);}
    const bool data=observed.active && ownTemporary(path) && !endsWith(path,".tde") && (flags&O_ACCMODE)!=O_RDONLY;
    if(data && ++observed.attempts==observed.failAttempt){errno=EIO;return -1;}
    const int fd=modeNeeded?__real_open(path,flags,mode):__real_open(path,flags);
    if(data && fd>=0){++observed.opened;++observed.live;observed.maximum=std::max(observed.maximum,observed.live);
        for(auto& saved:observed.dataFds)if(saved<0){saved=fd;break;}}
    return fd;
}
extern "C" int __wrap_close(int fd) {
    for(auto& saved:observed.dataFds)if(saved==fd){saved=-1;if(observed.live)--observed.live;break;}
    return __real_close(fd);
}

int main() {
    TypeRegistry::instance().bootstrap();cleanupTestDb(databaseName);const auto db=testDbPath(databaseName);
    if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);
    size_t checked=0,failed=0;
    const auto check=[&](const char* label,const std::function<void()>& run) {
        ++checked;bool good=false;std::string error;observed=OpenObservation{};
        try{run();good=noArtifacts(db);}catch(const DbError& x){error=x.sqlState()+":"+x.what();}
        catch(const std::exception& x){error=x.what();}
        observed.active=false;if(g_engine.inTransaction())g_engine.rollbackTransaction();
        failed+=!good;std::cout<<"REINDEX_SINGLE_PASS "<<checked<<' '<<label<<" good="<<good
            <<" data_open_attempts="<<observed.attempts<<" opened="<<observed.opened<<" max_data_pools="<<observed.maximum;
        if(!good)std::cout<<" error="<<std::quoted(error);
        std::cout<<'\n';
    };
    check("ordinary secondary and composite preserve full rows and order",[&] {
        Table t(db,"ordinary",true,128);t.auxiliary("tag");t.composite();standard(t,3);
    });
    check("eight total private trees take bounded single scan branch",[&] {
        Table t(db,"eight");for(int i=0;i<7;++i)t.auxiliary("c"+std::to_string(i));standard(t,8);
    });
    check("nine total trees use original serial resource path",[&] {
        Table t(db,"nine");for(int i=0;i<8;++i)t.auxiliary("c"+std::to_string(i));standard(t,1);
    });
    check("no primary key still builds genuine auxiliary trees",[&] {
        Table t(db,"without_pk",false);t.auxiliary("tag");t.composite();standard(t,3);
    });
    check("empty relation completes all private trees without input",[&] {
        Table t(db,"empty",false,0);t.auxiliary("tag");t.composite();standard(t,3);
    });
    check("only primary index retains original serial build",[&] {
        Table t(db,"only_pk");standard(t,1);
    });
    check("numeric canonical values C collation NULL and empty keys",[&] {
        Table t(db,"numeric_values");t.auxiliary("tag");t.auxiliary("amount");
        const auto before=g_engine.getTableSchema(db,t.name);standard(t,3);const auto after=g_engine.getTableSchema(db,t.name);
        require(before.len==after.len && before.cols[1].collation==after.cols[1].collation &&
            before.cols[1].resolvedCollation==after.cols[1].resolvedCollation && before.cols[3].dataType==after.cols[3].dataType,
            "original catalog type/collation identity preserved");
    });
    check("partial index keeps original predicate evaluation path",[&] {
        Table t(db,"partial_values");t.auxiliary("tag","active = true");t.auxiliary("c0");standard(t,1);
    });
    check("expression index keeps original index-major path",[&] {
        Table t(db,"expression_values");t.auxiliary("tag","","UPPER(tag)");t.auxiliary("c0");standard(t,1);
    });
    check("virtual generated source retains original evaluation path",[&] {
        Table t(db,"virtual_values",true,64,"VIRTUAL");t.auxiliary("doubled");t.auxiliary("c0");standard(t,1);
    });
    check("stored generated datum remains eligible physical input",[&] {
        Table t(db,"stored_values",true,64,"STORED");t.auxiliary("doubled");t.auxiliary("c0");standard(t,3);
    });
    check("active insert is reindexed and actual rollback repairs replacement",[&] {
        Table t(db,"rollback_insert");t.auxiliary("tag");t.composite();const auto original=t.model;
        require(g_engine.beginTransaction(db)==DBStatus::OK,"begin actual insert");Rollback rollback;
        const auto row=makeRow(999);t.insert(row);t.model.push_back(row);standard(t,3);
        require(g_engine.rollbackTransaction()==DBStatus::OK,"rollback insert");t.model=original;t.verify(g_engine);
        int64_t missing=-1;require(!g_engine.getPKIndex(db,t.name)->search("999",missing),"no rolled-back primary entry");
    });
    check("active update delete and rollback preserve physical index versions",[&] {
        Table t(db,"rollback_update");t.auxiliary("tag");t.composite();const auto original=t.model;
        require(g_engine.beginTransaction(db)==DBStatus::OK,"begin real mutation");Rollback rollback;
        require(g_engine.update(db,t.name,{{"tag","changed"}},{"=id 1"})==DBStatus::OK,"actual update new RID");
        require(g_engine.remove(db,t.name,{"=id 2"})==DBStatus::OK,"actual delete");
        for(auto& row:t.model)if(row.id==1)row.tag="changed";
        t.model.erase(std::remove_if(t.model.begin(),t.model.end(),[](const Row& row){return row.id==2;}),t.model.end());
        standard(t,3);require(g_engine.rollbackTransaction()==DBStatus::OK,"rollback actual update/delete");
        t.model=original;t.verify(g_engine);
    });
    check("commit then same-directory cold engine sees complete replacement",[&] {
        Table t(db,"cold_reopen");t.auxiliary("tag");t.composite();
        require(g_engine.beginTransaction(db)==DBStatus::OK,"begin commit control");Rollback rollback;
        auto row=makeRow(999);t.insert(row);t.model.push_back(row);standard(t,3);
        require(g_engine.commitTransaction()==DBStatus::OK && g_engine.checkpoint(db),"durable commit/checkpoint");
        StorageEngine cold;t.verify(cold);
        require(cold.getTableSchema(db,t.name).len==g_engine.getTableSchema(db,t.name).len,"real source schema after cold open");
    });
    check("first private data open failure leaves all live indexes unchanged",[&] {
        Table t(db,"fail_first");t.auxiliary("tag");t.composite();const auto before=t.indexOrder();
        require(rebuild(t,0,1)==DBStatus::IO_ERROR,"actual pre-publication open failure status");
        t.verify(g_engine);require(t.indexOrder()==before,"old index contents remain exact");
    });
    check("later private open failure cleans already opened trees",[&] {
        Table t(db,"fail_later");for(int i=0;i<7;++i)t.auxiliary("c"+std::to_string(i));const auto before=t.indexOrder();
        require(rebuild(t,0,4)==DBStatus::IO_ERROR && observed.opened==3,"actual fourth private open failure");
        t.verify(g_engine);require(t.indexOrder()==before,"all old live trees retained");
        standard(t,8);
    });
    setCurrentSession(previous);
    const bool cleaned=noArtifacts(db) && g_engine.dropDatabase(db)==DBStatus::OK;
    if(cleaned)cleanupTestDb(databaseName);else ++failed;
    if(checked!=16)++failed;
    std::cout<<"REINDEX_SINGLE_PASS_CHECKED="<<checked<<" FAILED="<<failed<<'\n';return failed?1:0;
}
