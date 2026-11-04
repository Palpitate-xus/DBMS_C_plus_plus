// REINDEX preserves complete stored values, NULLs, key/RID membership and rollback.
#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
using Cell=std::optional<std::string>;
using Row=std::map<std::string,Cell>;
void require(bool ok,const std::string& message){if(!ok)throw std::runtime_error(message);}
std::vector<int64_t> sorted(std::vector<int64_t> value){std::sort(value.begin(),value.end());return value;}
struct Index {std::string name;bool composite=false;std::vector<std::string> columns;};
struct Fixture {
    std::string db,name;std::vector<Row> rows;std::vector<Index> indexes;
    Fixture(const std::string& database,const std::string& table,size_t count,bool wide=true,bool quoted=false):db(database),name(table){
        Session session;session.username="admin";session.permission=1;session.currentDB=db;DdlExecutor ddl;
        const auto k=quoted?"\"KeyCase\"":"k";
        std::string schema="CREATE TABLE "+name+"(id INTEGER PRIMARY KEY,"+k+" INTEGER,k2 INTEGER";
        if(wide)schema+=",payload TEXT,ai INTEGER[],rr REGTYPE,ra REGTYPE[],tag TEXT COLLATE \"C\"";
        else schema+=",small_text VARCHAR(8)";
        schema+=")";require(!ddl.executeSql(schema,session),"real fixture DDL");
        require(g_engine.beginTransaction(db)==DBStatus::OK,"real fixture transaction");
        for(size_t i=1;i<=count;++i){
            Row row={{"id",std::to_string(i)},{quoted?"KeyCase":"k",i%13?Cell(std::to_string(i%5)):Cell{}},{"k2",std::to_string(i%3)}};
            if(wide){
                std::string payload;
                for(size_t j=0;j<6000;++j)payload+=char('!'+(j*73+i*19)%90);
                row["payload"]=i%7==0?Cell{}:i%7==1?Cell(""):Cell(payload);
                row["ai"]=i%5==0?Cell{}:Cell("[0:2]={"+std::to_string(i)+",NULL,"+std::to_string(i+1)+"}");
                row["rr"]=i%4==0?Cell{}:Cell("16");
                row["ra"]=i%6==0?Cell{}:i%6==1?Cell("{}"):Cell("{20,NULL,16}");
                row["tag"]=i%3==0?Cell{}:i%3==1?Cell(""):Cell("tag"+std::to_string(i%2));
            }else row["small_text"]="v"+std::to_string(i%3);
            require(g_engine.insertRow(db,name,row)==DBStatus::OK,"real stored values including arrays/reference OIDs");rows.push_back(row);
        }
        require(g_engine.commitTransaction()==DBStatus::OK,"fixture durable commit");
    }
    void auxiliary(const std::string& column,const std::vector<std::string>& included={}){
        require(g_engine.createIndex(db,name,column,true,included)==DBStatus::OK,"genuine plain index metadata");indexes.push_back({column,false,{column}});
    }
    void composite(const std::string& index,const std::vector<std::string>& columns){
        require(g_engine.createCompositeIndex(db,name,columns,index)==DBStatus::OK,"genuine composite metadata");indexes.push_back({index,true,columns});
    }
    std::string key(const Row& row,const Index& index)const{
        std::string result;
        for(const auto& column:index.columns){if(!result.empty())result+='\1';const auto& value=row.at(column);if(value)result+=*value;}
        return result;
    }
    std::vector<std::vector<int64_t>> verify(StorageEngine& engine)const{
        const auto schema=engine.getTableSchema(db,name);std::map<std::string,int64_t> rids;
        require(engine.forEachRow(db,name,[&](uint32_t page,uint16_t slot,const char* bytes,size_t length){
            const std::string raw(bytes,length);const auto id=engine.extractColumnValue(raw,schema,0,db);
            const auto model=std::find_if(rows.begin(),rows.end(),[&](const Row& r){return r.at("id")==Cell(id);});
            require(model!=rows.end(),"real heap occurrence has independent expected id");
            require(rids.emplace(id,StorageEngine::encodeRid(page,slot)).second,"unique real heap id");
            for(size_t col=0;col<schema.len;++col){bool isNull=false;const auto actual=engine.extractColumnValue(raw,schema,col,db,true,&isNull);const auto& expected=model->at(schema.cols[col].dataName);
                require(isNull==!expected,"true NULL distinct from empty: "+schema.cols[col].dataName);
                require(actual==expected.value_or(""),"complete original stored values: "+schema.cols[col].dataName);
            }
        }),"real complete heap scan");
        require(rids.size()==rows.size(),"complete heap row count");
        std::vector<std::vector<int64_t>> order;auto* primary=engine.getPKIndex(db,name);require(primary&&primary->isOpen(),"actual PK open");
        std::vector<int64_t> pkExpected;
        for(const auto& row:rows){const auto& id=*row.at("id");int64_t actual=-1;require(primary->search(id,actual)&&actual==rids.at(id),"real primary RID preserved");pkExpected.push_back(actual);}
        order.emplace_back();require(primary->allValuesChecked(order.back())&&sorted(order.back())==sorted(pkExpected),"complete PK membership");
        for(const auto& index:indexes){
            auto* tree=index.composite?engine.getCompositeIndexTree(db,name,index.name):engine.getSecondaryIndex(db,name,index.name);require(tree&&tree->isOpen(),"real auxiliary tree open");
            std::map<std::string,std::vector<int64_t>> expected;
            for(const auto& row:rows){const auto value=key(row,index);if(!value.empty())expected[value].push_back(rids.at(*row.at("id")));}
            std::vector<int64_t> all;
            for(const auto& entry:expected){std::vector<int64_t> actual;require(tree->searchMultiChecked(entry.first,actual)&&sorted(actual)==sorted(entry.second),"all duplicate key/RID memberships");all.insert(all.end(),entry.second.begin(),entry.second.end());}
            order.emplace_back();require(tree->allValuesChecked(order.back())&&sorted(order.back())==sorted(all),"no extra auxiliary RID");
        }
        return order;
    }
    void rebuild(StorageEngine& engine,size_t expectedRows)const{
        require(expectedRows==rows.size(),"independent complete expected row count");
        const auto before=verify(engine);
        require(engine.reindex(db,name)==DBStatus::OK,"actual REINDEX status");
        require(verify(engine)==before,"complete per-key/per-tree RID order unchanged");
    }
};
}
int main(){
    using namespace dbms;TypeRegistry::instance().bootstrap();const std::string test="reindex_materialization",db=testDbPath(test);cleanupTestDb(test);
    require(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"private database");Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);size_t controls=0,failed=0;
    const auto check=[&](const char* label,const std::function<void()>& run){++controls;std::string error;bool ok=true;try{run();}catch(const DbError& e){error=e.sqlState()+":"+e.what();ok=false;}catch(const std::exception& e){error=e.what();ok=false;}if(g_engine.inTransaction())g_engine.rollbackTransaction();failed+=!ok;std::cout<<"REINDEX_MATERIALIZATION_CONTROL "<<controls<<' '<<label<<" good="<<ok<<" error="<<std::quoted(error)<<'\n';};
    check("wide sparse key preserves all unindexed TOAST arrays and reference fields",[&]{Fixture f(db,"sparse",48);f.auxiliary("k");f.rebuild(g_engine,48);});
    check("INCLUDE preserves original dependency declarations",[&]{Fixture f(db,"included",32);f.auxiliary("k",{"payload","ai","rr"});f.rebuild(g_engine,32);});
    check("overlapping compound and ordinary keys keep all NULL and RID orders",[&]{Fixture f(db,"overlap",64);f.auxiliary("k");f.auxiliary("k2");f.composite("k_k2",{"k","k2"});f.rebuild(g_engine,64);});
    check("full-needed original map has complete values and order",[&]{Fixture f(db,"all_needed",12,false);f.composite("whole_row",{"id","k","k2","small_text"});f.rebuild(g_engine,12);});
    check("real quoted column identity has no casefolded guessed dependency",[&]{Fixture f(db,"quoted_names",16,true,true);f.auxiliary("KeyCase");f.rebuild(g_engine,16);});
    check("empty genuine relation preserves empty memberships",[&]{Fixture f(db,"empty",0);f.auxiliary("k");f.rebuild(g_engine,0);});
    check("same directory cold REINDEX preserves every physical value",[&]{Fixture f(db,"cold",24);f.auxiliary("k");f.composite("k_k2",{"k","k2"});require(g_engine.checkpoint(db),"original checkpoint");StorageEngine cold;f.rebuild(cold,24);});
    check("active update delete and original rollback repair rebuilt membership",[&]{Fixture f(db,"rollback",20);f.auxiliary("k");f.composite("k_k2",{"k","k2"});const auto original=f.rows;require(g_engine.beginTransaction(db)==DBStatus::OK,"real transaction");require(g_engine.update(db,f.name,{{"k","77"}},{"=id 1"})==DBStatus::OK,"real update");require(g_engine.remove(db,f.name,{"=id 2"})==DBStatus::OK,"real delete");for(auto& row:f.rows)if(row.at("id")==Cell("1"))row["k"]="77";f.rows.erase(std::remove_if(f.rows.begin(),f.rows.end(),[](const Row& row){return row.at("id")==Cell("2");}),f.rows.end());f.rebuild(g_engine,19);require(g_engine.rollbackTransaction()==DBStatus::OK,"original rollback");f.rows=original;f.verify(g_engine);});
    setCurrentSession(previous);require(g_engine.dropDatabase(db)==DBStatus::OK,"drop private database");cleanupTestDb(test);finalCleanupTestData();
    std::cout<<"REINDEX_MATERIALIZATION_TERMINAL controls="<<controls<<" expected=8 failures="<<failed<<'\n';return controls==8&&failed==0?0:1;
}
