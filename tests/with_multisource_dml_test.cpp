#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "executor/ExecutionPlan.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <set>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="with_multisource_dml",db=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target(id INT,a INT,t TEXT)",session));
    assert(!ddl.executeSql("CREATE TABLE source(id INT,a INT,t TEXT)",session));
    for(int i=0;i<2;++i)assert(g_engine.insertRow(db,"target",{{"id","1"},{"a","0"},{"t",std::nullopt}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"source",{{"id","1"},{"a","10"},{"t",""}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"source",{{"id","1"},{"a","20"},{"t","NULL"}})==DBStatus::OK);
    std::set<int64_t> matched,observed;
    StorageEngine::SqlMutationCallbacks callbacks;
    callbacks.matches=[&](int64_t rid,const StorageEngine::SqlRow& old){assert(!old.at("t"));assert(matched.insert(rid).second);return true;};
    callbacks.resolve=[&](int64_t rid,const StorageEngine::SqlRow& old,StorageEngine::SqlRow& values){assert(matched.count(rid));assert(!old.at("t"));values["a"]=std::to_string(rid==*matched.begin()?1:2);return true;};
    callbacks.observed=[&](int64_t rid,const StorageEngine::SqlRow& old,const StorageEngine::SqlRow& values){assert(matched.count(rid));assert(!old.at("t") && !values.at("t"));assert(observed.insert(rid).second);};
    size_t count=0;
    assert(g_engine.updateRows(db,"target",{}, {},nullptr,{}, {},&count,nullptr,&callbacks)==DBStatus::OK);
    assert(count==2 && matched.size()==2 && observed==matched);

    const auto execute=[&](const std::string& sql) {
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
        auto* with=dynamic_cast<WithStmt*>(query->ast.get());assert(with);
        size_t reads=0;
        PreparedDmlSourceFactory factory=[&](const Stmt* owner,const FromItem* item,const RowContext& outer) {
            assert(item && item->type==FromItem::Type::Table);
            const PreparedQuery::SourceRange* source=nullptr;
            for(const auto& range:query->sourceRanges)if(range.owner==owner && range.source==item && !range.mergedUsing)source=&range;
            assert(source && source->relationName=="source");
            auto carrier=std::make_shared<PreparedQueryExecution>(query,&g_engine,db);
            auto scan=std::make_shared<TableScanOp>(&g_engine,db,"source");
            const auto schema=g_engine.getTableSchema(db,"source");
            auto rows=std::make_shared<PreparedQueryRows>();auto complete=std::make_shared<bool>(false);
            auto opened=std::make_shared<bool>(false);
            PreparedDmlSourceRows output;output.occurrences={source->ordinal};
            output.read=[&,carrier,scan,schema,rows,complete,opened,source,outer](size_t index,RowContext& row) {
                ++reads;
                if(!*opened){assert(scan->open());*opened=true;}
                while(!*complete && rows->size()<=index) {
                    std::string raw;
                    if(scan->next(raw)) {
                        std::vector<ExprValue> cells;
                        for(size_t i=0;i<schema.len;++i) {
                            bool computedNull=false;
                            const auto value=g_engine.extractColumnValue(raw,schema,i,db,true,&computedNull);
                            cells.emplace_back(source->columns[i].type,value,computedNull || scan->lastColumnIsNull(i));
                        }
                        rows->push_back(std::move(cells));
                    } else {assert(!scan->hasError());scan->close();*complete=true;}
                }
                if(index>=rows->size())return false;
                row=outer;carrier->setSourceRow(row,source->ordinal,rows->at(index));return true;
            };
            return output;
        };
        prepareBoundDml(with->statement.get(),session,query,factory);
        assert(reads==0 && !g_engine.inTransaction());
        const auto result=executeAtomicDmlUnit(session,[&]{return executeBoundDml(with->statement.get(),session,query,{},factory);});
        assert(reads>0);return result;
    };
    auto result=execute("WITH p AS(SELECT 1) UPDATE target t SET a=s.a,t=s.t FROM source s WHERE t.id=s.id RETURNING old.a,new.a,t.t,s.a,s.t");
    assert(result.commandTag=="UPDATE 2" && result.rows.size()==2);
    for(const auto& row:result.rows){assert((row[0]=="1" || row[0]=="2") && row[1]==row[3] && row[2]==row[4]);assert(row[1]=="10" || row[1]=="20");}
    assert(result.columnTypes==std::vector<std::string>({"integer","integer","text","integer","text"}));
    result=execute("WITH p AS(SELECT 1) UPDATE target t SET a=s.a FROM source s WHERE t.id=s.id RETURNING *");
    assert(result.commandTag=="UPDATE 2" && result.columnTypes.size()==6 && result.rows.front().size()==6);
    result=execute("WITH p AS(SELECT 1) DELETE FROM target t USING source s WHERE t.id=s.id RETURNING old.*,new.*,s.*");
    assert(result.commandTag=="DELETE 2" && result.rows.size()==2 && result.columnTypes.size()==9);
    for(const auto& nulls:result.nulls)assert(nulls[3] && nulls[4] && nulls[5] && !nulls[8]);
    assert(g_engine.query(db,"target",{}, {"id"}).empty());
    assert(!ddl.executeSql("CREATE TABLE key_target(id INT PRIMARY KEY,v INT)",session));
    assert(!ddl.executeSql("CREATE TABLE key_child(pid INT REFERENCES key_target(id) ON UPDATE CASCADE ON DELETE CASCADE)",session));
    assert(g_engine.insertRow(db,"key_target",{{"id","1"},{"v","0"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"key_child",{{"pid","1"}})==DBStatus::OK);
    result=execute("WITH p AS(SELECT 1) UPDATE key_target t SET id=s.a FROM source s WHERE t.id=s.id RETURNING old.id,new.id,s.a");
    assert(result.commandTag=="UPDATE 1" && result.rows.size()==1 && result.rows[0][0]=="1" && result.rows[0][1]==result.rows[0][2]);
    auto child=g_engine.query(db,"key_child",{}, {"pid"});assert(child.size()==1);
    assert(child.front().find(result.rows[0][1])!=std::string::npos);
    result=execute("WITH p AS(SELECT 1) DELETE FROM key_target t USING source s WHERE t.id=s.a RETURNING t.id,s.a");
    assert(result.commandTag=="DELETE 1" && result.rows.size()==1 && result.rows[0][0]==result.rows[0][1]);
    assert(g_engine.query(db,"key_child",{}, {"pid"}).empty());
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"[WITH MULTISOURCE DML] real RID identity, no preparation reads, actual nullable source/transition/star channels passed\n";
}
