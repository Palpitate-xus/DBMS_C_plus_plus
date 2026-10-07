#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <optional>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("delete_exception_owner");
    std::vector<std::string> failures;
    const auto check=[&](const std::string& name,bool good) {
        if (!good) { failures.push_back(name);std::cerr<<"DELETE_OWNER_FAILURE "<<name<<'\n'; }
    };
    using Snapshot=std::pair<std::vector<std::vector<std::string>>,std::vector<std::vector<bool>>>;
    const auto snapshot=[&](StorageEngine& engine,const std::string& table) {
        Snapshot result;
        (void)engine.query(database,table,{},table=="target" ? std::set<std::string>{"id","payload","v"} : std::set<std::string>{"id"},
            {{"id",true}},false,false,false,0,{},&result.first,&result.second);
        return result;
    };
    const auto indexed=[&](StorageEngine& engine,const Snapshot& expected) {
        for(size_t i=0;i<expected.first.size();++i) {
            Snapshot actual;
            (void)engine.query(database,"target",{"=id "+expected.first[i][0]},{"id","payload","v"},{},false,false,false,0,{},&actual.first,&actual.second);
            check("indexed original id "+expected.first[i][0],actual.first==std::vector<std::vector<std::string>>{expected.first[i]} &&
                actual.second==std::vector<std::vector<bool>>{expected.second[i]});
        }
    };
    Snapshot coldExpected;
    {
        StorageEngine engine;
        assert(engine.createDatabase(database,"utf8")==DBStatus::OK);
        TableSchema target;target.tablename="target";
        target.append(makeIntColumn("id",false,4,true));target.append(makeTextColumn("v",true));target.append(makeTextColumn("payload",true));
        assert(engine.createTable(database,target)==DBStatus::OK);
        TableSchema audit;audit.tablename="audit";audit.append(makeIntColumn("id",false,4,true));
        assert(engine.createTable(database,audit)==DBStatus::OK);
        const std::vector<std::optional<std::string>> values={"a","ab","",std::nullopt,"NULL"};
        const std::vector<std::optional<std::string>> payloads={"plain",std::string(20000,'x'),"",std::nullopt,"NULL"};
        for(size_t i=0;i<values.size();++i)
            assert(engine.insertRow(database,"target",{{"id",std::to_string(i+1)},{"v",values[i]},{"payload",payloads[i]}})==DBStatus::OK);
        assert(engine.createIndex(database,"target","v")==DBStatus::OK);
        const auto original=snapshot(engine,"target");
        check("exact NULL empty text NULL control",original.first[2][1].empty() && !original.second[2][1] &&
            original.second[3][1] && original.second[3][2] && original.first[4][1]=="NULL" && !original.second[4][1]);
        const std::vector<StorageEngine::SqlRow> seed={{{"sentinel",std::string("original-output")}}};
        for(const std::string predicate:{"likev '_' ESCAPE 'xx'","likev E'a\\\\'"}) {
            auto output=seed;size_t affected=77;std::string state;
            try {(void)engine.removeRows(database,"target",{predicate},&output,{},&affected);}
            catch(const DbError& error){state=error.sqlState();}
            check("predicate exact SQLSTATE "+predicate,state=="22025");
            check("implicit owner released "+predicate,!engine.inTransaction());
            check("predicate output restored "+predicate,output==seed && affected==0);
            check("predicate full typed data "+predicate,snapshot(engine,"target")==original);
            indexed(engine,original);
            if(engine.inTransaction()) {
                std::cerr<<"DELETE_OWNER_DIAGNOSTIC_CLEANUP predicate leaked owner\n";
                assert(engine.rollbackTransaction()==DBStatus::OK);
            }
            assert(engine.beginTransaction(database)==DBStatus::OK);
            assert(engine.commitTransaction()==DBStatus::OK);
        }
        auto output=seed;size_t affected=77;size_t observed=0;std::string state;
        StorageEngine::SqlMutationCallbacks callbacks;
        callbacks.observed=[&](int64_t,const StorageEngine::SqlRow&,const StorageEngine::SqlRow&) {
            ++observed;
            assert(engine.insertRow(database,"audit",{{"id",std::to_string(observed)}})==DBStatus::OK);
            if(observed==2)throw DbError("22012","native DELETE observation failed");
        };
        try{(void)engine.removeRows(database,"target",{"likev '%'"},&output,{},&affected,&callbacks);}
        catch(const DbError& error){state=error.sqlState();}
        check("implicit observation SQLSTATE",state=="22012" && observed==2);
        check("implicit observation owner released",!engine.inTransaction());
        check("implicit observation output restored",output==seed && affected==0);
        check("implicit observation effects rolled back",snapshot(engine,"audit").first.empty());
        check("implicit observation NULL TOAST all rows",snapshot(engine,"target")==original);
        indexed(engine,original);
        if(engine.inTransaction()) {
            std::cerr<<"DELETE_OWNER_DIAGNOSTIC_CLEANUP observation leaked owner\n";
            assert(engine.rollbackTransaction()==DBStatus::OK);
        }
        assert(engine.beginTransaction(database)==DBStatus::OK);
        assert(engine.commitTransaction()==DBStatus::OK);

        for(const bool userSavepoint:{false,true}) {
            assert(engine.beginTransaction(database)==DBStatus::OK);
            assert(engine.updateRows(database,"target",{{"v",std::nullopt},{"payload",std::string(20000,'y')}},{"=id 3"})==DBStatus::OK);
            assert(engine.insertRow(database,"audit",{{"id",std::string("99")}})==DBStatus::OK);
            const auto parentData=snapshot(engine,"target");
            const auto parentAudit=snapshot(engine,"audit");
            if(userSavepoint)assert(engine.savepoint("caller_boundary")==DBStatus::OK);
            output=seed;affected=77;observed=0;state.clear();
            try{(void)engine.removeRows(database,"target",{"likev '%'"},&output,{},&affected,&callbacks);}
            catch(const DbError& error){state=error.sqlState();}
            check("parent original SQLSTATE",state=="22012" && observed==2);
            check("parent owner preserved",engine.inTransaction());
            check("parent output restored",output==seed && affected==0);
            check("parent earlier typed NULL TOAST writes retained",snapshot(engine,"target")==parentData);
            check("parent only statement effects rolled back",snapshot(engine,"audit")==parentAudit);
            indexed(engine,parentData);
            // Preserve the first full baseline's independent failures while
            // permitting the remaining original controls to reach terminal.
            if(snapshot(engine,"audit")!=parentAudit) {
                std::cerr<<"DELETE_OWNER_DIAGNOSTIC_CLEANUP parent statement effects\n";
                if(userSavepoint)assert(engine.rollbackToSavepoint("caller_boundary")==DBStatus::OK);
                else {
                    assert(engine.rollbackTransaction()==DBStatus::OK);
                    assert(engine.beginTransaction(database)==DBStatus::OK);
                    assert(engine.updateRows(database,"target",{{"v",std::nullopt},{"payload",std::string(20000,'y')}},{"=id 3"})==DBStatus::OK);
                    assert(engine.insertRow(database,"audit",{{"id",std::string("99")}})==DBStatus::OK);
                }
            }
            if(userSavepoint)assert(engine.releaseSavepoint("caller_boundary")==DBStatus::OK);
            assert(engine.commitTransaction()==DBStatus::OK);
            coldExpected=parentData;
            assert(engine.removeRows(database,"audit",{})==DBStatus::OK);
        }
        assert(!engine.inTransaction());
    }
    {
        StorageEngine reader;
        check("cold full typed data and TOAST",snapshot(reader,"target")==coldExpected);
        indexed(reader,coldExpected);
        check("cold audit effects absent",snapshot(reader,"audit").first.empty());
        assert(reader.beginTransaction(database)==DBStatus::OK);
        assert(reader.insertRow(database,"audit",{{"id",std::string("100")}})==DBStatus::OK);
        assert(reader.commitTransaction()==DBStatus::OK);
        check("next owner locks reusable",snapshot(reader,"audit").first.size()==1);
        assert(reader.dropDatabase(database)==DBStatus::OK);
    }
    std::cout<<"DELETE_OWNER_FAILURE_COUNT="<<failures.size()<<std::endl;
    assert(failures.empty());
}
