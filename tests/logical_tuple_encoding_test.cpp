#include "replication/LogicalDecoder.h"
#include "replication/ReplicationManager.h"
#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    using SqlRow = StorageEngine::SqlRow;
    const std::string db = testDbPath("logical_tuple_encoding");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session session;
    session.currentDB = db; session.username = "admin"; session.permission = 1;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE tuples(id INT PRIMARY KEY,a TEXT,b TEXT)",session));
    Publication publication;
    publication.name = "tuple_pub"; publication.owner = "admin";
    publication.tables = {"tuples"};
    std::string error;
    assert(PublicationCatalog::instance().create(db,publication,error));
    auto& replication = ReplicationManager::instance();
    const std::string slot = "tuple_encoding_slot";
    assert(replication.createReplicationSlot(slot,"logical","dbms_test_decoding",db));
    auto& store = LogicalChangeStore::instance();
    const std::string large(20000,'z');
    struct Case { std::optional<std::string> a,b; std::string json; };
    const std::vector<Case> cases = {
        {std::nullopt,"x","null,\"x\""}, {"","x","\"\",\"x\""},
        {"a|b","c","\"a|b\",\"c\""}, {"a","b|c","\"a\",\"b|c\""},
        {"NULL",std::nullopt,"\"NULL\",null"}, {"null","","\"null\",\"\""},
        {"a\"b","c\\d","\"a\\\"b\",\"c\\\\d\""},
        {"line\nnext","\r\t","\"line\\nnext\",\"\\r\\t\""},
        {"中文","é","\"中文\",\"é\""},
        {large,"tail|","\""+large+"\",\"tail|\""},
        {std::nullopt,std::nullopt,"null,null"}, {"","","\"\",\"\""}
    };
    int checks=0,failures=0;
    const auto check = [&](bool passed,const std::string& label) {
        ++checks;
        if(!passed){++failures;std::cerr<<"[LOGICAL TUPLE] FAILED "<<label<<'\n';}
    };
    const auto expected = [](size_t id,const std::string& cells) {
        bool quoted=false, escaped=false; size_t separator=std::string::npos;
        for(size_t i=0;i<cells.size();++i) {
            if(escaped){escaped=false;continue;}
            if(quoted && cells[i]=='\\'){escaped=true;continue;}
            if(cells[i]=='"')quoted=!quoted;
            if(cells[i]==',' && !quoted){separator=i;break;}
        }
        assert(separator!=std::string::npos);
        return "DBMS_TUPLE_V1:[[\"int\",\""+std::to_string(id)+"\"],[\"text\","+
            cells.substr(0,separator)+"],[\"text\","+cells.substr(separator+1)+"]]";
    };
    check(LogicalDecoder::encodeRow({})=="DBMS_TUPLE_V1:[]","zero-width tuple");
    check(LogicalDecoder::encodeRow({{"text",std::nullopt},{"text",""},{"text","NULL"}})==
        "DBMS_TUPLE_V1:[[\"text\",null],[\"text\",\"\"],[\"text\",\"NULL\"]]","null/empty/literal NULL codec");
    check(LogicalDecoder::encodeRow({{"numeric","1.00"},{"bigint","9223372036854775807"}})==
        "DBMS_TUPLE_V1:[[\"numeric\",\"1.00\"],[\"bigint\",\"9223372036854775807\"]]","declared numeric cells");
    check(LogicalDecoder::encodeRow({{"ty\"pe",std::string("\0\1\b\f\n\r\t",7)}})==
        "DBMS_TUPLE_V1:[[\"ty\\\"pe\",\"\\u0000\\u0001\\b\\f\\n\\r\\t\"]]","control bytes and declaration escaping");
    const auto oneChange = [&]() {
        const auto peek=store.peek(slot,0,100);
        assert(peek.batches.size()==1 && peek.batches[0].changes.size()==1);
        return peek.batches[0];
    };
    const auto acknowledge = [&](const LogicalChangeBatch& batch) {
        store.acknowledge(slot,batch.commitLsn);
        assert(store.depth(slot)==0);
    };
    for(size_t i=0;i<cases.size();++i) {
        const size_t id=i+1;
        const SqlRow row={{"id",std::to_string(id)},{"a",cases[i].a},{"b",cases[i].b}};
        assert(g_engine.insertRow(db,"tuples",row)==DBStatus::OK);
        auto batch=oneChange(); const auto& change=batch.changes[0];
        const std::string image=expected(id,cases[i].json);
        check(change.op==LogicalChange::Op::Insert && change.oldRow.empty() && change.newRow==image,"insert "+std::to_string(id));
        std::string readable,binary;
        assert(LogicalDecoder::format("dbms_test_decoding",batch,readable));
        assert(LogicalDecoder::format("dbms_pgoutput_preview",batch,binary));
        check(readable.find(" INSERT: "+image+" (xid ")!=std::string::npos &&
            readable.find('\n')==readable.size()-1,"readable "+std::to_string(id));
        check(binary.find(image)!=std::string::npos,"binary "+std::to_string(id));
        acknowledge(batch);
        const SqlRow updates={{"a",cases[(i+1)%cases.size()].a},{"b",cases[(i+1)%cases.size()].b}};
        assert(g_engine.updateRows(db,"tuples",updates,{"=id "+std::to_string(id)})==DBStatus::OK);
        batch=oneChange();
        const std::string replacement=expected(id,cases[(i+1)%cases.size()].json);
        check(batch.changes[0].op==LogicalChange::Op::Update && batch.changes[0].oldRow==image &&
            batch.changes[0].newRow==replacement,"update "+std::to_string(id));
        acknowledge(batch);
        assert(g_engine.remove(db,"tuples",{"=id "+std::to_string(id)})==DBStatus::OK);
        batch=oneChange();
        check(batch.changes[0].op==LogicalChange::Op::Delete && batch.changes[0].oldRow==replacement &&
            batch.changes[0].newRow.empty(),"delete "+std::to_string(id));
        acknowledge(batch);
    }
    const SqlRow special={{"id","99"},{"a",std::nullopt},{"b","a|\"b\\c\n"}};
    assert(g_engine.beginTransaction(db)==DBStatus::OK);
    assert(g_engine.insertRow(db,"tuples",special)==DBStatus::OK);
    check(store.depth(slot)==0,"no uncommitted publication");
    assert(g_engine.rollbackTransaction()==DBStatus::OK);
    check(store.depth(slot)==0,"rollback discards encoded tuple");
    assert(g_engine.beginTransaction(db)==DBStatus::OK);
    assert(g_engine.insertRow(db,"tuples",special)==DBStatus::OK);
    assert(g_engine.prepareTransaction("tuple_encoding_prepared")==DBStatus::OK);
    check(store.depth(slot)==0,"prepare is not publication");
    StorageEngine completingBackend;
    assert(completingBackend.commitPrepared("tuple_encoding_prepared")==DBStatus::OK);
    auto prepared=oneChange();
    check(prepared.changes[0].newRow==expected(99,"null,\"a|\\\"b\\\\c\\n\""),"prepared commit preserves tuple");
    acknowledge(prepared);
    assert(!ddl.executeSql("CREATE TABLE generated_tuples(id INT PRIMARY KEY,a TEXT,n INT GENERATED ALWAYS AS(length(a)) VIRTUAL,v TEXT GENERATED ALWAYS AS(coalesce(a,'fallback')) VIRTUAL)",session));
    publication.tables.push_back("generated_tuples");
    assert(PublicationCatalog::instance().update(db,publication,error));
    const std::vector<std::optional<std::string>> sources={std::nullopt,"","x|"};
    const std::vector<std::string> generatedImages={
        "DBMS_TUPLE_V1:[[\"int\",\"1\"],[\"text\",null],[\"int\",null],[\"text\",\"fallback\"]]",
        "DBMS_TUPLE_V1:[[\"int\",\"2\"],[\"text\",\"\"],[\"int\",\"0\"],[\"text\",\"\"]]",
        "DBMS_TUPLE_V1:[[\"int\",\"3\"],[\"text\",\"x|\"],[\"int\",\"2\"],[\"text\",\"x|\"]]"
    };
    for(size_t i=0;i<sources.size();++i) {
        const std::string id=std::to_string(i+1);
        assert(g_engine.insertRow(db,"generated_tuples",{{"id",id},{"a",sources[i]}})==DBStatus::OK);
        auto batch=oneChange();
        check(batch.changes[0].newRow==generatedImages[i],"virtual insert "+id);
        acknowledge(batch);
        assert(g_engine.updateRows(db,"generated_tuples",{{"a",sources[i]}},{"=id "+id})==DBStatus::OK);
        batch=oneChange();
        check(batch.changes[0].oldRow==generatedImages[i] && batch.changes[0].newRow==generatedImages[i],"virtual update "+id);
        acknowledge(batch);
        assert(g_engine.remove(db,"generated_tuples",{"=id "+id})==DBStatus::OK);
        batch=oneChange();
        check(batch.changes[0].oldRow==generatedImages[i],"virtual delete "+id);
        acknowledge(batch);
    }
    assert(replication.dropReplicationSlot(slot));
    assert(g_engine.dropDatabase(db)==DBStatus::OK);
    std::cout<<"[LOGICAL TUPLE] checks="<<checks<<" failures="<<failures<<'\n';
    return failures?1:0;
}
