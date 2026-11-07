#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/DatumMetadataFactory.h"
#include "expression/StoredRegtypeValue.h"
#include "network/PostgresProtocol.h"
#include "test_utils.h"
#include <cerrno>
#include <iostream>
#include <sys/socket.h>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
using Bytes=std::vector<uint8_t>;
unsigned frames=0;
void need(bool value,const std::string& reason){if(!value)throw std::runtime_error(reason);}
void append32(Bytes& bytes,uint32_t value){for(int shift=24;shift>=0;shift-=8)bytes.push_back(uint8_t(value>>shift));}
Bytes integer(uint32_t value){Bytes out;append32(out,value);return out;}
Bytes array(uint32_t oid,int32_t lower) {
    Bytes out;
    for(uint32_t value:{1U,1U,2205U,3U,uint32_t(lower),4U,oid,4U,0U,UINT32_MAX})append32(out,value);
    return out;
}
void frame(const ExprValue& value,DatumTypePtr type,int16_t format,const Bytes& cell,bool accepted=true) {
    ++frames;int descriptors[2];need(!::socketpair(AF_UNIX,SOCK_STREAM,0,descriptors),"actual socketpair");
    SecureSocket sender(descriptors[0]),receiver(descriptors[1]);timeval timeout{2,0};
    need(!::setsockopt(receiver.fd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout)),"bounded socket read");
    PostgresProtocol protocol(sender);PgColumnDescription column;
    column.name="reference";column.typeOid=type->actualOid;column.typeSize=type->codecSize;
    column.typeMetadata=std::move(type);column.formatCode=format;
    std::vector<ExprValue> actual{value};
    const bool sent=protocol.sendDataRow({value.value},{column},{value.isNull},&actual);
    need(sent==accepted,"actual owned data-row send result");
    if(!accepted) {
        uint8_t byte=0;const auto count=::recv(receiver.fd,&byte,1,MSG_DONTWAIT);
        need(count==-1 && (errno==EAGAIN || errno==EWOULDBLOCK),"rejected datum emits no partial frame");return;
    }
    Bytes body{0,1};append32(body,value.isNull?UINT32_MAX:uint32_t(cell.size()));
    if(!value.isNull)body.insert(body.end(),cell.begin(),cell.end());
    Bytes expected{'D'};append32(expected,uint32_t(body.size()+4));expected.insert(expected.end(),body.begin(),body.end());
    Bytes actualBytes(expected.size());const auto count=::recv(receiver.fd,actualBytes.data(),actualBytes.size(),MSG_WAITALL);
    need(count==ssize_t(expected.size()) && actualBytes==expected,"complete binary frame, actual OID and NULL bytes");
}
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();g_engine.setBackgroundIntervals(60000,60000);
        const auto database=testDbPath("regclass_binary_value");cleanupTestDb("regclass_binary_value");
        need(g_engine.createDatabase(database)==DBStatus::OK,"actual database");
        Session session;session.username="admin";session.permission=1;session.currentDB=database;
        struct Scope {Session* previous=currentSession();Scope(Session& value){setCurrentSession(&value);}~Scope(){setCurrentSession(previous);}} scope(session);
        DdlExecutor ddl;need(!ddl.executeSql("CREATE TABLE binary_reference_target(k integer)",session),"actual relation");
        const auto metadata=g_engine.catalogService().metadataSnapshot(database);
        const auto relation=std::find_if(metadata.relations.begin(),metadata.relations.end(),
            [](const auto& row){return row.relname=="binary_reference_target" && row.relnamespace==2200;});
        need(relation!=metadata.relations.end(),"actual class OID control");const auto oid=relation->oid;
        DatumMetadataFactory factory(metadata);const auto scalar=factory.type(2205),arrayType=factory.type(2210);
        need(scalar && scalar->actualOid==2205 && scalar->codecSize==4 && scalar->hasBinaryOutput &&
            arrayType && arrayType->kind==DatumKind::Array && arrayType->arrayElement->actualOid==2205,
            "actual builtin scalar and array send declarations");
        const auto evaluate=[&](const std::string& expression) {
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+expression);
            need(parsed.success,"actual expression parser");auto* select=dynamic_cast<SelectStmt*>(parsed.stmt.get());
            ExprEvaluator evaluator;evaluator.setCurrentDB(database);
            evaluator.bindScalarFunctions(select->selectList[0].expr.get(),&g_engine);
            return evaluator.eval(select->selectList[0].expr.get(),RowContext{});
        };
        for(uint32_t value:{0U,1U,127U,65535U,2147483647U,2147483648U,4294967294U,UINT32_MAX,oid}) {
            const auto actual=evaluate(std::to_string(value)+"::regclass");
            need(actual.objectOid==value,"actual evaluated physical identity");
            frame(actual,scalar,1,integer(value));frame(actual,scalar,0,Bytes(actual.value.begin(),actual.value.end()));
        }
        for(int32_t lower:{1,-2}) {
            const auto input="["+std::to_string(lower)+":"+std::to_string(lower+2)+"]={"+std::to_string(oid)+",0,NULL}";
            const auto actual=stored_regtype_detail::load(input,true,false,metadata,&session,arrayType,"regclass");
            frame(actual,arrayType,1,array(oid,lower));
        }
        Bytes emptyArray;for(uint32_t value:{0U,0U,2205U})append32(emptyArray,value);
        frame(evaluate("'{}'::regclass[]"),arrayType,1,emptyArray);
        frame(evaluate("NULL::regclass"),scalar,1,{});
        auto lost=evaluate("'binary_reference_target'::regclass");lost.objectOid.reset();frame(lost,scalar,1,{},false);
        auto lostArray=evaluate("ARRAY['binary_reference_target'::regclass]");lostArray.compound.reset();lostArray.elementOids.clear();
        frame(lostArray,arrayType,1,{},false);
        const auto original=evaluate("'binary_reference_target'::regclass");
        need(!ddl.executeSql("ALTER TABLE binary_reference_target RENAME TO binary_reference_moved",session),"actual rename");
        frame(original,scalar,1,integer(oid));
        need(frames==25,"all finite protocol value controls reached");
        std::cout<<"REGCLASS_BINARY_VALUE_FRAMES="<<frames<<" FAILED=0\n";return 0;
    } catch(const std::exception& error) {std::cerr<<"REGCLASS_BINARY_VALUE_FRAMES="<<frames<<" FAIL="<<error.what()<<'\n';return 1;}
}
