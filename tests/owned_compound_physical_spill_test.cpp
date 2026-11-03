// Owning compound datums: real prepared heap scans and sort spill lifecycle.
// test_link_flags: -Wl,--wrap=mkstemp,--wrap=pread,--wrap=pwrite
#include "executor/ExecutionPlan.h"
#include "commands/DdlExecutor.h"
#include "expression/prepared_query_execution.h"
#include "catalog/CatalogService.h"
#include "Config.h"
#include "Session.h"
#include "test_utils.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

namespace {
using namespace dbms;
using Row = std::vector<ExprValue>;
using Rows = std::vector<Row>;
constexpr size_t rowCount = 512;
constexpr size_t payloadWidth = 1024;
constexpr const char* table = "owned_spill_rows";
constexpr const char* domain = "owned_spill_int";

bool sortFile(int fd) {
    char link[64], target[4096];
    std::snprintf(link,sizeof(link),"/proc/self/fd/%d",fd);
    const auto size=::readlink(link,target,sizeof(target)-1);
    if(size<0)return false;
    target[size]='\0';
    return std::strstr(target,"/.dbms-sort-")!=nullptr;
}
size_t sortFds() {
    size_t result=0;
    for(const auto& entry:std::filesystem::directory_iterator("/proc/self/fd"))
        result+=sortFile(std::stoi(entry.path().filename().string()));
    return result;
}
bool clean() {
    if(sortFds())return false;
    for(const auto& entry:std::filesystem::directory_iterator("."))
        if(entry.path().filename().string().rfind(".dbms-sort-",0)==0)return false;
    return true;
}
// Actual syscall observations and public executing-node counters, not a
// replacement sort, allocator limit, private comparator hook, or timing test.
struct SortProfile {
    size_t creates=0,reads=0,writes=0,readBytes=0,writtenBytes=0,maxFds=0;
    void sample(){maxFds=std::max(maxFds,sortFds());}
} profile;

bool sameType(const DatumType* a,const DatumType* b) {
    if(!a || !b)return a==b;
    if(a->actualOid!=b->actualOid || a->codecOid!=b->codecOid || a->codecSize!=b->codecSize ||
       a->effectiveTypeMod!=b->effectiveTypeMod || a->kind!=b->kind || a->enumType!=b->enumType ||
       a->hasBinaryOutput!=b->hasBinaryOutput || a->recordFields.size()!=b->recordFields.size() ||
       !sameType(a->arrayElement.get(),b->arrayElement.get()))return false;
    for(size_t i=0;i<a->recordFields.size();++i)
        if(!sameType(&a->recordFields[i],&b->recordFields[i]))return false;
    return true;
}
bool same(const ExprValue& a,const ExprValue& b) {
    if(a.typeName!=b.typeName || a.value!=b.value || a.isNull!=b.isNull || a.collation!=b.collation ||
       a.objectOid!=b.objectOid || a.elementOids!=b.elementOids || bool(a.recordFields)!=bool(b.recordFields) ||
       bool(a.compound)!=bool(b.compound))return false;
    if(a.recordFields){if(a.recordFields->size()!=b.recordFields->size())return false;
        for(size_t i=0;i<a.recordFields->size();++i)if(!same(a.recordFields->at(i),b.recordFields->at(i)))return false;}
    if(!a.compound)return true;
    const auto& x=*a.compound;const auto& y=*b.compound;
    if(!sameType(x.type.get(),y.type.get()) || x.arrayNullBitmap!=y.arrayNullBitmap ||
       x.dimensions.size()!=y.dimensions.size() || bool(x.arrayElements)!=bool(y.arrayElements))return false;
    for(size_t i=0;i<x.dimensions.size();++i)
        if(x.dimensions[i].length!=y.dimensions[i].length || x.dimensions[i].lower!=y.dimensions[i].lower)return false;
    if(x.arrayElements){if(x.arrayElements->size()!=y.arrayElements->size())return false;
        for(size_t i=0;i<x.arrayElements->size();++i)if(!same(x.arrayElements->at(i),y.arrayElements->at(i)))return false;}
    return true;
}
bool same(const Rows& a,const Rows& b) {
    if(a.size()!=b.size())return false;
    for(size_t i=0;i<a.size();++i){if(a[i].size()!=b[i].size())return false;
        for(size_t j=0;j<a[i].size();++j)if(!same(a[i][j],b[i][j]))return false;}
    return true;
}
bool scalarType(const DatumType& type,uint32_t actual,uint32_t codec,int16_t size) {
    return type.actualOid==actual && type.codecOid==codec && type.codecSize==size &&
        type.kind==DatumKind::Scalar && !type.enumType && type.effectiveTypeMod==-1 && type.hasBinaryOutput;
}
bool recordType(const ExprValue& record,uint32_t domainOid,bool second) {
    if(record.isNull || record.typeName!="record" || !record.recordFields || record.recordFields->size()!=4 ||
       !record.compound || !record.compound->type)return false;
    const auto& type=*record.compound->type;
    if(type.actualOid!=2249 || type.codecOid!=2249 || type.kind!=DatumKind::Record ||
       type.codecSize!=-1 || !type.hasBinaryOutput || type.recordFields.size()!=4)return false;
    const auto& array=type.recordFields[0];
    return array.actualOid==1007 && array.codecOid==1007 && array.kind==DatumKind::Array &&
        array.arrayElement && scalarType(*array.arrayElement,23,23,4) &&
        scalarType(type.recordFields[1],20,20,8) && scalarType(type.recordFields[2],2206,2206,4) &&
        scalarType(type.recordFields[3],second?23:domainOid,23,4);
}
std::optional<std::string> storedArray(size_t id) {
    switch(id%4) {
        case 0:return {};
        case 1:return std::string("{}");
        case 2:return "[0:2]={"+std::to_string(id)+",NULL,-"+std::to_string(id)+"}";
        default:return "[-2:-1][4:5]={{"+std::to_string(id)+",0},{NULL,-"+std::to_string(id)+"}}";
    }
}
std::string payload(size_t id){return "physical-"+std::to_string(id)+"-"+std::string(payloadWidth,'a'+id%23);}
bool arrayValue(const ExprValue& array,size_t id) {
    const auto input=storedArray(id);
    if(array.typeName!="integer[]" || array.isNull!=!input)return false;
    // SQL NULL has no array element datum; its actual declaration remains in
    // the enclosing record layout. No bitmap is invented from NULL display.
    if(!input)return !array.compound || !array.compound->arrayElements;
    if(!array.compound || !array.compound->type || !array.compound->arrayElements)return false;
    const auto& type=*array.compound->type;
    if(type.actualOid!=1007 || type.codecOid!=1007 || type.kind!=DatumKind::Array ||
       !type.arrayElement || !scalarType(*type.arrayElement,23,23,4))return false;
    const auto& data=*array.compound;
    const auto& values=*data.arrayElements;
    if(id%4==1)return values.empty() && data.dimensions.empty() && data.arrayNullBitmap==false;
    if(data.arrayNullBitmap!=true)return false; // genuine stored NULL proves bitmap
    if(id%4==2) {
        if(data.dimensions.size()!=1 || data.dimensions[0].length!=3 || data.dimensions[0].lower!=0 || values.size()!=3)return false;
    } else if(data.dimensions.size()!=2 || data.dimensions[0].length!=2 || data.dimensions[0].lower!=-2 ||
              data.dimensions[1].length!=2 || data.dimensions[1].lower!=4 || values.size()!=4)return false;
    const size_t nullIndex=id%4==2?1:2;
    for(size_t i=0;i<values.size();++i) {
        const auto& value=values[i];
        if(value.typeName!="integer" || value.isNull!=(i==nullIndex) || value.objectOid)return false;
        if(!value.isNull) {
            const auto wanted=i==0?std::to_string(id):i+1==values.size()?"-"+std::to_string(id):"0";
            if(value.value!=wanted)return false;
        }
    }
    return true;
}
bool recordValue(const ExprValue& record,size_t id,uint32_t domainOid,bool second) {
    if(!recordType(record,domainOid,second))return false;
    const auto& f=*record.recordFields;
    return arrayValue(f[0],id) && f[1].typeName=="bigint" && f[1].isNull==second &&
        (second || f[1].value==std::to_string(id)) && f[2].typeName=="regtype" && !f[2].isNull &&
        f[2].objectOid==(second?20U:16U) && f[3].isNull;
}
bool rowValue(const Row& row,size_t id,uint32_t domainOid) {
    if(row.size()!=4 || row[0].typeName!="integer" || row[0].isNull || row[0].value!=std::to_string(id) ||
       row[3].typeName!="text" || row[3].isNull || row[3].value!=payload(id) ||
       !recordValue(row[1],id,domainOid,false))return false;
    const auto& array=row[2];
    if(array.typeName!="record[]" || array.isNull || !array.compound || !array.compound->type ||
       !array.compound->arrayElements || array.compound->arrayElements->size()!=2)return false;
    const auto& type=*array.compound->type;
    if(type.actualOid!=2287 || type.codecOid!=2287 || type.kind!=DatumKind::Array ||
       !type.arrayElement || type.arrayElement->actualOid!=2249 || type.arrayElement->codecOid!=2249 ||
       type.arrayElement->kind!=DatumKind::Record || array.compound->dimensions.size()!=1 ||
       array.compound->dimensions[0].length!=2 || array.compound->dimensions[0].lower!=1 ||
       array.compound->arrayNullBitmap!=false)return false;
    const auto& items=*array.compound->arrayElements;
    return recordValue(items[0],id,domainOid,false) && recordValue(items[1],id,domainOid,true) && same(row[1],items[0]);
}
std::vector<Operator*> graph(Operator* root) {
    std::vector<Operator*> result,pending{root};std::set<Operator*> visited;
    while(!pending.empty()) {
        auto* node=pending.back();pending.pop_back();if(!node || !visited.insert(node).second)continue;
        result.push_back(node);
        for(auto* child:node->preparedPlanChildren())pending.push_back(child);
        for(auto* child:node->children())if(auto* actual=dynamic_cast<Operator*>(child))pending.push_back(actual);
        if(auto* limit=dynamic_cast<LimitOp*>(node))pending.push_back(limit->child());
        if(auto* offset=dynamic_cast<OffsetOp*>(node))pending.push_back(offset->child());
    }
    return result;
}
struct Fixture {
    std::shared_ptr<PreparedQuery> query;
    std::unique_ptr<PreparedQueryCursor> cursor;
    Operator* sort=nullptr;
    TableScanOp* scan=nullptr;
    size_t sourceOrdinal=0;
};
std::string sql(const std::string& tail="ORDER BY id DESC,padding ASC") {
    const std::string record="ROW(a,id::bigint,'boolean'::regtype,NULL::owned_spill_int)";
    return "SELECT id,"+record+" AS r,ARRAY["+record+
        ",ROW(a,NULL::bigint,'bigint'::regtype,NULL::integer)] AS ar,padding FROM owned_spill_rows "+tail;
}
Fixture prepare(const std::string& db,const std::string& text) {
    Fixture f;f.query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,text));
    auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,f.query,f.query->ast.get());
    for(auto* node:graph(plan.get())) {
        if(node->preparedPlanNodeName()=="TypedSort") {if(f.sort)throw DbError("XX000","more than one actual TypedSort");f.sort=node;}
        if(auto* scan=dynamic_cast<TableScanOp*>(node)) {if(f.scan)throw DbError("XX000","more than one actual heap source");f.scan=scan;}
    }
    bool source=false;
    for(const auto& range:f.query->sourceRanges)if(range.owner==f.query->ast.get() && range.relationName==table) {
        if(source)throw DbError("XX000","ambiguous actual source range");source=true;f.sourceOrdinal=range.ordinal;
        if(range.columns.size()!=3)
            throw DbError("XX000","actual physical source width missing");
        // Keep the source's raw legacy attribute identity. For this genuine
        // INTEGER[] table the catalog stores the element atttypid plus
        // attndims, while typed output metadata resolves the actual array.
        const auto snapshot=g_engine.catalogService().metadataSnapshot(db);
        const auto ns=std::find_if(snapshot.namespaces.begin(),snapshot.namespaces.end(),
            [&](const auto& value){return value.nspname==range.relationSchema;});
        if(ns==snapshot.namespaces.end())throw DbError("XX000","actual source namespace missing");
        const auto relation=std::find_if(snapshot.relations.begin(),snapshot.relations.end(),
            [&](const auto& value){return value.relnamespace==ns->oid && value.relname==range.relationName;});
        if(relation==snapshot.relations.end())throw DbError("XX000","actual source relation missing");
        const auto attribute=std::find_if(snapshot.attributes.begin(),snapshot.attributes.end(),
            [&](const auto& value){return value.attrelid==relation->oid && value.attnum==2 &&
                !value.attisdropped && value.attname==range.columns[1].name;});
        if(attribute==snapshot.attributes.end() || attribute->attndims!=1 ||
           range.columns[1].typeOid!=attribute->atttypid)
            throw DbError("XX000","raw physical array attribute identity changed");
        const auto element=std::find_if(snapshot.types.begin(),snapshot.types.end(),
            [&](const auto& value){return value.oid==attribute->atttypid;});
        if(element==snapshot.types.end())throw DbError("XX000","actual array element catalog row missing");
        const auto array=std::find_if(snapshot.types.begin(),snapshot.types.end(),
            [&](const auto& value){return value.oid==element->typarray;});
        const auto& declaration=range.columns[1].typeMetadata;
        if(array==snapshot.types.end() || array->typelem!=element->oid ||
           !declaration || declaration->kind!=DatumKind::Array ||
           declaration->actualOid!=1007 || declaration->codecOid!=1007 ||
           declaration->actualOid!=array->oid || declaration->codecOid!=array->oid ||
           !declaration->arrayElement || declaration->arrayElement->actualOid!=23 ||
           declaration->arrayElement->codecOid!=23 || declaration->arrayElement->actualOid!=element->oid)
            throw DbError("XX000","actual array declaration/catalog pair missing");
    }
    if(!source || !f.scan)throw DbError("XX000","test requires genuine prepared physical scan");
    f.cursor=QueryPlanner::makePreparedCursor(std::move(plan),f.query->output);return f;
}
bool rowsGood(const Rows& rows,uint32_t domainOid,bool ascending=false,size_t offset=0,size_t expectedCount=rowCount) {
    if(rows.size()!=expectedCount)return false;
    for(size_t i=0;i<rows.size();++i) {
        const size_t id=ascending?i+offset+1:rowCount-i-offset;
        if(!rowValue(rows[i],id,domainOid))return false;
    }
    return true;
}
Rows consume(Fixture& f,bool inspectContext=true) {
    Rows rows;Row row;
    while(f.cursor->next(row)) {
        if(inspectContext) {
            RowContext actual;
            if(!f.sort || !f.sort->lastPreparedContext(actual) || actual.parameterCount()!=f.query->parameters.size() ||
               !row[1].recordFields || !same(actual.boundColumn(f.sourceOrdinal,1),row[1].recordFields->at(0)))
                throw DbError("XX000","sort lost the actual bound physical array/context snapshot");
        }
        rows.push_back(row);profile.sample();
    }
    return rows;
}
bool spilled(){return profile.creates>0 && profile.reads>0 && profile.writes>0 && profile.maxFds<=2;}
bool pure(const Fixture& f) {
    for(auto* node:graph(f.cursor->plan()))if(node->runtimeRows() || node->runtimeLoops())return false;
    return clean();
}
bool restored(const StorageEngine::NullRowBindingState& a,const StorageEngine::NullRowBindingState& b) {
    return a.engine==b.engine && a.database==b.database && a.table==b.table && a.rid==b.rid && a.natts==b.natts;
}
}

extern "C" int __real_mkstemp(char*);
extern "C" ssize_t __real_pread(int,void*,size_t,off_t);
extern "C" ssize_t __real_pwrite(int,const void*,size_t,off_t);
extern "C" int __wrap_mkstemp(char* name) {
    if(std::strncmp(name,".dbms-sort-",11))return __real_mkstemp(name);
    const int fd=__real_mkstemp(name);if(fd>=0){++profile.creates;profile.sample();}return fd;
}
extern "C" ssize_t __wrap_pread(int fd,void* data,size_t size,off_t offset) {
    const bool owns=sortFile(fd);const auto result=__real_pread(fd,data,size,offset);
    if(owns){++profile.reads;if(result>0)profile.readBytes+=size_t(result);profile.sample();}return result;
}
extern "C" ssize_t __wrap_pwrite(int fd,const void* data,size_t size,off_t offset) {
    const bool owns=sortFile(fd);const auto result=__real_pwrite(fd,data,size,offset);
    if(owns){++profile.writes;if(result>0)profile.writtenBytes+=size_t(result);profile.sample();}return result;
}

int main() {
    TypeRegistry::instance().bootstrap();cleanupTestDb("owned_compound_physical_spill");
    const auto db=testDbPath("owned_compound_physical_spill");
    if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);
    const auto originalBudget=g_config.workMemKb;g_config.workMemKb=64;session.workMem.assign(64,false);
    DdlExecutor ddl;
    bool setup=!ddl.executeSql("CREATE DOMAIN owned_spill_int AS integer",session) &&
        !ddl.executeSql("CREATE TABLE owned_spill_rows(id integer,a integer[],padding text COLLATE \"C\")",session);
    const auto catalog=g_engine.catalogService().acquire(db);
    const auto* nsp=catalog->findNamespaceByName("public");
    const auto type=nsp?catalog->getTypeByName(domain,nsp->oid):std::optional<PgTypeRow>{};
    setup=setup && type && type->typbasetype==23 && g_engine.beginTransaction(db)==DBStatus::OK;
    for(size_t id=1;setup && id<=rowCount;++id)
        setup=g_engine.insertRow(db,table,{{"id",std::to_string(id)},{"a",storedArray(id)},{"padding",payload(id)}})==DBStatus::OK;
    if(setup)setup=g_engine.commitTransaction()==DBStatus::OK;
    if(!setup){std::cerr<<"OWNED_COMPOUND_PHYSICAL_SPILL_SETUP_FAILED\n";g_config.workMemKb=originalBudget;setCurrentSession(previous);return 2;}
    const uint32_t domainOid=type->oid;
    size_t checked=0,failed=0;
    const auto check=[&](const char* label,const std::function<bool()>& action) {
        ++checked;profile={};session.workMem.assign(64,false);bool good=false;
        try{good=action();}catch(const DbError& error){std::cerr<<"SQLSTATE="<<error.sqlState()<<" message="<<error.message()<<'\n';}
        catch(const std::exception& error){std::cerr<<"exception="<<error.what()<<'\n';}
        good=good && clean() && StorageEngine::captureNullRowBinding().engine==nullptr;
        failed+=!good;
        std::cout<<"OWNED_COMPOUND_PHYSICAL_SPILL "<<checked<<' '<<label<<" good="<<good
            <<" creates="<<profile.creates<<" maxfds="<<profile.maxFds<<" reads="<<profile.reads
            <<" writes="<<profile.writes<<" readbytes="<<profile.readBytes<<" writtenbytes="<<profile.writtenBytes<<'\n';
    };
    check("metadata uses actual catalog/physical source without opening",[&] {
        auto f=prepare(db,sql());const auto& d=f.cursor->descriptor();
        return pure(f) && f.sort && f.cursor->supportsRestart() && d.size()==4 && d[0].typeOid==23 &&
            d[1].typeOid==2249 && d[2].typeOid==2287 && d[3].typeOid==25 && d[1].typeMetadata &&
            d[1].typeMetadata->recordFields.at(3).actualOid==domainOid && profile.creates==0;
    });
    Rows saved;
    check("low budget full physical ROW and record array spill",[&] {
        auto f=prepare(db,sql());saved=consume(f);
        const auto origin=f.scan->scanOrigin();
        const bool good=rowsGood(saved,domainOid) && spilled() && f.sort->runtimeRows()==rowCount &&
            f.sort->runtimeLoops()==rowCount+1 && f.scan->runtimeRows()==rowCount &&
            origin.engine==&g_engine && origin.dbname==db && origin.tablename==table;
        f.cursor->close();f.cursor->close();return good;
    });
    check("owning rows survive EOF close and graph destruction",[&] {return rowsGood(saved,domainOid);});
    check("early close releases live spilled tapes",[&] {
        auto f=prepare(db,sql());Row row;bool good=true;
        for(size_t i=0;i<7;++i)good=f.cursor->next(row) && rowValue(row,rowCount-i,domainOid) && good;
        good=good && spilled() && sortFds()>0 && f.sort->runtimeRows()==7;
        const auto reads=profile.reads,writes=profile.writes;
        f.cursor->close();f.cursor->close();return good && reads==profile.reads && writes==profile.writes;
    });
    check("same actual graph restarts complete typed source after early close",[&] {
        auto f=prepare(db,sql());auto* actualGraph=f.cursor->plan();Row row;
        if(!f.cursor->next(row) || !rowValue(row,rowCount,domainOid))return false;
        f.cursor->close();
        RowContext actualCaller; // this SQL has no parameters or outer source
        f.cursor->restart(actualCaller);const auto rows=consume(f);f.cursor->close();
        return f.cursor->plan()==actualGraph && same(rows,saved) && rowsGood(rows,domainOid) && spilled();
    });
    check("high budget control preserves exact owning metadata without tapes",[&] {
        session.workMem.assign(65536,false);auto f=prepare(db,sql());const auto rows=consume(f);f.cursor->close();
        session.workMem.assign(64,false);
        return rowsGood(rows,domainOid) && same(rows,saved) && profile.creates==0 && profile.reads==0 && profile.writes==0;
    });
    check("ascending multikey sort preserves physical lower bounds and row metadata",[&] {
        auto f=prepare(db,sql("ORDER BY id ASC,padding DESC"));const auto rows=consume(f);f.cursor->close();
        return rowsGood(rows,domainOid,true) && spilled();
    });
    check("bounded TopN OFFSET spills complete owning selected rows",[&] {
        auto f=prepare(db,sql("ORDER BY id DESC,padding ASC LIMIT 20 OFFSET 17"));
        const auto rows=consume(f);f.cursor->close();
        return rowsGood(rows,domainOid,false,17,20) && spilled() && f.scan->runtimeRows()==rowCount && f.sort->runtimeRows()==37;
    });
    check("LIMIT zero opens no real input or temporary tape",[&] {
        auto f=prepare(db,sql("ORDER BY id DESC,padding ASC LIMIT 0"));
        const auto rows=consume(f,false);f.cursor->close();
        return rows.empty() && profile.creates==0 && f.scan->runtimeRows()==0 && f.sort->runtimeRows()==0;
    });
    check("key error cleans compound spill and retains original diagnostic",[&] {
        // Bind a real caller heap tuple containing a stored SQL NULL, without
        // inventing a RID, source range, parameter frame or context provider.
        TableScanOp caller(&g_engine,db,table);std::string raw;
        if(!caller.open())return false;
        bool foundNull=false;
        while(caller.next(raw))if(caller.lastColumnIsNull(1)){foundNull=true;break;}
        if(!foundNull || caller.hasError())return false;
        const auto outer=StorageEngine::captureNullRowBinding();
        const auto origin=caller.scanOrigin();
        bool good=outer.engine==&g_engine && outer.database==db && outer.table==table && outer.natts==3 &&
            origin.engine==outer.engine && origin.dbname==outer.database && origin.tablename==outer.table && origin.rid==outer.rid;
        auto f=prepare(db,sql("ORDER BY 1/(id-257),id"));Row row;bool exact=false;
        try{(void)f.cursor->next(row);}catch(const DbError& error){
            exact=error.sqlState()=="22012" && error.message()=="division by zero";
        }
        // Check before ANY caller close: PreparedOperatorCursor's catch has
        // already closed its actual plan and rethrown the primary exception.
        good=exact && row.empty() && profile.creates>0 && profile.writes>0 && clean() &&
            restored(outer,StorageEngine::captureNullRowBinding()) && good;
        const auto creates=profile.creates,reads=profile.reads,writes=profile.writes;
        const auto readBytes=profile.readBytes,writtenBytes=profile.writtenBytes;
        const auto loops=f.sort->runtimeLoops(),sourceRows=f.scan->runtimeRows();
        bool terminal=false;
        try{(void)f.cursor->next(row);}catch(const DbError& error){
            // The public cursor's closed-state check precedes any plan next:
            // a failed cursor throws XX000, rather than reporting normal EOF.
            terminal=error.sqlState()=="XX000" && error.message()=="prepared cursor has already been closed";
        }
        good=terminal && row.empty() && clean() && restored(outer,StorageEngine::captureNullRowBinding()) &&
            creates==profile.creates && reads==profile.reads && writes==profile.writes &&
            readBytes==profile.readBytes && writtenBytes==profile.writtenBytes &&
            loops==f.sort->runtimeLoops() && sourceRows==f.scan->runtimeRows() && good;
        f.cursor->close();f.cursor->close();
        good=clean() && restored(outer,StorageEngine::captureNullRowBinding()) &&
            creates==profile.creates && reads==profile.reads && writes==profile.writes &&
            readBytes==profile.readBytes && writtenBytes==profile.writtenBytes && good;
        caller.close();caller.close();return good;
    });
    check("destructor releases early-open tape while caller keeps owning row",[&] {
        Row row;
        {auto f=prepare(db,sql());if(!f.cursor->next(row) || !spilled() || !sortFds())return false;}
        return rowValue(row,rowCount,domainOid);
    });
    check("actual outer heap NULL binding restored by sort/project close",[&] {
        TableScanOp caller(&g_engine,db,table);std::string raw;
        if(!caller.open() || !caller.next(raw))return false;
        const auto outer=StorageEngine::captureNullRowBinding();
        bool good=outer.engine==&g_engine && outer.database==db && outer.table==table;
        {auto f=prepare(db,sql());Row row;good=f.cursor->next(row) && rowValue(row,rowCount,domainOid) && good;
            f.cursor->close();good=restored(outer,StorageEngine::captureNullRowBinding()) && good;}
        caller.close();return good;
    });
    saved.clear();
    const bool dropped=g_engine.dropDatabase(db)==DBStatus::OK;
    g_config.workMemKb=originalBudget;setCurrentSession(previous);
    if(checked!=12 || !dropped)++failed;
    std::cout<<"OWNED_COMPOUND_PHYSICAL_SPILL_COMPLETE "<<checked<<" FAILURES "<<failed<<'\n';
    return failed?1:0;
}
