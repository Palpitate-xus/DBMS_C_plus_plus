// Covers actual heap generated-input identity validation across sort spill records.
#include "commands/DdlExecutor.h"
#include "executor/TypedSortSpill.h"
#include "expression/prepared_query_execution.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <array>
#include <cstdio>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
struct Temporary {
    int fd=-1;
    Temporary() {char name[]="/tmp/generated-context-spill-XXXXXX";fd=::mkstemp(name);
        if(fd<0)throw DbError("58030","fixture could not open its private codec file");
        if(::unlink(name)){::close(fd);fd=-1;throw DbError("58030","fixture could not unlink its private codec file");}}
    ~Temporary(){if(fd>=0)::close(fd);}
};
struct Frame {uint64_t offset=0,source=0,column=0,id=0;};
struct Written {
    Temporary file;
    typed_sort_detail::GeneratedRegistry registry;
    uint64_t extent=0;
    void write(const RowContext& row) {
        sort_detail::Writer writer(file.fd,4096);
        typed_sort_detail::writeContext(writer,row,&registry);writer.flush();extent=writer.position();
    }
    RowContext read() const {
        sort_detail::Reader reader(file.fd,0,extent,4096);
        auto row=typed_sort_detail::readContext(reader,&registry);
        if(reader.remaining())throw DbError("XX000","fixture codec did not consume its actual record");
        return row;
    }
    // Inspect only private framing to locate the genuine serialized identities.
    // Evaluated values and expected generated results are never inferred here.
    std::vector<Frame> frames() const {
        sort_detail::Reader reader(file.fd,0,extent,4096);
        const auto h=reader.scalar<typed_sort_detail::ContextHeader>();
        if(h.sql || h.values || h.parameters || h.generatedNamed)
            throw DbError("XX000","fixture unexpectedly contains a non-heap context lane");
        for(uint64_t i=0;i<h.bound;++i) {
            (void)reader.scalar<uint64_t>();(void)reader.scalar<uint64_t>();
            (void)typed_sort_detail::readValue(reader);
        }
        std::vector<Frame> result;
        for(uint64_t i=0;i<h.generatedBound;++i) {
            Frame frame;frame.offset=extent-reader.remaining();
            frame.source=reader.scalar<uint64_t>();frame.column=reader.scalar<uint64_t>();frame.id=reader.scalar<uint64_t>();
            (void)reader.scalar<uint8_t>();result.push_back(frame);
        }
        if(reader.remaining())throw DbError("XX000","fixture context framing has an unexpected tail");
        return result;
    }
    void replace64(uint64_t offset,uint64_t value) {
        sort_detail::writeAt(file.fd,offset,reinterpret_cast<const char*>(&value),sizeof(value));
    }
};
bool value(const ExprValue& cell,const char* expected) {
    return !cell.isNull && cell.typeName=="integer" && cell.value==expected &&
        !cell.objectOid && cell.elementOids.empty() && !cell.recordFields && !cell.compound;
}
bool beforeUse58030(const std::function<RowContext()>& read) {
    bool reachedUse=false;std::string state;
    try{auto row=read();reachedUse=true;(void)row;}catch(const DbError& e){state=e.sqlState();}
    return !reachedUse && state=="58030";
}
}
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();cleanupTestDb("generated_context_spill");
    const auto db=testDbPath("generated_context_spill");
    if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* prior=currentSession();setCurrentSession(&session);
    struct Restore {Session* prior;~Restore(){setCurrentSession(prior);}} restore{prior};
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE gcs_heap(id integer,v integer GENERATED ALWAYS AS(id+1) VIRTUAL,w integer GENERATED ALWAYS AS(id+2) VIRTUAL)",session))return 2;
    if(g_engine.insertRow(db,"gcs_heap",{{"id","5"}})!=DBStatus::OK)return 2;
    const std::string sql="SELECT l.v,l.w,r.v FROM gcs_heap AS l JOIN gcs_heap AS r ON l.id=r.id";
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
    auto execution=std::make_shared<PreparedQueryExecution>(query,&g_engine,db);
    const auto schema=g_engine.getTableSchema(db,"gcs_heap");
    std::vector<size_t> sources;
    for(const auto& source:query->sourceRanges)
        if(source.relationName=="gcs_heap")sources.push_back(source.ordinal);
    if(sources.size()!=2 || schema.len!=3)return 2;
    const auto column=[&](size_t source,const std::string& name) {
        const auto& columns=execution->sourceRange(source).columns;
        for(size_t i=0;i<columns.size();++i)if(columns[i].name==name)return i;
        throw DbError("XX000","actual bound heap attribute missing");
    };
    const auto left=sources[0],right=sources[1],id=column(left,"id"),v=column(left,"v"),w=column(left,"w");
    if(column(right,"id")!=id || column(right,"v")!=v || column(right,"w")!=w)return 2;
    RowContext actual=execution->context();bool saw=false;
    const bool scanned=g_engine.forEachVisibleRow(db,"gcs_heap","SELECT",[&](uint32_t page,uint16_t slot,const char* bytes,size_t length) {
        if(saw)throw DbError("XX000","fixture requires exactly one actual heap row");saw=true;
        const std::string raw(bytes,length);const auto rid=StorageEngine::encodeRid(page,slot);
        for(const auto source:sources) {
            std::vector<ExprValue> cells;
            for(size_t i=0;i<schema.len;++i) {
                if(schema.cols[i].generatedKind=='v'){cells.emplace_back();continue;}
                const auto null=g_engine.isColumnNullByRid(db,"gcs_heap",rid,i);
                cells.emplace_back(execution->sourceRange(source).columns[i].type,
                    null?std::string{}:g_engine.extractColumnValue(raw,schema,i,db,false),null);
            }
            execution->setSourceInputs(actual,source,cells,true);
        }
    });
    if(!scanned || !saw)return 2;
    const auto readV=execution->generatedSourceRead(left,v),readW=execution->generatedSourceRead(left,w),readRight=execution->generatedSourceRead(right,v);
    ExprEvaluator evaluator;evaluator.setCurrentDB(db);
    size_t count=0,failed=0;
    const auto check=[&](const std::string& label,const std::function<bool()>& body) {
        ++count;bool good=false;
        try{good=body();}catch(const DbError& e){std::cerr<<"SQLSTATE="<<e.sqlState()<<" message="<<e.message()<<'\n';}
        catch(const std::exception& e){std::cerr<<e.what()<<'\n';}
        failed+=!good;std::cout<<"GENERATED_CONTEXT_SPILL "<<count<<' '<<label<<" good="<<good<<'\n';
    };
    Written written;written.write(actual);
    check("actual heap metadata and full two-occurrence roundtrip",[&] {
        auto decoded=written.read();
        return readV && readW && readRight && readV!=readW && readV!=readRight &&
            readV->program->target.dataName=="v" && readV->sourceOrdinal==left &&
            execution->sourceRange(left).columns[v].typeOid==23 && written.frames().size()==4 &&
            value(evaluator.readBoundColumn(left,v,decoded),"6") &&
            value(evaluator.readBoundColumn(left,w,decoded),"7") &&
            value(evaluator.readBoundColumn(right,v,decoded),"6");
    });
    // A run's registry legitimately holds reads from previous actual rows.
    // Keep those real registered recipes, then serialize one demanded recipe
    // with the same actual tuple's physical cells; no synthetic program/owner.
    RowContext one=execution->context();
    for(const auto source:sources)for(size_t i=0;i<schema.len;++i)
        if(schema.cols[i].generatedKind!='v')one.setBoundColumn(source,i,actual.boundColumn(source,i));
    one.setGeneratedBound(left,v,readV,true);
    written.write(one);const auto frames=written.frames();if(frames.size()!=1)return 2;
    const auto frame=frames.front();
    const auto realId=[&](const GeneratedReadPtr& read) {
        const auto found=std::find(written.registry.reads.begin(),written.registry.reads.end(),read);
        if(found==written.registry.reads.end())throw DbError("XX000","writer omitted a genuine recipe");
        return uint64_t(found-written.registry.reads.begin());
    };
    check("valid selected-recipe record preserves its actual value",[&] {
        return frame.source==left && frame.column==v && frame.id==realId(readV) &&
            value(evaluator.readBoundColumn(left,v,written.read()),"6");
    });
    check("absent genuine occurrence preserves NULL without evaluation",[&] {
        auto absent=one;absent.setGeneratedBound(left,v,readV,false);written.write(absent);
        const auto result=evaluator.readBoundColumn(left,v,written.read());
        return result.isNull && result.typeName=="integer";
    });
    const auto corrupted=[&](uint64_t displacement,uint64_t replacement) {
        written.write(one);written.replace64(frame.offset+displacement,replacement);
        return beforeUse58030([&]{return written.read();});
    };
    check("serialized source changed to another real bound occurrence rejects before use",[&] {return corrupted(0,right);});
    check("serialized column changed to real physical id rejects before use",[&] {return corrupted(sizeof(uint64_t),id);});
    check("valid registry id for another generated attribute rejects before use",[&] {return corrupted(2*sizeof(uint64_t),realId(readW));});
    check("valid registry id for another real occurrence rejects before use",[&] {return corrupted(2*sizeof(uint64_t),realId(readRight));});
    check("out-of-range serialized registry id retains strict 58030",[&] {return corrupted(2*sizeof(uint64_t),written.registry.reads.size());});
    written.write(one);auto detached=written.read();
    check("decoded context owns its recipes after registry release",[&] {
        written.registry.reads.clear();return value(evaluator.readBoundColumn(left,v,detached),"6");
    });
    std::weak_ptr<PreparedQuery> queryLife=query;
    check("actual decoded recipe does not retain the parent query",[&] {
        execution.reset();query.reset();return queryLife.expired() && value(evaluator.readBoundColumn(left,v,detached),"6");
    });
    const bool dropped=g_engine.dropDatabase(db)==DBStatus::OK;cleanupTestDb("generated_context_spill");
    std::cout<<"GENERATED_CONTEXT_SPILL_TERMINAL controls="<<count<<" expected=10 failures="<<failed<<" dropped="<<dropped<<'\n';
    return count==10 && !failed && dropped?0:1;
}
