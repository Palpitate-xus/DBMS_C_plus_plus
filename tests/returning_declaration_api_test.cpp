#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/query_binding.h"
#include "parser/parser.h"
#include <tuple>
#include "test_utils.h"
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
struct Decl { QueryRowDescriptor output; std::vector<QueryReturningOrigin> origins; std::string state; };
bool types(const Decl& value,const std::vector<uint32_t>& expected) {
    if(!value.state.empty() || value.output.size()!=expected.size() || value.origins.size()!=expected.size())return false;
    for(size_t i=0;i<expected.size();++i)if(value.output[i].typeOid!=expected[i] || !value.output[i].typeMetadata ||
        value.output[i].typeMetadata->actualOid!=expected[i])return false;
    return true;
}
bool origin(const Decl& value,size_t i,uint32_t relation,uint16_t column) {
    return i<value.origins.size() && value.origins[i].relationOid==relation && value.origins[i].attributeNumber==column;
}
}
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="returning_declaration_api",db=testDbPath(name);
    cleanupTestDb(name);
    if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)throw std::runtime_error("fixture database failed");
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    DdlExecutor ddl;
    const auto create=[&](const std::string& sql) {
        if(ddl.executeSql(sql,session))throw std::runtime_error("fixture DDL failed: "+sql);
    };
    create("CREATE SEQUENCE decl_effects START 1");
    create("CREATE TABLE rd_target(id integer DEFAULT nextval('decl_effects'),padding text,a integer[])");
    create("CREATE TABLE rd_aux(id integer,n numeric(12,3))");
    create("CREATE TABLE rd_other(id integer,v text)");
    if(g_engine.createUDF(db,"decl_function",{}, {},"BEGIN PERFORM nextval('decl_effects'); RETURN 1; END;",'v',"plpgsql","integer")!=DBStatus::OK)
        throw std::runtime_error("fixture actual function failed");
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{currentSession()};
    setCurrentSession(&session);g_engine.setval(db,"decl_effects",1);
    const auto catalog=g_engine.catalogService().metadataSnapshot(db);
    const auto relation=[&](const std::string& name) {
        uint32_t schema=0;for(const auto& item:catalog.namespaces)if(item.nspname=="public")schema=item.oid;
        for(const auto& item:catalog.relations)if(item.relnamespace==schema && item.relname==name)return item.oid;
        throw std::runtime_error("fixture actual catalog identity missing");
    };
    const auto target=relation("rd_target"),aux=relation("rd_aux");
    const auto describe=[&](const std::string& sql,const std::vector<QueryBindingDatum>& inputs=std::vector<QueryBindingDatum>{}) {
        Decl result;
        try {result.output=g_engine.prepareReturningDeclaration(db,sql,inputs,&result.origins);}
        catch(const DbError& error){result.state=error.sqlState();}
        return result;
    };
    // Scope continuity, not a PG dynamic-count execution assertion: the
    // ordinary parser/whole-binding contract must not inherit this private
    // metadata purpose from either a successful or failed API invocation.
    const std::string ordinaryCount="SELECT id FROM rd_aux LIMIT 1/0";
    const auto ordinarySyntax=[&](bool binding) {
        SQLParser parser;auto result=binding?parser.parseForBinding(ordinaryCount):parser.parse(ordinaryCount);
        return std::make_tuple(result.success,result.error,result.sqlState,result.rowCountSyntaxError,
            bool(result.stmt),result.originalSql,result.command());
    };
    const auto ordinaryWhole=[&] {
        try {(void)g_engine.prepareBoundQuery(db,ordinaryCount);return std::string{};}
        catch(const DbError& error){return error.sqlState();}
    };
    const auto originalParse=ordinarySyntax(false),originalBindingParse=ordinarySyntax(true);
    const auto originalWhole=ordinaryWhole();
    size_t controls=0;std::vector<std::string> failed;
    const auto check=[&](const std::string& name,bool good) {
        ++controls;std::cout<<"RETURNING_DECLARATION_CONTROL "<<controls<<' '<<name<<' '<<(good?"PASS":"FAIL")<<'\n';
        if(!good)failed.push_back(name);
    };
    auto d=describe("INSERT INTO rd_target VALUES('bad'::integer,missing_input_function(),NULL) RETURNING WITH(OLD AS o,NEW AS n) o.id,n.id");
    check("INSERT input never bound",types(d,{23,23}) && origin(d,0,target,1) && origin(d,1,target,1));
    d=describe("UPDATE rd_target SET id='bad'::integer WHERE missing_predicate() RETURNING WITH(OLD AS o) o.a");
    check("SET and WHERE never bound",types(d,{1007}) && origin(d,0,target,3));
    d=describe("DELETE FROM rd_target WHERE 'bad'::integer=1 RETURNING WITH(OLD AS o,NEW AS n) o.id,n.a");
    check("DELETE predicate never bound",types(d,{23,1007}) && origin(d,0,target,1) && origin(d,1,target,3));
    d=describe("INSERT INTO rd_target SELECT missing_input_function(),NULL,NULL ON CONFLICT(id) DO UPDATE SET id='bad'::integer WHERE missing_conflict_predicate() RETURNING WITH(NEW AS n) n.padding");
    check("SELECT and conflict input never bound",types(d,{25}) && origin(d,0,target,2));
    d=describe("UPDATE rd_target AS actual SET id=id RETURNING WITH(OLD AS \"before.row\",NEW AS \"O\") \"before.row\".id,\"O\".padding,actual.a");
    check("quoted image and genuine target aliases",types(d,{23,25,1007}) && origin(d,0,target,1) && origin(d,1,target,2) && origin(d,2,target,3));
    d=describe("INSERT INTO rd_target DEFAULT VALUES RETURNING old.*,new.*");
    check("default images stars retain actual ordinal origin",types(d,{23,25,1007,23,25,1007}) &&
        origin(d,0,target,1) && origin(d,2,target,3) && origin(d,3,target,1) && origin(d,5,target,3));
    d=describe("UPDATE rd_target SET id=id RETURNING WITH(OLD AS o) ROW(o.a,NULL::numeric(12,3)) AS rec,ARRAY[o.id,NULL] AS a");
    check("compound declaration has fields and expression origin zero",types(d,{2249,1007}) &&
        origin(d,0,0,0) && origin(d,1,0,0) && d.output[0].typeMetadata->recordFields.size()==2 &&
        d.output[0].typeMetadata->recordFields[0].actualOid==1007 && d.output[0].typeMetadata->recordFields[1].actualOid==1700);
    d=describe("UPDATE rd_target AS t SET id=1 FROM rd_aux AS a WHERE 'bad'::integer=1 RETURNING WITH(OLD AS o) a.id,a.n,o.id");
    check("FROM physical declaration and typmod",types(d,{23,1700,23}) && origin(d,0,aux,1) && origin(d,1,aux,2) &&
        origin(d,2,target,1) && d.output[1].typeMod==786439);
    d=describe("DELETE FROM rd_target AS t USING rd_aux AS a WHERE missing_input() RETURNING WITH(OLD AS o) o.id,a.n");
    check("USING physical declaration",types(d,{23,1700}) && origin(d,0,target,1) && origin(d,1,aux,2));
    d=describe("WITH c AS(SELECT n FROM rd_aux WHERE 'bad'::integer=1 ORDER BY nextval('decl_effects') LIMIT 1/0) UPDATE rd_target SET id=1 FROM c RETURNING WITH(OLD AS o) c.n,o.id");
    check("CTE actual output chain skips nonoutput clauses",types(d,{1700,23}) && origin(d,0,aux,2) && origin(d,1,target,1));
    d=describe("UPDATE rd_target SET id=1 FROM(SELECT id,n FROM rd_aux WHERE 1/0=1) AS q(x,y) RETURNING WITH(OLD AS o) q.y,o.id");
    check("derived aliases preserve original source ordinal",types(d,{1700,23}) && origin(d,0,aux,2) && origin(d,1,target,1));
    d=describe("UPDATE rd_target SET id=1 FROM(SELECT id FROM rd_aux JOIN rd_other USING(id)) AS q RETURNING WITH(OLD AS o) q.id,o.id");
    check("merged USING output has no invented physical origin",types(d,{23,23}) && origin(d,0,0,0) && origin(d,1,target,1));
    d=describe("WITH c AS(SELECT id FROM rd_aux UNION ALL SELECT id FROM rd_other) DELETE FROM rd_target USING c RETURNING WITH(OLD AS o) c.id,o.id");
    check("set declaration has no left-branch invented origin",types(d,{23,23}) && origin(d,0,0,0) && origin(d,1,target,1));
    d=describe("MERGE INTO rd_target AS t USING rd_aux AS s ON missing_join_predicate() WHEN MATCHED THEN UPDATE SET id='bad'::integer RETURNING WITH(OLD AS o,NEW AS n) o.id,n.a,s.n");
    check("MERGE source declaration independent of mutation arms",types(d,{23,1007,1700}) && origin(d,0,target,1) && origin(d,1,target,3) && origin(d,2,aux,2));
    const std::vector<QueryBindingDatum> inputs={{"decl:1","","integer",{},false,{},1,ParameterOrigin::MetadataPlaceholder}};
    d=describe("UPDATE rd_target SET id=$1 RETURNING WITH(OLD AS o) $1,o.id",inputs);
    check("actual metadata input positions do not become source columns",types(d,{23,23}) && origin(d,0,0,0) && origin(d,1,target,1));
    d=describe("INSERT INTO rd_target DEFAULT VALUES RETURNING WITH(NEW AS n) nextval('decl_effects'),decl_function(),n.id");
    check("describing nextval, function and DEFAULT never executes them",types(d,{20,23,23}) && g_engine.currval(db,"decl_effects")==1);
    check("missing actual column error",describe("UPDATE rd_target SET id=1 RETURNING WITH(OLD AS o) o.missing").state=="42703");
    check("explicit alias hides default namespace",describe("UPDATE rd_target SET id=1 RETURNING WITH(OLD AS before_row) old.id").state=="42P01");
    check("actual duplicate namespace error",describe("UPDATE rd_target AS o SET id=1 RETURNING WITH(OLD AS o) o.id").state=="42712");
    bool original=false;
    try {(void)g_engine.prepareBoundQuery(db,"UPDATE rd_target SET id='bad'::integer RETURNING id");}
    catch(const DbError& error){original=error.sqlState()=="22P02";}
    check("whole binder retains original input validation",original && g_engine.currval(db,"decl_effects")==1);
    check("COALESCE explicit conditional input remains unsupported",describe("UPDATE rd_target SET id=1 RETURNING WITH(OLD AS o) COALESCE(o.id,'bad'::integer)").state=="0A000");
    check("CASE explicit conditional input remains unsupported",describe("UPDATE rd_target SET id=1 RETURNING WITH(OLD AS o) CASE WHEN true THEN o.id ELSE 'bad'::integer END").state=="0A000");
    check("shortcircuit and iif inputs are never analysed",
        describe("UPDATE rd_target SET id=1 RETURNING WITH(OLD AS o) false AND 'bad'::boolean").state=="0A000" &&
        describe("UPDATE rd_target SET id=1 RETURNING WITH(OLD AS o) iif(true,o.id,'bad'::integer)").state=="0A000");
    const auto names=[&](const std::string& sql) {
        try{return std::string(g_engine.validateReturningNamespace(db,sql)?"OK":"UNSUPPORTED");}
        catch(const DbError& error){return error.sqlState();}
    };
    check("namespace error precedes every actual effect",names("UPDATE rd_target SET id=nextval('decl_effects') RETURNING WITH(OLD AS o) ROW(nextval('decl_effects'),o.missing)")=="42703" && g_engine.currval(db,"decl_effects")==1);
    check("names-only hidden image error",names("UPDATE rd_target SET id=1 RETURNING WITH(OLD AS before_row) old.id")=="42P01");
    check("names-only collision precedes projection",names("DELETE FROM rd_target AS o RETURNING WITH(OLD AS o) nextval('decl_effects')")=="42712" && g_engine.currval(db,"decl_effects")==1);
    check("names-only COALESCE does not inspect explicit CAST input",names("UPDATE rd_target SET id=1 RETURNING WITH(OLD AS o) COALESCE(o.id,'bad'::integer)")=="OK");
    check("names-only conditional forms never coerce inputs",
        names("UPDATE rd_target SET id=1 RETURNING WITH(OLD AS o) CASE WHEN true THEN o.id ELSE 'bad'::integer END")=="OK" &&
        names("UPDATE rd_target SET id=1 RETURNING WITH(OLD AS o) false AND 'bad'::boolean")=="OK" &&
        names("UPDATE rd_target SET id=1 RETURNING WITH(OLD AS o) iif(true,o.id,'bad'::integer)")=="OK");
    check("names-only CTE output ignores value and predicate inputs",names("WITH c AS(SELECT 'bad'::integer AS x FROM rd_aux WHERE missing_predicate()) UPDATE rd_target SET id=1 FROM c RETURNING WITH(OLD AS o) c.x,o.id")=="OK");
    check("unsupported child namespace retains old complete fallback",names("UPDATE rd_target SET id=1 RETURNING WITH(OLD AS o) ROW(o.id,(SELECT nextval('decl_effects')))")=="UNSUPPORTED" && g_engine.currval(db,"decl_effects")==1);
    check("unsupported function source never opens provider",names("UPDATE rd_target SET id=1 FROM generate_series(1,2) AS s RETURNING WITH(OLD AS o) o.id")=="UNSUPPORTED" && g_engine.currval(db,"decl_effects")==1);
    check("EXTRACT bare field is grammar, actual value names still bound",names("UPDATE rd_target SET id=1 RETURNING old.id,EXTRACT(YEAR FROM TIMESTAMP '2020-01-01')")=="OK" && types(describe("UPDATE rd_target SET id=1 RETURNING old.id,EXTRACT(YEAR FROM TIMESTAMP '2020-01-01')"),{23,1700}));
    check("EXTRACT quoted field and whitespace preserve grammar role",names("UPDATE rd_target SET id=1 RETURNING old.id,EXTRACT (\"year\" FROM TIMESTAMP '2020-01-01')")=="OK");
    check("date_part field remains a real value argument",names("UPDATE rd_target SET id=1 RETURNING old.id,date_part(missing,TIMESTAMP '2020-01-01')")=="42703" && names("UPDATE rd_target SET id=1 RETURNING old.id,date_part(old.padding,TIMESTAMP '2020-01-01')")=="OK");
    // These are genuine declared stored routines, not fake callback roles.
    // Their bodies must not run during either pure namespace/type endpoint.
    if(g_engine.createUDF(db,"extract",{"arg1","arg2"},{"integer","timestamp"},
        "BEGIN PERFORM nextval('decl_effects'); RETURN arg1; END;",'v',"plpgsql","integer")!=DBStatus::OK)
        throw std::runtime_error("fixture actual stored extract failed");
    check("qualified stored extract keeps actual argument binding",names("UPDATE rd_target SET id=1 RETURNING old.id,public.extract(old.id,TIMESTAMP '2020-01-01')")=="OK" && types(describe("UPDATE rd_target SET id=1 RETURNING old.id,public.extract(old.id,TIMESTAMP '2020-01-01')"),{23,23}));
    check("quoted stored extract keeps actual argument binding",names("UPDATE rd_target SET id=1 RETURNING old.id,\"extract\"(old.id,TIMESTAMP '2020-01-01')")=="OK" && types(describe("UPDATE rd_target SET id=1 RETURNING old.id,\"extract\"(old.id,TIMESTAMP '2020-01-01')"),{23,23}));
    check("qualified extract never skips missing ordinary value",names("UPDATE rd_target SET id=1 RETURNING old.id,public.extract(missing,TIMESTAMP '2020-01-01')")=="42703");
    check("quoted extract never skips missing ordinary value",names("UPDATE rd_target SET id=1 RETURNING old.id,\"extract\"(missing,TIMESTAMP '2020-01-01')")=="42703");
    check("TRIM direction grammar is already lowered into value arguments",names("UPDATE rd_target SET id=1 RETURNING old.id,TRIM(BOTH 'x' FROM old.padding),TRIM(LEADING 'x' FROM old.padding),TRIM(TRAILING 'x' FROM old.padding)")=="OK");
    check("SUBSTRING and OVERLAY retain their actual value children",names("UPDATE rd_target SET id=1 RETURNING old.id,SUBSTRING(old.padding FROM old.id FOR 1),OVERLAY(old.padding PLACING 'x' FROM old.id FOR 1)")=="OK");
    check("SUBSTRING missing value is never treated as grammar",names("UPDATE rd_target SET id=1 RETURNING old.id,SUBSTRING(missing FROM 1)")=="42703");
    check("OVERLAPS row grammar carries only value arguments",names("UPDATE rd_target SET id=1 RETURNING old.id,(TIMESTAMP '2020-01-01',TIMESTAMP '2020-01-02') OVERLAPS (TIMESTAMP '2020-01-02',TIMESTAMP '2020-01-03')")=="OK");
    check("typed literals never become namespace columns or input validation",names("UPDATE rd_target SET id=1 RETURNING old.id,ROW(DATE '2020-01-01',INTERVAL '1 day')")=="OK" && names("UPDATE rd_target SET id=1 RETURNING old.id,TIMESTAMP 'bad'")=="OK" && g_engine.currval(db,"decl_effects")==1);
    const auto countSuccess=describe("WITH c AS(SELECT n FROM rd_aux WHERE 'bad'::integer=1 ORDER BY nextval('decl_effects') LIMIT 1/0) UPDATE rd_target SET id=1 FROM c RETURNING WITH(OLD AS o) c.n,o.id");
    check("private purpose restores ordinary parser and whole binding after success",
        types(countSuccess,{1700,23}) && !std::get<0>(originalBindingParse) &&
        originalWhole=="42601" && ordinarySyntax(false)==originalParse && ordinarySyntax(true)==originalBindingParse &&
        ordinaryWhole()==originalWhole && g_engine.currval(db,"decl_effects")==1);
    const auto countFailure=describe("WITH c AS(SELECT n FROM rd_aux LIMIT 1/0) UPDATE rd_target SET id=1 FROM c RETURNING WITH(OLD AS o) o.missing");
    check("private purpose restores ordinary parser and whole binding after API error",
        countFailure.state=="42703" && ordinarySyntax(false)==originalParse && ordinarySyntax(true)==originalBindingParse &&
        ordinaryWhole()==originalWhole && g_engine.currval(db,"decl_effects")==1);
    auto kept=describe("UPDATE rd_target SET id=1 RETURNING WITH(OLD AS o) ROW(o.id,o.a)");
    const bool unchanged=g_engine.query(db,"rd_target",{}, {"id"}).empty() && g_engine.query(db,"rd_aux",{}, {"id"}).empty() &&
        !g_engine.inTransaction() && g_engine.currval(db,"decl_effects")==1;
    if(g_engine.dropDatabase(db)!=DBStatus::OK)throw std::runtime_error("fixture drop failed");
    check("descriptor owns metadata after catalog/AST/owner lifetime",unchanged && types(kept,{2249}) &&
        kept.output[0].typeMetadata->recordFields.size()==2 && kept.output[0].typeMetadata->recordFields[0].actualOid==23 &&
        kept.output[0].typeMetadata->recordFields[1].actualOid==1007 && origin(kept,0,0,0));
    cleanupTestDb(name);finalCleanupTestData();
    std::cout<<"RETURNING_DECLARATION_TERMINAL controls="<<controls<<" expected=46 failures="<<failed.size()<<'\n';
    return controls==46 && failed.empty()?0:1;
}
