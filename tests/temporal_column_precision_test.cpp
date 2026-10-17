#include "catalog/type_registry.h"
#include "catalog/CatalogService.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "test_utils.h"

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    size_t checks=0,failures=0;
    const auto check=[&](bool pass,const std::string& label) {
        ++checks;failures+=!pass;
        std::cout<<"TEMPORAL_COLUMN_PRECISION_NATIVE "<<checks<<' '<<label<<" pass="<<pass<<'\n';
    };
    struct Family {const char* name;const char* prefix;Oid oid,arrayOid;size_t physical;int16_t logical;};
    const std::vector<Family> families={{"time","t",1083,1183,4,8},{"timetz","z",1266,1270,4,12},
        {"timestamp","p",1114,1115,8,8},{"timestamptz","q",1184,1185,8,8}};
    struct Field {std::string name,definition;Family family;int precision;bool array;size_t defaultIndex;};
    std::vector<Field> fields;
    TypeRegistry::instance().bootstrap();
    for(const bool array:{false,true})for(const auto& family:families) {
        const size_t defaultIndex=fields.size()%16+3;
        for(const int precision:{0,3,6,-1}) {
            const std::string name=std::string(family.prefix)+(array?"a":"")+(precision<0?"d":std::to_string(precision));
            const std::string declaration=std::string(family.name)+(precision<0?"":"("+std::to_string(precision)+")")+(array?"[]":"");
            fields.push_back({name,declaration,family,precision,array,defaultIndex});
            Column column;column.dataName="v";
            const auto error=TypeRegistry::instance().resolveColumnType(column,family.name,
                precision<0?std::vector<std::string>{}:std::vector<std::string>{std::to_string(precision)},array);
            check(error.empty(),"registry resolves "+declaration);
            check(column.typeMod==precision,"registry keeps modifier "+declaration);
            check(column.dsize==family.physical && column.isArray==array,"registry physical codec "+declaration);
        }
    }
    const std::string database=testDbPath("temporal_column_precision");
    check(g_engine.createDatabase(database,"utf8")==DBStatus::OK,"create isolated database");
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    setCurrentSession(&session);
    struct ClearSession {~ClearSession(){setCurrentSession(nullptr);}} clearSession;
    DdlExecutor ddl;
    const std::vector<std::string> tables={"scalar_precisions","array_precisions"};
    for(size_t group=0;group<tables.size();++group) {
        std::string create="CREATE TABLE "+tables[group]+"(";
        for(size_t i=0;i<16;++i)create+=(i?",":"")+fields[group*16+i].name+" "+fields[group*16+i].definition;
        create+=')';
        check(!ddl.executeSql(create,session),"create temporal declarations "+tables[group]);
    }
    const auto verify=[&](StorageEngine& engine,const std::string& phase) {
        auto& catalog=engine.catalogService().get(database);
        const auto space=catalog.getNamespaceByName("public");
        for(size_t group=0;group<tables.size();++group) {
          const auto schema=engine.getTableSchema(database,tables[group]);
          check(schema.len==16,phase+" schema count "+tables[group]);
          const auto relation=space?catalog.getClassByName(tables[group],space->oid):std::nullopt;
          for(size_t i=0;i<16;++i) {
            const auto& field=fields[group*16+i];
            const auto attribute=relation?catalog.getAttribute(relation->oid,field.name):std::nullopt;
            const Column* column=i<schema.len?&schema.cols[i]:nullptr;
            const Column* omitted=field.defaultIndex<schema.len?&schema.cols[field.defaultIndex]:nullptr;
            check(column && column->typeMod==field.precision,phase+" schema modifier "+field.name);
            check(column && omitted && column->dsize==omitted->dsize &&
                column->isVariableLength==omitted->isVariableLength && column->isArray==field.array &&
                column->storageCodec==omitted->storageCodec &&
                column->storageCodec==((field.array || std::string(field.family.name)=="timetz")
                    ? Column::StorageCodec::Legacy : Column::StorageCodec::TemporalMicros),
                phase+" no precision-based physical width "+field.name);
            check(attribute && attribute->atttypmod==field.precision,phase+" attribute modifier "+field.name);
            check(attribute && attribute->attlen==(field.array?-1:field.family.logical),phase+" logical width "+field.name);
            check(attribute && attribute->attnum==static_cast<int>(i+1) &&
                (attribute->atttypid==(field.array?field.family.arrayOid:field.family.oid) ||
                 (field.array && attribute->atttypid==field.family.oid && attribute->attndims>0)),
                phase+" real type and attribute identity "+field.name);
          }
        }
    };
    verify(g_engine,"new");
    check(g_engine.insertRow(database,"scalar_precisions",{})==DBStatus::OK,"insert all-NULL row");
    check(g_engine.insertRow(database,"scalar_precisions",{{"t3",std::string("12:34:56")}})==DBStatus::OK,"insert representable old TIME codec");
    auto before=g_engine.query(database,"scalar_precisions",{}, {"t3"},{});
    std::sort(before.begin(),before.end());
    check(before.size()==2,"preserved NULL and coarse data fixture");
    check(!ddl.executeSql("ALTER TABLE scalar_precisions RENAME COLUMN t0 TO renamed",session),"unrelated ALTER metadata sync");
    fields.front().name="renamed";
    verify(g_engine,"alter");
    g_engine.catalogService().evict(database);
    verify(g_engine,"catalog_reload");
    {
        StorageEngine cold;
        verify(cold,"cold_schema");
        auto actual=cold.query(database,"scalar_precisions",{}, {"t3"},{});
        std::sort(actual.begin(),actual.end());
        check(actual==before,"cold read preserves physical rows");
    }
    auto after=g_engine.query(database,"scalar_precisions",{}, {"t3"},{});
    std::sort(after.begin(),after.end());
    check(after==before,"global engine data unchanged");
    check(!ddl.executeSql("CREATE TABLE legacy_defaults(t TIME,z TIMETZ,p TIMESTAMP,q TIMESTAMPTZ,ta TIME[],za TIMETZ[],pa TIMESTAMP[],qa TIMESTAMPTZ[])",session),"create declarations without modifiers");
    {
        StorageEngine cold;
        const auto legacy=cold.getTableSchema(database,"legacy_defaults");
        check(legacy.len==8 && std::all_of(legacy.cols,legacy.cols+legacy.len,
            [](const auto& column){return column.typeMod==-1;}),"omitted modifiers remain -1 after cold load");
    }
    const auto format=[&](const std::string& table) {
        std::ifstream input(g_engine.dbPath(database)/(table+".stc"),std::ios::binary);
        int32_t value=0;input.read(reinterpret_cast<char*>(&value),sizeof(value));return input?value:0;
    };
    check(format("legacy_defaults")==0x4442000d,"new scalar codec requires D even without precision");
    for(const auto& table:tables)
        check(format(table)==(table=="scalar_precisions"?0x4442000d:0x4442000c),
              "scalar D and legacy-array C retain typed modifiers "+table);
    for(const auto& family:families)for(const bool array:{false,true})
        for(const auto invalid:{"-2","7","2147483647"}) {
            Column column;
            check(!TypeRegistry::instance().resolveColumnType(column,family.name,{invalid},array).empty(),
                std::string("registry retains invalid modifier guard ")+family.name+invalid);
        }
    std::cout<<"TEMPORAL_COLUMN_PRECISION_NATIVE_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    return fields.size()==32 && checks==782 && failures==0?0:1;
}
