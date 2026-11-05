#include "catalog/AnalyzedStatisticsStore.h"
#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "catalog/type_registry.h"
#include "common/Config.h"
#include "test_utils.h"
#include <fstream>
#include <iostream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
using namespace dbms;
namespace stats=analyzed_statistics;
size_t checks=0;
void require(bool good,const char* message){++checks;if(!good)throw std::runtime_error(message);}
std::string literal(const std::optional<std::string>& value){
    if(!value)return "NULL";std::string text="'";
    for(char c:*value){text+=c;if(c=='\'')text+=c;}return text+'\'';
}
void ddl(DdlExecutor& owner,Session& session,const std::string& sql){
    require(!owner.executeSql(sql,session),"real range DDL succeeds");
    std::cout<<"RANGE_REFERENCE_SQL "<<sql<<";\n";
}
void insert(const std::string& db,const std::string& table,const StorageEngine::SqlRow& row){
    require(g_engine.insertRow(db,table,row)==DBStatus::OK,"real range heap insert succeeds");
    std::string names,values;
    for(const auto& cell:row){if(!names.empty()){names+=',';values+=',';}names+=cell.first;values+=literal(cell.second);}
    std::cout<<"RANGE_REFERENCE_SQL INSERT INTO "<<table<<'('<<names<<") VALUES("<<values<<");\n";
}
void analyze(const std::string& db,const std::string& table){
    require(g_engine.analyzeTable(db,table),"real range ANALYZE succeeds");
    std::cout<<"RANGE_REFERENCE_SQL ANALYZE "<<table<<";\n";
}
const stats::Slot* slot(const stats::Column& c,stats::Kind kind){
    for(const auto& s:c.slots)if(s.kind==kind)return &s;
    return nullptr;
}
void validate(const stats::Column& c,uint32_t base){
    require(c.attributeType&&c.attributeType->codecOid==base,"actual underlying range declaration");
    require(!slot(c,stats::Kind::MostCommon)&&!slot(c,stats::Kind::Histogram)&&!slot(c,stats::Kind::Correlation),
        "range analysis does not invent scalar MCV, histogram or correlation slots");
    for(const auto& s:c.slots){
        require(s.collation==0,"range slot has its genuine no-collation role");
        if(s.kind==stats::Kind::RangeBounds){
            require(s.comparisonOperator==0&&s.values&&s.values->elementType->actualOid==base&&
                !s.numbers&&s.values->cells.size()>=2,"range bounds role and actual base value identity");
            for(const auto& v:s.values->cells)require(!v.isNull,"range histogram contains actual non-NULL datums");
        }else{
            require(s.kind==stats::Kind::RangeLength&&s.comparisonOperator==672&&s.numbers&&
                s.numbers->size()==1&&s.numbers->front()>=0&&s.numbers->front()<=1,
                "range length slot and empty fraction role");
            if(s.values)require(s.values->elementType->actualOid==701,"length histogram uses actual float8 values");
        }
    }
}
stats::Column actual(const std::string& db,const std::string& table,const std::string& name){
    const auto metadata=g_engine.catalogService().metadataSnapshot(db);
    uint32_t ns=0,relation=0;int16_t attribute=0;
    for(const auto& n:metadata.namespaces)if(n.nspname=="public")ns=n.oid;
    for(const auto& r:metadata.relations)if(r.relnamespace==ns&&r.relname==table)relation=r.oid;
    require(relation,"real range catalog relation identity");
    for(const auto& a:metadata.attributes)if(a.attrelid==relation&&!a.attisdropped&&a.attname==name)attribute=a.attnum;
    const auto image=stats::read(g_engine.dbPath(db)/".analyzed_statistics");
    require(image.has_value(),"real ANALYZE snapshot exists");
    const auto found=image->columns.find({relation,attribute,false});
    require(found!=image->columns.end(),"actual range attribute statistics were collected");
    require(found->second.physicalRelation==g_engine.getTableSchema(db,table).physicalRelationId,
        "range statistics retain actual physical relation identity");
    std::cout<<"RANGE_IDENTITY "<<table<<' '<<name<<' '<<relation<<' '<<attribute<<' '
        <<found->second.attributeType->actualOid<<' '<<found->second.attributeType->codecOid<<'\n';
    return found->second;
}
} // namespace

int main(int argc,char** argv){
    try{
        if(argc==3&&std::string(argv[1])=="--cold"){
            const auto image=stats::read(argv[2]);require(image&&image->columns.size()==18,"cold reader sees all real range and scalar rows");
            std::ifstream in(argv[2],std::ios::binary);const std::string raw{std::istreambuf_iterator<char>(in),{}};
            require(stats::encode(*image)==raw,"cold snapshot preserves every type, slot, special value and frame byte");
            for(const auto& pair:image->columns)if(pair.second.attributeType->codecOid!=23)
                validate(pair.second,pair.second.attributeType->codecOid);
            std::cout<<"ANALYZED_RANGE_COLD checks="<<checks<<" failures=0\n";return 0;
        }
        require(argc==1,"fixed native range regression protocol");TypeRegistry::instance().bootstrap();
        const auto db=testDbPath("analyzed_range_statistics");
        require(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"real database creation");
        Session session;session.username="admin";session.permission=1;session.currentDB=db;
        struct Scope{Session* before;bool automatic;~Scope(){setCurrentSession(before);g_config.autoAnalyzeEnabled=automatic;}}
            scope{currentSession(),g_config.autoAnalyzeEnabled};
        setCurrentSession(&session);g_config.autoAnalyzeEnabled=false;DdlExecutor owner;
        ddl(owner,session,"CREATE TABLE range_scan(i int4range,b int8range,n numrange,d daterange,t tsrange,z tstzrange,x integer,v integer GENERATED ALWAYS AS(1/(x-x)) VIRTUAL)");
        const std::vector<std::optional<std::string>> ints={{},"empty","[1,2)","(1,5]","(,10)","[3,)","(,)","[1,1]"};
        const std::vector<std::optional<std::string>> bigs={{},"empty","[9007199254740992,9007199254740993)","[-9223372036854775808,-9223372036854775807)","[0,9223372036854775807)","(-10,10]","(,)","[1,1]"};
        const std::vector<std::optional<std::string>> nums={{},"empty","[1.0,1.00]","[10000000000000000000000000000,10000000000000000000000000001)","(-Infinity,Infinity)","[NaN,NaN]","(,)","(-1,2]"};
        const std::vector<std::optional<std::string>> dates={{},"empty","(2024-02-28,2024-03-01]","[2000-01-01,2000-01-02)","(,2000-01-01)","[2000-01-01,)","(,)","[2024-02-29,2024-02-29]"};
        const std::vector<std::optional<std::string>> times={{},"empty","[\"2000-01-01 00:00:00.1\",\"2000-01-01 00:00:00.9\")","[\"9999-01-01 00:00:00\",\"9999-01-01 00:00:00.000001\")","[-infinity,infinity]","[\"2000-01-01 00:00:00\",\"2000-01-01 00:00:01\")","(,)","[\"2000-01-01 00:00:00\",\"2000-01-02 00:00:00\")"};
        const std::vector<std::optional<std::string>> zones={{},"empty","[\"2000-01-01 01:00:00+01\",\"2000-01-01 01:00:01+01\")","[\"2000-01-01 00:00:00+00\",\"2000-01-01 00:00:01+00\")","[-infinity,infinity]","[\"2000-01-01 01:00:00+01\",\"2000-01-01 03:00:00+03\")","(,)","[\"2000-01-01 00:00:00.1+00\",\"2000-01-01 00:00:00.9+00\")"};
        for(int r=0;r<40;++r)insert(db,"range_scan",{{"i",ints[r%8]},{"b",bigs[r%8]},
            {"n",nums[r%8]},{"d",dates[r%8]},{"t",times[r%8]},{"z",zones[r%8]},{"x",std::to_string(r)}});
        analyze(db,"range_scan");
        for(const auto& pair:std::vector<std::pair<std::string,uint32_t>>{{"i",3904},{"b",3926},{"n",3906},{"d",3912},{"t",3908},{"z",3910}}){
            const auto c=actual(db,"range_scan",pair.first);validate(c,pair.second);
            require(c.sampleRows==40&&c.nullRows==5&&c.nullFraction==.125f&&c.distinctEstimate==-.875f,
                "actual full-sample range NULL and distinct fields");
            const size_t nonempty=pair.first=="z"?25:30;
            require(c.slots.size()==2&&slot(c,stats::Kind::RangeBounds)->values->cells.size()==nonempty&&
                    slot(c,stats::Kind::RangeLength)->values->cells.size()==nonempty,
                "actual bounds and length histograms use every nonempty range");
        }
        require(actual(db,"range_scan","x").attributeType->actualOid==23,"mixed ordinary scalar column stays scalar");
        ddl(owner,session,"CREATE DOMAIN bounds_domain AS int4range");
        ddl(owner,session,"CREATE TABLE range_domain(a bounds_domain)");
        insert(db,"range_domain",{{"a","[1,10)"}});insert(db,"range_domain",{{"a","empty"}});insert(db,"range_domain",{{"a",{}}});
        analyze(db,"range_domain");auto c=actual(db,"range_domain","a");validate(c,3904);
        require(c.attributeType->actualOid!=3904&&c.attributeType->codecOid==3904,
            "actual catalog domain identity survives range analysis");
        const auto domainCatalog=g_engine.catalogService().metadataSnapshot(db);
        bool genuineDomain=false;
        for(const auto& row:domainCatalog.types)if(row.oid==c.attributeType->actualOid)
            genuineDomain=row.typname=="bounds_domain"&&row.typtype=='d'&&row.typbasetype==3904;
        require(genuineDomain,"range domain identity belongs to the actual copied local catalog");
        ddl(owner,session,"CREATE TABLE range_nulls(a int4range,n numrange,t tsrange)");
        ddl(owner,session,"CREATE TABLE range_empties(a int4range,n numrange,t tsrange)");
        ddl(owner,session,"CREATE TABLE range_empty(a int4range,n numrange,t tsrange)");
        for(int i=0;i<3;++i){insert(db,"range_nulls",{{"a",{}},{"n",{}},{"t",{}}});insert(db,"range_empties",{{"a","empty"},{"n","empty"},{"t","empty"}});}
        analyze(db,"range_nulls");analyze(db,"range_empties");analyze(db,"range_empty");
        for(const auto& name:{"a","n","t"}){
            c=actual(db,"range_nulls",name);require(c.nullFraction==1&&c.averageWidth==0&&c.slots.empty(),"real all-NULL range has no invented slots");
            c=actual(db,"range_empties",name);require(c.nullFraction==0&&c.distinctEstimate==-1&&c.slots.size()==1&&
                !slot(c,stats::Kind::RangeLength)->values&&slot(c,stats::Kind::RangeLength)->numbers->front()==1,
                "real all-empty range has only empty fraction and NULL length values");
        }
        ddl(owner,session,"CREATE TABLE range_one(a int4range,n numrange,t tsrange)");
        insert(db,"range_one",{{"a","[1,2)"},{"n","[1.0,2.5)"},{"t","[\"2000-01-01 00:00:00\",\"2000-01-01 00:00:01\")"}});
        analyze(db,"range_one");
        for(const auto& name:{"a","n","t"}){c=actual(db,"range_one",name);require(c.slots.size()==1&&!slot(c,stats::Kind::RangeLength)->values&&
            slot(c,stats::Kind::RangeLength)->numbers->front()==0,"real single range has no invented histogram values");}
        ddl(owner,session,"CREATE TABLE range_sample(a int4range)");ddl(owner,session,"ALTER TABLE range_sample ALTER COLUMN a SET STATISTICS 1");
        for(int i=0;i<450;++i)insert(db,"range_sample",{{"a","["+std::to_string(i)+","+std::to_string(i+1)+")"}});
        analyze(db,"range_sample");c=actual(db,"range_sample","a");
        require(c.sampleRows==300&&c.totalRows==450&&c.target==1&&
                slot(c,stats::Kind::RangeBounds)->values->cells.size()==2&&
                slot(c,stats::Kind::RangeLength)->values->cells.size()==2,
            "real bounded range reservoir and target-one histogram extent");
        const auto image=g_engine.dbPath(db)/".analyzed_statistics";
        require(stats::read(image)->columns.size()==18,"all real range and scalar records without empty-table or VIRTUAL inventions");
        const auto bytes=[](const std::filesystem::path& path){
            std::ifstream in(path,std::ios::binary);return std::string(std::istreambuf_iterator<char>(in),{});
        };
        for(const auto& failure:std::vector<std::pair<std::string,std::string>>{
            {"range_overflow","[0,1e400)"},{"range_underflow","[0,1e-400)"}}){
            ddl(owner,session,"CREATE TABLE "+failure.first+"(a numrange)");
            insert(db,failure.first,{{"a",failure.second}});
            const auto typedBefore=bytes(image),legacyBefore=bytes(g_engine.dbPath(db)/".stats");
            bool rejected=false;
            try{g_engine.analyzeTable(db,failure.first);}catch(const DbError& e){rejected=e.sqlState()=="22003";}
            require(rejected,"actual range subdiff overflow or underflow keeps SQLSTATE22003");
            require(bytes(image)==typedBefore&&bytes(g_engine.dbPath(db)/".stats")==legacyBefore,
                "failed real analysis leaves both existing statistics images unchanged");
            std::cout<<"RANGE_EXPECTED_ERROR_SQL ANALYZE "<<failure.first<<"; SQLSTATE=22003\n";
        }
        std::cout<<"ANALYZED_RANGE_IMAGE "<<std::filesystem::absolute(image)<<'\n';
        std::cout<<"ANALYZED_RANGE_STATISTICS checks="<<checks<<" failures=0\n";return 0;
    }catch(const std::exception& e){std::cerr<<"ANALYZED_RANGE_STATISTICS_FAIL checks="<<checks<<' '<<e.what()<<'\n';return 1;}
}
