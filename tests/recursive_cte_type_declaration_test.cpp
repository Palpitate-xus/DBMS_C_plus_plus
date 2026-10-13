#include "parser/query_binding.h"
#include "catalog/declared_type.h"
#include "common/DbError.h"
#include "expression/common_type.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    QueryBindingMetadata metadata;
    metadata.declaredType=[](const std::string& spelling) {
        const auto type=resolveDeclaredTypeName(spelling,nullptr,nullptr);
        return QueryOutputColumn{"",type.typeName,false,0,type.typeOid};
    };
    metadata.relation=[](const std::string& name) {
        if(name=="source")return QueryRelationMetadata{"public",name,{{"v","varchar",false,0,1043,5}}, {}};
        throw DbError("42P01","missing physical relation");
    };
    size_t controls=0, routineDescriptions=0;
    metadata.functionType=[&](const FunctionCallExpr*) {++routineDescriptions;return "bigint";};
    const auto prepare=[&](const std::string& sql) {++controls;return prepareQuery(sql,{},metadata);};
    const auto reject=[&](const std::string& sql) {
        std::string state;
        try {(void)prepare(sql);}catch(const DbError& error){state=error.sqlState();}
        std::cout<<"RECURSIVE_TYPE_NATIVE "<<sql<<" state="<<state<<std::endl;
        assert(state=="42804");
    };
    reject("WITH RECURSIVE q(n) AS(SELECT 1 UNION ALL SELECT n::bigint FROM q WHERE false) SELECT n FROM q");
    reject("WITH RECURSIVE q(n) AS(SELECT 1::smallint UNION ALL SELECT n+1 FROM q WHERE false) SELECT n FROM q");
    reject("WITH RECURSIVE q(n) AS(SELECT 'a'::varchar(1) UNION ALL SELECT n::varchar(2) FROM q WHERE false) SELECT n FROM q");
    reject("WITH RECURSIVE q(n) AS(SELECT v FROM source UNION ALL SELECT n::varchar(2) FROM q WHERE false) SELECT n FROM q");
    reject("WITH RECURSIVE q(n) AS(SELECT (SELECT 'a'::varchar(1)) UNION ALL SELECT n::varchar(2) FROM q WHERE false) SELECT n FROM q");
    reject("WITH RECURSIVE q(n) AS(SELECT v FROM(SELECT 'a'::varchar(1) AS v)s UNION ALL SELECT n::varchar(2) FROM q WHERE false) SELECT n FROM q");
    reject("WITH RECURSIVE q(n) AS(SELECT 1::numeric(3,1) UNION ALL SELECT n+1 FROM q WHERE false) SELECT n FROM q");
    reject("WITH RECURSIVE q(n) AS(SELECT nextval('never_executed')::integer UNION ALL SELECT n::bigint FROM q WHERE false) SELECT n FROM q");
    assert(routineDescriptions==1); // this harness has no executor/sequence
    const auto output=[&](const std::string& sql,const std::string& type,int32_t modifier) {
        const auto result=prepare(sql);
        assert(result.output.size()==1);
        std::cout<<"RECURSIVE_TYPE_NATIVE "<<sql<<" type="<<result.output[0].type
                 <<" modifier="<<result.output[0].typeMod<<std::endl;
        assert(common_type_detail::canonical(result.output[0].type)==type && result.output[0].typeMod==modifier);
    };
    output("WITH RECURSIVE q(n) AS(SELECT 1::bigint UNION ALL SELECT n::integer FROM q WHERE false) SELECT n FROM q","bigint",-1);
    output("WITH RECURSIVE q(n) AS(SELECT v FROM source UNION ALL SELECT n FROM q WHERE false) SELECT n FROM q","varchar",5);
    output("WITH RECURSIVE q(n) AS(SELECT 'a'::varchar UNION ALL SELECT n::varchar(2) FROM q WHERE false) SELECT n FROM q","varchar",-1);
    output("WITH RECURSIVE q(n) AS(SELECT NULL UNION ALL SELECT 'x'::text FROM q WHERE false) SELECT n FROM q","text",-1);
    output("WITH RECURSIVE q(n) AS(SELECT 1 UNION ALL SELECT 2::bigint) SELECT n FROM q","bigint",-1);
    output("WITH RECURSIVE q(n) AS(SELECT 'a'::varchar(1) UNION ALL SELECT 'ab'::varchar(2)) SELECT n FROM q","varchar",-1);
    output("WITH RECURSIVE q(n) AS(SELECT 1 UNION ALL SELECT n FROM(WITH q AS(SELECT 2::bigint AS n) SELECT n FROM q)s) SELECT n FROM q","bigint",-1);
    output("WITH RECURSIVE q(n) AS((SELECT 1::bigint) UNION ALL SELECT n::integer FROM q WHERE false) SELECT n FROM q","bigint",-1);
    output("WITH RECURSIVE q(n) AS(SELECT 'a'::varchar(1) UNION ALL SELECT CASE WHEN true THEN n ELSE 'b'::varchar(1) END FROM q WHERE false) SELECT n FROM q","varchar",5);
    reject("WITH RECURSIVE q(n) AS(SELECT 'a'::varchar(70000) UNION ALL SELECT n::varchar(70001) FROM q WHERE false) SELECT n FROM q");
    output("WITH RECURSIVE q(n) AS(SELECT '12:00:00'::time(6) UNION ALL SELECT n::time(7) FROM q WHERE false) SELECT n FROM q","time",6);
    reject("WITH RECURSIVE q(n) AS(SELECT '12:00:00'::time(5) UNION ALL SELECT n::time(7) FROM q WHERE false) SELECT n FROM q");
    const auto builtinDeclaration=metadata.declaredType;
    metadata.declaredType=[&](const std::string& spelling) {
        if(spelling=="d")return QueryOutputColumn{"","\"public\".\"d\"",false,0,90001};
        return builtinDeclaration(spelling);
    };
    metadata.baseType=[](const std::string& type,uint32_t oid) {
        return oid==90001?std::string("integer"):type;
    };
    output("WITH RECURSIVE q(n) AS(SELECT 1 UNION ALL SELECT n::d FROM q WHERE false) SELECT n FROM q","integer",-1);
    reject("WITH RECURSIVE q(n) AS(SELECT 1::d UNION ALL SELECT n::integer FROM q WHERE false) SELECT n FROM q");
    const auto domain=prepare("WITH RECURSIVE q(n) AS(SELECT 1::d UNION ALL SELECT n FROM q WHERE false) SELECT n FROM q");
    assert(domain.output[0].type=="\"public\".\"d\"" && domain.output[0].typeOid==90001);
    assert(controls==23);
    std::cout<<"[RECURSIVE CTE TYPE DECLARATION] all 23 metadata-only controls passed\n";
}
