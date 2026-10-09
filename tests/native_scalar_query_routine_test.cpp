#include "TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    const std::string db = testDbPath("native_scalar_query_routine");
    assert(owner.createDatabase(db,"utf8") == DBStatus::OK);
    assert(owner.createUDF(db,"native_failure",{},{},
        "BEGIN RAISE EXCEPTION 'native error'; END;",'v',"plpgsql","int") == DBStatus::OK);
    const auto function = owner.getUDF(db,"native_failure");
    assert(function.name=="native_failure" && function.paramNames.empty() && !function.expression.empty());
    auto result = owner.plpgsqlQuery(db,"SELECT native_failure()");
    if (result.sqlState!="P0001")
        std::cerr << "native_failure actual=" << result.sqlState << " " << result.message << '\n';
    assert(!result.ok && result.sqlState=="P0001");
    assert(owner.createUDF(db,"MiXeD",{"x"},{"bigint"},"SELECT x + 1",
        'i',"sql","bigint",true) == DBStatus::OK);
    result=owner.plpgsqlQuery(db,"SELECT public.\"MiXeD\"(2147483648::bigint)");
    assert(result.ok && result.columnTypes==std::vector<std::string>{"bigint"});
    assert(result.firstRow==std::vector<std::optional<std::string>>{"2147483649"});
    result=owner.plpgsqlQuery(db,"SELECT \"MiXeD\"(NULL::bigint)");
    assert(result.ok && result.columnTypes==std::vector<std::string>{"bigint"});
    assert(result.firstRow==std::vector<std::optional<std::string>>{std::nullopt});
    result=owner.plpgsqlQuery(db,"SELECT native_failure() LIMIT 0");
    assert(result.ok && result.rowCount==0 && result.columnTypes==std::vector<std::string>{"integer"});
    result=owner.plpgsqlQuery(db,"SELECT CASE WHEN false THEN native_failure() ELSE 7 END");
    assert(result.ok && result.firstRow==std::vector<std::optional<std::string>>{"7"});
    for (const auto& sql : {"SELECT native_failure(), no_such_function()",
                           "SELECT native_failure() ORDER BY no_such_function()",
                           "SELECT CASE WHEN false THEN no_such_function() ELSE 7 END"}) {
        result=owner.plpgsqlQuery(db,sql);
        assert(!result.ok && result.sqlState=="42883"); // prepare before RAISE
    }
    result=owner.plpgsqlQuery(db,"SELECT MiXeD(1)");
    assert(!result.ok && result.sqlState=="42883"); // quoted case stays distinct
    result=owner.plpgsqlQuery(db,"SELECT other.\"MiXeD\"(1)");
    assert(!result.ok && result.sqlState=="42883"); // no schema fallback
    assert(owner.createUDF(db,"sum",{},{},"SELECT 23",'i',"sql","int")==DBStatus::OK);
    result=owner.plpgsqlQuery(db,"SELECT public.sum()");
    if (!result.ok) std::cerr << "public.sum actual=" << result.sqlState << " " << result.message << '\n';
    assert(result.ok && result.columnTypes==std::vector<std::string>{"integer"});
    assert(result.firstRow==std::vector<std::optional<std::string>>{"23"});
    result=owner.plpgsqlQuery(db,"SELECT native_failure() WHERE other.coalesce(1,2)>0");
    assert(!result.ok && result.sqlState=="42883");
    TableSchema schema; schema.len=1;
    schema.cols[0].dataName="id"; schema.cols[0].dataType="int"; schema.cols[0].dsize=4;
    assert(owner.createTable(db,"source",schema)==DBStatus::OK);
    for (int id : {1,2,3})
        assert(owner.insertRow(db,"source",std::map<std::string,std::optional<std::string>>{{"id",std::to_string(id)}})==DBStatus::OK);
    result=owner.plpgsqlQuery(db,"SELECT \"MiXeD\"(id) AS n FROM source WHERE \"MiXeD\"(id)>2 ORDER BY n DESC LIMIT 1");
    assert(result.ok && result.rowCount==1 && result.columnTypes==std::vector<std::string>{"bigint"});
    assert(result.firstRow==std::vector<std::optional<std::string>>{"4"});
    // SQL function bodies retain a whole query and typed runtime arguments.
    assert(owner.createUDF(db,"native_reader",{"wanted"},{"int"},
        "SELECT id FROM source WHERE id > wanted ORDER BY id DESC",
        's',"sql","int")==DBStatus::OK);
    for (const auto& input : {std::string("0"), std::string("3"), std::string("NULL::int")}) {
        result=owner.plpgsqlQuery(db,"SELECT native_reader("+input+")");
        if (!result.ok) std::cerr << "native_reader actual=" << result.sqlState << " " << result.message << '\n';
        assert(result.ok && result.rowCount==1 && result.columnTypes==std::vector<std::string>{"integer"});
        assert(result.firstRow==std::vector<std::optional<std::string>>{
            input=="0" ? std::optional<std::string>{"3"} : std::nullopt});
    }
    assert(owner.createUDF(db,"native_positional",{""},{"bigint"},
        "SELECT $1 + 1",'i',"sql","bigint")==DBStatus::OK);
    result=owner.plpgsqlQuery(db,"SELECT native_positional(2147483648::bigint)");
    assert(result.ok && result.firstRow==std::vector<std::optional<std::string>>{"2147483649"});
    assert(owner.createUDF(db,"native_single_unnamed",std::string{},"SELECT $1 + 1",
        'i',"sql","bigint","bigint")==DBStatus::OK);
    const auto singleUnnamed=owner.getUDF(db,"native_single_unnamed");
    assert(singleUnnamed.paramNames==std::vector<std::string>{""} &&
        singleUnnamed.paramTypes==std::vector<std::string>{"bigint"});
    result=owner.plpgsqlQuery(db,"SELECT native_single_unnamed(2147483648::bigint)");
    assert(result.ok && result.firstRow==std::vector<std::optional<std::string>>{"2147483649"});
    assert(owner.createUDF(db,"native_column_wins",{"id"},{"int"},
        "SELECT id FROM source ORDER BY id DESC",'s',"sql","int")==DBStatus::OK);
    result=owner.plpgsqlQuery(db,"SELECT native_column_wins(99)");
    assert(result.ok && result.firstRow==std::vector<std::optional<std::string>>{"3"});
    assert(owner.createUDF(db,"native_qualified_arg",{"id"},{"int"},
        "SELECT native_qualified_arg.id FROM source ORDER BY source.id DESC",
        's',"sql","int")==DBStatus::OK);
    result=owner.plpgsqlQuery(db,"SELECT native_qualified_arg(99)");
    assert(result.ok && result.firstRow==std::vector<std::optional<std::string>>{"99"});
    assert(owner.dropDatabase(db)==DBStatus::OK);
    cleanupTestDb("native_scalar_query_routine");
    std::cout << "[NATIVE SCALAR QUERY ROUTINE] passed\n";
}
