#include "catalog/type_registry.h"
#include "catalog/systables.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include "parser/parser.h"
#include <cassert>
#include <iostream>

using namespace dbms;
extern StorageEngine g_engine;
int main() {
    TypeRegistry::instance().bootstrap();
    const std::string name="typed_scalar_in",db=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(db,"utf8")==DBStatus::OK);
    Session session;session.username="review_user";session.permission=1;session.currentDB=db;setCurrentSession(&session);
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE r282_lhs(id INT,v INT)",session));
    assert(!ddl.executeSql("CREATE TABLE r282_rhs(v INT)",session));
    assert(!ddl.executeSql("CREATE TABLE r282_empty(v INT)",session));
    for(int i=1;i<=3;++i)assert(g_engine.insertRow(db,"r282_lhs",{{"id",std::to_string(i)},{"v",i==3?std::optional<std::string>{}:std::optional<std::string>{std::to_string(i)}}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"r282_rhs",{{"v","2"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"r282_rhs",{{"v",std::nullopt}})==DBStatus::OK);
    using Rows=std::vector<std::vector<std::optional<std::string>>>;
    struct Case {const char* name;const char* sql;Rows rows;const char* state;std::vector<uint32_t> oids;int calls;};
    const Case cases[] = {
        {R"r282(one_one)r282",R"r282(SELECT 1 IN(SELECT 1))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(one_one_not)r282",R"r282(SELECT 1 NOT IN(SELECT 1))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(one_two)r282",R"r282(SELECT 1 IN(SELECT 2))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(one_two_not)r282",R"r282(SELECT 1 NOT IN(SELECT 2))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(one_null)r282",R"r282(SELECT 1 IN(SELECT CAST(NULL AS INT)))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(one_null_not)r282",R"r282(SELECT 1 NOT IN(SELECT CAST(NULL AS INT)))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(one_empty)r282",R"r282(SELECT 1 IN(SELECT 1 WHERE FALSE))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(one_empty_not)r282",R"r282(SELECT 1 NOT IN(SELECT 1 WHERE FALSE))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(one_mixed)r282",R"r282(SELECT 1 IN(SELECT 1 UNION ALL SELECT CAST(NULL AS INT)))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(one_mixed_not)r282",R"r282(SELECT 1 NOT IN(SELECT 1 UNION ALL SELECT CAST(NULL AS INT)))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(one_values)r282",R"r282(SELECT 1 IN(VALUES(1),(2)))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(one_values_not)r282",R"r282(SELECT 1 NOT IN(VALUES(1),(2)))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(one_cte)r282",R"r282(SELECT 1 IN(WITH q AS(SELECT 1 AS v) SELECT v FROM q))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(one_cte_not)r282",R"r282(SELECT 1 NOT IN(WITH q AS(SELECT 1 AS v) SELECT v FROM q))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(two_one)r282",R"r282(SELECT 2 IN(SELECT 1))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(two_one_not)r282",R"r282(SELECT 2 NOT IN(SELECT 1))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(two_two)r282",R"r282(SELECT 2 IN(SELECT 2))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(two_two_not)r282",R"r282(SELECT 2 NOT IN(SELECT 2))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(two_null)r282",R"r282(SELECT 2 IN(SELECT CAST(NULL AS INT)))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(two_null_not)r282",R"r282(SELECT 2 NOT IN(SELECT CAST(NULL AS INT)))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(two_empty)r282",R"r282(SELECT 2 IN(SELECT 1 WHERE FALSE))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(two_empty_not)r282",R"r282(SELECT 2 NOT IN(SELECT 1 WHERE FALSE))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(two_mixed)r282",R"r282(SELECT 2 IN(SELECT 1 UNION ALL SELECT CAST(NULL AS INT)))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(two_mixed_not)r282",R"r282(SELECT 2 NOT IN(SELECT 1 UNION ALL SELECT CAST(NULL AS INT)))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(two_values)r282",R"r282(SELECT 2 IN(VALUES(1),(2)))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(two_values_not)r282",R"r282(SELECT 2 NOT IN(VALUES(1),(2)))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(two_cte)r282",R"r282(SELECT 2 IN(WITH q AS(SELECT 1 AS v) SELECT v FROM q))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(two_cte_not)r282",R"r282(SELECT 2 NOT IN(WITH q AS(SELECT 1 AS v) SELECT v FROM q))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(null_one)r282",R"r282(SELECT NULL IN(SELECT 1))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(null_one_not)r282",R"r282(SELECT NULL NOT IN(SELECT 1))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(null_two)r282",R"r282(SELECT NULL IN(SELECT 2))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(null_two_not)r282",R"r282(SELECT NULL NOT IN(SELECT 2))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(null_null)r282",R"r282(SELECT NULL IN(SELECT CAST(NULL AS INT)))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(null_null_not)r282",R"r282(SELECT NULL NOT IN(SELECT CAST(NULL AS INT)))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(null_empty)r282",R"r282(SELECT NULL IN(SELECT 1 WHERE FALSE))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(null_empty_not)r282",R"r282(SELECT NULL NOT IN(SELECT 1 WHERE FALSE))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(null_mixed)r282",R"r282(SELECT NULL IN(SELECT 1 UNION ALL SELECT CAST(NULL AS INT)))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(null_mixed_not)r282",R"r282(SELECT NULL NOT IN(SELECT 1 UNION ALL SELECT CAST(NULL AS INT)))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(null_values)r282",R"r282(SELECT NULL IN(VALUES(1),(2)))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(null_values_not)r282",R"r282(SELECT NULL NOT IN(VALUES(1),(2)))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(null_cte)r282",R"r282(SELECT NULL IN(WITH q AS(SELECT 1 AS v) SELECT v FROM q))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(null_cte_not)r282",R"r282(SELECT NULL NOT IN(WITH q AS(SELECT 1 AS v) SELECT v FROM q))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(text_numeric_distinct)r282",R"r282(SELECT '001'::TEXT IN(SELECT '1'::TEXT))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(text_equal)r282",R"r282(SELECT '1'::TEXT IN(SELECT '1'::TEXT))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(text_quote)r282",R"r282(SELECT 'O''Brien' IN(SELECT 'O''Brien'))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(literal_null)r282",R"r282(SELECT 'NULL' IN(SELECT 'NULL'))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(true_null)r282",R"r282(SELECT CAST(NULL AS TEXT) IN(SELECT 'NULL'))r282",{{std::nullopt}},R"r282()r282", {16}, -1},
        {R"r282(unicode)r282",R"r282(SELECT '中文' IN(SELECT '中文'))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(text_not_equal)r282",R"r282(SELECT '001'::TEXT NOT IN(SELECT '1'::TEXT))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(unicode_not)r282",R"r282(SELECT '中文' NOT IN(SELECT '中文'))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(unknown_right)r282",R"r282(SELECT 1 IN(SELECT '1'))r282",{},R"r282(42883)r282", {}, -1},
        {R"r282(unknown_left)r282",R"r282(SELECT '1' IN(SELECT 1))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(bad_left)r282",R"r282(SELECT 'bad' IN(SELECT 1))r282",{},R"r282(22P02)r282", {}, -1},
        {R"r282(unknown_right_not)r282",R"r282(SELECT 1 NOT IN(SELECT '1'))r282",{},R"r282(42883)r282", {}, -1},
        {R"r282(unknown_left_not)r282",R"r282(SELECT '1' NOT IN(SELECT 1))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(bad_left_not)r282",R"r282(SELECT 'bad' NOT IN(SELECT 1))r282",{},R"r282(22P02)r282", {}, -1},
        {R"r282(projection)r282",R"r282(SELECT l.id,l.v IN(SELECT r.v FROM r282_rhs r) FROM r282_lhs l ORDER BY l.id)r282",{{R"r282(1)r282",std::nullopt},{R"r282(2)r282",R"r282(t)r282"},{R"r282(3)r282",std::nullopt}},R"r282()r282", {23,16}, -1},
        {R"r282(projection_not)r282",R"r282(SELECT l.id,l.v NOT IN(SELECT r.v FROM r282_rhs r) FROM r282_lhs l ORDER BY l.id)r282",{{R"r282(1)r282",std::nullopt},{R"r282(2)r282",R"r282(f)r282"},{R"r282(3)r282",std::nullopt}},R"r282()r282", {23,16}, -1},
        {R"r282(correlated)r282",R"r282(SELECT l.id,l.v IN(SELECT r.v FROM r282_rhs r WHERE r.v=l.v) FROM r282_lhs l ORDER BY l.id)r282",{{R"r282(1)r282",R"r282(f)r282"},{R"r282(2)r282",R"r282(t)r282"},{R"r282(3)r282",R"r282(f)r282"}},R"r282()r282", {23,16}, -1},
        {R"r282(correlated_not)r282",R"r282(SELECT l.id,l.v NOT IN(SELECT r.v FROM r282_rhs r WHERE r.v=l.v) FROM r282_lhs l ORDER BY l.id)r282",{{R"r282(1)r282",R"r282(t)r282"},{R"r282(2)r282",R"r282(f)r282"},{R"r282(3)r282",R"r282(t)r282"}},R"r282()r282", {23,16}, -1},
        {R"r282(where)r282",R"r282(SELECT id FROM r282_lhs WHERE v IN(SELECT v FROM r282_rhs) ORDER BY id)r282",{{R"r282(2)r282"}},R"r282()r282", {23}, -1},
        {R"r282(where_not)r282",R"r282(SELECT id FROM r282_lhs WHERE v NOT IN(SELECT v FROM r282_rhs) ORDER BY id)r282",{},R"r282()r282", {23}, -1},
        {R"r282(trivia)r282",R"r282(SELECT 2 IN /*(fake*/ (SELECT 2 /*')'*/))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(keyword_text)r282",R"r282(SELECT 'SELECT' IN(SELECT 'SELECT'))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(nested)r282",R"r282(SELECT 1 IN(SELECT 1 WHERE 1 IN(SELECT 1)))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, -1},
        {R"r282(nested_not)r282",R"r282(SELECT 1 NOT IN(SELECT 1 WHERE 1 NOT IN(SELECT 2)))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, -1},
        {R"r282(width)r282",R"r282(SELECT 1 IN(SELECT 1,2))r282",{},R"r282(42601)r282", {}, -1},
        {R"r282(missing_empty)r282",R"r282(SELECT 1 IN(SELECT missing_column FROM r282_empty))r282",{},R"r282(42703)r282", {}, -1},
        {R"r282(division)r282",R"r282(SELECT 1 IN(SELECT 1/0))r282",{},R"r282(22012)r282", {}, -1},
        {R"r282(missing_relation)r282",R"r282(SELECT 1 IN(SELECT v FROM r282_missing_relation))r282",{},R"r282(42P01)r282", {}, -1},
        {R"r282(empty_left)r282",R"r282(SELECT nextval('__SEQ__') IN(SELECT 1 WHERE FALSE))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, 0},
        {R"r282(hashed_left)r282",R"r282(SELECT nextval('__SEQ__') IN(SELECT 1 UNION ALL SELECT 2))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, 1},
        {R"r282(complete_right)r282",R"r282(SELECT 0 IN(SELECT nextval('__SEQ__') FROM r282_lhs))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, 3},
        {R"r282(filtered_right)r282",R"r282(SELECT 0 IN(SELECT nextval('__SEQ__') FROM r282_lhs WHERE FALSE))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, 0},
        {R"r282(empty_left_not)r282",R"r282(SELECT nextval('__SEQ__') NOT IN(SELECT 1 WHERE FALSE))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, 0},
        {R"r282(hashed_left_not)r282",R"r282(SELECT nextval('__SEQ__') NOT IN(SELECT 1 UNION ALL SELECT 2))r282",{{R"r282(f)r282"}},R"r282()r282", {16}, 1},
        {R"r282(complete_right_not)r282",R"r282(SELECT 0 NOT IN(SELECT nextval('__SEQ__') FROM r282_lhs))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, 3},
        {R"r282(filtered_right_not)r282",R"r282(SELECT 0 NOT IN(SELECT nextval('__SEQ__') FROM r282_lhs WHERE FALSE))r282",{{R"r282(t)r282"}},R"r282()r282", {16}, 0},
    };
    size_t controls=0,failures=0;int serial=0;
    const auto verify=[&](bool good,const std::string& label){++controls;if(good)return;++failures;std::cerr << "TYPED_IN_NATIVE_FAIL " << label << '\n';};
    for(const auto& test:cases){
        std::string sql=test.sql,state,message;Rows output;std::vector<uint32_t> oids;bool ok=false;
        const std::string sequence="typed_in_sequence_"+std::to_string(++serial);
        if(test.calls>=0){
            assert(!ddl.executeSql("CREATE SEQUENCE "+sequence+" START 1",session));
            const std::string marker="__SEQ__";
            for(size_t at=0;(at=sql.find(marker,at))!=std::string::npos;at+=sequence.size())sql.replace(at,marker.size(),sequence);
        }
        try{
            auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql,{}));
            auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,prepared,prepared->ast.get(),{});
            const auto result=QueryPlanner::executePlanChecked(std::move(plan));result.throwIfFailed();
            if(!result.structuredRowsAvailable)throw DbError("XX000","typed IN lost structured rows");
            for(const auto& column:prepared->output)oids.push_back(column.typeOid?column.typeOid:mapBuiltinTypeNameToOid(column.type));
            for(size_t row=0;row<result.structuredRows.size();++row){
                if(result.structuredNulls.size()!=result.structuredRows.size() || result.structuredNulls[row].size()!=result.structuredRows[row].size())throw DbError("XX000","typed IN lost NULL metadata");
                std::vector<std::optional<std::string>> values;
                for(size_t col=0;col<result.structuredRows[row].size();++col)values.push_back(result.structuredNulls[row][col]?std::nullopt:std::optional<std::string>{result.structuredRows[row][col]});
                output.push_back(std::move(values));
            }
            ok=true;
        }catch(const DbError& error){state=error.sqlState();message=error.message();}
        catch(const std::exception& error){message=error.what();}
        const bool expectedOk=!*test.state;
        verify(ok==expectedOk && state==test.state,std::string(test.name)+" state="+state+" "+message);
        verify(!expectedOk || (ok && output==test.rows && oids==test.oids),std::string(test.name)+" row/type mismatch");
        if(test.calls>=0){
            const auto current=g_engine.plpgsqlQuery(db,"SELECT currval('"+sequence+"')");
            verify(test.calls?current.ok && current.firstRow==std::vector<std::optional<std::string>>{std::to_string(test.calls)}:!current.ok && current.sqlState=="55000",std::string(test.name)+" currval");
            const auto next=g_engine.plpgsqlQuery(db,"SELECT nextval('"+sequence+"')");
            verify(next.ok && next.firstRow==std::vector<std::optional<std::string>>{std::to_string(test.calls+1)},std::string(test.name)+" nextval");
        }
    }
    std::cout << "TYPED_SCALAR_IN_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    size_t apiControls=0,apiFailures=0;
    const auto apiCheck=[&](const std::string& label,const std::function<bool()>& action){++apiControls;try{if(action())return;}catch(const std::exception& error){std::cerr<<error.what()<<'\n';}++apiFailures;std::cerr<<"TYPED_IN_API_FAIL "<<label<<'\n';};
    for(const std::string sql:{"SELECT 1 IN(SELECT 1)","SELECT 1 NOT IN(SELECT 1)",
            "SELECT 1 IN(VALUES(1),(2))","SELECT 1 NOT IN(VALUES(1),(2))",
            "SELECT 1 IN(WITH q AS(SELECT 1 AS v) SELECT v FROM q)","SELECT 1 IN /*(fake*/ (SELECT 1 /*')'*/)"}){
        apiCheck(sql,[&]{
            auto query=g_engine.prepareBoundQuery(db,sql,{});auto* select=dynamic_cast<SelectStmt*>(query.ast.get());
            if(!select || select->selectList.size()!=1)return false;
            const Expr* value=select->selectList.front().expr.get();
            if(sql.find(" NOT IN")!=std::string::npos){const auto* negation=dynamic_cast<const UnaryOpExpr*>(value);if(!negation || negation->op!="NOT")return false;value=negation->operand.get();}
            const auto* any=dynamic_cast<const QuantifiedComparisonExpr*>(value);
            if(!any || any->op!="=" || any->quantifier!=QuantifiedComparisonExpr::Quantifier::Any || !any->comparison || !any->right->preparedSubquery)return false;
            const auto output=query.statementOutputs.find(any->right->preparedSubquery.get());
            const auto begin=any->right->sourceBegin,end=any->right->sourceEnd;
            return output!=query.statementOutputs.end() && output->second.size()==1 && query.source==sql &&
                begin<end && end<=sql.size() && sql[begin]=='(' && sql[end-1]==')';
        });
    }
    apiCheck("normal parser exposes typed scalar query",[&]{
        SQLParser parser;auto parsed=parser.parse("SELECT 1 IN(SELECT 1)");
        const auto* select=dynamic_cast<const SelectStmt*>(parsed.stmt.get());
        const auto* any=select?dynamic_cast<const QuantifiedComparisonExpr*>(select->selectList.front().expr.get()):nullptr;
        const auto* child=any?dynamic_cast<const LiteralExpr*>(any->right.get()):nullptr;
        if(!parsed.success || !any || any->op!="=" || any->quantifier!=QuantifiedComparisonExpr::Quantifier::Any ||
            !child || child->value!="(SELECT 1)" || child->preparedSubquery)return false;
        auto negative=parser.parse("SELECT 1 NOT IN(SELECT 1)");
        const auto* negativeSelect=dynamic_cast<const SelectStmt*>(negative.stmt.get());
        const auto* negation=negativeSelect?dynamic_cast<const UnaryOpExpr*>(negativeSelect->selectList.front().expr.get()):nullptr;
        const auto* same=negation?dynamic_cast<const QuantifiedComparisonExpr*>(negation->operand.get()):nullptr;
        return negative.success && negation && negation->op=="NOT" && same && same->op=="=" &&
            same->quantifier==QuantifiedComparisonExpr::Quantifier::Any;
    });
    apiCheck("legacy row query role retained",[&]{
        SQLParser parser;auto parsed=parser.parseForBinding("SELECT ROW(1,2) IN(SELECT 1,2)");
        const auto* select=dynamic_cast<const SelectStmt*>(parsed.stmt.get());
        const auto* in=select?dynamic_cast<const BinaryOpExpr*>(select->selectList.front().expr.get()):nullptr;
        return parsed.isValid() && in && in->right->type==ExprType::Subquery;
    });
    std::cout << "TYPED_SCALAR_IN_API_COMPLETE " << apiControls << " FAILURES " << apiFailures << '\n';
    setCurrentSession(nullptr);assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    return failures || apiFailures?1:0;
}
