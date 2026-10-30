#include "utils/plpgsql.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name="plpgsql_scalar_queries",db=testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db,"utf8")==dbms::DBStatus::OK);
    Session session;session.username="review_user";session.permission=1;session.currentDB=db;
    dbms::setCurrentSession(&session);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE r281_rows(id INT,value TEXT)",session));
    assert(!ddl.executeSql("CREATE TABLE r281_empty(id INT)",session));
    for (int i=1;i<=3;++i)
        assert(g_engine.insertRow(db,"r281_rows",{{"id",std::to_string(i)},{"value",std::string(1,static_cast<char>('a'+i-1))}})==dbms::DBStatus::OK);
    struct Case {const char* name;const char* body;const char* expected;const char* state;bool isNull;int calls;};
    const Case cases[] = {
        {R"r281(if_one)r281", R"r281(BEGIN IF (SELECT 1) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281(yes)r281", R"r281()r281", false, -1},
        {R"r281(elsif_one)r281", R"r281(BEGIN IF FALSE THEN RETURN 'bad'; ELSIF (SELECT 1) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281(yes)r281", R"r281()r281", false, -1},
        {R"r281(while_one)r281", R"r281(BEGIN WHILE (SELECT 1) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r281", R"r281(yes)r281", R"r281()r281", false, -1},
        {R"r281(exit_one)r281", R"r281(BEGIN LOOP EXIT WHEN (SELECT 1); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r281", R"r281(yes)r281", R"r281()r281", false, -1},
        {R"r281(if_true)r281", R"r281(BEGIN IF (SELECT TRUE) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281(yes)r281", R"r281()r281", false, -1},
        {R"r281(elsif_true)r281", R"r281(BEGIN IF FALSE THEN RETURN 'bad'; ELSIF (SELECT TRUE) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281(yes)r281", R"r281()r281", false, -1},
        {R"r281(while_true)r281", R"r281(BEGIN WHILE (SELECT TRUE) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r281", R"r281(yes)r281", R"r281()r281", false, -1},
        {R"r281(exit_true)r281", R"r281(BEGIN LOOP EXIT WHEN (SELECT TRUE); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r281", R"r281(yes)r281", R"r281()r281", false, -1},
        {R"r281(if_null)r281", R"r281(BEGIN IF (SELECT NULL) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281(no)r281", R"r281()r281", false, -1},
        {R"r281(elsif_null)r281", R"r281(BEGIN IF FALSE THEN RETURN 'bad'; ELSIF (SELECT NULL) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281(no)r281", R"r281()r281", false, -1},
        {R"r281(while_null)r281", R"r281(BEGIN WHILE (SELECT NULL) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r281", R"r281(no)r281", R"r281()r281", false, -1},
        {R"r281(exit_null)r281", R"r281(BEGIN LOOP EXIT WHEN (SELECT NULL); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r281", R"r281(no)r281", R"r281()r281", false, -1},
        {R"r281(if_empty)r281", R"r281(BEGIN IF (SELECT 1 WHERE FALSE) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281(no)r281", R"r281()r281", false, -1},
        {R"r281(elsif_empty)r281", R"r281(BEGIN IF FALSE THEN RETURN 'bad'; ELSIF (SELECT 1 WHERE FALSE) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281(no)r281", R"r281()r281", false, -1},
        {R"r281(while_empty)r281", R"r281(BEGIN WHILE (SELECT 1 WHERE FALSE) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r281", R"r281(no)r281", R"r281()r281", false, -1},
        {R"r281(exit_empty)r281", R"r281(BEGIN LOOP EXIT WHEN (SELECT 1 WHERE FALSE); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r281", R"r281(no)r281", R"r281()r281", false, -1},
        {R"r281(if_invalid_boolean)r281", R"r281(BEGIN IF (SELECT 2) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281()r281", R"r281(22P02)r281", false, -1},
        {R"r281(elsif_invalid_boolean)r281", R"r281(BEGIN IF FALSE THEN RETURN 'bad'; ELSIF (SELECT 2) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281()r281", R"r281(22P02)r281", false, -1},
        {R"r281(while_invalid_boolean)r281", R"r281(BEGIN WHILE (SELECT 2) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r281", R"r281()r281", R"r281(22P02)r281", false, -1},
        {R"r281(exit_invalid_boolean)r281", R"r281(BEGIN LOOP EXIT WHEN (SELECT 2); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r281", R"r281()r281", R"r281(22P02)r281", false, -1},
        {R"r281(if_division)r281", R"r281(BEGIN IF (SELECT 1/0) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281()r281", R"r281(22012)r281", false, -1},
        {R"r281(elsif_division)r281", R"r281(BEGIN IF FALSE THEN RETURN 'bad'; ELSIF (SELECT 1/0) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281()r281", R"r281(22012)r281", false, -1},
        {R"r281(while_division)r281", R"r281(BEGIN WHILE (SELECT 1/0) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r281", R"r281()r281", R"r281(22012)r281", false, -1},
        {R"r281(exit_division)r281", R"r281(BEGIN LOOP EXIT WHEN (SELECT 1/0); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r281", R"r281()r281", R"r281(22012)r281", false, -1},
        {R"r281(if_multiple_rows)r281", R"r281(BEGIN IF (SELECT 1 UNION ALL SELECT 2) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281()r281", R"r281(21000)r281", false, -1},
        {R"r281(elsif_multiple_rows)r281", R"r281(BEGIN IF FALSE THEN RETURN 'bad'; ELSIF (SELECT 1 UNION ALL SELECT 2) THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281()r281", R"r281(21000)r281", false, -1},
        {R"r281(while_multiple_rows)r281", R"r281(BEGIN WHILE (SELECT 1 UNION ALL SELECT 2) LOOP RETURN 'yes'; END LOOP; RETURN 'no'; END;)r281", R"r281()r281", R"r281(21000)r281", false, -1},
        {R"r281(exit_multiple_rows)r281", R"r281(BEGIN LOOP EXIT WHEN (SELECT 1 UNION ALL SELECT 2); RETURN 'no'; END LOOP; RETURN 'yes'; END;)r281", R"r281()r281", R"r281(21000)r281", false, -1},
        {R"r281(return_select)r281", R"r281(BEGIN RETURN (SELECT 7); END;)r281", R"r281(7)r281", R"r281()r281", false, -1},
        {R"r281(return_arithmetic)r281", R"r281(BEGIN RETURN 1+(SELECT 7); END;)r281", R"r281(8)r281", R"r281()r281", false, -1},
        {R"r281(return_empty)r281", R"r281(BEGIN RETURN (SELECT 7 WHERE FALSE); END;)r281", R"r281()r281", R"r281()r281", true, -1},
        {R"r281(return_null)r281", R"r281(BEGIN RETURN (SELECT NULL); END;)r281", R"r281()r281", R"r281()r281", true, -1},
        {R"r281(return_text_null)r281", R"r281(BEGIN RETURN (SELECT 'null'); END;)r281", R"r281(null)r281", R"r281()r281", false, -1},
        {R"r281(return_text_empty)r281", R"r281(BEGIN RETURN (SELECT ''); END;)r281", R"r281()r281", R"r281()r281", false, -1},
        {R"r281(return_text_quote)r281", R"r281(BEGIN RETURN (SELECT 'O''Brien'); END;)r281", R"r281(O'Brien)r281", R"r281()r281", false, -1},
        {R"r281(return_division)r281", R"r281(BEGIN RETURN (SELECT 1/0); END;)r281", R"r281()r281", R"r281(22012)r281", false, -1},
        {R"r281(return_multiple_rows)r281", R"r281(BEGIN RETURN (SELECT 7 UNION ALL SELECT 8); END;)r281", R"r281()r281", R"r281(21000)r281", false, -1},
        {R"r281(return_multiple_columns)r281", R"r281(BEGIN RETURN (SELECT 7,8); END;)r281", R"r281()r281", R"r281(42601)r281", false, -1},
        {R"r281(return_exists)r281", R"r281(BEGIN RETURN EXISTS(SELECT 1); END;)r281", R"r281(true)r281", R"r281()r281", false, -1},
        {R"r281(return_exists_empty)r281", R"r281(BEGIN RETURN EXISTS(SELECT 1 WHERE FALSE); END;)r281", R"r281(false)r281", R"r281()r281", false, -1},
        {R"r281(return_in)r281", R"r281(BEGIN RETURN 1 IN(SELECT 1); END;)r281", R"r281(true)r281", R"r281()r281", false, -1},
        {R"r281(return_in_null)r281", R"r281(BEGIN RETURN 1 IN(SELECT CAST(NULL AS INT)); END;)r281", R"r281()r281", R"r281()r281", true, -1},
        {R"r281(return_in_untyped_null)r281", R"r281(BEGIN RETURN 1 IN(SELECT NULL); END;)r281", R"r281()r281", R"r281(42883)r281", false, -1},
        {R"r281(return_not_in_null)r281", R"r281(BEGIN RETURN 1 NOT IN(SELECT CAST(NULL AS INT)); END;)r281", R"r281()r281", R"r281()r281", true, -1},
        {R"r281(return_any)r281", R"r281(BEGIN RETURN 1=ANY(SELECT 1); END;)r281", R"r281(true)r281", R"r281()r281", false, -1},
        {R"r281(return_all)r281", R"r281(BEGIN RETURN 1=ALL(SELECT 1); END;)r281", R"r281(true)r281", R"r281()r281", false, -1},
        {R"r281(return_derived)r281", R"r281(BEGIN RETURN (SELECT d.x FROM (SELECT 7 AS x) d); END;)r281", R"r281(7)r281", R"r281()r281", false, -1},
        {R"r281(return_nested)r281", R"r281(BEGIN RETURN (SELECT (SELECT 7)); END;)r281", R"r281(7)r281", R"r281()r281", false, -1},
        {R"r281(return_typed_bigint)r281", R"r281(BEGIN RETURN (SELECT CAST(5000000000 AS BIGINT)); END;)r281", R"r281(5000000000)r281", R"r281()r281", false, -1},
        {R"r281(return_table_value)r281", R"r281(BEGIN RETURN (SELECT value FROM r281_rows WHERE id=2); END;)r281", R"r281(b)r281", R"r281()r281", false, -1},
        {R"r281(return_table_multiple)r281", R"r281(BEGIN RETURN (SELECT id FROM r281_rows); END;)r281", R"r281()r281", R"r281(21000)r281", false, -1},
        {R"r281(return_missing_empty_column)r281", R"r281(BEGIN RETURN (SELECT missing_column FROM r281_empty); END;)r281", R"r281()r281", R"r281(42703)r281", false, -1},
        {R"r281(default)r281", R"r281(DECLARE x INT:=(SELECT 7); BEGIN RETURN x; END;)r281", R"r281(7)r281", R"r281()r281", false, -1},
        {R"r281(assignment)r281", R"r281(DECLARE x INT; BEGIN x:=(SELECT 7); RETURN x; END;)r281", R"r281(7)r281", R"r281()r281", false, -1},
        {R"r281(default_null)r281", R"r281(DECLARE x INT:=(SELECT 7 WHERE FALSE); BEGIN RETURN x; END;)r281", R"r281()r281", R"r281()r281", true, -1},
        {R"r281(assignment_text_null)r281", R"r281(DECLARE x TEXT; BEGIN x:=(SELECT 'null'); RETURN x; END;)r281", R"r281(null)r281", R"r281()r281", false, -1},
        {R"r281(for_controls)r281", R"r281(BEGIN FOR i IN (SELECT 1)..(SELECT 1) BY (SELECT 1) LOOP RETURN i; END LOOP; RETURN 0; END;)r281", R"r281(1)r281", R"r281()r281", false, -1},
        {R"r281(local_parameter)r281", R"r281(DECLARE x INT:=7; BEGIN RETURN (SELECT x); END;)r281", R"r281(7)r281", R"r281()r281", false, -1},
        {R"r281(qualified_local)r281", R"r281(<<b>> DECLARE x INT:=7; BEGIN RETURN (SELECT b.x); END b;)r281", R"r281(7)r281", R"r281()r281", false, -1},
        {R"r281(quoted_local)r281", R"r281(DECLARE "X" INT:=7; x INT:=9; BEGIN RETURN (SELECT "X"+x); END;)r281", R"r281(16)r281", R"r281()r281", false, -1},
        {R"r281(dot_label)r281", R"r281(<<"b.dot">> DECLARE x INT:=7; BEGIN RETURN (SELECT "b.dot".x); END "b.dot";)r281", R"r281(7)r281", R"r281()r281", false, -1},
        {R"r281(shadowed_scope)r281", R"r281(<<outer_b>> DECLARE x INT:=7; BEGIN <<inner_b>> DECLARE x INT:=9; BEGIN RETURN (SELECT outer_b.x+inner_b.x); END inner_b; END outer_b;)r281", R"r281(16)r281", R"r281()r281", false, -1},
        {R"r281(source_collision)r281", R"r281(DECLARE id INT:=7; BEGIN RETURN (SELECT id FROM r281_empty); END;)r281", R"r281()r281", R"r281(42702)r281", false, -1},
        {R"r281(source_qualified)r281", R"r281(DECLARE id INT:=7; BEGIN RETURN (SELECT r281_rows.id FROM r281_rows WHERE r281_rows.id=2); END;)r281", R"r281(2)r281", R"r281()r281", false, -1},
        {R"r281(correlated_inner)r281", R"r281(DECLARE wanted INT:=2; BEGIN RETURN (SELECT t.id FROM r281_rows t WHERE t.id=wanted); END;)r281", R"r281(2)r281", R"r281()r281", false, -1},
        {R"r281(unreached)r281", R"r281(BEGIN IF FALSE THEN RETURN (SELECT 1/0); END IF; RETURN 'no'; END;)r281", R"r281(no)r281", R"r281()r281", false, -1},
        {R"r281(expression_alias)r281", R"r281(BEGIN RETURN 7 AS result; END;)r281", R"r281(7)r281", R"r281()r281", false, -1},
        {R"r281(expression_where_empty)r281", R"r281(BEGIN RETURN 7 WHERE FALSE; END;)r281", R"r281()r281", R"r281()r281", true, -1},
        {R"r281(expression_from)r281", R"r281(BEGIN RETURN id FROM r281_rows WHERE id=2; END;)r281", R"r281(2)r281", R"r281()r281", false, -1},
        {R"r281(expression_from_empty)r281", R"r281(BEGIN RETURN id FROM r281_empty; END;)r281", R"r281()r281", R"r281()r281", true, -1},
        {R"r281(expression_from_multiple)r281", R"r281(BEGIN RETURN id FROM r281_rows; END;)r281", R"r281()r281", R"r281(21000)r281", false, -1},
        {R"r281(expression_limit)r281", R"r281(BEGIN RETURN id FROM r281_rows ORDER BY id DESC LIMIT 1; END;)r281", R"r281(3)r281", R"r281()r281", false, -1},
        {R"r281(expression_count)r281", R"r281(BEGIN RETURN count(*) FROM r281_rows; END;)r281", R"r281(3)r281", R"r281()r281", false, -1},
        {R"r281(condition_count)r281", R"r281(BEGIN IF count(*)>0 FROM r281_rows THEN RETURN 'yes'; ELSE RETURN 'no'; END IF; END;)r281", R"r281(yes)r281", R"r281()r281", false, -1},
        {R"r281(missing_child_before_effect)r281", R"r281(BEGIN RETURN nextval('__SEQ__')+(SELECT missing_column FROM r281_empty); END;)r281", R"r281()r281", R"r281(42703)r281", false, 0},
        {R"r281(child_width_before_effect)r281", R"r281(BEGIN RETURN (SELECT nextval('__SEQ__'),1); END;)r281", R"r281()r281", R"r281(42601)r281", false, 0},
        {R"r281(outer_width_after_effect)r281", R"r281(BEGIN RETURN nextval('__SEQ__'),1; END;)r281", R"r281()r281", R"r281(42601)r281", false, 1},
        {R"r281(star_width_after_effect)r281", R"r281(BEGIN RETURN * FROM (SELECT nextval('__SEQ__') AS x,1 AS y) d; END;)r281", R"r281()r281", R"r281(42601)r281", false, 1},
        {R"r281(outer_all_rows_before_error)r281", R"r281(BEGIN RETURN nextval('__SEQ__') FROM r281_rows; END;)r281", R"r281()r281", R"r281(21000)r281", false, 3},
        {R"r281(child_two_row_demand)r281", R"r281(BEGIN RETURN (SELECT nextval('__SEQ__') FROM r281_rows); END;)r281", R"r281()r281", R"r281(21000)r281", false, 2},
        {R"r281(unreached_no_effect)r281", R"r281(BEGIN IF FALSE THEN RETURN (SELECT nextval('__SEQ__')); END IF; RETURN 'safe'; END;)r281", R"r281(safe)r281", R"r281()r281", false, 0},
        {R"r281(shortcircuit_no_effect)r281", R"r281(BEGIN RETURN CASE WHEN TRUE THEN 7 ELSE (SELECT nextval('__SEQ__')) END; END;)r281", R"r281(7)r281", R"r281()r281", false, 0},
        {R"r281(default_once)r281", R"r281(DECLARE x BIGINT:=(SELECT nextval('__SEQ__')); BEGIN RETURN x; END;)r281", R"r281(1)r281", R"r281()r281", false, 1},
        {R"r281(while_per_test_once)r281", R"r281(DECLARE n INT:=0; BEGIN WHILE (SELECT nextval('__SEQ__'))<3 LOOP n:=n+1; IF n>5 THEN EXIT; END IF; END LOOP; RETURN n; END;)r281", R"r281(2)r281", R"r281()r281", false, 3},
        {R"r281(raise_once)r281", R"r281(BEGIN RAISE NOTICE '%',(SELECT nextval('__SEQ__')); RETURN 'done'; END;)r281", R"r281(done)r281", R"r281()r281", false, 1},
        {R"r281(empty_no_effect)r281", R"r281(BEGIN RETURN nextval('__SEQ__') FROM r281_empty; END;)r281", R"r281()r281", R"r281()r281", true, 0},
    };
    size_t controls=0,failures=0;int serial=0;
    const auto verify=[&](bool good,const std::string& label) {
        ++controls;if(good)return;++failures;std::cerr << "SCALAR_QUERY_NATIVE_FAIL " << label << '\n';
    };
    for (const auto& test:cases) {
        const std::string function="scalar_query_"+std::to_string(++serial),sequence="scalar_seq_"+std::to_string(serial);
        std::string text=test.body;
        if (test.calls>=0) {
            assert(!ddl.executeSql("CREATE SEQUENCE "+sequence+" START 1",session));
            const std::string marker="__SEQ__";
            for(size_t at=0;(at=text.find(marker,at))!=std::string::npos;at+=sequence.size())text.replace(at,marker.size(),sequence);
        }
        assert(g_engine.createUDF(db,function,{}, {},text,'v',"plpgsql","text")==dbms::DBStatus::OK);
        std::string value,state,message;bool null=false,ok=false;
        try {ok=g_engine.callUDF(db,function,{},value,&null);}
        catch(const dbms::DbError& error){state=error.sqlState();message=error.message();}
        catch(const std::exception& error){message=error.what();}
        const bool expectedOk=!*test.state;
        verify(ok==expectedOk && state==test.state,std::string(test.name)+" state="+state+" error="+message);
        verify(!expectedOk || (ok && null==test.isNull && (null || value==test.expected)),std::string(test.name)+" value="+value+" null="+std::to_string(null));
        if(test.calls>=0){
            const auto current=g_engine.plpgsqlQuery(db,"SELECT currval('"+sequence+"')");
            verify(test.calls ? current.ok && current.firstRow==std::vector<std::optional<std::string>>{std::to_string(test.calls)} :
                !current.ok && current.sqlState=="55000",std::string(test.name)+" currval state="+current.sqlState);
            const auto next=g_engine.plpgsqlQuery(db,"SELECT nextval('"+sequence+"')");
            verify(next.ok && next.firstRow==std::vector<std::optional<std::string>>{std::to_string(test.calls+1)},std::string(test.name)+" nextval");
        }
    }
    std::cout << "PLPGSQL_SCALAR_QUERY_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    size_t apiControls=0,apiFailures=0;
    const auto apiCheck=[&](const std::string& label,const std::function<bool()>& action) {
        ++apiControls;
        try { if (action()) return; }
        catch (const std::exception& error) { std::cerr << error.what() << '\n'; }
        ++apiFailures;std::cerr << "SCALAR_QUERY_API_FAIL " << label << '\n';
    };
    const auto cell=[](std::optional<std::string> value,const std::string& type) {
        dbms::PlPgsqlQueryResult r;r.ok=true;r.rowCount=r.columnCount=1;r.columnTypes={type};r.firstRow={value};return r;
    };
    for (int mode=0;mode<7;++mode) {
        apiCheck("structured scalar/"+std::to_string(mode),[&] {
            dbms::PlPgsqlHost host;size_t queries=0,fallbacks=0;bool request=true;
            host.evalExprTyped=[&](const auto&,const auto&,const auto&,const auto&) {
                ++fallbacks;dbms::PlPgsqlQueryResult r;r.sqlState="XX001";r.message="wrong scalar fallback";return r;
            };
            host.queryPrepared=[&](const auto& sql,const auto&,const auto& options) {
                ++queries;request=request && sql=="SELECT probe()" && options.maxRows==0 &&
                    options.purpose==dbms::PlPgsqlQueryOptions::Purpose::StoredFunction;
                auto r=cell("7","integer");
                if (mode==1) {r.rowCount=0;r.firstRow.clear();}
                if (mode==2) r.rowCount=2;
                if (mode==3) {r.columnCount=2;r.columnTypes={"integer","integer"};r.firstRow={"7","8"};}
                if (mode==4) {r.ok=false;r.sqlState="22012";r.message="probe failed";}
                if (mode==5) r.firstRow.clear();
                if (mode==6) r.firstRow={std::nullopt};
                return r;
            };
            std::string value,error,state,type;bool null=false;
            const bool ok=dbms::PlPgsql::run("BEGIN RETURN probe(); END;",{},host,value,error,nullptr,&null,nullptr,&state,&type);
            if (!request || queries!=1 || fallbacks!=0) return false;
            if (mode==2 || mode==3 || mode==4 || mode==5)
                return !ok && state==(mode==2?"21000":mode==3?"42601":mode==4?"22012":"XX000");
            return ok && state.empty() && type=="integer" && null==(mode==1 || mode==6) && (null || value=="7");
        });
    }
    for (int mode=0;mode<4;++mode) {
        apiCheck("canonical namespace/"+std::to_string(mode),[&] {
            dbms::PlPgsqlHost host;size_t queries=0;bool good=true;
            host.functionLabel="routine_scope";host.parameterOrder={"p"};host.parameterTypes={{"p","integer"}};
            const std::string body=mode==1 ? "<<\"scope.dot\">> DECLARE \"x.y\" TEXT:='O''Brien'; BEGIN RETURN probe(); END \"scope.dot\";" :
                mode==2 ? "<<outer_b>> DECLARE x INT:=7; BEGIN <<inner_b>> DECLARE x INT:=9; BEGIN RETURN probe(); END inner_b; END outer_b;" :
                "BEGIN RETURN probe($1); END;";
            host.queryPrepared=[&](const auto& sql,const auto& bindings,const auto&) {
                ++queries;good=good && sql==(mode==1 || mode==2 ? "SELECT probe()" : "SELECT probe($1)");
                bool first=false,second=false;
                for (const auto& binding:bindings) {
                    if (mode==0 || mode==3) {
                        if (binding.name=="p" && binding.position==1 && binding.qualifiers==std::vector<std::string>{"routine_scope"})
                            first=binding.visible && binding.type=="integer" &&
                                (mode==3 ? !binding.value : binding.value==std::optional<std::string>{"3"});
                    } else if (mode==1 && binding.name=="x.y") {
                        first=binding.visible && binding.qualifiers==std::vector<std::string>{"scope.dot"} &&
                            binding.value==std::optional<std::string>{"O'Brien"};
                    } else if (mode==2 && binding.name=="x") {
                        if (binding.qualifiers==std::vector<std::string>{"inner_b"})first=binding.visible && binding.value==std::optional<std::string>{"9"};
                        if (binding.qualifiers==std::vector<std::string>{"outer_b"})second=!binding.visible && binding.value==std::optional<std::string>{"7"};
                    }
                }
                good=good && first && (mode!=2 || second);return cell(mode==3?std::nullopt:std::optional<std::string>{"7"},"integer");
            };
            std::string value,error,state;bool null=false;const std::set<std::string> nulls=mode==3 ? std::set<std::string>{"p"}:std::set<std::string>{};
            const bool ok=dbms::PlPgsql::run(body,{{"p","3"}},host,value,error,nullptr,&null,&nulls,&state);
            return ok && state.empty() && good && queries==1 && null==(mode==3);
        });
    }
    for (const std::string body : {
            "BEGIN IF TRUE THEN RETURN 'safe'; ELSIF probe() THEN RETURN 'bad'; END IF; END;",
            "BEGIN IF FALSE THEN RETURN probe(); END IF; RETURN 'safe'; END;"}) {
        apiCheck("unreached expression",[&] {
            dbms::PlPgsqlHost host;size_t queries=0;
            host.queryPrepared=[&](const auto&,const auto&,const auto&) {++queries;return cell("7","integer");};
            std::string value,error,state;
            return dbms::PlPgsql::run(body,{},host,value,error,nullptr,nullptr,nullptr,&state) && value=="safe" && queries==0;
        });
    }
    for (const bool evaluationError : {false,true}) {
        apiCheck("condition failure before effects",[&] {
            dbms::PlPgsqlHost host;size_t queries=0,notices=0;
            host.queryPrepared=[&](const auto&,const auto&,const auto&) {
                ++queries;auto r=cell("invalid","text");
                if (evaluationError) {r.ok=false;r.sqlState="22012";r.message="probe failed";}return r;
            };
            std::string value,error,state;
            const bool ok=dbms::PlPgsql::run("BEGIN IF probe() THEN RAISE NOTICE 'then'; ELSE RAISE NOTICE 'else'; END IF; RETURN 'bad'; END;",{},host,value,error,
                [&](const auto&,const auto&){++notices;},nullptr,nullptr,&state);
            return !ok && queries==1 && notices==0 && state==(evaluationError ? "22012":"22P02");
        });
    }
    apiCheck("WHILE one query per test",[&] {
        dbms::PlPgsqlHost host;size_t queries=0,notices=0;
        host.queryPrepared=[&](const auto& sql,const auto&,const auto& options) {
            if (sql!="SELECT probe()" || options.maxRows!=0) return dbms::PlPgsqlQueryResult{};
            return cell(++queries<3 ? "t":"f","boolean");
        };
        std::string value,error,state;
        const bool ok=dbms::PlPgsql::run("BEGIN WHILE probe() LOOP RAISE NOTICE 'tick'; END LOOP; RETURN 'done'; END;",{},host,value,error,
            [&](const auto&,const auto&){++notices;},nullptr,nullptr,&state);
        return ok && state.empty() && queries==3 && notices==2;
    });
    apiCheck("whole expression envelope",[&] {
        dbms::PlPgsqlHost host;size_t queries=0;
        host.queryPrepared=[&](const auto& sql,const auto&,const auto& options) {
            ++queries;if(sql!="SELECT value AS result FROM any_source" || options.maxRows!=0)return dbms::PlPgsqlQueryResult{};
            return cell("7","integer");
        };
        std::string value,error,state;
        return dbms::PlPgsql::run("BEGIN RETURN value AS result FROM any_source; END;",{},host,value,error,nullptr,nullptr,nullptr,&state) && value=="7" && queries==1;
    });
    for (const size_t demand : {size_t{0},size_t{1},size_t{2}}) {
        apiCheck("prepared SELECT owner/demand",[&] {
            size_t queries=0;bool good=true;
            g_engine.setPlpgsqlQueryExecutor([&](const auto&,const auto& sql,const auto& options) {
                ++queries;
                good=good && sql=="SELECT p" && options.maxRows==demand && options.preparedQuery &&
                    options.preparedStatement==options.preparedQuery->ast.get();
                if (options.preparedQuery)
                    good=good && options.preparedQuery->source=="SELECT p" && options.preparedQuery->parameters.size()==1 &&
                        options.preparedQuery->parameters.front().typeName=="integer" && options.preparedQuery->parameters.front().value=="7";
                return cell("7","integer");
            });
            struct Reset {~Reset(){g_engine.setPlpgsqlQueryExecutor({});}} reset;
            dbms::PlPgsqlQueryOptions options;options.maxRows=demand;
            dbms::QueryBindingDatum datum{"r281:p","p","integer",{"scope"},true,std::string{"7"},1};
            const auto result=g_engine.plpgsqlQueryPrepared(db,"SELECT p",{datum},options);
            return result.ok && good && queries==1;
        });
    }
    apiCheck("metadata failure before query executor",[&] {
        size_t queries=0;
        g_engine.setPlpgsqlQueryExecutor([&](const auto&,const auto&,const auto&) {++queries;return cell("7","integer");});
        struct Reset {~Reset(){g_engine.setPlpgsqlQueryExecutor({});}} reset;
        const auto result=g_engine.plpgsqlQueryPrepared(db,"SELECT missing_variable",{});
        return !result.ok && result.sqlState=="42703" && queries==0;
    });
    std::cout << "PLPGSQL_SCALAR_QUERY_API_COMPLETE " << apiControls << " FAILURES " << apiFailures << '\n';
    dbms::setCurrentSession(nullptr);
    const auto cleanup=g_engine.dropDatabase(db);
    if (cleanup!=dbms::DBStatus::OK) {std::cerr << "SCALAR_QUERY_CLEANUP_FAIL\n";return 1;}
    cleanupTestDb(name);
    return failures || apiFailures ? 1 : 0;
}
