#include "utils/plpgsql.h"
#include "expression/expr_helper.h"
#include "catalog/type_registry.h"
#include <iostream>

using namespace dbms;

static PlPgsqlQueryResult scalar(const ExprEvalResult& value) {
    PlPgsqlQueryResult result;
    result.ok = value.ok; result.sqlState = value.sqlState; result.message = value.error;
    if (value.ok) {
        result.rowCount = result.columnCount = 1; result.columnTypes = {value.typeName};
        result.firstRow = {value.isNull ? std::nullopt : std::optional<std::string>{value.value}};
    }
    return result;
}

int main() {
    TypeRegistry::instance().bootstrap();
    PlPgsqlHost host;
    host.evalExprTyped = [](const auto& text, const auto& vars, const auto& nulls, const auto& types) {
        return scalar(ExprHelper::evalStringWithNulls(text, vars, nulls, types));
    };
    host.coerceValueTyped = [](const std::optional<std::string>& value,
                               const std::string& sourceType, const std::string& targetType) {
        return scalar(ExprHelper::evalStringWithNulls("CAST(__input AS " + targetType + ")",
            {{"__input", value.value_or("")}}, value ? std::set<std::string>{} : std::set<std::string>{"__input"},
            {{"__input", sourceType.empty() ? "unknown" : sourceType}}));
    };
    struct Case { const char* name; const char* body; const char* expected; bool isNull; };
    const Case cases[]={
        {R"r287(block_0)r287", R"r287(BEGIN NULL; RETURN 'after'; END;)r287", R"r287(after)r287",false},
        {R"r287(if_0)r287", R"r287(DECLARE n INT:=0; BEGIN IF TRUE THEN NULL; ELSE n:=999; END IF; RETURN n; END;)r287", R"r287(0)r287",false},
        {R"r287(while_0)r287", R"r287(DECLARE n INT:=0; BEGIN WHILE n<3 LOOP NULL; n:=n+1; END LOOP; RETURN n; END;)r287", R"r287(3)r287",false},
        {R"r287(for_0)r287", R"r287(DECLARE n INT:=0; BEGIN FOR i IN 1..3 LOOP NULL; n:=n+i; END LOOP; RETURN n; END;)r287", R"r287(6)r287",false},
        {R"r287(loop_0)r287", R"r287(DECLARE n INT:=0; BEGIN LOOP NULL; n:=n+1; EXIT WHEN n=3; END LOOP; RETURN n; END;)r287", R"r287(3)r287",false},
        {R"r287(block_1)r287", R"r287(BEGIN null; RETURN 'after'; END;)r287", R"r287(after)r287",false},
        {R"r287(if_1)r287", R"r287(DECLARE n INT:=0; BEGIN IF TRUE THEN null; ELSE n:=999; END IF; RETURN n; END;)r287", R"r287(0)r287",false},
        {R"r287(while_1)r287", R"r287(DECLARE n INT:=0; BEGIN WHILE n<3 LOOP null; n:=n+1; END LOOP; RETURN n; END;)r287", R"r287(3)r287",false},
        {R"r287(for_1)r287", R"r287(DECLARE n INT:=0; BEGIN FOR i IN 1..3 LOOP null; n:=n+i; END LOOP; RETURN n; END;)r287", R"r287(6)r287",false},
        {R"r287(loop_1)r287", R"r287(DECLARE n INT:=0; BEGIN LOOP null; n:=n+1; EXIT WHEN n=3; END LOOP; RETURN n; END;)r287", R"r287(3)r287",false},
        {R"r287(block_2)r287", R"r287(BEGIN NuLl; RETURN 'after'; END;)r287", R"r287(after)r287",false},
        {R"r287(if_2)r287", R"r287(DECLARE n INT:=0; BEGIN IF TRUE THEN NuLl; ELSE n:=999; END IF; RETURN n; END;)r287", R"r287(0)r287",false},
        {R"r287(while_2)r287", R"r287(DECLARE n INT:=0; BEGIN WHILE n<3 LOOP NuLl; n:=n+1; END LOOP; RETURN n; END;)r287", R"r287(3)r287",false},
        {R"r287(for_2)r287", R"r287(DECLARE n INT:=0; BEGIN FOR i IN 1..3 LOOP NuLl; n:=n+i; END LOOP; RETURN n; END;)r287", R"r287(6)r287",false},
        {R"r287(loop_2)r287", R"r287(DECLARE n INT:=0; BEGIN LOOP NuLl; n:=n+1; EXIT WHEN n=3; END LOOP; RETURN n; END;)r287", R"r287(3)r287",false},
        {R"r287(block_3)r287", R"r287(BEGIN NULL /* comment */ ; RETURN 'after'; END;)r287", R"r287(after)r287",false},
        {R"r287(if_3)r287", R"r287(DECLARE n INT:=0; BEGIN IF TRUE THEN NULL /* comment */ ; ELSE n:=999; END IF; RETURN n; END;)r287", R"r287(0)r287",false},
        {R"r287(while_3)r287", R"r287(DECLARE n INT:=0; BEGIN WHILE n<3 LOOP NULL /* comment */ ; n:=n+1; END LOOP; RETURN n; END;)r287", R"r287(3)r287",false},
        {R"r287(for_3)r287", R"r287(DECLARE n INT:=0; BEGIN FOR i IN 1..3 LOOP NULL /* comment */ ; n:=n+i; END LOOP; RETURN n; END;)r287", R"r287(6)r287",false},
        {R"r287(loop_3)r287", R"r287(DECLARE n INT:=0; BEGIN LOOP NULL /* comment */ ; n:=n+1; EXIT WHEN n=3; END LOOP; RETURN n; END;)r287", R"r287(3)r287",false},
        {R"r287(block_4)r287", R"r287(BEGIN NULL -- line comment
; RETURN 'after'; END;)r287", R"r287(after)r287",false},
        {R"r287(if_4)r287", R"r287(DECLARE n INT:=0; BEGIN IF TRUE THEN NULL -- line comment
; ELSE n:=999; END IF; RETURN n; END;)r287", R"r287(0)r287",false},
        {R"r287(while_4)r287", R"r287(DECLARE n INT:=0; BEGIN WHILE n<3 LOOP NULL -- line comment
; n:=n+1; END LOOP; RETURN n; END;)r287", R"r287(3)r287",false},
        {R"r287(for_4)r287", R"r287(DECLARE n INT:=0; BEGIN FOR i IN 1..3 LOOP NULL -- line comment
; n:=n+i; END LOOP; RETURN n; END;)r287", R"r287(6)r287",false},
        {R"r287(loop_4)r287", R"r287(DECLARE n INT:=0; BEGIN LOOP NULL -- line comment
; n:=n+1; EXIT WHEN n=3; END LOOP; RETURN n; END;)r287", R"r287(3)r287",false},
        {R"r287(block_5)r287", R"r287(BEGIN /* leading */ NULL/* outer /* nested */ end */; RETURN 'after'; END;)r287", R"r287(after)r287",false},
        {R"r287(if_5)r287", R"r287(DECLARE n INT:=0; BEGIN IF TRUE THEN /* leading */ NULL/* outer /* nested */ end */; ELSE n:=999; END IF; RETURN n; END;)r287", R"r287(0)r287",false},
        {R"r287(while_5)r287", R"r287(DECLARE n INT:=0; BEGIN WHILE n<3 LOOP /* leading */ NULL/* outer /* nested */ end */; n:=n+1; END LOOP; RETURN n; END;)r287", R"r287(3)r287",false},
        {R"r287(for_5)r287", R"r287(DECLARE n INT:=0; BEGIN FOR i IN 1..3 LOOP /* leading */ NULL/* outer /* nested */ end */; n:=n+i; END LOOP; RETURN n; END;)r287", R"r287(6)r287",false},
        {R"r287(loop_5)r287", R"r287(DECLARE n INT:=0; BEGIN LOOP /* leading */ NULL/* outer /* nested */ end */; n:=n+1; EXIT WHEN n=3; END LOOP; RETURN n; END;)r287", R"r287(3)r287",false},
        {R"r287(repeat)r287", R"r287(BEGIN NULL; NULL; NULL; RETURN 'done'; END;)r287", R"r287(done)r287",false},
        {R"r287(return_null)r287", R"r287(BEGIN NULL; RETURN NULL; END;)r287", R"r287()r287",true},
        {R"r287(text_null)r287", R"r287(BEGIN NULL; RETURN 'null'; END;)r287", R"r287(null)r287",false},
        {R"r287(quoted_null)r287", R"r287(DECLARE "null" INT:=17; BEGIN NULL; RETURN "null"; END;)r287", R"r287(17)r287",false},
        {R"r287(prefix)r287", R"r287(DECLARE nullify INT:=7; BEGIN nullify:=9; NULL; RETURN nullify; END;)r287", R"r287(9)r287",false},
        {R"r287(found_false)r287", R"r287(BEGIN NULL; IF FOUND THEN RETURN 't'; ELSE RETURN 'f'; END IF; END;)r287", R"r287(f)r287",false},
        {R"r287(found_true)r287", R"r287(BEGIN FOR i IN 1..1 LOOP NULL; END LOOP; NULL; IF FOUND THEN RETURN 't'; ELSE RETURN 'f'; END IF; END;)r287", R"r287(t)r287",false},
        {R"r287(found_empty)r287", R"r287(BEGIN FOR i IN 2..1 LOOP NULL; END LOOP; NULL; IF FOUND THEN RETURN 't'; ELSE RETURN 'f'; END IF; END;)r287", R"r287(f)r287",false},
        {R"r287(after_return)r287", R"r287(BEGIN RETURN 'before'; NULL; END;)r287", R"r287(before)r287",false},
        {R"r287(unreached)r287", R"r287(BEGIN IF FALSE THEN NULL; ELSE RETURN 'reached'; END IF; END;)r287", R"r287(reached)r287",false},
    };
    size_t controls=0,failures=0;
    const auto check=[&](bool valid,const std::string& label) {
        ++controls;if(!valid){++failures;std::cerr<<"NULL_STATEMENT_NATIVE_FAIL "<<label<<'\n';}
    };
    for(int mode=0;mode<3;++mode)for(const auto& test:cases){
        auto embedding=mode==1?PlPgsqlHost{}:host;size_t calls=0;
        if(mode!=1)embedding.execStmt=[&](const auto&,const auto&){++calls;return mode==2;};
        std::string value,error,state;bool isNull=false;bool ok=false;
        try {ok=PlPgsql::run(test.body,{},embedding,value,error,nullptr,&isNull,nullptr,&state);}
        catch(const std::exception& e){error=e.what();}
        check(ok && error.empty() && state.empty() && isNull==test.isNull &&
              (isNull || value==test.expected) && calls==0,std::string(test.name)+" mode="+std::to_string(mode));
    }
    const std::vector<std::string> malformed={
        R"r287(NULL x;)r287",
        R"r287(NULL::text;)r287",
        R"r287(NULL+1;)r287",
        R"r287(NULL := 1;)r287",
        R"r287(NULL = 1;)r287",
        R"r287(NULL(NULL);)r287",
        R"r287(NULL NULL;)r287",
        R"r287(NULL THEN;)r287",
        R"r287(NULL END;)r287",
        R"r287(NULL RETURN 'x';)r287",
    };
    for(int mode=0;mode<3;++mode)for(const auto& text:malformed){
        auto embedding=mode==1?PlPgsqlHost{}:host;size_t calls=0;
        if(mode!=1)embedding.execStmt=[&](const auto&,const auto&){++calls;return true;};
        std::string value,error,state;bool ok=false;
        try {ok=PlPgsql::run("BEGIN PERFORM probe(); "+text+" END;",{},embedding,value,error,nullptr,nullptr,nullptr,&state);}
        catch(const std::exception& e){error=e.what();}
        check(!ok && state=="42601" && calls==0,"invalid NULL before any SQL: "+text);
    }
    for(int mode=0;mode<5;++mode){
        PlPgsqlHost probe;size_t calls=0;
        if(mode==1)probe.execStmt=[&](const auto&,const auto&){++calls;return false;};
        if(mode==2)probe.execStmt=[&](const auto&,const auto&)->bool{++calls;throw std::runtime_error("must not dispatch NULL");};
        if(mode==3)probe.queryPrepared=[&](const auto&,const auto&,const auto&){++calls;return PlPgsqlQueryResult{};};
        if(mode==4){
            probe.query=[&](const auto&,const auto&){++calls;return PlPgsqlQueryResult{};};
            probe.selectInto=[&](const auto&,const auto&,auto&){++calls;return 2;};
            probe.evalExprTyped=[&](const auto&,const auto&,const auto&,const auto&){++calls;return PlPgsqlQueryResult{};};
        }
        std::string value,error,state;bool isNull=false,ok=false;
        try {ok=PlPgsql::run("BEGIN NULL; END;",{},probe,value,error,nullptr,&isNull,nullptr,&state);}
        catch(const std::exception& e){error=e.what();}
        check(ok && isNull && error.empty() && state.empty() && calls==0,"no-op needs no host, mode="+std::to_string(mode));
    }
    for(bool succeed:{true,false}){
        auto probe=host;std::vector<std::string> sql;
        probe.execStmt=[&](const auto& text,const auto&){sql.push_back(text);return succeed;};
        std::string value,error,state;
        const bool ok=PlPgsql::run("BEGIN NULL; PERFORM 1; NULL; RETURN 'done'; END;",{},probe,value,error,nullptr,nullptr,nullptr,&state);
        check(sql==std::vector<std::string>{"SELECT 1"} && ok==succeed &&
              (succeed ? value=="done" && state.empty() : !error.empty() && state=="XX000"),"ordinary SQL still dispatched once");
    }
    if(controls!=157){++failures;std::cerr<<"NULL_STATEMENT_BAD_INVENTORY "<<controls<<'\n';}
    std::cout<<"PLPGSQL_NULL_STATEMENT_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
