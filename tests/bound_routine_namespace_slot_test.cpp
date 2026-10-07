#include "commands/TableManage.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include "test_utils.h"
#include "utils/Session.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="bound_routine_namespace_slot",db=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.currentDB=db;session.searchPath="public, pg_catalog";
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore{Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    assert(g_engine.createUDF(db,"bound_source",{}, {},"SELECT 11",'i',"sql","integer")==DBStatus::OK);
    SQLParser parser;auto query=parser.parseForBinding("SELECT bound_source()");assert(query.isValid());
    auto* select=static_cast<SelectStmt*>(query.stmt.get());
    auto* call=dynamic_cast<FunctionCallExpr*>(select->selectList[0].expr.get());assert(call);
    ExprEvaluator evaluator;evaluator.setCurrentDB(db);
    evaluator.bindScalarFunctions(call,&g_engine);
    const auto key=call->funcName;
    const auto identity=evaluator.scalarFunctionIdentity(call,&g_engine);
    // This is a distinct legal SQL routine, not an owned callback slot.
    assert(g_engine.createUDF(db,key,{}, {},"SELECT 'hijack'::TEXT",'v',"sql","text")==DBStatus::OK);
    assert(evaluator.scalarFunctionResultType(call,&g_engine)=="integer");
    assert(evaluator.scalarFunctionIdentity(call,&g_engine)==identity);
    assert(evaluator.scalarFunctionVolatility(call,&g_engine)=='i');
    evaluator.bindScalarFunctions(call,&g_engine);
    assert(call->funcName==key);
    const auto actual=evaluator.eval(call,RowContext{});
    assert(!actual.isNull && actual.typeName=="integer" && actual.value=="11");
    // An unbound SQL occurrence with that spelling must select the real
    // public routine, even when catalog explicitly precedes public. Internal
    // callback slots are not pg_catalog functions or a prefix whitelist.
    session.searchPath="pg_catalog, public";
    auto ordinary=parser.parseForBinding("SELECT "+key+"()");assert(ordinary.isValid());
    auto* other=dynamic_cast<FunctionCallExpr*>(static_cast<SelectStmt*>(ordinary.stmt.get())->selectList[0].expr.get());assert(other);
    assert(evaluator.scalarFunctionResultType(other,&g_engine)=="text");
    evaluator.bindScalarFunctions(other,&g_engine);assert(other->funcName!=key);
    const auto user=evaluator.eval(other,RowContext{});assert(!user.isNull && user.value=="hijack" && user.typeName=="text");
    auto catalog=parser.parseForBinding("SELECT pg_catalog."+key+"()");assert(catalog.isValid());
    const auto* nonexistent=dynamic_cast<const FunctionCallExpr*>(static_cast<SelectStmt*>(catalog.stmt.get())->selectList[0].expr.get());
    assert(nonexistent && !evaluator.hasScalarFunction(nonexistent,&g_engine));
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"[BOUND ROUTINE NAMESPACE SLOT] actual callback ownership survives SQL name collision and rebind\n";
}
