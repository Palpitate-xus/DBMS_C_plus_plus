#include "utils/Session.h"
#include <cassert>
#include <iostream>
int main() {
    Session session;session.searchPath="public";
    auto& state=session.searchPathTransaction;
    state.begin(session.searchPath);
    state.assign(session.searchPath,"public, pg_catalog",true);
    state.save("same",session.searchPath);
    state.assign(session.searchPath,"pg_catalog, public",false);
    state.save("same",session.searchPath);
    state.assign(session.searchPath,"pg_catalog",true);
    state.rollback("same",session.searchPath);assert(session.searchPath=="pg_catalog, public");
    state.release("same");
    state.rollback("same",session.searchPath);assert(session.searchPath=="public, pg_catalog");
    state.assign(session.searchPath,"\"Quoted.Path\"",false);
    state.assign(session.searchPath,"pg_catalog",true);
    // A short-rent backend copy carries the complete state rather than
    // referring to a returned backend's Session object.
    Session copy=session;copy.searchPathTransaction.finish(copy.searchPath,true);
    assert(copy.searchPath=="\"Quoted.Path\"" && !copy.searchPathTransaction.active);
    assert(session.searchPath=="pg_catalog" && state.active);
    state.finish(session.searchPath,false);assert(session.searchPath=="public" && !state.active);
    state.begin(session.searchPath);state.assign(session.searchPath,"pg_catalog",true);
    state.finish(session.searchPath,true);assert(session.searchPath=="public");
    std::cout<<"[SEARCH PATH TRANSACTION STATE] local/persistent, duplicate savepoints and value ownership passed\n";
}
