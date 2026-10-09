#include "parser/parser.h"
#include "common/DbError.h"
#include <cassert>
#include <cstdlib>
#include <iostream>

// Registered before production globals initialize, so this callback runs
// after function-local grammar objects first touched during main are gone.
// Engine shutdown can still need exactly these pure declaration parsers.
static void parseAtExit() {
    using namespace dbms;
    for (const auto& spelling : {"int","numeric(10,2)","character varying(7)",
                                 "timestamp with time zone"}) {
        const auto type=SQLParser::parseTypeSpecification(spelling);
        assert(!type.typeName.empty());
    }
    bool rejected=false;
    try { (void)SQLParser::parseTypeSpecification("SELECT"); }
    catch (const DbError& error) { rejected=error.sqlState()=="42601"; }
    assert(rejected);
    std::cout << "[PARSER TYPE EXIT LIFETIME] after-destruction declaration grammar passed\n";
}
struct RegisterTypeExit {
    RegisterTypeExit() { assert(std::atexit(parseAtExit)==0); }
};
static RegisterTypeExit registeredTypeExit;

int main() {
    for (const auto& spelling : {"int","numeric(10,2)","character varying(7)",
                                 "timestamp with time zone"})
        assert(!dbms::SQLParser::parseTypeSpecification(spelling).typeName.empty());
}
