#include "commands/TableManage.h"
#include "parser/query_binding.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    const auto database=testDbPath("generate_series_bound_metadata");
    assert(g_engine.createDatabase(database)==dbms::DBStatus::OK);
    // Preserve the newly discovered valid PG18 case as its own ordinary
    // binding requirement, not a negative expectation or an unsupported skip.
    const auto query=g_engine.prepareBoundQuery(database,
        "SELECT lower('ABC'),length('a'),abs(2),generate_series(1,2)");
    const std::vector<uint32_t> expected={25,23,23,23};
    assert(query.output.size()==expected.size());
    for(size_t i=0;i<expected.size();++i)assert(query.output[i].typeOid==expected[i]);
    std::cout<<"[GENERATE SERIES BOUND METADATA] PG18 scalar/SRF positive passed\n";
}
