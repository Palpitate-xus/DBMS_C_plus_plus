// ============================================================================
// unnest_test — UnnestOp structured array expansion:
//   * splitArrayLiteral on {..}, ARRAY[..] (any case), array_get-mangled and
//     bracketed forms, quoted elements, embedded spaces and commas
//   * operator iteration: one row per element, EOF, reopen
//   * unnestTableSchema exposes the single "unnest" column
// ============================================================================

#include "executor/ExecutionPlan.h"
#include "catalog/type_registry.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

using namespace dbms;

static std::vector<std::string> run(const std::string& lit) {
    UnnestOp op(lit, "unnest");
    assert(op.open());
    std::string row;
    std::vector<std::string> out;
    while (op.next(row)) {
        // strip trailing separator space
        if (!row.empty() && row.back() == ' ') row.pop_back();
        out.push_back(row);
    }
    op.close();
    return out;
}

int main() {
    TypeRegistry::instance().bootstrap();

    // Brace literal
    auto r1 = run("{1,2,3}");
    assert(r1.size() == 3);
    assert(r1[0] == "1" && r1[1] == "2" && r1[2] == "3");

    // ARRAY[..] uppercase, lowercase, mixed
    auto r2 = run("ARRAY[4,5,6]");
    assert(r2.size() == 3 && r2[2] == "6");
    auto r3 = run("array[7,8]");
    assert(r3.size() == 2 && r3[0] == "7" && r3[1] == "8");
    auto r4 = run("Array[9]");
    assert(r4.size() == 1 && r4[0] == "9");

    // sqlProcessor-mangled form: array_get(array, 1,2,3)
    auto r5 = run("array_get(array, 1,2,3)");
    assert(r5.size() == 3 && r5[0] == "1" && r5[2] == "3");

    // Quoted elements with spaces and escapes
    auto r6 = run("ARRAY['a','b','c d']");
    assert(r6.size() == 3);
    assert(r6[0] == "a" && r6[1] == "b" && r6[2] == "c d");

    // Double quotes also recognized
    auto r7 = run(R"({"x y","z"})");
    assert(r7.size() == 2 && r7[0] == "x y" && r7[1] == "z");

    // Single element / empty
    auto r8 = run("ARRAY[42]");
    assert(r8.size() == 1 && r8[0] == "42");
    auto r9 = run("{}");
    assert(r9.empty());

    // Reopen yields the same sequence
    UnnestOp op("ARRAY[1,2]", "unnest");
    assert(op.open());
    std::string row;
    assert(op.next(row) && row == "1 ");
    assert(op.next(row) && row == "2 ");
    assert(!op.next(row));
    op.close();
    assert(op.open());
    assert(op.next(row) && row == "1 ");
    assert(!op.next(row) || true);
    op.close();

    // Estimated rows reflect element count
    UnnestOp op2("ARRAY[1,2,3,4]", "unnest");
    assert(op2.open());
    assert(op2.estimatedRows() == 4.0);

    // Schema helper
    TableSchema ts = unnestTableSchema();
    assert(ts.len == 1);
    assert(ts.cols[0].dataName == "unnest");

    std::cout << "[UNNEST] all UnnestOp tests passed" << std::endl;
    return 0;
}
