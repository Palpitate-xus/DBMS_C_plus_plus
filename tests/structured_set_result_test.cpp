#include "catalog/type_registry.h"
#include "commands/DmlExecutor.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

namespace {

dbms::DmlResult oneColumn(
    const std::string& type,
    const std::vector<std::string>& values,
    const std::vector<bool>& nulls,
    const std::string& name = "v") {
    assert(values.size() == nulls.size());
    dbms::DmlResult result;
    result.available = true;
    result.columns = {name};
    result.columnTypes = {type};
    for (size_t i = 0; i < values.size(); ++i) {
        result.rows.push_back({values[i]});
        result.nulls.push_back({nulls[i]});
    }
    result.commandTag = "SELECT " + std::to_string(values.size());
    return result;
}

void testLosslessUnion() {
    const auto left = oneColumn(
        "text", {"a b", "", "NULL", "", "line1\nline2", "  edge  "},
        {false, false, false, true, false, false});
    const auto right = oneColumn(
        "text", {"a b", "", "NULL", "", "tail"},
        {false, false, false, true, false});
    dbms::DmlResult output;
    dbms::StructuredSetError error;
    assert(dbms::combineStructuredSetResults(
        left, right, dbms::StructuredSetOperation::Union, false,
        "", "", output, error));
    assert(output.columns == std::vector<std::string>{"v"});
    assert(output.columnTypes == std::vector<std::string>{"text"});
    assert(output.commandTag == "SELECT 7");
    assert((output.rows == std::vector<std::vector<std::string>>{
        {"a b"}, {""}, {"NULL"}, {""}, {"line1\nline2"},
        {"  edge  "}, {"tail"}}));
    assert((output.nulls == std::vector<std::vector<bool>>{
        {false}, {false}, {false}, {true}, {false}, {false}, {false}}));
}

void testNullAwareMultiset() {
    const auto left = oneColumn(
        "text", {"", "", "", "", "NULL"},
        {false, false, true, true, false});
    const auto right = oneColumn(
        "text", {"", "", "", "NULL", "NULL"},
        {false, true, true, false, false});
    dbms::DmlResult output;
    dbms::StructuredSetError error;
    assert(dbms::combineStructuredSetResults(
        left, right, dbms::StructuredSetOperation::Intersect, true,
        "", "", output, error));
    assert((output.rows == std::vector<std::vector<std::string>>{
        {""}, {""}, {""}, {"NULL"}}));
    assert((output.nulls == std::vector<std::vector<bool>>{
        {false}, {true}, {true}, {false}}));
}

void testCommonTypesAndFailures() {
    dbms::DmlResult output;
    dbms::StructuredSetError error;
    assert(dbms::combineStructuredSetResults(
        oneColumn("integer", {"1"}, {false}),
        oneColumn("bigint", {"1"}, {false}),
        dbms::StructuredSetOperation::Union, false,
        "", "", output, error));
    assert(output.columnTypes == std::vector<std::string>{"bigint"});
    assert(output.rows == std::vector<std::vector<std::string>>{{"1"}});

    assert(dbms::combineStructuredSetResults(
        oneColumn("name", {"alice"}, {false}),
        oneColumn("text", {"alice"}, {false}),
        dbms::StructuredSetOperation::Union, false,
        "", "", output, error));
    assert(output.columnTypes == std::vector<std::string>{"text"});
    assert(output.rows == std::vector<std::vector<std::string>>{{"alice"}});

    dbms::DmlResult twoColumns;
    twoColumns.available = true;
    twoColumns.columns = {"a", "b"};
    twoColumns.columnTypes = {"integer", "integer"};
    assert(!dbms::combineStructuredSetResults(
        oneColumn("integer", {}, {}), twoColumns,
        dbms::StructuredSetOperation::Union, false,
        "", "", output, error));
    assert(error.sqlState == "42601");

    assert(!dbms::combineStructuredSetResults(
        oneColumn("integer", {"1"}, {false}),
        oneColumn("date", {"2024-01-01"}, {false}),
        dbms::StructuredSetOperation::Union, false,
        "", "", output, error));
    assert(error.sqlState == "42804");

    dbms::DmlResult malformed = oneColumn("text", {"x"}, {false});
    malformed.nulls.clear();
    assert(!dbms::combineStructuredSetResults(
        malformed, oneColumn("text", {}, {}),
        dbms::StructuredSetOperation::Union, false,
        "", "", output, error));
    assert(error.sqlState == "0A000");
}

} // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    testLosslessUnion();
    testNullAwareMultiset();
    testCommonTypesAndFailures();
    std::cout << "[STRUCTURED SET RESULT] all passed" << std::endl;
    return 0;
}
