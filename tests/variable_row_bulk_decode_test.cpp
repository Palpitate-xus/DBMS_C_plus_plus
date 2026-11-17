#include "catalog/type_registry.h"
#include "commands/StoredRowNull.h"
#include "commands/TableManage.h"

#include <cstring>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
size_t checks = 0;
void need(bool ok, const char* message) {
    ++checks;
    if (!ok) throw std::runtime_error(message);
}
std::string row(const std::vector<std::string>& values, size_t fixed = 4) {
    std::string raw(fixed + values.size() * 4, '\0');
    const int32_t id = 42;
    std::memcpy(raw.data(), &id, sizeof(id));
    if (fixed == 8) {
        const int32_t middle = 77;
        std::memcpy(raw.data() + 4, &middle, sizeof(middle));
    }
    for (size_t i = 0; i < values.size(); ++i) {
        const auto offset = static_cast<uint16_t>(raw.size());
        const auto length = static_cast<uint16_t>(values[i].size());
        std::memcpy(raw.data() + fixed + i * 4, &offset, sizeof(offset));
        std::memcpy(raw.data() + fixed + 2 + i * 4, &length, sizeof(length));
        raw += values[i];
    }
    return raw;
}
}

int main() {
    using namespace dbms;
    try {
        TypeRegistry::instance().bootstrap();
        TableSchema schema;
        schema.tablename = "directory_decode";
        schema.formatVersion = DATA_FILE_FORMAT_VERSION;
        schema.append(makeIntColumn("id", false, 2));
        schema.append(makeTextColumn("a", true));
        schema.append(makeTextColumn("b", true));
        schema.append(makeTextColumn("c", true));
        need(schema.fixedDataSize() == 4 && schema.varColCount() == 3,
             "fixture uses one 4-byte integer and three directory entries");
        const auto raw = row({"alpha", "", "omega"});
        const std::vector<bool> nulls{false, false, true, false};
        std::vector<std::string> values;
        std::vector<bool> outputNulls;
        need(detail::decodeStoredRowProjection(g_engine, "", raw, schema, nulls,
                {"a", "c"}, values, outputNulls), "selected decode");
        need(values == std::vector<std::string>{"alpha", "omega"} &&
             outputNulls == std::vector<bool>{false, false}, "selected values");
        need(detail::decodeStoredRowProjection(g_engine, "", raw, schema, nulls,
                {}, values, outputNulls), "full decode");
        need(values == std::vector<std::string>{"42", "alpha", "", "omega"} &&
             outputNulls == nulls, "full values and NULL");
        bool resolved = false;
        {
            detail::ScopedStoredRowNullBitmap owner(schema, nulls);
            need(g_engine.resolveToastValues("", schema.tablename, raw, schema, &resolved) == raw && resolved,
                 "full-row resolver retains inline values");
        }

        auto bad = raw;
        const uint16_t invalid = 0;
        std::memcpy(bad.data() + 4, &invalid, sizeof(invalid));
        values = {"unchanged"}; outputNulls = {true};
        bool corrupt = false;
        try {
            (void)detail::decodeStoredRowProjection(g_engine, "", bad, schema,
                nulls, {"c"}, values, outputNulls);
        } catch (const DbError& error) { corrupt = error.sqlState() == "XX001"; }
        need(corrupt, "full directory checked before selecting later field");
        need(values == std::vector<std::string>{"unchanged"} && outputNulls == std::vector<bool>{true},
             "failure publishes no partial row");
        corrupt = false;
        try {
            detail::ScopedStoredRowNullBitmap owner(schema, nulls);
            (void)g_engine.resolveToastValues("", schema.tablename, bad, schema, &resolved);
        } catch (const DbError& error) { corrupt = error.sqlState() == "XX001"; }
        need(corrupt, "full resolver rejects corrupt directory");

        const std::vector<bool> allVariableNull{false, true, true, true};
        need(detail::decodeStoredRowProjection(g_engine, "", bad, schema,
                allVariableNull, {"a", "c"}, values, outputNulls), "NULL-only decode");
        need(values == std::vector<std::string>{"", ""} && outputNulls == std::vector<bool>{true, true},
             "NULL-only demand keeps original behavior");
        {
            detail::ScopedStoredRowNullBitmap owner(schema, allVariableNull);
            need(g_engine.resolveToastValues("", schema.tablename, bad, schema, &resolved) == bad && resolved,
                 "all-NULL resolver does not introduce directory demand");
            need(detail::decodeStoredRowProjection(g_engine, "", raw, schema,
                    nulls, {"a", "c"}, values, outputNulls), "nested decoder");
            need(values == std::vector<std::string>{"alpha", "omega"}, "nested values");
            need(StorageEngine::extractColumnValueStatic(raw, schema, 1).empty(),
                 "outer bitmap restored after nested decoder");
        }
        need(detail::decodeStoredRowProjection(g_engine, "", bad, schema,
                nulls, {"id"}, values, outputNulls), "fixed-only decode");
        need(values == std::vector<std::string>{"42"} && outputNulls == std::vector<bool>{false},
             "fixed-only demand does not inspect variable directory");
        need(!detail::decodeStoredRowProjection(g_engine, "", bad, schema,
                nulls, {"missing"}, values, outputNulls), "unknown selection keeps fallback");
        TableSchema mixed;
        mixed.tablename = "interleaved_directory_decode";
        mixed.formatVersion = DATA_FILE_FORMAT_VERSION;
        mixed.append(makeIntColumn("id", false, 2));
        mixed.append(makeTextColumn("a", true));
        mixed.append(makeIntColumn("middle", false, 2));
        mixed.append(makeTextColumn("b", true));
        mixed.append(makeTextColumn("c", true));
        const auto mixedRow = row({"alpha", "", "omega"}, 8);
        const std::vector<bool> mixedNulls{false, false, false, true, false};
        need(detail::decodeStoredRowProjection(g_engine, "", mixedRow, mixed,
                mixedNulls, {"a", "middle", "c"}, values, outputNulls), "interleaved projection");
        need(values == std::vector<std::string>{"alpha", "77", "omega"} &&
             outputNulls == std::vector<bool>{false, false, false}, "physical and variable ordinals differ");
        {
            detail::ScopedStoredRowNullBitmap owner(mixed, mixedNulls);
            need(g_engine.resolveToastValues("", mixed.tablename, mixedRow, mixed, &resolved) == mixedRow && resolved,
                 "interleaved resolver advances ordinal across NULL fields");
        }
        need(detail::decodeStoredRowProjection(g_engine, "", mixedRow, mixed,
                mixedNulls, {"c"}, values, outputNulls) && values == std::vector<std::string>{"omega"},
             "last field ordinal counts unselected fields");
        std::cout << "VARIABLE_ROW_BULK_DECODE_PASS checks=" << checks << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
