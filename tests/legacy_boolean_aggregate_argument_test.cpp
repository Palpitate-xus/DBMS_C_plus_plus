#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

struct Population { bool ok = false; size_t rows = 0, nonNull = 0; };

static Population effects(dbms::StorageEngine& owner, const std::string& database) {
    Population result;
    result.ok = owner.forEachRow(database, "effects", [&](uint32_t page, uint16_t slot, const char*, size_t) {
        ++result.rows;
        if (!owner.isColumnNullByRid(database, "effects", dbms::StorageEngine::encodeRid(page, slot), 0))
            ++result.nonNull;
    });
    return result;
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    const auto database = testDbPath("legacy_boolean_aggregate_argument");
    assert(owner.createDatabase(database) == DBStatus::OK);
    TableSchema table; table.tablename = "source";
    table.append(makeIntColumn("g", false, 2));
    table.append(makeIntColumn("v", true, 2));
    table.append(makeBooleanColumn("b", true));
    auto text = makeVarCharColumn("t", true, 64); text.collation = "C";
    table.append(text);
    assert(owner.createTable(database, table) == DBStatus::OK);
    TableSchema sink; sink.tablename = "effects"; sink.append(makeIntColumn("v", true, 2));
    assert(owner.createTable(database, sink) == DBStatus::OK);
    for (const auto& row : std::vector<StorageEngine::SqlRow>{
            {{"g", "1"}, {"v", "6"}, {"b", "true"}, {"t", ""}},
            {{"g", "1"}, {"v", "9"}, {"b", "true"}, {"t", "NULL"}},
            {{"g", "2"}, {"v", std::nullopt}, {"b", std::nullopt}, {"t", std::nullopt}},
            {{"g", "3"}, {"v", "1"}, {"b", "false"}, {"t", "zeta"}}})
        assert(owner.insertRow(database, "source", row) == DBStatus::OK);
    const auto actual = owner.getTableSchema(database, "source");
    assert(actual.len == 4 && actual.cols[0].dsize == 4 && actual.cols[1].dsize == 4 && actual.cols[2].dsize == 1);
    assert(owner.createUDF(database, "argument_truth", {"arg"}, {"int"},
        "BEGIN INSERT INTO effects(v) VALUES(arg); RETURN arg > 5; END;",
        'v', "plpgsql", "boolean") == DBStatus::OK);
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls; if (!good) ++failures;
        std::cout << "LEGACY_BOOLEAN_ARGUMENT " << label << " pass=" << good << std::endl;
    };
    const auto matches = [&](const std::string& function, const std::string& argument,
            const std::vector<std::string>& where, const std::vector<std::string>& filter,
            const std::optional<bool>& expected) {
        try {
            const auto rows = owner.aggregate(database, "source", where, {{function, argument, filter, {}}});
            const std::string value = expected ? (*expected ? "t " : "f ") : "NULL ";
            return rows == std::vector<std::string>{value};
        } catch (const std::exception& error) {
            std::cout << "LEGACY_BOOLEAN_ERROR " << argument << " " << error.what() << std::endl;
            return false;
        }
    };
    for (const auto* function : {"bool_and", "bool_or", "every"}) {
        for (const auto* argument : {"b", "v > 5", "v>5", "CAST(b AS BOOLEAN)",
                "COALESCE(b,NULL::boolean)", "distinct b", "distinct v>5", "distinct CAST(b AS BOOLEAN)"}) {
            for (int scope = 0; scope < 4; ++scope) {
                const auto where = scope == 2 ? std::vector<std::string>{"=g 2"} : std::vector<std::string>{};
                const auto filter = scope == 1 ? std::vector<std::string>{"=g 1"} :
                    scope == 3 ? std::vector<std::string>{"=g 999"} : std::vector<std::string>{};
                const auto expected = scope >= 2 ? std::optional<bool>{} :
                    std::optional<bool>(scope == 1 || std::string(function) == "bool_or");
                check(matches(function, argument, where, filter, expected),
                    std::string(function) + "(" + argument + ") scope=" + std::to_string(scope));
            }
        }
    }
    for (const auto& control : std::vector<std::pair<std::string, std::string>>{
            {"1 / (v - v) > 0", "22012"}, {"CAST(t AS integer) > 0", "22P02"}}) {
        std::string state;
        try { owner.aggregate(database, "source", {}, {{"bool_and", control.first, {}, {}}}); }
        catch (const DbError& error) { state = error.sqlState(); }
        catch (const std::exception&) { state = "XX000"; }
        check(state == control.second, "actual-error " + control.second);
    }
    check(matches("bool_and", "1 / (v - v) > 0", {}, {"=g 999"}, std::nullopt), "FILTER-before-row-error");
    check(owner.getLockManager().captureCheckpoint().tableCounts.empty() &&
          matches("bool_and", "b", {}, {}, false), "after-error-lock-release");
    for (const auto* function : {"bool_and", "bool_or", "every"}) {
        for (const bool filtered : {false, true}) {
            assert(owner.beginTransaction(database) == DBStatus::OK && owner.beginSqlCommand());
            const std::string label = std::string(function) + (filtered ? " filtered" : " all");
            check(matches(function, "distinct argument_truth(v)", {},
                filtered ? std::vector<std::string>{"=g 1"} : std::vector<std::string>{},
                filtered || std::string(function) == "bool_or"), label + " volatile-result");
            assert(owner.finishSqlCommand() && owner.beginSqlCommand());
            const auto population = effects(owner, database);
            const auto outsider = effects(g_engine, database);
            check(population.ok && population.rows == (filtered ? 2u : 4u) &&
                population.nonNull == (filtered ? 2u : 3u) && outsider.ok && outsider.rows == 0 &&
                owner.inTransaction() && !g_engine.inTransaction(), label + " exact-private-effects-and-owner");
            const auto status = owner.rollbackTransaction();
            const auto empty = effects(owner, database);
            check(status == DBStatus::OK && empty.ok && empty.rows == 0, label + " rollback");
        }
    }
    if (controls != 118) return 2;
    std::cout << "LEGACY_BOOLEAN_ARGUMENT_COMPLETE " << controls << " FAILURES " << failures << std::endl;
    return failures ? 1 : 0;
}
