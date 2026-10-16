#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

// Test the storage API's explicit BEFORE row-assignment extension, not a
// PostgreSQL CREATE FUNCTION/RETURNS trigger compatibility claim.
int main() {
    using namespace dbms;
    using Cell = StorageEngine::SqlCell;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    const auto database = testDbPath("trigger_assignment_typed_context");
    assert(owner.createDatabase(database) == DBStatus::OK);
    struct Case { std::string label, action, target; Cell expected, input; };
    const std::vector<Case> cases = {
        {"literal", "SET txt = 'NEW.a; OLD.a tg_name'", "txt", "NEW.a; OLD.a tg_name", "ordinary"},
        {"quoted-text", "NEW.txt = NEW.a", "txt", "O'Reilly; NEW.a", "O'Reilly; NEW.a"},
        {"empty-text", "SET txt = NEW.a", "txt", "", ""},
        {"null-word-text", "SET txt = NEW.a", "txt", "NULL", "NULL"},
        {"sql-null-text", "SET txt = NEW.a", "txt", std::nullopt, std::nullopt},
        {"prefix-text", "SET txt = NEW.ab", "txt", "right side", "left"},
        {"prefix-int", "SET num = NEW.xx", "num", "42", "left"},
        {"null-int", "SET num = NEW.x", "num", std::nullopt, "left"},
        {"null-predicate", "SET pred = NEW.x IS NULL", "pred", "t", "left"},
        {"coalesce", "SET num = COALESCE(NEW.x, NEW.xx)", "num", "42", "left"},
        {"lower-qualifier", "SET txt = new.a", "txt", "", ""},
        {"quoted-identifier", "SET txt = NEW.\"ab\"", "txt", "right side", "left"},
        {"sequential", "SET txt = NEW.a; SET txt = COALESCE(NEW.txt,'missing') || '!'", "txt", "missing!", std::nullopt},
        {"typed-length", "SET num = length(NEW.a)", "num", "4", "NULL"},
        {"literal-null", "SET txt = 'NULL'", "txt", "NULL", "left"},
        {"integer-expression", "SET num = NEW.xx / 2", "num", "21", "left"},
    };
    size_t controls = 0, failures = 0, callbacks = 0, expectedCallbacks = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) ++failures;
        std::cout << "TRIGGER_ASSIGNMENT " << label << " pass=" << good << std::endl;
    };
    owner.setTriggerExecutor([&](const std::string&) { ++callbacks; return false; });
    size_t ordinal = 0;
    for (const bool update : {false, true}) {
        auto population = cases;
        if (update) {
            population.push_back({"old-new-distinct", "SET txt = OLD.a || ' / ' || NEW.a", "txt", "old value / fresh", "fresh"});
            population.push_back({"old-null", "SET num = OLD.x", "num", std::nullopt, "fresh"});
            population.push_back({"old-prefix", "SET num = OLD.xx", "num", "70", "fresh"});
        }
        for (const auto& item : population) {
            const auto tableName = "assignment_" + std::to_string(++ordinal);
            TableSchema table; table.tablename = tableName; table.formatVersion = 2;
            table.append(makeIntColumn("id", false, 4, true));
            table.append(makeVarCharColumn("a", true, 128));
            table.append(makeVarCharColumn("ab", true, 128));
            table.append(makeIntColumn("x", true, 4));
            table.append(makeIntColumn("xx", false, 4));
            table.append(makeVarCharColumn("txt", true, 128));
            table.append(makeIntColumn("num", true, 4));
            table.append(makeBooleanColumn("pred", true));
            assert(owner.createTable(database, table) == DBStatus::OK);
            StorageEngine::SqlRow values{{"id", "1"}, {"a", item.input}, {"ab", "right side"},
                {"x", std::nullopt}, {"xx", "42"}, {"txt", "original"}, {"num", "7"}, {"pred", "false"}};
            if (update) {
                auto seed = values; seed["a"] = "old value"; seed["xx"] = "70";
                assert(owner.insertRow(database, tableName, seed) == DBStatus::OK);
            }
            assert(owner.createTrigger(database, {"rewrite", "before", update ? "update" : "insert",
                tableName, item.action, "", true, true, {}}) == DBStatus::OK);
            std::vector<StorageEngine::SqlRow> returned;
            const auto status = update
                ? owner.updateRows(database, tableName, values, {"=id 1"}, &returned)
                : owner.insertRow(database, tableName, values, &returned);
            ++expectedCallbacks;
            const auto label = std::string(update ? "UPDATE " : "INSERT ") + item.label;
            check(status == DBStatus::OK, label + " status=" + std::to_string(static_cast<int>(status)));
            const auto value = returned.size() == 1 && returned[0].count(item.target)
                ? returned[0].at(item.target) : Cell{};
            check(status == DBStatus::OK && returned.size() == 1 && returned[0].count(item.target) && value == item.expected,
                label + " returned=" + (value ? "[" + *value + "]" : "<SQL-NULL>"));
            const auto actualSchema = owner.getTableSchema(database, tableName);
            size_t targetIndex = actualSchema.len;
            for (size_t i = 0; i < actualSchema.len; ++i)
                if (actualSchema.cols[i].dataName == item.target) targetIndex = i;
            size_t rows = 0; bool nullBitMatches = true;
            const bool scanned = owner.forEachRow(database, tableName,
                [&](uint32_t page, uint16_t slot, const char*, size_t) {
                    ++rows;
                    nullBitMatches = nullBitMatches && targetIndex < actualSchema.len &&
                        owner.isColumnNullByRid(database, tableName,
                            StorageEngine::encodeRid(page, slot), targetIndex) == !item.expected.has_value();
                });
            const auto persisted = owner.query(database, tableName, {}, {item.target});
            check(status == DBStatus::OK && scanned && rows == 1 && nullBitMatches &&
                persisted == std::vector<std::string>{item.expected.value_or("NULL") + " "}, label + " stored-value-and-null-bit");
        }
    }
    for (const bool update : {false, true}) {
        for (const bool explicitTransaction : {false, true}) {
            for (const auto* expression : {"1 / 0", "NEW.missing", "CAST('bad' AS integer)"}) {
                const auto tableName = "failure_" + std::to_string(++ordinal);
                TableSchema table; table.tablename = tableName; table.formatVersion = 2;
                table.append(makeIntColumn("id", false, 4, true));
                table.append(makeVarCharColumn("txt", true, 128));
                assert(owner.createTable(database, table) == DBStatus::OK);
                if (update)
                    assert(owner.insertRow(database, tableName, {{"id", "1"}, {"txt", "original"}}) == DBStatus::OK);
                if (explicitTransaction) {
                    assert(owner.beginTransaction(database) == DBStatus::OK);
                    assert(owner.insertRow(database, tableName, {{"id", "9"}, {"txt", "survivor"}}) == DBStatus::OK);
                }
                assert(owner.createTrigger(database, {"reject", "before", update ? "update" : "insert",
                    tableName, std::string("SET txt = ") + expression, "", true, true, {}}) == DBStatus::OK);
                const std::vector<StorageEngine::SqlRow> sentinel{{{"sentinel", "keep"}}};
                auto returned = sentinel;
                const auto status = update
                    ? owner.updateRows(database, tableName, {{"txt", "changed"}}, {"=id 1"}, &returned)
                    : owner.insertRow(database, tableName, {{"id", "1"}, {"txt", "changed"}}, &returned);
                ++expectedCallbacks;
                const auto label = std::string(update ? "UPDATE " : "INSERT ") +
                    (explicitTransaction ? "transaction " : "autocommit ") + expression;
                check(status == DBStatus::INVALID_VALUE, label + " rejected");
                check(returned == sentinel, label + " returned-vector-unchanged");
                const auto rows = owner.query(database, tableName, {"=id 1"}, {"txt"});
                check(rows == (update ? std::vector<std::string>{"original "} : std::vector<std::string>{}) &&
                    owner.query(database, tableName, {"=id 9"}, {"txt"}) ==
                        (explicitTransaction ? std::vector<std::string>{"survivor "} : std::vector<std::string>{}),
                    label + " statement-atomic-and-prior-write-kept");
                const bool transactionKept = owner.inTransaction() == explicitTransaction;
                assert(owner.dropTrigger(database, "reject", tableName) == DBStatus::OK);
                const auto healthy = owner.insertRow(database, tableName, {{"id", "2"}, {"txt", "healthy"}});
                const auto committed = explicitTransaction ? owner.commitTransaction() : DBStatus::OK;
                check(transactionKept && healthy == DBStatus::OK && committed == DBStatus::OK &&
                    !owner.inTransaction() && owner.getLockManager().captureCheckpoint().tableCounts.empty() &&
                    owner.query(database, tableName, {"=id 2"}, {"txt"}) == std::vector<std::string>{"healthy "},
                    label + " transaction-and-lock-recovery");
            }
        }
    }
    check(callbacks == expectedCallbacks && expectedCallbacks == 47, "exact-callback-count");
    owner.setTriggerExecutor({});
    std::cout << "TRIGGER_ASSIGNMENT_COMPLETE " << controls << " FAILURES " << failures << std::endl;
    if (controls != 154) return 2;
    return failures ? 1 : 0;
}
