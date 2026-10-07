#include "commands/TableManage.h"
#include "expression/prepared_query_execution.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    const auto database = testDbPath("view_trigger_typed_parameter");
    assert(!owner.databaseExists(database));
    assert(owner.createDatabase(database, "utf8") == DBStatus::OK);
    TableSchema table; table.len = 4;
    const std::vector<std::string> names = {"id", "val", "wide", "nums"};
    const std::vector<std::string> types = {"integer", "text", "bigint", "integer"};
    for (size_t i = 0; i < names.size(); ++i) {
        table.cols[i].dataName = names[i];
        assert(TypeRegistry::instance().resolveColumnType(table.cols[i], types[i], {}, i == 3).empty());
    }
    assert(owner.createTable(database, "trigger_rows", table) == DBStatus::OK);
    assert(owner.createSequence(database, "trigger_prepare_calls", 1, 1) == DBStatus::OK);
    assert(owner.createUDF(database, "trigger_prepare_writer", {"p"}, {"integer"},
        "BEGIN PERFORM nextval('trigger_prepare_calls'); RETURN p; END;",
        'v', "plpgsql", "integer") == DBStatus::OK);
    for (const auto& value : std::vector<std::optional<std::string>>{
            std::nullopt, std::string{}, "NULL", "O'Brien, OLD.id; NEW.val"}) {
        std::vector<QueryBindingDatum> bindings = {
            {"new.val", "val", "text", {"new"}, false, value, 0},
            {"new.Case", "Case", "bigint", {"new"}, false, "9223372036854775807", 0},
            {"new.nums", "nums", "integer[]", {"new"}, false, "{3,NULL,4}", 0},
            {"old.id", "id", "integer", {"old"}, false, "10", 0},
        };
        auto prepared = std::make_shared<PreparedQuery>(owner.prepareBoundQuery(database,
            "UPDATE trigger_rows SET val=NEW.val,wide=NEW.\"Case\",nums=NEW.nums "
            "WHERE id=OLD.id AND val <> 'NEW.val'", bindings));
        auto* update = dynamic_cast<UpdateStmt*>(prepared->ast.get());
        assert(update && prepared->parameters.size() == 4 && prepared->uses.size() == 4);
        assert(prepared->legacySql().find("'NEW.val'") != std::string::npos);
        if (value && value->find("O'Brien") == 0)
            assert(prepared->legacySql().find("'O''Brien, OLD.id; NEW.val'") != std::string::npos);
        const PreparedQuery::SourceRange* target = nullptr;
        for (const auto& range : prepared->sourceRanges)
            if (range.owner == update && !range.source) target = &range;
        assert(target);
        PreparedQueryExecution execution(prepared, &owner, database);
        for (auto& assignment : update->setClauses) execution.prepareExpression(assignment.second.get());
        execution.prepareExpression(update->whereClause.get());
        auto row = execution.context();
        execution.setSourceRow(row, target->ordinal, {
            ExprValue("integer", "10", false), ExprValue("text", "OLD space", false),
            ExprValue("bigint", "0", false), ExprValue("integer[]", "{1,2}", false)});
        const auto cell = execution.evaluate(update->setClauses[0].second.get(), row);
        assert(cell.typeName == "text" && cell.isNull == !value.has_value());
        if (value) assert(cell.value == *value);
        const auto wide = execution.evaluate(update->setClauses[1].second.get(), row);
        assert(wide.typeName == "bigint" && wide.value == "9223372036854775807");
        const auto array = execution.evaluate(update->setClauses[2].second.get(), row);
        assert(array.typeName == "integer[]" && array.value == "{3,NULL,4}");
        assert(execution.evaluate(update->whereClause.get(), row).asBool());
    }
    bool missing = false;
    try {
        (void)owner.prepareBoundQuery(database,
            "UPDATE trigger_rows SET val=trigger_prepare_writer(1) WHERE missing=1");
    } catch (const DbError& error) { missing = error.sqlState() == "42703"; }
    assert(missing && owner.nextval(database, "trigger_prepare_calls") == 1);
    assert(!owner.inTransaction());
    assert(owner.dropDatabase(database) == DBStatus::OK);
    std::cout << "[VIEW TRIGGER TYPED PARAMETERS] declared types/NULL/datum bytes/slots/pure preparation passed\n";
}
