// Phase 1 parser feature tests
// Covers SET/SHOW/RESET, EXPLAIN, CREATE INDEX/VIEW, ALTER TABLE,
// SELECT extensions (GROUP BY ROLLUP/CUBE/GROUPING SETS, ORDER BY NULLS FIRST/LAST,
// LIMIT WITH TIES, FETCH FIRST), function calls (named args, window, schema-qualified),
// and VALUES, transaction option/savepoint parsing.

#include "parser.h"
#include "ast.h"
#include "Config.h"
#include <cassert>
#include <iostream>
#include <string>

using namespace dbms;

dbms::Config g_config;

static const SelectStmt* asSelect(const StmtPtr& stmt) {
    return dynamic_cast<const SelectStmt*>(stmt.get());
}

static const SetStmt* asSet(const StmtPtr& stmt) {
    return dynamic_cast<const SetStmt*>(stmt.get());
}

static const ExplainStmt* asExplain(const StmtPtr& stmt) {
    return dynamic_cast<const ExplainStmt*>(stmt.get());
}

static const CreateIndexStmt* asCreateIndex(const StmtPtr& stmt) {
    return dynamic_cast<const CreateIndexStmt*>(stmt.get());
}

static const CreateViewStmt* asCreateView(const StmtPtr& stmt) {
    return dynamic_cast<const CreateViewStmt*>(stmt.get());
}

static const RefreshMaterializedViewStmt* asRefreshMaterializedView(
    const StmtPtr& stmt) {
    return dynamic_cast<const RefreshMaterializedViewStmt*>(stmt.get());
}

static const CreateTableStmt* asCreateTable(const StmtPtr& stmt) {
    return dynamic_cast<const CreateTableStmt*>(stmt.get());
}

static const AlterTableStmt* asAlterTable(const StmtPtr& stmt) {
    return dynamic_cast<const AlterTableStmt*>(stmt.get());
}

static const FunctionCallExpr* asFuncCall(const ExprPtr& expr) {
    return dynamic_cast<const FunctionCallExpr*>(expr.get());
}

int main() {
    SQLParser parser;

    {
        const auto parsed = parser.parse(
            "DROP FUNCTION IF EXISTS pgdiff_drop_fn()");
        assert(parsed.success);
        const auto* drop = dynamic_cast<const DropStmt*>(parsed.stmt.get());
        assert(drop && drop->command == SqlCommand::DropFunction &&
               drop->ifExists);
        assert((drop->objectNames ==
                std::vector<std::string>{"pgdiff_drop_fn", "(", ")"}));
    }
    {
        const auto parsed = parser.parse(
            "DROP PROCEDURE IF EXISTS pgdiff_drop_proc()");
        assert(parsed.success);
        const auto* drop = dynamic_cast<const DropStmt*>(parsed.stmt.get());
        assert(drop && drop->command == SqlCommand::DropProcedure &&
               drop->ifExists);
        assert((drop->objectNames ==
                std::vector<std::string>{"pgdiff_drop_proc", "(", ")"}));
    }
    {
        const auto parsed = parser.parse(
            "DROP ROUTINE IF EXISTS pgdiff_drop_routine()");
        assert(parsed.success);
        const auto* drop = dynamic_cast<const DropStmt*>(parsed.stmt.get());
        assert(drop && drop->command == SqlCommand::DropRoutine &&
               drop->ifExists);
        assert((drop->objectNames ==
                std::vector<std::string>{"pgdiff_drop_routine", "(", ")"}));
    }

    // PostgreSQL's default NULL position follows the sort direction, and
    // OFFSET's optional ROW(S) must not consume the following FETCH clause.
    {
        const auto parsed = parser.parse(
            "SELECT id FROM items ORDER BY id DESC OFFSET 1 ROWS FETCH NEXT 1 ROW ONLY");
        assert(parsed.success);
        const auto* select = asSelect(parsed.stmt);
        assert(select && select->orderBy.size() == 1);
        assert(!select->orderBy[0].asc && select->orderBy[0].nullsFirst);
        assert(select->offset == 1 && select->limit == 1 && select->fetchFirst);
        const auto explicitNulls = parser.parse(
            "SELECT id FROM items ORDER BY id DESC NULLS LAST FETCH FIRST 1 ROW ONLY");
        assert(explicitNulls.success);
        assert(!asSelect(explicitNulls.stmt)->orderBy[0].nullsFirst);
    }

    // 1. classify()
    assert(SQLParser::classify("SET timezone = 'UTC'") == SqlCommand::Set);
    assert(SQLParser::classify("SHOW search_path") == SqlCommand::Show);
    assert(SQLParser::classify("RESET client_encoding") == SqlCommand::Reset);
    assert(SQLParser::classify("EXPLAIN SELECT 1") == SqlCommand::Explain);
    assert(SQLParser::classify("SELECT 1") == SqlCommand::Select);
    assert(SQLParser::classify("(SELECT 1 UNION SELECT 2)") ==
           SqlCommand::Select);
    assert(SQLParser::requiresQuerySnapshot(
        "((SELECT 1 UNION SELECT 2))"));
    assert(SQLParser::classify("CREATE INDEX idx ON t (a)") == SqlCommand::CreateIndex);
    assert(SQLParser::classify("REFRESH MATERIALIZED VIEW mv") ==
           SqlCommand::RefreshMaterializedView);
    assert(SQLParser::classify("REFRESH\nMATERIALIZED\tVIEW mv") ==
           SqlCommand::RefreshMaterializedView);
    std::cout << "[PARSER P1] classify OK\n";

    {
        auto parsed = parser.parse(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY reporting.mv WITH NO DATA;");
        assert(parsed.success);
        const auto* refresh = asRefreshMaterializedView(parsed.stmt);
        assert(refresh != nullptr);
        assert(refresh->viewName == "reporting.mv");
        assert(refresh->concurrently);
        assert(!refresh->withData);
        assert(!parser.parse("REFRESH MATERIALIZED VIEW").success);
        assert(!parser.parse(
            "REFRESH MATERIALIZED VIEW mv WITH NO DATA trailing").success);
    }
    std::cout << "[PARSER P1] REFRESH MATERIALIZED VIEW OK\n";

    // MySQL-only DML LIMIT syntax is intentionally rejected.  It used to have
    // a dead legacy implementation behind the typed DML fail-closed boundary.
    assert(!parser.parse("UPDATE users SET age = 0 LIMIT 10").success);
    assert(!parser.parse("DELETE FROM users WHERE age > 100 LIMIT 10").success);
    std::cout << "[PARSER P1] non-PG DML LIMIT rejected\n";

    // The project-only database switch still uses the common parser in
    // extended mode. Preserve one target token and reject missing/trailing
    // input so the executor never derives a name with substr().
    {
        auto parsed = parser.parse("USE DATABASE target_db;");
        assert(parsed.success);
        const auto* use = asSet(parsed.stmt);
        assert(use && use->command == SqlCommand::UseDatabase);
        assert(use->values.size() == 1 && use->values[0] == "target_db");
        parsed = parser.parse("USE short_db");
        assert(parsed.success && asSet(parsed.stmt)->values[0] == "short_db");
        assert(!parser.parse("USE").success);
        assert(!parser.parse("USE DATABASE").success);
        assert(!parser.parse("USE DATABASE a trailing").success);
    }
    std::cout << "[PARSER P1] USE target validation OK\n";

    // 2. SET
    {
        auto r = parser.parse("SET timezone = 'UTC'");
        assert(r.success);
        auto* s = asSet(r.stmt);
        assert(s);
        assert(s->name == "timezone");
        assert(s->values.size() == 1 && s->values[0] == "'UTC'");
        assert(!s->isShow && !s->isReset);
        std::cout << "[PARSER P1] SET OK\n";
    }

    // 3. SHOW
    {
        auto r = parser.parse("SHOW search_path");
        assert(r.success);
        auto* s = asSet(r.stmt);
        assert(s && s->isShow);
        assert(s->name == "search_path");
        std::cout << "[PARSER P1] SHOW OK\n";
    }

    // 4. RESET
    {
        auto r = parser.parse("RESET client_encoding");
        assert(r.success);
        auto* s = asSet(r.stmt);
        assert(s && s->isReset);
        assert(s->name == "client_encoding");
        std::cout << "[PARSER P1] RESET OK\n";
    }

    // 5. EXPLAIN with options
    {
        auto r = parser.parse("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT 1");
        assert(r.success);
        auto* e = asExplain(r.stmt);
        assert(e);
        assert(e->analyze);
        assert(e->buffers);
        assert(e->json);
        assert(e->query && e->query->command == SqlCommand::Select);
        std::cout << "[PARSER P1] EXPLAIN OK\n";
    }

    // 6. CREATE INDEX
    {
        auto r = parser.parse("CREATE UNIQUE INDEX idx ON t USING btree (a DESC, b NULLS FIRST) WHERE a > 0");
        assert(r.success);
        auto* c = asCreateIndex(r.stmt);
        assert(c);
        assert(c->unique);
        assert(c->indexName == "idx");
        assert(c->tableName == "t");
        assert(c->accessMethod == "btree");
        assert(c->columns.size() == 2);
        assert(!c->columns[0].ascending);
        assert(c->columns[1].nullsFirst);
        assert(c->whereClause);

        auto qualified = parser.parse(
            "CREATE INDEX events_created_idx ON audit.events (created_at)");
        assert(qualified.success);
        c = asCreateIndex(qualified.stmt);
        assert(c != nullptr);
        assert(c->indexName == "events_created_idx");
        assert(c->tableName == "audit.events");
        assert(c->columns.size() == 1);
        assert(c->columns[0].column == "created_at");

        auto fulltext = parser.parse(
            "CREATE FULLTEXT INDEX body_ft ON docs (body)");
        assert(fulltext.success);
        c = asCreateIndex(fulltext.stmt);
        assert(c && c->indexName == "body_ft" &&
               c->accessMethod == "fulltext" &&
               c->compatibilityShortcut == "fulltext");

        auto hash = parser.parse("CREATE HASH INDEX id_hash ON docs (id)");
        assert(hash.success);
        c = asCreateIndex(hash.stmt);
        assert(c && c->indexName == "id_hash" &&
               c->accessMethod == "hash" &&
               c->compatibilityShortcut == "hash");

        auto dropFulltext = parser.parse(
            "DROP FULLTEXT INDEX body_ft ON docs");
        assert(dropFulltext.success);
        auto* dropIndex = dynamic_cast<DropStmt*>(dropFulltext.stmt.get());
        assert(dropIndex && dropIndex->command == SqlCommand::DropIndex &&
               dropIndex->compatibilityShortcut == "fulltext" &&
               dropIndex->objectNames.size() == 1 &&
               dropIndex->objectNames[0] == "body_ft" &&
               dropIndex->tableName == "docs");

        auto qualifiedIndexName = parser.parse(
            "CREATE INDEX audit.events_created_idx ON audit.events (created_at)");
        assert(!qualifiedIndexName.success);
    std::cout << "[PARSER P1] CREATE INDEX OK\n";

    // CREATE TEMP/TEMPORARY must preserve session-local DDL flags for the
    // typed executor instead of silently becoming persistent CREATE TABLE.
    {
        auto temp = parser.parse("CREATE TEMP TABLE session_data (id INT)");
        assert(temp.success);
        auto* table = asCreateTable(temp.stmt);
        assert(table && table->temp && !table->localTemp);
        auto local = parser.parse("CREATE LOCAL TEMPORARY TABLE local_data (id INT)");
        assert(local.success);
        table = asCreateTable(local.stmt);
        assert(table && table->temp && table->localTemp);
        auto deleteRows = parser.parse(
            "CREATE TEMP TABLE delete_rows (id INT) ON COMMIT DELETE ROWS");
        assert(deleteRows.success);
        table = asCreateTable(deleteRows.stmt);
        assert(table && table->onCommitSpecified && table->onCommit == "delete");
        auto dropTable = parser.parse(
            "CREATE TEMP TABLE drop_table (id INT) ON COMMIT DROP");
        assert(dropTable.success);
        table = asCreateTable(dropTable.stmt);
        assert(table && table->onCommitSpecified && table->onCommit == "drop");
        auto ctas = parser.parse(
            "CREATE TEMP TABLE ctas_temp ON COMMIT DROP AS SELECT id FROM source_table");
        assert(ctas.success);
        table = asCreateTable(ctas.stmt);
        assert(table && table->onCommit == "drop" && !table->asSelect.empty());
        std::cout << "[PARSER P1] CREATE TEMP flags OK\n";
    }

    // Project-only MySQL column attributes must survive parsing so the
    // executor can gate them by compatibility mode instead of silently
    // discarding the tokens.
    {
        auto extensions = parser.parse(
            "CREATE TABLE mysql_attrs (id INTEGER UNSIGNED AUTO_INCREMENT)");
        assert(extensions.success);
        auto* table = asCreateTable(extensions.stmt);
        assert(table && table->columns.size() == 1);
        assert(table->columns[0].isUnsignedExtension);
        assert(table->columns[0].isAutoIncrementExtension);
        std::cout << "[PARSER P1] MySQL column attributes preserved OK\n";
    }
    }

    // 7. CREATE VIEW
    {
        auto r = parser.parse("CREATE VIEW v AS SELECT id FROM t");
        assert(r.success);
        auto* c = asCreateView(r.stmt);
        assert(c);
        assert(c->viewName == "v");
        assert(!c->replace);
        assert(c->query && c->query->command == SqlCommand::Select);
        std::cout << "[PARSER P1] CREATE VIEW OK\n";
    }

    {
        auto r = parser.parse("CREATE OR REPLACE VIEW v AS SELECT id FROM t");
        assert(r.success);
        auto* c = asCreateView(r.stmt);
        assert(c && c->replace);
        std::cout << "[PARSER P1] CREATE OR REPLACE VIEW OK\n";
    }

    {
        auto view = parser.parse(
            "CREATE VIEW reporting.summary AS SELECT id FROM t");
        assert(view.success);
        auto* createView = asCreateView(view.stmt);
        assert(createView && createView->viewName == "reporting.summary");

        auto materialized = parser.parse(
            "CREATE MATERIALIZED VIEW reporting.cached AS SELECT id FROM t");
        assert(materialized.success);
        auto* createMaterialized = asCreateView(materialized.stmt);
        assert(createMaterialized &&
               createMaterialized->viewName == "reporting.cached");

        auto dropViews = parser.parse(
            "DROP VIEW reporting.summary, public.other_view");
        assert(dropViews.success);
        auto* dropView = dynamic_cast<DropStmt*>(dropViews.stmt.get());
        assert(dropView && dropView->objectNames.size() == 2);
        assert(dropView->objectNames[0] == "reporting.summary");
        assert(dropView->objectNames[1] == "public.other_view");

        auto dropMaterialized = parser.parse(
            "DROP MATERIALIZED VIEW reporting.cached");
        assert(dropMaterialized.success);
        auto* dropMv = dynamic_cast<DropStmt*>(dropMaterialized.stmt.get());
        assert(dropMv && dropMv->objectNames.size() == 1);
        assert(dropMv->objectNames[0] == "reporting.cached");

        assert(!parser.parse(
            "CREATE MATERIALIZED VIEW reporting. AS SELECT id FROM t").success);
        assert(!parser.parse("DROP VIEW reporting.").success);
        std::cout << "[PARSER P1] qualified view names preserved OK\n";
    }

    {
        auto result = parser.parse(
            "DROP TABLE IF EXISTS inventory.items, public.audit CASCADE");
        assert(result.success);
        auto* drop = dynamic_cast<DropStmt*>(result.stmt.get());
        assert(drop && drop->ifExists && drop->cascade);
        assert(drop->objectNames.size() == 2);
        assert(drop->objectNames[0] == "inventory.items");
        assert(drop->objectNames[1] == "public.audit");
        std::cout << "[PARSER P1] qualified DROP TABLE list OK\n";
    }

    // 8. ALTER TABLE
    {
        auto r = parser.parse("ALTER TABLE t ADD COLUMN c INT, DROP COLUMN d, ADD CONSTRAINT pk PRIMARY KEY (id)");
        assert(r.success);
        auto* a = asAlterTable(r.stmt);
        assert(a);
        assert(a->tableName == "t");
        assert(a->subCommands.size() == 3);
        assert(a->subCommands[0].action == AlterTableStmt::Action::AddColumn);
        assert(a->subCommands[0].colDef.name == "c");
        assert(a->subCommands[1].action == AlterTableStmt::Action::DropColumn);
        assert(a->subCommands[1].name == "d");
        assert(a->subCommands[2].action == AlterTableStmt::Action::AddConstraint);
        assert(a->subCommands[2].constraint.type == "PRIMARY KEY");

        auto modifiers = parser.parse(
            "ALTER TABLE t ADD COLUMN score INT[] DEFAULT 5 NOT NULL "
            "CONSTRAINT score_positive CHECK (score > 0) UNIQUE");
        assert(modifiers.success);
        auto* modified = asAlterTable(modifiers.stmt);
        assert(modified && modified->subCommands.size() == 1);
        const auto& column = modified->subCommands[0].colDef;
        assert(column.name == "score" && column.typeName == "INT");
        assert(column.isArray && !column.isNull && column.isUnique);
        assert(column.defaultValue && column.defaultValue->toString() == "5");
        assert(column.checkExprs.size() == 1 &&
               column.checkExprs[0]->toString() == "score > 0");
        assert(column.checkNames.size() == 1 &&
               column.checkNames[0] == "score_positive");

        auto extensions = parser.parse(
            "ALTER TABLE t ADD COLUMN mysql_id INT UNSIGNED AUTO_INCREMENT");
        assert(extensions.success);
        auto* extensionAlter = asAlterTable(extensions.stmt);
        assert(extensionAlter && extensionAlter->subCommands.size() == 1);
        const auto& extensionColumn =
            extensionAlter->subCommands[0].colDef;
        assert(extensionColumn.isUnsignedExtension);
        assert(extensionColumn.isAutoIncrementExtension);

        auto generated = parser.parse(
            "ALTER TABLE t ADD COLUMN doubled INT "
            "GENERATED ALWAYS AS (id * 2) VIRTUAL");
        assert(generated.success);
        auto* generatedAlter = asAlterTable(generated.stmt);
        assert(generatedAlter && generatedAlter->subCommands.size() == 1);
        const auto& generatedColumn =
            generatedAlter->subCommands[0].colDef;
        assert(generatedColumn.generatedExpr == "id * 2");
        assert(generatedColumn.generatedKind == 'v');

        // Unsupported inline forms must fail closed instead of creating an
        // unconstrained column after silently discarding trailing tokens.
        assert(!parser.parse(
            "ALTER TABLE t ADD COLUMN parent_id INT REFERENCES p(id)").success);
        std::cout << "[PARSER P1] ALTER TABLE OK\n";
    }

    // Scientific numeric constants stay single literal tokens, including an
    // exponent sign and leading/trailing decimal point forms.
    {
        auto r = parser.parse(
            "SELECT 1e3, 1e-3, 1e+3, .5e2, 5.e1, 5., .5");
        assert(r.success);
        auto* s = asSelect(r.stmt);
        assert(s && s->selectList.size() == 7);
        const std::vector<std::string> expected = {
            "1e3", "1e-3", "1e+3", ".5e2", "5.e1", "5.", ".5"};
        for (size_t i = 0; i < expected.size(); ++i) {
            const auto* literal = dynamic_cast<const LiteralExpr*>(
                s->selectList[i].expr.get());
            assert(literal && literal->value == expected[i]);
        }
        std::cout << "[PARSER P1] scientific numeric literals OK\n";
    }

    // 9. SELECT GROUP BY ROLLUP/CUBE/GROUPING SETS
    {
        auto r = parser.parse("SELECT a, b FROM t GROUP BY ROLLUP(a), CUBE(b), GROUPING SETS((a),(b))");
        assert(r.success);
        auto* s = asSelect(r.stmt);
        assert(s);
        assert(s->groupByElems.size() == 3);
        assert(s->groupByElems[0].kind == SelectStmt::GroupByElem::Kind::Rollup);
        assert(s->groupByElems[1].kind == SelectStmt::GroupByElem::Kind::Cube);
        assert(s->groupByElems[2].kind == SelectStmt::GroupByElem::Kind::GroupingSets);
        std::cout << "[PARSER P1] GROUP BY extensions OK\n";
    }

    // 10. SELECT ORDER BY NULLS FIRST/LAST
    {
        auto r = parser.parse("SELECT * FROM t ORDER BY a NULLS FIRST, b DESC NULLS LAST");
        assert(r.success);
        auto* s = asSelect(r.stmt);
        assert(s && s->orderBy.size() == 2);
        assert(s->orderBy[0].nullsFirst);
        assert(!s->orderBy[1].nullsFirst);
        assert(!s->orderBy[1].asc);
        std::cout << "[PARSER P1] ORDER BY NULLS OK\n";
    }

    // 11. SELECT LIMIT WITH TIES / FETCH FIRST
    {
        auto r1 = parser.parse("SELECT * FROM t LIMIT 10 WITH TIES");
        assert(r1.success);
        auto* s1 = asSelect(r1.stmt);
        assert(s1 && s1->limit == 10 && s1->withTies);

        auto r2 = parser.parse("SELECT * FROM t FETCH FIRST 5 ROWS ONLY");
        assert(r2.success);
        auto* s2 = asSelect(r2.stmt);
        assert(s2 && s2->fetchFirst && s2->limit == 5);
        assert(s2->fromClause && s2->fromClause->type == FromItem::Type::Table);
        assert(s2->fromClause->tableName == "t");
        auto constantFetch = parser.parse("SELECT 42 FETCH FIRST ROW ONLY");
        assert(constantFetch.success);
        const auto* constantSelect = asSelect(constantFetch.stmt);
        assert(constantSelect && constantSelect->selectList.size() == 1);
        assert(!constantSelect->fromClause && constantSelect->fetchFirst && constantSelect->limit == 1);

        assert(!parser.parse("SELECT * FROM t LIMIT nope").success);
        assert(!parser.parse("SELECT * FROM t LIMIT -1").success);
        assert(!parser.parse("SELECT * FROM t OFFSET").success);
        assert(!parser.parse("SELECT * FROM t FETCH FIRST nope ROWS ONLY").success);
        auto r3 = parser.parse("SELECT * FROM t FETCH FIRST ROWS ONLY");
        assert(r3.success);
        auto* s3 = asSelect(r3.stmt);
        assert(s3 && s3->limit == 1);
        assert(s3->fromClause && s3->fromClause->tableName == "t");
        std::cout << "[PARSER P1] LIMIT/FETCH OK\n";
    }

    assert(!parser.parse("CREATE FUNCTION f() RETURNS int COST nope AS 'SELECT 1'").success);
    assert(!parser.parse("CREATE FUNCTION f() RETURNS int ROWS -1 AS 'SELECT 1'").success);
    auto strictFunction = parser.parse(
        "CREATE FUNCTION f(x int) RETURNS int RETURNS NULL ON NULL INPUT "
        "LANGUAGE sql AS 'SELECT x'");
    assert(strictFunction.success);
    auto* strictStmt = dynamic_cast<CreateFunctionStmt*>(strictFunction.stmt.get());
    assert(strictStmt && strictStmt->strict);
    auto calledFunction = parser.parse(
        "CREATE FUNCTION g(x int) RETURNS int CALLED ON NULL INPUT "
        "LANGUAGE sql AS 'SELECT x'");
    assert(calledFunction.success);
    auto* calledStmt = dynamic_cast<CreateFunctionStmt*>(calledFunction.stmt.get());
    assert(calledStmt && !calledStmt->strict);
    auto replaceFunction = parser.parse(
        "CREATE OR REPLACE FUNCTION h(x int) RETURNS int "
        "LANGUAGE sql AS 'SELECT x'");
    assert(replaceFunction.success);
    auto* replaceStmt =
        dynamic_cast<CreateFunctionStmt*>(replaceFunction.stmt.get());
    assert(replaceStmt && replaceStmt->replace);
    auto replaceProcedure = parser.parse(
        "CREATE OR REPLACE PROCEDURE p(x int) LANGUAGE sql "
        "AS 'SELECT ?x'");
    assert(replaceProcedure.success);
    auto* replaceProcedureStmt =
        dynamic_cast<CreateFunctionStmt*>(replaceProcedure.stmt.get());
    assert(replaceProcedureStmt && replaceProcedureStmt->replace &&
           replaceProcedureStmt->language == "sql" &&
           replaceProcedureStmt->body == "SELECT ?x");
    auto foldedRoutine = parser.parse(
        "CREATE PROCEDURE Mixed_Name(ArgValue INT) LANGUAGE SQL "
        "AS $$ SELECT ?ArgValue $$");
    assert(foldedRoutine.success);
    auto* foldedRoutineStmt =
        dynamic_cast<CreateFunctionStmt*>(foldedRoutine.stmt.get());
    assert(foldedRoutineStmt && foldedRoutineStmt->funcName == "mixed_name" &&
           foldedRoutineStmt->params.size() == 1 &&
           foldedRoutineStmt->params[0].first == "argvalue" &&
           foldedRoutineStmt->body == " SELECT ?ArgValue ");
    auto quotedRoutine = parser.parse(
        "CREATE FUNCTION \"Mixed\"(\"A\" INT) RETURNS INT LANGUAGE SQL "
        "AS $$ SELECT ?A $$");
    assert(quotedRoutine.success);
    auto* quotedRoutineStmt =
        dynamic_cast<CreateFunctionStmt*>(quotedRoutine.stmt.get());
    assert(quotedRoutineStmt && quotedRoutineStmt->funcName == "Mixed" &&
           quotedRoutineStmt->params.size() == 1 &&
           quotedRoutineStmt->params[0].first == "A");
    assert(!parser.parse(
        "CREATE PROCEDURE p() AS 'SELECT 1' LANGUAGE sql garbage").success);
    assert(!parser.parse(
        "CREATE FUNCTION bad(x int) RETURNS int RETURNS NULL ON NULL "
        "LANGUAGE sql AS 'SELECT x'").success);
    assert(!parser.parse("CREATE ROLE r CONNECTION LIMIT nope").success);
    auto unlimitedRole = parser.parse(
        "CREATE ROLE unlimited CONNECTION LIMIT -1");
    assert(unlimitedRole.success);
    auto* unlimited =
        dynamic_cast<CreateRoleStmt*>(unlimitedRole.stmt.get());
    assert(unlimited && unlimited->connectionLimit == -1);
    assert(!parser.parse("ALTER TABLE t ALTER COLUMN c SET STATISTICS nope").success);
    auto defaultStatistics = parser.parse(
        "ALTER TABLE t ALTER COLUMN c SET STATISTICS -1");
    assert(defaultStatistics.success);
    auto* statistics = asAlterTable(defaultStatistics.stmt);
    assert(statistics && statistics->subCommands.size() == 1 &&
           statistics->subCommands[0].statisticsTarget == -1);

    // 12. Function calls: schema-qualified, named args, window
    {
        auto r = parser.parse("SELECT pg_catalog.now(), f(a => 1, b => 2), row_number() OVER (PARTITION BY x ORDER BY y DESC) FROM t");
        assert(r.success);
        auto* s = asSelect(r.stmt);
        assert(s && s->selectList.size() == 3);

        auto* fc1 = asFuncCall(s->selectList[0].expr);
        assert(fc1 && fc1->schema == "pg_catalog" && fc1->funcName == "now");

        auto* fc2 = asFuncCall(s->selectList[1].expr);
        assert(fc2 && fc2->namedArgs.size() == 2);
        assert(fc2->namedArgs[0].name == "a");
        assert(fc2->namedArgs[1].name == "b");

        auto* fc3 = asFuncCall(s->selectList[2].expr);
        assert(fc3 && fc3->funcName == "row_number");
        assert(fc3->hasOver);
        assert(!fc3->over.partitionBy.empty());
        std::cout << "[PARSER P1] function calls OK\n";
    }

    // 13. VALUES
    {
        auto r = parser.parse("VALUES (1, 'a'), (2, 'b')");
        assert(r.success);
        auto* s = asSelect(r.stmt);
        assert(s && s->command == SqlCommand::Values);
        assert(s->valuesRows.size() == 2);
        assert(s->valuesRows[0].size() == 2);
        assert(s->valuesRows[0][0]);
        assert(s->valuesRows[0][0]->toString() == "1");
        assert(s->valuesRows[0][1]);
        assert(s->valuesRows[0][1]->toString() == "'a'");
        assert(s->valuesRows[1][0]);
        assert(s->valuesRows[1][0]->toString() == "2");
        assert(s->valuesRows[1][1]);
        assert(s->valuesRows[1][1]->toString() == "'b'");

        auto typed = parser.parse(
            "VALUES (DATE '2024-03-15'), "
            "(TIMESTAMP '2024-03-16 10:30:00');");
        assert(typed.success);
        auto* typedValues = asSelect(typed.stmt);
        assert(typedValues && typedValues->valuesRows.size() == 2);
        auto* dateLiteral = dynamic_cast<LiteralExpr*>(
            typedValues->valuesRows[0][0].get());
        auto* timestampLiteral = dynamic_cast<LiteralExpr*>(
            typedValues->valuesRows[1][0].get());
        assert(dateLiteral && dateLiteral->typeName == "date");
        assert(timestampLiteral &&
               timestampLiteral->typeName == "timestamp");

        for (const char* invalid : {
                 "VALUES", "VALUES ()", "VALUES (1,)", "VALUES (,1)",
                 "VALUES (1", "VALUES (1),", "VALUES (1), (2, 3)",
                 "VALUES (1 AS x)", "VALUES (1) trailing"}) {
            auto malformed = parser.parse(invalid);
            assert(!malformed.success);
        }
        std::cout << "[PARSER P1] VALUES OK\n";
    }

    // 14. INSERT AST keeps DEFAULT positional and distinguishes DEFAULT VALUES
    {
        auto r = parser.parse("INSERT INTO t (a, b) VALUES (DEFAULT, 1), (2, NULL)");
        assert(r.success);
        auto* i = dynamic_cast<InsertStmt*>(r.stmt.get());
        assert(i);
        assert(i->tableName == "t");
        assert(i->columns.size() == 2);
        assert(i->values.size() == 2);
        assert(i->values[0].size() == 2);
        auto* defaultExpr = dynamic_cast<LiteralExpr*>(i->values[0][0].get());
        assert(defaultExpr && defaultExpr->value == "default");
        assert(i->values[1].size() == 2);

        auto defaultRow = parser.parse("INSERT INTO t DEFAULT VALUES");
        assert(defaultRow.success);
        auto* defaultStmt = dynamic_cast<InsertStmt*>(defaultRow.stmt.get());
        assert(defaultStmt && defaultStmt->defaultValues);
        assert(defaultStmt->values.empty());
        auto malformed = parser.parse("INSERT INTO t VALUES (1), DEFAULT");
        assert(!malformed.success);
        assert(!parser.parse("UPDATE t SET a = 1 trailing").success);
        assert(!parser.parse("UPDATE t SET a = 1, WHERE a = 1").success);
        assert(!parser.parse("UPDATE t SET WHERE a = 1").success);
        assert(!parser.parse("DELETE FROM t WHERE a = 1 trailing").success);
        auto qualifiedInsert = parser.parse(
            "INSERT INTO app.t (a) VALUES (1)");
        auto qualifiedUpdate = parser.parse(
            "UPDATE app.t SET a = 2 WHERE a = 1");
        auto qualifiedDelete = parser.parse(
            "DELETE FROM app.t WHERE a = 2");
        auto qualifiedMerge = parser.parse(
            "MERGE INTO app.t USING public.s ON t.a = s.a "
            "WHEN MATCHED THEN DELETE");
        assert(qualifiedInsert.success);
        assert(dynamic_cast<InsertStmt*>(qualifiedInsert.stmt.get())
                   ->tableName == "app.t");
        assert(qualifiedUpdate.success);
        assert(dynamic_cast<UpdateStmt*>(qualifiedUpdate.stmt.get())
                   ->tableName == "app.t");
        assert(qualifiedDelete.success);
        assert(dynamic_cast<DeleteStmt*>(qualifiedDelete.stmt.get())
                   ->tableName == "app.t");
        assert(qualifiedMerge.success);
        assert(dynamic_cast<MergeStmt*>(qualifiedMerge.stmt.get())
                   ->targetTable == "app.t");
        assert(!parser.parse("INSERT INTO app. VALUES (1)").success);

        auto updateFrom = parser.parse(
            "UPDATE target AS dst SET val = src.val FROM source AS src "
            "WHERE dst.id = src.id");
        assert(updateFrom.success);
        auto* updateFromStmt = dynamic_cast<UpdateStmt*>(updateFrom.stmt.get());
        assert(updateFromStmt && updateFromStmt->alias == "dst");
        assert(updateFromStmt->fromClause &&
               updateFromStmt->fromClause->alias == "src");

        auto deleteUsing = parser.parse(
            "DELETE FROM target AS dst USING source AS src "
            "WHERE dst.id = src.id");
        assert(deleteUsing.success);
        auto* deleteUsingStmt = dynamic_cast<DeleteStmt*>(deleteUsing.stmt.get());
        assert(deleteUsingStmt && deleteUsingStmt->alias == "dst");
        assert(deleteUsingStmt->usingClause &&
               deleteUsingStmt->usingClause->alias == "src");

        auto updateCurrent = parser.parse(
            "UPDATE target SET val = 1 WHERE CURRENT OF update_cursor");
        auto deleteCurrent = parser.parse(
            "DELETE FROM target WHERE CURRENT OF delete_cursor");
        assert(updateCurrent.success && deleteCurrent.success);
        assert(dynamic_cast<UpdateStmt*>(updateCurrent.stmt.get())
                   ->whereCurrentOf == "update_cursor");
        assert(dynamic_cast<DeleteStmt*>(deleteCurrent.stmt.get())
                   ->whereCurrentOf == "delete_cursor");
        assert(!parser.parse(
            "UPDATE target SET val = 1 WHERE CURRENT OF").success);
        assert(!parser.parse(
            "DELETE FROM target WHERE CURRENT OF").success);

        auto insertReturning = parser.parse(
            "INSERT INTO target VALUES (1) "
            "RETURNING WITH (OLD AS o, NEW AS n) o.id, n.*");
        assert(insertReturning.success);
        auto* insertReturningStmt = dynamic_cast<InsertStmt*>(
            insertReturning.stmt.get());
        assert(insertReturningStmt);
        assert(insertReturningStmt->returningOptions.oldAliased);
        assert(insertReturningStmt->returningOptions.newAliased);
        assert(insertReturningStmt->returningOptions.oldAlias == "o");
        assert(insertReturningStmt->returningOptions.newAlias == "n");
        assert(insertReturningStmt->returning.size() == 2);

        auto updateReturning = parser.parse(
            "UPDATE target SET val = 2 "
            "RETURNING old.val AS before, new.val AS after");
        assert(updateReturning.success);
        auto* updateReturningStmt = dynamic_cast<UpdateStmt*>(
            updateReturning.stmt.get());
        assert(updateReturningStmt &&
               !updateReturningStmt->returningOptions.oldAliased &&
               !updateReturningStmt->returningOptions.newAliased &&
               updateReturningStmt->returning.size() == 2);

        auto deleteReturning = parser.parse(
            "DELETE FROM target RETURNING WITH (NEW AS n) old.*, n.*");
        assert(deleteReturning.success);
        auto* deleteReturningStmt = dynamic_cast<DeleteStmt*>(
            deleteReturning.stmt.get());
        assert(deleteReturningStmt &&
               !deleteReturningStmt->returningOptions.oldAliased &&
               deleteReturningStmt->returningOptions.newAliased &&
               deleteReturningStmt->returningOptions.newAlias == "n");

        auto mergeReturning = parser.parse(
            "MERGE INTO target AS dst USING source AS src "
            "ON dst.id = src.id WHEN MATCHED THEN DELETE "
            "RETURNING WITH (OLD AS before, NEW AS after) "
            "before.id, after.id, merge_action()");
        assert(mergeReturning.success);
        auto* mergeReturningStmt = dynamic_cast<MergeStmt*>(
            mergeReturning.stmt.get());
        assert(mergeReturningStmt &&
               mergeReturningStmt->returningOptions.oldAlias == "before" &&
               mergeReturningStmt->returningOptions.newAlias == "after" &&
               mergeReturningStmt->returning.size() == 3);

        for (const char* invalidReturning : {
                 "UPDATE target SET val = 2 RETURNING WITH old.id",
                 "UPDATE target SET val = 2 RETURNING WITH () val",
                 "UPDATE target SET val = 2 RETURNING WITH (OLD o) o.val",
                 "UPDATE target SET val = 2 RETURNING WITH (OLD AS o,) o.val",
                 "UPDATE target SET val = 2 RETURNING WITH (OLD AS o, OLD AS p) o.val",
                 "UPDATE target SET val = 2 RETURNING WITH (OLD AS new) new.val",
                 "UPDATE target SET val = 2 RETURNING WITH (NEW AS old) old.val",
                 "UPDATE target SET val = 2 RETURNING WITH (OLD AS o)"}) {
            assert(!parser.parse(invalidReturning).success);
        }
        std::cout << "[PARSER P1] INSERT AST DEFAULT handling OK\n";
    }

    // 15. Set-operation precedence and left associativity
    {
        auto leftAssoc = parser.parse(
            "SELECT a FROM t1 UNION SELECT a FROM t2 UNION SELECT a FROM t3");
        assert(leftAssoc.success);
        auto* root = asSelect(leftAssoc.stmt);
        assert(root && root->setOp == SetOp::Union && root->setOpLhs && root->setOpRhs);
        assert(asSelect(root->setOpLhs)->setOp == SetOp::Union);
        assert(asSelect(root->setOpRhs)->setOp == SetOp::None);

        auto precedence = parser.parse(
            "SELECT a FROM t1 INTERSECT SELECT a FROM t2 UNION SELECT a FROM t3");
        assert(precedence.success);
        auto* precedenceRoot = asSelect(precedence.stmt);
        assert(precedenceRoot && precedenceRoot->setOp == SetOp::Union);
        // INTERSECT binds tighter, so the left operand carries INTERSECT.
        assert(precedenceRoot->setOpLhs &&
               asSelect(precedenceRoot->setOpLhs)->setOp == SetOp::Intersect);
        assert(precedenceRoot->setOpRhs &&
               asSelect(precedenceRoot->setOpRhs)->setOp == SetOp::None);

        auto intersectFirst = parser.parse(
            "SELECT a FROM t1 UNION SELECT a FROM t2 INTERSECT ALL SELECT a FROM t3");
        assert(intersectFirst.success);
        auto* intersectRoot = asSelect(intersectFirst.stmt);
        assert(intersectRoot && intersectRoot->setOp == SetOp::Union && intersectRoot->setOpRhs);
        assert(asSelect(intersectRoot->setOpRhs)->setOp == SetOp::Intersect);
        assert(asSelect(intersectRoot->setOpRhs)->setOpAll);
        std::cout << "[PARSER P1] set-operation precedence OK\n";
    }

    // 15. COMMENT ON 多种对象类型
    {
        auto r1 = parser.parse("COMMENT ON SCHEMA public IS 'schema note'");
        assert(r1.success);
        auto* c1 = dynamic_cast<const CommentStmt*>(r1.stmt.get());
        assert(c1 && c1->objectType == "SCHEMA" && c1->objectName == "public" &&
               c1->comment == "schema note");

        auto r2 = parser.parse("COMMENT ON COLUMN t.id IS NULL");
        assert(r2.success);
        auto* c2 = dynamic_cast<const CommentStmt*>(r2.stmt.get());
        assert(c2 && c2->objectType == "COLUMN" && c2->objectName == "t" &&
               c2->columnName == "id" && c2->comment.empty() && c2->isNull);

        auto r3 = parser.parse("COMMENT ON MATERIALIZED VIEW mv IS 'mv note'");
        assert(r3.success);
        auto* c3 = dynamic_cast<const CommentStmt*>(r3.stmt.get());
        assert(c3 && c3->objectType == "MATERIALIZED VIEW" && c3->objectName == "mv");

        auto r4 = parser.parse(
            "CoMmEnT ON TABLE MixedName IS 'Mixed Case\nLine ''quoted'' | pipe';");
        assert(r4.success);
        auto* c4 = dynamic_cast<const CommentStmt*>(r4.stmt.get());
        assert(c4 && c4->objectType == "TABLE" &&
               c4->objectName == "mixedname" &&
               c4->comment == "Mixed Case\nLine 'quoted' | pipe");

        auto r5 = parser.parse(
            "COMMENT ON TABLE \"Object IS Name\" IS 'Payload IS Intact'");
        assert(r5.success);
        auto* c5 = dynamic_cast<const CommentStmt*>(r5.stmt.get());
        assert(c5 && c5->objectName == "Object IS Name" &&
               c5->comment == "Payload IS Intact");

        assert(!parser.parse("COMMENT ON TABLE missing_is_clause").success);
        assert(!parser.parse(
            "COMMENT ON TABLE t IS unquoted_comment").success);
        assert(!parser.parse(
            "COMMENT ON TABLE t IS \"identifier_not_string\"").success);
        assert(!parser.parse(
            "COMMENT ON TABLE t IS 'closed' trailing 'text'").success);
        std::cout << "[PARSER P1] COMMENT ON OK\n";
    }

    // 16. Transaction options and savepoint names
    {
        assert(SQLParser::classify("ROLLBACK TO SAVEPOINT sp1") ==
               SqlCommand::RollbackToSavepoint);
        assert(SQLParser::classify("ROLLBACK PREPARED 'gx1'") ==
               SqlCommand::RollbackPrepared);
        assert(SQLParser::classify("COMMIT PREPARED 'gx1'") ==
               SqlCommand::CommitPrepared);

        auto commitPrepared = parser.parse("COMMIT PREPARED 'gx1';");
        assert(commitPrepared.success);
        auto* commitPreparedTxn = dynamic_cast<const TransactionStmt*>(commitPrepared.stmt.get());
        assert(commitPreparedTxn && commitPreparedTxn->kind == TransactionStmt::Kind::CommitPrepared &&
               commitPreparedTxn->gid == "'gx1'");

        auto rollbackPrepared = parser.parse("ROLLBACK PREPARED 'gx1';");
        assert(rollbackPrepared.success);
        auto* rollbackPreparedTxn = dynamic_cast<const TransactionStmt*>(rollbackPrepared.stmt.get());
        assert(rollbackPreparedTxn && rollbackPreparedTxn->kind == TransactionStmt::Kind::RollbackPrepared &&
               rollbackPreparedTxn->gid == "'gx1'");

        auto begin = parser.parse(
            "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY");
        assert(begin.success);
        auto* beginTxn = dynamic_cast<const TransactionStmt*>(begin.stmt.get());
        assert(beginTxn && beginTxn->kind == TransactionStmt::Kind::Begin);
        assert(beginTxn->isolation == IsolationLevel::SERIALIZABLE);
        assert(beginTxn->readOnly && !beginTxn->deferrable);

        // PostgreSQL requires ISOLATION LEVEL, including after START.
        auto invalidStart = parser.parse("START TRANSACTION READ COMMITTED READ WRITE NOT DEFERRABLE");
        assert(!invalidStart.success);
        auto start = parser.parse("START TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE NOT DEFERRABLE");
        assert(start.success);
        auto* startTxn = dynamic_cast<const TransactionStmt*>(start.stmt.get());
        assert(startTxn && startTxn->kind == TransactionStmt::Kind::Start);
        assert(startTxn->isolation == IsolationLevel::READ_COMMITTED);
        assert(!startTxn->readOnly && !startTxn->deferrable);

        auto save = parser.parse("SAVEPOINT sp1");
        assert(save.success);
        auto* saveTxn = dynamic_cast<const TransactionStmt*>(save.stmt.get());
        assert(saveTxn && saveTxn->kind == TransactionStmt::Kind::Savepoint &&
               saveTxn->savepointName == "sp1");

        auto release = parser.parse("RELEASE SAVEPOINT sp1");
        assert(release.success);
        auto* releaseTxn = dynamic_cast<const TransactionStmt*>(release.stmt.get());
        assert(releaseTxn && releaseTxn->kind == TransactionStmt::Kind::Release &&
               releaseTxn->savepointName == "sp1");

        auto rollbackTo = parser.parse("ROLLBACK TO sp1;");
        assert(rollbackTo.success);
        auto* rollbackTxn = dynamic_cast<const TransactionStmt*>(rollbackTo.stmt.get());
        assert(rollbackTxn && rollbackTxn->kind == TransactionStmt::Kind::RollbackTo &&
               rollbackTxn->savepointName == "sp1");

        assert(!parser.parse("BEGIN ISOLATION LEVEL nonsense").success);
        assert(!parser.parse("SAVEPOINT").success);
        std::cout << "[PARSER P1] transaction options/savepoints OK\n";
    }

    // 17. LISTEN/NOTIFY/UNLISTEN preserve PostgreSQL identifier and literal
    // semantics instead of passing raw SQL fragments to the executor.
    {
        auto listen = parser.parse("LISTEN \"MiXed,Channel\";");
        assert(listen.success);
        auto* listenStmt = dynamic_cast<const ListenStmt*>(listen.stmt.get());
        assert(listenStmt && listenStmt->channel == "MiXed,Channel");

        auto doubledIdentifier = parser.parse("LISTEN \"odd\"\"channel\"");
        assert(doubledIdentifier.success);
        listenStmt = dynamic_cast<const ListenStmt*>(
            doubledIdentifier.stmt.get());
        assert(listenStmt && listenStmt->channel == "odd\"channel");

        auto notify = parser.parse(
            "NOTIFY \"MiXed,Channel\", 'It''s Mixed';");
        assert(notify.success);
        auto* notifyStmt = dynamic_cast<const NotifyStmt*>(notify.stmt.get());
        assert(notifyStmt && notifyStmt->channel == "MiXed,Channel" &&
               notifyStmt->payload == "It's Mixed");

        auto folded = parser.parse("NOTIFY FoLdEd_Channel");
        assert(folded.success);
        notifyStmt = dynamic_cast<const NotifyStmt*>(folded.stmt.get());
        assert(notifyStmt && notifyStmt->channel == "folded_channel" &&
               notifyStmt->payload.empty());

        auto longIdentifier = parser.parse(
            "LISTEN " + std::string(64, 'a'));
        assert(longIdentifier.success);
        listenStmt = dynamic_cast<const ListenStmt*>(
            longIdentifier.stmt.get());
        assert(listenStmt && listenStmt->channel == std::string(63, 'a'));

        auto multibyteBoundary = parser.parse(
            "LISTEN \"" + std::string(62, 'b') + "界\"");
        assert(multibyteBoundary.success);
        listenStmt = dynamic_cast<const ListenStmt*>(
            multibyteBoundary.stmt.get());
        assert(listenStmt && listenStmt->channel == std::string(62, 'b'));

        auto unlistenAll = parser.parse("UNLISTEN *;");
        assert(unlistenAll.success);
        auto* unlistenStmt = dynamic_cast<const UnlistenStmt*>(
            unlistenAll.stmt.get());
        assert(unlistenStmt && unlistenStmt->all);

        assert(!parser.parse("LISTEN two words").success);
        assert(!parser.parse("LISTEN \"\"").success);
        assert(!parser.parse("NOTIFY channel, payload").success);
        assert(!parser.parse("NOTIFY channel, 'payload' trailing").success);
        assert(!parser.parse("UNLISTEN channel trailing").success);
        std::cout << "[PARSER P1] LISTEN/NOTIFY/UNLISTEN syntax OK\n";
    }

    std::cout << "[PARSER P1] all passed\n";
    return 0;
}
