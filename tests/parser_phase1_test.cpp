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

    // 1. classify()
    assert(SQLParser::classify("SET timezone = 'UTC'") == SqlCommand::Set);
    assert(SQLParser::classify("SHOW search_path") == SqlCommand::Show);
    assert(SQLParser::classify("RESET client_encoding") == SqlCommand::Reset);
    assert(SQLParser::classify("EXPLAIN SELECT 1") == SqlCommand::Explain);
    assert(SQLParser::classify("SELECT 1") == SqlCommand::Select);
    assert(SQLParser::classify("CREATE INDEX idx ON t (a)") == SqlCommand::CreateIndex);
    std::cout << "[PARSER P1] classify OK\n";

    // MySQL-only DML LIMIT syntax is intentionally rejected.  It used to have
    // a dead legacy implementation behind the typed DML fail-closed boundary.
    assert(!parser.parse("UPDATE users SET age = 0 LIMIT 10").success);
    assert(!parser.parse("DELETE FROM users WHERE age > 100 LIMIT 10").success);
    std::cout << "[PARSER P1] non-PG DML LIMIT rejected\n";

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

        assert(!parser.parse("SELECT * FROM t LIMIT nope").success);
        assert(!parser.parse("SELECT * FROM t LIMIT -1").success);
        assert(!parser.parse("SELECT * FROM t OFFSET").success);
        assert(!parser.parse("SELECT * FROM t FETCH FIRST nope ROWS ONLY").success);
        auto r3 = parser.parse("SELECT * FROM t FETCH FIRST ROWS ONLY");
        assert(r3.success);
        auto* s3 = asSelect(r3.stmt);
        assert(s3 && s3->limit == 1);
        std::cout << "[PARSER P1] LIMIT/FETCH OK\n";
    }

    assert(!parser.parse("CREATE FUNCTION f() RETURNS int COST nope AS 'SELECT 1'").success);
    assert(!parser.parse("CREATE FUNCTION f() RETURNS int ROWS -1 AS 'SELECT 1'").success);
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
               c2->columnName == "id" && c2->comment.empty());

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
        assert(c5 && c5->objectName == "\"Object IS Name\"" &&
               c5->comment == "Payload IS Intact");

        assert(!parser.parse("COMMENT ON TABLE missing_is_clause").success);
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

        auto start = parser.parse("START TRANSACTION READ COMMITTED READ WRITE NOT DEFERRABLE");
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

    std::cout << "[PARSER P1] all passed\n";
    return 0;
}
