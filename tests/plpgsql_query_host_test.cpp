#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <optional>
#include <set>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "plpgsql_query_host";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = db;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE into_rows(id INT,payload TEXT)", session));
    const std::vector<std::optional<std::string>> payloads{
        "a b", std::nullopt, "null", "", "O'Brien"};
    for (size_t i = 0; i < payloads.size(); ++i)
        assert(g_engine.insertRow(db, "into_rows", {{"id", std::to_string(i + 1)},
            {"payload", payloads[i]}}) == dbms::DBStatus::OK);

    auto query = g_engine.plpgsqlQuery(db, "SELECT 7,'a b',NULL,'null','', 'O''Brien';");
    assert(query.ok && query.rowCount == 1 && query.columnCount == 6);
    assert(query.firstRow == (std::vector<std::optional<std::string>>{
        "7", "a b", std::nullopt, "null", "", "O'Brien"}));
    assert(query.columnTypes == (std::vector<std::string>{
        "integer", "text", "text", "text", "text", "text"}));
    query = g_engine.plpgsqlQuery(db,
        "SELECT r.id,r.payload FROM into_rows r WHERE r.id>=2 ORDER BY r.id DESC LIMIT 2 OFFSET 1;");
    assert(query.ok && query.rowCount == 2 && query.columnCount == 2);
    assert(query.firstRow == (std::vector<std::optional<std::string>>{"4", ""}));
    assert(query.columnTypes == (std::vector<std::string>{"integer", "text"}));

    assert(!ddl.executeSql("CREATE TABLE demand_rows(id INT,payload TEXT)", session));
    for (const auto& row : std::vector<std::pair<std::string, std::string>>{
             {"1", "11"}, {"2", "22"}, {"3", "bad"}})
        assert(g_engine.insertRow(db, "demand_rows", {{"id", row.first},
            {"payload", row.second}}) == dbms::DBStatus::OK);
    // The native receiver also stops before evaluating an unrequested row,
    // but must evaluate row two before a STRICT caller diagnoses P0003.
    for (size_t demand : {size_t{1}, size_t{2}}) {
        query = g_engine.plpgsqlQuery(db, "SELECT CAST(payload AS INT) FROM demand_rows;",
                                     dbms::PlPgsqlQueryOptions{demand});
        assert(query.ok && query.rowCount == demand && query.firstRow[0] == "11");
    }
    query = g_engine.plpgsqlQuery(db, "SELECT CAST(payload AS INT) FROM demand_rows;");
    assert(!query.ok && query.sqlState == "22P02");
    query = g_engine.plpgsqlQuery(db,
        "SELECT 12/(id-2) FROM demand_rows;", dbms::PlPgsqlQueryOptions{1});
    assert(query.ok && query.rowCount == 1 && query.firstRow[0] == "-12");
    query = g_engine.plpgsqlQuery(db,
        "SELECT 12/(id-2) FROM demand_rows;", dbms::PlPgsqlQueryOptions{2});
    assert(!query.ok && query.sqlState == "22012");
    query = g_engine.plpgsqlQuery(db,
        "SELECT CAST(payload AS INT) FROM demand_rows ORDER BY id;",
        dbms::PlPgsqlQueryOptions{1});
    assert(!query.ok && query.sqlState == "22P02");
    query = g_engine.plpgsqlQuery(db,
        "SELECT 1/(id-id) FROM demand_rows WHERE 1/0>0 ORDER BY 1/0 LIMIT 0;",
        dbms::PlPgsqlQueryOptions{1});
    assert(query.ok && query.rowCount == 0 && query.columnTypes ==
           std::vector<std::string>{"integer"});
    query = g_engine.plpgsqlQuery(db,
        "SELECT missing FROM demand_rows LIMIT 0;", dbms::PlPgsqlQueryOptions{1});
    assert(!query.ok && query.sqlState == "42703");
    query = g_engine.plpgsqlQuery(db,
        "SELECT payload FROM into_rows WHERE payload IS NULL ORDER BY id;");
    assert(query.ok && query.rowCount == 1 && query.firstRow[0] == std::nullopt);
    query = g_engine.plpgsqlQuery(db,
        "SELECT payload FROM into_rows WHERE payload='null';");
    assert(query.ok && query.rowCount == 1 && query.firstRow[0] == "null");
    query = g_engine.plpgsqlQuery(db,
        "SELECT CASE WHEN id=4 THEN payload ELSE 'wrong' END FROM into_rows WHERE id=4;");
    assert(query.ok && query.rowCount == 1 && query.firstRow[0] == "");
    query = g_engine.plpgsqlQuery(db,
        "SELECT id+10 AS shifted FROM into_rows WHERE id IN(1,3) ORDER BY shifted DESC;");
    assert(query.ok && query.rowCount == 2 && query.firstRow[0] == "13");
    query = g_engine.plpgsqlQuery(db,
        "SELECT coalesce(payload,'fallback') FROM into_rows WHERE id=2;");
    assert(query.ok && query.rowCount == 1 && query.firstRow[0] == "fallback");
    query = g_engine.plpgsqlQuery(db,
        "SELECT r.* FROM public.into_rows AS r WHERE r.id=5 ORDER BY 1;");
    assert(query.ok && query.columnCount == 2 && query.firstRow[1] == "O'Brien");
    query = g_engine.plpgsqlQuery(db, "SELECT id,payload FROM into_rows WHERE FALSE;");
    assert(query.ok && query.rowCount == 0 && query.columnCount == 2 && query.firstRow.empty());
    assert(query.columnTypes == (std::vector<std::string>{"integer", "text"}));

    assert(!ddl.executeSql("CREATE TABLE quoted_rows(id INT,\"ID\" INT,\"Value Name\" TEXT,\"a.b\" INT,\"Q\"\"Col\" TEXT)", session));
    assert(g_engine.insertRow(db, "quoted_rows", {{"id", "1"}, {"ID", "2"},
        {"Value Name", "a b"}, {"a.b", "3"}, {"Q\"Col", "exact \" value"}}) == dbms::DBStatus::OK);
    query = g_engine.plpgsqlQuery(db,
        "SELECT q.id,q.\"ID\",q.\"Value Name\" FROM quoted_rows AS q WHERE q.\"ID\"=2;");
    assert(query.ok && query.firstRow == (std::vector<std::optional<std::string>>{"1", "2", "a b"}));
    query = g_engine.plpgsqlQuery(db,
        "SELECT q.* FROM quoted_rows AS q ORDER BY id,\"ID\",\"Value Name\",\"a.b\",\"Q\"\"Col\";");
    assert(query.ok && query.firstRow ==
        (std::vector<std::optional<std::string>>{"1", "2", "a b", "3", "exact \" value"}));
    query = g_engine.plpgsqlQuery(db,
        "SELECT q.\"a.b\" AS \"Value.Name\" FROM quoted_rows AS q ORDER BY \"Value.Name\";");
    assert(query.ok && query.firstRow == (std::vector<std::optional<std::string>>{"3"}));
    query = g_engine.plpgsqlQuery(db, "SELECT ID,\"ID\" FROM quoted_rows;");
    assert(query.ok && query.firstRow == (std::vector<std::optional<std::string>>{"1", "2"}));
    assert(g_engine.createSchema(db, "CaseSchema") == dbms::DBStatus::OK);
    dbms::TableSchema quotedSchema = g_engine.getTableSchema(db, "quoted_rows");
    quotedSchema.tablename = "CaseSchema__CaseRows";
    assert(g_engine.createTable(db, quotedSchema) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, quotedSchema.tablename, {{"id", "1"}, {"ID", "2"},
        {"Value Name", "a b"}, {"a.b", "3"}, {"Q\"Col", "exact \" value"}}) == dbms::DBStatus::OK);
    query = g_engine.plpgsqlQuery(db,
        "SELECT \"R.Al\".id,\"R.Al\".\"ID\",\"R.Al\".\"Q\"\"Col\" "
        "FROM \"CaseSchema\".\"CaseRows\" AS \"R.Al\" WHERE \"R.Al\".\"ID\"=2;");
    assert(query.ok && query.firstRow ==
        (std::vector<std::optional<std::string>>{"1", "2", "exact \" value"}));
    query = g_engine.plpgsqlQuery(db,
        "SELECT \"CaseSchema\".\"CaseRows\".\"ID\" FROM \"CaseSchema\".\"CaseRows\" "
        "WHERE \"CaseSchema\".\"CaseRows\".id=1;");
    assert(query.ok && query.firstRow == (std::vector<std::optional<std::string>>{"2"}));

    const std::vector<std::pair<std::string, std::string>> failures{
        {"SELECT missing FROM into_rows WHERE FALSE;", "42703"},
        {"SELECT hidden.id FROM into_rows r;", "42P01"},
        {"SELECT q.id FROM quoted_rows AS \"Q\";", "42P01"},
        {"SELECT id FROM no_such_table;", "42P01"},
        {"SELECT missing_function(id) FROM into_rows WHERE FALSE;", "42883"},
        {"SELECT count(*) FROM into_rows;", "0A000"},
        {"SELECT r.id FROM into_rows r JOIN into_rows s ON r.id=s.id;", "0A000"},
        {"WITH c AS(SELECT 1) SELECT * FROM c;", "0A000"},
        {"SELECT id FROM into_rows GROUP BY id;", "0A000"},
        {"SELECT row_number() OVER() FROM into_rows;", "0A000"},
        {"SELECT ROW(id,payload) FROM into_rows;", "0A000"},
        {"SELECT 1 FROM into_rows r ignored;", "42601"},
        {"SELECT id FROM into_rows LIMIT 1 ignored;", "42601"},
        {"SELECT id FROM into_rows WHERE id=1 ignored;", "42601"},
        {"SELECT 1; SELECT 2;", "42601"}
    };
    for (const auto& failure : failures) {
        query = g_engine.plpgsqlQuery(db, failure.first);
        if (query.ok || query.sqlState != failure.second)
            std::cerr << "Native negative control: " << failure.first << " expected "
                << failure.second << " got ok=" << query.ok << " state=" << query.sqlState
                << " message=" << query.message << '\n';
        assert(!query.ok && query.sqlState == failure.second);
    }

    const auto function = [&](const std::string& functionName,
                              const std::string& returnType,
                              const std::string& body) {
        assert(g_engine.createUDF(db, functionName, std::vector<std::string>{},
            std::vector<std::string>{}, body, 'v', "plpgsql", returnType) == dbms::DBStatus::OK);
    };
    std::string value;
    bool isNull = false;
    function("into_scalar", "int", "DECLARE n INT; BEGIN SELECT 99 INTO n; RETURN n; END;");
    assert(g_engine.callUDF(db, "into_scalar", {}, value, &isNull) && value == "99" && !isNull);
    function("into_ordered", "int", "DECLARE n INT; BEGIN SELECT id INTO n FROM into_rows ORDER BY id DESC LIMIT 1 OFFSET 1; RETURN n; END;");
    assert(g_engine.callUDF(db, "into_ordered", {}, value, &isNull) && value == "4" && !isNull);
    for (size_t i = 0; i < payloads.size(); ++i) {
        const std::string functionName = "into_payload_" + std::to_string(i);
        function(functionName, "text", "DECLARE v TEXT; BEGIN SELECT payload INTO v FROM into_rows WHERE id=" + std::to_string(i + 1) + "; RETURN v; END;");
        assert(g_engine.callUDF(db, functionName, {}, value, &isNull));
        assert(isNull == !payloads[i]);
        if (payloads[i]) assert(value == *payloads[i]);
    }
    function("into_none", "int", "DECLARE n INT := 7; BEGIN SELECT id INTO n FROM into_rows WHERE id=99; RETURN n; END;");
    assert(g_engine.callUDF(db, "into_none", {}, value, &isNull) && isNull);

    // Public compatibility adapter routes the full query, including WHERE,
    // and allows typed callers to retain actual NULL instead of guessing.
    std::map<std::string, std::string> variables{{"n", "old"}, {"v", "old"}};
    std::set<std::string> variableNulls;
    assert(g_engine.plpgsqlSelectInto(db, "id,payload", "into_rows", "id=2",
        {"n", "v"}, variables, &variableNulls) == 0);
    assert(variables["n"] == "2" && variableNulls == std::set<std::string>{"v"});
    assert(g_engine.plpgsqlSelectInto(db, "payload", "into_rows", "id=3",
        {"v"}, variables, &variableNulls) == 0);
    assert(variables["v"] == "null" && variableNulls.empty());
    assert(g_engine.plpgsqlSelectInto(db, "payload", "into_rows", "id=4",
        {"v"}, variables, &variableNulls) == 0);
    assert(variables["v"].empty() && variableNulls.empty());
    assert(g_engine.plpgsqlSelectInto(db, "id", "into_rows", "id=99",
        {"n", "v"}, variables, &variableNulls) == 1);
    assert(variableNulls == (std::set<std::string>{"n", "v"}));
    assert(g_engine.plpgsqlSelectInto(db, "1,2", "", "", {"n"}, variables) == 0);
    assert(variables["n"] == "1");
    assert(g_engine.plpgsqlSelectInto(db, "1", "", "", {"n", "v"}, variables,
        &variableNulls) == 0 && variables["n"] == "1" && variableNulls.count("v"));
    assert(g_engine.plpgsqlSelectInto(db, "CAST(payload AS INT)", "demand_rows", "",
        {"n"}, variables, &variableNulls) == 0 && variables["n"] == "11" &&
        !variableNulls.count("n"));

    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[PLPGSQL QUERY HOST] passed\n";
}
