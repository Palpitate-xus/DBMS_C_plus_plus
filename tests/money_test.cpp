#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "access/BPTree.h"
#include "access/HashIndex.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include "test_utils.h"
#include "types/money.h"
#include "utils/Session.h"

#include <cassert>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

void testCodec() {
    dbms::Money value;
    assert(dbms::Money::parse("90071992547409.91", value));
    assert(value.minorUnits() == 9007199254740991LL);
    assert(value.format("C") == "$90,071,992,547,409.91");

    assert(dbms::Money::parse("$1,234.567", value, "C"));
    assert(value.minorUnits() == 123457);
    assert(value.format("C") == "$1,234.57");

    assert(dbms::Money::parse("($12.34)", value, "C"));
    assert(value.minorUnits() == -1234);
    assert(value.format("C") == "-$12.34");

    assert(dbms::Money::parse("92233720368547758.07", value, "C"));
    assert(value.minorUnits() == std::numeric_limits<int64_t>::max());
    assert(dbms::Money::parse("-92233720368547758.08", value, "C"));
    assert(value.minorUnits() == std::numeric_limits<int64_t>::min());
    assert(!dbms::Money::parse("92233720368547758.08", value, "C"));
    assert(!dbms::Money::parse("$not-money", value, "C"));

    // The installed en_US locale exercises the libc monetary facet rather
    // than the C-locale fallback.
    assert(dbms::Money::parse("$1,234.56", value, "en_US.utf8"));
    assert(value.minorUnits() == 123456);
    assert(value.format("en_US.utf8") == "$1,234.56");
    assert(value.decimalString("en_US.utf8") == "1234.56");
    assert(dbms::Money::parseDecimal("1234.565", value, "en_US.utf8"));
    assert(value.minorUnits() == 123457);
}

void testExpressions() {
    dbms::StorageEngine::setMoneyLocale("C");
    dbms::ExprEvaluator evaluator;
    auto literal = [](const std::string& type, const std::string& value) {
        auto expression = std::make_unique<dbms::LiteralExpr>();
        expression->typeName = type;
        expression->value = value;
        return expression;
    };
    auto binary = [&](const std::string& op, const std::string& leftType,
                      const std::string& leftValue,
                      const std::string& rightType,
                      const std::string& rightValue) {
        auto expression = std::make_unique<dbms::BinaryOpExpr>();
        expression->op = op;
        expression->left = literal(leftType, leftValue);
        expression->right = literal(rightType, rightValue);
        return evaluator.eval(expression.get(), {});
    };
    auto cast = [&](const std::string& sourceType,
                    const std::string& sourceValue,
                    const std::string& targetType) {
        auto expression = std::make_unique<dbms::CastExpr>();
        expression->operand = literal(sourceType, sourceValue);
        expression->typeName = targetType;
        return evaluator.eval(expression.get(), {});
    };

    assert(binary("+", "money", "$1,234.56", "money", "$0.44").value ==
           "$1,235.00");
    assert(binary("-", "money", "$1,234.56", "money", "$0.56").value ==
           "$1,234.00");
    assert(binary("*", "money", "$1,234.56", "integer", "2").value ==
           "$2,469.12");
    assert(binary("*", "numeric", "0.5", "money", "$1,234.56").value ==
           "$617.28");
    assert(binary("/", "money", "$1,234.56", "integer", "2").value ==
           "$617.28");
    assert(binary("/", "money", "$1,234.56", "money", "$617.28").value ==
           "2");
    assert(binary("<", "money", "$9,007,199,254,740.91", "money",
                  "$9,007,199,254,741.00").asBool());

    assert(cast("character varying", "12.345", "money").value == "$12.35");
    assert(cast("numeric", "1e3", "money").value == "$1,000.00");
    assert(cast("money", "$1,234.56", "numeric").value == "1234.56");

    auto unary = std::make_unique<dbms::UnaryOpExpr>();
    unary->op = "-";
    unary->operand = literal("money", "$12.34");
    assert(evaluator.eval(unary.get(), {}).value == "-$12.34");

    bool overflow = false;
    try {
        (void)binary("+", "money", "$92,233,720,368,547,758.07",
                     "money", "$0.01");
    } catch (const std::runtime_error& error) {
        overflow = std::string(error.what()).find("SQLSTATE 22003") !=
                   std::string::npos;
    }
    assert(overflow);
}

void testStorageAndIndexes(const std::string& database) {
    dbms::TableSchema schema;
    schema.tablename = "wallet";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    dbms::Column amount = dbms::makeMoneyColumn("amount", false);
    amount.isUnique = true;
    schema.append(amount);
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.createIndex(database, "wallet", "amount") ==
           dbms::DBStatus::OK);
    assert(g_engine.createHashIndex(database, "wallet", "amount") ==
           dbms::DBStatus::OK);

    assert(g_engine.insert(database, "wallet",
                           {{"id", "1"},
                            {"amount", "90071992547409.91"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "wallet",
                           {{"id", "2"}, {"amount", "$1,234.56"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "wallet",
                           {{"id", "3"}, {"amount", "-2.00"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "wallet",
                           {{"id", "4"}, {"amount", "10.00"}}) ==
           dbms::DBStatus::OK);

    // Equivalent locale spellings are one key and predicates remain exact
    // beyond IEEE-754's integer precision boundary.
    assert(g_engine.insert(database, "wallet",
                           {{"id", "5"}, {"amount", "1234.560"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.query(database, "wallet",
                          {"=amount 90071992547409.91"}, {"id"}) ==
           std::vector<std::string>{"1 "});
    assert(g_engine.query(database, "wallet", {"=id 1"}, {"amount"}) ==
           std::vector<std::string>{"$90,071,992,547,409.91 "});

    const std::string key = "800000000001e240";  // 123456 minor units
    assert(g_engine.getSecondaryIndex(database, "wallet", "amount")
               ->searchMulti(key).size() == 1);
    assert(g_engine.getHashIndex(database, "wallet", "amount")
               ->search(key).size() == 1);

    dbms::StorageEngine::OrderBySpec ascending;
    ascending.colName = "amount";
    assert(g_engine.query(database, "wallet", {}, {"id"}, {ascending}) ==
           (std::vector<std::string>{"3 ", "4 ", "2 ", "1 "}));

    assert(g_engine.update(database, "wallet",
                           {{"amount", "90071992547409.92"}},
                           {"=id 1"}) == dbms::DBStatus::OK);
    assert(g_engine.query(database, "wallet", {"=id 1"}, {"amount"}) ==
           std::vector<std::string>{"$90,071,992,547,409.92 "});
    assert(g_engine.update(database, "wallet", {{"amount", "invalid"}},
                           {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(database, "wallet",
                           {{"id", "6"},
                            {"amount", "92233720368547758.08"}}) ==
           dbms::DBStatus::INVALID_VALUE);
}

void testDdlAndCatalog(const std::string& database) {
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE ledger (amount money)", session));
    const dbms::TableSchema table =
        g_engine.getTableSchema(database, "ledger");
    assert(table.len == 1);
    assert(table.cols[0].dataType == "money");
    assert(table.cols[0].dsize == sizeof(int64_t));
    assert(!table.cols[0].isVariableLength);

    dbms::CatalogManager& catalog = g_engine.catalogService().get(database);
    const auto* publicNamespace = catalog.findNamespaceByName("public");
    assert(publicNamespace != nullptr);
    const auto* relation =
        catalog.findClassByName("ledger", publicNamespace->oid);
    assert(relation != nullptr);
    const auto* attribute = catalog.findAttribute(relation->oid, "amount");
    assert(attribute != nullptr);
    assert(attribute->atttypid == 790);
    assert(attribute->attlen == 8);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    testCodec();
    testExpressions();

    const std::string testName = "money";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    testStorageAndIndexes(database);
    testDdlAndCatalog(database);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[MONEY] exact storage, locale I/O, catalog and indexes OK\n";
    return 0;
}
