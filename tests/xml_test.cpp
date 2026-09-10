// ============================================================================
// XML type test — Phase 4 Wave 4.12
// xml columns validate well-formedness (CONTENT form) on INSERT/UPDATE:
// balanced/nested tags, quoted attributes, self-closing tags, comments, CDATA,
// processing instructions; mismatched/unclosed tags and unquoted attributes
// are rejected. Stored verbatim (no canonicalization).
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include "types/xml.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static std::string fetchOne(const std::string& db, const std::string& tbl,
                            const std::vector<std::string>& conds,
                            const std::string& col) {
    auto rows = g_engine.query(db, tbl, conds, {col}, {});
    assert(rows.size() == 1);
    std::string r = rows[0];
    while (!r.empty() && (r.back() == ' ' || r.back() == '\n')) r.pop_back();
    return r;
}

static void test_xml_valid() {
    std::string db = testDbPath("xml_ok");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, x XML)", s));

    // Simple element round-trips verbatim.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"x","<a>hello</a>"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "x") == "<a>hello</a>");
    // Nested + attributes + self-closing.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"x","<a id=\"1\"><b/><c>x</c></a>"}}) == dbms::DBStatus::OK);
    // Content fragment (multiple roots) is allowed.
    assert(g_engine.insert(db, "t", {{"id","3"}, {"x","<a/><b/>"}}) == dbms::DBStatus::OK);
    // Plain text content is allowed.
    assert(g_engine.insert(db, "t", {{"id","4"}, {"x","just text"}}) == dbms::DBStatus::OK);
    // Comment, CDATA, processing instruction.
    assert(g_engine.insert(db, "t", {{"id","5"}, {"x","<a><!-- c --><![CDATA[ <x> ]]></a>"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id","6"}, {"x","<?xml version=\"1.0\"?><a>x</a>"}}) == dbms::DBStatus::OK);
    // Character/entity references and namespace bindings are checked.
    assert(g_engine.insert(db, "t", {{"id","7"}, {"x","<n:a xmlns:n=\"urn:test\" v=\"&amp;\">&#x41;</n:a>"}}) == dbms::DBStatus::OK);
    // Empty content is a valid XML CONTENT value.
    assert(g_engine.insert(db, "t", {{"id","8"}, {"x",""}}) == dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[XML] well-formed accepted OK" << std::endl;
}

static void test_xml_invalid() {
    std::string db = testDbPath("xml_bad");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, x XML)", s));

    // Mismatched tags.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"x","<a></b>"}}) == dbms::DBStatus::INVALID_VALUE);
    // Unclosed tag.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"x","<a>text"}}) == dbms::DBStatus::INVALID_VALUE);
    // Unquoted attribute value.
    assert(g_engine.insert(db, "t", {{"id","3"}, {"x","<a id=1></a>"}}) == dbms::DBStatus::INVALID_VALUE);
    // Stray '<' in text.
    assert(g_engine.insert(db, "t", {{"id","4"}, {"x","a < b"}}) == dbms::DBStatus::INVALID_VALUE);
    // Unterminated comment.
    assert(g_engine.insert(db, "t", {{"id","5"}, {"x","<a><!-- oops</a>"}}) == dbms::DBStatus::INVALID_VALUE);
    // Unescaped/unknown entities, duplicate attributes and unbound prefixes.
    assert(g_engine.insert(db, "t", {{"id","6"}, {"x","<a>&bogus;</a>"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","7"}, {"x","<a x=\"1\" x=\"2\"/>"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","8"}, {"x","<p:a/>"}}) == dbms::DBStatus::INVALID_VALUE);
    // DTD/entity processing is deliberately fail-closed.
    assert(g_engine.insert(db, "t", {{"id","9"}, {"x","<!DOCTYPE a [<!ENTITY e SYSTEM 'file:///etc/passwd'>]><a>&e;</a>"}}) == dbms::DBStatus::INVALID_VALUE);
    // Declarations must be leading, ordered, and match the UTF-8 storage path.
    assert(g_engine.insert(db, "t", {{"id","10"}, {"x","<a/><?xml version=\"1.0\"?>"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","11"}, {"x","<?xml encoding=\"UTF-8\" version=\"1.0\"?><a/>"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","12"}, {"x","<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><a/>"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {}, {"id"}, {});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[XML] malformed rejected OK" << std::endl;
}

static const dbms::SelectStmt* parseSelect(dbms::SQLParser& parser,
                                           const std::string& sql,
                                           dbms::ParseResult& parsed) {
    parsed = parser.parse(sql);
    assert(parsed.success);
    const auto* select = dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get());
    assert(select);
    return select;
}

static void test_xml_expression_semantics() {
    dbms::SQLParser parser;
    dbms::ExprEvaluator evaluator;
    dbms::ParseResult parsed;
    const auto* select = parseSelect(
        parser,
        "SELECT XML '<a>&amp;</a>', '<b/>'::xml, "
        "xml_is_well_formed_content('<a/><b/>'), "
        "xml_is_well_formed_document('<a/><b/>'), "
        "xml_is_document('<a/>'::xml), "
        "xmlconcat(XML '<?xml version=\"1.0\"?><a/>', NULL, XML '<b/>'), "
        "xmlcomment('safe')",
        parsed);
    assert(select->selectList.size() == 7);
    const std::vector<std::string> expected = {
        "<a>&amp;</a>", "<b/>", "t", "f", "t", "<a/><b/>",
        "<!--safe-->"};
    const std::vector<std::string> types = {
        "xml", "xml", "boolean", "boolean", "boolean", "xml", "xml"};
    for (size_t i = 0; i < expected.size(); ++i) {
        const auto value = evaluator.eval(select->selectList[i].expr.get(), {});
        assert(!value.isNull && value.value == expected[i]);
        assert(value.typeName == types[i]);
    }

    const auto* nullSelect = parseSelect(
        parser, "SELECT xml_is_well_formed(NULL), xmlconcat(NULL, NULL)",
        parsed);
    assert(evaluator.eval(nullSelect->selectList[0].expr.get(), {}).isNull);
    assert(evaluator.eval(nullSelect->selectList[1].expr.get(), {}).isNull);

    const auto expectState = [&](const std::string& sql,
                                 const std::string& state) {
        const auto* statement = parseSelect(parser, sql, parsed);
        bool rejected = false;
        try {
            (void)evaluator.eval(statement->selectList[0].expr.get(), {});
        } catch (const dbms::DbError& error) {
            rejected = error.sqlState() == state;
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find(
                "SQLSTATE " + state) != std::string::npos;
        }
        assert(rejected);
    };
    expectState("SELECT '<a>&bad;</a>'::xml", "2200N");
    expectState("SELECT XML '<a></b>'", "2200N");
    expectState("SELECT xmlcomment('bad--comment')", "2200S");

    // Unsupported XPath/XMLTABLE capability is never guessed by the scalar
    // evaluator: generic XPath calls retain PostgreSQL's undefined-function
    // SQLSTATE instead of returning fabricated data.
    expectState("SELECT xpath('/a', XML '<a/>')", "42883");

    assert(dbms::validateXml("<a/>", dbms::XmlParseMode::Document).ok);
    assert(!dbms::validateXml("plain text", dbms::XmlParseMode::Document).ok);
    assert(!dbms::validateXml("<a/><b/>", dbms::XmlParseMode::Document).ok);
    assert(dbms::validateXml("plain &amp; text", dbms::XmlParseMode::Content).ok);
    assert(!dbms::validateXml("<a x=\"1\"y=\"2\"/>",
                              dbms::XmlParseMode::Content).ok);
    assert(!dbms::validateXml(std::string("<a>") + char(1) + "</a>",
                              dbms::XmlParseMode::Content).ok);
    std::cout << "[XML] casts/functions/document semantics OK" << std::endl;
}

static void test_xml_update() {
    std::string db = testDbPath("xml_upd");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, x XML)", s));

    assert(g_engine.insert(db, "t", {{"id","1"}, {"x","<a>1</a>"}}) == dbms::DBStatus::OK);
    // Valid update.
    assert(g_engine.update(db, "t", {{"x","<b>2</b>"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "x") == "<b>2</b>");
    // Invalid update rejected, row unchanged.
    assert(g_engine.update(db, "t", {{"x","<b>2"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "t", {"=id 1"}, "x") == "<b>2</b>");

    cleanup(db);
    std::cout << "[XML] update enforce/reject OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_xml_valid();
    test_xml_invalid();
    test_xml_update();
    test_xml_expression_semantics();
    std::cout << "[XML] all passed" << std::endl;
    return 0;
}
