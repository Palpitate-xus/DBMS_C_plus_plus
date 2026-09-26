#include "catalog/collation.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <set>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static std::string readBytes(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

static bool decodeHex(const std::string& encoded, std::string& value) {
    if (encoded.size() % 2 != 0) return false;
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    value.clear();
    for (size_t i = 0; i < encoded.size(); i += 2) {
        const int high = nibble(encoded[i]);
        const int low = nibble(encoded[i + 1]);
        if (high < 0 || low < 0) return false;
        value.push_back(static_cast<char>((high << 4) | low));
    }
    return true;
}

static bool readStoredCollation(const fs::path& path,
                                const std::string& expectedName,
                                std::string& provider,
                                std::string& locale) {
    static const std::string prefix = "DBMS_COLLATION_V2:";
    std::ifstream input(path, std::ios::binary);
    std::string line;
    while (std::getline(input, line)) {
        std::string name;
        std::string storedProvider;
        std::string storedLocale;
        if (line.rfind(prefix, 0) == 0) {
            const size_t nameEnd = line.find('|', prefix.size());
            const size_t providerEnd = nameEnd == std::string::npos
                ? std::string::npos : line.find('|', nameEnd + 1);
            if (nameEnd == std::string::npos ||
                providerEnd == std::string::npos ||
                !decodeHex(line.substr(
                    prefix.size(), nameEnd - prefix.size()), name) ||
                !decodeHex(line.substr(
                    nameEnd + 1, providerEnd - nameEnd - 1),
                    storedProvider) ||
                !decodeHex(line.substr(providerEnd + 1), storedLocale)) {
                continue;
            }
        } else {
            const size_t nameEnd = line.find('|');
            const size_t providerEnd = nameEnd == std::string::npos
                ? std::string::npos : line.find('|', nameEnd + 1);
            if (nameEnd == std::string::npos ||
                providerEnd == std::string::npos) continue;
            name = line.substr(0, nameEnd);
            storedProvider = line.substr(
                nameEnd + 1, providerEnd - nameEnd - 1);
            storedLocale = line.substr(providerEnd + 1);
        }
        if (name == expectedName) {
            provider = std::move(storedProvider);
            locale = std::move(storedLocale);
            return true;
        }
    }
    return false;
}

static void test_collation_provider() {
    using namespace dbms::collation;
    assert(normalizeName("C") == "c");
    assert(normalizeName("'POSIX'") == "posix");
    assert(normalizeName("en_US.UTF-8") == "en_us.utf8");
    assert(normalizeName("NOCASE") == "nocase");
    assert(isValid("C"));
    assert(isValid("POSIX"));
    assert(isValid("en_US.utf8"));
    assert(isValid("nocase"));
    assert(!isValid("nonexistent_collation"));
    assert(listBuiltins().size() >= 8);

    assert(compare("abc", "abc", "C") == 0);
    assert(compare("abc", "abc", "POSIX") == 0);
    assert(compare("abc", "abd", "C") < 0);
    assert(compare("abd", "abc", "C") > 0);
    // binary: 'B' < 'a'
    assert(compare("B", "a", "C") < 0);
    assert(compare("apple", "Zoo", "default") < 0);
    // nocase: 'B' == 'b'
    assert(compare("B", "b", "nocase") == 0);
    assert(compare("Apple", "apricot", "nocase") < 0);
    // reverse
    assert(compare("abc", "abd", "reverse") > 0);
    // Unknown collation falls back to binary
    assert(compare("abc", "abc", "zh_TW.utf8") == 0);
    std::cout << "[COLLATION] provider OK" << std::endl;
}

static void test_collate_persists_and_applies() {
    std::string db = testDbPath("collation_t1");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    // Column 'a' uses C collation; 'b' uses nocase.
    bool err = ddl.executeSql(
        "CREATE TABLE t (a VARCHAR(50) COLLATE C, b VARCHAR(50) COLLATE nocase)", s);
    assert(!err);

    dbms::TableSchema schema = g_engine.getTableSchema(db, "t");
    assert(schema.len == 2);
    assert(schema.cols[0].collation == "C");
    assert(schema.cols[1].collation == "nocase" || schema.cols[1].collation == "NOCASE");

    assert(g_engine.insert(db, "t", {{"a", "hello"}, {"b", "World"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"a", "Hello"}, {"b", "world"}}) == dbms::DBStatus::OK);

    // nocase equality: b='world' should match both rows (collation-aware comparison).
    auto rows = g_engine.query(db, "t", {"=b world"}, {"a", "b"});
    assert(rows.size() == 2);
    std::cout << "[COLLATION] nocase where-as-equality OK" << std::endl;

    // collation-aware range
    rows = g_engine.query(db, "t", {"<b WORLD"}, {"a", "b"});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[COLLATION] persists & applies OK" << std::endl;
}

static void test_collate_all_operators() {
    std::string db = testDbPath("collation_t2");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (c VARCHAR(50) COLLATE nocase)", s));
    assert(g_engine.insert(db, "t", {{"c", "Apple"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"c", "banana"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"c", "CHERRY"}}) == dbms::DBStatus::OK);

    // Equality under nocase.
    auto rows = g_engine.query(db, "t", {"=c apple"}, {"c"});
    assert(rows.size() == 1);

    // Inequality under nocase.
    rows = g_engine.query(db, "t", {"!=c apple"}, {"c"});
    assert(rows.size() == 2);

    // Less-than under nocase: order is apple < banana < cherry.
    rows = g_engine.query(db, "t", {"<c cherry"}, {"c"});
    assert(rows.size() == 2); // Apple and banana

    // Less-than-or-equal under nocase.
    rows = g_engine.query(db, "t", {"<=c cherry"}, {"c"});
    assert(rows.size() == 3); // Apple, banana and CHERRY

    // Greater-than under nocase.
    rows = g_engine.query(db, "t", {">c apple"}, {"c"});
    assert(rows.size() == 2); // banana and CHERRY

    // Greater-than-or-equal under nocase.
    rows = g_engine.query(db, "t", {">=c banana"}, {"c"});
    assert(rows.size() == 2); // banana and CHERRY

    cleanup(db);
    std::cout << "[COLLATION] all operators OK" << std::endl;
}

static void test_collate_mixed_columns() {
    std::string db = testDbPath("collation_t3");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    // 'bin' is binary (C); 'nc' is nocase; 'rev' is reverse.
    assert(!ddl.executeSql(
        "CREATE TABLE t (bin VARCHAR(50) COLLATE C, nc VARCHAR(50) COLLATE nocase, rev VARCHAR(50) COLLATE reverse)", s));

    assert(g_engine.insert(db, "t", {{"bin", "a"}, {"nc", "a"}, {"rev", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"bin", "A"}, {"nc", "A"}, {"rev", "A"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"bin", "b"}, {"nc", "b"}, {"rev", "b"}}) == dbms::DBStatus::OK);

    // Binary column: 'A' is not 'a'.
    auto rows = g_engine.query(db, "t", {"=bin a"}, {"bin", "nc", "rev"});
    assert(rows.size() == 1);

    // Nocase column: 'A' and 'a' both match.
    rows = g_engine.query(db, "t", {"=nc a"}, {"bin", "nc", "rev"});
    assert(rows.size() == 2);

    // Reverse column: ordering is inverted. 'a' > 'b' under reverse, so 'b' < 'a'.
    rows = g_engine.query(db, "t", {"<rev a"}, {"bin", "nc", "rev"});
    assert(rows.size() == 1); // only 'b' is less than 'a' in reverse order

    cleanup(db);
    std::cout << "[COLLATION] mixed columns OK" << std::endl;
}

static void test_collate_binary_collations() {
    using namespace dbms::collation;
    assert(isBinary(""));
    assert(!isBinary("default"));
    assert(isBinary("C"));
    assert(isBinary("POSIX"));
    assert(isBinary("ucs_basic"));
    assert(!isBinary("nocase"));
    assert(!isBinary("reverse"));
    assert(!isBinary("en_US.utf8"));

    std::string db = testDbPath("collation_t4");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (c_default VARCHAR(50), c_c VARCHAR(50) COLLATE C, c_posix VARCHAR(50) COLLATE POSIX, c_ucs VARCHAR(50) COLLATE ucs_basic)", s));

    dbms::TableSchema schema = g_engine.getTableSchema(db, "t");
    assert(schema.len == 4);

    // No explicit collation remains an empty schema marker; SQL uses the
    // database default locale for comparisons and ordering.
    assert(schema.cols[0].collation.empty());
    assert(schema.cols[1].collation == "C");
    assert(schema.cols[2].collation == "POSIX" || schema.cols[2].collation == "posix");
    assert(schema.cols[3].collation == "ucs_basic" || schema.cols[3].collation == "UCS_BASIC");

    assert(g_engine.insert(db, "t", {{"c_default", "x"}, {"c_c", "x"}, {"c_posix", "x"}, {"c_ucs", "x"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"c_default", "X"}, {"c_c", "X"}, {"c_posix", "X"}, {"c_ucs", "X"}}) == dbms::DBStatus::OK);

    // Both the deterministic default and explicit binary collations keep
    // lowercase 'x' distinct from uppercase 'X'.
    auto rows = g_engine.query(db, "t", {"=c_default x"}, {"c_default"});
    assert(rows.size() == 1);
    rows = g_engine.query(db, "t", {"=c_c x"}, {"c_c"});
    assert(rows.size() == 1);
    rows = g_engine.query(db, "t", {"=c_posix x"}, {"c_posix"});
    assert(rows.size() == 1);
    rows = g_engine.query(db, "t", {"=c_ucs x"}, {"c_ucs"});
    assert(rows.size() == 1);

    cleanup(db);
    std::cout << "[COLLATION] binary collations OK" << std::endl;
}

static void test_default_sql_sort_uses_locale() {
    std::string db = testDbPath("collation_default_sort");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (v TEXT)", s));
    // The low-level insert API treats the sentinel "NULL" as SQL NULL, so
    // use ordinary mixed-case text to isolate collation ordering here.
    for (const char* value : {"Zoo", "apple", ""}) {
        assert(g_engine.insert(db, "t", {{"v", value}}) ==
               dbms::DBStatus::OK);
    }
    dbms::StorageEngine::OrderBySpec order;
    order.colName = "v";
    auto rows = g_engine.query(db, "t", {}, {"v"}, {order});
    assert((rows == std::vector<std::string>{" ", "apple ", "Zoo "}));
    order.collation = "default";
    rows = g_engine.query(db, "t", {}, {"v"}, {order});
    assert((rows == std::vector<std::string>{" ", "apple ", "Zoo "}));
    order.collation = "C";
    rows = g_engine.query(db, "t", {}, {"v"}, {order});
    assert((rows == std::vector<std::string>{" ", "Zoo ", "apple "}));
    cleanup(db);
}

static void test_default_text_predicate_with_index() {
    std::string db = testDbPath("collation_default_predicate");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (v TEXT)", s));
    assert(g_engine.insert(db, "t", {{"v", "Zoo"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"v", "apple"}}) == dbms::DBStatus::OK);
    auto checkPredicates = [&] {
        auto rows = g_engine.query(db, "t", {"<v Zoo"}, {"v"});
        assert((rows == std::vector<std::string>{"apple "}));
        rows = g_engine.query(db, "t", {">v apple"}, {"v"});
        assert((rows == std::vector<std::string>{"Zoo "}));
    };
    checkPredicates();
    assert(!ddl.executeSql("CREATE INDEX t_v_idx ON t(v)", s));
    checkPredicates();
    cleanup(db);
}

static void test_collate_with_index() {
    std::string db = testDbPath("collation_t5");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (c VARCHAR(50) COLLATE nocase)", s));
    assert(g_engine.insert(db, "t", {{"c", "Hello"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"c", "HELLO"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"c", "world"}}) == dbms::DBStatus::OK);

    // Create a B-tree index on the nocase column. The engine must skip it for
    // non-binary collations and fall back to a full scan, otherwise binary key
    // lookup would miss case-differing values.
    assert(!ddl.executeSql("CREATE INDEX idx ON t(c)", s));

    auto rows = g_engine.query(db, "t", {"=c hello"}, {"c"});
    assert(rows.size() == 2); // Hello + HELLO

    rows = g_engine.query(db, "t", {"=c world"}, {"c"});
    assert(rows.size() == 1);

    // Range on a nocase column must also be collation-aware.
    rows = g_engine.query(db, "t", {">c hello"}, {"c"});
    assert(rows.size() == 1); // world only (HELLO == hello)

    cleanup(db);
    std::cout << "[COLLATION] index fallback OK" << std::endl;
}

static void test_collate_schema_persistence() {
    std::string db = testDbPath("collation_t6");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (a VARCHAR(50) COLLATE nocase, b VARCHAR(50) COLLATE C)", s));

    // Re-read the schema through the engine; this exercises the on-disk
    // schema serialization/deserialization path.
    dbms::TableSchema schema = g_engine.getTableSchema(db, "t");
    assert(schema.cols[0].collation == "nocase" || schema.cols[0].collation == "NOCASE");
    assert(schema.cols[1].collation == "C");

    cleanup(db);
    std::cout << "[COLLATION] schema persistence OK" << std::endl;
}

static void test_collation_metadata_is_atomic_and_backward_compatible() {
    using dbms::DBStatus;

    std::string db = testDbPath("collation_metadata_atomicity");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    const fs::path metadata = fs::path(db) / ".collations";
    {
        std::ofstream legacy(metadata, std::ios::binary);
        legacy << "legacy|libc|C\nneighbor|icu|en_US.UTF-8\n";
        assert(legacy.good());
    }

    dbms::StorageEngine engine;
    assert((engine.getCollationNames(db) ==
            std::vector<std::string>{"legacy", "neighbor"}));
    assert(engine.createCollation(
               db, "special", "provider|variant", "line\nlocale") ==
           DBStatus::OK);

    const std::string migrated = readBytes(metadata);
    assert(migrated.rfind("DBMS_COLLATION_V2:", 0) == 0);
    assert(migrated.find("legacy|libc|C") == std::string::npos);
    assert(migrated.find("provider|variant") == std::string::npos);
    assert(migrated.find("line\nlocale") == std::string::npos);

    dbms::StorageEngine reopened;
    assert((reopened.getCollationNames(db) ==
            std::vector<std::string>{"legacy", "neighbor", "special"}));
    assert(engine.createCollation(db, "special", "libc", "C") ==
           DBStatus::TABLE_ALREADY_EXISTS);
    assert(engine.createCollation(db, "bad|name", "libc", "C") ==
           DBStatus::INVALID_ARGUMENT);
    assert(readBytes(metadata) == migrated);

    const fs::perms originalPermissions = fs::status(db).permissions();
    fs::permissions(db, fs::perms::owner_read | fs::perms::owner_exec,
                    fs::perm_options::replace);
    const DBStatus createStatus =
        engine.createCollation(db, "blocked", "libc", "C");
    const DBStatus dropStatus = engine.dropCollation(db, "legacy");
    fs::permissions(db, originalPermissions, fs::perm_options::replace);
    assert(createStatus == DBStatus::IO_ERROR);
    assert(dropStatus == DBStatus::IO_ERROR);
    assert(readBytes(metadata) == migrated);

    assert(engine.dropCollation(db, "legacy") == DBStatus::OK);
    dbms::StorageEngine afterDrop;
    assert((afterDrop.getCollationNames(db) ==
            std::vector<std::string>{"neighbor", "special"}));

    cleanup(db);
    std::cout << "[COLLATION] metadata atomicity OK" << std::endl;
}

static void test_collation_ddl_preserves_options_and_namespace() {
    std::string db = testDbPath("collation_ddl_options");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA app", s));

    assert(!ddl.executeSql(
        "CREATE COLLATION app.us_locale "
        "(provider = LiBc, locale = 'en_US.UTF-8', deterministic = true)",
        s));
    assert(!ddl.executeSql(
        "CREATE COLLATION app.c_locale "
        "(provider=libc, lc_collate='C', lc_ctype='C')", s));
    const fs::path metadata = fs::path(db) / ".collations";
    std::string provider;
    std::string locale;
    assert(readStoredCollation(
        metadata, "app__us_locale", provider, locale));
    assert(provider == "libc");
    assert(locale == "en_US.UTF-8");
    assert(readStoredCollation(
        metadata, "app__c_locale", provider, locale));
    assert(provider == "libc");
    assert(locale == "C");

    const std::string originalBytes = readBytes(metadata);
    assert(!ddl.executeSql(
        "CREATE COLLATION IF NOT EXISTS app.us_locale "
        "(provider=libc, locale='different')", s));
    assert(readBytes(metadata) == originalBytes);
    assert(ddl.executeSql(
        "CREATE COLLATION app.us_locale (provider=libc, locale='C')", s));
    assert(ddl.executeSql(
        "CREATE COLLATION missing.bad (provider=libc, locale='C')", s));
    assert(ddl.executeSql(
        "CREATE COLLATION app.icu_locale (provider=icu, locale='en-US')", s));
    assert(ddl.executeSql(
        "CREATE COLLATION app.conflict "
        "(lc_collate='C', lc_ctype='en_US.UTF-8')", s));
    assert(ddl.executeSql(
        "CREATE COLLATION app.nondeterministic "
        "(provider=libc, locale='C', deterministic=false)", s));
    assert(ddl.executeSql("CREATE COLLATION app.copy FROM app.us_locale", s));
    assert(ddl.executeSql(
        "CREATE COLLATION app.invalid (provider libc, locale='C')", s));

    assert(!ddl.executeSql(
        "CREATE COLLATION public.public_locale "
        "(provider=libc, locale='C')", s));
    assert(readStoredCollation(
        metadata, "public_locale", provider, locale));
    assert(!ddl.executeSql(
        "DROP COLLATION IF EXISTS app.absent", s));

    assert(ddl.executeSql(
        "DROP COLLATION app.us_locale, app.c_locale", s));
    auto names = g_engine.getCollationNames(db);
    assert(std::find(names.begin(), names.end(), "app__us_locale") !=
           names.end());
    assert(std::find(names.begin(), names.end(), "app__c_locale") !=
           names.end());
    assert(!ddl.executeSql("DROP COLLATION app.us_locale", s));
    assert(!ddl.executeSql("DROP COLLATION app.c_locale", s));
    assert(!ddl.executeSql("DROP COLLATION public.public_locale", s));

    cleanup(db);
    std::cout << "[COLLATION] DDL options and namespace OK" << std::endl;
}

static void test_custom_collation_runtime_resolution() {
    using dbms::DBStatus;

    std::string db = testDbPath("collation_custom_runtime");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE SCHEMA app", s));
    assert(!ddl.executeSql(
        "CREATE COLLATION app.casefold "
        "(provider=libc, locale='nocase')", s));
    assert(ddl.executeSql(
        "CREATE COLLATION public.nocase "
        "(provider=libc, locale='reverse')", s));
    assert(!ddl.executeSql(
        "CREATE TABLE words (v VARCHAR(50) COLLATE app.casefold)", s));

    dbms::TableSchema schema = g_engine.getTableSchema(db, "words");
    assert(schema.len == 1);
    assert(schema.cols[0].collation == "app.casefold");
    assert(schema.cols[0].resolvedCollation == "nocase");
    assert(!schema.cols[0].resolvedCollationUsesLocale);
    assert(!schema.cols[0].resolvedCollationIsBinary);

    assert(g_engine.insert(db, "words", {{"v", "Hello"}}) == DBStatus::OK);
    assert(g_engine.insert(db, "words", {{"v", "HELLO"}}) == DBStatus::OK);
    assert(g_engine.insert(db, "words", {{"v", "world"}}) == DBStatus::OK);
    assert(!ddl.executeSql("CREATE INDEX words_v_idx ON words(v)", s));

    // The physical B-tree is binary.  A custom non-binary collation must use
    // a heap fallback or this lookup would miss both differently cased rows.
    auto rows = g_engine.query(db, "words", {"=v hello"}, {"v"});
    assert(rows.size() == 2);
    rows = g_engine.query(db, "words", {">v hello"}, {"v"});
    assert(rows.size() == 1);
    assert(rows.front() == "world ");

    assert(!ddl.executeSql(
        "CREATE TABLE ordered (v VARCHAR(50) COLLATE app.casefold)", s));
    assert(g_engine.insert(db, "ordered", {{"v", "Zoo"}}) == DBStatus::OK);
    assert(g_engine.insert(db, "ordered", {{"v", "apple"}}) == DBStatus::OK);
    assert(g_engine.insert(db, "ordered", {{"v", "Banana"}}) == DBStatus::OK);
    dbms::StorageEngine::OrderBySpec byColumn;
    byColumn.colName = "v";
    rows = g_engine.query(db, "ordered", {}, {"v"}, {byColumn});
    assert((rows == std::vector<std::string>{
                        "apple ", "Banana ", "Zoo "}));

    // An explicit ORDER BY collation is resolved through the same metadata,
    // even when the underlying column itself is binary.
    assert(!ddl.executeSql(
        "CREATE TABLE explicit_order (v VARCHAR(50) COLLATE C)", s));
    assert(g_engine.insert(db, "explicit_order", {{"v", "Zoo"}}) == DBStatus::OK);
    assert(g_engine.insert(db, "explicit_order", {{"v", "apple"}}) == DBStatus::OK);
    assert(g_engine.insert(db, "explicit_order", {{"v", "Banana"}}) == DBStatus::OK);
    byColumn.collation = "app.casefold";
    rows = g_engine.query(db, "explicit_order", {}, {"v"}, {byColumn});
    assert((rows == std::vector<std::string>{
                        "apple ", "Banana ", "Zoo "}));

    dbms::StorageEngine::OrderBySpec byLeft;
    byLeft.isExpression = true;
    byLeft.exprFunc = "left";
    byLeft.exprArg = "v";
    byLeft.exprArg2 = "2";
    byLeft.collation = "C";
    rows = g_engine.query(db, "explicit_order", {}, {"v"}, {byLeft});
    assert((rows == std::vector<std::string>{
                        "Banana ", "Zoo ", "apple "}));

    rows = g_engine.sortByExpression(
        db, "explicit_order", {"apple ", "Zoo ", "Banana "}, {byLeft});
    assert((rows == std::vector<std::string>{
                        "Banana ", "Zoo ", "apple "}));
    byLeft.exprArg2 = "-1";
    rows = g_engine.query(db, "explicit_order", {}, {"v"}, {byLeft});
    assert((rows == std::vector<std::string>{
                        "Banana ", "Zoo ", "apple "}));
    byLeft.exprArg2 = "+2";
    rows = g_engine.query(db, "explicit_order", {}, {"v"}, {byLeft});
    assert((rows == std::vector<std::string>{
                        "Banana ", "Zoo ", "apple "}));

    dbms::StorageEngine::OrderBySpec byRight = byLeft;
    byRight.exprFunc = "right";
    byRight.exprArg2 = "2";
    rows = g_engine.query(db, "explicit_order", {}, {"v"}, {byRight});
    assert((rows == std::vector<std::string>{
                        "apple ", "Banana ", "Zoo "}));
    rows = g_engine.sortByExpression(
        db, "explicit_order", {"Zoo ", "Banana ", "apple "}, {byRight});
    assert((rows == std::vector<std::string>{
                        "apple ", "Banana ", "Zoo "}));
    byRight.exprArg2 = "-1";
    rows = g_engine.query(db, "explicit_order", {}, {"v"}, {byRight});
    assert((rows == std::vector<std::string>{
                        "Banana ", "Zoo ", "apple "}));

    assert(!ddl.executeSql(
        "CREATE TABLE unicode_left (v VARCHAR(50) COLLATE C)", s));
    assert(g_engine.insert(db, "unicode_left", {{"v", "êclair"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(db, "unicode_left", {{"v", "éclair"}}) ==
           DBStatus::OK);
    byLeft.exprArg2 = "1";
    rows = g_engine.query(db, "unicode_left", {}, {"v"}, {byLeft});
    assert((rows == std::vector<std::string>{"éclair ", "êclair "}));

    assert(!ddl.executeSql(
        "CREATE TABLE unicode_right (v VARCHAR(50) COLLATE C)", s));
    assert(g_engine.insert(db, "unicode_right", {{"v", "a€"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(db, "unicode_right", {{"v", "aÿ"}}) ==
           DBStatus::OK);
    byRight.exprArg2 = "1";
    rows = g_engine.query(db, "unicode_right", {}, {"v"}, {byRight});
    assert((rows == std::vector<std::string>{"aÿ ", "a€ "}));

    assert(!ddl.executeSql("CREATE TABLE alter_target (id INT)", s));
    assert(!ddl.executeSql(
        "ALTER TABLE alter_target ADD COLUMN "
        "v VARCHAR(20) COLLATE app.casefold", s));
    schema = g_engine.getTableSchema(db, "alter_target");
    assert(schema.len == 2);
    assert(schema.cols[1].collation == "app.casefold");
    assert(schema.cols[1].resolvedCollation == "nocase");

    // Resolution is reconstructed from the persisted custom definition after
    // reopening the engine; it is not an in-memory CREATE COLLATION effect.
    dbms::StorageEngine reopened;
    rows = reopened.query(db, "words", {"=v hello"}, {"v"});
    assert(rows.size() == 2);

    // Unknown and unavailable custom definitions must not silently acquire C
    // semantics and publish a table whose comparisons change after restart.
    assert(ddl.executeSql(
        "CREATE TABLE missing_collation "
        "(v VARCHAR(10) COLLATE app.absent)", s));
    assert(!g_engine.tableExists(db, "missing_collation"));
    assert(ddl.executeSql(
        "CREATE TABLE malformed_collation "
        "(v VARCHAR(10) COLLATE app.)", s));
    assert(!g_engine.tableExists(db, "malformed_collation"));
    assert(!ddl.executeSql(
        "CREATE COLLATION app.unavailable "
        "(provider=libc, locale='dbms_locale_that_does_not_exist')", s));
    assert(ddl.executeSql(
        "CREATE TABLE unavailable_collation "
        "(v VARCHAR(10) COLLATE app.unavailable)", s));
    assert(!g_engine.tableExists(db, "unavailable_collation"));

    cleanup(db);
    std::cout << "[COLLATION] custom runtime resolution OK" << std::endl;
}

static void test_collation_aware_unique_constraints() {
    using dbms::DBStatus;

    std::string db = testDbPath("collation_unique_constraints");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE SCHEMA app", s));
    assert(!ddl.executeSql(
        "CREATE COLLATION app.casefold "
        "(provider=libc, locale='nocase')", s));

    // Column-level UNIQUE must use the column's equality semantics on INSERT
    // and UPDATE, while still allowing a case-only rewrite of the same row.
    assert(!ddl.executeSql(
        "CREATE TABLE inline_unique ("
        "id INT PRIMARY KEY, "
        "v VARCHAR(30) COLLATE nocase UNIQUE)", s));
    assert(g_engine.insert(
               db, "inline_unique", {{"id", "1"}, {"v", "Alpha"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(
               db, "inline_unique", {{"id", "2"}, {"v", "Bravo"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(
               db, "inline_unique", {{"id", "3"}, {"v", "alpha"}}) ==
           DBStatus::DUPLICATE_KEY);
    assert(g_engine.update(
               db, "inline_unique", {{"v", "ALPHA"}}, {"=id 1"}) ==
           DBStatus::OK);
    assert(g_engine.update(
               db, "inline_unique", {{"v", "alpha"}}, {"=id 2"}) ==
           DBStatus::DUPLICATE_KEY);
    assert(g_engine.query(
               db, "inline_unique", {"=v bravo"}, {"id"}).size() == 1);

    // A custom collation on a primary key needs the same heap fallback; its
    // physical B-tree stores the original bytes and cannot decide equality.
    assert(!ddl.executeSql(
        "CREATE TABLE custom_pk ("
        "v VARCHAR(30) COLLATE app.casefold PRIMARY KEY, payload INT)", s));
    assert(g_engine.insert(
               db, "custom_pk", {{"v", "Hello"}, {"payload", "1"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(
               db, "custom_pk", {{"v", "hello"}, {"payload", "2"}}) ==
           DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(
               db, "custom_pk", {{"v", "World"}, {"payload", "2"}}) ==
           DBStatus::OK);
    assert(g_engine.update(
               db, "custom_pk", {{"v", "hELLo"}}, {"=payload 2"}) ==
           DBStatus::DUPLICATE_KEY);
    assert(g_engine.query(
               db, "custom_pk", {"=v world"}, {"payload"}).size() == 1);

    // Composite UNIQUE compares each component under its own semantics and
    // preserves MATCH SIMPLE-style NULL behavior for uniqueness.
    assert(!ddl.executeSql(
        "CREATE TABLE composite_unique ("
        "id INT PRIMARY KEY, tenant INT, "
        "v VARCHAR(30) COLLATE app.casefold, "
        "CONSTRAINT tenant_word_key UNIQUE (tenant, v))", s));
    assert(g_engine.insert(
               db, "composite_unique",
               {{"id", "1"}, {"tenant", "10"}, {"v", "Hello"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(
               db, "composite_unique",
               {{"id", "2"}, {"tenant", "10"}, {"v", "HELLO"}}) ==
           DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(
               db, "composite_unique",
               {{"id", "2"}, {"tenant", "20"}, {"v", "HELLO"}}) ==
           DBStatus::OK);
    for (const char* id : {"3", "4"}) {
        assert(g_engine.insert(
                   db, "composite_unique",
                   {{"id", id}, {"tenant", "10"}, {"v", "NULL"}}) ==
               DBStatus::OK);
    }

    // ALTER must reject pre-existing collation-equal rows before publishing
    // either UNIQUE or PRIMARY KEY metadata.
    assert(!ddl.executeSql(
        "CREATE TABLE alter_unique ("
        "id INT PRIMARY KEY, v VARCHAR(30) COLLATE app.casefold)", s));
    assert(g_engine.insert(
               db, "alter_unique", {{"id", "1"}, {"v", "Key"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(
               db, "alter_unique", {{"id", "2"}, {"v", "KEY"}}) ==
           DBStatus::OK);
    assert(g_engine.alterTableAddUniqueConstraint(
               db, "alter_unique", "alter_unique_v_key", {"v"}) ==
           DBStatus::INVALID_VALUE);
    assert(g_engine.getTableSchema(db, "alter_unique")
               .uniqueConstraints.empty());
    assert(g_engine.update(
               db, "alter_unique", {{"v", "Other"}}, {"=id 2"}) ==
           DBStatus::OK);
    assert(g_engine.alterTableAddUniqueConstraint(
               db, "alter_unique", "alter_unique_v_key", {"v"}) ==
           DBStatus::OK);
    assert(g_engine.insert(
               db, "alter_unique", {{"id", "3"}, {"v", "kEy"}}) ==
           DBStatus::DUPLICATE_KEY);

    assert(!ddl.executeSql(
        "CREATE TABLE alter_pk ("
        "v VARCHAR(30) COLLATE app.casefold, payload INT)", s));
    assert(g_engine.insert(
               db, "alter_pk", {{"v", "Code"}, {"payload", "1"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(
               db, "alter_pk", {{"v", "CODE"}, {"payload", "2"}}) ==
           DBStatus::OK);
    assert(g_engine.alterTableAddPrimaryKey(
               db, "alter_pk", "alter_pk_pkey", {"v"}) ==
           DBStatus::INVALID_VALUE);
    assert(!g_engine.getTableSchema(db, "alter_pk").hasPrimaryKey());
    assert(g_engine.update(
               db, "alter_pk", {{"v", "Other"}}, {"=payload 2"}) ==
           DBStatus::OK);
    assert(g_engine.alterTableAddPrimaryKey(
               db, "alter_pk", "alter_pk_pkey", {"v"}) == DBStatus::OK);
    assert(g_engine.insert(
               db, "alter_pk", {{"v", "cOdE"}, {"payload", "3"}}) ==
           DBStatus::DUPLICATE_KEY);

    // Deferred single-column UNIQUE is checked again at COMMIT using the
    // resolved collation, not the raw payload bytes captured at INSERT time.
    assert(!ddl.executeSql(
        "CREATE TABLE deferred_unique ("
        "id INT PRIMARY KEY, v VARCHAR(30) COLLATE app.casefold, "
        "CONSTRAINT deferred_v_key UNIQUE (v) "
        "DEFERRABLE INITIALLY DEFERRED)", s));
    assert(g_engine.insert(
               db, "deferred_unique", {{"id", "1"}, {"v", "Deferred"}}) ==
           DBStatus::OK);
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(
               db, "deferred_unique", {{"id", "2"}, {"v", "DEFERRED"}}) ==
           DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::UNIQUE_VIOLATION);
    assert(g_engine.query(
               db, "deferred_unique", {}, {"id"}).size() == 1);

    // Reopening reconstructs the runtime collation and keeps enforcement.
    dbms::StorageEngine reopened;
    assert(reopened.insert(
               db, "inline_unique", {{"id", "3"}, {"v", "aLpHa"}}) ==
           DBStatus::DUPLICATE_KEY);

    cleanup(db);
    std::cout << "[COLLATION] UNIQUE and primary-key equality OK"
              << std::endl;
}

static void test_drop_collation_protects_table_dependencies() {
    using dbms::DBStatus;

    std::string db = testDbPath("collation_drop_dependencies");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE SCHEMA app", s));
    assert(!ddl.executeSql(
        "CREATE COLLATION app.casefold "
        "(provider=libc, locale='nocase')", s));
    assert(!ddl.executeSql(
        "CREATE COLLATION app.unused "
        "(provider=libc, locale='C')", s));
    assert(!ddl.executeSql(
        "CREATE TABLE public_words "
        "(v VARCHAR(30) COLLATE app.casefold)", s));
    assert(g_engine.insert(db, "public_words", {{"v", "Hello"}}) ==
           DBStatus::OK);

    const fs::path metadata = fs::path(db) / ".collations";
    const std::string originalBytes = readBytes(metadata);
    assert(g_engine.dropCollation(db, "app__casefold") ==
           DBStatus::INVALID_VALUE);
    assert(readBytes(metadata) == originalBytes);
    assert(ddl.executeSql("DROP COLLATION app.casefold", s));
    assert(ddl.executeSql("DROP COLLATION app.casefold CASCADE", s));
    assert(readBytes(metadata) == originalBytes);
    assert(g_engine.getTableSchema(db, "public_words").len == 1);
    assert(g_engine.query(
               db, "public_words", {"=v hello"}, {"v"}).size() == 1);

    // DROP SCHEMA must not bypass the same cross-schema dependency merely
    // because it is dropping the collation through its auxiliary worklist.
    assert(ddl.executeSql("DROP SCHEMA app CASCADE", s));
    assert(g_engine.schemaExists(db, "app"));
    assert(readBytes(metadata) == originalBytes);

    // An unrelated definition can still be removed while the dependency is
    // present; the scan is specific to the requested object.
    assert(!ddl.executeSql("DROP COLLATION app.unused", s));
    assert((g_engine.getCollationNames(db) ==
            std::vector<std::string>{"app__casefold"}));

    assert(!ddl.executeSql("DROP TABLE public_words", s));
    const fs::path corruptSchema = fs::path(db) / "orphan.stc";
    {
        std::ofstream output(corruptSchema, std::ios::binary);
        output << "not a table schema";
        assert(output.good());
    }
    const std::string beforeCorruptPreflight = readBytes(metadata);
    assert(g_engine.dropCollation(db, "missing") ==
           DBStatus::TABLE_NOT_FOUND);
    assert(g_engine.dropCollation(db, "app__casefold") ==
           DBStatus::CORRUPTED_DATA);
    assert(readBytes(metadata) == beforeCorruptPreflight);
    assert(fs::remove(corruptSchema));

    assert(!ddl.executeSql("DROP SCHEMA app CASCADE", s));
    assert(!g_engine.schemaExists(db, "app"));
    assert(!fs::exists(metadata));

    cleanup(db);
    std::cout << "[COLLATION] DROP dependency protection OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_collation_provider();
    test_collate_persists_and_applies();
    test_collate_all_operators();
    test_collate_mixed_columns();
    test_collate_binary_collations();
    test_default_sql_sort_uses_locale();
    test_default_text_predicate_with_index();
    test_collate_with_index();
    test_collate_schema_persistence();
    test_collation_metadata_is_atomic_and_backward_compatible();
    test_collation_ddl_preserves_options_and_namespace();
    test_custom_collation_runtime_resolution();
    test_collation_aware_unique_constraints();
    test_drop_collation_protects_table_dependencies();
    std::cout << "[COLLATION] all passed" << std::endl;
    return 0;
}
