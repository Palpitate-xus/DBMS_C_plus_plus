#include "catalog/type_registry.h"
#include "interfaces/table_schema.h"
#include "Config.h"
#include <cassert>
#include <iostream>
#include <string>

dbms::Config g_config;
using namespace dbms;

static void test_bootstrap_has_core_types() {
    TypeRegistry& reg = TypeRegistry::instance();
    assert(reg.findType("integer") != nullptr);
    assert(reg.findType("int") != nullptr);
    assert(reg.findType("int4") != nullptr);
    assert(reg.findType("bigint") != nullptr);
    assert(reg.findType("varchar") != nullptr);
    assert(reg.findType("character varying") != nullptr);
    assert(reg.findType("text") != nullptr);
    assert(reg.findType("timestamp") != nullptr);
    assert(reg.findType("timestamptz") != nullptr);
    assert(reg.findType("bytea") != nullptr);
    assert(reg.findType("uuid") != nullptr);
    assert(reg.findType("jsonb") != nullptr);
    assert(reg.findType("point") != nullptr);
    assert(reg.findType("inet") != nullptr);
    assert(reg.findType("tsvector") != nullptr);
    assert(reg.findType("int4range") != nullptr);
    std::cout << "test_bootstrap_has_core_types passed\n";
}

static void test_normalize_aliases() {
    TypeRegistry& reg = TypeRegistry::instance();
    assert(reg.normalizeTypeName("int") == "integer");
    assert(reg.normalizeTypeName("INT") == "integer");
    assert(reg.normalizeTypeName("Int4") == "integer");
    assert(reg.normalizeTypeName("varchar") == "character varying");
    assert(reg.normalizeTypeName("char") == "character");
    assert(reg.normalizeTypeName("bool") == "boolean");
    assert(reg.normalizeTypeName("float") == "real");
    assert(reg.normalizeTypeName("double") == "double precision");
    assert(reg.normalizeTypeName("decimal") == "numeric");
    assert(reg.normalizeTypeName("no_such_type").empty());
    std::cout << "test_normalize_aliases passed\n";
}

static void test_apply_type_mods_varchar() {
    TypeRegistry& reg = TypeRegistry::instance();
    TypeModResult r = reg.applyTypeMods("character varying", {"255"});
    assert(r.ok());
    assert(r.typmod == 255);
    assert(r.dsize == 255);
    assert(r.isVariableLength);

    r = reg.applyTypeMods("varchar", {"0"});
    assert(r.ok());
    assert(r.dsize == 0);

    r = reg.applyTypeMods("character varying", {});
    assert(r.ok());
    assert(r.dsize == 65535); // 默认最大长度
    std::cout << "test_apply_type_mods_varchar passed\n";
}

static void test_apply_type_mods_numeric() {
    TypeRegistry& reg = TypeRegistry::instance();
    TypeModResult r = reg.applyTypeMods("numeric", {"10", "2"});
    assert(r.ok());
    assert(r.dsize == 8);
    assert(!r.isVariableLength);

    r = reg.applyTypeMods("decimal", {"5"});
    assert(r.ok());

    r = reg.applyTypeMods("numeric", {"1001", "0"});
    assert(!r.ok());
    assert(r.error.find("precision") != std::string::npos);

    r = reg.applyTypeMods("numeric", {"5", "6"});
    assert(!r.ok());
    std::cout << "test_apply_type_mods_numeric passed\n";
}

static void test_apply_type_mods_timestamp() {
    TypeRegistry& reg = TypeRegistry::instance();
    TypeModResult r = reg.applyTypeMods("timestamp", {"3"});
    assert(r.ok());
    assert(r.typmod == 3);
    assert(r.dsize == 8);

    r = reg.applyTypeMods("timestamptz", {"7"});
    assert(!r.ok());
    std::cout << "test_apply_type_mods_timestamp passed\n";
}

static void test_resolve_column_type() {
    TypeRegistry& reg = TypeRegistry::instance();
    Column col;
    std::string err = reg.resolveColumnType(col, "varchar", {"100"}, false);
    assert(err.empty());
    assert(col.dataType == "character varying");
    assert(col.dsize == 100);
    assert(col.isVariableLength);
    assert(!col.isArray);

    err = reg.resolveColumnType(col, "integer", {}, false);
    assert(err.empty());
    assert(col.dataType == "integer");
    assert(col.dsize == 4);
    assert(!col.isVariableLength);

    err = reg.resolveColumnType(col, "integer", {}, true);
    assert(err.empty());
    assert(col.dataType == "integer[]");
    assert(col.isArray);
    assert(col.isVariableLength);

    err = reg.resolveColumnType(col, "unknown_type", {}, false);
    assert(!err.empty());
    std::cout << "test_resolve_column_type passed\n";
}

static void test_validate_column() {
    TypeRegistry& reg = TypeRegistry::instance();
    Column col;
    col.dataName = "c1";
    col.dataType = "int";
    col.dsize = 0;
    std::string err = reg.validateColumn(col);
    assert(err.empty());
    assert(col.dataType == "int"); // 不强制改名
    assert(col.dsize == 4);
    assert(!col.isVariableLength);

    col.dataType = "integer[]";
    col.dsize = 0;
    err = reg.validateColumn(col);
    assert(err.empty());
    assert(col.dsize == 4);
    assert(col.isVariableLength);
    assert(col.isArray);

    col.dataType = "not_a_type";
    col.dsize = 0;
    err = reg.validateColumn(col);
    assert(!err.empty());
    std::cout << "test_validate_column passed\n";
}

static void test_type_categories() {
    TypeRegistry& reg = TypeRegistry::instance();
    assert(reg.findType("boolean")->category == TypeCategory::Boolean);
    assert(reg.findType("integer")->category == TypeCategory::Numeric);
    assert(reg.findType("text")->category == TypeCategory::String);
    assert(reg.findType("bytea")->category == TypeCategory::Binary);
    assert(reg.findType("timestamp")->category == TypeCategory::DateTime);
    assert(reg.findType("point")->category == TypeCategory::Geometric);
    assert(reg.findType("inet")->category == TypeCategory::Network);
    assert(reg.findType("uuid")->category == TypeCategory::UUID);
    assert(reg.findType("jsonb")->category == TypeCategory::JSON);
    assert(reg.findType("tsvector")->category == TypeCategory::TextSearch);
    assert(reg.findType("int4range")->category == TypeCategory::Range);
    std::cout << "test_type_categories passed\n";
}

int main() {
    test_bootstrap_has_core_types();
    test_normalize_aliases();
    test_apply_type_mods_varchar();
    test_apply_type_mods_numeric();
    test_apply_type_mods_timestamp();
    test_resolve_column_type();
    test_validate_column();
    test_type_categories();
    std::cout << "All type_registry tests passed\n";
    return 0;
}
