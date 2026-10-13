#include "parser/query_binding.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    QueryBindingMetadata metadata;
    metadata.relation = [](const std::string& name) {
        assert(name == "empty_patterns");
        return QueryRelationMetadata{"public", name, {{"t", "text"}, {"b", "bytea"}}, {}};
    };
    size_t controls = 0;
    const auto check = [&](const std::string& predicate, const std::string& state,
                           const std::vector<QueryBindingDatum>& inputs = {}) {
        ++controls;
        std::string actual;
        try {
            auto query = prepareQuery("SELECT " + predicate +
                " AS matched FROM empty_patterns WHERE false", inputs, metadata);
            assert(query.output.size() == 1 && query.output.front().type == "boolean");
        } catch (const DbError& error) {
            actual = error.sqlState();
        }
        std::cout << "PATTERN_ESCAPE_TYPE " << predicate << " expected=" << state
                  << " actual=" << actual << std::endl;
        assert(actual == state);
    };
    for (const std::string op : {"LIKE", "NOT LIKE"}) {
        check("b " + op + " 'a#_%'::bytea ESCAPE '#'::bytea", "");
        check("b " + op + " 'a#_%' ESCAPE '#'", "");
        check("b " + op + " 'a%'::bytea ESCAPE NULL", "");
        check("b " + op + " 'a#_%'::bytea ESCAPE '#'::text", "42883");
        check("b " + op + " 'a%'::bytea ESCAPE 1", "42883");
    }
    for (const std::string op : {"LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE"}) {
        check("t " + op + " 'a#_%'::text ESCAPE '#'::text", "");
        check("t " + op + " 'a#_%' ESCAPE '#'", "");
        check("t " + op + " 'a%' ESCAPE NULL", "");
        check("t " + op + " 'a#_%' ESCAPE '#'::bytea", "42883");
        check("t " + op + " 'a%' ESCAPE 1", "42883");
    }
    const std::vector<QueryBindingDatum> byteaInput{
        {"argument:1", "escape", "bytea", {}, false, std::nullopt, 1}};
    const std::vector<QueryBindingDatum> textInput{
        {"argument:1", "escape", "text", {}, false, std::nullopt, 1}};
    check("b LIKE 'a#_%'::bytea ESCAPE $1", "", byteaInput);
    check("b LIKE 'a#_%'::bytea ESCAPE $1", "42883", textInput);
    check("t LIKE 'a#_%'::text ESCAPE $1", "", textInput);
    check("t LIKE 'a#_%'::text ESCAPE $1", "42883", byteaInput);
    assert(controls == 34);
    std::cout << "[PATTERN ESCAPE TYPE] all 34 metadata-only controls passed\n";
}
