#include "parser/parser.h"
#include <iostream>
#include <string>
#include <vector>

namespace {
struct Keyword { const char* word; bool reserved; };
// Factual PostgreSQL18.6 pg_get_keywords() categories, checked independently
// against the actual PG reference by the protocol fixture.
constexpr Keyword keywords[] = {
    {"abort", false}, {"absent", false}, {"absolute", false}, {"access", false},
    {"action", false}, {"add", false}, {"admin", false}, {"after", false},
    {"aggregate", false}, {"all", true}, {"also", false}, {"alter", false},
    {"always", false}, {"analyse", true}, {"analyze", true}, {"and", true},
    {"any", true}, {"array", true}, {"as", true}, {"asc", true},
    {"asensitive", false}, {"assertion", false}, {"assignment", false}, {"asymmetric", true},
    {"at", false}, {"atomic", false}, {"attach", false}, {"attribute", false},
    {"authorization", false}, {"backward", false}, {"before", false}, {"begin", false},
    {"between", false}, {"bigint", false}, {"binary", false}, {"bit", false},
    {"boolean", false}, {"both", true}, {"breadth", false}, {"by", false},
    {"cache", false}, {"call", false}, {"called", false}, {"cascade", false},
    {"cascaded", false}, {"case", true}, {"cast", true}, {"catalog", false},
    {"chain", false}, {"char", false}, {"character", false}, {"characteristics", false},
    {"check", true}, {"checkpoint", false}, {"class", false}, {"close", false},
    {"cluster", false}, {"coalesce", false}, {"collate", true}, {"collation", false},
    {"column", true}, {"columns", false}, {"comment", false}, {"comments", false},
    {"commit", false}, {"committed", false}, {"compression", false}, {"concurrently", false},
    {"conditional", false}, {"configuration", false}, {"conflict", false}, {"connection", false},
    {"constraint", true}, {"constraints", false}, {"content", false}, {"continue", false},
    {"conversion", false}, {"copy", false}, {"cost", false}, {"create", true},
    {"cross", false}, {"csv", false}, {"cube", false}, {"current", false},
    {"current_catalog", true}, {"current_date", true}, {"current_role", true}, {"current_schema", false},
    {"current_time", true}, {"current_timestamp", true}, {"current_user", true}, {"cursor", false},
    {"cycle", false}, {"data", false}, {"database", false}, {"day", false},
    {"deallocate", false}, {"dec", false}, {"decimal", false}, {"declare", false},
    {"default", true}, {"defaults", false}, {"deferrable", true}, {"deferred", false},
    {"definer", false}, {"delete", false}, {"delimiter", false}, {"delimiters", false},
    {"depends", false}, {"depth", false}, {"desc", true}, {"detach", false},
    {"dictionary", false}, {"disable", false}, {"discard", false}, {"distinct", true},
    {"do", true}, {"document", false}, {"domain", false}, {"double", false},
    {"drop", false}, {"each", false}, {"else", true}, {"empty", false},
    {"enable", false}, {"encoding", false}, {"encrypted", false}, {"end", true},
    {"enforced", false}, {"enum", false}, {"error", false}, {"escape", false},
    {"event", false}, {"except", true}, {"exclude", false}, {"excluding", false},
    {"exclusive", false}, {"execute", false}, {"exists", false}, {"explain", false},
    {"expression", false}, {"extension", false}, {"external", false}, {"extract", false},
    {"false", true}, {"family", false}, {"fetch", true}, {"filter", false},
    {"finalize", false}, {"first", false}, {"float", false}, {"following", false},
    {"for", true}, {"force", false}, {"foreign", true}, {"format", false},
    {"forward", false}, {"freeze", false}, {"from", true}, {"full", false},
    {"function", false}, {"functions", false}, {"generated", false}, {"global", false},
    {"grant", true}, {"granted", false}, {"greatest", false}, {"group", true},
    {"grouping", false}, {"groups", false}, {"handler", false}, {"having", true},
    {"header", false}, {"hold", false}, {"hour", false}, {"identity", false},
    {"if", false}, {"ilike", false}, {"immediate", false}, {"immutable", false},
    {"implicit", false}, {"import", false}, {"in", true}, {"include", false},
    {"including", false}, {"increment", false}, {"indent", false}, {"index", false},
    {"indexes", false}, {"inherit", false}, {"inherits", false}, {"initially", true},
    {"inline", false}, {"inner", false}, {"inout", false}, {"input", false},
    {"insensitive", false}, {"insert", false}, {"instead", false}, {"int", false},
    {"integer", false}, {"intersect", true}, {"interval", false}, {"into", true},
    {"invoker", false}, {"is", false}, {"isnull", false}, {"isolation", false},
    {"join", false}, {"json", false}, {"json_array", false}, {"json_arrayagg", false},
    {"json_exists", false}, {"json_object", false}, {"json_objectagg", false}, {"json_query", false},
    {"json_scalar", false}, {"json_serialize", false}, {"json_table", false}, {"json_value", false},
    {"keep", false}, {"key", false}, {"keys", false}, {"label", false},
    {"language", false}, {"large", false}, {"last", false}, {"lateral", true},
    {"leading", true}, {"leakproof", false}, {"least", false}, {"left", false},
    {"level", false}, {"like", false}, {"limit", true}, {"listen", false},
    {"load", false}, {"local", false}, {"localtime", true}, {"localtimestamp", true},
    {"location", false}, {"lock", false}, {"locked", false}, {"logged", false},
    {"mapping", false}, {"match", false}, {"matched", false}, {"materialized", false},
    {"maxvalue", false}, {"merge", false}, {"merge_action", false}, {"method", false},
    {"minute", false}, {"minvalue", false}, {"mode", false}, {"month", false},
    {"move", false}, {"name", false}, {"names", false}, {"national", false},
    {"natural", false}, {"nchar", false}, {"nested", false}, {"new", false},
    {"next", false}, {"nfc", false}, {"nfd", false}, {"nfkc", false},
    {"nfkd", false}, {"no", false}, {"none", false}, {"normalize", false},
    {"normalized", false}, {"not", true}, {"nothing", false}, {"notify", false},
    {"notnull", false}, {"nowait", false}, {"null", true}, {"nullif", false},
    {"nulls", false}, {"numeric", false}, {"object", false}, {"objects", false},
    {"of", false}, {"off", false}, {"offset", true}, {"oids", false},
    {"old", false}, {"omit", false}, {"on", true}, {"only", true},
    {"operator", false}, {"option", false}, {"options", false}, {"or", true},
    {"order", true}, {"ordinality", false}, {"others", false}, {"out", false},
    {"outer", false}, {"over", false}, {"overlaps", false}, {"overlay", false},
    {"overriding", false}, {"owned", false}, {"owner", false}, {"parallel", false},
    {"parameter", false}, {"parser", false}, {"partial", false}, {"partition", false},
    {"passing", false}, {"password", false}, {"path", false}, {"period", false},
    {"placing", true}, {"plan", false}, {"plans", false}, {"policy", false},
    {"position", false}, {"preceding", false}, {"precision", false}, {"prepare", false},
    {"prepared", false}, {"preserve", false}, {"primary", true}, {"prior", false},
    {"privileges", false}, {"procedural", false}, {"procedure", false}, {"procedures", false},
    {"program", false}, {"publication", false}, {"quote", false}, {"quotes", false},
    {"range", false}, {"read", false}, {"real", false}, {"reassign", false},
    {"recursive", false}, {"ref", false}, {"references", true}, {"referencing", false},
    {"refresh", false}, {"reindex", false}, {"relative", false}, {"release", false},
    {"rename", false}, {"repeatable", false}, {"replace", false}, {"replica", false},
    {"reset", false}, {"restart", false}, {"restrict", false}, {"return", false},
    {"returning", true}, {"returns", false}, {"revoke", false}, {"right", false},
    {"role", false}, {"rollback", false}, {"rollup", false}, {"routine", false},
    {"routines", false}, {"row", false}, {"rows", false}, {"rule", false},
    {"savepoint", false}, {"scalar", false}, {"schema", false}, {"schemas", false},
    {"scroll", false}, {"search", false}, {"second", false}, {"security", false},
    {"select", true}, {"sequence", false}, {"sequences", false}, {"serializable", false},
    {"server", false}, {"session", false}, {"session_user", true}, {"set", false},
    {"setof", false}, {"sets", false}, {"share", false}, {"show", false},
    {"similar", false}, {"simple", false}, {"skip", false}, {"smallint", false},
    {"snapshot", false}, {"some", true}, {"source", false}, {"sql", false},
    {"stable", false}, {"standalone", false}, {"start", false}, {"statement", false},
    {"statistics", false}, {"stdin", false}, {"stdout", false}, {"storage", false},
    {"stored", false}, {"strict", false}, {"string", false}, {"strip", false},
    {"subscription", false}, {"substring", false}, {"support", false}, {"symmetric", true},
    {"sysid", false}, {"system", false}, {"system_user", true}, {"table", true},
    {"tables", false}, {"tablesample", false}, {"tablespace", false}, {"target", false},
    {"temp", false}, {"template", false}, {"temporary", false}, {"text", false},
    {"then", true}, {"ties", false}, {"time", false}, {"timestamp", false},
    {"to", true}, {"trailing", true}, {"transaction", false}, {"transform", false},
    {"treat", false}, {"trigger", false}, {"trim", false}, {"true", true},
    {"truncate", false}, {"trusted", false}, {"type", false}, {"types", false},
    {"uescape", false}, {"unbounded", false}, {"uncommitted", false}, {"unconditional", false},
    {"unencrypted", false}, {"union", true}, {"unique", true}, {"unknown", false},
    {"unlisten", false}, {"unlogged", false}, {"until", false}, {"update", false},
    {"user", true}, {"using", true}, {"vacuum", false}, {"valid", false},
    {"validate", false}, {"validator", false}, {"value", false}, {"values", false},
    {"varchar", false}, {"variadic", true}, {"varying", false}, {"verbose", false},
    {"version", false}, {"view", false}, {"views", false}, {"virtual", false},
    {"volatile", false}, {"when", true}, {"where", true}, {"whitespace", false},
    {"window", true}, {"with", true}, {"within", false}, {"without", false},
    {"work", false}, {"wrapper", false}, {"write", false}, {"xml", false},
    {"xmlattributes", false}, {"xmlconcat", false}, {"xmlelement", false}, {"xmlexists", false},
    {"xmlforest", false}, {"xmlnamespaces", false}, {"xmlparse", false}, {"xmlpi", false},
    {"xmlroot", false}, {"xmlserialize", false}, {"xmltable", false}, {"year", false},
    {"yes", false}, {"zone", false},
};
struct Valid { const char* sql; std::vector<std::string> values; bool local; };
const Valid valid[] = {
    {"SET TimeZone='UTC'", {"'UTC'"}, false},
    {"SET SESSION\"TimeZone\"='UTC'", {"'UTC'"}, false},
    {"SET\"TimeZone\"TO'UTC'", {"'UTC'"}, false},
    {"SET TimeZone = - 5.5", {"-5.5"}, false},
    {"SET TimeZone=+5.e0", {"+5.e0"}, false},
    {"SET TIME ZONE DEFAULT", {"default"}, false},
    {"SET SESSION TIME ZONE LOCAL", {"default"}, false},
    {"SET TimeZone TO LOCAL", {"LOCAL"}, false},
    {"SET TimeZone FROM CURRENT", {}, false},
    {"SET SESSION TimeZone FROM CURRENT", {}, false},
    {"SET LOCAL TimeZone='UTC'", {"'UTC'"}, true},
    {"SET LOCAL TIME ZONE LOCAL", {"default"}, true},
    {"SET TimeZone TO 'UTC','GMT'", {"'UTC'", "'GMT'"}, false},
    {"SET TimeZone TO E'UT\\x43'", {"'UTC'"}, false},
    {"SET TimeZone TO $q$UTC$q$", {"'UTC'"}, false},
    {"SET TimeZone TO 'UT'\n'C'", {"'UTC'"}, false},
    {"SET TimeZone TO 'UT' -- x\n'C'", {"'UTC'"}, false},
    {"SET TIME ZONE INTERVAL '+05:30' HOUR TO MINUTE", {"INTERVAL '+05:30' HOUR TO MINUTE"}, false},
    {"SET TIME ZONE INTERVAL(3) '+05:30'", {"INTERVAL(3) '+05:30'"}, false},
    {"SET TIME ZONE INTERVAL '+05:30' MINUTE", {"INTERVAL '+05:30' MINUTE"}, false},
    {"SET TimeZone='UTC'; -- tail", {"'UTC'"}, false},
    {"SET /*a*/ SESSION /*b*/ \"TimeZone\" /*c*/ = 'UTC'", {"'UTC'"}, false},
};
}

int main() {
    dbms::SQLParser parser;
    size_t checks = 0, failures = 0;
    const auto inspect = [&](const std::string& sql, bool accepted,
                             const std::vector<std::string>* values,
                             bool local, bool binding) {
        const auto parsed = binding ? parser.parseForBinding(sql) : parser.parse(sql);
        const auto* node = dynamic_cast<const dbms::SetStmt*>(parsed.stmt.get());
        bool good = parsed.success == accepted;
        if (accepted) {
            std::string name = node ? node->name : "";
            if (name.size() >= 2 && name.front() == '"' && name.back() == '"')
                name = name.substr(1, name.size() - 2);
            good = good && node && dbms::SQLParser::toLower(name) == "timezone" && !node->isReset && !node->isShow;
            if (node) {
                good = good && node->scope == (local ? dbms::SetStmt::Scope::Local : dbms::SetStmt::Scope::Session);
                if (values) {
                    good = good && node->values.size() == values->size();
                    if (node->values.size() == values->size()) {
                        for (size_t i = 0; i < values->size(); ++i) {
                            const auto& actual = node->values[i];
                            const auto& expected = (*values)[i];
                            // Unquoted keyword spelling is case-insensitive;
                            // quoted data and compound values stay byte-exact.
                            const bool keyword = !expected.empty() &&
                                expected.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_") == std::string::npos;
                            good = good && (keyword ? dbms::SQLParser::toLower(actual) == dbms::SQLParser::toLower(expected)
                                                    : actual == expected);
                        }
                    }
                }
            }
        } else {
            good = good && !parsed.stmt && parsed.sqlState == "42601";
        }
        ++checks;
        if (!good) {
            ++failures;
            std::cerr << "TZ_GRAMMAR_NATIVE " << checks << " binding=" << binding
                      << " expected=" << accepted << " actual=" << parsed.success
                      << " state=" << parsed.sqlState << " sql=" << sql << '\n';
        }
    };
    for (const auto& keyword : keywords) {
        const std::string word = keyword.word;
        for (const std::string scope : {"", "SESSION "}) {
            for (const bool binding : {false, true}) {
                const bool generic = !keyword.reserved || word == "default" ||
                    word == "true" || word == "false" || word == "on";
                const std::vector<std::string> value = {word};
                inspect("SET " + scope + "TimeZone TO " + word, generic, &value, false, binding);
                const std::vector<std::string> normalized = {"default"};
                inspect("SET " + scope + "TIME ZONE " + word,
                    word == "default" || word == "local", &normalized, false, binding);
            }
        }
    }
    for (const auto& test : valid)
        for (const bool binding : {false, true})
            inspect(test.sql, true, &test.values, test.local, binding);
    const std::vector<std::string> invalid = {
        "SET TimeZone 'UTC'",
        "SET SESSION TimeZone 'UTC'",
        "SET \"TimeZone\" 'UTC'",
        "SET TimeZone DEFAULT",
        "SET SESSION TimeZone DEFAULT",
        "SET TIME ZONE = 'UTC'",
        "SET SESSION TIME ZONE = 'UTC'",
        "SET TIME ZONE = DEFAULT",
        "SET SESSION TIME ZONE = DEFAULT",
        "SET TIME ZONE TO 'UTC'",
        "SET SESSION TIME ZONE TO DEFAULT",
        "SET TimeZone",
        "SET TimeZone TO",
        "SET TimeZone=",
        "SET TIME ZONE",
        "SET TimeZone TO TO 'UTC'",
        "SET TimeZone == 'UTC'",
        "SET TimeZone TO 'UTC' junk",
        "SET TIME ZONE 'UTC' junk",
        "SET TimeZone TO 'UTC' 'GMT'",
        "SET TimeZone TO 'UT' /*\n*/ 'C'",
        "SET TimeZone TO 'UTC',",
        "SET TimeZone TO ,'UTC'",
        "SET TimeZone TO DEFAULT,'UTC'",
        "SET TIME ZONE 'UTC','GMT'",
        "SET TimeZone TO Asia/Shanghai",
        "SET TimeZone TO +05:30",
        "SET TimeZone TO (5)",
        "SET TimeZone TO 5+1",
        "SET TimeZone TO $1",
        "SET TimeZone TO helper()",
        "SET TimeZone FROM",
        "SET TimeZone FROM DEFAULT",
        "SET TIME ZONE FROM CURRENT",
        "SET TimeZone FROM CURRENT junk",
        "SET TimeZone TO INTERVAL '+05:30'",
        "SET TIME ZONE INTERVAL '+05:30' DAY",
        "SET TIME ZONE INTERVAL '+05:30' SECOND",
        "SET TimeZone DEFAULT; SHOW TimeZone",
        "SET TIME ZONE = DEFAULT; SHOW TimeZone",
        "SETTINGS TimeZone='UTC'",
        "SETUP TimeZone DEFAULT",
    };
    for (const auto& sql : invalid)
        for (const bool binding : {false, true})
            inspect(sql, false, nullptr, false, binding);
    // These are different parameter identities, never builtin TimeZone.
    for (const std::string sql : {"SET application_name='timezone'",
            "SET search_path TO public", "SET timezone_suffix='UTC'",
            "SET timezone.foo='UTC'", "SET \"TimeZone\".foo='UTC'"}) {
        for (const bool binding : {false, true}) {
            const auto parsed = binding ? parser.parseForBinding(sql) : parser.parse(sql);
            const auto* node = dynamic_cast<const dbms::SetStmt*>(parsed.stmt.get());
            const bool good = parsed.success && node && node->name != "timezone";
            ++checks;
            if (!good) { ++failures; std::cerr << "TZ_GRAMMAR_BOUNDARY " << sql << '\n'; }
        }
    }
    std::cout << "TIMEZONE_GRAMMAR_NATIVE_COMPLETE " << checks << " FAILURES " << failures << '\n';
    return checks == 4090 && failures == 0 ? 0 : 1;
}
