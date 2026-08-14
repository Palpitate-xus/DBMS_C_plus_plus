#include "parser.h"
#include <charconv>
#include <cctype>
#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <set>
#include <sstream>

namespace dbms {

static std::string stripQuotes(const std::string& s) {
    if (s.size() >= 2 && ((s.front() == '\'' && s.back() == '\'') ||
                          (s.front() == '"' && s.back() == '"'))) {
        return s.substr(1, s.size() - 2);
    }
    return s;
}

static bool parseNonNegativeInteger(const std::string& token, size_t& value) {
    if (token.empty()) return false;
    for (unsigned char c : token) {
        if (!std::isdigit(c)) return false;
    }
    try {
        size_t consumed = 0;
        const unsigned long long parsed = std::stoull(token, &consumed, 10);
        if (consumed != token.size() || parsed > std::numeric_limits<size_t>::max()) {
            return false;
        }
        value = static_cast<size_t>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

static bool parseSignedInteger(const std::string& token, int& value) {
    if (token.empty()) return false;
    size_t first = token[0] == '-' || token[0] == '+' ? 1 : 0;
    if (first == token.size()) return false;
    for (size_t i = first; i < token.size(); ++i) {
        if (!std::isdigit(static_cast<unsigned char>(token[i]))) return false;
    }
    try {
        size_t consumed = 0;
        const long long parsed = std::stoll(token, &consumed, 10);
        if (consumed != token.size() || parsed < std::numeric_limits<int>::min() ||
            parsed > std::numeric_limits<int>::max()) return false;
        value = static_cast<int>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

static bool parseInt64Token(const std::string& token, int64_t& value) {
    if (token.empty()) return false;
    const char* begin = token.data();
    const char* end = begin + token.size();
    const auto result = std::from_chars(begin, end, value);
    return result.ec == std::errc{} && result.ptr == end;
}

static bool parsePositiveDouble(const std::string& token, double& value) {
    try {
        size_t consumed = 0;
        const double parsed = std::stod(token, &consumed);
        if (consumed != token.size() || !std::isfinite(parsed) || parsed <= 0.0) return false;
        value = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

// ============================================================================
// Forward declarations for internal helper functions
// ============================================================================

static std::vector<std::string> collectParenthesized(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseSimpleExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseExpr(const std::vector<std::string>& tokens, size_t& pos);
static SelectItem parseSelectItem(const std::vector<std::string>& tokens, size_t& pos);
static std::unique_ptr<FromItem> parseFromItem(const std::vector<std::string>& tokens, size_t& pos);

struct SetOperatorLocation {
    size_t position = std::string::npos;
    SetOp op = SetOp::None;
    bool all = false;
};

// Set-operation precedence is lower than SELECT clauses, with INTERSECT
// binding more tightly than UNION/EXCEPT.  Selecting the rightmost operator
// at the chosen precedence builds a left-associative tree while recursively
// parsing the two operands (A UNION B UNION C => (A UNION B) UNION C).
static bool findTopLevelSetOperator(const std::vector<std::string>& tokens,
                                    SetOperatorLocation& result) {
    int depth = 0;
    SetOperatorLocation low;
    SetOperatorLocation intersect;
    for (size_t i = 0; i < tokens.size(); ++i) {
        const std::string word = SQLParser::toLower(tokens[i]);
        if (word == "(") { ++depth; continue; }
        if (word == ")") { if (depth > 0) --depth; continue; }
        if (depth != 0) continue;

        SetOp op = SetOp::None;
        if (word == "union") op = SetOp::Union;
        else if (word == "except") op = SetOp::Except;
        else if (word == "intersect") op = SetOp::Intersect;
        if (op == SetOp::None) continue;

        bool all = false;
        if (i + 1 < tokens.size()) {
            const std::string modifier = SQLParser::toLower(tokens[i + 1]);
            all = modifier == "all";
        }
        SetOperatorLocation candidate{i, op, all};
        if (op == SetOp::Intersect) {
            intersect = candidate;
        } else {
            low = candidate;
        }
    }
    if (low.position != std::string::npos) {
        result = low;
        return true;
    }
    if (intersect.position != std::string::npos) {
        result = intersect;
        return true;
    }
    return false;
}

static std::string joinParserTokens(const std::vector<std::string>& tokens,
                                    size_t begin, size_t end) {
    std::string result;
    for (size_t i = begin; i < end; ++i) {
        if (!result.empty()) result += ' ';
        result += tokens[i];
    }
    return result;
}

// ============================================================================
// 工具函数
// ============================================================================

std::string SQLParser::toLower(const std::string& s) {
    std::string r = s;
    std::transform(r.begin(), r.end(), r.begin(),
                   [](unsigned char c){ return std::tolower(c); });
    return r;
}

static std::string toUpper(const std::string& s) {
    std::string r = s;
    std::transform(r.begin(), r.end(), r.begin(),
                   [](unsigned char c){ return std::toupper(c); });
    return r;
}

static bool isNumericToken(const std::string& s) {
    if (s.empty()) return false;
    size_t i = 0;
    if (s[0] == '+' || s[0] == '-') i = 1;
    bool hasDigit = false, hasDot = false;
    for (; i < s.size(); ++i) {
        if (std::isdigit(static_cast<unsigned char>(s[i]))) { hasDigit = true; continue; }
        if (s[i] == '.') {
            if (hasDot) return false;
            hasDot = true;
            continue;
        }
        return false;
    }
    return hasDigit;
}

static bool isStringLiteralToken(const std::string& s) {
    return s.size() >= 2 && s.front() == '\'' && s.back() == '\'';
}

std::string SQLParser::trim(const std::string& s) {
    size_t a = 0;
    while (a < s.size() && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    size_t b = s.size();
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

std::vector<std::string> SQLParser::tokenize(const std::string& sql) {
    std::vector<std::string> tokens;
    std::string cur;
    bool inString = false;
    char stringChar = 0;
    bool inIdentifier = false; // "quoted identifier"

    for (size_t i = 0; i < sql.size(); ++i) {
        char c = sql[i];
        if (inString) {
            cur += c;
            if (c == stringChar) {
                // check escape
                size_t backslashCount = 0;
                for (size_t j = cur.size() - 2; j + 1 > 0 && cur[j] == '\\'; --j) {
                    ++backslashCount;
                }
                if (backslashCount % 2 == 0) {
                    inString = false;
                    tokens.push_back(cur);
                    cur.clear();
                }
            }
            continue;
        }
        if (inIdentifier) {
            cur += c;
            if (c == '"') {
                inIdentifier = false;
                tokens.push_back(cur);
                cur.clear();
            }
            continue;
        }
        if (c == '\'' || c == '"') {
            if (!cur.empty()) {
                tokens.push_back(cur);
                cur.clear();
            }
            if (c == '\'') {
                inString = true;
                stringChar = '\'';
            } else {
                inIdentifier = true;
            }
            cur += c;
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(c))) {
            if (!cur.empty()) {
                tokens.push_back(cur);
                cur.clear();
            }
            continue;
        }
        if (c == '(' || c == ')' || c == ',' || c == ';' || c == '*' ||
            c == '=' || c == '<' || c == '>' || c == '+' || c == '-' ||
            c == '/' || c == '%' || c == '^' || c == '~' || c == '!' ||
            c == '|' || c == '&' || c == '#' || c == '@' || c == '?' ||
            c == ':' || c == '[' || c == ']' || c == '.') {
            if (!cur.empty()) {
                tokens.push_back(cur);
                cur.clear();
            }
            // multi-char operators
            if (i + 1 < sql.size()) {
                char next = sql[i + 1];
                std::string two = std::string(1, c) + next;
                if (two == "<=" || two == ">=" || two == "<>" || two == "!=" ||
                    two == "::" || two == "||" || two == "->" || two == "~*" ||
                    two == "!~" || two == "@@" || two == "&&" || two == "<<" ||
                    two == ">>" || two == "=>") {
                    tokens.push_back(two);
                    ++i;
                    continue;
                }
                if (c == '-' && next == '-') {
                    // skip comment to end of line
                    while (i < sql.size() && sql[i] != '\n') ++i;
                    continue;
                }
                if (c == '/' && next == '*') {
                    // skip C-style comment
                    i += 2;
                    while (i + 1 < sql.size() && !(sql[i] == '*' && sql[i + 1] == '/')) ++i;
                    ++i; // skip '/'
                    continue;
                }
            }
            tokens.push_back(std::string(1, c));
            continue;
        }
        cur += c;
    }
    if (!cur.empty()) {
        tokens.push_back(cur);
    }
    return tokens;
}

bool SQLParser::match(const std::vector<std::string>& tokens, size_t pos, const std::string& word) {
    if (pos >= tokens.size()) return false;
    return toLower(tokens[pos]) == toLower(word);
}

bool SQLParser::matchAny(const std::vector<std::string>& tokens, size_t pos,
                         const std::vector<std::string>& words) {
    for (const auto& w : words) {
        if (match(tokens, pos, w)) return true;
    }
    return false;
}

bool SQLParser::isKeyword(const std::string& s) {
    static const std::set<std::string> keywords = {
        "select", "insert", "update", "delete", "merge", "values",
        "create", "drop", "alter", "truncate", "rename",
        "begin", "start", "commit", "rollback", "abort", "end",
        "savepoint", "release", "prepare", "transaction",
        "grant", "revoke", "deny",
        "set", "show", "reset", "use", "discard",
        "explain", "analyze", "vacuum", "checkpoint", "reindex", "cluster",
        "copy", "comment", "security", "label", "lock",
        "listen", "notify", "unlisten",
        "declare", "fetch", "move", "close",
        "prepare", "execute", "deallocate",
        "call", "do", "import",
        "table", "index", "view", "database", "schema", "sequence",
        "domain", "type", "function", "procedure", "trigger", "role",
        "user", "tablespace", "statistics", "policy", "rule",
        "extension", "publication", "subscription",
        "foreign", "server", "mapping", "cast", "collation", "conversion",
        "operator", "class", "family", "aggregate", "transform",
        "language", "access", "method", "text", "search",
        "configuration", "dictionary", "parser", "template",
        "materialized", "owned", "large", "object",
        "and", "or", "not", "null", "true", "false",
        "where", "from", "join", "on", "using", "as",
        "group", "by", "having", "order", "limit", "offset",
        "distinct", "all", "union", "intersect", "except",
        "inner", "left", "right", "full", "cross", "outer", "natural",
        "asc", "desc", "first", "last", "with", "recursive",
        "case", "when", "then", "else", "end",
        "exists", "in", "between", "like", "ilike", "similar", "to",
        "is", "nulls", "over", "partition", "range", "rows", "groups",
        "current", "session", "local", "time", "zone",
        "returning", "conflict", "nothing", "update", "do",
        "into", "outfile", "infile",
        "if", "exists", "cascade", "restrict", "restrictive",
        "concurrently", "replace", "or", "only", "including", "excluding",
        "inherits", "partition", "by", "of", "without", "oids",
        "default", "generated", "always", "identity", "serial",
        "primary", "key", "unique", "foreign", "references", "check",
        "exclude", "constraint", "deferrable", "initially", "immediate",
        "not", "no", "inherit", "force", "for", "row", "level", "security",
        "enable", "disable", "replica", "identity", "always", "full",
        "replica", "nothing", "default", "user", "system",
    };
    return keywords.count(toLower(s)) > 0;
}

// ============================================================================
// classify：快速命令分类（替代 execute() 中的字符串前缀匹配）
// ============================================================================

SqlCommand SQLParser::classify(const std::string& sql) {
    std::string lsql = toLower(trim(sql));
    if (lsql.empty()) return SqlCommand::Unknown;

    // Remove trailing semicolon
    while (!lsql.empty() && lsql.back() == ';') lsql.pop_back();

    // DQL
    if (lsql.substr(0, 6) == "select") return SqlCommand::Select;
    if (lsql.substr(0, 6) == "values") return SqlCommand::Values;

    // DML
    if (lsql.substr(0, 6) == "insert") return SqlCommand::Insert;
    if (lsql.substr(0, 6) == "update") return SqlCommand::Update;
    if (lsql.substr(0, 6) == "delete") return SqlCommand::Delete;
    if (lsql.substr(0, 5) == "merge") return SqlCommand::Merge;
    if (lsql.substr(0, 4) == "copy") return SqlCommand::Copy;
    if (lsql.substr(0, 4) == "call") return SqlCommand::Call;
    if (lsql.substr(0, 2) == "do") return SqlCommand::Do;

    // DDL — CREATE
    if (lsql.substr(0, 6) == "create") {
        size_t pos = 6;
        while (pos < lsql.size() && std::isspace(static_cast<unsigned char>(lsql[pos]))) ++pos;
        // Skip OR REPLACE
        if (lsql.substr(pos, 10) == "or replace") {
            pos += 10;
            while (pos < lsql.size() && std::isspace(static_cast<unsigned char>(lsql[pos]))) ++pos;
        }
        std::string rest = lsql.substr(pos);
        if (rest.substr(0, 9) == "database ") return SqlCommand::CreateDatabase;
        if (rest.substr(0, 7) == "schema ") return SqlCommand::CreateSchema;
        if (rest.substr(0, 12) == "tablespace ") return SqlCommand::CreateTablespace;
        if (rest.substr(0, 9) == "sequence ") return SqlCommand::CreateSequence;
        if (rest.substr(0, 7) == "domain ") return SqlCommand::CreateDomain;
        if (rest.substr(0, 5) == "type ") return SqlCommand::CreateType;
        if (rest.substr(0, 13) == "materialized ") return SqlCommand::CreateMaterializedView;
        if (rest.substr(0, 5) == "view ") return SqlCommand::CreateView;
        if (rest.substr(0, 9) == "function ") return SqlCommand::CreateFunction;
        if (rest.substr(0, 10) == "procedure ") return SqlCommand::CreateProcedure;
        if (rest.substr(0, 8) == "trigger ") return SqlCommand::CreateTrigger;
        if (rest.substr(0, 5) == "rule ") return SqlCommand::CreateRule;
        if (rest.substr(0, 6) == "event ") return SqlCommand::CreateEventTrigger;
        if (rest.substr(0, 5) == "role ") return SqlCommand::CreateRole;
        if (rest.substr(0, 5) == "user ") return SqlCommand::CreateUser;
        if (rest.substr(0, 6) == "group ") return SqlCommand::CreateRole; // legacy
        if (rest.substr(0, 11) == "statistics ") return SqlCommand::CreateStatistics;
        if (rest.substr(0, 7) == "policy ") return SqlCommand::CreatePolicy;
        if (rest.substr(0, 10) == "extension ") return SqlCommand::CreateExtension;
        if (rest.substr(0, 13) == "publication ") return SqlCommand::CreatePublication;
        if (rest.substr(0, 13) == "subscription ") return SqlCommand::CreateSubscription;
        if (rest.substr(0, 7) == "access ") return SqlCommand::CreateAccessMethod;
        if (rest.substr(0, 7) == "foreign") {
            if (rest.substr(8, 5) == "data ") return SqlCommand::CreateForeignDataWrapper;
            if (rest.substr(8, 6) == "table ") return SqlCommand::CreateForeignTable;
            if (rest.substr(8, 6) == "server") return SqlCommand::CreateServer; // 'foreign server'
        }
        if (rest.substr(0, 14) == "user mapping ") return SqlCommand::CreateUserMapping;
        if (rest.substr(0, 5) == "cast ") return SqlCommand::CreateCast;
        if (rest.substr(0, 10) == "collation ") return SqlCommand::CreateCollation;
        if (rest.substr(0, 11) == "conversion ") return SqlCommand::CreateConversion;
        if (rest.substr(0, 9) == "operator ") return SqlCommand::CreateOperator;
        if (rest.substr(0, 14) == "operator class") return SqlCommand::CreateOperatorClass;
        if (rest.substr(0, 15) == "operator family") return SqlCommand::CreateOperatorFamily;
        if (rest.substr(0, 11) == "aggregate ") return SqlCommand::CreateAggregate;
        if (rest.substr(0, 10) == "transform ") return SqlCommand::CreateTransform;
        if (rest.substr(0, 9) == "language ") return SqlCommand::CreateLanguage;
        if (rest.substr(0, 5) == "text ") {
            if (rest.substr(5, 7) == "search ") {
                std::string ts = rest.substr(12);
                if (ts.substr(0, 14) == "configuration ") return SqlCommand::CreateTextSearchConfiguration;
                if (ts.substr(0, 11) == "dictionary ") return SqlCommand::CreateTextSearchDictionary;
                if (ts.substr(0, 7) == "parser ") return SqlCommand::CreateTextSearchParser;
                if (ts.substr(0, 9) == "template ") return SqlCommand::CreateTextSearchTemplate;
            }
        }
        // CREATE TABLE must come after more specific patterns
        if (rest.substr(0, 6) == "table ") return SqlCommand::CreateTable;
        if (rest.substr(0, 6) == "index ") return SqlCommand::CreateIndex;
        return SqlCommand::CreateTable; // fallback
    }

    // DDL — DROP
    if (lsql.substr(0, 4) == "drop") {
        size_t pos = 4;
        while (pos < lsql.size() && std::isspace(static_cast<unsigned char>(lsql[pos]))) ++pos;
        std::string rest = lsql.substr(pos);
        if (rest.substr(0, 9) == "database ") return SqlCommand::DropDatabase;
        if (rest.substr(0, 7) == "schema ") return SqlCommand::DropSchema;
        if (rest.substr(0, 12) == "tablespace ") return SqlCommand::DropTablespace;
        if (rest.substr(0, 9) == "sequence ") return SqlCommand::DropSequence;
        if (rest.substr(0, 7) == "domain ") return SqlCommand::DropDomain;
        if (rest.substr(0, 5) == "type ") return SqlCommand::DropType;
        if (rest.substr(0, 13) == "materialized ") return SqlCommand::DropMaterializedView;
        if (rest.substr(0, 5) == "view ") return SqlCommand::DropView;
        if (rest.substr(0, 9) == "function ") return SqlCommand::DropFunction;
        if (rest.substr(0, 10) == "procedure ") return SqlCommand::DropProcedure;
        if (rest.substr(0, 8) == "routine ") return SqlCommand::DropRoutine;
        if (rest.substr(0, 8) == "trigger ") return SqlCommand::DropTrigger;
        if (rest.substr(0, 5) == "rule ") return SqlCommand::DropRule;
        if (rest.substr(0, 6) == "event ") return SqlCommand::DropEventTrigger;
        if (rest.substr(0, 5) == "role ") return SqlCommand::DropRole;
        if (rest.substr(0, 5) == "user ") return SqlCommand::DropUser;
        if (rest.substr(0, 6) == "group ") return SqlCommand::DropRole; // legacy
        if (rest.substr(0, 11) == "statistics ") return SqlCommand::DropStatistics;
        if (rest.substr(0, 7) == "policy ") return SqlCommand::DropPolicy;
        if (rest.substr(0, 10) == "extension ") return SqlCommand::DropExtension;
        if (rest.substr(0, 13) == "publication ") return SqlCommand::DropPublication;
        if (rest.substr(0, 13) == "subscription ") return SqlCommand::DropSubscription;
        if (rest.substr(0, 7) == "access ") return SqlCommand::DropAccessMethod;
        if (rest.substr(0, 7) == "foreign") {
            if (rest.substr(8, 5) == "data ") return SqlCommand::DropForeignDataWrapper;
            if (rest.substr(8, 6) == "table ") return SqlCommand::DropForeignTable;
            if (rest.substr(8, 6) == "server") return SqlCommand::DropServer;
        }
        if (rest.substr(0, 14) == "user mapping ") return SqlCommand::DropUserMapping;
        if (rest.substr(0, 5) == "cast ") return SqlCommand::DropCast;
        if (rest.substr(0, 10) == "collation ") return SqlCommand::DropCollation;
        if (rest.substr(0, 11) == "conversion ") return SqlCommand::DropConversion;
        if (rest.substr(0, 9) == "operator ") return SqlCommand::DropOperator;
        if (rest.substr(0, 14) == "operator class") return SqlCommand::DropOperatorClass;
        if (rest.substr(0, 15) == "operator family") return SqlCommand::DropOperatorFamily;
        if (rest.substr(0, 11) == "aggregate ") return SqlCommand::DropAggregate;
        if (rest.substr(0, 10) == "transform ") return SqlCommand::DropTransform;
        if (rest.substr(0, 9) == "language ") return SqlCommand::DropLanguage;
        if (rest.substr(0, 5) == "text ") {
            if (rest.substr(5, 7) == "search ") {
                std::string ts = rest.substr(12);
                if (ts.substr(0, 14) == "configuration ") return SqlCommand::DropTextSearchConfiguration;
                if (ts.substr(0, 11) == "dictionary ") return SqlCommand::DropTextSearchDictionary;
                if (ts.substr(0, 7) == "parser ") return SqlCommand::DropTextSearchParser;
                if (ts.substr(0, 9) == "template ") return SqlCommand::DropTextSearchTemplate;
            }
        }
        if (rest.substr(0, 6) == "owned ") return SqlCommand::DropOwned;
        if (rest.substr(0, 6) == "large ") return SqlCommand::DropLargeObject;
        if (rest.substr(0, 6) == "table ") return SqlCommand::DropTable;
        if (rest.substr(0, 6) == "index ") return SqlCommand::DropIndex;
        return SqlCommand::DropTable;
    }

    // DDL — ALTER
    if (lsql.substr(0, 5) == "alter") {
        size_t pos = 5;
        while (pos < lsql.size() && std::isspace(static_cast<unsigned char>(lsql[pos]))) ++pos;
        std::string rest = lsql.substr(pos);
        if (rest.substr(0, 9) == "database ") return SqlCommand::AlterDatabase;
        if (rest.substr(0, 7) == "schema ") return SqlCommand::AlterSchema;
        if (rest.substr(0, 12) == "tablespace ") return SqlCommand::AlterTablespace;
        if (rest.substr(0, 9) == "sequence ") return SqlCommand::AlterSequence;
        if (rest.substr(0, 7) == "domain ") return SqlCommand::AlterDomain;
        if (rest.substr(0, 5) == "type ") return SqlCommand::AlterType;
        if (rest.substr(0, 13) == "materialized ") return SqlCommand::AlterMaterializedView;
        if (rest.substr(0, 5) == "view ") return SqlCommand::AlterView;
        if (rest.substr(0, 9) == "function ") return SqlCommand::AlterFunction;
        if (rest.substr(0, 10) == "procedure ") return SqlCommand::AlterProcedure;
        if (rest.substr(0, 8) == "routine ") return SqlCommand::AlterRoutine;
        if (rest.substr(0, 8) == "trigger ") return SqlCommand::AlterTrigger;
        if (rest.substr(0, 5) == "rule ") return SqlCommand::AlterRule;
        if (rest.substr(0, 6) == "event ") return SqlCommand::AlterEventTrigger;
        if (rest.substr(0, 5) == "role ") return SqlCommand::AlterRole;
        if (rest.substr(0, 5) == "user ") return SqlCommand::AlterUser;
        if (rest.substr(0, 11) == "statistics ") return SqlCommand::AlterStatistics;
        if (rest.substr(0, 7) == "policy ") return SqlCommand::AlterPolicy;
        if (rest.substr(0, 10) == "extension ") return SqlCommand::AlterExtension;
        if (rest.substr(0, 13) == "publication ") return SqlCommand::AlterPublication;
        if (rest.substr(0, 13) == "subscription ") return SqlCommand::AlterSubscription;
        if (rest.substr(0, 8) == "default ") return SqlCommand::AlterDefaultPrivileges;
        if (rest.substr(0, 7) == "system ") return SqlCommand::AlterSystem;
        if (rest.substr(0, 7) == "foreign") {
            if (rest.substr(8, 5) == "data ") return SqlCommand::AlterForeignDataWrapper;
            if (rest.substr(8, 6) == "table ") return SqlCommand::AlterForeignTable;
            if (rest.substr(8, 6) == "server") return SqlCommand::AlterServer;
        }
        if (rest.substr(0, 14) == "user mapping ") return SqlCommand::AlterUserMapping;
        if (rest.substr(0, 5) == "text ") {
            if (rest.substr(5, 7) == "search ") {
                std::string ts = rest.substr(12);
                if (ts.substr(0, 14) == "configuration ") return SqlCommand::AlterTextSearchConfiguration;
                if (ts.substr(0, 11) == "dictionary ") return SqlCommand::AlterTextSearchDictionary;
                if (ts.substr(0, 7) == "parser ") return SqlCommand::AlterTextSearchParser;
                if (ts.substr(0, 9) == "template ") return SqlCommand::AlterTextSearchTemplate;
            }
        }
        if (rest.substr(0, 10) == "collation ") return SqlCommand::AlterCollation;
        if (rest.substr(0, 11) == "conversion ") return SqlCommand::AlterConversion;
        if (rest.substr(0, 9) == "operator ") return SqlCommand::AlterOperator;
        if (rest.substr(0, 14) == "operator class") return SqlCommand::AlterOperatorClass;
        if (rest.substr(0, 15) == "operator family") return SqlCommand::AlterOperatorFamily;
        if (rest.substr(0, 11) == "aggregate ") return SqlCommand::AlterAggregate;
        if (rest.substr(0, 9) == "language ") return SqlCommand::AlterLanguage;
        if (rest.substr(0, 6) == "large ") return SqlCommand::AlterLargeObject;
        if (rest.substr(0, 6) == "table ") return SqlCommand::AlterTable;
        if (rest.substr(0, 6) == "index ") return SqlCommand::AlterIndex;
        return SqlCommand::AlterTable;
    }

    if (lsql.substr(0, 8) == "truncate") return SqlCommand::Truncate;

    // TCL
    if (lsql.substr(0, 5) == "begin") return SqlCommand::Begin;
    if (lsql.substr(0, 5) == "start" && lsql.find("transaction") != std::string::npos)
        return SqlCommand::StartTransaction;
    // Specific transaction forms must precede their generic prefixes.
    // Otherwise ROLLBACK TO/PREPARED and COMMIT PREPARED are unreachable.
    if (lsql.substr(0, 15) == "commit prepared") return SqlCommand::CommitPrepared;
    if (lsql.substr(0, 17) == "rollback prepared") return SqlCommand::RollbackPrepared;
    if (lsql.substr(0, 11) == "rollback to" &&
        (lsql.size() == 11 || std::isspace(static_cast<unsigned char>(lsql[11]))))
        return SqlCommand::RollbackToSavepoint;
    if (lsql.substr(0, 6) == "commit") return SqlCommand::Commit;
    if (lsql.substr(0, 8) == "rollback") return SqlCommand::Rollback;
    if (lsql.substr(0, 5) == "abort") return SqlCommand::Abort;
    if (lsql.substr(0, 3) == "end") return SqlCommand::End;
    if (lsql.substr(0, 9) == "savepoint") return SqlCommand::Savepoint;
    if (lsql.substr(0, 7) == "release") return SqlCommand::ReleaseSavepoint;
    if (lsql.substr(0, 17) == "prepare transaction") return SqlCommand::PrepareTransaction;

    // DCL
    if (lsql.substr(0, 5) == "grant") return SqlCommand::Grant;
    if (lsql.substr(0, 6) == "revoke") return SqlCommand::Revoke;

    // Session / GUC
    if (lsql.substr(0, 3) == "set") {
        size_t pos = 3;
        while (pos < lsql.size() && std::isspace(static_cast<unsigned char>(lsql[pos]))) ++pos;
        std::string rest = lsql.substr(pos);
        if (rest.substr(0, 5) == "role ") return SqlCommand::SetRole;
        if (rest.substr(0, 25) == "session authorization") return SqlCommand::SetSessionAuthorization;
        if (rest.substr(0, 12) == "constraints ") return SqlCommand::SetConstraints;
        if (rest.substr(0, 11) == "transaction") return SqlCommand::SetTransaction;
        if (rest.substr(0, 5) == "time ") return SqlCommand::Set; // set time zone
        if (rest.substr(0, 8) == "timezone") return SqlCommand::Set; // set timezone
        return SqlCommand::Set;
    }
    if (lsql.substr(0, 4) == "show") return SqlCommand::Show;
    if (lsql.substr(0, 5) == "reset") return SqlCommand::Reset;
    if (lsql.substr(0, 3) == "use") return SqlCommand::UseDatabase;
    if (lsql.substr(0, 7) == "discard") return SqlCommand::Discard;

    // Utility
    if (lsql.substr(0, 7) == "explain") return SqlCommand::Explain;
    if (lsql.substr(0, 7) == "analyze") return SqlCommand::Analyze;
    if (lsql.substr(0, 6) == "vacuum") return SqlCommand::Vacuum;
    if (lsql.substr(0, 10) == "checkpoint") return SqlCommand::Checkpoint;
    if (lsql.substr(0, 7) == "reindex") return SqlCommand::Reindex;
    if (lsql.substr(0, 7) == "cluster") return SqlCommand::Cluster;
    if (lsql.substr(0, 7) == "comment") return SqlCommand::Comment;
    if (lsql.substr(0, 8) == "security" && lsql.find("label") != std::string::npos)
        return SqlCommand::SecurityLabel;
    if (lsql.substr(0, 4) == "lock") return SqlCommand::Lock;

    // Listen / Notify
    if (lsql.substr(0, 6) == "listen") return SqlCommand::Listen;
    if (lsql.substr(0, 6) == "notify") return SqlCommand::Notify;
    if (lsql.substr(0, 8) == "unlisten") return SqlCommand::Unlisten;

    // Cursor
    if (lsql.substr(0, 7) == "declare") return SqlCommand::Declare;
    if (lsql.substr(0, 5) == "fetch") return SqlCommand::Fetch;
    if (lsql.substr(0, 4) == "move") return SqlCommand::Move;
    if (lsql.substr(0, 5) == "close") return SqlCommand::Close;

    // Prepared statement
    if (lsql.substr(0, 7) == "prepare") return SqlCommand::Prepare;
    if (lsql.substr(0, 7) == "execute") return SqlCommand::Execute;
    if (lsql.substr(0, 11) == "deallocate") return SqlCommand::Deallocate;

    // Import
    if (lsql.substr(0, 21) == "import foreign schema") return SqlCommand::ImportForeignSchema;

    // Non-PG syntax (Phase 11 清理)
    if (lsql.substr(0, 7) == "replace") return SqlCommand::ReplaceInto;
    if (lsql.substr(0, 8) == "load data") return SqlCommand::LoadDataInfile;
    if (lsql.substr(0, 4) == "desc") return SqlCommand::Desc;
    if (lsql.substr(0, 9) == "view table") return SqlCommand::ViewTable;
    if (lsql.substr(0, 12) == "view database") return SqlCommand::ViewDatabase;

    return SqlCommand::Unknown;
}

// ============================================================================
// parse：完整解析入口
// ============================================================================

ParseResult SQLParser::parse(const std::string& sql) {
    ParseResult result;
    result.originalSql = sql;

    std::string lsql = toLower(trim(sql));
    if (lsql.empty()) {
        result.error = "empty SQL statement";
        return result;
    }

    SqlCommand cmd = classify(sql);

    switch (cmd) {
        case SqlCommand::Select:
            return parseSelect(sql);
        case SqlCommand::Insert:
            return parseInsert(sql);
        case SqlCommand::Update:
            return parseUpdate(sql);
        case SqlCommand::Delete:
            return parseDelete(sql);
        case SqlCommand::Merge:
            return parseMerge(sql);
        case SqlCommand::Values:
            return parseValues(sql);

        case SqlCommand::CreateTable: case SqlCommand::CreateIndex:
        case SqlCommand::CreateView: case SqlCommand::CreateDatabase:
        case SqlCommand::CreateSchema: case SqlCommand::CreateSequence:
        case SqlCommand::CreateDomain: case SqlCommand::CreateType:
        case SqlCommand::CreateFunction: case SqlCommand::CreateProcedure:
        case SqlCommand::CreateTrigger: case SqlCommand::CreateRole:
        case SqlCommand::CreateUser: case SqlCommand::CreateTablespace:
        case SqlCommand::CreateStatistics: case SqlCommand::CreatePolicy:
        case SqlCommand::CreateRule: case SqlCommand::CreateEventTrigger:
        case SqlCommand::CreateExtension: case SqlCommand::CreatePublication:
        case SqlCommand::CreateSubscription: case SqlCommand::CreateAccessMethod:
        case SqlCommand::CreateForeignDataWrapper: case SqlCommand::CreateForeignTable:
        case SqlCommand::CreateServer: case SqlCommand::CreateUserMapping:
        case SqlCommand::CreateCast: case SqlCommand::CreateCollation:
        case SqlCommand::CreateConversion: case SqlCommand::CreateOperator:
        case SqlCommand::CreateOperatorClass: case SqlCommand::CreateOperatorFamily:
        case SqlCommand::CreateAggregate: case SqlCommand::CreateTransform:
        case SqlCommand::CreateLanguage: case SqlCommand::CreateMaterializedView:
        case SqlCommand::CreateTextSearchConfiguration:
        case SqlCommand::CreateTextSearchDictionary:
        case SqlCommand::CreateTextSearchParser:
        case SqlCommand::CreateTextSearchTemplate:
            return parseCreate(sql);

        case SqlCommand::DropTable: case SqlCommand::DropIndex:
        case SqlCommand::DropView: case SqlCommand::DropMaterializedView:
        case SqlCommand::DropDatabase: case SqlCommand::DropSchema:
        case SqlCommand::DropSequence: case SqlCommand::DropDomain:
        case SqlCommand::DropType: case SqlCommand::DropFunction:
        case SqlCommand::DropProcedure: case SqlCommand::DropRoutine:
        case SqlCommand::DropTrigger: case SqlCommand::DropRole:
        case SqlCommand::DropUser: case SqlCommand::DropTablespace:
        case SqlCommand::DropStatistics: case SqlCommand::DropPolicy:
        case SqlCommand::DropRule: case SqlCommand::DropEventTrigger:
        case SqlCommand::DropExtension: case SqlCommand::DropPublication:
        case SqlCommand::DropSubscription: case SqlCommand::DropAccessMethod:
        case SqlCommand::DropForeignDataWrapper: case SqlCommand::DropForeignTable:
        case SqlCommand::DropServer: case SqlCommand::DropUserMapping:
        case SqlCommand::DropCast: case SqlCommand::DropCollation:
        case SqlCommand::DropConversion: case SqlCommand::DropOperator:
        case SqlCommand::DropOperatorClass: case SqlCommand::DropOperatorFamily:
        case SqlCommand::DropAggregate: case SqlCommand::DropTransform:
        case SqlCommand::DropLanguage: case SqlCommand::DropTextSearchConfiguration:
        case SqlCommand::DropTextSearchDictionary: case SqlCommand::DropTextSearchParser:
        case SqlCommand::DropTextSearchTemplate: case SqlCommand::DropOwned:
        case SqlCommand::DropLargeObject:
            return parseDrop(sql);

        case SqlCommand::AlterTable: case SqlCommand::AlterIndex:
        case SqlCommand::AlterView: case SqlCommand::AlterMaterializedView:
        case SqlCommand::AlterDatabase: case SqlCommand::AlterSchema:
        case SqlCommand::AlterSequence: case SqlCommand::AlterDomain:
        case SqlCommand::AlterType: case SqlCommand::AlterFunction:
        case SqlCommand::AlterProcedure: case SqlCommand::AlterRoutine:
        case SqlCommand::AlterTrigger: case SqlCommand::AlterRole:
        case SqlCommand::AlterUser: case SqlCommand::AlterSystem:
        case SqlCommand::AlterTablespace: case SqlCommand::AlterStatistics:
        case SqlCommand::AlterPolicy: case SqlCommand::AlterRule:
        case SqlCommand::AlterEventTrigger: case SqlCommand::AlterExtension:
        case SqlCommand::AlterPublication: case SqlCommand::AlterSubscription:
        case SqlCommand::AlterDefaultPrivileges: case SqlCommand::AlterForeignDataWrapper:
        case SqlCommand::AlterForeignTable: case SqlCommand::AlterServer:
        case SqlCommand::AlterUserMapping: case SqlCommand::AlterTextSearchConfiguration:
        case SqlCommand::AlterTextSearchDictionary: case SqlCommand::AlterTextSearchParser:
        case SqlCommand::AlterTextSearchTemplate: case SqlCommand::AlterCollation:
        case SqlCommand::AlterConversion: case SqlCommand::AlterOperator:
        case SqlCommand::AlterOperatorClass: case SqlCommand::AlterOperatorFamily:
        case SqlCommand::AlterAggregate: case SqlCommand::AlterLanguage:
        case SqlCommand::AlterLargeObject:
            return parseAlter(sql);

        case SqlCommand::Truncate:
            return parseTruncate(sql);

        case SqlCommand::Begin: case SqlCommand::StartTransaction:
            return parseBegin(sql);
        case SqlCommand::Commit: case SqlCommand::CommitPrepared:
            return parseCommit(sql);
        case SqlCommand::Rollback: case SqlCommand::Abort: case SqlCommand::End:
        case SqlCommand::RollbackToSavepoint: case SqlCommand::RollbackPrepared:
            return parseRollback(sql);
        case SqlCommand::Savepoint:
            return parseSavepoint(sql);
        case SqlCommand::ReleaseSavepoint:
            return parseRelease(sql);

        case SqlCommand::Set: case SqlCommand::SetRole:
        case SqlCommand::SetSessionAuthorization: case SqlCommand::SetConstraints:
        case SqlCommand::SetTransaction:
            return parseSet(sql);
        case SqlCommand::Show:
            return parseShow(sql);
        case SqlCommand::Reset:
            return parseReset(sql);
        case SqlCommand::Discard:
            return parseDiscard(sql);

        case SqlCommand::Explain:
            return parseExplain(sql);
        case SqlCommand::Analyze:
            return parseAnalyze(sql);
        case SqlCommand::Vacuum:
            return parseVacuum(sql);
        case SqlCommand::Checkpoint:
            return parseCheckpoint(sql);
        case SqlCommand::Reindex:
            return parseReindex(sql);
        case SqlCommand::Cluster:
            return parseCluster(sql);

        case SqlCommand::Copy:
            return parseCopy(sql);
        case SqlCommand::Comment:
            return parseComment(sql);
        case SqlCommand::SecurityLabel:
            return parseSecurityLabel(sql);
        case SqlCommand::Lock:
            return parseLock(sql);

        case SqlCommand::Listen:
            return parseListen(sql);
        case SqlCommand::Notify:
            return parseNotify(sql);
        case SqlCommand::Unlisten:
            return parseUnlisten(sql);

        case SqlCommand::Declare:
            return parseDeclare(sql);
        case SqlCommand::Fetch:
            return parseFetch(sql);
        case SqlCommand::Move:
            return parseMove(sql);
        case SqlCommand::Close:
            return parseClose(sql);

        case SqlCommand::Prepare:
            return parsePrepare(sql);
        case SqlCommand::Execute:
            return parseExecute(sql);
        case SqlCommand::Deallocate:
            return parseDeallocate(sql);

        case SqlCommand::Grant:
            return parseGrant(sql);
        case SqlCommand::Revoke:
            return parseRevoke(sql);

        case SqlCommand::Call:
            return parseCall(sql);
        case SqlCommand::Do:
            return parseDo(sql);
        case SqlCommand::ImportForeignSchema:
            return parseImportForeignSchema(sql);

        case SqlCommand::UseDatabase:
            return parseUse(sql);

        case SqlCommand::ReplaceInto:
        case SqlCommand::LoadDataInfile:
        case SqlCommand::Desc:
        case SqlCommand::ViewTable:
        case SqlCommand::ViewDatabase:
            result.error = "Non-PostgreSQL syntax: " + lsql.substr(0, 30);
            result.stmt = std::make_unique<Stmt>(cmd);
            return result;

        default:
            result.error = "unknown or unsupported SQL command";
            return result;
    }
}

// ============================================================================
// 各命令类型的解析实现（Phase 1 简化版：先分类，后续逐步完善参数解析）
// ============================================================================

// ============================================================================
// 表达式解析辅助函数
// ============================================================================

static ExprPtr parseSimpleExpr(const std::vector<std::string>& tokens, size_t& pos);

// ============================================================================
// Operator precedence expression parser (PostgreSQL-compatible)
// ============================================================================

static ExprPtr parseExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseOrExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseAndExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseNotExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseIsExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseComparisonExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseRangeExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseConcatExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseAddSubExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseMulDivModExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parsePowerExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseUnaryExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseCastExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parsePostfixExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parsePrimaryExpr(const std::vector<std::string>& tokens, size_t& pos);

// Entry point
static ExprPtr parseExpr(const std::vector<std::string>& tokens, size_t& pos) {
    return parseOrExpr(tokens, pos);
}

// OR (lowest precedence, left-associative)
static ExprPtr parseOrExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parseAndExpr(tokens, pos);
    while (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "or") {
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "OR";
        bin->left = std::move(left);
        bin->right = parseAndExpr(tokens, pos);
        left = std::move(bin);
    }
    return left;
}

// AND (left-associative)
static ExprPtr parseAndExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parseNotExpr(tokens, pos);
    while (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "and") {
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "AND";
        bin->left = std::move(left);
        bin->right = parseNotExpr(tokens, pos);
        left = std::move(bin);
    }
    return left;
}

// NOT (right-associative unary)
static ExprPtr parseNotExpr(const std::vector<std::string>& tokens, size_t& pos) {
    if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "not") {
        ++pos;
        auto unary = std::make_unique<UnaryOpExpr>();
        unary->op = "NOT";
        unary->operand = parseNotExpr(tokens, pos);
        return unary;
    }
    return parseIsExpr(tokens, pos);
}

// IS [NOT] (NULL | TRUE | FALSE | UNKNOWN | DOCUMENT | DISTINCT FROM ... | OF ...)
static ExprPtr parseIsExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parseComparisonExpr(tokens, pos);
    if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "is") {
        ++pos;
        std::string op = "IS";
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "not") {
            op = "IS NOT";
            ++pos;
        }
        if (pos < tokens.size()) {
            std::string pred = SQLParser::toLower(tokens[pos]);
            if (pred == "distinct") {
                ++pos;
                if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "from") {
                    ++pos;
                    auto right = parseComparisonExpr(tokens, pos);
                    auto bin = std::make_unique<BinaryOpExpr>();
                    bin->op = op + " DISTINCT FROM";
                    bin->left = std::move(left);
                    bin->right = std::move(right);
                    return bin;
                }
            } else if (pred == "of") {
                ++pos;
                std::string types;
                if (pos < tokens.size() && tokens[pos] == "(") {
                    ++pos;
                    while (pos < tokens.size() && tokens[pos] != ")") {
                        if (!types.empty()) types += " ";
                        types += tokens[pos++];
                    }
                    if (pos < tokens.size() && tokens[pos] == ")") ++pos;
                }
                auto unary = std::make_unique<UnaryOpExpr>();
                unary->op = op + " OF (" + types + ")";
                unary->operand = std::move(left);
                return unary;
            } else if (pred == "null" || pred == "true" || pred == "false"
                       || pred == "unknown" || pred == "document") {
                ++pos;
                auto unary = std::make_unique<UnaryOpExpr>();
                static const std::map<std::string, std::string> kIsPredUpper = {
                    {"null", "NULL"}, {"true", "TRUE"}, {"false", "FALSE"},
                    {"unknown", "UNKNOWN"}, {"document", "DOCUMENT"}
                };
                auto it = kIsPredUpper.find(pred);
                unary->op = op + " " + (it != kIsPredUpper.end() ? it->second : pred);
                unary->operand = std::move(left);
                return unary;
            } else {
                // Unknown predicate, consume it as-is
                ++pos;
                auto unary = std::make_unique<UnaryOpExpr>();
                unary->op = op + " " + pred;
                unary->operand = std::move(left);
                return unary;
            }
        }
        auto unary = std::make_unique<UnaryOpExpr>();
        unary->op = op;
        unary->operand = std::move(left);
        return unary;
    }
    return left;
}

// Comparison: =, <>, !=, <, >, <=, >=
static ExprPtr parseComparisonExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parseRangeExpr(tokens, pos);
    while (pos < tokens.size()) {
        std::string op = tokens[pos];
        static const std::set<std::string> cmpOps = {
            "=", "<>", "!=", "<", ">", "<=", ">="
        };
        if (cmpOps.count(op) == 0) break;
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = op;
        bin->left = std::move(left);
        bin->right = parseRangeExpr(tokens, pos);
        left = std::move(bin);
    }
    return left;
}

// BETWEEN, IN, LIKE, ILIKE, SIMILAR TO
static ExprPtr parseRangeExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parseConcatExpr(tokens, pos);
    if (pos >= tokens.size()) return left;

    std::string w = SQLParser::toLower(tokens[pos]);

    if (w == "between") {
        ++pos;
        auto lower = parseConcatExpr(tokens, pos);
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "and") ++pos;
        auto upper = parseConcatExpr(tokens, pos);
        auto betweenExpr = std::make_unique<FunctionCallExpr>();
        betweenExpr->funcName = "BETWEEN";
        betweenExpr->args.push_back(std::move(left));
        betweenExpr->args.push_back(std::move(lower));
        betweenExpr->args.push_back(std::move(upper));
        return betweenExpr;
    }

    if (w == "in") {
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "IN";
        bin->left = std::move(left);
        if (pos < tokens.size() && tokens[pos] == "(") {
            ++pos;
            auto list = std::make_unique<LiteralExpr>();
            std::string val;
            while (pos < tokens.size() && tokens[pos] != ")") {
                if (!val.empty()) val += " ";
                val += tokens[pos++];
            }
            if (pos < tokens.size() && tokens[pos] == ")") ++pos;
            list->value = val;
            bin->right = std::move(list);
        } else {
            bin->right = parseConcatExpr(tokens, pos);
        }
        return bin;
    }

    if (w == "like" || w == "ilike") {
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = (w == "like") ? "LIKE" : "ILIKE";
        bin->left = std::move(left);
        bin->right = parseConcatExpr(tokens, pos);
        // ESCAPE
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "escape") {
            ++pos;
            auto esc = parseConcatExpr(tokens, pos);
            // For now, encode escape in a FunctionCallExpr wrapper
            auto wrap = std::make_unique<FunctionCallExpr>();
            wrap->funcName = bin->op + " ESCAPE";
            wrap->args.push_back(std::move(bin->left));
            wrap->args.push_back(std::move(bin->right));
            wrap->args.push_back(std::move(esc));
            return wrap;
        }
        return bin;
    }

    if (w == "similar") {
        ++pos;
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "to") ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "SIMILAR TO";
        bin->left = std::move(left);
        bin->right = parseConcatExpr(tokens, pos);
        return bin;
    }

    return left;
}

// || (concatenation, left-associative)
static ExprPtr parseConcatExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parseAddSubExpr(tokens, pos);
    while (pos < tokens.size() && tokens[pos] == "||") {
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "||";
        bin->left = std::move(left);
        bin->right = parseAddSubExpr(tokens, pos);
        left = std::move(bin);
    }
    return left;
}

// +, - (binary, left-associative)
static ExprPtr parseAddSubExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parseMulDivModExpr(tokens, pos);
    while (pos < tokens.size()) {
        std::string op = tokens[pos];
        if (op != "+" && op != "-") break;
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = op;
        bin->left = std::move(left);
        bin->right = parseMulDivModExpr(tokens, pos);
        left = std::move(bin);
    }
    return left;
}

// *, /, % (left-associative)
static ExprPtr parseMulDivModExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parsePowerExpr(tokens, pos);
    while (pos < tokens.size()) {
        std::string op = tokens[pos];
        if (op != "*" && op != "/" && op != "%") break;
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = op;
        bin->left = std::move(left);
        bin->right = parsePowerExpr(tokens, pos);
        left = std::move(bin);
    }
    return left;
}

// ^ (power, left-associative in PG)
static ExprPtr parsePowerExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parseUnaryExpr(tokens, pos);
    while (pos < tokens.size() && tokens[pos] == "^") {
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "^";
        bin->left = std::move(left);
        bin->right = parseUnaryExpr(tokens, pos);
        left = std::move(bin);
    }
    return left;
}

// Unary +, -, NOT
static ExprPtr parseUnaryExpr(const std::vector<std::string>& tokens, size_t& pos) {
    if (pos < tokens.size() && (tokens[pos] == "+" || tokens[pos] == "-")) {
        std::string op = tokens[pos];
        ++pos;
        auto unary = std::make_unique<UnaryOpExpr>();
        unary->op = op;
        unary->operand = parseUnaryExpr(tokens, pos);
        return unary;
    }
    return parseCastExpr(tokens, pos);
}

// :: (cast, left-associative)
static ExprPtr parseCastExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parsePostfixExpr(tokens, pos);
    while (pos < tokens.size() && tokens[pos] == "::") {
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "::";
        bin->left = std::move(left);
        // Type name: read until next operator/terminator
        std::string typeName;
        while (pos < tokens.size()) {
            std::string w = SQLParser::toLower(tokens[pos]);
            if (w == "and" || w == "or" || w == "then" || w == "else" || w == "end"
                || w == "when" || w == "from" || w == "where" || w == "group"
                || w == "order" || w == "having" || w == "limit" || w == "offset"
                || w == "union" || w == "intersect" || w == "except" || w == "for"
                || w == "returning" || w == "on" || w == "using" || w == "set"
                || w == "into" || w == "values" || w == "by" || w == "asc" || w == "desc"
                || tokens[pos] == ")" || tokens[pos] == "," || tokens[pos] == ";"
                || tokens[pos] == "::" || tokens[pos] == "||"
                || tokens[pos] == "+" || tokens[pos] == "-"
                || tokens[pos] == "*" || tokens[pos] == "/" || tokens[pos] == "%"
                || tokens[pos] == "^" || tokens[pos] == "=" || tokens[pos] == "<"
                || tokens[pos] == ">" || tokens[pos] == "<=" || tokens[pos] == ">="
                || tokens[pos] == "<>" || tokens[pos] == "!=") {
                break;
            }
            if (tokens[pos] == "(") {
                ++pos;
                while (pos < tokens.size() && tokens[pos] != ")") {
                    if (!typeName.empty()) typeName += " ";
                    typeName += tokens[pos++];
                }
                if (pos < tokens.size() && tokens[pos] == ")") {
                    typeName += ")";
                    ++pos;
                }
                continue;
            }
            if (tokens[pos] == "[") {
                ++pos;
                if (pos < tokens.size() && tokens[pos] == "]") {
                    typeName += "[]";
                    ++pos;
                }
                continue;
            }
            if (!typeName.empty()) typeName += " ";
            typeName += tokens[pos++];
        }
        auto right = std::make_unique<LiteralExpr>();
        right->value = typeName;
        bin->right = std::move(right);
        left = std::move(bin);
    }
    return left;
}

// Postfix: array subscript [ ], IS NULL/NOT NULL (as postfix)
static ExprPtr parsePostfixExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parsePrimaryExpr(tokens, pos);

    // Array subscript: expr[expr] or expr[lower:upper]
    while (pos < tokens.size() && tokens[pos] == "[") {
        ++pos;
        auto idx = parseExpr(tokens, pos);
        if (pos < tokens.size() && tokens[pos] == "]") ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "[]";
        bin->left = std::move(left);
        bin->right = std::move(idx);
        left = std::move(bin);
    }

    // Postfix IS [NOT] NULL
    if (pos + 1 < tokens.size() && SQLParser::toLower(tokens[pos]) == "is") {
        if (SQLParser::toLower(tokens[pos + 1]) == "null") {
            pos += 2;
            auto unary = std::make_unique<UnaryOpExpr>();
            unary->op = "IS NULL";
            unary->operand = std::move(left);
            return unary;
        } else if (pos + 2 < tokens.size() && SQLParser::toLower(tokens[pos + 1]) == "not"
                   && SQLParser::toLower(tokens[pos + 2]) == "null") {
            pos += 3;
            auto unary = std::make_unique<UnaryOpExpr>();
            unary->op = "IS NOT NULL";
            unary->operand = std::move(left);
            return unary;
        }
    }

    return left;
}

// Primary: literals, column refs, function calls, parenthesized exprs, subqueries, CASE
static ExprPtr parsePrimaryExpr(const std::vector<std::string>& tokens, size_t& pos) {
    if (pos >= tokens.size()) return nullptr;

    // CASE expression
    if (SQLParser::toLower(tokens[pos]) == "case") {
        ++pos;
        auto caseExpr = std::make_unique<FunctionCallExpr>();
        caseExpr->funcName = "CASE";
        // Simple CASE: CASE expr WHEN ... END
        // Searched CASE: CASE WHEN ... END
        ExprPtr caseOperand;
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) != "when"
            && SQLParser::toLower(tokens[pos]) != "end") {
            caseOperand = parseExpr(tokens, pos);
        }
        if (caseOperand) caseExpr->args.push_back(std::move(caseOperand));

        while (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "when") {
            ++pos;
            auto whenExpr = parseExpr(tokens, pos);
            if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "then") ++pos;
            auto thenExpr = parseExpr(tokens, pos);
            if (whenExpr) caseExpr->args.push_back(std::move(whenExpr));
            if (thenExpr) caseExpr->args.push_back(std::move(thenExpr));
        }
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "else") {
            ++pos;
            auto elseExpr = parseExpr(tokens, pos);
            if (elseExpr) caseExpr->args.push_back(std::move(elseExpr));
        }
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "end") ++pos;
        return caseExpr;
    }

    // EXISTS (subquery)
    if (SQLParser::toLower(tokens[pos]) == "exists") {
        ++pos;
        if (pos < tokens.size() && tokens[pos] == "(") {
            ++pos;
            std::string subq;
            int depth = 1;
            while (pos < tokens.size() && depth > 0) {
                if (tokens[pos] == "(") ++depth;
                else if (tokens[pos] == ")") --depth;
                if (depth > 0) {
                    if (!subq.empty()) subq += " ";
                    subq += tokens[pos++];
                }
            }
            if (pos < tokens.size() && tokens[pos] == ")") ++pos;
            auto func = std::make_unique<FunctionCallExpr>();
            func->funcName = "EXISTS";
            auto lit = std::make_unique<LiteralExpr>();
            lit->value = "(" + subq + ")";
            func->args.push_back(std::move(lit));
            return func;
        }
        auto func = std::make_unique<FunctionCallExpr>();
        func->funcName = "EXISTS";
        return func;
    }

    // Parenthesized expression or subquery
    if (tokens[pos] == "(") {
        ++pos;
        // Check for subquery
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "select") {
            std::string subq;
            int depth = 1;
            while (pos < tokens.size() && depth > 0) {
                if (tokens[pos] == "(") ++depth;
                else if (tokens[pos] == ")") --depth;
                if (depth > 0) {
                    if (!subq.empty()) subq += " ";
                    subq += tokens[pos++];
                }
            }
            if (pos < tokens.size() && tokens[pos] == ")") ++pos;
            auto lit = std::make_unique<LiteralExpr>();
            lit->value = "(" + subq + ")";
            return lit;
        }
        auto inner = parseExpr(tokens, pos);
        if (pos < tokens.size() && tokens[pos] == ")") ++pos;
        return inner;
    }

    // Star
    if (tokens[pos] == "*") {
        ++pos;
        auto lit = std::make_unique<LiteralExpr>();
        lit->value = "*";
        return lit;
    }

    std::string first = tokens[pos];
    ++pos;

    // Literals: quoted strings, numbers, and boolean/null constants.
    if (isStringLiteralToken(first) || isNumericToken(first)) {
        auto lit = std::make_unique<LiteralExpr>();
        lit->value = first;
        return lit;
    }
    std::string firstLower = SQLParser::toLower(first);
    if (firstLower == "null" || firstLower == "true" || firstLower == "false") {
        auto lit = std::make_unique<LiteralExpr>();
        lit->value = firstLower;
        return lit;
    }

    // Collect possible qualified name parts before deciding function vs column.
    std::string second, third;
    bool hasSecond = false, hasThird = false;
    if (pos < tokens.size() && tokens[pos] == ".") {
        ++pos;
        if (pos < tokens.size()) { second = tokens[pos++]; hasSecond = true; }
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) { third = tokens[pos++]; hasThird = true; }
        }
    }

    // Function call: func_name( ... ) or schema.func( ... )
    if (pos < tokens.size() && tokens[pos] == "(") {
        ++pos;
        auto func = std::make_unique<FunctionCallExpr>();
        if (hasThird) {
            func->schema = first;
            func->funcName = third;
            (void)second;
        } else if (hasSecond) {
            func->schema = first;
            func->funcName = second;
        } else {
            func->funcName = first;
        }

        auto parseArg = [&](ExprPtr arg) {
            // Detect named argument: name => value
            if (arg && arg->type == ExprType::ColumnRef && pos + 1 < tokens.size() &&
                ((tokens[pos] == "=" && tokens[pos + 1] == ">") || tokens[pos] == "=>")) {
                FunctionCallExpr::NamedArg na;
                na.name = static_cast<ColumnRefExpr*>(arg.get())->column;
                if (tokens[pos] == "=>") {
                    ++pos;
                } else {
                    pos += 2; // skip '=' and '>'
                }
                na.value = parseExpr(tokens, pos);
                func->namedArgs.push_back(std::move(na));
            } else if (arg) {
                func->args.push_back(std::move(arg));
            }
        };

        while (pos < tokens.size() && tokens[pos] != ")") {
            if (SQLParser::toLower(tokens[pos]) == "distinct") {
                func->distinct = true;
                ++pos;
                continue;
            }
            // ORDER BY inside aggregate (e.g., ARRAY_AGG(x ORDER BY y))
            if (SQLParser::toLower(tokens[pos]) == "order") {
                ++pos;
                if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "by") ++pos;
                auto orderExpr = parseExpr(tokens, pos);
                bool asc = true;
                if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "asc") { asc = true; ++pos; }
                else if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "desc") { asc = false; ++pos; }
                if (orderExpr) {
                    auto orderLit = std::make_unique<LiteralExpr>();
                    orderLit->value = "ORDER BY " + orderExpr->toString() + (asc ? " ASC" : " DESC");
                    func->args.push_back(std::move(orderLit));
                }
                if (pos < tokens.size() && tokens[pos] == ",") { ++pos; continue; }
                continue;
            }
            auto arg = parseExpr(tokens, pos);
            parseArg(std::move(arg));
            if (pos < tokens.size() && tokens[pos] == ",") {
                ++pos;
                continue;
            }
        }
        if (pos < tokens.size() && tokens[pos] == ")") ++pos;

        // FILTER (WHERE ...)
        if (pos + 2 < tokens.size() && SQLParser::toLower(tokens[pos]) == "filter"
            && tokens[pos + 1] == "(") {
            pos += 2;
            if (SQLParser::toLower(tokens[pos]) == "where") ++pos;
            func->filter = parseExpr(tokens, pos);
            if (pos < tokens.size() && tokens[pos] == ")") ++pos;
        }

        // OVER (...) - window function
        if (pos + 1 < tokens.size() && SQLParser::toLower(tokens[pos]) == "over") {
            ++pos;
            WindowDef winDef;
            if (pos < tokens.size() && tokens[pos] == "(") {
                ++pos;
                while (pos < tokens.size() && tokens[pos] != ")") {
                    std::string w = SQLParser::toLower(tokens[pos]);
                    if (w == "partition") {
                        ++pos;
                        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "by") ++pos;
                        while (pos < tokens.size() && tokens[pos] != ")" &&
                               SQLParser::toLower(tokens[pos]) != "order" &&
                               SQLParser::toLower(tokens[pos]) != "rows" &&
                               SQLParser::toLower(tokens[pos]) != "range" &&
                               SQLParser::toLower(tokens[pos]) != "groups") {
                            auto part = parseExpr(tokens, pos);
                            if (part) winDef.partitionBy.push_back(std::move(part));
                            if (pos < tokens.size() && tokens[pos] == ",") ++pos;
                        }
                    } else if (w == "order") {
                        ++pos;
                        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "by") ++pos;
                        while (pos < tokens.size() && tokens[pos] != ")" &&
                               SQLParser::toLower(tokens[pos]) != "rows" &&
                               SQLParser::toLower(tokens[pos]) != "range" &&
                               SQLParser::toLower(tokens[pos]) != "groups") {
                            auto obExpr = parseExpr(tokens, pos);
                            bool asc = true;
                            if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "asc") { asc = true; ++pos; }
                            else if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "desc") { asc = false; ++pos; }
                            if (obExpr) winDef.orderBy.emplace_back(std::move(obExpr), asc);
                            if (pos < tokens.size() && tokens[pos] == ",") ++pos;
                        }
                    } else if (w == "rows" || w == "range" || w == "groups") {
                        winDef.frameMode = w; ++pos;
                        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "between") {
                            ++pos;
                            winDef.frameStart = parseExpr(tokens, pos);
                            if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "and") { ++pos; winDef.frameEnd = parseExpr(tokens, pos); }
                        } else {
                            winDef.frameStart = parseExpr(tokens, pos);
                        }
                        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "exclude") {
                            ++pos;
                            if (pos < tokens.size()) winDef.frameExclusion = SQLParser::toLower(tokens[pos++]);
                        }
                    } else {
                        ++pos;
                    }
                }
                if (pos < tokens.size() && tokens[pos] == ")") ++pos;
            } else if (pos < tokens.size()) {
                winDef.name = tokens[pos++]; // named window reference
            }
            // Attach window specification to the function call.
            func->hasOver = true;
            func->over = std::move(winDef);
        }

        return func;
    }

    // Column reference: schema.table.column, table.column, or just column
    std::string schemaName, tableName, colName = first;
    if (hasThird) {
        schemaName = first;
        tableName = second;
        colName = third;
    } else if (hasSecond) {
        tableName = first;
        colName = second;
    }

    auto colRef = std::make_unique<ColumnRefExpr>();
    colRef->schema = schemaName;
    colRef->table = tableName;
    colRef->column = colName;
    return colRef;
}

// Backward-compatible wrapper: delegates to full precedence parser
static ExprPtr parseSimpleExpr(const std::vector<std::string>& tokens, size_t& pos) {
    return parseExpr(tokens, pos);
}

// 解析单个 SELECT 项（expr [AS alias]）
static SelectItem parseSelectItem(const std::vector<std::string>& tokens, size_t& pos) {
    SelectItem item;
    item.expr = parseSimpleExpr(tokens, pos);
    if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "as") {
        ++pos;
        if (pos < tokens.size()) item.alias = tokens[pos++];
    } else if (pos < tokens.size() && !SQLParser::isKeyword(tokens[pos])
               && tokens[pos] != "," && tokens[pos] != "from"
               && tokens[pos] != ")") {
        // Implicit alias (no AS keyword)
        item.alias = tokens[pos++];
    }
    return item;
}

// 解析 FROM 项（简化版：支持表名、别名、JOIN）
static std::unique_ptr<FromItem> parseFromItem(const std::vector<std::string>& tokens, size_t& pos) {
    if (pos >= tokens.size()) return nullptr;

    auto item = std::make_unique<FromItem>();
    item->type = FromItem::Type::Table;
    item->tableName = tokens[pos++];

    // AS alias or implicit alias
    if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "as") {
        ++pos;
        if (pos < tokens.size()) item->alias = tokens[pos++];
    } else if (pos < tokens.size() && !SQLParser::isKeyword(tokens[pos])
               && tokens[pos] != "," && tokens[pos] != ")") {
        item->alias = tokens[pos++];
    }

    // JOIN handling (simplified)
    while (pos < tokens.size()) {
        std::string jkw = SQLParser::toLower(tokens[pos]);
        if (jkw == "join" || jkw == "inner" || jkw == "left" || jkw == "right"
            || jkw == "full" || jkw == "cross" || jkw == "natural") {
            std::string joinType = "INNER";
            if (jkw == "left") { joinType = "LEFT"; ++pos; if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "outer") ++pos; }
            else if (jkw == "right") { joinType = "RIGHT"; ++pos; if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "outer") ++pos; }
            else if (jkw == "full") { joinType = "FULL"; ++pos; if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "outer") ++pos; }
            else if (jkw == "cross") { joinType = "CROSS"; ++pos; }
            else if (jkw == "natural") { joinType = "NATURAL"; ++pos; if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "join") ++pos; }
            else if (jkw == "inner") { joinType = "INNER"; ++pos; }
            else if (jkw == "join") { ++pos; }

            if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "join") ++pos;

            auto rightItem = std::make_unique<FromItem>();
            rightItem->type = FromItem::Type::Table;
            if (pos < tokens.size()) rightItem->tableName = tokens[pos++];
            if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "as") {
                ++pos;
                if (pos < tokens.size()) rightItem->alias = tokens[pos++];
            } else if (pos < tokens.size() && !SQLParser::isKeyword(tokens[pos])
                       && tokens[pos] != "," && tokens[pos] != ")") {
                rightItem->alias = tokens[pos++];
            }

            auto joinNode = std::make_unique<FromItem>();
            joinNode->type = FromItem::Type::Join;
            joinNode->joinType = joinType;
            joinNode->left = std::move(item);
            joinNode->right = std::move(rightItem);

            if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "on") {
                ++pos;
                joinNode->joinCondition = parseSimpleExpr(tokens, pos);
            } else if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "using") {
                ++pos;
                if (pos < tokens.size() && tokens[pos] == "(") {
                    auto cols = collectParenthesized(tokens, pos);
                    for (const auto& c : cols) {
                        if (c != ",") joinNode->usingCols.push_back(c);
                    }
                }
            }
            item = std::move(joinNode);
        } else {
            break;
        }
    }

    return item;
}

ParseResult SQLParser::parseSelect(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 2) {
        r.error = "SELECT statement too short";
        return r;
    }

    // Split set operations before parsing SELECT clauses.  The previous
    // implementation parsed the first operator it encountered and treated
    // the entire remainder as its RHS, which made UNION/EXCEPT right-
    // associative and gave INTERSECT the wrong precedence in expressions
    // such as A INTERSECT B UNION C.
    SetOperatorLocation setLocation;
    if (findTopLevelSetOperator(tokens, setLocation)) {
        size_t rhsBegin = setLocation.position + 1;
        if (rhsBegin < tokens.size() &&
            (toLower(tokens[rhsBegin]) == "all" || toLower(tokens[rhsBegin]) == "distinct")) {
            ++rhsBegin;
        }
        if (setLocation.position == 0 || rhsBegin >= tokens.size()) {
            r.error = "set operation requires two SELECT operands";
            return r;
        }

        ParseResult left = parseSelect(joinParserTokens(tokens, 0, setLocation.position));
        if (!left.success || !left.stmt) {
            r.error = left.error.empty() ? "invalid left set-operation operand" : left.error;
            return r;
        }
        ParseResult right = parseSelect(joinParserTokens(tokens, rhsBegin, tokens.size()));
        if (!right.success || !right.stmt) {
            r.error = right.error.empty() ? "invalid right set-operation operand" : right.error;
            return r;
        }
        auto* leftSelect = dynamic_cast<SelectStmt*>(left.stmt.get());
        if (!leftSelect) {
            r.error = "set operation operand must be SELECT";
            return r;
        }
        if (leftSelect->setOp == SetOp::None && !leftSelect->setOpLhs) {
            leftSelect->setOp = setLocation.op;
            leftSelect->setOpAll = setLocation.all;
            leftSelect->setOpRhs = std::move(right.stmt);
            return left;
        }

        // A SelectStmt historically stores its first set operation inline.
        // Wrap an already-composed left operand so chains remain explicitly
        // left-associative without losing the existing AST API.
        auto wrapper = std::make_unique<SelectStmt>();
        wrapper->setOp = setLocation.op;
        wrapper->setOpAll = setLocation.all;
        wrapper->setOpLhs = std::move(left.stmt);
        wrapper->setOpRhs = std::move(right.stmt);
        r.success = true;
        r.stmt = std::move(wrapper);
        return r;
    }

    auto stmt = std::make_unique<SelectStmt>();
    size_t pos = 0;

    // WITH [RECURSIVE] cte_name [(cols)] AS [NOT] MATERIALIZED (query) [, ...]
    if (pos < tokens.size() && toLower(tokens[pos]) == "with") {
        ++pos;
        bool recursive = false;
        if (pos < tokens.size() && toLower(tokens[pos]) == "recursive") {
            recursive = true;
            ++pos;
        }
        while (pos < tokens.size()) {
            SelectStmt::CTE cte;
            cte.recursive = recursive;
            if (pos < tokens.size()) {
                cte.name = tokens[pos++];
            }
            if (pos < tokens.size() && tokens[pos] == "(") {
                auto cols = collectParenthesized(tokens, pos);
                for (const auto& c : cols) {
                    if (c != ",") cte.columnNames.push_back(c);
                }
            }
            if (pos < tokens.size() && toLower(tokens[pos]) == "as") ++pos;
            if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "not"
                && toLower(tokens[pos + 1]) == "materialized") {
                cte.materialized = false;
                pos += 2;
            } else if (pos < tokens.size() && toLower(tokens[pos]) == "materialized") {
                cte.materialized = true;
                ++pos;
            }
            if (pos < tokens.size() && tokens[pos] == "(") {
                ++pos;
                std::string subq;
                int depth = 1;
                while (pos < tokens.size() && depth > 0) {
                    if (tokens[pos] == "(") ++depth;
                    else if (tokens[pos] == ")") --depth;
                    if (depth > 0) {
                        if (!subq.empty()) subq += " ";
                        subq += tokens[pos++];
                    }
                }
                if (pos < tokens.size() && tokens[pos] == ")") ++pos;
                cte.query = parseSelect(subq).stmt;
            }
            stmt->ctes.push_back(std::move(cte));
            if (pos < tokens.size() && tokens[pos] == ",") {
                ++pos;
                continue;
            }
            break;
        }
    }

    // Skip SELECT
    if (pos < tokens.size() && toLower(tokens[pos]) == "select") {
        ++pos;
    }

    // DISTINCT / DISTINCT ON (...)
    if (pos < tokens.size() && toLower(tokens[pos]) == "distinct") {
        ++pos;
        if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "on" && tokens[pos + 1] == "(") {
            pos += 2;
            while (pos < tokens.size() && tokens[pos] != ")") {
                auto expr = parseSimpleExpr(tokens, pos);
                if (expr) stmt->distinctOn.push_back(std::move(expr));
                if (pos < tokens.size() && tokens[pos] == ",") ++pos;
            }
            if (pos < tokens.size() && tokens[pos] == ")") ++pos;
        } else {
            stmt->distinct = true;
        }
    } else if (pos < tokens.size() && toLower(tokens[pos]) == "all") {
        ++pos;
    }

    // Select list
    while (pos < tokens.size()) {
        if (toLower(tokens[pos]) == "from" || toLower(tokens[pos]) == "where"
            || toLower(tokens[pos]) == "group" || toLower(tokens[pos]) == "having"
            || toLower(tokens[pos]) == "order" || toLower(tokens[pos]) == "limit"
            || toLower(tokens[pos]) == "offset" || toLower(tokens[pos]) == "union"
            || toLower(tokens[pos]) == "intersect" || toLower(tokens[pos]) == "except"
            || toLower(tokens[pos]) == "for") {
            break;
        }
        stmt->selectList.push_back(parseSelectItem(tokens, pos));
        if (pos < tokens.size() && tokens[pos] == ",") {
            ++pos;
            continue;
        }
    }

    // FROM (supports comma join and explicit JOINs)
    if (pos < tokens.size() && toLower(tokens[pos]) == "from") {
        ++pos;
        auto firstItem = parseFromItem(tokens, pos);
        if (firstItem) {
            while (pos < tokens.size()) {
                std::string w = toLower(tokens[pos]);
                if (w == "where" || w == "group" || w == "having" || w == "order"
                    || w == "limit" || w == "offset" || w == "union"
                    || w == "intersect" || w == "except" || w == "for"
                    || w == ")" || w == ";") {
                    break;
                }
                auto joinItem = std::make_unique<FromItem>();
                joinItem->type = FromItem::Type::Join;
                joinItem->left = std::move(firstItem);
                if (tokens[pos] == ",") {
                    ++pos;
                    joinItem->joinType = "CROSS";
                } else if (w == "cross") {
                    ++pos;
                    if (pos < tokens.size() && toLower(tokens[pos]) == "join") ++pos;
                    joinItem->joinType = "CROSS";
                } else if (w == "natural") {
                    ++pos;
                    if (pos < tokens.size() && toLower(tokens[pos]) == "join") ++pos;
                    joinItem->joinType = "NATURAL";
                } else if (w == "inner") {
                    ++pos;
                    if (pos < tokens.size() && toLower(tokens[pos]) == "join") ++pos;
                    joinItem->joinType = "INNER";
                } else if (w == "join") {
                    ++pos;
                    joinItem->joinType = "INNER";
                } else if (w == "left" || w == "right" || w == "full") {
                    std::string jt = w;
                    ++pos;
                    if (pos < tokens.size() && toLower(tokens[pos]) == "outer") ++pos;
                    if (pos < tokens.size() && toLower(tokens[pos]) == "join") ++pos;
                    joinItem->joinType = jt;
                } else {
                    break;
                }
                joinItem->right = parseFromItem(tokens, pos);
                if (pos < tokens.size() && toLower(tokens[pos]) == "on") {
                    ++pos;
                    joinItem->joinCondition = parseExpr(tokens, pos);
                } else if (pos < tokens.size() && toLower(tokens[pos]) == "using") {
                    ++pos;
                    if (pos < tokens.size() && tokens[pos] == "(") {
                        auto cols = collectParenthesized(tokens, pos);
                        for (const auto& c : cols) {
                            if (c != ",") joinItem->usingCols.push_back(c);
                        }
                    }
                }
                firstItem = std::move(joinItem);
            }
            stmt->fromClause = std::move(firstItem);
        }
    }

    // WHERE
    if (pos < tokens.size() && toLower(tokens[pos]) == "where") {
        ++pos;
        stmt->whereClause = parseSimpleExpr(tokens, pos);
    }

    // GROUP BY (with ROLLUP / CUBE / GROUPING SETS support)
    if (pos < tokens.size() && toLower(tokens[pos]) == "group") {
        ++pos;
        if (pos < tokens.size() && toLower(tokens[pos]) == "by") ++pos;
        while (pos < tokens.size()) {
            std::string w = toLower(tokens[pos]);
            if (w == "having" || w == "order" || w == "limit" || w == "offset"
                || w == "union" || w == "intersect" || w == "except" || w == "for") break;

            SelectStmt::GroupByElem elem;
            if (w == "rollup") {
                elem.kind = SelectStmt::GroupByElem::Kind::Rollup; ++pos;
                if (pos < tokens.size() && tokens[pos] == "(") {
                    ++pos;
                    while (pos < tokens.size() && tokens[pos] != ")") {
                        auto expr = parseSimpleExpr(tokens, pos);
                        if (expr) elem.exprs.push_back(std::move(expr));
                        if (pos < tokens.size() && tokens[pos] == ",") ++pos;
                    }
                    if (pos < tokens.size() && tokens[pos] == ")") ++pos;
                }
            } else if (w == "cube") {
                elem.kind = SelectStmt::GroupByElem::Kind::Cube; ++pos;
                if (pos < tokens.size() && tokens[pos] == "(") {
                    ++pos;
                    while (pos < tokens.size() && tokens[pos] != ")") {
                        auto expr = parseSimpleExpr(tokens, pos);
                        if (expr) elem.exprs.push_back(std::move(expr));
                        if (pos < tokens.size() && tokens[pos] == ",") ++pos;
                    }
                    if (pos < tokens.size() && tokens[pos] == ")") ++pos;
                }
            } else if (w == "grouping") {
                elem.kind = SelectStmt::GroupByElem::Kind::GroupingSets; ++pos;
                if (pos < tokens.size() && toLower(tokens[pos]) == "sets") ++pos;
                if (pos < tokens.size() && tokens[pos] == "(") {
                    ++pos;
                    while (pos < tokens.size() && tokens[pos] != ")") {
                        if (tokens[pos] == "(") {
                            ++pos;
                            while (pos < tokens.size() && tokens[pos] != ")") {
                                auto expr = parseSimpleExpr(tokens, pos);
                                if (expr) elem.exprs.push_back(std::move(expr));
                                if (pos < tokens.size() && tokens[pos] == ",") ++pos;
                            }
                            if (pos < tokens.size() && tokens[pos] == ")") ++pos;
                        } else {
                            auto expr = parseSimpleExpr(tokens, pos);
                            if (expr) elem.exprs.push_back(std::move(expr));
                        }
                        if (pos < tokens.size() && tokens[pos] == ",") ++pos;
                    }
                    if (pos < tokens.size() && tokens[pos] == ")") ++pos;
                }
            } else {
                elem.kind = SelectStmt::GroupByElem::Kind::Plain;
                auto expr = parseSimpleExpr(tokens, pos);
                if (expr) elem.exprs.push_back(std::move(expr));
            }
            for (auto& e : elem.exprs) stmt->groupBy.push_back(std::move(e));
            stmt->groupByElems.push_back(std::move(elem));
            if (pos < tokens.size() && tokens[pos] == ",") { ++pos; continue; }
        }
    }

    // HAVING
    if (pos < tokens.size() && toLower(tokens[pos]) == "having") {
        ++pos;
        stmt->having = parseSimpleExpr(tokens, pos);
    }

    // ORDER BY
    if (pos < tokens.size() && toLower(tokens[pos]) == "order") {
        ++pos;
        if (pos < tokens.size() && toLower(tokens[pos]) == "by") ++pos;
        while (pos < tokens.size()) {
            std::string w = toLower(tokens[pos]);
            if (w == "limit" || w == "offset" || w == "union"
                || w == "intersect" || w == "except" || w == "for") break;
            auto expr = parseSimpleExpr(tokens, pos);
            bool asc = true;
            if (pos < tokens.size() && toLower(tokens[pos]) == "asc") { asc = true; ++pos; }
            else if (pos < tokens.size() && toLower(tokens[pos]) == "desc") { asc = false; ++pos; }
            if (expr) stmt->orderBy.push_back({std::move(expr), asc, false, ""});
            if (pos < tokens.size() && toLower(tokens[pos]) == "nulls") {
                ++pos;
                if (pos < tokens.size() && toLower(tokens[pos]) == "first") {
                    stmt->orderBy.back().nullsFirst = true; ++pos;
                } else if (pos < tokens.size() && toLower(tokens[pos]) == "last") {
                    stmt->orderBy.back().nullsFirst = false; ++pos;
                }
            }
            if (pos < tokens.size() && tokens[pos] == ",") { ++pos; continue; }
        }
    }

    // LIMIT / OFFSET / FETCH
    if (pos < tokens.size() && toLower(tokens[pos]) == "limit") {
        ++pos;
        if (pos >= tokens.size()) {
            r.error = "LIMIT requires a non-negative integer or ALL";
            return r;
        }
        if (toLower(tokens[pos]) == "all") {
            ++pos;
        } else {
            size_t limit = 0;
            if (!parseNonNegativeInteger(tokens[pos], limit)) {
                r.error = "LIMIT requires a non-negative integer or ALL";
                return r;
            }
            stmt->limit = limit;
            ++pos;
        }
        if (pos < tokens.size() && toLower(tokens[pos]) == "with") {
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "ties") {
                stmt->withTies = true; ++pos;
            }
        }
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "offset") {
        ++pos;
        if (pos >= tokens.size()) {
            r.error = "OFFSET requires a non-negative integer";
            return r;
        }
        size_t offset = 0;
        if (!parseNonNegativeInteger(tokens[pos], offset)) {
            r.error = "OFFSET requires a non-negative integer";
            return r;
        }
        stmt->offset = offset;
        ++pos;
    }
    // FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } { ONLY | WITH TIES }
    if (pos < tokens.size() && toLower(tokens[pos]) == "fetch") {
        ++pos;
        if (pos >= tokens.size() || (toLower(tokens[pos]) != "first" && toLower(tokens[pos]) != "next")) {
            r.error = "FETCH requires FIRST or NEXT";
            return r;
        }
        ++pos;
        stmt->fetchFirst = true;

        if (pos < tokens.size() && (toLower(tokens[pos]) == "row" || toLower(tokens[pos]) == "rows")) {
            stmt->limit = 1;
        } else {
            if (pos >= tokens.size()) {
                r.error = "FETCH requires a non-negative integer count";
                return r;
            }
            size_t limit = 0;
            if (!parseNonNegativeInteger(tokens[pos], limit)) {
                r.error = "FETCH requires a non-negative integer count";
                return r;
            }
            stmt->limit = limit;
            ++pos;
        }

        if (pos >= tokens.size() || (toLower(tokens[pos]) != "row" && toLower(tokens[pos]) != "rows")) {
            r.error = "FETCH count must be followed by ROW or ROWS";
            return r;
        }
        ++pos;
        if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
            ++pos;
        } else if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "with" && toLower(tokens[pos + 1]) == "ties") {
            stmt->withTies = true; pos += 2;
        } else {
            r.error = "FETCH requires ONLY or WITH TIES";
            return r;
        }
    }

    // FOR UPDATE / FOR SHARE
    if (pos < tokens.size() && toLower(tokens[pos]) == "for") {
        ++pos;
        SelectStmt::LockClause lc;
        if (pos < tokens.size() && toLower(tokens[pos]) == "update") {
            lc.strength = "UPDATE"; ++pos;
        } else if (pos < tokens.size() && toLower(tokens[pos]) == "share") {
            lc.strength = "SHARE"; ++pos;
        } else if (pos < tokens.size() && toLower(tokens[pos]) == "no") {
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "key") {
                ++pos;
                if (pos < tokens.size() && toLower(tokens[pos]) == "update") {
                    lc.strength = "NO KEY UPDATE"; ++pos;
                }
            }
        } else if (pos < tokens.size() && toLower(tokens[pos]) == "key") {
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "share") {
                lc.strength = "KEY SHARE"; ++pos;
            }
        }
        if (pos < tokens.size() && toLower(tokens[pos]) == "of") {
            ++pos;
            while (pos < tokens.size()) {
                std::string w = toLower(tokens[pos]);
                if (w == "nowait" || w == "skip" || w == ")" || w == ";" || w == "union"
                    || w == "intersect" || w == "except") break;
                lc.tables.push_back(tokens[pos++]);
                if (pos < tokens.size() && tokens[pos] == ",") ++pos;
            }
        }
        if (pos < tokens.size() && toLower(tokens[pos]) == "nowait") {
            lc.noWait = true; ++pos;
        }
        if (pos < tokens.size() && toLower(tokens[pos]) == "skip") {
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "locked") {
                lc.skipLocked = true; ++pos;
            }
        }
        stmt->locking.push_back(std::move(lc));
    }

    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseInsert(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 3) {
        r.error = "INSERT statement too short";
        return r;
    }

    auto stmt = std::make_unique<InsertStmt>();
    size_t pos = 1; // skip INSERT
    if (pos < tokens.size() && toLower(tokens[pos]) == "into") ++pos;

    // Table name
    if (pos < tokens.size()) {
        stmt->tableName = tokens[pos++];
    }

    // Optional column list: (col1, col2, ...)
    if (pos < tokens.size() && tokens[pos] == "(") {
        auto cols = collectParenthesized(tokens, pos);
        for (const auto& c : cols) {
            if (c != ",") stmt->columns.push_back(c);
        }
    }

    // VALUES or SELECT
    if (pos < tokens.size() && toLower(tokens[pos]) == "values") {
        ++pos;
        while (pos < tokens.size()) {
            if (tokens[pos] == "(") {
                ++pos;
                std::vector<ExprPtr> row;
                while (pos < tokens.size() && tokens[pos] != ")") {
                    if (toLower(tokens[pos]) == "default") {
                        // DEFAULT is a value expression, not the statement-level
                        // DEFAULT VALUES form.  Keeping it in the row preserves
                        // positional information for mixed rows such as
                        // VALUES (DEFAULT, 1).
                        auto defaultExpr = std::make_unique<LiteralExpr>();
                        defaultExpr->value = "default";
                        row.push_back(std::move(defaultExpr));
                        ++pos;
                    } else {
                        auto expr = parseSimpleExpr(tokens, pos);
                        if (expr) row.push_back(std::move(expr));
                    }
                    if (pos < tokens.size() && tokens[pos] == ",") ++pos;
                }
                if (pos < tokens.size() && tokens[pos] == ")") ++pos;
                stmt->values.push_back(std::move(row));
            }
            if (pos < tokens.size() && tokens[pos] == ",") {
                ++pos;
                continue;
            }
            if (pos < tokens.size() && toLower(tokens[pos]) == "on") break;
            if (pos < tokens.size() && toLower(tokens[pos]) == "returning") break;
            if (pos < tokens.size() && tokens[pos] == ";") break;
            // If next is not '(', break (e.g. started a new clause)
            if (pos < tokens.size() && tokens[pos] != "(") break;
        }
    } else if (pos < tokens.size() && toLower(tokens[pos]) == "default") {
        ++pos;
        if (pos < tokens.size() && toLower(tokens[pos]) == "values") {
            ++pos;
            stmt->defaultValues = true;
        }
    } else if (pos < tokens.size() && toLower(tokens[pos]) == "select") {
        // INSERT INTO ... SELECT ...
        std::string selectSql;
        size_t returningPos = tokens.size();
        int depth = 0;
        for (size_t i = pos; i < tokens.size(); ++i) {
            if (tokens[i] == "(") {
                ++depth;
            } else if (tokens[i] == ")" && depth > 0) {
                --depth;
            } else if (depth == 0 && toLower(tokens[i]) == "returning") {
                returningPos = i;
                break;
            }
        }
        for (size_t i = pos; i < returningPos; ++i) {
            if (!selectSql.empty()) selectSql += " ";
            selectSql += tokens[i];
        }
        ParseResult source = parseSelect(selectSql);
        if (!source.success || !source.stmt) {
            r.error = source.error.empty() ? "invalid INSERT SELECT source" : source.error;
            return r;
        }
        stmt->selectSource = std::move(source.stmt);
        pos = returningPos;
    }

    // ON CONFLICT
    if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "on"
        && toLower(tokens[pos + 1]) == "conflict") {
        pos += 2;
        if (pos < tokens.size() && tokens[pos] == "(") {
            auto cols = collectParenthesized(tokens, pos);
            for (const auto& c : cols) {
                if (c != ",") stmt->conflictTarget.push_back(c);
            }
        }
        if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "do"
            && toLower(tokens[pos + 1]) == "nothing") {
            stmt->conflictAction = "DO NOTHING";
            pos += 2;
        } else if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "do"
                   && toLower(tokens[pos + 1]) == "update") {
            stmt->conflictAction = "DO UPDATE";
            pos += 2;
            if (pos < tokens.size() && toLower(tokens[pos]) == "set") {
                ++pos;
                while (pos < tokens.size()) {
                    std::string w = toLower(tokens[pos]);
                    if (w == "where" || w == "returning" || tokens[pos] == ";") break;
                    if (pos + 1 < tokens.size() && tokens[pos + 1] == "=") {
                        std::string col = tokens[pos];
                        pos += 2; // skip col =
                        auto expr = parseSimpleExpr(tokens, pos);
                        stmt->conflictUpdateSet.emplace_back(col, std::move(expr));
                    } else {
                        ++pos;
                    }
                    if (pos < tokens.size() && tokens[pos] == ",") ++pos;
                }
            }
            if (pos < tokens.size() && toLower(tokens[pos]) == "where") {
                ++pos;
                stmt->conflictWhere = parseSimpleExpr(tokens, pos);
            }
        }
    }

    // RETURNING
    if (pos < tokens.size() && toLower(tokens[pos]) == "returning") {
        ++pos;
        while (pos < tokens.size() && tokens[pos] != ";") {
            stmt->returning.push_back(parseSelectItem(tokens, pos));
            if (pos < tokens.size() && tokens[pos] == ",") ++pos;
        }
    }

    // INSERT is one of the statements executed through a staged AST bridge.
    // Do not report success while silently leaving trailing tokens for a
    // legacy dispatcher to reinterpret.
    while (pos < tokens.size() && tokens[pos] == ";") ++pos;
    if (pos != tokens.size()) {
        r.error = "unexpected token in INSERT statement: " + tokens[pos];
        return r;
    }

    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseUpdate(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 4) {
        r.error = "UPDATE statement too short";
        return r;
    }

    auto stmt = std::make_unique<UpdateStmt>();
    size_t pos = 1; // skip UPDATE

    // Table name
    if (pos < tokens.size()) {
        stmt->tableName = tokens[pos++];
    }

    // SET clause
    if (pos >= tokens.size() || toLower(tokens[pos]) != "set") {
        r.error = "UPDATE requires SET";
        return r;
    }
    ++pos;
    while (pos < tokens.size()) {
        std::string w = toLower(tokens[pos]);
        if (w == "from" || w == "where" || w == "returning" || tokens[pos] == ";") break;
        if (tokens[pos] == ",") {
            r.error = "unexpected comma in UPDATE SET clause";
            return r;
        }
        if (pos + 1 >= tokens.size() || tokens[pos + 1] != "=") {
            r.error = "expected column = expression in UPDATE SET clause";
            return r;
        }
        std::string col = tokens[pos];
        pos += 2;
        auto expr = parseSimpleExpr(tokens, pos);
        if (!expr) {
            r.error = "missing expression in UPDATE SET clause";
            return r;
        }
        stmt->setClauses[col] = std::move(expr);
        if (pos < tokens.size() && tokens[pos] == ",") {
            ++pos;
            if (pos >= tokens.size() || tokens[pos] == ";" ||
                toLower(tokens[pos]) == "from" ||
                toLower(tokens[pos]) == "where" ||
                toLower(tokens[pos]) == "returning") {
                r.error = "trailing comma in UPDATE SET clause";
                return r;
            }
        }
    }

    if (stmt->setClauses.empty()) {
        r.error = "UPDATE requires at least one assignment";
        return r;
    }

    // FROM clause
    if (pos < tokens.size() && toLower(tokens[pos]) == "from") {
        ++pos;
        stmt->fromClause = parseFromItem(tokens, pos);
    }

    // WHERE
    if (pos < tokens.size() && toLower(tokens[pos]) == "where") {
        ++pos;
        stmt->whereClause = parseSimpleExpr(tokens, pos);
    }

    // RETURNING
    if (pos < tokens.size() && toLower(tokens[pos]) == "returning") {
        ++pos;
        while (pos < tokens.size() && tokens[pos] != ";") {
            stmt->returning.push_back(parseSelectItem(tokens, pos));
            if (pos < tokens.size() && tokens[pos] == ",") ++pos;
        }
    }

    while (pos < tokens.size() && tokens[pos] == ";") ++pos;
    if (pos != tokens.size()) {
        r.error = "unexpected token in UPDATE statement: " + tokens[pos];
        return r;
    }

    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseDelete(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 3) {
        r.error = "DELETE statement too short";
        return r;
    }

    auto stmt = std::make_unique<DeleteStmt>();
    size_t pos = 1; // skip DELETE

    if (pos < tokens.size() && toLower(tokens[pos]) == "from") ++pos;
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true;
        ++pos;
    }

    // Table name
    if (pos < tokens.size()) {
        stmt->tableName = tokens[pos++];
    }

    // USING
    if (pos < tokens.size() && toLower(tokens[pos]) == "using") {
        ++pos;
        stmt->usingClause = parseFromItem(tokens, pos);
    }

    // WHERE
    if (pos < tokens.size() && toLower(tokens[pos]) == "where") {
        ++pos;
        stmt->whereClause = parseSimpleExpr(tokens, pos);
    }

    // RETURNING
    if (pos < tokens.size() && toLower(tokens[pos]) == "returning") {
        ++pos;
        while (pos < tokens.size() && tokens[pos] != ";") {
            stmt->returning.push_back(parseSelectItem(tokens, pos));
            if (pos < tokens.size() && tokens[pos] == ",") ++pos;
        }
    }

    while (pos < tokens.size() && tokens[pos] == ";") ++pos;
    if (pos != tokens.size()) {
        r.error = "unexpected token in DELETE statement: " + tokens[pos];
        return r;
    }

    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseMerge(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 5) {
        r.error = "MERGE statement too short";
        return r;
    }

    auto stmt = std::make_unique<MergeStmt>();
    size_t pos = 1; // skip MERGE

    if (pos < tokens.size() && toLower(tokens[pos]) == "into") ++pos;

    // Target table
    if (pos < tokens.size()) {
        stmt->targetTable = tokens[pos++];
    }

    // USING
    if (pos < tokens.size() && toLower(tokens[pos]) == "using") {
        ++pos;
        stmt->source = parseFromItem(tokens, pos);
    }

    // ON
    if (pos < tokens.size() && toLower(tokens[pos]) == "on") {
        ++pos;
        stmt->joinCondition = parseSimpleExpr(tokens, pos);
    }

    // WHEN clauses
    while (pos < tokens.size() && toLower(tokens[pos]) == "when") {
        ++pos;
        MergeStmt::WhenClause wc;
        if (pos < tokens.size() && toLower(tokens[pos]) == "matched") {
            wc.matched = true;
            ++pos;
        } else if (pos < tokens.size() && toLower(tokens[pos]) == "not") {
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "matched") {
                wc.matched = false;
                ++pos;
                if (pos < tokens.size() && toLower(tokens[pos]) == "by") {
                    ++pos;
                    if (pos < tokens.size()) wc.bySource = toLower(tokens[pos++]);
                }
            }
        }
        // AND condition
        if (pos < tokens.size() && toLower(tokens[pos]) == "and") {
            ++pos;
            wc.condition = parseSimpleExpr(tokens, pos);
        }
        if (pos < tokens.size() && toLower(tokens[pos]) == "then") ++pos;
        if (pos < tokens.size() && toLower(tokens[pos]) == "do") ++pos;

        if (pos < tokens.size() && toLower(tokens[pos]) == "nothing") {
            wc.action = "DO NOTHING";
            ++pos;
        } else if (pos < tokens.size() && toLower(tokens[pos]) == "update") {
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "set") ++pos;
            wc.action = "UPDATE";
            while (pos < tokens.size()) {
                std::string w = toLower(tokens[pos]);
                if (w == "when" || w == "returning" || tokens[pos] == ";") break;
                if (pos + 1 < tokens.size() && tokens[pos + 1] == "=") {
                    std::string col = tokens[pos];
                    pos += 2;
                    auto expr = parseSimpleExpr(tokens, pos);
                    wc.updateSet[col] = std::move(expr);
                } else {
                    ++pos;
                }
                if (pos < tokens.size() && tokens[pos] == ",") ++pos;
            }
        } else if (pos < tokens.size() && toLower(tokens[pos]) == "insert") {
            ++pos;
            wc.action = "INSERT";
            if (pos < tokens.size() && toLower(tokens[pos]) == "(") {
                auto cols = collectParenthesized(tokens, pos);
                for (const auto& c : cols) {
                    if (c != ",") {
                        auto expr = std::make_unique<LiteralExpr>();
                        expr->value = c;
                        wc.insertCols.emplace_back(c, std::move(expr));
                    }
                }
            }
            if (pos < tokens.size() && toLower(tokens[pos]) == "values") {
                ++pos;
                if (pos < tokens.size() && tokens[pos] == "(") {
                    ++pos;
                    size_t columnIndex = 0;
                    while (pos < tokens.size() && tokens[pos] != ")") {
                        auto expr = parseSimpleExpr(tokens, pos);
                        if (expr && columnIndex < wc.insertCols.size()) {
                            wc.insertCols[columnIndex++].second = std::move(expr);
                        }
                        if (pos < tokens.size() && tokens[pos] == ",") {
                            ++pos;
                        } else if (pos < tokens.size() && tokens[pos] != ")") {
                            ++pos;
                        }
                    }
                    if (pos < tokens.size() && tokens[pos] == ")") ++pos;
                }
            }
        } else if (pos < tokens.size() && toLower(tokens[pos]) == "delete") {
            wc.action = "DELETE";
            ++pos;
        }
        stmt->whenClauses.push_back(std::move(wc));
    }

    // RETURNING
    if (pos < tokens.size() && toLower(tokens[pos]) == "returning") {
        ++pos;
        while (pos < tokens.size() && tokens[pos] != ";") {
            stmt->returning.push_back(parseSelectItem(tokens, pos));
            if (pos < tokens.size() && tokens[pos] == ",") ++pos;
        }
    }

    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseValues(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    auto stmt = std::make_unique<SelectStmt>();
    stmt->command = SqlCommand::Values;
    size_t pos = 1; // skip VALUES

    while (pos < tokens.size()) {
        if (tokens[pos] == "(") {
            ++pos;
            std::vector<ExprPtr> row;
            while (pos < tokens.size() && tokens[pos] != ")") {
                auto expr = parseSimpleExpr(tokens, pos);
                if (expr) row.push_back(std::move(expr));
                if (pos < tokens.size() && tokens[pos] == ",") ++pos;
            }
            if (pos < tokens.size() && tokens[pos] == ")") ++pos;
            // Back-compat: also flatten into selectList
            for (auto& expr : row) {
                SelectItem si;
                si.expr = std::move(expr);
                stmt->selectList.push_back(std::move(si));
            }
            stmt->valuesRows.push_back(std::move(row));
        }
        if (pos < tokens.size() && tokens[pos] == ",") {
            ++pos;
            continue;
        }
        break;
    }

    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

// ------------------------------------------------------------------------
// CREATE 解析分发
// ------------------------------------------------------------------------

ParseResult SQLParser::parseCreate(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 2) {
        r.error = "CREATE statement too short";
        return r;
    }
    size_t pos = 1; // skip CREATE
    bool isReplace = false;
    if (match(tokens, pos, "or") && match(tokens, pos + 1, "replace")) {
        pos += 2;
        isReplace = true;
    }
    if (match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        pos += 3;
    }
    bool isUnique = false;
    if (match(tokens, pos, "unique")) { isUnique = true; ++pos; }
    bool isTemp = false;
    bool isLocalTemp = false;
    if (match(tokens, pos, "local")) {
        if (match(tokens, pos + 1, "temp") || match(tokens, pos + 1, "temporary")) {
            isTemp = true;
            isLocalTemp = true;
            pos += 2;
        }
    } else if (match(tokens, pos, "temp") || match(tokens, pos, "temporary")) {
        isTemp = true;
        ++pos;
    }
    bool isUnlogged = false;
    if (match(tokens, pos, "unlogged")) { isUnlogged = true; ++pos; }
    if (match(tokens, pos, "materialized")) {
        ++pos;
        if (match(tokens, pos, "view")) ++pos;
        auto stmt = parseCreateView(tokens, pos);
        if (stmt) {
            auto* mvs = static_cast<CreateViewStmt*>(stmt.get());
            mvs->materialized = true;
            mvs->replace = isReplace;
            mvs->command = SqlCommand::CreateMaterializedView;
        }
        r.success = true;
        r.stmt = std::move(stmt);
        return r;
    }

    if (pos < tokens.size()) {
        std::string kw = toLower(tokens[pos]);
        ++pos;
        if (kw == "table") {
            r.stmt = parseCreateTable(tokens, pos);
            if (r.stmt) {
                auto* table = static_cast<CreateTableStmt*>(r.stmt.get());
                table->unlogged = isUnlogged;
                table->temp = isTemp;
                table->localTemp = isLocalTemp;
            }
        } else if (kw == "index") {
            r.stmt = parseCreateIndex(tokens, pos);
            if (r.stmt && isUnique) static_cast<CreateIndexStmt*>(r.stmt.get())->unique = true;
        } else if (kw == "view") {
            r.stmt = parseCreateView(tokens, pos);
            if (r.stmt) static_cast<CreateViewStmt*>(r.stmt.get())->replace = isReplace;
        } else if (kw == "database") {
            r.stmt = parseCreateDatabase(tokens, pos);
        } else if (kw == "schema") {
            r.stmt = parseCreateSchema(tokens, pos);
        } else if (kw == "sequence") {
            r.stmt = parseCreateSequence(tokens, pos);
        } else if (kw == "domain") {
            r.stmt = parseCreateDomain(tokens, pos);
        } else if (kw == "type") {
            r.stmt = parseCreateType(tokens, pos);
        } else if (kw == "function") {
            r.stmt = parseCreateFunction(tokens, pos);
        } else if (kw == "procedure") {
            r.stmt = parseCreateProcedure(tokens, pos);
        } else if (kw == "trigger") {
            r.stmt = parseCreateTrigger(tokens, pos);
        } else if (kw == "role" || kw == "user") {
            r.stmt = parseCreateRole(tokens, pos, /*isUser=*/(kw == "user"));
        } else if (kw == "group") {
            auto role = parseCreateRole(tokens, pos, /*isUser=*/false);
            if (role) static_cast<CreateRoleStmt*>(role.get())->isGroup = true;
            r.stmt = std::move(role);
        } else if (kw == "tablespace") {
            r.stmt = parseCreateTablespace(tokens, pos);
        } else if (kw == "statistics") {
            r.stmt = parseCreateStatistics(tokens, pos);
        } else if (kw == "policy") {
            r.stmt = parseCreatePolicy(tokens, pos);
        } else if (kw == "rule") {
            r.stmt = parseCreateRule(tokens, pos);
        } else if (kw == "event") {
            if (match(tokens, pos, "trigger")) ++pos;
            r.stmt = parseCreateEventTrigger(tokens, pos);
        } else if (kw == "extension") {
            r.stmt = parseCreateExtension(tokens, pos);
        } else if (kw == "publication") {
            r.stmt = parseCreatePublication(tokens, pos);
        } else if (kw == "subscription") {
            r.stmt = parseCreateSubscription(tokens, pos);
        } else if (kw == "access") {
            if (match(tokens, pos, "method")) ++pos;
            r.stmt = parseCreateAccessMethod(tokens, pos);
        } else if (kw == "foreign") {
            if (match(tokens, pos, "data")) {
                ++pos;
                if (match(tokens, pos, "wrapper")) ++pos;
                r.stmt = parseCreateForeignDataWrapper(tokens, pos);
            } else if (match(tokens, pos, "table")) {
                ++pos;
                r.stmt = parseCreateForeignTable(tokens, pos);
            } else if (match(tokens, pos, "server")) {
                ++pos;
                r.stmt = parseCreateServer(tokens, pos);
            }
        } else if (kw == "cast") {
            r.stmt = parseCreateCast(tokens, pos);
        } else if (kw == "collation") {
            r.stmt = parseCreateCollation(tokens, pos);
        } else if (kw == "conversion") {
            r.stmt = parseCreateConversion(tokens, pos);
        } else if (kw == "operator") {
            if (match(tokens, pos, "class")) {
                ++pos;
                r.stmt = parseCreateOperatorClass(tokens, pos);
            } else if (match(tokens, pos, "family")) {
                ++pos;
                r.stmt = parseCreateOperatorFamily(tokens, pos);
            } else {
                r.stmt = parseCreateOperator(tokens, pos);
            }
        } else if (kw == "aggregate") {
            r.stmt = parseCreateAggregate(tokens, pos);
        } else if (kw == "transform") {
            r.stmt = parseCreateTransform(tokens, pos);
        } else if (kw == "language") {
            r.stmt = parseCreateLanguage(tokens, pos);
        } else if (kw == "text") {
            if (match(tokens, pos, "search")) {
                ++pos;
                if (match(tokens, pos, "configuration")) {
                    ++pos;
                    r.stmt = parseCreateTextSearchConfiguration(tokens, pos);
                } else if (match(tokens, pos, "dictionary")) {
                    ++pos;
                    r.stmt = parseCreateTextSearchDictionary(tokens, pos);
                } else if (match(tokens, pos, "parser")) {
                    ++pos;
                    r.stmt = parseCreateTextSearchParser(tokens, pos);
                } else if (match(tokens, pos, "template")) {
                    ++pos;
                    r.stmt = parseCreateTextSearchTemplate(tokens, pos);
                }
            }
        } else {
            r.stmt = std::make_unique<CreateTableStmt>();
        }
    } else {
        r.stmt = std::make_unique<CreateTableStmt>();
    }
    r.success = r.stmt != nullptr;
    if (!r.success) r.error = "invalid CREATE statement";
    return r;
}

// ------------------------------------------------------------------------
// DROP 解析分发
// ------------------------------------------------------------------------

ParseResult SQLParser::parseDrop(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 2) {
        r.error = "DROP statement too short";
        return r;
    }
    size_t pos = 1;
    if (match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) pos += 2;
    if (match(tokens, pos, "table")) {
        pos++;
        r.stmt = parseDropTable(tokens, pos);
    } else if (match(tokens, pos, "index")) {
        pos++;
        r.stmt = parseDropIndex(tokens, pos);
    } else if (match(tokens, pos, "view")) {
        pos++;
        r.stmt = parseDropView(tokens, pos);
    } else if (match(tokens, pos, "materialized")) {
        pos++;
        if (match(tokens, pos, "view")) pos++;
        r.stmt = parseDropMaterializedView(tokens, pos);
    } else if (match(tokens, pos, "database")) {
        pos++;
        r.stmt = parseDropDatabase(tokens, pos);
    } else if (match(tokens, pos, "schema")) {
        pos++;
        r.stmt = parseDropSchema(tokens, pos);
    } else if (match(tokens, pos, "sequence")) {
        pos++;
        r.stmt = parseDropSequence(tokens, pos);
    } else if (match(tokens, pos, "domain")) {
        pos++;
        r.stmt = parseDropDomain(tokens, pos);
    } else if (match(tokens, pos, "type")) {
        pos++;
        r.stmt = parseDropType(tokens, pos);
    } else if (match(tokens, pos, "function")) {
        pos++;
        r.stmt = parseDropFunction(tokens, pos);
    } else if (match(tokens, pos, "procedure")) {
        pos++;
        r.stmt = parseDropProcedure(tokens, pos);
    } else if (match(tokens, pos, "routine")) {
        pos++;
        r.stmt = parseDropRoutine(tokens, pos);
    } else if (match(tokens, pos, "trigger")) {
        pos++;
        r.stmt = parseDropTrigger(tokens, pos);
    } else if (match(tokens, pos, "rule")) {
        pos++;
        r.stmt = parseDropRule(tokens, pos);
    } else if (match(tokens, pos, "event")) {
        pos++;
        if (match(tokens, pos, "trigger")) pos++;
        r.stmt = parseDropEventTrigger(tokens, pos);
    } else if (match(tokens, pos, "role")) {
        pos++;
        r.stmt = parseDropRole(tokens, pos);
    } else if (match(tokens, pos, "user")) {
        pos++;
        r.stmt = parseDropUser(tokens, pos);
    } else if (match(tokens, pos, "tablespace")) {
        pos++;
        r.stmt = parseDropTablespace(tokens, pos);
    } else if (match(tokens, pos, "statistics")) {
        pos++;
        r.stmt = parseDropStatistics(tokens, pos);
    } else if (match(tokens, pos, "policy")) {
        pos++;
        r.stmt = parseDropPolicy(tokens, pos);
    } else if (match(tokens, pos, "extension")) {
        pos++;
        r.stmt = parseDropExtension(tokens, pos);
    } else if (match(tokens, pos, "publication")) {
        pos++;
        r.stmt = parseDropPublication(tokens, pos);
    } else if (match(tokens, pos, "subscription")) {
        pos++;
        r.stmt = parseDropSubscription(tokens, pos);
    } else if (match(tokens, pos, "access")) {
        pos++;
        if (match(tokens, pos, "method")) pos++;
        r.stmt = parseDropAccessMethod(tokens, pos);
    } else if (match(tokens, pos, "foreign")) {
        pos++;
        if (match(tokens, pos, "data")) {
            pos++;
            if (match(tokens, pos, "wrapper")) pos++;
            r.stmt = parseDropForeignDataWrapper(tokens, pos);
        } else if (match(tokens, pos, "table")) {
            pos++;
            r.stmt = parseDropForeignTable(tokens, pos);
        } else if (match(tokens, pos, "server")) {
            pos++;
            r.stmt = parseDropServer(tokens, pos);
        }
    } else if (match(tokens, pos, "user")) {
        pos++;
        if (match(tokens, pos, "mapping")) {
            pos++;
            r.stmt = parseDropUserMapping(tokens, pos);
        }
    } else if (match(tokens, pos, "cast")) {
        pos++;
        r.stmt = parseDropCast(tokens, pos);
    } else if (match(tokens, pos, "collation")) {
        pos++;
        r.stmt = parseDropCollation(tokens, pos);
    } else if (match(tokens, pos, "conversion")) {
        pos++;
        r.stmt = parseDropConversion(tokens, pos);
    } else if (match(tokens, pos, "operator")) {
        pos++;
        if (match(tokens, pos, "class")) {
            pos++;
            r.stmt = parseDropOperatorClass(tokens, pos);
        } else if (match(tokens, pos, "family")) {
            pos++;
            r.stmt = parseDropOperatorFamily(tokens, pos);
        } else {
            r.stmt = parseDropOperator(tokens, pos);
        }
    } else if (match(tokens, pos, "aggregate")) {
        pos++;
        r.stmt = parseDropAggregate(tokens, pos);
    } else if (match(tokens, pos, "transform")) {
        pos++;
        r.stmt = parseDropTransform(tokens, pos);
    } else if (match(tokens, pos, "language")) {
        pos++;
        r.stmt = parseDropLanguage(tokens, pos);
    } else if (match(tokens, pos, "text")) {
        pos++;
        if (match(tokens, pos, "search")) {
            pos++;
            if (match(tokens, pos, "configuration")) {
                pos++;
                r.stmt = parseDropTextSearchConfiguration(tokens, pos);
            } else if (match(tokens, pos, "dictionary")) {
                pos++;
                r.stmt = parseDropTextSearchDictionary(tokens, pos);
            } else if (match(tokens, pos, "parser")) {
                pos++;
                r.stmt = parseDropTextSearchParser(tokens, pos);
            } else if (match(tokens, pos, "template")) {
                pos++;
                r.stmt = parseDropTextSearchTemplate(tokens, pos);
            }
        }
    } else if (match(tokens, pos, "owned")) {
        pos++;
        r.stmt = parseDropOwned(tokens, pos);
    } else if (match(tokens, pos, "large")) {
        pos++;
        if (match(tokens, pos, "object")) pos++;
        r.stmt = parseDropLargeObject(tokens, pos);
    } else {
        r.stmt = std::make_unique<DropStmt>(SqlCommand::DropTable);
    }
    r.success = r.stmt != nullptr;
    if (!r.success) r.error = "invalid DROP statement";
    return r;
}

// ------------------------------------------------------------------------
// ALTER 解析分发
// ------------------------------------------------------------------------

ParseResult SQLParser::parseAlter(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 2) {
        r.error = "ALTER statement too short";
        return r;
    }
    size_t pos = 1;
    if (match(tokens, pos, "table")) {
        pos++;
        r.stmt = parseAlterTable(tokens, pos);
    } else if (match(tokens, pos, "index")) {
        pos++;
        r.stmt = parseAlterIndex(tokens, pos);
    } else if (match(tokens, pos, "view")) {
        pos++;
        r.stmt = parseAlterView(tokens, pos);
    } else if (match(tokens, pos, "materialized")) {
        pos++;
        if (match(tokens, pos, "view")) pos++;
        r.stmt = parseAlterMaterializedView(tokens, pos);
    } else if (match(tokens, pos, "database")) {
        pos++;
        r.stmt = parseAlterDatabase(tokens, pos);
    } else if (match(tokens, pos, "schema")) {
        pos++;
        r.stmt = parseAlterSchema(tokens, pos);
    } else if (match(tokens, pos, "sequence")) {
        pos++;
        r.stmt = parseAlterSequence(tokens, pos);
    } else if (match(tokens, pos, "domain")) {
        pos++;
        r.stmt = parseAlterDomain(tokens, pos);
    } else if (match(tokens, pos, "type")) {
        pos++;
        r.stmt = parseAlterType(tokens, pos);
    } else if (match(tokens, pos, "function")) {
        pos++;
        r.stmt = parseAlterFunction(tokens, pos);
    } else if (match(tokens, pos, "procedure")) {
        pos++;
        r.stmt = parseAlterProcedure(tokens, pos);
    } else if (match(tokens, pos, "routine")) {
        pos++;
        r.stmt = parseAlterRoutine(tokens, pos);
    } else if (match(tokens, pos, "trigger")) {
        pos++;
        r.stmt = parseAlterTrigger(tokens, pos);
    } else if (match(tokens, pos, "rule")) {
        pos++;
        r.stmt = parseAlterRule(tokens, pos);
    } else if (match(tokens, pos, "event")) {
        pos++;
        if (match(tokens, pos, "trigger")) pos++;
        r.stmt = parseAlterEventTrigger(tokens, pos);
    } else if (match(tokens, pos, "role")) {
        pos++;
        r.stmt = parseAlterRole(tokens, pos);
    } else if (match(tokens, pos, "user")) {
        pos++;
        r.stmt = parseAlterUser(tokens, pos);
    } else if (match(tokens, pos, "system")) {
        pos++;
        r.stmt = parseAlterSystem(tokens, pos);
    } else if (match(tokens, pos, "tablespace")) {
        pos++;
        r.stmt = parseAlterTablespace(tokens, pos);
    } else if (match(tokens, pos, "statistics")) {
        pos++;
        r.stmt = parseAlterStatistics(tokens, pos);
    } else if (match(tokens, pos, "policy")) {
        pos++;
        r.stmt = parseAlterPolicy(tokens, pos);
    } else if (match(tokens, pos, "extension")) {
        pos++;
        r.stmt = parseAlterExtension(tokens, pos);
    } else if (match(tokens, pos, "publication")) {
        pos++;
        r.stmt = parseAlterPublication(tokens, pos);
    } else if (match(tokens, pos, "subscription")) {
        pos++;
        r.stmt = parseAlterSubscription(tokens, pos);
    } else if (match(tokens, pos, "default")) {
        pos++;
        if (match(tokens, pos, "privileges")) pos++;
        r.stmt = parseAlterDefaultPrivileges(tokens, pos);
    } else if (match(tokens, pos, "foreign")) {
        pos++;
        if (match(tokens, pos, "data")) {
            pos++;
            if (match(tokens, pos, "wrapper")) pos++;
            r.stmt = parseAlterForeignDataWrapper(tokens, pos);
        } else if (match(tokens, pos, "table")) {
            pos++;
            r.stmt = parseAlterForeignTable(tokens, pos);
        } else if (match(tokens, pos, "server")) {
            pos++;
            r.stmt = parseAlterServer(tokens, pos);
        }
    } else if (match(tokens, pos, "user")) {
        pos++;
        if (match(tokens, pos, "mapping")) {
            pos++;
            r.stmt = parseAlterUserMapping(tokens, pos);
        }
    } else if (match(tokens, pos, "text")) {
        pos++;
        if (match(tokens, pos, "search")) {
            pos++;
            if (match(tokens, pos, "configuration")) {
                pos++;
                r.stmt = parseAlterTextSearchConfiguration(tokens, pos);
            } else if (match(tokens, pos, "dictionary")) {
                pos++;
                r.stmt = parseAlterTextSearchDictionary(tokens, pos);
            } else if (match(tokens, pos, "parser")) {
                pos++;
                r.stmt = parseAlterTextSearchParser(tokens, pos);
            } else if (match(tokens, pos, "template")) {
                pos++;
                r.stmt = parseAlterTextSearchTemplate(tokens, pos);
            }
        }
    } else if (match(tokens, pos, "collation")) {
        pos++;
        r.stmt = parseAlterCollation(tokens, pos);
    } else if (match(tokens, pos, "conversion")) {
        pos++;
        r.stmt = parseAlterConversion(tokens, pos);
    } else if (match(tokens, pos, "operator")) {
        pos++;
        if (match(tokens, pos, "class")) {
            pos++;
            r.stmt = parseAlterOperatorClass(tokens, pos);
        } else if (match(tokens, pos, "family")) {
            pos++;
            r.stmt = parseAlterOperatorFamily(tokens, pos);
        } else {
            r.stmt = parseAlterOperator(tokens, pos);
        }
    } else if (match(tokens, pos, "aggregate")) {
        pos++;
        r.stmt = parseAlterAggregate(tokens, pos);
    } else if (match(tokens, pos, "language")) {
        pos++;
        r.stmt = parseAlterLanguage(tokens, pos);
    } else if (match(tokens, pos, "large")) {
        pos++;
        if (match(tokens, pos, "object")) pos++;
        r.stmt = parseAlterLargeObject(tokens, pos);
    } else {
        r.stmt = std::make_unique<AlterTableStmt>();
    }
    r.success = r.stmt != nullptr;
    if (!r.success) r.error = "invalid ALTER statement";
    return r;
}

ParseResult SQLParser::parseTruncate(const std::string& sql) {
    ParseResult r;
    const auto tokens = tokenize(sql);
    if (tokens.empty()) {
        r.error = "empty TRUNCATE statement";
        return r;
    }

    auto stmt = std::make_unique<TruncateStmt>();
    size_t pos = 1;
    if (pos < tokens.size() && toLower(tokens[pos]) == "table") ++pos;
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true;
        ++pos;
    }

    const auto isOption = [](const std::string& token) {
        const std::string word = toLower(token);
        return word == "restart" || word == "continue" || word == "cascade" ||
               word == "restrict" || token == ";";
    };
    bool expectingTable = true;
    while (pos < tokens.size() && !isOption(tokens[pos])) {
        if (tokens[pos] == ",") {
            if (expectingTable) {
                r.error = "TRUNCATE has an empty table name";
                return r;
            }
            expectingTable = true;
            ++pos;
            continue;
        }
        if (!expectingTable) {
            r.error = "TRUNCATE table names must be comma-separated";
            return r;
        }
        std::string tableName = tokens[pos++];
        if (pos + 1 < tokens.size() && tokens[pos] == ".") {
            tableName += "." + tokens[pos + 1];
            pos += 2;
        }
        stmt->tableNames.push_back(tableName);
        expectingTable = false;
    }
    if (stmt->tableNames.empty() || expectingTable) {
        r.error = "TRUNCATE requires at least one table";
        return r;
    }

    bool identityOptionSeen = false;
    while (pos < tokens.size()) {
        const std::string word = toLower(tokens[pos++]);
        if (word == ";") {
            if (pos != tokens.size()) {
                r.error = "TRUNCATE terminator must be last";
                return r;
            }
            break;
        }
        if (word == "restart" || word == "continue") {
            if (identityOptionSeen) {
                r.error = "TRUNCATE cannot specify multiple identity options";
                return r;
            }
            if (pos >= tokens.size() || toLower(tokens[pos]) != "identity") {
                r.error = "TRUNCATE identity option requires IDENTITY";
                return r;
            }
            ++pos;
            identityOptionSeen = true;
            stmt->restartIdentity = word == "restart";
            continue;
        }
        if (word == "cascade") {
            if (stmt->restrict || stmt->cascade) {
                r.error = "TRUNCATE cannot specify both CASCADE and RESTRICT";
                return r;
            }
            stmt->cascade = true;
            continue;
        }
        if (word == "restrict") {
            if (stmt->cascade || stmt->restrict) {
                r.error = "TRUNCATE cannot specify both CASCADE and RESTRICT";
                return r;
            }
            stmt->restrict = true;
            continue;
        }
        r.error = "unsupported TRUNCATE option: " + tokens[pos - 1];
        return r;
    }

    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

// ------------------------------------------------------------------------
// 事务语句解析
// ------------------------------------------------------------------------

ParseResult SQLParser::parseBegin(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.empty()) {
        r.error = "empty transaction statement";
        return r;
    }

    const std::string first = toLower(tokens[0]);
    TransactionStmt::Kind kind;
    size_t pos = 1;
    if (first == "begin") {
        kind = TransactionStmt::Kind::Begin;
        if (pos < tokens.size() &&
            (toLower(tokens[pos]) == "transaction" || toLower(tokens[pos]) == "work")) {
            ++pos;
        }
    } else if (first == "start") {
        kind = TransactionStmt::Kind::Start;
        if (pos >= tokens.size() || toLower(tokens[pos]) != "transaction") {
            r.error = "START requires TRANSACTION";
            return r;
        }
        ++pos;
    } else {
        r.error = "invalid transaction start";
        return r;
    }

    auto stmt = std::make_unique<TransactionStmt>(kind);
    bool isolationSeen = false;
    bool readModeSeen = false;
    bool deferrableSeen = false;
    while (pos < tokens.size()) {
        const std::string word = toLower(tokens[pos]);
        if (word == ";") {
            ++pos;
            if (pos != tokens.size()) {
                r.error = "transaction terminator must be last";
                return r;
            }
            break;
        }
        if (word == "isolation") {
            if (isolationSeen) {
                r.error = "transaction isolation specified more than once";
                return r;
            }
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "level") ++pos;
            if (pos >= tokens.size()) {
                r.error = "ISOLATION requires a level";
                return r;
            }
            const std::string level = toLower(tokens[pos++]);
            if (level == "serializable") {
                stmt->isolation = IsolationLevel::SERIALIZABLE;
            } else if (level == "repeatable" && pos < tokens.size() &&
                       toLower(tokens[pos]) == "read") {
                ++pos;
                stmt->isolation = IsolationLevel::REPEATABLE_READ;
            } else if (level == "read" && pos < tokens.size()) {
                const std::string mode = toLower(tokens[pos++]);
                if (mode == "committed") stmt->isolation = IsolationLevel::READ_COMMITTED;
                else if (mode == "uncommitted") stmt->isolation = IsolationLevel::READ_UNCOMMITTED;
                else {
                    r.error = "invalid transaction isolation level";
                    return r;
                }
            } else {
                r.error = "invalid transaction isolation level";
                return r;
            }
            isolationSeen = true;
            continue;
        }
        if (word == "read" && pos + 1 < tokens.size() &&
            (toLower(tokens[pos + 1]) == "committed" ||
             toLower(tokens[pos + 1]) == "uncommitted")) {
            if (isolationSeen) {
                r.error = "transaction isolation specified more than once";
                return r;
            }
            const std::string mode = toLower(tokens[pos + 1]);
            stmt->isolation = mode == "committed"
                ? IsolationLevel::READ_COMMITTED : IsolationLevel::READ_UNCOMMITTED;
            pos += 2;
            isolationSeen = true;
            continue;
        }
        if (word == "read" && pos + 1 < tokens.size() &&
            (toLower(tokens[pos + 1]) == "only" || toLower(tokens[pos + 1]) == "write")) {
            if (readModeSeen) {
                r.error = "transaction read mode specified more than once";
                return r;
            }
            stmt->readOnly = toLower(tokens[pos + 1]) == "only";
            pos += 2;
            readModeSeen = true;
            continue;
        }
        if (word == "deferrable" || (word == "not" && pos + 1 < tokens.size() &&
                                      toLower(tokens[pos + 1]) == "deferrable")) {
            if (deferrableSeen) {
                r.error = "transaction deferrability specified more than once";
                return r;
            }
            stmt->deferrable = word == "deferrable";
            pos += word == "deferrable" ? 1 : 2;
            deferrableSeen = true;
            continue;
        }
        r.error = "unsupported transaction option: " + tokens[pos];
        return r;
    }
    r.stmt = std::move(stmt);
    r.success = true;
    return r;
}

ParseResult SQLParser::parseCommit(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.empty()) {
        r.error = "empty commit statement";
        return r;
    }
    auto stmt = std::make_unique<TransactionStmt>(TransactionStmt::Kind::Commit);
    size_t pos = 1;
    if (pos < tokens.size() && toLower(tokens[pos]) == "prepared") {
        ++pos;
        if (pos >= tokens.size() || tokens[pos] == ";") {
            r.error = "COMMIT PREPARED requires a transaction ID";
            return r;
        }
        stmt = std::make_unique<TransactionStmt>(TransactionStmt::Kind::CommitPrepared);
        stmt->gid = tokens[pos++];
    }
    if (stmt->kind == TransactionStmt::Kind::Commit && pos < tokens.size() &&
        toLower(tokens[pos]) == "and") {
        if (pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "chain") {
            pos += 2;
        } else if (pos + 2 < tokens.size() && toLower(tokens[pos + 1]) == "no" &&
                   toLower(tokens[pos + 2]) == "chain") {
            pos += 3;
        } else {
            r.error = "invalid COMMIT chain option";
            return r;
        }
    }
    while (pos < tokens.size() && tokens[pos] == ";") ++pos;
    if (pos != tokens.size()) {
        r.error = "invalid COMMIT statement";
        return r;
    }
    r.stmt = std::move(stmt);
    r.success = true;
    return r;
}

ParseResult SQLParser::parseRollback(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.empty()) {
        r.error = "empty rollback statement";
        return r;
    }
    size_t pos = 1;
    TransactionStmt::Kind kind;
    const std::string first = toLower(tokens[0]);
    if (first == "abort") kind = TransactionStmt::Kind::Abort;
    else if (first == "end") kind = TransactionStmt::Kind::End;
    else if (first == "rollback") kind = TransactionStmt::Kind::Rollback;
    else {
        r.error = "invalid rollback statement";
        return r;
    }
    auto stmt = std::make_unique<TransactionStmt>(kind);
    if (first == "rollback" && pos < tokens.size() &&
        toLower(tokens[pos]) == "prepared") {
        ++pos;
        if (pos >= tokens.size() || tokens[pos] == ";") {
            r.error = "ROLLBACK PREPARED requires a transaction ID";
            return r;
        }
        stmt = std::make_unique<TransactionStmt>(TransactionStmt::Kind::RollbackPrepared);
        stmt->gid = tokens[pos++];
    }
    if (first == "rollback" && pos < tokens.size() && toLower(tokens[pos]) == "to") {
        ++pos;
        if (pos < tokens.size() && toLower(tokens[pos]) == "savepoint") ++pos;
        if (pos >= tokens.size() || tokens[pos] == ";") {
            r.error = "ROLLBACK TO requires a savepoint name";
            return r;
        }
        stmt->kind = TransactionStmt::Kind::RollbackTo;
        stmt->command = SqlCommand::RollbackToSavepoint;
        stmt->savepointName = tokens[pos++];
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "and") {
        if (stmt->kind != TransactionStmt::Kind::Rollback) {
            r.error = "ROLLBACK TO cannot use AND CHAIN";
            return r;
        }
        if (pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "chain") {
            pos += 2;
        } else if (pos + 2 < tokens.size() && toLower(tokens[pos + 1]) == "no" &&
                   toLower(tokens[pos + 2]) == "chain") {
            pos += 3;
        } else {
            r.error = "invalid ROLLBACK chain option";
            return r;
        }
    }
    while (pos < tokens.size() && tokens[pos] == ";") ++pos;
    if (pos != tokens.size()) {
        r.error = "invalid rollback statement";
        return r;
    }
    r.stmt = std::move(stmt);
    r.success = true;
    return r;
}

ParseResult SQLParser::parseSavepoint(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 2) {
        r.error = "SAVEPOINT requires a name";
        return r;
    }
    auto stmt = std::make_unique<TransactionStmt>(TransactionStmt::Kind::Savepoint);
    stmt->savepointName = tokens[1];
    size_t pos = 2;
    while (pos < tokens.size() && tokens[pos] == ";") ++pos;
    if (pos != tokens.size()) {
        r.error = "SAVEPOINT accepts exactly one name";
        return r;
    }
    r.stmt = std::move(stmt);
    r.success = true;
    return r;
}

ParseResult SQLParser::parseRelease(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 2) {
        r.error = "RELEASE requires a savepoint name";
        return r;
    }
    auto stmt = std::make_unique<TransactionStmt>(TransactionStmt::Kind::Release);
    size_t pos = 1;
    if (toLower(tokens[pos]) == "savepoint") ++pos;
    if (pos >= tokens.size() || tokens[pos] == ";") {
        r.error = "RELEASE requires a savepoint name";
        return r;
    }
    stmt->savepointName = tokens[pos++];
    while (pos < tokens.size() && tokens[pos] == ";") ++pos;
    if (pos != tokens.size()) {
        r.error = "RELEASE accepts exactly one savepoint name";
        return r;
    }
    r.stmt = std::move(stmt);
    r.success = true;
    return r;
}

// ------------------------------------------------------------------------
// SET / SHOW / RESET / USE / DISCARD
// ------------------------------------------------------------------------

ParseResult SQLParser::parseSet(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 2) {
        r.error = "SET statement too short";
        return r;
    }
    size_t pos = 1; // skip SET
    auto stmt = std::make_unique<SetStmt>();

    // SET [SESSION | LOCAL] name TO|=' value(s)
    if (pos < tokens.size() && toLower(tokens[pos]) == "session") {
        stmt->scope = SetStmt::Scope::Session; ++pos;
    } else if (pos < tokens.size() && toLower(tokens[pos]) == "local") {
        stmt->scope = SetStmt::Scope::Local; ++pos;
    }

    // Handle special SET ROLE / SESSION AUTHORIZATION forms
    if (pos < tokens.size() && toLower(tokens[pos]) == "role") {
        stmt->name = "role"; ++pos;
        while (pos < tokens.size() && tokens[pos] != ";") {
            stmt->values.push_back(tokens[pos++]);
        }
        r.success = true; r.stmt = std::move(stmt); return r;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "session" && pos + 1 < tokens.size() &&
        toLower(tokens[pos + 1]) == "authorization") {
        stmt->name = "session_authorization"; pos += 2;
        while (pos < tokens.size() && tokens[pos] != ";") {
            stmt->values.push_back(tokens[pos++]);
        }
        r.success = true; r.stmt = std::move(stmt); return r;
    }

    if (pos >= tokens.size() || tokens[pos] == ";") {
        r.error = "SET requires a parameter name";
        return r;
    }
    stmt->name = tokens[pos++];

    if (pos < tokens.size() && (toLower(tokens[pos]) == "to" || tokens[pos] == "=")) ++pos;

    while (pos < tokens.size() && tokens[pos] != ";") {
        stmt->values.push_back(tokens[pos++]);
    }

    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseShow(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 2) {
        r.error = "SHOW statement too short";
        return r;
    }
    size_t pos = 1; // skip SHOW
    auto stmt = std::make_unique<SetStmt>();
    stmt->isShow = true;
    if (pos < tokens.size() && toLower(tokens[pos]) == "all") {
        stmt->name = "all"; ++pos;
    } else if (pos < tokens.size()) {
        stmt->name = tokens[pos++];
    }
    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseReset(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 2) {
        r.error = "RESET statement too short";
        return r;
    }
    size_t pos = 1; // skip RESET
    auto stmt = std::make_unique<SetStmt>();
    stmt->isReset = true;
    if (pos < tokens.size() && toLower(tokens[pos]) == "all") {
        stmt->name = "all"; ++pos;
    } else if (pos < tokens.size()) {
        stmt->name = tokens[pos++];
    }
    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseUse(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    auto stmt = std::make_unique<SetStmt>();
    stmt->command = SqlCommand::UseDatabase;
    // USE DATABASE name -> mapped to SET search_path = name, public
    // Retain backward-compatible bare Stmt behavior for callers that expect it.
    size_t pos = 1;
    if (pos < tokens.size() && toLower(tokens[pos]) == "database") ++pos;
    if (pos < tokens.size()) {
        stmt->name = "search_path";
        stmt->values.push_back(tokens[pos]);
        stmt->values.push_back("public");
    }
    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseDiscard(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Discard);
    return r;
}

// ------------------------------------------------------------------------
// Utility 语句解析
// ------------------------------------------------------------------------

ParseResult SQLParser::parseExplain(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    if (tokens.size() < 2) {
        r.error = "EXPLAIN statement too short";
        return r;
    }
    size_t pos = 1; // skip EXPLAIN
    auto stmt = std::make_unique<ExplainStmt>();

    // Optional parenthesized option list: EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ...
    if (pos < tokens.size() && tokens[pos] == "(") {
        ++pos;
        while (pos < tokens.size() && tokens[pos] != ")") {
            std::string opt = toLower(tokens[pos++]);
            if (opt == "analyze") stmt->analyze = true;
            else if (opt == "verbose") stmt->verbose = true;
            else if (opt == "costs") stmt->costs = true;
            else if (opt == "buffers") stmt->buffers = true;
            else if (opt == "timing") stmt->timing = true;
            else if (opt == "settings") stmt->settings = true;
            else if (opt == "generic_plan") stmt->genericPlan = true;
            else if (opt == "format") {
                if (pos < tokens.size() && toLower(tokens[pos]) == "=") ++pos;
                if (pos < tokens.size()) {
                    std::string fmt = toLower(tokens[pos++]);
                    stmt->json = (fmt == "json");
                    stmt->xml  = (fmt == "xml");
                    stmt->yaml = (fmt == "yaml");
                }
            }
            if (pos < tokens.size() && tokens[pos] == ",") ++pos;
        }
        if (pos < tokens.size() && tokens[pos] == ")") ++pos;
    } else {
        // Legacy unparenthesized options: EXPLAIN ANALYZE, EXPLAIN VERBOSE
        while (pos < tokens.size()) {
            std::string w = toLower(tokens[pos]);
            if (w == "analyze") { stmt->analyze = true; ++pos; }
            else if (w == "verbose") { stmt->verbose = true; ++pos; }
            else break;
        }
    }

    // Remaining tokens are the statement to explain.
    std::string innerSql;
    for (size_t i = pos; i < tokens.size(); ++i) {
        if (!innerSql.empty()) innerSql += " ";
        innerSql += tokens[i];
    }
    if (!innerSql.empty()) {
        stmt->query = parse(innerSql).stmt;
    }

    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseAnalyze(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    size_t pos = 1; // skip ANALYZE
    // ANALYZE without EXPLAIN keyword is the vacuum-analyze utility, not EXPLAIN ANALYZE.
    if (pos < tokens.size() && toLower(tokens[pos]) == "(") {
        // EXPLAIN-style ANALYZE (...) is rare; treat as EXPLAIN ANALYZE.
        return parseExplain("EXPLAIN " + sql.substr(6));
    }
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Analyze);
    return r;
}

ParseResult SQLParser::parseVacuum(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Vacuum);
    return r;
}

ParseResult SQLParser::parseCheckpoint(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Checkpoint);
    return r;
}

ParseResult SQLParser::parseReindex(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Reindex);
    return r;
}

ParseResult SQLParser::parseCluster(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Cluster);
    return r;
}

ParseResult SQLParser::parseCopy(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<CopyStmt>();
    return r;
}

ParseResult SQLParser::parseComment(const std::string& sql) {
    ParseResult r;
    auto stmt = std::make_unique<CommentStmt>();
    std::string lsql = toLower(trim(sql));
    if (!lsql.empty() && lsql.back() == ';') lsql.pop_back();

    size_t onPos = lsql.find(" on ");
    if (onPos == std::string::npos) {
        r.success = false;
        return r;
    }

    std::string rest = trim(lsql.substr(onPos + 4));
    size_t isPos = rest.find(" is ");
    std::string beforeIs = (isPos == std::string::npos) ? rest : trim(rest.substr(0, isPos));
    std::string afterIs = (isPos == std::string::npos) ? "" : trim(rest.substr(isPos + 4));

    if (!afterIs.empty() && afterIs.size() >= 2 &&
        ((afterIs.front() == '\'' && afterIs.back() == '\'') ||
         (afterIs.front() == '"' && afterIs.back() == '"'))) {
        stmt->comment = afterIs.substr(1, afterIs.size() - 2);
    } else if (afterIs == "null") {
        stmt->comment.clear();
    } else {
        stmt->comment = afterIs;
    }

    auto startsWith = [&](const std::string& prefix) -> bool {
        return beforeIs.size() >= prefix.size() &&
               toLower(beforeIs.substr(0, prefix.size())) == prefix;
    };

    if (startsWith("materialized view ")) {
        stmt->objectType = "MATERIALIZED VIEW";
        stmt->objectName = trim(beforeIs.substr(18));
    } else if (startsWith("table ")) {
        stmt->objectType = "TABLE";
        stmt->objectName = trim(beforeIs.substr(6));
    } else if (startsWith("column ")) {
        stmt->objectType = "COLUMN";
        stmt->objectName = trim(beforeIs.substr(7));
    } else if (startsWith("schema ")) {
        stmt->objectType = "SCHEMA";
        stmt->objectName = trim(beforeIs.substr(7));
    } else if (startsWith("index ")) {
        stmt->objectType = "INDEX";
        stmt->objectName = trim(beforeIs.substr(6));
    } else if (startsWith("view ")) {
        stmt->objectType = "VIEW";
        stmt->objectName = trim(beforeIs.substr(5));
    } else if (startsWith("function ")) {
        stmt->objectType = "FUNCTION";
        stmt->objectName = trim(beforeIs.substr(9));
    } else if (startsWith("procedure ")) {
        stmt->objectType = "PROCEDURE";
        stmt->objectName = trim(beforeIs.substr(10));
    } else if (startsWith("sequence ")) {
        stmt->objectType = "SEQUENCE";
        stmt->objectName = trim(beforeIs.substr(9));
    } else if (startsWith("type ")) {
        stmt->objectType = "TYPE";
        stmt->objectName = trim(beforeIs.substr(5));
    } else {
        stmt->objectType = "UNKNOWN";
        stmt->objectName = beforeIs;
    }

    if (stmt->objectType == "COLUMN") {
        size_t dot = stmt->objectName.rfind('.');
        if (dot != std::string::npos) {
            stmt->columnName = trim(stmt->objectName.substr(dot + 1));
            stmt->objectName = trim(stmt->objectName.substr(0, dot));
        }
    }

    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseSecurityLabel(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::SecurityLabel);
    return r;
}

ParseResult SQLParser::parseLock(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Lock);
    return r;
}

// ------------------------------------------------------------------------
// Listen / Notify / Unlisten
// ------------------------------------------------------------------------

ParseResult SQLParser::parseListen(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Listen);
    return r;
}

ParseResult SQLParser::parseNotify(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Notify);
    return r;
}

ParseResult SQLParser::parseUnlisten(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Unlisten);
    return r;
}

// ------------------------------------------------------------------------
// Cursor
// ------------------------------------------------------------------------

ParseResult SQLParser::parseDeclare(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Declare);
    return r;
}

ParseResult SQLParser::parseFetch(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Fetch);
    return r;
}

ParseResult SQLParser::parseMove(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Move);
    return r;
}

ParseResult SQLParser::parseClose(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Close);
    return r;
}

// ------------------------------------------------------------------------
// Prepared statement
// ------------------------------------------------------------------------

ParseResult SQLParser::parsePrepare(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Prepare);
    return r;
}

ParseResult SQLParser::parseExecute(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Execute);
    return r;
}

ParseResult SQLParser::parseDeallocate(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Deallocate);
    return r;
}

// ------------------------------------------------------------------------
// Grant / Revoke
// ------------------------------------------------------------------------

ParseResult SQLParser::parseGrant(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<GrantStmt>();
    return r;
}

ParseResult SQLParser::parseRevoke(const std::string&) {
    ParseResult r;
    r.success = true;
    auto stmt = std::make_unique<GrantStmt>();
    stmt->isGrant = false;
    r.stmt = std::move(stmt);
    return r;
}

// ------------------------------------------------------------------------
// Call / Do / ImportForeignSchema
// ------------------------------------------------------------------------

ParseResult SQLParser::parseCall(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Call);
    return r;
}

ParseResult SQLParser::parseDo(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Do);
    return r;
}

ParseResult SQLParser::parseImportForeignSchema(const std::string&) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::ImportForeignSchema);
    return r;
}

// 从当前位置解析到匹配的 ')'，返回包含在内的 token 列表（不含外层括号）
static std::vector<std::string> collectParenthesized(const std::vector<std::string>& tokens, size_t& pos) {
    std::vector<std::string> inner;
    if (pos < tokens.size() && tokens[pos] == "(") {
        ++pos; // skip '('
        int depth = 1;
        while (pos < tokens.size() && depth > 0) {
            if (tokens[pos] == "(") ++depth;
            else if (tokens[pos] == ")") --depth;
            if (depth > 0) inner.push_back(tokens[pos]);
            ++pos;
        }
    }
    return inner;
}

// Parse an EXCLUDE constraint clause after the EXCLUDE keyword:
//   [ USING method ] ( elem WITH operator [, ...] ) [ WHERE ( predicate ) ]
static TableConstraint parseExcludeConstraint(const std::vector<std::string>& tokens, size_t& pos) {
    TableConstraint tc;
    tc.type = "EXCLUDE";
    if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "using") {
        ++pos;
        if (pos < tokens.size()) tc.accessMethod = SQLParser::toLower(tokens[pos++]);
    }
    if (pos < tokens.size() && tokens[pos] == "(") {
        ++pos; // skip '('
        while (pos < tokens.size() && tokens[pos] != ")") {
            std::string elem;
            if (tokens[pos] == "(") {
                // Expression element: collect raw tokens until matching ')'.
                int depth = 0;
                while (pos < tokens.size() && !(depth == 0 && tokens[pos] == ")")) {
                    if (tokens[pos] == "(") ++depth;
                    else if (tokens[pos] == ")") --depth;
                    if (!elem.empty() && elem.back() != '(') elem += " ";
                    elem += tokens[pos++];
                }
                if (pos < tokens.size() && tokens[pos] == ")") ++pos;
            } else {
                elem = tokens[pos++];
            }
            if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "with") ++pos;
            std::string op;
            if (pos < tokens.size()) op = tokens[pos++];
            tc.excludeElements.push_back({elem, op});
            if (pos < tokens.size() && tokens[pos] == ",") ++pos;
        }
        if (pos < tokens.size() && tokens[pos] == ")") ++pos;
    }
    if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "where") {
        ++pos;
        if (pos < tokens.size() && tokens[pos] == "(") {
            auto predTokens = collectParenthesized(tokens, pos);
            for (const auto& t : predTokens) {
                if (!tc.excludeWhere.empty() && tc.excludeWhere.back() != '(') tc.excludeWhere += " ";
                tc.excludeWhere += t;
            }
        }
    }
    return tc;
}

static void parseConstraintDeferrability(const std::vector<std::string>& tokens, size_t& pos, TableConstraint& tc) {
    if (pos + 1 < tokens.size() && SQLParser::toLower(tokens[pos]) == "not" &&
        SQLParser::toLower(tokens[pos + 1]) == "deferrable") {
        pos += 2;
        tc.deferrable = false;
        tc.initiallyDeferred = false;
    } else if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "deferrable") {
        ++pos;
        tc.deferrable = true;
        if (pos + 1 < tokens.size() && SQLParser::toLower(tokens[pos]) == "initially") {
            if (SQLParser::toLower(tokens[pos + 1]) == "deferred") {
                pos += 2;
                tc.initiallyDeferred = true;
            } else if (SQLParser::toLower(tokens[pos + 1]) == "immediate") {
                pos += 2;
                tc.initiallyDeferred = false;
            }
        }
    }
}

// ============================================================================
// CREATE 子命令解析（Phase 1.2 逐步完善）
// ============================================================================

static void parseCreateTableOnCommit(const std::vector<std::string>& tokens,
                                     size_t& pos,
                                     CreateTableStmt& stmt) {
    stmt.onCommitSpecified = true;
    pos += 2; // ON COMMIT
    if (pos >= tokens.size()) {
        stmt.onCommitValid = false;
        return;
    }
    const std::string action = SQLParser::toLower(tokens[pos++]);
    if (action == "preserve") {
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "rows") ++pos;
        stmt.onCommit = "preserve";
    } else if (action == "delete") {
        if (pos >= tokens.size() || SQLParser::toLower(tokens[pos]) != "rows") {
            stmt.onCommitValid = false;
        } else {
            ++pos;
            stmt.onCommit = "delete";
        }
    } else if (action == "drop") {
        stmt.onCommit = "drop";
    } else {
        stmt.onCommitValid = false;
    }
}

StmtPtr SQLParser::parseCreateTable(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateTableStmt>();

    // Parse table name (may be schema-qualified)
    if (pos < tokens.size()) {
        stmt->tableName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            // schema.table
            ++pos;
            if (pos < tokens.size()) {
                stmt->tableName += "." + tokens[pos++];
            }
        }
    }

    // CREATE TABLE child PARTITION OF parent FOR VALUES ...
    // is a CREATE TABLE statement in PostgreSQL, so keep it in the typed DDL
    // path instead of letting the legacy string dispatcher own the syntax.
    if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "partition" &&
        toLower(tokens[pos + 1]) == "of") {
        pos += 2;
        if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
        stmt->partitionOf = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
            stmt->partitionOf += "." + tokens[pos++];
        }
        std::string bound;
        while (pos < tokens.size() && tokens[pos] != ";") {
            if (!bound.empty()) bound += ' ';
            bound += tokens[pos++];
        }
        if (bound.empty()) return nullptr;
        stmt->partitionBoundSpec = std::move(bound);
        return stmt;
    }

    // PostgreSQL places ON COMMIT before AS in CREATE TEMP TABLE ... AS.
    if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "on" &&
        toLower(tokens[pos + 1]) == "commit") {
        parseCreateTableOnCommit(tokens, pos, *stmt);
    }

    // CREATE TABLE ... AS SELECT ...
    if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "as" && toLower(tokens[pos + 1]) == "select") {
        pos += 2;
        std::vector<std::string> sel;
        while (pos < tokens.size() && tokens[pos] != ";") {
            sel.push_back(tokens[pos++]);
        }
        // Strip trailing WITH [NO] DATA clause (default: WITH DATA).
        if (sel.size() >= 2 && toLower(sel.back()) == "data") {
            if (sel.size() >= 3 && toLower(sel[sel.size() - 3]) == "with" &&
                toLower(sel[sel.size() - 2]) == "no") {
                stmt->withData = false;
                sel.resize(sel.size() - 3);
            } else if (toLower(sel[sel.size() - 2]) == "with") {
                stmt->withData = true;
                sel.resize(sel.size() - 2);
            }
        }
        std::string selectSql = "SELECT";
        for (const auto& t : sel) selectSql += " " + t;
        stmt->asSelect = selectSql;
        return stmt;
    }

    // Parse column/constraint list if present
    if (pos < tokens.size() && tokens[pos] == "(") {
        ++pos; // skip '('
        bool first = true;
        while (pos < tokens.size() && tokens[pos] != ")") {
            if (!first && pos < tokens.size() && tokens[pos] == ",") {
                ++pos;
                continue;
            }
            first = false;
            if (pos >= tokens.size() || tokens[pos] == ")") break;

            // Check for table-level constraint keywords
            std::string ltok = toLower(tokens[pos]);
            if (ltok == "constraint") {
                ++pos;
                std::string cname;
                if (pos < tokens.size()) cname = tokens[pos++];
                // Now the actual constraint type
                if (pos < tokens.size()) {
                    std::string ctype = toLower(tokens[pos]);
                    if (ctype == "primary") {
                        ++pos; if (pos < tokens.size() && toLower(tokens[pos]) == "key") ++pos;
                        auto cols = collectParenthesized(tokens, pos);
                        TableConstraint tc;
                        tc.name = cname;
                        tc.type = "PRIMARY KEY";
                        for (const auto& c : cols) {
                            if (c != ",") tc.columns.push_back(c);
                        }
                        stmt->constraints.push_back(std::move(tc));
                    } else if (ctype == "unique") {
                        ++pos;
                        auto cols = collectParenthesized(tokens, pos);
                        TableConstraint tc;
                        tc.name = cname;
                        tc.type = "UNIQUE";
                        for (const auto& c : cols) {
                            if (c != ",") tc.columns.push_back(c);
                        }
                        stmt->constraints.push_back(std::move(tc));
                    } else if (ctype == "foreign") {
                        ++pos; if (pos < tokens.size() && toLower(tokens[pos]) == "key") ++pos;
                        auto cols = collectParenthesized(tokens, pos);
                        TableConstraint tc;
                        tc.name = cname;
                        tc.type = "FOREIGN KEY";
                        for (const auto& c : cols) {
                            if (c != ",") tc.columns.push_back(c);
                        }
                        if (pos < tokens.size() && toLower(tokens[pos]) == "references") {
                            ++pos;
                            if (pos < tokens.size()) {
                                tc.refTable = tokens[pos++];
                            }
                            if (pos < tokens.size() && tokens[pos] == "(") {
                                auto refcols = collectParenthesized(tokens, pos);
                                for (const auto& c : refcols) {
                                    if (c != ",") tc.refColumns.push_back(c);
                                }
                            }
                            // ON DELETE / ON UPDATE
                            while (pos < tokens.size()) {
                                std::string w = toLower(tokens[pos]);
                                if (w == "on") {
                                    if (pos + 2 < tokens.size() && toLower(tokens[pos + 1]) == "delete") {
                                        pos += 2;
                                        tc.onDelete = toLower(tokens[pos++]);
                                    } else if (pos + 2 < tokens.size() && toLower(tokens[pos + 1]) == "update") {
                                        pos += 2;
                                        tc.onUpdate = toLower(tokens[pos++]);
                                    } else break;
                                } else break;
                            }
                        }
                        stmt->constraints.push_back(std::move(tc));
                    } else if (ctype == "check") {
                        ++pos;
                        if (pos < tokens.size() && tokens[pos] == "(") {
                            auto checkExprTokens = collectParenthesized(tokens, pos);
                            TableConstraint tc;
                            tc.name = cname;
                            tc.type = "CHECK";
                            std::string expr;
                            for (const auto& t : checkExprTokens) {
                                if (!expr.empty() && expr.back() != '(') expr += " ";
                                expr += t;
                            }
                            tc.checkExpr = std::make_unique<LiteralExpr>();
                            static_cast<LiteralExpr*>(tc.checkExpr.get())->value = expr;
                            parseConstraintDeferrability(tokens, pos, tc);
                            stmt->constraints.push_back(std::move(tc));
                        }
                    } else if (ctype == "exclude") {
                        ++pos;
                        TableConstraint tc = parseExcludeConstraint(tokens, pos);
                        tc.name = cname;
                        stmt->constraints.push_back(std::move(tc));
                    } else {
                        // Unknown constraint, skip until comma or )
                        while (pos < tokens.size() && tokens[pos] != "," && tokens[pos] != ")") ++pos;
                    }
                }
            } else if (ltok == "primary") {
                ++pos; if (pos < tokens.size() && toLower(tokens[pos]) == "key") ++pos;
                auto cols = collectParenthesized(tokens, pos);
                TableConstraint tc;
                tc.type = "PRIMARY KEY";
                for (const auto& c : cols) {
                    if (c != ",") tc.columns.push_back(c);
                }
                stmt->constraints.push_back(std::move(tc));
            } else if (ltok == "unique") {
                ++pos;
                auto cols = collectParenthesized(tokens, pos);
                TableConstraint tc;
                tc.type = "UNIQUE";
                for (const auto& c : cols) {
                    if (c != ",") tc.columns.push_back(c);
                }
                stmt->constraints.push_back(std::move(tc));
            } else if (ltok == "foreign") {
                ++pos; if (pos < tokens.size() && toLower(tokens[pos]) == "key") ++pos;
                auto cols = collectParenthesized(tokens, pos);
                TableConstraint tc;
                tc.type = "FOREIGN KEY";
                for (const auto& c : cols) {
                    if (c != ",") tc.columns.push_back(c);
                }
                if (pos < tokens.size() && toLower(tokens[pos]) == "references") {
                    ++pos;
                    if (pos < tokens.size()) tc.refTable = tokens[pos++];
                    if (pos < tokens.size() && tokens[pos] == "(") {
                        auto refcols = collectParenthesized(tokens, pos);
                        for (const auto& c : refcols) {
                            if (c != ",") tc.refColumns.push_back(c);
                        }
                    }
                    while (pos < tokens.size()) {
                        std::string w = toLower(tokens[pos]);
                        if (w == "on") {
                            if (pos + 2 < tokens.size() && toLower(tokens[pos + 1]) == "delete") {
                                pos += 2;
                                tc.onDelete = toLower(tokens[pos++]);
                            } else if (pos + 2 < tokens.size() && toLower(tokens[pos + 1]) == "update") {
                                pos += 2;
                                tc.onUpdate = toLower(tokens[pos++]);
                            } else break;
                        } else break;
                    }
                }
                stmt->constraints.push_back(std::move(tc));
            } else if (ltok == "check") {
                ++pos;
                if (pos < tokens.size() && tokens[pos] == "(") {
                    auto checkExprTokens = collectParenthesized(tokens, pos);
                    TableConstraint tc;
                    tc.type = "CHECK";
                    std::string expr;
                    for (const auto& t : checkExprTokens) {
                        if (!expr.empty() && expr.back() != '(') expr += " ";
                        expr += t;
                    }
                    tc.checkExpr = std::make_unique<LiteralExpr>();
                    static_cast<LiteralExpr*>(tc.checkExpr.get())->value = expr;
                    parseConstraintDeferrability(tokens, pos, tc);
                    stmt->constraints.push_back(std::move(tc));
                }
            } else if (ltok == "exclude") {
                ++pos;
                TableConstraint tc = parseExcludeConstraint(tokens, pos);
                stmt->constraints.push_back(std::move(tc));
            } else if (ltok == "like") {
                // LIKE source_table [ { INCLUDING | EXCLUDING } option ] ...
                ++pos;
                CreateTableStmt::LikeClause lc;
                if (pos < tokens.size()) {
                    lc.tableName = tokens[pos++];
                    if (pos < tokens.size() && tokens[pos] == ".") {
                        ++pos;
                        if (pos < tokens.size()) lc.tableName += "." + tokens[pos++];
                    }
                }
                while (pos < tokens.size() &&
                       (toLower(tokens[pos]) == "including" || toLower(tokens[pos]) == "excluding")) {
                    bool incl = (toLower(tokens[pos]) == "including");
                    ++pos;
                    if (pos < tokens.size()) {
                        std::string opt = toLower(tokens[pos++]);
                        if (opt == "all") lc.includingAll = incl;
                        else if (opt == "defaults") lc.includingDefaults = incl;
                        else if (opt == "constraints") lc.includingConstraints = incl;
                        else if (opt == "indexes") lc.includingIndexes = incl;
                        else if (opt == "identity") lc.includingIdentity = incl;
                        // storage / comments / generated / statistics: accepted but ignored
                    }
                }
                if (!lc.tableName.empty()) {
                    stmt->likeClauses.push_back(lc);
                    stmt->likeTables.emplace_back(lc.tableName, ColumnDef());
                }
            } else {
                // Column definition
                ColumnDef col;
                col.name = tokens[pos++];
                if (pos < tokens.size()) {
                    col.typeName = tokens[pos++];
                    // Multi-word type names: "bit varying", "character varying",
                    // "double precision" — fold the second word in before (n).
                    if (pos < tokens.size()) {
                        std::string t0 = toLower(col.typeName);
                        std::string t1 = toLower(tokens[pos]);
                        if (((t0 == "bit" || t0 == "character") && t1 == "varying") ||
                            (t0 == "double" && t1 == "precision")) {
                            col.typeName += " " + tokens[pos++];
                        }
                    }
                    // Type may have parameters: VARCHAR(255), NUMERIC(10,2)
                    if (pos < tokens.size() && tokens[pos] == "(") {
                        ++pos; // skip '('
                        while (pos < tokens.size() && tokens[pos] != ")") {
                            if (tokens[pos] != ",") col.typeMods.push_back(tokens[pos]);
                            ++pos;
                        }
                        if (pos < tokens.size() && tokens[pos] == ")") ++pos; // skip ')'
                    }
                    // Array type: TYPE[]
                    if (pos < tokens.size() && tokens[pos] == "[") {
                        ++pos;
                        if (pos < tokens.size() && tokens[pos] == "]") ++pos;
                        col.isArray = true;
                    }
                }
                // Column constraints
                while (pos < tokens.size() && tokens[pos] != "," && tokens[pos] != ")") {
                    std::string ckw = toLower(tokens[pos]);
                    if (ckw == "not" && pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "null") {
                        col.isNull = false;
                        pos += 2;
                    } else if (ckw == "null") {
                        col.isNull = true;
                        ++pos;
                    } else if (ckw == "primary") {
                        ++pos;
                        if (pos < tokens.size() && toLower(tokens[pos]) == "key") ++pos;
                        col.isPrimaryKey = true;
                        col.isNull = false;
                    } else if (ckw == "unique") {
                        ++pos;
                        col.isUnique = true;
                    } else if (ckw == "default") {
                        ++pos;
                        std::string defVal;
                        // Collect default value (literal, expression, or function call).
                        // Respect parenthesis depth so that DEFAULT nextval('s') or
                        // DEFAULT (expr) does not terminate early at an inner ')'.
                        int depth = 0;
                        while (pos < tokens.size()) {
                            if (tokens[pos] == "(") {
                                ++depth;
                            } else if (tokens[pos] == ")") {
                                if (depth == 0) break;
                                --depth;
                            } else if (tokens[pos] == "," && depth == 0) {
                                break;
                            }
                            if (!defVal.empty() && defVal.back() != '(') defVal += " ";
                            defVal += tokens[pos++];
                        }
                        col.defaultValue = std::make_unique<LiteralExpr>();
                        static_cast<LiteralExpr*>(col.defaultValue.get())->value = defVal;
                    } else if (ckw == "check") {
                        ++pos;
                        if (pos < tokens.size() && tokens[pos] == "(") {
                            auto checkExprTokens = collectParenthesized(tokens, pos);
                            std::string expr;
                            for (const auto& t : checkExprTokens) {
                                if (!expr.empty() && expr.back() != '(') expr += " ";
                                expr += t;
                            }
                            col.checkExprs.push_back(std::make_unique<LiteralExpr>());
                            static_cast<LiteralExpr*>(col.checkExprs.back().get())->value = expr;
                        }
                    } else if (ckw == "generated") {
                        ++pos;
                        if (pos < tokens.size() && toLower(tokens[pos]) == "always") {
                            ++pos;
                            if (pos < tokens.size() && toLower(tokens[pos]) == "as") {
                                ++pos;
                                if (pos < tokens.size() && toLower(tokens[pos]) == "identity") {
                                    ++pos;
                                    col.isGeneratedIdentity = true;
                                    col.constraints.push_back("GENERATED ALWAYS AS IDENTITY");
                                } else if (pos < tokens.size() && tokens[pos] == "(") {
                                    auto genExprTokens = collectParenthesized(tokens, pos);
                                    std::string expr;
                                    for (const auto& t : genExprTokens) {
                                        if (!expr.empty() && expr.back() != '(') expr += " ";
                                        expr += t;
                                    }
                                    col.generatedExpr = expr;
                                    col.generatedKind = 's'; // default to STORED if not specified
                                    if (pos < tokens.size()) {
                                        std::string gkw = toLower(tokens[pos]);
                                        if (gkw == "stored") {
                                            col.generatedKind = 's';
                                            ++pos;
                                        } else if (gkw == "virtual") {
                                            col.generatedKind = 'v';
                                            ++pos;
                                        }
                                    }
                                    col.constraints.push_back("GENERATED ALWAYS AS (" + expr + ") " +
                                                              (col.generatedKind == 'v' ? "VIRTUAL" : "STORED"));
                                }
                            }
                        } else if (pos < tokens.size() && toLower(tokens[pos]) == "by") {
                            ++pos;
                            if (pos < tokens.size() && toLower(tokens[pos]) == "default") {
                                ++pos;
                                if (pos < tokens.size() && toLower(tokens[pos]) == "as") {
                                    ++pos;
                                    if (pos < tokens.size() && toLower(tokens[pos]) == "identity") {
                                        ++pos;
                                        col.isGeneratedIdentity = true;
                                        col.constraints.push_back("GENERATED BY DEFAULT AS IDENTITY");
                                    }
                                }
                            }
                        }
                    } else if (ckw == "collate") {
                        ++pos;
                        if (pos < tokens.size()) col.collation = tokens[pos++];
                    } else if (ckw == "references") {
                        ++pos;
                        if (pos < tokens.size()) {
                            std::string refTable = tokens[pos++];
                            std::string refCol;
                            if (pos < tokens.size() && tokens[pos] == "(") {
                                auto refcols = collectParenthesized(tokens, pos);
                                if (!refcols.empty()) refCol = refcols[0];
                            }
                            // Store as a simple foreign key constraint on this column
                            TableConstraint tc;
                            tc.type = "FOREIGN KEY";
                            tc.columns.push_back(col.name);
                            tc.refTable = refTable;
                            if (!refCol.empty()) tc.refColumns.push_back(refCol);
                            stmt->constraints.push_back(std::move(tc));
                        }
                    } else {
                        // Unknown token, skip
                        ++pos;
                    }
                }
                stmt->columns.push_back(std::move(col));
            }
        }
        if (pos < tokens.size() && tokens[pos] == ")") ++pos; // skip ')'
    }

    // Parse remaining options after column list
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string kw = toLower(tokens[pos]);
        if (kw == "inherits") {
            ++pos;
            if (pos < tokens.size() && tokens[pos] == "(") {
                auto parents = collectParenthesized(tokens, pos);
                for (const auto& p : parents) {
                    if (p != ",") stmt->inherits.push_back(p);
                }
            }
        } else if (kw == "partition") {
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "by") {
                ++pos;
                std::string ptype = toLower(tokens[pos++]); // range, list, hash
                stmt->partitionType = ptype;
                if (pos < tokens.size() && tokens[pos] == "(") {
                    auto pcols = collectParenthesized(tokens, pos);
                    for (const auto& c : pcols) {
                        if (c != ",") {
                            SelectItem si;
                            si.expr = std::make_unique<ColumnRefExpr>();
                            static_cast<ColumnRefExpr*>(si.expr.get())->column = c;
                            stmt->partitionBy.push_back(std::move(si));
                        }
                    }
                }
            }
        } else if (kw == "with") {
            ++pos;
            if (pos < tokens.size() && tokens[pos] == "(") {
                auto opts = collectParenthesized(tokens, pos);
                for (size_t i = 0; i < opts.size(); i += 2) {
                    if (i + 1 < opts.size() && opts[i + 1] == "=") {
                        if (i + 2 < opts.size()) {
                            stmt->options[opts[i]] = opts[i + 2];
                            i += 2;
                        }
                    } else if (i + 1 < opts.size()) {
                        stmt->options[opts[i]] = opts[i + 1];
                    }
                }
            }
        } else if (kw == "without") {
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "oids") {
                stmt->options["oids"] = "false";
                ++pos;
            }
        } else if (kw == "tablespace") {
            ++pos;
            if (pos < tokens.size()) stmt->tablespace = tokens[pos++];
        } else if (kw == "of") {
            ++pos;
            if (pos < tokens.size()) stmt->ofType = tokens[pos++];
        } else if (kw == "like") {
            ++pos;
            if (pos < tokens.size()) {
                CreateTableStmt::LikeClause lc;
                lc.tableName = tokens[pos++];
                if (pos < tokens.size() && tokens[pos] == ".") {
                    ++pos;
                    if (pos < tokens.size()) lc.tableName += "." + tokens[pos++];
                }
                while (pos < tokens.size() &&
                       (toLower(tokens[pos]) == "including" || toLower(tokens[pos]) == "excluding")) {
                    bool incl = (toLower(tokens[pos]) == "including");
                    ++pos;
                    if (pos < tokens.size()) {
                        std::string opt = toLower(tokens[pos++]);
                        if (opt == "all") lc.includingAll = incl;
                        else if (opt == "defaults") lc.includingDefaults = incl;
                        else if (opt == "constraints") lc.includingConstraints = incl;
                        else if (opt == "indexes") lc.includingIndexes = incl;
                        else if (opt == "identity") lc.includingIdentity = incl;
                    }
                }
                stmt->likeClauses.push_back(lc);
                stmt->likeTables.emplace_back(lc.tableName, ColumnDef());
            }
        } else if (kw == "on" && pos + 1 < tokens.size() &&
                   toLower(tokens[pos + 1]) == "commit") {
            parseCreateTableOnCommit(tokens, pos, *stmt);
        } else {
            ++pos;
        }
    }

    return stmt;
}

StmtPtr SQLParser::parseCreateIndex(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateIndexStmt>();
    if (pos < tokens.size() && match(tokens, pos, "unique")) {
        stmt->unique = true; ++pos;
    }
    if (pos < tokens.size() && match(tokens, pos, "concurrently")) {
        stmt->concurrently = true; ++pos;
    }
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->indexName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->indexName = tokens[pos++]; // schema was first token; not stored separately here
            }
        }
    }
    if (pos < tokens.size() && match(tokens, pos, "on")) ++pos;
    if (pos < tokens.size()) {
        stmt->tableName = tokens[pos++];
    }
    if (pos < tokens.size() && match(tokens, pos, "using")) {
        ++pos;
        if (pos < tokens.size()) stmt->accessMethod = tokens[pos++];
    }
    if (pos < tokens.size() && tokens[pos] == "(") {
        ++pos;
        while (pos < tokens.size() && tokens[pos] != ")") {
            IndexElem elem;
            if (tokens[pos] == "(") {
                // expression index
                auto exprTokens = collectParenthesized(tokens, pos);
                std::string exprStr;
                for (const auto& t : exprTokens) {
                    if (!exprStr.empty() && exprStr.back() != '(') exprStr += " ";
                    exprStr += t;
                }
                elem.expr = std::make_unique<LiteralExpr>();
                static_cast<LiteralExpr*>(elem.expr.get())->value = exprStr;
            } else {
                elem.column = tokens[pos++];
                if (pos < tokens.size() && toLower(tokens[pos]) == "collate") {
                    ++pos;
                    if (pos < tokens.size()) elem.collation = tokens[pos++];
                }
                if (pos < tokens.size() && tokens[pos] != "," && tokens[pos] != ")" &&
                    toLower(tokens[pos]) != "asc" && toLower(tokens[pos]) != "desc" &&
                    toLower(tokens[pos]) != "nulls") {
                    elem.opclass = tokens[pos++];
                }
                if (pos < tokens.size() && toLower(tokens[pos]) == "asc") { elem.ascending = true; ++pos; }
                else if (pos < tokens.size() && toLower(tokens[pos]) == "desc") { elem.ascending = false; ++pos; }
                if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "nulls" &&
                    (toLower(tokens[pos + 1]) == "first" || toLower(tokens[pos + 1]) == "last")) {
                    elem.nullsFirst = (toLower(tokens[pos + 1]) == "first");
                    pos += 2;
                }
            }
            stmt->columns.push_back(std::move(elem));
            if (pos < tokens.size() && tokens[pos] == ",") ++pos;
        }
        if (pos < tokens.size() && tokens[pos] == ")") ++pos;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string kw = toLower(tokens[pos]);
        if (kw == "using" && pos + 1 < tokens.size()) {
            ++pos;
            stmt->accessMethod = tokens[pos++];
        } else if (kw == "include" && pos + 1 < tokens.size() && tokens[pos + 1] == "(") {
            pos += 2;
            while (pos < tokens.size() && tokens[pos] != ")") {
                if (tokens[pos] != ",") stmt->includeCols.push_back(tokens[pos]);
                ++pos;
            }
            if (pos < tokens.size() && tokens[pos] == ")") ++pos;
        } else if (kw == "where") {
            ++pos;
            stmt->whereClause = parseExpr(tokens, pos);
        } else if (kw == "with" && pos + 1 < tokens.size() && tokens[pos + 1] == "(") {
            pos += 2;
            auto opts = collectParenthesized(tokens, pos);
            for (size_t i = 0; i < opts.size(); i += 2) {
                if (i + 1 < opts.size() && opts[i + 1] == "=") {
                    if (i + 2 < opts.size()) { stmt->options[opts[i]] = opts[i + 2]; i += 2; }
                } else if (i + 1 < opts.size()) {
                    stmt->options[opts[i]] = opts[i + 1];
                }
            }
        } else if (kw == "tablespace") {
            ++pos;
            if (pos < tokens.size()) stmt->tablespace = tokens[pos++];
        } else {
            ++pos;
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateView(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateViewStmt>();
    if (pos < tokens.size()) {
        stmt->viewName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) stmt->viewName = tokens[pos++];
        }
    }
    if (pos < tokens.size() && tokens[pos] == "(") {
        auto cols = collectParenthesized(tokens, pos);
        for (const auto& c : cols) {
            if (c != ",") stmt->columnNames.push_back(c);
        }
    }
    if (pos < tokens.size() && match(tokens, pos, "with")) {
        ++pos;
        if (pos + 1 < tokens.size() && match(tokens, pos, "check") && match(tokens, pos + 1, "option")) {
            pos += 2;
            if (pos < tokens.size()) stmt->checkOption = toLower(tokens[pos++]);
        }
    }
    if (pos < tokens.size() && match(tokens, pos, "as")) {
        ++pos;
        std::vector<std::string> sel;
        for (size_t i = pos; i < tokens.size(); ++i) sel.push_back(tokens[i]);
        // Strip trailing WITH [NO] DATA (materialized views); default WITH DATA.
        if (sel.size() >= 2 && toLower(sel.back()) == "data") {
            if (sel.size() >= 3 && toLower(sel[sel.size() - 3]) == "with" &&
                toLower(sel[sel.size() - 2]) == "no") {
                stmt->withData = false;
                sel.resize(sel.size() - 3);
            } else if (toLower(sel[sel.size() - 2]) == "with") {
                stmt->withData = true;
                sel.resize(sel.size() - 2);
            }
        }
        std::string selectSql;
        for (const auto& t : sel) {
            if (!selectSql.empty()) selectSql += " ";
            selectSql += t;
        }
        stmt->selectSql = selectSql;
        stmt->query = parseSelect(selectSql).stmt;
        pos = tokens.size();
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateDatabase(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateDatabase);
    stmt->objectType = "DATABASE";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateSchema(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateSchema);
    stmt->objectType = "SCHEMA";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateSequence(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateSequence);
    stmt->objectType = "SEQUENCE";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }

    auto lower = [&](const std::string& s) { return toLower(s); };
    auto peek = [&](size_t offset) -> std::string {
        if (pos + offset < tokens.size()) return lower(tokens[pos + offset]);
        return "";
    };
    auto numericOption = [&](const std::string& key, size_t skip) {
        if (pos + skip >= tokens.size() || tokens[pos + skip] == ";") return false;
        int64_t ignored = 0;
        if (!parseInt64Token(tokens[pos + skip], ignored)) return false;
        stmt->options[key] = tokens[pos + skip];
        pos += skip + 1;
        return true;
    };

    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string tok = lower(tokens[pos]);
        if (tok == "start") {
            if (peek(1) == "with") {
                if (!numericOption("start", 2)) return nullptr;
            } else if (!numericOption("start", 1)) return nullptr;
        } else if (tok == "increment") {
            if (peek(1) == "by") {
                if (!numericOption("increment", 2)) return nullptr;
            } else if (!numericOption("increment", 1)) return nullptr;
        } else if (tok == "minvalue") {
            if (!numericOption("minvalue", 1)) return nullptr;
        } else if (tok == "maxvalue") {
            if (!numericOption("maxvalue", 1)) return nullptr;
        } else if (tok == "cache") {
            if (!numericOption("cache", 1)) return nullptr;
        } else if (tok == "no") {
            if (peek(1) == "minvalue") { stmt->options["nominvalue"] = "1"; pos += 2; }
            else if (peek(1) == "maxvalue") { stmt->options["nomaxvalue"] = "1"; pos += 2; }
            else if (peek(1) == "cycle") { stmt->options["cycle"] = "no"; pos += 2; }
            else return nullptr;
        } else if (tok == "cycle") {
            stmt->options["cycle"] = "yes"; ++pos;
        } else if (tok == "owned") {
            if (peek(1) == "by") {
                if (pos + 2 < tokens.size()) {
                    std::string owner = tokens[pos + 2];
                    if (toLower(owner) == "none") {
                        stmt->options["ownedby"] = "none";
                        pos += 3;
                    } else if (pos + 4 < tokens.size() && tokens[pos + 3] == ".") {
                        stmt->options["ownedby"] = owner + "." + tokens[pos + 4];
                        pos += 5;
                    } else {
                        stmt->options["ownedby"] = owner;
                        pos += 3;
                    }
                } else {
                    return nullptr;
                }
            } else return nullptr;
        } else {
            return nullptr;
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateDomain(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateDomain);
    stmt->objectType = "DOMAIN";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }

    // AS base_type
    if (pos < tokens.size() && toLower(tokens[pos]) == "as") {
        ++pos;
        if (pos < tokens.size()) {
            stmt->options["base_type"] = tokens[pos++];
            // Capture optional type modifiers like numeric(10,2)
            if (pos < tokens.size() && tokens[pos] == "(") {
                std::string mods = "(";
                ++pos;
                while (pos < tokens.size() && tokens[pos] != ")") {
                    mods += tokens[pos++];
                    if (pos < tokens.size() && tokens[pos] == ",") {
                        mods += ",";
                        ++pos;
                    }
                }
                if (pos < tokens.size() && tokens[pos] == ")") {
                    mods += ")";
                    ++pos;
                }
                stmt->options["base_type"] += mods;
            }
        }
    }

    auto lower = [&](const std::string& s) { return toLower(s); };
    std::string constraintName;
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string tok = lower(tokens[pos]);
        if (tok == "default") {
            ++pos;
            std::string expr;
            while (pos < tokens.size() && tokens[pos] != ";" &&
                   lower(tokens[pos]) != "constraint" && lower(tokens[pos]) != "check") {
                if (!expr.empty()) expr += " ";
                expr += tokens[pos++];
            }
            stmt->options["default"] = expr;
        } else if (tok == "constraint") {
            ++pos;
            if (pos < tokens.size()) {
                constraintName = tokens[pos++];
            }
        } else if (tok == "check") {
            if (pos + 1 < tokens.size() && tokens[pos + 1] == "(") {
                pos += 2; // skip check (
                std::string expr;
                int depth = 1;
                while (pos < tokens.size() && depth > 0) {
                    if (tokens[pos] == "(") ++depth;
                    else if (tokens[pos] == ")") --depth;
                    if (depth > 0) {
                        if (!expr.empty()) expr += " ";
                        expr += tokens[pos];
                    }
                    ++pos;
                }
                // Combine multiple CHECK constraints with AND.
                auto it = stmt->options.find("check");
                if (it != stmt->options.end() && !it->second.empty()) {
                    it->second = "(" + it->second + ") AND (" + expr + ")";
                } else {
                    stmt->options["check"] = expr;
                }
                if (!constraintName.empty()) {
                    auto cnIt = stmt->options.find("constraint_name");
                    if (cnIt != stmt->options.end() && !cnIt->second.empty()) {
                        cnIt->second += ";" + constraintName;
                    } else {
                        stmt->options["constraint_name"] = constraintName;
                    }
                    constraintName.clear();
                }
            } else {
                ++pos;
            }
        } else {
            ++pos;
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateType(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateType);
    stmt->objectType = "TYPE";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }

    // CREATE TYPE name AS ENUM ('a', 'b', ...)
    if (pos + 2 < tokens.size() && toLower(tokens[pos]) == "as" && toLower(tokens[pos + 1]) == "enum") {
        pos += 2;
        stmt->options["type_kind"] = "enum";
        if (pos < tokens.size() && tokens[pos] == "(") {
            ++pos;
            std::string labels;
            while (pos < tokens.size() && tokens[pos] != ")") {
                if (tokens[pos] == ",") {
                    ++pos;
                    continue;
                }
                if (!labels.empty()) labels += ",";
                std::string label = tokens[pos++];
                // Strip quotes if present
                if (label.size() >= 2 && label.front() == '\'' && label.back() == '\'') {
                    label = label.substr(1, label.size() - 2);
                }
                labels += label;
            }
            if (pos < tokens.size() && tokens[pos] == ")") ++pos;
            stmt->options["enum_labels"] = labels;
        }
    }
    // CREATE TYPE name AS ( field type [, ...] )  -- composite (ROW) type
    else if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "as" && tokens[pos + 1] == "(") {
        pos += 2; // consume 'as' and '('
        stmt->options["type_kind"] = "composite";
        // Reconstruct a "name type" string per field; fields separated by ';'
        // so that type modifiers containing commas (e.g. numeric(10,2)) survive.
        auto appendTok = [](std::string& out, const std::string& tok) {
            bool noSpaceBefore = (tok == "(" || tok == ")" || tok == ",");
            bool prevNoSpaceAfter = !out.empty() && (out.back() == '(' || out.back() == ',');
            if (!out.empty() && !noSpaceBefore && !prevNoSpaceAfter) out += " ";
            out += tok;
        };
        std::string fields;
        while (pos < tokens.size() && tokens[pos] != ")") {
            if (tokens[pos] == ",") { ++pos; continue; }
            std::string fname = tokens[pos++];
            std::string ftype;
            int depth = 0;
            while (pos < tokens.size() &&
                   (depth > 0 || (tokens[pos] != "," && tokens[pos] != ")"))) {
                const std::string& t = tokens[pos];
                if (t == "(") ++depth;
                else if (t == ")") --depth;
                appendTok(ftype, t);
                ++pos;
            }
            if (!fname.empty() && !ftype.empty()) {
                if (!fields.empty()) fields += ";";
                fields += fname + " " + ftype;
            }
        }
        if (pos < tokens.size() && tokens[pos] == ")") ++pos;
        stmt->options["fields"] = fields;
    }
    // CREATE TYPE name AS RANGE (subtype = subtype_name, ...)
    if (!stmt->options.count("type_kind") && pos + 2 < tokens.size() &&
        toLower(tokens[pos]) == "as" && toLower(tokens[pos + 1]) == "range" && tokens[pos + 2] == "(") {
        pos += 3;
        stmt->options["type_kind"] = "range";
        auto parseKv = [&](const std::string& stopTok) {
            while (pos < tokens.size() && tokens[pos] != stopTok) {
                if (tokens[pos] == ",") { ++pos; continue; }
                std::string k = toLower(tokens[pos++]);
                if (pos < tokens.size() && tokens[pos] == "=") ++pos;
                std::string v;
                while (pos < tokens.size() && tokens[pos] != "," && tokens[pos] != stopTok) {
                    if (!v.empty()) v += " ";
                    v += tokens[pos++];
                }
                if (!k.empty()) stmt->options["range_" + k] = trim(v);
                if (pos < tokens.size() && tokens[pos] == ",") ++pos;
            }
            if (pos < tokens.size() && tokens[pos] == stopTok) ++pos;
        };
        parseKv(")");
    }
    // CREATE TYPE name (INPUT=..., OUTPUT=..., ...) -- base type shell
    else if (!stmt->options.count("type_kind") && pos < tokens.size() && tokens[pos] == "(") {
        ++pos;
        stmt->options["type_kind"] = "base";
        auto parseKv = [&](const std::string& stopTok) {
            while (pos < tokens.size() && tokens[pos] != stopTok) {
                if (tokens[pos] == ",") { ++pos; continue; }
                std::string k = toLower(tokens[pos++]);
                if (pos < tokens.size() && tokens[pos] == "=") ++pos;
                std::string v;
                while (pos < tokens.size() && tokens[pos] != "," && tokens[pos] != stopTok) {
                    if (!v.empty()) v += " ";
                    v += tokens[pos++];
                }
                if (!k.empty()) stmt->options["base_" + k] = trim(v);
                if (pos < tokens.size() && tokens[pos] == ",") ++pos;
            }
            if (pos < tokens.size() && tokens[pos] == stopTok) ++pos;
        };
        parseKv(")");
    }
    // CREATE TYPE name  -- shell type (no AS clause, no parentheses)
    else if (!stmt->options.count("type_kind") && pos >= tokens.size()) {
        stmt->options["type_kind"] = "shell";
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateFunction(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateFunctionStmt>();
    if (pos >= tokens.size()) return stmt;
    stmt->funcName = tokens[pos++];

    // Optional parameter list: (name type [, ...])
    if (pos < tokens.size() && tokens[pos] == "(") {
        ++pos;
        while (pos < tokens.size() && tokens[pos] != ")") {
            if (tokens[pos] == ",") { ++pos; continue; }
            std::string pname = tokens[pos++];
            std::string ptype;
            // Collect type tokens until comma or closing paren, handling typemods like varchar(20).
            int depth = 0;
            while (pos < tokens.size() && (tokens[pos] != "," || depth > 0) &&
                   (tokens[pos] != ")" || depth > 0)) {
                if (tokens[pos] == "(") ++depth;
                else if (tokens[pos] == ")") --depth;
                if (!ptype.empty()) ptype += " ";
                ptype += tokens[pos++];
            }
            if (!pname.empty()) stmt->params.emplace_back(pname, ptype);
            if (pos < tokens.size() && tokens[pos] == ",") ++pos;
        }
        if (pos < tokens.size() && tokens[pos] == ")") ++pos;
    }

    // RETURNS / RETURNS TABLE (...)
    if (match(tokens, pos, "returns")) {
        ++pos;
        if (match(tokens, pos, "table")) {
            stmt->returnType = "table";
            ++pos;
            if (pos < tokens.size() && tokens[pos] == "(") {
                // Skip table column list for now.
                int depth = 1;
                ++pos;
                while (pos < tokens.size() && depth > 0) {
                    if (tokens[pos] == "(") ++depth;
                    else if (tokens[pos] == ")") --depth;
                    if (depth > 0) ++pos;
                }
                if (pos < tokens.size() && tokens[pos] == ")") ++pos;
            }
        } else {
            std::string rtype;
            while (pos < tokens.size() && !match(tokens, pos, "as") &&
                   !match(tokens, pos, "language") && !match(tokens, pos, "immutable") &&
                   !match(tokens, pos, "stable") && !match(tokens, pos, "volatile") &&
                   !match(tokens, pos, "strict") && !match(tokens, pos, "security") &&
                   !match(tokens, pos, "parallel") && !match(tokens, pos, "cost") &&
                   !match(tokens, pos, "rows") && !match(tokens, pos, "set")) {
                if (!rtype.empty()) rtype += " ";
                rtype += tokens[pos++];
            }
            stmt->returnType = rtype;
        }
    }

    // Volatility / strict / security definer / parallel / cost / rows / SET options
    while (pos < tokens.size()) {
        if (match(tokens, pos, "immutable")) { stmt->immutable = true; stmt->volatile_ = false; ++pos; }
        else if (match(tokens, pos, "stable")) { stmt->stable = true; stmt->volatile_ = false; ++pos; }
        else if (match(tokens, pos, "volatile")) { stmt->volatile_ = true; ++pos; }
        else if (match(tokens, pos, "strict") || (match(tokens, pos, "returns") && pos + 1 < tokens.size() && match(tokens, pos + 1, "null") && match(tokens, pos + 2, "on") && match(tokens, pos + 3, "null"))) {
            // Simplified: STRICT keyword only
            stmt->strict = true; ++pos;
        }
        else if (match(tokens, pos, "security") && pos + 1 < tokens.size() && match(tokens, pos + 1, "definer")) {
            stmt->securityDefiner = true; pos += 2;
        }
        else if (match(tokens, pos, "leakproof")) { stmt->leakproof = true; ++pos; }
        else if (match(tokens, pos, "parallel") && pos + 1 < tokens.size()) {
            ++pos;
            if (match(tokens, pos, "safe")) { stmt->parallelSafe = true; ++pos; }
            else if (match(tokens, pos, "restricted")) { stmt->parallelRestricted = true; ++pos; }
            else if (match(tokens, pos, "unsafe")) { stmt->parallelUnsafe = true; ++pos; }
        }
        else if (match(tokens, pos, "cost")) {
            ++pos;
            if (pos >= tokens.size()) return nullptr;
            if (!parsePositiveDouble(tokens[pos], stmt->cost)) return nullptr;
            ++pos;
        }
        else if (match(tokens, pos, "rows")) {
            ++pos;
            if (pos >= tokens.size()) return nullptr;
            if (!parsePositiveDouble(tokens[pos], stmt->rows)) return nullptr;
            ++pos;
        }
        else if (match(tokens, pos, "set") && pos + 2 < tokens.size()) {
            ++pos;
            std::string item = tokens[pos++];
            if (pos < tokens.size() && tokens[pos] == "=") ++pos;
            if (pos < tokens.size()) item += "=" + tokens[pos++];
            stmt->setItems.push_back(item);
        }
        else {
            break;
        }
    }

    // AS body
    if (match(tokens, pos, "as")) {
        ++pos;
        if (pos < tokens.size()) {
            stmt->body = stripQuotes(tokens[pos]);
            ++pos;
        }
    }

    // LANGUAGE lang
    if (match(tokens, pos, "language") && pos + 1 < tokens.size()) {
        stmt->language = toLower(tokens[pos + 1]);
        pos += 2;
    }

    return stmt;
}

StmtPtr SQLParser::parseCreateProcedure(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateFunctionStmt>(true);
    if (pos >= tokens.size()) return stmt;
    stmt->funcName = tokens[pos++];

    if (pos < tokens.size() && tokens[pos] == "(") {
        ++pos;
        while (pos < tokens.size() && tokens[pos] != ")") {
            if (tokens[pos] == ",") { ++pos; continue; }
            std::string pname = tokens[pos++];
            std::string ptype;
            int depth = 0;
            while (pos < tokens.size() && (tokens[pos] != "," || depth > 0) &&
                   (tokens[pos] != ")" || depth > 0)) {
                if (tokens[pos] == "(") ++depth;
                else if (tokens[pos] == ")") --depth;
                if (!ptype.empty()) ptype += " ";
                ptype += tokens[pos++];
            }
            if (!pname.empty()) stmt->params.emplace_back(pname, ptype);
            if (pos < tokens.size() && tokens[pos] == ",") ++pos;
        }
        if (pos < tokens.size() && tokens[pos] == ")") ++pos;
    }

    if (match(tokens, pos, "as")) {
        ++pos;
        if (pos < tokens.size()) {
            stmt->body = stripQuotes(tokens[pos]);
            ++pos;
        }
    }
    if (match(tokens, pos, "language") && pos + 1 < tokens.size()) {
        stmt->language = toLower(tokens[pos + 1]);
        pos += 2;
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateTrigger(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateTriggerStmt>();
    if (pos >= tokens.size()) return stmt;
    stmt->triggerName = tokens[pos++];

    // Timing: BEFORE / AFTER / INSTEAD OF
    if (match(tokens, pos, "before")) {
        stmt->timing = "before";
        ++pos;
    } else if (match(tokens, pos, "after")) {
        stmt->timing = "after";
        ++pos;
    } else if (match(tokens, pos, "instead") && match(tokens, pos + 1, "of")) {
        stmt->timing = "instead of";
        pos += 2;
    }

    // Event: INSERT / UPDATE [OF cols] / DELETE / TRUNCATE
    if (match(tokens, pos, "insert")) {
        stmt->events.push_back("insert");
        ++pos;
    } else if (match(tokens, pos, "update")) {
        stmt->events.push_back("update");
        ++pos;
        if (match(tokens, pos, "of")) {
            ++pos;
            while (pos < tokens.size() && !match(tokens, pos, "on") &&
                   !match(tokens, pos, "for") && !match(tokens, pos, "when") &&
                   tokens[pos] != "(") {
                if (tokens[pos] != ",") stmt->events.push_back(tokens[pos]);
                ++pos;
            }
        }
    } else if (match(tokens, pos, "delete")) {
        stmt->events.push_back("delete");
        ++pos;
    } else if (match(tokens, pos, "truncate")) {
        stmt->events.push_back("truncate");
        ++pos;
    }

    // ON tableName
    if (match(tokens, pos, "on")) ++pos;
    if (pos < tokens.size()) {
        stmt->tableName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) stmt->tableName += "." + tokens[pos++];
        }
    }

    // FOR EACH ROW / STATEMENT
    if (match(tokens, pos, "for") && match(tokens, pos + 1, "each")) {
        pos += 2;
        if (match(tokens, pos, "row")) {
            stmt->forEachRow = true;
            ++pos;
        } else if (match(tokens, pos, "statement")) {
            stmt->forEachRow = false;
            ++pos;
        }
    }

    // WHEN (condition)
    if (match(tokens, pos, "when") && pos + 1 < tokens.size() && tokens[pos + 1] == "(") {
        pos += 2;
        std::string cond;
        int depth = 1;
        while (pos < tokens.size() && depth > 0) {
            if (tokens[pos] == "(") ++depth;
            else if (tokens[pos] == ")") --depth;
            if (depth > 0) {
                if (!cond.empty()) cond += " ";
                cond += tokens[pos];
            }
            ++pos;
        }
        if (!cond.empty()) {
            auto lit = std::make_unique<LiteralExpr>();
            lit->value = cond;
            lit->typeName = "varchar";
            stmt->whenCondition = std::move(lit);
        }
    }

    // EXECUTE FUNCTION/PROCEDURE name(args) OR remaining tokens as action SQL
    if (match(tokens, pos, "execute")) {
        ++pos;
        if (match(tokens, pos, "function") || match(tokens, pos, "procedure")) ++pos;
        if (pos < tokens.size()) {
            stmt->functionName = tokens[pos];
            // If next token is '(', consume args as raw string for now.
            if (pos + 1 < tokens.size() && tokens[pos + 1] == "(") {
                pos += 2;
                std::string args;
                int depth = 1;
                while (pos < tokens.size() && depth > 0) {
                    if (tokens[pos] == "(") ++depth;
                    else if (tokens[pos] == ")") --depth;
                    if (depth > 0) {
                        if (!args.empty()) args += " ";
                        args += tokens[pos];
                    }
                    ++pos;
                }
                // Store raw args as action for compatibility with the legacy trigger engine.
                stmt->action = stmt->functionName + "(" + args + ")";
            } else {
                ++pos;
            }
        }
    } else {
        // Remaining tokens form the action SQL (legacy style).
        std::string action;
        while (pos < tokens.size()) {
            if (!action.empty()) action += " ";
            action += tokens[pos++];
        }
        stmt->action = action;
    }

    return stmt;
}

StmtPtr SQLParser::parseCreateRole(const std::vector<std::string>& tokens, size_t& pos, bool isUser) {
    auto stmt = std::make_unique<CreateRoleStmt>();
    stmt->isUser = isUser;
    // PostgreSQL treats CREATE USER as CREATE ROLE ... LOGIN by default;
    // explicit NOLOGIN still overrides this below.
    stmt->login = isUser;
    // roleName is at current pos (kw "role"/"user" already consumed by caller).
    if (pos < tokens.size()) {
        stmt->roleName = tokens[pos++];
    }
    // Parse optional WITH and role attributes.
    if (pos < tokens.size() && toLower(tokens[pos]) == "with") ++pos;
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string kw = toLower(tokens[pos]);
        if (kw == "superuser") { stmt->superuser = true; ++pos; }
        else if (kw == "nosuperuser") { stmt->superuser = false; ++pos; }
        else if (kw == "createdb") { stmt->createdb = true; ++pos; }
        else if (kw == "nocreatedb") { stmt->createdb = false; ++pos; }
        else if (kw == "createrole") { stmt->createrole = true; ++pos; }
        else if (kw == "nocreaterole") { stmt->createrole = false; ++pos; }
        else if (kw == "inherit") { stmt->inherit = true; ++pos; }
        else if (kw == "noinherit") { stmt->inherit = false; ++pos; }
        else if (kw == "login") { stmt->login = true; ++pos; }
        else if (kw == "nologin") { stmt->login = false; ++pos; }
        else if (kw == "replication") { stmt->replication = true; ++pos; }
        else if (kw == "noreplication") { stmt->replication = false; ++pos; }
        else if (kw == "bypassrls") { stmt->bypassrls = true; ++pos; }
        else if (kw == "nobypassrls") { stmt->bypassrls = false; ++pos; }
        else if (kw == "connection") {
            if (pos + 2 >= tokens.size() || toLower(tokens[pos + 1]) != "limit") return nullptr;
            if (!parseSignedInteger(tokens[pos + 2], stmt->connectionLimit) || stmt->connectionLimit < -1) {
                return nullptr;
            }
            pos += 3;
        } else if (kw == "password") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->password = stripQuotes(tokens[pos]);
                ++pos;
            }
        } else if (kw == "valid" && pos + 2 < tokens.size() && toLower(tokens[pos + 1]) == "until") {
            stmt->validUntil = stripQuotes(tokens[pos + 2]);
            pos += 3;
        } else if (kw == "in" && pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "role") {
            pos += 2;
            while (pos < tokens.size() && tokens[pos] != ";" &&
                   toLower(tokens[pos]) != "login" && toLower(tokens[pos]) != "nologin" &&
                   toLower(tokens[pos]) != "superuser" && toLower(tokens[pos]) != "nosuperuser" &&
                   toLower(tokens[pos]) != "connection" && toLower(tokens[pos]) != "password" &&
                   toLower(tokens[pos]) != "valid") {
                stmt->inRole.push_back({tokens[pos], false});
                if (pos + 1 < tokens.size() && tokens[pos + 1] == ",") pos += 2;
                else ++pos;
            }
        } else {
            ++pos;  // skip unknown
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateTablespace(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateTablespace);
    stmt->objectType = "TABLESPACE";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateStatistics(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateStatistics);
    stmt->objectType = "STATISTICS";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreatePolicy(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreatePolicyStmt>();
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->policyName = tokens[pos++];
    }
    if (match(tokens, pos, "on") && pos + 1 < tokens.size()) {
        ++pos;
        stmt->tableName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) stmt->tableName += "." + tokens[pos++];
        }
    }

    // [AS {PERMISSIVE|RESTRICTIVE}]
    if (match(tokens, pos, "as") && pos + 1 < tokens.size()) {
        ++pos;
        if (match(tokens, pos, "permissive")) {
            stmt->permissive = true;
            ++pos;
        } else if (match(tokens, pos, "restrictive")) {
            stmt->permissive = false;
            ++pos;
        }
    }

    // [FOR cmd]
    if (match(tokens, pos, "for") && pos + 1 < tokens.size()) {
        ++pos;
        stmt->command = toUpper(tokens[pos++]);
    }

    // [TO role, ...]
    if (match(tokens, pos, "to")) {
        ++pos;
        while (pos < tokens.size() && !match(tokens, pos, "using") && !match(tokens, pos, "with")) {
            if (tokens[pos] != ",") stmt->roles.push_back(tokens[pos]);
            ++pos;
        }
    }

    // USING [(]expr[)]
    if (match(tokens, pos, "using")) {
        ++pos;
        std::string expr;
        if (pos < tokens.size() && tokens[pos] == "(") {
            ++pos;
            int depth = 1;
            while (pos < tokens.size() && depth > 0) {
                if (tokens[pos] == "(") ++depth;
                else if (tokens[pos] == ")") --depth;
                if (depth > 0) {
                    if (!expr.empty()) expr += " ";
                    expr += tokens[pos];
                }
                ++pos;
            }
        } else {
            while (pos < tokens.size() && !match(tokens, pos, "with")) {
                if (!expr.empty()) expr += " ";
                expr += tokens[pos++];
            }
        }
        stmt->usingExpr = stripQuotes(expr);
    }

    // WITH CHECK [(]expr[)]
    if (match(tokens, pos, "with")) {
        if (pos + 1 < tokens.size() && match(tokens, pos + 1, "check")) pos += 2;
        else ++pos;
        std::string expr;
        if (pos < tokens.size() && tokens[pos] == "(") {
            ++pos;
            int depth = 1;
            while (pos < tokens.size() && depth > 0) {
                if (tokens[pos] == "(") ++depth;
                else if (tokens[pos] == ")") --depth;
                if (depth > 0) {
                    if (!expr.empty()) expr += " ";
                    expr += tokens[pos];
                }
                ++pos;
            }
        } else {
            while (pos < tokens.size()) {
                if (!expr.empty()) expr += " ";
                expr += tokens[pos++];
            }
        }
        stmt->withCheckExpr = stripQuotes(expr);
    }

    return stmt;
}

StmtPtr SQLParser::parseCreateRule(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateRule);
    stmt->objectType = "RULE";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateEventTrigger(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateEventTrigger);
    stmt->objectType = "EVENT TRIGGER";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateExtension(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateExtension);
    stmt->objectType = "EXTENSION";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreatePublication(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreatePublication);
    stmt->objectType = "PUBLICATION";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateSubscription(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateSubscription);
    stmt->objectType = "SUBSCRIPTION";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateAccessMethod(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateAccessMethod);
    stmt->objectType = "ACCESS METHOD";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateForeignDataWrapper(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateForeignDataWrapper);
    stmt->objectType = "FOREIGN DATA WRAPPER";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateForeignTable(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateForeignTable);
    stmt->objectType = "FOREIGN TABLE";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateServer(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateServer);
    stmt->objectType = "SERVER";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateUserMapping(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateUserMapping);
    stmt->objectType = "USER MAPPING";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateCast(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateCast);
    stmt->objectType = "CAST";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateCollation(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateCollation);
    stmt->objectType = "COLLATION";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    // Parse optional FROM / provider locale specification
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string kw = toLower(tokens[pos]);
        if (kw == "from" && pos + 1 < tokens.size()) {
            ++pos;
            std::string rest;
            while (pos < tokens.size() && tokens[pos] != ";") {
                if (!rest.empty()) rest += " ";
                rest += tokens[pos++];
            }
            // Try to extract provider= and locale= from the rest
            std::istringstream iss(rest);
            std::string tok;
            while (iss >> tok) {
                std::string lower = toLower(tok);
                if (lower.substr(0, 8) == "provider" && lower.size() > 9 && lower[8] == '=') {
                    stmt->options["provider"] = lower.substr(9);
                } else if (lower.substr(0, 6) == "locale" && lower.size() > 7 && lower[6] == '=') {
                    stmt->options["locale"] = lower.substr(7);
                }
            }
        } else {
            ++pos;
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateConversion(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateConversion);
    stmt->objectType = "CONVERSION";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateOperator(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateOperator);
    stmt->objectType = "OPERATOR";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateOperatorClass(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateOperatorClass);
    stmt->objectType = "OPERATOR CLASS";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateOperatorFamily(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateOperatorFamily);
    stmt->objectType = "OPERATOR FAMILY";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateAggregate(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateAggregate);
    stmt->objectType = "AGGREGATE";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateTransform(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateTransform);
    stmt->objectType = "TRANSFORM";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateLanguage(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateLanguage);
    stmt->objectType = "LANGUAGE";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateTextSearchConfiguration(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateTextSearchConfiguration);
    stmt->objectType = "TEXT SEARCH CONFIGURATION";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateTextSearchDictionary(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateTextSearchDictionary);
    stmt->objectType = "TEXT SEARCH DICTIONARY";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateTextSearchParser(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateTextSearchParser);
    stmt->objectType = "TEXT SEARCH PARSER";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateTextSearchTemplate(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateTextSearchTemplate);
    stmt->objectType = "TEXT SEARCH TEMPLATE";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    return stmt;
}

// ============================================================================
// DROP 子命令解析 stub
// ============================================================================

StmtPtr SQLParser::parseDropTable(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropTable);
    stmt->objectType = "TABLE";
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (toLower(tokens[pos]) == "cascade") {
            stmt->cascade = true; ++pos; continue;
        }
        if (toLower(tokens[pos]) == "restrict") {
            ++pos; continue;
        }
        if (toLower(tokens[pos]) == "if" && pos + 2 < tokens.size() && toLower(tokens[pos + 1]) == "exists") {
            stmt->ifExists = true; pos += 2; continue;
        }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropIndex(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropIndex);
    stmt->objectType = "INDEX";
    if (pos < tokens.size() && toLower(tokens[pos]) == "concurrently") {
        stmt->concurrently = true; ++pos;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "if" && pos + 2 < tokens.size() && toLower(tokens[pos + 1]) == "exists") {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string word = toLower(tokens[pos]);
        if (word == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (word == "restrict" || tokens[pos] == ",") { ++pos; continue; }
        if (word == "on") {
            ++pos;
            if (pos >= tokens.size() || tokens[pos] == ";") break;
            stmt->tableName = tokens[pos++];
            if (pos < tokens.size() && tokens[pos] == ".") {
                ++pos;
                if (pos < tokens.size() && tokens[pos] != ";") {
                    stmt->tableName += "." + tokens[pos++];
                }
            }
            continue;
        }
        std::string name = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size() && tokens[pos] != ";") name += "." + tokens[pos++];
        }
        stmt->objectNames.push_back(std::move(name));
    }
    return stmt;
}

StmtPtr SQLParser::parseDropView(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropView);
    stmt->objectType = "VIEW";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropMaterializedView(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropMaterializedView);
    stmt->objectType = "MATERIALIZED VIEW";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropDatabase(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropDatabase);
    stmt->objectType = "DATABASE";
    if (pos < tokens.size()) stmt->objectNames.push_back(tokens[pos++]);
    return stmt;
}

StmtPtr SQLParser::parseDropSchema(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropSchema);
    stmt->objectType = "SCHEMA";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropSequence(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropSequence);
    stmt->objectType = "SEQUENCE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropDomain(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropDomain);
    stmt->objectType = "DOMAIN";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropType(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropType);
    stmt->objectType = "TYPE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropFunction(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropFunction);
    stmt->objectType = "FUNCTION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropProcedure(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropProcedure);
    stmt->objectType = "PROCEDURE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropRoutine(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropRoutine);
    stmt->objectType = "ROUTINE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropTrigger(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropTrigger);
    stmt->objectType = "TRIGGER";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropRole(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropRole);
    stmt->objectType = "ROLE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropUser(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropUser);
    stmt->objectType = "USER";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropTablespace(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropTablespace);
    stmt->objectType = "TABLESPACE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropStatistics(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropStatistics);
    stmt->objectType = "STATISTICS";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropPolicy(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropPolicy);
    stmt->objectType = "POLICY";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropRule(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropRule);
    stmt->objectType = "RULE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropEventTrigger(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropEventTrigger);
    stmt->objectType = "EVENT TRIGGER";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropExtension(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropExtension);
    stmt->objectType = "EXTENSION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropPublication(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropPublication);
    stmt->objectType = "PUBLICATION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropSubscription(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropSubscription);
    stmt->objectType = "SUBSCRIPTION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropAccessMethod(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropAccessMethod);
    stmt->objectType = "ACCESS METHOD";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropForeignDataWrapper(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropForeignDataWrapper);
    stmt->objectType = "FOREIGN DATA WRAPPER";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropForeignTable(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropForeignTable);
    stmt->objectType = "FOREIGN TABLE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropServer(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropServer);
    stmt->objectType = "SERVER";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropUserMapping(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropUserMapping);
    stmt->objectType = "USER MAPPING";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropCast(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropCast);
    stmt->objectType = "CAST";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropCollation(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropCollation);
    stmt->objectType = "COLLATION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropConversion(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropConversion);
    stmt->objectType = "CONVERSION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropOperator(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropOperator);
    stmt->objectType = "OPERATOR";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropOperatorClass(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropOperatorClass);
    stmt->objectType = "OPERATOR CLASS";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropOperatorFamily(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropOperatorFamily);
    stmt->objectType = "OPERATOR FAMILY";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropAggregate(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropAggregate);
    stmt->objectType = "AGGREGATE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropTransform(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropTransform);
    stmt->objectType = "TRANSFORM";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropLanguage(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropLanguage);
    stmt->objectType = "LANGUAGE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropTextSearchConfiguration(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropTextSearchConfiguration);
    stmt->objectType = "TEXT SEARCH CONFIGURATION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropTextSearchDictionary(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropTextSearchDictionary);
    stmt->objectType = "TEXT SEARCH DICTIONARY";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropTextSearchParser(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropTextSearchParser);
    stmt->objectType = "TEXT SEARCH PARSER";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropTextSearchTemplate(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropTextSearchTemplate);
    stmt->objectType = "TEXT SEARCH TEMPLATE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropOwned(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropOwned);
    stmt->objectType = "OWNED";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

StmtPtr SQLParser::parseDropLargeObject(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropLargeObject);
    stmt->objectType = "LARGE OBJECT";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string w = toLower(tokens[pos]);
        if (w == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (w == "restrict") { ++pos; continue; }
        if (w == ",") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
    }
    return stmt;
}

// ============================================================================
// ALTER 子命令解析 stub
// ============================================================================

StmtPtr SQLParser::parseAlterTable(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterTableStmt>();
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && match(tokens, pos, "only")) {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->tableName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                // schema.table; keep qualified name for now
                stmt->tableName += "." + tokens[pos++];
            }
        }
    }

    while (pos < tokens.size() && tokens[pos] != ";") {
        AlterTableStmt::SubCmd sub;
        bool recognized = false;
        std::string kw = toLower(tokens[pos]);
        if (kw == "add" && pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "column") {
            recognized = true;
            sub.action = AlterTableStmt::Action::AddColumn; pos += 2;
            if (pos + 2 < tokens.size() && toLower(tokens[pos]) == "if" &&
                toLower(tokens[pos + 1]) == "not" && toLower(tokens[pos + 2]) == "exists") {
                sub.ifNotExists = true;
                pos += 3;
            }
            if (pos < tokens.size()) sub.colDef.name = tokens[pos++];
            if (pos < tokens.size()) {
                sub.colDef.typeName = tokens[pos++];
                std::string type = toLower(sub.colDef.typeName);
                if ((type == "double" || type == "character" || type == "timestamp") &&
                    pos < tokens.size() && toLower(tokens[pos]) == "precision") {
                    sub.colDef.typeName += " " + tokens[pos++];
                } else if (type == "character" && pos < tokens.size() &&
                           toLower(tokens[pos]) == "varying") {
                    sub.colDef.typeName += " " + tokens[pos++];
                }
                if (pos < tokens.size() && tokens[pos] == "(") {
                    auto mods = collectParenthesized(tokens, pos);
                    for (const auto& mod : mods) if (mod != ",") sub.colDef.typeMods.push_back(mod);
                }
            }
            // Keep the common column modifiers in the AST. Unsupported
            // modifiers are left for the caller to reject rather than being
            // silently treated as a different column definition.
            while (pos < tokens.size() && tokens[pos] != "," && tokens[pos] != ";") {
                std::string modifier = toLower(tokens[pos]);
                if (modifier == "not" && pos + 1 < tokens.size() &&
                    toLower(tokens[pos + 1]) == "null") {
                    sub.colDef.isNull = false;
                    pos += 2;
                } else if (modifier == "default") {
                    ++pos;
                    sub.colDef.defaultValue = parseSimpleExpr(tokens, pos);
                } else {
                    ++pos;
                }
            }
        } else if (kw == "add") {
            recognized = true;
            sub.action = AlterTableStmt::Action::AddConstraint; ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "constraint") {
                ++pos;
                if (pos < tokens.size()) sub.constraint.name = tokens[pos++];
            }
            if (pos < tokens.size()) {
                sub.constraint.type = toUpper(tokens[pos]); ++pos;
                if (sub.constraint.type == "PRIMARY") { sub.constraint.type = "PRIMARY KEY"; ++pos; }
                if (sub.constraint.type == "FOREIGN") { sub.constraint.type = "FOREIGN KEY"; ++pos; }
                if (sub.constraint.type == "EXCLUDE") {
                    const std::string constraintName = sub.constraint.name;
                    sub.constraint = parseExcludeConstraint(tokens, pos);
                    sub.constraint.name = constraintName;
                    sub.constraint.type = "EXCLUDE";
                }
            }
            if (sub.constraint.type != "EXCLUDE" && pos < tokens.size() && tokens[pos] == "(") {
                if (sub.constraint.type == "CHECK") {
                    ++pos;
                    sub.constraint.checkExpr = parseSimpleExpr(tokens, pos);
                    if (pos < tokens.size() && tokens[pos] == ")") ++pos;
                } else {
                    auto cols = collectParenthesized(tokens, pos);
                    for (const auto& c : cols) if (c != ",") sub.constraint.columns.push_back(c);
                }
            }
            if (pos < tokens.size() && toLower(tokens[pos]) == "references") {
                ++pos;
                if (pos < tokens.size()) sub.constraint.refTable = tokens[pos++];
                if (pos < tokens.size() && tokens[pos] == "(") {
                    auto refcols = collectParenthesized(tokens, pos);
                    for (const auto& c : refcols) if (c != ",") sub.constraint.refColumns.push_back(c);
                }
            }
            while (pos < tokens.size() && tokens[pos] != "," && tokens[pos] != ";") {
                const std::string option = toLower(tokens[pos]);
                if (option == "not" && pos + 1 < tokens.size() &&
                    toLower(tokens[pos + 1]) == "valid") {
                    sub.constraint.notValid = true;
                    pos += 2;
                } else if (option == "deferrable") {
                    sub.constraint.deferrable = true;
                    ++pos;
                } else if (option == "not" && pos + 1 < tokens.size() &&
                           toLower(tokens[pos + 1]) == "deferrable") {
                    sub.constraint.deferrable = false;
                    sub.constraint.initiallyDeferred = false;
                    pos += 2;
                } else if (option == "initially" && pos + 1 < tokens.size()) {
                    const std::string mode = toLower(tokens[pos + 1]);
                    if (mode != "deferred" && mode != "immediate") break;
                    sub.constraint.initiallyDeferred = mode == "deferred";
                    if (sub.constraint.initiallyDeferred) sub.constraint.deferrable = true;
                    pos += 2;
                } else {
                    ++pos;
                }
            }
        } else if (kw == "drop" && pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "column") {
            recognized = true;
            sub.action = AlterTableStmt::Action::DropColumn; pos += 2;
            if (pos < tokens.size() && toLower(tokens[pos]) == "if") {
                sub.ifExists = true;
                pos += 3; // IF EXISTS
            }
            if (pos < tokens.size()) sub.name = tokens[pos++];
            if (pos < tokens.size() && toLower(tokens[pos]) == "cascade") { sub.options["cascade"] = "true"; ++pos; }
        } else if (kw == "drop") {
            recognized = true;
            sub.action = AlterTableStmt::Action::DropConstraint; ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "constraint") ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "if") {
                sub.ifExists = true;
                pos += 2; // IF EXISTS
            }
            if (pos < tokens.size()) sub.name = tokens[pos++];
            if (pos < tokens.size() && toLower(tokens[pos]) == "cascade") { sub.options["cascade"] = "true"; ++pos; }
        } else if (kw == "alter" && pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "column") {
            recognized = true;
            sub.action = AlterTableStmt::Action::AlterColumn; pos += 2;
            if (pos < tokens.size()) sub.name = tokens[pos++];
            if (pos < tokens.size() && toLower(tokens[pos]) == "set") {
                ++pos;
                if (pos < tokens.size() && toLower(tokens[pos]) == "default") {
                    ++pos;
                    sub.defaultValue = parseSimpleExpr(tokens, pos);
                } else if (pos < tokens.size() && toLower(tokens[pos]) == "not") {
                    pos += 2; // NOT NULL
                    sub.setNotNull = true;
                } else if (pos < tokens.size() && toLower(tokens[pos]) == "data") {
                    pos += 2; // DATA TYPE
                    if (pos < tokens.size()) {
                        sub.dataType = tokens[pos++];
                        if ((toLower(sub.dataType) == "double" ||
                             toLower(sub.dataType) == "character" ||
                             toLower(sub.dataType) == "timestamp") &&
                            pos < tokens.size() && toLower(tokens[pos]) == "precision") {
                            sub.dataType += " " + tokens[pos++];
                        } else if (toLower(sub.dataType) == "character" &&
                                   pos < tokens.size() && toLower(tokens[pos]) == "varying") {
                            sub.dataType += " " + tokens[pos++];
                        }
                        if (pos < tokens.size() && tokens[pos] == "(") {
                            sub.dataType += "(";
                            ++pos;
                            while (pos < tokens.size() && tokens[pos] != ")") {
                                if (tokens[pos] != ",") sub.dataType += tokens[pos];
                                ++pos;
                            }
                            if (pos < tokens.size() && tokens[pos] == ")") {
                                sub.dataType += ")";
                                ++pos;
                            }
                        }
                    }
                } else if (pos < tokens.size() && toLower(tokens[pos]) == "statistics") {
                    ++pos;
                    sub.action = AlterTableStmt::Action::SetStatistics;
                    if (pos < tokens.size()) {
                        int target = 0;
                        if (!parseSignedInteger(tokens[pos], target) || target < 0) return nullptr;
                        sub.statisticsTarget = target;
                        ++pos;
                    } else return nullptr;
                }
            } else if (pos < tokens.size() && toLower(tokens[pos]) == "drop") {
                ++pos;
                if (pos < tokens.size() && toLower(tokens[pos]) == "default") { sub.dropDefault = true; ++pos; }
                else if (pos < tokens.size() && toLower(tokens[pos]) == "not") { sub.dropNotNull = true; pos += 2; }
            } else if (pos < tokens.size() && toLower(tokens[pos]) == "type") {
                ++pos;
                if (pos < tokens.size()) sub.dataType = tokens[pos++];
            }
        } else if (kw == "rename") {
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "constraint") {
                recognized = true;
                sub.action = AlterTableStmt::Action::RenameConstraint;
                ++pos;
                if (pos + 2 < tokens.size() && toLower(tokens[pos]) == "if" &&
                    toLower(tokens[pos + 1]) == "exists") { sub.ifExists = true; pos += 2; }
                if (pos < tokens.size()) sub.name = tokens[pos++];
                if (pos < tokens.size() && toLower(tokens[pos]) == "to") ++pos;
                if (pos < tokens.size()) sub.newName = tokens[pos++];
            } else {
                bool explicitColumn = pos < tokens.size() && toLower(tokens[pos]) == "column";
                if (explicitColumn) ++pos;
                if (!explicitColumn && pos < tokens.size() && toLower(tokens[pos]) == "to") {
                    ++pos;
                    if (pos < tokens.size()) sub.newName = tokens[pos++];
                    sub.action = AlterTableStmt::Action::RenameTable;
                    recognized = true;
                } else {
                    if (pos + 2 < tokens.size() && toLower(tokens[pos]) == "if" &&
                        toLower(tokens[pos + 1]) == "exists") { sub.ifExists = true; pos += 2; }
                    if (pos < tokens.size()) sub.name = tokens[pos++];
                    if (pos < tokens.size() && toLower(tokens[pos]) == "to") ++pos;
                    if (pos < tokens.size()) sub.newName = tokens[pos++];
                    sub.action = AlterTableStmt::Action::RenameColumn;
                    recognized = true;
                }
            }
        } else if (kw == "validate" && pos + 1 < tokens.size() &&
                   toLower(tokens[pos + 1]) == "constraint") {
            recognized = true;
            sub.action = AlterTableStmt::Action::ValidateConstraint;
            pos += 2;
            if (pos < tokens.size()) sub.name = tokens[pos++];
        } else if (kw == "alter" && pos + 1 < tokens.size() &&
                   toLower(tokens[pos + 1]) == "constraint") {
            recognized = true;
            sub.action = AlterTableStmt::Action::AlterConstraint;
            pos += 2;
            if (pos < tokens.size()) sub.name = tokens[pos++];
            while (pos < tokens.size() && tokens[pos] != "," && tokens[pos] != ";") {
                const std::string option = toLower(tokens[pos]);
                if (option == "deferrable") {
                    sub.setDeferrable = true;
                    sub.deferrable = true;
                    ++pos;
                } else if (option == "not" && pos + 1 < tokens.size() &&
                           toLower(tokens[pos + 1]) == "deferrable") {
                    sub.setDeferrable = true;
                    sub.deferrable = false;
                    pos += 2;
                } else if (option == "initially" && pos + 1 < tokens.size()) {
                    const std::string mode = toLower(tokens[pos + 1]);
                    if (mode != "deferred" && mode != "immediate") break;
                    sub.setInitiallyDeferred = true;
                    sub.initiallyDeferred = mode == "deferred";
                    pos += 2;
                } else {
                    ++pos;
                }
            }
        } else if (kw == "cluster" && pos + 1 < tokens.size() &&
                   toLower(tokens[pos + 1]) == "on") {
            recognized = true;
            sub.action = AlterTableStmt::Action::ClusterOn;
            pos += 2;
            if (pos < tokens.size()) sub.name = tokens[pos++];
        } else if (kw == "set") {
            ++pos;
            if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "without" &&
                       toLower(tokens[pos + 1]) == "cluster") {
                sub.action = AlterTableStmt::Action::SetWithoutCluster;
                pos += 2; recognized = true;
            } else if (pos < tokens.size() && toLower(tokens[pos]) == "logged") {
                sub.action = AlterTableStmt::Action::SetLogged; ++pos; recognized = true;
            } else if (pos < tokens.size() && toLower(tokens[pos]) == "unlogged") {
                sub.action = AlterTableStmt::Action::SetUnlogged; ++pos; recognized = true;
            } else if (pos < tokens.size() && toLower(tokens[pos]) == "schema") {
                sub.action = AlterTableStmt::Action::SetSchema; ++pos;
                if (pos < tokens.size()) sub.newName = tokens[pos++];
                recognized = true;
            } else if (pos < tokens.size() && toLower(tokens[pos]) == "tablespace") {
                sub.action = AlterTableStmt::Action::SetTablespace; ++pos;
                if (pos < tokens.size()) sub.newName = tokens[pos++];
                recognized = true;
            } else if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "with" && tokens[pos + 1] == "(") {
                sub.action = AlterTableStmt::Action::SetOptions; pos += 2;
                recognized = true;
                auto opts = collectParenthesized(tokens, pos);
                for (size_t i = 0; i < opts.size(); i += 2) {
                    if (i + 1 < opts.size() && opts[i + 1] == "=") {
                        if (i + 2 < opts.size()) { sub.options[opts[i]] = opts[i + 2]; i += 2; }
                    } else if (i + 1 < opts.size()) {
                        sub.options[opts[i]] = opts[i + 1];
                    }
                }
            }
        } else if (kw == "replica" && pos + 1 < tokens.size() &&
                   toLower(tokens[pos + 1]) == "identity") {
            recognized = true;
            sub.action = AlterTableStmt::Action::SetReplicaIdentity;
            pos += 2;
            if (pos < tokens.size()) {
                const std::string mode = toLower(tokens[pos++]);
                if (mode == "using" && pos + 1 < tokens.size() &&
                    toLower(tokens[pos]) == "index") {
                    pos += 1;
                    sub.replicaIdentity = "index";
                    if (pos < tokens.size()) sub.name = tokens[pos++];
                } else if (mode == "default" || mode == "full" || mode == "nothing") {
                    sub.replicaIdentity = mode;
                }
            }
        } else if (kw == "reset") {
            recognized = true;
            sub.action = AlterTableStmt::Action::ResetOptions; ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "(") {
                auto opts = collectParenthesized(tokens, pos);
                for (const auto& o : opts) if (o != ",") sub.options[o] = "";
            }
        } else if (kw == "enable" && pos + 3 < tokens.size() &&
                   toLower(tokens[pos + 1]) == "row" &&
                   toLower(tokens[pos + 2]) == "level" &&
                   toLower(tokens[pos + 3]) == "security") {
            recognized = true;
            sub.action = AlterTableStmt::Action::EnableRowLevelSecurity;
            pos += 4;
        } else if (kw == "disable" && pos + 3 < tokens.size() &&
                   toLower(tokens[pos + 1]) == "row" &&
                   toLower(tokens[pos + 2]) == "level" &&
                   toLower(tokens[pos + 3]) == "security") {
            recognized = true;
            sub.action = AlterTableStmt::Action::DisableRowLevelSecurity;
            pos += 4;
        } else if (kw == "force" && pos + 3 < tokens.size() &&
                   toLower(tokens[pos + 1]) == "row" &&
                   toLower(tokens[pos + 2]) == "level" &&
                   toLower(tokens[pos + 3]) == "security") {
            recognized = true;
            sub.action = AlterTableStmt::Action::ForceRowLevelSecurity;
            pos += 4;
        } else if (kw == "no" && pos + 4 < tokens.size() &&
                   toLower(tokens[pos + 1]) == "force" &&
                   toLower(tokens[pos + 2]) == "row" &&
                   toLower(tokens[pos + 3]) == "level" &&
                   toLower(tokens[pos + 4]) == "security") {
            recognized = true;
            sub.action = AlterTableStmt::Action::NoForceRowLevelSecurity;
            pos += 5;
        } else if (kw == "enable" && pos + 1 < tokens.size() &&
                   toLower(tokens[pos + 1]) == "trigger") {
            recognized = true;
            sub.action = AlterTableStmt::Action::EnableTrigger;
            pos += 2;
            if (pos < tokens.size()) sub.name = tokens[pos++];
        } else if (kw == "disable" && pos + 1 < tokens.size() &&
                   toLower(tokens[pos + 1]) == "trigger") {
            recognized = true;
            sub.action = AlterTableStmt::Action::DisableTrigger;
            pos += 2;
            if (pos < tokens.size()) sub.name = tokens[pos++];
        } else if (kw == "attach") {
            recognized = true;
            sub.action = AlterTableStmt::Action::AttachPartition; pos += 2; // ATTACH PARTITION
            if (pos < tokens.size()) sub.name = tokens[pos++];
            int depth = 0;
            while (pos < tokens.size() && tokens[pos] != ";") {
                if (tokens[pos] == "(") ++depth;
                else if (tokens[pos] == ")" && depth > 0) --depth;
                if (tokens[pos] == "," && depth == 0) break;
                if (!sub.partitionSpec.empty()) sub.partitionSpec += " ";
                sub.partitionSpec += tokens[pos++];
            }
        } else if (kw == "detach") {
            recognized = true;
            sub.action = AlterTableStmt::Action::DetachPartition; pos += 2; // DETACH PARTITION
            if (pos < tokens.size()) sub.name = tokens[pos++];
        } else if (kw == "inherit") {
            recognized = true;
            sub.action = AlterTableStmt::Action::Inherit; ++pos;
            if (pos < tokens.size()) sub.parentTable = tokens[pos++];
        } else if (kw == "no" && pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "inherit") {
            recognized = true;
            sub.action = AlterTableStmt::Action::NoInherit; pos += 2;
            if (pos < tokens.size()) sub.parentTable = tokens[pos++];
        } else if (kw == "owner") {
            recognized = true;
            sub.action = AlterTableStmt::Action::Owner;
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "to") ++pos;
            if (pos < tokens.size()) sub.newName = tokens[pos++];
        } else {
            // Unknown subcommand; skip to next comma or semicolon
            while (pos < tokens.size() && tokens[pos] != ";" && tokens[pos] != ",") ++pos;
        }
        if (recognized) {
            stmt->subCommands.push_back(std::move(sub));
        }
        if (pos < tokens.size() && tokens[pos] == ",") ++pos;
    }
    return stmt;
}

StmtPtr SQLParser::parseAlterIndex(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterIndex);
    stmt->objectType = "INDEX";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterView(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterView);
    stmt->objectType = "VIEW";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterMaterializedView(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterMaterializedView);
    stmt->objectType = "MATERIALIZED VIEW";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterDatabase(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterDatabase);
    stmt->objectType = "DATABASE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterSchema(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterSchema);
    stmt->objectType = "SCHEMA";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterSequence(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterSequence);
    stmt->objectType = "SEQUENCE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterDomain(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterDomain);
    stmt->objectType = "DOMAIN";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterType(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterType);
    stmt->objectType = "TYPE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterFunction(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterFunction);
    stmt->objectType = "FUNCTION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterProcedure(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterProcedure);
    stmt->objectType = "PROCEDURE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterRoutine(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterRoutine);
    stmt->objectType = "ROUTINE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterTrigger(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterTrigger);
    stmt->objectType = "TRIGGER";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterRole(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterRole);
    stmt->objectType = "ROLE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterUser(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterUser);
    stmt->objectType = "USER";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterSystem(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterSystem);
    stmt->objectType = "SYSTEM";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterTablespace(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterTablespace);
    stmt->objectType = "TABLESPACE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterStatistics(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterStatistics);
    stmt->objectType = "STATISTICS";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterPolicy(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterPolicy);
    stmt->objectType = "POLICY";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterRule(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterRule);
    stmt->objectType = "RULE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterEventTrigger(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterEventTrigger);
    stmt->objectType = "EVENT TRIGGER";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterExtension(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterExtension);
    stmt->objectType = "EXTENSION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterPublication(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterPublication);
    stmt->objectType = "PUBLICATION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterSubscription(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterSubscription);
    stmt->objectType = "SUBSCRIPTION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterDefaultPrivileges(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterDefaultPrivilegesStmt>();
    size_t cursor = pos;

    // PostgreSQL accepts FOR ROLE and IN SCHEMA before the GRANT/REVOKE
    // clause. Parse them as fields instead of preserving a raw string.
    while (cursor < tokens.size() && tokens[cursor] != ";") {
        const std::string keyword = toLower(tokens[cursor]);
        if (keyword == "for" && cursor + 2 < tokens.size() &&
            toLower(tokens[cursor + 1]) == "role") {
            stmt->owner = tokens[cursor + 2];
            cursor += 3;
            continue;
        }
        if (keyword == "in" && cursor + 2 < tokens.size() &&
            toLower(tokens[cursor + 1]) == "schema") {
            stmt->schema = tokens[cursor + 2];
            cursor += 3;
            continue;
        }
        break;
    }

    if (cursor >= tokens.size() || tokens[cursor] == ";") return stmt;
    const std::string operation = toLower(tokens[cursor++]);
    if (operation != "grant" && operation != "revoke") return stmt;
    stmt->revoke = operation == "revoke";

    if (stmt->revoke && cursor + 2 < tokens.size() &&
        toLower(tokens[cursor]) == "grant" &&
        toLower(tokens[cursor + 1]) == "option" &&
        toLower(tokens[cursor + 2]) == "for") {
        stmt->grantOptionOnly = true;
        cursor += 3;
    }

    // Collect the privilege list up to ON. Commas are syntax separators.
    while (cursor < tokens.size() && toLower(tokens[cursor]) != "on") {
        const std::string token = toLower(tokens[cursor++]);
        if (token == ",") continue;
        if (token == "privileges" && !stmt->privileges.empty()) continue;
        stmt->privileges.push_back(token);
    }
    if (cursor >= tokens.size() || toLower(tokens[cursor]) != "on") return stmt;
    ++cursor;
    if (cursor >= tokens.size() || tokens[cursor] == ";") return stmt;
    stmt->objectType = toLower(tokens[cursor++]);

    const std::string targetKeyword = stmt->revoke ? "from" : "to";
    if (cursor >= tokens.size() || toLower(tokens[cursor]) != targetKeyword) return stmt;
    ++cursor;
    while (cursor < tokens.size() && tokens[cursor] != ";") {
        const std::string token = toLower(tokens[cursor++]);
        if (token == ",") continue;
        if (token == "with" && cursor + 1 < tokens.size() &&
            toLower(tokens[cursor]) == "grant" &&
            toLower(tokens[cursor + 1]) == "option") {
            stmt->withGrantOption = true;
            cursor += 2;
            continue;
        }
        if (token == "cascade") {
            stmt->cascade = true;
            continue;
        }
        if (token == "restrict") continue;
        stmt->grantees.push_back(tokens[cursor - 1]);
    }
    pos = cursor;
    return stmt;
}

StmtPtr SQLParser::parseAlterForeignDataWrapper(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterForeignDataWrapper);
    stmt->objectType = "FOREIGN DATA WRAPPER";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterForeignTable(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterForeignTable);
    stmt->objectType = "FOREIGN TABLE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterServer(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterServer);
    stmt->objectType = "SERVER";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterUserMapping(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterUserMapping);
    stmt->objectType = "USER MAPPING";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterTextSearchConfiguration(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterTextSearchConfiguration);
    stmt->objectType = "TEXT SEARCH CONFIGURATION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterTextSearchDictionary(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterTextSearchDictionary);
    stmt->objectType = "TEXT SEARCH DICTIONARY";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterTextSearchParser(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterTextSearchParser);
    stmt->objectType = "TEXT SEARCH PARSER";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterTextSearchTemplate(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterTextSearchTemplate);
    stmt->objectType = "TEXT SEARCH TEMPLATE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterCollation(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterCollation);
    stmt->objectType = "COLLATION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterConversion(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterConversion);
    stmt->objectType = "CONVERSION";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterOperator(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterOperator);
    stmt->objectType = "OPERATOR";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterOperatorClass(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterOperatorClass);
    stmt->objectType = "OPERATOR CLASS";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterOperatorFamily(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterOperatorFamily);
    stmt->objectType = "OPERATOR FAMILY";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterAggregate(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterAggregate);
    stmt->objectType = "AGGREGATE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterLanguage(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterLanguage);
    stmt->objectType = "LANGUAGE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

StmtPtr SQLParser::parseAlterLargeObject(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterLargeObject);
    stmt->objectType = "LARGE OBJECT";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true; ++pos;
    }
    if (pos < tokens.size()) {
        stmt->objectName = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = tokens[pos++];
            }
        }
    }
    std::string rest;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (!rest.empty()) rest += " ";
        rest += tokens[pos++];
    }
    stmt->subCommand = rest;
    return stmt;
}

// ============================================================================
// TransactionStmt::toString
// ============================================================================

std::string TransactionStmt::toString() const {
    switch (kind) {
        case Kind::Begin: return "BEGIN";
        case Kind::Start: return "START TRANSACTION";
        case Kind::Commit: return "COMMIT";
        case Kind::Rollback: return "ROLLBACK";
        case Kind::Abort: return "ABORT";
        case Kind::End: return "END";
        case Kind::Savepoint: return "SAVEPOINT";
        case Kind::Release: return "RELEASE SAVEPOINT";
        case Kind::RollbackTo: return "ROLLBACK TO SAVEPOINT";
        case Kind::Prepare: return "PREPARE TRANSACTION";
        case Kind::CommitPrepared: return "COMMIT PREPARED";
        case Kind::RollbackPrepared: return "ROLLBACK PREPARED";
    }
    return "TRANSACTION";
}

// ============================================================================
// FunctionCallExpr::toString
// ============================================================================

std::string FunctionCallExpr::toString() const {
    std::string s = funcName + "(";
    if (distinct) s += "DISTINCT ";
    for (size_t i = 0; i < args.size(); ++i) {
        if (i > 0) s += ", ";
        s += (args[i] ? args[i]->toString() : "?");
    }
    s += ")";
    if (filter) s += " FILTER (WHERE " + filter->toString() + ")";
    if (hasOver) s += " OVER(...)";
    return s;
}

} // namespace dbms
