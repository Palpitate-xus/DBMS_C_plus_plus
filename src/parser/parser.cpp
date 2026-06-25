#include "parser.h"
#include <cctype>
#include <algorithm>
#include <functional>
#include <set>

namespace dbms {

// ============================================================================
// Forward declarations for internal helper functions
// ============================================================================

static std::vector<std::string> collectParenthesized(const std::vector<std::string>& tokens, size_t& pos);
static std::string collectExpression(const std::vector<std::string>& tokens, size_t& pos,
                                      bool stopAtComma = false,
                                      const std::set<std::string>& stopWords = {});
static ExprPtr parseSimpleExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseExpr(const std::vector<std::string>& tokens, size_t& pos);
static SelectItem parseSelectItem(const std::vector<std::string>& tokens, size_t& pos);
static std::unique_ptr<FromItem> parseFromItem(const std::vector<std::string>& tokens, size_t& pos);

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
        "not", "no", "inherit", "force", "row", "level", "security",
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
        std::string rest = lsql.substr(pos);
        if (rest.substr(0, 9) == "database ") return SqlCommand::CreateDatabase;
        if (rest.substr(0, 7) == "schema ") return SqlCommand::CreateSchema;
        if (rest.substr(0, 12) == "tablespace ") return SqlCommand::CreateTablespace;
        if (rest.substr(0, 11) == "sequence ") return SqlCommand::CreateSequence;
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
        if (rest.substr(0, 11) == "sequence ") return SqlCommand::DropSequence;
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
        if (rest.substr(0, 11) == "sequence ") return SqlCommand::AlterSequence;
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
    if (lsql.substr(0, 6) == "commit") return SqlCommand::Commit;
    if (lsql.substr(0, 8) == "rollback") return SqlCommand::Rollback;
    if (lsql.substr(0, 5) == "abort") return SqlCommand::Abort;
    if (lsql.substr(0, 3) == "end") return SqlCommand::End;
    if (lsql.substr(0, 9) == "savepoint") return SqlCommand::Savepoint;
    if (lsql.substr(0, 7) == "release") return SqlCommand::ReleaseSavepoint;
    if (lsql.substr(0, 16) == "rollback to savepoint" ||
        lsql.substr(0, 19) == "rollback to savepoint")
        return SqlCommand::RollbackToSavepoint;
    if (lsql.substr(0, 17) == "prepare transaction") return SqlCommand::PrepareTransaction;
    if (lsql.substr(0, 15) == "commit prepared") return SqlCommand::CommitPrepared;
    if (lsql.substr(0, 17) == "rollback prepared") return SqlCommand::RollbackPrepared;

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
        case SqlCommand::Commit:
            return parseCommit(sql);
        case SqlCommand::Rollback: case SqlCommand::Abort: case SqlCommand::End:
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
        case SqlCommand::UpdateLimit:
        case SqlCommand::DeleteLimit:
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

// 解析逗号分隔的列表，每个元素用 parseItem 解析
static std::vector<ExprPtr> parseExprList(const std::vector<std::string>& tokens, size_t& pos) {
    std::vector<ExprPtr> result;
    while (pos < tokens.size()) {
        auto expr = parseSimpleExpr(tokens, pos);
        if (expr) result.push_back(std::move(expr));
        if (pos < tokens.size() && tokens[pos] == ",") {
            ++pos;
            continue;
        }
        break;
    }
    return result;
}

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
            if (expr) stmt->orderBy.push_back({std::move(expr), asc});
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
        if (pos < tokens.size()) {
            if (toLower(tokens[pos]) == "all") {
                ++pos;
            } else {
                try { stmt->limit = std::stoull(tokens[pos]); } catch (...) {}
                ++pos;
            }
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
        if (pos < tokens.size()) {
            try { stmt->offset = std::stoull(tokens[pos]); } catch (...) {}
            ++pos;
        }
    }
    // FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } { ONLY | WITH TIES }
    if (pos < tokens.size() && toLower(tokens[pos]) == "fetch") {
        ++pos;
        if (pos < tokens.size() && (toLower(tokens[pos]) == "first" || toLower(tokens[pos]) == "next")) {
            ++pos;
            stmt->fetchFirst = true;
        }
        if (pos < tokens.size()) {
            try { stmt->limit = std::stoull(tokens[pos]); ++pos; } catch (...) {}
        }
        if (pos < tokens.size() && (toLower(tokens[pos]) == "row" || toLower(tokens[pos]) == "rows")) ++pos;
        if (pos < tokens.size() && toLower(tokens[pos]) == "only") ++pos;
        if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "with" && toLower(tokens[pos + 1]) == "ties") {
            stmt->withTies = true; pos += 2;
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

    // UNION / INTERSECT / EXCEPT [ALL | DISTINCT]
    if (pos < tokens.size()) {
        std::string w = toLower(tokens[pos]);
        if (w == "union") {
            stmt->setOp = SetOp::Union;
            ++pos;
        } else if (w == "intersect") {
            stmt->setOp = SetOp::Intersect;
            ++pos;
        } else if (w == "except") {
            stmt->setOp = SetOp::Except;
            ++pos;
        }
        if (stmt->setOp != SetOp::None) {
            if (pos < tokens.size() && toLower(tokens[pos]) == "all") {
                stmt->setOpAll = true;
                ++pos;
            } else if (pos < tokens.size() && toLower(tokens[pos]) == "distinct") {
                ++pos;
            }
            // Collect remaining tokens as RHS query and parse recursively
            std::string rhsSql;
            for (size_t i = pos; i < tokens.size(); ++i) {
                if (!rhsSql.empty()) rhsSql += " ";
                rhsSql += tokens[i];
            }
            stmt->setOpRhs = parseSelect(rhsSql).stmt;
            pos = tokens.size();
        }
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
                        stmt->defaultValues = true;
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
        for (size_t i = pos; i < tokens.size(); ++i) {
            if (!selectSql.empty()) selectSql += " ";
            selectSql += tokens[i];
        }
        stmt->selectSource = parseSelect(selectSql).stmt;
        pos = tokens.size();
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
    if (pos < tokens.size() && toLower(tokens[pos]) == "set") {
        ++pos;
        while (pos < tokens.size()) {
            std::string w = toLower(tokens[pos]);
            if (w == "from" || w == "where" || w == "returning" || tokens[pos] == ";") break;
            if (pos + 1 < tokens.size() && tokens[pos + 1] == "=") {
                std::string col = tokens[pos];
                pos += 2;
                auto expr = parseSimpleExpr(tokens, pos);
                stmt->setClauses[col] = std::move(expr);
            } else {
                ++pos;
            }
            if (pos < tokens.size() && tokens[pos] == ",") ++pos;
        }
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
                    auto vals = collectParenthesized(tokens, pos);
                    for (size_t i = 0, j = 0; i < vals.size() && j < wc.insertCols.size(); ++i) {
                        if (vals[i] == ",") continue;
                        auto expr = std::make_unique<LiteralExpr>();
                        expr->value = vals[i];
                        wc.insertCols[j].second = std::move(expr);
                        ++j;
                    }
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
    if (match(tokens, pos, "or") && match(tokens, pos + 1, "replace")) {
        pos += 2;
    }
    if (match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        pos += 3;
    }
    bool isUnique = false;
    if (match(tokens, pos, "unique")) { isUnique = true; ++pos; }
    if (match(tokens, pos, "temp") || match(tokens, pos, "temporary")) ++pos;
    if (match(tokens, pos, "materialized")) {
        ++pos;
        auto stmt = parseCreateView(tokens, pos);
        if (stmt) static_cast<CreateViewStmt*>(stmt.get())->materialized = true;
        r.success = true;
        r.stmt = std::move(stmt);
        return r;
    }

    if (pos < tokens.size()) {
        std::string kw = toLower(tokens[pos]);
        ++pos;
        if (kw == "table") {
            r.stmt = parseCreateTable(tokens, pos);
        } else if (kw == "index") {
            r.stmt = parseCreateIndex(tokens, pos);
            if (r.stmt && isUnique) static_cast<CreateIndexStmt*>(r.stmt.get())->unique = true;
        } else if (kw == "view") {
            r.stmt = parseCreateView(tokens, pos);
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
            r.stmt = parseCreateRole(tokens, pos);
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
    r.success = true;
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
    r.success = true;
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
    r.success = true;
    return r;
}

ParseResult SQLParser::parseTruncate(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Truncate);
    return r;
}

// ------------------------------------------------------------------------
// 事务语句解析
// ------------------------------------------------------------------------

ParseResult SQLParser::parseBegin(const std::string& sql) {
    ParseResult r;
    r.success = true;
    auto stmt = std::make_unique<TransactionStmt>(TransactionStmt::Kind::Begin);
    // TODO: 解析隔离级别、只读、deferrable
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseCommit(const std::string& sql) {
    ParseResult r;
    r.success = true;
    auto stmt = std::make_unique<TransactionStmt>(TransactionStmt::Kind::Commit);
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseRollback(const std::string& sql) {
    ParseResult r;
    r.success = true;
    SqlCommand cmd = classify(sql);
    TransactionStmt::Kind kind;
    switch (cmd) {
        case SqlCommand::Rollback: kind = TransactionStmt::Kind::Rollback; break;
        case SqlCommand::Abort:    kind = TransactionStmt::Kind::Abort; break;
        case SqlCommand::End:      kind = TransactionStmt::Kind::End; break;
        default:                   kind = TransactionStmt::Kind::Rollback; break;
    }
    r.stmt = std::make_unique<TransactionStmt>(kind);
    return r;
}

ParseResult SQLParser::parseSavepoint(const std::string& sql) {
    ParseResult r;
    r.success = true;
    auto stmt = std::make_unique<TransactionStmt>(TransactionStmt::Kind::Savepoint);
    // TODO: 解析 savepoint 名称
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseRelease(const std::string& sql) {
    ParseResult r;
    r.success = true;
    auto stmt = std::make_unique<TransactionStmt>(TransactionStmt::Kind::Release);
    r.stmt = std::move(stmt);
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

ParseResult SQLParser::parseDiscard(const std::string& sql) {
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

ParseResult SQLParser::parseVacuum(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Vacuum);
    return r;
}

ParseResult SQLParser::parseCheckpoint(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Checkpoint);
    return r;
}

ParseResult SQLParser::parseReindex(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Reindex);
    return r;
}

ParseResult SQLParser::parseCluster(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Cluster);
    return r;
}

ParseResult SQLParser::parseCopy(const std::string& sql) {
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

ParseResult SQLParser::parseSecurityLabel(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::SecurityLabel);
    return r;
}

ParseResult SQLParser::parseLock(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Lock);
    return r;
}

// ------------------------------------------------------------------------
// Listen / Notify / Unlisten
// ------------------------------------------------------------------------

ParseResult SQLParser::parseListen(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Listen);
    return r;
}

ParseResult SQLParser::parseNotify(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Notify);
    return r;
}

ParseResult SQLParser::parseUnlisten(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Unlisten);
    return r;
}

// ------------------------------------------------------------------------
// Cursor
// ------------------------------------------------------------------------

ParseResult SQLParser::parseDeclare(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Declare);
    return r;
}

ParseResult SQLParser::parseFetch(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Fetch);
    return r;
}

ParseResult SQLParser::parseMove(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Move);
    return r;
}

ParseResult SQLParser::parseClose(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Close);
    return r;
}

// ------------------------------------------------------------------------
// Prepared statement
// ------------------------------------------------------------------------

ParseResult SQLParser::parsePrepare(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Prepare);
    return r;
}

ParseResult SQLParser::parseExecute(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Execute);
    return r;
}

ParseResult SQLParser::parseDeallocate(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Deallocate);
    return r;
}

// ------------------------------------------------------------------------
// Grant / Revoke
// ------------------------------------------------------------------------

ParseResult SQLParser::parseGrant(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<GrantStmt>();
    return r;
}

ParseResult SQLParser::parseRevoke(const std::string& sql) {
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

ParseResult SQLParser::parseCall(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Call);
    return r;
}

ParseResult SQLParser::parseDo(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::Do);
    return r;
}

ParseResult SQLParser::parseImportForeignSchema(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::ImportForeignSchema);
    return r;
}

// ============================================================================
// 表达式解析辅助（Phase 1.2 简化版：收集 token 直到匹配括号）
// ============================================================================

static std::string collectExpression(const std::vector<std::string>& tokens, size_t& pos,
                                      bool stopAtComma,
                                      const std::set<std::string>& stopWords) {
    std::string expr;
    int parenDepth = 0;
    while (pos < tokens.size()) {
        const std::string& tok = tokens[pos];
        if (tok == "(") {
            ++parenDepth;
            expr += "(";
            ++pos;
            continue;
        }
        if (tok == ")") {
            if (parenDepth > 0) {
                --parenDepth;
                expr += ")";
                ++pos;
                continue;
            }
            // 顶层右括号，结束
            break;
        }
        if (parenDepth == 0) {
            if (tok == ",") {
                if (stopAtComma) break;
                expr += ",";
                ++pos;
                continue;
            }
            if (stopWords.count(SQLParser::toLower(tok)) > 0) break;
        }
        if (!expr.empty() && expr.back() != '(' && expr.back() != ',') expr += " ";
        expr += tok;
        ++pos;
    }
    // trim trailing space
    while (!expr.empty() && expr.back() == ' ') expr.pop_back();
    return expr;
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

// ============================================================================
// CREATE 子命令解析（Phase 1.2 逐步完善）
// ============================================================================

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
                            stmt->constraints.push_back(std::move(tc));
                        }
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
                    stmt->constraints.push_back(std::move(tc));
                }
            } else {
                // Column definition
                ColumnDef col;
                col.name = tokens[pos++];
                if (pos < tokens.size()) {
                    col.typeName = tokens[pos++];
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
                        // Collect default value (may be literal, expression, or function call)
                        while (pos < tokens.size() && tokens[pos] != "," && tokens[pos] != ")") {
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
                                    col.constraints.push_back("GENERATED ALWAYS AS (" + expr + ") STORED");
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
                ColumnDef likeDef;
                likeDef.name = tokens[pos++];
                stmt->likeTables.emplace_back(likeDef.name, ColumnDef());
            }
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
        if (kw == "include" && pos + 1 < tokens.size() && tokens[pos + 1] == "(") {
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
    if (pos < tokens.size() && match(tokens, pos, "as")) ++pos;
    if (pos < tokens.size()) {
        std::string selectSql;
        for (size_t i = pos; i < tokens.size(); ++i) {
            if (!selectSql.empty()) selectSql += " ";
            selectSql += tokens[i];
        }
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
        if (pos + skip < tokens.size()) {
            try {
                stmt->options[key] = tokens[pos + skip];
                pos += skip + 1;
            } catch (...) {
                ++pos;
            }
        } else {
            ++pos;
        }
    };

    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string tok = lower(tokens[pos]);
        if (tok == "start") {
            if (peek(1) == "with") numericOption("start", 2);
            else numericOption("start", 1);
        } else if (tok == "increment") {
            if (peek(1) == "by") numericOption("increment", 2);
            else numericOption("increment", 1);
        } else if (tok == "minvalue") {
            numericOption("minvalue", 1);
        } else if (tok == "maxvalue") {
            numericOption("maxvalue", 1);
        } else if (tok == "cache") {
            numericOption("cache", 1);
        } else if (tok == "no") {
            if (peek(1) == "minvalue") { stmt->options["nominvalue"] = "1"; pos += 2; }
            else if (peek(1) == "maxvalue") { stmt->options["nomaxvalue"] = "1"; pos += 2; }
            else if (peek(1) == "cycle") { stmt->options["cycle"] = "no"; pos += 2; }
            else ++pos;
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
                    pos += 2;
                }
            } else ++pos;
        } else {
            ++pos;
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
    return stmt;
}

StmtPtr SQLParser::parseCreateFunction(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateFunctionStmt>();
    if (pos < tokens.size()) {
        stmt->funcName = tokens[pos++];
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateProcedure(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateFunctionStmt>(true);
    if (pos < tokens.size()) {
        stmt->funcName = tokens[pos++];
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateTrigger(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateTriggerStmt>();
    if (pos < tokens.size()) {
        stmt->triggerName = tokens[pos++];
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateRole(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateRoleStmt>();
    if (pos < tokens.size()) {
        stmt->roleName = tokens[pos++];
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
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreatePolicy);
    stmt->objectType = "POLICY";
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
        if (toLower(tokens[pos]) == "cascade") { stmt->cascade = true; ++pos; continue; }
        if (toLower(tokens[pos]) == "restrict") { ++pos; continue; }
        stmt->objectNames.push_back(tokens[pos++]);
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
        std::string kw = toLower(tokens[pos]);
        if (kw == "add" && pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "column") {
            sub.action = AlterTableStmt::Action::AddColumn; pos += 2;
            if (pos < tokens.size() && toLower(tokens[pos]) == "if") {
                pos += 3; // IF NOT EXISTS
            }
            if (pos < tokens.size()) sub.colDef.name = tokens[pos++];
            if (pos < tokens.size()) sub.colDef.typeName = tokens[pos++];
            // TODO: column constraints, type mods
        } else if (kw == "add") {
            sub.action = AlterTableStmt::Action::AddConstraint; ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "constraint") {
                ++pos;
                if (pos < tokens.size()) sub.constraint.name = tokens[pos++];
            }
            if (pos < tokens.size()) {
                sub.constraint.type = toUpper(tokens[pos]); ++pos;
                if (sub.constraint.type == "PRIMARY") { sub.constraint.type = "PRIMARY KEY"; ++pos; }
                if (sub.constraint.type == "FOREIGN") { sub.constraint.type = "FOREIGN KEY"; ++pos; }
            }
            if (pos < tokens.size() && tokens[pos] == "(") {
                auto cols = collectParenthesized(tokens, pos);
                for (const auto& c : cols) if (c != ",") sub.constraint.columns.push_back(c);
            }
            if (pos < tokens.size() && toLower(tokens[pos]) == "references") {
                ++pos;
                if (pos < tokens.size()) sub.constraint.refTable = tokens[pos++];
                if (pos < tokens.size() && tokens[pos] == "(") {
                    auto refcols = collectParenthesized(tokens, pos);
                    for (const auto& c : refcols) if (c != ",") sub.constraint.refColumns.push_back(c);
                }
            }
        } else if (kw == "drop" && pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "column") {
            sub.action = AlterTableStmt::Action::DropColumn; pos += 2;
            if (pos < tokens.size() && toLower(tokens[pos]) == "if") {
                pos += 3; // IF EXISTS
            }
            if (pos < tokens.size()) sub.name = tokens[pos++];
            if (pos < tokens.size() && toLower(tokens[pos]) == "cascade") { sub.options["cascade"] = "true"; ++pos; }
        } else if (kw == "drop") {
            sub.action = AlterTableStmt::Action::DropConstraint; ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "constraint") ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "if") {
                pos += 2; // IF EXISTS
            }
            if (pos < tokens.size()) sub.name = tokens[pos++];
            if (pos < tokens.size() && toLower(tokens[pos]) == "cascade") { sub.options["cascade"] = "true"; ++pos; }
        } else if (kw == "alter" && pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "column") {
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
                    if (pos < tokens.size()) sub.dataType = tokens[pos++];
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
            if (pos < tokens.size() && toLower(tokens[pos]) == "column") ++pos;
            if (pos < tokens.size()) sub.name = tokens[pos++];
            if (pos < tokens.size() && toLower(tokens[pos]) == "to") ++pos;
            if (pos < tokens.size()) sub.newName = tokens[pos++];
            sub.action = sub.name.empty() ? AlterTableStmt::Action::RenameTable : AlterTableStmt::Action::RenameColumn;
        } else if (kw == "set") {
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "schema") {
                sub.action = AlterTableStmt::Action::SetSchema; ++pos;
                if (pos < tokens.size()) sub.newName = tokens[pos++];
            } else if (pos < tokens.size() && toLower(tokens[pos]) == "tablespace") {
                sub.action = AlterTableStmt::Action::SetTablespace; ++pos;
                if (pos < tokens.size()) sub.newName = tokens[pos++];
            } else if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "with" && tokens[pos + 1] == "(") {
                sub.action = AlterTableStmt::Action::SetOptions; pos += 2;
                auto opts = collectParenthesized(tokens, pos);
                for (size_t i = 0; i < opts.size(); i += 2) {
                    if (i + 1 < opts.size() && opts[i + 1] == "=") {
                        if (i + 2 < opts.size()) { sub.options[opts[i]] = opts[i + 2]; i += 2; }
                    } else if (i + 1 < opts.size()) {
                        sub.options[opts[i]] = opts[i + 1];
                    }
                }
            }
        } else if (kw == "reset") {
            sub.action = AlterTableStmt::Action::ResetOptions; ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "(") {
                auto opts = collectParenthesized(tokens, pos);
                for (const auto& o : opts) if (o != ",") sub.options[o] = "";
            }
        } else if (kw == "attach") {
            sub.action = AlterTableStmt::Action::AttachPartition; pos += 2; // ATTACH PARTITION
            if (pos < tokens.size()) sub.partitionSpec = tokens[pos++];
        } else if (kw == "detach") {
            sub.action = AlterTableStmt::Action::DetachPartition; pos += 2; // DETACH PARTITION
            if (pos < tokens.size()) sub.partitionSpec = tokens[pos++];
        } else {
            // Unknown subcommand; skip to next comma or semicolon
            while (pos < tokens.size() && tokens[pos] != ";" && tokens[pos] != ",") ++pos;
        }
        if (sub.action != AlterTableStmt::Action::AddColumn || !sub.colDef.name.empty() ||
            sub.action != AlterTableStmt::Action::DropColumn || !sub.name.empty() ||
            sub.action != AlterTableStmt::Action::AddConstraint || !sub.constraint.type.empty() ||
            sub.action != AlterTableStmt::Action::AlterColumn || !sub.name.empty() ||
            sub.action != AlterTableStmt::Action::RenameColumn || !sub.newName.empty() ||
            sub.action == AlterTableStmt::Action::RenameTable ||
            sub.action == AlterTableStmt::Action::SetSchema ||
            sub.action == AlterTableStmt::Action::SetTablespace ||
            sub.action == AlterTableStmt::Action::SetOptions ||
            sub.action == AlterTableStmt::Action::ResetOptions ||
            sub.action == AlterTableStmt::Action::AttachPartition ||
            sub.action == AlterTableStmt::Action::DetachPartition) {
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
    auto stmt = std::make_unique<AlterObjectStmt>(SqlCommand::AlterDefaultPrivileges);
    stmt->objectType = "DEFAULT PRIVILEGES";
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
