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
            c == ':' || c == '[' || c == ']') {
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
                    two == ">>") {
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

// 简化表达式解析：识别字面量、列引用、函数调用、基本运算
static ExprPtr parseSimpleExpr(const std::vector<std::string>& tokens, size_t& pos) {
    if (pos >= tokens.size()) return nullptr;

    // Handle unary NOT
    if (SQLParser::toLower(tokens[pos]) == "not") {
        ++pos;
        auto expr = std::make_unique<UnaryOpExpr>();
        expr->op = "NOT";
        expr->operand = parseSimpleExpr(tokens, pos);
        return expr;
    }

    // Parenthesized expression or subquery
    if (tokens[pos] == "(") {
        ++pos;
        auto inner = parseSimpleExpr(tokens, pos);
        if (pos < tokens.size() && tokens[pos] == ")") ++pos;
        return inner;
    }

    // Star
    if (tokens[pos] == "*") {
        ++pos;
        auto expr = std::make_unique<LiteralExpr>();
        expr->value = "*";
        return expr;
    }

    std::string first = tokens[pos];
    size_t savedPos = pos;
    ++pos;

    // Function call: func_name( ... )
    if (pos < tokens.size() && tokens[pos] == "(") {
        ++pos; // skip '('
        auto func = std::make_unique<FunctionCallExpr>();
        func->funcName = first;
        // Parse arguments
        while (pos < tokens.size() && tokens[pos] != ")") {
            if (tokens[pos] == "distinct" || tokens[pos] == "DISTINCT") {
                func->distinct = true;
                ++pos;
                continue;
            }
            auto arg = parseSimpleExpr(tokens, pos);
            if (arg) func->args.push_back(std::move(arg));
            if (pos < tokens.size() && tokens[pos] == ",") {
                ++pos;
                continue;
            }
        }
        if (pos < tokens.size() && tokens[pos] == ")") ++pos;

        // FILTER (WHERE ...)
        if (pos + 2 < tokens.size() && SQLParser::toLower(tokens[pos]) == "filter"
            && tokens[pos + 1] == "(") {
            pos += 2; // skip FILTER (
            if (SQLParser::toLower(tokens[pos]) == "where") ++pos;
            func->filter = parseSimpleExpr(tokens, pos);
            if (pos < tokens.size() && tokens[pos] == ")") ++pos;
        }

        // Check for binary operator after function call
        if (pos < tokens.size()) {
            std::string op = tokens[pos];
            static const std::set<std::string> binOps = {
                "=", "<>", "!=", "<", ">", "<=", ">=", "+", "-", "*", "/", "%", "^",
                "and", "or", "like", "ilike", "in", "between", "is"
            };
            if (binOps.count(SQLParser::toLower(op)) > 0) {
                ++pos;
                auto bin = std::make_unique<BinaryOpExpr>();
                bin->op = op;
                bin->left = std::move(func);
                bin->right = parseSimpleExpr(tokens, pos);
                return bin;
            }
        }
        return func;
    }

    // Column reference: table.column or just column
    std::string tableName, colName = first;
    if (pos < tokens.size() && tokens[pos] == ".") {
        ++pos;
        if (pos < tokens.size()) {
            tableName = first;
            colName = tokens[pos++];
        }
    }

    auto colRef = std::make_unique<ColumnRefExpr>();
    colRef->table = tableName;
    colRef->column = colName;

    // Check for binary operator
    if (pos < tokens.size()) {
        std::string op = tokens[pos];
        static const std::set<std::string> binOps = {
            "=", "<>", "!=", "<", ">", "<=", ">=", "+", "-", "*", "/", "%", "^",
            "and", "or", "like", "ilike", "in", "between", "is", "||", "->", "->>", "::"
        };
        if (binOps.count(SQLParser::toLower(op)) > 0) {
            ++pos;
            auto bin = std::make_unique<BinaryOpExpr>();
            bin->op = op;
            bin->left = std::move(colRef);
            if (SQLParser::toLower(op) == "in" && pos < tokens.size() && tokens[pos] == "(") {
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
            } else if (SQLParser::toLower(op) == "between") {
                auto betweenExpr = std::make_unique<LiteralExpr>();
                std::string val;
                while (pos < tokens.size() && SQLParser::toLower(tokens[pos]) != "and") {
                    if (!val.empty()) val += " ";
                    val += tokens[pos++];
                }
                if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "and") {
                    val += " AND ";
                    ++pos;
                    while (pos < tokens.size()) {
                        std::string w = SQLParser::toLower(tokens[pos]);
                        if (w == "and" || w == "or" || w == ")" || w == ",") break;
                        if (!val.empty() && val.back() != ' ') val += " ";
                        val += tokens[pos++];
                    }
                }
                betweenExpr->value = val;
                bin->right = std::move(betweenExpr);
            } else if (SQLParser::toLower(op) == "is") {
                std::string val;
                while (pos < tokens.size()) {
                    std::string w = SQLParser::toLower(tokens[pos]);
                    if (w == "and" || w == "or" || w == ")" || w == ",") break;
                    if (!val.empty()) val += " ";
                    val += tokens[pos++];
                }
                auto right = std::make_unique<LiteralExpr>();
                right->value = val;
                bin->right = std::move(right);
            } else {
                bin->right = parseSimpleExpr(tokens, pos);
            }
            return bin;
        }
    }

    // Postfix IS NULL / IS NOT NULL
    if (pos + 1 < tokens.size() && SQLParser::toLower(tokens[pos]) == "is") {
        if (SQLParser::toLower(tokens[pos + 1]) == "null") {
            pos += 2;
            auto unary = std::make_unique<UnaryOpExpr>();
            unary->op = "IS NULL";
            unary->operand = std::move(colRef);
            return unary;
        } else if (pos + 2 < tokens.size() && SQLParser::toLower(tokens[pos + 1]) == "not"
                   && SQLParser::toLower(tokens[pos + 2]) == "null") {
            pos += 3;
            auto unary = std::make_unique<UnaryOpExpr>();
            unary->op = "IS NOT NULL";
            unary->operand = std::move(colRef);
            return unary;
        }
    }

    // Literal (string, number)
    auto lit = std::make_unique<LiteralExpr>();
    lit->value = first;
    return lit;
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
    size_t pos = 1; // skip SELECT

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

    // FROM
    if (pos < tokens.size() && toLower(tokens[pos]) == "from") {
        ++pos;
        // Multiple tables or JOINs
        auto firstItem = parseFromItem(tokens, pos);
        if (firstItem) {
            while (pos < tokens.size() && tokens[pos] == ",") {
                ++pos;
                auto nextItem = parseFromItem(tokens, pos);
                if (nextItem) {
                    auto crossJoin = std::make_unique<FromItem>();
                    crossJoin->type = FromItem::Type::Join;
                    crossJoin->joinType = "CROSS";
                    crossJoin->left = std::move(firstItem);
                    crossJoin->right = std::move(nextItem);
                    firstItem = std::move(crossJoin);
                }
            }
            stmt->fromClause = std::move(firstItem);
        }
    }

    // WHERE
    if (pos < tokens.size() && toLower(tokens[pos]) == "where") {
        ++pos;
        stmt->whereClause = parseSimpleExpr(tokens, pos);
    }

    // GROUP BY
    if (pos < tokens.size() && toLower(tokens[pos]) == "group") {
        ++pos;
        if (pos < tokens.size() && toLower(tokens[pos]) == "by") ++pos;
        while (pos < tokens.size()) {
            std::string w = toLower(tokens[pos]);
            if (w == "having" || w == "order" || w == "limit" || w == "offset"
                || w == "union" || w == "intersect" || w == "except" || w == "for") break;
            auto expr = parseSimpleExpr(tokens, pos);
            if (expr) stmt->groupBy.push_back(std::move(expr));
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
            if (expr) stmt->orderBy.emplace_back(std::move(expr), asc);
            if (pos < tokens.size() && tokens[pos] == ",") { ++pos; continue; }
        }
    }

    // LIMIT / OFFSET
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
    }
    if (pos < tokens.size() && toLower(tokens[pos]) == "offset") {
        ++pos;
        if (pos < tokens.size()) {
            try { stmt->offset = std::stoull(tokens[pos]); } catch (...) {}
            ++pos;
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
            // Store as a SelectItem in selectList for now (VALUES as query expr)
            for (auto& expr : row) {
                SelectItem si;
                si.expr = std::move(expr);
                stmt->selectList.push_back(std::move(si));
            }
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
    if (match(tokens, pos, "unique")) ++pos;
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
    r.success = true;
    auto stmt = std::make_unique<SetStmt>();
    // TODO: 解析参数名和值
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseShow(const std::string& sql) {
    ParseResult r;
    r.success = true;
    auto stmt = std::make_unique<SetStmt>();
    stmt->isShow = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseReset(const std::string& sql) {
    ParseResult r;
    r.success = true;
    auto stmt = std::make_unique<SetStmt>();
    stmt->isReset = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseUse(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<Stmt>(SqlCommand::UseDatabase);
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
    r.success = true;
    auto stmt = std::make_unique<ExplainStmt>();
    // TODO: 解析选项 (ANALYZE, BUFFERS, etc.)
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseAnalyze(const std::string& sql) {
    ParseResult r;
    r.success = true;
    auto stmt = std::make_unique<ExplainStmt>();
    stmt->analyze = true;
    r.stmt = std::move(stmt);
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
    r.success = true;
    r.stmt = std::make_unique<CommentStmt>();
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
                                if (pos < tokens.size() && tokens[pos] == "identity") {
                                    ++pos;
                                    col.constraints.push_back("GENERATED ALWAYS AS IDENTITY");
                                } else if (pos < tokens.size() && tokens[pos] == "(") {
                                    auto genExprTokens = collectParenthesized(tokens, pos);
                                    std::string expr;
                                    for (const auto& t : genExprTokens) {
                                        if (!expr.empty() && expr.back() != '(') expr += " ";
                                        expr += t;
                                    }
                                    col.constraints.push_back("GENERATED ALWAYS AS (" + expr + ") STORED");
                                }
                            }
                        } else if (pos < tokens.size() && toLower(tokens[pos]) == "by") {
                            ++pos;
                            if (pos < tokens.size() && toLower(tokens[pos]) == "default") {
                                ++pos;
                                if (pos < tokens.size() && toLower(tokens[pos]) == "as") {
                                    ++pos;
                                    if (pos < tokens.size() && tokens[pos] == "identity") {
                                        ++pos;
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
    if (pos < tokens.size()) {
        stmt->indexName = tokens[pos++];
    }
    if (pos < tokens.size() && match(tokens, pos, "on")) {
        ++pos;
        if (pos < tokens.size()) {
            stmt->tableName = tokens[pos++];
        }
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateView(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateViewStmt>();
    if (pos < tokens.size()) {
        stmt->viewName = tokens[pos++];
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateDatabase(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<Stmt>(SqlCommand::CreateDatabase);
    return stmt;
}

StmtPtr SQLParser::parseCreateSchema(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<Stmt>(SqlCommand::CreateSchema);
    return stmt;
}

StmtPtr SQLParser::parseCreateSequence(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<Stmt>(SqlCommand::CreateSequence);
    return stmt;
}

StmtPtr SQLParser::parseCreateDomain(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<Stmt>(SqlCommand::CreateDomain);
    return stmt;
}

StmtPtr SQLParser::parseCreateType(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<Stmt>(SqlCommand::CreateType);
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
    return std::make_unique<Stmt>(SqlCommand::CreateTablespace);
}

StmtPtr SQLParser::parseCreateStatistics(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateStatistics);
}

StmtPtr SQLParser::parseCreatePolicy(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreatePolicy);
}

StmtPtr SQLParser::parseCreateRule(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateRule);
}

StmtPtr SQLParser::parseCreateEventTrigger(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateEventTrigger);
}

StmtPtr SQLParser::parseCreateExtension(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateExtension);
}

StmtPtr SQLParser::parseCreatePublication(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreatePublication);
}

StmtPtr SQLParser::parseCreateSubscription(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateSubscription);
}

StmtPtr SQLParser::parseCreateAccessMethod(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateAccessMethod);
}

StmtPtr SQLParser::parseCreateForeignDataWrapper(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateForeignDataWrapper);
}

StmtPtr SQLParser::parseCreateForeignTable(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateForeignTable);
}

StmtPtr SQLParser::parseCreateServer(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateServer);
}

StmtPtr SQLParser::parseCreateUserMapping(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateUserMapping);
}

StmtPtr SQLParser::parseCreateCast(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateCast);
}

StmtPtr SQLParser::parseCreateCollation(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateCollation);
}

StmtPtr SQLParser::parseCreateConversion(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateConversion);
}

StmtPtr SQLParser::parseCreateOperator(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateOperator);
}

StmtPtr SQLParser::parseCreateOperatorClass(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateOperatorClass);
}

StmtPtr SQLParser::parseCreateOperatorFamily(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateOperatorFamily);
}

StmtPtr SQLParser::parseCreateAggregate(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateAggregate);
}

StmtPtr SQLParser::parseCreateTransform(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateTransform);
}

StmtPtr SQLParser::parseCreateLanguage(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateLanguage);
}

StmtPtr SQLParser::parseCreateTextSearchConfiguration(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateTextSearchConfiguration);
}

StmtPtr SQLParser::parseCreateTextSearchDictionary(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateTextSearchDictionary);
}

StmtPtr SQLParser::parseCreateTextSearchParser(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateTextSearchParser);
}

StmtPtr SQLParser::parseCreateTextSearchTemplate(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::CreateTextSearchTemplate);
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
    return stmt;
}

StmtPtr SQLParser::parseDropMaterializedView(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropMaterializedView);
    stmt->objectType = "MATERIALIZED VIEW";
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
    return stmt;
}

StmtPtr SQLParser::parseDropSequence(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropSequence);
    stmt->objectType = "SEQUENCE";
    return stmt;
}

StmtPtr SQLParser::parseDropDomain(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropDomain);
    stmt->objectType = "DOMAIN";
    return stmt;
}

StmtPtr SQLParser::parseDropType(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropType);
    stmt->objectType = "TYPE";
    return stmt;
}

StmtPtr SQLParser::parseDropFunction(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropFunction);
    stmt->objectType = "FUNCTION";
    return stmt;
}

StmtPtr SQLParser::parseDropProcedure(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropProcedure);
    stmt->objectType = "PROCEDURE";
    return stmt;
}

StmtPtr SQLParser::parseDropRoutine(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropRoutine);
    stmt->objectType = "ROUTINE";
    return stmt;
}

StmtPtr SQLParser::parseDropTrigger(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropTrigger);
    stmt->objectType = "TRIGGER";
    return stmt;
}

StmtPtr SQLParser::parseDropRole(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropRole);
    stmt->objectType = "ROLE";
    return stmt;
}

StmtPtr SQLParser::parseDropUser(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropUser);
    stmt->objectType = "USER";
    return stmt;
}

StmtPtr SQLParser::parseDropTablespace(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropTablespace);
}

StmtPtr SQLParser::parseDropStatistics(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropStatistics);
}

StmtPtr SQLParser::parseDropPolicy(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropPolicy);
}

StmtPtr SQLParser::parseDropRule(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropRule);
}

StmtPtr SQLParser::parseDropEventTrigger(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropEventTrigger);
}

StmtPtr SQLParser::parseDropExtension(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropExtension);
}

StmtPtr SQLParser::parseDropPublication(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropPublication);
}

StmtPtr SQLParser::parseDropSubscription(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropSubscription);
}

StmtPtr SQLParser::parseDropAccessMethod(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropAccessMethod);
}

StmtPtr SQLParser::parseDropForeignDataWrapper(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropForeignDataWrapper);
}

StmtPtr SQLParser::parseDropForeignTable(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropForeignTable);
}

StmtPtr SQLParser::parseDropServer(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropServer);
}

StmtPtr SQLParser::parseDropUserMapping(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropUserMapping);
}

StmtPtr SQLParser::parseDropCast(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropCast);
}

StmtPtr SQLParser::parseDropCollation(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropCollation);
}

StmtPtr SQLParser::parseDropConversion(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropConversion);
}

StmtPtr SQLParser::parseDropOperator(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropOperator);
}

StmtPtr SQLParser::parseDropOperatorClass(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropOperatorClass);
}

StmtPtr SQLParser::parseDropOperatorFamily(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropOperatorFamily);
}

StmtPtr SQLParser::parseDropAggregate(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropAggregate);
}

StmtPtr SQLParser::parseDropTransform(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropTransform);
}

StmtPtr SQLParser::parseDropLanguage(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropLanguage);
}

StmtPtr SQLParser::parseDropTextSearchConfiguration(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropTextSearchConfiguration);
}

StmtPtr SQLParser::parseDropTextSearchDictionary(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropTextSearchDictionary);
}

StmtPtr SQLParser::parseDropTextSearchParser(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropTextSearchParser);
}

StmtPtr SQLParser::parseDropTextSearchTemplate(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropTextSearchTemplate);
}

StmtPtr SQLParser::parseDropOwned(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropOwned);
}

StmtPtr SQLParser::parseDropLargeObject(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<DropStmt>(SqlCommand::DropLargeObject);
}

// ============================================================================
// ALTER 子命令解析 stub
// ============================================================================

StmtPtr SQLParser::parseAlterTable(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<AlterTableStmt>();
    if (pos < tokens.size()) {
        stmt->tableName = tokens[pos++];
    }
    return stmt;
}

StmtPtr SQLParser::parseAlterIndex(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<AlterTableStmt>();
}

StmtPtr SQLParser::parseAlterView(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<AlterTableStmt>();
}

StmtPtr SQLParser::parseAlterMaterializedView(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<AlterTableStmt>();
}

StmtPtr SQLParser::parseAlterDatabase(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterDatabase);
}

StmtPtr SQLParser::parseAlterSchema(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterSchema);
}

StmtPtr SQLParser::parseAlterSequence(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterSequence);
}

StmtPtr SQLParser::parseAlterDomain(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterDomain);
}

StmtPtr SQLParser::parseAlterType(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterType);
}

StmtPtr SQLParser::parseAlterFunction(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterFunction);
}

StmtPtr SQLParser::parseAlterProcedure(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterProcedure);
}

StmtPtr SQLParser::parseAlterRoutine(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterRoutine);
}

StmtPtr SQLParser::parseAlterTrigger(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterTrigger);
}

StmtPtr SQLParser::parseAlterRole(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterRole);
}

StmtPtr SQLParser::parseAlterUser(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterUser);
}

StmtPtr SQLParser::parseAlterSystem(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterSystem);
}

StmtPtr SQLParser::parseAlterTablespace(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterTablespace);
}

StmtPtr SQLParser::parseAlterStatistics(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterStatistics);
}

StmtPtr SQLParser::parseAlterPolicy(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterPolicy);
}

StmtPtr SQLParser::parseAlterRule(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterRule);
}

StmtPtr SQLParser::parseAlterEventTrigger(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterEventTrigger);
}

StmtPtr SQLParser::parseAlterExtension(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterExtension);
}

StmtPtr SQLParser::parseAlterPublication(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterPublication);
}

StmtPtr SQLParser::parseAlterSubscription(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterSubscription);
}

StmtPtr SQLParser::parseAlterDefaultPrivileges(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterDefaultPrivileges);
}

StmtPtr SQLParser::parseAlterForeignDataWrapper(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterForeignDataWrapper);
}

StmtPtr SQLParser::parseAlterForeignTable(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterForeignTable);
}

StmtPtr SQLParser::parseAlterServer(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterServer);
}

StmtPtr SQLParser::parseAlterUserMapping(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterUserMapping);
}

StmtPtr SQLParser::parseAlterTextSearchConfiguration(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterTextSearchConfiguration);
}

StmtPtr SQLParser::parseAlterTextSearchDictionary(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterTextSearchDictionary);
}

StmtPtr SQLParser::parseAlterTextSearchParser(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterTextSearchParser);
}

StmtPtr SQLParser::parseAlterTextSearchTemplate(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterTextSearchTemplate);
}

StmtPtr SQLParser::parseAlterCollation(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterCollation);
}

StmtPtr SQLParser::parseAlterConversion(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterConversion);
}

StmtPtr SQLParser::parseAlterOperator(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterOperator);
}

StmtPtr SQLParser::parseAlterOperatorClass(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterOperatorClass);
}

StmtPtr SQLParser::parseAlterOperatorFamily(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterOperatorFamily);
}

StmtPtr SQLParser::parseAlterAggregate(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterAggregate);
}

StmtPtr SQLParser::parseAlterLanguage(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterLanguage);
}

StmtPtr SQLParser::parseAlterLargeObject(const std::vector<std::string>& tokens, size_t& pos) {
    return std::make_unique<Stmt>(SqlCommand::AlterLargeObject);
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
    return s;
}

} // namespace dbms
