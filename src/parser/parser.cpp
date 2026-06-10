#include "parser.h"
#include <cctype>
#include <algorithm>

namespace dbms {

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

ParseResult SQLParser::parseSelect(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<SelectStmt>();
    // TODO: Phase 1.2 实现完整 SELECT 解析
    return r;
}

ParseResult SQLParser::parseInsert(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<InsertStmt>();
    return r;
}

ParseResult SQLParser::parseUpdate(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<UpdateStmt>();
    return r;
}

ParseResult SQLParser::parseDelete(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<DeleteStmt>();
    return r;
}

ParseResult SQLParser::parseMerge(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<MergeStmt>();
    return r;
}

ParseResult SQLParser::parseValues(const std::string& sql) {
    ParseResult r;
    r.success = true;
    r.stmt = std::make_unique<SelectStmt>();
    static_cast<SelectStmt*>(r.stmt.get())->command = SqlCommand::Values;
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

    static const std::map<std::string, std::function<StmtPtr(const std::vector<std::string>&, size_t&)>> creators = {
        {"table", &SQLParser::parseCreateTable},
        {"index", &SQLParser::parseCreateIndex},
        {"view", &SQLParser::parseCreateView},
        {"database", &SQLParser::parseCreateDatabase},
        {"schema", &SQLParser::parseCreateSchema},
        {"sequence", &SQLParser::parseCreateSequence},
        {"domain", &SQLParser::parseCreateDomain},
        {"type", &SQLParser::parseCreateType},
        {"function", &SQLParser::parseCreateFunction},
        {"procedure", &SQLParser::parseCreateProcedure},
        {"trigger", &SQLParser::parseCreateTrigger},
        {"role", &SQLParser::parseCreateRole},
        {"user", &SQLParser::parseCreateRole},
        {"tablespace", &SQLParser::parseCreateTablespace},
        {"statistics", &SQLParser::parseCreateStatistics},
        {"policy", &SQLParser::parseCreatePolicy},
        {"rule", &SQLParser::parseCreateRule},
        {"event", &SQLParser::parseCreateEventTrigger},
        {"extension", &SQLParser::parseCreateExtension},
        {"publication", &SQLParser::parseCreatePublication},
        {"subscription", &SQLParser::parseCreateSubscription},
        {"access", &SQLParser::parseCreateAccessMethod},
        {"foreign", &SQLParser::parseCreateForeignDataWrapper},
        {"cast", &SQLParser::parseCreateCast},
        {"collation", &SQLParser::parseCreateCollation},
        {"conversion", &SQLParser::parseCreateConversion},
        {"operator", &SQLParser::parseCreateOperator},
        {"aggregate", &SQLParser::parseCreateAggregate},
        {"transform", &SQLParser::parseCreateTransform},
        {"language", &SQLParser::parseCreateLanguage},
        {"text", &SQLParser::parseCreateTextSearchConfiguration},
    };

    if (pos < tokens.size()) {
        std::string kw = toLower(tokens[pos]);
        auto it = creators.find(kw);
        if (it != creators.end()) {
            if (kw == "user" || kw == "role") {
                ++pos;
                r.stmt = parseCreateRole(tokens, pos);
            } else if (kw == "foreign") {
                ++pos;
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
            } else if (kw == "text") {
                ++pos;
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
                ++pos;
                r.stmt = it->second(tokens, pos);
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
// CREATE 子命令解析 stub（Phase 1.2+ 逐步完善）
// ============================================================================

StmtPtr SQLParser::parseCreateTable(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateTableStmt>();
    if (pos < tokens.size()) {
        stmt->tableName = tokens[pos++];
    }
    // TODO: 解析列定义、约束、选项
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
