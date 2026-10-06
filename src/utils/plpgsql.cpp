// ============================================================================
// PlPgsql — minimal PL/pgSQL interpreter implementation.
// See plpgsql.h for the supported subset.
//
// Two phases:
//   1. parse: the body text is parsed once into a statement tree
//      (compound/assign/if/while/for/return/exit/raise/sql).
//   2. interpret: the tree runs against variable bindings with host
//      callbacks for scalar expressions, SQL execution and SELECT INTO.
// ============================================================================

#include "utils/plpgsql.h"
#include "common/SqlSyntax.h"

#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <set>
#include <sstream>

namespace dbms {
namespace plpgsql_impl {

// ---------------------------------------------------------------------------
// utilities
// ---------------------------------------------------------------------------
std::string trimCopy(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

std::string lowerCopy(const std::string& s) {
    std::string out = s;
    for (char& c : out) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return out;
}

// Decode one SQL identifier, preserving delimited case and doubled quotes.
bool readIdentifier(const std::string& text, size_t start,
                    std::string& name, size_t& end) {
    name.clear();
    if (start >= text.size()) return false;
    if (text[start] == '"') {
        for (end = start + 1; end < text.size();) {
            if (text[end] == '"') {
                ++end;
                if (end < text.size() && text[end] == '"') {
                    name += '"';
                    ++end;
                } else {
                    return !name.empty();
                }
            } else {
                name += text[end++];
            }
        }
        return false;
    }
    const unsigned char first = static_cast<unsigned char>(text[start]);
    if (!std::isalpha(first) && first != '_' && first < 0x80) return false;
    end = start + 1;
    while (end < text.size() &&
           sqlIdentifierContinuation(static_cast<unsigned char>(text[end]))) ++end;
    name = lowerCopy(text.substr(start, end - start));
    return true;
}

// Match a grammar unit, not individual keywords: quoted identifiers never
// participate and comments are whitespace. The operand after the unit is
// deliberately left for the ordinary typed data-variable substitution.
size_t sqlKeywordUnitEnd(const std::string& text, size_t start,
                         std::initializer_list<const char*> words) {
    size_t cursor = start;
    for (const char* word : words) {
        cursor = skipLeadingSqlTrivia(text, cursor);
        std::string identifier;
        size_t end;
        if (cursor == std::string::npos || cursor >= text.size() ||
            text[cursor] == '"' || !readIdentifier(text, cursor, identifier, end) ||
            identifier != word) return std::string::npos;
        cursor = end;
    }
    return cursor;
}

size_t protectedUnitEnd(const std::string& text, size_t start) {
    if (text.compare(start, 2, "--") == 0 || text.compare(start, 2, "/*") == 0) {
        const size_t end = skipLeadingSqlTrivia(text, start);
        return end == std::string::npos ? text.size() : end;
    }
    if (text[start] == '\'' || text[start] == '"') {
        const char quote = text[start];
        const bool escaped = quote == '\'' && start &&
            (text[start - 1] == 'e' || text[start - 1] == 'E') &&
            (start < 2 || !sqlIdentifierContinuation(static_cast<unsigned char>(text[start - 2])));
        for (size_t end = start + 1; end < text.size();) {
            if (escaped && text[end] == '\\') {
                end += std::min<size_t>(2, text.size() - end);
            } else if (text[end] == quote) {
                ++end;
                if (end < text.size() && text[end] == quote) ++end;
                else return end;
            } else ++end;
        }
        return text.size();
    }
    if (text[start] == '$') {
        const size_t delimiterEnd = text.find('$', start + 1);
        if (delimiterEnd != std::string::npos) {
            const std::string delimiter = text.substr(start, delimiterEnd - start + 1);
            const size_t close = text.find(delimiter, delimiterEnd + 1);
            return close == std::string::npos ? text.size() : close + delimiter.size();
        }
    }
    return start + 1;
}

// Character-level scanner with string/comment skipping.
struct Scanner {
    std::string src;
    std::vector<bool> protectedBytes;
    size_t pos = 0;
    explicit Scanner(std::string s) : src(std::move(s)), protectedBytes(sqlProtectedBytes(src)) {}

    void skipWs() {
        const size_t after = skipLeadingSqlTrivia(src, pos);
        pos = after == std::string::npos ? src.size() : after;
    }
    bool eof() { skipWs(); return pos >= src.size(); }

    std::string peekWord(size_t* at = nullptr) {
        skipWs();
        if (at) *at = pos;
        size_t p = pos;
        while (p < src.size() &&
               sqlIdentifierContinuation(static_cast<unsigned char>(src[p]))) ++p;
        return src.substr(pos, p - pos);
    }
    std::string peekKeyword(size_t* at = nullptr) {
        return lowerCopy(peekWord(at));
    }
    bool matchKeyword(const char* kw) {
        size_t at;
        if (peekKeyword(&at) != kw) return false;
        pos = at + std::strlen(kw);
        return true;
    }
    std::string ident() {
        skipWs();
        size_t after;
        std::string out;
        if (!readIdentifier(src, pos, out, after)) return {};
        pos = after;
        return out;
    }
    char peekChar() { skipWs(); return pos < src.size() ? src[pos] : '\0'; }
    bool matchOp(const char* op) {
        skipWs();
        size_t n = std::strlen(op);
        if (src.compare(pos, n, op) == 0) { pos += n; return true; }
        return false;
    }
};

// Raw text until a top-level ';' (quotes/parens aware). Consumes the ';'.
std::string readUntilSemicolon(Scanner& sc) {
    sc.skipWs();
    size_t start = sc.pos;
    int depth = 0;
    while (sc.pos < sc.src.size()) {
        char c = sc.src[sc.pos];
        if (sc.protectedBytes[sc.pos]) {
            ++sc.pos;
            continue;
        }
        if (c == '(' || c == '[') {
            ++depth;
        } else if (c == ')' || c == ']') {
            --depth;
        } else if (c == ';' && depth == 0) {
            break;
        }
        ++sc.pos;
    }
    std::string out = sc.src.substr(start, sc.pos - start);
    if (sc.pos < sc.src.size()) ++sc.pos;
    return trimCopy(out);
}

// Raw text until a top-level keyword (e.g. THEN/LOOP), quotes/parens aware.
// Consumes the keyword.  Returns false when not found.
bool readUntilKeyword(Scanner& sc, const char* kw, std::string& out) {
    sc.skipWs();
    size_t start = sc.pos;
    int depth = 0;
    while (sc.pos < sc.src.size()) {
        char c = sc.src[sc.pos];
        if (sc.protectedBytes[sc.pos]) {
            ++sc.pos;
            continue;
        }
        if (c == '(' || c == '[') {
            ++depth;
        } else if (c == ')' || c == ']') {
            --depth;
        } else if (depth == 0 &&
                   std::isalpha(static_cast<unsigned char>(c)) &&
                   (sc.pos == start ||
                    !sqlIdentifierContinuation(static_cast<unsigned char>(sc.src[sc.pos - 1])))) {
            // word start: compare keyword
            size_t e = sc.pos;
            while (e < sc.src.size() &&
                   sqlIdentifierContinuation(static_cast<unsigned char>(sc.src[e]))) ++e;
            if (lowerCopy(sc.src.substr(sc.pos, e - sc.pos)) == kw) {
                out = trimCopy(sc.src.substr(start, sc.pos - start));
                sc.pos = e;
                return true;
            }
        }
        ++sc.pos;
    }
    return false;
}

// ---------------------------------------------------------------------------
// statement AST
// ---------------------------------------------------------------------------
struct Stmt {
    virtual ~Stmt() = default;
};
using StmtPtr = std::unique_ptr<Stmt>;

struct CompoundStmt : Stmt {
    std::vector<StmtPtr> body;
};
struct AssignStmt : Stmt {
    std::string var;
    std::string expr;
};
struct SqlStmt : Stmt {
    std::string text;               // SQL passthrough (incl. SELECT INTO)
};
struct ReturnStmt : Stmt {
    bool hasExpr = false;
    std::string expr;
};
struct ExitStmt : Stmt {
    bool hasWhen = false;
    std::string cond;
};
struct RaiseStmt : Stmt {
    std::string level;
    std::string fmt;
    std::vector<std::string> args;
};
struct IfStmt : Stmt {
    struct Branch { std::string cond; bool hasCond; CompoundStmt body; };
    std::vector<Branch> branches;   // last may be else (hasCond=false)
};
struct WhileStmt : Stmt {
    std::string cond;
    CompoundStmt body;
};
struct ForStmt : Stmt {
    std::string var;
    bool reverse = false;
    std::string from, to;
    CompoundStmt body;
};
struct Declaration {
    std::string name;
    std::string type;
    std::string defaultExpr;
};

// ---------------------------------------------------------------------------
// parser
// ---------------------------------------------------------------------------
struct Parser {
    Scanner sc;
    std::string error;

    explicit Parser(const std::string& body) : sc(body) {}

    bool fail(const std::string& m) {
        if (error.empty()) error = m;
        return false;
    }

    // Parse DECLARE var [type] [:= expr]; blocks until BEGIN.
    bool parseDeclares(std::vector<Declaration>& defaults) {
        while (true) {
            if (sc.matchKeyword("begin")) return true;
            std::string name = sc.ident();
            if (name.empty()) return fail("DECLARE: expected variable name");
            // optional type words until := or ';'
            std::string tail = readUntilSemicolon(sc);
            size_t assignPos = std::string::npos;
            // find ":=" outside quotes
            {
                const auto protectedBytes = sqlProtectedBytes(tail);
                for (size_t i = 0; i + 1 < tail.size(); ++i) {
                    if (protectedBytes[i] || protectedBytes[i + 1]) continue;
                    if (tail[i] == ':' && tail[i + 1] == '=') { assignPos = i; break; }
                }
            }
            size_t assignLength = 2;
            const size_t defaultPos = findTopLevelSqlKeyword(tail, "default");
            if (defaultPos != std::string::npos &&
                (assignPos == std::string::npos || defaultPos < assignPos)) {
                assignPos = defaultPos;
                assignLength = 7;
            }
            defaults.push_back({name,
                trimCopy(tail.substr(0, assignPos)),
                assignPos == std::string::npos ? std::string() :
                    trimCopy(tail.substr(assignPos + assignLength))});
        }
    }

    StmtPtr parseStatement() {
        size_t at;
        std::string kw = sc.peekKeyword(&at);
        if (kw == "if") return parseIf();
        if (kw == "while") return parseWhile();
        if (kw == "for") return parseFor();
        if (kw == "loop") return parseLoop();
        if (kw == "return") {
            sc.pos = at + kw.size();
            auto s = std::make_unique<ReturnStmt>();
            std::string rest = trimCopy(readUntilSemicolon(sc));
            if (!rest.empty()) { s->hasExpr = true; s->expr = rest; }
            return s;
        }
        if (kw == "exit") {
            sc.pos = at + kw.size();
            auto s = std::make_unique<ExitStmt>();
            std::string rest = trimCopy(readUntilSemicolon(sc));
            std::string low = lowerCopy(rest);
            if (low.compare(0, 5, "when ") == 0) {
                s->hasWhen = true;
                s->cond = trimCopy(rest.substr(5));
            }
            return s;
        }
        if (kw == "raise") {
            sc.pos = at + kw.size();
            return parseRaise();
        }
        if (kw == "perform") {
            // PERFORM sql; → SELECT sql; (result discarded)
            sc.pos = at + kw.size();
            auto s = std::make_unique<SqlStmt>();
            s->text = "SELECT " + readUntilSemicolon(sc);
            return s;
        }
        // assignment: ident := expr;
        if ((!kw.empty() && (std::isalpha(static_cast<unsigned char>(kw[0])) || kw[0] == '_')) ||
            sc.peekChar() == '"') {
            // lookahead for ":=" after the identifier
            size_t save = sc.pos;
            std::string name = sc.ident();
            if (sc.matchOp(":=")) {
                auto s = std::make_unique<AssignStmt>();
                s->var = name;
                s->expr = readUntilSemicolon(sc);
                return s;
            }
            sc.pos = save;
        }
        // SQL passthrough
        auto s = std::make_unique<SqlStmt>();
        s->text = readUntilSemicolon(sc);
        if (s->text.empty()) { error = "empty statement"; return nullptr; }
        return s;
    }

    StmtPtr parseRaise() {
        auto s = std::make_unique<RaiseStmt>();
        s->level = "notice";
        std::string kw = sc.peekKeyword();
        if (kw == "notice" || kw == "warning" || kw == "error" || kw == "exception" ||
            kw == "log" || kw == "info" || kw == "debug") {
            s->level = kw;
            sc.matchKeyword(kw.c_str());
        }
        std::string text = readUntilSemicolon(sc);
        size_t p = 0;
        if (p < text.size() && text[p] == '\'') {
            ++p;
            while (p < text.size()) {
                if (text[p] == '\'') {
                    if (p + 1 < text.size() && text[p + 1] == '\'') { s->fmt += '\''; p += 2; }
                    else { ++p; break; }
                } else s->fmt += text[p++];
            }
        } else {
            s->fmt = text;
        }
        std::string rest = text.substr(p);
        size_t comma = rest.find(',');
        std::string argText = comma == std::string::npos ? "" : rest.substr(comma + 1);
        if (!trimCopy(argText).empty()) {
            std::string cur;
            int depth = 0; bool inS = false;
            for (char c : argText) {
                if (inS) { cur += c; if (c == '\'') inS = false; continue; }
                if (c == '\'') { inS = true; cur += c; continue; }
                if (c == '(') ++depth;
                if (c == ')') --depth;
                if (c == ',' && depth == 0) {
                    if (!trimCopy(cur).empty()) s->args.push_back(trimCopy(cur));
                    cur.clear(); continue;
                }
                cur += c;
            }
            if (!trimCopy(cur).empty()) s->args.push_back(trimCopy(cur));
        }
        return s;
    }

    StmtPtr parseIf() {
        if (!sc.matchKeyword("if")) return nullptr;
        auto s = std::make_unique<IfStmt>();
        std::string cond;
        if (!readUntilKeyword(sc, "then", cond)) { fail("IF without THEN"); return nullptr; }
        IfStmt::Branch b;
        b.hasCond = true;
        b.cond = cond;
        if (!parseCompoundInto(b.body)) return nullptr;
        s->branches.push_back(std::move(b));
        while (true) {
            if (sc.matchKeyword("elsif")) {
                IfStmt::Branch eb;
                std::string c2;
                if (!readUntilKeyword(sc, "then", c2)) { fail("ELSIF without THEN"); return nullptr; }
                eb.hasCond = true;
                eb.cond = c2;
                if (!parseCompoundInto(eb.body)) return nullptr;
                s->branches.push_back(std::move(eb));
                continue;
            }
            if (sc.matchKeyword("else")) {
                IfStmt::Branch eb;
                eb.hasCond = false;
                if (!parseCompoundInto(eb.body)) return nullptr;
                s->branches.push_back(std::move(eb));
            }
            if (!sc.matchKeyword("end")) { fail("IF without END IF"); return nullptr; }
            if (!sc.matchKeyword("if")) { fail("IF without END IF"); return nullptr; }
            if (!sc.matchOp(";")) { fail("missing ';' after END IF"); return nullptr; }
            return s;
        }
    }

    StmtPtr parseWhile() {
        if (!sc.matchKeyword("while")) return nullptr;
        auto s = std::make_unique<WhileStmt>();
        if (!readUntilKeyword(sc, "loop", s->cond)) { fail("WHILE without LOOP"); return nullptr; }
        if (!parseCompoundInto(s->body)) return nullptr;
        if (!expectEnd("loop", ";")) return nullptr;
        return s;
    }

    StmtPtr parseFor() {
        if (!sc.matchKeyword("for")) return nullptr;
        auto s = std::make_unique<ForStmt>();
        s->var = sc.ident();
        if (!sc.matchKeyword("in")) { fail("FOR without IN"); return nullptr; }
        if (sc.matchKeyword("reverse")) s->reverse = true;
        std::string range;
        if (!readUntilKeyword(sc, "loop", range)) { fail("FOR without LOOP"); return nullptr; }
        // a..b
        size_t dd = std::string::npos;
        for (size_t i = 0; i + 1 < range.size(); ++i) {
            if (range[i] == '.' && range[i + 1] == '.') { dd = i; break; }
        }
        if (dd == std::string::npos) { fail("FOR range without '..'"); return nullptr; }
        s->from = trimCopy(range.substr(0, dd));
        s->to = trimCopy(range.substr(dd + 2));
        if (!parseCompoundInto(s->body)) return nullptr;
        if (!expectEnd("loop", ";")) return nullptr;
        return s;
    }

    StmtPtr parseLoop() {
        if (!sc.matchKeyword("loop")) return nullptr;
        // plain LOOP → modeled as WHILE 'true'
        auto s = std::make_unique<WhileStmt>();
        s->cond = "true";
        if (!parseCompoundInto(s->body)) return nullptr;
        if (!expectEnd("loop", ";")) return nullptr;
        return s;
    }

    bool expectEnd(const char* kw, const char* term) {
        if (!sc.matchKeyword("end")) return fail(std::string("missing END ") + kw);
        if (!sc.matchKeyword(kw)) return fail(std::string("missing END ") + kw);
        if (term && !sc.matchOp(term)) return fail(std::string("missing '") + term + "'");
        return true;
    }

    // Parse statements until END (terminator not consumed).
    bool parseCompoundInto(CompoundStmt& out) {
        while (true) {
            if (sc.eof()) return fail("unexpected end of body");
            std::string kw = sc.peekKeyword();
            if (kw == "end" || kw == "elsif" || kw == "else") return true;
            StmtPtr s = parseStatement();
            if (!s) return false;
            out.body.push_back(std::move(s));
        }
    }
};

// silence unused warning for the placeholder
static void touchUnused() {}

// ---------------------------------------------------------------------------
// interpreter
// ---------------------------------------------------------------------------
struct Interp {
    const PlPgsqlHost& host;
    std::map<std::string, std::string> vars;
    std::set<std::string> nullVars;
    std::map<std::string, std::string> variableTypes;
    std::string returnValue;
    std::string returnType = "unknown";
    bool returnIsNull = false;
    bool hasReturn = false;
    bool exitLoop = false;
    std::string error;
    std::string errorSqlState;
    std::function<void(const std::string&, const std::string&)> notice;
    int steps = 0;
    static constexpr int kMaxSteps = 200000;  // runaway guard

    Interp(const PlPgsqlHost& h, const std::map<std::string, std::string>& params,
           std::function<void(const std::string&, const std::string&)> n)
        : host(h), vars(params), variableTypes(h.parameterTypes), notice(std::move(n)) {
        vars["found"] = "f";
        variableTypes["found"] = "boolean";
    }

    bool fail(const std::string& m, const std::string& sqlState = "XX000") {
        if (error.empty()) {
            error = m;
            errorSqlState = sqlState.empty() ? "XX000" : sqlState;
        }
        return false;
    }
    bool budget() {
        if (++steps > kMaxSteps) { fail("PL/pgSQL step budget exceeded"); return false; }
        return true;
    }

    // Substitution retains the NULL bitmap and SQL quoting. Qualified SQL
    // references are indivisible: an unrelated scalar variable named `id`
    // must not rewrite `source.id`. Explicit trigger bindings (NEW.id) remain
    // available as complete dotted variable keys.
    std::string substitute(const std::string& expr, bool sqlStatement = false) const {
        std::string out;
        const auto protectedBytes = sqlProtectedBytes(expr);
        struct SqlRole {
            bool selectList = false;
            bool projectionMayEnd = false;
            bool distinctOnExpected = false;
            bool projectionModifierExpected = false;
            bool projectionModifier = false;
            bool fromClause = false;
            bool relationExpected = false;
            bool aliasExpected = false;
            bool explicitAliasExpected = false;
            bool cteList = false;
            bool cteNameExpected = false;
            bool cteColumnsExpected = false;
            bool columnLabelsExpected = false;
            bool identifiersAreLabels = false;
            bool castFunctionExpected = false;
            bool castFunction = false;
            bool typeNameExpected = false;
            bool typeClause = false;
            bool collationNameExpected = false;
        };
        std::vector<SqlRole> roles(1);
        size_t i = 0;
        while (i < expr.size()) {
            if ((expr[i] == 'e' || expr[i] == 'E') && i + 1 < expr.size() &&
                expr[i + 1] == '\'' && protectedBytes[i + 1] &&
                (i == 0 || !sqlIdentifierContinuation(static_cast<unsigned char>(expr[i - 1])))) {
                const size_t end = protectedUnitEnd(expr, i + 1);
                out += expr.substr(i, end - i);
                roles.back().projectionMayEnd = true;
                i = end;
                continue;
            }
            if (protectedBytes[i] && expr[i] != '"') {
                const size_t end = protectedUnitEnd(expr, i);
                out += expr.substr(i, end - i);
                if (expr[i] == '\'' || expr[i] == '$')
                    roles.back().projectionMayEnd = true;
                i = end;
                continue;
            }
            std::string name;
            size_t end;
            if (readIdentifier(expr, i, name, end)) {
                const bool quoted = expr[i] == '"';
                size_t next = skipLeadingSqlTrivia(expr, end);
                while (next != std::string::npos && next < expr.size() && expr[next] == '.') {
                    const size_t component = skipLeadingSqlTrivia(expr, next + 1);
                    std::string field;
                    size_t fieldEnd;
                    if (component == std::string::npos ||
                        !readIdentifier(expr, component, field, fieldEnd)) {
                        // Also protect source.* from a scalar named source.
                        if (component < expr.size() && expr[component] == '*') {
                            end = component + 1;
                            name += ".*";
                        }
                        break;
                    }
                    name += "." + field;
                    end = fieldEnd;
                    next = skipLeadingSqlTrivia(expr, end);
                }
                if (sqlStatement && !quoted && name == "at") {
                    const size_t operatorEnd = sqlKeywordUnitEnd(expr, i, {"at", "time", "zone"});
                    if (operatorEnd != std::string::npos) {
                        out += expr.substr(i, operatorEnd - i);
                        roles.back().projectionMayEnd = false;
                        i = operatorEnd;
                        continue;
                    }
                }
                const auto variable = vars.find(name);
                const bool functionCall = next != std::string::npos &&
                    next < expr.size() && expr[next] == '(';
                bool sqlRole = false;
                SqlRole& role = roles.back();
                static const std::set<std::string> typeQualifiers = {
                    "precision", "varying", "with", "without", "time", "zone",
                    "character"
                };
                if (role.collationNameExpected) {
                    // COLLATE takes one (possibly qualified and delimited)
                    // name, not a data expression. readIdentifier above has
                    // consumed the complete qualified unit, including trivia.
                    role.collationNameExpected = false;
                    sqlRole = true;
                } else if (role.typeNameExpected) {
                    role.typeNameExpected = false;
                    role.typeClause = true;
                    sqlRole = true;
                } else if (role.typeClause && !quoted && typeQualifiers.count(name)) {
                    sqlRole = true;
                } else {
                    role.typeClause = false;
                }
                if (!quoted && name == "as" && role.castFunction) {
                    role.typeNameExpected = true;
                    sqlRole = true;
                }
                if (sqlStatement && !sqlRole) {
                    if (role.identifiersAreLabels) {
                        sqlRole = true;
                    } else if (!quoted && name == "collate") {
                        role.collationNameExpected = true;
                        sqlRole = true;
                    } else if (!quoted && name == "with") {
                        role.cteList = true;
                        role.cteNameExpected = true;
                        sqlRole = true;
                    } else if (role.cteNameExpected) {
                        sqlRole = true;
                        if (quoted || name != "recursive") {
                            role.cteNameExpected = false;
                            role.cteColumnsExpected = true;
                        }
                    } else if (!quoted && (name == "from" || name == "join")) {
                        role.selectList = false;
                        role.fromClause = true;
                        role.relationExpected = true;
                        role.aliasExpected = false;
                        role.explicitAliasExpected = false;
                        sqlRole = true;
                    } else if (!quoted && (name == "select" || name == "where" ||
                               name == "group" || name == "having" || name == "order" ||
                               name == "limit" || name == "offset" || name == "union" ||
                               name == "intersect" || name == "except" || name == "window")) {
                        role = {};
                        role.selectList = name == "select";
                        sqlRole = true;
                    } else if (!quoted && name == "as") {
                        role.explicitAliasExpected = true;
                        role.cteColumnsExpected = false;
                        sqlRole = true;
                    } else if (!quoted && (name == "on" || name == "using" ||
                               name == "inner" || name == "left" || name == "right" ||
                               name == "full" || name == "outer" || name == "cross" ||
                               name == "natural")) {
                        role.aliasExpected = false;
                        role.columnLabelsExpected = name == "using";
                        sqlRole = true;
                    } else if (role.explicitAliasExpected) {
                        // NOT MATERIALIZED belongs to CTE syntax, not to
                        // the variable namespace or an output alias.
                        if (!role.cteList || quoted || name != "not")
                            role.explicitAliasExpected = false;
                        role.aliasExpected = false;
                        sqlRole = true;
                    } else if (role.relationExpected) {
                        sqlRole = true;
                        if (quoted || (name != "lateral" && name != "only")) {
                            role.relationExpected = false;
                            role.aliasExpected = true;
                        }
                    } else if (role.aliasExpected) {
                        role.aliasExpected = false;
                        sqlRole = true;
                    } else if (role.selectList && role.projectionMayEnd &&
                               name.find('.') == std::string::npos) {
                        // An optional projection alias follows a completed
                        // expression and is immediately followed by the list
                        // delimiter or a SELECT clause. Do not interpret an
                        // operand after an operator as an alias.
                        bool aliasEnd = next == std::string::npos || next == expr.size() ||
                            (next < expr.size() && (expr[next] == ',' || expr[next] == ';' ||
                                                  expr[next] == ')'));
                        if (!aliasEnd) {
                            std::string following;
                            size_t followingEnd;
                            if (next < expr.size() && expr[next] != '"' &&
                                readIdentifier(expr, next, following, followingEnd)) {
                                static const std::set<std::string> clauses = {
                                    "from", "where", "group", "having", "order", "limit",
                                    "offset", "union", "intersect", "except", "window", "fetch", "for"
                                };
                                aliasEnd = clauses.count(following) != 0;
                            }
                        }
                        sqlRole = aliasEnd;
                    }
                }
                if (functionCall && !quoted && name == "cast")
                    role.castFunctionExpected = true;
                if (sqlStatement && role.selectList && !quoted && name == "distinct") {
                    role.distinctOnExpected = true;
                } else if (sqlStatement && role.selectList && !quoted && name == "on" &&
                           role.distinctOnExpected) {
                    role.projectionModifierExpected = true;
                    role.distinctOnExpected = false;
                } else if (role.distinctOnExpected) {
                    role.distinctOnExpected = false;
                }
                if (variable == vars.end() || functionCall || sqlRole) {
                    out += expr.substr(i, end - i);
                } else {
                    std::string literal;
                    if (nullVars.count(name)) {
                        literal = "NULL";
                    } else {
                        literal += '\'';
                        for (const char c : variable->second) {
                            literal += c;
                            if (c == '\'') literal += c;
                        }
                        literal += '\'';
                    }
                    const auto type = variableTypes.find(name);
                    if (sqlStatement && type != variableTypes.end() &&
                        !type->second.empty() && lowerCopy(type->second) != "unknown")
                        out += "CAST(" + literal + " AS " + type->second + ")";
                    else out += literal;
                }
                static const std::set<std::string> incompleteExpressionWords = {
                    "select", "as", "case", "when", "then", "else", "is", "not",
                    "and", "or", "between", "like", "ilike", "in", "collate",
                    "distinct", "all", "on"
                };
                role.projectionMayEnd = quoted || !incompleteExpressionWords.count(name);
                i = end;
                continue;
            }
            if (expr[i] == ':' && i + 1 < expr.size() && expr[i + 1] == ':') {
                roles.back().typeNameExpected = true;
                roles.back().projectionMayEnd = false;
                out += "::";
                i += 2;
                continue;
            }
            if (expr[i] == '(') {
                const bool labels = roles.back().cteColumnsExpected ||
                    roles.back().columnLabelsExpected || roles.back().typeClause;
                const bool castFunction = roles.back().castFunctionExpected;
                const bool projectionModifier = roles.back().projectionModifierExpected;
                roles.back().castFunctionExpected = false;
                roles.back().projectionModifierExpected = false;
                roles.back().columnLabelsExpected = false;
                if (sqlStatement && roles.back().relationExpected) {
                    roles.back().relationExpected = false;
                    roles.back().aliasExpected = true;
                }
                roles.emplace_back();
                roles.back().identifiersAreLabels = labels;
                roles.back().castFunction = castFunction;
                roles.back().projectionModifier = projectionModifier;
            } else if (expr[i] == ')' && roles.size() > 1) {
                const bool modifier = roles.back().projectionModifier;
                roles.pop_back();
                roles.back().projectionMayEnd = !modifier;
            } else if (expr[i] == ',') {
                roles.back().projectionMayEnd = false;
                roles.back().typeClause = false;
                if (sqlStatement && roles.back().cteList) {
                    roles.back().cteNameExpected = true;
                    roles.back().cteColumnsExpected = false;
                    roles.back().explicitAliasExpected = false;
                } else if (sqlStatement && roles.back().fromClause) {
                    roles.back().relationExpected = true;
                    roles.back().aliasExpected = false;
                }
            } else if (std::isdigit(static_cast<unsigned char>(expr[i])) || expr[i] == ']') {
                roles.back().projectionMayEnd = true;
            } else if (!std::isspace(static_cast<unsigned char>(expr[i]))) {
                roles.back().projectionMayEnd = false;
                if (expr[i] != '[') roles.back().typeClause = false;
            }
            out += expr[i++];
        }
        return out;
    }

    // Evaluate a PL/pgSQL expression: numeric/boolean/text operations the
    // native evaluator understands are computed locally; anything else
    // (function calls, ||, column expressions) defers to the host.
    std::optional<std::string> eval(const std::string& raw,
                                    bool* valueIsNull = nullptr,
                                    std::string* valueType = nullptr) {
        if (valueIsNull) *valueIsNull = false;
        if (valueType) *valueType = "unknown";
        std::string expr = trimCopy(raw);
        if (expr.empty()) {
            if (valueIsNull) *valueIsNull = true;
            return std::string("null");
        }
        std::string low = lowerCopy(expr);
        if (low == "true" || low == "false") {
            if (valueType) *valueType = "boolean";
            return std::string(low == "true" ? "t" : "f");
        }
        if (low == "null") {
            if (valueIsNull) *valueIsNull = true;
            return std::string("null");
        }
        // numeric literal
        if (expr.find_first_not_of("0123456789.-") == std::string::npos && !expr.empty()) {
            if (valueType) {
                *valueType = expr.find('.') == std::string::npos ? "integer" : "numeric";
                if (*valueType == "integer") {
                    try {
                        const auto number = std::stoll(expr);
                        if (number < -2147483648LL || number > 2147483647LL)
                            *valueType = "bigint";
                    } catch (...) { *valueType = "numeric"; }
                }
            }
            return expr;
        }
        // quoted literal
        if (expr.size() >= 2 && expr.front() == '\'' && expr.back() == '\'' &&
            protectedUnitEnd(expr, 0) == expr.size()) {
            if (valueType) *valueType = "text";
            std::string literal;
            for (size_t i = 1; i + 1 < expr.size(); ++i) {
                literal += expr[i];
                if (expr[i] == '\'' && i + 2 < expr.size() && expr[i + 1] == '\'') ++i;
            }
            return literal;
        }
        // Preserve NULL identity for variables instead of confusing it with
        // the perfectly valid text value "null".
        std::string name;
        size_t nameEnd;
        if (readIdentifier(expr, 0, name, nameEnd) && nameEnd == expr.size()) {
            auto variable = vars.find(name);
            if (variable != vars.end()) {
                if (valueIsNull) *valueIsNull = nullVars.count(name) != 0;
                if (valueType) {
                    const auto type = variableTypes.find(name);
                    if (type != variableTypes.end()) *valueType = type->second;
                }
                return variable->second;
            }
        }
        if (host.evalExprTyped) {
            const auto result = host.evalExprTyped(expr, vars, nullVars, variableTypes);
            if (!result.ok) {
                fail(result.message.empty() ? "expression evaluation failed" : result.message,
                     result.sqlState);
                return std::nullopt;
            }
            if (result.rowCount != 1 || result.columnCount != 1 || result.firstRow.size() != 1) {
                fail("expression evaluator returned invalid scalar metadata");
                return std::nullopt;
            }
            if (valueIsNull) *valueIsNull = !result.firstRow.front();
            if (valueType && result.columnTypes.size() == 1)
                *valueType = result.columnTypes.front();
            return result.firstRow.front().value_or("null");
        }
        auto native = nativeEval(expr);
        if (native) return native;
        if (host.evalExpr) {
            auto v = host.evalExpr(substitute(expr), vars);
            if (v) return *v;
        }
        return std::nullopt;
    }

    bool assignValue(const std::string& name,
                     std::optional<std::string> value,
                     const std::string& sourceType) {
        const auto target = variableTypes.find(name);
        if (host.coerceValueTyped && target != variableTypes.end() &&
            !target->second.empty() && lowerCopy(target->second) != "unknown") {
            const auto coerced = host.coerceValueTyped(value, sourceType, target->second);
            if (!coerced.ok)
                return fail(coerced.message.empty() ? "variable assignment failed" : coerced.message,
                            coerced.sqlState);
            if (coerced.rowCount != 1 || coerced.columnCount != 1 ||
                coerced.firstRow.size() != 1)
                return fail("variable coercion returned invalid scalar metadata");
            value = coerced.firstRow.front();
        }
        vars[name] = value.value_or("null");
        if (value) nullVars.erase(name);
        else nullVars.insert(name);
        if (target == variableTypes.end() && !sourceType.empty())
            variableTypes[name] = sourceType;
        return true;
    }

    // Tiny fallback evaluator: AND/OR/NOT + numeric comparisons + +-*/ on
    // numeric operands and bound variables (unquoted identifier values).
    std::optional<std::string> nativeEval(const std::string& expr) {
        // boolean connectors at top level (lowest precedence: OR, then AND)
        int depth = 0; bool inS = false;
        std::vector<size_t> ors, ands;
        for (size_t i = 0; i < expr.size(); ++i) {
            char c = expr[i];
            if (inS) { if (c == '\'') inS = false; continue; }
            if (c == '\'') { inS = true; continue; }
            if (c == '(') ++depth;
            else if (c == ')') --depth;
            else if (depth == 0 && i + 1 < expr.size()) {
                std::string two = expr.substr(i, 2);
                if (two == "||") continue;
                // word-boundary OR / AND
                if (i + 2 <= expr.size()) {
                    std::string w3 = lowerCopy(expr.substr(i, 3));
                    bool lw = (i == 0) || !std::isalnum(static_cast<unsigned char>(expr[i-1])) && expr[i-1] != '_';
                    bool rw = (i + 3 >= expr.size()) || (!std::isalnum(static_cast<unsigned char>(expr[i+3])) && expr[i+3] != '_');
                    if (w3 == "or " && lw) { ors.push_back(i); continue; }
                    if (w3 == "and" && lw && rw && (i + 3 == expr.size() || std::isspace(static_cast<unsigned char>(expr[i+3])))) {
                        ands.push_back(i); continue;
                    }
                }
            }
        }
        if (!ors.empty()) {
            size_t p = ors.front();
            auto l = nativeEval(expr.substr(0, p));
            auto r = nativeEval(expr.substr(p + 2));
            if (l && r) return (lowerCopy(*l) == "t" || lowerCopy(*r) == "t") ? std::string("t") : std::string("f");
            return std::nullopt;
        }
        if (!ands.empty()) {
            size_t p = ands.back();
            auto l = nativeEval(trimCopy(expr.substr(0, p)));
            auto r = nativeEval(trimCopy(expr.substr(p + 3)));
            if (l && r) return (lowerCopy(*l) == "t" && lowerCopy(*r) == "t") ? std::string("t") : std::string("f");
            return std::nullopt;
        }
        std::string e = trimCopy(expr);
        if (lowerCopy(e).compare(0, 4, "not ") == 0) {
            auto v = nativeEval(trimCopy(e.substr(4)));
            if (v) return lowerCopy(*v) == "t" ? std::string("f") : std::string("t");
            return std::nullopt;
        }
        // parentheses
        if (e.size() >= 2 && e.front() == '(' && e.back() == ')') {
            return nativeEval(e.substr(1, e.size() - 2));
        }
        // comparison operators
        static const char* ops[] = {"<=", ">=", "!=", "<>", "=", "<", ">"};
        for (const char* op : ops) {
            int d2 = 0; bool q2 = false;
            for (size_t i = 0; i + strlen(op) <= e.size(); ++i) {
                char c = e[i];
                if (q2) { if (c == '\'') q2 = false; continue; }
                if (c == '\'') { q2 = true; continue; }
                if (c == '(') ++d2;
                else if (c == ')') --d2;
                else if (d2 == 0 && e.compare(i, strlen(op), op) == 0) {
                    // longest-match guard for <= / >= vs = <
                    if ((op[0] == '<' || op[0] == '>' || op[0] == '!') &&
                        strlen(op) == 1 && i + 1 < e.size() &&
                        (e[i+1] == '=')) continue;
                    std::string ls = trimCopy(e.substr(0, i));
                    std::string rs = trimCopy(e.substr(i + strlen(op)));
                    auto lv = evalLiteral(ls);
                    auto rv = evalLiteral(rs);
                    if (!lv || !rv) return std::nullopt;
                    bool ln = isNum(*lv), rn = isNum(*rv);
                    if (ln && rn) {
                        double a = std::stod(*lv), b = std::stod(*rv);
                        bool res = (std::string(op) == "=") ? a == b :
                                   (std::string(op) == "<") ? a < b :
                                   (std::string(op) == ">") ? a > b :
                                   (std::string(op) == "<=") ? a <= b :
                                   (std::string(op) == ">=") ? a >= b :
                                   (std::string(op) == "!=" || std::string(op) == "<>") ? a != b : false;
                        return res ? std::string("t") : std::string("f");
                    }
                    // text compare
                    int cmp = lv->compare(*rv);
                    bool res = (std::string(op) == "=") ? cmp == 0 :
                               (std::string(op) == "<") ? cmp < 0 :
                               (std::string(op) == ">") ? cmp > 0 :
                               (std::string(op) == "<=") ? cmp <= 0 :
                               (std::string(op) == ">=") ? cmp >= 0 :
                               (std::string(op) == "!=" || std::string(op) == "<>") ? cmp != 0 : false;
                    return res ? std::string("t") : std::string("f");
                }
            }
        }
        // arithmetic: left-associative, leftmost top-level binary
        // occurrence wins; + and - are tried before * and / so that
        // 1 + 2 * 3 splits at + first and the right side recurses.
        static const char* aops[] = {"+", "-", "*", "/"};
        for (const char* op : aops) {
            std::optional<size_t> hit;
            int d2 = 0; bool q2 = false;
            for (size_t i = 0; i + 1 < e.size(); ++i) {
                char c = e[i];
                if (q2) { if (c == '\'') q2 = false; continue; }
                if (c == '\'') { q2 = true; continue; }
                if (c == '(') ++d2;
                else if (c == ')') --d2;
                else if (i > 0 && d2 == 0 && c == op[0] &&
                         i + 1 < e.size()) {
                    size_t p2 = i;
                    while (p2 > 0 && std::isspace(static_cast<unsigned char>(e[p2 - 1]))) --p2;
                    if (p2 == 0) continue;  // leading sign: unary
                    char prev = e[p2 - 1];
                    if (!(std::isalnum(static_cast<unsigned char>(prev)) ||
                          prev == ')' || prev == '_' || prev == '\'')) {
                        continue;  // unary after an operator
                    }
                    hit = i;  // keep scanning: RIGHTMOST wins (left assoc)
                }
            }
            if (!hit) continue;
            size_t i = *hit;
            std::string ls = trimCopy(e.substr(0, i));
            std::string rs = trimCopy(e.substr(i + 1));
            auto lv = evalLiteral(ls);
            auto rv = evalLiteral(rs);
            if (!lv || !rv || !isNum(*lv) || !isNum(*rv)) return std::nullopt;
            double a = std::stod(*lv), b = std::stod(*rv);
            double r = (op[0] == '+') ? a + b :
                       (op[0] == '-') ? a - b :
                       (op[0] == '*') ? a * b :
                       (b != 0 ? a / b : 0);
            if (r == std::floor(r) && std::abs(r) < 1e15) {
                return std::to_string(static_cast<long long>(r));
            }
            std::ostringstream os;
            os << r;
            return os.str();
        }
        // bound variable?
        auto it = vars.find(lowerCopy(e));
        if (it != vars.end()) return it->second;
        return std::nullopt;
    }

    // Resolve a literal/variable/parenthesized operand.
    std::optional<std::string> evalLiteral(const std::string& s) {
        std::string t = trimCopy(s);
        if (t.empty()) return std::nullopt;
        if (t.size() >= 2 && t.front() == '\'' && t.back() == '\'') {
            return t.substr(1, t.size() - 2);
        }
        if (t.find_first_not_of("0123456789.-") == std::string::npos) return t;
        if (t.front() == '(' && t.back() == ')') {
            auto inner = nativeEval(t.substr(1, t.size() - 2));
            if (inner) return inner;
        }
        auto it = vars.find(lowerCopy(t));
        if (it != vars.end()) return it->second;
        auto v = nativeEval(t);
        return v;
    }

    static bool isNum(const std::string& s) {
        return !s.empty() && s.find_first_not_of("0123456789.-") == std::string::npos;
    }

    bool execCompound(const CompoundStmt& c) {
        for (const auto& s : c.body) {
            if (!budget()) return false;
            if (!exec(*s) || !error.empty()) return false;
            if (hasReturn || exitLoop) return true;
        }
        return true;
    }

    bool exec(const Stmt& s) {
        if (auto* a = dynamic_cast<const AssignStmt*>(&s)) {
            bool valueIsNull = false;
            std::string valueType;
            auto v = eval(a->expr, &valueIsNull, &valueType);
            if (!v) return fail("assignment evaluation failed: " + a->expr.substr(0, 40));
            return assignValue(a->var, valueIsNull ? std::nullopt : v, valueType);
        }
        if (auto* r = dynamic_cast<const ReturnStmt*>(&s)) {
            if (r->hasExpr) {
                auto v = eval(r->expr, &returnIsNull, &returnType);
                if (!v) return fail("RETURN evaluation failed: " + r->expr.substr(0, 40));
                returnValue = *v;
            } else {
                returnIsNull = true;
            }
            hasReturn = true;
            return true;
        }
        if (auto* x = dynamic_cast<const ExitStmt*>(&s)) {
            if (x->hasWhen) {
                auto v = eval(x->cond);
                if (v && lowerCopy(*v) != "t") return true;
            }
            exitLoop = true;
            return true;
        }
        if (auto* rs = dynamic_cast<const RaiseStmt*>(&s)) {
            std::vector<std::string> vals;
            for (const auto& a : rs->args) {
                if (a.size() >= 2 && a.front() == '\'' && a.back() == '\'') {
                    vals.push_back(a.substr(1, a.size() - 2));
                } else {
                    auto v = eval(a);
                    vals.push_back(v ? *v : std::string());
                }
            }
            // PG RAISE formats: % = next arg (any type), %% = literal %.
            std::string msg;
            size_t ai = 0;
            for (size_t i = 0; i < rs->fmt.size(); ++i) {
                if (rs->fmt[i] == '%' && i + 1 < rs->fmt.size() &&
                    rs->fmt[i+1] == '%') {
                    msg += '%'; ++i;
                } else if (rs->fmt[i] == '%' && ai < vals.size()) {
                    msg += vals[ai++];
                } else {
                    msg += rs->fmt[i];
                }
            }
            if (notice) notice(rs->level, msg);
            if (rs->level == "error" || rs->level == "exception") {
                return fail("PL/pgSQL exception: " + msg);
            }
            return true;
        }
        if (auto* i = dynamic_cast<const IfStmt*>(&s)) {
            for (const auto& b : i->branches) {
                if (!b.hasCond) return execCompound(b.body);
                auto v = eval(b.cond);
                if (v && (lowerCopy(*v) == "t" || *v == "1" || lowerCopy(*v) == "true")) {
                    return execCompound(b.body);
                }
            }
            return true;
        }
        if (auto* w = dynamic_cast<const WhileStmt*>(&s)) {
            while (true) {
                if (!budget()) return false;
                auto v = eval(w->cond);
                if (!v || lowerCopy(*v) != "t") break;
                exitLoop = false;
                if (!execCompound(w->body)) return false;
                if (hasReturn) return true;
                if (exitLoop) { exitLoop = false; break; }
            }
            return true;
        }
        if (auto* f = dynamic_cast<const ForStmt*>(&s)) {
            auto fromV = eval(f->from);
            auto toV = eval(f->to);
            if (!fromV || !toV || !isNum(*fromV) || !isNum(*toV)) {
                return fail("FOR range not numeric");
            }
            long long a = std::stoll(*fromV), b = std::stoll(*toV);
            // Integer FOR declares an iterator local to this loop. Its
            // numeric value must not inherit a same-named outer NULL bit,
            // and nested loops/RETURN/errors restore that outer binding.
            struct IteratorScope {
                std::map<std::string, std::string>& values;
                std::set<std::string>& nulls;
                std::map<std::string, std::string>& types;
                std::string name;
                std::optional<std::string> previous;
                bool previousIsNull;
                std::optional<std::string> previousType;
                ~IteratorScope() {
                    if (previous) values[name] = *previous;
                    else values.erase(name);
                    if (previousIsNull) nulls.insert(name);
                    else nulls.erase(name);
                    if (previousType) types[name] = *previousType;
                    else types.erase(name);
                }
            };
            const auto previous = vars.find(f->var);
            const auto previousType = variableTypes.find(f->var);
            IteratorScope iteratorScope{vars, nullVars, variableTypes, f->var,
                previous == vars.end() ? std::nullopt : std::optional<std::string>{previous->second},
                nullVars.count(f->var) != 0,
                previousType == variableTypes.end() ? std::nullopt :
                    std::optional<std::string>{previousType->second}};
            variableTypes[f->var] = "integer";
            if (!f->reverse) {
                for (long long i = a; i <= b; ++i) {
                    if (!budget()) return false;
                    vars[f->var] = std::to_string(i);
                    nullVars.erase(f->var);
                    exitLoop = false;
                    if (!execCompound(f->body)) return false;
                    if (hasReturn) return true;
                    if (exitLoop) { exitLoop = false; break; }
                }
            } else {
                for (long long i = a; i >= b; --i) {
                    if (!budget()) return false;
                    vars[f->var] = std::to_string(i);
                    nullVars.erase(f->var);
                    exitLoop = false;
                    if (!execCompound(f->body)) return false;
                    if (hasReturn) return true;
                    if (exitLoop) { exitLoop = false; break; }
                }
            }
            return true;
        }
        if (auto* q = dynamic_cast<const SqlStmt*>(&s)) {
            return execSql(q->text);
        }
        return fail("unknown statement");
    }

    bool execSql(const std::string& stmtText) {
        std::string sql = trimCopy(stmtText);
        if (sql.empty()) return true;
        const size_t selectPos = findTopLevelSqlKeyword(sql, "select");
        const size_t first = skipLeadingSqlTrivia(sql);
        const bool selectStatement = selectPos == first ||
            findTopLevelSqlKeyword(sql, "with") == first;
        if (selectStatement && selectPos != std::string::npos) {
            const size_t intoPos = findTopLevelSqlKeyword(sql, "into");
            if (intoPos != std::string::npos) {
                Scanner targets(sql);
                targets.pos = intoPos + 4;
                const bool strict = targets.matchKeyword("strict");
                std::vector<std::string> intoVars;
                size_t targetEnd = targets.pos;
                while (true) {
                    const std::string name = targets.ident();
                    if (name.empty()) return fail("SELECT INTO: expected target variable", "42601");
                    if (!vars.count(name)) {
                        return fail("SELECT INTO: unknown target variable " + name, "42601");
                    }
                    intoVars.push_back(name);
                    targetEnd = targets.pos;
                    if (!targets.matchOp(",")) break;
                }
                // Keep every SQL clause and protected byte outside INTO.
                // A space prevents concatenating SELECT/target expressions
                // when INTO has no surrounding whitespace (e.g. INTO "n").
                std::string prefix = sql.substr(0, intoPos);
                std::string suffix = sql.substr(targetEnd);
                if (!prefix.empty() && !suffix.empty() &&
                    std::isspace(static_cast<unsigned char>(prefix.back())) &&
                    std::isspace(static_cast<unsigned char>(suffix.front()))) {
                    suffix.erase(0, 1);
                } else if (!prefix.empty() && !suffix.empty() &&
                           !std::isspace(static_cast<unsigned char>(prefix.back())) &&
                           !std::isspace(static_cast<unsigned char>(suffix.front()))) {
                    prefix += ' ';
                }
                const std::string query = substitute(trimCopy(prefix + suffix), true);
                if (host.query) {
                    const PlPgsqlQueryResult result = host.query(query);
                    if (!result.ok) {
                        return fail(result.message.empty() ? "SELECT INTO failed" : result.message,
                                    result.sqlState);
                    }
                    if (result.rowCount && result.firstRow.size() != result.columnCount) {
                        return fail("SELECT INTO: invalid host result shape");
                    }
                    if (strict && !result.rowCount) return fail("query returned no rows", "P0002");
                    if (strict && result.rowCount > 1) return fail("query returned more than one row", "P0003");
                    for (size_t i = 0; i < intoVars.size(); ++i) {
                        const auto value = result.rowCount && i < result.firstRow.size()
                            ? result.firstRow[i] : std::nullopt;
                        const std::string sourceType = i < result.columnTypes.size()
                            ? result.columnTypes[i] : "unknown";
                        if (!assignValue(intoVars[i], value, sourceType)) return false;
                    }
                    vars["found"] = result.rowCount ? "t" : "f";
                    nullVars.erase("found");
                    return true;
                }
                if (!host.selectInto) return fail("SELECT INTO unsupported by host");
                if (strict) return fail("SELECT INTO STRICT requires a row-count-aware host", "0A000");
                // Compatibility adapter: the historical callback accepts
                // SELECT-rest, not the whole SELECT. It cannot represent a
                // per-cell NULL bitmap or count multiple rows.
                const size_t querySelect = findTopLevelSqlKeyword(query, "select");
                if (querySelect != skipLeadingSqlTrivia(query)) {
                    return fail("CTE SELECT INTO requires a complete-query host", "0A000");
                }
                const int rc = host.selectInto(trimCopy(query.substr(querySelect + 6)), intoVars, vars);
                if (rc != 0 && rc != 1) return fail("SELECT INTO failed");
                for (const auto& v : intoVars) {
                    if (rc == 1) { vars[v] = "null"; nullVars.insert(v); }
                    else nullVars.erase(v);
                }
                vars["found"] = rc ? "f" : "t";
                nullVars.erase("found");
                return true;
            }
        }
        if (!host.execStmt) return fail("SQL execution unsupported by host");
        if (!host.execStmt(substitute(sql, true), vars)) {
            return fail("SQL failed: " + sql.substr(0, 60));
        }
        return true;
    }
};

}  // namespace plpgsql_impl

// ---------------------------------------------------------------------------
// public entry
// ---------------------------------------------------------------------------
bool PlPgsql::run(const std::string& body,
                  const std::map<std::string, std::string>& params,
                  const PlPgsqlHost& host,
                  std::string& returnValue,
                  std::string& error,
                  NoticeSink notice,
                  bool* returnIsNull,
                  const std::set<std::string>* nullParams,
                  std::string* errorSqlState,
                  std::string* returnType) {
    using namespace plpgsql_impl;
    returnValue.clear();
    error.clear();
    if (errorSqlState) errorSqlState->clear();
    if (returnType) *returnType = "unknown";
    if (returnIsNull) *returnIsNull = false;

    Parser parser(body);
    std::vector<Declaration> defaults;
    parser.sc.matchKeyword("declare");  // optional leading DECLARE section
    if (!parser.parseDeclares(defaults)) {
        error = parser.error.empty() ? "DECLARE parse failed" : parser.error;
        if (errorSqlState) *errorSqlState = "42601";
        return false;
    }
    CompoundStmt program;
    if (!parser.parseCompoundInto(program)) {
        error = parser.error.empty() ? "body parse failed" : parser.error;
        if (errorSqlState) *errorSqlState = "42601";
        return false;
    }
    // trailing END (of BEGIN block): tolerate optional "end;" / "end;"
    parser.sc.matchKeyword("end");
    parser.sc.matchOp(";");
    if (!parser.sc.eof() && !trimCopy(parser.sc.src.substr(parser.sc.pos)).empty()) {
        // tolerate trailing whitespace only
        std::string rest = trimCopy(parser.sc.src.substr(parser.sc.pos));
        lowerCopy(rest);
        if (rest == ";" || rest.empty()) {
            // ok
        }
    }

    std::function<void(const std::string&, const std::string&)> sink = notice;
    Interp interp(host, params, sink);
    if (nullParams) interp.nullVars.insert(nullParams->begin(), nullParams->end());
    for (const auto& d : defaults) {
        if (interp.vars.count(d.name)) continue;
        if (!d.type.empty()) interp.variableTypes[d.name] = d.type;
        if (trimCopy(d.defaultExpr).empty()) {
            if (!interp.assignValue(d.name, std::nullopt, "unknown")) {
                error = interp.error;
                if (errorSqlState) *errorSqlState = interp.errorSqlState;
                return false;
            }
            continue;
        }
        // Evaluate the default expression in an environment holding only
        // previously declared defaults (PL/pgSQL allows earlier vars).
        bool valueIsNull = false;
        std::string valueType;
        auto v = interp.eval(d.defaultExpr, &valueIsNull, &valueType);
        if (!interp.error.empty()) {
            error = interp.error;
            if (errorSqlState) *errorSqlState = interp.errorSqlState;
            return false;
        }
        if (!interp.assignValue(d.name, valueIsNull ? std::nullopt :
                std::optional<std::string>{v.value_or(d.defaultExpr)}, valueType)) {
            error = interp.error;
            if (errorSqlState) *errorSqlState = interp.errorSqlState;
            return false;
        }
    }
    if (!interp.execCompound(program)) {
        error = interp.error.empty() ? "runtime error" : interp.error;
        if (errorSqlState) *errorSqlState = interp.errorSqlState.empty() ? "XX000" : interp.errorSqlState;
        return false;
    }
    if (interp.hasReturn) {
        returnValue = interp.returnValue;
        if (returnIsNull) *returnIsNull = interp.returnIsNull;
        if (returnType) *returnType = interp.returnType;
        return true;
    }
    // no RETURN: function body ends (NULL for functions)
    returnValue = "null";
    if (returnIsNull) *returnIsNull = true;
    return true;
}

}  // namespace dbms
