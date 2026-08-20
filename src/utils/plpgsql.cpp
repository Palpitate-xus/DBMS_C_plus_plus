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

#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
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

// Character-level scanner with string/comment skipping.
struct Scanner {
    std::string src;
    size_t pos = 0;
    explicit Scanner(std::string s) : src(std::move(s)) {}

    void skipWs() {
        while (pos < src.size()) {
            char c = src[pos];
            if (std::isspace(static_cast<unsigned char>(c))) { ++pos; continue; }
            if (c == '-' && pos + 1 < src.size() && src[pos + 1] == '-') {
                while (pos < src.size() && src[pos] != '\n') ++pos;
                continue;
            }
            if (c == '/' && pos + 1 < src.size() && src[pos + 1] == '*') {
                pos += 2;
                while (pos + 1 < src.size() &&
                       !(src[pos] == '*' && src[pos + 1] == '/')) ++pos;
                pos = std::min(pos + 2, src.size());
                continue;
            }
            break;
        }
    }
    bool eof() { skipWs(); return pos >= src.size(); }

    std::string peekWord(size_t* at = nullptr) {
        skipWs();
        if (at) *at = pos;
        size_t p = pos;
        while (p < src.size() &&
               (std::isalnum(static_cast<unsigned char>(src[p])) || src[p] == '_')) ++p;
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
        if (pos < src.size() && src[pos] == '"') {
            size_t p = ++pos;
            std::string out;
            while (p < src.size() && src[p] != '"') out += src[p++];
            pos = p < src.size() ? p + 1 : p;
            return out;
        }
        size_t p = pos;
        while (p < src.size() &&
               (std::isalnum(static_cast<unsigned char>(src[p])) || src[p] == '_')) ++p;
        std::string out = src.substr(pos, p - pos);
        pos = p;
        return lowerCopy(out);
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
    bool inS = false, inD = false;
    int depth = 0;
    while (sc.pos < sc.src.size()) {
        char c = sc.src[sc.pos];
        if (inS) {
            if (c == '\'') {
                if (sc.pos + 1 < sc.src.size() && sc.src[sc.pos + 1] == '\'') ++sc.pos;
                else inS = false;
            }
        } else if (inD) {
            if (c == '"') inD = false;
        } else if (c == '\'') {
            inS = true;
        } else if (c == '"') {
            inD = true;
        } else if (c == '(') {
            ++depth;
        } else if (c == ')') {
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
    bool inS = false, inD = false;
    int depth = 0;
    const size_t klen = std::strlen(kw);
    while (sc.pos < sc.src.size()) {
        char c = sc.src[sc.pos];
        if (inS) {
            if (c == '\'') {
                if (sc.pos + 1 < sc.src.size() && sc.src[sc.pos + 1] == '\'') ++sc.pos;
                else inS = false;
            }
        } else if (inD) {
            if (c == '"') inD = false;
        } else if (c == '\'') {
            inS = true;
        } else if (c == '"') {
            inD = true;
        } else if (c == '(') {
            ++depth;
        } else if (c == ')') {
            --depth;
        } else if (depth == 0 &&
                   std::isalpha(static_cast<unsigned char>(c)) &&
                   (sc.pos == start ||
                    (!std::isalnum(static_cast<unsigned char>(sc.src[sc.pos - 1])) &&
                     sc.src[sc.pos - 1] != '_'))) {
            // word start: compare keyword
            size_t e = sc.pos;
            while (e < sc.src.size() &&
                   (std::isalnum(static_cast<unsigned char>(sc.src[e])) || sc.src[e] == '_')) ++e;
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
    bool parseDeclares(std::map<std::string, std::string>& defaults) {
        while (true) {
            if (sc.matchKeyword("begin")) return true;
            std::string name = sc.ident();
            if (name.empty()) return fail("DECLARE: expected variable name");
            // optional type words until := or ';'
            std::string tail = readUntilSemicolon(sc);
            std::string low = lowerCopy(tail);
            size_t assignPos = std::string::npos;
            // find ":=" outside quotes
            {
                bool inS = false;
                for (size_t i = 0; i + 1 < tail.size(); ++i) {
                    if (inS) { if (tail[i] == '\'') inS = false; continue; }
                    if (tail[i] == '\'') { inS = true; continue; }
                    if (tail[i] == ':' && tail[i + 1] == '=') { assignPos = i; break; }
                }
            }
            if (assignPos != std::string::npos) {
                defaults[name] = trimCopy(tail.substr(assignPos + 2));
            }
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
        if (!kw.empty() && std::isalpha(static_cast<unsigned char>(kw[0]))) {
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
    std::string returnValue;
    bool hasReturn = false;
    bool exitLoop = false;
    std::string error;
    std::function<void(const std::string&, const std::string&)> notice;
    int steps = 0;
    static constexpr int kMaxSteps = 200000;  // runaway guard

    Interp(const PlPgsqlHost& h, const std::map<std::string, std::string>& params,
           std::function<void(const std::string&, const std::string&)> n)
        : host(h), vars(params), notice(std::move(n)) {}

    bool fail(const std::string& m) {
        if (error.empty()) error = m;
        return false;
    }
    bool budget() {
        if (++steps > kMaxSteps) { fail("PL/pgSQL step budget exceeded"); return false; }
        return true;
    }

    // Substitute bound identifiers with literal values.
    std::string substitute(const std::string& expr) const {
        std::string out;
        size_t i = 0;
        while (i < expr.size()) {
            if (expr[i] == '\'') {
                out += expr[i];
                size_t j = i + 1;
                while (j < expr.size()) {
                    out += expr[j];
                    if (expr[j] == '\'') {
                        if (j + 1 < expr.size() && expr[j + 1] == '\'') out += expr[++j];
                        ++j;
                        break;
                    }
                    ++j;
                }
                i = j;
                continue;
            }
            if (std::isalpha(static_cast<unsigned char>(expr[i])) || expr[i] == '_') {
                size_t j = i;
                while (j < expr.size() &&
                       (std::isalnum(static_cast<unsigned char>(expr[j])) || expr[j] == '_')) ++j;
                std::string word = expr.substr(i, j - i);
                auto it = vars.find(lowerCopy(word));
                if (it != vars.end()) {
                    const std::string& v = it->second;
                    bool numeric = !v.empty() &&
                        v.find_first_not_of("0123456789.-") == std::string::npos;
                    bool nullv = lowerCopy(v) == "null" || v.empty();
                    if (nullv) out += "NULL";
                    else if (numeric) out += v;
                    else out += "'" + v + "'";
                } else {
                    out += word;
                }
                i = j;
                continue;
            }
            out += expr[i++];
        }
        return out;
    }

    // Evaluate a PL/pgSQL expression: numeric/boolean/text operations the
    // native evaluator understands are computed locally; anything else
    // (function calls, ||, column expressions) defers to the host.
    std::optional<std::string> eval(const std::string& raw) {
        std::string expr = trimCopy(raw);
        if (expr.empty()) return std::string("null");
        std::string low = lowerCopy(expr);
        if (low == "true") return std::string("t");
        if (low == "false") return std::string("f");
        if (low == "null") return std::string("null");
        // numeric literal
        if (expr.find_first_not_of("0123456789.-") == std::string::npos && !expr.empty()) {
            return expr;
        }
        // quoted literal
        if (expr.size() >= 2 && expr.front() == '\'' && expr.back() == '\'') {
            return expr.substr(1, expr.size() - 2);
        }
        auto native = nativeEval(expr);
        if (native) return native;
        if (host.evalExpr) {
            auto v = host.evalExpr(substitute(expr), vars);
            if (v) return *v;
        }
        return std::nullopt;
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
            if (!exec(*s)) return false;
            if (hasReturn || exitLoop) return true;
        }
        return true;
    }

    bool exec(const Stmt& s) {
        if (auto* a = dynamic_cast<const AssignStmt*>(&s)) {
            auto v = eval(a->expr);
            if (!v) return fail("assignment evaluation failed: " + a->expr.substr(0, 40));
            vars[a->var] = *v;
            return true;
        }
        if (auto* r = dynamic_cast<const ReturnStmt*>(&s)) {
            if (r->hasExpr) {
                auto v = eval(r->expr);
                if (!v) return fail("RETURN evaluation failed: " + r->expr.substr(0, 40));
                returnValue = *v;
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
            if (!f->reverse) {
                for (long long i = a; i <= b; ++i) {
                    if (!budget()) return false;
                    vars[f->var] = std::to_string(i);
                    exitLoop = false;
                    if (!execCompound(f->body)) return false;
                    if (hasReturn) return true;
                    if (exitLoop) { exitLoop = false; break; }
                }
            } else {
                for (long long i = a; i >= b; --i) {
                    if (!budget()) return false;
                    vars[f->var] = std::to_string(i);
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
        std::string low = lowerCopy(sql);
        if (low.compare(0, 6, "select") == 0) {
            // top-level " into " detection
            bool inS = false; int depth = 0;
            size_t intoPos = std::string::npos;
            for (size_t i = 0; i + 5 <= sql.size(); ++i) {
                char c = sql[i];
                if (inS) { if (c == '\'') inS = false; continue; }
                if (c == '\'') { inS = true; continue; }
                if (c == '(') { ++depth; continue; }
                if (c == ')') { --depth; continue; }
                if (depth == 0 && lowerCopy(sql.substr(i, 5)) == " into" &&
                    (i + 5 == sql.size() ||
                     std::isspace(static_cast<unsigned char>(sql[i + 5])))) {
                    intoPos = i;
                    break;
                }
            }
            if (intoPos != std::string::npos) {
                std::string selectPart = trimCopy(sql.substr(0, intoPos));
                std::string intoPart = trimCopy(sql.substr(intoPos + 5));
                std::vector<std::string> intoVars;
                std::string cur;
                for (char c : intoPart) {
                    if (c == ',') { intoVars.push_back(lowerCopy(trimCopy(cur))); cur.clear(); }
                    else cur += c;
                }
                intoVars.push_back(lowerCopy(trimCopy(cur)));
                if (!host.selectInto) return fail("SELECT INTO unsupported by host");
                int rc = host.selectInto(substitute(selectPart), intoVars, vars);
                if (rc == 2) return fail("SELECT INTO failed");
                if (rc == 1) {
                    for (const auto& v : intoVars) vars[v] = "null";
                }
                return true;
            }
        }
        if (!host.execStmt) return fail("SQL execution unsupported by host");
        if (!host.execStmt(substitute(sql), vars)) {
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
                  NoticeSink notice) {
    using namespace plpgsql_impl;
    returnValue.clear();
    error.clear();

    Parser parser(body);
    std::map<std::string, std::string> defaults;
    parser.sc.matchKeyword("declare");  // optional leading DECLARE section
    if (!parser.parseDeclares(defaults)) {
        error = parser.error.empty() ? "DECLARE parse failed" : parser.error;
        return false;
    }
    CompoundStmt program;
    if (!parser.parseCompoundInto(program)) {
        error = parser.error.empty() ? "body parse failed" : parser.error;
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
    for (const auto& d : defaults) {
        if (interp.vars.count(d.first)) continue;
        if (trimCopy(d.second).empty()) {
            interp.vars[d.first] = "null";
            continue;
        }
        // Evaluate the default expression in an environment holding only
        // previously declared defaults (PL/pgSQL allows earlier vars).
        auto v = interp.eval(d.second);
        interp.vars[d.first] = v ? *v : d.second;
    }
    if (!interp.execCompound(program)) {
        error = interp.error.empty() ? "runtime error" : interp.error;
        return false;
    }
    if (interp.hasReturn) {
        returnValue = interp.returnValue;
        return true;
    }
    // no RETURN: function body ends (NULL for functions)
    returnValue = "null";
    return true;
}

}  // namespace dbms
