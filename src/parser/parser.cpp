#include "parser.h"
#include "catalog/catalog.h"
#include "common/SqlTrivia.h"
#include "common/DbError.h"
#include "utils/interval_type.h"
#include <charconv>
#include <cctype>
#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <set>
#include <sstream>
#include <unordered_map>

namespace dbms {

namespace {
struct TokenProvenance { std::vector<std::pair<size_t, size_t>> spans; };
struct BindingParseContext {
    const std::string& source;
    size_t base = 0;
    size_t pendingBase = 0;
    bool fetchGrammarError = false;
    std::unordered_map<const std::string*, TokenProvenance> tokens;
};
thread_local BindingParseContext* bindingParse = nullptr;
thread_local std::string* declarationSyntaxError = nullptr;
struct DeclarationSyntaxAbort {};
static void retainDeclarationSyntaxError(const DbError& error) {
    if(declarationSyntaxError) {
        if(declarationSyntaxError->empty())*declarationSyntaxError=error.what();
        // Stop this parse rather than return an unconsumed/null expression
        // to a SELECT/CASE/function loop. The public parse API catches this.
        throw DeclarationSyntaxAbort{};
    }
}
struct BindingSourceScope {
    size_t oldBase = 0;
    explicit BindingSourceScope(size_t leading) {
        if (!bindingParse) return;
        oldBase = bindingParse->base;
        bindingParse->base = bindingParse->pendingBase + leading;
    }
    ~BindingSourceScope() { if (bindingParse) bindingParse->base = oldBase; }
};
void markSource(Expr* expression, const std::vector<std::string>& tokens,
                size_t begin, size_t end) {
    if (!bindingParse || !expression || begin >= end) return;
    const auto found = bindingParse->tokens.find(tokens.data());
    if (found == bindingParse->tokens.end() || end > found->second.spans.size()) return;
    expression->sourceBegin = found->second.spans[begin].first;
    expression->sourceEnd = found->second.spans[end - 1].second;
}
std::pair<size_t, size_t> statementSource(const std::vector<std::string>& tokens,
                                        size_t begin, size_t end) {
    if (bindingParse && begin < end) {
        const auto found = bindingParse->tokens.find(tokens.data());
        if (found != bindingParse->tokens.end() && end <= found->second.spans.size())
            return {found->second.spans[begin].first, found->second.spans[end - 1].second};
    }
    return {std::string::npos, std::string::npos};
}
}

// Reassemble a lexer token stream back into SQL text.  Qualified references
// arrive as three tokens ("jb", ".", "bid") because '.' is its own token
// class; the naive space join produces "jb . bid", which downstream
// executors treat as an unknown column.  Collapse identifier-dot-identifier
// triples into "jb.bid" so views/CTEs stored from parsed statements stay
// executable.
static std::string joinSqlTokens(const std::vector<std::string>& tokens) {
    std::string out;
    auto isIdent = [](const std::string& s) {
        if (s.empty()) return false;
        for (char c : s) {
            if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_') return false;
        }
        return true;
    };
    for (size_t i = 0; i < tokens.size();) {
        if (i + 2 < tokens.size() && tokens[i + 1] == "." &&
            isIdent(tokens[i]) && isIdent(tokens[i + 2])) {
            if (!out.empty()) out += ' ';
            out += tokens[i] + "." + tokens[i + 2];
            i += 3;
            // further .x segments (a.b.c)
            while (i + 1 < tokens.size() && tokens[i] == "." && isIdent(tokens[i + 1])) {
                out += "." + tokens[i + 1];
                i += 2;
            }
            continue;
        }
        if (!out.empty()) {
            // Tight SQL reconstruction: no space before '(' / ')' / ',' / ';'
            // and none after '(' or '.' — keeps stored view SQL executable
            // ('sum(v)' rather than 'sum ( v )', which downstream derived-
            // table expansion cannot parse).
            const std::string& prev = out;
            const char nextCh = tokens[i].empty() ? ' ' : tokens[i][0];
            const char lastCh = prev.back();
            const bool prevIsIdent = std::isalnum(static_cast<unsigned char>(lastCh)) || lastCh == '_';
            const bool noSpace = nextCh == ')' || nextCh == ',' || nextCh == ';' ||
                                 (nextCh == '(' && prevIsIdent) || lastCh == '(' || lastCh == '.';
            if (!noSpace) out += ' ';
        }
        out += tokens[i];
        ++i;
    }
    return out;
}

static std::string stripQuotes(const std::string& s) {
    if (s.size() >= 2 && ((s.front() == '\'' && s.back() == '\'') ||
                          (s.front() == '"' && s.back() == '"'))) {
        std::string inner = s.substr(1, s.size() - 2);
        // PostgreSQL escaping: '' inside a single-quoted literal is one '.
        // The tokenizer keeps both quotes in the token; collapse them here
        // so consumers see the decoded value. String literals only — quoted
        // identifiers "x" have no doubling escape.
        if (s.front() == '\'') {
            std::string out;
            out.reserve(inner.size());
            for (size_t i = 0; i < inner.size(); ++i) {
                out += inner[i];
                if (inner[i] == '\'' && i + 1 < inner.size() && inner[i + 1] == '\'') {
                    ++i; // skip the doubled second quote
                }
            }
            return out;
        }
        return inner;
    }
    return s;
}

static std::string parseRoutineIdentifier(const std::string& token) {
    if (token.size() >= 2 && token.front() == '"' && token.back() == '"') {
        std::string identifier;
        identifier.reserve(token.size() - 2);
        for (size_t i = 1; i + 1 < token.size(); ++i) {
            if (token[i] == '"' && i + 2 < token.size() &&
                token[i + 1] == '"') {
                ++i;
            }
            identifier.push_back(token[i]);
        }
        return identifier;
    }
    std::string identifier = token;
    std::transform(identifier.begin(), identifier.end(), identifier.begin(),
                   [](unsigned char value) {
                       return static_cast<char>(std::tolower(value));
                   });
    return identifier;
}

static bool declaredTypePrefixEligible(const std::string& token) {
    if(!token.empty() && token.front()=='"')return true;
    // PostgreSQL18.6 pg_get_keywords() categories: reserved words and
    // non-type COL_NAME keywords cannot begin generic type/function names.
    // Standard type grammar remains available, independently of type lookup.
    // Declaration parsing is also needed while global storage engines flush
    // domain schemas at shutdown. Immutable grammar data must outlive that
    // phase; a late-initialized heap set is destroyed before those engines.
    static constexpr const char* excluded[]={
        "all","analyse","analyze","and","any","array",
        "as","asc","asymmetric","between","both","case",
        "cast","check","coalesce","collate","column","constraint",
        "create","current_catalog","current_date","current_role","current_time","current_timestamp",
        "current_user","default","deferrable","desc","distinct","do",
        "else","end","except","exists","extract","false",
        "fetch","for","foreign","from","grant","greatest",
        "group","grouping","having","in","initially","inout",
        "intersect","into","json_array","json_arrayagg","json_exists","json_object",
        "json_objectagg","json_query","json_scalar","json_serialize","json_table","json_value",
        "lateral","leading","least","limit","localtime","localtimestamp",
        "merge_action","none","normalize","not","null","nullif",
        "offset","on","only","or","order","out",
        "overlay","placing","position","precision","primary","references",
        "returning","row","select","session_user","setof","some",
        "substring","symmetric","system_user","table","then","to",
        "trailing","treat","trim","true","union","unique",
        "user","using","values","variadic","when","where",
        "window","with","xmlattributes","xmlconcat","xmlelement","xmlexists",
        "xmlforest","xmlnamespaces","xmlparse","xmlpi","xmlroot","xmlserialize",
        "xmltable"
    };
    const auto keyword=SQLParser::toLower(token);
    for (const char* entry : excluded)
        if (keyword==entry) return false;
    return true;
}

static ColumnDef consumeDeclaredType(const std::vector<std::string>& tokens,
                                     size_t& pos) {
    ColumnDef definition;
    const auto identifier = [](const std::string& token) {
        return !token.empty() && (token.front() == '"' ||
            std::isalpha(static_cast<unsigned char>(token.front())) || token.front() == '_');
    };
    if (pos >= tokens.size() || !identifier(tokens[pos]) || !declaredTypePrefixEligible(tokens[pos]))
        throw DbError("42601", "type name is required");
    definition.typeName = tokens[pos++];
    bool qualified = false;
    if (pos < tokens.size() && tokens[pos] == ".") {
        qualified = true;
        ++pos;
        if (pos >= tokens.size() || !identifier(tokens[pos]))
            throw DbError("42601", "qualified type name is incomplete");
        definition.typeName += "." + tokens[pos++];
    }
    const bool quoted = definition.typeName.front() == '"';
    const std::string initial = SQLParser::toLower(definition.typeName);
    if (!qualified && !quoted && pos < tokens.size()) {
        const std::string next = SQLParser::toLower(tokens[pos]);
        if (((initial == "bit" || initial == "character") && next == "varying") ||
            (initial == "double" && next == "precision"))
            definition.typeName += " " + tokens[pos++];
    }
    if (!qualified && !quoted && initial == "interval") {
        int mask = interval_type_detail::fullRange;
        bool hasFields = false;
        if (pos < tokens.size()) {
            std::string phrase = SQLParser::toLower(tokens[pos]);
            if (interval_type_detail::fieldMask(phrase)) {
                hasFields = true;
                ++pos;
                if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "to") {
                    ++pos;
                    if (pos >= tokens.size()) throw DbError("42601", "incomplete interval field range");
                    phrase += " to " + SQLParser::toLower(tokens[pos++]);
                }
                mask = interval_type_detail::fieldMask(phrase);
                if (!mask) throw DbError("42601", "invalid interval field range");
                definition.typeMods.push_back(std::to_string(mask));
            }
        }
        if (pos < tokens.size() && tokens[pos] == "(") {
            if (hasFields && !(mask & interval_type_detail::second))
                throw DbError("42601", "interval field precision requires SECOND");
            ++pos;
            if (pos >= tokens.size() || tokens[pos].empty() ||
                !std::all_of(tokens[pos].begin(), tokens[pos].end(), [](unsigned char c) { return std::isdigit(c); }))
                throw DbError("42601", "interval precision requires one unsigned integer");
            if (!hasFields) definition.typeMods.push_back(std::to_string(mask));
            definition.typeMods.push_back(tokens[pos++]);
            if (pos >= tokens.size() || tokens[pos++] != ")")
                throw DbError("42601", "invalid interval precision");
        }
        if (pos < tokens.size() && interval_type_detail::fieldMask(SQLParser::toLower(tokens[pos])))
            throw DbError("42601", "unexpected interval field qualifier");
    } else if (pos < tokens.size() && tokens[pos] == "(") {
        ++pos;
        bool needsValue = true;
        while (pos < tokens.size() && tokens[pos] != ")") {
            if (needsValue) {
                std::string value;
                if (tokens[pos] == "+" || tokens[pos] == "-") value = tokens[pos++];
                if (pos >= tokens.size() || tokens[pos] == "," || tokens[pos] == ")")
                    throw DbError("42601", "invalid type modifier");
                value += tokens[pos++];
                definition.typeMods.push_back(value);
            } else if (tokens[pos++] != ",") {
                throw DbError("42601", "invalid type modifier separator");
            }
            needsValue = !needsValue;
        }
        if (pos >= tokens.size() || needsValue)
            throw DbError("42601", "invalid type modifier list");
        ++pos;
    }
    if (!qualified && !quoted && initial == "float") {
        // FLOAT(p) selects a physical IEEE type; p is not a runtime typmod.
        // SQL's omitted precision is float8, 1..24 chooses float4, 25..53
        // chooses float8. Its grammar accepts one unsigned integer only.
        unsigned precision = 53;
        if (!definition.typeMods.empty()) {
            const auto& modifier = definition.typeMods.front();
            if (definition.typeMods.size() != 1 || modifier.empty() ||
                !std::all_of(modifier.begin(), modifier.end(), [](unsigned char c) { return std::isdigit(c); }))
                throw DbError("42601", "FLOAT precision requires one unsigned integer");
            const auto parsed = std::from_chars(modifier.data(), modifier.data()+modifier.size(), precision);
            if (parsed.ec != std::errc() || parsed.ptr != modifier.data()+modifier.size())
                throw DbError("22023", "precision for type float must be less than 54 bits");
        }
        if (precision < 1) throw DbError("22023", "precision for type float must be at least 1 bit");
        if (precision > 53) throw DbError("22023", "precision for type float must be less than 54 bits");
        definition.typeName = precision <= 24 ? "real" : "double precision";
        definition.typeMods.clear();
    }
    if(!qualified && !quoted && !definition.typeMods.empty()) {
        static constexpr const char* noModifierGrammar[]={
            "bigint","boolean","int","integer","real","smallint","double precision"};
        const auto name=SQLParser::toLower(definition.typeName);
        for (const char* entry : noModifierGrammar)
            if(name==entry)
                throw DbError("42601","type declaration does not allow modifiers");
        if(initial=="char" || initial=="character" || initial=="varchar") {
            const auto& modifier=definition.typeMods.front();
            if(definition.typeMods.size()!=1 || modifier.empty() ||
               !std::all_of(modifier.begin(),modifier.end(),[](unsigned char c){return std::isdigit(c);}))
                throw DbError("42601","character length requires one unsigned integer");
        }
    }
    if (!qualified && !quoted && (initial == "time" || initial == "timestamp") &&
        pos + 2 < tokens.size()) {
        const std::string zone = SQLParser::toLower(tokens[pos]);
        if ((zone == "with" || zone == "without") &&
            SQLParser::toLower(tokens[pos + 1]) == "time" &&
            SQLParser::toLower(tokens[pos + 2]) == "zone") {
            definition.typeName = initial + (zone == "with" ? "tz" : "");
            pos += 3;
        }
    }
    while (pos < tokens.size() && tokens[pos] == "[") {
        ++pos;
        if (pos < tokens.size() && tokens[pos] != "]") {
            size_t ignored = 0;
            const auto& bound = tokens[pos++];
            const auto parsed = std::from_chars(bound.data(), bound.data() + bound.size(), ignored);
            if (parsed.ec != std::errc() || parsed.ptr != bound.data() + bound.size())
                throw DbError("42601", "invalid array type bound");
        }
        if (pos >= tokens.size() || tokens[pos++] != "]")
            throw DbError("42601", "array type suffix is incomplete");
        definition.isArray = true;
    }
    return definition;
}

ColumnDef SQLParser::parseTypeSpecification(const std::string& sql) {
    const auto tokens = tokenize(sql);
    size_t pos = 0;
    auto definition = consumeDeclaredType(tokens, pos);
    if (pos != tokens.size())
        throw DbError("42601", "unexpected tokens in type specification");
    return definition;
}

static std::string renderDeclaredType(const ColumnDef& definition) {
    std::string result=definition.typeName;
    if (SQLParser::toLower(result) == "interval") {
        result = interval_type_detail::render(result, definition.typeMods);
    } else if(!definition.typeMods.empty()) {
        result+='(';
        for(size_t i=0;i<definition.typeMods.size();++i)
            result+=(i?",":"")+definition.typeMods[i];
        result+=')';
    }
    if(definition.isArray)result+="[]";
    return result;
}

static bool parseNonNegativeInteger(const std::string& token, size_t& value) {
    if (token.empty()) return false;
    const bool negative = token.front() == '-';
    const size_t firstDigit =
        token.front() == '+' || negative ? 1 : 0;
    if (firstDigit == token.size()) return false;
    for (size_t index = firstDigit; index < token.size(); ++index) {
        const unsigned char c = static_cast<unsigned char>(token[index]);
        if (!std::isdigit(c)) return false;
    }
    if (negative && std::any_of(
            token.begin() + firstDigit,
            token.end(), [](char c) { return c != '0'; })) {
        return false;
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

static bool parseNonNegativeInteger(const std::vector<std::string>& tokens,
                                    size_t& pos, size_t& value) {
    if (pos >= tokens.size()) return false;

    bool negative = false;
    size_t numberPos = pos;
    if (tokens[numberPos] == "+" || tokens[numberPos] == "-") {
        negative = tokens[numberPos] == "-";
        ++numberPos;
    }
    if (numberPos >= tokens.size()) return false;

    size_t parsed = 0;
    if (!parseNonNegativeInteger(tokens[numberPos], parsed) ||
        (negative && parsed != 0)) {
        return false;
    }

    value = parsed;
    pos = numberPos + 1;
    return true;
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

static bool parseSignedInteger(const std::vector<std::string>& tokens,
                               size_t& position, int& value) {
    if (position >= tokens.size()) return false;
    std::string token = tokens[position];
    size_t consumed = 1;
    if ((token == "+" || token == "-") && position + 1 < tokens.size()) {
        token += tokens[position + 1];
        consumed = 2;
    }
    if (!parseSignedInteger(token, value)) return false;
    position += consumed;
    return true;
}

static bool parseInt64Token(const std::string& token, int64_t& value) {
    if (token.empty()) return false;
    const char* begin = token.data();
    const char* end = begin + token.size();
    if (*begin == '+') {
        ++begin;
        if (begin == end) return false;
    }
    const auto result = std::from_chars(begin, end, value);
    return result.ec == std::errc{} && result.ptr == end;
}

static bool parseFetchInteger(const std::vector<std::string>& tokens,size_t& position,int64_t& value) {
    if(position>=tokens.size())return false;
    std::string token=tokens[position];size_t consumed=1;
    if((token=="+" || token=="-") && position+1<tokens.size()) {
        token+=tokens[position+1];consumed=2;
    }
    if(!parseInt64Token(token,value))return false;
    position+=consumed;return true;
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

static bool parseQualifiedObjectName(
    const std::vector<std::string>& tokens, size_t& pos,
    std::string& name) {
    const auto invalidName = [](const std::string& token,
                                bool rejectClauseKeyword) {
        static const std::set<std::string> clauseKeywords = {
            "values", "default", "select", "set", "using", "on",
            "where", "returning"
        };
        return token.empty() || token == "." || token == "," ||
               token == ";" ||
               (rejectClauseKeyword && token.front() != '"' &&
                clauseKeywords.count(SQLParser::toLower(token)) != 0);
    };
    if (pos >= tokens.size() || invalidName(tokens[pos], false)) {
        return false;
    }
    name = tokens[pos++];
    if (pos < tokens.size() && tokens[pos] == ".") {
        if (pos + 1 >= tokens.size() ||
            invalidName(tokens[pos + 1], true)) {
            return false;
        }
        name += "." + tokens[pos + 1];
        pos += 2;
    }
    return true;
}

static bool parseForeignKeyTarget(
    const std::vector<std::string>& tokens, size_t& pos,
    std::string& name) {
    const size_t first = pos;
    std::string raw;
    if (!parseQualifiedObjectName(tokens, pos, raw)) return false;
    // A relation spelling is resolved later, unlike a decoded column name.
    // Preserve quoted component boundaries so embedded dots/spaces/quotes
    // cannot become qualification separators, while folding unquoted names.
    const auto relationPart = [](const std::string& token) {
        return token.size() >= 2 && token.front() == '"' && token.back() == '"'
            ? token : parseRoutineIdentifier(token);
    };
    name = relationPart(tokens[first]);
    if (pos > first + 1) {
        name += "." + relationPart(tokens[first + 2]);
    }
    return true;
}
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
    if (bindingParse && begin < end) {
        const auto found = bindingParse->tokens.find(tokens.data());
        if (found != bindingParse->tokens.end() && end <= found->second.spans.size()) {
            const size_t first = found->second.spans[begin].first;
            const size_t last = found->second.spans[end - 1].second;
            bindingParse->pendingBase = first;
            return bindingParse->source.substr(first, last - first);
        }
    }
    std::string result;
    for (size_t i = begin; i < end; ++i) {
        if (!result.empty()) result += ' ';
        result += tokens[i];
    }
    return result;
}

// Scalar SQL envelopes are otherwise bound later. Validate only this actual
// child grammar role before any outer catalog lookup, retaining the original
// AST/source envelope and excluding quoted FETCH data/identifiers.
static bool subqueryFetchGrammarValid(const std::vector<std::string>& tokens,size_t begin,size_t end) {
    if(begin>=end || end>tokens.size() || std::none_of(tokens.begin()+begin,tokens.begin()+end,
        [](const auto& token){return SQLParser::toLower(token)=="fetch";}))return true;
    static thread_local size_t depth=0;
    if(depth>=127)throw DbError("54001","query binding nesting limit exceeded");
    ++depth;struct RestoreDepth{size_t& value;~RestoreDepth(){--value;}}restore{depth};
    SQLParser parser;const auto parsed=parser.parseForBinding(joinParserTokens(tokens,begin,end));
    if(parsed.error!="WITH TIES cannot be specified without ORDER BY clause")return true;
    if(bindingParse)bindingParse->fetchGrammarError=true;
    return false;
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

static bool isDecimalMantissaToken(const std::string& s) {
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

static bool isNumericExponentPrefix(const std::string& s) {
    return s.size() > 1 && (s.back() == 'e' || s.back() == 'E') &&
           isDecimalMantissaToken(s.substr(0, s.size() - 1));
}

static bool terminatesTrailingDecimal(char ch) {
    const unsigned char value = static_cast<unsigned char>(ch);
    return !std::isalnum(value) && ch != '_' && ch != '.';
}

static bool isNumericToken(const std::string& s) {
    if (s.empty()) return false;
    size_t exponent = s.find_first_of("eE");
    if (exponent == std::string::npos)
        return isDecimalMantissaToken(s);
    if (!isDecimalMantissaToken(s.substr(0, exponent))) return false;

    size_t pos = exponent + 1;
    if (pos < s.size() && (s[pos] == '+' || s[pos] == '-')) ++pos;
    if (pos == s.size()) return false;
    for (; pos < s.size(); ++pos) {
        if (!std::isdigit(static_cast<unsigned char>(s[pos]))) return false;
    }
    return true;
}

static bool isStringLiteralToken(const std::string& s) {
    return s.size() >= 2 && s.front() == '\'' && s.back() == '\'';
}

static bool isBitStringLiteralToken(const std::string& s) {
    return s.size() >= 3 &&
           (s.front() == 'B' || s.front() == 'b' ||
            s.front() == 'X' || s.front() == 'x') &&
           s[1] == '\'' && s.back() == '\'';
}

static std::string normalizeEscapeStringToken(const std::string& token) {
    // Convert E'...' into an equivalent standard-conforming quoted token so
    // every downstream parser consumer sees the same decoded literal form.
    if (token.size() < 3 || (token[0] != 'E' && token[0] != 'e') ||
        token[1] != '\'' || token.back() != '\'') {
        return token;
    }
    const auto hexValue = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    const auto appendCodePoint = [](std::string& output, uint32_t value) {
        if (value <= 0x7f) {
            output.push_back(static_cast<char>(value));
        } else if (value <= 0x7ff) {
            output.push_back(static_cast<char>(0xc0 | (value >> 6)));
            output.push_back(static_cast<char>(0x80 | (value & 0x3f)));
        } else if (value <= 0xffff) {
            output.push_back(static_cast<char>(0xe0 | (value >> 12)));
            output.push_back(
                static_cast<char>(0x80 | ((value >> 6) & 0x3f)));
            output.push_back(static_cast<char>(0x80 | (value & 0x3f)));
        } else if (value <= 0x10ffff) {
            output.push_back(static_cast<char>(0xf0 | (value >> 18)));
            output.push_back(
                static_cast<char>(0x80 | ((value >> 12) & 0x3f)));
            output.push_back(
                static_cast<char>(0x80 | ((value >> 6) & 0x3f)));
            output.push_back(static_cast<char>(0x80 | (value & 0x3f)));
        }
    };

    std::string decoded;
    for (size_t i = 2; i + 1 < token.size(); ++i) {
        const char c = token[i];
        if (c == '\'' && i + 2 < token.size() && token[i + 1] == '\'') {
            decoded.push_back('\'');
            ++i;
            continue;
        }
        if (c != '\\' || i + 2 >= token.size()) {
            decoded.push_back(c);
            continue;
        }
        const char escaped = token[++i];
        switch (escaped) {
            case 'b': decoded.push_back('\b'); continue;
            case 'f': decoded.push_back('\f'); continue;
            case 'n': decoded.push_back('\n'); continue;
            case 'r': decoded.push_back('\r'); continue;
            case 't': decoded.push_back('\t'); continue;
            case '\n': continue;
            default: break;
        }
        if (escaped >= '0' && escaped <= '7') {
            unsigned int value = static_cast<unsigned int>(escaped - '0');
            size_t digits = 1;
            while (digits < 3 && i + 1 < token.size() - 1 &&
                   token[i + 1] >= '0' && token[i + 1] <= '7') {
                value = value * 8 +
                        static_cast<unsigned int>(token[++i] - '0');
                ++digits;
            }
            decoded.push_back(static_cast<char>(value & 0xff));
            continue;
        }
        if (escaped == 'x') {
            unsigned int value = 0;
            size_t digits = 0;
            while (digits < 2 && i + 1 < token.size() - 1) {
                const int digit = hexValue(token[i + 1]);
                if (digit < 0) break;
                value = value * 16 + static_cast<unsigned int>(digit);
                ++i;
                ++digits;
            }
            if (digits != 0) {
                decoded.push_back(static_cast<char>(value));
                continue;
            }
        }
        if (escaped == 'u' || escaped == 'U') {
            const size_t required = escaped == 'u' ? 4 : 8;
            uint32_t value = 0;
            size_t digits = 0;
            while (digits < required && i + 1 < token.size() - 1) {
                const int digit = hexValue(token[i + 1]);
                if (digit < 0) break;
                value = value * 16 + static_cast<uint32_t>(digit);
                ++i;
                ++digits;
            }
            if (digits == required) {
                appendCodePoint(decoded, value);
                continue;
            }
        }
        // PostgreSQL escape strings accept a backslash before an otherwise
        // ordinary character as that character itself.
        decoded.push_back(escaped);
    }

    std::string normalized = "'";
    for (char c : decoded) {
        normalized.push_back(c);
        if (c == '\'') normalized.push_back('\'');
    }
    normalized.push_back('\'');
    return normalized;
}

std::string SQLParser::trim(const std::string& s) {
    size_t a = 0;
    while (a < s.size() && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    size_t b = s.size();
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

std::vector<std::string> SQLParser::tokenize(const std::string& sql) {
    return tokenizeImpl(sql, nullptr);
}

std::string SQLParser::lexicalError(const std::string& sql) {
    std::string error;
    (void)tokenizeImpl(sql, &error);
    return error;
}

std::optional<std::string> SQLParser::duplicateCteName(const std::string& sql) {
    std::string lexicalError;
    const auto tokens = tokenizeImpl(sql, &lexicalError);
    if (!lexicalError.empty()) return std::nullopt;
    // Build matching parentheses once: skipping nested CTE/query bodies must
    // not repeatedly traverse them for every enclosing WITH scope.
    std::vector<size_t> closes(tokens.size(), std::string::npos), opens;
    for (size_t i = 0; i < tokens.size(); ++i) {
        if (tokens[i] == "(") opens.push_back(i);
        else if (tokens[i] == ")" && !opens.empty()) {
            closes[opens.back()] = i;
            opens.pop_back();
        }
    }
    const auto isIdentifier = [](const std::string& token) {
        if (token.size() >= 2 && token.front() == '"' && token.back() == '"')
            return token.size() > 2;
        if (token.empty()) return false;
        const unsigned char first = static_cast<unsigned char>(token.front());
        if (!std::isalpha(first) && first != '_' && first < 0x80) return false;
        for (unsigned char ch : token)
            if (!std::isalnum(ch) && ch != '_' && ch != '$' && ch < 0x80) return false;
        return true;
    };
    for (size_t with = 0; with < tokens.size(); ++with) {
        if (toLower(tokens[with]) != "with") continue;
        size_t pos = with + 1;
        if (pos < tokens.size() && toLower(tokens[pos]) == "recursive") ++pos;
        std::set<std::string> names;
        while (pos < tokens.size() && isIdentifier(tokens[pos])) {
            const std::string name = parseRoutineIdentifier(tokens[pos++]);
            if (pos < tokens.size() && tokens[pos] == "(") {
                if (closes[pos] == std::string::npos) break;
                pos = closes[pos] + 1;
            }
            if (pos == tokens.size() || toLower(tokens[pos]) != "as") break;
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "not") {
                if (pos + 1 == tokens.size() || toLower(tokens[pos + 1]) != "materialized") break;
                pos += 2;
            } else if (pos < tokens.size() && toLower(tokens[pos]) == "materialized") ++pos;
            if (pos == tokens.size() || tokens[pos] != "(" ||
                closes[pos] == std::string::npos) break;
            pos = closes[pos] + 1;
            if (!names.insert(name).second) return name;
            if (pos == tokens.size() || tokens[pos] != ",") break;
            ++pos;
        }
    }
    return std::nullopt;
}

std::vector<std::string> SQLParser::tokenizeImpl(const std::string& sql, std::string* error) {
    std::vector<std::string> tokens;
    std::vector<std::pair<size_t, size_t>> spans;
    std::string cur;
    size_t curBegin = 0;
    const auto emit = [&](std::string value, size_t begin, size_t end) {
        tokens.push_back(std::move(value));
        spans.emplace_back(begin + (bindingParse ? bindingParse->base : 0),
                           end + (bindingParse ? bindingParse->base : 0));
    };
    const auto flush = [&](size_t end) {
        if (!cur.empty()) { emit(cur, curBegin, end); cur.clear(); }
    };
    bool inString = false;
    bool inEscapeString = false;
    char stringChar = 0;
    bool inIdentifier = false; // "quoted identifier"
    const auto identifierContinuation = [](unsigned char c) {
        return std::isalnum(c) || c == '_' || c == '$' || c >= 0x80;
    };

    for (size_t i = 0; i < sql.size(); ++i) {
        char c = sql[i];
        // Dollar-quoted string: $tag$ ... $tag$ (PostgreSQL). The whole
        // construct becomes ONE token that carries the raw body including
        // quotes/newlines/semicolons, so function bodies survive tokenization.
        // Token shape: $tag$<body>$tag$; stripQuotes()/body extraction can
        // split it because the delimiter is recorded verbatim at both ends.
        if (!inString && !inIdentifier && c == '$' &&
            (i == 0 || !identifierContinuation(static_cast<unsigned char>(sql[i - 1])))) {
            // Parse $tag$ opener: '$', {alpha|_|digit}*, '$' (tag must not
            // start with a digit, matching PostgreSQL). Empty tag ($$) is the
            // common anonymous form.
            size_t j = i + 1;
            while (j < sql.size()) {
                const char tc = sql[j];
                if (std::isalnum(static_cast<unsigned char>(tc)) || tc == '_') {
                    // A leading digit after '$' is not a tag (positional
                    // params etc.); only continue on non-first chars.
                    if (j == i + 1 && std::isdigit(static_cast<unsigned char>(tc))) break;
                    ++j;
                } else {
                    break;
                }
            }
            if (j < sql.size() && sql[j] == '$') {
                const std::string delim = sql.substr(i, j - i + 1); // "$tag$"
                const size_t bodyStart = j + 1;
                const size_t close = sql.find(delim, bodyStart);
                if (close != std::string::npos) {
                    if (!cur.empty()) {
                        flush(i);
                    }
                    // Emit as a single-quoted string token so downstream
                    // string handling (stripQuotes, literals) treats it as a
                    // string value: '<body>' with embedded quotes escaped the
                    // way the rest of the parser expects ('' per ').
                    std::string body = sql.substr(bodyStart, close - bodyStart);
                    std::string quoted = "'";
                    for (char bc : body) {
                        quoted += bc;
                        if (bc == '\'') quoted += '\''; // double-up escapes
                    }
                    quoted += '\'';
                    emit(quoted, i, close + delim.size());
                    i = close + delim.size() - 1; // continue after closer
                    continue;
                }
                if (error) {
                    *error = "unterminated dollar-quoted string";
                    return tokens;
                }
                // Unchecked token consumers retain their existing fallback;
                // SQL execution validates the complete raw input first.
            }
        }
        if (inString) {
            cur += c;
            if (c == stringChar) {
                // With standard_conforming_strings, only E strings treat
                // backslashes as escapes. A plain trailing backslash must
                // not hide the closing quote or a subsequent block comment.
                size_t backslashCount = 0;
                if (inEscapeString) {
                    for (size_t j = cur.size() - 2; j + 1 > 0 && cur[j] == '\\'; --j) {
                        ++backslashCount;
                    }
                }
                if (backslashCount % 2 != 0) continue;
                // An E-string escaped quote followed immediately by its
                // closing quote is not a doubled-quote escape. Check the
                // backslash first, then the ordinary doubled-quote rule.
                if (i + 1 < sql.size() && sql[i + 1] == stringChar) {
                    cur += sql[i + 1];
                    ++i;
                    continue;
                }
                inString = false;
                emit(inEscapeString ? normalizeEscapeStringToken(cur) : cur, curBegin, i + 1);
                cur.clear();
                inEscapeString = false;
            }
            continue;
        }
        if (inIdentifier) {
            cur += c;
            if (c == '"') {
                if (i + 1 < sql.size() && sql[i + 1] == '"') {
                    cur += sql[++i];
                    continue;
                }
                inIdentifier = false;
                emit(cur, curBegin, i + 1);
                cur.clear();
            }
            continue;
        }
        if (c == '\'' || c == '"') {
            if (c == '\'' && (cur == "E" || cur == "e")) {
                inString = true;
                inEscapeString = true;
                stringChar = '\'';
                cur += c;
                continue;
            }
            // PostgreSQL bit-string constants have no whitespace between the
            // B/X introducer and the quote. Preserve the introducer in the
            // token so the expression analyzer can distinguish them from
            // ordinary unknown string literals.
            if (c == '\'' &&
                (cur == "B" || cur == "b" || cur == "X" || cur == "x")) {
                inString = true;
                inEscapeString = false;
                stringChar = '\'';
                cur += c;
                continue;
            }
            if (!cur.empty()) {
                flush(i);
            }
            if (c == '\'') {
                inString = true;
                inEscapeString = false;
                stringChar = '\'';
            } else {
                inIdentifier = true;
            }
            if (cur.empty()) curBegin = i;
            cur += c;
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(c))) {
            if (!cur.empty()) {
                flush(i);
            }
            continue;
        }
        if (c == '(' || c == ')' || c == ',' || c == ';' || c == '*' ||
            c == '=' || c == '<' || c == '>' || c == '+' || c == '-' ||
            c == '/' || c == '%' || c == '^' || c == '~' || c == '!' ||
            c == '|' || c == '&' || c == '#' || c == '@' || c == '?' ||
            c == ':' || c == '[' || c == ']' || c == '.') {
            // Keep decimal points in numeric tokens, including PostgreSQL's
            // legal leading/trailing forms (.5 / 5.) and scientific 5.e1.
            const bool startsLeadingDecimal =
                c == '.' && cur.empty() && i + 1 < sql.size() &&
                std::isdigit(static_cast<unsigned char>(sql[i + 1]));
            const bool continuesDecimal =
                c == '.' && isDecimalMantissaToken(cur) &&
                cur.find('.') == std::string::npos &&
                (i + 1 == sql.size() ||
                 std::isdigit(static_cast<unsigned char>(sql[i + 1])) ||
                 sql[i + 1] == 'e' || sql[i + 1] == 'E' ||
                 terminatesTrailingDecimal(sql[i + 1]));
            if (startsLeadingDecimal || continuesDecimal) {
                if (cur.empty()) curBegin = i;
                cur += c;
                continue;
            }
            // A sign immediately after a complete decimal mantissa plus e/E
            // belongs to the exponent; ordinary arithmetic signs still split.
            if ((c == '+' || c == '-') && i + 1 < sql.size() &&
                std::isdigit(static_cast<unsigned char>(sql[i + 1])) &&
                isNumericExponentPrefix(cur)) {
                cur += c;
                continue;
            }
            if (!cur.empty()) {
                flush(i);
            }
            // multi-char operators
            if (i + 1 < sql.size()) {
                char next = sql[i + 1];
                std::string two = std::string(1, c) + next;
                if (i + 2 < sql.size() &&
                    ((two == "<<" || two == ">>") && sql[i + 2] == '=')) {
                    emit(two + "=", i, i + 3);
                    i += 2;
                    continue;
                }
                if (two == "<=" || two == ">=" || two == "<>" || two == "!=" ||
                    two == "::" || two == "||" || two == "->" || two == "~*" ||
                    two == "!~" || two == "@@" || two == "&&" || two == "<<" ||
                    two == ">>" || two == "=>" || two == "#>" || two == "@>") {
                    // Three-char JSON operators bind tighter than their
                    // two-char prefixes: check ->> and #>> before emitting
                    // -> / #>.
                    if (i + 2 < sql.size() && ((two == "->" && sql[i + 2] == '>') ||
                                               (two == "#>" && sql[i + 2] == '>') ||
                                               (two == "!~" && sql[i + 2] == '*'))) {
                        emit(two + (sql[i + 2] == '*' ? '*' : '>'), i, i + 3);
                        i += 2;
                        continue;
                    }
                    emit(two, i, i + 2);
                    ++i;
                    continue;
                }
                if (two == "<@") {
                    emit(two, i, i + 2);
                    ++i;
                    continue;
                }
                if ((c == '-' && next == '-') || (c == '/' && next == '*')) {
                    // This branch is outside both literal and identifier
                    // quotes. Reuse nested-block and CR/LF trivia scanning.
                    const size_t after = skipLeadingSqlTrivia(sql, i);
                    if (after == std::string::npos && error) {
                        *error = "unterminated block comment";
                        return tokens;
                    }
                    i = after == std::string::npos ? sql.size() : after - 1;
                    continue;
                }
            }
            emit(std::string(1, c), i, i + 1);
            continue;
        }
        if (cur.empty()) curBegin = i;
        cur += c;
    }
    if (error && (inString || inIdentifier)) {
        *error = inIdentifier ? "unterminated quoted identifier" : "unterminated quoted string";
        return tokens;
    }
    if (!cur.empty()) {
        emit(cur, curBegin, sql.size());
    }
    // PostgreSQL POSITION(needle IN haystack): rewrite the IN token to a
    // comma when it appears inside a position()/strpos() argument list, so
    // the generic expression parser sees two arguments instead of an IN
    // comparison.
    {
        int depth = 0;
        int posFuncDepth = -1;  // paren depth of an open position( call
        std::string prev;
        for (size_t i = 0; i < tokens.size(); ++i) {
            const std::string& t = tokens[i];
            std::string tl = toLower(t);
            if (t == "(") {
                ++depth;
                if (tl == "(" && (prev == "position" || prev == "strpos") &&
                    posFuncDepth < 0) {
                    posFuncDepth = depth;
                }
            } else if (t == ")") {
                if (posFuncDepth > 0 && depth == posFuncDepth) posFuncDepth = -1;
                --depth;
            } else if (posFuncDepth > 0 && depth == posFuncDepth && tl == "in") {
                const_cast<std::string&>(tokens[i]) = ",";
            }
            if (t != "(") prev = tl;
            else prev.clear();
        }
    }
    // PostgreSQL OVERLAPS: rewrite the row-value form
    //   (s1, e1) OVERLAPS (s2, e2)
    // into a plain function call  overlaps(s1, e1, s2, e2)
    // so the generic expression parser never sees row constructors.
    {
        for (size_t i = 0; i < tokens.size(); ++i) {
            if (toLower(tokens[i]) != "overlaps") continue;
            if (i == 0 || tokens[i - 1] != ")") continue;
            size_t depth = 0;
            size_t ls = i;
            bool found = false;
            for (size_t j = i - 1; j-- > 0;) {
                if (tokens[j] == ")") ++depth;
                else if (tokens[j] == "(") {
                    if (depth == 0) { ls = j; found = true; break; }
                    --depth;
                }
            }
            if (!found) continue;
            size_t ge = i + 1;
            if (ge >= tokens.size() || tokens[ge] != "(") continue;
            depth = 0;
            size_t re = ge;
            for (size_t j = ge; j < tokens.size(); ++j) {
                if (tokens[j] == "(") ++depth;
                else if (tokens[j] == ")") { --depth; if (depth == 0) { re = j; break; } }
            }
            if (re == ge) continue;
            auto topLevelComma = [&](size_t a, size_t b) -> int {
                int d = 0, n = 0;
                for (size_t j = a; j <= b; ++j) {
                    if (tokens[j] == "(") ++d;
                    else if (tokens[j] == ")") --d;
                    else if (tokens[j] == "," && d == 0) ++n;
                }
                return n;
            };
            if (topLevelComma(ls + 1, i - 2) != 1) continue;
            if (topLevelComma(ge + 1, re - 1) != 1) continue;
            std::vector<std::string> out;
            std::vector<std::pair<size_t, size_t>> reordered;
            const auto append = [&](const std::string& value, size_t origin) {
                out.push_back(value); reordered.push_back(spans[origin]);
            };
            out.reserve(tokens.size());
            for (size_t j = 0; j < ls; ++j) append(tokens[j], j);
            append("overlaps", i); append("(", ls);
            for (size_t j = ls + 1; j <= i - 2; ++j) append(tokens[j], j);
            append(",", i);
            for (size_t j = ge + 1; j <= re - 1; ++j) append(tokens[j], j);
            append(")", re);
            for (size_t j = re + 1; j < tokens.size(); ++j) append(tokens[j], j);
            tokens = std::move(out); spans = std::move(reordered);
            break;
        }
    }

    if (bindingParse) bindingParse->tokens[tokens.data()] = {std::move(spans)};
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

bool SQLParser::isSetTransactionStatement(const std::string& sql) {
    const auto tokens = tokenize(sql);
    return tokens.size() >= 2 && toLower(tokens[0]) == "set" &&
           toLower(tokens[1]) == "transaction";
}

bool SQLParser::requiresQuerySnapshot(const std::string& sql) {
    const auto tokens = tokenize(sql);
    size_t first = 0;
    while (first < tokens.size() && tokens[first] == "(") ++first;
    if (first == tokens.size()) return false;
    const auto keyword = toLower(tokens[first]);
    return keyword == "select" || keyword == "values" || keyword == "with" ||
           keyword == "table" || keyword == "insert" || keyword == "update" ||
           keyword == "delete" || keyword == "merge";
}

static bool databaseIndependentExpression(const Expr* expression) {
    if (!expression || expression->preparedSubquery) return false;
    if (const auto* literal = dynamic_cast<const LiteralExpr*>(expression)) {
        // Legacy SQL children/array bounds/aggregate decorations can also
        // occupy LiteralExpr. Only a genuine primitive datum is a proof;
        // declared/unknown types can require catalog-dependent coercion.
        if (!literal->typeName.empty() || !SQLParser::lexicalError(literal->value).empty() ||
            SQLParser::tokenize(literal->value).size() != 1) return false;
        const auto value = SQLParser::toLower(literal->value);
        return isNumericToken(literal->value) || isStringLiteralToken(literal->value) ||
               isBitStringLiteralToken(literal->value) ||
               value == "null" || value == "true" || value == "false";
    }
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression)) {
        const auto op = SQLParser::toLower(unary->op);
        return (op == "+" || op == "-" || op == "not") &&
               databaseIndependentExpression(unary->operand.get());
    }
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression)) {
        // These primitive operators execute directly; casts, COLLATE and
        // operators without that contract remain owned. There is no routine
        // name/volatility whitelist and no evaluation during this proof.
        static const std::set<std::string> primitiveOperators = {
            "+", "-", "*", "/", "%", "||", "and", "or", "=", "!=", "<>",
            "<", ">", "<=", ">=", "in", "not in", "between", "not between"
        };
        return primitiveOperators.count(SQLParser::toLower(binary->op)) &&
               databaseIndependentExpression(binary->left.get()) &&
               databaseIndependentExpression(binary->right.get());
    }
    if (const auto* row = dynamic_cast<const RowExpr*>(expression)) {
        if (row->elements.empty()) return false;
        for (const auto& element : row->elements)
            if (!databaseIndependentExpression(element.get())) return false;
        return true;
    }
    // Column/parameter references, routines (including builtins), subqueries,
    // casts and all unhandled nodes retain the database transaction. CASE is
    // conservative too: legacy execution can lower it to synthetic calls.
    return false;
}

bool SQLParser::isDatabaseIndependentQuery(const Stmt& statement) {
    const auto* select = dynamic_cast<const SelectStmt*>(&statement);
    if (!select || select->command != SqlCommand::Select || select->fromClause ||
        !select->ctes.empty() || select->setOp != SetOp::None || select->setOpLhs || select->setOpRhs ||
        !select->valuesRows.empty() || !select->locking.empty() || select->withTies ||
        !select->groupBy.empty() || !select->groupByElems.empty() || select->having ||
        !select->windowDefs.empty() || select->selectList.empty()) return false;
    for (const auto& target : select->selectList)
        if (!databaseIndependentExpression(target.expr.get())) return false;
    if (select->whereClause && !databaseIndependentExpression(select->whereClause.get())) return false;
    for (const auto& key : select->orderBy)
        if (!key.usingOp.empty() || !databaseIndependentExpression(key.expr.get())) return false;
    for (const auto& target : select->distinctOn)
        if (!databaseIndependentExpression(target.get())) return false;
    return true;
}

SqlCommand SQLParser::classify(const std::string& sql) {
    const size_t offset = skipLeadingSqlTrivia(sql);
    if (offset == std::string::npos) return SqlCommand::Unknown;
    std::string lsql = toLower(trim(sql.substr(offset)));
    if (lsql.empty()) return SqlCommand::Unknown;

    // Remove trailing semicolon
    while (!lsql.empty() && lsql.back() == ';') lsql.pop_back();

    // Parenthesized query expressions are complete SQL statements too. Keep
    // the lightweight command classifier aligned with the parser and Simple
    // Query splitter; query analysis still validates the closing parens.
    size_t commandOffset = skipLeadingSqlTrivia(lsql);
    while (commandOffset != std::string::npos && commandOffset < lsql.size() &&
           lsql[commandOffset] == '(') {
        commandOffset = skipLeadingSqlTrivia(lsql, commandOffset + 1);
    }
    if (commandOffset == std::string::npos) lsql.clear();
    else lsql = trim(lsql.substr(commandOffset));

    // DQL
    if (lsql.substr(0, 6) == "select") return SqlCommand::Select;
    if (lsql.substr(0, 6) == "values") return SqlCommand::Values;
    // The primary command follows the complete WITH envelope, never a
    // SELECT/DML keyword inside an auxiliary query or a quoted identifier.
    if (lsql.compare(0, 4, "with") == 0) {
        const auto queryTokens = tokenize(sql.substr(offset));
        if (queryTokens.empty() || toLower(queryTokens.front()) != "with")
            return SqlCommand::Unknown;
        int depth = 0;
        for (size_t index = 1; index < queryTokens.size(); ++index) {
            if (queryTokens[index] == "(") { ++depth; continue; }
            if (queryTokens[index] == ")") { --depth; continue; }
            if (depth != 0) continue;
            const std::string word = toLower(queryTokens[index]);
            if (word == "select") return SqlCommand::Select;
            if (word == "insert") return SqlCommand::Insert;
            if (word == "update") return SqlCommand::Update;
            if (word == "delete") return SqlCommand::Delete;
            if (word == "merge") return SqlCommand::Merge;
        }
    }

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
        if (rest.compare(0, 11, "tablespace ") == 0) return SqlCommand::CreateTablespace;
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
        if (rest.compare(0, 13, "user mapping ") == 0) return SqlCommand::CreateUserMapping;
        if (rest.substr(0, 5) == "user ") return SqlCommand::CreateUser;
        if (rest.substr(0, 6) == "group ") return SqlCommand::CreateRole; // legacy
        if (rest.substr(0, 11) == "statistics ") return SqlCommand::CreateStatistics;
        if (rest.substr(0, 7) == "policy ") return SqlCommand::CreatePolicy;
        if (rest.substr(0, 10) == "extension ") return SqlCommand::CreateExtension;
        if (rest.compare(0, 12, "publication ") == 0) return SqlCommand::CreatePublication;
        if (rest.substr(0, 13) == "subscription ") return SqlCommand::CreateSubscription;
        if (rest.substr(0, 7) == "access ") return SqlCommand::CreateAccessMethod;
        if (rest.substr(0, 7) == "foreign") {
            if (rest.substr(8, 5) == "data ") return SqlCommand::CreateForeignDataWrapper;
            if (rest.substr(8, 6) == "table ") return SqlCommand::CreateForeignTable;
            if (rest.substr(8, 6) == "server") return SqlCommand::CreateServer; // 'foreign server'
        }
        if (rest.substr(0, 5) == "cast ") return SqlCommand::CreateCast;
        if (rest.substr(0, 10) == "collation ") return SqlCommand::CreateCollation;
        if (rest.substr(0, 11) == "conversion ") return SqlCommand::CreateConversion;
        if (rest.substr(0, 14) == "operator class") return SqlCommand::CreateOperatorClass;
        if (rest.substr(0, 15) == "operator family") return SqlCommand::CreateOperatorFamily;
        if (rest.substr(0, 9) == "operator ") return SqlCommand::CreateOperator;
        if (rest.compare(0, 10, "aggregate ") == 0) return SqlCommand::CreateAggregate;
        if (rest.compare(0, 10, "assertion ") == 0) return SqlCommand::CreateAssertion;
        if (rest.compare(0, 15, "fulltext index ") == 0) return SqlCommand::CreateFullTextIndex;
        if (rest.compare(0, 11, "hash index ") == 0) return SqlCommand::CreateHashIndex;
        if (rest.compare(0, 7, "server ") == 0) return SqlCommand::CreateServer;
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
        if (rest.compare(0, 11, "tablespace ") == 0) return SqlCommand::DropTablespace;
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
        if (rest.compare(0, 13, "user mapping ") == 0) return SqlCommand::DropUserMapping;
        if (rest.substr(0, 5) == "user ") return SqlCommand::DropUser;
        if (rest.substr(0, 6) == "group ") return SqlCommand::DropRole; // legacy
        if (rest.substr(0, 11) == "statistics ") return SqlCommand::DropStatistics;
        if (rest.substr(0, 7) == "policy ") return SqlCommand::DropPolicy;
        if (rest.substr(0, 10) == "extension ") return SqlCommand::DropExtension;
        if (rest.compare(0, 12, "publication ") == 0) return SqlCommand::DropPublication;
        if (rest.substr(0, 13) == "subscription ") return SqlCommand::DropSubscription;
        if (rest.substr(0, 7) == "access ") return SqlCommand::DropAccessMethod;
        if (rest.substr(0, 7) == "foreign") {
            if (rest.substr(8, 5) == "data ") return SqlCommand::DropForeignDataWrapper;
            if (rest.substr(8, 6) == "table ") return SqlCommand::DropForeignTable;
            if (rest.substr(8, 6) == "server") return SqlCommand::DropServer;
        }
        if (rest.substr(0, 5) == "cast ") return SqlCommand::DropCast;
        if (rest.substr(0, 10) == "collation ") return SqlCommand::DropCollation;
        if (rest.substr(0, 11) == "conversion ") return SqlCommand::DropConversion;
        if (rest.substr(0, 14) == "operator class") return SqlCommand::DropOperatorClass;
        if (rest.substr(0, 15) == "operator family") return SqlCommand::DropOperatorFamily;
        if (rest.substr(0, 9) == "operator ") return SqlCommand::DropOperator;
        if (rest.compare(0, 10, "aggregate ") == 0) return SqlCommand::DropAggregate;
        if (rest.compare(0, 10, "assertion ") == 0) return SqlCommand::DropAssertion;
        if (rest.compare(0, 15, "fulltext index ") == 0) return SqlCommand::DropFullTextIndex;
        if (rest.compare(0, 7, "server ") == 0) return SqlCommand::DropServer;
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
        if (rest.compare(0, 11, "tablespace ") == 0) return SqlCommand::AlterTablespace;
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
        if (rest.compare(0, 13, "user mapping ") == 0) return SqlCommand::AlterUserMapping;
        if (rest.substr(0, 5) == "user ") return SqlCommand::AlterUser;
        if (rest.substr(0, 11) == "statistics ") return SqlCommand::AlterStatistics;
        if (rest.substr(0, 7) == "policy ") return SqlCommand::AlterPolicy;
        if (rest.substr(0, 10) == "extension ") return SqlCommand::AlterExtension;
        if (rest.compare(0, 12, "publication ") == 0) return SqlCommand::AlterPublication;
        if (rest.substr(0, 13) == "subscription ") return SqlCommand::AlterSubscription;
        if (rest.substr(0, 8) == "default ") return SqlCommand::AlterDefaultPrivileges;
        if (rest.substr(0, 7) == "system ") return SqlCommand::AlterSystem;
        if (rest.substr(0, 7) == "foreign") {
            if (rest.substr(8, 5) == "data ") return SqlCommand::AlterForeignDataWrapper;
            if (rest.substr(8, 6) == "table ") return SqlCommand::AlterForeignTable;
            if (rest.substr(8, 6) == "server") return SqlCommand::AlterServer;
        }
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
        if (rest.substr(0, 14) == "operator class") return SqlCommand::AlterOperatorClass;
        if (rest.substr(0, 15) == "operator family") return SqlCommand::AlterOperatorFamily;
        if (rest.substr(0, 9) == "operator ") return SqlCommand::AlterOperator;
        if (rest.compare(0, 10, "aggregate ") == 0) return SqlCommand::AlterAggregate;
        if (rest.compare(0, 7, "server ") == 0) return SqlCommand::AlterServer;
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
    if (lsql.substr(0, 8) == "rollback") {
        const auto tokens = tokenize(sql);
        size_t pos = 1;
        if (pos < tokens.size() &&
            (toLower(tokens[pos]) == "work" || toLower(tokens[pos]) == "transaction"))
            ++pos;
        if (pos < tokens.size() && toLower(tokens[pos]) == "to")
            return SqlCommand::RollbackToSavepoint;
    }
    if (lsql.substr(0, 6) == "commit") return SqlCommand::Commit;
    if (lsql.substr(0, 8) == "rollback") return SqlCommand::Rollback;
    if (lsql.substr(0, 5) == "abort") return SqlCommand::Abort;
    if (lsql.substr(0, 3) == "end") return SqlCommand::End;
    if (lsql.substr(0, 9) == "savepoint") return SqlCommand::Savepoint;
    if (lsql.substr(0, 7) == "release") return SqlCommand::ReleaseSavepoint;
    if (lsql.compare(0, 19, "prepare transaction") == 0) return SqlCommand::PrepareTransaction;

    // DCL
    if (lsql.substr(0, 5) == "grant") return SqlCommand::Grant;
    if (lsql.substr(0, 6) == "revoke") return SqlCommand::Revoke;

    // Session / GUC
    if (lsql.substr(0, 3) == "set") {
        size_t pos = 3;
        while (pos < lsql.size() && std::isspace(static_cast<unsigned char>(lsql[pos]))) ++pos;
        std::string rest = lsql.substr(pos);
        if (rest.substr(0, 5) == "role ") return SqlCommand::SetRole;
        if (rest.compare(0, 21, "session authorization") == 0) return SqlCommand::SetSessionAuthorization;
        if (rest.substr(0, 12) == "constraints ") return SqlCommand::SetConstraints;
        if (rest.substr(0, 11) == "transaction") return SqlCommand::SetTransaction;
        if (rest.substr(0, 5) == "time ") return SqlCommand::Set; // set time zone
        if (rest.substr(0, 8) == "timezone") return SqlCommand::Set; // set timezone
        return SqlCommand::Set;
    }
    if (lsql.substr(0, 4) == "show") return SqlCommand::Show;
    if (lsql.substr(0, 5) == "reset") return SqlCommand::Reset;
    // "use" must be followed by a word boundary: "user_xyz" or "utils"
    // are identifiers, not this command.
    if (lsql.substr(0, 3) == "use" &&
        (lsql.size() == 3 || isspace(static_cast<unsigned char>(lsql[3])))) {
        return SqlCommand::UseDatabase;
    }
    if (lsql.substr(0, 7) == "discard") return SqlCommand::Discard;

    // Utility
    if (lsql.substr(0, 7) == "explain") return SqlCommand::Explain;
    if (lsql.substr(0, 7) == "analyze") return SqlCommand::Analyze;
    if (lsql.substr(0, 6) == "vacuum") return SqlCommand::Vacuum;
    if (lsql.substr(0, 10) == "checkpoint") return SqlCommand::Checkpoint;
    if (lsql.substr(0, 7) == "reindex") return SqlCommand::Reindex;
    if (lsql.compare(0, 7, "refresh") == 0 &&
        lsql.size() > 7 &&
        std::isspace(static_cast<unsigned char>(lsql[7]))) {
        const auto tokens = tokenize(lsql);
        if (tokens.size() >= 3 && tokens[0] == "refresh" &&
            tokens[1] == "materialized" && tokens[2] == "view") {
            return SqlCommand::RefreshMaterializedView;
        }
    }
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
    if (lsql.compare(0, 10, "deallocate") == 0 &&
        (lsql.size() == 10 || std::isspace(static_cast<unsigned char>(lsql[10]))))
        return SqlCommand::Deallocate;

    // Import
    if (lsql.substr(0, 21) == "import foreign schema") return SqlCommand::ImportForeignSchema;

    // Non-PG syntax (Phase 11 清理)
    if (lsql.substr(0, 7) == "replace") return SqlCommand::ReplaceInto;
    if (lsql.compare(0, 9, "load data") == 0) return SqlCommand::LoadDataInfile;
    if (lsql.substr(0, 4) == "desc") return SqlCommand::Desc;
    if (lsql.compare(0, 10, "view table") == 0) return SqlCommand::ViewTable;
    if (lsql.compare(0, 13, "view database") == 0) return SqlCommand::ViewDatabase;

    return SqlCommand::Unknown;
}

// ============================================================================
// parse：完整解析入口
// ============================================================================

ParseResult SQLParser::parse(const std::string& inputSql) {
    std::string declarationError;
    auto* previousError=declarationSyntaxError;
    declarationSyntaxError=&declarationError;
    struct RestoreError {std::string* previous;~RestoreError(){declarationSyntaxError=previous;}} restoreError{previousError};
    ParseResult parsed;
    try { parsed=[&]() -> ParseResult {
    ParseResult result;
    result.originalSql = inputSql;
    const size_t offset = skipLeadingSqlTrivia(inputSql);
    if (offset == std::string::npos) {
        result.error = "unterminated block comment";
        return result;
    }
    const std::string sql = inputSql.substr(offset);
    BindingSourceScope sourceScope(offset);
    result.error = lexicalError(sql);
    if (!result.error.empty()) return result;
    if (const auto duplicate = duplicateCteName(sql)) {
        result.error = "WITH query name \"" + *duplicate +
            "\" specified more than once (SQLSTATE 42712)";
        return result;
    }

    std::string lsql = toLower(trim(sql));
    if (lsql.empty()) {
        result.error = "empty SQL statement";
        return result;
    }

    SqlCommand cmd = classify(sql);

    // parseSelect owns the WITH grammar head and delegates a genuine DML
    // primary statement to its own parser after retaining exact byte spans.
    const auto leadingTokens = tokenize(sql);
    if (!leadingTokens.empty() && toLower(leadingTokens.front()) == "with")
        return parseSelect(sql);

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
        case SqlCommand::CreateFullTextIndex: case SqlCommand::CreateHashIndex:
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
        case SqlCommand::DropFullTextIndex:
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
        case SqlCommand::RefreshMaterializedView:
            return parseRefreshMaterializedView(sql);
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
    }(); } catch(const DeclarationSyntaxAbort&) {}
    if(!declarationError.empty()) {
        parsed.success=false;
        parsed.stmt.reset();
        parsed.originalSql=inputSql;
        parsed.error=declarationError;
        parsed.sqlState="42601";
        if(previousError && previousError->empty())*previousError=declarationError;
    }
    return parsed;
}

ParseResult SQLParser::parseForBinding(const std::string& sql) {
    BindingParseContext context{sql};
    auto* saved = bindingParse;
    bindingParse = &context;
    struct Restore { BindingParseContext* saved; ~Restore() { bindingParse = saved; } } restore{saved};
    auto parsed=parse(sql);
    if(context.fetchGrammarError) {
        parsed.success=false;parsed.error="WITH TIES cannot be specified without ORDER BY clause";
    }
    return parsed;
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
static ExprPtr parseJsonOpExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseConcatExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseAddSubExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseMulDivModExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parsePowerExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseAtTimeZoneExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseUnaryExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parseCastExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parsePostfixExpr(const std::vector<std::string>& tokens, size_t& pos);
static ExprPtr parsePrimaryExpr(const std::vector<std::string>& tokens, size_t& pos);

// Entry point
static ExprPtr parseExpr(const std::vector<std::string>& tokens, size_t& pos) {
    const size_t begin=pos;
    auto result=parseOrExpr(tokens,pos);
    // Composite roots are not returned through parsePrimaryExpr's source
    // wrapper. Give them their real token interval as well; preserve an
    // already tagged inner/child site rather than widening its identity.
    if(result && result->sourceBegin==std::string::npos)
        markSource(result.get(),tokens,begin,pos);
    return result;
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
            "=", "<>", "!=", "<", ">", "<=", ">=", "~", "~*", "!~", "!~*"
        };
        if (cmpOps.count(op) == 0) break;
        ++pos;
        if (pos + 1 < tokens.size() && tokens[pos + 1] == "(") {
            const auto word = SQLParser::toLower(tokens[pos]);
            if (word == "any" || word == "some" || word == "all") {
                ++pos;
                auto quantified = std::make_unique<QuantifiedComparisonExpr>();
                quantified->op = op;
                quantified->quantifier = word == "all" ? QuantifiedComparisonExpr::Quantifier::All
                    : QuantifiedComparisonExpr::Quantifier::Any;
                quantified->left = std::move(left);
                quantified->right = parseRangeExpr(tokens, pos);
                if (!quantified->left || !quantified->right) return nullptr;
                quantified->sourceBegin = quantified->left->sourceBegin;
                quantified->sourceEnd = quantified->right->sourceEnd;
                left = std::move(quantified);
                continue;
            }
        }
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = op;
        bin->left = std::move(left);
        bin->right = parseRangeExpr(tokens, pos);
        left = std::move(bin);
    }
    // PostgreSQL: IS [NOT] NULL binds LOOSER than comparison operators
    // ("a = b IS NULL" is "(a = b) IS NULL", not "a = (b IS NULL)").
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

// BETWEEN, IN, LIKE, ILIKE, SIMILAR TO  (each optionally NOT-prefixed:
// "x NOT IN (...)", "x NOT LIKE ...", "x NOT BETWEEN ...")
static ExprPtr parseRangeExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parseConcatExpr(tokens, pos);
    if (pos >= tokens.size()) return left;

    std::string w = SQLParser::toLower(tokens[pos]);
    bool negated = false;
    if (w == "not" && pos + 1 < tokens.size()) {
        std::string next = SQLParser::toLower(tokens[pos + 1]);
        if (next == "in" || next == "between" || next == "like" ||
            next == "ilike" || next == "similar") {
            negated = true;
            ++pos;
            w = next;
        } else {
            return left;   // prefix-NOT belongs to the caller's grammar
        }
    }

    if (w == "between") {
        ++pos;
        auto lower = parseConcatExpr(tokens, pos);
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "and") ++pos;
        auto upper = parseConcatExpr(tokens, pos);
        auto betweenExpr = std::make_unique<FunctionCallExpr>();
        betweenExpr->funcName = negated ? "NOT BETWEEN" : "BETWEEN";
        betweenExpr->args.push_back(std::move(left));
        betweenExpr->args.push_back(std::move(lower));
        betweenExpr->args.push_back(std::move(upper));
        return betweenExpr;
    }

    if (w == "in") {
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = negated ? "NOT IN" : "IN";
        bin->left = std::move(left);
        if (pos < tokens.size() && tokens[pos] == "(") {
            ++pos;
            if (pos < tokens.size() &&
                SQLParser::toLower(tokens[pos]) == "select") {
                // Subquery IN predicates are planned separately; retain their
                // SQL text for that path rather than treating SELECT as a
                // scalar list element.
                auto list = std::make_unique<LiteralExpr>();
                std::string value;
                int depth = 1;
                while (pos < tokens.size() && depth > 0) {
                    if (tokens[pos] == "(") ++depth;
                    else if (tokens[pos] == ")") --depth;
                    if (depth > 0) {
                        if (!value.empty()) value += " ";
                        value += tokens[pos++];
                    }
                }
                if (pos < tokens.size() && tokens[pos] == ")") ++pos;
                list->value = value;
                bin->right = std::move(list);
            } else {
                auto list = std::make_unique<RowExpr>();
                while (pos < tokens.size() && tokens[pos] != ")") {
                    const size_t elementStart = pos;
                    auto element = parseExpr(tokens, pos);
                    if (!element || pos == elementStart) break;
                    list->elements.push_back(std::move(element));
                    if (pos < tokens.size() && tokens[pos] == ",") {
                        ++pos;
                        continue;
                    }
                    break;
                }
                if (pos < tokens.size() && tokens[pos] == ")") ++pos;
                bin->right = std::move(list);
            }
        } else {
            bin->right = parseConcatExpr(tokens, pos);
        }
        return bin;
    }

    if (w == "like" || w == "ilike") {
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        if (negated) bin->op = (w == "like") ? "NOT LIKE" : "NOT ILIKE";
        else bin->op = (w == "like") ? "LIKE" : "ILIKE";
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
        bin->op = negated ? "NOT SIMILAR TO" : "SIMILAR TO";
        bin->left = std::move(left);
        bin->right = parseConcatExpr(tokens, pos);
        // ESCAPE clause: wrap into a FunctionCallExpr like the LIKE path.
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "escape") {
            ++pos;
            auto esc = parseConcatExpr(tokens, pos);
            auto wrap = std::make_unique<FunctionCallExpr>();
            wrap->funcName = bin->op + " ESCAPE";
            wrap->args.push_back(std::move(bin->left));
            wrap->args.push_back(std::move(bin->right));
            wrap->args.push_back(std::move(esc));
            return wrap;
        }
        return bin;
    }

    return left;
}

// PostgreSQL's generic-operator precedence tier.  Besides concatenation this
// carries the bit-string boolean and shift operators.  Operands parse at the
// JSON-operator level so ->/->>/#>/#>>/@>/<@ bind tighter.
static ExprPtr parseConcatExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parseJsonOpExpr(tokens, pos);
    static const std::set<std::string> genericOperators = {
        "||", "&", "|", "#", "<<", "<<=", ">>", ">>="
    };
    while (pos < tokens.size() && genericOperators.count(tokens[pos]) != 0) {
        const std::string op = tokens[pos];
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = op;
        bin->left = std::move(left);
        bin->right = parseJsonOpExpr(tokens, pos);
        left = std::move(bin);
    }
    return left;
}

// JSON access operators (left-associative, bind tighter than || per PG):
//   json  ->  int|text   -> json   (json_extract_path, keeps JSON form)
//   json  ->> int|text   -> text   (json_extract_path_text, unquoted)
//   json  #>  path       -> json   (path as text like 'a,b,0')
//   json  #>> path       -> text
//   json/jsonb @> json/jsonb -> boolean (contains)
//   json/jsonb <@ json/jsonb -> boolean (contained, args swapped @>)
//
// Precedence: tighter than ||, looser than + (PostgreSQL's own ordering).
// Operands parse at the additive level.
static ExprPtr parseJsonOpExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parseAddSubExpr(tokens, pos);
    while (pos < tokens.size()) {
        const std::string& op = tokens[pos];
        if (op != "->" && op != "->>" && op != "#>" && op != "#>>" &&
            op != "&&" &&
            op != "@>" && op != "<@" && op != "@@") {
            break;
        }
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = op;
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
    auto left = parseAtTimeZoneExpr(tokens, pos);
    while (pos < tokens.size() && tokens[pos] == "^") {
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "^";
        bin->left = std::move(left);
        bin->right = parseAtTimeZoneExpr(tokens, pos);
        left = std::move(bin);
    }
    return left;
}

// Unary +, -, ~.  NOT has its own lower-precedence grammar level.
static ExprPtr parseUnaryExpr(const std::vector<std::string>& tokens, size_t& pos) {
    if (pos < tokens.size() &&
        (tokens[pos] == "+" || tokens[pos] == "-" || tokens[pos] == "~")) {
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
// True when tokens[pos..pos+2] are AT TIME ZONE (case-insensitive).
static bool isAtTimeZone(const std::vector<std::string>& tokens, size_t pos) {
    return pos + 2 < tokens.size()
        && SQLParser::toLower(tokens[pos]) == "at"
        && SQLParser::toLower(tokens[pos + 1]) == "time"
        && SQLParser::toLower(tokens[pos + 2]) == "zone";
}

// AT has lower precedence than casts/unary expressions and higher precedence
// than exponentiation. Keep both operands as AST values; embedding the zone
// token in a unary operator loses bindings, NULLs and compound expressions.
static ExprPtr parseAtTimeZoneExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parseUnaryExpr(tokens, pos);
    while (pos < tokens.size() && isAtTimeZone(tokens, pos)) {
        pos += 3;
        auto zone = parseUnaryExpr(tokens, pos);
        if (!left || !zone) return nullptr;
        auto timezone = std::make_unique<FunctionCallExpr>();
        timezone->funcName = "timezone";
        timezone->args.push_back(std::move(zone));
        timezone->args.push_back(std::move(left));
        left = std::move(timezone);
    }
    return left;
}

static ExprPtr parseCastExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parsePostfixExpr(tokens, pos);
    while (pos < tokens.size() && tokens[pos] == "::") {
        ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "::";
        bin->left = std::move(left);
        // CAST and :: consume one genuine declared-type envelope, not an
        // arbitrary run of words that can absorb a target alias/operator.
        ColumnDef declaration;
        try {declaration=consumeDeclaredType(tokens,pos);}
        catch(const DbError& error) {
            if(error.sqlState()=="42601"){retainDeclarationSyntaxError(error);return nullptr;}
            throw;
        }
        const std::string typeName=renderDeclaredType(declaration);
        auto right = std::make_unique<LiteralExpr>();
        right->value = typeName;
        bin->right = std::move(right);
        left = std::move(bin);
    }
    while (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "collate") {
        ++pos;
        if (pos >= tokens.size() || tokens[pos] == ")" || tokens[pos] == ",")
            return nullptr;
        auto collated = std::make_unique<UnaryOpExpr>();
        collated->op = "COLLATE " + tokens[pos++];
        collated->operand = std::move(left);
        left = std::move(collated);
    }
    return left;
}

// Postfix: array subscript [ ], IS NULL/NOT NULL (as postfix)
static ExprPtr parsePostfixExpr(const std::vector<std::string>& tokens, size_t& pos) {
    auto left = parsePrimaryExpr(tokens, pos);
    if (!left) return {};

    // Array subscript: expr[expr] (index) or expr[lower:upper] (slice).
    // Index  -> BinaryOpExpr op "[]"  right = index expression
    // Slice  -> BinaryOpExpr op "[:]" right = LiteralExpr "lower[:upper]"
    //           (empty side = open bound, e.g. [2:], [:3])
    while (pos < tokens.size() && tokens[pos] == "[") {
        ++pos;
        // Detect slice form: scan for a top-level ':' before ']'.
        {
            size_t scan = pos;
            int depth = 0;
            bool isSlice = false;
            while (scan < tokens.size()) {
                const std::string& tk = tokens[scan];
                if (tk == "(") { ++depth; ++scan; continue; }
                if (tk == ")") { if (depth > 0) --depth; ++scan; continue; }
                if (depth == 0 && tk == "]") break;
                if (depth == 0 && tk == ":") { isSlice = true; break; }
                ++scan;
            }
            if (isSlice) {
                // Parse lower (may be empty), expect ':', parse upper (may be
                // empty), expect ']'.
                std::string lower, upper;
                if (pos < tokens.size() && tokens[pos] != ":" && tokens[pos] != "]") {
                    auto lo = parseExpr(tokens, pos);
                    if (lo) lower = lo->toString();
                }
                if (pos < tokens.size() && tokens[pos] == ":") {
                    ++pos;
                    if (pos < tokens.size() && tokens[pos] != "]") {
                        auto hi = parseExpr(tokens, pos);
                        if (hi) upper = hi->toString();
                    }
                }
                if (pos < tokens.size() && tokens[pos] == "]") ++pos;
                auto bin = std::make_unique<BinaryOpExpr>();
                bin->op = "[:]";
                bin->left = std::move(left);
                auto bound = std::make_unique<LiteralExpr>();
                bound->value = lower + ":" + upper; // "l:u", ":u", "l:", ":"
                bin->right = std::move(bound);
                left = std::move(bin);
                continue;
            }
        }
        auto idx = parseExpr(tokens, pos);
        if (pos < tokens.size() && tokens[pos] == "]") ++pos;
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "[]";
        bin->left = std::move(left);
        bin->right = std::move(idx);
        left = std::move(bin);
    }

    // Postfix IS [NOT] NULL is NOT consumed here: it binds looser than
    // comparisons in PostgreSQL ("a = b IS NULL" is "(a = b) IS NULL"),
    // so it is handled at parseComparisonExpr and above. Keeping it here
    // would grab the right operand of a comparison first.

    return left;
}

// The bracket shorthand is valid only inside an ARRAY constructor. Keep it
// recursive here rather than accepting a bare '[' as a general expression.
static ExprPtr parseArrayConstructorContents(const std::vector<std::string>& tokens,
                                              size_t& pos) {
    if (pos >= tokens.size() || tokens[pos] != "[") return {};
    ++pos;
    auto array = std::make_unique<ArrayExpr>();
    if (pos < tokens.size() && tokens[pos] == "]") {
        ++pos;
        return array;
    }
    while (pos < tokens.size()) {
        auto element = tokens[pos] == "["
            ? parseArrayConstructorContents(tokens, pos) : parseExpr(tokens, pos);
        if (!element) return {};
        array->elements.push_back(std::move(element));
        if (pos < tokens.size() && tokens[pos] == "]") {
            ++pos;
            return array;
        }
        if (pos >= tokens.size() || tokens[pos] != ",") return {};
        ++pos;
        if (pos >= tokens.size() || tokens[pos] == "]") return {};
    }
    return {};
}

// Primary: literals, column refs, function calls, parenthesized exprs, subqueries, CASE
static ExprPtr parsePrimaryExprImpl(const std::vector<std::string>& tokens, size_t& pos) {
    if (pos >= tokens.size()) return nullptr;
    if (tokens[pos] == "[" || tokens[pos] == "]") return nullptr;

    // CAST(expr AS type [mods]): prefix form of the :: cast operator.
    if (SQLParser::toLower(tokens[pos]) == "cast" && pos + 1 < tokens.size()
        && tokens[pos + 1] == "(") {
        size_t save = pos;
        pos += 2;
        auto operand = parseExpr(tokens, pos);
        if (operand && pos < tokens.size()
            && SQLParser::toLower(tokens[pos]) == "as") {
            ++pos;
            ColumnDef declaration;
            try {declaration=consumeDeclaredType(tokens,pos);}
            catch(const DbError& error) {
                if(error.sqlState()=="42601"){retainDeclarationSyntaxError(error);return nullptr;}
                throw;
            }
            if (pos < tokens.size() && tokens[pos] == ")") {
                ++pos;
                auto cast = std::make_unique<CastExpr>();
                cast->operand = std::move(operand);
                cast->typeName = declaration.typeName+(declaration.isArray?"[]":"");
                cast->typeMods = declaration.typeMods;
                return cast;
            }
        }
        pos = save; // fall through to generic handling on malformed input
    }
    // ARRAY's expression grammar owns '[' and its value children before
    // any speculative declared-type suffix/constant parsing.
    if (SQLParser::toLower(tokens[pos]) == "array" && pos + 1 < tokens.size()
        && tokens[pos + 1] == "[") {
        ++pos;
        return parseArrayConstructorContents(tokens, pos);
    }
    // A declared type followed by a real string constant is a type-input
    // grammar role, including named/qualified/quoted and modified types.
    // A function call without that following constant retains its callee.
    const bool eligibleTypePrefix=declaredTypePrefixEligible(tokens[pos]);
    if(!eligibleTypePrefix && SQLParser::toLower(tokens[pos])!="case" &&
       pos+1<tokens.size() && tokens[pos+1].size()>=2 &&
       tokens[pos+1].front()=='\'' && tokens[pos+1].back()=='\'')
        retainDeclarationSyntaxError(DbError("42601","grammar keyword cannot introduce a type constant"));
    if (eligibleTypePrefix) {
        size_t typeEnd=pos;
        try {
            auto declaration=consumeDeclaredType(tokens,typeEnd);
            if(typeEnd<tokens.size() && tokens[typeEnd].size()>=2 &&
               tokens[typeEnd].front()=='\'' && tokens[typeEnd].back()=='\'') {
                size_t afterConstant = typeEnd + 1;
                // INTERVAL string constants own their field qualifiers after
                // the string. A declaration's prefix field grammar is only
                // valid for a type target, not INTERVAL DAY '2'. Quoted and
                // qualified physical names retain generic constant grammar.
                if (SQLParser::toLower(tokens[pos]) == "interval" && !declaration.isArray) {
                    if (!declaration.typeMods.empty() &&
                        declaration.typeMods.front() != std::to_string(interval_type_detail::fullRange))
                        throw DbError("42601", "interval constant fields must follow the string");
                    if (afterConstant < tokens.size() &&
                        interval_type_detail::fieldMask(SQLParser::toLower(tokens[afterConstant]))) {
                        if (!declaration.typeMods.empty())
                            throw DbError("42601", "interval constant precision cannot precede field qualifiers");
                        std::vector<std::string> suffix{tokens[pos]};
                        suffix.insert(suffix.end(), tokens.begin() + afterConstant, tokens.end());
                        size_t suffixEnd = 0;
                        const auto qualified = consumeDeclaredType(suffix, suffixEnd);
                        declaration.typeMods = qualified.typeMods;
                        afterConstant += suffixEnd - 1;
                    }
                }
                auto literal=std::make_unique<LiteralExpr>();
                literal->value=tokens[typeEnd];
                literal->typeName=renderDeclaredType(declaration);
                pos=afterConstant;
                return literal;
            }
        } catch(const DbError& error) {
            if(error.sqlState()!="42601")throw;
            int parentheses=0,brackets=0;
            for(size_t token=pos;token<typeEnd;++token) {
                if(tokens[token]=="(")++parentheses;
                else if(tokens[token]==")")--parentheses;
                else if(tokens[token]=="[")++brackets;
                else if(tokens[token]=="]")--brackets;
            }
            // A following string proves this is type input, not a function
            // call. Its malformed declaration must not fall through to a
            // scalar callee or a successful SELECT with a null AST item.
            if(parentheses==0 && brackets==0 && typeEnd>pos && typeEnd<tokens.size() && tokens[typeEnd].size()>=2 &&
                tokens[typeEnd].front()=='\'' && tokens[typeEnd].back()=='\'') {
                retainDeclarationSyntaxError(error);return nullptr;
            }
            // This may instead be ordinary CASE/ARRAY/operator/function
            // syntax. Its actual expression owner supplies any diagnosis.
        }
    }

    // CASE expression
    if (SQLParser::toLower(tokens[pos]) == "case") {
        ++pos;
        auto caseExpr = std::make_unique<CaseExpr>();
        // Simple CASE: CASE expr WHEN v1 THEN r1 ... END
        // Searched CASE: CASE WHEN c1 THEN r1 ... END
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) != "when"
            && SQLParser::toLower(tokens[pos]) != "end") {
            caseExpr->switchExpr = parseExpr(tokens, pos);
        }

        while (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "when") {
            ++pos;
            auto whenExpr = parseExpr(tokens, pos);
            if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "then") ++pos;
            auto thenExpr = parseExpr(tokens, pos);
            if (whenExpr && thenExpr) {
                caseExpr->whenClauses.emplace_back(std::move(whenExpr), std::move(thenExpr));
            }
        }
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "else") {
            ++pos;
            caseExpr->elseExpr = parseExpr(tokens, pos);
        }
        if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "end") ++pos;
        return caseExpr;
    }

    // EXISTS (subquery)
    if (SQLParser::toLower(tokens[pos]) == "exists") {
        ++pos;
        if (pos < tokens.size() && tokens[pos] == "(") {
            const size_t queryBegin = pos;
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
            if(!subqueryFetchGrammarValid(tokens,queryBegin+1,pos-1))return nullptr;
            auto func = std::make_unique<FunctionCallExpr>();
            func->funcName = "EXISTS";
            auto lit = std::make_unique<LiteralExpr>();
            lit->value = "(" + subq + ")";
            markSource(lit.get(), tokens, queryBegin, pos);
            func->args.push_back(std::move(lit));
            return func;
        }
        auto func = std::make_unique<FunctionCallExpr>();
        func->funcName = "EXISTS";
        return func;
    }

    // Parenthesized expression or subquery
    if (tokens[pos] == "(") {
        const size_t queryBegin=pos;
        ++pos;
        // Check for subquery
        if (pos < tokens.size() &&
            (SQLParser::toLower(tokens[pos]) == "select" ||
             SQLParser::toLower(tokens[pos]) == "with")) {
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
            if(!subqueryFetchGrammarValid(tokens,queryBegin+1,pos-1))return nullptr;
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
    if (first.size() > 1 && first.front() == '$' &&
        std::all_of(first.begin() + 1, first.end(), [](unsigned char c) { return std::isdigit(c); })) {
        size_t number = 0;
        if (!parseNonNegativeInteger(first.substr(1), number) || number == 0) return nullptr;
        auto parameter = std::make_unique<ParameterExpr>();
        parameter->slot = number - 1;
        parameter->origin = ParameterOrigin::StatementInput;
        return parameter;
    }

    // Literals: quoted strings, numbers, and boolean/null constants.
    if (isStringLiteralToken(first) || isBitStringLiteralToken(first) ||
        isNumericToken(first)) {
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
    // SQL value-function keywords are grammar, unlike their quoted spelling.
    // Model them structurally during preparation; no clock/session callback
    // is invoked to discover the expression's namespace.
    static const std::set<std::string> valueFunctions = {
        "current_date", "current_time", "current_timestamp", "localtime", "localtimestamp",
        "current_catalog", "current_schema", "current_user", "session_user", "current_role"
    };
    if (bindingParse && valueFunctions.count(firstLower) &&
        (pos == tokens.size() || tokens[pos] != "(")) {
        auto value = std::make_unique<FunctionCallExpr>();
        value->schema = "pg_catalog"; value->funcName = firstLower; return value;
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

        // PG keyword-arg syntax: normalize inside overlay/trim/extract calls.
        std::string fnLow = SQLParser::toLower(func->funcName);
        if (fnLow == "overlay" || fnLow == "trim" || fnLow == "extract") {
            std::vector<std::string>& tok2 = const_cast<std::vector<std::string>&>(tokens);
            // tok2 aliases the caller-owned token buffer; keyword rewrite below.
            int depth = 1; size_t e2 = pos;
            while (e2 < tokens.size() && depth > 0) {
                if (tok2[e2] == "(") ++depth;
                else if (tok2[e2] == ")") --depth;
                ++e2;
            }
            for (size_t k2 = pos; k2 < e2; ) {
                std::string lk2 = SQLParser::toLower(tok2[k2]);
                if (fnLow == "overlay") {
                    if (lk2 == "placing" || lk2 == "from" || lk2 == "for") tok2[k2] = ",";
                } else if (fnLow == "trim") {
                    if (lk2 == "both" || lk2 == "leading" || lk2 == "trailing") tok2[k2] = "'" + lk2 + "'";
                    else if (lk2 == "from") tok2[k2] = ",";
                } else if (fnLow == "extract") {
                    if (lk2 == "from") tok2[k2] = ",";
                }
                ++k2;
            }
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
            // PostgreSQL POSITION(needle IN haystack): the IN keyword acts
            // as the argument separator inside position()/strpos().
            if (pos + 1 < tokens.size() && tokens[pos] == "," ) {
                ++pos;
                continue;
            }
            if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "in" &&
                (SQLParser::toLower(func->funcName) == "position" ||
                 SQLParser::toLower(func->funcName) == "strpos")) {
                ++pos;
                auto arg2 = parseExpr(tokens, pos);
                parseArg(std::move(arg2));
            }
            // SUBSTRING(string FROM start [FOR len]) and TRIM(... FROM s):
            // the FROM keyword separates the start argument and FOR
            // introduces the length, PostgreSQL special call syntaxes.
            if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "from" &&
                (SQLParser::toLower(func->funcName) == "substring" ||
                 SQLParser::toLower(func->funcName) == "substr" ||
                 SQLParser::toLower(func->funcName) == "trim" ||
                 SQLParser::toLower(func->funcName) == "ltrim" ||
                 SQLParser::toLower(func->funcName) == "rtrim" ||
                 SQLParser::toLower(func->funcName) == "btrim" ||
                 SQLParser::toLower(func->funcName) == "overlay" ||
                 SQLParser::toLower(func->funcName) == "position" ||
                 SQLParser::toLower(func->funcName) == "strpos")) {
                ++pos;
                auto arg2 = parseExpr(tokens, pos);
                parseArg(std::move(arg2));
                if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "for" &&
                    (SQLParser::toLower(func->funcName) == "substring" ||
                     SQLParser::toLower(func->funcName) == "substr" ||
                     SQLParser::toLower(func->funcName) == "overlay")) {
                    ++pos;
                    auto arg3 = parseExpr(tokens, pos);
                    parseArg(std::move(arg3));
                }
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
    colRef->schema = schemaName.empty()
        ? "" : parseRoutineIdentifier(schemaName);
    colRef->table = tableName.empty()
        ? "" : parseRoutineIdentifier(tableName);
    colRef->column = parseRoutineIdentifier(colName);
    return colRef;
}

static ExprPtr parsePrimaryExpr(const std::vector<std::string>& tokens, size_t& pos) {
    const size_t begin = pos;
    auto result = parsePrimaryExprImpl(tokens, pos);
    // Do not widen an already tagged inner expression across parentheses.
    if (result && result->sourceBegin == std::string::npos)
        markSource(result.get(), tokens, begin, pos);
    return result;
}

// Backward-compatible wrapper: delegates to full precedence parser
static ExprPtr parseSimpleExpr(const std::vector<std::string>& tokens, size_t& pos) {
    return parseExpr(tokens, pos);
}

// 解析单个 SELECT 项（expr [AS alias]）
static SelectItem parseSelectItem(const std::vector<std::string>& tokens, size_t& pos) {
    SelectItem item;
    item.expr = parseSimpleExpr(tokens, pos);
    if (bindingParse && pos > 0) {
        const auto found = bindingParse->tokens.find(tokens.data());
        if (found != bindingParse->tokens.end() && pos <= found->second.spans.size())
            item.sourceExpressionEnd = found->second.spans[pos - 1].second;
    }
    if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "as") {
        ++pos;
        if (pos < tokens.size()) item.alias = tokens[pos++];
    } else if (pos < tokens.size() && !SQLParser::isKeyword(tokens[pos])
               && tokens[pos] != "," && tokens[pos] != "from"
               && tokens[pos] != ")" && tokens[pos] != ";") {
        // Implicit alias (no AS keyword)
        item.alias = tokens[pos++];
    }
    return item;
}

static bool parseReturningClause(const std::vector<std::string>& tokens,
                                 size_t& pos,
                                 std::vector<SelectItem>& returning,
                                 ReturningOptions& options,
                                 std::string& error) {
    if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "with") {
        ++pos;
        if (pos >= tokens.size() || tokens[pos] != "(") {
            error = "RETURNING WITH requires a parenthesized alias list";
            return false;
        }
        ++pos;
        bool sawAlias = false;
        while (pos < tokens.size() && tokens[pos] != ")") {
            const std::string kind = SQLParser::toLower(tokens[pos++]);
            if (kind != "old" && kind != "new") {
                error = "RETURNING WITH accepts only OLD or NEW aliases";
                return false;
            }
            bool& aliased = kind == "old"
                ? options.oldAliased : options.newAliased;
            std::string& alias = kind == "old"
                ? options.oldAlias : options.newAlias;
            if (aliased) {
                error = "RETURNING WITH specifies " + kind + " more than once";
                return false;
            }
            if (pos >= tokens.size() ||
                SQLParser::toLower(tokens[pos]) != "as") {
                error = "RETURNING WITH alias requires AS";
                return false;
            }
            ++pos;
            if (pos >= tokens.size() || tokens[pos] == "," ||
                tokens[pos] == ")" || tokens[pos] == ";" ||
                SQLParser::isKeyword(tokens[pos])) {
                error = "RETURNING WITH requires an output alias";
                return false;
            }
            alias = tokens[pos++];
            aliased = true;
            sawAlias = true;
            if (pos < tokens.size() && tokens[pos] == ",") {
                ++pos;
                if (pos >= tokens.size() || tokens[pos] == ")") {
                    error = "RETURNING WITH alias is missing after comma";
                    return false;
                }
                continue;
            }
            if (pos >= tokens.size() || tokens[pos] != ")") {
                error = "RETURNING WITH alias list requires comma or ')'";
                return false;
            }
        }
        if (!sawAlias || pos >= tokens.size() || tokens[pos] != ")") {
            error = "RETURNING WITH requires at least one complete alias";
            return false;
        }
        ++pos;
        // Alias visibility is semantic, not grammar. Preserve canonical case
        // and let binding decide existing-range conflicts, masked defaults,
        // and duplicate explicit aliases after the target/input transforms.
    }

    while (pos < tokens.size() && tokens[pos] != ";") {
        const size_t itemStart = pos;
        SelectItem item = parseSelectItem(tokens, pos);
        if (!item.expr || pos == itemStart) {
            error = "RETURNING requires an expression";
            return false;
        }
        returning.push_back(std::move(item));
        if (pos < tokens.size() && tokens[pos] == ",") {
            ++pos;
            if (pos >= tokens.size() || tokens[pos] == ";") {
                error = "RETURNING expression is missing after comma";
                return false;
            }
            continue;
        }
        break;
    }
    if (returning.empty()) {
        error = "RETURNING requires an expression";
        return false;
    }
    return true;
}

// Parse one relation or derived query before composing JOINs. Never turn the
// opening parenthesis into a physical relation name and lose its query tree.
static std::unique_ptr<FromItem> parseFromAtom(const std::vector<std::string>& tokens, size_t& pos) {
    if (pos >= tokens.size()) return nullptr;

    auto item = std::make_unique<FromItem>();
    if (tokens[pos] == "(") {
        const auto nestedTokens = collectParenthesized(tokens, pos);
        SQLParser parser;
        auto nested = parser.parse(joinParserTokens(nestedTokens, 0, nestedTokens.size()));
        if (!nested.isValid() || !dynamic_cast<SelectStmt*>(nested.stmt.get())) return nullptr;
        item->type = FromItem::Type::Subquery;
        item->subquery = std::move(nested.stmt);
    } else {
        item->type = FromItem::Type::Table;
        if (!parseQualifiedObjectName(tokens, pos, item->tableName)) return nullptr;
    }

    // AS alias or implicit alias
    if (pos < tokens.size() && SQLParser::toLower(tokens[pos]) == "as") {
        ++pos;
        if (pos < tokens.size()) item->alias = tokens[pos++];
    } else if (pos < tokens.size() && !SQLParser::isKeyword(tokens[pos])
               && tokens[pos] != "," && tokens[pos] != ")") {
        item->alias = tokens[pos++];
    }

    if(!item->alias.empty() && pos<tokens.size() && tokens[pos]=="(") {
        ++pos;
        while(pos<tokens.size() && tokens[pos]!=")") {
            if(tokens[pos]=="," || tokens[pos]=="(" || tokens[pos]==";" || SQLParser::isKeyword(tokens[pos]))return nullptr;
            item->columnAliases.push_back(tokens[pos++]);
            if(pos<tokens.size() && tokens[pos]==","){++pos;if(pos>=tokens.size() || tokens[pos]==")")return nullptr;}
            else break;
        }
        if(item->columnAliases.empty() || pos>=tokens.size() || tokens[pos]!=")")return nullptr;
        ++pos;
    }

    return item;
}

// Preserve the NATURAL modifier independently of the row-preservation kind
// in the existing string representation (e.g. NATURAL LEFT / NATURAL FULL).
// Plain NATURAL JOIN retains its original NATURAL spelling for callers.
static bool parseFromJoinType(const std::vector<std::string>& tokens,
                              size_t& pos, std::string& joinType) {
    if (pos >= tokens.size()) return false;
    const bool natural = SQLParser::toLower(tokens[pos]) == "natural";
    if (natural) ++pos;
    if (pos >= tokens.size()) return false;
    const std::string word = SQLParser::toLower(tokens[pos]);
    std::string kind = "INNER";
    const bool explicitKind = word == "inner" || word == "left" ||
        word == "right" || word == "full" || word == "cross";
    if (explicitKind) {
        kind = word;
        std::transform(kind.begin(), kind.end(), kind.begin(),
            [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
        ++pos;
        if (word == "left" || word == "right" || word == "full") {
            if (pos < tokens.size() &&
                SQLParser::toLower(tokens[pos]) == "outer") ++pos;
        }
    }
    if (natural && kind == "CROSS") return false;
    if (pos >= tokens.size() ||
        SQLParser::toLower(tokens[pos]) != "join") return false;
    ++pos;
    joinType = natural ? (explicitKind ? "NATURAL " + kind : "NATURAL")
                       : kind;
    return true;
}

// 解析 FROM 项（支持表名、别名、JOIN）
static std::unique_ptr<FromItem> parseFromItem(const std::vector<std::string>& tokens, size_t& pos) {
    auto item = parseFromAtom(tokens, pos);
    if (!item) return nullptr;
    // JOIN handling (simplified)
    while (pos < tokens.size()) {
        std::string jkw = SQLParser::toLower(tokens[pos]);
        if (jkw == "join" || jkw == "inner" || jkw == "left" || jkw == "right"
            || jkw == "full" || jkw == "cross" || jkw == "natural") {
            std::string joinType;
            if (!parseFromJoinType(tokens, pos, joinType)) return nullptr;

            auto rightItem = parseFromAtom(tokens, pos);
            if (!rightItem) return nullptr;

            auto joinNode = std::make_unique<FromItem>();
            joinNode->type = FromItem::Type::Join;
            joinNode->joinType = joinType;
            joinNode->left = std::move(item);
            joinNode->right = std::move(rightItem);

            const bool natural = joinType.rfind("NATURAL", 0) == 0;
            // A following ON can belong to the enclosing MERGE statement,
            // not to this NATURAL source join. Leave it to the caller.
            if (!natural && pos < tokens.size() &&
                SQLParser::toLower(tokens[pos]) == "on") {
                ++pos;
                joinNode->joinCondition = parseSimpleExpr(tokens, pos);
            } else if (!natural && pos < tokens.size() &&
                       SQLParser::toLower(tokens[pos]) == "using") {
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

// The structured DML executor must never interpret an ordinary JOIN without
// its required condition as CROSS JOIN. SELECT still has additional legacy
// FROM forms; validate the DML source tree before publishing its AST.
static bool dmlSourceJoinConditionsPresent(const FromItem* item) {
    if (!item) return false;
    if (item->type != FromItem::Type::Join) return true;
    const std::string type = SQLParser::toLower(item->joinType);
    const bool implicitKeys = type == "cross" || type == "natural" ||
        type.rfind("natural ", 0) == 0;
    if (!implicitKeys && !item->joinCondition && item->usingCols.empty())
        return false;
    return dmlSourceJoinConditionsPresent(item->left.get()) &&
           dmlSourceJoinConditionsPresent(item->right.get());
}

// Comma has lower precedence than an explicit JOIN. Preserve that grammar
// as an actual CROSS node, rather than dropping all but the first source or
// making an ON clause incorrectly see the preceding comma sibling.
static std::unique_ptr<FromItem> parseDmlSourceList(const std::vector<std::string>& tokens,size_t& pos) {
    auto source=parseFromItem(tokens,pos);
    if(!source)return nullptr;
    while(pos<tokens.size() && tokens[pos]==",") {
        ++pos;
        if(pos>=tokens.size() || tokens[pos]==";" || SQLParser::toLower(tokens[pos])=="where" ||
            SQLParser::toLower(tokens[pos])=="returning")return nullptr;
        auto right=parseFromItem(tokens,pos);
        if(!right)return nullptr;
        auto cross=std::make_unique<FromItem>();cross->type=FromItem::Type::Join;cross->joinType="CROSS";
        cross->left=std::move(source);cross->right=std::move(right);source=std::move(cross);
    }
    return source;
}

// Clauses outside a parenthesized query or after a complete set expression
// belong to that query root, not the rightmost SELECT body. This helper
// consumes the original token stream so expression byte provenance survives.
static std::string parseQueryRootClauses(const std::vector<std::string>& tokens,
                                        size_t& pos, SelectStmt& statement) {
    const auto word = [&](size_t index) { return index < tokens.size() ? SQLParser::toLower(tokens[index]) : std::string{}; };
    if (word(pos) == "order") {
        if (!statement.orderBy.empty()) return "multiple ORDER BY clauses";
        ++pos;
        if (word(pos) != "by") return "ORDER requires BY";
        ++pos;
        while (pos < tokens.size()) {
            const auto next = word(pos);
            if (next == "limit" || next == "offset" || next == "fetch" || next == "for" || tokens[pos] == ";") break;
            auto expression = parseSimpleExpr(tokens, pos);
            if (!expression) return "ORDER BY requires an expression";
            bool ascending = true;
            if (word(pos) == "asc") ++pos;
            else if (word(pos) == "desc") { ascending = false; ++pos; }
            statement.orderBy.push_back({std::move(expression), ascending, !ascending, ""});
            if (word(pos) == "nulls") {
                ++pos;
                if (word(pos) == "first") { statement.orderBy.back().nullsFirst = true; ++pos; }
                else if (word(pos) == "last") { statement.orderBy.back().nullsFirst = false; ++pos; }
                else return "NULLS requires FIRST or LAST";
            }
            if (pos < tokens.size() && tokens[pos] == ",") { ++pos; continue; }
            break;
        }
        if (statement.orderBy.empty() || (pos && tokens[pos - 1] == ",")) return "ORDER BY requires an expression";
    }
    bool sawLimit = statement.limit.has_value() || statement.signedFetchCount.has_value(), sawOffset = statement.offset.has_value();
    while (pos < tokens.size()) {
        const auto clause = word(pos);
        if (clause == "limit") {
            if (sawLimit) return "multiple LIMIT/FETCH clauses";
            sawLimit = true; ++pos;
            if (word(pos) == "all") ++pos;
            else {
                size_t count = 0;
                if (!parseNonNegativeInteger(tokens, pos, count)) return "LIMIT requires a non-negative integer or ALL";
                statement.limit = count;
            }
        } else if (clause == "offset") {
            if (sawOffset) return "multiple OFFSET clauses";
            sawOffset = true; ++pos;
            size_t count = 0;
            if (!parseNonNegativeInteger(tokens, pos, count)) return "OFFSET requires a non-negative integer";
            statement.offset = count;
            if (word(pos) == "row" || word(pos) == "rows") ++pos;
        } else if (clause == "fetch") {
            if (sawLimit) return "multiple LIMIT/FETCH clauses";
            sawLimit = true; ++pos;
            if (word(pos) != "first" && word(pos) != "next") return "FETCH requires FIRST or NEXT";
            ++pos; statement.fetchFirst = true;
            if (word(pos) == "row" || word(pos) == "rows") {statement.limit = 1;statement.signedFetchCount=1;}
            else {
                int64_t count=0;
                if(!parseFetchInteger(tokens,pos,count))return "FETCH requires a signed integer count";
                statement.signedFetchCount=count;
                if(count>=0)statement.limit=static_cast<size_t>(count);else statement.limit.reset();
            }
            if (word(pos) != "row" && word(pos) != "rows") return "FETCH count must be followed by ROW or ROWS";
            ++pos;
            if (word(pos) == "only") ++pos;
            else if (word(pos) == "with" && word(pos + 1) == "ties") { statement.withTies = true; pos += 2; }
            else return "FETCH requires ONLY or WITH TIES";
        } else break;
    }
    while (pos < tokens.size() && tokens[pos] == ";") ++pos;
    return pos == tokens.size() ? std::string{} : "unexpected token after query expression: " + tokens[pos];
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

        size_t tail = tokens.size();
        int tailDepth = 0;
        for (size_t index = rhsBegin; index < tokens.size(); ++index) {
            if (tokens[index] == "(") { ++tailDepth; continue; }
            if (tokens[index] == ")") { --tailDepth; continue; }
            const auto word = toLower(tokens[index]);
            if (!tailDepth && (word == "order" || word == "limit" || word == "offset" || word == "fetch" || word == "for")) { tail = index; break; }
        }
        const auto leftSql = joinParserTokens(tokens, 0, setLocation.position);
        ParseResult left = bindingParse ? parse(leftSql) : parseSelect(leftSql);
        if (!left.success || !left.stmt) {
            r.error = left.error.empty() ? "invalid left set-operation operand" : left.error;
            return r;
        }
        const auto rightSql = joinParserTokens(tokens, rhsBegin, tail);
        ParseResult right = bindingParse ? parse(rightSql) : parseSelect(rightSql);
        if (!right.success || !right.stmt) {
            r.error = right.error.empty() ? "invalid right set-operation operand" : right.error;
            return r;
        }
        auto* leftSelect = dynamic_cast<SelectStmt*>(left.stmt.get());
        if (!leftSelect) {
            r.error = "set operation operand must be SELECT";
            return r;
        }
        if (tokens.front() != "(" && (!leftSelect->orderBy.empty() || leftSelect->limit || leftSelect->offset || !leftSelect->locking.empty())) {
            r.error = "set operand with ORDER/LIMIT requires parentheses";
            return r;
        }
        if (tail == tokens.size() && leftSelect->setOp == SetOp::None && !leftSelect->setOpLhs &&
            leftSelect->orderBy.empty() && !leftSelect->limit && !leftSelect->offset && leftSelect->locking.empty()) {
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
        wrapper->ctes = std::move(leftSelect->ctes);
        wrapper->setOpLhs = std::move(left.stmt);
        wrapper->setOpRhs = std::move(right.stmt);
        if (tail < tokens.size()) {
            size_t position = tail;
            r.error = parseQueryRootClauses(tokens, position, *wrapper);
            if (!r.error.empty()) return r;
        }
        r.success = true;
        r.stmt = std::move(wrapper);
        return r;
    }

    if (tokens.front() == "(") {
        size_t closing = 0; int nesting = 0;
        for (; closing < tokens.size(); ++closing) {
            if (tokens[closing] == "(") ++nesting;
            else if (tokens[closing] == ")" && --nesting == 0) break;
        }
        if (closing == tokens.size()) { r.error = "unterminated parenthesized query"; return r; }
        auto child = parse(joinParserTokens(tokens, 1, closing));
        auto* query = child.isValid() ? dynamic_cast<SelectStmt*>(child.stmt.get()) : nullptr;
        if (!query) { r.error = child.error.empty() ? "parenthesized query requires SELECT/VALUES" : child.error; return r; }
        size_t position = closing + 1;
        r.error = parseQueryRootClauses(tokens, position, *query);
        if (!r.error.empty()) return r;
        return child;
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
                cte.materializationSpecified = true;
                pos += 2;
            } else if (pos < tokens.size() && toLower(tokens[pos]) == "materialized") {
                cte.materialized = true;
                cte.materializationSpecified = true;
                ++pos;
            }
            if (pos < tokens.size() && tokens[pos] == "(") {
                ++pos;
                const size_t childBegin = pos;
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
                const size_t childEnd = pos;
                const auto span = statementSource(tokens, childBegin, childEnd);
                cte.queryBegin = span.first;
                cte.queryEnd = span.second;
                if (pos < tokens.size() && tokens[pos] == ")") ++pos;
                if (bindingParse) {
                    subq = joinParserTokens(tokens, childBegin, childEnd);
                    auto child = parse(subq);
                    if (!child.isValid()) {
                        r.error = child.error.empty() ? "invalid WITH query" : child.error;
                        return r;
                    }
                    cte.query = std::move(child.stmt);
                } else cte.query = parse(subq).stmt;
            }
            if (bindingParse && !cte.query) { r.error = "WITH requires AS (query)"; return r; }
            stmt->ctes.push_back(std::move(cte));
            if (pos < tokens.size() && tokens[pos] == ",") {
                ++pos;
                continue;
            }
            break;
        }
    }

    if (pos < tokens.size()) {
        const std::string primary = toLower(tokens[pos]);
        if (primary == "insert" || primary == "update" || primary == "delete" || primary == "merge") {
            const auto span = statementSource(tokens, pos, tokens.size());
            auto child = parse(joinParserTokens(tokens, pos, tokens.size()));
            if (!child.isValid()) {
                r.error = child.error.empty() ? "invalid WITH primary statement" : child.error;
                return r;
            }
            auto envelope = std::make_unique<WithStmt>(child.stmt->command);
            envelope->ctes = std::move(stmt->ctes);
            envelope->statement = std::move(child.stmt);
            envelope->statementBegin = span.first;
            envelope->statementEnd = span.second;
            r.stmt = std::move(envelope);
            r.success = true;
            return r;
        }
    }

    // Skip SELECT
    if (pos < tokens.size() && toLower(tokens[pos]) == "select") {
        ++pos;
    } else if (bindingParse) { r.error = "WITH requires a SELECT body"; return r; }

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
        if (tokens[pos] == ";" || toLower(tokens[pos]) == "from" || toLower(tokens[pos]) == "where"
            || toLower(tokens[pos]) == "group" || toLower(tokens[pos]) == "having"
            || toLower(tokens[pos]) == "order" || toLower(tokens[pos]) == "limit"
            || toLower(tokens[pos]) == "offset" || toLower(tokens[pos]) == "union"
            || toLower(tokens[pos]) == "intersect" || toLower(tokens[pos]) == "except"
            || toLower(tokens[pos]) == "for" || toLower(tokens[pos]) == "fetch") {
            break;
        }
        auto item = parseSelectItem(tokens, pos);
        if (bindingParse && !item.expr) { r.error = "SELECT requires an expression"; return r; }
        stmt->selectList.push_back(std::move(item));
        if (pos < tokens.size() && tokens[pos] == ",") {
            ++pos;
            continue;
        }
        if (bindingParse) break;
    }
    if (bindingParse && (stmt->selectList.empty() || (pos && tokens[pos - 1] == ","))) {
        r.error = "SELECT requires an expression"; return r;
    }

    // FROM (supports comma join and explicit JOINs)
    if (pos < tokens.size() && toLower(tokens[pos]) == "from") {
        ++pos;
        auto firstItem = parseFromItem(tokens, pos);
        if (!firstItem) {
            r.error = "invalid relation or JOIN in FROM clause";
            return r;
        }
        if (firstItem) {
            while (pos < tokens.size()) {
                std::string w = toLower(tokens[pos]);
                if (w == "where" || w == "group" || w == "having" || w == "order"
                    || w == "limit" || w == "offset" || w == "union"
                    || w == "intersect" || w == "except" || w == "for" || w == "fetch"
                    || w == ")" || w == ";") {
                    break;
                }
                std::string joinType;
                if (tokens[pos] == ",") {
                    ++pos;
                    joinType = "CROSS";
                } else if (w == "join" || w == "inner" || w == "left" ||
                           w == "right" || w == "full" || w == "cross" ||
                           w == "natural") {
                    if (!parseFromJoinType(tokens, pos, joinType)) {
                        r.error = "invalid JOIN type in FROM clause";
                        return r;
                    }
                } else {
                    break;
                }
                auto joinItem = std::make_unique<FromItem>();
                joinItem->type = FromItem::Type::Join;
                joinItem->joinType = joinType;
                joinItem->left = std::move(firstItem);
                joinItem->right = parseFromItem(tokens, pos);
                if (!joinItem->right) {
                    r.error = "invalid right relation in FROM clause";
                    return r;
                }
                const bool natural = joinType.rfind("NATURAL", 0) == 0;
                if (!natural && pos < tokens.size() &&
                    toLower(tokens[pos]) == "on") {
                    ++pos;
                    joinItem->joinCondition = parseExpr(tokens, pos);
                } else if (!natural && pos < tokens.size() &&
                           toLower(tokens[pos]) == "using") {
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
            if (pos < tokens.size() &&
                (toLower(tokens[pos]) == "on" ||
                 toLower(tokens[pos]) == "using")) {
                r.error = "unexpected JOIN condition in FROM clause";
                return r;
            }
            stmt->fromClause = std::move(firstItem);
        }
    }

    // WHERE
    if (pos < tokens.size() && toLower(tokens[pos]) == "where") {
        ++pos;
        stmt->whereClause = parseSimpleExpr(tokens, pos);
        if (bindingParse && !stmt->whereClause) { r.error = "WHERE requires an expression"; return r; }
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
        if (bindingParse && !stmt->having) { r.error = "HAVING requires an expression"; return r; }
    }

    // ORDER BY
    if (pos < tokens.size() && toLower(tokens[pos]) == "order") {
        ++pos;
        if (pos < tokens.size() && toLower(tokens[pos]) == "by") ++pos;
        while (pos < tokens.size()) {
            std::string w = toLower(tokens[pos]);
            if (w == "limit" || w == "offset" || w == "union"
                || w == "intersect" || w == "except" || w == "for" || w == "fetch"
                || w == ";") break;
            auto expr = parseSimpleExpr(tokens, pos);
            if (bindingParse && !expr) { r.error = "ORDER BY requires an expression"; return r; }
            bool asc = true;
            if (pos < tokens.size() && toLower(tokens[pos]) == "asc") { asc = true; ++pos; }
            else if (pos < tokens.size() && toLower(tokens[pos]) == "desc") { asc = false; ++pos; }
            if (expr) stmt->orderBy.push_back({std::move(expr), asc, !asc, ""});
            if (pos < tokens.size() && toLower(tokens[pos]) == "nulls") {
                ++pos;
                if (pos < tokens.size() && toLower(tokens[pos]) == "first") {
                    stmt->orderBy.back().nullsFirst = true; ++pos;
                } else if (pos < tokens.size() && toLower(tokens[pos]) == "last") {
                    stmt->orderBy.back().nullsFirst = false; ++pos;
                }
            }
            if (pos < tokens.size() && tokens[pos] == ",") { ++pos; continue; }
            if (bindingParse) break;
        }
        if (bindingParse && (stmt->orderBy.empty() || (pos && tokens[pos - 1] == ","))) {
            r.error = "ORDER BY requires an expression"; return r;
        }
    }

    // LIMIT and OFFSET may appear in either order. Keep their independent
    // grammar state (including LIMIT ALL) rather than silently leaving a
    // legal trailing LIMIT unparsed after OFFSET.
    bool sawLimit = false, sawOffset = false;
    while (pos < tokens.size()) {
    const auto rowClause = toLower(tokens[pos]);
    if (rowClause == "limit") {
        if (sawLimit) { r.error = "multiple LIMIT/FETCH clauses"; return r; }
        sawLimit = true;
        ++pos;
        if (pos >= tokens.size()) {
            r.error = "LIMIT requires a non-negative integer or ALL";
            return r;
        }
        if (toLower(tokens[pos]) == "all") {
            ++pos;
        } else {
            size_t limit = 0;
            if (!parseNonNegativeInteger(tokens, pos, limit)) {
                r.error = "LIMIT requires a non-negative integer or ALL";
                return r;
            }
            stmt->limit = limit;
        }
        if (pos < tokens.size() && toLower(tokens[pos]) == "with") {
            r.error = "WITH TIES requires FETCH, not LIMIT";
            r.sqlState = "42601";
            return r;
        }
        continue;
    }
    if (rowClause == "offset") {
        if (sawOffset) { r.error = "multiple OFFSET clauses"; return r; }
        sawOffset = true;
        ++pos;
        if (pos >= tokens.size()) {
            r.error = "OFFSET requires a non-negative integer";
            return r;
        }
        size_t offset = 0;
        if (!parseNonNegativeInteger(tokens, pos, offset)) {
            r.error = "OFFSET requires a non-negative integer";
            return r;
        }
        stmt->offset = offset;
        if (pos < tokens.size() &&
            (toLower(tokens[pos]) == "row" || toLower(tokens[pos]) == "rows")) ++pos;
        continue;
    }
    // FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } { ONLY | WITH TIES }
    if (rowClause == "fetch") {
        if (sawLimit) { r.error = "multiple LIMIT/FETCH clauses"; return r; }
        sawLimit = true;
        ++pos;
        if (pos >= tokens.size() || (toLower(tokens[pos]) != "first" && toLower(tokens[pos]) != "next")) {
            r.error = "FETCH requires FIRST or NEXT";
            return r;
        }
        ++pos;
        stmt->fetchFirst = true;

        if (pos < tokens.size() && (toLower(tokens[pos]) == "row" || toLower(tokens[pos]) == "rows")) {
            stmt->limit = 1;stmt->signedFetchCount=1;
        } else {
            if (pos >= tokens.size()) {
                r.error = "FETCH requires a signed integer count";
                return r;
            }
            int64_t count=0;
            if(!parseFetchInteger(tokens,pos,count)) {
                r.error = "FETCH requires a signed integer count";
                return r;
            }
            stmt->signedFetchCount=count;
            if(count>=0)stmt->limit=static_cast<size_t>(count);else stmt->limit.reset();
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
        continue;
    }
    break;
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

    if (bindingParse) {
        while (pos < tokens.size() && tokens[pos] == ";") ++pos;
        if (pos != tokens.size()) {
            r.error = "unexpected token in SELECT statement: " + tokens[pos];
            return r;
        }
    }

    if(stmt->withTies && stmt->orderBy.empty()) {
        r.error="WITH TIES cannot be specified without ORDER BY clause";
        r.sqlState="42601";return r;
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
    if (!parseQualifiedObjectName(tokens, pos, stmt->tableName)) {
        r.error = "INSERT requires a valid target table";
        return r;
    }

    // Optional column list: (col1, col2, ...)
    if (pos < tokens.size() && tokens[pos] == "(") {
        auto cols = collectParenthesized(tokens, pos);
        for (const auto& c : cols) {
            if (c != ",") stmt->columns.push_back(c);
        }
    }

    // PostgreSQL places the identity override clause between the optional
    // target-column list and VALUES/SELECT/DEFAULT VALUES.
    if (pos < tokens.size() && toLower(tokens[pos]) == "overriding") {
        ++pos;
        if (pos + 1 >= tokens.size()) {
            r.error = "OVERRIDING requires SYSTEM VALUE or USER VALUE";
            return r;
        }
        const std::string source = toLower(tokens[pos++]);
        if ((source != "system" && source != "user") ||
            toLower(tokens[pos++]) != "value") {
            r.error = "OVERRIDING requires SYSTEM VALUE or USER VALUE";
            return r;
        }
        stmt->override_ = source;
    }

    // VALUES or SELECT
    if (pos < tokens.size() && toLower(tokens[pos]) == "values") {
        ++pos;
        while (true) {
            if (pos >= tokens.size() || tokens[pos] != "(") {
                r.error = "VALUES requires a parenthesized row";
                return r;
            }
            ++pos;
            std::vector<ExprPtr> row;
            while (true) {
                if (pos >= tokens.size() || tokens[pos] == ")" || tokens[pos] == ",") {
                    r.error = "VALUES requires an expression";
                    return r;
                }
                if (toLower(tokens[pos]) == "default") {
                    // DEFAULT retains its own positional value slot.
                    auto defaultExpr = std::make_unique<LiteralExpr>();
                    defaultExpr->value = "default";
                    row.push_back(std::move(defaultExpr));
                    ++pos;
                } else {
                    auto expr = parseSimpleExpr(tokens, pos);
                    const auto* literal = dynamic_cast<const LiteralExpr*>(expr.get());
                    if (!expr || (literal && literal->value == "*")) {
                        r.error = "VALUES requires a valid expression";
                        return r;
                    }
                    row.push_back(std::move(expr));
                }
                if (pos < tokens.size() && tokens[pos] == ",") { ++pos; continue; }
                if (pos >= tokens.size() || tokens[pos] != ")") {
                    r.error = "VALUES expressions require commas and a closing parenthesis";
                    return r;
                }
                ++pos;
                break;
            }
            stmt->values.push_back(std::move(row));
            if (pos < tokens.size() && tokens[pos] == ",") {
                ++pos;
                continue;
            }
            // Row width is contextual analysis, not raw grammar. Preserve
            // source-order input coercion before a later row-width error.
            break;
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
        if (bindingParse) selectSql = joinParserTokens(tokens, pos, returningPos);
        ParseResult source = bindingParse ? parse(selectSql) : parseSelect(selectSql);
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
        if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "on" &&
            toLower(tokens[pos + 1]) == "constraint") {
            pos += 2;
            if (pos >= tokens.size() || tokens[pos] == ";" ||
                toLower(tokens[pos]) == "do") {
                r.error = "ON CONFLICT ON CONSTRAINT requires a constraint name";
                return r;
            }
            stmt->conflictConstraint = tokens[pos++];
        } else if (pos < tokens.size() && tokens[pos] == "(") {
            auto cols = collectParenthesized(tokens, pos);
            for (const auto& c : cols) {
                if (c != ",") stmt->conflictTarget.push_back(c);
            }
            if (stmt->conflictTarget.empty()) {
                r.error = "ON CONFLICT requires a non-empty inference target";
                return r;
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
            bool sawSet = false;
            if (pos < tokens.size() && toLower(tokens[pos]) == "set") {
                sawSet = true;
                ++pos;
                while (pos < tokens.size()) {
                    std::string w = toLower(tokens[pos]);
                    if (w == "where" || w == "returning" || tokens[pos] == ";") break;
                    if (pos + 1 < tokens.size() && tokens[pos + 1] == "=") {
                        std::string col = tokens[pos];
                        pos += 2; // skip col =
                        auto expr = parseSimpleExpr(tokens, pos);
                        if (!expr) {
                            r.error = "ON CONFLICT DO UPDATE requires a valid SET expression";
                            return r;
                        }
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
            if (!sawSet || stmt->conflictUpdateSet.empty()) {
                r.error = "ON CONFLICT DO UPDATE requires SET assignments";
                return r;
            }
            if (stmt->conflictTarget.empty() &&
                stmt->conflictConstraint.empty()) {
                r.error = "ON CONFLICT DO UPDATE requires an inference target";
                return r;
            }
        } else {
            r.error = "ON CONFLICT requires DO NOTHING or DO UPDATE";
            return r;
        }
    }

    // RETURNING
    if (pos < tokens.size() && toLower(tokens[pos]) == "returning") {
        ++pos;
        if (!parseReturningClause(tokens, pos, stmt->returning,
                                  stmt->returningOptions, r.error)) {
            return r;
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

    // UPDATE ONLY table — inheritance-restricted update
    if (pos < tokens.size() && toLower(tokens[pos]) == "only") {
        stmt->only = true;
        ++pos;
    }

    // Table name, optional [AS] alias (PG: UPDATE t [AS] x SET ...)
    if (!parseQualifiedObjectName(tokens, pos, stmt->tableName)) {
        r.error = "UPDATE requires a valid target table";
        return r;
    }
    std::string updateAlias;
    if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "as") {
        updateAlias = tokens[pos + 1];
        pos += 2;
    } else if (pos < tokens.size() && !isKeyword(tokens[pos]) &&
               toLower(tokens[pos]) != "set" && tokens[pos] != ";" &&
               toLower(tokens[pos]) != "where" && toLower(tokens[pos]) != "returning") {
        updateAlias = tokens[pos++];
    }
    if (!updateAlias.empty()) stmt->alias = updateAlias;

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
        const size_t expressionBegin = pos;
        ExprPtr expr;
        if (pos < tokens.size() && toLower(tokens[pos]) == "default") {
            auto value = std::make_unique<LiteralExpr>();
            value->value = "default";
            ++pos;
            markSource(value.get(), tokens, expressionBegin, pos);
            expr = std::move(value);
        } else {
            expr = parseSimpleExpr(tokens, pos);
        }
        if (!expr) {
            r.error = "missing expression in UPDATE SET clause";
            return r;
        }
        stmt->setClauses.emplace_back(col, std::move(expr));
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
        stmt->fromClause = parseDmlSourceList(tokens, pos);
        if (!stmt->fromClause) {
            r.error = "UPDATE FROM requires a valid relation or JOIN";
            return r;
        }
        if (!dmlSourceJoinConditionsPresent(stmt->fromClause.get())) {
            r.error = "UPDATE FROM JOIN requires ON or USING";
            return r;
        }
    }

    // WHERE
    if (pos < tokens.size() && toLower(tokens[pos]) == "where") {
        ++pos;
        if (pos + 1 < tokens.size() &&
            toLower(tokens[pos]) == "current" &&
            toLower(tokens[pos + 1]) == "of") {
            pos += 2;
            if (pos >= tokens.size() || tokens[pos] == ";" ||
                toLower(tokens[pos]) == "returning") {
                r.error = "WHERE CURRENT OF requires a cursor name";
                return r;
            }
            stmt->whereCurrentOf = tokens[pos++];
        } else {
            stmt->whereClause = parseSimpleExpr(tokens, pos);
            if (!stmt->whereClause) {
                r.error = "UPDATE WHERE requires an expression";
                return r;
            }
        }
    }

    // RETURNING
    if (pos < tokens.size() && toLower(tokens[pos]) == "returning") {
        ++pos;
        if (!parseReturningClause(tokens, pos, stmt->returning,
                                  stmt->returningOptions, r.error)) {
            return r;
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
    if (!parseQualifiedObjectName(tokens, pos, stmt->tableName)) {
        r.error = "DELETE requires a valid target table";
        return r;
    }

    // PostgreSQL hides the original relation name once an alias is supplied.
    // Preserve the alias in the typed AST so USING predicates resolve against
    // the same namespace as UPDATE ... FROM.
    if (pos < tokens.size() && toLower(tokens[pos]) == "as") {
        ++pos;
        if (pos >= tokens.size() || tokens[pos] == ";" ||
            isKeyword(tokens[pos])) {
            r.error = "DELETE AS requires an alias";
            return r;
        }
        stmt->alias = tokens[pos++];
    } else if (pos < tokens.size() && !isKeyword(tokens[pos]) &&
               tokens[pos] != ";") {
        stmt->alias = tokens[pos++];
    }

    // USING
    if (pos < tokens.size() && toLower(tokens[pos]) == "using") {
        ++pos;
        stmt->usingClause = parseDmlSourceList(tokens, pos);
        if (!stmt->usingClause) {
            r.error = "DELETE USING requires a valid relation or JOIN";
            return r;
        }
        if (!dmlSourceJoinConditionsPresent(stmt->usingClause.get())) {
            r.error = "DELETE USING JOIN requires ON or USING";
            return r;
        }
    }

    // WHERE
    if (pos < tokens.size() && toLower(tokens[pos]) == "where") {
        ++pos;
        if (pos + 1 < tokens.size() &&
            toLower(tokens[pos]) == "current" &&
            toLower(tokens[pos + 1]) == "of") {
            pos += 2;
            if (pos >= tokens.size() || tokens[pos] == ";" ||
                toLower(tokens[pos]) == "returning") {
                r.error = "WHERE CURRENT OF requires a cursor name";
                return r;
            }
            stmt->whereCurrentOf = tokens[pos++];
        } else {
            stmt->whereClause = parseSimpleExpr(tokens, pos);
            if (!stmt->whereClause) {
                r.error = "DELETE WHERE requires an expression";
                return r;
            }
        }
    }

    // RETURNING
    if (pos < tokens.size() && toLower(tokens[pos]) == "returning") {
        ++pos;
        if (!parseReturningClause(tokens, pos, stmt->returning,
                                  stmt->returningOptions, r.error)) {
            return r;
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
    const auto fail = [&](const std::string& message) {
        ParseResult failed;
        failed.error = message;
        return failed;
    };
    if (tokens.size() < 5) return fail("MERGE statement too short");

    auto stmt = std::make_unique<MergeStmt>();
    size_t pos = 1; // skip MERGE
    if (pos < tokens.size() && toLower(tokens[pos]) == "into") ++pos;
    if (!parseQualifiedObjectName(tokens, pos, stmt->targetTable)) {
        return fail("MERGE requires a valid target table");
    }

    // PostgreSQL permits a target alias.  Parsing it here is important: the
    // old permissive parser otherwise skipped the alias together with the
    // rest of the statement and returned a half-populated, executable AST.
    if (pos < tokens.size() && toLower(tokens[pos]) == "as") {
        ++pos;
        if (pos >= tokens.size() || isKeyword(tokens[pos]) ||
            tokens[pos] == ";") {
            return fail("MERGE target alias is missing");
        }
        stmt->targetAlias = tokens[pos++];
    } else if (pos < tokens.size() && toLower(tokens[pos]) != "using" &&
               !isKeyword(tokens[pos]) && tokens[pos] != ";") {
        stmt->targetAlias = tokens[pos++];
    }

    if (pos >= tokens.size() || toLower(tokens[pos]) != "using") {
        return fail("MERGE requires USING");
    }
    ++pos;
    stmt->source = parseFromItem(tokens, pos);
    if (!stmt->source) return fail("MERGE requires a valid source relation");

    if (pos >= tokens.size() || toLower(tokens[pos]) != "on") {
        return fail("MERGE requires ON");
    }
    ++pos;
    const size_t joinStart = pos;
    stmt->joinCondition = parseSimpleExpr(tokens, pos);
    if (!stmt->joinCondition || pos == joinStart) {
        return fail("MERGE ON requires an expression");
    }

    bool sawWhen = false;
    bool unconditionalMatched = false;
    bool unconditionalByTarget = false;
    bool unconditionalBySource = false;
    while (pos < tokens.size() && toLower(tokens[pos]) == "when") {
        sawWhen = true;
        ++pos;
        MergeStmt::WhenClause wc;
        if (pos < tokens.size() && toLower(tokens[pos]) == "matched") {
            wc.matched = true;
            ++pos;
        } else if (pos + 1 < tokens.size() &&
                   toLower(tokens[pos]) == "not" &&
                   toLower(tokens[pos + 1]) == "matched") {
            wc.matched = false;
            wc.bySource = "target"; // bare NOT MATCHED means BY TARGET
            pos += 2;
            if (pos < tokens.size() && toLower(tokens[pos]) == "by") {
                ++pos;
                if (pos >= tokens.size()) {
                    return fail("MERGE NOT MATCHED BY requires SOURCE or TARGET");
                }
                wc.bySource = toLower(tokens[pos++]);
                if (wc.bySource != "source" && wc.bySource != "target") {
                    return fail("MERGE NOT MATCHED BY requires SOURCE or TARGET");
                }
            }
        } else {
            return fail("MERGE WHEN requires MATCHED or NOT MATCHED");
        }

        if (pos < tokens.size() && toLower(tokens[pos]) == "and") {
            ++pos;
            const size_t conditionStart = pos;
            wc.condition = parseSimpleExpr(tokens, pos);
            if (!wc.condition || pos == conditionStart) {
                return fail("MERGE WHEN AND requires an expression");
            }
        }
        if (pos >= tokens.size() || toLower(tokens[pos]) != "then") {
            return fail("MERGE WHEN clause requires THEN");
        }
        ++pos;

        if (pos < tokens.size() && toLower(tokens[pos]) == "do") {
            ++pos;
            if (pos >= tokens.size() || toLower(tokens[pos]) != "nothing") {
                return fail("MERGE DO must be followed by NOTHING");
            }
            wc.action = "DO NOTHING";
            ++pos;
        } else if (pos < tokens.size() && toLower(tokens[pos]) == "update") {
            wc.action = "UPDATE";
            ++pos;
            if (pos >= tokens.size() || toLower(tokens[pos]) != "set") {
                return fail("MERGE UPDATE requires SET");
            }
            ++pos;
            while (true) {
                if (pos >= tokens.size() || tokens[pos] == ";" ||
                    toLower(tokens[pos]) == "when" ||
                    toLower(tokens[pos]) == "returning") {
                    return fail("MERGE UPDATE SET requires an assignment");
                }
                const std::string column = tokens[pos++];
                if (pos >= tokens.size() || tokens[pos] != "=") {
                    return fail("MERGE UPDATE assignment requires =");
                }
                ++pos;
                const size_t expressionStart = pos;
                auto expression = parseSimpleExpr(tokens, pos);
                if (!expression || pos == expressionStart) {
                    return fail("MERGE UPDATE assignment requires an expression");
                }
                wc.updateSet.emplace_back(column, std::move(expression));
                if (pos >= tokens.size() || tokens[pos] != ",") break;
                ++pos;
            }
        } else if (pos < tokens.size() && toLower(tokens[pos]) == "insert") {
            wc.action = "INSERT";
            ++pos;
            // This executor intentionally requires an explicit target column
            // list.  Omitted-column and DEFAULT VALUES forms remain outside
            // its capability boundary and are rejected before execution.
            if (pos >= tokens.size() || tokens[pos] != "(") {
                return fail("MERGE INSERT requires an explicit column list");
            }
            ++pos;
            std::vector<std::string> columns;
            while (pos < tokens.size() && tokens[pos] != ")") {
                if (tokens[pos] == ",") {
                    return fail("MERGE INSERT column name is missing");
                }
                columns.push_back(tokens[pos++]);
                if (pos < tokens.size() && tokens[pos] == ",") {
                    ++pos;
                    if (pos < tokens.size() && tokens[pos] == ")") {
                        return fail("MERGE INSERT column name is missing");
                    }
                } else if (pos < tokens.size() && tokens[pos] != ")") {
                    return fail("MERGE INSERT column list is malformed");
                }
            }
            if (columns.empty() || pos >= tokens.size() || tokens[pos] != ")") {
                return fail("MERGE INSERT column list is malformed");
            }
            ++pos;
            if (pos >= tokens.size() || toLower(tokens[pos]) != "values") {
                return fail("MERGE INSERT requires VALUES");
            }
            ++pos;
            if (pos >= tokens.size() || tokens[pos] != "(") {
                return fail("MERGE INSERT VALUES requires a parenthesized list");
            }
            ++pos;
            std::vector<ExprPtr> values;
            while (pos < tokens.size() && tokens[pos] != ")") {
                const size_t expressionStart = pos;
                auto expression = parseSimpleExpr(tokens, pos);
                if (!expression || pos == expressionStart) {
                    return fail("MERGE INSERT value expression is missing");
                }
                values.push_back(std::move(expression));
                if (pos < tokens.size() && tokens[pos] == ",") {
                    ++pos;
                    if (pos < tokens.size() && tokens[pos] == ")") {
                        return fail("MERGE INSERT value expression is missing");
                    }
                } else if (pos < tokens.size() && tokens[pos] != ")") {
                    return fail("MERGE INSERT VALUES list is malformed");
                }
            }
            if (pos >= tokens.size() || tokens[pos] != ")") {
                return fail("MERGE INSERT VALUES list is malformed");
            }
            ++pos;
            if (columns.size() != values.size()) {
                return fail("MERGE INSERT has more target columns than expressions");
            }
            for (size_t i = 0; i < columns.size(); ++i) {
                wc.insertCols.emplace_back(columns[i], std::move(values[i]));
            }
        } else if (pos < tokens.size() && toLower(tokens[pos]) == "delete") {
            wc.action = "DELETE";
            ++pos;
        } else {
            return fail("MERGE WHEN clause requires an action");
        }

        const std::string action = toLower(wc.action);
        if (wc.matched && action == "insert") {
            return fail("MERGE MATCHED clause cannot INSERT");
        }
        if (!wc.matched && wc.bySource != "source" &&
            action != "insert" && action != "do nothing") {
            return fail("MERGE NOT MATCHED BY TARGET clause can only INSERT or DO NOTHING");
        }
        if (!wc.matched && wc.bySource == "source" && action == "insert") {
            return fail("MERGE NOT MATCHED BY SOURCE clause cannot INSERT");
        }

        bool* unconditional = wc.matched
            ? &unconditionalMatched
            : (wc.bySource == "source"
                   ? &unconditionalBySource : &unconditionalByTarget);
        if (*unconditional) {
            return fail("unreachable MERGE WHEN clause after unconditional clause");
        }
        if (!wc.condition) *unconditional = true;
        stmt->whenClauses.push_back(std::move(wc));
    }
    if (!sawWhen) return fail("MERGE requires at least one WHEN clause");

    if (pos < tokens.size() && toLower(tokens[pos]) == "returning") {
        ++pos;
        std::string returningError;
        if (!parseReturningClause(tokens, pos, stmt->returning,
                                  stmt->returningOptions,
                                  returningError)) {
            return fail("MERGE " + returningError);
        }
    }

    while (pos < tokens.size() && tokens[pos] == ";") ++pos;
    if (pos != tokens.size()) {
        return fail("unexpected token in MERGE statement: " + tokens[pos]);
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
    size_t expectedColumns = 0;

    const auto fail = [&](const std::string& message) {
        ParseResult failed;
        failed.error = message;
        return failed;
    };

    if (tokens.size() <= 1 || tokens[pos] == ";") {
        return fail("VALUES requires at least one row");
    }

    while (pos < tokens.size()) {
        if (tokens[pos] != "(") {
            return fail("VALUES requires parenthesized rows");
        }
        ++pos;
        if (pos >= tokens.size() || tokens[pos] == ")" ||
            tokens[pos] == ",") {
            return fail("VALUES row contains an empty expression");
        }

        std::vector<ExprPtr> row;
        while (true) {
            const size_t expressionStart = pos;
            auto expression = parseSimpleExpr(tokens, pos);
            if (!expression || pos == expressionStart) {
                return fail("invalid expression in VALUES row");
            }
            row.push_back(std::move(expression));

            if (pos >= tokens.size()) {
                return fail("unterminated VALUES row");
            }
            if (tokens[pos] == ")") {
                ++pos;
                break;
            }
            if (tokens[pos] != ",") {
                return fail("unexpected token in VALUES row: " + tokens[pos]);
            }
            ++pos;
            if (pos >= tokens.size() || tokens[pos] == ")" ||
                tokens[pos] == ",") {
                return fail("VALUES row contains an empty expression");
            }
        }

        if (expectedColumns == 0) {
            expectedColumns = row.size();
        } else if (row.size() != expectedColumns) {
            return fail("VALUES lists must all be the same length");
        }
        stmt->valuesRows.push_back(std::move(row));

        if (pos == tokens.size()) break;
        if (tokens[pos] == ";") {
            ++pos;
            if (pos != tokens.size()) {
                return fail("unexpected token after VALUES statement");
            }
            break;
        }
        if (tokens[pos] == ",") {
            ++pos;
            if (pos >= tokens.size() || tokens[pos] == ";") {
                return fail("trailing comma after VALUES row");
            }
            continue;
        }
        return fail("unexpected token after VALUES statement: " +
                    tokens[pos]);
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
        r.success = stmt != nullptr;
        if (!r.success) r.error = "invalid CREATE MATERIALIZED VIEW statement";
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
        } else if (kw == "fulltext" || kw == "hash") {
            if (!match(tokens, pos, "index")) {
                r.stmt.reset();
            } else {
                ++pos;
                r.stmt = parseCreateIndex(tokens, pos);
                if (r.stmt) {
                    auto* index = static_cast<CreateIndexStmt*>(r.stmt.get());
                    index->accessMethod = kw;
                    index->compatibilityShortcut = kw;
                }
            }
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
            if (r.stmt)
                static_cast<CreateFunctionStmt*>(r.stmt.get())->replace =
                    isReplace;
        } else if (kw == "procedure") {
            r.stmt = parseCreateProcedure(tokens, pos);
            if (r.stmt)
                static_cast<CreateFunctionStmt*>(r.stmt.get())->replace =
                    isReplace;
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
        } else if (kw == "server") {
            r.stmt = parseCreateServer(tokens, pos);
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
    } else if (match(tokens, pos, "fulltext")) {
        ++pos;
        if (!match(tokens, pos, "index")) {
            r.stmt.reset();
        } else {
            ++pos;
            r.stmt = parseDropIndex(tokens, pos);
            if (r.stmt) {
                static_cast<DropStmt*>(r.stmt.get())->compatibilityShortcut =
                    "fulltext";
            }
        }
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
    } else if (first == "set" && tokens.size() >= 2 && toLower(tokens[1]) == "transaction") {
        kind = TransactionStmt::Kind::SetCharacteristics;
        pos = 2;
        if (pos >= tokens.size() || tokens[pos] == ";") {
            r.error = "SET TRANSACTION requires a transaction mode";
            return r;
        }
    } else {
        r.error = "invalid transaction start";
        return r;
    }

    auto stmt = std::make_unique<TransactionStmt>(kind);
    bool isolationSeen = false;
    bool readModeSeen = false;
    bool deferrableSeen = false;
    bool optionBeforeSeparator = false;
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
        if (word == ",") {
            if (!optionBeforeSeparator || pos + 1 >= tokens.size() ||
                tokens[pos + 1] == ";") {
                r.error = "transaction mode separator requires options on both sides";
                return r;
            }
            ++pos;
            optionBeforeSeparator = false;
            continue;
        }
        if (word == "isolation") {
            ++pos;
            if (pos >= tokens.size() || toLower(tokens[pos]) != "level") {
                r.error = "ISOLATION requires LEVEL";
                return r;
            }
            ++pos;
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
            stmt->modes.push_back({TransactionStmt::Mode::Kind::Isolation, stmt->isolation, false});
            optionBeforeSeparator = true;
            continue;
        }
        if (word == "read" && pos + 1 < tokens.size() &&
            (toLower(tokens[pos + 1]) == "only" || toLower(tokens[pos + 1]) == "write")) {
            stmt->readOnly = toLower(tokens[pos + 1]) == "only";
            pos += 2;
            readModeSeen = true;
            stmt->modes.push_back({TransactionStmt::Mode::Kind::ReadOnly,
                                   IsolationLevel::READ_COMMITTED, stmt->readOnly});
            optionBeforeSeparator = true;
            continue;
        }
        if (word == "deferrable" || (word == "not" && pos + 1 < tokens.size() &&
                                      toLower(tokens[pos + 1]) == "deferrable")) {
            stmt->deferrable = word == "deferrable";
            pos += word == "deferrable" ? 1 : 2;
            deferrableSeen = true;
            stmt->modes.push_back({TransactionStmt::Mode::Kind::Deferrable,
                                   IsolationLevel::READ_COMMITTED, stmt->deferrable});
            optionBeforeSeparator = true;
            continue;
        }
        r.error = "unsupported transaction option: " + tokens[pos];
        return r;
    }
    stmt->isolationSpecified = isolationSeen;
    stmt->readOnlySpecified = readModeSeen;
    stmt->deferrableSpecified = deferrableSeen;
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
        (toLower(tokens[pos]) == "work" || toLower(tokens[pos]) == "transaction")) {
        ++pos;
    }
    if (stmt->kind == TransactionStmt::Kind::Commit && pos < tokens.size() &&
        toLower(tokens[pos]) == "and") {
        if (pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "chain") {
            stmt->chainSpecified = true;
            stmt->chain = true;
            pos += 2;
        } else if (pos + 2 < tokens.size() && toLower(tokens[pos + 1]) == "no" &&
                   toLower(tokens[pos + 2]) == "chain") {
            stmt->chainSpecified = true;
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
    if ((kind == TransactionStmt::Kind::Rollback ||
         kind == TransactionStmt::Kind::Abort || kind == TransactionStmt::Kind::End) &&
        stmt->kind != TransactionStmt::Kind::RollbackPrepared && pos < tokens.size() &&
        (toLower(tokens[pos]) == "work" || toLower(tokens[pos]) == "transaction")) {
        ++pos;
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
        if (stmt->kind != TransactionStmt::Kind::Rollback &&
            stmt->kind != TransactionStmt::Kind::Abort &&
            stmt->kind != TransactionStmt::Kind::End) {
            r.error = "ROLLBACK TO cannot use AND CHAIN";
            return r;
        }
        if (pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "chain") {
            stmt->chainSpecified = true;
            stmt->chain = true;
            pos += 2;
        } else if (pos + 2 < tokens.size() && toLower(tokens[pos + 1]) == "no" &&
                   toLower(tokens[pos + 2]) == "chain") {
            stmt->chainSpecified = true;
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
    if (isSetTransactionStatement(sql)) return parseBegin(sql);
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
    const std::string parameter = toLower(stmt->name);
    const bool isolationName = parameter == "transaction_isolation" ||
                               parameter == "\"transaction_isolation\"";
    if (parameter == "transaction") {
        if (pos + 1 >= tokens.size() ||
            toLower(tokens[pos]) != "isolation" ||
            toLower(tokens[pos + 1]) != "level") {
            r.error = "expected ISOLATION LEVEL after SHOW TRANSACTION";
            return r;
        }
        pos += 2;
    }
    if (isolationName || parameter == "transaction") {
        stmt->name = "transaction_isolation";
        if (pos < tokens.size() && tokens[pos] == ";") ++pos;
        if (pos != tokens.size()) {
            r.error = "unexpected input after SHOW transaction_isolation";
            return r;
        }
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
    size_t pos = 1;
    if (pos < tokens.size() && toLower(tokens[pos]) == "database") ++pos;
    if (pos >= tokens.size() || tokens[pos] == ";") {
        r.error = "USE requires a database name";
        return r;
    }
    stmt->values.push_back(tokens[pos++]);
    if (pos < tokens.size() && tokens[pos] == ";") ++pos;
    if (pos != tokens.size()) {
        r.error = "unexpected token after database name";
        return r;
    }
    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseDiscard(const std::string& sql) {
    ParseResult r;
    const auto tokens = tokenize(sql);
    if ((tokens.size() != 2 &&
         !(tokens.size() == 3 && tokens.back() == ";")) ||
        toLower(tokens[0]) != "discard") {
        r.error = "DISCARD requires exactly one target";
        return r;
    }
    auto stmt = std::make_unique<DiscardStmt>();
    const auto target = toLower(tokens[1]);
    if (target == "all") stmt->target = DiscardStmt::Target::All;
    else if (target == "plans") stmt->target = DiscardStmt::Target::Plans;
    else if (target == "sequences") stmt->target = DiscardStmt::Target::Sequences;
    else if (target == "temp" || target == "temporary")
        stmt->target = DiscardStmt::Target::Temp;
    else {
        r.error = "invalid DISCARD target";
        return r;
    }
    r.success = true;
    r.stmt = std::move(stmt);
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
        if (bindingParse) innerSql = joinParserTokens(tokens, pos, tokens.size());
        auto inner = parse(innerSql);
        if (bindingParse && !inner.isValid()) { r.error = inner.error; return r; }
        stmt->query = std::move(inner.stmt);
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

ParseResult SQLParser::parseRefreshMaterializedView(const std::string& sql) {
    ParseResult result;
    const auto tokens = tokenize(sql);
    size_t pos = 0;
    const auto fail = [&](const std::string& message) {
        result.success = false;
        result.error = message;
    };

    if (tokens.size() < 4 || !match(tokens, pos, "refresh")) {
        fail("invalid REFRESH MATERIALIZED VIEW statement");
        return result;
    }
    ++pos;
    if (pos >= tokens.size() || !match(tokens, pos, "materialized")) {
        fail("expected MATERIALIZED VIEW after REFRESH");
        return result;
    }
    ++pos;
    if (pos >= tokens.size() || !match(tokens, pos, "view")) {
        fail("expected VIEW after REFRESH MATERIALIZED");
        return result;
    }
    ++pos;

    auto stmt = std::make_unique<RefreshMaterializedViewStmt>();
    if (pos < tokens.size() && match(tokens, pos, "concurrently")) {
        stmt->concurrently = true;
        ++pos;
    }
    if (pos >= tokens.size() || tokens[pos] == ";") {
        fail("REFRESH MATERIALIZED VIEW requires a relation name");
        return result;
    }
    stmt->viewName = tokens[pos++];
    if (pos < tokens.size() && tokens[pos] == ".") {
        ++pos;
        if (pos >= tokens.size() || tokens[pos] == ";") {
            fail("invalid qualified materialized-view name");
            return result;
        }
        stmt->viewName += "." + tokens[pos++];
    }

    if (pos < tokens.size() && match(tokens, pos, "with")) {
        ++pos;
        if (pos < tokens.size() && match(tokens, pos, "no")) {
            stmt->withData = false;
            ++pos;
        }
        if (pos >= tokens.size() || !match(tokens, pos, "data")) {
            fail("expected DATA after WITH in REFRESH MATERIALIZED VIEW");
            return result;
        }
        ++pos;
    }
    if (pos < tokens.size() && tokens[pos] == ";") ++pos;
    if (pos != tokens.size()) {
        fail("unexpected trailing input in REFRESH MATERIALIZED VIEW");
        return result;
    }

    result.success = true;
    result.stmt = std::move(stmt);
    return result;
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
    std::string statement = trim(sql);
    if (!statement.empty() && statement.back() == ';') {
        statement.pop_back();
        statement = trim(statement);
    }
    const std::string lsql = toLower(statement);
    auto findKeywordOutsideQuotes = [](const std::string& original,
                                       const std::string& lowered,
                                       const std::string& keyword,
                                       size_t start = 0) {
        bool singleQuoted = false;
        bool doubleQuoted = false;
        for (size_t i = start; i + keyword.size() <= original.size(); ++i) {
            const char c = original[i];
            if (singleQuoted) {
                if (c == '\'' && i + 1 < original.size() &&
                    original[i + 1] == '\'') {
                    ++i;
                } else if (c == '\'') {
                    singleQuoted = false;
                }
                continue;
            }
            if (doubleQuoted) {
                if (c == '"' && i + 1 < original.size() &&
                    original[i + 1] == '"') {
                    ++i;
                } else if (c == '"') {
                    doubleQuoted = false;
                }
                continue;
            }
            if (c == '\'') {
                singleQuoted = true;
                continue;
            }
            if (c == '"') {
                doubleQuoted = true;
                continue;
            }
            if (lowered.compare(i, keyword.size(), keyword) == 0) return i;
        }
        return std::string::npos;
    };

    size_t onPos = findKeywordOutsideQuotes(statement, lsql, " on ");
    if (onPos == std::string::npos) {
        r.success = false;
        return r;
    }

    std::string rest = trim(statement.substr(onPos + 4));
    const std::string lowerRest = toLower(rest);
    size_t isPos = findKeywordOutsideQuotes(rest, lowerRest, " is ");
    if (isPos == std::string::npos) {
        r.success = false;
        return r;
    }
    std::string beforeIs = trim(rest.substr(0, isPos));
    std::string afterIs = trim(rest.substr(isPos + 4));

    if (!afterIs.empty() && afterIs.size() >= 2 &&
        afterIs.front() == '\'' && afterIs.back() == '\'') {
        const char quote = '\'';
        for (size_t i = 1; i + 1 < afterIs.size(); ++i) {
            if (afterIs[i] == quote && i + 2 < afterIs.size() &&
                afterIs[i + 1] == quote) {
                stmt->comment.push_back(quote);
                ++i;
            } else if (afterIs[i] == quote) {
                r.success = false;
                r.error = "unexpected text after COMMENT string literal";
                return r;
            } else {
                stmt->comment.push_back(afterIs[i]);
            }
        }
    } else if (toLower(afterIs) == "null") {
        stmt->isNull = true;
        stmt->comment.clear();
    } else {
        r.success = false;
        r.error = "COMMENT text must be a string literal or NULL";
        return r;
    }

    std::string normalizedBeforeIs = beforeIs;
    bool quotedIdentifier = false;
    for (size_t i = 0; i < normalizedBeforeIs.size(); ++i) {
        char& c = normalizedBeforeIs[i];
        if (c == '"') {
            if (quotedIdentifier && i + 1 < normalizedBeforeIs.size() &&
                normalizedBeforeIs[i + 1] == '"') {
                ++i;
            } else {
                quotedIdentifier = !quotedIdentifier;
            }
        } else if (!quotedIdentifier) {
            c = static_cast<char>(
                std::tolower(static_cast<unsigned char>(c)));
        }
    }
    const std::string lowerBeforeIs = toLower(beforeIs);
    auto startsWith = [&](const std::string& prefix) -> bool {
        return lowerBeforeIs.size() >= prefix.size() &&
               lowerBeforeIs.compare(0, prefix.size(), prefix) == 0;
    };

    if (startsWith("materialized view ")) {
        stmt->objectType = "MATERIALIZED VIEW";
        stmt->objectName = trim(normalizedBeforeIs.substr(18));
    } else if (startsWith("table ")) {
        stmt->objectType = "TABLE";
        stmt->objectName = trim(normalizedBeforeIs.substr(6));
    } else if (startsWith("column ")) {
        stmt->objectType = "COLUMN";
        stmt->objectName = trim(normalizedBeforeIs.substr(7));
    } else if (startsWith("schema ")) {
        stmt->objectType = "SCHEMA";
        stmt->objectName = trim(normalizedBeforeIs.substr(7));
    } else if (startsWith("index ")) {
        stmt->objectType = "INDEX";
        stmt->objectName = trim(normalizedBeforeIs.substr(6));
    } else if (startsWith("view ")) {
        stmt->objectType = "VIEW";
        stmt->objectName = trim(normalizedBeforeIs.substr(5));
    } else if (startsWith("function ")) {
        stmt->objectType = "FUNCTION";
        stmt->objectName = trim(normalizedBeforeIs.substr(9));
    } else if (startsWith("procedure ")) {
        stmt->objectType = "PROCEDURE";
        stmt->objectName = trim(normalizedBeforeIs.substr(10));
    } else if (startsWith("sequence ")) {
        stmt->objectType = "SEQUENCE";
        stmt->objectName = trim(normalizedBeforeIs.substr(9));
    } else if (startsWith("type ")) {
        stmt->objectType = "TYPE";
        stmt->objectName = trim(normalizedBeforeIs.substr(5));
    } else {
        stmt->objectType = "UNKNOWN";
        stmt->objectName = normalizedBeforeIs;
    }

    const auto normalizeQualifiedIdentifier = [](const std::string& input,
                                                  std::string& output) {
        std::vector<std::string> parts;
        std::string current;
        bool quoted = false;
        for (size_t index = 0; index < input.size(); ++index) {
            const char c = input[index];
            if (c == '"') {
                current.push_back(c);
                if (quoted && index + 1 < input.size() &&
                    input[index + 1] == '"') {
                    current.push_back(input[++index]);
                } else {
                    quoted = !quoted;
                }
                continue;
            }
            if (c == '.' && !quoted) {
                parts.push_back(trim(current));
                current.clear();
                continue;
            }
            current.push_back(c);
        }
        if (quoted) return false;
        parts.push_back(trim(current));

        output.clear();
        for (std::string part : parts) {
            if (part.empty()) return false;
            std::string decoded;
            if (part.front() == '"') {
                if (part.size() < 2 || part.back() != '"') return false;
                for (size_t index = 1; index + 1 < part.size(); ++index) {
                    if (part[index] != '"') {
                        decoded.push_back(part[index]);
                        continue;
                    }
                    if (index + 2 >= part.size() ||
                        part[index + 1] != '"') {
                        return false;
                    }
                    decoded.push_back('"');
                    ++index;
                }
                // The current catalog name API does not carry quote metadata;
                // accepting a quoted dot would later retarget the object.
                if (decoded.empty() || decoded.find('.') != std::string::npos)
                    return false;
            } else {
                if (part.find('"') != std::string::npos) return false;
                for (const unsigned char c : part) {
                    if (std::isspace(c)) return false;
                    decoded.push_back(static_cast<char>(std::tolower(c)));
                }
            }
            if (!output.empty()) output.push_back('.');
            output += decoded;
        }
        return !output.empty();
    };

    if (stmt->objectType != "UNKNOWN" &&
        stmt->objectType != "FUNCTION" &&
        stmt->objectType != "PROCEDURE") {
        std::string identifier;
        if (!normalizeQualifiedIdentifier(stmt->objectName, identifier)) {
            r.success = false;
            r.error = "invalid COMMENT object identifier";
            return r;
        }
        stmt->objectName = std::move(identifier);
    }

    if (stmt->objectType == "COLUMN") {
        size_t dot = stmt->objectName.rfind('.');
        if (dot != std::string::npos) {
            stmt->columnName = trim(stmt->objectName.substr(dot + 1));
            stmt->objectName = trim(stmt->objectName.substr(0, dot));
        }
    }

    if (stmt->objectName.empty() ||
        (stmt->objectType == "COLUMN" && stmt->columnName.empty())) {
        r.success = false;
        r.error = "COMMENT object name is missing or invalid";
        return r;
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

static bool parseNotificationIdentifier(const std::string& token,
                                        std::string& identifier) {
    const auto truncateIdentifier = [](std::string& value) {
        if (value.size() <= 63) return;
        size_t boundary = 63;
        while (boundary > 0 &&
               (static_cast<unsigned char>(value[boundary]) & 0xc0) == 0x80) {
            --boundary;
        }
        value.resize(boundary);
    };
    if (token.size() >= 2 && token.front() == '"' && token.back() == '"') {
        identifier.clear();
        for (size_t i = 1; i + 1 < token.size(); ++i) {
            if (token[i] == '"') {
                if (i + 2 >= token.size() || token[i + 1] != '"') {
                    return false;
                }
                ++i;
            }
            identifier.push_back(token[i]);
        }
        truncateIdentifier(identifier);
        return !identifier.empty() && identifier.find('\0') == std::string::npos;
    }

    if (token.empty()) return false;
    const auto isHighByte = [](unsigned char value) { return value >= 0x80; };
    const unsigned char first = static_cast<unsigned char>(token.front());
    if (!std::isalpha(first) && token.front() != '_' && !isHighByte(first)) {
        return false;
    }
    identifier.clear();
    identifier.reserve(token.size());
    for (unsigned char value : token) {
        if (!std::isalnum(value) && value != '_' && value != '$' &&
            !isHighByte(value)) {
            return false;
        }
        identifier.push_back(value < 0x80
            ? static_cast<char>(std::tolower(value))
            : static_cast<char>(value));
    }
    truncateIdentifier(identifier);
    return true;
}

static void discardTrailingSemicolons(std::vector<std::string>& tokens) {
    while (!tokens.empty() && tokens.back() == ";") tokens.pop_back();
}

ParseResult SQLParser::parseListen(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    discardTrailingSemicolons(tokens);
    if (tokens.size() != 2) {
        r.error = "LISTEN requires exactly one channel identifier";
        return r;
    }
    auto stmt = std::make_unique<ListenStmt>();
    if (!parseNotificationIdentifier(tokens[1], stmt->channel)) {
        r.error = "invalid LISTEN channel identifier";
        return r;
    }
    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseNotify(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    discardTrailingSemicolons(tokens);
    if (tokens.size() != 2 && tokens.size() != 4) {
        r.error = "NOTIFY requires a channel and optional string payload";
        return r;
    }
    auto stmt = std::make_unique<NotifyStmt>();
    if (!parseNotificationIdentifier(tokens[1], stmt->channel)) {
        r.error = "invalid NOTIFY channel identifier";
        return r;
    }
    if (tokens.size() == 4) {
        if (tokens[2] != "," || !isStringLiteralToken(tokens[3])) {
            r.error = "NOTIFY payload must be a string literal";
            return r;
        }
        stmt->payload = stripQuotes(tokens[3]);
    }
    r.success = true;
    r.stmt = std::move(stmt);
    return r;
}

ParseResult SQLParser::parseUnlisten(const std::string& sql) {
    ParseResult r;
    auto tokens = tokenize(sql);
    discardTrailingSemicolons(tokens);
    if (tokens.size() != 2) {
        r.error = "UNLISTEN requires exactly one channel identifier or *";
        return r;
    }
    auto stmt = std::make_unique<UnlistenStmt>();
    if (tokens[1] == "*") {
        stmt->all = true;
    } else if (!parseNotificationIdentifier(tokens[1], stmt->channel)) {
        r.error = "invalid UNLISTEN channel identifier";
        return r;
    }
    r.success = true;
    r.stmt = std::move(stmt);
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
    const size_t begin = pos + 1;
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
    if (bindingParse && !inner.empty()) {
        const auto found = bindingParse->tokens.find(tokens.data());
        if (found != bindingParse->tokens.end() && begin + inner.size() <= found->second.spans.size()) {
            TokenProvenance child;
            child.spans.assign(found->second.spans.begin() + begin,
                               found->second.spans.begin() + begin + inner.size());
            bindingParse->tokens[inner.data()] = std::move(child);
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

static std::string parseReferentialAction(
    const std::vector<std::string>& tokens, size_t& pos) {
    if (pos >= tokens.size()) return "invalid";
    const std::string action = SQLParser::toLower(tokens[pos++]);
    if (action == "cascade" || action == "restrict") return action;
    if (action == "set") {
        if (pos >= tokens.size()) return "invalid";
        const std::string target = SQLParser::toLower(tokens[pos]);
        if (target == "null") {
            ++pos;
            return "setnull";
        }
        if (target == "default") {
            ++pos;
            return "setdefault";
        }
        return "invalid";
    }
    if (action == "no") {
        if (pos < tokens.size() &&
            SQLParser::toLower(tokens[pos]) == "action") {
            ++pos;
            return "noaction";
        }
        return "invalid";
    }
    return "invalid";
}

static void parseReferentialActions(
    const std::vector<std::string>& tokens, size_t& pos,
    TableConstraint& constraint) {
    while (pos + 1 < tokens.size() &&
           SQLParser::toLower(tokens[pos]) == "on") {
        const std::string event = SQLParser::toLower(tokens[pos + 1]);
        if (event != "delete" && event != "update") break;
        pos += 2;
        const std::string action = parseReferentialAction(tokens, pos);
        std::string& destination = event == "delete"
            ? constraint.onDelete : constraint.onUpdate;
        destination = destination.empty() ? action : "invalid";
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

static void applyCreateTableLikeOption(CreateTableStmt::LikeClause& clause,
                                       const std::string& option,
                                       bool include) {
    auto applyAll = [&]() {
        clause.includingComments = include;
        clause.includingCompression = include;
        clause.includingConstraints = include;
        clause.includingDefaults = include;
        clause.includingGenerated = include;
        clause.includingIdentity = include;
        clause.includingIndexes = include;
        clause.includingStatistics = include;
        clause.includingStorage = include;
    };

    if (option == "all") {
        clause.includingAll = include;
        applyAll();
    } else if (option == "comments") {
        clause.includingComments = include;
    } else if (option == "compression") {
        clause.includingCompression = include;
    } else if (option == "constraints") {
        clause.includingConstraints = include;
    } else if (option == "defaults") {
        clause.includingDefaults = include;
    } else if (option == "generated") {
        clause.includingGenerated = include;
    } else if (option == "identity") {
        clause.includingIdentity = include;
    } else if (option == "indexes") {
        clause.includingIndexes = include;
    } else if (option == "statistics") {
        clause.includingStatistics = include;
    } else if (option == "storage") {
        clause.includingStorage = include;
    } else {
        clause.optionsValid = false;
        if (clause.invalidOption.empty()) clause.invalidOption = option;
    }
}

static void parseCreateTableLikeOptions(const std::vector<std::string>& tokens,
                                        size_t& pos,
                                        CreateTableStmt::LikeClause& clause) {
    while (pos < tokens.size() &&
           (SQLParser::toLower(tokens[pos]) == "including" ||
            SQLParser::toLower(tokens[pos]) == "excluding")) {
        const bool include = SQLParser::toLower(tokens[pos++]) == "including";
        if (pos >= tokens.size() || tokens[pos] == "," ||
            tokens[pos] == ")" || tokens[pos] == ";") {
            clause.optionsValid = false;
            if (clause.invalidOption.empty()) clause.invalidOption = "<missing>";
            return;
        }
        applyCreateTableLikeOption(
            clause, SQLParser::toLower(tokens[pos++]), include);
    }
}

StmtPtr SQLParser::parseCreateTable(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateTableStmt>();

    if (pos + 2 < tokens.size() && match(tokens, pos, "if") &&
        match(tokens, pos + 1, "not") &&
        match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true;
        pos += 3;
    }

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

    if (pos >= tokens.size() ||
        (tokens[pos] != "(" && !match(tokens, pos, "as") &&
         !match(tokens, pos, "of"))) {
        return nullptr;
    }

    // CREATE TABLE ... AS SELECT ...
    if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "as" &&
        (toLower(tokens[pos + 1]) == "select" ||
         (bindingParse && toLower(tokens[pos + 1]) == "with"))) {
        const size_t queryBegin = pos + 1;
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
        if (bindingParse) {
            const size_t queryEnd = queryBegin + 1 + sel.size();
            selectSql = joinParserTokens(tokens, queryBegin, queryEnd);
            SQLParser parser;
            auto parsed = parser.parse(selectSql);
            if (!parsed.isValid()) return nullptr;
            stmt->asSelect = selectSql;
            stmt->preparedAsQuery = std::move(parsed.stmt);
        }
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
                if (pos < tokens.size()) cname = parseRoutineIdentifier(tokens[pos++]);
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
                            if (c != ",") tc.columns.push_back(parseRoutineIdentifier(c));
                        }
                        parseConstraintDeferrability(tokens, pos, tc);
                        stmt->constraints.push_back(std::move(tc));
                    } else if (ctype == "unique") {
                        ++pos;
                        auto cols = collectParenthesized(tokens, pos);
                        TableConstraint tc;
                        tc.name = cname;
                        tc.type = "UNIQUE";
                        for (const auto& c : cols) {
                            if (c != ",") tc.columns.push_back(parseRoutineIdentifier(c));
                        }
                        parseConstraintDeferrability(tokens, pos, tc);
                        stmt->constraints.push_back(std::move(tc));
                    } else if (ctype == "foreign") {
                        ++pos; if (pos < tokens.size() && toLower(tokens[pos]) == "key") ++pos;
                        auto cols = collectParenthesized(tokens, pos);
                        TableConstraint tc;
                        tc.name = cname;
                        tc.type = "FOREIGN KEY";
                        for (const auto& c : cols) {
                            if (c != ",") tc.columns.push_back(parseRoutineIdentifier(c));
                        }
                        if (pos < tokens.size() && toLower(tokens[pos]) == "references") {
                            ++pos;
                            if (!parseForeignKeyTarget(
                                    tokens, pos, tc.refTable)) return nullptr;
                            if (pos < tokens.size() && tokens[pos] == "(") {
                                auto refcols = collectParenthesized(tokens, pos);
                                for (const auto& c : refcols) {
                                    if (c != ",") tc.refColumns.push_back(parseRoutineIdentifier(c));
                                }
                            }
                            parseReferentialActions(tokens, pos, tc);
                        }
                        parseConstraintDeferrability(tokens, pos, tc);
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
                        parseConstraintDeferrability(tokens, pos, tc);
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
                    if (c != ",") tc.columns.push_back(parseRoutineIdentifier(c));
                }
                parseConstraintDeferrability(tokens, pos, tc);
                stmt->constraints.push_back(std::move(tc));
            } else if (ltok == "unique") {
                ++pos;
                auto cols = collectParenthesized(tokens, pos);
                TableConstraint tc;
                tc.type = "UNIQUE";
                for (const auto& c : cols) {
                    if (c != ",") tc.columns.push_back(parseRoutineIdentifier(c));
                }
                parseConstraintDeferrability(tokens, pos, tc);
                stmt->constraints.push_back(std::move(tc));
            } else if (ltok == "foreign") {
                ++pos; if (pos < tokens.size() && toLower(tokens[pos]) == "key") ++pos;
                auto cols = collectParenthesized(tokens, pos);
                TableConstraint tc;
                tc.type = "FOREIGN KEY";
                for (const auto& c : cols) {
                    if (c != ",") tc.columns.push_back(parseRoutineIdentifier(c));
                }
                if (pos < tokens.size() && toLower(tokens[pos]) == "references") {
                    ++pos;
                    if (!parseForeignKeyTarget(
                            tokens, pos, tc.refTable)) return nullptr;
                    if (pos < tokens.size() && tokens[pos] == "(") {
                        auto refcols = collectParenthesized(tokens, pos);
                        for (const auto& c : refcols) {
                            if (c != ",") tc.refColumns.push_back(parseRoutineIdentifier(c));
                        }
                    }
                    parseReferentialActions(tokens, pos, tc);
                }
                parseConstraintDeferrability(tokens, pos, tc);
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
                parseConstraintDeferrability(tokens, pos, tc);
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
                parseCreateTableLikeOptions(tokens, pos, lc);
                if (!lc.tableName.empty()) {
                    stmt->likeClauses.push_back(lc);
                    stmt->likeTables.emplace_back(lc.tableName, ColumnDef());
                }
            } else {
                // Column definition
                ColumnDef col;
                col.name = parseRoutineIdentifier(tokens[pos++]);
                if (pos < tokens.size()) {
                    const auto type = consumeDeclaredType(tokens, pos);
                    col.typeName = type.typeName;
                    col.typeMods = type.typeMods;
                    col.isArray = type.isArray;
                }
                // Column constraints
                std::string pendingCheckName;
                while (pos < tokens.size() && tokens[pos] != "," && tokens[pos] != ")") {
                    std::string ckw = toLower(tokens[pos]);
                    if (ckw == "constraint" && pos + 2 < tokens.size() &&
                        toLower(tokens[pos + 2]) == "check") {
                        pendingCheckName = parseRoutineIdentifier(tokens[pos + 1]);
                        pos += 2;
                    } else if (ckw == "not" && pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "null") {
                        col.isNull = false;
                        col.constraints.push_back("NOT NULL");
                        pos += 2;
                    } else if (ckw == "null") {
                        col.isNull = true;
                        col.constraints.push_back("NULL");
                        ++pos;
                    } else if (ckw == "primary") {
                        ++pos;
                        if (pos < tokens.size() && toLower(tokens[pos]) == "key") ++pos;
                        col.isPrimaryKey = true;
                        col.isNull = false;
                    } else if (ckw == "unique") {
                        ++pos;
                        col.isUnique = true;
                    } else if (ckw == "auto_increment") {
                        ++pos;
                        col.isAutoIncrementExtension = true;
                        col.constraints.push_back("AUTO_INCREMENT");
                    } else if (ckw == "unsigned") {
                        ++pos;
                        col.isUnsignedExtension = true;
                        col.constraints.push_back("UNSIGNED");
                    } else if (ckw == "default") {
                        ++pos;
                        const size_t defaultBegin=pos;
                        std::string defVal;
                        // Collect default value (literal, expression, or function call).
                        // Respect parenthesis depth so that DEFAULT nextval('s') or
                        // DEFAULT (expr) does not terminate early at an inner ')'.
                        int depth = 0, caseDepth = 0;
                        std::vector<std::string> delimiters;
                        while (pos < tokens.size()) {
                            if (depth == 0 && caseDepth == 0 && !defVal.empty()) {
                                const std::string next = toLower(tokens[pos]);
                                if (next == "not" || next == "null" ||
                                    next == "primary" || next == "unique" ||
                                    next == "check" || next == "generated" ||
                                    next == "collate" || next == "references" ||
                                    next == "constraint") {
                                    break;
                                }
                            }
                            const auto word=toLower(tokens[pos]);
                            if(word=="case")++caseDepth;
                            else if(word=="end" && caseDepth)--caseDepth;
                            if (tokens[pos] == "(" || tokens[pos] == "[") {
                                delimiters.push_back(tokens[pos]);
                                ++depth;
                            } else if (tokens[pos] == ")" || tokens[pos] == "]") {
                                if (depth == 0) break;
                                if((tokens[pos]==")" && delimiters.back()!="(") ||
                                   (tokens[pos]=="]" && delimiters.back()!="["))return nullptr;
                                delimiters.pop_back();
                                --depth;
                            } else if (tokens[pos] == "," && depth == 0 && caseDepth == 0) {
                                break;
                            }
                            if (!defVal.empty() && defVal.back() != '(') defVal += " ";
                            defVal += tokens[pos++];
                        }
                        if(defVal.empty() || depth || caseDepth)return nullptr;
                        col.defaultValue = std::make_unique<LiteralExpr>();
                        markSource(col.defaultValue.get(),tokens,defaultBegin,pos);
                        if(bindingParse && col.defaultValue->sourceBegin!=std::string::npos)
                            defVal=bindingParse->source.substr(col.defaultValue->sourceBegin,
                                col.defaultValue->sourceEnd-col.defaultValue->sourceBegin);
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
                            col.checkNames.push_back(pendingCheckName);
                            pendingCheckName.clear();
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
                                    col.identityKind = 'a';
                                    col.constraints.push_back("GENERATED ALWAYS AS IDENTITY");
                                    if (pos < tokens.size() && tokens[pos] == "(") {
                                        (void)collectParenthesized(tokens, pos);
                                        col.hasIdentityOptions = true;
                                    }
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
                                        col.identityKind = 'd';
                                        col.constraints.push_back("GENERATED BY DEFAULT AS IDENTITY");
                                        if (pos < tokens.size() && tokens[pos] == "(") {
                                            (void)collectParenthesized(tokens, pos);
                                            col.hasIdentityOptions = true;
                                        }
                                    }
                                }
                            }
                        }
                    } else if (ckw == "collate") {
                        ++pos;
                        if (pos >= tokens.size() || tokens[pos] == "," ||
                            tokens[pos] == ")" || tokens[pos] == ";") {
                            return nullptr;
                        }
                        col.collation = tokens[pos++];
                        if (pos < tokens.size() && tokens[pos] == ".") {
                            if (pos + 1 >= tokens.size() ||
                                tokens[pos + 1] == "," ||
                                tokens[pos + 1] == ")" ||
                                tokens[pos + 1] == ";") {
                                return nullptr;
                            }
                            col.collation += "." + tokens[pos + 1];
                            pos += 2;
                            if (pos < tokens.size() && tokens[pos] == ".") {
                                return nullptr;
                            }
                        }
                    } else if (ckw == "references") {
                        ++pos;
                        if (pos < tokens.size()) {
                            std::string refTable;
                            if (!parseForeignKeyTarget(
                                    tokens, pos, refTable)) return nullptr;
                            std::string refCol;
                            if (pos < tokens.size() && tokens[pos] == "(") {
                                auto refcols = collectParenthesized(tokens, pos);
                                if (!refcols.empty()) refCol = parseRoutineIdentifier(refcols[0]);
                            }
                            // Store as a simple foreign key constraint on this column
                            TableConstraint tc;
                            tc.type = "FOREIGN KEY";
                            tc.columns.push_back(col.name);
                            tc.refTable = refTable;
                            if (!refCol.empty()) tc.refColumns.push_back(refCol);
                            parseReferentialActions(tokens, pos, tc);
                            parseConstraintDeferrability(tokens, pos, tc);
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
            if (pos >= tokens.size() || tokens[pos] != "(") return nullptr;
            auto parents = collectParenthesized(tokens, pos);
            for (const auto& p : parents) {
                if (p != ",") stmt->inherits.push_back(p);
            }
            if (stmt->inherits.empty()) return nullptr;
        } else if (kw == "partition") {
            ++pos;
            if (pos + 2 >= tokens.size() || toLower(tokens[pos]) != "by") {
                return nullptr;
            }
            ++pos;
            const std::string ptype = toLower(tokens[pos++]);
            if (ptype != "range" && ptype != "list" && ptype != "hash") {
                return nullptr;
            }
            if (tokens[pos] != "(") return nullptr;
            auto pcols = collectParenthesized(tokens, pos);
            stmt->partitionType = ptype;
            for (const auto& c : pcols) {
                if (c != ",") {
                    SelectItem si;
                    si.expr = std::make_unique<ColumnRefExpr>();
                    static_cast<ColumnRefExpr*>(si.expr.get())->column = c;
                    stmt->partitionBy.push_back(std::move(si));
                }
            }
            if (stmt->partitionBy.empty()) return nullptr;
        } else if (kw == "with") {
            ++pos;
            if (pos >= tokens.size() || tokens[pos] != "(") return nullptr;
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
        } else if (kw == "without") {
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "oids") {
                stmt->options["oids"] = "false";
                ++pos;
            }
        } else if (kw == "tablespace") {
            ++pos;
            if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
            stmt->tablespace = tokens[pos++];
        } else if (kw == "of") {
            ++pos;
            if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
            stmt->ofType = tokens[pos++];
        } else if (kw == "like") {
            ++pos;
            if (pos < tokens.size()) {
                CreateTableStmt::LikeClause lc;
                lc.tableName = tokens[pos++];
                if (pos < tokens.size() && tokens[pos] == ".") {
                    ++pos;
                    if (pos < tokens.size()) lc.tableName += "." + tokens[pos++];
                }
                parseCreateTableLikeOptions(tokens, pos, lc);
                stmt->likeClauses.push_back(lc);
                stmt->likeTables.emplace_back(lc.tableName, ColumnDef());
            }
        } else if (kw == "on" && pos + 1 < tokens.size() &&
                   toLower(tokens[pos + 1]) == "commit") {
            parseCreateTableOnCommit(tokens, pos, *stmt);
        } else {
            // An unknown table option is a syntax error, not an instruction
            // to create the table while silently discarding SQL text.
            return nullptr;
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
    if (pos < tokens.size() && !match(tokens, pos, "on")) {
        stmt->indexName = parseRoutineIdentifier(tokens[pos++]);
        if (pos < tokens.size() && tokens[pos] == ".") {
            // PostgreSQL always creates an index in its parent table's
            // namespace and does not permit a schema-qualified index name.
            // Reject this instead of silently discarding the qualifier and
            // creating a differently named object.
            return nullptr;
        }
    }
    if (pos < tokens.size() && match(tokens, pos, "on")) ++pos;
    if (pos < tokens.size()) {
        stmt->tableName = tokens[pos++];
        if (pos + 1 < tokens.size() && tokens[pos] == ".") {
            stmt->tableName += "." + tokens[pos + 1];
            pos += 2;
        }
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
                elem.column = parseRoutineIdentifier(tokens[pos++]);
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
        } else if (kw == "nulls" && pos + 1 < tokens.size()) {
            if (toLower(tokens[pos + 1]) == "not" &&
                pos + 2 < tokens.size() &&
                toLower(tokens[pos + 2]) == "distinct") {
                stmt->nullsNotDistinct = true;
                pos += 3;
            } else if (toLower(tokens[pos + 1]) == "distinct") {
                pos += 2;
            } else {
                ++pos;
            }
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
            // Do not create an index after silently ignoring an unknown
            // trailing clause.
            return nullptr;
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
            if (pos >= tokens.size() || tokens[pos] == ";" ||
                match(tokens, pos, "as") || tokens[pos] == "(") {
                return nullptr;
            }
            stmt->viewName += "." + tokens[pos++];
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
        std::string selectSql = joinSqlTokens(sel);
        stmt->selectSql = selectSql;
        stmt->query = parseSelect(selectSql).stmt;
        pos = tokens.size();
    }
    return stmt;
}

StmtPtr SQLParser::parseCreateDatabase(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateDatabaseStmt>();

    const auto decodeQuotedIdentifier = [](const std::string& token,
                                           std::string& decoded) {
        if (token.size() < 2 || token.front() != '"' ||
            token.back() != '"') {
            return false;
        }
        decoded.clear();
        const std::string inner = token.substr(1, token.size() - 2);
        for (size_t i = 0; i < inner.size(); ++i) {
            decoded += inner[i];
            if (inner[i] == '"' && i + 1 < inner.size() &&
                inner[i + 1] == '"') {
                ++i;
            }
        }
        return true;
    };
    const auto isUnquotedIdentifier = [](const std::string& token) {
        if (token.empty()) return false;
        const unsigned char first = static_cast<unsigned char>(token.front());
        if (!(std::isalpha(first) || first == '_' || first >= 0x80)) {
            return false;
        }
        for (size_t i = 1; i < token.size(); ++i) {
            const unsigned char c = static_cast<unsigned char>(token[i]);
            if (!(std::isalnum(c) || c == '_' || c == '$' || c >= 0x80)) {
                return false;
            }
        }
        return true;
    };
    const auto isNumericOnly = [](const std::string& token) {
        size_t i = 0;
        if (i < token.size() && (token[i] == '+' || token[i] == '-')) ++i;
        bool beforeDecimal = false;
        while (i < token.size() &&
               std::isdigit(static_cast<unsigned char>(token[i]))) {
            beforeDecimal = true;
            ++i;
        }
        bool afterDecimal = false;
        if (i < token.size() && token[i] == '.') {
            ++i;
            while (i < token.size() &&
                   std::isdigit(static_cast<unsigned char>(token[i]))) {
                afterDecimal = true;
                ++i;
            }
        }
        if (!beforeDecimal && !afterDecimal) return false;
        if (i < token.size() && (token[i] == 'e' || token[i] == 'E')) {
            ++i;
            if (i < token.size() && (token[i] == '+' || token[i] == '-')) ++i;
            const size_t exponentStart = i;
            while (i < token.size() &&
                   std::isdigit(static_cast<unsigned char>(token[i]))) ++i;
            if (i == exponentStart) return false;
        }
        return i == token.size();
    };
    static const std::set<std::string> reservedNameKeywords = {
        // PostgreSQL 18's RESERVED_KEYWORD and TYPE_FUNC_NAME_KEYWORD
        // categories cannot reduce to the `name`/ColId production.
        "all", "analyse", "analyze", "and", "any", "array", "as", "asc",
        "asymmetric", "authorization", "binary", "both", "case", "cast",
        "check", "collate", "collation", "column", "concurrently",
        "constraint", "create", "cross", "current_catalog", "current_date",
        "current_role", "current_schema", "current_time", "current_timestamp",
        "current_user", "default", "deferrable", "desc", "distinct", "do",
        "else", "end", "except", "false", "fetch", "for", "foreign",
        "freeze", "from", "full", "grant", "group", "having", "ilike", "in",
        "initially", "inner", "intersect", "into", "is", "isnull", "join",
        "lateral", "leading", "left", "like", "limit", "localtime",
        "localtimestamp", "natural", "not", "notnull", "null", "offset", "on",
        "only", "or", "order", "outer", "overlaps", "placing", "primary",
        "references", "returning", "right", "select", "session_user", "similar",
        "some", "symmetric", "system_user", "table", "tablesample", "then", "to",
        "trailing", "true", "union", "unique", "user", "using", "variadic",
        "verbose", "when", "where", "window", "with"
    };
    static const std::set<std::string> typeFunctionNameKeywords = {
        "authorization", "binary", "collation", "concurrently", "cross",
        "current_schema", "freeze", "full", "ilike", "inner", "is",
        "isnull", "join", "left", "like", "natural", "notnull", "outer",
        "overlaps", "right", "similar", "tablesample", "verbose"
    };

    // PostgreSQL has no CREATE DATABASE IF NOT EXISTS form.  Database names
    // are also never schema-qualified.
    if (pos >= tokens.size() || tokens[pos] == ";") {
        return nullptr;
    }
    const std::string nameToken = tokens[pos++];
    if (decodeQuotedIdentifier(nameToken, stmt->databaseName)) {
        // Quoted identifiers retain case and may spell a keyword.
    } else {
        if (!isUnquotedIdentifier(nameToken) ||
            reservedNameKeywords.count(toLower(nameToken)) != 0) {
            return nullptr;
        }
        stmt->databaseName = toLower(nameToken);
    }
    if (stmt->databaseName.empty()) return nullptr;

    if (pos < tokens.size() && match(tokens, pos, "with")) {
        stmt->withClause = true;
        ++pos;
    }

    static const std::set<std::string> optionNames = {
        "owner", "template", "encoding", "strategy", "locale",
        "lc_collate", "lc_ctype", "builtin_locale", "icu_locale",
        "icu_rules", "locale_provider", "collation_version",
        "tablespace", "allow_connections", "is_template", "oid",
        "connection_limit",
        // Kept by PostgreSQL 18's grammar for compatibility; PostgreSQL
        // itself warns that LOCATION is no longer supported.
        "location"
    };
    const auto punctuation = [](const std::string& token) {
        static const std::set<std::string> invalid = {
            ".", ",", "(", ")", "[", "]", "*", "/", "%", "^",
            "~", "!", "|", "&", "#", "@", "?", ":", "<", ">",
            "=>", "::"
        };
        return invalid.count(token) != 0;
    };

    while (pos < tokens.size() && tokens[pos] != ";") {
        const std::string optionToken = tokens[pos++];
        std::string option;
        const bool quotedOption = decodeQuotedIdentifier(optionToken, option);
        if (!quotedOption) {
            if (!isUnquotedIdentifier(optionToken)) return nullptr;
            option = toLower(optionToken);
        }
        if (!quotedOption && option == "connection") {
            if (pos >= tokens.size() || !match(tokens, pos, "limit")) {
                return nullptr;
            }
            ++pos;
            option = "connection_limit";
        } else if (optionNames.count(option) == 0) {
            return nullptr;
        }
        if (stmt->options.count(option) != 0) return nullptr;

        if (pos < tokens.size() && tokens[pos] == "=") ++pos;
        if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
        if (match(tokens, pos, "default")) {
            stmt->options.emplace(option, std::nullopt);
            ++pos;
            continue;
        }

        std::string value = tokens[pos++];
        if ((value == "+" || value == "-") && pos < tokens.size()) {
            const std::string& magnitude = tokens[pos];
            if (!isNumericOnly(magnitude)) {
                return nullptr;
            }
            value += magnitude;
            ++pos;
        }
        std::string ignoredQuotedIdentifier;
        const bool singleQuoted = value.size() >= 2 &&
            value.front() == '\'' && value.back() == '\'';
        const std::string loweredValue = toLower(value);
        const bool validValue = singleQuoted ||
            decodeQuotedIdentifier(value, ignoredQuotedIdentifier) ||
            isNumericOnly(value) ||
            (isUnquotedIdentifier(value) &&
             (loweredValue == "true" || loweredValue == "false" ||
              reservedNameKeywords.count(loweredValue) == 0 ||
              typeFunctionNameKeywords.count(loweredValue) != 0));
        if (!validValue || value == "+" || value == "-" || value == "=" ||
            punctuation(value)) {
            return nullptr;
        }
        stmt->options.emplace(option, std::move(value));
    }

    if (pos < tokens.size() && tokens[pos] == ";") ++pos;
    if (pos != tokens.size()) return nullptr;
    return stmt;
}

StmtPtr SQLParser::parseCreateSchema(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateObjectStmt>(SqlCommand::CreateSchema);
    stmt->objectType = "SCHEMA";
    if (pos + 2 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "not") && match(tokens, pos + 2, "exists")) {
        stmt->ifNotExists = true; pos += 3;
    }
    if (pos < tokens.size()) {
        stmt->objectName = parseRoutineIdentifier(tokens[pos++]);
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = parseRoutineIdentifier(tokens[pos++]);
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
        const size_t valuePosition = pos + skip;
        std::string valueToken = tokens[valuePosition];
        size_t consumed = 1;
        if ((valueToken == "+" || valueToken == "-") &&
            valuePosition + 1 < tokens.size() &&
            tokens[valuePosition + 1] != ";") {
            valueToken += tokens[valuePosition + 1];
            consumed = 2;
        }
        int64_t ignored = 0;
        if (!parseInt64Token(valueToken, ignored)) return false;
        stmt->options[key] = std::move(valueToken);
        pos = valuePosition + consumed;
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
                    } else if (pos + 6 < tokens.size() &&
                               tokens[pos + 3] == "." &&
                               tokens[pos + 5] == ".") {
                        stmt->options["ownedby"] =
                            owner + "." + tokens[pos + 4] + "." +
                            tokens[pos + 6];
                        pos += 7;
                    } else if (pos + 4 < tokens.size() &&
                               tokens[pos + 3] == ".") {
                        stmt->options["ownedby"] = owner + "." + tokens[pos + 4];
                        pos += 5;
                    } else {
                        return nullptr;
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
    }
    const size_t typeBegin = pos;
    consumeDeclaredType(tokens, pos);
    std::string declaredType;
    for (size_t i = typeBegin; i < pos; ++i) {
        if (!declaredType.empty()) declaredType += ' ';
        declaredType += tokens[i];
    }
    stmt->options["base_type"] = declaredType;

    auto lower = [&](const std::string& s) { return toLower(s); };
    std::string constraintName;
    while (pos < tokens.size() && tokens[pos] != ";") {
        std::string tok = lower(tokens[pos]);
        if (tok == "default") {
            ++pos;
            std::string expr;
            int depth = 0;
            while (pos < tokens.size() && tokens[pos] != ";" &&
                   !(depth == 0 && !expr.empty() &&
                     (lower(tokens[pos]) == "constraint" || lower(tokens[pos]) == "check" ||
                      lower(tokens[pos]) == "not" || lower(tokens[pos]) == "null"))) {
                if (tokens[pos] == "(") ++depth;
                if (tokens[pos] == ")") --depth;
                if (!expr.empty()) expr += " ";
                expr += tokens[pos++];
            }
            if (expr.empty() || depth != 0 || stmt->options.count("default"))
                throw DbError("42601", "invalid or duplicate domain DEFAULT");
            stmt->options["default"] = expr;
        } else if (tok == "not" && pos + 1 < tokens.size() && lower(tokens[pos + 1]) == "null") {
            if (stmt->options.count("nullable")) throw DbError("42601", "conflicting domain NULL declarations");
            stmt->options["not_null"] = "true";
            pos += 2;
        } else if (tok == "null") {
            if (stmt->options.count("not_null")) throw DbError("42601", "conflicting domain NULL declarations");
            stmt->options["nullable"] = "true";
            ++pos;
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
                if (depth != 0 || expr.empty())
                    throw DbError("42601", "invalid domain CHECK expression");
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
                throw DbError("42601", "domain CHECK requires parentheses");
            }
        } else {
            throw DbError("42601", "unexpected domain declaration token: " + tokens[pos]);
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
        stmt->objectName = parseRoutineIdentifier(tokens[pos++]);
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos < tokens.size()) {
                stmt->schema = stmt->objectName;
                stmt->objectName = parseRoutineIdentifier(tokens[pos++]);
            }
        }
    }

    // CREATE TYPE name AS ENUM ('a', 'b', ...)
    if (pos + 2 < tokens.size() && toLower(tokens[pos]) == "as" && toLower(tokens[pos + 1]) == "enum") {
        pos += 2;
        stmt->options["type_kind"] = "enum";
        if (pos < tokens.size() && tokens[pos] == "(") {
            ++pos;
            while (pos < tokens.size() && tokens[pos] != ")") {
                if (tokens[pos] == ",") {
                    ++pos;
                    continue;
                }
                stmt->enumLabels.push_back(stripQuotes(tokens[pos++]));
            }
            if (pos < tokens.size() && tokens[pos] == ")") ++pos;
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
    std::string declared;
    if (!parseQualifiedObjectName(tokens, pos, declared)) return nullptr;
    CatalogManager::QualifiedName routine;
    if (!CatalogManager::parseQualifiedName(declared, routine, true)) return nullptr;
    stmt->schema = routine.schema;
    stmt->funcName = routine.name;

    // Optional parameter list: ([name] type [, ...]). A whole declared type
    // has no argument name; use the existing type grammar so multiword types,
    // modifiers and array suffixes are not mistaken for names or separators.
    if (pos < tokens.size() && tokens[pos] == "(") {
        ++pos;
        while (pos < tokens.size() && tokens[pos] != ")") {
            if (tokens[pos] == ",") return nullptr;
            const size_t begin = pos;
            const auto boundary = [&](size_t end) {
                return end < tokens.size() && (tokens[end] == "," || tokens[end] == ")");
            };
            std::string pname;
            std::string ptype;
            size_t typeEnd = begin;
            try {
                const auto type = consumeDeclaredType(tokens, typeEnd);
                if (boundary(typeEnd)) ptype = renderDeclaredType(type);
            } catch (const DbError&) {
                // The first token can instead be the argument identifier.
            }
            if (ptype.empty()) {
                pname = parseRoutineIdentifier(tokens[pos++]);
                if (pname.empty()) return nullptr;
                try {
                    const auto type = consumeDeclaredType(tokens, pos);
                    if (!boundary(pos)) return nullptr;
                    ptype = renderDeclaredType(type);
                } catch (const DbError&) { return nullptr; }
            } else {
                pos = typeEnd;
            }
            stmt->params.emplace_back(pname, ptype);
            if (tokens[pos] == ",") {
                ++pos;
                if (pos >= tokens.size() || tokens[pos] == ")") return nullptr;
            }
        }
        if (pos >= tokens.size() || tokens[pos] != ")") return nullptr;
        ++pos;
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
                   !match(tokens, pos, "strict") && !match(tokens, pos, "returns") &&
                   !match(tokens, pos, "called") && !match(tokens, pos, "security") &&
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
        else if (match(tokens, pos, "strict")) {
            stmt->strict = true;
            ++pos;
        }
        else if (match(tokens, pos, "returns") && pos + 4 < tokens.size() &&
                 match(tokens, pos + 1, "null") &&
                 match(tokens, pos + 2, "on") &&
                 match(tokens, pos + 3, "null") &&
                 match(tokens, pos + 4, "input")) {
            stmt->strict = true;
            pos += 5;
        }
        else if (match(tokens, pos, "called") && pos + 3 < tokens.size() &&
                 match(tokens, pos + 1, "on") &&
                 match(tokens, pos + 2, "null") &&
                 match(tokens, pos + 3, "input")) {
            stmt->strict = false;
            pos += 4;
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

    // LANGUAGE lang and AS body in either order (PG commonly writes
    // LANGUAGE plpgsql AS $$...$$; dumps sometimes reverse them).
    for (int round = 0; round < 2 && pos < tokens.size(); ++round) {
        if (match(tokens, pos, "language") && pos + 1 < tokens.size()) {
            stmt->language = toLower(tokens[pos + 1]);
            pos += 2;
            continue;
        }
        if (match(tokens, pos, "as")) {
            ++pos;
            if (pos < tokens.size()) {
                stmt->body = stripQuotes(tokens[pos]);
                ++pos;
            }
            continue;
        }
        break;
    }

    // Never accept an option prefix while silently discarding the remaining
    // function definition. Unsupported option order/shape must fail closed.
    if (pos < tokens.size()) return nullptr;

    return stmt;
}

StmtPtr SQLParser::parseCreateProcedure(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<CreateFunctionStmt>(true);
    if (pos >= tokens.size()) return stmt;
    stmt->funcName = parseRoutineIdentifier(tokens[pos++]);

    if (pos < tokens.size() && tokens[pos] == "(") {
        ++pos;
        while (pos < tokens.size() && tokens[pos] != ")") {
            if (tokens[pos] == ",") { ++pos; continue; }
            std::string pname = parseRoutineIdentifier(tokens[pos++]);
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

    // PostgreSQL permits the AS and LANGUAGE clauses in either order.
    for (int round = 0; round < 2 && pos < tokens.size(); ++round) {
        if (match(tokens, pos, "language") && pos + 1 < tokens.size()) {
            stmt->language = toLower(tokens[pos + 1]);
            pos += 2;
            continue;
        }
        if (match(tokens, pos, "as") && pos + 1 < tokens.size()) {
            stmt->body = stripQuotes(tokens[pos + 1]);
            pos += 2;
            continue;
        }
        break;
    }
    if (pos < tokens.size()) return nullptr;
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

    // REFERENCING {NEW TABLE [AS] name | OLD TABLE [AS] name} [...]
    // Transition tables: statement-level row sets exposed to the trigger
    // action.  Stored as "new <name>" / "old <name>" entries.
    while (match(tokens, pos, "referencing")) {
        ++pos;
        bool parsedAny = false;
        while (pos + 1 < tokens.size() && match(tokens, pos, "new") &&
               match(tokens, pos + 1, "table")) {
            pos += 2;
            if (match(tokens, pos, "as")) ++pos;
            if (pos < tokens.size()) {
                stmt->transitionTableNames.push_back("new " + tokens[pos++]);
                parsedAny = true;
            }
        }
        while (pos + 1 < tokens.size() && match(tokens, pos, "old") &&
               match(tokens, pos + 1, "table")) {
            pos += 2;
            if (match(tokens, pos, "as")) ++pos;
            if (pos < tokens.size()) {
                stmt->transitionTableNames.push_back("old " + tokens[pos++]);
                parsedAny = true;
            }
        }
        if (!parsedAny) break;  // malformed; leave for later error handling
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
        // Remaining tokens form the action SQL (legacy style).  Qualified
        // references (NEW.col / OLD.col) must survive storage with their
        // dots intact, so reuse the collapsing join.
        std::vector<std::string> rest(tokens.begin() + pos, tokens.end());
        stmt->action = joinSqlTokens(rest);
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
            size_t valuePosition = pos + 2;
            if (!parseSignedInteger(tokens, valuePosition,
                                    stmt->connectionLimit) ||
                stmt->connectionLimit < -1) {
                return nullptr;
            }
            pos = valuePosition;
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
    if (pos >= tokens.size() || tokens[pos] == ";") return stmt;

    if (toLower(tokens[pos]) == "from") {
        ++pos;
        if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
        std::string source = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
            source += "." + tokens[pos++];
        }
        if (pos < tokens.size() && tokens[pos] != ";") return nullptr;
        stmt->options["source"] = source;
        return stmt;
    }

    if (tokens[pos] != "(") return nullptr;
    ++pos;
    bool sawOption = false;
    while (pos < tokens.size() && tokens[pos] != ";") {
        if (tokens[pos] == ")") {
            if (!sawOption) return nullptr;
            ++pos;
            if (pos < tokens.size() && tokens[pos] != ";") return nullptr;
            return stmt;
        }
        const std::string key = toLower(tokens[pos++]);
        if (key != "provider" && key != "locale" &&
            key != "lc_collate" && key != "lc_ctype" &&
            key != "deterministic" && key != "rules" &&
            key != "version") {
            return nullptr;
        }
        if (stmt->options.count(key) != 0 || pos >= tokens.size() ||
            tokens[pos] != "=") {
            return nullptr;
        }
        ++pos;
        if (pos >= tokens.size() || tokens[pos] == "," ||
            tokens[pos] == ")" || tokens[pos] == ";") {
            return nullptr;
        }
        stmt->options[key] = stripQuotes(tokens[pos++]);
        sawOption = true;
        if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
        if (tokens[pos] == ")") continue;
        if (tokens[pos] != ",") return nullptr;
        ++pos;
        if (pos >= tokens.size() || tokens[pos] == ")" ||
            tokens[pos] == ";") {
            return nullptr;
        }
    }
    return nullptr;
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
    if (pos + 1 < tokens.size() && toLower(tokens[pos]) == "if" &&
        toLower(tokens[pos + 1]) == "exists") {
        stmt->ifExists = true;
        pos += 2;
    }
    const auto identifierToken = [](const std::string& token) {
        if (token.size() >= 2 && token.front() == '"' && token.back() == '"')
            return true;
        if (token.empty()) return false;
        const auto first = static_cast<unsigned char>(token.front());
        if (!(std::isalpha(first) || first == '_' || first >= 128)) return false;
        for (const unsigned char ch : token)
            if (!(std::isalnum(ch) || ch == '_' || ch == '$' || ch >= 128))
                return false;
        return true;
    };
    while (true) {
        if (pos >= tokens.size() || !identifierToken(tokens[pos])) return nullptr;
        std::string name = tokens[pos++];
        while (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos >= tokens.size() || !identifierToken(tokens[pos])) return nullptr;
            name += "." + tokens[pos++];
        }
        stmt->objectNames.push_back(std::move(name));
        if (pos >= tokens.size() || tokens[pos] == ";") break;
        if (tokens[pos] == ",") {
            ++pos;
            continue;
        }
        const std::string behavior = toLower(tokens[pos]);
        if (behavior != "cascade" && behavior != "restrict") return nullptr;
        stmt->cascade = behavior == "cascade";
        ++pos;
        if (pos < tokens.size() && tokens[pos] != ";") return nullptr;
        break;
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
        std::string name = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
            name += "." + tokens[pos++];
        }
        stmt->objectNames.push_back(std::move(name));
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
        std::string name = tokens[pos++];
        if (pos < tokens.size() && tokens[pos] == ".") {
            ++pos;
            if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
            name += "." + tokens[pos++];
        }
        stmt->objectNames.push_back(std::move(name));
    }
    return stmt;
}

StmtPtr SQLParser::parseDropDatabase(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropDatabase);
    stmt->objectType = "DATABASE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") &&
        match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true;
        pos += 2;
    }
    if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
    stmt->objectNames.push_back(tokens[pos++]);
    if (pos < tokens.size() && tokens[pos] == ";") ++pos;
    // Unsupported options must not be silently discarded after the target.
    if (pos != tokens.size()) return nullptr;
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
        stmt->objectNames.push_back(parseRoutineIdentifier(tokens[pos++]));
    }
    return stmt;
}

StmtPtr SQLParser::parseDropSequence(const std::vector<std::string>& tokens, size_t& pos) {
    auto stmt = std::make_unique<DropStmt>(SqlCommand::DropSequence);
    stmt->objectType = "SEQUENCE";
    if (pos + 1 < tokens.size() && match(tokens, pos, "if") && match(tokens, pos + 1, "exists")) {
        stmt->ifExists = true; pos += 2;
    }
    bool expectName = true;
    bool behaviorSeen = false;
    while (pos < tokens.size() && tokens[pos] != ";") {
        const std::string word = toLower(tokens[pos]);
        if (!expectName && (word == "cascade" || word == "restrict")) {
            if (behaviorSeen) return nullptr;
            behaviorSeen = true;
            stmt->cascade = word == "cascade";
            ++pos;
            if (pos < tokens.size() && tokens[pos] != ";") return nullptr;
            break;
        }
        if (expectName) {
            if (tokens[pos] == "," || word == "cascade" ||
                word == "restrict") return nullptr;
            std::string name = tokens[pos++];
            if (pos < tokens.size() && tokens[pos] == ".") {
                ++pos;
                if (pos >= tokens.size() || tokens[pos] == ";" ||
                    tokens[pos] == ",") return nullptr;
                name += "." + tokens[pos++];
            }
            stmt->objectNames.push_back(std::move(name));
            expectName = false;
            continue;
        }
        if (tokens[pos] != ",") return nullptr;
        ++pos;
        expectName = true;
    }
    if (expectName && !stmt->objectNames.empty()) return nullptr;
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
    bool expectName = true;
    bool behaviorSeen = false;
    while (pos < tokens.size() && tokens[pos] != ";") {
        const std::string word = toLower(tokens[pos]);
        if (!expectName && (word == "cascade" || word == "restrict")) {
            if (behaviorSeen) return nullptr;
            behaviorSeen = true;
            stmt->cascade = word == "cascade";
            ++pos;
            if (pos < tokens.size() && tokens[pos] != ";") return nullptr;
            break;
        }
        if (expectName) {
            if (tokens[pos] == "," || word == "cascade" ||
                word == "restrict") return nullptr;
            std::string name = tokens[pos++];
            if (pos < tokens.size() && tokens[pos] == ".") {
                ++pos;
                if (pos >= tokens.size() || tokens[pos] == ";" ||
                    tokens[pos] == ",") return nullptr;
                name += "." + tokens[pos++];
            }
            stmt->objectNames.push_back(std::move(name));
            expectName = false;
            continue;
        }
        if (tokens[pos] != ",") return nullptr;
        ++pos;
        expectName = true;
    }
    if (expectName && !stmt->objectNames.empty()) return nullptr;
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
    if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
    stmt->objectNames.push_back(tokens[pos++]);
    if (pos >= tokens.size() || !match(tokens, pos, "on")) return nullptr;
    ++pos;
    if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
    stmt->tableName = tokens[pos++];
    if (pos < tokens.size() && tokens[pos] == ".") {
        ++pos;
        if (pos >= tokens.size() || tokens[pos] == ";") return nullptr;
        stmt->tableName += "." + tokens[pos++];
    }
    bool behaviorSpecified = false;
    while (pos < tokens.size() && tokens[pos] != ";") {
        const std::string option = toLower(tokens[pos++]);
        if (option != "cascade" && option != "restrict") return nullptr;
        if (behaviorSpecified) return nullptr;
        behaviorSpecified = true;
        stmt->cascade = option == "cascade";
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
    bool expectName = true;
    bool behaviorSeen = false;
    while (pos < tokens.size() && tokens[pos] != ";") {
        const std::string word = toLower(tokens[pos]);
        if (!expectName && (word == "cascade" || word == "restrict")) {
            if (behaviorSeen) return nullptr;
            behaviorSeen = true;
            stmt->cascade = word == "cascade";
            ++pos;
            if (pos < tokens.size() && tokens[pos] != ";") return nullptr;
            break;
        }
        if (expectName) {
            if (tokens[pos] == "," || word == "cascade" ||
                word == "restrict") return nullptr;
            std::string name = tokens[pos++];
            if (pos < tokens.size() && tokens[pos] == ".") {
                ++pos;
                if (pos >= tokens.size() || tokens[pos] == ";" ||
                    tokens[pos] == ",") return nullptr;
                name += "." + tokens[pos++];
            }
            stmt->objectNames.push_back(std::move(name));
            expectName = false;
            continue;
        }
        if (tokens[pos] != ",") return nullptr;
        ++pos;
        expectName = true;
    }
    if (expectName && !stmt->objectNames.empty()) return nullptr;
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
    const auto columnType = [&](std::string& type) {
        try {
            const size_t begin = pos;
            (void)consumeDeclaredType(tokens, pos);
            // Validate with the shared grammar, but retain the complete
            // declaration envelope. Its semantic isArray bit cannot retain
            // repeated [] or explicit bounds from the original token stream.
            type.clear();
            for (size_t i = begin; i < pos; ++i) {
                const auto& token = tokens[i];
                const bool punctuation = token == "." || token == "(" || token == ")" ||
                    token == "[" || token == "]" || token == ",";
                const bool adjacent = i == begin || tokens[i - 1] == "." ||
                    tokens[i - 1] == "(" || tokens[i - 1] == "[" ||
                    tokens[i - 1] == "," || tokens[i - 1] == "+" || tokens[i - 1] == "-";
                if (!punctuation && !adjacent) type += ' ';
                type += token;
            }
            return true;
        } catch (const DbError& error) {
            if (error.sqlState() != "42601") throw;
            retainDeclarationSyntaxError(error);
            return false;
        }
    };
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
            if (pos < tokens.size()) {
                sub.colDef.name = parseRoutineIdentifier(tokens[pos++]);
            }
            if (pos < tokens.size()) {
                const auto type = consumeDeclaredType(tokens, pos);
                sub.colDef.typeName = type.typeName;
                sub.colDef.typeMods = type.typeMods;
                sub.colDef.isArray = type.isArray;
            }
            // Preserve every column modifier the storage layer supports.  An
            // unknown modifier must fail the parse: reporting success after
            // silently dropping a constraint changes the declared schema.
            std::string pendingCheckName;
            while (pos < tokens.size() && tokens[pos] != "," && tokens[pos] != ";") {
                std::string modifier = toLower(tokens[pos]);
                if (modifier == "not" && pos + 1 < tokens.size() &&
                    toLower(tokens[pos + 1]) == "null") {
                    sub.colDef.isNull = false;
                    sub.colDef.constraints.push_back("NOT NULL");
                    pos += 2;
                } else if (modifier == "null") {
                    sub.colDef.isNull = true;
                    sub.colDef.constraints.push_back("NULL");
                    ++pos;
                } else if (modifier == "primary" && pos + 1 < tokens.size() &&
                           toLower(tokens[pos + 1]) == "key") {
                    sub.colDef.isPrimaryKey = true;
                    sub.colDef.isNull = false;
                    pos += 2;
                } else if (modifier == "unique") {
                    sub.colDef.isUnique = true;
                    ++pos;
                } else if (modifier == "auto_increment") {
                    sub.colDef.isAutoIncrementExtension = true;
                    sub.colDef.constraints.push_back("AUTO_INCREMENT");
                    ++pos;
                } else if (modifier == "unsigned") {
                    sub.colDef.isUnsignedExtension = true;
                    sub.colDef.constraints.push_back("UNSIGNED");
                    ++pos;
                } else if (modifier == "default") {
                    ++pos;
                    sub.colDef.defaultValue = parseSimpleExpr(tokens, pos);
                    if (!sub.colDef.defaultValue) return nullptr;
                } else if (modifier == "constraint") {
                    if (pos + 2 >= tokens.size() ||
                        toLower(tokens[pos + 2]) != "check") {
                        return nullptr;
                    }
                    pendingCheckName = parseRoutineIdentifier(tokens[pos + 1]);
                    pos += 2;
                } else if (modifier == "check") {
                    ++pos;
                    if (pos >= tokens.size() || tokens[pos] != "(") {
                        return nullptr;
                    }
                    ++pos;
                    auto expression = parseSimpleExpr(tokens, pos);
                    if (!expression || pos >= tokens.size() ||
                        tokens[pos] != ")") {
                        return nullptr;
                    }
                    ++pos;
                    sub.colDef.checkExprs.push_back(std::move(expression));
                    sub.colDef.checkNames.push_back(pendingCheckName);
                    pendingCheckName.clear();
                } else if (modifier == "generated") {
                    ++pos;
                    bool always = false;
                    if (pos < tokens.size() &&
                        toLower(tokens[pos]) == "always") {
                        always = true;
                        ++pos;
                    } else if (pos + 1 < tokens.size() &&
                               toLower(tokens[pos]) == "by" &&
                               toLower(tokens[pos + 1]) == "default") {
                        pos += 2;
                    } else {
                        return nullptr;
                    }
                    if (pos >= tokens.size() ||
                        toLower(tokens[pos]) != "as") {
                        return nullptr;
                    }
                    ++pos;
                    if (pos < tokens.size() &&
                        toLower(tokens[pos]) == "identity") {
                        ++pos;
                        sub.colDef.isGeneratedIdentity = true;
                        sub.colDef.identityKind = always ? 'a' : 'd';
                        sub.colDef.constraints.push_back(
                            always ? "GENERATED ALWAYS AS IDENTITY"
                                   : "GENERATED BY DEFAULT AS IDENTITY");
                        if (pos < tokens.size() && tokens[pos] == "(") {
                            (void)collectParenthesized(tokens, pos);
                            sub.colDef.hasIdentityOptions = true;
                        }
                    } else if (always && pos < tokens.size() &&
                               tokens[pos] == "(") {
                        const auto expressionTokens =
                            collectParenthesized(tokens, pos);
                        if (expressionTokens.empty()) return nullptr;
                        std::string expression;
                        for (const auto& token : expressionTokens) {
                            if (!expression.empty() &&
                                expression.back() != '(') {
                                expression += ' ';
                            }
                            expression += token;
                        }
                        sub.colDef.generatedExpr = std::move(expression);
                        sub.colDef.generatedKind = 's';
                        if (pos < tokens.size()) {
                            const std::string kind = toLower(tokens[pos]);
                            if (kind == "stored") {
                                ++pos;
                            } else if (kind == "virtual") {
                                sub.colDef.generatedKind = 'v';
                                ++pos;
                            }
                        }
                    } else {
                        return nullptr;
                    }
                } else if (modifier == "collate") {
                    ++pos;
                    if (pos >= tokens.size() || tokens[pos] == "," ||
                        tokens[pos] == ";") {
                        return nullptr;
                    }
                    sub.colDef.collation = tokens[pos++];
                    if (pos < tokens.size() && tokens[pos] == ".") {
                        if (pos + 1 >= tokens.size() ||
                            tokens[pos + 1] == "," ||
                            tokens[pos + 1] == ";") {
                            return nullptr;
                        }
                        sub.colDef.collation += "." + tokens[pos + 1];
                        pos += 2;
                        if (pos < tokens.size() && tokens[pos] == ".") {
                            return nullptr;
                        }
                    }
                } else {
                    return nullptr;
                }
            }
            if (!pendingCheckName.empty()) return nullptr;
        } else if (kw == "add") {
            recognized = true;
            sub.action = AlterTableStmt::Action::AddConstraint; ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "constraint") {
                ++pos;
                if (pos < tokens.size()) {
                    sub.constraint.name = parseRoutineIdentifier(tokens[pos++]);
                }
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
                    for (const auto& c : cols) if (c != ",") sub.constraint.columns.push_back(parseRoutineIdentifier(c));
                }
            }
            if (pos < tokens.size() && toLower(tokens[pos]) == "references") {
                ++pos;
                if (!parseForeignKeyTarget(
                        tokens, pos, sub.constraint.refTable)) return nullptr;
                if (pos < tokens.size() && tokens[pos] == "(") {
                    auto refcols = collectParenthesized(tokens, pos);
                    for (const auto& c : refcols) if (c != ",") sub.constraint.refColumns.push_back(parseRoutineIdentifier(c));
                }
            }
            while (pos < tokens.size() && tokens[pos] != "," && tokens[pos] != ";") {
                const std::string option = toLower(tokens[pos]);
                if (option == "on" &&
                    sub.constraint.type == "FOREIGN KEY" &&
                    pos + 1 < tokens.size() &&
                    (toLower(tokens[pos + 1]) == "delete" ||
                     toLower(tokens[pos + 1]) == "update")) {
                    parseReferentialActions(tokens, pos, sub.constraint);
                } else if (option == "not" && pos + 1 < tokens.size() &&
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
            if (pos < tokens.size()) {
                sub.name = parseRoutineIdentifier(tokens[pos++]);
            }
            if (pos < tokens.size() && toLower(tokens[pos]) == "cascade") { sub.options["cascade"] = "true"; ++pos; }
        } else if (kw == "drop") {
            recognized = true;
            sub.action = AlterTableStmt::Action::DropConstraint; ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "constraint") ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "if") {
                sub.ifExists = true;
                pos += 2; // IF EXISTS
            }
            if (pos < tokens.size()) sub.name = parseRoutineIdentifier(tokens[pos++]);
            if (pos < tokens.size() && toLower(tokens[pos]) == "cascade") { sub.options["cascade"] = "true"; ++pos; }
        } else if (kw == "alter" && pos + 1 < tokens.size() && toLower(tokens[pos + 1]) == "column") {
            recognized = true;
            sub.action = AlterTableStmt::Action::AlterColumn; pos += 2;
            if (pos < tokens.size()) {
                sub.name = parseRoutineIdentifier(tokens[pos++]);
            }
            if (pos < tokens.size() && toLower(tokens[pos]) == "set") {
                ++pos;
                if (pos < tokens.size() && toLower(tokens[pos]) == "default") {
                    ++pos;
                    sub.defaultValue = parseSimpleExpr(tokens, pos);
                } else if (pos < tokens.size() && toLower(tokens[pos]) == "not") {
                    pos += 2; // NOT NULL
                    sub.setNotNull = true;
                } else if (pos < tokens.size() && toLower(tokens[pos]) == "data") {
                    if (pos + 1 >= tokens.size() || toLower(tokens[pos + 1]) != "type") return nullptr;
                    pos += 2;
                    if (!columnType(sub.dataType)) return nullptr;
                } else if (pos < tokens.size() && toLower(tokens[pos]) == "statistics") {
                    ++pos;
                    sub.action = AlterTableStmt::Action::SetStatistics;
                    if (pos < tokens.size()) {
                        int target = 0;
                        if (!parseSignedInteger(tokens, pos, target) ||
                            target < -1 || target > 10000) {
                            return nullptr;
                        }
                        sub.statisticsTarget = target;
                    } else return nullptr;
                } else if (pos < tokens.size() &&
                           toLower(tokens[pos]) == "generated") {
                    ++pos;
                    if (pos < tokens.size() &&
                        toLower(tokens[pos]) == "always") {
                        sub.identityKind = 'a';
                        ++pos;
                    } else if (pos + 1 < tokens.size() &&
                               toLower(tokens[pos]) == "by" &&
                               toLower(tokens[pos + 1]) == "default") {
                        sub.identityKind = 'd';
                        pos += 2;
                    } else {
                        return nullptr;
                    }
                    sub.identityAction = "set";
                }
            } else if (pos < tokens.size() && toLower(tokens[pos]) == "drop") {
                ++pos;
                if (pos < tokens.size() && toLower(tokens[pos]) == "default") { sub.dropDefault = true; ++pos; }
                else if (pos < tokens.size() && toLower(tokens[pos]) == "not") { sub.dropNotNull = true; pos += 2; }
                else if (pos < tokens.size() &&
                         toLower(tokens[pos]) == "identity") {
                    ++pos;
                    sub.identityAction = "drop";
                    if (pos + 1 < tokens.size() &&
                        toLower(tokens[pos]) == "if" &&
                        toLower(tokens[pos + 1]) == "exists") {
                        sub.identityIfExists = true;
                        pos += 2;
                    }
                }
            } else if (pos < tokens.size() &&
                       toLower(tokens[pos]) == "add") {
                ++pos;
                if (pos >= tokens.size() ||
                    toLower(tokens[pos]) != "generated") {
                    return nullptr;
                }
                ++pos;
                if (pos < tokens.size() &&
                    toLower(tokens[pos]) == "always") {
                    sub.identityKind = 'a';
                    ++pos;
                } else if (pos + 1 < tokens.size() &&
                           toLower(tokens[pos]) == "by" &&
                           toLower(tokens[pos + 1]) == "default") {
                    sub.identityKind = 'd';
                    pos += 2;
                } else {
                    return nullptr;
                }
                if (pos + 1 >= tokens.size() ||
                    toLower(tokens[pos]) != "as" ||
                    toLower(tokens[pos + 1]) != "identity") {
                    return nullptr;
                }
                pos += 2;
                sub.identityAction = "add";
                if (pos < tokens.size() && tokens[pos] == "(") {
                    (void)collectParenthesized(tokens, pos);
                    sub.colDef.hasIdentityOptions = true;
                }
            } else if (pos < tokens.size() && toLower(tokens[pos]) == "type") {
                ++pos;
                if (!columnType(sub.dataType)) return nullptr;
            }
        } else if (kw == "rename") {
            ++pos;
            if (pos < tokens.size() && toLower(tokens[pos]) == "constraint") {
                recognized = true;
                sub.action = AlterTableStmt::Action::RenameConstraint;
                ++pos;
                if (pos + 2 < tokens.size() && toLower(tokens[pos]) == "if" &&
                    toLower(tokens[pos + 1]) == "exists") { sub.ifExists = true; pos += 2; }
                if (pos < tokens.size()) sub.name = parseRoutineIdentifier(tokens[pos++]);
                if (pos < tokens.size() && toLower(tokens[pos]) == "to") ++pos;
                if (pos < tokens.size()) sub.newName = parseRoutineIdentifier(tokens[pos++]);
            } else {
                bool explicitColumn = pos < tokens.size() && toLower(tokens[pos]) == "column";
                if (explicitColumn) ++pos;
                if (!explicitColumn && pos < tokens.size() && toLower(tokens[pos]) == "to") {
                    ++pos;
                    if (pos < tokens.size()) {
                        sub.newName = parseRoutineIdentifier(tokens[pos++]);
                    }
                    sub.action = AlterTableStmt::Action::RenameTable;
                    recognized = true;
                } else {
                    if (pos + 2 < tokens.size() && toLower(tokens[pos]) == "if" &&
                        toLower(tokens[pos + 1]) == "exists") { sub.ifExists = true; pos += 2; }
                    if (pos < tokens.size()) {
                        sub.name = parseRoutineIdentifier(tokens[pos++]);
                    }
                    if (pos < tokens.size() && toLower(tokens[pos]) == "to") ++pos;
                    if (pos < tokens.size()) {
                        sub.newName = parseRoutineIdentifier(tokens[pos++]);
                    }
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
    const auto beginning = [&](std::string command) {
        if (!modes.empty()) {
            for (const auto& mode : modes) {
                if (mode.kind == Mode::Kind::Isolation) {
                    command += " ISOLATION LEVEL ";
                    switch (mode.isolation) {
                        case IsolationLevel::READ_UNCOMMITTED: command += "READ UNCOMMITTED"; break;
                        case IsolationLevel::READ_COMMITTED: command += "READ COMMITTED"; break;
                        case IsolationLevel::REPEATABLE_READ: command += "REPEATABLE READ"; break;
                        case IsolationLevel::SERIALIZABLE: command += "SERIALIZABLE"; break;
                    }
                } else if (mode.kind == Mode::Kind::ReadOnly) {
                    command += mode.value ? " READ ONLY" : " READ WRITE";
                } else {
                    command += mode.value ? " DEFERRABLE" : " NOT DEFERRABLE";
                }
            }
            return command;
        }
        if (isolationSpecified) {
            command += " ISOLATION LEVEL ";
            switch (isolation) {
                case IsolationLevel::READ_UNCOMMITTED: command += "READ UNCOMMITTED"; break;
                case IsolationLevel::READ_COMMITTED: command += "READ COMMITTED"; break;
                case IsolationLevel::REPEATABLE_READ: command += "REPEATABLE READ"; break;
                case IsolationLevel::SERIALIZABLE: command += "SERIALIZABLE"; break;
            }
        }
        if (readOnlySpecified) command += readOnly ? " READ ONLY" : " READ WRITE";
        if (deferrableSpecified) command += deferrable ? " DEFERRABLE" : " NOT DEFERRABLE";
        return command;
    };
    const auto ending = [&](const std::string& command) {
        return command + (chainSpecified
            ? (chain ? " AND CHAIN" : " AND NO CHAIN") : "");
    };
    switch (kind) {
        case Kind::Begin: return beginning("BEGIN");
        case Kind::Start: return beginning("START TRANSACTION");
        case Kind::SetCharacteristics: return beginning("SET TRANSACTION");
        case Kind::Commit: return ending("COMMIT");
        case Kind::Rollback: return ending("ROLLBACK");
        case Kind::Abort: return ending("ABORT");
        case Kind::End: return ending("END");
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
