#include <algorithm>
#include <chrono>
#include <cctype>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "TableManage.h"
#include "ExecutionPlan.h"
#include "NetworkServer.h"
#include "logs.h"
#include "permissions.h"
#include "Session.h"

using namespace std;
using dbms::Column;
using dbms::makeDateColumn;
using dbms::makeIntColumn;
using dbms::makeStringColumn;
using dbms::makeVarCharColumn;
using dbms::OpResult;
using dbms::StorageEngine;
using dbms::TableSchema;

StorageEngine g_engine;

// ========================================================================
// Utility
// ========================================================================
static string toLower(string s) {
    for (char& c : s) c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
    return s;
}

static string trim(const string& s) {
    size_t a = 0;
    while (a < s.size() && isspace(static_cast<unsigned char>(s[a]))) ++a;
    size_t b = s.size();
    while (b > a && isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

static string stripQuotes(const string& s) {
    if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') {
        return s.substr(1, s.size() - 2);
    }
    return s;
}

static vector<string> tokenize(const string& sql) {
    vector<string> tokens;
    size_t i = 0;
    while (i < sql.size()) {
        while (i < sql.size() && isspace(static_cast<unsigned char>(sql[i]))) ++i;
        if (i >= sql.size()) break;
        if (sql[i] == '(' || sql[i] == ')') {
            tokens.emplace_back(1, sql[i]);
            ++i;
            continue;
        }
        size_t j = i;
        while (j < sql.size() && !isspace(static_cast<unsigned char>(sql[j]))
               && sql[j] != '(' && sql[j] != ')')
            ++j;
        tokens.push_back(sql.substr(i, j - i));
        i = j;
    }
    return tokens;
}

// ========================================================================
// CASE WHEN preprocessing forward declaration
// ========================================================================
static string preprocessCaseWhen(string s);

// ========================================================================
// SQL preprocessing
// ========================================================================
static string sqlProcessor(string raw) {
    raw = toLower(raw);
    raw.erase(remove(raw.begin(), raw.end(), '\n'), raw.end());
    raw.erase(remove(raw.begin(), raw.end(), '\t'), raw.end());
    raw.erase(remove(raw.begin(), raw.end(), '\r'), raw.end());
    if (!raw.empty() && raw.back() == ';') raw.pop_back();
    raw = preprocessCaseWhen(raw);
    return raw;
}

// ========================================================================
// CASE WHEN preprocessing: "case when a then b when c then d else e end"
//                         → "case_when(a,b,c,d,e)"
// ========================================================================
static string normalizeCaseCondition(string s) {
    s = trim(s);
    static const char* ops[] = {">=", "<=", "!=", "<>", ">", "<", "="};
    for (const char* op : ops) {
        size_t len = strlen(op);
        size_t pos = s.find(op);
        if (pos != string::npos) {
            string before = trim(s.substr(0, pos));
            string after = trim(s.substr(pos + len));
            return string(op) + before + " " + after;
        }
    }
    size_t isnotPos = s.find("is not null");
    if (isnotPos != string::npos) {
        return "isnotnull " + trim(s.substr(0, isnotPos));
    }
    size_t isPos = s.find("is null");
    if (isPos != string::npos) {
        return "isnull " + trim(s.substr(0, isPos));
    }
    size_t likePos = s.find("like");
    if (likePos != string::npos) {
        string before = trim(s.substr(0, likePos));
        string after = trim(s.substr(likePos + 4));
        return "like" + before + " " + after;
    }
    return s;
}

static string preprocessCaseWhen(string s) {
    size_t pos = 0;
    while (true) {
        size_t casePos = s.find("case when", pos);
        if (casePos == string::npos) break;
        size_t endPos = s.find("end", casePos);
        if (endPos == string::npos) break;
        string content = trim(s.substr(casePos + 9, endPos - casePos - 9));

        vector<pair<string, string>> whenThen;
        string elseVal;
        size_t curr = 0;
        while (curr < content.size()) {
            size_t thenPos = content.find("then", curr);
            if (thenPos == string::npos) break;
            string cond = normalizeCaseCondition(content.substr(curr, thenPos - curr));

            size_t nextWhen = content.find("when", thenPos + 4);
            size_t elsePos = content.find("else", thenPos + 4);

            if (elsePos != string::npos && (nextWhen == string::npos || elsePos < nextWhen)) {
                string thenVal = trim(content.substr(thenPos + 4, elsePos - thenPos - 4));
                whenThen.emplace_back(cond, thenVal);
                elseVal = trim(content.substr(elsePos + 4));
                break;
            } else if (nextWhen != string::npos) {
                string thenVal = trim(content.substr(thenPos + 4, nextWhen - thenPos - 4));
                whenThen.emplace_back(cond, thenVal);
                curr = nextWhen + 4;
            } else {
                string thenVal = trim(content.substr(thenPos + 4));
                whenThen.emplace_back(cond, thenVal);
                break;
            }
        }

        string replacement = "case_when(";
        for (size_t i = 0; i < whenThen.size(); ++i) {
            if (i > 0) replacement += ",";
            replacement += whenThen[i].first + "," + whenThen[i].second;
        }
        if (!elseVal.empty()) {
            if (!whenThen.empty()) replacement += ",";
            replacement += elseVal;
        }
        replacement += ")";

        s = s.substr(0, casePos) + replacement + s.substr(endPos + 3);
        pos = casePos + replacement.size();
    }
    return s;
}

// ========================================================================
// Scalar function helpers
// ========================================================================
static bool isScalarFunc(const string& name) {
    static const set<string> scalars = {"length", "upper", "lower", "trim", "substring", "concat",
                                         "abs", "round", "ceil", "floor",
                                         "now", "current_timestamp", "extract",
                                         "case_when", "cast"};
    return scalars.find(name) != scalars.end();
}

static vector<string> splitSelectColumns(const string& s) {
    vector<string> cols;
    size_t i = 0;
    int parenDepth = 0;
    string current;
    while (i < s.size()) {
        if (s[i] == '(') parenDepth++;
        else if (s[i] == ')') parenDepth--;
        else if (s[i] == ',' && parenDepth == 0) {
            cols.push_back(trim(current));
            current.clear();
            ++i;
            continue;
        }
        current += s[i];
        ++i;
    }
    if (!current.empty()) cols.push_back(trim(current));
    return cols;
}

static vector<string> splitFuncArgs(const string& s) {
    vector<string> args;
    size_t i = 0;
    while (i < s.size()) {
        while (i < s.size() && isspace(static_cast<unsigned char>(s[i]))) ++i;
        if (i >= s.size()) break;
        string arg;
        if (s[i] == '\'') {
            arg += s[i++];
            while (i < s.size() && s[i] != '\'') arg += s[i++];
            if (i < s.size()) arg += s[i++];
        } else {
            while (i < s.size() && s[i] != ',') arg += s[i++];
        }
        args.push_back(trim(arg));
        if (i < s.size() && s[i] == ',') ++i;
    }
    return args;
}

// ========================================================================
// Window function support
// ========================================================================
struct WindowFunc {
    string name;
    string arg;
    string orderByCol;
    bool orderByAsc;
};

static bool parseWindowFunc(const string& item, WindowFunc& wf) {
    string low = toLower(item);
    size_t overPos = low.find("over");
    if (overPos == string::npos) return false;

    string funcPart = trim(item.substr(0, overPos));
    string overPart = trim(item.substr(overPos + 4));

    size_t lp = funcPart.find('(');
    size_t rp = funcPart.rfind(')');
    if (lp == string::npos || rp == string::npos) return false;
    wf.name = toLower(trim(funcPart.substr(0, lp)));
    wf.arg = trim(funcPart.substr(lp + 1, rp - lp - 1));

    size_t overLp = overPart.find('(');
    size_t overRp = overPart.rfind(')');
    if (overLp == string::npos || overRp == string::npos) return false;
    string overContent = trim(overPart.substr(overLp + 1, overRp - overLp - 1));

    string lowContent = toLower(overContent);
    size_t orderPos = lowContent.find("order by");
    if (orderPos == string::npos) return false;

    string orderRest = trim(overContent.substr(orderPos + 8));
    vector<string> ot = tokenize(orderRest);
    if (ot.empty()) return false;
    wf.orderByCol = ot[0];
    wf.orderByAsc = true;
    if (ot.size() > 1 && toLower(ot[1]) == "desc") wf.orderByAsc = false;

    return true;
}

// ========================================================================
// Temporary table helpers
// ========================================================================
static string tempTablePrefix(const string& name) { return "__tmp_" + name; }

static string resolveTableName(Session& s, const string& name) {
    if (s.tempTables.count(name)) return tempTablePrefix(name);
    return name;
}

static bool isTempTable(Session& s, const string& name) {
    return s.tempTables.count(name) != 0;
}

// ========================================================================
// Normalize condition string: remove spaces around operators so tokenize
// keeps each condition as a single token (e.g. "score > 80" → "score>80")
// ========================================================================
// ========================================================================
// Split condition string by " and " into individual conditions
// ========================================================================
static vector<string> splitConds(const string& s) {
    vector<string> result;
    size_t start = 0;
    while (start < s.size()) {
        size_t andPos = s.find(" and ", start);
        if (andPos == string::npos) {
            result.push_back(trim(s.substr(start)));
            break;
        }
        result.push_back(trim(s.substr(start, andPos - start)));
        start = andPos + 5;
    }
    return result;
}

// ========================================================================
// CSV parsing helpers
// ========================================================================
static vector<string> parseCSVLine(const string& line) {
    vector<string> fields;
    size_t i = 0;
    while (i < line.size()) {
        string field;
        if (line[i] == '"') {
            ++i; // skip opening quote
            while (i < line.size()) {
                if (line[i] == '"') {
                    if (i + 1 < line.size() && line[i + 1] == '"') {
                        field += '"';
                        i += 2;
                    } else {
                        ++i; // skip closing quote
                        break;
                    }
                } else {
                    field += line[i++];
                }
            }
        } else {
            while (i < line.size() && line[i] != ',') {
                field += line[i++];
            }
        }
        fields.push_back(field);
        if (i < line.size() && line[i] == ',') ++i;
    }
    return fields;
}

static string escapeCSVField(const string& val) {
    bool needsQuote = val.find(',') != string::npos ||
                      val.find('"') != string::npos ||
                      val.find('\n') != string::npos;
    if (!needsQuote) return val;
    string result = "\"";
    for (char c : val) {
        if (c == '"') result += "\"\"";
        else result += c;
    }
    result += '"';
    return result;
}

static string normalizeConditionStr(string s) {
    static const char* ops[] = {">=", "<=", "!=", "<>", ">", "<", "="};
    for (const char* op : ops) {
        size_t len = strlen(op);
        size_t pos = 0;
        while ((pos = s.find(op, pos)) != string::npos) {
            size_t before = pos;
            while (before > 0 && isspace(static_cast<unsigned char>(s[before - 1]))) before--;
            size_t after = pos + len;
            while (after < s.size() && isspace(static_cast<unsigned char>(s[after]))) after++;
            if (before != pos || after != pos + len) {
                s = s.substr(0, before) + op + s.substr(after);
                pos = before + len;
            } else {
                pos += len;
            }
        }
    }
    // Normalize LIKE keyword: "name like 'a%'" → "namelike'a%'"
    size_t pos = 0;
    while ((pos = s.find("like", pos)) != string::npos) {
        size_t before = pos;
        while (before > 0 && isspace(static_cast<unsigned char>(s[before - 1]))) before--;
        size_t after = pos + 4;
        while (after < s.size() && isspace(static_cast<unsigned char>(s[after]))) after++;
        if (before != pos || after != pos + 4) {
            s = s.substr(0, before) + "like" + s.substr(after);
            pos = before + 4;
        } else {
            pos += 4;
        }
    }
    // Normalize IS NOT NULL (before IS NULL to avoid partial match)
    pos = 0;
    while ((pos = s.find("is not null", pos)) != string::npos) {
        size_t before = pos;
        while (before > 0 && isspace(static_cast<unsigned char>(s[before - 1]))) before--;
        size_t after = pos + 11;
        while (after < s.size() && isspace(static_cast<unsigned char>(s[after]))) after++;
        if (before != pos || after != pos + 11) {
            s = s.substr(0, before) + "isnotnull" + s.substr(after);
            pos = before + 9;
        } else {
            pos += 11;
        }
    }
    // Normalize IS NULL
    pos = 0;
    while ((pos = s.find("is null", pos)) != string::npos) {
        size_t before = pos;
        while (before > 0 && isspace(static_cast<unsigned char>(s[before - 1]))) before--;
        size_t after = pos + 7;
        while (after < s.size() && isspace(static_cast<unsigned char>(s[after]))) after++;
        if (before != pos || after != pos + 7) {
            s = s.substr(0, before) + "isnull" + s.substr(after);
            pos = before + 6;
        } else {
            pos += 7;
        }
    }
    return s;
}

// ========================================================================
// Condition conversion: "col=value" → "=col value"
// ========================================================================
static string modifyLogic(const string& logic) {
    if (logic == "(" || logic == ")" || logic == "and" || logic == "or") return logic;
    // Handle LIKE
    size_t likePos = logic.find("like");
    if (likePos != string::npos) {
        string before = logic.substr(0, likePos);
        string after = logic.substr(likePos + 4);
        return "like" + before + " " + after;
    }
    // Handle IS NOT NULL
    size_t isnotPos = logic.find("isnotnull");
    if (isnotPos != string::npos && isnotPos + 9 == logic.size()) {
        return "isnotnull " + logic.substr(0, isnotPos);
    }
    // Handle IS NULL
    size_t isPos = logic.find("isnull");
    if (isPos != string::npos && isPos + 6 == logic.size()) {
        return "isnull " + logic.substr(0, isPos);
    }
    size_t opStart = string::npos;
    size_t opLen = 0;
    for (size_t i = 0; i < logic.size(); ++i) {
        if (logic[i] == '>' || logic[i] == '<' || logic[i] == '=' || logic[i] == '!') {
            opStart = i;
            opLen = 1;
            if (i + 1 < logic.size() && (logic[i + 1] == '=' || logic[i + 1] == '>')) {
                if ((logic[i] == '<' && logic[i + 1] == '=') ||
                    (logic[i] == '>' && logic[i + 1] == '=') ||
                    (logic[i] == '!' && logic[i + 1] == '=') ||
                    (logic[i] == '<' && logic[i + 1] == '>')) {
                    opLen = 2;
                }
            }
            break;
        }
    }
    if (opStart == string::npos) return "";
    string op = logic.substr(opStart, opLen);
    string before = logic.substr(0, opStart);
    string after = logic.substr(opStart + opLen);
    return op + before + " " + after;
}

// ========================================================================
// Breakdown logic expressions into DNF (OR of ANDs)
// ========================================================================
static vector<vector<string>> breakDownConditions(const vector<string>& tokens) {
    if (tokens.size() == 3) {  // single condition wrapped in parens: ( cond )
        return {{tokens[1]}};
    }

    struct Frame {
        vector<vector<string>> groups;
    };

    vector<Frame> stack;
    stack.push_back(Frame());
    vector<string> operandStack;
    vector<string> opStack;

    auto applyAnd = [&](const string& right) {
        Frame& cur = stack.back();
        if (!operandStack.empty()) {
            string left = operandStack.back(); operandStack.pop_back();
            if (cur.groups.empty()) {
                cur.groups.push_back({left, right});
            } else {
                for (auto& g : cur.groups) g.push_back(right);
            }
        } else {
            if (cur.groups.empty()) {
                cur.groups.push_back({right});
            } else {
                for (auto& g : cur.groups) g.push_back(right);
            }
        }
    };

    auto applyOr = [&](const string& right) {
        Frame& cur = stack.back();
        if (!operandStack.empty()) {
            string left = operandStack.back(); operandStack.pop_back();
            if (cur.groups.empty()) {
                cur.groups.push_back({left});
                cur.groups.push_back({right});
            } else {
                auto old = cur.groups;
                cur.groups.clear();
                for (auto& g : old) {
                    auto g2 = g;
                    g2.push_back(right);
                    cur.groups.push_back(std::move(g));
                    cur.groups.push_back(std::move(g2));
                }
            }
        } else {
            // Left side already in groups; just add right as a new group
            cur.groups.push_back({right});
        }
    };

    auto flushOp = [&](const string& op, const string& right) {
        if (op == "and") applyAnd(right);
        else if (op == "or") applyOr(right);
    };

    for (const string& tok : tokens) {
        if (tok == "and" || tok == "or") {
            opStack.push_back(tok);
        } else if (tok == "(") {
            stack.push_back(Frame());
            opStack.clear();
            operandStack.clear();
        } else if (tok == ")") {
            Frame cur = std::move(stack.back());
            stack.pop_back();
            if (cur.groups.empty() && !operandStack.empty()) {
                cur.groups.push_back({operandStack.back()});
            }
            // Merge into parent
            if (stack.empty()) {
                stack.push_back(std::move(cur));
                break;
            }
            if (cur.groups.empty()) continue;
            if (stack.back().groups.empty()) {
                stack.back().groups = std::move(cur.groups);
            } else {
                auto old = stack.back().groups;
                stack.back().groups.clear();
                for (auto& g1 : old) {
                    for (auto& g2 : cur.groups) {
                        vector<string> merged = g1;
                        merged.insert(merged.end(), g2.begin(), g2.end());
                        stack.back().groups.push_back(std::move(merged));
                    }
                }
            }
            operandStack.clear();
            opStack.clear();
        } else {
            if (!opStack.empty()) {
                string op = opStack.back(); opStack.pop_back();
                flushOp(op, tok);
            } else {
                operandStack.push_back(tok);
            }
        }
    }

    if (!operandStack.empty() && stack.back().groups.empty()) {
        stack.back().groups.push_back({operandStack.back()});
    }
    return stack.empty() ? vector<vector<string>>{} : stack.back().groups;
}

// ========================================================================
// Parse CREATE TABLE columns
// ========================================================================
static TableSchema parseTableColumns(const string& sql, size_t nameEnd) {
    TableSchema tbl;
    size_t pos = nameEnd;
    while (pos < sql.size()) {
        if (sql[pos] != '{' && sql[pos] != '(') { ++pos; continue; }
        bool isBrace = (sql[pos] == '{');
        char closeChar = isBrace ? '}' : ')';
        ++pos;
        while (pos < sql.size()) {
            size_t comma = sql.find(',', pos);
            size_t brace = sql.find(closeChar, pos);
            size_t endPos = min({comma, brace});
            if (endPos == string::npos) break;

            string segment = trim(sql.substr(pos, endPos - pos));
            if (segment.empty()) { pos = endPos + 1; continue; }

            string cname, ctype;
            bool isNull = true;
            bool isPK = false;
            bool isUnique = false;
            std::string defaultVal;
            std::string checkExpr;
            dbms::ForeignKey fk;

            vector<string> parts;
            if (isBrace) {
                // {col:type flags} format
                size_t colon = segment.find(':');
                if (colon == string::npos) break;
                cname = trim(segment.substr(0, colon));
                ctype = trim(segment.substr(colon + 1));
            } else {
                // (col type flags) format
                parts = tokenize(segment);
                if (parts.empty()) break;
                cname = parts[0];
                if (parts.size() >= 2) {
                    ctype = parts[1];
                    for (size_t i = 2; i < parts.size(); ++i) {
                        if (parts[i] == "primary") {
                            if (i + 1 < parts.size() && parts[i + 1] == "key") {
                                isPK = true; ++i;
                            }
                        } else if (parts[i] == "key") {
                            // handled above
                        } else if (parts[i] == "not" && i + 1 < parts.size() && parts[i + 1] == "null") {
                            isNull = false; ++i;
                        } else if (parts[i] == "0") {
                            isNull = false;
                        } else if (parts[i] == "unique") {
                            isUnique = true;
                        } else if (parts[i] == "default" && i + 1 < parts.size()) {
                            defaultVal = parts[i + 1];
                            if (defaultVal.size() >= 2 && defaultVal.front() == '\'' && defaultVal.back() == '\'') {
                                defaultVal = defaultVal.substr(1, defaultVal.size() - 2);
                            }
                            ++i;
                        } else if (parts[i] == "check" && i + 1 < parts.size()) {
                            // Collect CHECK expression
                            ++i;
                            int parenDepth = 0;
                            std::string expr;
                            for (; i < parts.size(); ++i) {
                                if (parts[i] == "(") {
                                    if (parenDepth > 0) expr += "(";
                                    ++parenDepth;
                                } else if (parts[i] == ")") {
                                    --parenDepth;
                                    if (parenDepth > 0) expr += ")";
                                } else {
                                    expr += parts[i];
                                }
                                if (parenDepth == 0 && !expr.empty()) break;
                            }
                            // Store normalized expression
                            checkExpr = normalizeConditionStr(expr);
                        }
                    }
                }
            }

            if (cname.empty() || ctype.empty()) break;

            // Parse type and flags for brace format
            if (isBrace) {
                size_t fkPos = ctype.find("->");
                string typeAndFlags = (fkPos == string::npos) ? ctype : trim(ctype.substr(0, fkPos));
                string fkStr = (fkPos == string::npos) ? "" : trim(ctype.substr(fkPos + 2));

                size_t sp = typeAndFlags.find(' ');
                string typeName = (sp == string::npos) ? typeAndFlags : trim(typeAndFlags.substr(0, sp));
                string flagsStr = (sp == string::npos) ? "" : trim(typeAndFlags.substr(sp + 1));
                {
                    stringstream fs(flagsStr);
                    string f;
                    while (fs >> f) {
                        if (f == "0") isNull = false;
                        if (f == "pk" || f == "PK") isPK = true;
                    }
                }

                if (!fkStr.empty()) {
                    size_t lp = fkStr.find('(');
                    size_t rp = fkStr.find(')');
                    if (lp != string::npos && rp != string::npos && rp > lp) {
                        fk.colName = cname;
                        fk.refTable = trim(fkStr.substr(0, lp));
                        fk.refCol = trim(fkStr.substr(lp + 1, rp - lp - 1));
                        fk.onDelete = "restrict";
                        // Parse ON DELETE action
                        string afterRp = trim(fkStr.substr(rp + 1));
                        size_t odPos = afterRp.find("on delete");
                        if (odPos != string::npos) {
                            string action = trim(afterRp.substr(odPos + 9));
                            if (action.size() >= 7 && action.substr(0, 7) == "cascade")
                                fk.onDelete = "cascade";
                            else if ((action.size() >= 7 && action.substr(0, 7) == "setnull") ||
                                     (action.size() >= 8 && action.substr(0, 8) == "set null"))
                                fk.onDelete = "setnull";
                        }
                    }
                }
                ctype = typeName;
            }

            if (ctype.substr(0, 6) == "serial") {
                Column col = makeIntColumn(cname, false, 2, isPK);
                col.isAutoIncrement = true;
                tbl.append(col);
            } else if (ctype.substr(0, 3) == "int") {
                tbl.append(makeIntColumn(cname, isNull, 2, isPK));
            } else if (ctype.substr(0, 4) == "tiny") {
                tbl.append(makeIntColumn(cname, isNull, 1, isPK));
            } else if (ctype.substr(0, 4) == "long") {
                tbl.append(makeIntColumn(cname, isNull, 3, isPK));
            } else if (ctype.substr(0, 4) == "date") {
                tbl.append(makeDateColumn(cname, isNull, isPK));
            } else if (ctype.substr(0, 4) == "char") {
                size_t len = 0;
                // Parentheses format: type and length may be in separate parts
                if (!isBrace && parts.size() >= 3) {
                    string lenStr = parts[2];
                    for (char c : lenStr)
                        if (isdigit(static_cast<unsigned char>(c))) len = len * 10 + (c - '0');
                }
                if (len == 0) {
                    size_t start = 4;
                    if (start < ctype.size() && ctype[start] == '(') ++start;
                    for (size_t i = start; i < ctype.size() && isdigit(static_cast<unsigned char>(ctype[i])); ++i)
                        len = len * 10 + (ctype[i] - '0');
                }
                if (len == 0) len = 1;
                tbl.append(makeStringColumn(cname, isNull, len, isPK));
            } else if (ctype.substr(0, 7) == "varchar") {
                size_t len = 0;
                if (!isBrace && parts.size() >= 3) {
                    string lenStr = parts[2];
                    for (char c : lenStr)
                        if (isdigit(static_cast<unsigned char>(c))) len = len * 10 + (c - '0');
                }
                if (len == 0) {
                    size_t start = 7;
                    if (start < ctype.size() && ctype[start] == '(') ++start;
                    for (size_t i = start; i < ctype.size() && isdigit(static_cast<unsigned char>(ctype[i])); ++i)
                        len = len * 10 + (ctype[i] - '0');
                }
                if (len == 0) len = 1;
                tbl.append(makeVarCharColumn(cname, isNull, len, isPK));
            }
            // Apply unique/default/check to last appended column
            if (tbl.len > 0) {
                tbl.cols[tbl.len - 1].isUnique = isUnique;
                tbl.cols[tbl.len - 1].defaultValue = defaultVal;
                tbl.cols[tbl.len - 1].checkExpr = checkExpr;
            }
            if (!fk.colName.empty()) tbl.appendFK(fk);
            pos = endPos + 1;
        }
        break;
    }
    return tbl;
}

// ========================================================================
// Parse UPDATE SET clause: "col1=val1, col2='val 2'"
// ========================================================================
static std::map<std::string, std::string> parseSetClause(const std::string& sql, size_t startPos, size_t endPos) {
    std::map<std::string, std::string> updates;
    std::string clause = trim(sql.substr(startPos, endPos - startPos));
    size_t pos = 0;
    while (pos < clause.size()) {
        size_t eqPos = clause.find('=', pos);
        size_t commaPos = clause.find(',', pos);
        if (eqPos == std::string::npos) break;
        std::string col = trim(clause.substr(pos, eqPos - pos));
        size_t valEnd = (commaPos == std::string::npos) ? clause.size() : commaPos;
        std::string val = trim(clause.substr(eqPos + 1, valEnd - eqPos - 1));
        updates[col] = stripQuotes(val);
        pos = (commaPos == std::string::npos) ? clause.size() : commaPos + 1;
    }
    return updates;
}

// ========================================================================
// Subquery helpers
// ========================================================================
static std::vector<std::string> runSubQuery(const std::string& rawSql, Session& s) {
    std::string sql = sqlProcessor(rawSql);
    size_t fromPos = sql.find("from");
    if (fromPos == std::string::npos) return {};
    std::string columns = trim(sql.substr(6, fromPos - 6));

    size_t wherePos = sql.find("where", fromPos);
    size_t tnameEnd = (wherePos != std::string::npos) ? wherePos : sql.size();
    std::string tname = trim(sql.substr(fromPos + 4, tnameEnd - fromPos - 4));

    if (!g_engine.tableExists(s.currentDB, tname)) return {};

    std::set<std::string> selectCols;
    if (columns != "*") {
        std::stringstream css(columns);
        std::string item;
        while (std::getline(css, item, ',')) selectCols.insert(trim(item));
    }

    std::vector<std::string> answers;
    if (wherePos != std::string::npos) {
        std::string condStr = normalizeConditionStr(trim(sql.substr(wherePos + 5)));
        std::vector<std::string> tokens = tokenize(condStr);
        tokens.insert(tokens.begin(), "(");
        tokens.push_back(")");
        for (auto& t : tokens) t = modifyLogic(t);
        auto groups = breakDownConditions(tokens);
        std::set<std::string> seen;
        for (const auto& g : groups) {
            auto part = g_engine.query(s.currentDB, tname, g, selectCols, {});
            for (const auto& row : part) {
                if (seen.insert(row).second) answers.push_back(row);
            }
        }
    } else {
        answers = g_engine.query(s.currentDB, tname, {}, selectCols, {});
    }

    for (auto& s : answers) {
        s = trim(s);
    }
    return answers;
}

// Helper: find matching closing paren from start position
static size_t findMatchingParen(const std::string& s, size_t start) {
    int depth = 1;
    for (size_t i = start + 1; i < s.size(); ++i) {
        if (s[i] == '(') ++depth;
        else if (s[i] == ')') { --depth; if (depth == 0) return i; }
    }
    return std::string::npos;
}

// Expand IN (...) / EXISTS (...) / ANY / ALL subqueries into plain conditions
static std::string expandSubqueries(std::string sql, Session& s) {
    // ---------- EXISTS ----------
    while (true) {
        size_t pos = sql.find("exists");
        if (pos == std::string::npos) break;
        size_t parenStart = sql.find('(', pos);
        if (parenStart == std::string::npos) break;
        size_t parenEnd = findMatchingParen(sql, parenStart);
        if (parenEnd == std::string::npos) break;

        std::string inner = trim(sql.substr(parenStart + 1, parenEnd - parenStart - 1));
        bool hasResult = false;
        if (inner.size() >= 6 && inner.substr(0, 6) == "select") {
            auto rows = runSubQuery(inner, s);
            hasResult = !rows.empty();
        }
        // Replace with special markers recognized by evalConditionOnRow
        std::string replacement = hasResult ? "__true__=1" : "__false__=1";
        sql = sql.substr(0, pos) + replacement + sql.substr(parenEnd + 1);
    }

    // ---------- ANY / ALL ----------
    while (true) {
        auto findAnyAll = [&](const std::string& key) -> size_t {
            size_t p = sql.find(" " + key + " ");
            if (p != std::string::npos) return p + 1; // point to key itself
            p = sql.find(" " + key + "(");
            if (p != std::string::npos) return p + 1;
            return std::string::npos;
        };
        size_t anyPos = findAnyAll("any");
        size_t allPos = findAnyAll("all");
        size_t pos = std::string::npos;
        bool isAny = false;
        if (anyPos != std::string::npos && allPos != std::string::npos) {
            pos = std::min(anyPos, allPos);
            isAny = (pos == anyPos);
        } else if (anyPos != std::string::npos) {
            pos = anyPos; isAny = true;
        } else if (allPos != std::string::npos) {
            pos = allPos; isAny = false;
        } else {
            break;
        }

        // Find operator before colName (e.g. "> any", "= any")
        size_t opStart = pos;
        while (opStart > 0 && std::isspace(static_cast<unsigned char>(sql[opStart - 1]))) --opStart;
        size_t opEnd = opStart;
        while (opEnd > 0 && (sql[opEnd - 1] == '<' || sql[opEnd - 1] == '>' || sql[opEnd - 1] == '=' || sql[opEnd - 1] == '!')) --opEnd;
        if (opEnd == opStart) break; // no operator found
        std::string op = sql.substr(opEnd, opStart - opEnd);

        size_t colNameEnd = opEnd;
        while (colNameEnd > 0 && std::isspace(static_cast<unsigned char>(sql[colNameEnd - 1]))) --colNameEnd;
        size_t colNameStart = colNameEnd;
        while (colNameStart > 0 && !std::isspace(static_cast<unsigned char>(sql[colNameStart - 1]))) --colNameStart;
        std::string colName = trim(sql.substr(colNameStart, colNameEnd - colNameStart));

        size_t parenStart = sql.find('(', pos);
        if (parenStart == std::string::npos) break;
        size_t parenEnd = findMatchingParen(sql, parenStart);
        if (parenEnd == std::string::npos) break;

        std::string inner = trim(sql.substr(parenStart + 1, parenEnd - parenStart - 1));
        std::vector<std::string> values;
        if (inner.size() >= 6 && inner.substr(0, 6) == "select") {
            values = runSubQuery(inner, s);
        }

        std::string replacement;
        if (values.empty()) {
            // SQL standard: ANY(empty) = false, ALL(empty) = true
            replacement = isAny ? "1=0" : "1=1";
        } else {
            for (size_t i = 0; i < values.size(); ++i) {
                if (i > 0) replacement += isAny ? " or " : " and ";
                replacement += colName + op + values[i];
            }
        }
        sql = sql.substr(0, colNameStart) + replacement + sql.substr(parenEnd + 1);
    }

    // ---------- IN ----------
    while (true) {
        size_t pos = sql.find(" in ");
        if (pos == std::string::npos) break;

        size_t parenStart = sql.find('(', pos);
        if (parenStart == std::string::npos) break;

        bool onlySpace = true;
        for (size_t i = pos + 4; i < parenStart; ++i) {
            if (!std::isspace(static_cast<unsigned char>(sql[i]))) { onlySpace = false; break; }
        }
        if (!onlySpace) {
            sql.erase(pos, 4);
            sql.insert(pos, " in ");
            pos += 4;
            continue;
        }

        size_t parenEnd = findMatchingParen(sql, parenStart);
        if (parenEnd == std::string::npos) break;

        size_t colStart = pos;
        while (colStart > 0 && std::isspace(static_cast<unsigned char>(sql[colStart - 1]))) --colStart;
        size_t colNameStart = colStart;
        while (colNameStart > 0 && !std::isspace(static_cast<unsigned char>(sql[colNameStart - 1]))) --colNameStart;
        std::string colName = trim(sql.substr(colNameStart, colStart - colNameStart));

        std::string inner = trim(sql.substr(parenStart + 1, parenEnd - parenStart - 1));
        std::vector<std::string> values;
        if (inner.size() >= 6 && inner.substr(0, 6) == "select") {
            values = runSubQuery(inner, s);
        } else {
            size_t vpos = 0;
            while (vpos < inner.size()) {
                size_t comma = inner.find(',', vpos);
                std::string val = trim((comma == std::string::npos) ? inner.substr(vpos) : inner.substr(vpos, comma - vpos));
                values.push_back(val);
                if (comma == std::string::npos) break;
                vpos = comma + 1;
            }
        }

        std::string replacement;
        if (values.empty()) {
            replacement = colName + "=__empty__ __empty__";
        } else {
            for (size_t i = 0; i < values.size(); ++i) {
                if (i > 0) replacement += " or ";
                replacement += colName + "=" + values[i];
            }
        }
        sql = sql.substr(0, colNameStart) + replacement + sql.substr(parenEnd + 1);
    }
    return sql;
}

// ========================================================================
// SQL execution
// ========================================================================
static bool checkAdmin(const Session& s) {
    if (s.permission == 0) {
        cout << "permission denied" << endl;
        log(s.username, "permission denied", getTime());
        return false;
    }
    return true;
}

static bool checkTablePermission(Session& s, const string& tname,
                                  dbms::StorageEngine::TablePrivilege priv) {
    if (s.permission == 1) return true; // admin bypass
    if (!g_engine.hasPermission(s.currentDB, tname, s.username, priv)) {
        cout << "permission denied on table " << tname << endl;
        return false;
    }
    return true;
}

static bool checkDB(const Session& s) {
    if (!g_engine.databaseExists(s.currentDB)) {
        cout << "Invalid Database name:" << s.currentDB << endl;
        log(s.username, "invalid database name", getTime());
        return false;
    }
    return true;
}

void logSlowQuery(const string& sql, double ms) {
    std::ofstream ofs("slow_query.log", std::ios::app);
    if (ofs) {
        auto now = std::chrono::system_clock::now();
        auto t = std::chrono::system_clock::to_time_t(now);
        char buf[64];
        std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
        ofs << buf << " [" << std::fixed << std::setprecision(2) << ms << "ms] " << sql << "\n";
    }
}

bool execute(const string& rawSql, Session& s) {
    auto start = std::chrono::steady_clock::now();
    string sql = sqlProcessor(rawSql);
    if (sql.substr(0, 3) == "use") {
        if (sql.substr(4, 8) == "database") {
            string dbname = trim(sql.substr(13));
            if (!g_engine.databaseExists(dbname)) {
                cout << "Database not found" << endl;
                s.currentDB = "";
                log(s.username, "use database error", getTime());
                return true;
            }
            s.currentDB = dbname;
            cout << "set Database to " << dbname << endl;
            log(s.username, "use database success", getTime());
            return false;
        }
    }

    if (sql.substr(0, 6) == "create") {
        if (sql.substr(7, 4) == "user") {
            if (!checkAdmin(s)) return true;
            string rest = trim(sql.substr(12));
            vector<string> parts;
            stringstream ss(rest);
            string part;
            while (ss >> part) parts.push_back(part);
            if (parts.size() < 3) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            user temp;
            temp.username = parts[0];
            temp.password = parts[1];
            temp.permission = parts[2];
            if (permissionQuery(temp.username) != -1) {
                cout << "error: user already exist" << endl;
                log(s.username, "error: user already exist", getTime());
                return true;
            }
            createUser(temp);
            cout << "create user  " << temp.username << "  succeeded" << endl;
            return false;
        }

        if (sql.substr(7, 8) == "database") {
            if (!checkAdmin(s)) return true;
            string dbname = trim(sql.substr(16));
            auto res = g_engine.createDatabase(dbname);
            if (res == OpResult::TableAlreadyExist) {
                cout << "Failed:Database " << dbname << " already exists" << endl;
                log(s.username, "create database error", getTime());
            } else {
                cout << "Create Database succeeded" << endl;
                log(s.username, "create database succeeded", getTime());
            }
            return res != OpResult::Success;
        }

        if (sql.substr(7, 9) == "temporary") {
            // create temporary table tname (...)
            if (!checkAdmin(s)) return true;
            if (!checkDB(s)) return true;
            string rest = trim(sql.substr(17)); // skip "create temporary "
            if (rest.substr(0, 5) != "table") {
                cout << "SQL syntax error" << endl;
                return true;
            }
            rest = trim(rest.substr(5));
            size_t sp = rest.find(' ');
            if (sp == string::npos) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string origName = rest.substr(0, sp);
            string tmpName = tempTablePrefix(origName);
            TableSchema tbl = parseTableColumns(sql, 17 + 5 + 1 + sp + 1);
            tbl.tablename = tmpName;
            auto res = g_engine.createTable(s.currentDB, tbl);
            if (res == OpResult::TableAlreadyExist) {
                cout << "Temporary table " << origName << " already exists" << endl;
                return true;
            }
            s.tempTables.insert(origName);
            cout << "Temporary table " << origName << " created" << endl;
            log(s.username, "temporary table create succeeded", getTime());
            return false;
        }

        if (sql.substr(7, 5) == "table") {
            if (!checkAdmin(s)) return true;
            if (!checkDB(s)) return true;
            string rest = trim(sql.substr(13));
            size_t sp = rest.find(' ');
            if (sp == string::npos) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string tname = rest.substr(0, sp);
            TableSchema tbl = parseTableColumns(sql, 13 + sp + 1);
            tbl.tablename = tname;
            auto res = g_engine.createTable(s.currentDB, tbl);
            if (res == OpResult::TableAlreadyExist) {
                cout << "Table " << tname << " already exists" << endl;
                log(s.username, "table already exists", getTime());
                return true;
            }
            cout << "Table create succeeded" << endl;
            log(s.username, "table create succeeded", getTime());
            return false;
        }

        if (sql.substr(7, 5) == "index") {
            if (!checkAdmin(s)) return true;
            if (!checkDB(s)) return true;
            // create index idxname on tname(colname)
            string rest = trim(sql.substr(13));
            size_t onPos = rest.find(" on ");
            if (onPos == string::npos) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string idxName = trim(rest.substr(0, onPos));
            string afterOn = trim(rest.substr(onPos + 4));
            size_t lp = afterOn.find('(');
            size_t rp = afterOn.find(')');
            if (lp == string::npos || rp == string::npos || rp <= lp + 1) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string tnameOrig = trim(afterOn.substr(0, lp));
            string tname = resolveTableName(s, tnameOrig);
            string colname = trim(afterOn.substr(lp + 1, rp - lp - 1));
            auto res = g_engine.createIndex(s.currentDB, tname, colname);
            if (res != OpResult::Success) {
                cout << "Create index failed" << endl;
                return true;
            }
            cout << "Index created" << endl;
            return false;
        }

        if (sql.substr(7, 4) == "view") {
            if (!checkAdmin(s)) return true;
            if (!checkDB(s)) return true;
            // create view viewname as select ...
            string rest = trim(sql.substr(12));
            size_t asPos = rest.find(" as ");
            if (asPos == string::npos) {
                cout << "SQL syntax error: VIEW requires AS clause" << endl;
                return true;
            }
            string viewname = trim(rest.substr(0, asPos));
            string viewSql = trim(rest.substr(asPos + 4));
            auto res = g_engine.createView(s.currentDB, viewname, viewSql);
            if (res == OpResult::TableAlreadyExist) {
                cout << "View " << viewname << " already exists" << endl;
                return true;
            }
            cout << "View " << viewname << " created" << endl;
            return false;
        }
    }

    if (sql.substr(0, 5) == "alter") {
        if (!checkAdmin(s)) return true;
        if (!checkDB(s)) return true;
        vector<string> tokens = tokenize(sql.substr(5));
        if (tokens.size() < 4 || tokens[0] != "table") {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tnameOrig = tokens[1];
        string tname = resolveTableName(s, tnameOrig);
        string op = tokens[2];
        if (op == "add" && tokens.size() >= 4 && tokens[3] == "column") {
            // alter table tname add column colname:type [0|1]
            if (tokens.size() < 5) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string colDef = tokens[4];
            size_t colon = colDef.find(':');
            if (colon == string::npos) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string cname = colDef.substr(0, colon);
            string ctype = colDef.substr(colon + 1);
            size_t sp = ctype.find(' ');
            string typeName = (sp == string::npos) ? ctype : trim(ctype.substr(0, sp));
            string nullFlag = (sp == string::npos) ? "" : trim(ctype.substr(sp + 1));
            bool isNull = (nullFlag != "0");

            Column col;
            if (typeName.substr(0, 3) == "int") {
                col = makeIntColumn(cname, isNull, 2);
            } else if (typeName.substr(0, 4) == "tiny") {
                col = makeIntColumn(cname, isNull, 1);
            } else if (typeName.substr(0, 4) == "long") {
                col = makeIntColumn(cname, isNull, 3);
            } else if (typeName.substr(0, 4) == "date") {
                col = makeDateColumn(cname, isNull);
            } else if (typeName.substr(0, 4) == "char") {
                size_t len = 0;
                for (size_t i = 4; i < typeName.size() && isdigit(static_cast<unsigned char>(typeName[i])); ++i)
                    len = len * 10 + (typeName[i] - '0');
                if (len == 0) len = 1;
                col = makeStringColumn(cname, isNull, len);
            } else if (typeName.substr(0, 7) == "varchar") {
                size_t len = 0;
                size_t start = 7;
                if (start < typeName.size() && typeName[start] == '(') ++start;
                for (size_t i = start; i < typeName.size() && isdigit(static_cast<unsigned char>(typeName[i])); ++i)
                    len = len * 10 + (typeName[i] - '0');
                if (len == 0) len = 1;
                col = makeVarCharColumn(cname, isNull, len);
            } else {
                cout << "Unknown data type" << endl;
                return true;
            }
            auto res = g_engine.alterTableAddColumn(s.currentDB, tname, col);
            if (res == OpResult::TableAlreadyExist) {
                cout << "Column already exists" << endl;
                return true;
            }
            cout << "Column added" << endl;
            return false;
        }
        if (op == "drop" && tokens.size() >= 4 && tokens[3] == "column") {
            if (tokens.size() < 5) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            auto res = g_engine.alterTableDropColumn(s.currentDB, tname, tokens[4]);
            if (res == OpResult::InvalidValue) {
                cout << "Column not found" << endl;
                return true;
            }
            cout << "Column dropped" << endl;
            return false;
        }
        cout << "SQL syntax error" << endl;
        return true;
    }

    if (sql.substr(0, 12) == "insert into ") {
        if (!checkDB(s)) return true;
        vector<string> tokens = tokenize(sql.substr(12));
        if (tokens.empty()) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = tokens[0];
        string resolvedName = resolveTableName(s, tname);
        if (!g_engine.tableExists(s.currentDB, resolvedName)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        if (!isTempTable(s, tname) && !checkTablePermission(s, tname, dbms::StorageEngine::TablePrivilege::Insert)) return true;

        // Find column list and value list
        size_t colStart = sql.find('(', 12);
        size_t colEnd = sql.find(')', colStart);
        size_t valStart = sql.find('(', colEnd);
        size_t valEnd = sql.find(')', valStart);
        if (colStart == string::npos || valStart == string::npos) {
            cout << "SQL syntax error" << endl;
            return true;
        }

        string colsStr = trim(sql.substr(colStart + 1, colEnd - colStart - 1));

        // Parse all value rows (multi-row INSERT)
        vector<string> allValStrs;
        allValStrs.push_back(trim(sql.substr(valStart + 1, valEnd - valStart - 1)));
        size_t nextPos = valEnd + 1;
        while (nextPos < sql.size()) {
            while (nextPos < sql.size() && isspace(static_cast<unsigned char>(sql[nextPos]))) ++nextPos;
            if (nextPos >= sql.size() || sql[nextPos] != ',') break;
            ++nextPos;
            while (nextPos < sql.size() && isspace(static_cast<unsigned char>(sql[nextPos]))) ++nextPos;
            if (nextPos >= sql.size() || sql[nextPos] != '(') break;
            size_t nextValEnd = sql.find(')', nextPos);
            if (nextValEnd == string::npos) break;
            allValStrs.push_back(trim(sql.substr(nextPos + 1, nextValEnd - nextPos - 1)));
            nextPos = nextValEnd + 1;
        }

        vector<string> cols;
        {
            stringstream css(colsStr);
            string item;
            while (getline(css, item, ',')) cols.push_back(trim(item));
        }

        int inserted = 0;
        for (const string& valsStr : allValStrs) {
            vector<string> vals;
            {
                stringstream vss(valsStr);
                string item;
                while (getline(vss, item, ',')) vals.push_back(trim(item));
            }
            if (cols.size() != vals.size()) {
                cout << "SQL syntax error: column count mismatch" << endl;
                return true;
            }

            map<string, string> values;
            for (size_t i = 0; i < cols.size(); ++i) values[cols[i]] = stripQuotes(vals[i]);

            auto res = g_engine.insert(s.currentDB, resolvedName, values);
            if (res == OpResult::DuplicateKey) {
                cout << "Duplicate key" << endl;
                return true;
            }
            if (res != OpResult::Success) {
                cout << "Invalid data, please check" << endl;
                return true;
            }
            ++inserted;
        }
        cout << inserted << " row(s) inserted" << endl;
        log(s.username, to_string(inserted) + " row(s) inserted", getTime());
        return false;
    }

    if (sql.substr(0, 12) == "delete from ") {
        if (!checkDB(s)) return true;
        string delRest = expandSubqueries(sql.substr(12), s);
        vector<string> tokens = tokenize(normalizeConditionStr(delRest));
        if (tokens.empty()) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = tokens[0];
        string resolvedName = resolveTableName(s, tname);
        if (!g_engine.tableExists(s.currentDB, resolvedName)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        if (!isTempTable(s, tname) && !checkTablePermission(s, tname, dbms::StorageEngine::TablePrivilege::Delete)) return true;

        tokens.erase(tokens.begin());
        if (tokens.empty()) {
            auto res = g_engine.remove(s.currentDB, resolvedName, {});
            if (res != OpResult::Success) {
                cout << "Delete failed: foreign key constraint violation or other error" << endl;
                return true;
            }
            cout << "Delete done" << endl;
            log(s.username, "delete done", getTime());
            return false;
        }
        if (tokens.size() == 1) {
            cout << "SQL syntax error" << endl;
            return true;
        }

        // Wrap in parens and convert conditions
        tokens.insert(tokens.begin(), "(");
        tokens.push_back(")");
        for (auto& t : tokens) t = modifyLogic(t);
        auto groups = breakDownConditions(tokens);
        for (const auto& g : groups) {
            auto res = g_engine.remove(s.currentDB, resolvedName, g);
            if (res != OpResult::Success) {
                cout << "Delete failed: foreign key constraint violation or other error" << endl;
                return true;
            }
        }
        cout << "Delete done" << endl;
        log(s.username, "delete done", getTime());
        return false;
    }

    if (sql.substr(0, 6) == "update") {
        if (!checkDB(s)) return true;
        size_t setPos = sql.find("set");
        if (setPos == std::string::npos) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = trim(sql.substr(6, setPos - 6));
        string resolvedName = resolveTableName(s, tname);
        if (!g_engine.tableExists(s.currentDB, resolvedName)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        if (!isTempTable(s, tname) && !checkTablePermission(s, tname, dbms::StorageEngine::TablePrivilege::Update)) return true;

        size_t wherePos = sql.find("where", setPos);
        auto updates = parseSetClause(sql, setPos + 3,
                                       (wherePos == std::string::npos) ? sql.size() : wherePos);
        if (updates.empty()) {
            cout << "SQL syntax error" << endl;
            return true;
        }

        vector<string> conds;
        if (wherePos != std::string::npos) {
            string whereClause = trim(sql.substr(wherePos + 5));
            whereClause = expandSubqueries(whereClause, s);
            whereClause = normalizeConditionStr(whereClause);
            vector<string> tokens = tokenize(whereClause);
            tokens.insert(tokens.begin(), "(");
            tokens.push_back(")");
            for (auto& t : tokens) t = modifyLogic(t);
            auto groups = breakDownConditions(tokens);
            for (const auto& g : groups) {
                auto res = g_engine.update(s.currentDB, resolvedName, updates, g);
                if (res != OpResult::Success) {
                    cout << "Update failed" << endl;
                    return true;
                }
            }
        } else {
            auto res = g_engine.update(s.currentDB, resolvedName, updates, {});
            if (res != OpResult::Success) {
                cout << "Update failed" << endl;
                return true;
            }
        }
        cout << "Update done" << endl;
        log(s.username, "update done", getTime());
        return false;
    }

    if (sql.substr(0, 7) == "analyze") {
        if (!checkAdmin(s)) return true;
        if (!checkDB(s)) return true;
        string rest = trim(sql.substr(7));
        if (rest.substr(0, 5) != "table") {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = trim(rest.substr(5));
        g_engine.analyzeTable(s.currentDB, tname);
        cout << "Table " << tname << " analyzed" << endl;
        return false;
    }

    if (sql.substr(0, 5) == "begin") {
        if (!checkDB(s)) return true;
        // Parse optional isolation level: "begin read committed"
        string rest = trim(sql.substr(5));
        if (!rest.empty()) {
            if (rest == "read uncommitted") {
                g_engine.setIsolationLevel(dbms::StorageEngine::IsolationLevel::ReadUncommitted);
            } else if (rest == "read committed") {
                g_engine.setIsolationLevel(dbms::StorageEngine::IsolationLevel::ReadCommitted);
            } else if (rest == "repeatable read") {
                g_engine.setIsolationLevel(dbms::StorageEngine::IsolationLevel::RepeatableRead);
            } else if (rest == "serializable") {
                g_engine.setIsolationLevel(dbms::StorageEngine::IsolationLevel::Serializable);
            }
        }
        auto res = g_engine.beginTransaction(s.currentDB);
        if (res != OpResult::Success) {
            cout << "Begin transaction failed" << endl;
            return true;
        }
        cout << "Transaction started" << endl;
        log(s.username, "begin transaction", getTime());
        return false;
    }

    if (sql.substr(0, 25) == "set transaction isolation" ||
        sql.substr(0, 31) == "set transaction isolation level") {
        string rawRest;
        if (sql.substr(0, 31) == "set transaction isolation level") {
            rawRest = sql.substr(31);
        } else {
            rawRest = sql.substr(25);
        }
        string rest = trim(rawRest);
        if (rest.find("read uncommitted") != string::npos) {
            g_engine.setIsolationLevel(dbms::StorageEngine::IsolationLevel::ReadUncommitted);
            cout << "Isolation level set to READ UNCOMMITTED" << endl;
        } else if (rest.find("read committed") != string::npos) {
            g_engine.setIsolationLevel(dbms::StorageEngine::IsolationLevel::ReadCommitted);
            cout << "Isolation level set to READ COMMITTED" << endl;
        } else if (rest.find("repeatable read") != string::npos) {
            g_engine.setIsolationLevel(dbms::StorageEngine::IsolationLevel::RepeatableRead);
            cout << "Isolation level set to REPEATABLE READ" << endl;
        } else if (rest.find("serializable") != string::npos) {
            g_engine.setIsolationLevel(dbms::StorageEngine::IsolationLevel::Serializable);
            cout << "Isolation level set to SERIALIZABLE" << endl;
        } else {
            cout << "Unknown isolation level" << endl;
            return true;
        }
        return false;
    }

    if (sql.substr(0, 6) == "commit") {
        auto res = g_engine.commitTransaction();
        if (res != OpResult::Success) {
            cout << "Commit failed" << endl;
            return true;
        }
        cout << "Transaction committed" << endl;
        log(s.username, "commit transaction", getTime());
        return false;
    }

    // SAVEPOINT name
    if (sql.substr(0, 10) == "savepoint ") {
        if (!g_engine.inTransaction()) {
            cout << "Not in transaction" << endl;
            return true;
        }
        string name = sql.substr(10);
        // trim trailing whitespace
        while (!name.empty() && isspace((unsigned char)name.back())) name.pop_back();
        if (name.empty()) {
            cout << "Savepoint name required" << endl;
            return true;
        }
        auto res = g_engine.savepoint(name);
        if (res != OpResult::Success) {
            cout << "Savepoint failed" << endl;
            return true;
        }
        cout << "Savepoint " << name << " created" << endl;
        log(s.username, "savepoint " + name, getTime());
        return false;
    }

    // RELEASE SAVEPOINT name
    if (sql.substr(0, 18) == "release savepoint ") {
        if (!g_engine.inTransaction()) {
            cout << "Not in transaction" << endl;
            return true;
        }
        string name = sql.substr(18);
        while (!name.empty() && isspace((unsigned char)name.back())) name.pop_back();
        if (name.empty()) {
            cout << "Savepoint name required" << endl;
            return true;
        }
        auto res = g_engine.releaseSavepoint(name);
        if (res != OpResult::Success) {
            cout << "Savepoint not found" << endl;
            return true;
        }
        cout << "Savepoint " << name << " released" << endl;
        log(s.username, "release savepoint " + name, getTime());
        return false;
    }

    // ROLLBACK TO SAVEPOINT name (must check before "rollback")
    if (sql.substr(0, 21) == "rollback to savepoint") {
        if (!g_engine.inTransaction()) {
            cout << "Not in transaction" << endl;
            return true;
        }
        string name = sql.substr(21);
        while (!name.empty() && isspace((unsigned char)name.front())) name.erase(name.begin());
        while (!name.empty() && isspace((unsigned char)name.back())) name.pop_back();
        if (name.empty()) {
            cout << "Savepoint name required" << endl;
            return true;
        }
        auto res = g_engine.rollbackToSavepoint(name);
        if (res != OpResult::Success) {
            cout << "Savepoint not found" << endl;
            return true;
        }
        cout << "Rolled back to savepoint " << name << endl;
        log(s.username, "rollback to savepoint " + name, getTime());
        return false;
    }

    if (sql.substr(0, 8) == "rollback") {
        auto res = g_engine.rollbackTransaction();
        if (res != OpResult::Success) {
            cout << "Rollback failed" << endl;
            return true;
        }
        cout << "Transaction rolled back" << endl;
        log(s.username, "rollback transaction", getTime());
        return false;
    }

    if (sql.substr(0, 10) == "checkpoint") {
        if (!checkDB(s)) return true;
        g_engine.checkpoint(s.currentDB);
        cout << "Checkpoint completed" << endl;
        log(s.username, "checkpoint", getTime());
        return false;
    }

    if (sql.substr(0, 6) == "vacuum") {
        if (!checkAdmin(s)) return true;
        if (!checkDB(s)) return true;
        string tname = trim(sql.substr(6));
        if (tname.empty()) {
            // Vacuum all tables in current database (excluding temp tables)
            auto tables = g_engine.getTableNames(s.currentDB);
            size_t totalFreed = 0;
            for (const auto& tbl : tables) {
                totalFreed += g_engine.vacuum(s.currentDB, tbl);
            }
            cout << "VACUUM completed, " << totalFreed << " pages freed" << endl;
        } else {
            string resolvedName = resolveTableName(s, tname);
            size_t freed = g_engine.vacuum(s.currentDB, resolvedName);
            cout << "VACUUM completed, " << freed << " pages freed" << endl;
        }
        return false;
    }

    if (sql.substr(0, 4) == "drop") {
        if (!checkAdmin(s)) return true;
        if (!checkDB(s)) return true;
        vector<string> tokens = tokenize(sql.substr(4));
        if (tokens.size() < 2) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string op = tokens[0];
        string name = tokens[1];
        if (op == "temporary") {
            if (tokens.size() < 3 || tokens[1] != "table") {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string origName = tokens[2];
            if (!s.tempTables.count(origName)) {
                cout << "Temporary table " << origName << " not exist" << endl;
                return true;
            }
            string tmpName = tempTablePrefix(origName);
            auto res = g_engine.dropTable(s.currentDB, tmpName);
            if (res == OpResult::TableNotExist) {
                cout << "Temporary table " << origName << " not exist" << endl;
                return true;
            }
            s.tempTables.erase(origName);
            cout << "Temporary table dropped" << endl;
            return false;
        }
        if (op == "table") {
            // Check if it's a temp table first
            if (s.tempTables.count(name)) {
                string tmpName = tempTablePrefix(name);
                auto res = g_engine.dropTable(s.currentDB, tmpName);
                if (res != OpResult::Success) {
                    cout << "Table " << name << " not exist" << endl;
                    return true;
                }
                s.tempTables.erase(name);
                cout << "Table dropped" << endl;
                return false;
            }
            auto res = g_engine.dropTable(s.currentDB, name);
            if (res == OpResult::TableNotExist) {
                cout << "Table " << name << " not exist" << endl;
                return true;
            }
            cout << "Table dropped" << endl;
            return false;
        }
        if (op == "database") {
            auto res = g_engine.dropDatabase(name);
            if (res == OpResult::DatabaseNotExist) {
                cout << "Database " << name << " not exist" << endl;
                return true;
            }
            cout << "Database dropped" << endl;
            log(s.username, "database dropped", getTime());
            return false;
        }
        if (op == "index") {
            // drop index idxname on tname
            if (tokens.size() < 3 || tokens[2] != "on") {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string tnameOrig = tokens[3];
            string tname = resolveTableName(s, tnameOrig);
            auto res = g_engine.dropIndex(s.currentDB, tname, name);
            if (res != OpResult::Success) {
                cout << "Drop index failed" << endl;
                return true;
            }
            cout << "Index dropped" << endl;
            return false;
        }
        if (op == "view") {
            auto res = g_engine.dropView(s.currentDB, name);
            if (res == OpResult::TableNotExist) {
                cout << "View " << name << " not exist" << endl;
                return true;
            }
            cout << "View dropped" << endl;
            return false;
        }
        cout << "SQL syntax error" << endl;
        return true;
    }

    // UNION / UNION ALL
    size_t unionAllPos = sql.find("union all");
    size_t unionPos = sql.find("union");
    bool isUnionAll = false;
    size_t actualUnionPos = string::npos;
    if (unionAllPos != string::npos) {
        isUnionAll = true;
        actualUnionPos = unionAllPos;
    } else if (unionPos != string::npos) {
        actualUnionPos = unionPos;
    }
    if (actualUnionPos != string::npos) {
        if (!checkDB(s)) return true;
        string leftSql = trim(sql.substr(0, actualUnionPos));
        string rightSql = trim(sql.substr(actualUnionPos + (isUnionAll ? 9 : 5)));
        if (leftSql.empty() || rightSql.empty()) {
            cout << "SQL syntax error: invalid UNION" << endl;
            return true;
        }
        // Execute left query, capture output
        auto* oldBuf = cout.rdbuf();
        stringstream leftSs;
        cout.rdbuf(leftSs.rdbuf());
        execute(leftSql, s);
        cout.rdbuf(oldBuf);
        // Execute right query, capture output
        stringstream rightSs;
        cout.rdbuf(rightSs.rdbuf());
        execute(rightSql, s);
        cout.rdbuf(oldBuf);
        // Parse outputs: first line is header, rest are data rows
        vector<string> leftLines, rightLines;
        string line;
        while (getline(leftSs, line)) leftLines.push_back(line);
        while (getline(rightSs, line)) rightLines.push_back(line);
        // Print header from left query
        if (!leftLines.empty()) {
            cout << leftLines[0] << endl;
        } else if (!rightLines.empty()) {
            cout << rightLines[0] << endl;
        }
        // Merge data rows (skip headers)
        set<string> seen;
        for (size_t i = 1; i < leftLines.size(); ++i) {
            if (isUnionAll || seen.insert(leftLines[i]).second) {
                cout << leftLines[i] << endl;
            }
        }
        for (size_t i = 1; i < rightLines.size(); ++i) {
            if (isUnionAll || seen.insert(rightLines[i]).second) {
                cout << rightLines[i] << endl;
            }
        }
        return false;
    }

    // SHOW CONNECTIONS / SHOW STATUS
    if (sql.substr(0, 5) == "show ") {
        string rest = trim(sql.substr(5));
        if (rest == "connections") {
            auto& s = dbms::getServerStats();
            cout << "active_connections: " << s.activeConnections.load() << endl;
            cout << "total_connections: " << s.totalConnections.load() << endl;
            cout << "max_connections: " << s.maxConnections.load() << endl;
            cout << "rejected_connections: " << s.rejectedConnections.load() << endl;
            return false;
        }
        if (rest == "status") {
            auto& s = dbms::getServerStats();
            cout << "active_connections " << s.activeConnections.load() << endl;
            cout << "total_connections " << s.totalConnections.load() << endl;
            cout << "max_connections " << s.maxConnections.load() << endl;
            cout << "rejected_connections " << s.rejectedConnections.load() << endl;
            return false;
        }
        if (rest == "tables") {
            if (!checkDB(s)) return true;
            auto names = g_engine.getTableNames(s.currentDB);
            for (const auto& n : names) cout << n << endl;
            return false;
        }
        if (rest.substr(0, 7) == "columns") {
            if (!checkDB(s)) return true;
            string colsRest = trim(rest.substr(7));
            size_t fromPos = colsRest.find("from ");
            size_t inPos = colsRest.find("in ");
            string tname;
            if (fromPos != string::npos) tname = trim(colsRest.substr(fromPos + 5));
            else if (inPos != string::npos) tname = trim(colsRest.substr(inPos + 3));
            else tname = colsRest;
            tname = resolveTableName(s, tname);
            TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
            if (tbl.len == 0) {
                cout << "Table not exist" << endl;
                return true;
            }
            cout << "field type null key default extra" << endl;
            for (size_t i = 0; i < tbl.len; ++i) {
                const auto& c = tbl.cols[i];
                cout << c.dataName << ' '
                     << c.dataType << ' '
                     << (c.isNull ? "yes" : "no") << ' '
                     << (c.isPrimaryKey ? "pri" : "") << ' '
                     << c.defaultValue << ' '
                     << (c.isAutoIncrement ? "auto_increment" : "")
                     << endl;
            }
            return false;
        }
        cout << "Unknown SHOW command" << endl;
        return true;
    }

    // GRANT privilege ON table TO user
    if (sql.substr(0, 6) == "grant ") {
        if (!checkAdmin(s)) return true;
        if (!checkDB(s)) return true;
        string rest = trim(sql.substr(6));
        size_t onPos = rest.find(" on ");
        size_t toPos = rest.find(" to ");
        if (onPos == string::npos || toPos == string::npos) {
            cout << "SQL syntax error: GRANT privilege ON table TO user" << endl;
            return true;
        }
        string privStr = trim(rest.substr(0, onPos));
        string tname = trim(rest.substr(onPos + 4, toPos - onPos - 4));
        string uname = trim(rest.substr(toPos + 4));
        dbms::StorageEngine::TablePrivilege priv;
        if (privStr == "select") priv = dbms::StorageEngine::TablePrivilege::Select;
        else if (privStr == "insert") priv = dbms::StorageEngine::TablePrivilege::Insert;
        else if (privStr == "update") priv = dbms::StorageEngine::TablePrivilege::Update;
        else if (privStr == "delete") priv = dbms::StorageEngine::TablePrivilege::Delete;
        else if (privStr == "all") priv = dbms::StorageEngine::TablePrivilege::All;
        else {
            cout << "Unknown privilege: " << privStr << endl;
            return true;
        }
        g_engine.grant(s.currentDB, tname, uname, priv);
        cout << "Granted " << privStr << " on " << tname << " to " << uname << endl;
        return false;
    }

    // REVOKE privilege ON table FROM user
    if (sql.substr(0, 7) == "revoke ") {
        if (!checkAdmin(s)) return true;
        if (!checkDB(s)) return true;
        string rest = trim(sql.substr(7));
        size_t onPos = rest.find(" on ");
        size_t fromPos = rest.find(" from ");
        if (onPos == string::npos || fromPos == string::npos) {
            cout << "SQL syntax error: REVOKE privilege ON table FROM user" << endl;
            return true;
        }
        string privStr = trim(rest.substr(0, onPos));
        string tname = trim(rest.substr(onPos + 4, fromPos - onPos - 4));
        string uname = trim(rest.substr(fromPos + 6));
        dbms::StorageEngine::TablePrivilege priv;
        if (privStr == "select") priv = dbms::StorageEngine::TablePrivilege::Select;
        else if (privStr == "insert") priv = dbms::StorageEngine::TablePrivilege::Insert;
        else if (privStr == "update") priv = dbms::StorageEngine::TablePrivilege::Update;
        else if (privStr == "delete") priv = dbms::StorageEngine::TablePrivilege::Delete;
        else if (privStr == "all") priv = dbms::StorageEngine::TablePrivilege::All;
        else {
            cout << "Unknown privilege: " << privStr << endl;
            return true;
        }
        g_engine.revoke(s.currentDB, tname, uname, priv);
        cout << "Revoked " << privStr << " on " << tname << " from " << uname << endl;
        return false;
    }

    // PREPARE stmt_name FROM 'sql_template'
    if (sql.substr(0, 8) == "prepare ") {
        string rest = trim(sql.substr(8));
        size_t fromPos = rest.find(" from ");
        if (fromPos == string::npos) {
            cout << "SQL syntax error: expected FROM" << endl;
            return true;
        }
        string stmtName = trim(rest.substr(0, fromPos));
        string templateSql = trim(rest.substr(fromPos + 6));
        templateSql = stripQuotes(templateSql);
        if (templateSql.empty()) {
            cout << "SQL syntax error: empty statement" << endl;
            return true;
        }
        s.preparedStmts[stmtName] = templateSql;
        cout << "Statement " << stmtName << " prepared" << endl;
        return false;
    }

    // EXECUTE stmt_name USING (val1, val2, ...)
    if (sql.substr(0, 8) == "execute ") {
        string rest = trim(sql.substr(8));
        size_t usingPos = rest.find(" using ");
        string stmtName, usingClause;
        if (usingPos == string::npos) {
            stmtName = rest;
        } else {
            stmtName = trim(rest.substr(0, usingPos));
            usingClause = trim(rest.substr(usingPos + 7));
        }
        auto it = s.preparedStmts.find(stmtName);
        if (it == s.preparedStmts.end()) {
            cout << "Prepared statement " << stmtName << " not found" << endl;
            return true;
        }
        string expanded = it->second;
        if (!usingClause.empty()) {
            // Parse values: remove surrounding () if present
            if (usingClause.size() >= 2 && usingClause.front() == '(' && usingClause.back() == ')') {
                usingClause = trim(usingClause.substr(1, usingClause.size() - 2));
            }
            stringstream vss(usingClause);
            string val;
            vector<string> values;
            while (getline(vss, val, ',')) values.push_back(trim(val));
            size_t vidx = 0;
            size_t pos = 0;
            while ((pos = expanded.find('?', pos)) != string::npos) {
                if (vidx >= values.size()) {
                    cout << "Not enough parameters for prepared statement" << endl;
                    return true;
                }
                expanded = expanded.substr(0, pos) + values[vidx] + expanded.substr(pos + 1);
                pos += values[vidx].size();
                ++vidx;
            }
            if (vidx < values.size()) {
                cout << "Too many parameters for prepared statement" << endl;
                return true;
            }
        }
        return execute(expanded, s);
    }

    // DEALLOCATE PREPARE stmt_name
    if (sql.substr(0, 19) == "deallocate prepare ") {
        string stmtName = trim(sql.substr(19));
        if (s.preparedStmts.erase(stmtName)) {
            cout << "Statement " << stmtName << " deallocated" << endl;
        } else {
            cout << "Prepared statement " << stmtName << " not found" << endl;
        }
        return false;
    }

    // LOAD DATA INFILE: import CSV
    if (sql.substr(0, 17) == "load data infile ") {
        if (!checkAdmin(s)) return true;
        if (!checkDB(s)) return true;
        string rest = trim(sql.substr(17));
        // Parse: 'file.csv' into table tname
        size_t q1 = rest.find('\'');
        if (q1 == string::npos) {
            cout << "SQL syntax error: missing filename" << endl;
            return true;
        }
        size_t q2 = rest.find('\'', q1 + 1);
        if (q2 == string::npos) {
            cout << "SQL syntax error: unclosed filename" << endl;
            return true;
        }
        string filename = rest.substr(q1 + 1, q2 - q1 - 1);
        string afterFile = trim(rest.substr(q2 + 1));
        if (afterFile.substr(0, 11) != "into table ") {
            cout << "SQL syntax error: expected INTO TABLE" << endl;
            return true;
        }
        string tname = trim(afterFile.substr(11));
        if (!g_engine.tableExists(s.currentDB, tname)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
        ifstream csvIn(filename);
        if (!csvIn) {
            cout << "Cannot open file: " << filename << endl;
            return true;
        }
        size_t imported = 0, skipped = 0;
        string line;
        bool firstLine = true;
        while (getline(csvIn, line)) {
            if (trim(line).empty()) continue;
            auto fields = parseCSVLine(line);
            if (fields.size() != tbl.len) {
                // Try treating first line as header
                if (firstLine) { firstLine = false; continue; }
                skipped++;
                continue;
            }
            firstLine = false;
            map<string, string> values;
            for (size_t i = 0; i < tbl.len; ++i) {
                values[tbl.cols[i].dataName] = trim(fields[i]);
            }
            auto res = g_engine.insert(s.currentDB, tname, values);
            if (res == OpResult::Success) imported++;
            else skipped++;
        }
        cout << "Imported " << imported << " rows, skipped " << skipped << endl;
        return false;
    }

    // EXPLAIN: show query plan without executing
    if (sql.substr(0, 7) == "explain") {
        if (!checkDB(s)) return true;
        string inner = trim(sql.substr(7));
        if (inner.size() < 6 || inner.substr(0, 6) != "select") {
            cout << "EXPLAIN only supports SELECT" << endl;
            return true;
        }
        // Parse the SELECT to extract table name and conditions
        size_t fromPos = inner.find("from");
        if (fromPos == string::npos) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string columns = trim(inner.substr(6, fromPos - 6));
        bool isDistinct = false;
        if (columns.size() >= 9 && columns.substr(0, 9) == "distinct ") {
            isDistinct = true;
            columns = trim(columns.substr(9));
        }
        size_t wherePos = inner.find("where", fromPos);
        size_t orderPos = inner.find("order by", fromPos);
        size_t limitPos = inner.find("limit", fromPos);
        string tname = trim(inner.substr(fromPos + 4,
            (wherePos != string::npos) ? (wherePos - fromPos - 4)
            : (orderPos != string::npos) ? (orderPos - fromPos - 4)
            : (limitPos != string::npos) ? (limitPos - fromPos - 4)
            : (inner.size() - fromPos - 4)));
        vector<string> conds;
        if (wherePos != string::npos) {
            size_t condEnd = (orderPos != string::npos) ? orderPos
                           : (limitPos != string::npos) ? limitPos
                           : inner.size();
            string condStr = normalizeConditionStr(trim(inner.substr(wherePos + 5, condEnd - wherePos - 5)));
            if (!condStr.empty()) {
                vector<string> rawConds = splitConds(condStr);
                for (auto& c : rawConds) {
                    string mc = modifyLogic(c);
                    if (!mc.empty()) conds.push_back(mc);
                }
            }
        }
        string orderByCol = "";
        bool orderByAsc = true;
        if (orderPos != string::npos) {
            size_t sortEnd = (limitPos != string::npos) ? limitPos : inner.size();
            string sortStr = trim(inner.substr(orderPos + 8, sortEnd - orderPos - 8));
            if (!sortStr.empty()) {
                if (sortStr.size() >= 5 && sortStr.substr(sortStr.size() - 4) == "desc") {
                    orderByCol = trim(sortStr.substr(0, sortStr.size() - 4));
                    orderByAsc = false;
                } else {
                    orderByCol = trim(sortStr);
                }
            }
        }
        size_t limitVal = 0;
        if (limitPos != string::npos) {
            string lstr = trim(inner.substr(limitPos + 5));
            try { limitVal = static_cast<size_t>(std::stoull(lstr)); } catch (...) {}
        }
        set<string> selectCols;
        bool selectAll = (columns == "*");
        if (!selectAll) {
            for (const auto& item : splitSelectColumns(columns)) {
                selectCols.insert(trim(item));
            }
        }
        dbms::PlanContext ctx;
        ctx.dbname = s.currentDB;
        ctx.tablename = tname;
        ctx.conds = dbms::StorageEngine::parseConditions(conds);
        ctx.selectCols = selectCols;
        ctx.orderByCol = orderByCol;
        ctx.orderByAsc = orderByAsc;
        ctx.limit = limitVal;
        ctx.distinct = isDistinct;
        auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
        cout << dbms::QueryPlanner::explain(plan, &g_engine, s.currentDB);
        return false;
    }

    if (sql.substr(0, 6) == "select") {
        if (!checkDB(s)) return true;

        // Check for INTO OUTFILE clause
        string outfile;
        size_t intoPos = sql.find("into outfile");
        if (intoPos != string::npos) {
            size_t q1 = sql.find('\'', intoPos);
            if (q1 != string::npos) {
                size_t q2 = sql.find('\'', q1 + 1);
                if (q2 != string::npos) {
                    outfile = sql.substr(q1 + 1, q2 - q1 - 1);
                    sql = trim(sql.substr(0, intoPos));
                }
            }
        }

        size_t fromPos = sql.find("from");
        if (fromPos == string::npos) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string columns = trim(sql.substr(6, fromPos - 6));
        bool isDistinct = false;
        if (columns.size() >= 9 && columns.substr(0, 9) == "distinct ") {
            isDistinct = true;
            columns = trim(columns.substr(9));
        }

        size_t wherePos = sql.find("where", fromPos);
        size_t groupPos = sql.find("group by", fromPos);
        size_t havingPos = sql.find("having", fromPos);
        size_t orderPos = sql.find("order by", fromPos);
        size_t limitPos = sql.find("limit", fromPos);
        size_t offsetPos = sql.find("offset", fromPos);

        auto parseLimitOffset = [&](size_t& limitVal, size_t& offsetVal) {
            limitVal = 0; offsetVal = 0;
            if (limitPos != string::npos) {
                size_t limEnd = (offsetPos != string::npos) ? offsetPos
                              : sql.size();
                string lstr = trim(sql.substr(limitPos + 5, limEnd - limitPos - 5));
                try { limitVal = static_cast<size_t>(std::stoull(lstr)); } catch (...) {}
            }
            if (offsetPos != string::npos) {
                string ostr = trim(sql.substr(offsetPos + 6));
                try { offsetVal = static_cast<size_t>(std::stoull(ostr)); } catch (...) {}
            }
        };

        // Check for JOIN
        size_t leftJoinPos = sql.find("left join", fromPos);
        size_t rightJoinPos = sql.find("right join", fromPos);
        size_t fullOuterJoinPos = sql.find("full outer join", fromPos);
        size_t crossJoinPos = sql.find("cross join", fromPos);
        size_t innerJoinPos = sql.find("inner join", fromPos);
        size_t joinPos = sql.find("join", fromPos);

        enum class JoinType { Inner, Left, Right, FullOuter, Cross };
        JoinType jt = JoinType::Inner;
        size_t actualJoinPos = joinPos;

        if (leftJoinPos != string::npos &&
            (wherePos == string::npos || leftJoinPos < wherePos) &&
            (orderPos == string::npos || leftJoinPos < orderPos)) {
            jt = JoinType::Left;
            actualJoinPos = leftJoinPos;
        } else if (rightJoinPos != string::npos &&
                   (wherePos == string::npos || rightJoinPos < wherePos) &&
                   (orderPos == string::npos || rightJoinPos < orderPos)) {
            jt = JoinType::Right;
            actualJoinPos = rightJoinPos;
        } else if (fullOuterJoinPos != string::npos &&
                   (wherePos == string::npos || fullOuterJoinPos < wherePos) &&
                   (orderPos == string::npos || fullOuterJoinPos < orderPos)) {
            jt = JoinType::FullOuter;
            actualJoinPos = fullOuterJoinPos;
        } else if (crossJoinPos != string::npos &&
                   (wherePos == string::npos || crossJoinPos < wherePos) &&
                   (orderPos == string::npos || crossJoinPos < orderPos)) {
            jt = JoinType::Cross;
            actualJoinPos = crossJoinPos;
        } else if (innerJoinPos != string::npos &&
                   (wherePos == string::npos || innerJoinPos < wherePos) &&
                   (orderPos == string::npos || innerJoinPos < orderPos)) {
            jt = JoinType::Inner;
            actualJoinPos = innerJoinPos;
        } else if (joinPos != string::npos &&
                   (wherePos == string::npos || joinPos < wherePos) &&
                   (orderPos == string::npos || joinPos < orderPos)) {
            jt = JoinType::Inner;
            actualJoinPos = joinPos;
        } else {
            actualJoinPos = string::npos;
        }

        bool isJoin = (actualJoinPos != string::npos);

        if (isJoin) {
            string leftTableOrig = trim(sql.substr(fromPos + 4, actualJoinPos - fromPos - 4));
            bool isCrossJoin = (jt == JoinType::Cross);
            size_t onPos = sql.find("on", actualJoinPos);
            size_t tableNameStart = actualJoinPos;
            if (jt == JoinType::Left) tableNameStart += 9;
            else if (jt == JoinType::Right) tableNameStart += 10;
            else if (jt == JoinType::FullOuter) tableNameStart += 15;
            else if (jt == JoinType::Cross) tableNameStart += 10;
            else if (jt == JoinType::Inner) tableNameStart += 10;
            else tableNameStart += 4;

            size_t clauseEnd = (wherePos != string::npos) ? wherePos
                             : (orderPos != string::npos) ? orderPos : sql.size();
            string rightTableOrig;
            string leftOnCol, rightOnCol;

            if (isCrossJoin) {
                rightTableOrig = trim(sql.substr(tableNameStart, clauseEnd - tableNameStart));
            } else {
                if (onPos == string::npos) {
                    cout << "SQL syntax error: missing ON clause" << endl;
                    return true;
                }
                rightTableOrig = trim(sql.substr(tableNameStart, onPos - tableNameStart));
                string onClause = normalizeConditionStr(trim(sql.substr(onPos + 2, clauseEnd - onPos - 2)));
                size_t eqPos = onClause.find('=');
                if (eqPos == string::npos) {
                    cout << "SQL syntax error: invalid ON clause" << endl;
                    return true;
                }
                leftOnCol = trim(onClause.substr(0, eqPos));
                rightOnCol = trim(onClause.substr(eqPos + 1));
                size_t dot = leftOnCol.find('.');
                if (dot != string::npos) leftOnCol = leftOnCol.substr(dot + 1);
                dot = rightOnCol.find('.');
                if (dot != string::npos) rightOnCol = rightOnCol.substr(dot + 1);
            }

            string leftTable = resolveTableName(s, leftTableOrig);
            string rightTable = resolveTableName(s, rightTableOrig);
            if (!g_engine.tableExists(s.currentDB, leftTable) ||
                !g_engine.tableExists(s.currentDB, rightTable)) {
                cout << "Table not exist" << endl;
                return true;
            }
            if (!isTempTable(s, leftTableOrig) && !checkTablePermission(s, leftTableOrig, dbms::StorageEngine::TablePrivilege::Select)) return true;
            if (!isTempTable(s, rightTableOrig) && !checkTablePermission(s, rightTableOrig, dbms::StorageEngine::TablePrivilege::Select)) return true;

            set<string> selectCols;
            bool selectAll = (columns == "*");
            if (!selectAll) {
                for (const auto& item : splitSelectColumns(columns)) {
                    selectCols.insert(trim(item));
                }
            }

            vector<string> condTokens;
            if (wherePos != string::npos) {
                size_t condEnd = (orderPos != string::npos) ? orderPos : sql.size();
                string whereClause = trim(sql.substr(wherePos + 5, condEnd - wherePos - 5));
                whereClause = expandSubqueries(whereClause, s);
                string condStr = normalizeConditionStr(whereClause);
                condTokens = tokenize(condStr);
            }

            TableSchema leftTbl = g_engine.getTableSchema(s.currentDB, leftTable);
            TableSchema rightTbl = g_engine.getTableSchema(s.currentDB, rightTable);
            if (selectAll) {
                for (size_t i = 0; i < leftTbl.len; ++i)
                    cout << leftTableOrig << "." << leftTbl.cols[i].dataName << ' ';
                for (size_t i = 0; i < rightTbl.len; ++i)
                    cout << rightTableOrig << "." << rightTbl.cols[i].dataName << ' ';
            } else {
                for (const auto& c : selectCols) cout << c << ' ';
            }
            cout << '\n';

            vector<string> answers;
            auto runJoin = [&](const vector<string>& conds) -> vector<string> {
                if (jt == JoinType::Left) {
                    return g_engine.leftJoin(s.currentDB, leftTable, rightTable,
                                              leftOnCol, rightOnCol, conds, selectCols);
                } else if (jt == JoinType::Right) {
                    return g_engine.rightJoin(s.currentDB, leftTable, rightTable,
                                               leftOnCol, rightOnCol, conds, selectCols);
                } else if (jt == JoinType::FullOuter) {
                    return g_engine.fullOuterJoin(s.currentDB, leftTable, rightTable,
                                                   leftOnCol, rightOnCol, conds, selectCols);
                } else if (jt == JoinType::Cross) {
                    return g_engine.crossJoin(s.currentDB, leftTable, rightTable, conds, selectCols);
                } else {
                    return g_engine.join(s.currentDB, leftTable, rightTable,
                                          leftOnCol, rightOnCol, conds, selectCols);
                }
            };

            if (condTokens.empty()) {
                answers = runJoin({});
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = runJoin(g);
                    for (const auto& row : part) {
                        if (seen.insert(row).second) answers.push_back(row);
                    }
                }
            }
            if (isDistinct) {
                vector<string> deduped;
                set<string> seen;
                for (const auto& row : answers) {
                    if (seen.insert(row).second) deduped.push_back(row);
                }
                answers = std::move(deduped);
            }
            size_t jlim = 0, joff = 0;
            parseLimitOffset(jlim, joff);
            if (joff < answers.size()) {
                if (jlim > 0 && joff + jlim < answers.size())
                    answers.erase(answers.begin() + joff + jlim, answers.end());
                if (joff > 0)
                    answers.erase(answers.begin(), answers.begin() + joff);
            }
            for (const auto& row : answers) {
                cout << row << endl;
                log(s.username, row, getTime());
            }
            return false;
        }

        // Non-JOIN query
        size_t tnameEnd = (wherePos != string::npos) ? wherePos
                         : (groupPos != string::npos) ? groupPos
                         : (havingPos != string::npos) ? havingPos
                         : (orderPos != string::npos) ? orderPos
                         : (limitPos != string::npos) ? limitPos
                         : (offsetPos != string::npos) ? offsetPos : sql.size();
        string tnameOrig = trim(sql.substr(fromPos + 4, tnameEnd - fromPos - 4));
        string tname = resolveTableName(s, tnameOrig);

        if (!g_engine.tableExists(s.currentDB, tname)) {
            if (g_engine.viewExists(s.currentDB, tnameOrig)) {
                string viewSql = g_engine.getViewSQL(s.currentDB, tnameOrig);
                if (!viewSql.empty()) {
                    execute(viewSql, s);
                    return false;
                }
            }
            cout << "Table " << tnameOrig << " not exist" << endl;
            return true;
        }
        if (!isTempTable(s, tnameOrig) && !checkTablePermission(s, tnameOrig, dbms::StorageEngine::TablePrivilege::Select)) return true;

        vector<dbms::StorageEngine::OrderBySpec> orderBySpecs;
        if (orderPos != string::npos) {
            size_t orderEnd = (limitPos != string::npos) ? limitPos
                            : (offsetPos != string::npos) ? offsetPos : sql.size();
            string orderRest = trim(sql.substr(orderPos + 8, orderEnd - orderPos - 8));
            vector<string> orderParts;
            {
                stringstream oss(orderRest);
                string part;
                while (getline(oss, part, ',')) orderParts.push_back(trim(part));
            }
            for (const string& part : orderParts) {
                vector<string> ot = tokenize(part);
                if (!ot.empty()) {
                    dbms::StorageEngine::OrderBySpec spec;
                    spec.colName = ot[0];
                    spec.ascending = !(ot.size() > 1 && ot[1] == "desc");
                    orderBySpecs.push_back(spec);
                }
            }
        }

        vector<string> groupByCols;
        if (groupPos != string::npos) {
            size_t groupEnd = (havingPos != string::npos) ? havingPos
                            : (orderPos != string::npos) ? orderPos
                            : (limitPos != string::npos) ? limitPos
                            : (offsetPos != string::npos) ? offsetPos : sql.size();
            string groupRest = trim(sql.substr(groupPos + 8, groupEnd - groupPos - 8));
            {
                stringstream gss(groupRest);
                string part;
                while (getline(gss, part, ',')) groupByCols.push_back(trim(part));
            }
        }

        if (havingPos != string::npos && groupPos == string::npos) {
            cout << "SQL syntax error: HAVING without GROUP BY" << endl;
            return true;
        }

        vector<string> havingConds;
        if (havingPos != string::npos) {
            size_t havingEnd = (orderPos != string::npos) ? orderPos
                             : (limitPos != string::npos) ? limitPos
                             : (offsetPos != string::npos) ? offsetPos : sql.size();
            string havingClause = normalizeConditionStr(trim(sql.substr(havingPos + 6, havingEnd - havingPos - 6)));
            size_t pos = 0;
            while (pos < havingClause.size()) {
                size_t andPos = havingClause.find("and", pos);
                if (andPos == string::npos) {
                    havingConds.push_back(trim(havingClause.substr(pos)));
                    break;
                }
                havingConds.push_back(trim(havingClause.substr(pos, andPos - pos)));
                pos = andPos + 3;
            }
        }

        TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
        set<string> selectCols;
        bool selectAll = (columns == "*");

        // Detect aggregate functions, window functions, and scalar functions in columns
        vector<pair<string, string>> aggItems;
        vector<WindowFunc> windowFuncs;
        vector<dbms::StorageEngine::SelectExpr> selectExprs;
        vector<int> exprTypes; // 0=normal, 1=agg, 2=window, 3=scalar
        bool hasAgg = false;
        bool hasWindow = false;
        bool hasScalar = false;
        {
            for (const auto& itemRaw : splitSelectColumns(columns)) {
                string item = trim(itemRaw);
                WindowFunc wf;
                if (parseWindowFunc(item, wf)) {
                    windowFuncs.push_back(wf);
                    hasWindow = true;
                    exprTypes.push_back(2);
                    aggItems.emplace_back(wf.name, wf.arg);
                    dbms::StorageEngine::SelectExpr expr;
                    expr.displayName = item;
                    expr.isScalar = false;
                    expr.colName = item;
                    selectExprs.push_back(expr);
                } else {
                    size_t lp = item.find('(');
                    size_t rp = item.find(')');
                    if (lp != string::npos && rp != string::npos && rp > lp) {
                        string func = item.substr(0, lp);
                        string arg = item.substr(lp + 1, rp - lp - 1);
                        // Preprocess cast: "expr as type" → "expr,type"
                        if (func == "cast") {
                            size_t asPos = arg.find(" as ");
                            if (asPos != string::npos) {
                                string expr = trim(arg.substr(0, asPos));
                                string type = trim(arg.substr(asPos + 4));
                                arg = expr + "," + type;
                            }
                        }
                        // Normalize count(distinct col) → "distinct col"
                        if (func == "count") {
                            arg = trim(arg);
                            if (arg.size() > 9 && arg.substr(0, 9) == "distinct ") {
                                arg = "distinct " + trim(arg.substr(9));
                            }
                        }
                        if (isScalarFunc(func)) {
                            dbms::StorageEngine::SelectExpr expr;
                            expr.displayName = item;
                            expr.isScalar = true;
                            expr.funcName = func;
                            expr.funcArgs = splitFuncArgs(arg);
                            selectExprs.push_back(expr);
                            hasScalar = true;
                            exprTypes.push_back(3);
                            // Add referenced columns to selectCols for fetching
                            for (const auto& a : expr.funcArgs) {
                                string ta = trim(a);
                                if (ta.size() >= 2 && ta.front() == '\'' && ta.back() == '\'') continue;
                                for (size_t ci = 0; ci < tbl.len; ++ci) {
                                    if (tbl.cols[ci].dataName == ta) {
                                        selectCols.insert(ta);
                                        break;
                                    }
                                }
                            }
                        } else {
                            aggItems.emplace_back(func, arg);
                            hasAgg = true;
                            exprTypes.push_back(1);
                            dbms::StorageEngine::SelectExpr expr;
                            expr.displayName = item;
                            expr.isScalar = false;
                            expr.colName = item;
                            selectExprs.push_back(expr);
                        }
                    } else {
                        aggItems.emplace_back("", item);
                        exprTypes.push_back(0);
                        dbms::StorageEngine::SelectExpr expr;
                        expr.displayName = item;
                        expr.isScalar = false;
                        expr.colName = item;
                        selectExprs.push_back(expr);
                        if (!selectAll) {
                            bool found = false;
                            for (size_t i = 0; i < tbl.len; ++i) {
                                if (tbl.cols[i].dataName == item) { found = true; break; }
                            }
                            if (!found) {
                                cout << "Invalid column name " << item << endl;
                                return true;
                            }
                            selectCols.insert(item);
                        }
                    }
                }
            }
        }

        // WHERE clause
        vector<string> condTokens;
        if (wherePos != string::npos) {
            size_t condEnd = (groupPos != string::npos) ? groupPos
                           : (havingPos != string::npos) ? havingPos
                           : (orderPos != string::npos) ? orderPos
                           : (limitPos != string::npos) ? limitPos
                           : (offsetPos != string::npos) ? offsetPos : sql.size();
            string whereClause = trim(sql.substr(wherePos + 5, condEnd - wherePos - 5));
            whereClause = expandSubqueries(whereClause, s);
            string condStr = normalizeConditionStr(whereClause);
            condTokens = tokenize(condStr);
        }

        vector<string> answers;
        if (!groupByCols.empty()) {
            for (const auto& gc : groupByCols) cout << gc << ' ';
            vector<pair<string, string>> pureAgg;
            for (const auto& it : aggItems) {
                if (!it.first.empty()) {
                    cout << it.first << '(' << it.second << ") ";
                    pureAgg.push_back(it);
                }
            }
            cout << '\n';
            if (condTokens.empty()) {
                answers = g_engine.groupAggregate(s.currentDB, tname, {}, pureAgg, groupByCols, havingConds);
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = g_engine.groupAggregate(s.currentDB, tname, g, pureAgg, groupByCols, havingConds);
                    for (const auto& row : part) {
                        if (seen.insert(row).second) answers.push_back(row);
                    }
                }
            }
        } else if (hasAgg) {
            for (const auto& it : aggItems) {
                if (it.first.empty()) cout << it.second << ' ';
                else cout << it.first << '(' << it.second << ") ";
            }
            cout << '\n';
            vector<pair<string, string>> pureAgg;
            for (const auto& it : aggItems) {
                if (!it.first.empty()) pureAgg.push_back(it);
            }
            if (pureAgg.empty()) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            if (condTokens.empty()) {
                answers = g_engine.aggregate(s.currentDB, tname, {}, pureAgg);
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = g_engine.aggregate(s.currentDB, tname, g, pureAgg);
                    for (const auto& row : part) {
                        if (seen.insert(row).second) answers.push_back(row);
                    }
                }
            }
        } else if (hasWindow) {
            // Output header
            for (size_t i = 0; i < aggItems.size(); ++i) {
                if (exprTypes[i] == 2) {
                    cout << aggItems[i].first << "(" << aggItems[i].second << ") over (order by "
                         << windowFuncs[0].orderByCol << ") ";
                } else if (exprTypes[i] == 1) {
                    cout << aggItems[i].first << "(" << aggItems[i].second << ") ";
                } else {
                    cout << aggItems[i].second << " ";
                }
            }
            cout << '\n';

            // Fetch all data (need all columns for window function computation)
            set<string> allCols;
            vector<string> rawAnswers;
            if (condTokens.empty()) {
                rawAnswers = g_engine.query(s.currentDB, tname, {}, allCols, {});
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = g_engine.query(s.currentDB, tname, g, allCols, {});
                    for (const auto& row : part) {
                        if (seen.insert(row).second) rawAnswers.push_back(row);
                    }
                }
            }

            // Parse rows into column name -> value maps
            vector<map<string, string>> rows;
            for (const auto& rowStr : rawAnswers) {
                map<string, string> rowData;
                stringstream ss(rowStr);
                string val;
                for (size_t i = 0; i < tbl.len && ss >> val; ++i) {
                    rowData[tbl.cols[i].dataName] = val;
                }
                rows.push_back(rowData);
            }

            // Sort by window function ORDER BY column
            const WindowFunc& wf0 = windowFuncs[0];
            std::stable_sort(rows.begin(), rows.end(), [&](const auto& a, const auto& b) {
                auto itA = a.find(wf0.orderByCol);
                auto itB = b.find(wf0.orderByCol);
                if (itA == a.end() || itB == b.end()) return false;
                try {
                    int64_t va = stoll(itA->second);
                    int64_t vb = stoll(itB->second);
                    return wf0.orderByAsc ? (va < vb) : (va > vb);
                } catch (...) {
                    return wf0.orderByAsc ? (itA->second < itB->second) : (itA->second > itB->second);
                }
            });

            // Compute window function values
            for (size_t i = 0; i < rows.size(); ++i) {
                for (size_t wi = 0; wi < windowFuncs.size(); ++wi) {
                    const auto& wf = windowFuncs[wi];
                    string val;
                    if (wf.name == "row_number") {
                        val = to_string(i + 1);
                    } else if (wf.name == "rank") {
                        if (i == 0) {
                            val = "1";
                        } else {
                            auto itCur = rows[i].find(wf.orderByCol);
                            auto itPrev = rows[i - 1].find(wf.orderByCol);
                            if (itCur != rows[i].end() && itPrev != rows[i - 1].end()
                                && itCur->second == itPrev->second) {
                                val = rows[i - 1]["_win_" + to_string(wi)];
                            } else {
                                val = to_string(i + 1);
                            }
                        }
                    } else if (wf.name == "lag") {
                        if (i == 0) val = "NULL";
                        else {
                            auto it = rows[i - 1].find(wf.arg);
                            val = (it != rows[i - 1].end()) ? it->second : "NULL";
                        }
                    } else if (wf.name == "lead") {
                        if (i == rows.size() - 1) val = "NULL";
                        else {
                            auto it = rows[i + 1].find(wf.arg);
                            val = (it != rows[i + 1].end()) ? it->second : "NULL";
                        }
                    }
                    rows[i]["_win_" + to_string(wi)] = val;
                }
            }

            // Format results as strings
            vector<string> winAnswers;
            for (const auto& row : rows) {
                string line;
                for (size_t i = 0; i < aggItems.size(); ++i) {
                    if (exprTypes[i] == 2) {
                        size_t wi = 0;
                        for (size_t j = 0; j < i; ++j) {
                            if (exprTypes[j] == 2) wi++;
                        }
                        auto it = row.find("_win_" + to_string(wi));
                        if (it != row.end()) line += it->second + " ";
                    } else if (exprTypes[i] == 0) {
                        auto it = row.find(aggItems[i].second);
                        if (it != row.end()) line += it->second + " ";
                    }
                }
                if (!line.empty() && line.back() == ' ') line.pop_back();
                winAnswers.push_back(line);
            }

            // DISTINCT
            if (isDistinct) {
                vector<string> deduped;
                set<string> seen;
                for (const auto& row : winAnswers) {
                    if (seen.insert(row).second) deduped.push_back(row);
                }
                winAnswers = std::move(deduped);
            }

            // LIMIT / OFFSET
            size_t wlim = 0, woff = 0;
            parseLimitOffset(wlim, woff);
            if (woff < winAnswers.size()) {
                if (wlim > 0 && woff + wlim < winAnswers.size())
                    winAnswers.erase(winAnswers.begin() + woff + wlim, winAnswers.end());
                if (woff > 0)
                    winAnswers.erase(winAnswers.begin(), winAnswers.begin() + woff);
            }

            // Output
            for (const auto& row : winAnswers) {
                cout << row << endl;
                log(s.username, row, getTime());
            }
            return false;
        } else if (hasScalar) {
            for (const auto& expr : selectExprs) {
                cout << expr.displayName << ' ';
            }
            cout << '\n';
            if (condTokens.empty()) {
                answers = g_engine.queryExpr(s.currentDB, tname, {}, selectExprs, orderBySpecs);
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = g_engine.queryExpr(s.currentDB, tname, g, selectExprs, orderBySpecs);
                    for (const auto& row : part) {
                        if (seen.insert(row).second) answers.push_back(row);
                    }
                }
            }
        } else {
            for (size_t i = 0; i < tbl.len; ++i) {
                if (!selectAll && selectCols.find(tbl.cols[i].dataName) == selectCols.end()) continue;
                cout << tbl.cols[i].dataName << ' ';
            }
            cout << '\n';
            if (condTokens.empty()) {
                answers = g_engine.query(s.currentDB, tname, {}, selectCols, orderBySpecs);
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = g_engine.query(s.currentDB, tname, g, selectCols, orderBySpecs);
                    for (const auto& row : part) {
                        if (seen.insert(row).second) answers.push_back(row);
                    }
                }
            }
        }
        if (isDistinct) {
            vector<string> deduped;
            set<string> seen;
            for (const auto& row : answers) {
                if (seen.insert(row).second) deduped.push_back(row);
            }
            answers = std::move(deduped);
        }
        size_t nlim = 0, noff = 0;
        parseLimitOffset(nlim, noff);
        if (noff < answers.size()) {
            if (nlim > 0 && noff + nlim < answers.size())
                answers.erase(answers.begin() + noff + nlim, answers.end());
            if (noff > 0)
                answers.erase(answers.begin(), answers.begin() + noff);
        }
        if (!outfile.empty()) {
            ofstream ofs(outfile);
            if (!ofs) {
                cout << "Cannot open file for writing: " << outfile << endl;
                return true;
            }
            // Write header based on column selection
            if (!groupByCols.empty()) {
                bool first = true;
                for (const auto& gc : groupByCols) {
                    if (!first) ofs << ",";
                    first = false;
                    ofs << escapeCSVField(gc);
                }
                for (const auto& it : aggItems) {
                    if (!it.first.empty()) {
                        ofs << "," << escapeCSVField(it.first + "(" + it.second + ")");
                    }
                }
                ofs << "\n";
            } else if (hasAgg) {
                bool first = true;
                for (const auto& it : aggItems) {
                    if (!first) ofs << ",";
                    first = false;
                    if (it.first.empty()) ofs << escapeCSVField(it.second);
                    else ofs << escapeCSVField(it.first + "(" + it.second + ")");
                }
                ofs << "\n";
            } else if (hasScalar) {
                bool first = true;
                for (const auto& expr : selectExprs) {
                    if (!first) ofs << ",";
                    first = false;
                    ofs << escapeCSVField(expr.displayName);
                }
                ofs << "\n";
            } else {
                bool first = true;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (!selectAll && selectCols.find(tbl.cols[i].dataName) == selectCols.end()) continue;
                    if (!first) ofs << ",";
                    first = false;
                    ofs << escapeCSVField(tbl.cols[i].dataName);
                }
                ofs << "\n";
            }
            for (const auto& row : answers) {
                // Row values are space-separated; split and re-join as CSV
                stringstream rss(row);
                string val;
                bool first = true;
                while (rss >> val) {
                    if (!first) ofs << ",";
                    first = false;
                    ofs << escapeCSVField(val);
                }
                ofs << "\n";
            }
            ofs.close();
            cout << "Query result saved to " << outfile << " (" << answers.size() << " rows)" << endl;
        } else {
            for (const auto& row : answers) {
                cout << row << endl;
                log(s.username, row, getTime());
            }
        }
        return false;
    }

    if (sql.substr(0, 5) == "desc " || sql.substr(0, 9) == "describe ") {
        if (!checkDB(s)) return true;
        string tname = (sql.substr(0, 5) == "desc ") ? trim(sql.substr(5)) : trim(sql.substr(9));
        tname = resolveTableName(s, tname);
        TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
        if (tbl.len == 0) {
            cout << "Table not exist" << endl;
            return true;
        }
        cout << "field type null key default extra" << endl;
        for (size_t i = 0; i < tbl.len; ++i) {
            const auto& c = tbl.cols[i];
            cout << c.dataName << ' '
                 << c.dataType << ' '
                 << (c.isNull ? "yes" : "no") << ' '
                 << (c.isPrimaryKey ? "pri" : "") << ' '
                 << c.defaultValue << ' '
                 << (c.isAutoIncrement ? "auto_increment" : "")
                 << endl;
        }
        return false;
    }

    if (sql.substr(0, 4) == "view") {
        string rest = trim(sql.substr(4));
        vector<string> tokens = tokenize(rest);
        if (tokens.empty()) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string op = tokens[0];
        if (op == "table") {
            if (tokens.size() < 2) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string tname = resolveTableName(s, tokens[1]);
            TableSchema tbl = g_engine.getTableSchema(s.currentDB, tname);
            if (tbl.len == 0) {
                cout << "Table " << tokens[1] << " not exist" << endl;
                return true;
            }
            tbl.print();
            return false;
        }
        if (op == "database") {
            auto names = g_engine.getTableNames(s.currentDB);
            for (const auto& n : names) cout << n << endl;
            log(s.username, "view database", getTime());
            return false;
        }
        cout << "SQL syntax error" << endl;
        return true;
    }

    cout << "SQL syntax error" << endl;
    return true;
}

// ========================================================================
// Main
// ========================================================================
int main(int argc, char* argv[]) {
    // Server mode: ./dbms_main --server PORT
    if (argc >= 3 && std::string(argv[1]) == "--server") {
        int port = std::stoi(argv[2]);
        dbms::startServer(port);
        return 0;
    }

    Session s;
    string username, password;
    cout << "login" << endl;
    cin >> username >> password;
    if (login(username, password)) {
        s.username = username;
        log(s.username, "login", getTime());
        s.permission = permissionQuery(username);
        cin.ignore(numeric_limits<streamsize>::max(), '\n');
        int sqlCount = 0;
        const int CHECKPOINT_INTERVAL = 30;  // auto checkpoint every N SQLs
        while (true) {
            string sql;
            getline(cin, sql);
            if (trim(sql) == "exit") break;
            auto start = std::chrono::steady_clock::now();
            bool ok = execute(sql, s);
            auto end = std::chrono::steady_clock::now();
            double ms = std::chrono::duration<double, std::milli>(end - start).count();
            if (ms > 100.0) {
                logSlowQuery(sql, ms);
            }
            if (ok && !s.currentDB.empty()) {
                if (++sqlCount >= CHECKPOINT_INTERVAL) {
                    g_engine.checkpoint(s.currentDB);
                    sqlCount = 0;
                }
            }
        }
    } else {
        cout << "wrong username or password" << endl;
    }
    return 0;
}
