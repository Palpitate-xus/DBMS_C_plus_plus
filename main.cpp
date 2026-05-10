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

using namespace std;
using dbms::Column;
using dbms::makeDateColumn;
using dbms::makeIntColumn;
using dbms::makeStringColumn;
using dbms::OpResult;
using dbms::StorageEngine;
using dbms::TableSchema;

int g_nowPermission = 0;
string g_nowUser;
string g_currentDB = "info";
StorageEngine g_engine;
map<string, string> g_preparedStmts;

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
// SQL preprocessing
// ========================================================================
static string sqlProcessor(string raw) {
    raw = toLower(raw);
    raw.erase(remove(raw.begin(), raw.end(), '\n'), raw.end());
    raw.erase(remove(raw.begin(), raw.end(), '\t'), raw.end());
    raw.erase(remove(raw.begin(), raw.end(), '\r'), raw.end());
    if (!raw.empty() && raw.back() == ';') raw.pop_back();
    return raw;
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
            dbms::ForeignKey fk;

            if (isBrace) {
                // {col:type flags} format
                size_t colon = segment.find(':');
                if (colon == string::npos) break;
                cname = trim(segment.substr(0, colon));
                ctype = trim(segment.substr(colon + 1));
            } else {
                // (col type flags) format
                vector<string> parts;
                stringstream ss(segment);
                string part;
                while (ss >> part) parts.push_back(part);
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
                    if (lp != string::npos && rp != string::npos && rp > lp + 1) {
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

            if (ctype.substr(0, 3) == "int") {
                tbl.append(makeIntColumn(cname, isNull, 2, isPK));
            } else if (ctype.substr(0, 4) == "tiny") {
                tbl.append(makeIntColumn(cname, isNull, 1, isPK));
            } else if (ctype.substr(0, 4) == "long") {
                tbl.append(makeIntColumn(cname, isNull, 3, isPK));
            } else if (ctype.substr(0, 4) == "date") {
                tbl.append(makeDateColumn(cname, isNull, isPK));
            } else if (ctype.substr(0, 4) == "char") {
                size_t len = 0;
                size_t start = 4;
                // Skip optional '(' for standard SQL syntax: char(20)
                if (start < ctype.size() && ctype[start] == '(') ++start;
                for (size_t i = start; i < ctype.size() && isdigit(static_cast<unsigned char>(ctype[i])); ++i)
                    len = len * 10 + (ctype[i] - '0');
                if (len == 0) len = 1;
                tbl.append(makeStringColumn(cname, isNull, len, isPK));
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
static std::vector<std::string> runSubQuery(const std::string& rawSql) {
    std::string sql = sqlProcessor(rawSql);
    size_t fromPos = sql.find("from");
    if (fromPos == std::string::npos) return {};
    std::string columns = trim(sql.substr(6, fromPos - 6));

    size_t wherePos = sql.find("where", fromPos);
    size_t tnameEnd = (wherePos != std::string::npos) ? wherePos : sql.size();
    std::string tname = trim(sql.substr(fromPos + 4, tnameEnd - fromPos - 4));

    if (!g_engine.tableExists(g_currentDB, tname)) return {};

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
            auto part = g_engine.query(g_currentDB, tname, g, selectCols, "", true);
            for (const auto& row : part) {
                if (seen.insert(row).second) answers.push_back(row);
            }
        }
    } else {
        answers = g_engine.query(g_currentDB, tname, {}, selectCols, "", true);
    }

    for (auto& s : answers) {
        s = trim(s);
    }
    return answers;
}

// Expand IN (...) subqueries / value lists into OR conditions
static std::string expandSubqueries(std::string sql) {
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

        int depth = 1;
        size_t parenEnd = std::string::npos;
        for (size_t i = parenStart + 1; i < sql.size(); ++i) {
            if (sql[i] == '(') ++depth;
            else if (sql[i] == ')') { --depth; if (depth == 0) { parenEnd = i; break; } }
        }
        if (parenEnd == std::string::npos) break;

        size_t colStart = pos;
        while (colStart > 0 && std::isspace(static_cast<unsigned char>(sql[colStart - 1]))) --colStart;
        size_t colNameStart = colStart;
        while (colNameStart > 0 && !std::isspace(static_cast<unsigned char>(sql[colNameStart - 1]))) --colNameStart;
        std::string colName = trim(sql.substr(colNameStart, colStart - colNameStart));

        std::string inner = trim(sql.substr(parenStart + 1, parenEnd - parenStart - 1));
        std::vector<std::string> values;
        if (inner.size() >= 6 && inner.substr(0, 6) == "select") {
            values = runSubQuery(inner);
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
static bool checkAdmin() {
    if (g_nowPermission == 0) {
        cout << "permission denied" << endl;
        log(g_nowUser, "permission denied", getTime());
        return false;
    }
    return true;
}

static bool checkTablePermission(const string& tname, dbms::StorageEngine::TablePrivilege priv) {
    if (g_nowPermission == 1) return true; // admin bypass
    if (!g_engine.hasPermission(g_currentDB, tname, g_nowUser, priv)) {
        cout << "permission denied on table " << tname << endl;
        return false;
    }
    return true;
}

static bool checkDB() {
    if (!g_engine.databaseExists(g_currentDB)) {
        cout << "Invalid Database name:" << g_currentDB << endl;
        log(g_nowUser, "invalid database name", getTime());
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

bool execute(const string& rawSql) {
    auto start = std::chrono::steady_clock::now();
    string sql = sqlProcessor(rawSql);
    if (sql.substr(0, 3) == "use") {
        if (sql.substr(4, 8) == "database") {
            string dbname = trim(sql.substr(13));
            if (!g_engine.databaseExists(dbname)) {
                cout << "Database not found" << endl;
                g_currentDB = "";
                log(g_nowUser, "use database error", getTime());
                return true;
            }
            g_currentDB = dbname;
            cout << "set Database to " << dbname << endl;
            log(g_nowUser, "use database success", getTime());
            return false;
        }
    }

    if (sql.substr(0, 6) == "create") {
        if (sql.substr(7, 4) == "user") {
            if (!checkAdmin()) return true;
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
                log(g_nowUser, "error: user already exist", getTime());
                return true;
            }
            createUser(temp);
            cout << "create user  " << temp.username << "  succeeded" << endl;
            return false;
        }

        if (sql.substr(7, 8) == "database") {
            if (!checkAdmin()) return true;
            string dbname = trim(sql.substr(16));
            auto res = g_engine.createDatabase(dbname);
            if (res == OpResult::TableAlreadyExist) {
                cout << "Failed:Database " << dbname << " already exists" << endl;
                log(g_nowUser, "create database error", getTime());
            } else {
                cout << "Create Database succeeded" << endl;
                log(g_nowUser, "create database succeeded", getTime());
            }
            return res != OpResult::Success;
        }

        if (sql.substr(7, 5) == "table") {
            if (!checkAdmin()) return true;
            if (!checkDB()) return true;
            string rest = trim(sql.substr(13));
            size_t sp = rest.find(' ');
            if (sp == string::npos) {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string tname = rest.substr(0, sp);
            TableSchema tbl = parseTableColumns(sql, 13 + sp + 1);
            tbl.tablename = tname;
            auto res = g_engine.createTable(g_currentDB, tbl);
            if (res == OpResult::TableAlreadyExist) {
                cout << "Table " << tname << " already exists" << endl;
                log(g_nowUser, "table already exists", getTime());
                return true;
            }
            cout << "Table create succeeded" << endl;
            log(g_nowUser, "table create succeeded", getTime());
            return false;
        }

        if (sql.substr(7, 5) == "index") {
            if (!checkAdmin()) return true;
            if (!checkDB()) return true;
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
            string tname = trim(afterOn.substr(0, lp));
            string colname = trim(afterOn.substr(lp + 1, rp - lp - 1));
            auto res = g_engine.createIndex(g_currentDB, tname, colname);
            if (res != OpResult::Success) {
                cout << "Create index failed" << endl;
                return true;
            }
            cout << "Index created" << endl;
            return false;
        }

        if (sql.substr(7, 4) == "view") {
            if (!checkAdmin()) return true;
            if (!checkDB()) return true;
            // create view viewname as select ...
            string rest = trim(sql.substr(12));
            size_t asPos = rest.find(" as ");
            if (asPos == string::npos) {
                cout << "SQL syntax error: VIEW requires AS clause" << endl;
                return true;
            }
            string viewname = trim(rest.substr(0, asPos));
            string viewSql = trim(rest.substr(asPos + 4));
            auto res = g_engine.createView(g_currentDB, viewname, viewSql);
            if (res == OpResult::TableAlreadyExist) {
                cout << "View " << viewname << " already exists" << endl;
                return true;
            }
            cout << "View " << viewname << " created" << endl;
            return false;
        }
    }

    if (sql.substr(0, 5) == "alter") {
        if (!checkAdmin()) return true;
        if (!checkDB()) return true;
        vector<string> tokens = tokenize(sql.substr(5));
        if (tokens.size() < 4 || tokens[0] != "table") {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = tokens[1];
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
            } else {
                cout << "Unknown data type" << endl;
                return true;
            }
            auto res = g_engine.alterTableAddColumn(g_currentDB, tname, col);
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
            auto res = g_engine.alterTableDropColumn(g_currentDB, tname, tokens[4]);
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
        if (!checkDB()) return true;
        vector<string> tokens = tokenize(sql.substr(12));
        if (tokens.empty()) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = tokens[0];
        if (!g_engine.tableExists(g_currentDB, tname)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        if (!checkTablePermission(tname, dbms::StorageEngine::TablePrivilege::Insert)) return true;

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
        string valsStr = trim(sql.substr(valStart + 1, valEnd - valStart - 1));

        vector<string> cols;
        vector<string> vals;
        {
            stringstream css(colsStr);
            string item;
            while (getline(css, item, ',')) cols.push_back(trim(item));
        }
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

        auto res = g_engine.insert(g_currentDB, tname, values);
        if (res == OpResult::DuplicateKey) {
            cout << "Duplicate primary key" << endl;
            return true;
        }
        if (res != OpResult::Success) {
            cout << "Invalid data, please check" << endl;
            return true;
        }
        cout << "Data inserted" << endl;
        log(g_nowUser, "data inserted", getTime());
        return false;
    }

    if (sql.substr(0, 12) == "delete from ") {
        if (!checkDB()) return true;
        string delRest = expandSubqueries(sql.substr(12));
        vector<string> tokens = tokenize(normalizeConditionStr(delRest));
        if (tokens.empty()) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = tokens[0];
        if (!g_engine.tableExists(g_currentDB, tname)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        if (!checkTablePermission(tname, dbms::StorageEngine::TablePrivilege::Delete)) return true;

        tokens.erase(tokens.begin());
        if (tokens.empty()) {
            auto res = g_engine.remove(g_currentDB, tname, {});
            if (res != OpResult::Success) {
                cout << "Delete failed: foreign key constraint violation or other error" << endl;
                return true;
            }
            cout << "Delete done" << endl;
            log(g_nowUser, "delete done", getTime());
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
            auto res = g_engine.remove(g_currentDB, tname, g);
            if (res != OpResult::Success) {
                cout << "Delete failed: foreign key constraint violation or other error" << endl;
                return true;
            }
        }
        cout << "Delete done" << endl;
        log(g_nowUser, "delete done", getTime());
        return false;
    }

    if (sql.substr(0, 6) == "update") {
        if (!checkDB()) return true;
        size_t setPos = sql.find("set");
        if (setPos == std::string::npos) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = trim(sql.substr(6, setPos - 6));
        if (!g_engine.tableExists(g_currentDB, tname)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        if (!checkTablePermission(tname, dbms::StorageEngine::TablePrivilege::Update)) return true;

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
            whereClause = expandSubqueries(whereClause);
            whereClause = normalizeConditionStr(whereClause);
            vector<string> tokens = tokenize(whereClause);
            tokens.insert(tokens.begin(), "(");
            tokens.push_back(")");
            for (auto& t : tokens) t = modifyLogic(t);
            auto groups = breakDownConditions(tokens);
            for (const auto& g : groups) {
                auto res = g_engine.update(g_currentDB, tname, updates, g);
                if (res != OpResult::Success) {
                    cout << "Update failed" << endl;
                    return true;
                }
            }
        } else {
            auto res = g_engine.update(g_currentDB, tname, updates, {});
            if (res != OpResult::Success) {
                cout << "Update failed" << endl;
                return true;
            }
        }
        cout << "Update done" << endl;
        log(g_nowUser, "update done", getTime());
        return false;
    }

    if (sql.substr(0, 7) == "analyze") {
        if (!checkAdmin()) return true;
        if (!checkDB()) return true;
        string rest = trim(sql.substr(7));
        if (rest.substr(0, 5) != "table") {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = trim(rest.substr(5));
        g_engine.analyzeTable(g_currentDB, tname);
        cout << "Table " << tname << " analyzed" << endl;
        return false;
    }

    if (sql.substr(0, 5) == "begin") {
        if (!checkDB()) return true;
        auto res = g_engine.beginTransaction(g_currentDB);
        if (res != OpResult::Success) {
            cout << "Begin transaction failed" << endl;
            return true;
        }
        cout << "Transaction started" << endl;
        log(g_nowUser, "begin transaction", getTime());
        return false;
    }

    if (sql.substr(0, 6) == "commit") {
        auto res = g_engine.commitTransaction();
        if (res != OpResult::Success) {
            cout << "Commit failed" << endl;
            return true;
        }
        cout << "Transaction committed" << endl;
        log(g_nowUser, "commit transaction", getTime());
        return false;
    }

    if (sql.substr(0, 8) == "rollback") {
        auto res = g_engine.rollbackTransaction();
        if (res != OpResult::Success) {
            cout << "Rollback failed" << endl;
            return true;
        }
        cout << "Transaction rolled back" << endl;
        log(g_nowUser, "rollback transaction", getTime());
        return false;
    }

    if (sql.substr(0, 4) == "drop") {
        if (!checkAdmin()) return true;
        if (!checkDB()) return true;
        vector<string> tokens = tokenize(sql.substr(4));
        if (tokens.size() < 2) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string op = tokens[0];
        string name = tokens[1];
        if (op == "table") {
            auto res = g_engine.dropTable(g_currentDB, name);
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
            log(g_nowUser, "database dropped", getTime());
            return false;
        }
        if (op == "index") {
            // drop index idxname on tname
            if (tokens.size() < 3 || tokens[2] != "on") {
                cout << "SQL syntax error" << endl;
                return true;
            }
            string tname = tokens[3];
            auto res = g_engine.dropIndex(g_currentDB, tname, name);
            if (res != OpResult::Success) {
                cout << "Drop index failed" << endl;
                return true;
            }
            cout << "Index dropped" << endl;
            return false;
        }
        if (op == "view") {
            auto res = g_engine.dropView(g_currentDB, name);
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
        if (!checkDB()) return true;
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
        execute(leftSql);
        cout.rdbuf(oldBuf);
        // Execute right query, capture output
        stringstream rightSs;
        cout.rdbuf(rightSs.rdbuf());
        execute(rightSql);
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
        cout << "Unknown SHOW command" << endl;
        return true;
    }

    // GRANT privilege ON table TO user
    if (sql.substr(0, 6) == "grant ") {
        if (!checkAdmin()) return true;
        if (!checkDB()) return true;
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
        g_engine.grant(g_currentDB, tname, uname, priv);
        cout << "Granted " << privStr << " on " << tname << " to " << uname << endl;
        return false;
    }

    // REVOKE privilege ON table FROM user
    if (sql.substr(0, 7) == "revoke ") {
        if (!checkAdmin()) return true;
        if (!checkDB()) return true;
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
        g_engine.revoke(g_currentDB, tname, uname, priv);
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
        g_preparedStmts[stmtName] = templateSql;
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
        auto it = g_preparedStmts.find(stmtName);
        if (it == g_preparedStmts.end()) {
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
        return execute(expanded);
    }

    // DEALLOCATE PREPARE stmt_name
    if (sql.substr(0, 19) == "deallocate prepare ") {
        string stmtName = trim(sql.substr(19));
        if (g_preparedStmts.erase(stmtName)) {
            cout << "Statement " << stmtName << " deallocated" << endl;
        } else {
            cout << "Prepared statement " << stmtName << " not found" << endl;
        }
        return false;
    }

    // LOAD DATA INFILE: import CSV
    if (sql.substr(0, 17) == "load data infile ") {
        if (!checkAdmin()) return true;
        if (!checkDB()) return true;
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
        if (!g_engine.tableExists(g_currentDB, tname)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        TableSchema tbl = g_engine.getTableSchema(g_currentDB, tname);
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
            auto res = g_engine.insert(g_currentDB, tname, values);
            if (res == OpResult::Success) imported++;
            else skipped++;
        }
        cout << "Imported " << imported << " rows, skipped " << skipped << endl;
        return false;
    }

    // EXPLAIN: show query plan without executing
    if (sql.substr(0, 7) == "explain") {
        if (!checkDB()) return true;
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
            stringstream css(columns);
            string item;
            while (getline(css, item, ',')) selectCols.insert(trim(item));
        }
        dbms::PlanContext ctx;
        ctx.dbname = g_currentDB;
        ctx.tablename = tname;
        ctx.conds = dbms::StorageEngine::parseConditions(conds);
        ctx.selectCols = selectCols;
        ctx.orderByCol = orderByCol;
        ctx.orderByAsc = orderByAsc;
        ctx.limit = limitVal;
        ctx.distinct = isDistinct;
        auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
        cout << dbms::QueryPlanner::explain(plan, &g_engine, g_currentDB);
        return false;
    }

    if (sql.substr(0, 6) == "select") {
        if (!checkDB()) return true;

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
        size_t innerJoinPos = sql.find("inner join", fromPos);
        size_t joinPos = sql.find("join", fromPos);

        enum class JoinType { Inner, Left, Right };
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
            string leftTable = trim(sql.substr(fromPos + 4, actualJoinPos - fromPos - 4));
            size_t onPos = sql.find("on", actualJoinPos);
            if (onPos == string::npos) {
                cout << "SQL syntax error: missing ON clause" << endl;
                return true;
            }
            size_t tableNameStart = actualJoinPos;
            if (jt == JoinType::Left) tableNameStart += 9;
            else if (jt == JoinType::Right) tableNameStart += 10;
            else if (jt == JoinType::Inner) tableNameStart += 10;
            else tableNameStart += 4;
            string rightTable = trim(sql.substr(tableNameStart, onPos - tableNameStart));
            size_t onEnd = (wherePos != string::npos) ? wherePos
                          : (orderPos != string::npos) ? orderPos : sql.size();
            string onClause = normalizeConditionStr(trim(sql.substr(onPos + 2, onEnd - onPos - 2)));

            size_t eqPos = onClause.find('=');
            if (eqPos == string::npos) {
                cout << "SQL syntax error: invalid ON clause" << endl;
                return true;
            }
            string leftOnCol = trim(onClause.substr(0, eqPos));
            string rightOnCol = trim(onClause.substr(eqPos + 1));
            size_t dot = leftOnCol.find('.');
            if (dot != string::npos) leftOnCol = leftOnCol.substr(dot + 1);
            dot = rightOnCol.find('.');
            if (dot != string::npos) rightOnCol = rightOnCol.substr(dot + 1);

            if (!g_engine.tableExists(g_currentDB, leftTable) ||
                !g_engine.tableExists(g_currentDB, rightTable)) {
                cout << "Table not exist" << endl;
                return true;
            }
            if (!checkTablePermission(leftTable, dbms::StorageEngine::TablePrivilege::Select)) return true;
            if (!checkTablePermission(rightTable, dbms::StorageEngine::TablePrivilege::Select)) return true;

            set<string> selectCols;
            bool selectAll = (columns == "*");
            if (!selectAll) {
                stringstream css(columns);
                string item;
                while (getline(css, item, ',')) {
                    item = trim(item);
                    selectCols.insert(item);
                }
            }

            vector<string> condTokens;
            if (wherePos != string::npos) {
                size_t condEnd = (orderPos != string::npos) ? orderPos : sql.size();
                string whereClause = trim(sql.substr(wherePos + 5, condEnd - wherePos - 5));
                whereClause = expandSubqueries(whereClause);
                string condStr = normalizeConditionStr(whereClause);
                condTokens = tokenize(condStr);
            }

            TableSchema leftTbl = g_engine.getTableSchema(g_currentDB, leftTable);
            TableSchema rightTbl = g_engine.getTableSchema(g_currentDB, rightTable);
            if (selectAll) {
                for (size_t i = 0; i < leftTbl.len; ++i)
                    cout << leftTable << "." << leftTbl.cols[i].dataName << ' ';
                for (size_t i = 0; i < rightTbl.len; ++i)
                    cout << rightTable << "." << rightTbl.cols[i].dataName << ' ';
            } else {
                for (const auto& c : selectCols) cout << c << ' ';
            }
            cout << '\n';

            vector<string> answers;
            auto runJoin = [&](const vector<string>& conds) -> vector<string> {
                if (jt == JoinType::Left) {
                    return g_engine.leftJoin(g_currentDB, leftTable, rightTable,
                                              leftOnCol, rightOnCol, conds, selectCols);
                } else if (jt == JoinType::Right) {
                    return g_engine.rightJoin(g_currentDB, leftTable, rightTable,
                                               leftOnCol, rightOnCol, conds, selectCols);
                } else {
                    return g_engine.join(g_currentDB, leftTable, rightTable,
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
                log(g_nowUser, row, getTime());
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
        string tname = trim(sql.substr(fromPos + 4, tnameEnd - fromPos - 4));

        if (!g_engine.tableExists(g_currentDB, tname)) {
            if (g_engine.viewExists(g_currentDB, tname)) {
                string viewSql = g_engine.getViewSQL(g_currentDB, tname);
                if (!viewSql.empty()) {
                    execute(viewSql);
                    return false;
                }
            }
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }
        if (!checkTablePermission(tname, dbms::StorageEngine::TablePrivilege::Select)) return true;

        string orderByCol;
        bool orderByAsc = true;
        if (orderPos != string::npos) {
            size_t orderEnd = (limitPos != string::npos) ? limitPos
                            : (offsetPos != string::npos) ? offsetPos : sql.size();
            string orderRest = trim(sql.substr(orderPos + 8, orderEnd - orderPos - 8));
            vector<string> ot = tokenize(orderRest);
            if (!ot.empty()) orderByCol = ot[0];
            if (ot.size() > 1 && ot[1] == "desc") orderByAsc = false;
        }

        string groupByCol;
        if (groupPos != string::npos) {
            size_t groupEnd = (havingPos != string::npos) ? havingPos
                            : (orderPos != string::npos) ? orderPos
                            : (limitPos != string::npos) ? limitPos
                            : (offsetPos != string::npos) ? offsetPos : sql.size();
            groupByCol = trim(sql.substr(groupPos + 8, groupEnd - groupPos - 8));
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

        TableSchema tbl = g_engine.getTableSchema(g_currentDB, tname);
        set<string> selectCols;
        bool selectAll = (columns == "*");

        // Detect aggregate functions in columns
        vector<pair<string, string>> aggItems;
        bool hasAgg = false;
        {
            stringstream css(columns);
            string item;
            while (getline(css, item, ',')) {
                item = trim(item);
                size_t lp = item.find('(');
                size_t rp = item.find(')');
                if (lp != string::npos && rp != string::npos && rp > lp + 1) {
                    string func = item.substr(0, lp);
                    string arg = item.substr(lp + 1, rp - lp - 1);
                    aggItems.emplace_back(func, arg);
                    hasAgg = true;
                } else {
                    aggItems.emplace_back("", item);
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

        // WHERE clause
        vector<string> condTokens;
        if (wherePos != string::npos) {
            size_t condEnd = (groupPos != string::npos) ? groupPos
                           : (havingPos != string::npos) ? havingPos
                           : (orderPos != string::npos) ? orderPos
                           : (limitPos != string::npos) ? limitPos
                           : (offsetPos != string::npos) ? offsetPos : sql.size();
            string whereClause = trim(sql.substr(wherePos + 5, condEnd - wherePos - 5));
            whereClause = expandSubqueries(whereClause);
            string condStr = normalizeConditionStr(whereClause);
            condTokens = tokenize(condStr);
        }

        vector<string> answers;
        if (!groupByCol.empty()) {
            cout << groupByCol << ' ';
            vector<pair<string, string>> pureAgg;
            for (const auto& it : aggItems) {
                if (!it.first.empty()) {
                    cout << it.first << '(' << it.second << ") ";
                    pureAgg.push_back(it);
                }
            }
            cout << '\n';
            if (condTokens.empty()) {
                answers = g_engine.groupAggregate(g_currentDB, tname, {}, pureAgg, groupByCol, havingConds);
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = g_engine.groupAggregate(g_currentDB, tname, g, pureAgg, groupByCol, havingConds);
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
                answers = g_engine.aggregate(g_currentDB, tname, {}, pureAgg);
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = g_engine.aggregate(g_currentDB, tname, g, pureAgg);
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
                answers = g_engine.query(g_currentDB, tname, {}, selectCols, orderByCol, orderByAsc);
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = g_engine.query(g_currentDB, tname, g, selectCols, orderByCol, orderByAsc);
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
            if (!groupByCol.empty()) {
                ofs << escapeCSVField(groupByCol);
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
                log(g_nowUser, row, getTime());
            }
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
            TableSchema tbl = g_engine.getTableSchema(g_currentDB, tokens[1]);
            if (tbl.len == 0) {
                cout << "Table " << tokens[1] << " not exist" << endl;
                return true;
            }
            tbl.print();
            return false;
        }
        if (op == "database") {
            auto names = g_engine.getTableNames(g_currentDB);
            for (const auto& n : names) cout << n << endl;
            log(g_nowUser, "view database", getTime());
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

    string username, password;
    cout << "login" << endl;
    cin >> username >> password;
    if (login(username, password)) {
        g_nowUser = username;
        log(g_nowUser, "login", getTime());
        g_nowPermission = permissionQuery(username);
        cin.ignore(numeric_limits<streamsize>::max(), '\n');
        while (true) {
            string sql;
            getline(cin, sql);
            if (trim(sql) == "exit") break;
            auto start = std::chrono::steady_clock::now();
            execute(sql);
            auto end = std::chrono::steady_clock::now();
            double ms = std::chrono::duration<double, std::milli>(end - start).count();
            if (ms > 100.0) {
                logSlowQuery(sql, ms);
            }
        }
    } else {
        cout << "wrong username or password" << endl;
    }
    return 0;
}
