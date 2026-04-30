#include <algorithm>
#include <cctype>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "TableManage.h"
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

static int g_nowPermission = 0;
static string g_nowUser;
static string g_currentDB = "info";
static StorageEngine g_engine;

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
    return raw;
}

// ========================================================================
// Normalize condition string: remove spaces around operators so tokenize
// keeps each condition as a single token (e.g. "score > 80" → "score>80")
// ========================================================================
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
    return s;
}

// ========================================================================
// Condition conversion: "col=value" → "=col value"
// ========================================================================
static string modifyLogic(const string& logic) {
    if (logic == "(" || logic == ")" || logic == "and" || logic == "or") return logic;
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
        string left = operandStack.back(); operandStack.pop_back();
        if (cur.groups.empty()) {
            cur.groups.push_back({left, right});
        } else {
            for (auto& g : cur.groups) g.push_back(right);
        }
    };

    auto applyOr = [&](const string& right) {
        Frame& cur = stack.back();
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
        if (sql[pos] != '{') { ++pos; continue; }
        ++pos;
        while (pos < sql.size()) {
            size_t colon = sql.find(':', pos);
            size_t comma = sql.find(',', pos);
            size_t brace = sql.find('}', pos);
            size_t endPos = min({comma, brace});
            if (colon == string::npos || endPos == string::npos || colon > endPos) break;

            string cname = trim(sql.substr(pos, colon - pos));
            string ctype = trim(sql.substr(colon + 1, endPos - colon - 1));
            if (cname.empty() || ctype.empty()) break;

            // Parse type and flags (null flag + PK) and foreign key
            size_t fkPos = ctype.find("->");
            string typeAndFlags = (fkPos == string::npos) ? ctype : trim(ctype.substr(0, fkPos));
            string fkStr = (fkPos == string::npos) ? "" : trim(ctype.substr(fkPos + 2));

            size_t sp = typeAndFlags.find(' ');
            string typeName = (sp == string::npos) ? typeAndFlags : trim(typeAndFlags.substr(0, sp));
            string flagsStr = (sp == string::npos) ? "" : trim(typeAndFlags.substr(sp + 1));
            bool isNull = true;
            bool isPK = false;
            {
                stringstream fs(flagsStr);
                string f;
                while (fs >> f) {
                    if (f == "0") isNull = false;
                    if (f == "pk" || f == "PK") isPK = true;
                }
            }

            // Parse foreign key: "refTable(refCol)"
            dbms::ForeignKey fk;
            if (!fkStr.empty()) {
                size_t lp = fkStr.find('(');
                size_t rp = fkStr.find(')');
                if (lp != string::npos && rp != string::npos && rp > lp + 1) {
                    fk.colName = cname;
                    fk.refTable = trim(fkStr.substr(0, lp));
                    fk.refCol = trim(fkStr.substr(lp + 1, rp - lp - 1));
                    fk.onDelete = "restrict";
                }
            }

            if (typeName.substr(0, 3) == "int") {
                tbl.append(makeIntColumn(cname, isNull, 2, isPK));
            } else if (typeName.substr(0, 4) == "tiny") {
                tbl.append(makeIntColumn(cname, isNull, 1, isPK));
            } else if (typeName.substr(0, 4) == "long") {
                tbl.append(makeIntColumn(cname, isNull, 3, isPK));
            } else if (typeName.substr(0, 4) == "date") {
                tbl.append(makeDateColumn(cname, isNull, isPK));
            } else if (typeName.substr(0, 4) == "char") {
                size_t len = 0;
                for (size_t i = 4; i < typeName.size() && isdigit(static_cast<unsigned char>(typeName[i])); ++i)
                    len = len * 10 + (typeName[i] - '0');
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

static bool checkDB() {
    if (!g_engine.databaseExists(g_currentDB)) {
        cout << "Invalid Database name:" << g_currentDB << endl;
        log(g_nowUser, "invalid database name", getTime());
        return false;
    }
    return true;
}

static bool execute(const string& rawSql) {
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
        vector<string> tokens = tokenize(normalizeConditionStr(sql.substr(12)));
        if (tokens.empty()) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string tname = tokens[0];
        if (!g_engine.tableExists(g_currentDB, tname)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }

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

        size_t wherePos = sql.find("where", setPos);
        auto updates = parseSetClause(sql, setPos + 3,
                                       (wherePos == std::string::npos) ? sql.size() : wherePos);
        if (updates.empty()) {
            cout << "SQL syntax error" << endl;
            return true;
        }

        vector<string> conds;
        if (wherePos != std::string::npos) {
            string whereClause = normalizeConditionStr(trim(sql.substr(wherePos + 5)));
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
        cout << "SQL syntax error" << endl;
        return true;
    }

    if (sql.substr(0, 6) == "select") {
        if (!checkDB()) return true;
        size_t fromPos = sql.find("from");
        if (fromPos == string::npos) {
            cout << "SQL syntax error" << endl;
            return true;
        }
        string columns = trim(sql.substr(6, fromPos - 6));

        size_t wherePos = sql.find("where", fromPos);
        size_t orderPos = sql.find("order by", fromPos);

        // Check for JOIN
        size_t joinPos = sql.find("join", fromPos);
        bool isJoin = (joinPos != string::npos &&
                       (wherePos == string::npos || joinPos < wherePos) &&
                       (orderPos == string::npos || joinPos < orderPos));

        if (isJoin) {
            // select cols from t1 join t2 on t1.col = t2.col [where ...]
            string leftTable = trim(sql.substr(fromPos + 4, joinPos - fromPos - 4));
            size_t onPos = sql.find("on", joinPos);
            if (onPos == string::npos) {
                cout << "SQL syntax error: missing ON clause" << endl;
                return true;
            }
            string rightTable = trim(sql.substr(joinPos + 4, onPos - joinPos - 4));
            size_t onEnd = (wherePos != string::npos) ? wherePos
                          : (orderPos != string::npos) ? orderPos : sql.size();
            string onClause = normalizeConditionStr(trim(sql.substr(onPos + 2, onEnd - onPos - 2)));

            // Parse ON clause: "t1.col=t2.col" (normalize removes spaces around =)
            size_t eqPos = onClause.find('=');
            if (eqPos == string::npos) {
                cout << "SQL syntax error: invalid ON clause" << endl;
                return true;
            }
            string leftOnCol = trim(onClause.substr(0, eqPos));
            string rightOnCol = trim(onClause.substr(eqPos + 1));
            // Strip table prefix if present for the join column args
            size_t dot = leftOnCol.find('.');
            if (dot != string::npos) leftOnCol = leftOnCol.substr(dot + 1);
            dot = rightOnCol.find('.');
            if (dot != string::npos) rightOnCol = rightOnCol.substr(dot + 1);

            if (!g_engine.tableExists(g_currentDB, leftTable) ||
                !g_engine.tableExists(g_currentDB, rightTable)) {
                cout << "Table not exist" << endl;
                return true;
            }

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

            // WHERE clause
            vector<string> condTokens;
            if (wherePos != string::npos) {
                size_t condEnd = (orderPos != string::npos) ? orderPos : sql.size();
                string condStr = normalizeConditionStr(trim(sql.substr(wherePos + 5, condEnd - wherePos - 5)));
                condTokens = tokenize(condStr);
            }

            // Print header
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
            if (condTokens.empty()) {
                answers = g_engine.join(g_currentDB, leftTable, rightTable,
                                         leftOnCol, rightOnCol, {}, selectCols);
            } else {
                condTokens.insert(condTokens.begin(), "(");
                condTokens.push_back(")");
                for (auto& t : condTokens) t = modifyLogic(t);
                auto groups = breakDownConditions(condTokens);
                set<string> seen;
                for (const auto& g : groups) {
                    auto part = g_engine.join(g_currentDB, leftTable, rightTable,
                                               leftOnCol, rightOnCol, g, selectCols);
                    for (const auto& row : part) {
                        if (seen.insert(row).second) answers.push_back(row);
                    }
                }
            }
            for (const auto& row : answers) {
                cout << row << endl;
                log(g_nowUser, row, getTime());
            }
            return false;
        }

        // Non-JOIN query
        size_t tnameEnd = (wherePos != string::npos) ? wherePos
                         : (orderPos != string::npos) ? orderPos : sql.size();
        string tname = trim(sql.substr(fromPos + 4, tnameEnd - fromPos - 4));

        if (!g_engine.tableExists(g_currentDB, tname)) {
            cout << "Table " << tname << " not exist" << endl;
            return true;
        }

        string orderByCol;
        bool orderByAsc = true;
        if (orderPos != string::npos) {
            string orderRest = trim(sql.substr(orderPos + 8));
            vector<string> ot = tokenize(orderRest);
            if (!ot.empty()) orderByCol = ot[0];
            if (ot.size() > 1 && ot[1] == "desc") orderByAsc = false;
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
            size_t condEnd = (orderPos != string::npos) ? orderPos : sql.size();
            string condStr = normalizeConditionStr(trim(sql.substr(wherePos + 5, condEnd - wherePos - 5)));
            condTokens = tokenize(condStr);
        }

        vector<string> answers;
        if (hasAgg) {
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
        for (const auto& row : answers) {
            cout << row << endl;
            log(g_nowUser, row, getTime());
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
int main() {
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
            execute(sql);
        }
    } else {
        cout << "wrong username or password" << endl;
    }
    return 0;
}
