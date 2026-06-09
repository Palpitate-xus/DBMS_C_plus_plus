#pragma once

#include <ctime>
#include <fstream>
#include <iostream>
#include <string>

// 获取当前时间
inline std::string getTime() {
    time_t now = time(0);
    std::string now_dt = ctime(&now);
    return now_dt;
}

// 写入日志文件
inline int log(const std::string& user, const std::string& operation, const std::string& time) {
    std::ofstream oFile("dbms.log", std::ios::binary | std::ios::out | std::ios::app);
    if (!oFile) {
        std::cout << "error 1" << std::endl;
    }
    oFile << time << "    " << user << "    " << operation << std::endl;
    return 0;
}

// Audit log: timestamp, username, database, sql_type, sql_text, status
// auditLevel: 0=none, 1=DDL only, 2=DML+DDL, 3=all
inline void auditLog(int auditLevel, const std::string& user, const std::string& dbname,
                     const std::string& sql, const std::string& status) {
    if (auditLevel <= 0) return;
    // Classify SQL type
    std::string sqlType = "other";
    std::string lowerSql;
    for (char c : sql) lowerSql += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    bool isDDL = false, isDML = false;
    if (lowerSql.find("create") == 0 || lowerSql.find("drop") == 0 ||
        lowerSql.find("alter") == 0 || lowerSql.find("truncate") == 0) {
        isDDL = true; sqlType = "ddl";
    } else if (lowerSql.find("select") == 0 || lowerSql.find("insert") == 0 ||
               lowerSql.find("update") == 0 || lowerSql.find("delete") == 0 ||
               lowerSql.find("merge") == 0 || lowerSql.find("replace") == 0) {
        isDML = true; sqlType = "dml";
    } else if (lowerSql.find("grant") == 0 || lowerSql.find("revoke") == 0 ||
               lowerSql.find("create user") == 0 || lowerSql.find("drop user") == 0 ||
               lowerSql.find("alter user") == 0) {
        sqlType = "dcl";
    }
    if (auditLevel == 1 && !isDDL) return;
    if (auditLevel == 2 && !isDDL && !isDML) return;
    std::ofstream ofs("audit.log", std::ios::app);
    if (!ofs) return;
    time_t now = time(0);
    std::string ts = ctime(&now);
    if (!ts.empty() && ts.back() == '\n') ts.pop_back();
    // Truncate long SQL for readability
    std::string sqlTrunc = sql;
    if (sqlTrunc.size() > 200) sqlTrunc = sqlTrunc.substr(0, 197) + "...";
    ofs << ts << " | user=" << user << " | db=" << dbname
        << " | type=" << sqlType << " | status=" << status
        << " | sql=" << sqlTrunc << "\n";
}
