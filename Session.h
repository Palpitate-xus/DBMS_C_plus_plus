#pragma once

#include <map>
#include <set>
#include <string>

// Per-connection session context.
// Replaces the previous global session variables (g_nowUser, g_nowPermission, etc.)
struct Session {
    std::string username;
    int permission = 0;
    std::string currentDB = "info";
    std::map<std::string, std::string> preparedStmts;
    int isolationLevel = 2; // 0=READ UNCOMMITTED, 1=READ COMMITTED, 2=REPEATABLE READ, 3=SERIALIZABLE
    std::set<std::string> tempTables; // temporary table names in this session
};
