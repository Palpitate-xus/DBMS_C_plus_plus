#pragma once

#include <map>
#include <string>

// Per-connection session context.
// Replaces the previous global session variables (g_nowUser, g_nowPermission, etc.)
struct Session {
    std::string username;
    int permission = 0;
    std::string currentDB = "info";
    std::map<std::string, std::string> preparedStmts;
};
