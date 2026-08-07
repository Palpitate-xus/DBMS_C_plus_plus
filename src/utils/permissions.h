#pragma once

#include <iostream>
#include <string>
#include <vector>
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "common/scram_sha256.h"

extern dbms::StorageEngine g_engine;

struct user {
    std::string username;
    std::string password;
    std::string permission;
};

// Authentication and role state is cluster-scoped and stored in the catalog
// database. The old user.dat/role.dat files are intentionally not read or
// written: this release has a hard storage boundary and does not provide a
// legacy authentication migration path.

inline dbms::CatalogManager& authCatalog() {
    return g_engine.catalogService().get("info");
}

inline void persistAuthCatalog() {
    authCatalog().persistAll();
}

inline bool roleExists(const std::string& roleName) {
    return authCatalog().findAuthIdByName(roleName) != nullptr;
}

inline int createRole(const std::string& roleName) {
    if (roleExists(roleName)) return -1; // already exists
    dbms::PgAuthIdRow row;
    row.rolname = roleName;
    row.rolcanlogin = false;
    authCatalog().createAuthId(row);
    persistAuthCatalog();
    return 0;
}

inline bool dropRole(const std::string& roleName) {
    const auto* role = authCatalog().findAuthIdByName(roleName);
    if (!role) return false;
    const bool ok = authCatalog().dropAuthId(role->oid);
    if (ok) persistAuthCatalog();
    return ok;
}

inline int grantRoleToUser(const std::string& roleName, const std::string& username) {
    const auto* role = authCatalog().findAuthIdByName(roleName);
    const auto* member = authCatalog().findAuthIdByName(username);
    if (!role || !member) return -1;
    for (const auto& relation : authCatalog().findAuthMembers(role->oid)) {
        if (relation.member == member->oid) return -2;
    }
    dbms::PgAuthMembersRow relation;
    relation.roleid = role->oid;
    relation.member = member->oid;
    relation.grantor = role->oid;
    authCatalog().addAuthMember(relation);
    persistAuthCatalog();
    return 0;
}

inline bool revokeRoleFromUser(const std::string& roleName, const std::string& username) {
    const auto* role = authCatalog().findAuthIdByName(roleName);
    const auto* member = authCatalog().findAuthIdByName(username);
    if (!role || !member) return false;
    const bool ok = authCatalog().removeAuthMember(role->oid, member->oid);
    if (ok) persistAuthCatalog();
    return ok;
}

inline std::vector<std::string> getUserRoles(const std::string& username) {
    std::vector<std::string> roles;
    const auto* member = authCatalog().findAuthIdByName(username);
    if (!member) return roles;
    for (const auto& relation : authCatalog().findAuthMemberships(member->oid)) {
        const auto* role = authCatalog().findAuthId(relation.roleid);
        if (role) roles.push_back(role->rolname);
    }
    return roles;
}

inline bool userHasRole(const std::string& username, const std::string& roleName) {
    const auto* role = authCatalog().findAuthIdByName(roleName);
    const auto* member = authCatalog().findAuthIdByName(username);
    if (!role || !member) return false;
    for (const auto& relation : authCatalog().findAuthMembers(role->oid)) {
        if (relation.member == member->oid) return true;
    }
    return false;
}

inline bool userIsAdminViaRole(const std::string& username) {
    const auto* member = authCatalog().findAuthIdByName(username);
    if (member && member->rolsuper) return true;
    return userHasRole(username, "admin");
}

inline int checkPasswordStrength(const std::string& pw) {
    // Returns 0-100 score based on complexity
    int score = 0;
    if (pw.length() >= 6) score += 20;
    if (pw.length() >= 8) score += 20;
    bool hasLower = false, hasUpper = false, hasDigit = false, hasSpecial = false;
    for (char c : pw) {
        if (c >= 'a' && c <= 'z') hasLower = true;
        else if (c >= 'A' && c <= 'Z') hasUpper = true;
        else if (c >= '0' && c <= '9') hasDigit = true;
        else hasSpecial = true;
    }
    if (hasLower) score += 15;
    if (hasUpper) score += 15;
    if (hasDigit) score += 15;
    if (hasSpecial) score += 15;
    return score;
}

inline std::string passwordStrengthMessage(int score) {
    if (score >= 80) return "strong";
    if (score >= 50) return "medium";
    return "weak";
}

// Verify credentials without producing frontend-specific output. Network
// protocols must return authentication failures on their own wire, not via
// the process-wide stdout stream used by the legacy interactive frontend.
inline bool getStoredUserPassword(const std::string& username, std::string& storedPassword) {
    const auto* account = authCatalog().findAuthIdByName(username);
    if (!account || !account->rolcanlogin) return false;
    storedPassword = account->rolpassword;
    return true;
}

inline bool verifyUserPassword(const std::string& username, const std::string& password) {
    std::string storedPassword;
    if (!getStoredUserPassword(username, storedPassword)) return false;
    return storedPassword.rfind("SCRAM-SHA-256$", 0) == 0 &&
           dbms::scram::verifyPassword(password, storedPassword);
}

inline int login(const std::string& username, const std::string& password) {
    if (verifyUserPassword(username, password)) {
        std::cout << "successfully login" << std::endl;
        return 1;
    }
    std::cout << "wrong username or password" << std::endl;
    return -1;
}

inline int permissionQuery(const std::string& username) {
    const auto* account = authCatalog().findAuthIdByName(username);
    if (!account) return -1;
    return (account->rolsuper || account->rolname == "admin") ? 1 : 0;
}

inline int createUser(const user& new_user) {
    if (roleExists(new_user.username)) return -1;
    std::string hashedPw;
    if (new_user.password.rfind("SCRAM-SHA-256$", 0) == 0) {
        hashedPw = new_user.password;
    } else {
        hashedPw = dbms::scram::makeRandomVerifier(new_user.password);
    }
    dbms::PgAuthIdRow row;
    row.rolname = new_user.username;
    row.rolpassword = hashedPw;
    row.rolcanlogin = true;
    row.rolsuper = new_user.permission == "1" || new_user.permission == "admin";
    row.rolcreaterole = row.rolsuper;
    row.rolcreatedb = row.rolsuper;
    authCatalog().createAuthId(row);
    persistAuthCatalog();
    return 0;
}

inline bool renameUser(const std::string& oldName, const std::string& newName) {
    const auto* account = authCatalog().findAuthIdByName(oldName);
    if (!account || authCatalog().findAuthIdByName(newName)) return false;
    dbms::PgAuthIdRow updated = *account;
    updated.rolname = newName;
    const bool ok = authCatalog().updateAuthId(account->oid, updated);
    if (ok) persistAuthCatalog();
    return ok;
}

inline bool deleteUser(const std::string& username) {
    return dropRole(username);
}
