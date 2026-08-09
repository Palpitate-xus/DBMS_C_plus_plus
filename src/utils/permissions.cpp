#include "permissions.h"

#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "common/scram_sha256.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <unordered_set>

extern dbms::StorageEngine g_engine;

dbms::CatalogManager& authCatalog() {
    return g_engine.catalogService().get("info");
}

void persistAuthCatalog() {
    authCatalog().persistAll();
}

namespace {

std::optional<dbms::PgAuthIdRow> authByName(const std::string& name) {
    return authCatalog().getAuthIdByName(name);
}

bool roleMemberRecursive(dbms::Oid memberOid, dbms::Oid roleOid,
                         std::unordered_set<dbms::Oid>& visited) {
    if (!visited.insert(memberOid).second) return false;
    for (const auto& membership : authCatalog().findAuthMemberships(memberOid)) {
        if (membership.roleid == roleOid) return true;
        if (roleMemberRecursive(membership.roleid, roleOid, visited)) return true;
    }
    return false;
}

bool parseValidUntil(const std::string& value,
                    std::chrono::system_clock::time_point& result) {
    if (value.empty() || value == "infinity") return false;
    std::string input = value;
    if (!input.empty() && input.back() == 'Z') input.pop_back();
    std::tm tm{};
    std::istringstream stream(input);
    stream >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    if (stream.fail()) {
        stream.clear();
        stream.str(input);
        stream >> std::get_time(&tm, "%Y-%m-%d");
    }
    if (stream.fail()) return false;
#if defined(_WIN32)
    const std::time_t seconds = _mkgmtime(&tm);
#else
    const std::time_t seconds = timegm(&tm);
#endif
    if (seconds == static_cast<std::time_t>(-1)) return false;
    result = std::chrono::system_clock::from_time_t(seconds);
    return true;
}

bool loginAllowed(const dbms::PgAuthIdRow& account) {
    if (!account.rolcanlogin) return false;
    if (account.rolvaliduntil.empty() || account.rolvaliduntil == "infinity") return true;
    std::chrono::system_clock::time_point expiry;
    if (!parseValidUntil(account.rolvaliduntil, expiry)) return false;
    return std::chrono::system_clock::now() < expiry;
}

} // namespace

bool roleExists(const std::string& roleName) {
    return authByName(roleName).has_value();
}

int createRole(const std::string& roleName) {
    if (roleExists(roleName)) return -1;
    dbms::PgAuthIdRow row;
    row.rolname = roleName;
    row.rolcanlogin = false;
    authCatalog().createAuthId(row);
    persistAuthCatalog();
    return 0;
}

bool dropRole(const std::string& roleName) {
    const auto role = authByName(roleName);
    if (!role) return false;
    const bool ok = authCatalog().dropAuthId(role->oid);
    if (ok) persistAuthCatalog();
    return ok;
}

int grantRoleToUser(const std::string& roleName, const std::string& username,
                    bool adminOption, const std::string& grantorName) {
    const auto role = authByName(roleName);
    const auto member = authByName(username);
    if (!role || !member) return -1;
    dbms::Oid grantorOid = role->oid;
    if (!grantorName.empty()) {
        const auto grantor = authByName(grantorName);
        if (!grantor) return -1;
        grantorOid = grantor->oid;
    }
    if (role->oid == member->oid || userIsMemberOfRole(roleName, username)) return -3;
    for (const auto& relation : authCatalog().findAuthMembers(role->oid)) {
        if (relation.member != member->oid) continue;
        if (!adminOption || relation.admin_option) return -2;
        auto upgraded = relation;
        upgraded.admin_option = true;
        upgraded.grantor = grantorOid;
        if (!authCatalog().updateAuthMember(relation.oid, upgraded)) return -1;
        persistAuthCatalog();
        return 0;
    }
    dbms::PgAuthMembersRow relation;
    relation.roleid = role->oid;
    relation.member = member->oid;
    relation.grantor = grantorOid;
    relation.admin_option = adminOption;
    authCatalog().addAuthMember(relation);
    persistAuthCatalog();
    return 0;
}

bool revokeRoleFromUser(const std::string& roleName, const std::string& username) {
    const auto role = authByName(roleName);
    const auto member = authByName(username);
    if (!role || !member) return false;
    const bool ok = authCatalog().removeAuthMember(role->oid, member->oid);
    if (ok) persistAuthCatalog();
    return ok;
}

bool revokeRoleAdminOption(const std::string& roleName, const std::string& username) {
    const auto role = authByName(roleName);
    const auto member = authByName(username);
    if (!role || !member) return false;
    for (const auto& relation : authCatalog().findAuthMembers(role->oid)) {
        if (relation.member != member->oid || !relation.admin_option) continue;
        auto updated = relation;
        updated.admin_option = false;
        const bool ok = authCatalog().updateAuthMember(relation.oid, updated);
        if (ok) persistAuthCatalog();
        return ok;
    }
    return false;
}

bool hasRoleAdminOption(const std::string& roleName, const std::string& grantorName) {
    const auto role = authByName(roleName);
    const auto grantor = authByName(grantorName);
    if (!role || !grantor) return false;
    for (const auto& relation : authCatalog().findAuthMembers(role->oid)) {
        if (relation.member == grantor->oid && relation.admin_option) return true;
    }
    return false;
}

std::vector<std::string> getUserRoles(const std::string& username) {
    std::vector<std::string> roles;
    const auto member = authByName(username);
    if (!member) return roles;
    for (const auto& relation : authCatalog().findAuthMemberships(member->oid)) {
        const auto role = authCatalog().getAuthId(relation.roleid);
        if (role) roles.push_back(role->rolname);
    }
    return roles;
}

bool userIsMemberOfRole(const std::string& username, const std::string& roleName) {
    const auto role = authByName(roleName);
    const auto member = authByName(username);
    if (!role || !member) return false;
    std::unordered_set<dbms::Oid> visited;
    return roleMemberRecursive(member->oid, role->oid, visited);
}

bool userHasRole(const std::string& username, const std::string& roleName) {
    const auto member = authByName(username);
    if (!member || !member->rolinherit) return false;
    return userIsMemberOfRole(username, roleName);
}

bool userIsAdminViaRole(const std::string& username) {
    const auto member = authByName(username);
    if (member && member->rolsuper) return true;
    if (member && !member->rolinherit) return false;
    return userHasRole(username, "admin");
}

bool canSetRole(const std::string& sessionUser, const std::string& targetRole) {
    const auto session = authByName(sessionUser);
    const auto target = authByName(targetRole);
    if (!session || !target) return false;
    if (session->rolsuper || session->oid == target->oid) return true;
    // Explicit SET ROLE follows membership itself; rolinherit only controls
    // automatic privilege inheritance and must not grant implicit SET ROLE.
    return userIsMemberOfRole(sessionUser, targetRole);
}

bool canAlterTableOwner(const std::string& sessionUser,
                        const std::string& effectiveRole,
                        const std::string& tableOwner,
                        const std::string& targetOwner) {
    const auto actor = authByName(effectiveRole);
    if (!actor || !roleExists(targetOwner)) return false;
    if (actor->rolsuper) return true;
    if (actor->rolname != tableOwner) return false;
    return canSetRole(sessionUser, targetOwner);
}

std::string effectiveSessionRole(const Session& session) {
    return session.currentRole.empty() ? session.username : session.currentRole;
}

bool sessionIsAdmin(const Session& session) {
    const std::string role = effectiveSessionRole(session);
    // Unit-level callers may intentionally use an unauthenticated synthetic
    // Session with permission=1. Network sessions always carry an auth user,
    // so SET ROLE correctly drops inherited administrator privileges there.
    if (session.authenticatedUser.empty()) {
        return session.permission == 1 || userIsAdminViaRole(role);
    }
    return permissionQuery(role) == 1;
}

int checkPasswordStrength(const std::string& password) {
    int score = 0;
    if (password.length() >= 6) score += 20;
    if (password.length() >= 8) score += 20;
    bool hasLower = false, hasUpper = false, hasDigit = false, hasSpecial = false;
    for (char c : password) {
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

std::string passwordStrengthMessage(int score) {
    if (score >= 80) return "strong";
    if (score >= 50) return "medium";
    return "weak";
}

bool getStoredUserPassword(const std::string& username, std::string& storedPassword) {
    const auto account = authByName(username);
    if (!account || !loginAllowed(*account)) return false;
    storedPassword = account->rolpassword;
    return true;
}

bool verifyUserPassword(const std::string& username, const std::string& password) {
    std::string storedPassword;
    if (!getStoredUserPassword(username, storedPassword)) return false;
    return storedPassword.rfind("SCRAM-SHA-256$", 0) == 0 &&
           dbms::scram::verifyPassword(password, storedPassword);
}

int login(const std::string& username, const std::string& password) {
    if (verifyUserPassword(username, password)) {
        std::cout << "successfully login" << std::endl;
        return 1;
    }
    std::cout << "wrong username or password" << std::endl;
    return -1;
}

int permissionQuery(const std::string& username) {
    const auto account = authByName(username);
    if (!account) return -1;
    return (account->rolsuper || account->rolname == "admin" ||
            userIsAdminViaRole(username)) ? 1 : 0;
}

int createUser(const user& newUser) {
    if (roleExists(newUser.username)) return -1;
    const std::string verifier = newUser.password.rfind("SCRAM-SHA-256$", 0) == 0
                                     ? newUser.password
                                     : dbms::scram::makeRandomVerifier(newUser.password);
    dbms::PgAuthIdRow row;
    row.rolname = newUser.username;
    row.rolpassword = verifier;
    row.rolcanlogin = true;
    row.rolsuper = newUser.permission == "1" || newUser.permission == "admin";
    row.rolcreaterole = row.rolsuper;
    row.rolcreatedb = row.rolsuper;
    authCatalog().createAuthId(row);
    persistAuthCatalog();
    return 0;
}

bool renameUser(const std::string& oldName, const std::string& newName) {
    const auto account = authByName(oldName);
    if (!account || authByName(newName)) return false;
    auto updated = *account;
    updated.rolname = newName;
    const bool ok = authCatalog().updateAuthId(account->oid, updated);
    if (ok) persistAuthCatalog();
    return ok;
}

bool deleteUser(const std::string& username) {
    return dropRole(username);
}
