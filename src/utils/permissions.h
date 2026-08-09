#pragma once

#include "Session.h"

#include <string>
#include <vector>

namespace dbms { class CatalogManager; }

struct user {
    std::string username;
    std::string password;
    std::string permission;
};

dbms::CatalogManager& authCatalog();
void persistAuthCatalog();

// Authentication and role state is cluster-scoped and stored in the catalog
// database. The old user.dat/role.dat files are intentionally not read or
// written: this release has a hard storage boundary and no legacy migration.

bool roleExists(const std::string& roleName);
int createRole(const std::string& roleName);
bool dropRole(const std::string& roleName);
int grantRoleToUser(const std::string& roleName, const std::string& username);
bool revokeRoleFromUser(const std::string& roleName, const std::string& username);
std::vector<std::string> getUserRoles(const std::string& username);
// Raw membership is used by role selectors such as pg_hba.conf. It does not
// imply inherited privileges; privilege checks must use userHasRole().
bool userIsMemberOfRole(const std::string& username, const std::string& roleName);
// Effective membership for ACL/RLS checks. A NOINHERIT login role does not
// automatically receive privileges from its memberships.
bool userHasRole(const std::string& username, const std::string& roleName);
bool userIsAdminViaRole(const std::string& username);
// SET ROLE is based on membership, not INHERIT: NOINHERIT users may still
// explicitly switch into a role they are allowed to become.
bool canSetRole(const std::string& sessionUser, const std::string& targetRole);
// ALTER TABLE ... OWNER TO requires the effective role to own the table (or
// be superuser) and the session user to be able to SET ROLE to the target.
bool canAlterTableOwner(const std::string& sessionUser,
                        const std::string& effectiveRole,
                        const std::string& tableOwner,
                        const std::string& targetOwner);
std::string effectiveSessionRole(const Session& session);
bool sessionIsAdmin(const Session& session);

int checkPasswordStrength(const std::string& password);
std::string passwordStrengthMessage(int score);

bool getStoredUserPassword(const std::string& username, std::string& storedPassword);
bool verifyUserPassword(const std::string& username, const std::string& password);
int login(const std::string& username, const std::string& password);
int permissionQuery(const std::string& username);
int createUser(const user& newUser);
bool renameUser(const std::string& oldName, const std::string& newName);
bool deleteUser(const std::string& username);
