#pragma once

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include "sha256.h"

struct user {
    std::string username;
    std::string password;
    std::string permission;
};

// ===================== Role Management =====================
// role.dat format: role_name username
// username = "__ROLE__" means the role itself exists

inline bool roleExists(const std::string& roleName) {
    // Check role.dat for explicit roles
    std::ifstream rolefile("role.dat");
    if (rolefile) {
        std::string r, u;
        while (rolefile >> r >> u) {
            if (r == roleName && u == "__ROLE__") return true;
        }
    }
    // In PostgreSQL, users are also roles; check user.dat
    std::ifstream userfile("user.dat");
    if (userfile) {
        user temp;
        while (userfile >> temp.username >> temp.password >> temp.permission) {
            if (temp.username == roleName) return true;
        }
    }
    return false;
}

inline int createRole(const std::string& roleName) {
    if (roleExists(roleName)) return -1; // already exists
    std::ofstream fs("role.dat", std::ios::binary | std::ios::out | std::ios::app);
    fs << roleName << " __ROLE__" << std::endl;
    return 0;
}

inline bool dropRole(const std::string& roleName) {
    std::ifstream infile("role.dat");
    if (!infile) return false;
    std::vector<std::pair<std::string, std::string>> entries;
    std::string r, u;
    bool found = false;
    while (infile >> r >> u) {
        if (r == roleName) {
            found = true;
            continue;
        }
        entries.emplace_back(r, u);
    }
    if (!found) return false;
    std::ofstream outfile("role.dat", std::ios::trunc);
    for (size_t i = 0; i < entries.size(); ++i) {
        if (i > 0) outfile << '\n';
        outfile << entries[i].first << " " << entries[i].second;
    }
    if (!entries.empty()) outfile << std::endl;
    return true;
}

inline int grantRoleToUser(const std::string& roleName, const std::string& username) {
    if (!roleExists(roleName)) return -1;
    std::ifstream infile("role.dat");
    std::string r, u;
    while (infile >> r >> u) {
        if (r == roleName && u == username) return -2; // already granted
    }
    std::ofstream fs("role.dat", std::ios::binary | std::ios::out | std::ios::app);
    fs << roleName << " " << username << std::endl;
    return 0;
}

inline bool revokeRoleFromUser(const std::string& roleName, const std::string& username) {
    std::ifstream infile("role.dat");
    if (!infile) return false;
    std::vector<std::pair<std::string, std::string>> entries;
    std::string r, u;
    bool found = false;
    while (infile >> r >> u) {
        if (r == roleName && u == username) {
            found = true;
            continue;
        }
        entries.emplace_back(r, u);
    }
    if (!found) return false;
    std::ofstream outfile("role.dat", std::ios::trunc);
    for (size_t i = 0; i < entries.size(); ++i) {
        if (i > 0) outfile << '\n';
        outfile << entries[i].first << " " << entries[i].second;
    }
    if (!entries.empty()) outfile << std::endl;
    return true;
}

inline std::vector<std::string> getUserRoles(const std::string& username) {
    std::vector<std::string> roles;
    std::ifstream infile("role.dat");
    if (!infile) return roles;
    std::string r, u;
    while (infile >> r >> u) {
        if (u == username && r != "__ROLE__") {
            roles.push_back(r);
        }
    }
    return roles;
}

inline bool userHasRole(const std::string& username, const std::string& roleName) {
    std::ifstream infile("role.dat");
    if (!infile) return false;
    std::string r, u;
    while (infile >> r >> u) {
        if (r == roleName && u == username) return true;
    }
    return false;
}

inline bool userIsAdminViaRole(const std::string& username) {
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

inline bool isSha256Hash(const std::string& pw) {
    if (pw.size() != 64) return false;
    for (char c : pw) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
    }
    return true;
}

inline bool isMd5Hash(const std::string& pw) {
    if (pw.size() != 32) return false;
    for (char c : pw) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
    }
    return true;
}

inline bool isHashedPassword(const std::string& pw) {
    return isSha256Hash(pw) || isMd5Hash(pw);
}

inline int login(const std::string& username, const std::string& password) {
    std::ifstream infile("user.dat");
    if (!infile) return 0;

    user temp;
    while (infile >> temp.username >> temp.password >> temp.permission) {
        if (temp.username == username) {
            std::string checkPw = password;
            if (isSha256Hash(temp.password)) {
                checkPw = sha256(password);
            } else if (isMd5Hash(temp.password)) {
                checkPw = md5(password);
            }
            if (temp.password == checkPw) {
                std::cout << "successfully login" << std::endl;
                return 1;
            }
            std::cout << "wrong password" << std::endl;
            return -1;
        }
    }
    std::cout << "this user is not exist" << std::endl;
    return 0;
}

inline int permissionQuery(const std::string& username) {
    std::ifstream infile("user.dat");
    if (!infile) return -1;

    user temp;
    while (infile >> temp.username >> temp.password >> temp.permission) {
        if (temp.username == username) {
            return (temp.permission == "admin" || temp.permission == "1") ? 1 : 0;
        }
    }
    return -1;
}

inline int createUser(const user& new_user, const std::string& hashAlgo = "sha256") {
    std::ofstream fs("user.dat", std::ios::binary | std::ios::out | std::ios::app);
    std::string hashedPw;
    if (isHashedPassword(new_user.password)) {
        hashedPw = new_user.password;
    } else if (hashAlgo == "md5") {
        hashedPw = md5(new_user.password);
    } else {
        hashedPw = sha256(new_user.password);
    }
    fs << '\n' << new_user.username << " " << hashedPw << " " << new_user.permission << std::endl;
    return 0;
}

inline bool deleteUser(const std::string& username) {
    std::ifstream infile("user.dat");
    if (!infile) return false;
    std::vector<user> users;
    user temp;
    bool found = false;
    while (infile >> temp.username >> temp.password >> temp.permission) {
        if (temp.username == username) {
            found = true;
            continue;
        }
        users.push_back(temp);
    }
    if (!found) return false;
    std::ofstream outfile("user.dat", std::ios::trunc);
    for (size_t i = 0; i < users.size(); ++i) {
        if (i > 0) outfile << '\n';
        outfile << users[i].username << " " << users[i].password << " " << users[i].permission;
    }
    if (!users.empty()) outfile << std::endl;
    return true;
}
