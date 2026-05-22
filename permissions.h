#pragma once

#include <fstream>
#include <iostream>
#include <string>
#include "sha256.h"

struct user {
    std::string username;
    std::string password;
    std::string permission;
};

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

inline bool isHashedPassword(const std::string& pw) {
    if (pw.size() != 64) return false;
    for (char c : pw) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
    }
    return true;
}

inline int login(const std::string& username, const std::string& password) {
    std::ifstream infile("user.dat");
    if (!infile) return 0;

    user temp;
    while (infile >> temp.username >> temp.password >> temp.permission) {
        if (temp.username == username) {
            std::string checkPw = password;
            if (isHashedPassword(temp.password)) {
                checkPw = sha256(password);
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
            return (temp.permission == "admin") ? 1 : 0;
        }
    }
    return -1;
}

inline int createUser(const user& new_user) {
    std::ofstream fs("user.dat", std::ios::binary | std::ios::out | std::ios::app);
    std::string hashedPw = isHashedPassword(new_user.password) ? new_user.password : sha256(new_user.password);
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
