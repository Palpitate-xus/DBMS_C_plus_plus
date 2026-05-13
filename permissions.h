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
