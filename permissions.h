#pragma once

#include <fstream>
#include <iostream>
#include <string>

struct user {
    std::string username;
    std::string password;
    std::string permission;
};

inline int login(const std::string& username, const std::string& password) {
    std::ifstream infile("user.dat");
    if (!infile) return 0;

    user temp;
    while (infile >> temp.username >> temp.password >> temp.permission) {
        if (temp.username == username) {
            if (temp.password == password) {
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
    fs << '\n' << new_user.username << " " << new_user.password << " " << new_user.permission << std::endl;
    return 0;
}
