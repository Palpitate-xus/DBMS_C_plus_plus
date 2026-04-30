#pragma once

#include <ctime>
#include <fstream>
#include <iostream>
#include <string>

// 获取当前时间
inline std::string getTime() {
    time_t now = time(0);
    std::string now_dt = ctime(&now);
    std::cout << now_dt;
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
