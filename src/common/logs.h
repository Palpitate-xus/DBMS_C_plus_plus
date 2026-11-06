#pragma once

#include <cerrno>
#include <ctime>
#include <fcntl.h>
#include <limits>
#include <sys/uio.h>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include <string>

// 获取当前时间
// ctime()/localtime() return libc static buffers and are data races when
// two connection threads format timestamps concurrently; use the
// reentrant variants everywhere (observed live under TSAN on the
// protocol server).
inline std::string getTime() {
    time_t now = time(0);
    char buf[64];
    if (!ctime_r(&now, buf)) return std::string();
    return std::string(buf);
}

// 写入日志文件
inline int log(const std::string& user, const std::string& operation, const std::string& time) {
    const int fd = ::open("dbms.log", O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (fd < 0) {
        std::cout << "error 1" << std::endl;
        return 0;
    }
    struct CloseOnce {
        int fd;
        ~CloseOnce() { (void)::close(fd); }
    } closeOnce{fd};
    constexpr char spaces[] = "    ";
    constexpr char newline[] = "\n";
    iovec pending[6] = {
        {const_cast<char*>(time.data()), time.size()},
        {const_cast<char*>(spaces), sizeof(spaces) - 1},
        {const_cast<char*>(user.data()), user.size()},
        {const_cast<char*>(spaces), sizeof(spaces) - 1},
        {const_cast<char*>(operation.data()), operation.size()},
        {const_cast<char*>(newline), sizeof(newline) - 1}
    };
    int first = 0;
    while (first < 6) {
        while (first < 6 && pending[first].iov_len == 0) ++first;
        if (first == 6) break;
        size_t budget = static_cast<size_t>(std::numeric_limits<ssize_t>::max());
        int end = first;
        int capped = -1;
        size_t originalLength = 0;
        while (end < 6) {
            if (pending[end].iov_len > budget) {
                capped = end;
                originalLength = pending[end].iov_len;
                pending[end].iov_len = budget;
                ++end;
                break;
            }
            budget -= pending[end].iov_len;
            ++end;
            if (budget == 0) break;
        }
        const ssize_t written = ::writev(fd, pending + first, end - first);
        // Restore before consuming a positive short write: the unwritten
        // original suffix still belongs to this same pending segment.
        if (capped >= 0) pending[capped].iov_len = originalLength;
        if (written < 0 && errno == EINTR) continue;
        if (written <= 0) break;
        size_t consumed = static_cast<size_t>(written);
        while (consumed > 0) {
            if (consumed >= pending[first].iov_len) {
                consumed -= pending[first].iov_len;
                ++first;
            } else {
                pending[first].iov_base = static_cast<char*>(pending[first].iov_base) + consumed;
                pending[first].iov_len -= consumed;
                consumed = 0;
            }
        }
    }
    return 0;
}

// Audit log: timestamp, username, database, sql_type, sql_text, status
// auditLevel: 0=none, 1=DDL only, 2=DML+DDL, 3=all
inline void auditLog(int auditLevel, const std::string& user, const std::string& dbname,
                     const std::string& sql, const std::string& status) {
    if (auditLevel <= 0) return;
    // Classify SQL type
    std::string sqlType = "other";
    std::string lowerSql;
    for (char c : sql) lowerSql += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    bool isDDL = false, isDML = false;
    if (lowerSql.find("create") == 0 || lowerSql.find("drop") == 0 ||
        lowerSql.find("alter") == 0 || lowerSql.find("truncate") == 0) {
        isDDL = true; sqlType = "ddl";
    } else if (lowerSql.find("select") == 0 || lowerSql.find("insert") == 0 ||
               lowerSql.find("update") == 0 || lowerSql.find("delete") == 0 ||
               lowerSql.find("merge") == 0 || lowerSql.find("replace") == 0) {
        isDML = true; sqlType = "dml";
    } else if (lowerSql.find("grant") == 0 || lowerSql.find("revoke") == 0 ||
               lowerSql.find("create user") == 0 || lowerSql.find("drop user") == 0 ||
               lowerSql.find("alter user") == 0) {
        sqlType = "dcl";
    }
    if (auditLevel == 1 && !isDDL) return;
    if (auditLevel == 2 && !isDDL && !isDML) return;
    std::ofstream ofs("audit.log", std::ios::app);
    if (!ofs) return;
    time_t now = time(0);
    char tsBuf[64];
    if (!ctime_r(&now, tsBuf)) tsBuf[0] = '\0';
    std::string ts = tsBuf;
    if (!ts.empty() && ts.back() == '\n') ts.pop_back();
    // Truncate long SQL for readability
    std::string sqlTrunc = sql;
    if (sqlTrunc.size() > 200) sqlTrunc = sqlTrunc.substr(0, 197) + "...";
    ofs << ts << " | user=" << user << " | db=" << dbname
        << " | type=" << sqlType << " | status=" << status
        << " | sql=" << sqlTrunc << "\n";
}
