#pragma once

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace dbms {

// pg_hba.conf 记录类型
enum class HbaMethod {
    Trust,          // 免密
    Reject,         // 拒绝
    Md5,            // MD5 认证
    ScramSha256,    // SCRAM-SHA-256
    Password,       // 明文密码
    Ident,          // ident 映射
    Peer,           // peer (unix socket)
    Cert,           // client certificate
    Pam,            // PAM
    Ldap,           // LDAP
    RADIUS,         // RADIUS
   reject_all = Reject
};

struct HbaRecord {
    std::string connectionType;  // "local" 或 "host"/"hostssl"/"hostnossl"
    std::string database;        // 数据库名 或 "all" 或 "sameuser" 等
    std::string user;            // 用户名 或 "all" 或 "+" 角色
    std::string address;         // IP/mask (local 时为空)
    HbaMethod method;
    std::string options;         // 额外选项
};

class PgHbaFile {
public:
    // 解析 pg_hba.conf 文件
    static std::vector<HbaRecord> parse(const std::string& path);

    // 匹配给定连接参数，返回首个匹配的 method。
    // 若无匹配，返回 Reject。
    static HbaMethod match(const std::vector<HbaRecord>& records,
                           const std::string& connType,    // "local" / "host"
                           const std::string& database,
                           const std::string& user,
                           const std::string& ip = "");    // 客户端 IP (IPv4 or IPv6)

private:
    static bool ipInCidr(const std::string& ip, const std::string& cidr);
};

} // namespace dbms
