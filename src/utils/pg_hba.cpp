#include "pg_hba.h"

#include <algorithm>
#include <arpa/inet.h>
#include <cctype>
#include <cstring>

namespace dbms {

static HbaMethod parseMethod(const std::string& s) {
    std::string l;
    for (char c : s) l += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (l == "trust") return HbaMethod::Trust;
    if (l == "reject") return HbaMethod::Reject;
    if (l == "md5") return HbaMethod::Md5;
    if (l == "scram-sha-256") return HbaMethod::ScramSha256;
    if (l == "password") return HbaMethod::Password;
    if (l == "ident") return HbaMethod::Ident;
    if (l == "peer") return HbaMethod::Peer;
    if (l == "cert") return HbaMethod::Cert;
    if (l == "pam") return HbaMethod::Pam;
    if (l == "ldap") return HbaMethod::Ldap;
    if (l == "radius") return HbaMethod::RADIUS;
    return HbaMethod::Reject;  // invalid methods must fail closed
}

std::vector<HbaRecord> PgHbaFile::parse(const std::string& path) {
    std::vector<HbaRecord> records;
    std::ifstream ifs(path);
    if (!ifs) return records;
    std::string line;
    while (std::getline(ifs, line)) {
        // Strip comments
        auto cp = line.find('#');
        if (cp != std::string::npos) line = line.substr(0, cp);
        // Trim
        size_t start = line.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) continue;
        size_t end = line.find_last_not_of(" \t\r\n");
        line = line.substr(start, end - start + 1);
        if (line.empty()) continue;

        std::istringstream iss(line);
        HbaRecord rec;
        if (!(iss >> rec.connectionType)) continue;
        if (!(iss >> rec.database)) continue;
        if (!(iss >> rec.user)) continue;
        // For "host" types there is an address field; for "local" there isn't.
        if (rec.connectionType == "local") {
            std::string methodStr;
            if (iss >> methodStr) rec.method = parseMethod(methodStr);
            // Read rest as options
            std::string rest;
            std::getline(iss, rest);
            rec.options = rest;
        } else {
            if (!(iss >> rec.address)) continue;
            std::string methodStr;
            if (iss >> methodStr) rec.method = parseMethod(methodStr);
            std::string rest;
            std::getline(iss, rest);
            rec.options = rest;
        }
        records.push_back(rec);
    }
    return records;
}

// Match IP against CIDR like "192.168.1.0/24" or "10.0.0.0/8".
// Also supports single IP without mask.
bool PgHbaFile::ipInCidr(const std::string& ip, const std::string& cidr) {
    if (cidr == "all") return true;
    auto slash = cidr.find('/');
    std::string netIp = (slash == std::string::npos) ? cidr : cidr.substr(0, slash);
    int maskBits = -1;
    if (slash != std::string::npos) {
        try { maskBits = std::stoi(cidr.substr(slash + 1)); } catch (...) { return false; }
    }
    unsigned char ipBytes[sizeof(in6_addr)]{};
    unsigned char netBytes[sizeof(in6_addr)]{};
    const int family = ip.find(':') == std::string::npos ? AF_INET : AF_INET6;
    if (inet_pton(family, ip.c_str(), ipBytes) != 1 ||
        inet_pton(family, netIp.c_str(), netBytes) != 1) return false;
    const int addressBits = family == AF_INET ? 32 : 128;
    if (maskBits < 0) maskBits = addressBits;
    if (maskBits < 0 || maskBits > addressBits) return false;
    const int fullBytes = maskBits / 8;
    const int remainingBits = maskBits % 8;
    if (std::memcmp(ipBytes, netBytes, static_cast<size_t>(fullBytes)) != 0) return false;
    if (remainingBits == 0) return true;
    const unsigned char mask = static_cast<unsigned char>(0xffu << (8 - remainingBits));
    return (ipBytes[fullBytes] & mask) == (netBytes[fullBytes] & mask);
}

static std::vector<std::string> splitHbaList(const std::string& value) {
    std::vector<std::string> result;
    size_t begin = 0;
    while (begin <= value.size()) {
        const size_t comma = value.find(',', begin);
        const size_t end = comma == std::string::npos ? value.size() : comma;
        size_t first = value.find_first_not_of(" \t", begin);
        if (first == std::string::npos || first >= end) {
            begin = comma == std::string::npos ? value.size() + 1 : comma + 1;
            continue;
        }
        size_t last = value.find_last_not_of(" \t", end - 1);
        result.push_back(value.substr(first, last - first + 1));
        begin = comma == std::string::npos ? value.size() + 1 : comma + 1;
    }
    return result;
}

static bool matchesDatabase(const std::string& field, const std::string& database,
                            const std::string& user,
                            const PgHbaFile::RoleMembershipChecker& roleMembership) {
    for (const auto& token : splitHbaList(field)) {
        if (token == "all" || token == database) return true;
        if (token == "sameuser" && database == user) return true;
        if ((token == "samerole" || token == "samegroup") && roleMembership &&
            roleMembership(user, database)) return true;
    }
    return false;
}

static bool matchesUser(const std::string& field, const std::string& user,
                        const PgHbaFile::RoleMembershipChecker& roleMembership) {
    for (const auto& token : splitHbaList(field)) {
        if (token == "all" || token == user) return true;
        if (token.size() > 1 && token.front() == '+' && roleMembership &&
            roleMembership(user, token.substr(1))) return true;
    }
    return false;
}

HbaMethod PgHbaFile::match(const std::vector<HbaRecord>& records,
                           const std::string& connType,
                           const std::string& database,
                           const std::string& user,
                           const std::string& ip,
                           RoleMembershipChecker roleMembership) {
    for (const auto& rec : records) {
        // Connection type
        if (rec.connectionType == "local") {
            if (connType != "local") continue;
        } else if (rec.connectionType == "host" ||
                   rec.connectionType == "hostssl" ||
                   rec.connectionType == "hostnossl") {
            if (connType == "local") continue;
            if (rec.connectionType == "hostssl" && connType != "hostssl") continue;
            if (rec.connectionType == "hostnossl" && connType != "hostnossl") continue;
        } else {
            continue;  // unknown
        }
        // Database match
        if (!matchesDatabase(rec.database, database, user, roleMembership)) continue;
        // User match
        if (!matchesUser(rec.user, user, roleMembership)) continue;
        // IP match (for host connections)
        if (connType != "local" && !rec.address.empty()) {
            if (!ipInCidr(ip, rec.address)) continue;
        }
        return rec.method;
    }
    return HbaMethod::Reject;  // default if no match
}

} // namespace dbms
