#include "pg_hba.h"

#include <algorithm>
#include <array>
#include <cstdint>

namespace dbms {

// Convert an IPv4 address string to uint32_t (host byte order). Returns 0 on failure.
static uint32_t ipv4ToInt(const std::string& ip) {
    uint32_t result = 0;
    int octet = 0;
    int shift = 24;
    for (char c : ip) {
        if (c == '.') {
            if (shift < 0) return 0;
            result |= (static_cast<uint32_t>(octet) << shift);
            shift -= 8;
            octet = 0;
        } else if (c >= '0' && c <= '9') {
            octet = octet * 10 + (c - '0');
            if (octet > 255) return 0;
        } else {
            return 0;
        }
    }
    if (shift == 0) {
        result |= octet;
        return result;
    }
    return 0;
}

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
    return HbaMethod::Trust;  // default
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
    auto slash = cidr.find('/');
    std::string netIp = (slash == std::string::npos) ? cidr : cidr.substr(0, slash);
    int maskBits = 32;
    if (slash != std::string::npos) {
        try { maskBits = std::stoi(cidr.substr(slash + 1)); } catch (...) { maskBits = 32; }
    }
    if (maskBits < 0 || maskBits > 32) maskBits = 32;
    uint32_t ipVal = ipv4ToInt(ip);
    uint32_t netVal = ipv4ToInt(netIp);
    // ipv4ToInt returns 0 on parse failure (not for valid 0.0.0.0).
    // We detect parse failure by checking if the original string had non-digit/non-dot chars.
    auto isValidIp = [](const std::string& s) -> bool {
        if (s.empty()) return false;
        for (char c : s) {
            if (!((c >= '0' && c <= '9') || c == '.')) return false;
        }
        return true;
    };
    if (!isValidIp(ip) || !isValidIp(netIp)) return false;
    if (maskBits == 0) return true;  // 0.0.0.0/0 matches everything
    uint32_t mask = 0xFFFFFFFFu << (32 - maskBits);
    return (ipVal & mask) == (netVal & mask);
}

HbaMethod PgHbaFile::match(const std::vector<HbaRecord>& records,
                           const std::string& connType,
                           const std::string& database,
                           const std::string& user,
                           const std::string& ip) {
    for (const auto& rec : records) {
        // Connection type
        if (rec.connectionType == "local") {
            if (connType != "local") continue;
        } else if (rec.connectionType == "host" ||
                   rec.connectionType == "hostssl" ||
                   rec.connectionType == "hostnossl") {
            if (connType == "local") continue;
        } else {
            continue;  // unknown
        }
        // Database match
        if (rec.database != "all" && rec.database != "sameuser" &&
            rec.database != database) {
            if (rec.database != "samerole" && rec.database != "+__role__") continue;
            // TODO: group role match
            continue;
        }
        // User match
        if (rec.user != "all" && rec.user != user) {
            if (!rec.user.empty() && rec.user[0] == '+') continue; // role group
            continue;
        }
        // IP match (for host connections)
        if (connType != "local" && !rec.address.empty()) {
            if (!ipInCidr(ip, rec.address)) continue;
        }
        return rec.method;
    }
    return HbaMethod::Reject;  // default if no match
}

} // namespace dbms
