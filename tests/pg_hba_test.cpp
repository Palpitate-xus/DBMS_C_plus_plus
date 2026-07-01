#include "utils/pg_hba.h"
#include <cassert>
#include <fstream>
#include <iostream>

static void writeTestHba(const std::string& path, const std::string& content) {
    std::ofstream ofs(path);
    ofs << content;
}

static void test_parse_local() {
    writeTestHba("/tmp/pg_hba_test.conf",
        "local all all trust\n"
        "host all all 127.0.0.1/32 md5\n"
        "host all all 192.168.1.0/24 scram-sha-256\n"
        "hostssl all all 10.0.0.0/8 cert\n"
        "# comment line\n"
        "host all admin 0.0.0.0/0 reject\n");

    auto records = dbms::PgHbaFile::parse("/tmp/pg_hba_test.conf");
    assert(records.size() == 5);

    assert(records[0].connectionType == "local");
    assert(records[0].database == "all");
    assert(records[0].user == "all");
    assert(records[0].method == dbms::HbaMethod::Trust);

    assert(records[1].connectionType == "host");
    assert(records[1].address == "127.0.0.1/32");
    assert(records[1].method == dbms::HbaMethod::Md5);

    assert(records[2].method == dbms::HbaMethod::ScramSha256);
    assert(records[3].connectionType == "hostssl");
    assert(records[3].method == dbms::HbaMethod::Cert);
    assert(records[4].method == dbms::HbaMethod::Reject);

    std::remove("/tmp/pg_hba_test.conf");
    std::cout << "[PG_HBA] parse OK" << std::endl;
}

static void test_match() {
    writeTestHba("/tmp/pg_hba_match.conf",
        "local all all trust\n"
        "host all all 127.0.0.1/32 md5\n"
        "host all all 192.168.0.0/16 scram-sha-256\n");

    auto records = dbms::PgHbaFile::parse("/tmp/pg_hba_match.conf");

    // local connection → trust
    auto m1 = dbms::PgHbaFile::match(records, "local", "mydb", "user1");
    assert(m1 == dbms::HbaMethod::Trust);

    // host from 127.0.0.1 → md5
    auto m2 = dbms::PgHbaFile::match(records, "host", "mydb", "user1", "127.0.0.1");
    assert(m2 == dbms::HbaMethod::Md5);

    // host from 192.168.5.5 → scram
    auto m3 = dbms::PgHbaFile::match(records, "host", "mydb", "user1", "192.168.5.5");
    assert(m3 == dbms::HbaMethod::ScramSha256);

    // host from 10.0.0.1 (no match) → reject
    auto m4 = dbms::PgHbaFile::match(records, "host", "mydb", "user1", "10.0.0.1");
    assert(m4 == dbms::HbaMethod::Reject);

    std::remove("/tmp/pg_hba_match.conf");
    std::cout << "[PG_HBA] match OK" << std::endl;
}

static void test_ip_cidr() {
    writeTestHba("/tmp/pg_hba_cidr.conf",
        "host all all 0.0.0.0/0 trust\n");

    auto records = dbms::PgHbaFile::parse("/tmp/pg_hba_cidr.conf");
    auto m = dbms::PgHbaFile::match(records, "host", "db", "u", "8.8.8.8");
    assert(m == dbms::HbaMethod::Trust);

    std::remove("/tmp/pg_hba_cidr.conf");
    std::cout << "[PG_HBA] CIDR OK" << std::endl;
}

int main() {
    test_parse_local();
    test_match();
    test_ip_cidr();
    std::cout << "[PG_HBA] all passed" << std::endl;
    return 0;
}
