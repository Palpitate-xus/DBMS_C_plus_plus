// Configuration parsing, validation and atomic persistence regression.

#include "common/Config.h"

#include <cassert>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <unistd.h>

int main() {
    namespace fs = std::filesystem;
    const fs::path path = "config_test.conf";
    fs::remove(path);
    fs::remove(path.string() + ".tmp." + std::to_string(::getpid()));

    dbms::Config config;
    assert(config.validate());
    const dbms::Config defaults = config;

    assert(config.setParameter("max_connections", "128"));
    assert(config.maxConnections == 128);
    assert(config.setParameter("enable_seq_scan", "off"));
    assert(!config.enableSeqScan);
    assert(config.setParameter("work_mem", "8192"));
    assert(config.workMemKb == 8192);

    const dbms::Config beforeInvalid = config;
    assert(!config.setParameter("max_connections", "0"));
    assert(config.maxConnections == beforeInvalid.maxConnections);
    assert(!config.setParameter("enable_seq_scan", "maybe"));
    assert(config.enableSeqScan == beforeInvalid.enableSeqScan);
    assert(!config.setParameter("slow_query_threshold", "nan"));
    assert(config.slowQueryThresholdMs == beforeInvalid.slowQueryThresholdMs);
    assert(!config.setParameter("unknown_parameter", "1"));
    assert(config.maxConnections == beforeInvalid.maxConnections);

    assert(config.save(path.string()));
    dbms::Config loaded;
    assert(loaded.load(path.string()));
    assert(loaded.maxConnections == 128);
    assert(!loaded.enableSeqScan);
    assert(loaded.workMemKb == 8192);

    {
        std::ofstream out(path, std::ios::trunc);
        out << "max_connections=broken\n";
    }
    const dbms::Config beforeMalformedLoad = loaded;
    assert(!loaded.load(path.string()));
    assert(loaded.maxConnections == beforeMalformedLoad.maxConnections);
    assert(loaded.workMemKb == beforeMalformedLoad.workMemKb);

    {
        std::ofstream out(path, std::ios::trunc);
        out << "max_connections=128\nmax_connections=256\n";
    }
    assert(!loaded.load(path.string()));
    assert(loaded.maxConnections == beforeMalformedLoad.maxConnections);

    dbms::Config invalid = defaults;
    invalid.autoExplainThresholdMs = std::numeric_limits<double>::quiet_NaN();
    assert(!invalid.validate());
    assert(!invalid.save(path.string()));

    fs::remove(path);
    std::cout << "[CONFIG] strict validation, rollback and persistence OK\n";
    return 0;
}
