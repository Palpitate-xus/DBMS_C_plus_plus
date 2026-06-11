#include "ClusterLayout.h"
#include <filesystem>
#include <fstream>

namespace dbms {

ClusterLayout::ClusterLayout(const std::string& dataDir)
    : dataDir_(dataDir) {
    if (!dataDir_.empty() && dataDir_.back() == '/') {
        dataDir_.pop_back();
    }
}

bool ClusterLayout::ensureDir(const std::string& path) {
    try {
        std::filesystem::create_directories(path);
        return true;
    } catch (...) {
        return false;
    }
}

bool ClusterLayout::initCluster() {
    bool ok = true;
    ok &= ensureDir(dataDir_);
    ok &= ensureDir(dataDir_ + "/base");
    ok &= ensureDir(dataDir_ + "/global");
    ok &= ensureDir(dataDir_ + "/pg_wal");
    ok &= ensureDir(dataDir_ + "/pg_wal/archive_status");
    ok &= ensureDir(dataDir_ + "/pg_xact");
    ok &= ensureDir(dataDir_ + "/pg_tblspc");
    ok &= ensureDir(dataDir_ + "/pg_commit_ts");
    ok &= ensureDir(dataDir_ + "/pg_dynshmem");
    ok &= ensureDir(dataDir_ + "/pg_logical");
    ok &= ensureDir(dataDir_ + "/pg_logical/snapshots");
    ok &= ensureDir(dataDir_ + "/pg_logical/mappings");
    ok &= ensureDir(dataDir_ + "/pg_multixact");
    ok &= ensureDir(dataDir_ + "/pg_multixact/members");
    ok &= ensureDir(dataDir_ + "/pg_multixact/offsets");
    ok &= ensureDir(dataDir_ + "/pg_notify");
    ok &= ensureDir(dataDir_ + "/pg_replslot");
    ok &= ensureDir(dataDir_ + "/pg_serial");
    ok &= ensureDir(dataDir_ + "/pg_snapshots");
    ok &= ensureDir(dataDir_ + "/pg_stat");
    ok &= ensureDir(dataDir_ + "/pg_stat_tmp");
    ok &= ensureDir(dataDir_ + "/pg_subtrans");
    ok &= ensureDir(dataDir_ + "/pg_twophase");
    if (ok) {
        writeVersion("18");
    }
    return ok;
}

std::string ClusterLayout::databasePath(Oid dbOid) const {
    return dataDir_ + "/base/" + std::to_string(dbOid);
}

bool ClusterLayout::createDatabaseDir(Oid dbOid) {
    return ensureDir(databasePath(dbOid));
}

bool ClusterLayout::dropDatabaseDir(Oid dbOid) {
    try {
        std::filesystem::remove_all(databasePath(dbOid));
        return true;
    } catch (...) {
        return false;
    }
}

std::string ClusterLayout::globalPath() const {
    return dataDir_ + "/global";
}

std::string ClusterLayout::walPath() const {
    return dataDir_ + "/pg_wal";
}

std::string ClusterLayout::xactPath() const {
    return dataDir_ + "/pg_xact";
}

std::string ClusterLayout::tablespacePath() const {
    return dataDir_ + "/pg_tblspc";
}

std::string ClusterLayout::tablespaceLinkPath(Oid spcOid) const {
    return tablespacePath() + "/" + std::to_string(spcOid);
}

bool ClusterLayout::createTablespaceLink(Oid spcOid, const std::string& realPath) {
    try {
        std::filesystem::create_symlink(realPath, tablespaceLinkPath(spcOid));
        return true;
    } catch (...) {
        return false;
    }
}

bool ClusterLayout::writeVersion(const std::string& version) {
    std::ofstream out(dataDir_ + "/PG_VERSION");
    if (!out) return false;
    out << version << "\n";
    return true;
}

std::string ClusterLayout::readVersion() const {
    std::ifstream in(dataDir_ + "/PG_VERSION");
    std::string version;
    if (in) std::getline(in, version);
    return version;
}

std::string ClusterLayout::relationPath(Oid dbOid, Oid relOid, ForkType fork) const {
    std::string base = databasePath(dbOid) + "/" + std::to_string(relOid);
    switch (fork) {
        case ForkType::Main: return base;
        case ForkType::FSM:  return base + "_fsm";
        case ForkType::VM:   return base + "_vm";
        case ForkType::Init: return base + "_init";
    }
    return base;
}

} // namespace dbms
