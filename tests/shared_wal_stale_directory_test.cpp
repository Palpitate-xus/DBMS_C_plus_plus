#include "storage/WAL.h"

#include <cassert>
#include <filesystem>
#include <iostream>

using namespace dbms;

int main() {
    const std::filesystem::path path = "__t_retired_wal";
    WALManager retired(path);
    assert(retired.ensureOpen());
    assert(retired.XLogFlush(0));
    std::filesystem::rename(path, "__t_retired_wal.saved");
    const bool flushed = retired.XLogFlush(0);
    const bool recreated = std::filesystem::exists(path);
    std::cerr << "[STALE WAL] flush=" << flushed
              << " recreated=" << recreated << '\n';
    assert(!flushed && !recreated);
    WALManager replacement(path);
    assert(replacement.ensureOpen());
    assert(replacement.XLogFlush(0));
    assert(!retired.XLogFlush(0));
    assert(replacement.XLogFlush(0));
    WALManager runtimeMissing("__t_absent_runtime_wal/pg_wal");
    assert(!runtimeMissing.ensureOpen(false));
    assert(!std::filesystem::exists("__t_absent_runtime_wal"));
    // Standalone creation remains explicitly supported.
    assert(runtimeMissing.ensureOpen());
    assert(runtimeMissing.XLogFlush(0));
    std::cout << "[STALE WAL] old manager cannot resurrect or flush new directory\n";
}
