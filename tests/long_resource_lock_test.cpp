// test_sources: src/transaction/LockManager.cpp
// Long resource identities must remain usable without truncating lock names.
#include "transaction/LockManager.h"
#include <algorithm>
#include <filesystem>
#include <functional>
#include <iostream>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;
using Manager = dbms::LockManager;

static bool process(const std::function<bool()>& body) {
    const pid_t child = fork();
    if (child == 0) { const bool ok = body(); _exit(ok ? 0 : 1); }
    int status = 0;
    return child > 0 && waitpid(child,&status,0) == child &&
        WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

int main() {
    std::string pattern = (fs::temp_directory_path()/"dbms-long-resource-lock-XXXXXX").string();
    char* created = mkdtemp(pattern.data());
    if (!created) return 2;
    const fs::path root(created);
    const std::string first = (root/"a").string(), second = (root/"b").string();
    fs::create_directory(first); fs::create_directory(second);
    size_t controls = 0, failures = 0;
    auto check = [&](const std::string& label, bool valid) {
        ++controls; if (!valid) { ++failures; std::cerr << "FAIL " << label << '\n'; }
    };
    {
        Manager manager; manager.setResourceNamespace(first); manager.setLockTimeout(20);
        for (const size_t length : {3,119,120,121,122,123,124,125,126,127,128,255,1024}) {
            const std::string name(length,'x');
            for (const auto mode : {Manager::LockMode::Shared,Manager::LockMode::Exclusive,
                    Manager::LockMode::IntentShared,Manager::LockMode::IntentExclusive,Manager::LockMode::Metadata}) {
                bool acquired = false;
                switch (mode) {
                    case Manager::LockMode::Shared: acquired=manager.lockShared(name);break;
                    case Manager::LockMode::Exclusive: acquired=manager.lockExclusive(name);break;
                    case Manager::LockMode::IntentShared: acquired=manager.lockIntentShared(name);break;
                    case Manager::LockMode::IntentExclusive: acquired=manager.lockIntentExclusive(name);break;
                    case Manager::LockMode::Metadata: acquired=manager.lockMetadata(name);break;
                }
                check("table "+std::to_string(length)+" mode "+std::to_string(int(mode)),acquired);
                if (acquired) manager.unlock(name);
            }
            check("row shared "+std::to_string(length),manager.rowLockSharedNoWait(name,INT64_MAX));
            manager.rowUnlock(name,INT64_MAX);
            check("row exclusive "+std::to_string(length),manager.rowLockExclusiveNoWait(name,INT64_MAX));
            manager.rowUnlock(name,INT64_MAX);
            check("page shared "+std::to_string(length),manager.pageLockShared(first,name,UINT32_MAX));
            manager.pageUnlock(first,name,UINT32_MAX);
            check("page exclusive "+std::to_string(length),manager.pageLockExclusive(first,name,UINT32_MAX));
            manager.pageUnlock(first,name,UINT32_MAX);
            check("gap "+std::to_string(length),manager.lockGap(name,"a","z"));
            manager.unlockGaps(name);
        }
        const std::string target(128,'q'), neighbor=std::string(127,'q')+"r";
        auto competitor = [&](const std::string& database,const std::string& name,bool expected) {
            return process([&] { Manager other;other.setResourceNamespace(database);other.setLockTimeout(20);
                const bool acquired=other.lockExclusive(name);if(acquired)other.unlock(name);return acquired==expected; });
        };
        check("hold long resource",manager.lockExclusive(target));
        check("same resource blocks another process",competitor(first,target,false));
        check("different suffix remains independent",competitor(first,neighbor,true));
        check("same resource in another database remains independent",competitor(second,target,true));
        manager.unlock(target);
        check("release permits another process",competitor(first,target,true));
        // Short names retain their historical byte-for-byte pathname, so old
        // and new backends still meet at the same existing advisory token.
        check("short acquisition",manager.lockShared("abc"));manager.unlock("abc");
        check("legacy short filename",fs::is_regular_file(fs::path(first)/".lockmgr/table-616263.lock"));
        // Encoding remains injective for the old short-name path spellings.
        for (const auto& name : std::vector<std::string>{"a/b","a_b","a\\b","quo\"ted",std::string("a\xffz",3)})
            check("distinct encoded resource",manager.lockExclusive(name));
        manager.unlockAll();
        check("all lock filenames bounded",std::all_of(fs::directory_iterator(fs::path(first)/".lockmgr"),
            fs::directory_iterator(),[](const auto& e){return e.path().filename().string().size()<=255;}));
        // Prepared ownership serializes full logical names, not file locators.
        check("prepare acquisition",manager.lockExclusive(target));
        std::vector<Manager::PreparedLockInfo> records;
        check("prepared record retains full identity",manager.getPreparedLockInfos(records) && records.size()==1 &&
            records[0].table==target && records[0].dbname==first);
        check("suspend prepared lock",manager.suspendCurrentLocksForPrepared(223));
        check("prepared lock blocks another process",competitor(first,target,false));
        manager.releasePreparedLocks(223);
        check("restore prepared lock",manager.restorePreparedLocks(224,first,records));
        check("restored prepared lock blocks another process",competitor(first,target,false));
        manager.releasePreparedLocks(224);
        check("prepared release permits another process",competitor(first,target,true));
    }
    // Only this invocation's mkdtemp-owned directory is removed.
    fs::remove_all(root);
    std::cout << "LONG_RESOURCE_LOCK_COMPLETE " << controls << " FAILURES " << failures << std::endl;
    return failures ? 1 : 0;
}
