#include "catalog/catalog.h"
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/resource.h>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>

#ifdef EXPECT_SINGLE_PASS_CATALOG_PERSISTENCE
#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <chrono>
#include <iterator>
#include <map>
#include <string_view>
#include <tuple>
#endif

namespace fs = std::filesystem;
using dbms::CatalogManager;
namespace {
unsigned checked = 0;
void check(bool value, const char* what) {
    ++checked;
    if (!value) throw std::runtime_error(what);
}
#ifdef EXPECT_BATCHED_CATALOG_SCAN
thread_local bool raceEnabled = false;
thread_local bool raceFired = false;
thread_local int raceMode = 1;
std::string liveDirectory, displacedDirectory, replacementDirectory;
#endif
#ifdef EXPECT_SINGLE_PASS_CATALOG_PERSISTENCE
thread_local std::map<std::string, unsigned>* countedImages = nullptr;
thread_local unsigned replacementLoadProbes = 0;
std::string liveDescription, stagedDescription;

bool catalogImage(std::string_view name) {
    return name.size() > 7 && name.substr(0, 3) == "pg_" && name.substr(name.size() - 4) == ".cat";
}

void countCatalogImage(const char* path) {
    if (!countedImages || !path) return;
    const auto name = fs::path(path).filename().string();
    if (catalogImage(name)) ++(*countedImages)[name];
}

using CatalogInventory = std::map<std::string, std::tuple<ino_t, fs::file_time_type, std::string>>;

CatalogInventory catalogInventory(const fs::path& directory) {
    CatalogInventory inventory;
    for (const auto& entry : fs::directory_iterator(directory)) {
        const auto name = entry.path().filename().string();
        if (!catalogImage(name) && name != ".oid_counter") continue;
        struct stat file{};
        check(::lstat(entry.path().c_str(), &file) == 0 && S_ISREG(file.st_mode), "regular catalog image");
        std::ifstream input(entry.path(), std::ios::binary);
        check(input.is_open(), "open catalog inventory image");
        std::string bytes((std::istreambuf_iterator<char>(input)), {});
        inventory.emplace(name, std::make_tuple(file.st_ino, entry.last_write_time(), std::move(bytes)));
    }
    return inventory;
}

bool persistDuringRace(dbms::CatalogService& service, const std::string& database, int mode) {
    struct ResetRace {
        ~ResetRace() { raceEnabled = false; }
    } reset;
    raceMode = mode;
    raceFired = false;
    raceEnabled = true;
    return service.persistAll(database);
}

void checkSecondPublication(dbms::StorageEngine& engine, const std::string& database,
                            const fs::path& directory,
                            const std::shared_ptr<CatalogManager>& lease, dbms::Oid oid) {
    using namespace dbms;
    auto& service = engine.catalogService();
    StorageEngine blocker;
    blocker.setBackgroundIntervals(60000, 60000);
    const auto original = lease->getDescription(oid, PgClassOid_Namespace, 0);
    CatalogManager peer(directory.string(), false);
    peer.setDescription(oid, PgClassOid_Namespace, 0, "first peer");
    check(peer.persistAll(), "publish the first peer generation");
    const auto description = directory / "pg_description.cat";
    const auto staged = directory.parent_path() / ".test-peer-next.cat";
    {
        std::ifstream input(description, std::ios::binary);
        std::string bytes((std::istreambuf_iterator<char>(input)), {});
        const auto start = bytes.find("first peer");
        check(start != std::string::npos, "find the first peer's text in the real image");
        bytes.replace(start, std::string_view("first peer").size(), "next peer");
        std::ofstream output(staged, std::ios::binary);
        output << bytes;
        output.close();
        check(bool(output), "stage a second valid catalog image");
    }
    liveDescription = description.string();
    stagedDescription = staged.string();
    replacementLoadProbes = 0;
    const bool published = persistDuringRace(service, database, 4);
    check(raceFired && !published, "peer image arriving before install prevents stale publication");
    check(lease->getDescription(oid, PgClassOid_Namespace, 0) == original,
          "concurrent peer leaves the retained original model unchanged");

    // Under a foreign writer, acquire must return the cached generation
    // without loading the current files and thereby masking a stale install.
    check(blocker.beginTransaction(database, true) == DBStatus::OK, "hold catalog read ownership");
    check(service.acquire(database) == lease, "failed refresh never installed the stale generation");
    check(blocker.commitTransaction() == DBStatus::OK, "release catalog read ownership");
    check(service.persistAll(database), "retry publishes the newest complete peer generation");
    const auto newest = service.acquire(database);
    check(newest != lease && newest->getDescription(oid, PgClassOid_Namespace, 0) == "next peer",
          "successful retry replaces the cached generation without rewriting the peer");
}

void checkServicePublication() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    engine.setBackgroundIntervals(60000, 60000);
    const auto database = testDbPath("catalog_single_pass");
    check(engine.createDatabase(database) == DBStatus::OK, "create catalog publication database");
    auto& service = engine.catalogService();
    (void)service.get(database); // Install the real manager in the service cache.
    check(service.persistAll(database), "warm catalog publication receipts");
    const auto lease = service.acquire(database);
    const auto* row = lease->findNamespaceByName("public");
    check(row != nullptr, "borrow an actual cached namespace");
    const auto rowOid = row->oid;
    const auto directory = engine.dbPath(database) / "pg_catalog";
    std::map<std::string, unsigned> expected;
    for (const auto& entry : fs::directory_iterator(directory)) {
        const auto name = entry.path().filename().string();
        if (catalogImage(name)) expected.emplace(name, 1);
    }
    check(!expected.empty(), "catalog contains actual persisted families");
    std::map<std::string, unsigned> observed;
    {
        struct ResetCount {
            ~ResetCount() { countedImages = nullptr; }
        } reset;
        countedImages = &observed;
        check(service.persistAll(database), "warm service publication succeeds");
    }
    unsigned probes = 0;
    for (const auto& image : observed) probes += image.second;
    std::cout << "CATALOG_SERVICE_SCAN families=" << observed.size() << " probes=" << probes << '\n';
    check(observed == expected, "warm service publication checks each family once");

    const auto alternate = directory.parent_path() / ".test-catalog-alternate";
    const auto displaced = directory.parent_path() / ".test-catalog-displaced";
    fs::copy(directory, alternate, fs::copy_options::recursive);
    // Detect even a same-byte counter write without relying on clock precision.
    for (const auto& target : {directory, alternate}) {
        const auto counter = target / ".oid_counter";
        fs::last_write_time(counter, fs::last_write_time(counter) - std::chrono::hours(1));
    }
    const auto original = catalogInventory(directory);
    const auto replacement = catalogInventory(alternate);
    liveDirectory = directory.string();
    displacedDirectory = displaced.string();
    replacementDirectory = alternate.string();
    const bool published = persistDuringRace(service, database, 1);
    check(raceFired, "directory swap interleaved with service publication");
    const bool unchanged = original == catalogInventory(displaced) && replacement == catalogInventory(directory);
    fs::remove_all(directory);
    fs::rename(displaced, directory);
    check(!published, "service publication rejects a directory swap");
    check(unchanged, "service publication leaves both directory generations untouched");
    check(service.acquire(database) == lease && lease->findNamespaceByName("public") == row &&
              row->oid == rowOid, "failed publication keeps the old service lease intact");
    check(service.persistAll(database), "service publication retries after directory restoration");
    checkSecondPublication(engine, database, directory, lease, rowOid);
    check(engine.dropDatabase(database) == DBStatus::OK, "drop catalog publication database");
}
#endif
}
#ifdef EXPECT_BATCHED_CATALOG_SCAN
extern "C" int __real_fstatat(int, const char*, struct stat*, int);
extern "C" int __wrap_fstatat(int dirfd, const char* name, struct stat* info, int flags) {
#ifdef EXPECT_SINGLE_PASS_CATALOG_PERSISTENCE
    countCatalogImage(name);
    if (raceEnabled && raceMode == 4) {
        // Cold loading inspects the image before and after reading it. Wait for
        // the replacement's following publication scan before swapping it.
        if (!raceFired && replacementLoadProbes >= 2 && name && std::string_view(name) == "pg_description.cat") {
            raceFired = true;
            if (::rename(stagedDescription.c_str(), liveDescription.c_str()) != 0) std::abort();
        }
        return __real_fstatat(dirfd, name, info, flags);
    }
#endif
    if (raceEnabled && !raceFired && name && std::string(name).find("pg_") == 0) {
        raceFired = true;
        if (::rename(liveDirectory.c_str(), displacedDirectory.c_str()) != 0)
            std::abort();
        if (raceMode==2) {
            if (::symlink(replacementDirectory.c_str(), liveDirectory.c_str()) != 0) std::abort();
        } else if (::rename(replacementDirectory.c_str(), liveDirectory.c_str()) != 0) std::abort();
        if (raceMode==3) {
            const int result=__real_fstatat(dirfd, name, info, flags);
            const int priorErrno=errno;
            if (::rename(liveDirectory.c_str(), replacementDirectory.c_str()) != 0 ||
                ::rename(displacedDirectory.c_str(), liveDirectory.c_str()) != 0) std::abort();
            errno=priorErrno;
            return result;
        }
    }
    return __real_fstatat(dirfd, name, info, flags);
}
#endif
#ifdef EXPECT_SINGLE_PASS_CATALOG_PERSISTENCE
extern "C" int __real_lstat(const char*, struct stat*);
extern "C" int __wrap_lstat(const char* path, struct stat* info) {
    countCatalogImage(path);
    if (raceEnabled && raceMode == 4 && path && liveDescription == path) ++replacementLoadProbes;
    return __real_lstat(path, info);
}
#endif
int main() {
    char temporary[]="/tmp/dbms-catalog-publication-scan-XXXXXX";
    char* made=::mkdtemp(temporary);
    if (!made) return 2;
    const fs::path base(made), root=base/"catalog";
    try {
        fs::create_directory(root);
        {
            CatalogManager owner(root.string(), false);
            const auto ns=owner.createNamespace("public", 1);
            dbms::PgClassRow relation; relation.relname="rows";relation.relnamespace=ns;
            const auto oid=owner.createClass(relation);
            owner.setDescription(oid,1259,0,"before");
            check(owner.persistAll(),"publish initial catalog");
            CatalogManager observer(root.string(), false);
            const auto current=CatalogManager::PublicationState::Current;
            const auto peer=CatalogManager::PublicationState::CleanPeerChange;
            const auto conflict=CatalogManager::PublicationState::Conflict;
            check(observer.publicationState()==current,"initial current");
            const pid_t child=::fork();
            check(child>=0,"fork peer");
            if (child==0) {
                try {
                    CatalogManager peerOwner(root.string(), false);
                    peerOwner.setDescription(oid,1259,0,"after!");
                    ::_exit(peerOwner.persistAll()?0:1);
                } catch (...) { ::_exit(2); }
            }
            int status=0;check(::waitpid(child,&status,0)==child && WIFEXITED(status) && WEXITSTATUS(status)==0,"peer committed in separate process");
            check(observer.publicationState()==peer,"cross-process change detected");
            check(observer.getDescription(oid,1259,0)=="before","peer check preserves old snapshot");
            observer.loadAll();
            check(observer.publicationState()==current && observer.getDescription(oid,1259,0)=="after!","load peer generation");
            auto* borrowed=const_cast<dbms::PgClassRow*>(observer.findClass(oid));
            check(borrowed!=nullptr,"borrow local row");borrowed->relpages=91;
            {
                CatalogManager writer(root.string(), false);
                writer.setDescription(oid,1259,0,"peer!!");
                check(writer.persistAll(),"second peer publication");
            }
            check(observer.publicationState()==conflict,"dirty local model conflicts");
            check(observer.findClass(oid)->relpages==91 && !observer.persistAll(),"dirty model retained on conflict");
            observer.loadAll();check(observer.publicationState()==current,"refresh clears dirty conflict");
            const fs::path description=root/"pg_description.cat", saved=base/"description.saved";
            fs::rename(description,saved);
            check(observer.publicationState()==conflict,"missing catalog file fails closed");
            fs::create_symlink(saved,description);
            check(observer.publicationState()==conflict,"catalog symlink is not followed");
            fs::remove(description);fs::create_directory(description);
            check(observer.publicationState()==conflict,"nonregular catalog fails closed");
            fs::remove(description);fs::rename(saved,description);
            observer.loadAll();check(observer.publicationState()==current,"restore original file");
            fs::copy_file(description,base/"replacement.cat");fs::rename(base/"replacement.cat",description);
            check(observer.publicationState()==peer,"atomic same-bytes replacement detected");
            observer.loadAll();check(observer.publicationState()==current,"load atomic replacement");
            // An image initially absent is part of the baseline, not an
            // automatic conflict. Its later appearance is a peer change.
            {
                const auto freshPath=base/"initially-absent";
                fs::create_directory(freshPath);
                CatalogManager fresh(freshPath.string(), false);
                check(fresh.publicationState()==current,"absent initial files are current");
                { std::ofstream out(freshPath/"pg_description.cat");check(bool(out),"create previously absent image"); }
                check(fresh.publicationState()==peer,"previously absent image appearance detected");
            }
            const fs::path moved=base/"catalog.moved";
            fs::rename(root,moved);
            check(observer.publicationState()==conflict,"missing catalog directory fails closed");
            fs::create_directory_symlink(moved,root);
            check(observer.publicationState()==current,"parent symlink follows same directory inode");
            fs::remove(root);fs::copy(moved,root,fs::copy_options::recursive);
            check(observer.publicationState()==peer,"replacement directory detected");
            fs::remove_all(root);fs::rename(moved,root);
            check(observer.publicationState()==current,"original directory identity restored");
            if (::geteuid()!=0) {
                check(::chmod(root.c_str(),0111)==0,"set execute-only directory");
                check(::access(root.c_str(),R_OK)!=0,"directory read denied");
                check(observer.publicationState()==current,"execute-only directory supports scan");
                check(::chmod(root.c_str(),0700)==0,"restore directory permissions");
            } else std::cout<<"CATALOG_SCAN_EXECUTE_ONLY=SKIPPED_ROOT\n";
            {
                struct rlimit original{}, lowered{};
                check(::getrlimit(RLIMIT_NOFILE,&original)==0,"read file descriptor limit");
                lowered=original;lowered.rlim_cur=std::min<rlim_t>(original.rlim_cur,64);
                check(::setrlimit(RLIMIT_NOFILE,&lowered)==0,"lower file descriptor limit");
                std::vector<int> held;
                while (true) { int fd=::open("/dev/null",O_RDONLY|O_CLOEXEC);if(fd<0)break;held.push_back(fd); }
                const bool exhausted=errno==EMFILE;
                const auto state=observer.publicationState();
                for(int fd:held)::close(fd);
                const bool restored=::setrlimit(RLIMIT_NOFILE,&original)==0;
                check(restored,"restore file descriptor limit");
                check(exhausted,"exercise descriptor exhaustion");
                check(state==current,"descriptor exhaustion preserves stat-only current read");
            }
#ifdef EXPECT_BATCHED_CATALOG_SCAN
            const auto alternate=base/"alternate", displaced=base/"displaced";
            fs::copy(root,alternate,fs::copy_options::recursive);
            liveDirectory=root.string();displacedDirectory=displaced.string();replacementDirectory=alternate.string();
            raceEnabled=true;
            const auto raced=observer.publicationState();
            raceEnabled=false;
            check(raceFired,"directory swap interleaved with file scan");
            check(raced==conflict,"directory pathname change during scan fails closed");
            fs::remove_all(root);fs::rename(displaced,root);
            check(observer.publicationState()==current,"restore original after concurrent directory swap");
            fs::copy(root,alternate,fs::copy_options::recursive);
            raceMode=2;raceFired=false;raceEnabled=true;
            const auto symlinkRace=observer.publicationState();raceEnabled=false;
            check(raceFired && symlinkRace==conflict,"mid-scan parent symlink replacement fails closed");
            fs::remove(root);fs::remove_all(alternate);fs::rename(displaced,root);
            check(observer.publicationState()==current,"restore after parent symlink replacement");
            fs::copy(root,alternate,fs::copy_options::recursive);
            raceMode=3;raceFired=false;raceEnabled=true;
            const auto aba=observer.publicationState();raceEnabled=false;
            check(raceFired && aba==current,"swap-and-restore ABA is not detected");
            fs::remove_all(alternate);
#endif
        }
#ifdef EXPECT_SINGLE_PASS_CATALOG_PERSISTENCE
        checkServicePublication();
#endif
        fs::remove_all(base);
        std::cout<<"CATALOG_PUBLICATION_SCAN_CHECKED="<<checked<<" FAILURES=0\n";
        return 0;
    } catch (const std::exception& e) {
        ::chmod(root.c_str(),0700);
        std::cerr<<"CATALOG_PUBLICATION_SCAN_FAIL "<<checked<<" "<<e.what()<<"\n";
        fs::remove_all(base);
        return 1;
    }
}
