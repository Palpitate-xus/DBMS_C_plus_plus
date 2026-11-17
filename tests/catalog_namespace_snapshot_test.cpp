// Namespace reads must preserve catalog visibility, ownership and file validation.
// The test runner provides a fresh private working directory.
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <algorithm>
#include <atomic>
#include <map>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <iomanip>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <vector>
using namespace dbms;
namespace fs=std::filesystem;
using Row=std::tuple<Oid,std::string,Oid>;
struct Result { std::string state, message; std::vector<Row> rows; };
std::atomic<unsigned> checks{0};
void need(bool ok,const std::string& label) { ++checks; if(!ok) throw std::runtime_error(label); }
Result take(StorageEngine& e,const std::string& db,bool projected) {
    Result r;
    try {
        auto rows=projected ? e.catalogService().namespaceSnapshot(db) : e.catalogService().metadataSnapshot(db).namespaces;
        for(const auto& row:rows) r.rows.emplace_back(row.oid,row.nspname,row.nspowner);
    } catch(const DbError& ex) { r.state=ex.sqlState(); r.message=ex.what(); }
    return r;
}
Result compare(StorageEngine& e,const std::string& db,const std::string& label) {
    const auto a=take(e,db,false),b=take(e,db,true);
    std::cout<<label<<" old_state="<<std::quoted(a.state)<<" old_message="<<std::quoted(a.message)
             <<" new_state="<<std::quoted(b.state)<<" new_message="<<std::quoted(b.message)<<std::endl;
    for(const auto& [oid,name,owner]:a.rows) std::cout<<"  old "<<oid<<" "<<std::quoted(name)<<" "<<owner<<std::endl;
    for(const auto& [oid,name,owner]:b.rows) std::cout<<"  new "<<oid<<" "<<std::quoted(name)<<" "<<owner<<std::endl;
    need(a.state==b.state && a.message==b.message && a.rows==b.rows,label+" mismatch");
    return a;
}
bool has(const Result& r,const std::string& n) {
    return std::any_of(r.rows.begin(),r.rows.end(),[&](const auto& row){return std::get<1>(row)==n;});
}
std::map<std::string,std::string> images(const fs::path& directory) {
    std::map<std::string,std::string> result;
    for (const auto& file:fs::recursive_directory_iterator(directory)) {
        if (!file.is_regular_file()) continue;
        std::ifstream input(file.path(),std::ios::binary);
        result.emplace(file.path().lexically_relative(directory).string(),
            std::string(std::istreambuf_iterator<char>(input),{}));
    }
    return result;
}
int main() {
 try {
    TypeRegistry::instance().bootstrap();
    const std::string db="__t_namespace_projection", missing="__t_namespace_projection_missing";
    need(!fs::exists(db) && !fs::exists(missing),"fresh private CWD required");
    StorageEngine writer, warm, cold;
    writer.setBackgroundIntervals(60000,60000); warm.setBackgroundIntervals(60000,60000); cold.setBackgroundIntervals(60000,60000);
    need(writer.createDatabase(db)==DBStatus::OK,"create database");
    need(writer.createSchema(db,"before")==DBStatus::OK,"create schema");
    need(has(compare(writer,db,"warm"),"before"),"warm expected");
    need(writer.createSchema(db,"Quoted.Schema")==DBStatus::OK,"quoted schema storage name");
    const auto owned=take(writer,db,true);
    need(has(owned,"before") && has(owned,"Quoted.Schema"),"owned ordered namespace values");
    const auto directory=writer.dbPath(db)/"pg_catalog";
    const auto before=images(directory);
    need(has(compare(cold,db,"cold"),"before"),"cold expected");
    need(images(directory)==before,"cold namespace read does not change catalog files");
    need(!cold.catalogService().has(db),"cold namespace read does not populate cache");
    const auto full=writer.catalogService().metadataSnapshot(db);
    need(full.defaultObjectsIncluded && !full.relations.empty() && !full.types.empty(),
         "complete public snapshot still contains unrelated declarations");
    Result afterDestruction;
    {
        StorageEngine temporary;
        temporary.setBackgroundIntervals(60000,60000);
        afterDestruction=take(temporary,db,true);
    }
    need(afterDestruction.rows==owned.rows,"returned namespace values outlive reader engine");
    need(compare(writer,missing,"missing").rows.empty(),"missing empty");
    compare(writer,"info","info");
    const std::string empty="__t_namespace_projection_empty";
    need(!fs::exists(empty),"empty path must be fresh");
    fs::create_directory(empty);
    compare(cold,empty,"unbootstrapped-directory");
    need(!fs::exists(writer.dbPath(empty)/"pg_catalog"),"lookup must not bootstrap catalog");
    fs::remove(empty);
    (void)warm.catalogService().get(db);
    need(writer.createSchema(db,"peer_update")==DBStatus::OK,"peer update");
    need(has(compare(warm,db,"peer-update"),"peer_update"),"peer update visible");
    need(writer.beginTransaction(db,true)==DBStatus::OK,"begin transaction");
    need(writer.createSchema(db,"pending")==DBStatus::OK,"uncommitted schema");
    need(has(compare(writer,db,"transaction-owner"),"pending"),"owner sees pending");
    need(writer.savepoint("namespace_stage")==DBStatus::OK,"actual savepoint");
    need(writer.createSchema(db,"after_savepoint")==DBStatus::OK,"schema after savepoint");
    need(has(compare(writer,db,"savepoint-owner"),"after_savepoint"),"owner sees savepoint change");
    need(writer.rollbackToSavepoint("namespace_stage")==DBStatus::OK,"restore actual savepoint");
    const auto restored=compare(writer,db,"savepoint-restored");
    need(has(restored,"pending") && !has(restored,"after_savepoint"),"savepoint keeps preceding owner changes");
    need(!has(compare(warm,db,"transaction-peer"),"pending"),"peer sees committed");
    // Writer keeps its transaction open while a separate backend thread reads.
    std::promise<std::string> verdict; auto done=verdict.get_future();
    std::thread reader([&] {
        try {
            need(!has(compare(writer,db,"foreign-same-engine"),"pending"),"foreign same engine sees committed");
            need(!has(compare(warm,db,"foreign-warm-engine"),"pending"),"foreign warm engine sees committed");
            need(compare(cold,db,"foreign-cold-engine").state=="55P03","cold read fails closed");
            verdict.set_value("");
        } catch(const std::exception& e) { verdict.set_value(e.what()); }
    });
    if(done.wait_for(std::chrono::seconds(5))!=std::future_status::ready) {
        writer.rollbackTransaction(); reader.join(); throw std::runtime_error("reader blocked over five seconds");
    }
    const auto peerError=done.get(); reader.join(); need(peerError.empty(),peerError);
    need(writer.rollbackTransaction()==DBStatus::OK,"rollback");
    need(!has(compare(writer,db,"rollback"),"pending"),"rollback restored");
    need(!has(compare(warm,db,"peer-rollback"),"pending"),"peer rollback restored");
    const auto dirty=warm.catalogService().acquire(db);
    const auto dirtyOid=dirty->createNamespace("unpublished_local",INVALID_OID);
    need(writer.createSchema(db,"peer_conflict")==DBStatus::OK,"peer publishes");
    need(compare(warm,db,"publication-conflict").state=="58030","conflict fails closed");
    std::string error;
    need(dirty->dropObject(PgClassOid_Namespace,dirtyOid,CatalogManager::DropBehavior::Restrict,&error),"remove local edit");
    need(has(compare(warm,db,"conflict-recovery"),"peer_conflict"),"recovery");
    (void)warm.catalogService().get(db); // refresh to the clean current image before file faults
    const auto file=writer.dbPath(db)/"pg_catalog"/"pg_description.cat";
    const auto saved=file.string()+".validation-saved";
    need(fs::is_regular_file(file) && !fs::exists(saved),"real catalog file exists");
    fs::rename(file,saved);
    try {
        need(compare(warm,db,"missing-unrelated-catalog").state=="58030","missing unrelated family must conflict");
        // The relative link is interpreted from the catalog directory. Verify
        // that it reaches the saved regular file before testing rejection.
        fs::create_symlink(fs::path(saved).filename(),file);
        need(fs::is_symlink(file) && fs::is_regular_file(file) && fs::equivalent(file,saved),
             "symlink target must be the real existing saved catalog");
        need(compare(warm,db,"symlink-unrelated-catalog").state=="58030","valid symlink must conflict");
        fs::remove(file);
        fs::create_directory(file);
        need(compare(warm,db,"directory-unrelated-catalog").state=="58030","nonregular must conflict");
        fs::remove(file);
    } catch(...) { if(fs::exists(file)||fs::is_symlink(file)) fs::remove_all(file);fs::rename(saved,file);throw; }
    fs::rename(saved,file);
    compare(warm,db,"file-restored");
    (void)warm.catalogService().get(db);
    fs::rename(file,saved);
    try {
        { std::ofstream bad(file,std::ios::binary); bad<<"not a valid catalog image\n"; need(bool(bad),"write invalid image"); }
        compare(warm,db,"invalid-unrelated-image"); // Compare loader behavior even if it accepts this image.
        fs::remove(file);
    } catch(...) { if(fs::exists(file)) fs::remove_all(file);fs::rename(saved,file);throw; }
    fs::rename(saved,file);
    compare(warm,db,"invalid-image-restored");
    need(writer.dropDatabase(db)==DBStatus::OK,"drop physical database");
    need(writer.createDatabase(db)==DBStatus::OK,"replace physical database");
    need(writer.createSchema(db,"replacement")==DBStatus::OK,"replacement schema");
    const auto replaced=compare(warm,db,"physical-replacement");
    need(has(replaced,"replacement") && !has(replaced,"before"),"new physical generation");
    warm.catalogService().evict(db);
    need(has(compare(warm,db,"after-eviction"),"replacement"),"cold read after cache eviction");
    need(has(owned,"before") && has(owned,"Quoted.Schema") && !has(owned,"pending") &&
         !has(owned,"replacement"),"earlier owned values survive mutations and physical replacement");
    need(writer.dropDatabase(db)==DBStatus::OK,"cleanup database");
    std::cout<<"CATALOG_NAMESPACE_SNAPSHOT_PASS checks="<<checks.load()<<"\n";return 0;
 } catch(const std::exception& e) { std::cerr<<"FAIL: "<<e.what()<<"\n";return 1; }
}
