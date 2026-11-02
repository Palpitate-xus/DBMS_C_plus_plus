#include "executor/ExecutionPlan.h"
#include "catalog/type_registry.h"
#include "Config.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <optional>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

static size_t sortDescriptors() {
    size_t count=0;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
        std::error_code error;const auto link=std::filesystem::read_symlink(entry.path(),error).string();
        count += !error && link.find("/.dbms-sort-")!=std::string::npos;
    }
    return count;
}
static std::optional<std::string> payload(size_t id) {
    if (id%11==0) return std::nullopt;
    if (id%11==1) return std::string();
    if (id%11==2) return std::string("NULL");
    return "row β " + std::to_string(id) + " " + std::string(8192,'a'+id%23);
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto original=g_config.workMemKb;g_config.workMemKb=1;
    const auto database=testDbPath("sort_spill_heap");
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    TableSchema schema;schema.tablename="rows";schema.formatVersion=DATA_FILE_FORMAT_VERSION;
    schema.append(makeIntColumn("id",false,2));
    schema.append(makeIntColumn("k",true,4));schema.append(makeTextColumn("payload",true));
    assert(g_engine.createTable(database,schema)==DBStatus::OK);
    assert(g_engine.beginTransaction(database)==DBStatus::OK);
    for (size_t id=0;id<64;++id)
        assert(g_engine.insertRow(database,schema.tablename,
            {{"id",std::to_string(id)},
             {"k",id%7==0 ? std::nullopt : std::optional<std::string>(std::to_string(9007199254740993LL+id))},
             {"payload",payload(id)}})==DBStatus::OK);
    assert(g_engine.commitTransaction()==DBStatus::OK);
    schema=g_engine.getTableSchema(database,schema.tablename);
    size_t checked=0,failures=0;
    const auto check=[&](const char* label,bool valid) {
        ++checked;failures+=!valid;std::cout<<"SORT_SPILL_HEAP "<<label<<" passed="<<valid<<'\n';
    };
    for (bool ascending : {false,true}) for (bool first : {false,true}) {
        auto sort=std::make_unique<SortOp>(std::make_unique<TableScanOp>(&g_engine,database,schema.tablename),
            schema,"k",ascending,first,true);
        auto* observed=sort.get();ProjectOp plan(std::move(sort),schema,{});
        bool good=plan.open() && sortDescriptors()==1;
        std::vector<bool> seen(64,false);std::string raw;size_t count=0;
        int64_t previous=0;bool havePrevious=false,previousNull=false;
        while (plan.next(raw)) {
            std::vector<std::string> cells;std::vector<bool> nulls,stored;
            if (!plan.lastStructuredRow(cells,nulls) || cells.size()!=3 || nulls.size()!=3) {good=false;break;}
            const auto id=std::stoul(cells[0]);
            if (id>=64) {good=false;break;}
            const auto text=payload(id);const bool keyNull=id%7==0;
            good=good && !seen[id] && nulls==std::vector<bool>({false,keyNull,!text}) &&
                (keyNull || cells[1]==std::to_string(9007199254740993LL+id)) &&
                (text ? cells[2]==*text : cells[2].empty());
            const auto origin=observed->scanOrigin();
            good=good && origin.engine==&g_engine && origin.dbname==database && origin.tablename==schema.tablename &&
                origin.rid>0 && observed->lastStoredNullBitmap(stored) && stored==nulls;
            const int64_t value=keyNull ? 0 : std::stoll(cells[1]);
            if (havePrevious) {
                if (previousNull!=keyNull) good=good && previousNull==first;
                else if (!keyNull) good=good && (ascending ? previous<=value : previous>=value);
            }
            previous=value;previousNull=keyNull;havePrevious=true;seen[id]=true;++count;
        }
        good=good && count==64 && !plan.hasError();plan.close();
        good=good && sortDescriptors()==0 && g_engine.getLockManager().captureCheckpoint().tableCounts.empty();
        check("full scan values, bitmap, origin and close",good);
    }
    auto sort=std::make_unique<SortOp>(std::make_unique<TableScanOp>(&g_engine,database,schema.tablename),schema,"id",true);
    ProjectOp plan(std::move(sort),schema,{});assert(plan.open());std::string raw;
    for(size_t i=0;i<5;++i)assert(plan.next(raw));plan.close();
    check("early release",sortDescriptors()==0 && g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(plan.open());size_t count=0;
    while(plan.next(raw))++count;
    plan.close();check("same graph reopen",count==64 && !plan.hasError() && sortDescriptors()==0);
    assert(g_engine.dropDatabase(database)==DBStatus::OK);g_config.workMemKb=original;
    assert(checked==6);
    std::cout<<"SORT_SPILL_HEAP_COMPLETE "<<checked<<" FAILURES "<<failures<<'\n';
    return failures ? 1 : 0;
}
