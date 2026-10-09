#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t checked = 0, failures = 0;
    const auto require = [&](bool pass, const std::string& role) {
        ++checked; failures += !pass;
        std::cout << "VIRTUAL_SHADOW " << role << " pass=" << pass << std::endl;
    };
    const auto database = testDbPath("virtual_relation_shadow_binding");
    require(g_engine.createDatabase(database) == DBStatus::OK,"isolated database");
    Session session; session.username="testuser"; session.permission=1; session.currentDB=database;
    const auto previous=currentSession(); setCurrentSession(&session);
    struct Restore { Session* value; ~Restore(){setCurrentSession(value);} } restore{previous};
    for (const auto* name : {"pg_stat_activity", "pg_settings", "pg_type"}) {
        TableSchema table; table.tablename=name; table.append(makeIntColumn("id",false,2));
        require(g_engine.createTable(database,table)==DBStatus::OK,std::string(name)+" physical user table");
        for (const auto& sql : {"SELECT id FROM "+std::string(name),
             "SELECT id FROM public."+std::string(name),
             "INSERT INTO "+std::string(name)+" VALUES(23)",
             "UPDATE "+std::string(name)+" SET id=DEFAULT"}) {
            try {
                const auto query=g_engine.prepareBoundQuery(database,sql);
                for(const auto& range:query.sourceRanges) {
                    std::cout << "VIRTUAL_SHADOW_DESCRIPTOR " << sql << ' ' << range.relationSchema << '.' << range.relationName;
                    for(const auto& column:range.columns)std::cout << ' ' << column.name << ':' << column.type;
                    std::cout << std::endl;
                }
                require(query.sourceRanges.size()==1 && query.sourceRanges[0].relationSchema=="public" &&
                    query.sourceRanges[0].relationName==name && query.sourceRanges[0].columns.size()==1 &&
                    query.sourceRanges[0].columns[0].name=="id" &&
                    query.sourceRanges[0].columns[0].type=="integer",sql+" physical identity/descriptor");
            } catch (const DbError& error) {
                require(false,sql+" unexpected "+error.sqlState()+" "+error.what());
            }
        }
        const std::string catalogColumn=std::string(name)=="pg_stat_activity"?"pid":
            std::string(name)=="pg_settings"?"name":"oid";
        try {
            const auto query=g_engine.prepareBoundQuery(database,"SELECT "+catalogColumn+" FROM pg_catalog."+name);
            require(query.sourceRanges.size()==1 && query.sourceRanges[0].relationSchema=="pg_catalog" &&
                query.sourceRanges[0].relationName==name && query.output.size()==1 &&
                query.output[0].name==catalogColumn,std::string(name)+" explicit virtual identity");
        } catch (const DbError& error) {
            require(false,std::string(name)+" explicit catalog unexpected "+error.sqlState());
        }
    }
    std::cout << "VIRTUAL_SHADOW_CHECKED=" << checked << " FAILED=" << failures << '\n';
    return failures?1:0;
}
