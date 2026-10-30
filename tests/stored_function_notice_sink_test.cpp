#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "process/OutputCapture.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <sstream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    const auto database = testDbPath("stored_function_notice_sink");
    assert(owner.createDatabase(database) == DBStatus::OK);
    assert(owner.createUDF(database, "notice_sink", {}, {},
        "BEGIN RAISE NOTICE 'native notice'; RAISE WARNING 'native warning'; RETURN 7; END;",
        'v', "plpgsql", "integer") == DBStatus::OK);
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls; if (!good) ++failures;
        std::cout << "STORED_NOTICE " << label << " pass=" << good << std::endl;
    };
    for (const bool trigger : {false, true}) {
        std::ostringstream output;
        std::string value;
        bool isNull = true, ok = false;
        {
            ScopedOutputCapture capture(output);
            ok = trigger
                ? owner.callUDFWithCtx(database, "notice_sink", {}, StorageEngine::TriggerCtx{}, value, &isNull)
                : owner.callUDF(database, "notice_sink", {}, value, &isNull);
        }
        check(ok && !isNull && value == "7", trigger ? "trigger-context-result" : "ordinary-result");
        check(output.str() == "NOTICE: native notice\nWARNING: native warning\n",
              trigger ? "trigger-context-diagnostics" : "ordinary-diagnostics");
        check(!owner.inTransaction(), trigger ? "trigger-context-owner-complete" : "ordinary-owner-complete");
    }
    assert(owner.createUDF(database, "notice_error", {}, {},
        "BEGIN RAISE NOTICE 'before failure'; RAISE SQLSTATE '22012'; RETURN 7; END;",
        'v', "plpgsql", "integer") == DBStatus::OK);
    std::ostringstream output;
    std::string state, value;
    {
        ScopedOutputCapture capture(output);
        try { (void)owner.callUDF(database, "notice_error", {}, value); }
        catch (const DbError& error) { state = error.sqlState(); }
    }
    check(state == "22012" && output.str() == "NOTICE: before failure\n", "notice-before-exact-error");
    check(!owner.inTransaction() && owner.getLockManager().captureCheckpoint().tableCounts.empty(), "error-owner-and-lock-cleanup");
    std::cout << "STORED_NOTICE_COMPLETE " << controls << " FAILURES " << failures << std::endl;
    if (controls != 8) return 2;
    return failures ? 1 : 0;
}
