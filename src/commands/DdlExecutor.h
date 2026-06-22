// ============================================================================
// DDL AST Executor — Phase 4 Wave 0.3
//
// 将 Parser 产出的 DDL AST 直接驱动 StorageEngine，逐步替代 main.cpp 中基于
// 字符串前缀的 DDL 分发。当前覆盖核心 DDL：
//   - CREATE / DROP DATABASE
//   - CREATE / DROP SCHEMA
//   - CREATE / DROP TABLE
//   - CREATE / DROP INDEX
//   - CREATE / DROP SEQUENCE
//   - CREATE / DROP DOMAIN
//   - CREATE / DROP TYPE (composite)
//   - COMMENT ON TABLE / COLUMN
//
// 未覆盖命令返回 false，由 main.cpp 回退到原有字符串处理分支。
// ============================================================================

#pragma once

#include "parser/ast.h"
#include "commands/TableManage.h"
#include "commands/DdlTransaction.h"
#include "Session.h"
#include <string>

namespace dbms {

class DdlExecutor {
public:
    DdlExecutor() = default;

    // 执行已解析的 DDL AST。返回值语义与 main.cpp::execute() 一致：
    //   false = 成功，true = 错误。
    bool execute(const StmtPtr& stmt, Session& s);

    // 便捷入口：解析并执行 SQL。
    bool executeSql(const std::string& sql, Session& s);

private:
    bool executeCreateTable(const CreateTableStmt* stmt, Session& s);
    bool executeDropTable(const DropStmt* stmt, Session& s);
    bool executeCreateIndex(const CreateIndexStmt* stmt, Session& s);
    bool executeCreateSequence(const CreateObjectStmt* stmt, Session& s);
    bool executeDropSequence(const DropStmt* stmt, Session& s);
    bool executeCreateDomain(const CreateObjectStmt* stmt, Session& s);
    bool executeDropDomain(const DropStmt* stmt, Session& s);
    bool executeCreateType(const CreateObjectStmt* stmt, Session& s);
    bool executeDropType(const DropStmt* stmt, Session& s);
    bool executeCreateDatabase(const CreateObjectStmt* stmt, Session& s);
    bool executeDropDatabase(const DropStmt* stmt, Session& s);
    bool executeCreateSchema(const CreateObjectStmt* stmt, Session& s);
    bool executeDropSchema(const DropStmt* stmt, Session& s);
    bool executeComment(const CommentStmt* stmt, Session& s);

    // 辅助：AST -> StorageEngine 结构转换
    static Column columnDefToColumn(const ColumnDef& cd);
    static ForeignKey tableConstraintToForeignKey(const TableConstraint& tc);
    static void recordConstraintCompat(const std::string& dbname,
                                       const std::string& tablename,
                                       const TableConstraint& tc);

    // 事务隐式提交（PG 语义；后续 Wave 5 移除）
    static void checkAndImplicitCommit(Session& s);
};

// DDL AST bridge 入口。由 main.cpp::execute() 在字符串分发前调用。
//   - handled 出参：true 表示 sql 属于 bridge 覆盖的 DDL 类型（14 种），并已执行。
//   - 返回值：false=成功，true=错误（与 main.cpp::execute() 一致）。
//   - CTAS (CREATE TABLE ... AS SELECT ...) 因 parser 暂不捕获 SELECT 子句，
//     这里设置 handled=false，让调用方回退到原有内联 CTAS 路径。
bool tryDdlBridge(const std::string& sql, dbms::SqlCommand parsedCmd,
                  Session& s, bool& handled);

} // namespace dbms
