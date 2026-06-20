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
#include "Session.h"
#include <string>

namespace dbms {

class DdlExecutor {
public:
    DdlExecutor() = default;

    // 若该命令已被本执行器处理，返回 true；否则返回 false，调用方应回退到
    // 原有的字符串分发逻辑。
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

} // namespace dbms
