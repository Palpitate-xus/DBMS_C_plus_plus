#pragma once

#include "dbms_defs.h"
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <optional>

namespace dbms {

// ============================================================================
// SQL 命令类型枚举
// Phase 1：替代 execute() 中的字符串前缀匹配
// ============================================================================

enum class SqlCommand {
    Unknown,

    // DQL
    Select,
    Values,

    // DML
    Insert,
    Update,
    Delete,
    Merge,
    Copy,
    Call,
    Do,

    // DDL — CREATE
    CreateTable,
    CreateTableAs,
    CreateIndex,
    CreateView,
    CreateMaterializedView,
    CreateDatabase,
    CreateSchema,
    CreateSequence,
    CreateDomain,
    CreateType,
    CreateFunction,
    CreateProcedure,
    CreateTrigger,
    CreateRole,
    CreateUser,
    CreateTablespace,
    CreateStatistics,
    CreatePolicy,
    CreateRule,
    CreateEventTrigger,
    CreateExtension,
    CreatePublication,
    CreateSubscription,
    CreateForeignDataWrapper,
    CreateForeignTable,
    CreateServer,
    CreateUserMapping,
    CreateTextSearchConfiguration,
    CreateTextSearchDictionary,
    CreateTextSearchParser,
    CreateTextSearchTemplate,
    CreateCast,
    CreateCollation,
    CreateConversion,
    CreateOperator,
    CreateOperatorClass,
    CreateOperatorFamily,
    CreateAggregate,
    CreateTransform,
    CreateLanguage,
    CreateAccessMethod,

    // DDL — DROP
    DropTable,
    DropIndex,
    DropView,
    DropMaterializedView,
    DropDatabase,
    DropSchema,
    DropSequence,
    DropDomain,
    DropType,
    DropFunction,
    DropProcedure,
    DropRoutine,
    DropTrigger,
    DropRole,
    DropUser,
    DropTablespace,
    DropStatistics,
    DropPolicy,
    DropRule,
    DropEventTrigger,
    DropExtension,
    DropPublication,
    DropSubscription,
    DropForeignDataWrapper,
    DropForeignTable,
    DropServer,
    DropUserMapping,
    DropTextSearchConfiguration,
    DropTextSearchDictionary,
    DropTextSearchParser,
    DropTextSearchTemplate,
    DropCast,
    DropCollation,
    DropConversion,
    DropOperator,
    DropOperatorClass,
    DropOperatorFamily,
    DropAggregate,
    DropTransform,
    DropLanguage,
    DropAccessMethod,
    DropOwned,
    DropLargeObject,

    // DDL — ALTER
    AlterTable,
    AlterIndex,
    AlterView,
    AlterMaterializedView,
    AlterDatabase,
    AlterSchema,
    AlterSequence,
    AlterDomain,
    AlterType,
    AlterFunction,
    AlterProcedure,
    AlterRoutine,
    AlterTrigger,
    AlterRole,
    AlterUser,
    AlterSystem,
    AlterTablespace,
    AlterStatistics,
    AlterPolicy,
    AlterRule,
    AlterEventTrigger,
    AlterExtension,
    AlterPublication,
    AlterSubscription,
    AlterForeignDataWrapper,
    AlterForeignTable,
    AlterServer,
    AlterUserMapping,
    AlterTextSearchConfiguration,
    AlterTextSearchDictionary,
    AlterTextSearchParser,
    AlterTextSearchTemplate,
    AlterCollation,
    AlterConversion,
    AlterOperator,
    AlterOperatorClass,
    AlterOperatorFamily,
    AlterAggregate,
    AlterTransform,
    AlterLanguage,
    AlterLargeObject,
    AlterDefaultPrivileges,

    // DDL — TRUNCATE
    Truncate,

    // DDL — RENAME
    Rename,

    // TCL
    Begin,
    StartTransaction,
    Commit,
    Rollback,
    Abort,
    End,
    Savepoint,
    ReleaseSavepoint,
    RollbackToSavepoint,
    PrepareTransaction,
    CommitPrepared,
    RollbackPrepared,

    // DCL
    Grant,
    Revoke,
    AlterDefaultPrivilegesGrant,
    AlterDefaultPrivilegesRevoke,

    // Session / GUC
    Set,
    Show,
    Reset,
    SetRole,
    SetSessionAuthorization,
    SetConstraints,
    SetTransaction,
    Discard,

    // Utility
    Explain,
    Analyze,
    Vacuum,
    Checkpoint,
    Reindex,
    RefreshMaterializedView,
    Cluster,
    Comment,
    SecurityLabel,
    Listen,
    Notify,
    Unlisten,
    Load,

    // Locking
    Lock,

    // Cursor
    Declare,
    Fetch,
    Move,
    Close,
    Deallocate,

    // Prepared statement
    Prepare,
    Execute,

    // Import / Export
    ImportForeignSchema,

    // Non-PG syntax (Phase 11 清理)
    UseDatabase,
    ReplaceInto,
    LoadDataInfile,
    SelectIntoOutfile,
    Desc,
    ViewTable,
    ViewDatabase,
    UpdateLimit,
    DeleteLimit,
};

// ============================================================================
// AST 节点基类
// ============================================================================

class ASTNode {
public:
    virtual ~ASTNode() = default;
    virtual std::string toString() const = 0;
};

// ============================================================================
// 表达式节点
// ============================================================================

enum class ExprType {
    Literal,
    ColumnRef,
    UnaryOp,
    BinaryOp,
    FunctionCall,
    CaseExpr,
    CastExpr,
    Subquery,
    ArrayExpr,
    RowExpr,
    Parameter,
    A_Star,       // SELECT *
};

class Expr : public ASTNode {
public:
    ExprType type;
    virtual ~Expr() = default;
};

using ExprPtr = std::unique_ptr<Expr>;

// 字面量
struct LiteralExpr : public Expr {
    std::string value;     // 原始文本
    std::string typeName;  // 推断/指定的类型
    LiteralExpr() { type = ExprType::Literal; }
    std::string toString() const override { return value; }
};

// 列引用
struct ColumnRefExpr : public Expr {
    std::string schema;    // 可为空（无 schema 限定）
    std::string table;     // 可为空（无表限定）
    std::string column;
    ColumnRefExpr() { type = ExprType::ColumnRef; }
    std::string toString() const override {
        if (!schema.empty()) return schema + "." + table + "." + column;
        return table.empty() ? column : table + "." + column;
    }
};

// 一元操作符
struct UnaryOpExpr : public Expr {
    std::string op;        // +, -, NOT, IS NULL, IS NOT NULL
    ExprPtr operand;
    UnaryOpExpr() { type = ExprType::UnaryOp; }
    std::string toString() const override {
        return op + " " + (operand ? operand->toString() : "?");
    }
};

// 二元操作符
struct BinaryOpExpr : public Expr {
    std::string op;        // =, <>, <, >, <=, >=, +, -, *, /, %, AND, OR, LIKE, ILIKE, IN, BETWEEN
    ExprPtr left;
    ExprPtr right;
    BinaryOpExpr() { type = ExprType::BinaryOp; }
    std::string toString() const override {
        return (left ? left->toString() : "?") + " " + op + " " +
               (right ? right->toString() : "?");
    }
};

// 窗口函数定义（ also used by FunctionCallExpr for OVER clauses）
struct WindowDef {
    std::string name;
    std::vector<ExprPtr> partitionBy;
    std::vector<std::pair<ExprPtr, bool>> orderBy;
    std::string frameMode;  // ROWS, RANGE, GROUPS
    ExprPtr frameStart;
    ExprPtr frameEnd;
    std::string frameExclusion; // CURRENT ROW, GROUP, TIES, NO OTHERS
};

// 函数调用
struct FunctionCallExpr : public Expr {
    std::string schema;          // schema-qualified function (pg_catalog.now())
    std::string funcName;
    std::vector<ExprPtr> args;
    bool distinct = false;       // COUNT(DISTINCT col)
    ExprPtr filter;              // FILTER (WHERE ...)
    std::string orderBy;         // 聚合中的 ORDER BY within aggregate

    struct NamedArg {
        std::string name;
        ExprPtr value;
    };
    std::vector<NamedArg> namedArgs; // func(a => 1, b => 2)
    WindowDef over;              // window specification for row_number() OVER (...)
    bool hasOver = false;        // true if OVER clause was parsed

    FunctionCallExpr() { type = ExprType::FunctionCall; }
    std::string toString() const override;
};

// CAST(expr AS type)
struct CastExpr : public Expr {
    ExprPtr operand;
    std::string typeName;
    std::vector<std::string> typeMods;
    CastExpr() { type = ExprType::CastExpr; }
    std::string toString() const override {
        return "CAST(" + (operand ? operand->toString() : "?") + " AS " + typeName + ")";
    }
};

// CASE [expr] WHEN ... THEN ... ELSE ... END
struct CaseExpr : public Expr {
    ExprPtr switchExpr;                     // 简单 CASE 的判别式；搜索 CASE 为空
    std::vector<std::pair<ExprPtr, ExprPtr>> whenClauses; // (condition, result)
    ExprPtr elseExpr;
    CaseExpr() { type = ExprType::CaseExpr; }
    std::string toString() const override { return "CASE"; }
};

// ARRAY[expr, ...]
struct ArrayExpr : public Expr {
    std::vector<ExprPtr> elements;
    ArrayExpr() { type = ExprType::ArrayExpr; }
    std::string toString() const override { return "ARRAY[...]"; }
};

// ROW(expr, ...)
struct RowExpr : public Expr {
    std::vector<ExprPtr> elements;
    RowExpr() { type = ExprType::RowExpr; }
    std::string toString() const override { return "ROW(...)"; }
};

// ============================================================================
// 语句节点基类
// ============================================================================

class Stmt : public ASTNode {
public:
    SqlCommand command;
    Stmt(SqlCommand cmd = SqlCommand::Unknown) : command(cmd) {}
    virtual ~Stmt() = default;
    std::string toString() const override {
        switch (command) {
            case SqlCommand::Select: return "SELECT";
            case SqlCommand::Insert: return "INSERT";
            case SqlCommand::Update: return "UPDATE";
            case SqlCommand::Delete: return "DELETE";
            case SqlCommand::Merge: return "MERGE";
            case SqlCommand::Copy: return "COPY";
            case SqlCommand::Call: return "CALL";
            case SqlCommand::Do: return "DO";
            case SqlCommand::Begin: return "BEGIN";
            case SqlCommand::StartTransaction: return "START TRANSACTION";
            case SqlCommand::Commit: return "COMMIT";
            case SqlCommand::Rollback: return "ROLLBACK";
            case SqlCommand::Abort: return "ABORT";
            case SqlCommand::End: return "END";
            case SqlCommand::Savepoint: return "SAVEPOINT";
            case SqlCommand::ReleaseSavepoint: return "RELEASE SAVEPOINT";
            case SqlCommand::RollbackToSavepoint: return "ROLLBACK TO SAVEPOINT";
            case SqlCommand::PrepareTransaction: return "PREPARE TRANSACTION";
            case SqlCommand::CommitPrepared: return "COMMIT PREPARED";
            case SqlCommand::RollbackPrepared: return "ROLLBACK PREPARED";
            case SqlCommand::Grant: return "GRANT";
            case SqlCommand::Revoke: return "REVOKE";
            case SqlCommand::Set: return "SET";
            case SqlCommand::Show: return "SHOW";
            case SqlCommand::Reset: return "RESET";
            case SqlCommand::SetRole: return "SET ROLE";
            case SqlCommand::SetSessionAuthorization: return "SET SESSION AUTHORIZATION";
            case SqlCommand::SetConstraints: return "SET CONSTRAINTS";
            case SqlCommand::SetTransaction: return "SET TRANSACTION";
            case SqlCommand::Discard: return "DISCARD";
            case SqlCommand::Explain: return "EXPLAIN";
            case SqlCommand::Analyze: return "ANALYZE";
            case SqlCommand::Vacuum: return "VACUUM";
            case SqlCommand::Checkpoint: return "CHECKPOINT";
            case SqlCommand::Reindex: return "REINDEX";
            case SqlCommand::Cluster: return "CLUSTER";
            case SqlCommand::Comment: return "COMMENT ON";
            case SqlCommand::SecurityLabel: return "SECURITY LABEL";
            case SqlCommand::Lock: return "LOCK";
            case SqlCommand::Listen: return "LISTEN";
            case SqlCommand::Notify: return "NOTIFY";
            case SqlCommand::Unlisten: return "UNLISTEN";
            case SqlCommand::Declare: return "DECLARE";
            case SqlCommand::Fetch: return "FETCH";
            case SqlCommand::Move: return "MOVE";
            case SqlCommand::Close: return "CLOSE";
            case SqlCommand::Prepare: return "PREPARE";
            case SqlCommand::Execute: return "EXECUTE";
            case SqlCommand::Deallocate: return "DEALLOCATE";
            case SqlCommand::ImportForeignSchema: return "IMPORT FOREIGN SCHEMA";
            case SqlCommand::UseDatabase: return "USE DATABASE";
            case SqlCommand::ReplaceInto: return "REPLACE INTO";
            case SqlCommand::LoadDataInfile: return "LOAD DATA INFILE";
            case SqlCommand::Desc: return "DESC";
            case SqlCommand::ViewTable: return "VIEW TABLE";
            case SqlCommand::ViewDatabase: return "VIEW DATABASE";
            case SqlCommand::UpdateLimit: return "UPDATE ... LIMIT";
            case SqlCommand::DeleteLimit: return "DELETE ... LIMIT";
            case SqlCommand::Truncate: return "TRUNCATE";
            case SqlCommand::CreateTable: return "CREATE TABLE";
            case SqlCommand::CreateIndex: return "CREATE INDEX";
            case SqlCommand::CreateView: return "CREATE VIEW";
            case SqlCommand::CreateMaterializedView: return "CREATE MATERIALIZED VIEW";
            case SqlCommand::CreateDatabase: return "CREATE DATABASE";
            case SqlCommand::CreateSchema: return "CREATE SCHEMA";
            case SqlCommand::CreateSequence: return "CREATE SEQUENCE";
            case SqlCommand::CreateDomain: return "CREATE DOMAIN";
            case SqlCommand::CreateType: return "CREATE TYPE";
            case SqlCommand::CreateFunction: return "CREATE FUNCTION";
            case SqlCommand::CreateProcedure: return "CREATE PROCEDURE";
            case SqlCommand::CreateTrigger: return "CREATE TRIGGER";
            case SqlCommand::CreateRole: return "CREATE ROLE";
            case SqlCommand::CreateUser: return "CREATE USER";
            case SqlCommand::CreateTablespace: return "CREATE TABLESPACE";
            case SqlCommand::CreateStatistics: return "CREATE STATISTICS";
            case SqlCommand::CreatePolicy: return "CREATE POLICY";
            case SqlCommand::CreateRule: return "CREATE RULE";
            case SqlCommand::CreateEventTrigger: return "CREATE EVENT TRIGGER";
            case SqlCommand::CreateExtension: return "CREATE EXTENSION";
            case SqlCommand::CreatePublication: return "CREATE PUBLICATION";
            case SqlCommand::CreateSubscription: return "CREATE SUBSCRIPTION";
            case SqlCommand::CreateAccessMethod: return "CREATE ACCESS METHOD";
            case SqlCommand::CreateForeignDataWrapper: return "CREATE FOREIGN DATA WRAPPER";
            case SqlCommand::CreateForeignTable: return "CREATE FOREIGN TABLE";
            case SqlCommand::CreateServer: return "CREATE SERVER";
            case SqlCommand::CreateUserMapping: return "CREATE USER MAPPING";
            case SqlCommand::CreateCast: return "CREATE CAST";
            case SqlCommand::CreateCollation: return "CREATE COLLATION";
            case SqlCommand::CreateConversion: return "CREATE CONVERSION";
            case SqlCommand::CreateOperator: return "CREATE OPERATOR";
            case SqlCommand::CreateOperatorClass: return "CREATE OPERATOR CLASS";
            case SqlCommand::CreateOperatorFamily: return "CREATE OPERATOR FAMILY";
            case SqlCommand::CreateAggregate: return "CREATE AGGREGATE";
            case SqlCommand::CreateTransform: return "CREATE TRANSFORM";
            case SqlCommand::CreateLanguage: return "CREATE LANGUAGE";
            case SqlCommand::CreateTextSearchConfiguration: return "CREATE TEXT SEARCH CONFIGURATION";
            case SqlCommand::CreateTextSearchDictionary: return "CREATE TEXT SEARCH DICTIONARY";
            case SqlCommand::CreateTextSearchParser: return "CREATE TEXT SEARCH PARSER";
            case SqlCommand::CreateTextSearchTemplate: return "CREATE TEXT SEARCH TEMPLATE";
            case SqlCommand::DropTable: return "DROP TABLE";
            case SqlCommand::DropIndex: return "DROP INDEX";
            case SqlCommand::DropView: return "DROP VIEW";
            case SqlCommand::DropMaterializedView: return "DROP MATERIALIZED VIEW";
            case SqlCommand::DropDatabase: return "DROP DATABASE";
            case SqlCommand::DropSchema: return "DROP SCHEMA";
            case SqlCommand::DropSequence: return "DROP SEQUENCE";
            case SqlCommand::DropDomain: return "DROP DOMAIN";
            case SqlCommand::DropType: return "DROP TYPE";
            case SqlCommand::DropFunction: return "DROP FUNCTION";
            case SqlCommand::DropProcedure: return "DROP PROCEDURE";
            case SqlCommand::DropRoutine: return "DROP ROUTINE";
            case SqlCommand::DropTrigger: return "DROP TRIGGER";
            case SqlCommand::DropRole: return "DROP ROLE";
            case SqlCommand::DropUser: return "DROP USER";
            case SqlCommand::DropTablespace: return "DROP TABLESPACE";
            case SqlCommand::DropStatistics: return "DROP STATISTICS";
            case SqlCommand::DropPolicy: return "DROP POLICY";
            case SqlCommand::DropRule: return "DROP RULE";
            case SqlCommand::DropEventTrigger: return "DROP EVENT TRIGGER";
            case SqlCommand::DropExtension: return "DROP EXTENSION";
            case SqlCommand::DropPublication: return "DROP PUBLICATION";
            case SqlCommand::DropSubscription: return "DROP SUBSCRIPTION";
            case SqlCommand::DropAccessMethod: return "DROP ACCESS METHOD";
            case SqlCommand::DropForeignDataWrapper: return "DROP FOREIGN DATA WRAPPER";
            case SqlCommand::DropForeignTable: return "DROP FOREIGN TABLE";
            case SqlCommand::DropServer: return "DROP SERVER";
            case SqlCommand::DropUserMapping: return "DROP USER MAPPING";
            case SqlCommand::DropCast: return "DROP CAST";
            case SqlCommand::DropCollation: return "DROP COLLATION";
            case SqlCommand::DropConversion: return "DROP CONVERSION";
            case SqlCommand::DropOperator: return "DROP OPERATOR";
            case SqlCommand::DropOperatorClass: return "DROP OPERATOR CLASS";
            case SqlCommand::DropOperatorFamily: return "DROP OPERATOR FAMILY";
            case SqlCommand::DropAggregate: return "DROP AGGREGATE";
            case SqlCommand::DropTransform: return "DROP TRANSFORM";
            case SqlCommand::DropLanguage: return "DROP LANGUAGE";
            case SqlCommand::DropTextSearchConfiguration: return "DROP TEXT SEARCH CONFIGURATION";
            case SqlCommand::DropTextSearchDictionary: return "DROP TEXT SEARCH DICTIONARY";
            case SqlCommand::DropTextSearchParser: return "DROP TEXT SEARCH PARSER";
            case SqlCommand::DropTextSearchTemplate: return "DROP TEXT SEARCH TEMPLATE";
            case SqlCommand::DropOwned: return "DROP OWNED";
            case SqlCommand::DropLargeObject: return "DROP LARGE OBJECT";
            case SqlCommand::AlterTable: return "ALTER TABLE";
            case SqlCommand::AlterIndex: return "ALTER INDEX";
            case SqlCommand::AlterView: return "ALTER VIEW";
            case SqlCommand::AlterMaterializedView: return "ALTER MATERIALIZED VIEW";
            case SqlCommand::AlterDatabase: return "ALTER DATABASE";
            case SqlCommand::AlterSchema: return "ALTER SCHEMA";
            case SqlCommand::AlterSequence: return "ALTER SEQUENCE";
            case SqlCommand::AlterDomain: return "ALTER DOMAIN";
            case SqlCommand::AlterType: return "ALTER TYPE";
            case SqlCommand::AlterFunction: return "ALTER FUNCTION";
            case SqlCommand::AlterProcedure: return "ALTER PROCEDURE";
            case SqlCommand::AlterRoutine: return "ALTER ROUTINE";
            case SqlCommand::AlterTrigger: return "ALTER TRIGGER";
            case SqlCommand::AlterRole: return "ALTER ROLE";
            case SqlCommand::AlterUser: return "ALTER USER";
            case SqlCommand::AlterSystem: return "ALTER SYSTEM";
            case SqlCommand::AlterTablespace: return "ALTER TABLESPACE";
            case SqlCommand::AlterStatistics: return "ALTER STATISTICS";
            case SqlCommand::AlterPolicy: return "ALTER POLICY";
            case SqlCommand::AlterRule: return "ALTER RULE";
            case SqlCommand::AlterEventTrigger: return "ALTER EVENT TRIGGER";
            case SqlCommand::AlterExtension: return "ALTER EXTENSION";
            case SqlCommand::AlterPublication: return "ALTER PUBLICATION";
            case SqlCommand::AlterSubscription: return "ALTER SUBSCRIPTION";
            case SqlCommand::AlterDefaultPrivileges: return "ALTER DEFAULT PRIVILEGES";
            case SqlCommand::AlterForeignDataWrapper: return "ALTER FOREIGN DATA WRAPPER";
            case SqlCommand::AlterForeignTable: return "ALTER FOREIGN TABLE";
            case SqlCommand::AlterServer: return "ALTER SERVER";
            case SqlCommand::AlterUserMapping: return "ALTER USER MAPPING";
            case SqlCommand::AlterTextSearchConfiguration: return "ALTER TEXT SEARCH CONFIGURATION";
            case SqlCommand::AlterTextSearchDictionary: return "ALTER TEXT SEARCH DICTIONARY";
            case SqlCommand::AlterTextSearchParser: return "ALTER TEXT SEARCH PARSER";
            case SqlCommand::AlterTextSearchTemplate: return "ALTER TEXT SEARCH TEMPLATE";
            case SqlCommand::AlterCollation: return "ALTER COLLATION";
            case SqlCommand::AlterConversion: return "ALTER CONVERSION";
            case SqlCommand::AlterOperator: return "ALTER OPERATOR";
            case SqlCommand::AlterOperatorClass: return "ALTER OPERATOR CLASS";
            case SqlCommand::AlterOperatorFamily: return "ALTER OPERATOR FAMILY";
            case SqlCommand::AlterAggregate: return "ALTER AGGREGATE";
            case SqlCommand::AlterLanguage: return "ALTER LANGUAGE";
            case SqlCommand::AlterLargeObject: return "ALTER LARGE OBJECT";
            default: return "UNKNOWN";
        }
    }
};

using StmtPtr = std::unique_ptr<Stmt>;

// ============================================================================
// SELECT 语句
// ============================================================================

struct SelectItem {
    ExprPtr expr;
    std::string alias;         // AS alias
};

struct FromItem {
    enum class Type { Table, Subquery, Join, Function };
    Type type = Type::Table;
    std::string tableName;
    std::string alias;
    StmtPtr subquery;          // 子查询 (type == Subquery)
    std::string joinType;      // INNER, LEFT, RIGHT, FULL, CROSS
    std::unique_ptr<FromItem> left;
    std::unique_ptr<FromItem> right;
    ExprPtr joinCondition;     // ON expr
    std::vector<std::string> usingCols;
};

enum class SetOp { None, Union, Intersect, Except };

struct SelectStmt : public Stmt {
    std::vector<SelectItem> selectList;
    std::unique_ptr<FromItem> fromClause;
    ExprPtr whereClause;
    std::vector<ExprPtr> groupBy;

    // GROUP BY with ROLLUP/CUBE/GROUPING SETS support
    struct GroupByElem {
        enum class Kind { Plain, Rollup, Cube, GroupingSets };
        Kind kind = Kind::Plain;
        std::vector<ExprPtr> exprs;
    };
    std::vector<GroupByElem> groupByElems;

    ExprPtr having;

    struct OrderByElem {
        ExprPtr expr;
        bool asc = true;
        bool nullsFirst = false;   // NULLS FIRST / LAST (per column)
        std::string usingOp;       // ORDER BY col USING operator
    };
    std::vector<OrderByElem> orderBy;
    bool nullsFirst = false;       // legacy single flag
    std::optional<size_t> limit;
    std::optional<size_t> offset;
    bool withTies = false;         // WITH TIES
    bool fetchFirst = false;       // FETCH FIRST / NEXT
    SetOp setOp = SetOp::None;
    bool setOpAll = false;         // UNION ALL vs UNION
    StmtPtr setOpRhs;              // 右侧 SELECT

    // VALUES rows (proper row structure, independent of selectList)
    std::vector<std::vector<ExprPtr>> valuesRows;

    // Locking
    struct LockClause {
        std::string strength;  // UPDATE, SHARE, NO KEY UPDATE, KEY SHARE
        std::vector<std::string> tables;
        bool noWait = false;
        bool skipLocked = false;
    };
    std::vector<LockClause> locking;

    // CTE
    struct CTE {
        std::string name;
        std::vector<std::string> columnNames;
        StmtPtr query;
        bool recursive = false;
        bool materialized = true;  // MATERIALIZED / NOT MATERIALIZED
    };
    std::vector<CTE> ctes;

    // DISTINCT / ALL
    bool distinct = false;
    std::vector<ExprPtr> distinctOn; // DISTINCT ON (expr, ...)

    // Window functions
    std::vector<WindowDef> windowDefs;

    SelectStmt() : Stmt(SqlCommand::Select) {}
    std::string toString() const override { return "SELECT"; }
};

// ============================================================================
// INSERT 语句
// ============================================================================

struct InsertStmt : public Stmt {
    std::string tableName;
    std::vector<std::string> columns;
    std::vector<std::vector<ExprPtr>> values;  // VALUES (...), (...)
    StmtPtr selectSource;                      // INSERT INTO ... SELECT ...
    std::string conflictAction;                // ON CONFLICT DO NOTHING / UPDATE
    std::vector<std::string> conflictTarget;   // ON CONFLICT (col) ...
    std::vector<std::pair<std::string, ExprPtr>> conflictUpdateSet;
    ExprPtr conflictWhere;
    std::vector<SelectItem> returning;
    bool defaultValues = false;                // DEFAULT VALUES
    std::string override_;                     // OVERRIDING SYSTEM VALUE / USER VALUE

    InsertStmt() : Stmt(SqlCommand::Insert) {}
    std::string toString() const override { return "INSERT"; }
};

// ============================================================================
// UPDATE 语句
// ============================================================================

struct UpdateStmt : public Stmt {
    std::string tableName;
    std::map<std::string, ExprPtr> setClauses;
    ExprPtr whereClause;
    std::unique_ptr<FromItem> fromClause;      // UPDATE ... FROM ...
    std::vector<SelectItem> returning;

    UpdateStmt() : Stmt(SqlCommand::Update) {}
    std::string toString() const override { return "UPDATE"; }
};

// ============================================================================
// DELETE 语句
// ============================================================================

struct DeleteStmt : public Stmt {
    std::string tableName;
    ExprPtr whereClause;
    std::unique_ptr<FromItem> usingClause;     // DELETE ... USING ...
    std::vector<SelectItem> returning;
    bool only = false;                         // DELETE FROM ONLY table

    DeleteStmt() : Stmt(SqlCommand::Delete) {}
    std::string toString() const override { return "DELETE"; }
};

// ============================================================================
// MERGE 语句
// ============================================================================

struct MergeStmt : public Stmt {
    std::string targetTable;
    std::unique_ptr<FromItem> source;
    ExprPtr joinCondition;

    struct WhenClause {
        bool matched = true;                   // MATCHED / NOT MATCHED
        std::string bySource;                  // BY SOURCE / BY TARGET
        ExprPtr condition;                     // AND condition
        std::string action;                    // UPDATE / INSERT / DELETE / DO NOTHING
        std::map<std::string, ExprPtr> updateSet;
        std::vector<std::pair<std::string, ExprPtr>> insertCols;
    };
    std::vector<WhenClause> whenClauses;
    std::vector<SelectItem> returning;

    MergeStmt() : Stmt(SqlCommand::Merge) {}
    std::string toString() const override { return "MERGE"; }
};

// ============================================================================
// CREATE TABLE 语句
// ============================================================================

struct ColumnDef {
    std::string name;
    std::string typeName;
    std::vector<std::string> typeMods;         // VARCHAR(255) -> ["255"]
    ExprPtr defaultValue;
    bool isNull = true;                        // true = 可空（默认）
    bool isPrimaryKey = false;
    bool isUnique = false;
    std::vector<ExprPtr> checkExprs;
    std::vector<std::string> checkNames;
    std::string collation;
    std::string generatedExpr;                 // GENERATED ALWAYS AS (expr) 的表达式体
    bool isGeneratedIdentity = false;          // GENERATED ... AS IDENTITY
    std::vector<std::string> constraints;      // GENERATED, IDENTITY 等
    bool isArray = false;
};

struct TableConstraint {
    std::string name;
    std::string type;                          // PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK, EXCLUDE
    std::vector<std::string> columns;
    std::string refTable;
    std::vector<std::string> refColumns;
    std::string onDelete;
    std::string onUpdate;
    ExprPtr checkExpr;
    bool deferrable = false;
    bool initiallyDeferred = false;
};

struct CreateTableStmt : public Stmt {
    std::string tableName;
    std::vector<ColumnDef> columns;
    std::vector<TableConstraint> constraints;
    std::vector<std::string> inherits;         // INHERITS (parent1, parent2)
    std::map<std::string, std::string> options; // WITH (...) reloptions
    bool ifNotExists = false;
    bool unlogged = false;
    bool temp = false;
    bool localTemp = false;
    std::string tablespace;
    std::string accessMethod;
    std::string ofType;                        // OF type_name
    std::vector<std::pair<std::string, ColumnDef>> likeTables; // LIKE table [INCLUDING ...]
    std::vector<SelectItem> partitionBy;       // PARTITION BY ...

    CreateTableStmt() : Stmt(SqlCommand::CreateTable) {}
    std::string toString() const override { return "CREATE TABLE"; }
};

// ============================================================================
// CREATE INDEX 语句
// ============================================================================

struct IndexElem {
    std::string column;        // 列名（与 expr 互斥）
    ExprPtr expr;              // 表达式索引
    std::string collation;
    std::string opclass;
    bool ascending = true;
    bool nullsFirst = false;
};

struct CreateIndexStmt : public Stmt {
    std::string indexName;
    std::string tableName;
    std::vector<IndexElem> columns;
    std::string accessMethod;  // btree, hash, gin, gist, brin, spgist
    bool unique = false;
    bool ifNotExists = false;
    bool concurrently = false;
    ExprPtr whereClause;       // partial index
    std::vector<std::string> includeCols;
    std::map<std::string, std::string> options;
    std::string tablespace;

    CreateIndexStmt() : Stmt(SqlCommand::CreateIndex) {}
    std::string toString() const override { return "CREATE INDEX"; }
};

// ============================================================================
// Generic CREATE object statement for lightweight DDL stubs
// ============================================================================

struct CreateObjectStmt : public Stmt {
    std::string objectType;    // DATABASE, SCHEMA, SEQUENCE, DOMAIN, TYPE, ...
    std::string objectName;    // may be schema-qualified
    std::string schema;        // extracted schema qualifier
    bool ifNotExists = false;
    bool replace = false;
    std::map<std::string, std::string> options;

    CreateObjectStmt(SqlCommand cmd) : Stmt(cmd) {}
    std::string toString() const override { return "CREATE " + objectType; }
};

// ============================================================================
// DROP 语句
// ============================================================================

struct DropStmt : public Stmt {
    std::vector<std::string> objectNames;
    std::string objectType;    // TABLE, INDEX, VIEW, SCHEMA, DATABASE, ...
    bool ifExists = false;
    bool cascade = false;
    bool concurrently = false; // DROP INDEX CONCURRENTLY

    DropStmt(SqlCommand cmd = SqlCommand::DropTable) : Stmt(cmd) {}
    std::string toString() const override { return "DROP"; }
};

// ============================================================================
// Generic ALTER object statement for lightweight DDL stubs
// ============================================================================

struct AlterObjectStmt : public Stmt {
    std::string objectType;    // TABLE, INDEX, SCHEMA, SEQUENCE, ...
    std::string objectName;    // may be schema-qualified
    std::string schema;        // extracted schema qualifier
    bool ifExists = false;
    bool only = false;
    std::string subCommand;    // raw remainder of statement for later analysis

    AlterObjectStmt(SqlCommand cmd) : Stmt(cmd) {}
    std::string toString() const override { return "ALTER " + objectType; }
};

// ============================================================================
// ALTER TABLE 语句
// ============================================================================

struct AlterTableStmt : public Stmt {
    std::string tableName;
    bool ifExists = false;
    bool only = false;

    enum class Action {
        AddColumn, DropColumn, AlterColumn, RenameColumn,
        RenameTable, SetSchema, SetTablespace,
        AddConstraint, DropConstraint, AlterConstraint,
        AttachPartition, DetachPartition,
        SetOptions, ResetOptions,
        SetLogged, SetUnlogged, ForceRowLevelSecurity,
        EnableTrigger, DisableTrigger, EnableRule, DisableRule,
        ClusterOn, SetWithoutCluster,
        SetAccessMethod,
        SetReplicaIdentity,
    };

    struct SubCmd {
        Action action;
        std::string name;              // column/constraint/index name
        ColumnDef colDef;              // for AddColumn / AlterColumn
        TableConstraint constraint;    // for AddConstraint
        std::string newName;
        std::string dataType;
        ExprPtr defaultValue;
        bool dropDefault = false;
        bool setNotNull = false;
        bool dropNotNull = false;
        std::string partitionSpec;
        std::map<std::string, std::string> options;
        std::string replicaIdentity;
    };
    std::vector<SubCmd> subCommands;

    AlterTableStmt() : Stmt(SqlCommand::AlterTable) {}
    std::string toString() const override { return "ALTER TABLE"; }
};

// ============================================================================
// SET / SHOW / RESET 语句
// ============================================================================

struct SetStmt : public Stmt {
    std::string name;          // 参数名（如 "timezone", "search_path"）
    std::vector<std::string> values;
    enum class Scope { Local, Session, Global } scope = Scope::Session;
    bool isReset = false;      // RESET vs SET
    bool isShow = false;       // SHOW vs SET

    SetStmt() : Stmt(SqlCommand::Set) {}
    std::string toString() const override { return isShow ? "SHOW" : (isReset ? "RESET" : "SET"); }
};

// ============================================================================
// BEGIN / COMMIT / ROLLBACK 语句
// ============================================================================

struct TransactionStmt : public Stmt {
    enum class Kind { Begin, Start, Commit, Rollback, Abort, End,
                      Savepoint, Release, RollbackTo,
                      Prepare, CommitPrepared, RollbackPrepared };
    Kind kind;
    std::string savepointName;
    std::string gid;           // two-phase commit XID
    IsolationLevel isolation = IsolationLevel::READ_COMMITTED;
    bool readOnly = false;
    bool deferrable = false;

    TransactionStmt(Kind k) : Stmt(SqlCommand::Begin), kind(k) {
        switch (k) {
            case Kind::Begin: command = SqlCommand::Begin; break;
            case Kind::Start: command = SqlCommand::StartTransaction; break;
            case Kind::Commit: command = SqlCommand::Commit; break;
            case Kind::Rollback: command = SqlCommand::Rollback; break;
            case Kind::Abort: command = SqlCommand::Abort; break;
            case Kind::End: command = SqlCommand::End; break;
            case Kind::Savepoint: command = SqlCommand::Savepoint; break;
            case Kind::Release: command = SqlCommand::ReleaseSavepoint; break;
            case Kind::RollbackTo: command = SqlCommand::RollbackToSavepoint; break;
            case Kind::Prepare: command = SqlCommand::PrepareTransaction; break;
            case Kind::CommitPrepared: command = SqlCommand::CommitPrepared; break;
            case Kind::RollbackPrepared: command = SqlCommand::RollbackPrepared; break;
        }
    }
    std::string toString() const override;
};

// ============================================================================
// EXPLAIN 语句
// ============================================================================

struct ExplainStmt : public Stmt {
    StmtPtr query;             // 被 EXPLAIN 的语句
    bool analyze = false;
    bool verbose = false;
    bool costs = true;
    bool buffers = false;
    bool timing = true;
    bool settings = false;
    bool json = false;
    bool xml = false;
    bool yaml = false;
    bool genericPlan = false;  // GENERIC_PLAN

    ExplainStmt() : Stmt(SqlCommand::Explain) {}
    std::string toString() const override { return "EXPLAIN"; }
};

// ============================================================================
// COPY 语句
// ============================================================================

struct CopyStmt : public Stmt {
    std::string tableName;
    std::vector<std::string> columns;
    bool isFrom = true;        // COPY FROM vs COPY TO
    std::string filename;      // 文件名或 "STDIN"/"STDOUT"
    bool isStdio = false;      // STDIN / STDOUT
    bool isProgram = false;    // PROGRAM '...'
    bool binary = false;
    bool csv = false;
    bool header = false;
    bool freeze = false;
    char delimiter = ',';
    char quote = '"';
    char escape = '"';
    std::string nullStr = "\\N";
    std::string encoding;
    ExprPtr whereClause;       // COPY ... WHERE condition

    CopyStmt() : Stmt(SqlCommand::Copy) {}
    std::string toString() const override { return "COPY"; }
};

// ============================================================================
// CREATE VIEW / MATERIALIZED VIEW 语句
// ============================================================================

struct CreateViewStmt : public Stmt {
    std::string viewName;
    std::vector<std::string> columnNames; // 显式列名（可选）
    StmtPtr query;                        // SELECT 语句
    bool replace = false;                 // CREATE OR REPLACE
    bool temp = false;
    bool recursive = false;               // 递归视图
    bool materialized = false;            // MATERIALIZED VIEW
    bool withData = true;                 // WITH [NO] DATA
    bool securityBarrier = false;
    bool securityInvoker = false;
    std::string checkOption;              // CASCADED / LOCAL

    CreateViewStmt(bool mat = false)
        : Stmt(mat ? SqlCommand::CreateMaterializedView : SqlCommand::CreateView),
          materialized(mat) {}
    std::string toString() const override {
        return materialized ? "CREATE MATERIALIZED VIEW" : "CREATE VIEW";
    }
};

// ============================================================================
// GRANT / REVOKE 语句
// ============================================================================

struct GrantStmt : public Stmt {
    bool isGrant = true;       // true = GRANT, false = REVOKE
    std::vector<std::string> privileges;
    std::vector<std::string> columns;     // 列级权限
    std::string objectType;               // TABLE, SEQUENCE, FUNCTION, DATABASE, SCHEMA, ...
    std::vector<std::string> objectNames;
    std::vector<std::string> grantees;
    bool withGrantOption = false;
    bool grantOptionFor = false;          // REVOKE GRANT OPTION FOR
    bool cascade = false;                 // REVOKE ... CASCADE
    bool allPrivileges = false;
    std::string grantedBy;

    GrantStmt() : Stmt(SqlCommand::Grant) {}
    std::string toString() const override { return isGrant ? "GRANT" : "REVOKE"; }
};

// ============================================================================
// COMMENT ON 语句
// ============================================================================

struct CommentStmt : public Stmt {
    std::string objectType;
    std::string objectName;
    std::string comment;       // 空字符串 = 删除注释
    std::string columnName;    // COMMENT ON COLUMN table.column

    CommentStmt() : Stmt(SqlCommand::Comment) {}
    std::string toString() const override { return "COMMENT ON"; }
};

// ============================================================================
// CREATE FUNCTION / PROCEDURE 语句
// ============================================================================

struct CreateFunctionStmt : public Stmt {
    std::string funcName;
    std::vector<std::pair<std::string, std::string>> params; // (name, type)
    std::string returnType;
    std::string language;      // sql, plpgsql, c, internal
    std::string body;
    bool isProcedure = false;
    bool replace = false;
    bool immutable = false;
    bool stable = false;
    bool volatile_ = true;
    bool strict = false;
    bool securityDefiner = false;
    bool leakproof = false;
    bool parallelSafe = false;
    bool parallelRestricted = false;
    bool parallelUnsafe = false;
    double cost = 100.0;
    double rows = 1000.0;
    std::vector<std::string> setItems; // SET configuration_parameter = value

    CreateFunctionStmt(bool proc = false)
        : Stmt(proc ? SqlCommand::CreateProcedure : SqlCommand::CreateFunction),
          isProcedure(proc) {}
    std::string toString() const override {
        return isProcedure ? "CREATE PROCEDURE" : "CREATE FUNCTION";
    }
};

// ============================================================================
// CREATE TRIGGER 语句
// ============================================================================

struct CreateTriggerStmt : public Stmt {
    std::string triggerName;
    std::string tableName;
    std::string timing;        // BEFORE, AFTER, INSTEAD OF
    std::vector<std::string> events; // INSERT, UPDATE [OF cols], DELETE, TRUNCATE
    bool forEachRow = true;
    bool constraint = false;
    bool deferrable = false;
    bool initiallyDeferred = false;
    ExprPtr whenCondition;
    std::string functionName;
    std::vector<ExprPtr> functionArgs;
    std::vector<std::string> transitionTableNames; // OLD TABLE, NEW TABLE

    CreateTriggerStmt() : Stmt(SqlCommand::CreateTrigger) {}
    std::string toString() const override { return "CREATE TRIGGER"; }
};

// ============================================================================
// CREATE ROLE / USER 语句
// ============================================================================

struct CreateRoleStmt : public Stmt {
    std::string roleName;
    bool isUser = false;       // CREATE USER vs CREATE ROLE
    bool isGroup = false;      // CREATE GROUP (legacy)
    bool superuser = false;
    bool createdb = false;
    bool createrole = false;
    bool inherit = true;
    bool login = false;
    bool replication = false;
    bool bypassrls = false;
    int connectionLimit = -1;  // -1 = 无限制
    std::string password;
    bool encryptedPassword = true;
    std::string validUntil;
    std::vector<std::pair<std::string, bool>> inRole;     // (roleName, adminOption)
    std::vector<std::pair<std::string, bool>> roleMembers; // (roleName, adminOption)
    std::vector<std::string> setItems;

    CreateRoleStmt() : Stmt(SqlCommand::CreateRole) {}
    std::string toString() const override {
        return isUser ? "CREATE USER" : (isGroup ? "CREATE GROUP" : "CREATE ROLE");
    }
};

// ============================================================================
// 解析结果包装
// ============================================================================

struct ParseResult {
    bool success = false;
    std::string error;
    StmtPtr stmt;              // 解析后的 AST
    std::string originalSql;   // 原始 SQL（保留大小写和格式）

    bool isValid() const { return success && stmt != nullptr; }
    SqlCommand command() const { return stmt ? stmt->command : SqlCommand::Unknown; }
};

} // namespace dbms
