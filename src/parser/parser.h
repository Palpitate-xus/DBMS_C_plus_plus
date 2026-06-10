#pragma once

#include "ast.h"
#include "dbms_defs.h"
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <optional>

namespace dbms {

// ============================================================================
// SQL Parser 接口
// Phase 1：从 execute() 的字符串分发器迁移到结构化解析
//
// 当前策略：
//   1. 先实现命令分类（替代字符串前缀匹配）
//   2. 再逐步完善每个命令的参数解析
// ============================================================================

class SQLParser {
public:
    SQLParser() = default;

    // 主入口：解析 SQL 字符串，返回 AST
    ParseResult parse(const std::string& sql);

    // 快速命令分类（不构建完整 AST）
    // 用于 execute() 迁移期：先替换分类逻辑
    static SqlCommand classify(const std::string& sql);

    // 工具函数
    static std::string toLower(const std::string& s);
    static std::string trim(const std::string& s);
    static std::vector<std::string> tokenize(const std::string& sql);
    static bool isKeyword(const std::string& s);

private:
    // 各命令类型的解析实现
    ParseResult parseSelect(const std::string& sql);
    ParseResult parseInsert(const std::string& sql);
    ParseResult parseUpdate(const std::string& sql);
    ParseResult parseDelete(const std::string& sql);
    ParseResult parseMerge(const std::string& sql);
    ParseResult parseValues(const std::string& sql);

    ParseResult parseCreate(const std::string& sql);
    ParseResult parseDrop(const std::string& sql);
    ParseResult parseAlter(const std::string& sql);
    ParseResult parseTruncate(const std::string& sql);

    ParseResult parseSet(const std::string& sql);
    ParseResult parseShow(const std::string& sql);
    ParseResult parseReset(const std::string& sql);
    ParseResult parseUse(const std::string& sql);
    ParseResult parseDiscard(const std::string& sql);

    ParseResult parseBegin(const std::string& sql);
    ParseResult parseCommit(const std::string& sql);
    ParseResult parseRollback(const std::string& sql);
    ParseResult parseSavepoint(const std::string& sql);
    ParseResult parseRelease(const std::string& sql);

    ParseResult parseExplain(const std::string& sql);
    ParseResult parseAnalyze(const std::string& sql);
    ParseResult parseVacuum(const std::string& sql);
    ParseResult parseCheckpoint(const std::string& sql);
    ParseResult parseReindex(const std::string& sql);
    ParseResult parseCluster(const std::string& sql);

    ParseResult parseCopy(const std::string& sql);
    ParseResult parseComment(const std::string& sql);
    ParseResult parseSecurityLabel(const std::string& sql);
    ParseResult parseLock(const std::string& sql);

    ParseResult parseListen(const std::string& sql);
    ParseResult parseNotify(const std::string& sql);
    ParseResult parseUnlisten(const std::string& sql);

    ParseResult parseDeclare(const std::string& sql);
    ParseResult parseFetch(const std::string& sql);
    ParseResult parseMove(const std::string& sql);
    ParseResult parseClose(const std::string& sql);

    ParseResult parsePrepare(const std::string& sql);
    ParseResult parseExecute(const std::string& sql);
    ParseResult parseDeallocate(const std::string& sql);

    ParseResult parseGrant(const std::string& sql);
    ParseResult parseRevoke(const std::string& sql);

    ParseResult parseCall(const std::string& sql);
    ParseResult parseDo(const std::string& sql);
    ParseResult parseImportForeignSchema(const std::string& sql);

    // CREATE 子命令解析
    StmtPtr parseCreateTable(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateIndex(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateView(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateDatabase(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateSchema(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateSequence(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateDomain(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateType(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateFunction(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateProcedure(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateTrigger(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateRole(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateTablespace(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateStatistics(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreatePolicy(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateExtension(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreatePublication(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateSubscription(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateRule(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateEventTrigger(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateAccessMethod(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateForeignDataWrapper(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateForeignTable(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateServer(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateUserMapping(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateCast(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateCollation(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateConversion(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateOperator(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateOperatorClass(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateOperatorFamily(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateAggregate(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateTransform(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateLanguage(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateTextSearchConfiguration(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateTextSearchDictionary(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateTextSearchParser(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseCreateTextSearchTemplate(const std::vector<std::string>& tokens, size_t& pos);

    // ALTER 子命令解析
    StmtPtr parseAlterTable(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterIndex(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterView(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterMaterializedView(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterDatabase(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterSchema(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterSequence(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterDomain(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterType(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterFunction(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterProcedure(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterRoutine(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterTrigger(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterRole(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterUser(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterSystem(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterTablespace(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterStatistics(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterPolicy(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterRule(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterEventTrigger(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterExtension(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterPublication(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterSubscription(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterDefaultPrivileges(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterForeignDataWrapper(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterForeignTable(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterServer(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterUserMapping(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterTextSearchConfiguration(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterTextSearchDictionary(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterTextSearchParser(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterTextSearchTemplate(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterCollation(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterConversion(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterOperator(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterOperatorClass(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterOperatorFamily(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterAggregate(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterLanguage(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseAlterLargeObject(const std::vector<std::string>& tokens, size_t& pos);

    // DROP 子命令解析
    StmtPtr parseDropTable(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropIndex(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropView(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropMaterializedView(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropDatabase(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropSchema(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropSequence(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropDomain(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropType(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropFunction(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropProcedure(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropRoutine(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropTrigger(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropRole(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropUser(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropTablespace(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropStatistics(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropPolicy(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropRule(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropEventTrigger(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropExtension(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropPublication(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropSubscription(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropAccessMethod(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropForeignDataWrapper(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropForeignTable(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropServer(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropUserMapping(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropCast(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropCollation(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropConversion(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropOperator(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropOperatorClass(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropOperatorFamily(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropAggregate(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropTransform(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropLanguage(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropTextSearchConfiguration(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropTextSearchDictionary(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropTextSearchParser(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropTextSearchTemplate(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropOwned(const std::vector<std::string>& tokens, size_t& pos);
    StmtPtr parseDropLargeObject(const std::vector<std::string>& tokens, size_t& pos);

    // 辅助函数
    static bool match(const std::vector<std::string>& tokens, size_t pos, const std::string& word);
    static bool matchAny(const std::vector<std::string>& tokens, size_t pos,
                         const std::vector<std::string>& words);
};

} // namespace dbms
