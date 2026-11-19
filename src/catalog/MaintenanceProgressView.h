#pragma once

#include "catalog/catalog.h"
#include "common/DbError.h"
#include "parser/ast.h"
#include <algorithm>
#include <string>
#include <utility>
#include <vector>

namespace dbms::maintenance_progress_view {

enum class Kind { Vacuum, Analyze, Cluster };
using Fields = std::vector<std::pair<std::string, std::string>>;

inline const Fields& fields(Kind kind) {
    static const Fields vacuum{{"pid","integer"},{"datid","oid"},{"datname","name"},
        {"relid","oid"},{"phase","text"},{"heap_blks_total","bigint"},
        {"heap_blks_scanned","bigint"},{"heap_blks_vacuumed","bigint"},
        {"index_vacuum_count","bigint"},{"max_dead_tuple_bytes","bigint"},
        {"dead_tuple_bytes","bigint"},{"num_dead_item_ids","bigint"},
        {"indexes_total","bigint"},{"indexes_processed","bigint"},{"delay_time","double precision"}};
    static const Fields analyze{{"pid","integer"},{"datid","oid"},{"datname","name"},
        {"relid","oid"},{"phase","text"},{"sample_blks_total","bigint"},
        {"sample_blks_scanned","bigint"},{"ext_stats_total","bigint"},
        {"ext_stats_computed","bigint"},{"child_tables_total","bigint"},
        {"child_tables_done","bigint"},{"current_child_table_relid","oid"},
        {"delay_time","double precision"}};
    static const Fields cluster{{"pid","integer"},{"datid","oid"},{"datname","name"},
        {"relid","oid"},{"command","text"},{"phase","text"},{"cluster_index_relid","oid"},
        {"heap_tuples_scanned","bigint"},{"heap_tuples_written","bigint"},
        {"heap_blks_total","bigint"},{"heap_blks_scanned","bigint"},{"index_rebuild_count","bigint"}};
    return kind == Kind::Vacuum ? vacuum : kind == Kind::Analyze ? analyze : cluster;
}

inline std::string unquote(const std::string& token) {
    if (token.size() < 2 || token.front() != '"' || token.back() != '"') return token;
    std::string value;
    for (size_t i = 1; i + 1 < token.size(); ++i) {
        if (token[i] == '"' && i + 2 < token.size() && token[i + 1] == '"') ++i;
        value += token[i];
    }
    return value;
}

inline bool source(const SelectStmt* select, Kind& kind, std::string& name) {
    // WITH belongs to the existing CTE binder. An unqualified CTE can
    // shadow a progress view and must not be mistaken for its source.
    if (!select || !select->ctes.empty() || !select->fromClause ||
        select->fromClause->type != FromItem::Type::Table)
        return false;
    CatalogManager::QualifiedName relation;
    if (!CatalogManager::parseQualifiedName(select->fromClause->tableName, relation, true) ||
        (!relation.schema.empty() && relation.schema != "pg_catalog")) return false;
    name = relation.name;
    if (name == "pg_stat_progress_vacuum") kind = Kind::Vacuum;
    else if (name == "pg_stat_progress_analyze") kind = Kind::Analyze;
    else if (name == "pg_stat_progress_cluster") kind = Kind::Cluster;
    else return false;
    return true;
}

struct Projection { size_t index; std::string name; };

inline std::vector<Projection> projection(const SelectStmt& select, Kind kind,
                                          const std::string& name) {
    if (!select.ctes.empty() || select.distinct || !select.groupBy.empty() ||
        !select.groupByElems.empty() || select.having || select.setOp != SetOp::None ||
        !select.locking.empty() || !select.distinctOn.empty() || !select.windowDefs.empty() ||
        select.withTies || !select.orderBy.empty() || !select.fromClause->columnAliases.empty())
        throw DbError("0A000", "complex progress view query is not supported");
    const auto& descriptor = fields(kind);
    const auto qualifier = select.fromClause->alias.empty() ? name : unquote(select.fromClause->alias);
    std::vector<Projection> result;
    for (const auto& item : select.selectList) {
        // The parser represents a bare star as LiteralExpr; qualified stars
        // are ColumnRefExpr and follow the qualifier checks below.
        const auto* literal = dynamic_cast<const LiteralExpr*>(item.expr.get());
        if (literal && literal->value == "*" && literal->typeName.empty() && item.alias.empty()) {
            for (size_t i = 0; i < descriptor.size(); ++i) result.push_back({i, descriptor[i].first});
            continue;
        }
        const auto* column = dynamic_cast<const ColumnRefExpr*>(item.expr.get());
        if (!column) throw DbError("0A000", "progress view expression is not supported");
        if ((!column->table.empty() && column->table != qualifier) ||
            (!column->schema.empty() && (column->schema != "pg_catalog" ||
                                         !select.fromClause->alias.empty())))
            throw DbError("42P01", "missing FROM-clause entry");
        if (column->column == "*") {
            for (size_t i = 0; i < descriptor.size(); ++i) result.push_back({i, descriptor[i].first});
            continue;
        }
        const auto found = std::find_if(descriptor.begin(), descriptor.end(),
            [&](const auto& field) { return field.first == column->column; });
        if (found == descriptor.end()) throw DbError("42703", "column does not exist: " + column->column);
        result.push_back({size_t(found - descriptor.begin()),
                          item.alias.empty() ? column->column : unquote(item.alias)});
    }
    if (result.empty()) throw DbError("42601", "progress view has no projection");
    return result;
}

} // namespace dbms::maintenance_progress_view
