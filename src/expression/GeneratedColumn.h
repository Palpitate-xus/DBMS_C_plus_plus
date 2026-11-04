#pragma once
#include "parser/ast.h"
#include "common/DbError.h"
#include "interfaces/table_schema.h"
#include <map>
#include <memory>
#include <string>
#include <vector>
namespace dbms {
class StorageEngine;
class GeneratedColumnReadError : public DbError {public:GeneratedColumnReadError(const DbError& e):DbError(e.sqlState(),e.message()) {}};
struct DatumMetadataContext;
// A real, frozen physical declaration. No PreparedQuery, Session, row/frame,
// provider, callback, or evaluated result is retained by a recipe.
struct GeneratedColumnProgram {
    Column target;
    std::vector<Column> inputs;
    std::vector<DatumTypePtr> inputTypes;
    DatumTypePtr resultType;
    std::shared_ptr<const Expr> expression;
    std::string syntaxState,syntaxError;
    std::string assignmentType,assignmentState,assignmentError;
    std::string database;
    std::string sourceSchema,sourceName;
    std::shared_ptr<const DatumMetadataContext> metadata;
    StorageEngine* engine=nullptr; // same ordinary engine as the actual reader
    static ExprPtr parseDefinition(const std::string& source);
};
struct GeneratedRelationDeclaration {
    std::string schema,relation;
    std::vector<std::shared_ptr<const GeneratedColumnProgram>> columns;
};
using GeneratedRelationPtr=std::shared_ptr<const GeneratedRelationDeclaration>;
// Bound occurrence identity, or the existing helper's genuine private names.
// This is immutable executable metadata, never a parameter/source provider.
struct GeneratedColumnRead {
    std::shared_ptr<const GeneratedColumnProgram> program;
    size_t sourceOrdinal=0;
    std::map<std::string,std::string> names;
    bool named=false;
    // True only for a genuine raw SqlRow/heap-string data receiver. Already
    // typed cells (including valid REGTYPE OID 0) must never be reloaded by
    // inspecting their display or treating zero as an absent identity.
    bool rawStoredInputs=false;
};
using GeneratedReadPtr=std::shared_ptr<const GeneratedColumnRead>;
struct GeneratedInputState {
    struct Input {GeneratedReadPtr read;bool present=false;};
    std::map<std::pair<size_t,size_t>,Input> bound;
    std::map<std::string,Input> named;
};
GeneratedRelationPtr makeGeneratedRelation(const TableSchema&,const std::string&,
    StorageEngine*,const std::vector<DatumTypePtr>& types={},
    std::shared_ptr<const DatumMetadataContext> metadata={},
    const std::string& schema={},const std::string& relation={});
GeneratedReadPtr bindGeneratedColumn(std::shared_ptr<const GeneratedColumnProgram>,
    size_t sourceOrdinal,const std::map<std::string,std::string>& names={},bool rawStoredInputs=false);
} // namespace dbms
