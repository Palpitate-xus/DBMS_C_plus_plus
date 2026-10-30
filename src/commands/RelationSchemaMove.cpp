#include "commands/RelationSchemaMove.h"
#include "commands/TableManage.h"
#include "commands/DdlTransaction.h"
#include "commands/SequenceStorageName.h"
#include "catalog/CatalogService.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include "utils/Session.h"
#include <algorithm>
#include <map>
#include <set>

namespace dbms {
namespace {

std::string qualified(const std::string& schema,const std::string& name) {
    return SQLParser::quoteIdentifier(schema)+"."+SQLParser::quoteIdentifier(name);
}

std::string sqlLiteral(const std::string& value) {
    std::string result="'";
    for (char c:value) { result+=c;if(c=='\'')result+='\''; }
    return result+"'";
}

bool validNamespace(const std::string& value) {
    return !value.empty() && value.size()<64 && value!="." && value!=".." &&
        value.find_first_of(std::string("\0/\\",3))==std::string::npos;
}

void moveNamespaceDependency(CatalogManager& catalog,Oid kind,Oid oid,Oid target) {
    for(const auto& dep:catalog.findDepends(kind,oid)) {
        if(dep.objsubid!=0 || dep.refclassid!=PgClassOid_Namespace)continue;
        if(!catalog.removeDepend(kind,oid,0,dep.refclassid,dep.refobjid,dep.refobjsubid))
            throw DbError("XX001","namespace dependency disappeared during move");
    }
    PgDependRow edge;edge.classid=kind;edge.objid=oid;
    edge.refclassid=PgClassOid_Namespace;edge.refobjid=target;edge.deptype='n';
    catalog.addDepend(edge);
}

struct SequenceMove {
    PgClassRow relation;
    std::string oldStorage,newStorage;
    SequenceInfo info;
};

// Replace only parsed builtin sequence-call literal sites. No function body,
// argument expression or sequence is evaluated during this metadata rewrite.
// Byte provenance keeps unrelated strings, comments and SQL spelling intact.
std::string rewriteSequenceCalls(const std::string& input,StorageEngine& engine,
    const std::string& database,const std::string& ownerSchema,
    CatalogManager& catalog,const std::map<Oid,std::string>& moved) {
    if(input.empty() || moved.empty())return input;
    SQLParser parser;
    const std::string sql="SELECT "+input;
    auto parsed=parser.parseForBinding(sql);
    const auto* select=dynamic_cast<const SelectStmt*>(parsed.stmt.get());
    if(!parsed.success || !select || select->selectList.size()!=1)
        throw DbError("XX001","stored default expression cannot be parsed during namespace move");
    ExprEvaluator evaluator;evaluator.setCurrentDB(database);
    struct Edit {size_t begin,end;std::string value;};
    std::vector<Edit> edits;
    std::function<void(const Expr*)> visit=[&](const Expr* node) {
        if(!node)return;
        if(const auto* fn=dynamic_cast<const FunctionCallExpr*>(node)) {
            CatalogManager::QualifiedName function;
            const std::string spelling=fn->schema.empty()?fn->funcName:fn->schema+"."+fn->funcName;
            if(CatalogManager::parseQualifiedName(spelling,function,true) &&
                (function.name=="nextval" || function.name=="currval" || function.name=="setval") &&
                !fn->args.empty() && evaluator.scalarFunctionIdentity(fn,&engine).rfind("builtin",0)==0) {
                const Expr* arg=fn->args.front().get();
                for(;;) {
                    if(const auto* cast=dynamic_cast<const CastExpr*>(arg)){arg=cast->operand.get();continue;}
                    if(const auto* cast=dynamic_cast<const BinaryOpExpr*>(arg);cast && cast->op=="::"){
                        arg=cast->left.get();continue;
                    }
                    break;
                }
                if(const auto* value=dynamic_cast<const LiteralExpr*>(arg)) {
                    // A real numeric/OID constant is already name-independent.
                    const auto decoded=evaluator.eval(value,RowContext{});
                    if(!decoded.isNull && (decoded.typeName=="text" || decoded.typeName=="character varying" ||
                        decoded.typeName=="character" || decoded.typeName=="unknown" || decoded.typeName.empty())) {
                        const auto relation=catalog.resolveRelationSnapshot(decoded.value,{ownerSchema,"public"});
                        if(relation && relation->relkind=='S') {
                            const auto replacement=moved.find(relation->oid);
                            if(replacement!=moved.end()) {
                                if(value->sourceBegin<7 || value->sourceEnd<value->sourceBegin || value->sourceEnd>sql.size())
                                    throw DbError("XX001","stored sequence reference has no source identity");
                                edits.push_back({value->sourceBegin-7,value->sourceEnd-7,sqlLiteral(replacement->second)});
                            }
                        }
                    }
                }
            }
            for(const auto& arg:fn->args)visit(arg.get());
            for(const auto& arg:fn->namedArgs)visit(arg.value.get());
            for(const auto& order:fn->aggregateOrderKeys)visit(order.expr.get());
            visit(fn->filter.get());return;
        }
        if(const auto* n=dynamic_cast<const UnaryOpExpr*>(node))visit(n->operand.get());
        else if(const auto* n=dynamic_cast<const BinaryOpExpr*>(node)) {visit(n->left.get());visit(n->right.get());}
        else if(const auto* n=dynamic_cast<const CastExpr*>(node))visit(n->operand.get());
        else if(const auto* n=dynamic_cast<const CaseExpr*>(node)) {
            visit(n->switchExpr.get());for(const auto& arm:n->whenClauses){visit(arm.first.get());visit(arm.second.get());}visit(n->elseExpr.get());
        } else if(const auto* n=dynamic_cast<const ArrayExpr*>(node))for(const auto& e:n->elements)visit(e.get());
        else if(const auto* n=dynamic_cast<const RowExpr*>(node))for(const auto& e:n->elements)visit(e.get());
        else if(const auto* n=dynamic_cast<const QuantifiedComparisonExpr*>(node)){visit(n->left.get());visit(n->right.get());}
    };
    visit(select->selectList.front().expr.get());
    std::sort(edits.begin(),edits.end(),[](const Edit& a,const Edit& b){return a.begin<b.begin;});
    std::string output;size_t cursor=0;
    for(const auto& edit:edits) {
        if(edit.begin<cursor || edit.end>input.size())throw DbError("XX001","overlapping stored sequence references");
        output.append(input,cursor,edit.begin-cursor);output+=edit.value;cursor=edit.end;
    }
    output.append(input,cursor,std::string::npos);
    return output;
}
}

DBStatus moveTableSchema(StorageEngine& engine,const std::string& database,
    const std::string& physicalName,const std::string& targetSchema,
    const std::function<bool(const TableSchema&)>& persistSchema) {
    if(!engine.databaseExists(database))return DBStatus::DATABASE_NOT_FOUND;
    if(!validNamespace(targetSchema))throw DbError("3F000","invalid destination schema");
    if(engine.isReadOnly())throw DbError("25006","cannot execute ALTER TABLE in a read-only transaction");
    if(engine.inTransaction() && engine.transactionDatabase()!=database)
        throw DbError("25001","namespace move belongs to a different database transaction");
    DdlTransaction transaction(engine,database);
    if(!transaction.enableSnapshotRollback() || !transaction.begin())return DBStatus::IO_ERROR;
    if(!engine.tableExists(database,physicalName))return DBStatus::TABLE_NOT_FOUND;
    const auto original=CatalogService::logicalName(physicalName);
    const std::string sourceSchema=original.schema.empty()?"public":original.schema;
    if(physicalName.rfind("__tmp_",0)==0 || targetSchema=="pg_temp" || targetSchema.rfind("pg_temp_",0)==0)
        throw DbError("0A000","cannot move objects into or out of temporary schemas");
    // Keep one real mutable generation leased while storage helpers close
    // their caches. No raw catalog row survives another lookup/publication.
    (void)engine.catalogService().get(database);
    const auto owner=engine.catalogService().acquire(database);
    auto& catalog=*owner;
    const auto destination=catalog.getNamespaceByName(targetSchema);
    if(!destination)throw DbError("3F000","schema \""+targetSchema+"\" does not exist");
    if(sourceSchema==targetSchema)return transaction.commit()?DBStatus::OK:DBStatus::IO_ERROR;
    const std::string newPhysical=targetSchema=="public"?original.name:targetSchema+"__"+original.name;
    if(engine.tableExists(database,newPhysical))throw DbError("42P07","relation already exists in destination schema");
    const auto sourceNamespace=catalog.getNamespaceByName(sourceSchema);
    const auto table=sourceNamespace?catalog.getClassByName(original.name,sourceNamespace->oid):std::nullopt;
    std::vector<PgClassRow> classes;
    std::vector<PgTypeRow> types;
    std::vector<SequenceMove> sequences;
    std::set<Oid> seen;
    if(table) {
        if(table->relkind!='r' && table->relkind!='p')throw DbError("42809","object is not a table");
        classes.push_back(*table);seen.insert(table->oid);
        for(const auto& dependency:catalog.findAllDependents(PgClassOid_Class,table->oid)) {
            if(dependency.classid!=PgClassOid_Class || seen.count(dependency.objid))continue;
            const auto related=catalog.getClass(dependency.objid);
            if(!related || (related->relkind!='i' && related->relkind!='S'))continue;
            if(related->relkind=='S') {
                if(dependency.deptype!='a' && dependency.deptype!='i')continue;
                const auto ns=catalog.getNamespace(related->relnamespace);
                if(!ns)throw DbError("XX001","owned sequence namespace is missing");
                const std::string storage=sequenceStorageName(ns->nspname,related->relname);
                SequenceInfo info;
                if(engine.getSequenceInfo(database,storage,info)!=DBStatus::OK)
                    throw DbError("XX001","owned sequence definition is missing");
                CatalogManager::QualifiedName owned;
                const bool legacyOwned=CatalogManager::parseQualifiedName(info.ownedByTable,owned,true) &&
                    owned.name==original.name && (owned.schema.empty()?"public":owned.schema)==sourceSchema;
                if(dependency.refobjsubid==0 && !legacyOwned)continue;
                sequences.push_back({*related,storage,sequenceStorageName(targetSchema,related->relname),info});
            }
            classes.push_back(*related);seen.insert(related->oid);
        }
        std::set<Oid> typeIds;
        if(table->reltype)typeIds.insert(table->reltype);
        const auto snapshot=catalog.metadataSnapshot();
        for(const auto& type:snapshot.types)if(type.typrelid==table->oid)typeIds.insert(type.oid);
        for(const auto& type:snapshot.types)if(typeIds.count(type.oid) && type.typarray)typeIds.insert(type.typarray);
        for(const auto& type:snapshot.types)if(typeIds.count(type.oid))types.push_back(type);
    }
    // Complete conflict validation precedes physical/catalog publication.
    for(const auto& relation:classes) {
        if(catalog.getClassByName(relation.relname,destination->oid))
            throw DbError("42P07","relation \""+relation.relname+"\" already exists in destination schema");
    }
    for(const auto& type:types)if(catalog.getTypeByName(type.typname,destination->oid))
        throw DbError("42710","type \""+type.typname+"\" already exists in destination schema");
    for(const auto& sequence:sequences)if(engine.sequenceExists(database,sequence.newStorage))
        throw DbError("42P07","sequence already exists in destination schema");
    std::map<Oid,std::string> sequenceReferences;
    for(const auto& sequence:sequences)sequenceReferences[sequence.relation.oid]=qualified(targetSchema,sequence.relation.relname);
    std::vector<TableSchema> defaultRewrites;
    for(const auto& name:engine.getTableNames(database)) {
        auto schema=engine.getTableSchema(database,name);bool changed=false;
        const auto logical=CatalogService::logicalName(name);
        const std::string ns=logical.schema.empty()?"public":logical.schema;
        for(size_t i=0;i<schema.len;++i) {
            const auto value=rewriteSequenceCalls(schema.cols[i].defaultValue,engine,database,ns,catalog,sequenceReferences);
            if(value!=schema.cols[i].defaultValue){schema.cols[i].defaultValue=value;changed=true;}
        }
        if(changed)defaultRewrites.push_back(std::move(schema));
    }
    transaction.markSnapshotDirty();
    const auto renamed=engine.alterTableRenameTable(database,physicalName,newPhysical);
    if(renamed!=DBStatus::OK)return renamed;
    for(const auto& sequence:sequences) {
        const auto status=engine.renameSequence(database,sequence.oldStorage,sequence.newStorage);
        if(status!=DBStatus::OK)return status;
        SequenceInfo update;update.ownedBySpecified=true;
        update.ownedByTable=qualified(targetSchema,original.name);update.ownedByColumn=sequence.info.ownedByColumn;
        if(engine.alterSequence(database,sequence.newStorage,update)!=DBStatus::OK)
            throw DbError("58030","cannot update moved sequence ownership");
    }
    for(auto relation:classes) {
        relation.relnamespace=destination->oid;
        if(!catalog.updateClass(relation.oid,relation))throw DbError("XX001","moved catalog relation is missing");
        moveNamespaceDependency(catalog,PgClassOid_Class,relation.oid,destination->oid);
    }
    for(auto type:types) {
        type.typnamespace=destination->oid;
        if(!catalog.updateType(type.oid,type))throw DbError("XX001","moved row type is missing");
        moveNamespaceDependency(catalog,PgClassOid_Type,type.oid,destination->oid);
    }
    for(auto& schema:defaultRewrites) {
        if(schema.tablename==physicalName)schema.tablename=newPhysical;
        // The rename primitive may have rewritten inbound/self FKs. Preserve
        // that current schema, changing only the planned default expressions.
        auto current=engine.getTableSchema(database,schema.tablename);
        if(current.len!=schema.len)throw DbError("XX001","moved schema declaration changed unexpectedly");
        for(size_t i=0;i<current.len;++i)current.cols[i].defaultValue=schema.cols[i].defaultValue;
        if(!persistSchema(current))throw DbError("58030","cannot publish moved sequence default references");
    }
    if(!catalog.persistAll())throw DbError("58030","cannot publish namespace move catalog");
    transaction.recordUpdate(DdlObjectKind::Table,physicalName,newPhysical);
    return transaction.commit()?DBStatus::OK:DBStatus::IO_ERROR;
}
}
