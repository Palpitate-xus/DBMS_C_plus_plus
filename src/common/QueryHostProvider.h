#pragma once

#include "common/DbError.h"
#include "common/NotificationManager.h"
#include "expression/ExprEvaluator.h"
#include "utils/Session.h"
#include <array>

namespace dbms {
// The query host, not the scalar evaluator, owns these actual row providers.
// Binding/ownership and execution consume the same immutable registration.
struct QueryHostSetReturningProvider {
    QuerySetReturningBinding::Kind kind;
    const char* name;
    const char* identity;
    size_t arity;
    bool fixedSignature;
    std::string (*resultType)(const std::vector<std::string>&);
    std::vector<ExprValue> (*read)(const std::vector<ExprValue>&,
                                 const Session*, const std::string&);
};

inline const std::array<QueryHostSetReturningProvider,2>& queryHostSetReturningProviders() {
    static const std::array<QueryHostSetReturningProvider,2> providers{{
        {QuerySetReturningBinding::Kind::Unnest,"unnest","builtin:pg_catalog.unnest(anyarray)",1,false,
            [](const std::vector<std::string>& types) {
                if(types.size()!=1)throw DbError("42883","function unnest does not exist for these arguments");
                const auto& input=types.front();
                if(input=="unknown")throw DbError("42725","function unnest(unknown) is not unique");
                if(input.size()<2 || input.compare(input.size()-2,2,"[]")!=0)
                    throw DbError("42883","function unnest("+input+") does not exist");
                return input.substr(0,input.size()-2);
            },
            [](const std::vector<ExprValue>& arguments,const Session*,const std::string&) {
                if(arguments.size()!=1)throw DbError("XX000","bound unnest argument count changed");
                return ExprEvaluator::arrayElements(arguments.front());
            }},
        {QuerySetReturningBinding::Kind::ListeningChannels,"pg_listening_channels",
            "builtin:pg_catalog.pg_listening_channels()",0,true,
            [](const std::vector<std::string>& types) {
                if(!types.empty())throw DbError("42883","function pg_listening_channels does not exist for these arguments");
                return std::string("text");
            },
            [](const std::vector<ExprValue>& arguments,const Session* backend,const std::string& database) {
                if(!arguments.empty())throw DbError("XX000","bound channel provider argument count changed");
                if(!backend)throw DbError("55000","channel provider requires an actual backend session");
                if(backend->currentDB!=database)
                    throw DbError("XX000","channel provider belongs to a different backend database");
                std::vector<ExprValue> rows;
                for(const auto& channel:notificationManager().subscriptions(backend->pid,database))
                    rows.emplace_back("text",channel,false);
                return rows;
            }}
    }};
    return providers;
}

inline const QueryHostSetReturningProvider* queryHostSetReturningProvider(const std::string& name) {
    for(const auto& provider:queryHostSetReturningProviders())if(name==provider.name)return &provider;
    return nullptr;
}
inline const QueryHostSetReturningProvider& queryHostSetReturningProvider(const QuerySetReturningBinding& binding) {
    for(const auto& provider:queryHostSetReturningProviders())
        if(binding.kind==provider.kind && binding.identity==provider.identity)return provider;
    throw DbError("XX000","bound query-host provider has no actual implementation");
}
using PreparedSetReturningReader=std::function<std::vector<ExprValue>(
    const QuerySetReturningBinding&,const std::vector<ExprValue>&)>;
} // namespace dbms
