// Public evaluator behavior: registrations, aliases, clocks and threads.
#include "expression/ExprEvaluator.h"
#include "catalog/type_registry.h"
#include "parser/parser.h"
#include <atomic>
#include <chrono>
#include <ctime>
#include <memory>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
using namespace dbms;
void require(bool value,const char* message) {
    if(!value)throw std::runtime_error(message);
}
ExprValue evaluate(ExprEvaluator& evaluator,const std::string& sql) {
    SQLParser parser;auto parsed=parser.parse("SELECT "+sql);
    auto* select=parsed.stmt?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
    require(parsed.success && select && select->selectList.size()==1,"parse expression");
    return evaluator.eval(select->selectList.front().expr.get(),{});
}
void value(ExprEvaluator& evaluator,const std::string& sql,const std::string& wanted) {
    const auto actual=evaluate(evaluator,sql);
    require(!actual.isNull && actual.value==wanted,"expression value");
}
// A native argument callback may register its outer function while the
// argument is evaluated. The actual replacement receives the original datum.
void replacementDuringArgument(const std::string& outer,const std::string& type,
                               const std::string& payload,bool existing) {
    ExprEvaluator evaluator;size_t mutatorCalls=0,replacementCalls=0,previousCalls=0;
    if(existing)evaluator.registerFunction(outer,[&](const std::vector<ExprValue>& args) {
        ++previousCalls;return args.at(0);
    });
    evaluator.registerFunction("native_registry_mutator",[&](const std::vector<ExprValue>&) {
        ++mutatorCalls;
        evaluator.registerFunction(outer,[&](const std::vector<ExprValue>& args) {
            ++replacementCalls;
            require(args.size()==1 && args[0].typeName==type &&
                !args[0].isNull && args[0].value==payload,"replacement receives original native argument");
            return args[0];
        });
        return ExprValue(type,payload,false);
    });
    FunctionCallExpr call;call.funcName=outer;
    auto mutator=std::make_unique<FunctionCallExpr>();mutator->funcName="native_registry_mutator";
    call.args.push_back(std::move(mutator));
    const auto result=evaluator.eval(&call,RowContext{});
    require(result.typeName==type && !result.isNull && result.value==payload,"replacement result");
    require(mutatorCalls==1 && replacementCalls==1 && previousCalls==0,"each native site called once");
}

}
int main() {
    using namespace dbms;
    try {
        TypeRegistry::instance().bootstrap();size_t checked=0;
        // A registration made while other evaluators are being created must
        // remain private to its actual evaluator, including volatility.
        constexpr size_t workers=8;
        std::atomic<size_t> ready{0};std::atomic<bool> start{false};
        std::vector<std::string> errors(workers);std::vector<std::thread> threads;
        for(size_t i=0;i<workers;++i)threads.emplace_back([&,i] {
            ++ready;while(!start.load())std::this_thread::yield();
            try {
                for(size_t iteration=0;iteration<12;++iteration) {
                    ExprEvaluator original,overridden;
                    const auto marker=std::to_string(i*100+iteration);
                    overridden.registerFunction("abs",[marker](const std::vector<ExprValue>&) {
                        return ExprValue("text",marker);
                    });
                    overridden.registerFunction("only_local",[marker](const std::vector<ExprValue>&) {
                        return ExprValue("text",marker);
                    },'s');
                    require(original.hasFunction("abs") && !original.hasFunction("only_local"),"private function presence");
                    require(original.volatility("abs")=='i' && overridden.volatility("abs")=='v' &&
                        overridden.volatility("only_local")=='s',"private volatility");
                    value(original,"abs(-2)","2");value(overridden,"abs(-2)",marker);
                    value(overridden,"only_local()",marker);
                    ExprEvaluator later;value(later,"abs(-2)","2");
                }
            } catch(const std::exception& error) {errors[i]=error.what();}
        });
        while(ready.load()!=workers)std::this_thread::yield();start=true;
        for(auto& thread:threads)thread.join();
        for(const auto& error:errors)require(error.empty(),error.c_str());++checked;

        ExprEvaluator first,second;
        require(first.hasFunction("PoW") && first.hasFunction("iif"),"builtin aliases");
        value(first,"pow(2,3)","8");value(second,"pow(2,3)","8");
        first.registerFunction("power",[](const std::vector<ExprValue>&) {return ExprValue("integer","37");});
        value(first,"power(2,3)","37");value(first,"pow(2,3)","8");
        value(second,"power(2,3)","8");++checked;
        first.registerFunction("if",[](const std::vector<ExprValue>&) {return ExprValue("text","overridden");});
        value(first,"if(true,'left','right')","overridden");
        value(first,"iif(true,'left','right')","left");
        value(second,"if(true,'left','right')","left");++checked;

        // SQL value syntax has an owner separate from ordinary named calls.
        const std::vector<std::string> keywords={"current_date","current_timestamp","localtimestamp","current_time","localtime"};
        for(const auto& keyword:keywords) {
            require(!first.hasFunction(keyword),"keyword was not an ordinary callback");
            const auto before=evaluate(first,keyword);
            first.registerFunction(keyword,[](const std::vector<ExprValue>&) {return ExprValue("text","ordinary shadow");},'v');
            const auto after=evaluate(first,keyword);
            require(before.value==after.value && before.typeName==after.typeName && before.isNull==after.isNull,
                "keyword snapshot unaffected by ordinary registration");
            require(!second.hasFunction(keyword),"keyword registration private");++checked;
        }
        const auto transaction=evaluate(first,"transaction_timestamp()");
        const auto statement=evaluate(first,"statement_timestamp()");
        first.registerFunction("now",[](const std::vector<ExprValue>&) {return ExprValue("text","now shadow");});
        value(first,"now()","now shadow");
        require(evaluate(first,"transaction_timestamp()").value==transaction.value &&
            evaluate(first,"statement_timestamp()").value==statement.value,"copied clock aliases remain independent");
        require(evaluate(second,"now()").value!="now shadow","clock override private");++checked;

        // Legacy no-session clocks are snapshots of each constructor, not of
        // the first registry user. Waiting for an actual second makes this
        // independent of timestamp formatting precision or execution speed.
        ExprEvaluator earlier;
        const auto oldNow=evaluate(earlier,"now()");const auto clock=std::time(nullptr);
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(3);
        while(std::time(nullptr)==clock && std::chrono::steady_clock::now()<deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        require(std::time(nullptr)>clock,"wall clock advanced");
        ExprEvaluator later;
        require(evaluate(earlier,"now()").value==oldNow.value,"earlier constructor snapshot retained");
        require(evaluate(later,"now()").value!=oldNow.value,"later constructor owns a fresh snapshot");++checked;
        require(first.volatility("age")=='v' && second.volatility("clock_timestamp")=='v' &&
            second.volatility("random")=='v',"legacy volatility metadata retained");++checked;
        replacementDuringArgument("abs","text","native text",false);++checked;
        replacementDuringArgument("lower","bytea","\x410042",false);++checked;
        replacementDuringArgument("abs","text","native overlay",true);++checked;
        std::cout<<"BUILTIN_REGISTRY_ISOLATION_CHECKED="<<checked<<" FAILED=0\n";
        return 0;
    } catch(const std::exception& error) {
        std::cerr<<"BUILTIN_REGISTRY_ISOLATION_FAILED="<<error.what()<<'\n';return 1;
    }
}
