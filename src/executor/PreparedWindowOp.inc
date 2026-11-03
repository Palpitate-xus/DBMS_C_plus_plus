// Metadata-only window result identities in the existing private executor
// namespace. No public API or planning/frame owner is introduced.
static void prepareWindowResultSharing(PreparedSelectState& state,bool actualFinalSort) {
    const auto count=state.windows.size();
    state.windowResultSortBarrier=actualFinalSort;
    state.windowBaseOwners.resize(count);state.windowSortedOwners.resize(count);
    state.windowBaseOrder.clear();state.windowSortedOrder.clear();
    for(size_t i=0;i<count;++i) {
        state.windowBaseOwners[i]=state.windowSortedOwners[i]=i;
        state.windowBaseOrder.push_back(i);
    }
    if(count<2) {state.windowSortedOrder=state.windowBaseOrder;return;}
    std::map<const FunctionCallExpr*,size_t> indexes;
    std::vector<std::string> clauses(count),identities(count);
    std::vector<bool> volatileCalls(count);
    for(size_t i=0;i<count;++i)indexes.emplace(state.windows[i].call,i);
    for(const auto& stage:state.windowStages)
        for(size_t index:stage.producers)clauses.at(index)=stage.identity;
    const auto field=[](const std::string& value){return std::to_string(value.size())+":"+value;};
    const auto site=[&](const Expr* value){return "original-site"+field(std::to_string(reinterpret_cast<uintptr_t>(value)));};
    // Reuse only a pure metadata string for the same original AST site
    // within this preparation call. No value or execution context is cached.
    std::map<const Expr*,std::string> identityMemo;
    std::function<std::string(const Expr*)> identity;
    identity=[&](const Expr* expression)->std::string {
        if(const auto cached=identityMemo.find(expression);cached!=identityMemo.end())return cached->second;
        std::string result;
        try {
            result=ExprHelper::scalarExpressionIdentity(expression,[&](const ColumnRefExpr& column) {
                if(!column.binding)return site(&column);
                const auto& b=*column.binding;uint32_t oid=b.typeOid;
                for(const auto& range:state.query->sourceRanges)if(range.ordinal==b.sourceOrdinal) {
                    oid=b.columnOrdinal<range.columns.size()?range.columns[b.columnOrdinal].typeOid:0;break;
                }
                return field(std::to_string(b.scopeDepth))+field(std::to_string(b.sourceOrdinal))+
                    field(std::to_string(b.columnOrdinal))+field(b.declaredType)+field(std::to_string(oid))+
                    field(b.mergedUsing?"merged":"ordinary");
            },state.dbname,state.engine,[&](const Expr* node)->std::optional<std::string> {
                // SubPlan-like original child sites stay independent. Never
                // normalize/reparse a child or inspect its execution owner.
                if(node->preparedSubquery)return site(node);
                const auto* call=dynamic_cast<const FunctionCallExpr*>(node);
                if(!call)return {};
                if(call->sqlValue!=FunctionCallExpr::SqlValue::None)
                    return "sql-value"+field(std::to_string(static_cast<unsigned>(call->sqlValue)))+
                        field(call->sqlValuePrecision?std::to_string(*call->sqlValuePrecision):"default");
                std::string key;
                if(const auto found=indexes.find(call);found!=indexes.end()) {
                    const auto index=found->second;const auto& producer=state.windows[index];
                    key="window"+field(clauses.at(index))+field(producer.name)+field(producer.inputType)+
                        field(producer.resultType)+field(producer.valueComparison?producer.valueComparison->identity:"");
                } else {
                    const auto aggregateFound=std::find_if(state.aggregates.begin(),state.aggregates.end(),
                        [&](const auto& aggregate){return aggregate.call==call;});
                    if(aggregateFound==state.aggregates.end())return {};
                    key="actual-aggregate"+field(aggregateFound->name)+field(aggregateFound->argumentType)+field(aggregateFound->resultType);
                }
                key+=field(call->distinct?"distinct":"all")+field(call->orderBy);
                for(const auto& value:call->args)key+=field(identity(value.get()));
                for(const auto& value:call->namedArgs)key+=field(value.name)+field(identity(value.value.get()));
                for(const auto& value:call->aggregateOrderKeys)key+=field(identity(value.expr.get()))+
                    field(value.ascending?"asc":"desc")+field(value.nullsFirst?"first":"last")+
                    field(value.enumComparison?value.enumComparison->identity:"")+field(value.usingOperator);
                return key+field(identity(call->filter.get()));
            });
        } catch(const DbError& error) {
            if(error.sqlState()!="0A000")throw;
            result=site(expression); // Opaque identity never rejects execution.
        }
        identityMemo.emplace(expression,result);
        return result;
    };
    const auto windowIdentity=[&](size_t index)->const std::string& {
        auto& key=identities.at(index);
        if(key.empty())key=identity(state.windows.at(index).call);
        return key;
    };
    const auto sameSignature=[&](size_t first,size_t second) {
        const auto& a=state.windows.at(first);const auto& b=state.windows.at(second);
        return clauses.at(first)==clauses.at(second) && a.name==b.name &&
            a.inputType==b.inputType && a.resultType==b.resultType;
    };
    std::function<bool(const Expr*)> containsVolatile;
    containsVolatile=[&](const Expr* expression) {
        if(!expression || expression->preparedSubquery)return false;
        if(const auto* call=dynamic_cast<const FunctionCallExpr*>(expression)) {
            if(!indexes.count(call) && call->sqlValue==FunctionCallExpr::SqlValue::None &&
               std::none_of(state.aggregates.begin(),state.aggregates.end(),[&](const auto& a){return a.call==call;}) &&
               state.evaluator.scalarFunctionVolatility(call,state.engine)=='v')return true;
        }
        bool result=false;
        prepared_window_detail::spec_detail::valueChildren(const_cast<Expr*>(expression),[&](Expr* child) {
            if(!result)result=containsVolatile(child);
        });
        return result;
    };
    for(size_t i=0;i<count;++i) {
        volatileCalls[i]=containsVolatile(state.windows[i].call);
        if(!volatileCalls[i])for(size_t j=0;j<i;++j)
            if(!volatileCalls[j] && sameSignature(i,j) && windowIdentity(i)==windowIdentity(j)) {
                state.windowBaseOwners[i]=state.windowBaseOwners[j];break;
            }
    }
    if(!actualFinalSort) {
        state.windowSortedOwners=state.windowBaseOwners;state.windowSortedOrder=state.windowBaseOrder;return;
    }
    // A virtual target remains a *whole* expression. Its nested WindowFunc
    // is not an already available top-level value for a later pulled leaf.
    std::vector<bool> retained(state.targets.size(),state.select().distinct);
    for(size_t i=0;i<retained.size();++i)
        retained[i]=retained[i] || !containsVolatile(state.targets[i]);
    for(const auto& key:state.keys)if(key.target!=PreparedSelectState::noTarget)
        retained.at(state.targetOwners.at(key.target))=true;
    const bool hasPostponed=std::any_of(retained.begin(),retained.end(),[](bool value){return !value;});
    std::vector<const Expr*> roots;
    std::vector<std::string> rootIdentities;
    const auto appendRoot=[&](const Expr* expression) {
        roots.push_back(expression);
        if(hasPostponed)rootIdentities.push_back(identity(expression));
    };
    // Keep the original virtual-root traversal even when nothing is pulled.
    // Hidden sort roots/priorKey still determine its exact producer order.
    for(size_t i=0;i<state.targets.size();++i)if(retained[i])appendRoot(state.targets[i]);
    for(const auto& key:state.keys)if(key.target==PreparedSelectState::noTarget &&
        key.priorKey==PreparedSelectState::noTarget)appendRoot(key.expression);
    std::function<void(const Expr*,const std::function<void(size_t)>&)> windows;
    windows=[&](const Expr* expression,const std::function<void(size_t)>& visit) {
        if(!expression || expression->preparedSubquery)return;
        if(const auto* call=dynamic_cast<const FunctionCallExpr*>(expression)) {
            if(const auto found=indexes.find(call);found!=indexes.end()){visit(found->second);return;}
            if(std::any_of(state.aggregates.begin(),state.aggregates.end(),[&](const auto& a){return a.call==call;}))return;
        }
        prepared_window_detail::spec_detail::valueChildren(const_cast<Expr*>(expression),
            [&](Expr* child){windows(child,visit);});
    };
    const auto addOrder=[&](size_t index) {
        if(std::find(state.windowSortedOrder.begin(),state.windowSortedOrder.end(),index)==state.windowSortedOrder.end())
            state.windowSortedOrder.push_back(index);
    };
    for(const auto* expression:roots)windows(expression,addOrder);
    for(size_t i=0;i<state.targets.size();++i)if(!retained[i])windows(state.targets[i],[&](size_t index) {
        const auto found=std::find(rootIdentities.begin(),rootIdentities.end(),windowIdentity(index));
        if(found!=rootIdentities.end()) {
            const auto position=static_cast<size_t>(found-rootIdentities.begin());
            const auto* call=dynamic_cast<const FunctionCallExpr*>(roots.at(position));
            const auto owner=indexes.find(call);
            if(owner==indexes.end())throw DbError("XX000","window target identity lost its actual producer");
            state.windowSortedOwners[index]=owner->second;
        } else {
            appendRoot(state.windows[index].call);addOrder(index);
        }
    });
    // ExecInitWindowAgg's additional nonvolatile equal() reuse applies
    // regardless of whether an upper Sort retained the extracted target.
    for(size_t i=0;i<count;++i)
        state.windowSortedOwners[i]=state.windowBaseOwners.at(state.windowSortedOwners[i]);
    for(size_t i=0;i<count;++i)addOrder(i);
}

// The real input projection is below every WindowAgg. Keys are owning typed
// values, evaluated once before any window producer, and survive later sorts.
class PreparedWindowInputOp final : public Operator {
    OpPtr child_;
    std::shared_ptr<PreparedSelectState> state_;
    RowContext current_;
    std::vector<ExprValue> values_;
    std::vector<size_t> active_;
    bool opened_=false,childOpened_=false,available_=false;
    friend class PreparedWindowOp;
public:
    PreparedWindowInputOp(OpPtr child,std::shared_ptr<PreparedSelectState> state)
        :child_(std::move(child)),state_(std::move(state)) {}
    ~PreparedWindowInputOp() override {try{close();}catch(...) {}}
    bool open() override {
        OpenInstrument instrument(this);clearError();close();opened_=true;
        for(size_t i=0;i<state_->windowInputs.size();++i)
            if(state_->windowInputDemanded(state_->windowInputs[i]))active_.push_back(i);
        return true;
    }
    bool next(std::string& display) override {
        NextInstrument instrument(this);
        if(!opened_)return false;
        try {
            checkForQueryInterrupt();
            if(!childOpened_) {
                childOpened_=true;
                if(!child_->open()) {
                    propagateChildError(child_.get(),"window input projection open failed");
                    throw DbError("XX000",errorMessage());
                }
            }
            if(!child_->next(display)) {
                if(child_->hasError()) {
                    propagateChildError(child_.get(),"window input projection failed");
                    throw DbError("XX000",errorMessage());
                }
                close();return false;
            }
            current_=state_->context(*child_,display);
            values_.assign(state_->windowInputs.size(),ExprValue{});
            for(size_t index:active_) {
                const auto& input=state_->windowInputs[index];
                values_[index]=state_->evaluate(input.expression,current_);
                for(const auto& column:input.columns)
                    current_.setBoundColumn(column.sourceOrdinal,column.columnOrdinal,values_[index]);
            }
            available_=true;display.clear();instrument.emitted=true;return true;
        } catch(...) {const auto failure=std::current_exception();try{close();}catch(...){}std::rethrow_exception(failure);}
    }
    void close() override {
        opened_=false;available_=false;current_=RowContext{};values_.clear();active_.clear();
        if(childOpened_){childOpened_=false;child_->close();}
    }
    const std::vector<ExprValue>& values() const {
        if(!available_)throw DbError("XX000","window input projection lost its current typed keys");
        return values_;
    }
    bool supportsPreparedContexts() const override {return true;}
    bool lastPreparedContext(RowContext& row) const override {if(!available_)return false;row=current_;return true;}
    std::string preparedPlanNodeName() const override {return "WindowInput";}
    std::vector<Operator*> preparedPlanChildren() const override {
        std::vector<Operator*> result{child_.get()};
        for(const auto& input:state_->windowInputs) {
            auto children=state_->execution->childPlans(input.expression);
            result.insert(result.end(),children.begin(),children.end());
        }
        return result;
    }
};

// Included inside ExecutionPlan.cpp's private operator namespace. The host
// retains real owning row contexts; WindowKernel owns ordinal indexes only.
class PreparedWindowOp final : public Operator {
    using Layout=window_detail::Layout;
    using Lookup=window_detail::Lookup;
    using Frame=window_detail::Frame;
    using Exclusion=window_detail::Exclusion;
    using Producer=prepared_window_detail::Producer;
    struct IntegerKey {
        enum class State : uint8_t { Unparsed, Invalid, Ready };
        State state=State::Unparsed;
        int64_t value=0;
    };
    struct Input {
        RowContext context;
        std::vector<ExprValue> keys;
        // Empty until an actual eligible key comparison reaches this row.
        std::vector<IntegerKey> integerKeys;
    };
    struct Transition {
        std::unique_ptr<prepared_window_detail::AggregateState> state;
        size_t partition=0,first=0,last=0;
        bool valid=false;
        std::optional<int64_t> buckets;
    };
    OpPtr child_;
    std::shared_ptr<PreparedSelectState> state_;
    prepared_window_detail::Stage stage_;
    PreparedWindowOp* previous_=nullptr;
    PreparedWindowInputOp* inputProjection_=nullptr;
    std::vector<size_t> partitionIndexes_,orderIndexes_;
    std::vector<Input> input_;
    Layout layout_;
    WindowFrame frame_;
    Exclusion exclusion_=Exclusion::None;
    std::vector<size_t> active_;
    struct ResultAlias {size_t producer,slot;bool last=false;};
    std::vector<ResultAlias> resultAliases_;
    std::vector<Transition> transitions_;
    SetOutputComparisons keyComparisons_,valueComparisons_;
    // nullopt means unexamined, OID0 means examined but not authoritative.
    // This metadata cache is also created only at actual comparison demand.
    std::vector<std::optional<uint32_t>> keyTypeOids_;
    RowContext current_;
    std::vector<ExprValue> currentKeys_;
    size_t position_=0;
    bool opened_=false,childOpened_=false,eof_=false,initialized_=false;
    bool sorted_=false,reuseKeys_=false,available_=false;
    bool unchangedResults_=false; // Frozen metadata; survives close/restart.

    bool demanded() const {
        return std::any_of(stage_.producers.begin(),stage_.producers.end(),[&](size_t i) {
            return state_->execution->aggregateColumnDemanded(state_->windows.at(i).call);
        });
    }
    bool hasOriginalResultMapping() const {
        const auto& producers=stage_.producers;
        const auto belongs=[&](size_t index) {
            return std::find(producers.begin(),producers.end(),index)!=producers.end();
        };
        const auto ownersUnchanged=[&](const std::vector<size_t>& owners) {
            if(owners.size()!=state_->windows.size())return false;
            for(size_t index=0;index<owners.size();++index) {
                if(owners[index]>=owners.size())return false;
                if(belongs(index)) {if(owners[index]!=index)return false;}
                else if(belongs(owners[index]))return false;
            }
            return true;
        };
        const auto orderUnchanged=[&](const std::vector<size_t>& order) {
            size_t position=0;
            for(size_t index:order) {
                if(index>=state_->windows.size())return false;
                if(!belongs(index))continue;
                if(position>=producers.size() || producers[position++]!=index)return false;
            }
            return position==producers.size();
        };
        // Check both policies for the entire stage, not only this invocation's
        // demanded subset. A top-stage/provider/demand change must stay valid.
        return ownersUnchanged(state_->windowBaseOwners) && ownersUnchanged(state_->windowSortedOwners) &&
            orderUnchanged(state_->windowBaseOrder) && orderUnchanged(state_->windowSortedOrder);
    }
    uint32_t boundKeyTypeOid(size_t index) {
        if(keyTypeOids_.empty())keyTypeOids_.resize(state_->windowInputs.size());
        auto& cached=keyTypeOids_.at(index);
        if(cached)return *cached;
        const auto* expression=state_->windowInputs.at(index).expression;
        if(!expression){cached=uint32_t{0};return 0;}
        uint32_t oid=0;
        if(const auto* column=dynamic_cast<const ColumnRefExpr*>(expression)) {
            if(!column->binding){cached=uint32_t{0};return 0;}
            const auto& binding=*column->binding;
            for(const auto& range:state_->query->sourceRanges)
                if(range.ordinal==binding.sourceOrdinal) {
                    // This real descriptor wins even when its OID is zero.
                    oid=binding.columnOrdinal<range.columns.size()
                        ?range.columns[binding.columnOrdinal].typeOid:0;
                    cached=oid;return oid;
                }
            cached=binding.typeOid;return binding.typeOid;
        }
        if(expression && expression->preparedSubquery) {
            const auto output=state_->query->statementOutputs.find(expression->preparedSubquery.get());
            if(output!=state_->query->statementOutputs.end() && output->second.size()==1)
                oid=output->second.front().typeOid;
            cached=oid;return oid;
        }
        // Only the exact original projected site has an authoritative result
        // descriptor here. No spelling, operator inference or catalog lookup.
        const auto projections=state_->query->projectionBindings.find(state_->statement);
        const auto outputs=state_->query->statementOutputs.find(state_->statement);
        bool matched=false;
        if(projections!=state_->query->projectionBindings.end() &&
           outputs!=state_->query->statementOutputs.end())
            for(size_t i=0;i<projections->second.size();++i)
                if(projections->second[i].expression==expression) {
                    if(i>=outputs->second.size() || !outputs->second[i].typeOid ||
                       (matched && oid!=outputs->second[i].typeOid)) {
                        cached=uint32_t{0};return 0;
                    }
                    oid=outputs->second[i].typeOid;matched=true;
                }
        cached=oid;return oid;
    }
    static std::optional<int64_t> cachedInteger(Input& row,size_t index,const ExprValue& value) {
        if(row.integerKeys.empty())row.integerKeys.resize(row.keys.size());
        auto& cached=row.integerKeys.at(index);
        if(cached.state==IntegerKey::State::Unparsed) {
            int64_t integer=0;const auto end=value.value.data()+value.value.size();
            const auto parsed=std::from_chars(value.value.data(),end,integer);
            if(parsed.ec==std::errc{} && parsed.ptr==end &&
               (value.typeName!="smallint" || (integer>=INT16_MIN && integer<=INT16_MAX)) &&
               (value.typeName!="integer" || (integer>=INT32_MIN && integer<=INT32_MAX))) {
                cached.value=integer;cached.state=IntegerKey::State::Ready;
            } else cached.state=IntegerKey::State::Invalid;
        }
        return cached.state==IntegerKey::State::Ready?std::optional<int64_t>(cached.value):std::nullopt;
    }
    std::optional<int> integerCompare(Input& left,Input& right,size_t index,
        const ExprValue& a,const ExprValue& b,
        const std::optional<QueryComparisonBinding>& enumeration) {
        if(enumeration || a.typeName!=b.typeName)return std::nullopt;
        const uint32_t expected=a.typeName=="smallint"?21:a.typeName=="integer"?23:
            a.typeName=="bigint"?20:0;
        if(!expected || boundKeyTypeOid(index)!=expected)return std::nullopt;
        const auto first=cachedInteger(left,index,a),second=cachedInteger(right,index,b);
        if(!first || !second)return std::nullopt;
        return (*first>*second)-(*first<*second);
    }
    int compare(const ExprValue& a,const ExprValue& b,size_t key,bool first,
                const std::optional<QueryComparisonBinding>& enumeration,
                Input& left,Input& right,size_t inputIndex) {
        if(a.isNull || b.isNull)return a.isNull==b.isNull?0:(a.isNull==first?-1:1);
        if(const auto order=integerCompare(left,right,inputIndex,a,b,enumeration))return *order;
        return keyComparisons_.compare(key,state_->evaluator,a,b,true,enumeration);
    }
    int partitionCompare(size_t a,size_t b) {
        for(size_t i=0;i<stage_.spec.partition.size();++i) {
            const size_t index=partitionIndexes_.at(i);
            const auto& left=input_[a].keys.at(index);
            const auto& right=input_[b].keys.at(index);
            const int order=compare(left,right,i,
                stage_.spec.partitionNullsFirst.at(i),stage_.spec.partitionComparisons.at(i),input_[a],input_[b],index);
            if(order) {
                if(left.isNull || right.isNull)return order;
                return stage_.spec.partitionAsc.at(i)?order:-order;
            }
        }
        return 0;
    }
    int orderCompare(size_t a,size_t b) {
        for(size_t i=0;i<stage_.spec.order.size();++i) {
            const auto& key=stage_.spec.order[i];
            const size_t index=orderIndexes_.at(i);
            const auto& left=input_[a].keys.at(index);
            const auto& right=input_[b].keys.at(index);
            const int order=compare(left,right,i+stage_.spec.partition.size(),
                key.nullsFirst,key.comparison,input_[a],input_[b],index);
            if(order) {
                if(left.isNull || right.isNull)return order;
                return key.asc?order:-order;
            }
        }
        return 0;
    }
    void prefix() {
        if(sorted_)return;
        if(stage_.spec.partition.empty() && stage_.spec.order.empty())layout_.buildIdentityPrefix(input_.size(),eof_);
        else layout_.appendOrderedPrefix(input_.size(),eof_,
            [&](size_t a,size_t b){return partitionCompare(a,b);},
            [&](size_t a,size_t b){return orderCompare(a,b);});
    }
    bool read() {
        if(eof_)return false;
        checkForQueryInterrupt();
        std::string display;
        if(!child_->next(display)) {
            if(child_->hasError()) {
                propagateChildError(child_.get(),"window input failed");
                throw DbError("XX000",errorMessage());
            }
            eof_=true;
            if(childOpened_){childOpened_=false;child_->close();}
            prefix();return false;
        }
        Input row;
        // These exact children retain their owning current row until the
        // next child operation. Borrow only for this synchronous copy through
        // the original execution owner; retain no reference or frame here.
        if(previous_ && child_.get()==previous_ && previous_->available_)
            row.context=state_->execution->context(previous_->current_);
        else if(inputProjection_ && child_.get()==inputProjection_ && inputProjection_->available_)
            row.context=state_->execution->context(inputProjection_->current_);
        else row.context=state_->context(*child_,display);
        if(previous_) {
            if(!previous_->available_)throw DbError("XX000","window stage lost its actual input key stream");
            row.keys=previous_->currentKeys_;
        } else row.keys=inputProjection_->values();
        // Retain the original eager key-index validation without copying cells.
        for(size_t index:partitionIndexes_)(void)row.keys.at(index);
        for(size_t index:orderIndexes_)(void)row.keys.at(index);
        input_.push_back(std::move(row));prefix();return true;
    }
    void initialize() {
        if(initialized_)return;
        initialized_=true;
        frame_.mode=stage_.spec.frameMode.empty()?"range":stage_.spec.frameMode;
        frame_.start=stage_.spec.start;frame_.end=stage_.spec.end;
        if(frame_.start==WindowDef::BoundKind::Unspecified) {
            frame_.start=WindowDef::BoundKind::UnboundedPreceding;
            frame_.end=stage_.spec.order.empty()?WindowDef::BoundKind::UnboundedFollowing:WindowDef::BoundKind::CurrentRow;
        } else if(frame_.end==WindowDef::BoundKind::Unspecified)frame_.end=WindowDef::BoundKind::CurrentRow;
        const auto context=state_->execution->context(state_->outerRow);
        if(WindowFrame::offsetBound(frame_.start))frame_.startOffset=state_->evaluate(stage_.spec.startOffset,context);
        if(WindowFrame::offsetBound(frame_.end))frame_.endOffset=state_->evaluate(stage_.spec.endOffset,context);
        frame_.validateOffsets();
        const auto& exclusion=stage_.spec.exclusion;
        exclusion_=exclusion=="current row"?Exclusion::CurrentRow:exclusion=="group"?Exclusion::Group:
            exclusion=="ties"?Exclusion::Ties:Exclusion::None;
        childOpened_=true;
        if(!child_->open()) {
            propagateChildError(child_.get(),"window input open failed");
            throw DbError("XX000",errorMessage());
        }
        if(!reuseKeys_ && (!stage_.spec.partition.empty() || !stage_.spec.order.empty())) {
            // A genuine sort consumes its input, but window values are still
            // evaluated only by next(). A later equal-key stage streams this
            // actual sorted key output instead of repeating its expressions.
            sorted_=true;
            while(read()){}
            layout_.build(input_.size(),[&](size_t a,size_t b){return partitionCompare(a,b);},
                [&](size_t a,size_t b){return orderCompare(a,b);},
                [](auto begin,auto end,auto less){interruptibleStableSort(begin,end,less);});
        } else prefix();
    }
    bool ensure(size_t position) {
        while(position>=layout_.size() && !eof_)read();
        return position<layout_.size();
    }
    void completePartition(size_t position) {
        while(!layout_.partitionComplete(position))read();
    }
    ExprValue argument(const Producer& producer,size_t index,size_t source) {
        if(index>=producer.call->args.size())throw DbError("XX000","window argument index is invalid");
        return state_->evaluate(producer.call->args[index].get(),input_.at(source).context);
    }
    ExprValue converted(ExprValue value,const std::string& type) {
        if(ExprHelper::canonicalResultTypeName(value.typeName)==ExprHelper::canonicalResultTypeName(type))return value;
        CastExpr expression;expression.typeName=type;expression.implicit=true;
        auto input=std::make_unique<ParameterExpr>();input->origin=ParameterOrigin::RuntimeCell;
        input->declaredType=value.typeName;expression.operand=std::move(input);
        std::vector<ExprValue> cells;cells.push_back(std::move(value));RowContext row;row.setParameters(std::move(cells));
        return state_->evaluator.eval(&expression,row);
    }
    Frame bounds(size_t position) {
        for(;;) {
            const auto result=layout_.frameBounds(position,frame_,[&](size_t source) {
                const auto& row=input_.at(source);
                if(orderIndexes_.empty())throw DbError("XX000","RANGE frame lost its actual order key");
                return row.keys.at(orderIndexes_.front());
            },stage_.spec.order.empty()?true:stage_.spec.order.front().asc,
              stage_.spec.order.empty()?false:stage_.spec.order.front().nullsFirst);
            if(result.headComplete)return result;
            read();
        }
    }
    Lookup frameMember(size_t position,uint64_t count,bool fromEnd) {
        for(;;) {
            const auto window=bounds(position);
            const auto target=layout_.frameNth(position,window,count,fromEnd,exclusion_);
            if(target.state!=Lookup::State::NeedMore)return target;
            read();
        }
    }
    void advanceAggregates(size_t position,const std::vector<size_t>& indexes) {
        if(indexes.empty())return;
        auto window=bounds(position);
        while(!window.complete()){read();window=bounds(position);}
        // Probe the first tuple beyond a finite frame once for this stage.
        if(!layout_.partitionComplete(position) || window.last<layout_.partition(position).last)
            (void)ensure(window.last);
        const size_t partition=layout_.partition(position).first;
        std::vector<size_t> beginnings(transitions_.size(),window.last);
        size_t first=window.last;
        for(size_t stateIndex:indexes) {
            const size_t producerIndex=active_.at(stateIndex);
            const auto& producer=state_->windows.at(producerIndex);
            auto& transition=transitions_.at(stateIndex);
            if(!transition.state)transition.state=std::make_unique<prepared_window_detail::AggregateState>(
                producer.name,producer.inputType,producer.resultType,state_->evaluator,
                [&,stateIndex,producerIndex](const ExprValue& a,const ExprValue& b) {
                    return valueComparisons_.compare(stateIndex,state_->evaluator,a,b,true,
                        state_->windows.at(producerIndex).valueComparison);
                });
            const bool append=transition.valid && transition.partition==partition && transition.first==window.first &&
                transition.last<=window.last && exclusion_==Exclusion::None;
            beginnings[stateIndex]=append?transition.last:window.first;
            first=std::min(first,beginnings[stateIndex]);
            if(!append)transition.state->reset();
        }
        // PostgreSQL advances all aggregates for one source tuple before
        // advancing to the next tuple. Volatile FILTER/argument calls retain
        // that order even when several producers share a peer/frame boundary.
        for(size_t member=first;member<window.last;++member) {
            checkForQueryInterrupt();
            if(!layout_.frameContains(position,window,member,exclusion_))continue;
            const size_t source=layout_.sourceAt(member);
            for(size_t stateIndex:indexes) {
                if(member<beginnings[stateIndex])continue;
                const auto& producer=state_->windows.at(active_.at(stateIndex));
                if(producer.call->filter) {
                    const auto filter=state_->evaluate(producer.call->filter.get(),input_[source].context);
                    if(filter.isNull || !filter.asBool())continue;
                }
                std::vector<ExprValue> values;values.reserve(producer.call->args.size());
                for(size_t i=0;i<producer.call->args.size();++i) {
                    const auto* star=dynamic_cast<const LiteralExpr*>(producer.call->args[i].get());
                    values.push_back(star && star->value=="*"?ExprValue("boolean","t"):argument(producer,i,source));
                }
                transitions_[stateIndex].state->add(values);
            }
        }
        for(size_t stateIndex:indexes) {
            auto& transition=transitions_[stateIndex];transition.valid=true;transition.partition=partition;
            transition.first=window.first;transition.last=window.last;
        }
    }
    ExprValue compute(size_t producerIndex,size_t position,size_t stateIndex) {
        const auto& producer=state_->windows.at(producerIndex);
        const auto& name=producer.name;
        const auto result=[&](size_t number){return ExprValue(producer.resultType,std::to_string(number));};
        const auto null=[&]{return ExprValue(producer.resultType,"",true);};
        if(name=="row_number")return result(layout_.rowNumber(position));
        if(name=="rank")return result(layout_.rank(position));
        if(name=="dense_rank")return result(layout_.denseRank(position));
        if(name=="percent_rank" || name=="cume_dist" || name=="ntile") {
            completePartition(position);
            const auto part=layout_.partition(position);
            if(name=="percent_rank")return ExprValue("double precision",float8_output_detail::format(part.last-part.first<=1?0.0:
                double(layout_.rank(position)-1)/double(part.last-part.first-1)));
            if(name=="cume_dist")return ExprValue("double precision",float8_output_detail::format(
                double(layout_.peers(position).last-part.first)/double(part.last-part.first)));
            auto& transition=transitions_.at(stateIndex);
            if(!transition.valid || transition.partition!=part.first) {
                transition.valid=true;transition.partition=part.first;transition.buckets.reset();
            }
            if(!transition.buckets) {
                const auto count=argument(producer,0,layout_.sourceAt(position));
                if(count.isNull)return null();
                const int64_t buckets=count.asInt();
                if(buckets<=0)throw DbError("22014","argument of ntile must be greater than zero");
                transition.buckets=buckets;
                // NULL bucket arguments leave PostgreSQL's bucket counter
                // uninitialized. Counting starts at the first non-NULL call.
                transition.first=position;
            }
            const uint64_t buckets=static_cast<uint64_t>(*transition.buckets);
            const size_t size=part.last-part.first,index=position-transition.first;
            if(buckets>=size)return result(index+1);
            const size_t base=size/static_cast<size_t>(buckets),extra=size%static_cast<size_t>(buckets);
            const size_t leading=extra?(base+1)*extra:0;
            return result(index<leading?index/(base+1)+1:extra+(index-leading)/base+1);
        }
        if(name=="lag" || name=="lead") {
            const size_t current=layout_.sourceAt(position);
            int64_t offset=1;
            if(producer.call->args.size()>1) {
                const auto value=argument(producer,1,current);if(value.isNull)return null();offset=value.asInt();
            }
            if(offset<std::numeric_limits<int32_t>::min() || offset>std::numeric_limits<int32_t>::max())
                throw DbError("22003","integer out of range");
            if(name=="lag")offset=-offset; // widened INTEGER, including INT32_MIN
            Lookup target;
            do {target=layout_.partitionOffset(position,offset);if(target.state==Lookup::State::NeedMore)read();}
            while(target.state==Lookup::State::NeedMore);
            if(target.found())return converted(argument(producer,0,target.source),producer.resultType);
            return producer.call->args.size()>2?converted(argument(producer,2,current),producer.resultType):null();
        }
        if(name=="first_value" || name=="last_value" || name=="nth_value") {
            uint64_t nth=1;
            if(name=="nth_value") {
                const auto number=argument(producer,1,layout_.sourceAt(position));if(number.isNull)return null();
                const int64_t count=number.asInt();if(count<=0)throw DbError("22016","argument of nth_value must be greater than zero");
                nth=static_cast<uint64_t>(count);
            }
            const auto target=frameMember(position,nth,name=="last_value");
            return target.found()?converted(argument(producer,0,target.source),producer.resultType):null();
        }
        if(!transitions_.at(stateIndex).state)throw DbError("XX000","window aggregate transition was not advanced");
        return transitions_[stateIndex].state->finish();
    }
public:
    PreparedWindowOp(OpPtr child,std::shared_ptr<PreparedSelectState> state,prepared_window_detail::Stage stage,
                     PreparedWindowInputOp* inputProjection)
        :child_(std::move(child)),state_(std::move(state)),stage_(std::move(stage)),inputProjection_(inputProjection) {
        previous_=dynamic_cast<PreparedWindowOp*>(child_.get());
        for(const auto* key:stage_.spec.partition)partitionIndexes_.push_back(state_->windowInputIndex(key));
        for(const auto& key:stage_.spec.order)orderIndexes_.push_back(state_->windowInputIndex(key.expression));
        unchangedResults_=hasOriginalResultMapping();
    }
    ~PreparedWindowOp() override {try{close();}catch(...) {}}
    bool open() override {
        OpenInstrument instrument(this);clearError();close();opened_=true;
        if(unchangedResults_) {
            for(size_t index:stage_.producers)
                if(state_->execution->aggregateColumnDemanded(state_->windows.at(index).call))active_.push_back(index);
        } else {
            std::vector<size_t> demanded;
            for(size_t index:stage_.producers)if(state_->execution->aggregateColumnDemanded(state_->windows.at(index).call))demanded.push_back(index);
            if(demanded.size()<2) {
                active_=demanded;
                if(!demanded.empty())resultAliases_.push_back({demanded.front(),0,true});
            } else {
            bool lastDemanded=false;
            for(const auto& stage:state_->windowStages) {
                if(stage.identity==stage_.identity){lastDemanded=true;continue;}
                if(lastDemanded && std::any_of(stage.producers.begin(),stage.producers.end(),[&](size_t index) {
                    return state_->execution->aggregateColumnDemanded(state_->windows.at(index).call);
                })){lastDemanded=false;break;}
            }
            const bool sorted=state_->windowResultSortBarrier && lastDemanded;
            const auto& owners=sorted?state_->windowSortedOwners:state_->windowBaseOwners;
            const auto& order=sorted?state_->windowSortedOrder:state_->windowBaseOrder;
            std::map<size_t,size_t> groups;
            for(size_t index:order) {
                const auto owner=owners.at(index);
                if(groups.count(owner))continue;
                const auto representative=std::find_if(demanded.begin(),demanded.end(),[&](size_t value){return owners.at(value)==owner;});
                if(representative==demanded.end())continue;
                groups.emplace(owner,active_.size());active_.push_back(*representative);
            }
            std::vector<size_t> lastAlias(active_.size(),std::numeric_limits<size_t>::max());
            for(size_t index:demanded) {
                const auto slot=groups.at(owners.at(index));lastAlias.at(slot)=resultAliases_.size();
                resultAliases_.push_back({index,slot,false});
            }
            for(size_t index:lastAlias)resultAliases_.at(index).last=true;
            }
        }
        auto* preceding=previous_;
        while(preceding && !preceding->demanded())preceding=preceding->previous_;
        reuseKeys_=preceding && preceding->stage_.keyIdentity==stage_.keyIdentity;
        keyComparisons_.reset(stage_.spec.partition.size()+stage_.spec.order.size());
        valueComparisons_.reset(active_.size());transitions_.resize(active_.size());
        return true;
    }
    bool next(std::string& display) override {
        NextInstrument instrument(this);
        if(!opened_)return false;
        try {
            checkForQueryInterrupt();
            if(active_.empty()) {
                if(!childOpened_){childOpened_=true;if(!child_->open()){propagateChildError(child_.get(),"window passthrough open failed");throw DbError("XX000",errorMessage());}}
                if(!child_->next(display)) {if(child_->hasError()){propagateChildError(child_.get(),"window passthrough failed");throw DbError("XX000",errorMessage());}close();return false;}
                current_=state_->context(*child_,display);
                currentKeys_=previous_?previous_->currentKeys_:inputProjection_->values();
                available_=true;instrument.emitted=true;return true;
            }
            initialize();
            if(!ensure(position_)){close();return false;}
            // Plain window functions precede aggregate transitions. Only
            // approved equal window results share a representative; ordinary
            // scalar sites and every original result column keep their owner.
            std::vector<std::optional<ExprValue>> values(active_.size());
            const auto aggregateCall=[&](size_t index) {
                const auto& n=state_->windows.at(index).name;
                return n!="row_number" && n!="rank" && n!="dense_rank" && n!="percent_rank" && n!="cume_dist" &&
                    n!="ntile" && n!="lag" && n!="lead" && n!="first_value" && n!="last_value" && n!="nth_value";
            };
            std::vector<size_t> aggregates;
            for(size_t i=0;i<active_.size();++i) {
                if(aggregateCall(active_[i]))aggregates.push_back(i);
                else values[i]=compute(active_[i],position_,i);
            }
            advanceAggregates(position_,aggregates);
            for(size_t i:aggregates)values[i]=compute(active_[i],position_,i);
            const size_t source=layout_.sourceAt(position_);
            current_=input_[source].context;currentKeys_=input_[source].keys;
            if(unchangedResults_) {
                // The original typed host loop: all result columns and their
                // evaluation order are unchanged by either metadata policy.
                for(size_t i=0;i<active_.size();++i) {
                    const auto& producer=state_->windows.at(active_[i]);
                    current_.setBoundColumn(producer.column.sourceOrdinal,producer.column.columnOrdinal,std::move(*values[i]));
                }
            } else {
                for(const auto& alias:resultAliases_) {
                    const auto& producer=state_->windows.at(alias.producer);
                    auto& value=*values.at(alias.slot);
                    if(alias.last)current_.setBoundColumn(producer.column.sourceOrdinal,producer.column.columnOrdinal,std::move(value));
                    else current_.setBoundColumn(producer.column.sourceOrdinal,producer.column.columnOrdinal,value);
                }
            }
            ++position_;display.clear();available_=true;instrument.emitted=true;return true;
        } catch(...) {const auto primary=std::current_exception();try{close();}catch(...){}std::rethrow_exception(primary);}
    }
    void close() override {
        opened_=false;available_=false;initialized_=false;eof_=false;sorted_=false;reuseKeys_=false;position_=0;
        input_.clear();layout_=Layout{};active_.clear();resultAliases_.clear();transitions_.clear();current_=RowContext{};
        currentKeys_.clear();keyComparisons_.reset();valueComparisons_.reset();
        std::vector<std::optional<uint32_t>>().swap(keyTypeOids_);
        if(childOpened_){childOpened_=false;child_->close();}
    }
    bool supportsPreparedContexts() const override {return true;}
    bool lastPreparedContext(RowContext& row) const override {if(!available_)return false;row=current_;return true;}
    std::vector<ExprValue> orderKeys(const std::vector<size_t>& indexes) const {
        if(!available_)throw DbError("XX000","ordered window lost its actual current key slots");
        std::vector<ExprValue> result;result.reserve(indexes.size());
        for(size_t index:indexes)result.push_back(currentKeys_.at(index));
        return result;
    }
    std::string preparedPlanNodeName() const override {return "WindowAgg";}
    std::vector<Operator*> preparedPlanChildren() const override {
        std::vector<Operator*> result{child_.get()};
        const auto append=[&](const Expr* expression) {if(!expression)return;auto children=state_->execution->childPlans(expression);result.insert(result.end(),children.begin(),children.end());};
        for(size_t index:stage_.producers) {const auto* call=state_->windows.at(index).call;for(const auto& arg:call->args)append(arg.get());append(call->filter.get());}
        for(const auto* key:stage_.spec.partition)append(key);
        for(const auto& key:stage_.spec.order)append(key.expression);
        append(stage_.spec.startOffset);append(stage_.spec.endOffset);return result;
    }
};

static OpPtr makePreparedWindowStages(OpPtr source,const std::shared_ptr<PreparedSelectState>& state) {
    auto input=std::make_unique<PreparedWindowInputOp>(std::move(source),state);
    auto* projection=input.get();source=std::move(input);
    PreparedWindowOp* last=nullptr;
    for(const auto& stage:state->windowStages) {
        auto window=std::make_unique<PreparedWindowOp>(std::move(source),state,stage,projection);
        last=window.get();source=std::move(window);
    }
    if(!state->windowOrderIndexes.empty()) {
        if(!last)throw DbError("XX000","window order proof lost its real producer");
        state->windowOrderKeys=[last,indexes=state->windowOrderIndexes]{return last->orderKeys(indexes);};
    }
    return source;
}

// WITH TIES over an already ordered WindowAgg reads its actual key cells.
// The boundary probe still consumes a real projection/window row.
class PreparedWindowTiesLimitOp final : public Operator {
    OpPtr child_;
    std::shared_ptr<PreparedSelectState> state_;
    SetOutputComparisons comparisons_;
    size_t limit_,emitted_=0;
    std::vector<ExprValue> boundary_;
    bool opened_=false,done_=false;
    bool peers(const std::vector<ExprValue>& left,const std::vector<ExprValue>& right) {
        for(size_t i=0;i<state_->keys.size();++i) {
            const auto& a=left.at(i);const auto& b=right.at(i);
            if(a.isNull || b.isNull){if(a.isNull!=b.isNull)return false;continue;}
            if(!comparisons_.equal(i,state_->evaluator,a,b,state_->keys[i].enumComparison))return false;
        }
        return true;
    }
public:
    PreparedWindowTiesLimitOp(OpPtr child,std::shared_ptr<PreparedSelectState> state,size_t limit)
        :child_(std::move(child)),state_(std::move(state)),limit_(limit) {}
    ~PreparedWindowTiesLimitOp() override {try{close();}catch(...) {}}
    bool open() override {
        OpenInstrument instrument(this);clearError();close();done_=!limit_;
        comparisons_.reset(state_->keys.size());
        if(done_)return true;
        try {
            opened_=true;
            if(!child_->open()) {
                propagateChildError(child_.get(),"ordered window ties input open failed");
                throw DbError("XX000",errorMessage());
            }
            return true;
        } catch(...) {const auto error=std::current_exception();try{close();}catch(...){}std::rethrow_exception(error);}
    }
    bool next(std::string& row) override {
        NextInstrument instrument(this);
        if(done_ || !opened_)return false;
        try {
            checkForQueryInterrupt();
            if(!child_->next(row)) {
                if(child_->hasError()) {
                    propagateChildError(child_.get(),"ordered window ties input failed");
                    throw DbError("XX000",errorMessage());
                }
                done_=true;return false;
            }
            if(!state_->windowOrderKeys)throw DbError("XX000","ordered window ties lost its actual keys");
            const auto keys=state_->windowOrderKeys();
            if(emitted_>=limit_ && !peers(keys,boundary_)){done_=true;return false;}
            if(++emitted_==limit_)boundary_=keys;
            instrument.emitted=true;return true;
        } catch(...) {const auto error=std::current_exception();try{close();}catch(...){}std::rethrow_exception(error);}
    }
    void close() override {
        done_=true;emitted_=0;boundary_.clear();comparisons_.reset();
        if(opened_){opened_=false;child_->close();}
    }
    bool supportsStructuredRows() const override {return child_->supportsStructuredRows();}
    bool lastStructuredValues(std::vector<ExprValue>& values) const override {return child_->lastStructuredValues(values);}
    bool lastStructuredRow(std::vector<std::string>& cells,std::vector<bool>& nulls) const override {return child_->lastStructuredRow(cells,nulls);}
    bool lastColumnIsNull(size_t ordinal) const override {return child_->lastColumnIsNull(ordinal);}
    bool supportsPreparedContexts() const override {return child_->supportsPreparedContexts();}
    bool lastPreparedContext(RowContext& row) const override {return child_->lastPreparedContext(row);}
    bool supportsPreparedOuterRow() const override {return child_->supportsPreparedOuterRow();}
    bool bindPreparedOuterRow(const RowContext& row) override {return child_->bindPreparedOuterRow(row);}
    std::string preparedPlanNodeName() const override {return "LimitWithTies";}
    std::vector<Operator*> preparedPlanChildren() const override {return {child_.get()};}
};

static OpPtr makePreparedWindowTiesLimit(OpPtr source,const std::shared_ptr<PreparedSelectState>& state,size_t limit) {
    return std::make_unique<PreparedWindowTiesLimitOp>(std::move(source),state,limit);
}
