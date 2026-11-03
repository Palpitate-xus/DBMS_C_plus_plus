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

    bool demanded() const {
        return std::any_of(stage_.producers.begin(),stage_.producers.end(),[&](size_t i) {
            return state_->execution->aggregateColumnDemanded(state_->windows.at(i).call);
        });
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
    }
    ~PreparedWindowOp() override {try{close();}catch(...) {}}
    bool open() override {
        OpenInstrument instrument(this);clearError();close();opened_=true;
        for(size_t index:stage_.producers)if(state_->execution->aggregateColumnDemanded(state_->windows.at(index).call))active_.push_back(index);
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
            // Plain window functions run before window aggregate transitions,
            // as in the actual WindowAgg executor. Every producer keeps its
            // own call site; volatile argument/default sites are never merged.
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
            for(size_t i=0;i<active_.size();++i) {
                const auto& producer=state_->windows.at(active_[i]);
                current_.setBoundColumn(producer.column.sourceOrdinal,producer.column.columnOrdinal,std::move(*values[i]));
            }
            ++position_;display.clear();available_=true;instrument.emitted=true;return true;
        } catch(...) {const auto primary=std::current_exception();try{close();}catch(...){}std::rethrow_exception(primary);}
    }
    void close() override {
        opened_=false;available_=false;initialized_=false;eof_=false;sorted_=false;reuseKeys_=false;position_=0;
        input_.clear();layout_=Layout{};active_.clear();transitions_.clear();current_=RowContext{};
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
