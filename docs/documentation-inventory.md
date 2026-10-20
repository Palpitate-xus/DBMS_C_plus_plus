# 全部文档检查清单

最新有限修复：[R183 STRING_TO_ARRAY回调身份](issue-review-r183-string-to-array-callback-identity.md)
已按实际callback提供数组元数据，opaque替换保持未知声明/原生身份，不再错误强转数组。
旧native60完整1/60失败，非60个独立bug；Root及accepted-only clean各21入口实际全0，
含新native60、原完整数组库、80/96/294/80/190/654及三个真实Main。
每侧75对象17当前程序/127头核对；fresh1生产2native，57生产11native/stub/3Main为匹配donor，
全部新链接当前库；Root normal1CPP并链接、repeat零编译零链接，clean显式编译。
2204已接受源码投影与tested clean相同，Root2323保留外来改动及2322源码/118缓存恢复副本。
跟踪603份MD、920native+567注册Python+3Main=1490入口；Root606份MD、926+573+3=1502，
原12未接受入口分离；[R182拆分性能](issue-review-r182-utf8-split-linear.md)仍未接受，V1失败原样保留，需新版本继续。
R164仍未解决，R180及其他暂缓不恢复；原273仍22/166/70/15，总目标未完成。
不是全1490/完整72/TLS/整库sanitizer验收；不push，Actions禁用。以下记录按当时状态解释。

最新开放：[R182共享UTF-8拆分](issue-review-r182-utf8-split-linear.md)已完成PG329与旧版329全0对照，
候选已编译但未验收；首轮原生检查因错误ARRAY类型期望及真实opaque回调数组强转异常而中止，完整失败保留。
[R183 STRING_TO_ARRAY回调身份](issue-review-r183-string-to-array-callback-identity.md)单列真实缺陷，
先独立修复实际callee元数据，再恢复R182严格验证；不删除失败检查、不将候选计作已修复。
跟踪603份MD、919native+567注册Python+3Main=1489入口；Root606份MD、925+573+3=1501，
新增文档不等于新增接受测试，原12未接受入口仍分离。R164未解决，R180等暂缓不恢复。
原273仍22/166/70/15，总目标未完成；不push，Actions禁用。以下记录按当时状态解释。

最新有限修复：[R181 REVERSE线性扫描](issue-review-r181-reverse-linear.md)消除长文本反转的平方级前缀扫描，
保留UTF-8码点、TEXT/CHAR、NULL、BYTEA及实参求值语义；这是性能修复，不是旧版兼容性错值。
PG完整96、旧版完整96及native80语义全0；Root及accepted-only clean各19入口实际全0，
包含新80/96、原294/80/190/654及三个新链接当前库的真实Main，clean本轮fresh3、Root验证复用3。
每侧73对象15程序/127头已核对；fresh1生产/2native，57生产/9native/stub为已验证donor。
Root normal1CPP并链接、repeat零编译零链接；clean显式编译，未冒称normal/repeat或donor fresh。
全部2203已接受源码投影与tested clean一致；Root2322保留其他改动及2320源码/118缓存恢复副本。
跟踪601份MD、919native+567注册Python+3Main=1489入口；工作区604份MD、925+573+3=1501，
原12未接受入口和三份文档仍分离；不是1489全量、完整72、TLS或整库sanitizer验收。
R180按最新skip it暂缓，其他暂缓不恢复；R164超时仍未解决。原273仍22/166/70/15，总目标未完成。
不push，Actions禁用。以下记录中的状态和数量均指记录当时。

最新用户暂缓：[R180 POSITION语法/参数约定](issue-review-r180-position-call-identity.md)按最新“skip it”停止实现。
两侧各24次观察及修改方法继续保留；没有生产修改、注册测试或修复通过声明，普通continue不恢复本项。
下一项独立审查为字符串REVERSE的重复UTF-8前缀扫描；目前仅源码观察，尚未验收修复。
已接受仍为下述R179有限修复；R164及总273继续未完成，其他用户暂缓不恢复。不push，Actions禁用。

最新有限修复：[R179 STRPOS声明](issue-review-r179-strpos-signatures.md)已完成真实TEXT/TEXT签名、
错误阶段/空输入/参数推断/同名重载及opaque native覆盖修复。PG294全0；旧294有140断言差异、
旧native76有49失败，均完整记录，非140/49个独立缺陷。Root及accepted-only clean各16入口实际全0，
含native76、完整294/80/190/654和三个本轮fresh编译、新链接当前库的真实Main。
每侧71对象13程序/127头独立核对；Root normal3CPP并链接、repeat零编译/零链接，clean显式3CPP加55已核对donor，
未冒称clean normal/repeat或donor fresh；全部2201已接受源码投影与tested clean一致，Root2320保留原有其他改动。
跟踪600份MD、918native+566注册Python+3Main=1487入口；工作区603份MD、924+572+3=1499，
原12未接受入口及三份文档仍分离。[R180 POSITION](issue-review-r180-position-call-identity.md)仍开放，不用本项关闭。
这不是1487全量/完整72/TLS/整库sanitizer验收；R164两次超时仍未解决，R177等用户暂缓不恢复。
原273仍22 complete/166 partial/70 unverified/15 deferred，总目标未完成；不push，Actions禁用。

最新有限修复：[R178空字符串REPEAT](issue-review-r178-repeat-empty-input.md)在参数校验后直接返回，
避免按次数进行空追加；不是兼容性错值修复，PG也有相似旧耗时。PG完整80项通过，
工作区及accepted-only clean各11入口（8native/3协议）实际全0，包含80/190/654完整协议。
原生增加4个空输入边界控制；首轮8通过/3启动失败及数据目录驱动修正记录保留。
跟踪598份MD、917native+565注册Python+3Main=1485入口；工作区601份MD、923+571+3=1497，
原12个未接受入口及三份文档仍分开保留；这不是1485全量、完整72、TLS或整库sanitizer验收。
R177按用户要求暂缓，不恢复R173/R156等分支；R164超时仍未解决，非新增暂缓。
原273仍22 complete /166 partial /70 unverified /15 deferred，总目标未完成；不push，Actions禁用。

## 历史记录（以下状态与数量均指记录当时）

## 当前版本：R176有限排序修复，完整矩阵仍有失败

[R176](issue-review-r176-ordinary-aggregate-order-keys.md)的有限源码修复已本地提交`a35658c0`：
保留全局SUM/COUNT真实ORDER键，纯绑定及错误阶段、FILTER/NULL需求、缓存typed排序、
共享参数/重复键和方向身份有native61及完整协议190回归，两侧专项均0。
完整PG190对照0；旧版本190次完整观察59通过131失败，不把这些重复模式称为131个独立缺陷。
工作区完整72入口实际全0；干净版两次完整72均外层1，各71项通过，均在R164入口socket超时，
无完整956成功footer，清理出现延迟响应错位。超时根因未确定，也未证明与R176无关；
保留全部失败，不提高超时、不改持久化、不移除断言，不宣称双侧完整矩阵通过。
每侧35native/34协议/3真实Main顺序及原70断言保留；每侧97对象/39程序及源文件独立核对。
三个Main采用accepted测试体的路径投影及显式私有数据目录，不夹带工作区session测试改动。
Root fresh58、normal wrapper复用当前58链接、repeat零编译/零链接；clean fresh2+56当前header
donor源自独立fresh58 V3，未伪造clean常规build/repeat。提交的2198源文件与tested clean逐blob一致。
跟踪597份MD、917native+564注册Python+3Main=1484入口；工作区600份MD、923+570+3=1496，
原12个未纳入版本的测试入口和三份文档分离保留。这不是1484全量、TLS或整库sanitizer通过声明。
R176的GROUP BY排序、更多类型/算子/参数/query owner边界仍开放；R164超时仍普通未解决，非用户暂缓。
原273仍22 complete /166 partial /70 unverified /15 deferred，总目标未完成。
R173、R156及其他用户暂缓分支不恢复；不push，Actions禁用。

## 历史记录（以下状态与数量均指记录当时）

## 当前接受状态：R170/R171/R172/R174/R175

五项有限修复已分别本地提交，版本对应见[审查索引](review-index.md)。
最终组合在工作区及accepted-only clean各完整70入口（34native、33wire、3真实Main）实际全0；
完整791/314/331/906及原956均0，已接受源2195文件与实际测试clean逐字节对应。
首次Root70实际1的清理超时仍保留、原因未确定，复测0不等于超时根因已修；
源码、58生产对象、server及每端96对象/38程序均独立核对，详见[R170最终证据](issue-review-r170-quoted-unqualified-scalar-projection.md)。
已接受596MD、916native+563已注册Python+3Main=1482；工作区599MD、922+569+3=1494，
原12个未接受入口和三份文档保持分离。这不是1482全量、TLS/整库sanitizer或各中间提交独立全矩阵。
原273仍22 complete /166 partial /70 unverified /15 deferred，总目标未完成。
R173函数签名类型依赖及R176真实聚合排序仍独立开放；R156等用户暂缓模块不恢复，不push，Actions禁用。

## 历史记录（以下状态与数量均指记录当时）

审查输入和发现见[代码审查报告](code-review-2026-10-13.md)。基准提交 `a502ffa3` 包含 412 份已跟踪 Markdown；首次审查含新增报告共 417 份。当前本轮逐文件复查 603 份 Markdown，下表覆盖全部；历史数量不改写。

检查逐文件读取 UTF-8 内容并验证 Markdown 内联链接的本地文件目标是否存在。它不验证外部网址、锚点、所有引用式链接，也不证明文中每个历史技术结论在当前版本成立。
使用手册、部署、命令参考、模块 README 和状态入口另作人工一致性核查；`issue-*.md` 保留原始提交和运行证据，其结论须按对应输入读取。

历史 TODO 的两处失效档案链接已修正。当前本地文件链接检查为 通过。

| 文档 | 分类 | 本地文件链接 |
| --- | --- | --- |
| [CHANGELOG.md](../CHANGELOG.md) | 使用/开发/状态文档 | 通过 |
| [README.md](../README.md) | 使用/开发/状态文档 | 通过 |
| [RELEASE-NOTES.md](../RELEASE-NOTES.md) | 使用/开发/状态文档 | 通过 |
| [docs/MANUAL.md](MANUAL.md) | 使用/开发/状态文档 | 通过 |
| [docs/PACKAGING.md](PACKAGING.md) | 使用/开发/状态文档 | 通过 |
| [docs/README.md](README.md) | 使用/开发/状态文档 | 通过 |
| [docs/all-gaps-todo.md](all-gaps-todo.md) | 使用/开发/状态文档 | 通过 |
| [docs/archive/phase4-plan.md](archive/phase4-plan.md) | 历史档案 | 通过 |
| [docs/archive/postgresql-complete-gap-analysis.md](archive/postgresql-complete-gap-analysis.md) | 历史档案 | 通过 |
| [docs/code-review-2026-10-13.md](code-review-2026-10-13.md) | 使用/开发/状态文档 | 通过 |
| [docs/code-review-progress.md](code-review-progress.md) | 使用/开发/状态文档 | 通过 |
| [docs/commandsList.md](commandsList.md) | 使用/开发/状态文档 | 通过 |
| [docs/compatibility-contract.md](compatibility-contract.md) | 使用/开发/状态文档 | 通过 |
| [docs/deferred-scope.md](deferred-scope.md) | 使用/开发/状态文档 | 通过 |
| [docs/documentation-inventory.md](documentation-inventory.md) | 使用/开发/状态文档 | 通过 |
| [docs/feature-gaps.md](feature-gaps.md) | 使用/开发/状态文档 | 通过 |
| [docs/full-gap-execution-plan.md](full-gap-execution-plan.md) | 使用/开发/状态文档 | 通过 |
| [docs/implementation-plan.md](implementation-plan.md) | 使用/开发/状态文档 | 通过 |
| [docs/issue-958-temp-prepared-access.md](issue-958-temp-prepared-access.md) | 历史问题证据 | 通过 |
| [docs/issue-959-parenthesized-set-operations.md](issue-959-parenthesized-set-operations.md) | 历史问题证据 | 通过 |
| [docs/issue-960-set-operation-multi-key-order.md](issue-960-set-operation-multi-key-order.md) | 历史问题证据 | 通过 |
| [docs/issue-961-ambiguous-set-order-name.md](issue-961-ambiguous-set-order-name.md) | 历史问题证据 | 通过 |
| [docs/issue-962-fetch-ties-order-error.md](issue-962-fetch-ties-order-error.md) | 历史问题证据 | 通过 |
| [docs/issue-963-set-fetch-with-ties.md](issue-963-set-fetch-with-ties.md) | 历史问题证据 | 通过 |
| [docs/issue-964-ordinary-fetch-with-ties.md](issue-964-ordinary-fetch-with-ties.md) | 历史问题证据 | 通过 |
| [docs/issue-965-fetch-ties-column-equality.md](issue-965-fetch-ties-column-equality.md) | 历史问题证据 | 通过 |
| [docs/issue-966-fetch-signed-counts.md](issue-966-fetch-signed-counts.md) | 历史问题证据 | 通过 |
| [docs/issue-967-tochar-currency-fixture-locale.md](issue-967-tochar-currency-fixture-locale.md) | 历史问题证据 | 通过 |
| [docs/issue-968-hash-bloom-sidecar-generations.md](issue-968-hash-bloom-sidecar-generations.md) | 历史问题证据 | 通过 |
| [docs/issue-969-reindex-closed-corrupt-index.md](issue-969-reindex-closed-corrupt-index.md) | 历史问题证据 | 通过 |
| [docs/issue-971-composite-not-in-null-semantics.md](issue-971-composite-not-in-null-semantics.md) | 历史问题证据 | 通过 |
| [docs/issue-972-order-by-null-semantics-after-subquery.md](issue-972-order-by-null-semantics-after-subquery.md) | 历史问题证据 | 通过 |
| [docs/issue-973-semi-join-empty-text-null.md](issue-973-semi-join-empty-text-null.md) | 历史问题证据 | 通过 |
| [docs/issue-974-xid-tuple-width-guard.md](issue-974-xid-tuple-width-guard.md) | 历史问题证据 | 通过 |
| [docs/issue-975-range-partition-bounds.md](issue-975-range-partition-bounds.md) | 历史问题证据 | 通过 |
| [docs/issue-976-range-attach-default-validation.md](issue-976-range-attach-default-validation.md) | 历史问题证据 | 通过 |
| [docs/issue-977-attach-missing-partition-relation.md](issue-977-attach-missing-partition-relation.md) | 历史问题证据 | 通过 |
| [docs/issue-978-attach-relation-storage-guard.md](issue-978-attach-relation-storage-guard.md) | 历史问题证据 | 通过 |
| [docs/issue-979-relative-data-directory.md](issue-979-relative-data-directory.md) | 历史问题证据 | 通过 |
| [docs/issue-980-ssi-empty-index-predicate-granularity.md](issue-980-ssi-empty-index-predicate-granularity.md) | 历史问题证据 | 通过 |
| [docs/issue-981-correlated-subquery-execution-scope.md](issue-981-correlated-subquery-execution-scope.md) | 历史问题证据 | 通过 |
| [docs/issue-982-user-operator-gate-audit.md](issue-982-user-operator-gate-audit.md) | 历史问题证据 | 通过 |
| [docs/issue-983-rule-event-trigger-gate-audit.md](issue-983-rule-event-trigger-gate-audit.md) | 历史问题证据 | 通过 |
| [docs/issue-984-pg-catalog-error-state.md](issue-984-pg-catalog-error-state.md) | 历史问题证据 | 通过 |
| [docs/issue-985-pg-catalog-fail-closed.md](issue-985-pg-catalog-fail-closed.md) | 历史问题证据 | 通过 |
| [docs/issue-986-query-rewrite-capability-audit.md](issue-986-query-rewrite-capability-audit.md) | 历史问题证据 | 通过 |
| [docs/issue-987-pg-database-typed-subset.md](issue-987-pg-database-typed-subset.md) | 历史问题证据 | 通过 |
| [docs/issue-988-pg-database-count-aggregate.md](issue-988-pg-database-count-aggregate.md) | 历史问题证据 | 通过 |
| [docs/issue-989-pg-stats-shape-fail-closed.md](issue-989-pg-stats-shape-fail-closed.md) | 历史问题证据 | 通过 |
| [docs/issue-990-pg-namespace-shape-fail-closed.md](issue-990-pg-namespace-shape-fail-closed.md) | 历史问题证据 | 通过 |
| [docs/issue-991-pg-class-incomplete-projection.md](issue-991-pg-class-incomplete-projection.md) | 历史问题证据 | 通过 |
| [docs/issue-992-pg-type-enum-query-fail-closed.md](issue-992-pg-type-enum-query-fail-closed.md) | 历史问题证据 | 通过 |
| [docs/issue-993-pg-roles-view-fail-closed.md](issue-993-pg-roles-view-fail-closed.md) | 历史问题证据 | 通过 |
| [docs/issue-994-pg-settings-typed-subset.md](issue-994-pg-settings-typed-subset.md) | 历史问题证据 | 通过 |
| [docs/issue-995-pg-stat-activity-privacy.md](issue-995-pg-stat-activity-privacy.md) | 历史问题证据 | 通过 |
| [docs/issue-996-catalog-not-mvcc-audit.md](issue-996-catalog-not-mvcc-audit.md) | 历史问题证据 | 通过 |
| [docs/issue-997-statement-timeout-cooperative-cancel.md](issue-997-statement-timeout-cooperative-cancel.md) | 历史问题证据 | 通过 |
| [docs/issue-add-column-clean-validation-rollback.md](issue-add-column-clean-validation-rollback.md) | 历史问题证据 | 通过 |
| [docs/issue-aggregate-argument-expression-dispatch.md](issue-aggregate-argument-expression-dispatch.md) | 历史问题证据 | 通过 |
| [docs/issue-aggregate-order-role.md](issue-aggregate-order-role.md) | 历史问题证据 | 通过 |
| [docs/issue-aggregate-result-root-owner.md](issue-aggregate-result-root-owner.md) | 历史问题证据 | 通过 |
| [docs/issue-alter-array-envelope-retention.md](issue-alter-array-envelope-retention.md) | 历史问题证据 | 通过 |
| [docs/issue-alter-array-type-envelope.md](issue-alter-array-type-envelope.md) | 历史问题证据 | 通过 |
| [docs/issue-alter-array-unique-rewrite-sqlstate.md](issue-alter-array-unique-rewrite-sqlstate.md) | 历史问题证据 | 通过 |
| [docs/issue-alter-default-source-span.md](issue-alter-default-source-span.md) | 历史问题证据 | 通过 |
| [docs/issue-alter-sequence-storage-sqlstate.md](issue-alter-sequence-storage-sqlstate.md) | 历史问题证据 | 通过 |
| [docs/issue-api-scalar-decoded-literal.md](issue-api-scalar-decoded-literal.md) | 历史问题证据 | 通过 |
| [docs/issue-append-canonical-cell-type.md](issue-append-canonical-cell-type.md) | 历史问题证据 | 通过 |
| [docs/issue-arithmetic-structured-error.md](issue-arithmetic-structured-error.md) | 历史问题证据 | 通过 |
| [docs/issue-array-concat-typed-execution.md](issue-array-concat-typed-execution.md) | 历史问题证据 | 通过 |
| [docs/issue-array-element-typmod-catalog.md](issue-array-element-typmod-catalog.md) | 历史问题证据 | 通过 |
| [docs/issue-array-nested-brackets.md](issue-array-nested-brackets.md) | 历史问题证据 | 通过 |
| [docs/issue-array-subscript-native-oracle.md](issue-array-subscript-native-oracle.md) | 历史问题证据 | 通过 |
| [docs/issue-backend-session-srf-provider.md](issue-backend-session-srf-provider.md) | 历史问题证据 | 通过 |
| [docs/issue-between-predicate-boolean-type.md](issue-between-predicate-boolean-type.md) | 历史问题证据 | 通过 |
| [docs/issue-between-result-type.md](issue-between-result-type.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-array-constructor-typmod.md](issue-bit-array-constructor-typmod.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-between-current-root-composition.md](issue-bit-between-current-root-composition.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-between-demand.md](issue-bit-between-demand.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-between-input-preparation.md](issue-bit-between-input-preparation.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-compact-list-admission.md](issue-bit-compact-list-admission.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-comparison-value-identity.md](issue-bit-comparison-value-identity.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-current-root-composition.md](issue-bit-current-root-composition.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-literal-operand-type.md](issue-bit-literal-operand-type.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-null-current-root-composition.md](issue-bit-null-current-root-composition.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-predicate-pre-scan-type.md](issue-bit-predicate-pre-scan-type.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-predicate-routine-owner.md](issue-bit-predicate-routine-owner.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-reverse-comparison-owner.md](issue-bit-reverse-comparison-owner.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-scalar-parameter-source.md](issue-bit-scalar-parameter-source.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-stored-literal-adapter.md](issue-bit-stored-literal-adapter.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-text-input-codec.md](issue-bit-text-input-codec.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-unknown-comparison-typmod.md](issue-bit-unknown-comparison-typmod.md) | 历史问题证据 | 通过 |
| [docs/issue-bit-unknown-mixed-list-coercion.md](issue-bit-unknown-mixed-list-coercion.md) | 历史问题证据 | 通过 |
| [docs/issue-boolean-character-cast.md](issue-boolean-character-cast.md) | 历史问题证据 | 通过 |
| [docs/issue-bound-dml-paired-query-carrier.md](issue-bound-dml-paired-query-carrier.md) | 历史问题证据 | 通过 |
| [docs/issue-bpchar-array-type-alias.md](issue-bpchar-array-type-alias.md) | 历史问题证据 | 通过 |
| [docs/issue-btree-checked-fixture-contract.md](issue-btree-checked-fixture-contract.md) | 历史问题证据 | 通过 |
| [docs/issue-c-utf8-pattern-alias.md](issue-c-utf8-pattern-alias.md) | 历史问题证据 | 通过 |
| [docs/issue-canonical-source-range-scope.md](issue-canonical-source-range-scope.md) | 历史问题证据 | 通过 |
| [docs/issue-case-common-types-and-insert-consumers.md](issue-case-common-types-and-insert-consumers.md) | 历史问题证据 | 通过 |
| [docs/issue-case-else-projection-labels.md](issue-case-else-projection-labels.md) | 历史问题证据 | 通过 |
| [docs/issue-case-integer-literal-width.md](issue-case-integer-literal-width.md) | 历史问题证据 | 通过 |
| [docs/issue-cast-declared-type-envelope.md](issue-cast-declared-type-envelope.md) | 历史问题证据 | 通过 |
| [docs/issue-cast-pattern-operator-boundary.md](issue-cast-pattern-operator-boundary.md) | 历史问题证据 | 通过 |
| [docs/issue-cast-structured-error.md](issue-cast-structured-error.md) | 历史问题证据 | 通过 |
| [docs/issue-catalog-clean-publication.md](issue-catalog-clean-publication.md) | 历史问题证据 | 通过 |
| [docs/issue-catalog-public-bootstrap.md](issue-catalog-public-bootstrap.md) | 历史问题证据 | 通过 |
| [docs/issue-catalog-scalar-followup.md](issue-catalog-scalar-followup.md) | 历史问题证据 | 通过 |
| [docs/issue-checked-plan-structured-sqlstate.md](issue-checked-plan-structured-sqlstate.md) | 历史问题证据 | 通过 |
| [docs/issue-clean-retired-heap-backup.md](issue-clean-retired-heap-backup.md) | 历史问题证据 | 通过 |
| [docs/issue-cold-routine-creation-namespace.md](issue-cold-routine-creation-namespace.md) | 历史问题证据 | 通过 |
| [docs/issue-cold-start-transaction-lock-registry.md](issue-cold-start-transaction-lock-registry.md) | 历史问题证据 | 通过 |
| [docs/issue-compact-quoted-comparison-column.md](issue-compact-quoted-comparison-column.md) | 历史问题证据 | 通过 |
| [docs/issue-conflict-target-fixture.md](issue-conflict-target-fixture.md) | 历史问题证据 | 通过 |
| [docs/issue-conflict-unique-index-namespace-fixture.md](issue-conflict-unique-index-namespace-fixture.md) | 历史问题证据 | 通过 |
| [docs/issue-conflict-where-ambiguous-fixture.md](issue-conflict-where-ambiguous-fixture.md) | 历史问题证据 | 通过 |
| [docs/issue-correlated-integer-parameter-coverage.md](issue-correlated-integer-parameter-coverage.md) | 历史问题证据 | 通过 |
| [docs/issue-correlated-typed-child-carrier.md](issue-correlated-typed-child-carrier.md) | 历史问题证据 | 通过 |
| [docs/issue-cross-process-transaction-id-state.md](issue-cross-process-transaction-id-state.md) | 历史问题证据 | 通过 |
| [docs/issue-cte-duplicate-name-preflight.md](issue-cte-duplicate-name-preflight.md) | 历史问题证据 | 通过 |
| [docs/issue-cte-inherited-preparation-owner.md](issue-cte-inherited-preparation-owner.md) | 历史问题证据 | 通过 |
| [docs/issue-cte-relation-scope-and-lexical-boundaries.md](issue-cte-relation-scope-and-lexical-boundaries.md) | 历史问题证据 | 通过 |
| [docs/issue-date-native-intrinsic-fixture.md](issue-date-native-intrinsic-fixture.md) | 历史问题证据 | 通过 |
| [docs/issue-ddl-catalog-wal-test-cursor.md](issue-ddl-catalog-wal-test-cursor.md) | 历史问题证据 | 通过 |
| [docs/issue-declared-builtin-null-bind.md](issue-declared-builtin-null-bind.md) | 历史问题证据 | 通过 |
| [docs/issue-declared-type-boundaries.md](issue-declared-type-boundaries.md) | 历史问题证据 | 通过 |
| [docs/issue-delete-boolean-returning-descriptor.md](issue-delete-boolean-returning-descriptor.md) | 历史问题证据 | 通过 |
| [docs/issue-delete-bound-returning-consumer.md](issue-delete-bound-returning-consumer.md) | 历史问题证据 | 通过 |
| [docs/issue-delete-exception-owner.md](issue-delete-exception-owner.md) | 历史问题证据 | 通过 |
| [docs/issue-delete-where-boolean-context.md](issue-delete-where-boolean-context.md) | 历史问题证据 | 通过 |
| [docs/issue-derived-map-clean-peer-publication.md](issue-derived-map-clean-peer-publication.md) | 历史问题证据 | 通过 |
| [docs/issue-derived-map-close-pending.md](issue-derived-map-close-pending.md) | 历史问题证据 | 通过 |
| [docs/issue-derived-map-peer-coherence.md](issue-derived-map-peer-coherence.md) | 历史问题证据 | 通过 |
| [docs/issue-derived-map-snapshot-ownership.md](issue-derived-map-snapshot-ownership.md) | 历史问题证据 | 通过 |
| [docs/issue-distinct-equality-dispatch.md](issue-distinct-equality-dispatch.md) | 历史问题证据 | 通过 |
| [docs/issue-dml-03-missing-source-parser.md](issue-dml-03-missing-source-parser.md) | 历史问题证据 | 通过 |
| [docs/issue-dml-comma-source-grammar.md](issue-dml-comma-source-grammar.md) | 历史问题证据 | 通过 |
| [docs/issue-dml-view-target-namespace.md](issue-dml-view-target-namespace.md) | 历史问题证据 | 通过 |
| [docs/issue-domain-ancestry-foundation.md](issue-domain-ancestry-foundation.md) | 历史问题证据 | 通过 |
| [docs/issue-domain-ddl-io-error-result.md](issue-domain-ddl-io-error-result.md) | 历史问题证据 | 通过 |
| [docs/issue-domain-declaration-native-oracle.md](issue-domain-declaration-native-oracle.md) | 历史问题证据 | 通过 |
| [docs/issue-domain-default-native-oracle.md](issue-domain-default-native-oracle.md) | 历史问题证据 | 通过 |
| [docs/issue-domain-default-origin.md](issue-domain-default-origin.md) | 历史问题证据 | 通过 |
| [docs/issue-domain-default-snapshot.md](issue-domain-default-snapshot.md) | 历史问题证据 | 通过 |
| [docs/issue-domain-foreign-key-base.md](issue-domain-foreign-key-base.md) | 历史问题证据 | 通过 |
| [docs/issue-domain-namespace-facts.md](issue-domain-namespace-facts.md) | 历史问题证据 | 通过 |
| [docs/issue-drop-routine-qualified-name.md](issue-drop-routine-qualified-name.md) | 历史问题证据 | 通过 |
| [docs/issue-engine-owner-receiver-numeric-followups.md](issue-engine-owner-receiver-numeric-followups.md) | 历史问题证据 | 通过 |
| [docs/issue-enum-aggregate-argument-binding.md](issue-enum-aggregate-argument-binding.md) | 历史问题证据 | 通过 |
| [docs/issue-enum-comparison-binding.md](issue-enum-comparison-binding.md) | 历史问题证据 | 通过 |
| [docs/issue-enum-current-candidate-hold.md](issue-enum-current-candidate-hold.md) | 历史问题证据 | 通过 |
| [docs/issue-enum-empty-hash-key.md](issue-enum-empty-hash-key.md) | 历史问题证据 | 通过 |
| [docs/issue-enum-empty-hash-null-scan.md](issue-enum-empty-hash-null-scan.md) | 历史问题证据 | 通过 |
| [docs/issue-enum-empty-label-schema.md](issue-enum-empty-label-schema.md) | 历史问题证据 | 通过 |
| [docs/issue-enum-quoted-type-identity.md](issue-enum-quoted-type-identity.md) | 历史问题证据 | 通过 |
| [docs/issue-explain-prepared-descriptor.md](issue-explain-prepared-descriptor.md) | 历史问题证据 | 通过 |
| [docs/issue-explain-primary-error-cleanup.md](issue-explain-primary-error-cleanup.md) | 历史问题证据 | 通过 |
| [docs/issue-explain-read-root-constant-planning.md](issue-explain-read-root-constant-planning.md) | 历史问题证据 | 通过 |
| [docs/issue-explain-typed-execution.md](issue-explain-typed-execution.md) | 历史问题证据 | 通过 |
| [docs/issue-explicit-array-bounds.md](issue-explicit-array-bounds.md) | 历史问题证据 | 通过 |
| [docs/issue-extended-literal-transaction-demand.md](issue-extended-literal-transaction-demand.md) | 历史问题证据 | 通过 |
| [docs/issue-fromless-projection-and-planning-demand.md](issue-fromless-projection-and-planning-demand.md) | 历史问题证据 | 通过 |
| [docs/issue-full-registered-18d85f4e.md](issue-full-registered-18d85f4e.md) | 历史问题证据 | 通过 |
| [docs/issue-full-registered-27bac6ee.md](issue-full-registered-27bac6ee.md) | 完整回归与后续方法 | 通过 |
| [docs/issue-full-registered-332bcff4.md](issue-full-registered-332bcff4.md) | 完整回归与后续方法 | 通过 |
| [docs/issue-full-registered-5091b39c.md](issue-full-registered-5091b39c.md) | 完整回归与后续方法 | 通过 |
| [docs/issue-full-registered-8f0465fc.md](issue-full-registered-8f0465fc.md) | 完整回归与后续方法 | 通过 |
| [docs/issue-full-registered-ecf632e9.md](issue-full-registered-ecf632e9.md) | 完整回归与后续方法 | 通过 |
| [docs/issue-full-registered-795479c6.md](issue-full-registered-795479c6.md) | 历史问题证据 | 通过 |
| [docs/issue-full-registered-a85e653e.md](issue-full-registered-a85e653e.md) | 历史问题证据 | 通过 |
| [docs/issue-full-registered-b2b376a3.md](issue-full-registered-b2b376a3.md) | 历史问题证据 | 通过 |
| [docs/issue-full-registered-b5def9ba.md](issue-full-registered-b5def9ba.md) | 完整回归与后续方法 | 通过 |
| [docs/issue-full-registered-canonical-1e0a8c5a.md](issue-full-registered-canonical-1e0a8c5a.md) | 历史问题证据 | 通过 |
| [docs/issue-full-registered-canonical-b353fe8b.md](issue-full-registered-canonical-b353fe8b.md) | 历史问题证据 | 通过 |
| [docs/issue-full-registered-d974c80d.md](issue-full-registered-d974c80d.md) | 完整回归与后续方法 | 通过 |
| [docs/issue-full-registered-dc89dfbb.md](issue-full-registered-dc89dfbb.md) | 历史问题证据 | 通过 |
| [docs/issue-full-registered-f048f155.md](issue-full-registered-f048f155.md) | 历史问题证据 | 通过 |
| [docs/issue-full-registered-f4087f5d.md](issue-full-registered-f4087f5d.md) | 历史问题证据 | 通过 |
| [docs/issue-function-drop-namespace.md](issue-function-drop-namespace.md) | 历史问题证据 | 通过 |
| [docs/issue-function-procedure-canonical-type-oracle.md](issue-function-procedure-canonical-type-oracle.md) | 历史问题证据 | 通过 |
| [docs/issue-generate-series-cursor.md](issue-generate-series-cursor.md) | 历史问题证据 | 通过 |
| [docs/issue-geometric-cast-input.md](issue-geometric-cast-input.md) | 历史问题证据 | 通过 |
| [docs/issue-geometric-equality-values.md](issue-geometric-equality-values.md) | 历史问题证据 | 通过 |
| [docs/issue-geometric-qualified-result-type.md](issue-geometric-qualified-result-type.md) | 历史问题证据 | 通过 |
| [docs/issue-geometric-sidecar-test-expectation.md](issue-geometric-sidecar-test-expectation.md) | 历史问题证据 | 通过 |
| [docs/issue-geometric-typed-literals.md](issue-geometric-typed-literals.md) | 历史问题证据 | 通过 |
| [docs/issue-having-and-legacy-routine-arguments.md](issue-having-and-legacy-routine-arguments.md) | 历史问题证据 | 通过 |
| [docs/issue-having-grammar-and-parser-lifetime.md](issue-having-grammar-and-parser-lifetime.md) | 历史问题证据 | 通过 |
| [docs/issue-heap-wal-retirement-completion.md](issue-heap-wal-retirement-completion.md) | 历史问题证据 | 通过 |
| [docs/issue-heap-wal-table-generation.md](issue-heap-wal-table-generation.md) | 历史问题证据 | 通过 |
| [docs/issue-index-residual-execution-once.md](issue-index-residual-execution-once.md) | 历史问题证据 | 通过 |
| [docs/issue-insert-conflict-binding.md](issue-insert-conflict-binding.md) | 历史问题证据 | 通过 |
| [docs/issue-insert-default-planning.md](issue-insert-default-planning.md) | 历史问题证据 | 通过 |
| [docs/issue-insert-exception-transaction-owner.md](issue-insert-exception-transaction-owner.md) | 历史问题证据 | 通过 |
| [docs/issue-insert-omitted-null.md](issue-insert-omitted-null.md) | 历史问题证据 | 通过 |
| [docs/issue-insert-width-input-priority.md](issue-insert-width-input-priority.md) | 历史问题证据 | 通过 |
| [docs/issue-integer-between-input.md](issue-integer-between-input.md) | 历史问题证据 | 通过 |
| [docs/issue-integer-current-root-composition.md](issue-integer-current-root-composition.md) | 历史问题证据 | 通过 |
| [docs/issue-integer-current-root-held-verification.md](issue-integer-current-root-held-verification.md) | 历史问题证据 | 通过 |
| [docs/issue-integer-overflow-test-expectation.md](issue-integer-overflow-test-expectation.md) | 历史问题证据 | 通过 |
| [docs/issue-integer-reverse-physical-comparison.md](issue-integer-reverse-physical-comparison.md) | 历史问题证据 | 通过 |
| [docs/issue-integer-unknown-comparison-preparation.md](issue-integer-unknown-comparison-preparation.md) | 历史问题证据 | 通过 |
| [docs/issue-interval-arithmetic-range.md](issue-interval-arithmetic-range.md) | 历史问题证据 | 通过 |
| [docs/issue-interval-column-modifier.md](issue-interval-column-modifier.md) | 历史问题证据 | 通过 |
| [docs/issue-interval-comparison-value-codec.md](issue-interval-comparison-value-codec.md) | 历史问题证据 | 通过 |
| [docs/issue-interval-field-grammar.md](issue-interval-field-grammar.md) | 历史问题证据 | 通过 |
| [docs/issue-interval-insert-select-input.md](issue-interval-insert-select-input.md) | 历史问题证据 | 通过 |
| [docs/issue-interval-literal-and-column-modifiers.md](issue-interval-literal-and-column-modifiers.md) | 历史问题证据 | 通过 |
| [docs/issue-interval-mixed-sign-output.md](issue-interval-mixed-sign-output.md) | 历史问题证据 | 通过 |
| [docs/issue-join-range-identity.md](issue-join-range-identity.md) | 历史问题证据 | 通过 |
| [docs/issue-join-using-common-type.md](issue-join-using-common-type.md) | 历史问题证据 | 通过 |
| [docs/issue-json-xml-catalog-identities.md](issue-json-xml-catalog-identities.md) | 历史问题证据 | 通过 |
| [docs/issue-legacy-scalar-cardinality-sqlstate.md](issue-legacy-scalar-cardinality-sqlstate.md) | 历史问题证据 | 通过 |
| [docs/issue-legacy-scalar-static-child-descriptor.md](issue-legacy-scalar-static-child-descriptor.md) | 历史问题证据 | 通过 |
| [docs/issue-like-trailing-escape-native-expectation.md](issue-like-trailing-escape-native-expectation.md) | 历史问题证据 | 通过 |
| [docs/issue-literal-in-frontend.md](issue-literal-in-frontend.md) | 历史问题证据 | 通过 |
| [docs/issue-literal-query-owner-demand.md](issue-literal-query-owner-demand.md) | 历史问题证据 | 通过 |
| [docs/issue-long-default-parser-fixture.md](issue-long-default-parser-fixture.md) | 历史问题证据 | 通过 |
| [docs/issue-materialized-query-composition.md](issue-materialized-query-composition.md) | 历史问题证据 | 通过 |
| [docs/issue-materialized-view-dml-target.md](issue-materialized-view-dml-target.md) | 历史问题证据 | 通过 |
| [docs/issue-memory-index-checked-fixture-contract.md](issue-memory-index-checked-fixture-contract.md) | 历史问题证据 | 通过 |
| [docs/issue-name-comparison-text-codec.md](issue-name-comparison-text-codec.md) | 历史问题证据 | 通过 |
| [docs/issue-name-type-registration.md](issue-name-type-registration.md) | 历史问题证据 | 通过 |
| [docs/issue-native-delete-using-bound-consumer.md](issue-native-delete-using-bound-consumer.md) | 历史问题证据 | 通过 |
| [docs/issue-native-dml-session-scope.md](issue-native-dml-session-scope.md) | 历史问题证据 | 通过 |
| [docs/issue-native-query-error-unlock.md](issue-native-query-error-unlock.md) | 历史问题证据 | 通过 |
| [docs/issue-native-scalar-query-routines.md](issue-native-scalar-query-routines.md) | 历史问题证据 | 通过 |
| [docs/issue-native-source-dml-oracle.md](issue-native-source-dml-oracle.md) | 历史问题证据 | 通过 |
| [docs/issue-native-source-status-contract.md](issue-native-source-status-contract.md) | 历史问题证据 | 通过 |
| [docs/issue-native-sql-pattern-consumers.md](issue-native-sql-pattern-consumers.md) | 历史问题证据 | 通过 |
| [docs/issue-nth-value-default-peer-frame.md](issue-nth-value-default-peer-frame.md) | 历史问题证据 | 通过 |
| [docs/issue-numeric-column-modifier.md](issue-numeric-column-modifier.md) | 历史问题证据 | 通过 |
| [docs/issue-opt-01-path-planner-audit.md](issue-opt-01-path-planner-audit.md) | 历史问题证据 | 通过 |
| [docs/issue-opt-05-small-join-guc-cost-override.md](issue-opt-05-small-join-guc-cost-override.md) | 历史问题证据 | 通过 |
| [docs/issue-order-metadata-arithmetic-combination.md](issue-order-metadata-arithmetic-combination.md) | 历史问题证据 | 通过 |
| [docs/issue-ordinary-case-prepared-execution.md](issue-ordinary-case-prepared-execution.md) | 历史问题证据 | 通过 |
| [docs/issue-ordinary-scalar-order.md](issue-ordinary-scalar-order.md) | 历史问题证据 | 通过 |
| [docs/issue-ordinary-scalar-where.md](issue-ordinary-scalar-where.md) | 历史问题证据 | 通过 |
| [docs/issue-ordinary-union-all-consumer.md](issue-ordinary-union-all-consumer.md) | 历史问题证据 | 通过 |
| [docs/issue-original-465-add-column-timeout-observations.md](issue-original-465-add-column-timeout-observations.md) | 历史问题证据 | 通过 |
| [docs/issue-parameter-input-origin.md](issue-parameter-input-origin.md) | 历史问题证据 | 通过 |
| [docs/issue-parameter-integer-current-root-composition.md](issue-parameter-integer-current-root-composition.md) | 历史问题证据 | 通过 |
| [docs/issue-pattern-predicate-binding.md](issue-pattern-predicate-binding.md) | 历史问题证据 | 通过 |
| [docs/issue-pg-typeof-declared-input.md](issue-pg-typeof-declared-input.md) | 历史问题证据 | 通过 |
| [docs/issue-pg18-reference-bootstrap.md](issue-pg18-reference-bootstrap.md) | 历史问题证据 | 通过 |
| [docs/issue-physical-array-prepared-consumer.md](issue-physical-array-prepared-consumer.md) | 历史问题证据 | 通过 |
| [docs/issue-physical-array-protocol-descriptor.md](issue-physical-array-protocol-descriptor.md) | 历史问题证据 | 通过 |
| [docs/issue-physical-bit-unknown-input.md](issue-physical-bit-unknown-input.md) | 历史问题证据 | 通过 |
| [docs/issue-physical-floating-catalog-identity.md](issue-physical-floating-catalog-identity.md) | 历史问题证据 | 通过 |
| [docs/issue-pitr-forked-backup-selected-archive.md](issue-pitr-forked-backup-selected-archive.md) | 历史问题证据 | 通过 |
| [docs/issue-plpgsql-lexical-and-source-comparisons.md](issue-plpgsql-lexical-and-source-comparisons.md) | 历史问题证据 | 通过 |
| [docs/issue-plpgsql-query-binding-preflight.md](issue-plpgsql-query-binding-preflight.md) | 历史问题证据 | 通过 |
| [docs/issue-plpgsql-query-destination.md](issue-plpgsql-query-destination.md) | 历史问题证据 | 通过 |
| [docs/issue-plpgsql-raise-sqlstate.md](issue-plpgsql-raise-sqlstate.md) | 历史问题证据 | 通过 |
| [docs/issue-plpgsql-scalar-binding-and-native-errors.md](issue-plpgsql-scalar-binding-and-native-errors.md) | 历史问题证据 | 通过 |
| [docs/issue-plpgsql-select-into-execution-demand.md](issue-plpgsql-select-into-execution-demand.md) | 历史问题证据 | 通过 |
| [docs/issue-plpgsql-select-into-typed-query.md](issue-plpgsql-select-into-typed-query.md) | 历史问题证据 | 通过 |
| [docs/issue-plpgsql-sql-value-intrinsics.md](issue-plpgsql-sql-value-intrinsics.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-and-projection-combination.md](issue-prepared-and-projection-combination.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-constant-planning.md](issue-prepared-constant-planning.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-dml-plan.md](issue-prepared-dml-plan.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-exists.md](issue-prepared-exists.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-global-aggregate.md](issue-prepared-global-aggregate.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-primitive-assignment-input.md](issue-prepared-primitive-assignment-input.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-projection-labels.md](issue-prepared-projection-labels.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-quantified-cursor-demand.md](issue-prepared-quantified-cursor-demand.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-query-array-metadata.md](issue-prepared-query-array-metadata.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-query-cursor.md](issue-prepared-query-cursor.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-read-root-planning.md](issue-prepared-read-root-planning.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-scalar-checked-consumers.md](issue-prepared-scalar-checked-consumers.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-scalar-fetch-ties.md](issue-prepared-scalar-fetch-ties.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-scalar-host-cursor-ownership.md](issue-prepared-scalar-host-cursor-ownership.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-scalar-query-context.md](issue-prepared-scalar-query-context.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-source-contexts-and-view-execution.md](issue-prepared-source-contexts-and-view-execution.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-srf-host-ownership.md](issue-prepared-srf-host-ownership.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-srf-unknown-input.md](issue-prepared-srf-unknown-input.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-star-binding-consumer.md](issue-prepared-star-binding-consumer.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-text-parameter-width.md](issue-prepared-text-parameter-width.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-union-all-execution.md](issue-prepared-union-all-execution.md) | 历史问题证据 | 通过 |
| [docs/issue-prepared-unknown-input.md](issue-prepared-unknown-input.md) | 历史问题证据 | 通过 |
| [docs/issue-private-aggregate-array-followup.md](issue-private-aggregate-array-followup.md) | 历史问题证据 | 通过 |
| [docs/issue-private-sql-value-repair.md](issue-private-sql-value-repair.md) | 历史问题证据 | 通过 |
| [docs/issue-private-virtual-shadow-repair.md](issue-private-virtual-shadow-repair.md) | 历史问题证据 | 通过 |
| [docs/issue-projectset-ordinary-receiver.md](issue-projectset-ordinary-receiver.md) | 历史问题证据 | 通过 |
| [docs/issue-projectset-prepared-children.md](issue-projectset-prepared-children.md) | 历史问题证据 | 通过 |
| [docs/issue-protocol-parameter-physical-identity.md](issue-protocol-parameter-physical-identity.md) | 历史问题证据 | 通过 |
| [docs/issue-protocol-physical-column-origin.md](issue-protocol-physical-column-origin.md) | 历史问题证据 | 通过 |
| [docs/issue-public-consumers-current-root-composition.md](issue-public-consumers-current-root-composition.md) | 历史问题证据 | 通过 |
| [docs/issue-qry-03-multijoin-order-and-where.md](issue-qry-03-multijoin-order-and-where.md) | 历史问题证据 | 通过 |
| [docs/issue-qualified-null-cast-identity.md](issue-qualified-null-cast-identity.md) | 历史问题证据 | 通过 |
| [docs/issue-qualified-public-namespace.md](issue-qualified-public-namespace.md) | 历史问题证据 | 通过 |
| [docs/issue-qualified-routine-frontend.md](issue-qualified-routine-frontend.md) | 历史问题证据 | 通过 |
| [docs/issue-qualified-source-canonical-identity.md](issue-qualified-source-canonical-identity.md) | 历史问题证据 | 通过 |
| [docs/issue-qualified-view-target-dispatch-open.md](issue-qualified-view-target-dispatch-open.md) | 历史问题证据 | 通过 |
| [docs/issue-quantified-prepared-execution.md](issue-quantified-prepared-execution.md) | 历史问题证据 | 通过 |
| [docs/issue-quantified-qualification-planning.md](issue-quantified-qualification-planning.md) | 历史问题证据 | 通过 |
| [docs/issue-query-projection-binding-and-lateral-scope.md](issue-query-projection-binding-and-lateral-scope.md) | 历史问题证据 | 通过 |
| [docs/issue-quoted-bit-column-predicate.md](issue-quoted-bit-column-predicate.md) | 历史问题证据 | 通过 |
| [docs/issue-quoted-range-column-projection.md](issue-quoted-range-column-projection.md) | 历史问题证据 | 通过 |
| [docs/issue-quoted-type-spelling.md](issue-quoted-type-spelling.md) | 历史问题证据 | 通过 |
| [docs/issue-read-owner-index-snapshot.md](issue-read-owner-index-snapshot.md) | 历史问题证据 | 通过 |
| [docs/issue-ready-source-integration.md](issue-ready-source-integration.md) | 历史问题证据 | 通过 |
| [docs/issue-retained-scalar-projection-runtime.md](issue-retained-scalar-projection-runtime.md) | 历史问题证据 | 通过 |
| [docs/issue-returning-array-canonical-typmods.md](issue-returning-array-canonical-typmods.md) | 历史问题证据 | 通过 |
| [docs/issue-returning-array-declared-metadata.md](issue-returning-array-declared-metadata.md) | 历史问题证据 | 通过 |
| [docs/issue-returning-hidden-range-fixture.md](issue-returning-hidden-range-fixture.md) | 历史问题证据 | 通过 |
| [docs/issue-returning-transition-namespaces.md](issue-returning-transition-namespaces.md) | 历史问题证据 | 通过 |
| [docs/issue-review-o01-table-owner-health-retry.md](issue-review-o01-table-owner-health-retry.md) | 历史问题证据 | 通过 |
| [docs/issue-review-o02-index-access-path-expectations.md](issue-review-o02-index-access-path-expectations.md) | 历史问题证据 | 通过 |
| [docs/issue-review-o03-explain-join-source-graph.md](issue-review-o03-explain-join-source-graph.md) | 历史问题证据 | 通过 |
| [docs/issue-review-o04-having-boolean-context.md](issue-review-o04-having-boolean-context.md) | 历史问题证据 | 通过 |
| [docs/issue-review-o05-order-by-duplicate-alias.md](issue-review-o05-order-by-duplicate-alias.md) | 历史问题证据 | 通过 |
| [docs/issue-review-o06-matview-refresh-revalidation.md](issue-review-o06-matview-refresh-revalidation.md) | 历史问题证据 | 通过 |
| [docs/issue-review-o07-alter-array-revalidation.md](issue-review-o07-alter-array-revalidation.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r01-numeric-square-root.md](issue-review-r01-numeric-square-root.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r02-window-declared-comparisons.md](issue-review-r02-window-declared-comparisons.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r03-window-partition-equivalence.md](issue-review-r03-window-partition-equivalence.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r04-window-frame-boundaries.md](issue-review-r04-window-frame-boundaries.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r05-ntile-distribution.md](issue-review-r05-ntile-distribution.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r06-ntile-argument.md](issue-review-r06-ntile-argument.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r07-window-signed-offset.md](issue-review-r07-window-signed-offset.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r08-binary-float.md](issue-review-r08-binary-float.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r09-pool-wait-reference.md](issue-review-r09-pool-wait-reference.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r10-pool-database-capacity.md](issue-review-r10-pool-database-capacity.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r11-window-exact-accumulator.md](issue-review-r11-window-exact-accumulator.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r12-test-registration.md](issue-review-r12-test-registration.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r13-logical-tuple-encoding.md](issue-review-r13-logical-tuple-encoding.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r14-publication-read-errors.md](issue-review-r14-publication-read-errors.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r16-catalog-complete-read.md](issue-review-r16-catalog-complete-read.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r16-optional-catalog-directory.md](issue-review-r16-optional-catalog-directory.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r17-rollback-recovery-ownership.md](issue-review-r17-rollback-recovery-ownership.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r170-quoted-unqualified-scalar-projection.md](issue-review-r170-quoted-unqualified-scalar-projection.md) | quoted标量投影身份的有限修复、两端70入口证据及未完成边界 | 通过 |
| [docs/issue-review-r171-quoted-scalar-protocol-analysis.md](issue-review-r171-quoted-scalar-protocol-analysis.md) | Parse发布前及Describe纯绑定的有限修复、两端70入口证据及未完成边界 | 通过 |
| [docs/issue-review-r172-qualified-column-scalar-argument.md](issue-review-r172-qualified-column-scalar-argument.md) | abs参数的真实列身份的有限修复、两端70入口证据及未完成边界 | 通过 |
| [docs/issue-review-r173-routine-signature-type-dependencies.md](issue-review-r173-routine-signature-type-dependencies.md) | 真实签名依赖缺失、跳过/未验收状态与不完整证据边界 | 通过 |
| [docs/issue-review-r174-ordinary-source-descriptor.md](issue-review-r174-ordinary-source-descriptor.md) | computed来源声明及typmod的有限修复、两端70入口证据及未完成边界 | 通过 |
| [docs/issue-review-r175-ordinary-aggregate-identity.md](issue-review-r175-ordinary-aggregate-identity.md) | SUM/COUNT入口及decoded列身份的有限修复、两端70入口证据及未完成边界 | 通过 |
| [docs/issue-review-r176-ordinary-aggregate-order-keys.md](issue-review-r176-ordinary-aggregate-order-keys.md) | 普通SUM/COUNT排序键有限修复、61/190通过、Root72通过及两次clean72超时失败；其余边界开放 | 部分 |
| [docs/issue-review-r177-grouped-aggregate-order.md](issue-review-r177-grouped-aggregate-order.md) | 用户暂缓；128/扩展142观察及未验收候选保留，无生产修复或新增注册测试 | 暂缓 |
| [docs/issue-review-r178-repeat-empty-input.md](issue-review-r178-repeat-empty-input.md) | REPEAT空输入省去按次数空追加；4个原生边界、PG80及两端11定向入口，历史启动失败保留 | 部分 |
| [docs/issue-review-r179-strpos-signatures.md](issue-review-r179-strpos-signatures.md) | STRPOS有限声明修复，native76/协议294、两端完整16与fresh真实Main3；旧49/140失败保留，其他边界开放 | 部分 |
| [docs/issue-review-r180-position-call-identity.md](issue-review-r180-position-call-identity.md) | 用户最新skip it暂缓；24双侧观察与方法保留，无生产修改或验收 | 用户暂缓 |
| [docs/issue-review-r181-reverse-linear.md](issue-review-r181-reverse-linear.md) | REVERSE线性UTF-8码点扫描；80native/96协议与两端19入口，旧语义通过及耗时观测保留 | 部分 |
| [docs/issue-review-r182-utf8-split-linear.md](issue-review-r182-utf8-split-linear.md) | PG/旧329全0，候选仅构建；原生-6中止、9错误类型期望与真实回调强转差异保留，未验收 | 部分 |
| [docs/issue-review-r183-string-to-array-callback-identity.md](issue-review-r183-string-to-array-callback-identity.md) | 实际callback元数据/身份；旧native60完整60失败，两端21入口全0，R182性能仍未接受 | 部分 |
| [docs/issue-review-r18-catalog-value-snapshots.md](issue-review-r18-catalog-value-snapshots.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r19-stale-temp-startup.md](issue-review-r19-stale-temp-startup.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r20-returning-query-owner.md](issue-review-r20-returning-query-owner.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r21-conflict-boolean-context.md](issue-review-r21-conflict-boolean-context.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r22-dml-required-keywords.md](issue-review-r22-dml-required-keywords.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r23-maintenance-persistence-gate.md](issue-review-r23-maintenance-persistence-gate.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r24-execute-argument-values.md](issue-review-r24-execute-argument-values.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r25-numeric-precision.md](issue-review-r25-numeric-precision.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r26-recursive-cte-types.md](issue-review-r26-recursive-cte-types.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r27-page-compaction.md](issue-review-r27-page-compaction.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r28-page-insert-capacity.md](issue-review-r28-page-insert-capacity.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r29-pattern-escape-type.md](issue-review-r29-pattern-escape-type.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r30-session-metadata.md](issue-review-r30-session-metadata.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r31-delete-only-typed-owner.md](issue-review-r31-delete-only-typed-owner.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r32-scientific-numeric-literals.md](issue-review-r32-scientific-numeric-literals.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r33-window-final-order.md](issue-review-r33-window-final-order.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r34-scientific-numeric-arithmetic.md](issue-review-r34-scientific-numeric-arithmetic.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r35-insert-select-conflict-boundary.md](issue-review-r35-insert-select-conflict-boundary.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r36-gcd-lcm-result-types.md](issue-review-r36-gcd-lcm-result-types.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r37-numeric-text-arithmetic.md](issue-review-r37-numeric-text-arithmetic.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r38-scientific-column-projections.md](issue-review-r38-scientific-column-projections.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r39-insert-select-owner.md](issue-review-r39-insert-select-owner.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r40-temp-cleanup-file-ownership.md](issue-review-r40-temp-cleanup-file-ownership.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r41-insert-source-preflight.md](issue-review-r41-insert-source-preflight.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r42-index-rollback-fixture.md](issue-review-r42-index-rollback-fixture.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r43-function-attribute-order.md](issue-review-r43-function-attribute-order.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r44-window-internal-namespace.md](issue-review-r44-window-internal-namespace.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r45-window-aggregate-arguments.md](issue-review-r45-window-aggregate-arguments.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r46-native-schema-ownership.md](issue-review-r46-native-schema-ownership.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r47-bit-integer-casts.md](issue-review-r47-bit-integer-casts.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r48-pending-rollback-fixtures.md](issue-review-r48-pending-rollback-fixtures.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r49-materialized-insert-sources.md](issue-review-r49-materialized-insert-sources.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r50-bit-type-modifiers.md](issue-review-r50-bit-type-modifiers.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r53-insert-interval-context.md](issue-review-r53-insert-interval-context.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r54-missing-parameter-priority.md](issue-review-r54-missing-parameter-priority.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r55-bytea-catalog-identity.md](issue-review-r55-bytea-catalog-identity.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r56-window-array-aggregate.md](issue-review-r56-window-array-aggregate.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r57-pending-rollback-protocol.md](issue-review-r57-pending-rollback-protocol.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r60-window-routine-identity.md](issue-review-r60-window-routine-identity.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r61-create-initialization-io.md](issue-review-r61-create-initialization-io.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r63-default-expression-role.md](issue-review-r63-default-expression-role.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r62-scalar-signatures.md](issue-review-r62-scalar-signatures.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r64-catalog-generation-fixtures.md](issue-review-r64-catalog-generation-fixtures.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r66-background-catalog-errors.md](issue-review-r66-background-catalog-errors.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r67-view-replacement-contract.md](issue-review-r67-view-replacement-contract.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r68-abs-integer-bounds.md](issue-review-r68-abs-integer-bounds.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r69-protocol-timeout-fixture.md](issue-review-r69-protocol-timeout-fixture.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r70-text-search-type-identities.md](issue-review-r70-text-search-type-identities.md) | 问题与修复证据 | 通过 |
| [docs/issue-review-r71-transaction-catalog-scope.md](issue-review-r71-transaction-catalog-scope.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r72-rename-alias-lock.md](issue-review-r72-rename-alias-lock.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r73-independent-binding.md](issue-review-r73-independent-binding.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r74-owner-fixture-generations.md](issue-review-r74-owner-fixture-generations.md) | 夹具修正与证据 | 通过 |
| [docs/issue-review-r76-restore-parameter-status.md](issue-review-r76-restore-parameter-status.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r77-restore-catalog-staging.md](issue-review-r77-restore-catalog-staging.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r78-restore-poll-publication.md](issue-review-r78-restore-poll-publication.md) | 夹具修正与方法 | 通过 |
| [docs/issue-review-r79-native-binary-length.md](issue-review-r79-native-binary-length.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r80-unbound-scalar-inference.md](issue-review-r80-unbound-scalar-inference.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r81-restore-stage-completion.md](issue-review-r81-restore-stage-completion.md) | 夹具修正与方法 | 通过 |
| [docs/issue-review-r83-float8-scalar-signatures.md](issue-review-r83-float8-scalar-signatures.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r84-integral-rounding-result-types.md](issue-review-r84-integral-rounding-result-types.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r85-numeric-sign-null.md](issue-review-r85-numeric-sign-null.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r86-array-position-empty-null-start.md](issue-review-r86-array-position-empty-null-start.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r87-mod-result-domains.md](issue-review-r87-mod-result-domains.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r88-analyze-sort-comparison-cost.md](issue-review-r88-analyze-sort-comparison-cost.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r89-round-single-result-domains.md](issue-review-r89-round-single-result-domains.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r90-toast-durable-id-reservations.md](issue-review-r90-toast-durable-id-reservations.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r91-savepoint-sequence-runtime-images.md](issue-review-r91-savepoint-sequence-runtime-images.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r92-toast-missing-counter.md](issue-review-r92-toast-missing-counter.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r93-boolean-aggregate-arguments.md](issue-review-r93-boolean-aggregate-arguments.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r94-show-parallel-workers.md](issue-review-r94-show-parallel-workers.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r95-legacy-global-boolean-arguments.md](issue-review-r95-legacy-global-boolean-arguments.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r96-collation-exit-user-deferred.md](issue-review-r96-collation-exit-user-deferred.md) | 用户暂缓与原始证据 | 通过 |
| [docs/issue-review-r97-typed-before-row-assignments.md](issue-review-r97-typed-before-row-assignments.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r100-geometric-coordinate-delimiters.md](issue-review-r100-geometric-coordinate-delimiters.md) | 历史问题证据 | 通过 |
| [docs/issue-review-r101-macaddr-input-and-widths.md](issue-review-r101-macaddr-input-and-widths.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r102-network-input-notation.md](issue-review-r102-network-input-notation.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r103-macaddr-name-input.md](issue-review-r103-macaddr-name-input.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r104-network-value-conversions.md](issue-review-r104-network-value-conversions.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r105-network-name-input.md](issue-review-r105-network-name-input.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r106-coalesce-explicit-input-analysis.md](issue-review-r106-coalesce-explicit-input-analysis.md) | 用户暂缓与原始证据 | 通过 |
| [docs/issue-review-r107-string-length-signatures.md](issue-review-r107-string-length-signatures.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r108-array-position-typed-equality.md](issue-review-r108-array-position-typed-equality.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r110-array-binary-result.md](issue-review-r110-array-binary-result.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r112-unconstrained-named-array-input.md](issue-review-r112-unconstrained-named-array-input.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r113-interval-binary-result.md](issue-review-r113-interval-binary-result.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r114-physical-attribute-length.md](issue-review-r114-physical-attribute-length.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r115-scalar-alias-projection-metadata.md](issue-review-r115-scalar-alias-projection-metadata.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r116-view-command-tags.md](issue-review-r116-view-command-tags.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r117-time-binary-result.md](issue-review-r117-time-binary-result.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r118-temporal-view-materialization.md](issue-review-r118-temporal-view-materialization.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r119-timestamp-binary-result.md](issue-review-r119-timestamp-binary-result.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r120-date-cycle-inverse.md](issue-review-r120-date-cycle-inverse.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r121-time-input-normalization.md](issue-review-r121-time-input-normalization.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r122-time-explicit-precision.md](issue-review-r122-time-explicit-precision.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r123-temporal-column-precision.md](issue-review-r123-temporal-column-precision.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r124-timetz-postfix-projection.md](issue-review-r124-timetz-postfix-projection.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r125-time-precision-value.md](issue-review-r125-time-precision-value.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r126-time-precision-grammar.md](issue-review-r126-time-precision-grammar.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r127-time-excess-precision.md](issue-review-r127-time-excess-precision.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r128-time-binary-parameter.md](issue-review-r128-time-binary-parameter.md) | 问题与修复方法 | 通过 |
| [docs/issue-review-r129-native-array-projection-types.md](issue-review-r129-native-array-projection-types.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r130-native-result-hint-precedence.md](issue-review-r130-native-result-hint-precedence.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r131-timestamptz-binary-timezone.md](issue-review-r131-timestamptz-binary-timezone.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r132-timezone-set-to.md](issue-review-r132-timezone-set-to.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r133-timestamp-binary-microseconds.md](issue-review-r133-timestamp-binary-microseconds.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r134-core-native-callback-ownership.md](issue-review-r134-core-native-callback-ownership.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r135-timestamptz-returning-timezone.md](issue-review-r135-timestamptz-returning-timezone.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r136-temporal-microsecond-storage.md](issue-review-r136-temporal-microsecond-storage.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r137-foreign-key-full-value.md](issue-review-r137-foreign-key-full-value.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r138-session-timezone.md](issue-review-r138-session-timezone.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r139-timezone-default.md](issue-review-r139-timezone-default.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r140-timezone-set-grammar.md](issue-review-r140-timezone-set-grammar.md) | 问题、方法与暂缓记录 | 通过 |
| [docs/issue-review-r141-set-parameter-identity.md](issue-review-r141-set-parameter-identity.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r142-quoted-set-parameter.md](issue-review-r142-quoted-set-parameter.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r143-show-guc-protocol-results.md](issue-review-r143-show-guc-protocol-results.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r145-search-path-value-identity.md](issue-review-r145-search-path-value-identity.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r146-primary-key-full-value.md](issue-review-r146-primary-key-full-value.md) | 问题、方法与暂缓记录 | 通过 |
| [docs/issue-review-r147-interval-calendar-order.md](issue-review-r147-interval-calendar-order.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r148-temporal-arithmetic-result-types.md](issue-review-r148-temporal-arithmetic-result-types.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r149-width-bucket-boundaries.md](issue-review-r149-width-bucket-boundaries.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r150-threshold-width-bucket.md](issue-review-r150-threshold-width-bucket.md) | 开放问题、方法与观察 | 通过 |
| [docs/issue-review-r151-enum-array-catalog.md](issue-review-r151-enum-array-catalog.md) | 用户暂缓、未验收候选与失败证据 | 通过 |
| [docs/issue-review-r152-regtype-identifier-output.md](issue-review-r152-regtype-identifier-output.md) | 有限修复、专项通过与未全绿门禁 | 通过 |
| [docs/issue-review-r155-enum-namespace-owner.md](issue-review-r155-enum-namespace-owner.md) | 问题、修复与验证 | 通过 |
| [docs/issue-review-r156-regtype-physical-constants.md](issue-review-r156-regtype-physical-constants.md) | 用户暂缓、物理身份与未验证草稿 | 通过 |
| [docs/issue-review-r157-dotted-set-target.md](issue-review-r157-dotted-set-target.md) | 普通点号目标有限修复与完整验证 | 通过 |
| [docs/issue-review-r158-angle-conversion-range.md](issue-review-r158-angle-conversion-range.md) | 角度转换有限边界修复与组合回归证据 | 通过 |
| [docs/issue-review-r159-encoding-boundaries.md](issue-review-r159-encoding-boundaries.md) | 三类编码边界修复、完整回归与未核实线索 | 通过 |
| [docs/issue-review-r160-angle-constant-demand.md](issue-review-r160-angle-constant-demand.md) | 开放规划时机与完整观察 | 通过 |
| [docs/issue-review-r161-qualified-projection-routine-identity.md](issue-review-r161-qualified-projection-routine-identity.md) | 限定标量投影身份有限修复与组合回归证据 | 通过 |
| [docs/issue-review-r162-generic-stored-signatures.md](issue-review-r162-generic-stored-signatures.md) | 存储标量签名归属有限修复与组合回归证据 | 通过 |
| [docs/issue-review-r163-qualified-parse-validation.md](issue-review-r163-qualified-parse-validation.md) | 限定标量Parse校验有限修复与组合回归证据 | 通过 |
| [docs/issue-review-r164-routine-custom-type-declarations.md](issue-review-r164-routine-custom-type-declarations.md) | 自定义标量声明有限修复与组合回归证据 | 通过 |
| [docs/issue-review-r165-builtin-array-binary-bind.md](issue-review-r165-builtin-array-binary-bind.md) | 五种内置数组输入有限修复与独立实测 | 通过 |
| [docs/issue-review-r166-scalar-protocol-parameter-inference.md](issue-review-r166-scalar-protocol-parameter-inference.md) | 整式标量参数推断有限修复与组合回归证据 | 通过 |
| [docs/issue-review-r167-domain-drop-targets.md](issue-review-r167-domain-drop-targets.md) | DROP DOMAIN目标有限修复与组合回归证据 | 通过 |
| [docs/issue-review-r168-composite-namespace-owner.md](issue-review-r168-composite-namespace-owner.md) | composite命名空间归属有限修复与组合回归证据 | 通过 |
| [docs/issue-review-r169-simple-query-raw-syntax.md](issue-review-r169-simple-query-raw-syntax.md) | Simple Query有限原始语法修复与组合回归证据 | 通过 |
| [docs/issue-root-numeric-publication.md](issue-root-numeric-publication.md) | 历史问题证据 | 通过 |
| [docs/issue-root-row-publication.md](issue-root-row-publication.md) | 历史问题证据 | 通过 |
| [docs/issue-root-sql-value-publication.md](issue-root-sql-value-publication.md) | 历史问题证据 | 通过 |
| [docs/issue-routine-array-declarations.md](issue-routine-array-declarations.md) | 历史问题证据 | 通过 |
| [docs/issue-routine-creation-search-path.md](issue-routine-creation-search-path.md) | 历史问题证据 | 通过 |
| [docs/issue-routine-namespace-declaration.md](issue-routine-namespace-declaration.md) | 历史问题证据 | 通过 |
| [docs/issue-routine-result-type-oid.md](issue-routine-result-type-oid.md) | 历史问题证据 | 通过 |
| [docs/issue-row-constructor.md](issue-row-constructor.md) | 历史问题证据 | 通过 |
| [docs/issue-row-description-literal-source.md](issue-row-description-literal-source.md) | 历史问题证据 | 通过 |
| [docs/issue-runtime-parameter-child-context.md](issue-runtime-parameter-child-context.md) | 历史问题证据 | 通过 |
| [docs/issue-savepoint-image-alias.md](issue-savepoint-image-alias.md) | 历史问题证据 | 通过 |
| [docs/issue-savepoint-no-effect-image.md](issue-savepoint-no-effect-image.md) | 历史问题证据 | 通过 |
| [docs/issue-scalar-aggregate-ambiguity.md](issue-scalar-aggregate-ambiguity.md) | 历史问题证据 | 通过 |
| [docs/issue-scalar-current-public-api-validation.md](issue-scalar-current-public-api-validation.md) | 历史问题证据 | 通过 |
| [docs/issue-scalar-distinct-cardinality-fixture.md](issue-scalar-distinct-cardinality-fixture.md) | 历史问题证据 | 通过 |
| [docs/issue-scalar-sql-null-condition-carrier.md](issue-scalar-sql-null-condition-carrier.md) | 历史问题证据 | 通过 |
| [docs/issue-scalar-subquery-sort-slots.md](issue-scalar-subquery-sort-slots.md) | 历史问题证据 | 通过 |
| [docs/issue-schema-format-parser-fixtures.md](issue-schema-format-parser-fixtures.md) | 历史问题证据 | 通过 |
| [docs/issue-search-path-transaction-local.md](issue-search-path-transaction-local.md) | 历史问题证据 | 通过 |
| [docs/issue-select-where-boolean-context.md](issue-select-where-boolean-context.md) | 历史问题证据 | 通过 |
| [docs/issue-sequence-alter-allocation-position.md](issue-sequence-alter-allocation-position.md) | 历史问题证据 | 通过 |
| [docs/issue-sequence-ddl-rollback-generations.md](issue-sequence-ddl-rollback-generations.md) | 历史问题证据 | 通过 |
| [docs/issue-sequence-format-test-adaptation.md](issue-sequence-format-test-adaptation.md) | 历史问题证据 | 通过 |
| [docs/issue-session-execution-lifetime.md](issue-session-execution-lifetime.md) | 历史问题证据 | 通过 |
| [docs/issue-set-clause-binding.md](issue-set-clause-binding.md) | 历史问题证据 | 通过 |
| [docs/issue-set-clause-execution.md](issue-set-clause-execution.md) | 历史问题证据 | 通过 |
| [docs/issue-set-operation-bound-types.md](issue-set-operation-bound-types.md) | 历史问题证据 | 通过 |
| [docs/issue-set-operation-prepared-metadata.md](issue-set-operation-prepared-metadata.md) | 历史问题证据 | 通过 |
| [docs/issue-shared-engine-btree-cache-coherence.md](issue-shared-engine-btree-cache-coherence.md) | 历史问题证据 | 通过 |
| [docs/issue-shared-engine-heap-cache-coherence.md](issue-shared-engine-heap-cache-coherence.md) | 历史问题证据 | 通过 |
| [docs/issue-shared-heap-extent-publication.md](issue-shared-heap-extent-publication.md) | 历史问题证据 | 通过 |
| [docs/issue-signed-fetch-runtime-demand.md](issue-signed-fetch-runtime-demand.md) | 历史问题证据 | 通过 |
| [docs/issue-similar-class-conversion.md](issue-similar-class-conversion.md) | 历史问题证据 | 通过 |
| [docs/issue-similar-quote-separator-state.md](issue-similar-quote-separator-state.md) | 历史问题证据 | 通过 |
| [docs/issue-simple-case-constant-demand.md](issue-simple-case-constant-demand.md) | 历史问题证据 | 通过 |
| [docs/issue-simple-case-equality-binding.md](issue-simple-case-equality-binding.md) | 历史问题证据 | 通过 |
| [docs/issue-snapshot-exporter-xid-visibility.md](issue-snapshot-exporter-xid-visibility.md) | 历史问题证据 | 通过 |
| [docs/issue-source-dml-returning-oracle.md](issue-source-dml-returning-oracle.md) | 历史问题证据 | 通过 |
| [docs/issue-sql-function-query-body.md](issue-sql-function-query-body.md) | 历史问题证据 | 通过 |
| [docs/issue-sql-pattern-unicode-runtime.md](issue-sql-pattern-unicode-runtime.md) | 历史问题证据 | 通过 |
| [docs/issue-sql-value-keyword-binding.md](issue-sql-value-keyword-binding.md) | 历史问题证据 | 通过 |
| [docs/issue-sql-value-public-parser.md](issue-sql-value-public-parser.md) | 历史问题证据 | 通过 |
| [docs/issue-srf-search-path-shadow.md](issue-srf-search-path-shadow.md) | 历史问题证据 | 通过 |
| [docs/issue-static-arithmetic-and-quoted-row-identity.md](issue-static-arithmetic-and-quoted-row-identity.md) | 历史问题证据 | 通过 |
| [docs/issue-sto-01-control-format-integrity.md](issue-sto-01-control-format-integrity.md) | 历史问题证据 | 通过 |
| [docs/issue-sto-02-relation-storage-namespace.md](issue-sto-02-relation-storage-namespace.md) | 历史问题证据 | 通过 |
| [docs/issue-sto-03-page-identity.md](issue-sto-03-page-identity.md) | 历史问题证据 | 通过 |
| [docs/issue-sto-04-buffer-invalidation-pins.md](issue-sto-04-buffer-invalidation-pins.md) | 历史问题证据 | 通过 |
| [docs/issue-sto-05-vacuum-visibility-horizon.md](issue-sto-05-vacuum-visibility-horizon.md) | 历史问题证据 | 通过 |
| [docs/issue-sto-06-toast-format-audit.md](issue-sto-06-toast-format-audit.md) | 历史问题证据 | 通过 |
| [docs/issue-sto-07-large-object-path-integrity.md](issue-sto-07-large-object-path-integrity.md) | 历史问题证据 | 通过 |
| [docs/issue-stored-function-arity.md](issue-stored-function-arity.md) | 历史问题证据 | 通过 |
| [docs/issue-stored-function-engine-owner.md](issue-stored-function-engine-owner.md) | 历史问题证据 | 通过 |
| [docs/issue-stored-function-metadata-namespace.md](issue-stored-function-metadata-namespace.md) | 历史问题证据 | 通过 |
| [docs/issue-stored-function-order-execution.md](issue-stored-function-order-execution.md) | 历史问题证据 | 通过 |
| [docs/issue-stored-function-statement-atomicity.md](issue-stored-function-statement-atomicity.md) | 历史问题证据 | 通过 |
| [docs/issue-stored-function-where-execution.md](issue-stored-function-where-execution.md) | 历史问题证据 | 通过 |
| [docs/issue-table-list-publication-fault-boundary.md](issue-table-list-publication-fault-boundary.md) | 历史问题证据 | 通过 |
| [docs/issue-timetz-array-type-identity.md](issue-timetz-array-type-identity.md) | 历史问题证据 | 通过 |
| [docs/issue-transaction-begin-error-state.md](issue-transaction-begin-error-state.md) | 历史问题证据 | 通过 |
| [docs/issue-transaction-wal-physical-owner.md](issue-transaction-wal-physical-owner.md) | 历史问题证据 | 通过 |
| [docs/issue-truncate-recovery-wal-fixture-identity.md](issue-truncate-recovery-wal-fixture-identity.md) | 历史问题证据 | 通过 |
| [docs/issue-typed-null-current-root-composition.md](issue-typed-null-current-root-composition.md) | 历史问题证据 | 通过 |
| [docs/issue-typed-query-explain-update-combination.md](issue-typed-query-explain-update-combination.md) | 历史问题证据 | 通过 |
| [docs/issue-typename-current-parser-checkpoint.md](issue-typename-current-parser-checkpoint.md) | 历史问题证据 | 通过 |
| [docs/issue-unary-builtin-signature-binding.md](issue-unary-builtin-signature-binding.md) | 历史问题证据 | 通过 |
| [docs/issue-unary-interval-component-values.md](issue-unary-interval-component-values.md) | 历史问题证据 | 通过 |
| [docs/issue-unary-overflow-structured-errors.md](issue-unary-overflow-structured-errors.md) | 历史问题证据 | 通过 |
| [docs/issue-unknown-physical-catalog.md](issue-unknown-physical-catalog.md) | 历史问题证据 | 通过 |
| [docs/issue-unlogged-relation-retirement.md](issue-unlogged-relation-retirement.md) | 历史问题证据 | 通过 |
| [docs/issue-unordered-window-admission.md](issue-unordered-window-admission.md) | 历史问题证据 | 通过 |
| [docs/issue-unordered-window-current-root-composition.md](issue-unordered-window-current-root-composition.md) | 历史问题证据 | 通过 |
| [docs/issue-update-canonical-quoted-target.md](issue-update-canonical-quoted-target.md) | 历史问题证据 | 通过 |
| [docs/issue-update-default-binding.md](issue-update-default-binding.md) | 历史问题证据 | 通过 |
| [docs/issue-update-default-consumer.md](issue-update-default-consumer.md) | 历史问题证据 | 通过 |
| [docs/issue-update-duplicate-target.md](issue-update-duplicate-target.md) | 历史问题证据 | 通过 |
| [docs/issue-update-returning-preparation-errors.md](issue-update-returning-preparation-errors.md) | 历史问题证据 | 通过 |
| [docs/issue-update-source-bound-consumer.md](issue-update-source-bound-consumer.md) | 历史问题证据 | 通过 |
| [docs/issue-update-typed-predicate-execution.md](issue-update-typed-predicate-execution.md) | 历史问题证据 | 通过 |
| [docs/issue-user-sql-value-intrinsic.md](issue-user-sql-value-intrinsic.md) | 历史问题证据 | 通过 |
| [docs/issue-vac-03-maintenance-options.md](issue-vac-03-maintenance-options.md) | 历史问题证据 | 通过 |
| [docs/issue-vacuum-toast-repeatable-read-fixture.md](issue-vacuum-toast-repeatable-read-fixture.md) | 历史问题证据 | 通过 |
| [docs/issue-values-common-type-binding.md](issue-values-common-type-binding.md) | 历史问题证据 | 通过 |
| [docs/issue-version-ordinary-scalar.md](issue-version-ordinary-scalar.md) | 历史问题证据 | 通过 |
| [docs/issue-versioned-update-typed-binding.md](issue-versioned-update-typed-binding.md) | 历史问题证据 | 通过 |
| [docs/issue-view-trigger-typed-old-new.md](issue-view-trigger-typed-old-new.md) | 历史问题证据 | 通过 |
| [docs/issue-virtual-relation-shadow-binding.md](issue-virtual-relation-shadow-binding.md) | 历史问题证据 | 通过 |
| [docs/issue-volcano-scalar-open-contract-fixture.md](issue-volcano-scalar-open-contract-fixture.md) | 历史问题证据 | 通过 |
| [docs/issue-wal-01-resource-manager-coverage-audit.md](issue-wal-01-resource-manager-coverage-audit.md) | 历史问题证据 | 通过 |
| [docs/issue-wal-02-insertion-segments-audit.md](issue-wal-02-insertion-segments-audit.md) | 历史问题证据 | 通过 |
| [docs/issue-wal-03-checkpoint-audit.md](issue-wal-03-checkpoint-audit.md) | 历史问题证据 | 通过 |
| [docs/issue-wal-04-standby-and-timeline-audit.md](issue-wal-04-standby-and-timeline-audit.md) | 历史问题证据 | 通过 |
| [docs/issue-wal-05-startup-recovery-crash-matrix.md](issue-wal-05-startup-recovery-crash-matrix.md) | 历史问题证据 | 通过 |
| [docs/issue-wal-06-index-checksum-coverage.md](issue-wal-06-index-checksum-coverage.md) | 历史问题证据 | 通过 |
| [docs/issue-wal-07-no-replace-schema-marker-durability.md](issue-wal-07-no-replace-schema-marker-durability.md) | 历史问题证据 | 通过 |
| [docs/issue-wal-08-stale-temp-startup-recovery.md](issue-wal-08-stale-temp-startup-recovery.md) | 历史问题证据 | 通过 |
| [docs/issue-where-index-null-safe-join-combination.md](issue-where-index-null-safe-join-combination.md) | 历史问题证据 | 通过 |
| [docs/issue-window-default-current-root-composition.md](issue-window-default-current-root-composition.md) | 历史问题证据 | 通过 |
| [docs/issue-window-default-order-initialization.md](issue-window-default-order-initialization.md) | 历史问题证据 | 通过 |
| [docs/issue-window-groups-unbounded-boundary.md](issue-window-groups-unbounded-boundary.md) | 历史问题证据 | 通过 |
| [docs/issue-window-null-order-policy.md](issue-window-null-order-policy.md) | 历史问题证据 | 通过 |
| [docs/issue-window-query-metadata.md](issue-window-query-metadata.md) | 历史问题证据 | 通过 |
| [docs/issue-with-dml-range-namespace.md](issue-with-dml-range-namespace.md) | 历史问题证据 | 通过 |
| [docs/issue-with-multisource-dml-runtime.md](issue-with-multisource-dml-runtime.md) | 历史问题证据 | 通过 |
| [docs/issue-with-primary-dml-execution.md](issue-with-primary-dml-execution.md) | 历史问题证据 | 通过 |
| [docs/issue-with-primary-dml-preparation.md](issue-with-primary-dml-preparation.md) | 历史问题证据 | 通过 |
| [docs/issues/issue-native-resolved-array-storage.md](issues/issue-native-resolved-array-storage.md) | 历史问题证据 | 通过 |
| [docs/postgresql-18-gap-audit.md](postgresql-18-gap-audit.md) | 使用/开发/状态文档 | 通过 |
| [docs/postgresql-18-implementation-blueprint.md](postgresql-18-implementation-blueprint.md) | 使用/开发/状态文档 | 通过 |
| [docs/postgresql-comparison.md](postgresql-comparison.md) | 使用/开发/状态文档 | 通过 |
| [docs/production-status.md](production-status.md) | 使用/开发/状态文档 | 通过 |
| [docs/review-closeout-plan.md](review-closeout-plan.md) | 使用/开发/状态文档 | 通过 |
| [docs/review-evidence-2026-10-13.md](review-evidence-2026-10-13.md) | 使用/开发/状态文档 | 通过 |
| [docs/review-fixes-2026-10-13.md](review-fixes-2026-10-13.md) | 使用/开发/状态文档 | 通过 |
| [docs/review-index.md](review-index.md) | 使用/开发/状态文档 | 通过 |
| [docs/test-report.md](test-report.md) | 使用/开发/状态文档 | 通过 |
| [src/catalog/README.md](../src/catalog/README.md) | 使用/开发/状态文档 | 通过 |
| [src/executor/README.md](../src/executor/README.md) | 使用/开发/状态文档 | 通过 |
| [src/parser/README.md](../src/parser/README.md) | 使用/开发/状态文档 | 通过 |
| [src/replication/README.md](../src/replication/README.md) | 使用/开发/状态文档 | 通过 |
