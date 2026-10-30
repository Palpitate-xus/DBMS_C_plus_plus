# R222：普通函数缺少参数类型签名身份（用户暂缓，未验收）

本项仍开放。数据库当前接受版本为 `421f3269`，不是重载修复版本。
用户最新要求 `skip it`，本项停止继续实现和验收，普通继续不恢复。
已有未提交候选已接入生产清单、StorageEngine、DDL、目录和表达式解析器；保留工作区改动，
不将其作为已接受修复提交。工作区完整 61 单元构建及重复构建退出 0；纯提交候选构建
在 `DdlExecutor.cpp:205` 因 `emitNotice` 未声明退出 1，未生成纯候选服务器。
8 个工作区主要单元的语法检查退出 0 不能替代两侧完整构建或数据库运行验收。
独立组件通过不等于数据库重载可用，不提升 SQL-09/FUNC-05 或总清单状态。

## 已确认的问题与修改方法

| 问题 | 当前证据 | 必须采用的修改方法 |
| --- | --- | --- |
| 同模式同名函数不能按输入类型共存 | `udfPath` 只使用模式/名称；两组实际创建不同参数的函数报 42P07 | 用 `(namespace, name, ordered input type OIDs)` 作完整身份；不是只按参数个数，也不把返回类型、参数名或 typmod 当成重载键 |
| CREATE OR REPLACE 会拿不同输入签名的函数比较 | DDL 先调用单个 `getUDF(name,schema)`；新签名可能被当成修改旧签名 | 查找准确签名，只替换该对象并保留 OID；新签名创建新对象；相同签名、类型别名的重复创建报 42723 |
| 解析器拿不到同一模式中的候选集合 | 三个 scalar resolver 路径各取一个 getUDF | 收集完整候选，按真实参数声明和隐式转换选择，保留 schema 遮蔽、歧义和 UNKNOWN 推断；不能执行参数值或函数体来挑选 |
| UNKNOWN 多参数的类别选择可能依赖遍历顺序 | PG 18.6 对交叉 `(int,int)/(int,text)/(text,int)` 的 `pick(NULL,NULL)` 报 42725；当前 resolver 顺次删除候选的做法不适合这个集合 | 从同一阶段的候选集合分析所有未知位置的类别和 preferred type，再统一筛选；不能前一位置筛完后随便选唯一剩余者 |
| DROP 不能定位输入签名 | 当前 typed DROP 对非空签名直接报 0A000；bare-name 查询只有一个声明 | 解析完整签名和多目标，先验证再改动；bare name 有多重载时报 42725；typed DROP 只删除准确对象 |
| CREATE 撤销记录不足以区分重载 | DdlTransaction 只记录 name/schema，undo 也按 name/schema 删除 | 将确切签名纳入对象变更/undo 身份，确保新建重载回滚不删除原有重载；替换、删除、移动都由同一事务快照保护 |
| 目录、改名/迁移模式和 prepared 生命周期未整合 | scalar DDL 未写入 pg_proc；ALTER FUNCTION 仍走兼容对象路径 | 维护真实例程 OID/输入和返回类型、准确签名的改名与迁移，保持失败原子性；禁止旧 RowDescription 与新 DataRow 类型不一致 |

R219 是已提交的表 ACL 定位修复；不在本项重做认证或安全/TDE 专项。
R173 的类型依赖删除专项及 R217 的普通 CASE 命名/CTE Describe 已由用户跳过，不恢复。

## 完整协议参考与旧版本反证

当前草稿 `tests/routine_overload_identity_protocol_draft.py` 共 1,952 项，尚未注册。
所有表达式显式命名为 v，不借本项修复 R217 的输出标签；原 R217 1,808 项反证另保留。

覆盖零/一/二/三参数、同参数个数的 integer/bigint/text/boolean/array 签名、引用身份、
搜索路径和精确签名优先级、UNKNOWN/NULL/歧义、错误和零行输入、真实列参数、
两种输入/输出格式及同一个 prepared statement 的重复 Bind/Execute。
描述和未需求分支用序列验证没有提前执行函数。生命周期还包含替换、准确删除、
多个目标的失败原子性、BEGIN/ROLLBACK、SAVEPOINT、改名和迁移模式。
prepared statement 另外检查：替换后使用新函数体；删除或移动后若结果类型变化，
报 0A000 而不是发送与旧描述不一致的数据；重新建立可用身份后可继续执行。

| 完整实际运行 | 结果与边界 |
| --- | --- |
| 首版 PG 18.6 1,931 项 | 退出 1，60 项失败；全部是夹具把交叉签名的 NULL/NULL 歧义错写为 text/int 成功。首版源文件和失败记录保留 |
| 纠正预期后的同 1,931 项 PG 18.6 | 退出 0，实例停止退出 0；未删除 SQL 或控制，只将对应结果改为参考要求的 42725 |
| 修复前工作区与纯提交版本，各完整 1,931 项 | 两侧都退出 1，各 1,626 项失败；含上游创建失败导致的后续连锁失败，不能称为 1,626 个独立缺陷 |
| 新增 21 个 prepared 生命周期控制后的完整 1,952 项 PG 18.6 | 退出 0，实例停止退出 0；原 1,931 项全部保留 |
| 修复前工作区与纯提交版本，各完整 1,952 项 | 两侧都退出 1，各 1,640 项失败；最新整套旧基线单独保存，不覆盖 1,931 项记录 |
| 完整 1,952 项产品验收 | 未运行整合候选验收；纯候选完整构建失败后用户暂缓，不能记为通过 |

本地资料目录：`/tmp/dbms-r222-routine-overloads.s7ByDtDQ`。
首版参考记录：`/tmp/dbms-private-pg18-full-fixture.lnyhp0z2/completed-reference-receipt.json`，
SHA256 `fc1be3ef0cb0faafda65d76dc734323f822c44a59c83570375961a4f509bba03`。
纠正后 1,931 项参考记录：`/tmp/dbms-private-pg18-full-fixture.5eofydh6/completed-reference-receipt.json`，
SHA256 `a40b01873cdc02277a2776f00632f86db41f57c87d23d17a19a6d49974b68638`。
完整 1,952 项参考记录：`/tmp/dbms-private-pg18-full-fixture.jc69z96o/completed-reference-receipt.json`，
SHA256 `5549115d3e1caf14e5653553291f17988c4933712638dae29d4c9a5a2d874fa2`。

## 新存储组件的证据边界

`RoutineStore.h/.cpp` 候选使用完整输入 OID 向量作为身份，并在记录中保存模式、名称、
例程 OID、参数名及声明、返回声明/OID、语言、函数体、strict 和 volatility。
文件名摘要只用来定位；读取时还核对完整记录身份，不能把摘要当作类型身份。
V3 记录长度编码、校验和及原子发布保留字段边界；同个数不同类型、参数顺序、
零参数、重复/替换、改名、迁移、准确删除和 I/O 错误都需要保持正确身份。

独立草稿 `tests/routine_overload_store_test.cpp.draft` 共 790 项，O2 和 ASan/UBSan
均实际退出 0；包括 0 至 100 参数、完整记录往返、重新打开读取、截断/损坏、
相邻重载不变、替换 OID、旧记录回调/retirement、不复活旧体、失败屏障和 no-clobber 发布。
这里旧记录使用的是 adapter 合约夹具，真实 PARAMS/PARAMS2 解码及数据库迁移整合尚未验证。
二进制字段往返不等于 SQL 接受 NUL，也不等于已执行 PL/pgSQL 或修复暂缓的多行函数模块。

独立记录 `completed-standalone-store-receipt.json`，
SHA256 `fa2adf56338204ea63b25239a9dbd6edf8b25f18d1a81e444fa66284162539ec`。
最初一次组件编译因 exists 的 ADL 歧义失败，已改名后重编；另一次草稿编译指令误把
对象文件当成 C++ 输入，已终止该轮自己的两个编译进程，未终止数据库或用户进程。
随后组件/测试各自重新编译、正常链接并分别运行 O2 与 sanitizer，才产生上述成功记录。

## 恢复本项时仍需完成的门槛（当前不执行）

1. 将真实旧读取器接入 adapter；StorageEngine 提供完整候选和准确签名的查找/写入/删除，不能改成任意选第一个。
2. 更新所有元数据探测和真实调用消费者，描述不得执行函数，native 调用和绑定回调继续拥有声明及值。
3. 统一 CREATE/REPLACE/DROP/ALTER、例程目录 OID 与 undo 身份，验证旧数据往返、失败恢复和真实重启。
4. 增加实际数据库原生/目录/重启验证；公共头或结构布局改变后，两侧各自全量重编生产依赖，不混用 OutputCapture 对象。
5. 完整 1,952 项及后续新增项必须在工作区和纯提交树均通过，再跑相关既有回归；不可只接起来若干通过记录。
6. 文档和注册检查通过后，才用独立 index 本地提交本项。原 staged 18 路径及其他改动保持不动，不 push，不启用 Actions。

原 273 项总账与冻结 1,492 项结果不重写；总清单仍未完成。
