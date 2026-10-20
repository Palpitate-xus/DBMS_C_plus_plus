# R193：pg_class 的已绑定读取必须使用真实虚拟目录来源

状态：普通目录 COUNT 及临时索引目录生命周期反例已定向修复；不是完整目录或聚合族验收。

## 原因与修改方法

原 1492 全量中的 `pg_class_query_protocol_e2e_test.py` 和
`temp_index_catalog_protocol_e2e_test.py` 都在普通 COUNT 上报 XX000：
`mutation source descriptor differs from physical relation`。
修复前的纯提交服务器和工作区服务器各运行两个完整入口，四次均复现同一错误。

绑定器把 `pg_catalog.pg_class` 定义为七列虚拟目录，但 Main 的已绑定来源执行器
只接入了 pg_settings 虚拟行。直接 COUNT 进入该执行器后，pg_class 错误地继续走
物理表扫描，并将七列逻辑描述与不存在的物理目录表比较。
这不是损坏的用户堆表，不能移除或放宽所有物理关系的描述一致性检查。

修改在 Main 增加 `preparedPgClassRows`，从已有 CatalogService 的 `listClasses()`
生成真实目录行，按已有绑定描述的顺序和类型提供：
oid/oid、relname/name、relnamespace/oid、relkind/"char"、relnatts/smallint、
relpersistence/"char"、relowner/oid。真实值与非 NULL 标志均逐列保留。
来源分支只匹配已绑定的 `pg_catalog`/`pg_class` 身份，不匹配 SQL 文本片段或猜测物理名称。
实际堆表仍执行原描述检查和原 TableScan，未伪造结果或静默回退。

## 原始检查与验证

原 `pg_class_query_protocol_e2e_test.py` 文件逐字节保持 HEAD 原值。
完整保留其简单/扩展协议、Describe OID、COUNT NULL、WHERE、别名、排序、LIMIT/OFFSET、
NULL 次序、错误码及只读事务检查，两个模式均到达原完成标记。
临时索引夹具保留所有原 COUNT、发布/删除、保存点回滚、事务回滚、DISCARD TEMP 检查，
并在每个原 COUNT 旁增加精确 relname/relkind/relpersistence 行值检查。
没有增加注册入口、修改原 SQL 预期、调整超时或削弱持久化设置。

两套实际不同的生产库分别完成以下十个完整入口，共二十个退出 0：
pg_class_query、temp_index_catalog、pg_settings_query、owned_type_catalog_query、
pg_catalog_unavailable_sqlstate、prepared_global_aggregate、ordinary_aggregate_order_keys、
aggregate_argument_expression、cte_prepared_owner、temp_alter_column_catalog。
逐项核对各自真实 footer；另核对 owned type 的 30 个原始结果和 ordered aggregate 的
190 条原始观察全部 compatibilityPass。全局聚合保留完整 55 项检查。

本项两套 Main TU 各实际 fresh 编译、链接一次；每套其余 57 个生产对象、127 个公共头、
编译器与参数均逐字节核对。未改共享头或存储格式；未声称 fresh 编译全部生产代码，
该候选阶段未声称正常增量构建或修复后全 1492；后续正常构建见末节。工作区构建包含原有未接受改动，
定向通过不代表接受它们。两个真实服务器均在私有测试数据目录中运行。

新建私有 PostgreSQL 18.6 实例实际通过两个完整最终夹具；确认版本 180006，
保持默认 fsync，测试后该自有实例正常停止 0。未连接、修改或停止既有用户/参考实例。

## 额外聚合探针的历史失败与后续 R196 修复

最初额外扩展探针还发现两个不同的聚合入场问题：

| SQL 形状 | PostgreSQL 18.6 | R193 候选（历史） | 修改方法 |
| --- | --- | --- | --- |
| `SELECT count(*)+1 FROM pg_catalog.pg_class WHERE relname='已建表名'` | 2 | 0A000，complex catalog aggregate | 检查已绑定全局聚合入场条件，不能让嵌套于算术目标的 COUNT 落回旧目录适配器。 |
| `SELECT count(*) FROM (SELECT relname FROM pg_catalog.pg_class WHERE …) AS c` | 精确子查询行数 | 42P01，把整个派生来源当作关系名称 | 让既有逻辑来源执行器保留并消费派生 AST；不得把 FROM 子树重新解释为表名字符串。 |

首版完整额外探针原文保存在下列 V2 目录中，PG 侧同一探针实际通过。
V1/V2 的两个 pg_class 失败日志、执行退出码均保留，不能改写为通过。
这些是新追加探针暴露的后续问题，不是本项原始目录来源反例的修复证明，也不是用户要求跳过的项。
R193 当时用未改动的完整原 pg_class 夹具验收，未将新表达式/派生表探针计为已修复。
后续 [R196](issue-review-r196-global-aggregate-source-admission.md) 独立处理聚合入场及派生 COUNT(*) 的 Describe，
将原额外探针完整加入永久夹具，并取得 PostgreSQL 18.6 参考、两套完整 36 入口和永久文件验证。
两条有限反例现在已修复，原 R193 失败证据仍保留；这不表示全部目录列或整个聚合族完成。
V1 的通用 footer 分类器还将四类实际退出 0 的入口错误标为 footer 不匹配；
V2 以每项固定 footer、190/30 条原始记录独立核对，没有把分类器错误当成产品 SQL 失败。

另一次最初参考实例配置尝试因公共客户端不接收 trust 启动而在 SQL 执行前失败；
原日志保留，自有实例停止 0，没有编译 Main 或运行产品查询。
随后只把新建参考实例配置为客户端支持的普通测试连接方式，未修改协议客户端或执行认证审计。

## 本机证据和边界

- 最终二十入口组合 receipt：`/tmp/dbms-r193-catalog-source-v3.a2_vl01i/completed-candidate-receipt.json`，SHA256 `20a3c939c3e06a4b4c342d56731341280c9438109ebc8352848850b094f6ccc3`。
  其中两次原 pg_class 是 V3 实际新执行；其余十八次复用 V2 完整已结束结果，源码/程序/日志 hash 不变，不冒称 V3 再执行二十次。
- V2 完整二十次运行：`/tmp/dbms-r193-catalog-source-v2.13rcmo_d/completed-candidate-receipt.json`，SHA256 `a216c05d5207a03e79ade3bfdca1b56250226e2d680637b779b504948924776f`。
- V1 fresh Main/原错误四次复现及完整候选运行：`/tmp/dbms-r193-catalog-source.pa5hrthk/completed-candidate-receipt.json`，SHA256 `30ddfc6e5cd33a9b1e61c54e07a02e196fc8cb39149ca045375ef65d15b33919`。
- 最初额外探针全文：`/tmp/dbms-r193-catalog-source-v2.13rcmo_d/strict-extra-aggregate-expression-full-fixture.py`。
- 安装与工作区字节保留：`/tmp/dbms-r193-install.y423pe4o/completed-install-receipt.json`，SHA256 `43155224939224bf6509e7ae3cfccbb7d57a5427e3a66ecb4cf2157a31224b53`。

这些 `/tmp` 是本机临时证据，不是可移植发布包。最终参考完整运行及原始日志路径由最终 receipt 引用。
提交只包含本项 Main hunk、临时索引夹具和自有文档；原有工作区 Main 改动逐字节保留。
R185、默认协议 search_path 错误码及其他用户已跳过项目不恢复。没有关闭原 273 项中的任何整项，
没有完整 TLS、sanitizer、native DML 目录来源或全部目录列证明。不 push，Actions 保持禁用。

## 后续正常工作区构建

R193 提交后的工作区实际执行原 `scripts/build.sh`，只 fresh 编译 Main 并链接，
重复构建零编译/零链接；其余 57 个生产对象、全部头、源码及暂存内容字节保持不变。
正常生成的默认 `dbms_main` 完整执行原 pg_class 与增强临时索引两个入口，均退出 0。
receipt：`/tmp/dbms-r193-root-normal.10u_a247/completed-normal-build-receipt.json`，
SHA256 `8e87469a18202cb5b6412ef32a91bc1be02d52659af4d699ef2ef7b219b75d5d`。

随后 R195 正常构建只更新 NetworkServer；该默认服务器再次完整通过这两个目录入口。
见 [R195 正常构建记录](issue-review-r195-aborted-begin-syntax-priority.md)。
两个正常构建阶段的历史对象/服务器已先保存副本，原 receipt 的历史 hash 不改写。
工作区原有未接受改动仍不因此接受；该次正常构建不能用来关闭额外聚合探针，后续修复见 R196。
