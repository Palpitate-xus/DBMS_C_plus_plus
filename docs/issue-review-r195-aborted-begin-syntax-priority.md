# R195：中止事务中的非法 BEGIN 必须保留原始语法错误

状态：扩展协议 BEGIN 语法优先级反例已修复；不是所有协议错误优先级的完整验收。

## 复现与原因

原 1492 全量的 `transaction_begin_grammar_protocol_e2e_test.py` 在中止保存点内
再次执行非法 `BEGIN READ COMMITTED` 时，预期 42601，实际收到 25P02。
完整原夹具加诊断上下文的独立观察确认失败发生于扩展协议，简单协议已经正确。
PostgreSQL 18.6 的同一完整原夹具实际通过，不能直接把预期改成 25P02。
R193 后的两套当前服务器也各完整复现一次相同失败。

扩展 Parse 先调用 `parseForBinding`，但旧事务语法解析失败只设置 error/success，
不一定填写 `ParseResult.sqlState`。网络层只转发显式 42601，遗漏了这些实际语法错误，
随后由中止事务状态门返回 25P02，错误优先级因此改变。

## 修改方法

在原 Parse 语法检查中识别 BEGIN、START TRANSACTION、SET TRANSACTION 的语法身份。
当解析失败且旧结果 SQLSTATE 为空时，按原始语法错误返回 42601。
显式 42601 保留原路径，其他显式 SQLSTATE 与不属于该事务语法的未支持形式不被覆盖。
判断仅做词法/语法检查，不绑定、启动、执行事务或求值表达式。

合法 BEGIN 在中止块中仍返回 25P02；不会重新开始事务或发出 nested-BEGIN Notice。
原保存点恢复、父事务行存活、最终 ROLLBACK、ReadyForQuery T/E/I、无成功命令标签及
无 Notice 等检查全部保留。未修改任何原 SQL、错误码、超时或持久化设置。

## 回归与参考

完整既有夹具保留四种起始前缀、13 种非法选项、11 种合法选项以及简单/扩展协议。
新增在已中止块中验证 START TRANSACTION 与带前导注释 BEGIN 的相同 13 种非法选项，
两个协议模式共 52 项，精确检查完成计数。失败诊断另显示实际协议模式与期望 SQLSTATE。
每种模式仍实际 ROLLBACK TO parent，检查只留下父级 id=1，再最终回滚为空表。

两套候选各完整执行以下十个入口，共二十个实际退出 0，并核对每项固定完成标记：
transaction_begin_grammar、transaction_begin_error_state、set_transaction_modes、
savepoint_read_only、transaction_chain_read_only、transaction_chain_block、
transaction_isolation、show_transaction_isolation、subtransaction_error_lock_release、
dml_lock_error_savepoint。两侧均实际完成原夹具及新增 52 项。

新建私有 PostgreSQL 18.6 实例完整执行相同原检查和新增 52 项，实际通过；
永久夹具落盘后另一次私有 PG 完整参考也通过。版本检查为 180006，保留默认 fsync，
每个自有 PG 实例完成后正常停止 0，未连接或修改既有用户/参考服务。

候选两侧各 fresh 编译 1 个 NetworkServer TU 并重新链接，分别核对其余 57 个生产对象、
127 个公共头、stub、编译器和参数。没有共享头或存储格式修改，未声称 fresh 全部生产代码。
正常工作区先为 R193 实际编译 1 个 Main 并链接，重复构建零编译/零链接，两个完整目录入口通过；
本项落盘后正常构建再实际编译 1 个 NetworkServer 并链接，重复也零编译/零链接。
实际默认 `dbms_main` 随后完整通过 BEGIN+52、set_transaction_modes、
subtransaction_error_lock_release、pg_class_query、temp_index_catalog 五个入口。
两次正常构建均在本项提交前进行，按实际源码 hash 绑定；不是纯提交源码的正常构建证明。

最初落盘时，新增诊断断言的缩进生成错误被字节一致性守卫在编译/运行前拦截；
按已通过候选原文校正后才继续构建，没有删除检查或将该准备错误计为产品 SQL 失败。
旧可变 Main/NetworkServer 对象与默认服务器在每次正常构建前均保存副本与原 hash。

## 证据与边界

- 完整原参考：`/tmp/dbms-private-pg18-full-fixture.sdp0gml2/completed-reference-receipt.json`，SHA256 `ec81ca5aea6812dbd25c4e96d3d3b5e95f668eb59d4a633daf2cb443211d4948`。
- 原始协议模式诊断：`/tmp/dbms-begin-aborted-parse-observation.gl822y0u/completed-observation-receipt.json`，SHA256 `9e824049f127a3eb0bf5d9a50ec740f57401cc0a06fb9eeb3b06af101de7fa5e`。
- 两套完整二十入口、旧红及 PG+52：`/tmp/dbms-r195-begin-w83lble0/completed-candidate-receipt.json`，SHA256 `797b6cd8a145308c8b1709844f626aea0ba1fac5d438efc62426f4dd75e575e6`。
- R193 正常 Main 与两个目录入口：`/tmp/dbms-r193-root-normal.10u_a247/completed-normal-build-receipt.json`，SHA256 `8e87469a18202cb5b6412ef32a91bc1be02d52659af4d699ef2ef7b219b75d5d`。
- 本项正常 NetworkServer、永久 PG+52 与五个入口：`/tmp/dbms-r195-root-normal.pv_j29v8/completed-normal-build-receipt.json`，SHA256 `9769ef6b0118891da3af8901aceb03cbcda3541a664fa966ed9bc8d74d8ba6d1`。

这些 `/tmp` 文件为本机临时证据，不是可移植发布包、完整 TLS 或 sanitizer 结果。
原冻结版本 1492 的失败不被定向结果改写，未运行修复后完整 1492，也未关闭原 273 项要求。
工作区原 NetworkServer 改动通过反向移除本项单一 hunk 逐字节核对，暂存的 18 个 R185 路径保留。
用户跳过的默认协议 search_path 错误码、R185 及其他暂缓分支不恢复；不 push，Actions 不启用。
