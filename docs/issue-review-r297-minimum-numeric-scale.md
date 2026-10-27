# R297：min_scale(numeric) 有限实现与暂缓的 binary1000 边界

状态：完成 MIN_SCALE 的声明、精确计算与现有前端接入；完整协议仍有用户暂缓
R271 的输入边界失败，不能称为整批全通过。原 TYPE-01/FUNC-01 与总 273 条不关闭。

## 问题、影响与修改方法

| 问题 | 实际使用影响 | 方法与本轮状态 |
|---|---|---|
| 缺少 min_scale(numeric) | 字面量、列、text/binary 参数报 42883；不能查询去除小数尾零后的最小 scale | 在实际内建回调拥有的共享声明中加入唯一 NUMERIC 输入/int4 输出；从已解析 Numeric 值的规范表示检查小数末尾，不经 double，也不从 SQL 原文猜测类型；本轮已实现 |
| 声明与前端分析入口需要同时接入 | 空表、未执行分支、参数推断、无 DB 的类型推断和调用角色不能只依赖执行阶段 | 复用 SCALE 已有的 Simple/Extended、query binding、ExprHelper 归属与错误阶段入口；参数 OID 1700、结果 OID 23/width 4；本轮已接入 |
| 非零 binary NUMERIC dscale=1000 被既有解码预算拒绝 | 同值 text 成功、binary 为 22P03，PG 成功；本轮完整 602 仍各有两个失败 | 用户暂缓 [R271](issue-review-r271-numeric-runtime-domain-deferred.md) 的输入域/数字预算统一；未来区分有效数字、显示 scale、前导整数零，联合检查解析/运算/存储/协议。本轮保留证据和断言，不实施、不绕过此限制 |

零值的最小 scale 为 0，而不是其 display scale；有限值去掉小数部分的尾零，
整数末尾的零不参与缩小。NULL、NaN、正负 Infinity 返回 int4 NULL。整数可隐式
转 NUMERIC；TEXT、REAL、DOUBLE PRECISION 等不匹配。PostgreSQL 18.6 源码的
pg_proc OID 5042 / numeric_min_scale 与真实服务器行为相互核对，不声称产品函数目录全集完成。

只改五个 CPP（Evaluator/Main/Network/query binding/helper），不改共享头、AST、
解析器语法、Numeric 上限、PostgresNumeric 解码或存储格式。实际 builtin 标记拥有
新声明；同名 registerFunction 原生替换保留自己的参数个数、结果域和 volatility。
TRIM_SCALE 是后续独立项；R271/R294、安全/TDE和其他所有用户暂缓范围均不恢复。

## 同一完整夹具的真实结果

| 夹具 | PostgreSQL 18.6 | 旧 Root / 旧纯提交 | 新 Root / 新纯提交 |
|---|---:|---:|---:|
| 原生 6112 | 不适用 | 6075 / 6075 失败 | 0 / 0 失败 |
| 完整 wire 602 | 0 失败 | 558 / 563 失败 | 2 / 2 失败 |

原生覆盖 124 个描述符、类型推断、错误阶段、native typed row、opaque callback、
实参需求与 immutable 控制，以及 0..1000 全部 1001 个零值、4987 个极小数/尾零
组合。完整 wire 覆盖三种查询模式、空表/未执行分支、错误优先级、真实 metadata、
text/binary NUMERIC、NULL/特殊值、参数推断、序列纯绑定和错误后的连接恢复。
原生 6112 在旧/新两侧使用同一 C++ 夹具；协议 602 在 PG 及旧/新两侧使用同一
Python 夹具。SQL、断言和期限不变，不将重复失败数量称为独立 bug 数。

602 中其余 600 项断言通过；两处失败均是非零 binary dscale=1000 的 Bind 输入
22P03，仍按 PG 正确结果作为预期，没有改成产品错误码以凑绿。完整夹具保存在
`tests/numeric_min_scale_function_known_gap.py`，手动运行仍真实 exit 1，未注册为默认
E2E 假通过入口；新原生按既有 *_test.cpp 自动发现。

```sh
DBMS_MAIN=/path/to/current/server python3 tests/numeric_min_scale_function_known_gap.py
```

独立边界对照使用 R297 修改前的已接受 R296 两台服务器及原有 SCALE($1)，无需
MIN_SCALE：text 999/1000、binary999、binary零1000 全成功；两个非零 binary1000
输入均 22P03，PG 八个控制全成功。证明该限制不是新 MIN_SCALE 引入，不倒推它已解决。

## 构建、验证与版本管理

正常有限 58 入口：56 通过、上述两个完整 602 wire 入口非零；另有 2 个 6112
专项 UBSan 入口通过。30 native、6 真实 Main 及原 20 wire 保留；新 602 不被过滤。
原 SCALE 124/416、原 binary-zero 230/431、EXECUTE 419、prepared identifier 226
及其他既有数值/签名相邻回归均保留。不是全 1590 入口或原 273 条总清单验收。

两侧 fresh 10 个生产 TU，112 个生产对象与 2 个 stub 核对源码/头/依赖/工具链后
复用；30 个 native TU 和 6 个真实 Main TU fresh，2 服务器/36 测试程序均新链接。
生产 Main O2，Main 测试沿用 window O2、cleanup/session O0。UBSan 仅覆盖两侧
Evaluator 和新原生夹具的 4 个 fresh TU，其余库正常，Main 未 sanitizer，不能称全库/TLS/ASan/TSan 验收。
共享头仍为已接受 R293 AST/R290 Stats，不接受暂缓的 R291 候选头、对象和测试。

提交仅接受 tested pure projection；Root 其他草稿、原十八处暂存 diff 原样保留。
11 个分支隔离映射保留，只有 Main/Network/query binding 的本项 SHA 随修复更新。
冻结1492历史失败、R291 CHECKPOINT 原因和全部用户暂缓项不借此关闭；原273状态
仍 22 complete /166 partial /70 unverified /15 deferred。总目标未完成。
仅本地独立 commit，不 push，GitHub Actions 继续禁用。

证据目录 `/tmp/dbms-r297-minimum-numeric-scale.q8gXuvAF` 不保证长期保存。

| 收据 | SHA-256 |
|---|---|
| completed-preflight-receipt.json | `03d8e43b4b9b390c9e255147b9eb6f71c03c4fb4d5b80bfbb4fedcf85c0afd04` |
| completed-observations-receipt.json | `670ca1af93a38de093d3b8b53a1492117b3fa96ee0dbe3d9143a1d2399cf3f25` |
| completed-fixtures-frozen-receipt.json | `85f30a6d397c2ca58aa39a69809319e8c330816a28d5468bb5c68f8d44dbb88e` |
| completed-old-reference-receipt.json | `f08341b26aa8fb94fcd635e7ad4b6e0fc17a8fe331735f7360dcbc9d5f05e672` |
| completed-existing-boundary-observations-receipt.json | `3b061c908c38316c4880bf9202d7a982563c9584a3f125215998971e54c38203` |
| completed-primary-build-receipt.json | `555314775fde9662836a93f226c3bf45d117849323f894a4270ab2fca3a29822` |
| completed-early-primary-receipt.json | `33844daa4234d76c9778c6f25b6986bd6d098799a9068b75e98486ecf7e3954b` |
| completed-regression-build-receipt.json | `91c25d072ebbe826b1dc14d85acf86567b7f7c90b0ae9f3cab8abda9dd889988` |
| completed-candidate-receipt.json | `ccd00ec372689f61ddd91beb133c77dab6aa31e25fc8ea267b5ec3eaba7051fa` |
| completed-scoped-ubsan-receipt.json | `d28515ede3928078a35fb755dad703ab5211820b548e802611d8d02437d43a63` |
