# R300：GCD/LCM 的声明、歧义与不可变内建规划

状态：完成本项的真实声明、前端接入及既有 primitive 数值范围内的内建常量规划。
不关闭整个数学函数兼容性、通用常量系统或原 273 条清单。

## 问题、影响与修改方法

| 问题 | 使用影响 | 本项方法与状态 |
|---|---|---|
| 缺少实际拥有的重载声明 | smallint/smallint、UNKNOWN/UNKNOWN 应报 42725，却按显示值或操作数推断成 int4 | 实际内建回调接入已有 DeclaredCore 所有权，声明 int4/int4→int4、int8/int8→int8、numeric/numeric→numeric；复用现有候选解析算法，已修复 |
| 错误类型、数量与 NULL 过宽接纳 | TEXT/REAL/DOUBLE/boolean/日期/数组和错误 arity 可能错误成功或延后报无关错误；NULL 不能掩盖缺失签名 | 按实际声明在分析阶段拒绝；合法混合类型及严格 NULL 保持选定重载的域，已修复 |
| 前端/无 DB 类型推断未共享声明 | 空表、未执行分支、参数推断、结果 OID/typmod 和角色诊断走旧入口 | 接入 Main/Network/query binding/ExprHelper，保留真正 callee 身份；仅对该数值声明家族归一化既有 ORDER 合成参数，不改 parser grammar，已修复 |
| 缺少内建 immutable 及相应规划路径 | GCD int4 溢出常量在零行查询里漏报 PG 的 22003 | 实际内建标记 i；既有规划入口仅接纳真实 builtin 身份、i 和 int4/int8/numeric 输出；UNKNOWN 临时 datum 按已选输入/输出域转换，已修复 |

OVER/DISTINCT/FILTER/ORDER 的非法角色在真实签名选择后、UNKNOWN 输入转换前
诊断；FILTER 名称/布尔类型仍按完整查询分析。opaque 替换保留自己的 arity、
返回域、i/v 与求值需求；真实 stored callee 不被按同名当成内建规划 datum。
没有修改共享候选选择算法、Catalog/TableManage/存储函数模块、输入 codec 或数值域。

## 冻结的完整验证

| 同一最终夹具 | PostgreSQL 18.6 | 旧 Root / 旧纯提交 | 新 Root / 新纯提交 |
|---|---:|---:|---:|
| 原生 1063 | 不适用 | 660 / 660 失败 | 0 / 0 失败 |
| 完整协议 5244 | 0 失败 | 3352 / 3352 失败 | 0 / 0 失败 |
| 补充原生规划 27 | C++ API 约束，另对照 SQL 行为 | 3 / 3 失败 | 0 / 0 失败 |

原生 1063 包含 431 个纯绑定/执行阶段控制、98 个无 DB 类型推断、512 个 typed row、
16 个 opaque 替换、4 个需求及 2 个 immutable 控制。完整协议保留全部原 353 条
观察，另含 typed NULL、真实行、空表/WHERE false/LIMIT 0、三种查询模式、参数
推断与四种 text/binary 格式组合、严格 metadata/datum、序列纯绑定和限定 routine。
签名预期由三组真实声明构造，不从产品输出提取；1063/5244 在生产修改前固定，
SQL、断言、预期和期限保持字节不变。

V1 候选两侧原生 1063 全通过，但完整 5244 各有 6 处零行 GCD 常量溢出失败。
这些失败原样保存，没有删查询或改 oracle。V2 补齐实际内建规划后，同一完整
5244 两侧均通过。范围错误断言要求 22003、无 DataRow/完成 tag 和 ReadyForQuery；
不将所有规划/运行时错误一概宣称为同一 Parse/Bind/Execute 阶段。

27 项额外验证实际 planning API、UNKNOWN 已选域、NUMERIC scale/NaN、NULL、
零行溢出、lazy CASE/COALESCE、volatile nextval、真正 IMMUTABLE 用户 routine
以及不可提前求值的列。C++ API 明确不取用户 routine 作为规划 datum，该所有权
检查不是宣称 PostgreSQL 不会规划用户 IMMUTABLE 函数。初版 raw createSequence
未发布 SQL 目录，两个 42P01 是夹具 setup 错误，非产品 bug；保留初版与日志，
改用真实 CREATE SEQUENCE 后，27 个预期/断言不变，旧两侧各剩 3 处真实规划失败。

原 353 条观察各侧 rows/SQLSTATE/OID 差异从 286 变为 0；R299 原 42 条重放也
全部一致，历史两个 smallint 歧义差异据此关闭。重复差异不计作独立 bug 数。
原 GCD/LCM 范围 4178/2133、类型 63/61、SCALE 124/416、MIN_SCALE 6112/602、
原 binary-zero 230/431、EXECUTE 419、prepared identifier 226 等相邻完整入口保留。

## 构建、门禁与边界

正常有限 76 入口：74 通过，原 MIN_SCALE 两个完整 602 入口仍各有两个已暂缓
R271 binary NUMERIC dscale=1000 失败；另有 4 个专项 UBSan 入口通过。即 80 个
有限记录中 78 个 exit 0、2 个真实 exit 1，不是全绿或全 1595 库存运行。
原始 353/42 的四侧重放另作为观察证据，不混计为独立入口。

两侧 12 个生产 TU fresh（最初五 CPP 共 10 个保持当前源码，另 fresh Planner 2），
110 个生产对象与 2 个 stub 按源码/头/依赖/工具链匹配复用。4 个新 native TU、
6 个真实 Main TU fresh；38 个未变 native TU 匹配复用，48 个测试程序与两台服务
器均重新链接。生产 Main O2，Main 测试保持 window O2、cleanup/session O0。
共享头未改，仍用当前 R293 AST/已接受 R290 Stats；不接受暂缓的 R291 候选头。
UBSan 只覆盖两侧 Evaluator、Planner 及两个新原生夹具共 8 个 fresh TU/4 个入口，
其他库正常匹配，Main 未 sanitizer；不能称全库 ASan/LSan/TSan/TLS 验收。

只扩展已有 int4/int8/NUMERIC 内建规划，不扩展 R156 的完整类型化常量、REGTYPE、
呈现或 JSON 身份；也不关闭 R160 其他函数的规划问题。R298 TRIM_SCALE、R271、
R294、R291、R288/R99、R222、R51、R267 和所有用户明确暂缓范围保持暂缓。
原总账仍 22 complete / 166 partial / 70 unverified / 15 deferred，尚未整体完成。

只接受 tested pure projection，保留 Root 其他草稿、11 个隔离映射、十八处暂存
差异及冻结 1492 历史证据。仅本地独立 commit，不 push，GitHub Actions 继续禁用。

```sh
bash scripts/build_one_test.sh gcd_lcm_signature_test
bash scripts/build_one_test.sh gcd_lcm_constant_planning_test
DBMS_MAIN=/path/to/current/server python3 tests/gcd_lcm_signature_protocol_e2e_test.py
```

临时证据目录 `/tmp/dbms-r300-gcd-lcm-signatures.agCHklKf` 不保证长期保存。

| 收据 | SHA-256 |
|---|---|
| completed-preflight-receipt.json | `62d4353fffbec92e66fe35836b9d3c7d0c559dbdd01ccd38de9e3911e3e82ff4` |
| completed-observations-receipt.json | `d078b2d3aa657a58b031108d894c946e091c680f3a78f613bcc5c800309d170c` |
| completed-fixtures-frozen-receipt.json | `4ee63107f5675dc85095df651598946c7169edab04d54d2a7f58477d6ee10229` |
| completed-old-reference-receipt.json | `3fd46315b68cb5af830e5a9c332930a86465a9283ddd6497ff5850a4a144f845` |
| completed-early-primary-receipt.json | `8f66f8ecbdc522d47fc978375a17170451ee1fdb8c6a8f24f42873eb0b52ade6` |
| completed-planning-fixture-setup-correction-receipt.json | `fd3cb2019314548d3ebbeac7b0752f7ca1b4759c9569cb42545e8affa4d940f1` |
| completed-old-planning-v2-baseline-receipt.json | `1bb4b505770c9f90bfafa9e27d76712dc90150b621d7eca78fcfe5a17c640dda` |
| completed-v2-primary-build-receipt.json | `928e8610b748f6d30fac3f8da8c91e1761369941839921d1ac592bbed5f43051` |
| completed-six-main-test-objects-receipt.json | `8de8215788963820f2ceabf0232524858d6556a98cf8bd821dcead2aa547a10e` |
| completed-early-v2-primary-receipt.json | `5368cd3513a7e1492c7de77083793b131e65cad140eaa7023a390be6024d0e92` |
| completed-regression-build-receipt.json | `0ec12ef92fb4a3447f406e3f20a34e302df6a64c878d2b85d1f2ed9f24039dbc` |
| completed-candidate-receipt.json | `511279221987ac5d80c10da6e143949cb1741ccefe94ba7fa6fd849c5871a62e` |
| completed-scoped-ubsan-receipt.json | `7fecc7b3971ae03d86247b867cdfeaa44fa941fd0d826780ae2ff92375a7c0a7` |
| completed-post-observations-receipt.json | `5b5b8281e356d461cb1b2e836bc6645411e40bf183a4e314c007885aa0dd4a8f` |
