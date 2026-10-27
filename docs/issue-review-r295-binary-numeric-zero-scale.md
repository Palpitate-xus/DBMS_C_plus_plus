# R295：二进制 NUMERIC 零值丢失显示小数位数

状态：完成有限解码修复；不是 NUMERIC 全部功能完成，也不是完整协议夹具全绿。
R294 按用户要求暂缓，不恢复 R271 的无约束数值范围或其他暂缓模块。

## 问题、影响与方法

| 问题 | 实际影响 | 修改方法 |
|---|---|---|
| `decodePostgresNumeric()` 在 ndigits=0 时直接返回 `"0"`，丢弃 dscale | 二进制 Bind 传入 0.00 后，查询、转 TEXT、二进制返回及列写入错误变为 0；文本输入原本可保留小数位数 | 在原有 scale/长度/符号校验之后，按 dscale 构造 `0` 或 `0.` 加对应数量的零；不改非零或特殊值路径 |

只修改 `src/network/PostgresNumeric.cpp` 的零幅度分支，仍保持现有 scale 0..1000
准入。符号为负的零归一为正零，零的 weight 不影响结果，返回 binary 时 weight=0。
不提高 `Numeric::kMaxPrecision`，不改变声明、存储格式、运行时范围或超限准入。

## 固定夹具与实际结果

原生 `tests/numeric_binary_zero_scale_test.cpp`：230 项，覆盖 7 个 scale、5 个
weight、正负零、精确文本和 canonical binary 往返、Numeric 值、既有错误准入及
非零/NaN/Infinity。旧两侧均 180 失败；新两侧及各自专项 UBSan 均 0。

完整真实协议保存在 `tests/numeric_binary_zero_scale_known_gap.py`，手动运行：

```sh
DBMS_MAIN=/path/to/current/server python3 tests/numeric_binary_zero_scale_known_gap.py
```

这是明确会返回非零的已知缺口夹具，没有冒充已通过入口注册进默认 E2E 列表。
新原生夹具按既有 `*_test.cpp` 自动发现。协议完整保留 431 项，精确检查 Bind、
RowDescription/ParameterDescription、DataRow 字节、SQLSTATE、tag、ReadyForQuery，
包括 text/binary 结果、TEXT 转换、NULL、非零控制、写入后读取及连接恢复。

| 相同最终夹具 | PostgreSQL 18.6 | 旧 Root / 旧纯提交 | 新 Root / 新纯提交 |
|---|---:|---:|---:|
| native 230 | 不适用 | 各 180 失败 | 各 0 失败 |
| 完整 wire 431 | 0 失败 | 各 384 失败 | 各 141 失败 |

修复后 141 个失败全部来自独立的 [R296 缺少 scale(numeric)](issue-review-r296-numeric-scale-function.md)：
140 个参数查询和 1 个列查询均为 42883。其余完整 290 项断言通过；不能把整项
431 写为通过，也不删除 SCALE 控制、替换 SQLSTATE 期待或提升超时凑绿。

首次驱动将实际 431 项误计为 435，且直接 RETURNING 列被错按无表来源字段检查；
原始失败日志及 v1/v2 源码均保留。最终写入控制使用表达式 `RETURNING v::text AS v`，
并用独立表名避免参考端重跑残留。最终同一 230/431 夹具均在旧/新两侧运行，
参考端完整 431 全过；这些测试驱动修正不是产品修复。

## 验证范围和版本管理

本轮正常有限 32 个入口：30 通过、2 个完整 wire 因 R296 返回 1；另有 2 个
230 项专项 UBSan 入口通过。16 个 native、6 个真实 Main、8 个既有 wire 通过，
没有隐去另外 2 个已知缺口 wire。不是全 1586 入口、原 273 条、TLS 或全库 sanitizer 验收。

头文件/Main 均未改。两侧共 fresh 编译 2 个生产 TU、16 个 native TU；120 个
生产对象、2 个 stub、6 个真实 Main 对象核对源码/头/依赖/工具链后复用，未称 fresh。
2 台服务器及 22 个测试程序均新链接。UBSan 只覆盖 codec、Numeric 和新原生夹具，
两侧共 6 个 fresh TU，不把它描述成整个数据库或 Main 的 sanitizer 验收。

Root 的其他草稿、R291 新头/候选、原十八处暂存 diff 均原样保留；提交只接受
tested pure projection。原冻结 1492 历史失败、R291 CHECKPOINT 原因和所有用户暂缓项不变。
原 273 状态仍 22 complete / 166 partial / 70 unverified / 15 deferred，总目标未完成。
仅本地独立 commit，不 push；GitHub Actions 继续禁用。

证据目录 `/tmp/dbms-r295-binary-numeric-zero.89mEPmcr` 不保证长期保存。

| 收据 | SHA-256 |
|---|---|
| completed-preflight-receipt.json | `7bfaafefa68bd0f1dd32fc55460d208aa004790b351998c3905de27a35b6cb8d` |
| completed-v3-old-reference-receipt.json | `abc68de87abff77e9b1aaf2e3ac6b646d0a1cd6bb16be6bb1fffbc9228fec246` |
| completed-old-native-receipt.json | `009671c480812ad3fa410ab7ef3b96f9edd9aecd12eb5bdac975d772a963edad` |
| completed-build-receipt.json | `543e8615cc0d58e062b3463faf706ad883aaaa465d9210d9e19a77885eab2166` |
| completed-candidate-receipt.json | `2c67f659d7dc6c5fb1cadb076d6db8d50935b11aac622efebc0259b4259467a4` |
| completed-scoped-ubsan-receipt.json | `94ced34db0e933d2bb92d70877810cfadd81ed12ccd4f54aed43669d1e557584` |

## R296 后续闭环

上文 141 个失败/未注册说明保留 R295 提交时的真实历史。随后 [R296](issue-review-r296-numeric-scale-function.md)
补齐 scale(numeric) 声明及实际前端接入后，本文件引用的原完整 431 项夹具在两侧均
实际全过；源码、SQL、断言和期限未改，文件名保留，现在按原路径注册到默认 E2E
列表一次。R295 当时的失败日志和非零收据不改写为成功，也不由此关闭其他 NUMERIC 缺口。
