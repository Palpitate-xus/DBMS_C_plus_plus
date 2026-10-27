# R299：GCD/LCM 整数结果范围

状态：完成 int4/int8 已选定重载的结果范围修复；不关闭整个函数兼容性或原 273 条总清单。

## 问题与方法

| 问题 | 使用影响 | 修改方法与状态 |
|---|---|---|
| GCD 只检查 int64 最大值 | `gcd('-2147483648'::integer,0::integer)` 返回 2147483648，超出声明的 int4 | 根据实际结果域选 int32/int64 上限；超限报 22003，本轮修复 |
| LCM 只检查 int64 最大值 | `lcm(2147483647::integer,2::integer)` 返回 4294967294；binary 输出还可能转为无关的 0A000 | 在 unsigned 128 位乘积上检查已选结果域的上限，编码前报 22003；本轮修复 |
| 两个 smallint 实参的重载歧义不一致 | PG18 的 `lcm(32767::smallint,2::smallint)` 为 42725，产品却选择 int4；另一个负数 CAST 控制也有不同错误优先级 | 独立 OPEN 问题，尚未修复。后续检查真实候选集合、隐式转换与相同类别优先级；需保留 int4/int8/numeric、native replacement 和真实 routine 归属，不用名称特判补错误码 |

生产只修改 ExprEvaluator.cpp 中两处结果上限。保留先处理 LCM 零值和严格 NULL、
安全计算 INT_MIN 绝对值、Euclid 算法及 unsigned 128 位中间乘积；不改数值域、
参数/函数签名、原生替换回调、Main、parser、共享头或协议编码器。
合法的 bigint 结果不被 int4 上限误拒绝，例如 bigint 输入的 4294967294。

注意负数测试必须使用正确的输入身份：`'-2147483648'::integer` 与
`-2147483648::integer` 不同；后者先把正数 2147483648 转 int4，会在函数运行前
报 22003。原 24 条观察未删改；扩展为 42 条保留这些 CAST 阶段控制，并补上
正确的负数输入。旧两侧各 12 处 rows/SQLSTATE 差异；新两侧各剩 2 处 smallint
歧义相关差异，不能称为全部 42 条通过。这些重复模式不是 12 个独立 bug。
SIGN(float8 NaN) 的疑点亦被排除：实际 PG 与两侧产品都为 0，没有改写正确行为。

## 固定夹具与验证

| 同一夹具 | PostgreSQL 18.6 | 旧 Root / 旧纯提交 | 新 Root / 新纯提交 |
|---|---:|---:|---:|
| 原生 4178 | 不适用 | 200 / 200 失败 | 0 / 0 失败 |
| 协议 2133 | 0 失败 | 194 / 194 失败 | 0 / 0 失败 |

原生以无符号 std::gcd 和 128 位独立预期验证正负边界、INT32/64_MIN、
int4/int8/mixed 域、零/NULL 与 opaque 替换回调。协议预期由 Python 无界整数和
math.gcd 构造，覆盖 Simple/Extended/binary、精确 OID/width/typmod/来源、原始
int4/int8 binary datum、四种输入/输出格式组合、参数重用、真实行、空表/零行、
溢出后的连接恢复和私有 schema 清理。固定前后不改 SQL、预期或期限。
协议错误断言覆盖真实 SQLSTATE、无 DataRow/完成 tag 和 ReadyForQuery；常量
可能在 Bind 规划时报错、变量在 Execute 报错，不宣称本项统一了所有错误阶段。

新原生自动发现；新协议仅注册一次。旧 GCD/LCM 类型、projection、math errors、
SIGN NULL/NaN、共享签名、SCALE 124/416、MIN_SCALE 6112/602、原 binary-zero
230/431、EXECUTE 419、prepared identifier 226 等完整相邻入口保留。
正常有限 70 入口：68 通过，MIN_SCALE 的两个完整 602 入口仍各有两处已暂缓
R271 binary NUMERIC dscale=1000 失败；另外两个新原生专项 UBSan 入口通过。
即 72 个有限记录中 70 个 exit 0、2 个真实 exit 1，不是全绿，也不是全 1592
库存或原 273 条验收。上述 42 条观察另计为采集证据，仍有两个 OPEN 差异。

两侧 fresh 2 个生产 Evaluator TU，120 个生产对象和 2 个 stub 按源码/头/依赖/
工具链匹配复用；fresh 8 个 native 测试 TU，36 个未变测试 TU 匹配复用（含
6 个真实 Main 测试对象），2 个服务器和 44 个测试程序全部重新链接。
Main 和共享头未改，未复用与当前 Main/头不匹配的对象；Stats 头仍为已接受的
R290 版本，不接受已暂缓的 R291 候选。UBSan 只覆盖 Evaluator 与新原生夹具
共 4 个 fresh TU，其他库正常匹配，Main 未 sanitizer；不能称全库 sanitizer。

## 版本管理与未完成项

本项只提交 tested pure projection 的生产文件、注册、新原生/协议夹具和本文。
Root 的其他草稿、原十八处暂存差异、11 个隔离映射和冻结 1492 历史证据保留。
R298 TRIM_SCALE、R271 输入域、R294 语法、R291 发布及所有用户暂缓范围不重开。
smallint 歧义问题仍 OPEN；先补完整 PG 对照再单独修复和 commit。
原总账仍为 22 complete / 166 partial / 70 unverified / 15 deferred，总目标未完成。
只做本地 commit，不 push，GitHub Actions 继续禁用。

可重跑本项：

```sh
bash scripts/build_one_test.sh gcd_lcm_integer_range_test
DBMS_MAIN=/path/to/current/server python3 tests/gcd_lcm_integer_range_protocol_e2e_test.py
```

临时证据目录 `/tmp/dbms-r299-gcd-lcm-int4.pCa64nh2` 不保证长期保存。

| 收据 | SHA-256 |
|---|---|
| completed-preflight-receipt.json | `5c349588bf26b3f5ffa8a3148f5a3c05148ca38fee46d31acd491e32dbf8c67c` |
| completed-observations-receipt.json | `e8e2122d53e449b7370a4204607e2eefcb8925e151cf1d0aa275efdfc1a5a946` |
| completed-expanded-observations-receipt.json | `e0c680d432b77b010674cbe77701bd9ae03f79b17cd415026f30d483d2fcf51f` |
| completed-fixtures-frozen-receipt.json | `3fb31fea3af02bdd806b1ae446b6b1c272dc15285098353167ea6b3fee299454` |
| completed-old-reference-receipt.json | `a71ec8a9b22c72caab4e48176a5b12a73106ecb402df346ec3040cc68778e42b` |
| completed-primary-build-receipt.json | `d32642853b70690228bbf6134c0d88a89a8c24adffa2924600635570ac5b9c26` |
| completed-regression-build-receipt.json | `e0c899685885bb11e5d848528e91af0c12275ccc3b5cdab77b5ffc5d402f8649` |
| completed-candidate-receipt.json | `92ba7d3ebb3183758637f83607a03fe839b4f799535f685931c01b7960d25107` |
| completed-scoped-ubsan-receipt.json | `5ec3b2070c9506a998eb212a13cbaa10a691636161c0ad1058237342184ad213` |
| completed-post-observations-receipt.json | `78d5cf051e2865537480a19adfdb5dd13976ac97541581e22b765bdc75586ec2` |
