# O05：ORDER BY 重复输出别名的绑定与实际值 owner

旧 binder 找到第一个同名输出便返回；legacy 别名 map 则覆盖成最后一个。
不同表达式因此可能静默选择不同列排序，空输入或 LIMIT 0 也隐藏了歧义。
例如下列语句应在读取任何行前返回 42702，而不是挑一个 x：

```sql
SELECT id AS x, v AS x FROM example ORDER BY x;
```

重复名字本身不是错误：同一已分析表达式的两个输出允许 `ORDER BY x`。
它们必须共用该排序输出的实际值；否则 `nextval()` 等 VOLATILE 表达式
会多执行一次。没有引用该名字的 ORDER BY 时，两个输出仍是独立求值站点。

## 修复方法

纯 binder 按 expanded projection ordinal 检查所有同名输出，星号展开的
列也保留真实 source occurrence/column ordinal。比较使用绑定表达式和
实际 routine metadata，不读取第一行、不比较显示值，也不比较原始 SQL。
同一物理列的限定/非限定引用及无实际转换的 builtin cast 可以相等；不同
来源即使每行值相同，也不能视为相等。不同类型、NUMERIC 显示 scale、
子查询输出声明和不同 parsed window 定义继续区分。

真正 UNKNOWN 字符串的 builtin 输入转换是分析期 typed Const，不是执行
用户函数或做代数化简。`1` 与 `'01'::integer` 可以相等；`1+1` 与 `2`
不能通过当前值相等取得同一表达式身份。裸 UNKNOWN 与显式 typed TEXT
也不能在排序名字分析前随意合并。

PreparedQuery 保存每个实际输出的 sort owner。Sort/Project 和常量规划
消费该映射，排序键引用第一个 owner，不再重新选择最后一个重复输出。
普通重复别名查询走 retained typed query；legacy 窗口复用原输出映射，
不重复计算同一窗口值或执行其参数。其它重复目标不自动共享。

该规则仅属于裸输出名字引用；`ORDER BY x+0`、`x COLLATE "C"` 等表达式
中的 x 按输入命名空间查找。序号 ORDER BY 2 仍可明确选择第二个输出。

## 验证记录

证据目录 `/tmp/dbms-review-fix-donor.N9sdas6f/`。原版本 `c71a7526` 的冻结
程序 `root-before-o05.frozen` SHA-256：
`2659dae4c85dcd797aa9e55cd410cdcfe17a580fb34e99eb94780ea025302ab1`。

| 检查 | 实际结果 |
| --- | --- |
| 原版本 79 协议控制 | 56 失败，exit 1；`o05-original79-protocol.log` |
| 原版本 107 协议控制 | 75 失败，exit 1；`o05-original107-protocol.log` |
| PostgreSQL 18.6 / 180006，107 控制 | 全通过，exit 0；`o05-final107-postgres-reference.log` |
| 扩展 121 控制，PG18.6 | 全通过，exit 0；`o05-final121-postgres-reference.log` |
| 当前 O05 117 控制，PG18.6 | 全通过，exit 0；`o05-final117-postgres-reference.log`，窗口 SUM 表达式参数四控制转入独立 R45，不放宽预期 |
| 原版本最终 O05 117 控制 | 85 失败，exit 1；`o05-original117-protocol.log` |
| 当前 native74 诊断 | 全通过，exit 0；`o05-second-native-diagnostic.log`，57 个 native 生产输入按当前头/flags/source 核对，两变化单元实际隔离重编；不是最终 Main 验收 |
| 当前标准输入 native74 | exit 0；`o05-second-native74.log`，全部58生产receipts及终态输入一致；当时协议仍有四项命名窗口失败，不混称全部通过 |
| 最终逻辑的 O0 Main 诊断 | 117 控制及16完整协议全0；`o05-third-o0-diagnostic117-protocol.log` / `o05-third-o0-diagnostic-protocols.log`，57库对象为当前标准配置；不是最终O2生产验收 |
| Root 最终 O2 生产构建 | exit 0；`root-o05-third-candidate-build.log`，Main实际重编并标准链接 |
| Root 最终 74 native 及十三完整原生邻居 | 十四完整驱动exit 0；`o05-final-native.log`，全部58 receipts及终态输入一致，含R04 native349及R07 native122 |
| Root 最终 O2 117 协议控制及十五完整邻居 | 十六完整驱动exit 0；`o05-final-protocols.log` / `o05-second-*_test.py.log`，包含完整 postgres_protocol、R04 307/78、R07/R33/R44、逻辑解码与EXPLAIN JOIN |
| 文档/注册/语法检查 | 469 Markdown UTF-8/本地文件链接/inventory、480唯一注册/463 standalone、499 Python AST/13 shell语法全通过；文档状态/版本/兼容声明及六个总账单元测试通过 |

协议逐项核对行、NULL、列名、OID、SELECT 行数和 SQLSTATE；覆盖普通列、
JOIN 来源、星号、子查询、builtin 声明、窗口定义、空输入及 LIMIT 0。
序列分别检查歧义分析的零副作用、相同排序输出只执行一次、LIMIT 对
sort-key 的需求，以及没有 ORDER BY 时仍独立求值。窗口 LAG 的 VOLATILE
参数和 SUM 的 frame 初始化也核对次数。初始 SUM 表达式参数控制实际发现
新缺陷 R45，保留全部四项预期并转为独立回归，在 O05 后继续修复，不把
NULL/TEXT 改成通过。native 驱动还检查真实输出
ordinal→owner 映射及纯准备不写入源表。

初始 PostgreSQL 64 控制有两项预期错误：把不同 raw ORDER BY 限定语法
的两个 row_number 认为是同一 window；实际 42702，保留
`o05-first64-postgres-reference.log`，调整并扩展后最终全通过。
首批 fresh58 暴露 ExprHelper 头文件缺少 SelectStmt 前置声明，保留单元
编译错误；已修正并停止该自有构建批次（exit 143），未发布为成功二进制。
第二批补充 Numeric 依赖前有一个编译错误，exit 123；后续标准构建的
输入栅栏也保留了源码变化拒绝发布的 exit 1。首候选 121 协议有 14 失败、
native 在命名 WINDOW 处 abort，均保留；CAST/`::` 双表示、WINDOW 关键字/
定义保留和实际 owner 的初始化检查继续修复，不将失败轮次记为全绿。
第二候选117有4项命名窗口失败，保留 `o05-second-candidate117-protocol.log`；
legacy 的命名定义仅匹配带空格的 ` AS `，遗漏合法 `AS(`。改为实际顶层
关键字边界后诊断全通过。WINDOW/inline OVER 共用同一解析器，保留 SQL
child 的原 token 坐标；未定义名字为42704、同层重复定义为42P20。首轮
119参考将重复定义误预期为42710的三次失败同样保留，最终按真实参考核对。
最终标准构建、十四完整native和十六完整协议已全部结束并核对当前58对象
及输入不变；诊断版与中间失败没有冒充此终态。R45 仍未修复，后续单独提交；
此处的完整邻居门禁也不是最终全注册回归。

专项验收不替代最终完整注册回归，也不宣称全部 PostgreSQL 排序/表达式
语义兼容；273 项功能状态独立维护。
