# R152：用户 REGTYPE 名称的最小引用与统一呈现

状态：本项显示修复通过完整专项；相邻完整门禁仍有一个已确认的旧缺陷，
未报告全绿。[R157 普通 SET 目标](issue-review-r157-dotted-set-target.md)及
[R156 物理身份](issue-review-r156-regtype-physical-constants.md)另列开放。
用户暂缓的 R151 枚举自动数组不恢复。

## 问题、修复与边界

~~~sql
CREATE SCHEMA type_display;
CREATE TYPE type_display.plain AS ENUM ('z','a');
SELECT pg_typeof(NULL::type_display.plain)::text;
~~~

原程序输出 "type_display"."plain"，可见时仍输出 "plain"；
PG 输出 type_display.plain 或 plain。需要引用的特殊名称必须继续正确转义。

| 位置 | 修改 |
| --- | --- |
| regtype_input.h | 精确查找串始终引用；仅显示串按最小引用规则呈现 schema/类型 |
| parser.h / parser.cpp | 提取已有正确的 quoteIdentifier，关键字分类由同一私有 owner 提供 |
| ExprEvaluator.cpp | quote_ident、format %I 委托统一规则，删除重复关键字集合 |
| 搜索路径 | 两个原调用改用公共函数，保留原关键字分类及列表语义 |
| 已有数组元数据 | 递归呈现真实元素身份，不创建或伪造数组目录 |

实现使用 PG 18.6 ruleutils.c quote_identifier 的默认规则：
ASCII 小写或下划线起始，后接小写/数字/下划线，非 unreserved 关键字要引用；
嵌入双引号成对转义，美元符号不能免引用。没有按区域设置判断字母，
没有删除任意数据字符串中的引号，也没有将显示串反用作精确身份查找。

## 完整实际结果

| 输入 | 控制 | 结果 |
| --- | --- | --- |
| 实际 PG 18.6，本轮重跑 | 1675 wire | 0 失败 |
| R155 后、修复前 Root / accepted-only clean | 各 1675 wire | 各 66 失败、退出 1 |
| 修复前原生 | 5197 | 1718 失败、退出 1 |
| 修复后 Root / accepted-only clean | 各 5197 native、1675 wire | 均 0 失败 |
| Root 完整相邻入口 | 24 native + 18 wire + 3 Main = 45 | 全部退出 0 |
| clean 完整相邻入口 | 15 native + 12 wire + 3 Main = 30 | 29 通过、1 个原生退出 134，整批退出 1 |

[原生夹具](../tests/regtype_identifier_output_test.cpp)覆盖全部 494 个独立关键字事实、
22 类特殊名称、五类 schema、精确身份 roundtrip、NULL 和未知 OID。
[协议夹具](../tests/regtype_identifier_output_protocol_e2e_test.py)覆盖相同关键字/特殊字符、
实际标量枚举的可见/限定/遮蔽名字、Simple/Extended text/binary、
原始 TEXT OID/长度/typmod/格式/数据、命令标签与 Ready。
原生数组仅使用合法的复制元数据输入，不调用 R151 自动数组创建器。

两夹具本轮前后字节相同，完整 5197/1675 没有删减。
历史 1675/69 旧红包含三项 R155 清理错误；本轮重新实测为 66，不用减法代替结果。
历史两次测试编译错误、5197/1719 中误要求 legacy format 抛 DbError 的一处
测试错误及修正后的 5197/1718 均保留；协议独立检查真实 22004。

## 相邻失败没有被隐藏

clean 的 search_path_values_grammar_test 在末尾 dotted 名称断言中止，未完成到 footer。
已在冻结的 R155 已接受版本，使用同一未改夹具、匹配的旧头/flags/生产对象重现同一
第 86 行断言、退出 134。它把 search_path.extra 错写成 search_path；
工作区旧待定代码恰好包含完整名称循环，因此工作区通过不能替代 clean 验证。

原 30 入口日志、整批退出 1 和要求全部通过的严格检查器退出 1 全部保留。
另一个输入/提交边界审计精确核对 45+30 全部结果及这一个已确认旧失败，
其审计通过不表示完整门禁变绿。不修改或删除原断言，R157 仍须独立修复。

## 构建与版本范围

共享头变化后，Root 与独立 clean 均正常 fresh58 生产 O2、repeat0、全部 receipt 匹配。
两端各三个真实 Main 测试 TU 均在新头后重新编译，测试按既有 O0/O2/O0；
新链接各自当前 57 个 fresh O2 库并隔离实际执行。session 使用已接受 320 命令源码，
未执行 R140 暂缓的 21 条扩展。相同对象 SHA 不被当作复用，fresh 命令/新目录/退出码保留。
真实 server 不链接测试 stubs；既有 TLS fallback 不变，没有本轮 TLS 验收。
新/旧 clean 与 donor 的所有已跟踪文件永久冻结。

本地提交只接受本项五个 partial 源/注册、两个完整夹具及相关文档。
其它原有待定源码、R151/其它用户暂缓项不混入；R156/R157 只保存问题和方法。
已接受 577 MD、908 native + 547 Python + 3 Main = 1458；
工作区 580 MD、914 + 553 + 3 = 1470，仍有 12 个未接受入口。
未运行新 1458 全注册/整库 sanitizer；原 273 状态仍 22/166/70/15，
总清单未完成，不 push、不启用 Actions。
