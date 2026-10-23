# R234：正则拆分的末尾字段与零长度匹配

状态：有限边界修复通过；FUNC-01、TYPE-04 仍未整体完成。

## 问题与修改方法

旧 `regexp_split_to_array` 将 `sregex_token_iterator` 的未匹配区间直接当成
数组字段，导致 `regexp_split_to_array('a,', ',')` 错返 `{a}`，遗漏末尾空字段；
空正则拆分 `abc` 又多出开头空字段。零长度匹配按字节前进还会拆断 UTF-8
码点。末尾空字段是实际数据，不应以空数组/NULL 或删除测试来掩盖。

现在显式遍历匹配位置：忽略字符串首尾和紧接前次匹配的零长度匹配，保留非空
分隔符产生的首尾空字段，循环后始终写入最后一个字段。空输入仍产生单个空
字符串。零长度匹配之后跳过一个完整 UTF-8 字符；保留相邻组合码点，不按字素
聚类。继续在原字符串迭代器上搜索并提供前一字符信息，不把后缀误作新的 `^` 起点。
字段仍通过既有数组元素引用规则输出，不将 NULL 字样或引号/反斜线当作结构。

规则来自 [PostgreSQL 18 正则拆分文档](https://www.postgresql.org/docs/18/functions-matching.html)
及本机 PostgreSQL 18.6 的实际完整对照，不是凭 C++ 分词行为设置产品 oracle。

## 实际验证

| 完整夹具 | 旧版 Root / 纯提交 | 修复后 Root / 纯提交 |
| --- | --- | --- |
| 原生 67 控制 | 各退出 1，44 失败 | 各退出 0，零失败 |
| 协议 128 控制 | 各退出 1，74 失败 | 各退出 0，零失败 |
| 每侧既有 6 native +5 协议入口 | 本轮未作旧版全回归声明 | 全部退出 0 |

候选共 26 次完整程序/驱动运行通过。私有 PostgreSQL 18.6 的同一完整 128 项
退出 0，并正常停止；默认 fsync、超时和持久化选项未弱化。失败数字是重复
断言数，不是 44/74 个独立缺陷；不是全套或所有历史尝试通过的声明。

最初原生夹具在最后的 override 检查返回非法数组文本 `opaque`，两侧均 abort134。
首源码、日志保留，不能计成完整 67 项旧基线。只将该测试回调改为合法 owned
ARRAY `{opaque}`，保留同一 override 断言，再取得上述完整旧红；协议夹具未修改。
没有为验收删除控制、改期望字段、延长 timeout 或对失败做过滤。

检查包含空/相邻/末尾字段、捕获分组不成为字段、零长度相邻规则、锚点、大小写
选项、ASCII/UTF-8/组合码点、CHAR/VARCHAR、数组引用字符、严格 NULL、非法正则/
选项精确错误、单次求值与 native callback 覆盖。协议还检查真实表行、空结果、
LIMIT 0、原数组 OID1009、Describe，以及文本/二进制绑定和文本结果编码。
二进制结果测试针对显式 CAST 后的 TEXT；不冒称新增数组二进制编码器的完整验证。

既有回归为 regexp_functions、string_functions、utf8_split_linear、split_part
bounded memory、native scalar signature ownership、unbound scalar preparation；
协议覆盖 UTF-8 split、split_part、空 REPEAT、字符串长度及 scalar signatures。

## 版本管理与未关闭范围

不替换 ECMAScript 引擎，不声称完整 PostgreSQL ARE、全部 Unicode 正则操作符、
所有受支持 flags 或 SRF/ProjectSet 已完成；没有恢复被跳过的 R201/R233 等模块。
R233 的等待/失败升级问题、复现夹具及暂缓记录独立保留，没有生产修复或提交。
原 273 清单与冻结 1492 的历史结果不重写，总目标仍未完成。

两侧分别 fresh 编译 ExprEvaluator、新/六个既有原生测试，自己的其他生产对象、
编译器、头文件和 stub 核验匹配后复用并新链接。Root62生产对象/134头、纯提交60/
131，各自布局不混用；不是 normal/repeat、全部 fresh 或当前全工作区源码构建。
Root Main 用明确的 pre-R229 匹配 donor，不编译/运行跳过草稿或默认旧 dbms_main。

2256 个纯源码输入与实际测试组合对应；943 native+590 注册 Python+3 Main=1536
只是入口清单，不是全 1536 验收。独立 Git index 仅提交本项五路径；18 个原暂存
路径、其他工作区字节及所有用户暂缓保留。不 push，GitHub Actions 保持禁用。

本地证据目录：`/tmp/dbms-r234-regexp-split.COTFg2xi`。

| 记录 | SHA256 |
| --- | --- |
| 首次旧基线含夹具中止 | `db0944b1ae3f9af864741841a3dc411dc148b4ce4f740c3f0fb44bdc65d09775` |
| 合法回调夹具修正 | `2217e715d6606b516ac1c234eb762683c9341b2f4037e998f0f960d6cbbd5200` |
| 完整旧基线 | `dc530f117f802140a581ccbe184da60818671deba506a67e685507492d32afd4` |
| 实际构建 | `ac7be10c0bbb73164c91042dbeeb9e842132ef7f43614d9e3d0acac6737688f1` |
| 完整 PG128 | `d6d53d6d53e1e80b5e42cf30e157d8b64d17f68bcf81baf1aa1fe18017f97817` |
| 最终26次运行 | `02aec8704f9c66ec4b438bd829e42fea96aae24653c799954c991b97301f14fd` |
