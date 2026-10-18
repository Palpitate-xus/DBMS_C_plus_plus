# R137：外键把截断前缀命中当成完整父键（用户暂缓）

状态：用户于2026-10-18要求“skip it”。本项候选保留，未提交、未验收，
不计已修复；普通继续不恢复本项，也不扩大到全部外键或其它普通SQL问题。
此前文档中的“有限修复完成”已撤回，不能用初稿的窄覆盖替代扩展后的真实失败。

## 问题与修改方法

| 观察 | 已确认事实与拟处理方法 |
| --- | --- |
| 无效引用被接受 | 父表仅21字节文本A时，同20字节前缀的C在旧版普通单PK外键INSERT中被接受并留下孤儿行；PG18.6报23503且不写行 |
| 原因 | 该路径把B+Tree的20字节前缀命中当成父键存在证明，没有核对完整可见堆值 |
| 候选方法 | checked index区分Error/NotFound，通过现有页锁/MVCC边界读取候选父行，再用真正的完整类型相等检查；保留IO_ERROR、真实NULL及写入原子性 |
| 初稿回归 | 通用compareValues不是所有键类型的相等实现；缺少INET/CIDR/MACADDR/MACADDR8分支，导致合法引用被拒绝 |
| 修订方法 | 优先复用约束canonical equality，网络/MAC按类型解析，Legacy日期/时间处理已接受别名；必须由扩展正负例和干净组合验证，不能仅比较格式串 |
| 其它普通缺口 | 长文本第二个PK只在20字节后不同时旧版58030；双timestamp复合PK最后微秒不同时旧版23505，参考均成功。这两项不是本次跳过范围或已修复项 |

后续普通主键完整值问题已单独复现并登记为[R146](issue-review-r146-primary-key-full-value.md)，
用户随后也要求暂缓；上表最后一行保留本项当时的范围，不作为重开R146的授权。

## 跳过前证据，按实际候选区分

| 输入与完整控制 | 实际结果与限制 |
| --- | --- |
| 实际PG18.6/180006，最终扩展协议364 | 全部通过；14类型/边界族，Simple、Extended-text、Extended-binary三模式 |
| 冻结已接受8b50程序，同364 | 96断言失败，实际退出1；残留行的后续断言不是96个独立根因 |
| 初稿5dad程序，最初协议208/native57 | 全过，但未覆盖合法网络类型，不能据此验收 |
| 初稿5dad程序，扩展协议364/native89 | 分别48/4断言失败，均实际退出1；合法网络值正例及行数控制暴露回归 |
| 初稿Root完整45入口 | 44实际通过、1失败，driver和独立终态均1；18Python/3Main通过不让整批变绿 |
| 初稿干净候选7入口 | 原57/208夹具下均0；1新O2 TM加57个核对过的R136生产donor（含真实Main），不是fresh58构建，也不是修订版证明 |
| 最新修订 | 私有TM正常O2重编1个CPP、repeat0，均实际0；修订后native89/协议364、Root45、clean7和独立验收均未运行，按用户要求停止 |

最终扩展协议候选为`tests/foreign_key_full_value_protocol_e2e_test.py`，
原生候选为`tests/foreign_key_full_value_test.cpp`；两者及注册均未进入已接受版本。
原生另含空文本、字符串NULL、INET/MAC别名、Legacy DATE/TSTZ的正负控制。
这些文件是保留草稿，不能因留在工作区就列为已接受永久回归。

初稿`foreign_key_empty_text_test`在原第51行末尾
`assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK)`实际134；
之前外键断言执行不等于完整入口通过。该夹具不是本项单PK快路径，
具体DROP状态和锁因未确定；沿用户既有R51边界只登记，不改断言、不重试覆盖。

## 保存的本机证据与版本边界

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`保留：
`r137-reference364.log`、`r137-old83c-expanded364.log`、
`r137-initial-fix-expanded364.log`、`r137-initial-fix-expanded-native89.log`、
初208/312与类型正例诊断、`r137-final-gate.log`、`r137-clean-gate.log`、
`r137-independent-final.log`及`r137-v2-normal-driver.log`。
用户跳过时的完整候选另存`r137-user-skip-candidate.tar.gz`，
原19路径暂存补丁另存`r137-user-skip-staged.patch`；已撤销本项代码暂存，
保留工作区字节，不提交为修复。

初稿binary为`5dadb3e6bbc910a6d4128a1fd06b60c6a5c66977eb350f650e39c9b3c0faf4c5`。
修订版仅构建的binary为`d02ba23a875c4465e8a2f35451bdb3e6664febad081813b6e074a68daa2285f6`，
cache为`29e7d71983e314290a1612d8ab6ce26d00d08cae9ccb52be3a12767b038f8117`，
src/tests/scripts封印为`48ccd149828b83ef04f2d3d4a84b738ed16f566fd1537c9bbad0bdd9c9978b3c`。
这些临时产物不是发行验收；原始旧程序、初稿干净候选和R136 donor保持冻结。

本次只提交暂缓记录。已接受测试数量保持902native+537Python+3Main=1442，
Markdown含本记录为560；工作区563MD及906native+541Python+3Main=1450
还含原六个未接受入口与本项两个候选。原273条目及22complete/166partial/
70unverified/15deferred计数不变；总清单未完成。不push，GitHub Actions保持禁用。
