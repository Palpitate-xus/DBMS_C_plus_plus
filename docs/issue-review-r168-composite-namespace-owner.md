# R168：复合类型sidecar缺少可用的namespace身份（开放）

状态：已复现合法DROP SCHEMA失败；尚无生产修复或验收提交。

## 实测与原因

[R164](issue-review-r164-routine-custom-type-declarations.md)完整956项夹具创建了普通
enum/domain/composite，包括quoted点号及双引号类型名。其三个合法DROP SCHEMA CASCADE
在组合Root均报XX001、invalid auxiliary namespace identity；PG同一956项全0。
清理前的声明/归属控制通过，不应删掉清理SQL来宣称整批通过。

[辅助对象收集器](../src/commands/DdlExecutor.cpp)逐项把composite物理名重解析成SQL。
但[现有.types格式](../src/commands/TableManage.cpp)只保存decoded的
schema+'.'+name及字段；public不写schema前缀，也没有composite pg_type登记。
`R164.Scope.Mixed.C`或含裸双引号的物理字节不能重新当作SQL解析。
无关的一个引用复合名因此能阻止删除其他schema。

还存在需要专项实测的静态歧义：public的`"s.t"`与schema s下的t会生成相同物理名s.t。
仅凭该字符串无法证明namespace；最长前缀或“有schema就归给它”可能选错对象。
这里不把尚未运行的存活性控制说成已实测的数据误删。

## 安全修改方法与验证要求

必须由真实CREATE TYPE声明保存明确、decoded的schema/type-name，
在同一.types文件加入可区分旧记录的版本化表示，并由现有原子写及DDL快照管理。
现有物理API可以继续精确键语义；SQL声明/删除需要核对真实owner，不能造pg_type行。
CREATE、ALTER字段、rename、DROP、冷读及事务恢复均须保存并验证这对身份。
checked loader必须区分不存在、损坏与I/O，不能把坏owner当成空列表后删namespace。

旧记录没有足够信息时不能自动猜测迁移。无点号旧物理名可确认public；
dotted旧记录涉及破坏性操作时应明确失败，等待有依据的身份恢复。
仅增加collector的字符串拆分无法修复此问题。

待验证两种独立历史的public dotted survivor/真实namespaced deletion、quoted大小写、
冷重启、rename、field alter、回滚/保存点、坏V2及legacy歧义无修改。
不改变R51缓存锁，也不恢复用户暂缓的数组工厂或类型化常量。

证据：`/tmp/dbms-review-r46.ifu6SWLp/r162-v6-root-results/`
下的custom_scalar_routine_declaration_protocol_e2e_test.log，三条DROP SCHEMA错误完整保留。
