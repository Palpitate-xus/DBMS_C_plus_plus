# R77：物理恢复在staging前过早淘汰当前目录

状态：staging缓存时序已修复并验收；实际publication冲突仍按既有规则报告。

## 问题与复现

R76阻止ParameterStatus异常终止server后，原DIV14仍在等待RESTORE时得到
55P03。physicalRestore在校验备份和复制staging之前就evict当前catalog；
此时旧目录尚未改变，已存在的干净generation本可按既有ownership规则供
元数据读者读取，却被变成禁止的cold read。真实管理员检查仍应正常执行，
不能通过缓存权限结论或删除原断言来绕过。

新增physical_restore_catalog_staging_test检查无效源、主动取消不改变缓存代，
并在真实restore callback内使用另一线程读取：该线程明确没有当前文件读取
ownership，但应能取得同一个当前committed代。恢复完成后新代不得含备份后
新增的namespace，显式持有的旧代仍保留其原值。修改前完整新入口exit134，
在无效源不应淘汰缓存的断言失败；日志为
`/tmp/dbms-review-r46.ifu6SWLp/r77-old-native.log`。

## 修改方法与边界

把public physicalRestore的catalog淘汰延后到全部staging、来源二次校验和
取消检查通过后的实际目录publication之前。数据库排他锁、真实ownership、
heap/index/WAL cache关闭、原子替换/回滚和恢复后重建均不变。无效或取消的
staging没有替换目录，故无需丢弃仍然有效的catalog代。

本项不更改认证、管理员判断、权限规则、CTAS或后台checkpoint专项。实际
目录rename窗口的cold read仍可返回原有冲突，不把旧代无条件伪装成新目录。
原DIV14完整驱动仍须另行验收，不据原因推断成功。

## 主工作区验收

TableManage真实标准O2重编、链接与repeat均0，`r77-standard-build.log`；
binary SHA256为2e6fda283a11d98b0e2a2d917c83f274891a0ea5b89cd2da999291e68ca7fa8c。
`r77-final-native.log`十个完整入口全0：新staging、原恢复一致性/源验证、备份
一致性/替换、catalog clean/snapshot、两个snapshot exporter及rollback recovery。
新入口保留取消/错误源不淘汰、真实foreign reader、恢复后新代及旧lease全部断言。

`r77-verified-protocols.log`新增恢复窗口、完整PG、真实取消协议、CLI、COPY五个
完整入口全0，首尾生产输入/binary/58份receipt一致。所有日志均在
`/tmp/dbms-review-r46.ifu6SWLp/`。

原样DIV14仍在真正目录publication期间收到精确55P03，保留
`r77-final-protocol-logs/div14_feature_gate_test.log`。这不能通过删除冷读保护或
借用旧身份权限结论来修复；R78单独收窄轮询夹具的临时冲突处理，原30秒期限、
严格完成/数据断言及其余错误必须保留。首个native门禁因调用者写错rollback
入口名称在预检退出1，未执行测试，保留`r77-accepted-native.log`；更正后的
十入口完整终态才是本项验收。最初文档patch上下文不匹配，原子拒绝、未改文件。
