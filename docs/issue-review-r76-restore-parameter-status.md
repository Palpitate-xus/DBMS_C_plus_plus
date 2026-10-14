# R76：恢复期间的ParameterStatus读取逃出客户端线程

状态：协议状态观察崩溃已修复并完成本范围门禁；DIV14的独立55P03仍需后续处理。

## 原始证据

冻结全量的DIV14在维护restore期间连接断开；当前fa806755组合原样重跑仍在
wait_for_maintenance的SHOW MAINTENANCE JOB处失败。相同串行批次前四个完整
驱动（PG、真实取消协议、CLI、COPY锁释放）均退出0，但DIV14失败，批次未到
最后fence，不能记为整体通过。

外部observer只把原驱动子进程stdout/stderr保存为日志，未改SQL、断言或期限，
确认进程SIGABRT/-6而非正常关闭连接。有限cold-catalog throw栈解析为：

CatalogService::acquireImpl → get → authCatalog → mutableProtocolParameterStatuses
→ handleClient的ParameterStatus发送lambda → 客户端thread。真实异常为55P03，
不是恢复runner抛出的异常或权限决策失败。

本机证据目录`/tmp/dbms-review-r46.ifu6SWLp/`：
`r75-full-regression-protocols.log`及独立driver日志、`r76-div14-observed-driver.log`、
`r76-div14-server.log`、`r76-div14-trace-driver.log`、`r76-div14-trace-server.log`。
修改前binary SHA256为9077e1792f136fa60172e67c95965818535db1d921ad590b51896e53d4e12361。

## 修复方法与边界

协议状态是观察，不是鉴权决策。初始化直接保留已经发送的startup值；普通刷新
可以读取实际目录，但不能经显式bootstrap入口创建目录。物理restore持有名称锁
并暂时evict目录时，只有effective role完全没变，才可暂留已报告的is_superuser；
其他session GUC仍正常发送，目录可读后继续重新观察。改变role的状态不能借用
旧身份的值，真实I/O错误也不能被当作普通冲突吞掉。

状态footer异常还须在其自己的边界内处理，不能逃出线程终止整个server或覆盖
已发送的主要SQL错误。本项不修改实际认证、角色/权限策略、CTAS、后台checkpoint
或用户暂缓模块的实现。

## 新回归控制

restore_parameter_status_protocol_e2e使用真实backup/restore与既有RATE限流，
在实际restore_staging目录存在前后检查SET application_name仍正常发送状态、
同一role不被误报off；最后检查恢复后数据、GUC及连接可用。
没有延长现有socket/statement timeout。修改前该完整新入口也以连接断开失败，
保留`r76-old-strong-protocol.log`。

首次候选不再崩溃，但新入口对SHOW JOB的无条件成功期待不成立：该命令有真实
管理员目录读取，不同于只观察会话GUC。永久回归要求它成功返回running或精确
55P03，且都必须保留ReadyForQuery及活连接；随后待实际staging消失，再严格
核对任务succeeded、恢复数据及会话状态。不绕过管理员检查，不吞其他错误。
首次原样DIV14也因此从连接断开变成明确55P03，不能把该驱动记为通过。
独立缓存淘汰时序继续调查，不能用这项修复关闭整个恢复功能。

## 主工作区验收

公共头仅补充纯builtin与用户routine的既有规划边界说明，但仍真实重编58个
生产单元，`r76-final-fresh58.log`全部0；Network最终使用owned普通acquire，
无bootstrap目录创建或借用代悬垂，标准重编与repeat均0，
`r76-network-final-build.log`。最终binary SHA256为
28590511c870572832421e375fcca56b5819b1e8df39fe243a5b2047a0d42f1a。

`r76-final-protocols.log`五个完整入口全部0：新增17控制、原完整PG协议、真实
statement timeout协议、CLI及原COPY错误锁释放；前后58份生产receipt、输入和
binary不变。原样DIV14的55P03保留在`r76-original-protocols.log`及独立日志。
早期拼错COPY驱动名称的批次在预检查exit2、未运行任何入口，原样保留；首次
新增入口的SHOW JOB期待失败也保留在`r76-accepted-protocol-logs/`。
静态497 Markdown/795目标、518 Python AST、13 shell及注册498入口检查0。
本项不是全注册套件或原273项完成证明。
