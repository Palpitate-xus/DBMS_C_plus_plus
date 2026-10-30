# R233：等待/失败的锁升级丢失原有弱锁（用户暂缓）

最新用户指令为“skip it”：本项停止实现，保留未提交的复现夹具与观察记录。
没有 R233 生产代码改动，没有修复验收或 commit；普通 continue 不代表恢复本项。
后续审查移至其他独立问题，原总清单不能据此标记完成。

对应 TXN-07、TXN-05、P0-04 与 OPT-17 的所有权/错误恢复条件。成功升级的深度
修复 R232 不能证明失败路径；当前 table/row/page 升级都先释放旧弱 token，再等待。
超时、取消或竞争后失败时，已有 holder 可能消失，checkpoint 无法重新创建它。

需要用真实持有者和等待图验证整个等待期间、失败返回之后以及竞争者退出后的
原持有者排斥能力，并覆盖多升级者循环、嵌套深度和进程 advisory 锁。升级仍是模式
转换，不增加 token；不能改掉旧的单次转换/单次 unlock 约定。

Linux [flock(2) 官方手册](https://man7.org/linux/man-pages/man2/flock.2.html)
说明模式转换可能先移除旧锁再建立新锁，不能把非阻塞转换失败理解为原锁仍在。
实现时必须同时解决逻辑所有权和实际进程锁窗口，不能只补一个计数字段。

当前新原生夹具记录 table/row/page、超时/取消/等待后成功和两个升级者的真实调度；
尚未验收修复或提交。不恢复 R229/其他暂缓模块，不 push，Actions 保持禁用。

暂缓时的旧版证据位于 `/tmp/dbms-r233-upgrade-retention.BEwl9Y3G`：
`completed-old-baseline-receipt.json` 记录 Root 与 accepted-only 各完整 866 控制、
258 失败；`completed-old-process-baseline-receipt.json` 记录两侧各完整 232 控制、
53 失败。均为缺陷复现，不是通过或修复证据，也不是独立缺陷数量。
