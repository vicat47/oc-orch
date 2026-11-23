"""runtime：Supervisor 循环、事件队列与连接生命周期。

只依赖 application 端口、domain 规则与组合根（composition）的默认装配；
不直接接触旧 client/ledger/db/config/results 门面或 adapters 实现。
"""