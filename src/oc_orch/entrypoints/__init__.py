"""oc-orch 入口层：CLI 与本地 Web 控制面的参数/HTTP 边界。

依赖规则（scripts/check_boundaries.py）：entrypoints 只能导入
domain / application / entrypoints / runtime / composition / util，
不得直接依赖旧 client/ledger/db/config/results 门面或传输实现。
"""