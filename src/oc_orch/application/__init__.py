"""应用层端口契约：外部依赖（OpenCode 服务、时间）的窄接口。

runtime（Supervisor）与 future application services 只依赖这些 Protocol，
不直接认识 adapters 的具体实现。默认装配（SystemClock）先放在本模块，
后续由组合根/adapters 迁移；OpenCodeClient 的具体实现仍为
oc_orch.client.Client，fake 实现见 tests/fakes.py。
"""