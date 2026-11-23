"""跨端口的稳定错误分类，供应用、适配器与兼容入口共同使用。"""


class LedgerError(Exception):
    def __init__(self, code: str, message: str, detail: dict | None = None):
        self.code = code
        self.detail = detail or {}
        super().__init__(f"{code}: {message}")


class OrchError(Exception):
    code = "ORCH_ERROR"


class ApiError(OrchError):
    def __init__(self, status: int, path: str, tag: str | None, message: str, body: str = ""):
        self.status = status
        self.path = path
        self.tag = tag
        self.message = message
        self.body = body
        self.code = tag or f"HTTP_{status}"
        super().__init__(f"{self.code} HTTP {status} {path}: {message}"[:800])


class TransportError(OrchError):
    code = "TRANSPORT"

    def __init__(self, path: str, message: str):
        self.path = path
        self.message = message
        super().__init__(f"TRANSPORT {path}: {message}")


class StreamError(OrchError):
    code = "STREAM"

    def __init__(self, message: str):
        self.message = message
        super().__init__(f"STREAM: {message}")


class ActionError(Exception):
    """用例拒绝；code 保持已有 CLI/JSON 分类，不携带传输层状态码。"""

    def __init__(self, code: str, message: str, detail=None):
        self.code, self.message, self.detail = code, message, detail
        super().__init__(message)
