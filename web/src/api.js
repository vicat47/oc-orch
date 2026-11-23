export class ApiError extends Error {
  constructor(message, status = 0, code = "NETWORK") {
    super(message);
    this.status = status;
    this.code = code;
  }
}

// 真实 404（对象缺失/未知路径）与临时网络错误分开：仅 404 视为“不存在”。
export const isNotFound = error =>
  !!error && error instanceof ApiError &&
  (error.status === 404 || /NOT_FOUND/.test(String(error.code || "")));

export async function api(path, options = {}) {
  const controller = new AbortController();
  // GET 有界，写用例（尤其 verify --check / Supervisor）允许后端完成。
  const timeout = setTimeout(() => controller.abort(), options.method === "POST" ? 120000 : 12000);
  try {
    const response = await fetch(path, {credentials: "same-origin", ...options, signal: controller.signal});
    let body = {};
    try { body = await response.json(); }
    catch { throw new ApiError("响应不是有效 JSON", response.status, "BAD_RESPONSE"); }
    if (!response.ok) throw new ApiError(body.message || String(response.status), response.status, body.code);
    return body;
  } catch (error) {
    if (error instanceof ApiError) throw error;
    throw new ApiError(error.name === "AbortError" ? "请求超时，请稍后重试" : error.message);
  } finally {
    clearTimeout(timeout);
  }
}

export function post(path, body = {}) {
  return api(path, {
    method: "POST",
    headers: {"Content-Type": "application/json", "X-OC-Orch-Web": "1"},
    body: JSON.stringify(body),
  });
}
