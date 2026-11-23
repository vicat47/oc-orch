#!/usr/bin/env python3
"""测试用故障代理：在不动共享 OpenCode 服务的前提下注入断流/丢响应/限流。

控制文件 JSON：
  {"generation": 1, "drop_active": false, "fail_http": [{"method":"POST","path_re":"/prompt","status":429}]}

- drop_active + generation 变化：关闭所有活动连接（模拟 SSE 断流/端点故障）。
- fail_http：匹配请求直接返回状态码，不转发。
- drop_response：匹配请求转发到上游后立即关闭客户端，模拟“请求已受理但响应丢失”。

仅用于本机测试；不记录 Authorization 内容。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import threading
import time


class Proxy:
    def __init__(self, listen: str, target: str, control: str, stats: str):
        self.lhost, self.lport = listen.rsplit(":", 1)
        self.lhost = self.lhost or "127.0.0.1"
        self.lport = int(self.lport)
        self.thost, self.tport = target.rsplit(":", 1)
        self.tport = int(self.tport)
        self.control_path = control
        self.stats_path = stats
        self.lock = threading.Lock()
        self.active: list[socket.socket] = []
        self.control = {"generation": 0, "drop_active": False, "rules": []}
        self.stats = {"connections": 0, "requests": 0, "dropped": 0,
                      "fail_http": 0, "drop_response": 0, "sse": 0}
        self.stop = False
        self._last_gen = 0

    def load_control(self):
        try:
            with open(self.control_path, "r", encoding="utf-8") as f:
                c = json.load(f)
            self.control = c
            gen = int(c.get("generation", 0))
            if gen != self._last_gen:
                self._last_gen = gen
                if c.get("drop_active"):
                    self.drop_active()
        except (OSError, ValueError):
            pass

    def drop_active(self):
        with self.lock:
            socks = list(self.active)
            self.active.clear()
        for s in socks:
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                s.close()
            except OSError:
                pass
        with self.lock:
            self.stats["dropped"] += len(socks)
        self.flush_stats()

    def track(self, s: socket.socket):
        with self.lock:
            self.active.append(s)

    def untrack(self, s: socket.socket):
        with self.lock:
            try:
                self.active.remove(s)
            except ValueError:
                pass

    def flush_stats(self):
        tmp = self.stats_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.stats, f, ensure_ascii=False)
            os.replace(tmp, self.stats_path)
        except OSError:
            pass

    def match(self, method: str, path: str):
        for r in self.control.get("rules", []):
            if r.get("method") and r["method"] != method:
                continue
            try:
                if re.search(r.get("path_re", ""), path):
                    return r
            except re.error:
                continue
        return None

    def handle(self, client: socket.socket):
        self.track(client)
        upstream = None
        try:
            client.settimeout(15)
            head = b""
            while b"\r\n\r\n" not in head and len(head) < 65536:
                chunk = client.recv(4096)
                if not chunk:
                    return
                head += chunk
            head, rest = head.split(b"\r\n\r\n", 1)
            lines = head.decode("latin1").split("\r\n")
            method, path, _ = lines[0].split(" ", 2)
            headers = {}
            for line in lines[1:]:
                if ":" in line:
                    k, v = line.split(":", 1)
                    headers[k.strip().lower()] = v.strip()
            clen = int(headers.get("content-length", "0") or 0)
            body = rest
            while len(body) < clen:
                chunk = client.recv(65536)
                if not chunk:
                    break
                body += chunk
            with self.lock:
                self.stats["requests"] += 1
                if "/api/event" in path:
                    self.stats["sse"] += 1
            rule = self.match(method, path)
            if rule and rule.get("status"):
                status = int(rule["status"])
                payload = json.dumps({"_tag": "InjectedFault",
                                      "message": f"fault proxy {status}"}).encode()
                client.sendall(
                    f"HTTP/1.1 {status} Injected\r\nContent-Type: application/json\r\n"
                    f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode()
                    + payload
                )
                with self.lock:
                    self.stats["fail_http"] += 1
                return
            upstream = socket.create_connection((self.thost, self.tport), timeout=15)
            self.track(upstream)
            upstream.sendall(head + b"\r\n\r\n" + body)
            if rule and rule.get("drop_response"):
                with self.lock:
                    self.stats["drop_response"] += 1
                # 先把上游响应读完/等服务完成受理，再关闭客户端；
                # 否则服务端可能因客户端提前断开而回滚请求。
                try:
                    upstream.settimeout(5)
                    deadline = time.time() + 5
                    while time.time() < deadline:
                        chunk = upstream.recv(65536)
                        if not chunk:
                            break
                        # HTTP 响应体通常不大；这里只读到结束/超时
                        if b"\r\n\r\n" in chunk and b"Content-Length" not in chunk:
                            break
                except (OSError, socket.timeout):
                    pass
                time.sleep(0.2)
                return
            # 双向泵
            client.setblocking(False)
            upstream.setblocking(False)
            socks = [client, upstream]
            while not self.stop:
                try:
                    import select
                    r, _, x = select.select(socks, [], socks, 1.0)
                except (OSError, ValueError):
                    break
                if x:
                    break
                if not r:
                    continue
                for s in r:
                    try:
                        data = s.recv(65536)
                    except (BlockingIOError, InterruptedError):
                        continue
                    except OSError:
                        return
                    if not data:
                        return
                    other = upstream if s is client else client
                    try:
                        other.sendall(data)
                    except OSError:
                        return
        except (OSError, ValueError):
            pass
        finally:
            for s in (client, upstream):
                if s is None:
                    continue
                self.untrack(s)
                try:
                    s.close()
                except OSError:
                    pass
            self.flush_stats()

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.lhost, self.lport))
        srv.listen(128)
        srv.settimeout(0.5)
        self.flush_stats()
        while not self.stop:
            self.load_control()
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self.lock:
                self.stats["connections"] += 1
            threading.Thread(target=self.handle, args=(conn,), daemon=True).start()
        try:
            srv.close()
        except OSError:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("--control", required=True)
    ap.add_argument("--stats", required=True)
    args = ap.parse_args()
    p = Proxy(args.listen, args.target, args.control, args.stats)
    try:
        p.run()
    except KeyboardInterrupt:
        p.stop = True


if __name__ == "__main__":
    main()
