#!/usr/bin/env python3
"""speedex 扫链测速透传隧道（2026-10-10）——只做 HTTP CONNECT 字节转发，不解密。

替换 tinyproxy：tinyproxy 不设 TCP_NODELAY，Nagle + 延迟 ACK 让小 WS 帧在节点→EU 段排队（HK A/B 实测两家推送
都比 MITM 路径晚 16–65ms），隧道本身成了偏置源。本实现两侧套接字显式 TCP_NODELAY（与 mitmproxy/asyncio 一致）。

策略（fail-closed）：仅 --allow 列出的客户端 IP；仅 `CONNECT <hostname>:443`（拒字面 IP、本地/私网名、其它端口、
其它方法）；请求头 ≤8KB/10s；上游连接 10s 超时；并发上限。内核级拒回环/私网出站由 systemd IPAddressDeny 承担
（主机名解析到回环亦拦截）。不记录目标主机、不落任何负载。
"""
import argparse
import asyncio
import ipaddress
import re
import socket
import sys

HEAD_MAX = 8192
HEAD_TIMEOUT = 10.0
CONNECT_TIMEOUT = 10.0
CHUNK = 65536
TARGET_RE = re.compile(r"^([A-Za-z0-9.-]{1,253}):443$")
LOCAL_NAME_RE = re.compile(r"^(localhost|localtest\.me|lvh\.me)$|\.(localhost|local|internal|lan|home|arpa)$", re.I)


def connect_target(head: bytes):
    """请求头 → 目标主机名（合法 CONNECT host:443）或 None。纯函数，供离线测试。"""
    try:
        line = head.split(b"\r\n", 1)[0].decode("ascii")
    except UnicodeDecodeError:
        return None
    parts = line.split(" ")
    if len(parts) != 3 or parts[0] != "CONNECT" or parts[2] not in ("HTTP/1.1", "HTTP/1.0"):
        return None
    m = TARGET_RE.match(parts[1])
    if not m:
        return None
    host = m.group(1).lower().rstrip(".")
    if not host or "." not in host or LOCAL_NAME_RE.search(host):
        return None
    try:
        ipaddress.ip_address(host)
        return None  # 字面 IP 一律拒
    except ValueError:
        return host


def _nodelay(writer):
    sock = writer.get_extra_info("socket")
    if sock is not None:
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass


async def _pipe(reader, writer):
    try:
        while True:
            data = await reader.read(CHUNK)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError, asyncio.CancelledError):
        pass
    finally:
        try:
            if writer.can_write_eof():
                writer.write_eof()
        except (OSError, RuntimeError):
            pass


class Tunnel:
    def __init__(self, allow, max_conns=512):
        self.allow = set(allow)
        self.slots = asyncio.Semaphore(max_conns)

    async def handle(self, creader, cwriter):
        peer = (cwriter.get_extra_info("peername") or ("",))[0]
        if peer not in self.allow or self.slots.locked():
            cwriter.close()
            return
        async with self.slots:
            _nodelay(cwriter)
            uwriter = None
            try:
                head = await asyncio.wait_for(creader.readuntil(b"\r\n\r\n"), HEAD_TIMEOUT)
                if len(head) > HEAD_MAX:
                    raise ValueError("head too large")
                host = connect_target(head)
                if host is None:
                    cwriter.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                    await cwriter.drain()
                    return
                try:
                    ureader, uwriter = await asyncio.wait_for(
                        asyncio.open_connection(host, 443, happy_eyeballs_delay=0.25, interleave=1), CONNECT_TIMEOUT
                    )
                except (OSError, asyncio.TimeoutError):
                    cwriter.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
                    await cwriter.drain()
                    return
                _nodelay(uwriter)
                cwriter.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await cwriter.drain()
                await asyncio.gather(_pipe(creader, uwriter), _pipe(ureader, cwriter))
            except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError, ValueError, ConnectionError, OSError):
                pass
            finally:
                for w in (cwriter, uwriter):
                    if w is not None:
                        w.close()


async def serve(host, port, allow):
    t = Tunnel(allow)
    server = await asyncio.start_server(t.handle, host, port, limit=HEAD_MAX, reuse_address=True)
    async with server:
        await server.serve_forever()


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8444)
    ap.add_argument("--allow", action="append", required=True, help="client IPv4/IPv6 allowed to use the tunnel")
    a = ap.parse_args(argv)
    for ip in a.allow:
        ipaddress.ip_address(ip)
    asyncio.run(serve(a.listen, a.port, a.allow))


if __name__ == "__main__":
    sys.exit(main())
