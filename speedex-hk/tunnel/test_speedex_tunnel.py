"""Offline tests for speedex_tunnel (connect_target parsing, client allowlist, 403, TCP_NODELAY). No network."""
import asyncio
import socket
import unittest

import speedex_tunnel as st


class ConnectTargetTest(unittest.TestCase):
    def test_accepts_hostname_443(self):
        for h in (b"ws.gmgn.ai", b"wsdexpri.okx.com", b"web3.okx.com"):
            self.assertEqual(st.connect_target(b"CONNECT " + h + b":443 HTTP/1.1\r\nHost: x\r\n\r\n"), h.decode())

    def test_rejects(self):
        bad = [
            b"CONNECT ws.gmgn.ai:80 HTTP/1.1\r\n\r\n",
            b"CONNECT 127.0.0.1:443 HTTP/1.1\r\n\r\n",
            b"CONNECT 192.0.2.80:443 HTTP/1.1\r\n\r\n",
            b"CONNECT [::1]:443 HTTP/1.1\r\n\r\n",
            b"CONNECT localhost:443 HTTP/1.1\r\n\r\n",
            b"CONNECT lvh.me:443 HTTP/1.1\r\n\r\n",
            b"CONNECT node.internal:443 HTTP/1.1\r\n\r\n",
            b"CONNECT gmgn:443 HTTP/1.1\r\n\r\n",
            b"GET http://ws.gmgn.ai/ HTTP/1.1\r\n\r\n",
            b"CONNECT ws.gmgn.ai:443 HTTP/2\r\n\r\n",
            b"\xff\xfe\r\n\r\n",
        ]
        for h in bad:
            self.assertIsNone(st.connect_target(h), h)


class RelayTest(unittest.TestCase):
    def test_client_allowlist_and_403(self):
        async def run():
            t = st.Tunnel(allow=["192.0.2.1"])  # 127.0.0.1 不在放行表
            srv = await asyncio.start_server(t.handle, "127.0.0.1", 0)
            port = srv.sockets[0].getsockname()[1]
            r, w = await asyncio.open_connection("127.0.0.1", port)
            w.write(b"CONNECT ws.gmgn.ai:443 HTTP/1.1\r\n\r\n")
            await w.drain()
            try:
                got = await r.read(100)
            except ConnectionResetError:
                got = b""  # 服务端未读即关闭 → RST，同为断开
            self.assertEqual(got, b"", "未放行客户端直接断开")
            w.close()
            t.allow.add("127.0.0.1")
            r, w = await asyncio.open_connection("127.0.0.1", port)
            w.write(b"CONNECT 127.0.0.1:443 HTTP/1.1\r\n\r\n")
            await w.drain()
            self.assertTrue((await r.read(100)).startswith(b"HTTP/1.1 403"))
            w.close()
            srv.close()
            await srv.wait_closed()

        asyncio.run(run())

    def test_nodelay_helper(self):
        async def run():
            srv = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
            port = srv.sockets[0].getsockname()[1]
            _, w = await asyncio.open_connection("127.0.0.1", port)
            sock = w.get_extra_info("socket")
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)
            st._nodelay(w)
            self.assertEqual(sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY), 1)
            w.close()
            srv.close()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
