#!/bin/sh
# 用法（节点 root）：sh install-tunnel.sh <EU_IPV4>
# 幂等：安装 speedex_tunnel.py + speedex-tunnel.service（:8444）→ 重启 → 自检；若存在旧 tinyproxy 隧道则停用并清除
# （tinyproxy 不设 TCP_NODELAY，隧道本身引入延迟——2026-10-10 HK A/B）。
set -eu
EU_IP="$1"
case "$EU_IP" in *[!0-9.]*|'') echo "bad EU ip" >&2; exit 2;; esac
HERE="$(cd "$(dirname "$0")" && pwd)"
install -d -m 0755 /usr/local/lib/speedex-tunnel
install -m 0644 "$HERE/speedex_tunnel.py" /usr/local/lib/speedex-tunnel/speedex_tunnel.py
RES=""
if grep -qE '^nameserver[[:space:]]+127\.0\.0\.53' /etc/resolv.conf; then RES="IPAddressAllow=127.0.0.53/32"; fi
sed -e "s/__EU_IP__/$EU_IP/" -e "s#__RESOLVER_ALLOW__#$RES#" "$HERE/speedex-tunnel.service.in" > /etc/systemd/system/speedex-tunnel.service
if dpkg -s tinyproxy >/dev/null 2>&1; then
  systemctl disable --now tinyproxy >/dev/null 2>&1 || true
  DEBIAN_FRONTEND=noninteractive apt-get purge -y tinyproxy tinyproxy-bin >/dev/null 2>&1 || true
  rm -rf /etc/systemd/system/tinyproxy.service.d /etc/tinyproxy
fi
systemctl daemon-reload
systemctl enable speedex-tunnel >/dev/null 2>&1
systemctl restart speedex-tunnel
sleep 1
systemctl is-active speedex-tunnel
ss -ltn | grep -q ':8444 ' && echo "listening 8444"
