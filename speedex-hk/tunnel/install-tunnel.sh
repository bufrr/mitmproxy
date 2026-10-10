#!/bin/sh
# 用法（节点 root）：sh install-tunnel.sh <EU_IPV4>
# 幂等：写配置 → 安装 tinyproxy（保留预置配置）→ systemd drop-in → 重启 → 自检监听 8444。
set -eu
EU_IP="$1"
case "$EU_IP" in *[!0-9.]*|'') echo "bad EU ip" >&2; exit 2;; esac
HERE="$(cd "$(dirname "$0")" && pwd)"
mkdir -p /etc/tinyproxy /etc/systemd/system/tinyproxy.service.d
sed "s/__EU_IP__/$EU_IP/" "$HERE/tinyproxy.conf.in" > /etc/tinyproxy/tinyproxy.conf
install -m 0644 "$HERE/speedex-filter" /etc/tinyproxy/speedex-filter
RES=""
if grep -qE '^nameserver[[:space:]]+127\.0\.0\.53' /etc/resolv.conf; then RES="IPAddressAllow=127.0.0.53/32"; fi
sed "s#__RESOLVER_ALLOW__#$RES#" "$HERE/tinyproxy-speedex.conf.in" > /etc/systemd/system/tinyproxy.service.d/speedex.conf
if ! dpkg -s tinyproxy >/dev/null 2>&1; then
  DEBIAN_FRONTEND=noninteractive apt-get install -y -o Dpkg::Options::=--force-confold tinyproxy >/dev/null
fi
systemctl daemon-reload
systemctl enable tinyproxy >/dev/null 2>&1
systemctl restart tinyproxy
sleep 1
systemctl is-active tinyproxy
ss -ltn | grep -q ':8444 ' && echo "listening 8444"
