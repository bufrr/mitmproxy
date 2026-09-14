// HK 部署件安全合同（review R04，2026-09-05；二轮残留：restart-on-change / venv 版本校验 /
// 口令不经 environ）：
//  - gen-htpasswd.sh 行为级：从 proxy.env 生成 {SHA} htpasswd——口令不出现在
//    stdout/stderr、产出 0600、{SHA} 校验往返成立（本机 python3 hashlib 实跑，
//    无外部依赖）；二轮：口令不经子进程 environ（AGENTS：secret 不得落在
//    process-environ 邻接面）——env 里放 stale 值必须被忽略（python 直读 0600
//    proxy.env）。1dj3c 事故修订：bcrypt→{SHA}——bcrypt 每 CONNECT 全量验算在
//    单核 HK VM 压满 mitmproxy 事件循环（py-spy 实锤）；{SHA} 验证 O(μs)，
//    mitmproxy 11/12/13 全支持。离线爆破抗性牺牲可接受：htpasswd 与明文
//    proxy.env 同 0600 root-only 边界，一次性研究凭据。
//  - install.sh / speedex-mitm.service 的静态合同（umask 077 前置、口令不上 argv、
//    不再打印含凭据 URL、unit/addon 变更后 restart、venv python 版本校验、末尾版本摘要）
//    ——bash -n 语法 + 关键行存在性。
//
// 本机依赖：本测试直接 exec 本机 python3 实跑 gen-htpasswd.sh 与 {SHA} 校验往返——
// 只用标准库 hashlib/base64，无外部模块依赖（bcrypt 门已随 1dj3c 修订移除）。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync, execSync } from 'child_process';
import fs from 'fs';
import os from 'os';
import path from 'path';

// 2026-09-14 起本测试随代码迁到 fork（speedex-hk/tests/）。HK_CTRL_PORT=8072 是
// 跨仓合同：addon `_ctrl_port()` 缺省 + units + speedex 主仓 scripts/lib/
// hk-timing-client.mjs 的 HK_CTRL_PORT——改 8072 必须三处同步（fork 内此门
// 直接钉字面量，不跨仓 import）。
const HK_CTRL_PORT = 8072;
// 兼容旧调用形状：readSource('deploy/hk-proxy/<rel>') → fork 的 speedex-hk/<rel>
const HK_DIR = path.join(path.dirname(new URL(import.meta.url).pathname), '..');
const readSource = (rel) => fs.readFileSync(path.join(HK_DIR, rel.replace(/^deploy\/hk-proxy\//, '')), 'utf8');
const ROOT = HK_DIR;
const INSTALL = path.join(ROOT, 'install.sh');
const GEN = path.join(ROOT, 'gen-htpasswd.sh');
const SERVICE = path.join(ROOT, 'speedex-mitm.service');
const TEST_PASS = 'TestPass-R04-9x';

// python3 可用性硬门（hashlib/base64 是标准库——缺失即 python 本身坏，fail-fast）
try {
  execFileSync('python3', ['-c', 'import hashlib, base64'], { stdio: 'pipe' });
} catch {
  assert.fail('本机 python3 缺标准库 hashlib/base64——python 环境损坏，修复后再跑；不许静默 skip');
}

test('R04：gen-htpasswd.sh 生成 {SHA} htpasswd——口令不打印、文件 0600、校验往返', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'speedex-htpasswd-'));
  fs.writeFileSync(path.join(dir, 'proxy.env'), `PROXY_AUTH=speedex:${TEST_PASS}\n`, { mode: 0o600 });
  const out = path.join(dir, 'htpasswd');
  const r = execFileSync('bash', [GEN], {
    encoding: 'utf8',
    // R04 二轮：environ 里放 stale 错误值——口令只认 0600 proxy.env 文件，env 必须被忽略
    env: { ...process.env, APP: dir, HTPASSWD_PYTHON: 'python3', HTPASSWD_OUT: out, SPEEDEX_PROXY_AUTH: 'speedex:env-stale-WRONG' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  assert.ok(!String(r).includes(TEST_PASS), 'stdout 绝不含口令');
  assert.equal(fs.statSync(out).mode & 0o777, 0o600);
  const line = fs.readFileSync(out, 'utf8').trim();
  assert.ok(line.startsWith('speedex:{SHA}'), `htpasswd 必须是 {SHA}（1dj3c 性能事故后 bcrypt 已废）：${line.slice(0, 22)}…`);
  // {SHA} 校验往返（mitmproxy 11 passlib / 12 自研 HtpasswdFile / 13+ 同算法）
  const check = (pw) =>
    execSync(
      `HTPW="$HTPW_FILE" python3 -c "import os,hashlib,base64;u,h=open(os.environ['HTPW']).read().strip().split(':',1);print('ok' if h=='{SHA}'+base64.b64encode(hashlib.sha1(os.environ['PW'].encode()).digest()).decode() else 'bad')"`,
      { encoding: 'utf8', env: { ...process.env, HTPW_FILE: out, HTPW: out, PW: pw }, shell: '/bin/bash' },
    ).trim();
  assert.equal(check(TEST_PASS), 'ok', '正确口令必须验过');
  assert.equal(check('wrong-pass'), 'bad', '错误口令必须拒');
  assert.equal(check('env-stale-WRONG'), 'bad', 'environ 里的 stale 口令绝不得生效——口令只从 proxy.env 文件读');
  // 缺 PROXY_AUTH → fail-closed 非零
  const dir2 = fs.mkdtempSync(path.join(os.tmpdir(), 'speedex-htpasswd-'));
  fs.writeFileSync(path.join(dir2, 'proxy.env'), 'OTHER=1\n');
  assert.throws(() =>
    execFileSync('bash', [GEN], { env: { ...process.env, APP: dir2, HTPASSWD_PYTHON: 'python3', HTPASSWD_OUT: path.join(dir2, 'htpasswd') }, stdio: 'pipe' }),
  );
});

test('R04：install.sh / service 静态合同——umask 077、口令不上 argv、不打印含凭据 URL', () => {
  execFileSync('bash', ['-n', INSTALL]); // 语法
  execFileSync('bash', ['-n', GEN]);
  const src = readSource('deploy/hk-proxy/install.sh');
  const gen = readSource('deploy/hk-proxy/gen-htpasswd.sh');
  assert.ok(src.includes('umask 077'), 'install.sh 必须 umask 077 前置');
  assert.ok(src.indexOf('umask 077') < src.indexOf('proxy.env'), 'umask 必须先于任何产出');
  assert.ok(!src.includes('${PASS}@'), 'stdout 不得再打印含凭据的完整 URL');
  assert.ok(!src.includes('--proxyauth "${PROXY_AUTH}"'), 'install.sh 不得保留 argv 形态的 proxyauth');
  // R04 二轮残留：口令不得经子进程 environ（AGENTS：secret 不上 process-environ 邻接面）
  assert.ok(!gen.includes('SPEEDEX_PROXY_AUTH'), 'gen-htpasswd 不得再经 SPEEDEX_PROXY_AUTH environ 传口令——python 直读 0600 proxy.env');
  // R04 二轮残留：unit 或 addon 变更后必须 restart——否则对 active 旧部署升级时
  // 新 ExecStart/addon 不生效（is-active 旧进程继续跑旧代码）
  assert.ok(src.includes('RESTART_NEEDED'), 'install.sh 必须检测 unit/addon 变更（RESTART_NEEDED）');
  // 2026-09-08 起 unit 名参数化（$UNIT，实验实例脚手架）：钉两点而非旧字面量——
  // ①生产分支默认 UNIT=speedex-mitm（生产合同不变；\b 边界防 speedex-mitm-exp 误配）；
  // ②变更过且 active → 必须经 systemctl restart "$UNIT"（幂等，restart 行为仍存在）
  assert.ok(/^\s*UNIT=speedex-mitm$/m.test(src), '生产分支必须默认 UNIT=speedex-mitm（生产路径不变）');
  assert.ok(src.includes('systemctl restart "$UNIT"'), '变更过且 active → 必须 systemctl restart "$UNIT"（幂等）');
  // R29 残留：venv 复用前校验 python 版本与当前钉选集匹配，不匹配重建；末尾打印版本摘要
  assert.ok(src.includes('VENV_PYVER'), 'venv 复用前必须校验其 python 版本（VENV_PYVER）');
  assert.ok(src.includes('版本摘要'), 'install 末尾必须打印实际运行版本摘要（mitmproxy/python/关键依赖）');
  const svc = readSource('deploy/hk-proxy/speedex-mitm.service');
  assert.ok(svc.includes('--proxyauth @/opt/speedex-mitm/htpasswd'), 'service 必须走 htpasswd 文件');
  assert.ok(!svc.includes('${PROXY_AUTH}'), 'service 不得把凭据展开为 argv');
  assert.ok(!svc.includes('EnvironmentFile'), 'service 不再需要 EnvironmentFile 注入凭据');
  assert.ok(src.includes(`:${HK_CTRL_PORT}`), 'install.sh 控制面端口与 hk-timing-client HK_CTRL_PORT 一致（漂移门）');
});

// 实验端口漂移门（2026-09-09 review ③）：install.sh 实验分支常量与实验 unit 是
// 两份可独立编辑的端口面——漂移则隔离护栏比对的是过期常量。静态钉：两边一致、
// 值锁定 18544/18072、且与生产 8443/8072 零冲突（全平台可跑，无 GNU 能力依赖）。
test('实验端口漂移门：install.sh 实验分支 ↔ speedex-mitm-exp.service 一致且 ≠ 生产 8443/8072', () => {
  const src = readSource('deploy/hk-proxy/install.sh');
  const expUnit = readSource('deploy/hk-proxy/speedex-mitm-exp.service');
  const branchPorts = (name) => {
    const block = src.match(new RegExp(`^\\s*${name}\\)([\\s\\S]*?);;`, 'm'));
    assert.ok(block, `install.sh 缺 ${name} 分支`);
    const get = (key) => {
      const m = block[1].match(new RegExp(`^\\s*${key}=(\\d+)\\s*$`, 'm'));
      assert.ok(m, `${name} 分支缺 ${key}`);
      return Number(m[1]);
    };
    return { listen: get('LISTEN_PORT'), ctrl: get('CTRL_PORT') };
  };
  const prod = branchPorts('production');
  const exp = branchPorts('experiment');
  // 生产合同不变（8072 = hk-timing-client HK_CTRL_PORT 单一来源）
  assert.deepEqual(prod, { listen: 8443, ctrl: HK_CTRL_PORT }, '生产端口必须保持 8443/8072');
  // 实验常量钉值（改值=有意变更，须同步本钉与 unit）
  assert.deepEqual(exp, { listen: 18544, ctrl: 18072 }, '实验端口锁定 18544/18072');
  // unit 文件与 install 常量一致（unit: Environment=SPEEDEX_HK_CTRL_PORT / --listen-port）
  const unitListen = expUnit.match(/--listen-port (\d+)/);
  const unitCtrl = expUnit.match(/^Environment=SPEEDEX_HK_CTRL_PORT=(\d+)$/m);
  assert.ok(unitListen && unitCtrl, '实验 unit 缺 --listen-port / SPEEDEX_HK_CTRL_PORT');
  assert.equal(Number(unitListen[1]), exp.listen, 'unit --listen-port 必须与 install 实验 LISTEN_PORT 一致');
  assert.equal(Number(unitCtrl[1]), exp.ctrl, 'unit SPEEDEX_HK_CTRL_PORT 必须与 install 实验 CTRL_PORT 一致');
  // 与生产零冲突（install.sh 隔离护栏的合同源：同机并存不 bind 冲突）
  for (const p of [exp.listen, exp.ctrl]) {
    assert.ok(p !== prod.listen && p !== prod.ctrl, `实验端口 ${p} 不得撞生产端口 8443/8072`);
  }
  assert.notEqual(exp.listen, exp.ctrl, '实验代理/控制面端口不得相同');
});

// GNU realpath -m 能力门（2026-09-09 review）：install.sh 的隔离门是 GNU 语义，部署目标
// 全为 Linux；macOS 系统 realpath 无 -m——本测试在**无能力的平台**不退化为假绿：
// 静态钉住「规范化实现存在」并显式记录 capability skip（tests/AGENTS：能力 skip 显式登记）。
const HAS_GNU_REALPATH = (() => {
  try {
    return execFileSync('realpath', ['-m', '--', '/tmp/x'], { stdio: ['ignore', 'pipe', 'pipe'], encoding: 'utf8' }).trim() === '/tmp/x';
  } catch {
    return false;
  }
})();

test('RHF-19：install.sh 实验目录隔离按规范化真实路径——尾斜杠/点段/子目录/symlink 别名一律在写入前拒绝', () => {
  const src = readSource('deploy/hk-proxy/install.sh');
  assert.ok(src.includes('realpath -m'), '隔离门必须经 realpath -m 规范化（静态合同——全平台钉）');
  if (!HAS_GNU_REALPATH) {
    // 能力缺失（macOS）：行为用例属 GNU 语义——显式登记 skip，不冒充已验证
    console.log('capability skip: GNU realpath -m 不可用（非 Linux 测试环境）——行为用例跳过，静态合同已钉');
    return;
  }
  // 仅执行真实脚本的前置护栏（拒绝路径在 apt/任何写入前 exit）——不做实际安装
  const run = (app) => {
    try {
      execFileSync('bash', [INSTALL], {
        encoding: 'utf8',
        env: { ...process.env, SPEEDEX_HK_INSTANCE: 'experiment', APP: app },
        stdio: ['ignore', 'pipe', 'pipe'],
        timeout: 15000,
      });
      return { status: 0, stderr: '' };
    } catch (e) {
      return { status: e.status, stderr: String(e.stderr || '') };
    }
  };
  for (const app of ['/opt/speedex-mitm', '/opt/speedex-mitm/', '/opt//speedex-mitm/./', '/opt/speedex-mitm/sub', '/opt/speedex-mitm/sub/../']) {
    const r = run(app);
    assert.notEqual(r.status, 0, `${app} 必须拒绝（旧裸字符串比较被尾斜杠绕过——RHF-19 反例）`);
    assert.ok(r.stderr.includes('拒绝：实验实例不得使用生产 APP'), `${app} stderr 缺拒绝文案: ${r.stderr.slice(0, 120)}`);
  }
  // symlink 别名（realpath -m 解析已存在组件——本机无 /opt/speedex-mitm 也可解析）
  const linkDir = fs.mkdtempSync(path.join(os.tmpdir(), 'speedex-rhf19-'));
  const link = path.join(linkDir, 'prod-alias');
  fs.symlinkSync('/opt/speedex-mitm', link);
  const rLink = run(link);
  assert.notEqual(rLink.status, 0, 'symlink 别名必须拒绝');
  assert.ok(rLink.stderr.includes('拒绝：实验实例不得使用生产 APP'), `symlink stderr 缺拒绝文案: ${rLink.stderr.slice(0, 120)}`);
});
