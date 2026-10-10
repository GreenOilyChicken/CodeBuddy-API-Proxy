'use strict';

/**
 * 实时数据内存回归。
 *
 * 覆盖环形缓冲不再留住大请求体：
 *   - 超过 32KB 的正文被截断，且截断路径仍脱敏（密钥在 JSON 前部）
 *   - 只保留最新 20 条 bodyText，更旧的摘要还在
 *   - 无订阅者时不把大字符串放进事件
 *   - 事件上没有 body 对象
 *   - 慢消费者会被退订
 *   - 代理请求体上限、流式上游超时
 */

const http = require('http');
const assert = require('assert');
const path = require('path');
const { EventEmitter } = require('events');

const ROOT = path.join(__dirname, '..');
const live = require(path.join(ROOT, 'core', 'live.js'));
const util = require(path.join(ROOT, 'core', 'util.js'));

let passed = 0;
let failed = 0;
function check(name, fn) {
  try { fn(); console.log(`  ok   ${name}`); passed++; }
  catch (e) { console.error(`  FAIL ${name}\n       ${e.message}`); failed++; }
}

function mockRes({ block = false } = {}) {
  return {
    headersSent: false,
    writableEnded: false,
    destroyed: false,
    writes: [],
    writeHead() { this.headersSent = true; },
    write(chunk) { this.writes.push(String(chunk)); return !block; },
    end() { this.writableEnded = true; },
    once() {},
  };
}

function resetLive() {
  live.clear();
}

const SECRET = 'sk-live-' + 'd'.repeat(20);
const NOTE = 'sk-' + 'e'.repeat(16);
const BEARER = 'Bearer super-secret-token';

check('常量：正文窗口是 20 条 × 32KB，不再是 1MB', () => {
  assert.strictEqual(live.MAX_BODY_EVENTS, 20);
  assert.strictEqual(live.MAX_BODY_BYTES, 32 * 1024);
  assert.strictEqual(live.MAX_EVENTS, 200);
  assert.ok(util.MAX_PROXY_BODY_BYTES >= 1024 * 1024);
});

check('小请求体：parse 成功时按键名和值脱敏，max_tokens 保留', () => {
  const raw = Buffer.from(JSON.stringify({
    model: 'm',
    max_tokens: 10,
    api_key: SECRET,
    nested: { access_token: 'not-a-prefix-value' },
  }));
  const parsed = util.parseBodyForLive(raw, live.MAX_BODY_BYTES);
  assert.strictEqual(parsed.truncated, false);
  assert.strictEqual(parsed.parseError, null);
  assert.strictEqual(parsed.body.api_key, '***');
  assert.strictEqual(parsed.body.nested.access_token, '***');
  assert.strictEqual(parsed.body.max_tokens, 10);
  assert.ok(!parsed.bodyText.includes(SECRET));
  assert.ok(!parsed.bodyText.includes('not-a-prefix-value'));
  assert.strictEqual(parsed.bodyBytes, raw.length);
});

check('截断 + 密钥在 JSON 前部：bodyText 不含明文', () => {
  const tokenValue = 'hello-secret-value';
  const raw = Buffer.from(JSON.stringify({
    max_tokens: 10,
    token: tokenValue,
    authorization: BEARER,
    api_key: SECRET,
    note: NOTE,
    blob: 'z'.repeat(60 * 1024),
  }));
  assert.ok(raw.length > live.MAX_BODY_BYTES);
  const parsed = util.parseBodyForLive(raw, live.MAX_BODY_BYTES);
  assert.strictEqual(parsed.truncated, true);
  assert.strictEqual(parsed.body, null);
  assert.ok(parsed.parseError);
  assert.ok(Buffer.byteLength(parsed.bodyText) <= live.MAX_BODY_BYTES);
  assert.ok(!parsed.bodyText.includes(SECRET), 'sk-live 泄漏');
  assert.ok(!parsed.bodyText.includes(NOTE), 'sk- 值泄漏');
  assert.ok(!parsed.bodyText.includes('super-secret-token'), 'authorization 值泄漏');
  assert.ok(!parsed.bodyText.includes(tokenValue), 'token 键的值泄漏');
  assert.ok(parsed.bodyText.includes('"max_tokens":10'), '非敏感键应保留');
  assert.ok(parsed.bodyText.includes('***'));
});

check('UTF-8 截断不超字节上限', () => {
  const raw = Buffer.from('你'.repeat(20000));
  const parsed = util.parseBodyForLive(raw, live.MAX_BODY_BYTES);
  assert.strictEqual(parsed.truncated, true);
  assert.ok(Buffer.byteLength(parsed.bodyText) <= live.MAX_BODY_BYTES);
  assert.strictEqual(parsed.body, null);
});

check('无订阅者：事件里没有大字符串，也没有密钥', () => {
  resetLive();
  assert.strictEqual(live.subscriberCount(), 0);
  const raw = Buffer.from(JSON.stringify({ api_key: SECRET, blob: 'q'.repeat(80 * 1024) }));
  const ev = live.publish({
    source: '/v1/chat/completions', kind: 'openai', model: 'big-model', stream: true,
    accountName: 'acc', rawBody: raw,
  });
  const dumped = JSON.stringify(ev);
  assert.ok(dumped.length < 1500, `无订阅者事件被放大了: ${dumped.length}`);
  assert.ok(!dumped.includes(SECRET));
  assert.ok(!dumped.includes('qqqqqqq'));
  assert.strictEqual(ev.body, null);
  assert.strictEqual(ev.bodyText, '');
  assert.strictEqual(ev.bodyBytes, raw.length);
  assert.strictEqual(ev.model, 'big-model');
  assert.strictEqual(ev.accountName, 'acc');
  assert.strictEqual(ev.rawBody, undefined);
});

check('有订阅者：连续 30 条只留最新 20 条正文，摘要仍在', () => {
  resetLive();
  const res = mockRes();
  live.subscribe(res, { limit: 50 });
  assert.strictEqual(live.subscriberCount(), 1);
  try {
    for (let i = 0; i < 30; i++) {
      const raw = Buffer.from(JSON.stringify({
        i,
        api_key: SECRET,
        note: NOTE,
        authorization: BEARER,
        messages: [{ role: 'user', content: 'y'.repeat(40 * 1024) }],
      }));
      live.publish({
        source: '/v1/responses',
        kind: 'responses',
        model: 'gpt-test',
        stream: false,
        accountId: 'id-' + i,
        accountName: 'name-' + i,
        headers: { 'content-type': 'application/json' },
        rawBody: raw,
      });
    }
    const all = live.snapshot();
    assert.strictEqual(all.length, 30);
    for (const ev of all) {
      assert.strictEqual(ev.body, null);
      assert.strictEqual(ev.rawBody, undefined);
      const dumped = JSON.stringify(ev);
      assert.ok(!dumped.includes(SECRET));
      assert.ok(!dumped.includes(NOTE));
      assert.ok(!dumped.includes('super-secret-token'));
    }
    for (const ev of all.slice(0, 10)) {
      assert.strictEqual(ev.bodyText, '', '窗口外不应留正文');
      assert.ok(ev.bodyBytes > live.MAX_BODY_BYTES);
      assert.strictEqual(ev.model, 'gpt-test');
      assert.strictEqual(ev.source, '/v1/responses');
      assert.ok(ev.accountName);
    }
    let textBytes = 0;
    for (const ev of all.slice(10)) {
      assert.ok(ev.bodyText, '窗口内应有 bodyText');
      const n = Buffer.byteLength(ev.bodyText);
      assert.ok(n <= live.MAX_BODY_BYTES, `bodyText ${n} 超过 32KB`);
      textBytes += n;
      assert.strictEqual(ev.truncated, true);
      assert.ok(ev.parseError);
    }
    const st = live.stats();
    assert.strictEqual(st.withBody, 20);
    assert.strictEqual(st.bodyTextBytes, textBytes);
    assert.ok(st.bodyTextBytes <= live.MAX_BODY_EVENTS * live.MAX_BODY_BYTES);
  } finally {
    live.unsubscribe(res);
  }
});

check('退订之后的新请求不再解析正文', () => {
  resetLive();
  const res = mockRes();
  live.subscribe(res, { limit: 10 });
  live.unsubscribe(res);
  assert.strictEqual(live.subscriberCount(), 0);
  const raw = Buffer.from(JSON.stringify({ api_key: SECRET, blob: 'z'.repeat(50 * 1024) }));
  const ev = live.publish({ source: '/v1/responses', kind: 'responses', model: 'later', rawBody: raw });
  assert.strictEqual(ev.bodyText, '');
  assert.strictEqual(ev.body, null);
  assert.ok(!JSON.stringify(ev).includes(SECRET));
  assert.strictEqual(ev.bodyBytes, raw.length);
});

check('慢消费者：积压超过上限后退订，队列被丢掉', () => {
  resetLive();
  const slow = mockRes({ block: true });
  live.subscribe(slow, { limit: 10 });
  assert.strictEqual(live.subscriberCount(), 1);
  const raw = Buffer.from(JSON.stringify({ blob: 'x'.repeat(40 * 1024), api_key: SECRET }));
  for (let i = 0; i < 40; i++) {
    live.publish({ source: '/v1/chat/completions', kind: 'openai', model: 'm', rawBody: raw });
  }
  assert.strictEqual(live.subscriberCount(), 0, '慢消费者应被退订');
  assert.strictEqual(slow.writableEnded, true);
  assert.ok(slow.__liveQueue == null || slow.__liveQueued === 0);
  const kept = live.snapshot();
  let textBytes = 0;
  for (const ev of kept) {
    assert.strictEqual(ev.body, null);
    if (ev.bodyText) textBytes += Buffer.byteLength(ev.bodyText);
  }
  assert.ok(textBytes <= live.MAX_BODY_EVENTS * live.MAX_BODY_BYTES);
});

function emitterReq() {
  const req = new EventEmitter();
  req.destroy = () => { req.destroyed = true; };
  return req;
}

async function testReadBody() {
  const okReq = emitterReq();
  const okP = util.readBody(okReq, 100);
  okReq.emit('data', Buffer.from('abc'));
  okReq.emit('end');
  const buf = await okP;
  assert.strictEqual(buf.toString(), 'abc');

  const bad = emitterReq();
  const badP = util.readBody(bad, 8);
  bad.emit('data', Buffer.alloc(5, 1));
  bad.emit('data', Buffer.alloc(5, 2));
  await assert.rejects(badP, (err) => err && err.code === 'BODY_TOO_LARGE');
  assert.strictEqual(bad.destroyed, true);
}

async function testStreamTimeout() {
  const srv = http.createServer(() => { /* 挂起，不回包 */ });
  await new Promise((resolve) => srv.listen(0, '127.0.0.1', resolve));
  const port = srv.address().port;
  const clientRes = {
    headersSent: false,
    writeHead() { this.headersSent = true; },
    end() { this.ended = true; },
  };
  const started = Date.now();
  try {
    await assert.rejects(
      util.pipeSseToClient(clientRes, `http://127.0.0.1:${port}/`, {
        method: 'POST', body: '{}', timeoutMs: 200,
      }),
      (err) => err && /timeout/i.test(err.message),
    );
    assert.ok(Date.now() - started < 5000, '超时应在数秒内结束');
    assert.strictEqual(clientRes.headersSent, true);
  } finally {
    await new Promise((resolve) => srv.close(resolve));
  }
}

(async () => {
  try {
    await testReadBody();
    check('readBody：超过上限返回 BODY_TOO_LARGE 并销毁连接', () => {});
  } catch (e) {
    check('readBody：超过上限返回 BODY_TOO_LARGE 并销毁连接', () => { throw e; });
  }

  try {
    await testStreamTimeout();
    check('流式转发：空闲超时会中断挂起的上游连接', () => {});
  } catch (e) {
    check('流式转发：空闲超时会中断挂起的上游连接', () => { throw e; });
  }

  resetLive();
  console.log(`\n断言：${passed} 通过, ${failed} 失败`);
  process.exit(failed ? 1 : 0);
})();
