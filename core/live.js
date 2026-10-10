'use strict';

/**
 * 实时数据（Live Monitor）：内存 ring buffer + SSE 广播中心。
 *
 * 代理入口把客户端原始请求体交给 publish()。无订阅者时只留摘要（字节数、
 * 模型、账号等），不把正文解析进堆。有订阅者时也只给最新 MAX_BODY_EVENTS 条
 * 保留一份截断后的 bodyText，更旧的事件主动清掉正文。进程重启即清空，不落盘。
 */

/** ring buffer 容量：超过后丢弃最旧的事件。摘要很小，200 条可以留着。 */
const MAX_EVENTS = 200;
/** 带正文的事件窗口。超出后立刻清掉 bodyText，不必等被 200 条挤出。 */
const MAX_BODY_EVENTS = 20;
/** 单条正文上限。由 publish 传给 parseBodyForLive，避免两处各写一个魔法数。 */
const MAX_BODY_BYTES = 32 * 1024;
/** 单个 SSE 订阅者在用户态队列里最多积压这么多字节，超出视为慢消费者并退订。 */
const MAX_PENDING_BYTES = 1024 * 1024;
/** write() 返回 false 后若一直等不到 drain，就退订，避免内核缓冲一直占着。 */
const DRAIN_TIMEOUT_MS = 10000;
/** 心跳间隔：防止中间代理按空闲超时掐断长连接 */
const HEARTBEAT_MS = 15000;

let seq = 0;
let events = [];                 // 旧 → 新
const subscribers = new Set();   // SSE 客户端 res 集合

const util = require('./util');

function armDrainTimer(res) {
  if (!res || res.__liveDrainTimer || res.__liveDropped) return;
  const timer = setTimeout(() => {
    res.__liveDrainTimer = null;
    dropSlowSubscriber(res);
  }, DRAIN_TIMEOUT_MS);
  if (timer.unref) timer.unref();
  res.__liveDrainTimer = timer;
}

function clearDrainTimer(res) {
  if (res && res.__liveDrainTimer) {
    clearTimeout(res.__liveDrainTimer);
    res.__liveDrainTimer = null;
  }
}

/** 慢消费者：丢掉尚未写出的队列并退订，队列里的大字符串可以马上被回收。 */
function dropSlowSubscriber(res) {
  if (!res) return;
  if (res.__liveDropped) {
    unsubscribe(res);
    return;
  }
  res.__liveDropped = true;
  res.__liveQueue = [];
  res.__liveQueued = 0;
  unsubscribe(res);
  try {
    if (!res.writableEnded && typeof res.end === 'function') res.end();
  } catch { /* 连接已死时 end 会抛，忽略 */ }
}

/**
 * 把一段 SSE 文本放进该订阅者的队列并尽量写出。
 * opts.dropOnOverflow === false 时，队列放不下就停（用于回放），不立刻踢掉连接。
 * @returns {boolean} false 表示这条没写入（已退订，或回放被量控截断）
 */
function writeChunk(res, text, opts) {
  const dropOnOverflow = !opts || opts.dropOnOverflow !== false;
  if (!res || res.__liveDropped || res.writableEnded || res.destroyed) {
    unsubscribe(res);
    return false;
  }
  const bytes = Buffer.byteLength(text);
  const queued = res.__liveQueued || 0;
  if (queued + bytes > MAX_PENDING_BYTES) {
    if (dropOnOverflow) dropSlowSubscriber(res);
    return false;
  }
  if (!res.__liveQueue) res.__liveQueue = [];
  res.__liveQueue.push(text);
  res.__liveQueued = queued + bytes;
  flushSubscriber(res);
  return true;
}

function flushSubscriber(res) {
  if (!res || res.__liveDropped || res.__liveFlushing) return;
  const queue = res.__liveQueue;
  if (!queue || !queue.length) return;
  res.__liveFlushing = true;
  while (queue.length) {
    const text = queue[0];
    let ok = true;
    try {
      ok = res.write(text);
    } catch {
      res.__liveFlushing = false;
      unsubscribe(res);
      return;
    }
    queue.shift();
    res.__liveQueued -= Buffer.byteLength(text);
    if (res.__liveQueued < 0) res.__liveQueued = 0;
    if (!ok) {
      armDrainTimer(res);
      res.once('drain', () => {
        clearDrainTimer(res);
        res.__liveFlushing = false;
        if (!res.__liveDropped) flushSubscriber(res);
      });
      return;
    }
  }
  res.__liveFlushing = false;
  clearDrainTimer(res);
}

/** 序列化一条 SSE 消息（消息末尾的空行是 SSE 的分帧边界） */
function sseMessage(event, data) {
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

/** 只保留摘要字段。rawBody / body 对象一律不进事件。 */
function blankEvent(input) {
  const src = input && typeof input === 'object' ? input : {};
  return {
    id: ++seq,
    ts: Date.now(),
    source: src.source || '',
    kind: src.kind || '',
    model: src.model || '',
    stream: !!src.stream,
    accountId: src.accountId || '',
    accountName: src.accountName || '',
    apiKeyId: src.apiKeyId || '',
    apiKeyName: src.apiKeyName || '',
    headers: src.headers && typeof src.headers === 'object' ? src.headers : {},
    body: null,
    bodyText: '',
    bodyBytes: Number.isFinite(src.bodyBytes) ? src.bodyBytes : 0,
    truncated: !!src.truncated,
    parseError: src.parseError || null,
  };
}

function rawBodyBytes(rawBody) {
  if (Buffer.isBuffer(rawBody)) return rawBody.length;
  if (typeof rawBody === 'string') return Buffer.byteLength(rawBody);
  if (rawBody == null) return 0;
  return Buffer.byteLength(String(rawBody));
}

/** 正文窗口之外的事件立刻丢掉 body / bodyText，摘要字段不动。 */
function releaseOldBodies() {
  const keepFrom = Math.max(0, events.length - MAX_BODY_EVENTS);
  for (let i = 0; i < keepFrom; i++) {
    const ev = events[i];
    if (!ev) continue;
    if (ev.bodyText) ev.bodyText = '';
    if (ev.body != null) ev.body = null;
  }
}

/**
 * 写入一条事件。调用方传 rawBody（原始 Buffer / 字符串），由这里决定要不要解析。
 * 订阅数为 0 时只记字节数，不 toString、不 JSON.parse、不深拷贝。
 * 有订阅者时才截断、解析、脱敏，并且只给最新 MAX_BODY_EVENTS 条留 bodyText。
 * @returns {object} 最终写入的事件对象（含 id / ts）
 */
function publish(evt) {
  const input = evt && typeof evt === 'object' ? evt : {};
  const hasRaw = Object.prototype.hasOwnProperty.call(input, 'rawBody');
  const event = blankEvent(input);

  if (hasRaw) {
    const rawBody = input.rawBody;
    // 只有「正在看实时页」时才解析正文。新事件总是落在最新 20 条里，
    // 所以这里解析一次即可；滑出窗口的旧正文由 releaseOldBodies 清掉。
    if (subscribers.size > 0 && rawBody != null) {
      const parsed = util.parseBodyForLive(rawBody, MAX_BODY_BYTES);
      event.bodyBytes = parsed.bodyBytes;
      event.truncated = !!parsed.truncated;
      event.parseError = parsed.parseError;
      event.bodyText = typeof parsed.bodyText === 'string' ? parsed.bodyText : '';
    } else {
      event.bodyBytes = rawBodyBytes(rawBody);
      event.truncated = false;
      event.parseError = null;
      event.bodyText = '';
    }
    event.body = null;
  }

  events.push(event);
  if (events.length > MAX_EVENTS) events = events.slice(events.length - MAX_EVENTS);
  releaseOldBodies();

  if (subscribers.size) {
    const frame = sseMessage('live', event);
    for (const res of [...subscribers]) writeChunk(res, frame);
  }
  return event;
}

/**
 * 取最近的事件快照。返回的是事件对象本身（正文已被窗口裁过），不是深拷贝。
 * @param {number} [limit] 最多返回条数，默认全部，上限 MAX_EVENTS
 * @returns {object[]} 按 ts 升序（旧 → 新）
 */
function snapshot(limit) {
  let n = Number(limit);
  if (!Number.isFinite(n) || n <= 0) n = events.length;
  n = Math.min(Math.floor(n), MAX_EVENTS);
  return n >= events.length ? events.slice() : events.slice(events.length - n);
}

/**
 * 注册一个 SSE 订阅者：写好响应头，先发 ready，再回放历史事件。
 * 回放走同一条带背压的队列：慢客户端最多积压 MAX_PENDING_BYTES，放不下就截断回放。
 * 历史里没有正文是预期行为——没人订阅时根本没存正文。
 * @param {import('http').ServerResponse} res
 * @param {{limit?: number}} [opts]
 */
function subscribe(res, opts) {
  const limit = Math.min(Math.max(parseInt((opts && opts.limit), 10) || 100, 1), MAX_EVENTS);
  subscribers.add(res);
  res.__liveDropped = false;
  res.__liveQueue = [];
  res.__liveQueued = 0;

  if (!res.headersSent && typeof res.writeHead === 'function') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
      ...util.corsHeaders(),
    });
  }

  const replay = snapshot(limit);
  // 回放放不下时停，不把这个刚连上的客户端立刻踢掉；后续实时事件仍走 drop 路径。
  const replayOpts = { dropOnOverflow: false };
  writeChunk(res, sseMessage('ready', { connected: true, seq, replayed: replay.length }), replayOpts);
  if (!res.__liveDropped) {
    for (const ev of replay) {
      if (!writeChunk(res, sseMessage('live', ev), replayOpts)) break;
    }
  }

  if (res.__liveDropped) return;
  const heartbeat = setInterval(() => { writeChunk(res, ':hb\n\n'); }, HEARTBEAT_MS);
  if (heartbeat.unref) heartbeat.unref();
  res.__liveHeartbeat = heartbeat;
}

/** 注销一个 SSE 订阅者，并清掉它的心跳、积压队列和 drain 定时器 */
function unsubscribe(res) {
  if (!res) return;
  subscribers.delete(res);
  clearDrainTimer(res);
  if (res.__liveHeartbeat) {
    clearInterval(res.__liveHeartbeat);
    res.__liveHeartbeat = null;
  }
  res.__liveQueue = null;
  res.__liveQueued = 0;
  res.__liveDropped = true;
}

/** 当前 SSE 订阅者数量。为 0 时 publish 不解析正文。 */
function subscriberCount() { return subscribers.size; }

/** 清空 ring buffer（不动 id 自增，避免前端看到 id 回退） */
function clear() { events = []; }

/** 运行时统计。bodyTextBytes / withBody 用来核对正文窗口有没有重新胀起来。 */
function stats() {
  let bodyTextBytes = 0;
  let withBody = 0;
  for (const ev of events) {
    const text = ev && ev.bodyText;
    if (typeof text === 'string' && text.length) {
      withBody++;
      bodyTextBytes += Buffer.byteLength(text);
    }
  }
  return {
    seq,
    count: events.length,
    subscribers: subscribers.size,
    maxEvents: MAX_EVENTS,
    maxBodyEvents: MAX_BODY_EVENTS,
    maxBodyBytes: MAX_BODY_BYTES,
    withBody,
    bodyTextBytes,
  };
}

module.exports = {
  publish, snapshot, subscribe, unsubscribe, subscriberCount, clear, stats,
  MAX_EVENTS, MAX_BODY_EVENTS, MAX_BODY_BYTES, MAX_PENDING_BYTES,
};
