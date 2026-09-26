import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';

// Exercise the exact embedded console script without a browser dependency.
// Any HTML injection API is forbidden by the stub; server text stays text.
const html = readFileSync(new URL('../crates/agentd-server/src/console.html', import.meta.url), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
const runId = '11111111-1111-4111-8111-111111111111';
const requestId = '22222222-2222-4222-8222-222222222222';
const run = { run_id: runId, agent_ref: 'bot', scope: 'chat', status: 'succeeded', created_at: '2026-09-26T00:00:00Z' };
const event = (id, extra = {}) => ({
  id, ts: '2026-09-26T00:00:00Z', tenant: 'live', actor_kind: 'api', actor_id: 'shared_api_token',
  action: 'memory.put', outcome: 'succeeded', request_id: requestId, run_id: runId,
  resource_type: 'memory', resource_id: 'entry', details: { count: id }, ...extra,
});
const settle = () => new Promise(resolve => setImmediate(resolve));

class Element {
  constructor(tagName, id = '') {
    this.tagName = tagName.toUpperCase();
    this.id = id;
    this.children = [];
    this.value = '';
    this._text = '';
    this.disabled = false;
    this.hidden = false;
    this.attributes = new Map();
  }
  get textContent() { return this._text + this.children.map(child => child.textContent).join(''); }
  set textContent(value) { this._text = String(value ?? ''); this.children = []; }
  set innerHTML(_) { throw new Error('Untrusted HTML insertion is forbidden'); }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) {
    this._text = '';
    this.children = children;
    if (this.tagName === 'SELECT') this.value = children[0]?.value || '';
  }
  setAttribute(name, value) { this.attributes.set(name, value); }
}

async function harness({ tenants = ['live'], audit = () => ({ events: [], next_before_id: null }) } = {}) {
  const elements = new Map();
  for (const match of html.matchAll(/<([\w-]+)\b([^>]*\bid="([^"]+)"[^>]*)>/g)) {
    const element = new Element(match[1], match[3]);
    element.disabled = /\bdisabled\b/.test(match[2]);
    element.hidden = /\bhidden\b/.test(match[2]);
    elements.set(match[3], element);
  }
  const calls = [];
  const tags = [];
  const saved = new Map([['agentd.console.token', 'saved-private-token']]);
  const responder = { audit };
  const context = vm.createContext({
    document: {
      getElementById(id) { assert.ok(elements.has(id), `Unknown DOM element: ${id}`); return elements.get(id); },
      createElement(tag) { tags.push(tag); return new Element(tag); },
    },
    sessionStorage: {
      getItem: key => saved.get(key) ?? null,
      setItem: (key, value) => saved.set(key, value),
      removeItem: key => saved.delete(key),
    },
    URLSearchParams,
    setInterval() { throw new Error('The console must not poll automatically'); },
    fetch: async (path, options) => {
      const url = new URL(path, 'http://agentd.local');
      calls.push({ path, url, options });
      let body;
      if (url.pathname.endsWith('/audit')) body = await responder.audit(url);
      else if (url.pathname === '/v1/tenants') body = { tenants };
      else if (url.pathname.endsWith('/agents')) body = [{ name: 'bot' }];
      else if (url.pathname.endsWith('/runs')) body = [run];
      else if (url.pathname.endsWith('/trace')) body = [];
      else if (url.pathname.endsWith('/deliveries')) body = { deliveries: [] };
      else if (url.pathname.endsWith('/runs/' + runId)) body = run;
      else throw new Error('Unexpected request: ' + path);
      const status = body?.httpStatus || 200;
      return { ok: status < 400, status, text: async () => JSON.stringify(body) };
    },
  });
  vm.runInContext(script, context, { filename: 'console.html' });
  await settle();
  return {
    context, calls, tags, responder,
    $: id => elements.get(id),
    auditCalls: () => calls.filter(call => call.url.pathname.endsWith('/audit')),
    ids: () => JSON.parse(vm.runInContext('JSON.stringify(state.auditEvents.map(event => event.id))', context)),
  };
}

test('Runs remain available; Audit uses descending pagination, refresh and the saved token', async () => {
  const app = await harness({ audit: url => url.searchParams.has('before_id')
    ? { events: [event(28)], next_before_id: null }
    : { events: [event(30), event(29)], next_before_id: 29 } });
  assert.equal(app.$('count').textContent, '1');
  assert.equal(JSON.parse(app.$('run').textContent).run_id, runId);
  await app.$('view-audit').onclick();
  assert.equal(app.$('audit-view').hidden, false);
  assert.equal(app.$('run-view').hidden, true);
  assert.equal(app.auditCalls()[0].url.pathname, '/v1/audit');
  assert.equal(app.auditCalls()[0].url.searchParams.get('limit'), '50');
  assert.deepEqual(app.ids(), [30, 29]);
  assert.equal(app.$('audit-more').hidden, false);
  await app.$('audit-more').onclick();
  assert.equal(app.auditCalls()[1].url.searchParams.get('before_id'), '29');
  assert.deepEqual(app.ids(), [30, 29, 28]);
  assert.equal(app.$('audit-more').hidden, true);
  app.responder.audit = () => ({ events: [event(31), event(30)], next_before_id: 30 });
  await app.$('audit-refresh').onclick();
  assert.equal(app.auditCalls().at(-1).url.searchParams.has('before_id'), false);
  assert.deepEqual(app.ids(), [31, 30]);
  assert.ok(app.calls.every(call => call.options.headers.authorization === 'Bearer saved-private-token'));
  assert.ok(app.calls.every(call => !call.options.method || call.options.method === 'GET'));
  const count = app.calls.length;
  await settle();
  await settle();
  assert.equal(app.calls.length, count, 'No background polling');
  await app.$('view-runs').onclick();
  assert.equal(app.$('run-view').hidden, false);
  assert.equal(JSON.parse(app.$('run').textContent).run_id, runId);
});

test('Deleted tenant and all filters are encoded; blank tenant selects global history', async () => {
  const app = await harness();
  app.$('audit-tenant').value = 'deleted / tenant';
  app.$('audit-action').value = 'memory.put';
  app.$('audit-outcome').value = 'succeeded';
  app.$('audit-request-id').value = requestId;
  app.$('audit-run-id').value = runId;
  await app.$('view-audit').onclick();
  const url = app.auditCalls().at(-1).url;
  assert.equal(url.pathname, '/v1/tenants/deleted%20%2F%20tenant/audit');
  for (const [key, value] of [['action', 'memory.put'], ['outcome', 'succeeded'], ['request_id', requestId], ['run_id', runId]]) {
    assert.equal(url.searchParams.get(key), value);
  }
  assert.equal(url.searchParams.has('tenant'), false);
  app.$('audit-tenant').value = ' ';
  await app.$('audit-tenant').onchange();
  assert.equal(app.auditCalls().at(-1).url.pathname, '/v1/audit');
});

test('A selected run opens its own audit and resets unrelated filters', async () => {
  const app = await harness();
  for (const id of ['audit-action', 'audit-outcome', 'audit-request-id']) app.$(id).value = 'old-filter';
  app.$('run-audit').onclick();
  await settle();
  const url = app.auditCalls().at(-1).url;
  assert.equal(url.pathname, '/v1/tenants/live/audit');
  assert.equal(url.searchParams.get('run_id'), runId);
  for (const key of ['action', 'outcome', 'request_id']) assert.equal(url.searchParams.has(key), false);
});

test('Event fields and details are rendered as literal text', async () => {
  const untrusted = '<img src=x onerror="globalThis.pwned=true">';
  const app = await harness({ audit: () => ({ events: [event(9, {
    action: untrusted, actor_id: untrusted, details: { nested: { message: untrusted } },
  }), event(8)], next_before_id: null }) });
  await app.$('view-audit').onclick();
  assert.ok(app.$('audit-events').textContent.includes(untrusted));
  assert.ok(app.$('audit-details').textContent.includes('<img'));
  assert.equal(JSON.parse(app.$('audit-details').textContent).nested.message, untrusted);
  assert.equal(app.context.pwned, undefined);
  assert.equal(app.tags.includes('img'), false);
  app.$('audit-events').children[1].onclick();
  assert.equal(JSON.parse(app.$('audit-event').textContent).id, 8);
  assert.equal(JSON.parse(app.$('audit-details').textContent).count, 8);
});

test('Stale pages cannot replace newer filters; failed More retains its cursor for retry', async () => {
  let release;
  const app = await harness({ audit: () => new Promise(resolve => { release = resolve; }) });
  const oldRequest = app.$('view-audit').onclick();
  await settle();
  app.responder.audit = () => ({ events: [event(20)], next_before_id: 20 });
  app.$('audit-action').value = 'run.submit';
  await app.$('audit-action').onchange();
  release({ events: [event(99)], next_before_id: 99 });
  await oldRequest;
  assert.deepEqual(app.ids(), [20]);
  app.responder.audit = () => ({ httpStatus: 500, error: '<b>temporary error</b>' });
  await app.$('audit-more').onclick();
  assert.deepEqual(app.ids(), [20]);
  assert.equal(app.$('audit-message').textContent, '<b>temporary error</b>');
  assert.equal(app.$('audit-more').disabled, false);
  app.responder.audit = () => ({ events: [event(19)], next_before_id: null });
  await app.$('audit-more').onclick();
  assert.equal(app.auditCalls().at(-1).url.searchParams.get('before_id'), '20');
  assert.deepEqual(app.ids(), [20, 19]);
});

test('Audit works with no live tenants, and editing a filter before More starts a fresh page', async () => {
  const app = await harness({ tenants: [], audit: () => ({ events: [event(10)], next_before_id: 10 }) });
  assert.equal(app.$('tenant').disabled, true);
  assert.equal(app.$('audit-tenant').disabled, false);
  await app.$('view-audit').onclick();
  app.$('audit-tenant').value = 'deleted-only';
  app.responder.audit = () => ({ events: [event(7)], next_before_id: null });
  await app.$('audit-more').onclick();
  const url = app.auditCalls().at(-1).url;
  assert.equal(url.pathname, '/v1/tenants/deleted-only/audit');
  assert.equal(url.searchParams.has('before_id'), false);
  assert.deepEqual(app.ids(), [7]);
});
