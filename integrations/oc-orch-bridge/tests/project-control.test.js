import {test} from "node:test"
import assert from "node:assert/strict"
import {normalizeConfig} from "../src/bridge.js"
import {ProjectControlBridge, createControlJournal, CONTROL_JOURNAL_KEY} from "../src/project-control.js"
import {makeTuiPlugin} from "../src/tui-core.js"
import {makeConfig, makeView, jsonBody} from "./helpers.js"

function storage(state = {schema: 1, entries: []}, options = {}) {
  let opens = 0, writes = 0
  return {
    state, get opens() {return opens}, get writes() {return writes},
    store(key) {
      opens++
      assert.equal(key, CONTROL_JOURNAL_KEY)
      return [state, async mutate => {
        writes++
        if (options.fail) throw new Error("disk full")
        const draft = structuredClone(state)
        mutate(draft)
        Object.assign(state, draft)
      }]
    },
  }
}

const rawConfig = () => makeConfig({expected: {...makeConfig().expected, releaseId: "rel-1"}})
test("durable key uses a host-valid storage segment (real 2.2.5 rejects colon)", () => {
  assert.match(CONTROL_JOURNAL_KEY, /^[a-zA-Z0-9_-]+$/)
})
function view(patch = {}) {
  return {ok: true, schema: "oc-orch.project-control.v1", project: "proj-a", session_id: "ses-1",
    read_only: false, project_control: {status: "running", revision: 0},
    session_control: {held: false, user_turn_active: false},
    lease: {holder_session: "ses-1", epoch: 7, expires_at: Date.now() + 60000, recovery_blocked: 0},
    runtime: makeView().runtime, operation: null, ...patch}
}
function fixture({fetcher = async () => {throw new TypeError("lost")}, config = rawConfig(), store = storage()} = {}) {
  const normalized = normalizeConfig(config).config
  const journal = createControlJournal(store)
  const bridge = new ProjectControlBridge(normalized, {journal, fetcher})
  return {bridge, store, journal, config}
}
function plan(bridge, patch = {}) {
  return bridge.plan({action: "pause", project: "proj-a", sessionId: "ses-1", view: view(),
    operationId: "op-control", reason: "test", confirm: "proj-a", ...patch})
}
function response(record, patch = {}) {
  return {ok: true, executed: true, action: "pause", project: "proj-a", session_id: "ses-1",
    result: {executed: true, applied: true, operation_id: record.operationId, project: "proj-a", session_id: "ses-1",
      revision: 1, epoch: 7, snapshot: {project: "proj-a", status: "paused", revision: 1}}, ...patch}
}

test("journal is lazy; real host storage is only opened on explicit use", () => {
  const {store, bridge} = fixture()
  assert.equal(store.opens, 0)
  bridge.dispose()
  assert.equal(store.opens, 0)
})

test("before POST the exact confirmed intent is durably possible", async () => {
  let record, calls = 0
  const f = fixture({fetcher: async () => {
    calls++
    assert.equal(f.store.writes, 1)
    assert.equal(f.store.state.entries[0].status, "possible")
    assert.deepEqual(f.store.state.entries[0].body, record.body)
    return jsonBody(response(record))
  }})
  record = plan(f.bridge).record
  const result = await f.bridge.executeControl(record)
  assert.equal(result.ok, true)
  assert.equal(result.status, "paused")
  assert.equal(calls, 1)
  assert.equal(f.store.state.entries[0].status, "acknowledged")
})

test("missing storage or failed durable write means zero POST", async () => {
  for (const store of [undefined, storage(undefined, {fail: true})]) {
    let calls = 0
    const bridge = new ProjectControlBridge(normalizeConfig(rawConfig()).config,
      {journal: createControlJournal(store), fetcher: async () => {calls++; return jsonBody({})}})
    assert.equal((await bridge.executeControl(plan(bridge).record)).code, "PERSISTENCE_REQUIRED")
    assert.equal(calls, 0)
  }
})

for (const [name, patch] of [
  ["readonly", {view: view({read_only: true})}],
  ["wrong holder", {view: view({lease: {...view().lease, holder_session: "other"}})}],
  ["expired", {view: view({lease: {...view().lease, expires_at: 1}})}],
  ["wrong scope", {project: "other"}],
  ["inexact confirm", {confirm: "proj-a "}],
  ["bool revision", {view: view({project_control: {status: "running", revision: false}})}],
  ["unknown action", {action: "unlock"}],
]) test(`plan fails closed: ${name}`, () => {
  const {bridge} = fixture()
  assert.equal(plan(bridge, patch).ok, false)
})

test("manifest false and unpinned release cannot be relaxed for control", () => {
  const {bridge} = fixture({config: makeConfig({expected: {writerContract: "continuity-v2", requireManifest: false}})})
  assert.equal(plan(bridge).ok, false)
})

test("unknown survives a reconstructed store/bridge; reconcile is GET-only and preserves newer current", async () => {
  const first = fixture()
  const record = plan(first.bridge).record
  assert.equal((await first.bridge.executeControl(record)).code, "UNKNOWN_OUTCOME")
  first.bridge.dispose()
  const durable = structuredClone(first.store.state)
  const calls = []
  const second = fixture({store: storage(durable), fetcher: async (url, init) => {
    calls.push({url, init})
    return jsonBody(view({project_control: {status: "running", revision: 2},
      operation: {operation_id: "op-control", action: "pause", project: "proj-a", session_id: "ses-1", revision_after: 1}}))
  }})
  assert.equal(second.bridge.pending().length, 1)
  const result = await second.bridge.reconcile("op-control")
  assert.equal(result.ok, true)
  assert.equal(result.applied_revision, 1)
  assert.equal(result.revision, 2)
  assert.equal(result.status, "running")
  assert.equal(calls.length, 1)
  assert.equal(calls[0].init.method, "GET")
  assert.equal(new URL(calls[0].url).searchParams.get("operation_id"), "op-control")
  assert.equal(second.bridge.pending().length, 0)
})

test("no original receipt never means not executed and never resends", async () => {
  const f = fixture()
  await f.bridge.executeControl(plan(f.bridge).record)
  let posts = 0
  f.bridge.fetcher = async (_, init) => {if (init.method === "POST") posts++; return jsonBody(view())}
  assert.equal((await f.bridge.reconcile("op-control")).code, "UNKNOWN_OUTCOME")
  assert.equal(posts, 0)
  assert.equal(f.bridge.pending().length, 1)
})

test("same confirmed record cannot be executed twice", async () => {
  let posts = 0
  const f = fixture({fetcher: async () => {posts++; throw new Error("lost")}})
  const p = plan(f.bridge).record
  await f.bridge.executeControl(p)
  assert.equal((await f.bridge.executeControl(p)).code, "ALREADY_REGISTERED")
  assert.equal(posts, 1)
  assert.throws(() => {p.body.confirm = "other"}, TypeError)
})

test("release/baseUrl change cannot retarget an old intent", async () => {
  const first = fixture()
  await first.bridge.executeControl(plan(first.bridge).record)
  for (const config of [rawConfig(), {...rawConfig(), baseUrl: "http://127.0.0.1:9999"}]) {
    if (config.baseUrl.endsWith("8787")) config.expected.releaseId = "other"
    let calls = 0
    const f = fixture({config, store: storage(structuredClone(first.store.state)), fetcher: async () => {calls++; return jsonBody(view())}})
    assert.equal((await f.bridge.reconcile("op-control")).ok, false)
    assert.equal(calls, 0)
  }
})

test("500 JSON or malformed successful receipt remains possible", async () => {
  for (const reply of [jsonBody({ok: false, code: "internal"}, 500), jsonBody({ok: true}),
      jsonBody({ok: true, executed: false}), new Response("not json")]) {
    const f = fixture({fetcher: async () => reply})
    assert.equal((await f.bridge.executeControl(plan(f.bridge).record)).ok, false)
    assert.equal(f.bridge.pending().length, 1)
  }
})

test("definitive CAS refusal is not silently retried", async () => {
  const f = fixture({fetcher: async () => jsonBody({ok: false, code: "control_refused", message: "revision-changed"}, 400)})
  assert.equal((await f.bridge.executeControl(plan(f.bridge).record)).code, "control_refused")
  assert.equal(f.bridge.pending().length, 0)
})

test("dispose while persisting prevents POST but preserves possible identity", async () => {
  const f = fixture()
  const put = f.journal.put.bind(f.journal)
  f.journal.put = async entry => {await put(entry); f.bridge.dispose()}
  let calls = 0
  f.bridge.fetcher = async () => {calls++; return jsonBody({})}
  assert.equal((await f.bridge.executeControl(plan(f.bridge).record)).code, "UNKNOWN_OUTCOME")
  assert.equal(calls, 0)
  assert.equal(f.store.state.entries.length, 1)
})

test("corrupt durable records fail closed, never overwritten", () => {
  const f = fixture({store: storage({schema: 1, entries: [{operationId: "bad"}]})})
  assert.throws(() => f.bridge.pending(), /损坏/)
  assert.equal(f.store.writes, 0)
})

test("concurrent execution of the same intent reserves atomically, one POST only", async () => {
  let posts = 0
  const f = fixture({fetcher: async () => {posts++; throw new Error("lost")}})
  const record = plan(f.bridge).record
  const results = await Promise.all([f.bridge.executeControl(record), f.bridge.executeControl(record)])
  assert.equal(posts, 1)
  assert.ok(results.every(r => !r.ok))
  assert.equal(f.bridge.pending().length, 1)
})

test("readonly observation can resolve original receipt without regaining write permission", async () => {
  const f = fixture()
  await f.bridge.executeControl(plan(f.bridge).record)
  f.bridge.fetcher = async () => jsonBody(view({read_only: true, project_control: {status: "paused", revision: 1},
    operation: {operation_id: "op-control", action: "pause", project: "proj-a", session_id: "ses-1", revision_after: 1}}))
  assert.equal((await f.bridge.reconcile("op-control")).ok, true)
})

test("wrong-operation successful receipt is not acknowledged", async () => {
  let record
  const f = fixture({fetcher: async () => {
    const data = response(record)
    data.result.operation_id = "wrong"
    return jsonBody(data)
  }})
  record = plan(f.bridge).record
  assert.equal((await f.bridge.executeControl(record)).code, "UNKNOWN_OUTCOME")
  assert.equal(f.bridge.pending().length, 1)
})

test("journal capacity never evicts 128 unknown intents to permit another POST", async () => {
  const f = fixture()
  for (let i = 0; i < 128; i++) {
    const record = plan(f.bridge, {operationId: `op-${i}`}).record
    await f.journal.put({...record, status: "possible"})
  }
  let posts = 0
  f.bridge.fetcher = async () => {posts++; return jsonBody({})}
  assert.equal((await f.bridge.executeControl(plan(f.bridge).record)).code, "PERSISTENCE_REQUIRED")
  assert.equal(posts, 0)
  assert.equal(f.bridge.pending().length, 128)
})

test("real TUI command core registers three controls, confirms exact project, then calls shared REST", async () => {
  const calls = [], toasts = [], state = storage()
  let commands, posted
  const context = {options: rawConfig(), storage: state,
    keymap: {layer: fn => {commands = fn().commands}}, data: {session: {list: () => []}},
    ui: {dialog: {prompt: async () => "proj-a"}, toast: {show: value => toasts.push(value)}}}
  const cleanup = makeTuiPlugin(context, {fetcher: async (url, init) => {
    calls.push(init.method)
    if (init.method === "GET") return jsonBody(view())
    posted = JSON.parse(init.body)
    return jsonBody(response({operationId: posted.operation_id}))
  }})
  assert.equal(state.opens, 0)
  assert.equal(calls.length, 0)
  assert.equal(commands.length, 10)
  assert.ok(commands.every(c => !c.bind))
  await commands.find(c => c.id === "oc-orch.project.pause").run()
  assert.deepEqual(calls, ["GET", "POST"])
  assert.equal(posted.expected_epoch, 7)
  assert.equal(posted.expected_revision, 0)
  assert.ok(toasts.some(t => t.message.includes("当前=paused")))
  cleanup()
})
