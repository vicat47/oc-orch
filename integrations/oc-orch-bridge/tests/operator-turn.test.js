import {test} from "node:test"
import assert from "node:assert/strict"
import {OperatorTurnBridge} from "../src/operator-turn.js"
import {createControlJournal} from "../src/project-control.js"
import {normalizeConfig} from "../src/bridge.js"
import {makeConfig, makeView, jsonBody} from "./helpers.js"

function fixture({failStorage = false, fetcher} = {}) {
  const state = {schema: 1, entries: []}, calls = []
  const storage = {store: () => [state, async fn => {
    if (failStorage) throw new Error("disk full")
    const draft = structuredClone(state); fn(draft); Object.assign(state, draft)
  }]}
  const journal = createControlJournal(storage, {key: "operator-turn-intents-v1", actions: ["message"]})
  const config = normalizeConfig(makeConfig({expected: {...makeConfig().expected, releaseId: "rel-1"}})).config
  const bridge = new OperatorTurnBridge(config, {journal, fetcher: async (url, init = {}) => {
    calls.push({url, init}); assert.ok(state.entries.length, "durable before POST")
    return fetcher ? fetcher(url, init) : Promise.reject(new Error("lost"))
  }})
  const view = {ok: true, schema: "oc-orch.project-control.v1", project: "proj-a", session_id: "ses-1", read_only: false,
    project_control: {status: "paused", revision: 2}, session_control: {held: true, user_turn_active: false, revision: 4},
    lease: {holder_session: "ses-1", epoch: 7, expires_at: Date.now() + 60000, recovery_blocked: 0}, runtime: makeView().runtime}
  const plan = (patch = {}) => bridge.planMessage({project: "proj-a", sessionId: "ses-1", view,
    operationId: "message-op", text: "supplement", confirm: "proj-a", ...patch})
  return {bridge, plan, journal, state, calls, view}
}

test("explicit message uses session revision, keeps pause/hold unchanged", () => {
  const f = fixture(); const p = f.plan()
  assert.equal(p.ok, true); assert.equal(p.record.body.expected_revision, 4)
  assert.equal(f.view.project_control.status, "paused")
  assert.equal(f.view.session_control.held, true)
  assert.equal(f.calls.length, 0)
})

test("unknown exact intent persists, second send zero new POST", async () => {
  const f = fixture(); const record = f.plan().record
  assert.equal((await f.bridge.sendMessage(record)).code, "UNKNOWN_OUTCOME")
  assert.equal((await f.bridge.sendMessage(record)).code, "PERSISTENCE_REQUIRED")
  assert.equal(f.calls.length, 1); assert.equal(f.state.entries[0].status, "possible")
})

test("durable failure and inexact confirmation zero POST", async () => {
  const f = fixture({failStorage: true})
  assert.equal(f.plan({confirm: "proj-a "}).ok, false)
  assert.equal((await f.bridge.sendMessage(f.plan().record)).code, "PERSISTENCE_REQUIRED")
  assert.equal(f.calls.length, 0)
})

test("missing actual session revision and forged record fail closed", async () => {
  const f = fixture()
  assert.equal(f.plan({view: {...f.view, session_control: {...f.view.session_control, revision: undefined}}}).ok, false)
  assert.equal((await f.bridge.sendMessage({...f.plan().record})).ok, false)
  assert.equal(f.calls.length, 0)
})

test("valid receipt acknowledges only exact original scope", async () => {
  const f = fixture({fetcher: async () => jsonBody({ok: true, operation: {operation_id: "message-op", project: "proj-a",
    session_id: "ses-1", message_id: "msg-real", state: "admitted"}})})
  assert.equal((await f.bridge.sendMessage(f.plan().record)).ok, true)
  assert.equal(f.state.entries[0].status, "acknowledged")
})

test("foreign receipt stays possible rather than successful", async () => {
  const f = fixture({fetcher: async () => jsonBody({ok: true, operation: {operation_id: "foreign", project: "proj-a",
    session_id: "ses-1", message_id: "msg-real", state: "admitted"}})})
  assert.equal((await f.bridge.sendMessage(f.plan().record)).code, "UNKNOWN_OUTCOME")
  assert.equal(f.state.entries[0].status, "possible")
})

test("restart journal observes original scope using GET then observation-only POST", async () => {
  let view
  const operation = {operation_id: "message-op", project: "proj-a", session_id: "ses-1", message_id: "msg-real", state: "admitted"}
  const f = fixture({fetcher: async (url, init) => {
    if (url.includes("/control?")) return jsonBody(view)
    if (url.includes("/message?")) return jsonBody({ok: true, operation})
    if (url.endsWith("/message/observe")) return jsonBody({ok: true, operation: {...operation, state: "finished"}, finished: true})
    return jsonBody({ok: true, operation})
  }})
  view = f.view
  assert.equal((await f.bridge.sendMessage(f.plan().record)).ok, true)
  const observed = await f.bridge.observeMessage("message-op")
  assert.equal(observed.finished, true)
  assert.equal(f.state.entries[0].status, "finished")
  assert.equal(f.calls.filter(c => c.url.endsWith("/coordinator/message")).length, 1)
  assert.equal(f.calls.filter(c => c.url.endsWith("/message/observe")).length, 1)
})

test("missing original receipt never calls observe or resends model input", async () => {
  let view
  const f = fixture({fetcher: async (url) => url.includes("/control?") ? jsonBody(view) : jsonBody({ok: true, operation: null})})
  view = f.view
  await f.bridge.sendMessage(f.plan().record)
  assert.equal((await f.bridge.observeMessage("message-op")).code, "UNKNOWN_OUTCOME")
  assert.equal(f.calls.filter(c => c.url.endsWith("/message/observe")).length, 0)
  assert.equal(f.calls.filter(c => c.url.endsWith("/coordinator/message")).length, 1)
})

test("another ID cannot bypass outstanding same-scope message journal", async () => {
  const f = fixture()
  await f.bridge.sendMessage(f.plan().record)
  const next = f.plan({operationId: "fresh-id"})
  assert.equal(next.ok, true)
  assert.equal((await f.bridge.sendMessage(next.record)).code, "PERSISTENCE_REQUIRED")
  assert.equal(f.calls.length, 1)
})
