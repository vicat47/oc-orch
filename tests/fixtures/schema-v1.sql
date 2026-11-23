
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  project TEXT NOT NULL,
  title TEXT NOT NULL,
  objective TEXT NOT NULL DEFAULT '',
  acceptance TEXT NOT NULL DEFAULT '[]',
  status TEXT NOT NULL,
  coordinator_session TEXT,
  write_scope TEXT NOT NULL DEFAULT '[]',
  priority INTEGER NOT NULL DEFAULT 0,
  next_check_at INTEGER,
  current_attempt_id TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  accepted_at INTEGER,
  closed_at INTEGER,
  blocked_reason TEXT,
  metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS tasks_project ON tasks(project);

CREATE TABLE IF NOT EXISTS attempts (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  generation INTEGER NOT NULL,
  session_id TEXT,
  directory TEXT NOT NULL,
  agent TEXT,
  model TEXT,
  variant TEXT,
  status TEXT NOT NULL,
  dispatch_state TEXT NOT NULL DEFAULT 'prepared',
  dispatch_msg_id TEXT,
  session_meta TEXT NOT NULL DEFAULT '{}',
  started_at INTEGER,
  dispatched_at INTEGER,
  last_progress_at INTEGER,
  last_observed_at INTEGER,
  deadline_at INTEGER,
  stall_after INTEGER,
  terminal_at INTEGER,
  terminal_kind TEXT,
  terminal_reason TEXT,
  stop_requested_at INTEGER,
  stop_confirmed_at INTEGER,
  site_released_at INTEGER,
  result_collected_at INTEGER,
  result_path TEXT,
  result_summary TEXT,
  verification TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  metadata TEXT NOT NULL DEFAULT '{}',
  UNIQUE(task_id, generation)
);
CREATE INDEX IF NOT EXISTS attempts_status ON attempts(status);
CREATE INDEX IF NOT EXISTS attempts_session ON attempts(session_id);
CREATE INDEX IF NOT EXISTS attempts_task ON attempts(task_id);

CREATE TABLE IF NOT EXISTS deliverables (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  attempt_id TEXT NOT NULL REFERENCES attempts(id),
  kind TEXT NOT NULL,
  target TEXT NOT NULL,
  required INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'expected',
  evidence TEXT,
  checked_at INTEGER,
  UNIQUE(attempt_id, kind, target)
);

CREATE TABLE IF NOT EXISTS turns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  attempt_id TEXT NOT NULL REFERENCES attempts(id),
  start_event_id TEXT,
  end_event_id TEXT,
  started_at INTEGER,
  ended_at INTEGER,
  outcome TEXT,
  UNIQUE(attempt_id, start_event_id)
);

CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  session_id TEXT,
  task_id TEXT,
  attempt_id TEXT,
  seq INTEGER,
  version INTEGER,
  created_ms INTEGER,
  observed_at INTEGER NOT NULL,
  duplicate INTEGER NOT NULL DEFAULT 0,
  stale INTEGER NOT NULL DEFAULT 0,
  payload TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS events_session ON events(session_id);
CREATE INDEX IF NOT EXISTS events_attempt ON events(attempt_id);
CREATE INDEX IF NOT EXISTS events_type ON events(type);

CREATE TABLE IF NOT EXISTS notifications (
  id TEXT PRIMARY KEY,
  project TEXT,
  kind TEXT NOT NULL,
  target_session TEXT,
  task_id TEXT,
  attempt_id TEXT,
  dedupe_key TEXT NOT NULL,
  payload TEXT NOT NULL DEFAULT '{}',
  state TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  next_attempt_at INTEGER NOT NULL,
  message_id TEXT,
  created_at INTEGER NOT NULL,
  sent_at INTEGER,
  acked_at INTEGER,
  last_error TEXT,
  claim_owner TEXT,
  claim_at INTEGER,
  metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS notifications_due ON notifications(state, next_attempt_at);
CREATE UNIQUE INDEX IF NOT EXISTS notifications_active_dedupe
  ON notifications(dedupe_key) WHERE state IN ('pending','inflight','sent');

CREATE TABLE IF NOT EXISTS alerts (
  id TEXT PRIMARY KEY,
  severity TEXT NOT NULL,
  kind TEXT NOT NULL,
  title TEXT NOT NULL,
  detail TEXT NOT NULL,
  task_id TEXT,
  attempt_id TEXT,
  dedupe_key TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'new',
  created_at INTEGER NOT NULL,
  acked_at INTEGER,
  delivered_at INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS alerts_active_dedupe
  ON alerts(dedupe_key) WHERE state = 'new';

CREATE TABLE IF NOT EXISTS coordinator_leases (
  project TEXT PRIMARY KEY,
  holder_session TEXT NOT NULL,
  epoch INTEGER NOT NULL,
  acquired_at INTEGER NOT NULL,
  renewed_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  failure_count INTEGER NOT NULL DEFAULT 0,
  last_failure_at INTEGER,
  recovery_blocked INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at INTEGER NOT NULL,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  entity TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  from_state TEXT,
  to_state TEXT,
  detail TEXT
);
CREATE INDEX IF NOT EXISTS audit_entity ON audit(entity, entity_id);
