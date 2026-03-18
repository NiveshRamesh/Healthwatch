import React, { useState, useCallback } from 'react';
import { Badge, Tip, Chip } from './Shared';
import { fmt } from '../utils';
import Modal from './Modal';

const BASE = '/healthwatch';

/* ── KPI Status Strip — horizontal cards for connectivity checks ── */
function StatusStrip({ checks }) {
  const items = Object.entries(checks)
    .filter(([k, v]) => !k.startsWith('__') && v?.status)
    .map(([name, check]) => ({ name, ...check }));

  return (
    <div style={{ display: 'grid', gridTemplateColumns: `repeat(${items.length}, 1fr)`, gap: 12, padding: '16px 20px' }}>
      {items.map(({ name, status, detail }) => {
        const rgb = status === 'ok' ? '16,185,129' : status === 'warn' ? '245,158,11' : '239,68,68';
        return (
          <div key={name} style={{
            background: `linear-gradient(160deg, rgba(${rgb},0.1), rgba(${rgb},0.02))`,
            border: `1px solid rgba(${rgb},0.18)`, borderTop: `3px solid rgba(${rgb},0.7)`,
            borderRadius: 10, padding: '14px 16px',
          }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
              <span style={{ fontSize: '0.82rem', fontWeight: 600, color: 'var(--text)' }}>{name}</span>
              <Badge status={status} size="sm" />
            </div>
            <div style={{ fontSize: '0.7rem', color: 'var(--muted)', fontFamily: 'var(--mono)', lineHeight: 1.4 }}>{detail}</div>
          </div>
        );
      })}
    </div>
  );
}

/* ── Group divider — collapsible full-width section header ────────── */
function GroupDivider({ label, open, onToggle }) {
  return (
    <div
      onClick={onToggle}
      style={{
        gridColumn: 'span 2', display: 'flex', alignItems: 'center', gap: 12,
        padding: '16px 4px 6px', marginTop: 4, cursor: 'pointer',
        userSelect: 'none',
      }}
    >
      <span style={{
        fontSize: '0.68rem', fontFamily: 'var(--mono)', fontWeight: 700,
        color: 'var(--accent)', textTransform: 'uppercase', letterSpacing: '1.5px',
        whiteSpace: 'nowrap',
      }}>{label}</span>
      <div style={{ flex: 1, height: 1, background: 'linear-gradient(90deg, rgba(0,212,170,0.3), transparent)' }} />
      <span style={{
        color: 'var(--accent)', fontSize: '0.7rem', transition: 'transform 0.25s',
        transform: open ? 'rotate(180deg)' : 'none', flexShrink: 0,
      }}>▼</span>
    </div>
  );
}

/* ── Check card — status-tinted with gradient ─────────────────────── */
function ChCheckRow({ icon, label, status, detail, children, tip }) {
  const [open, setOpen] = useState(false);
  const hasDetail = !!children;
  const rgb = status === 'ok' ? '16,185,129' : status === 'warn' ? '245,158,11' : '239,68,68';
  const cssVar = status === 'ok' ? 'ok' : status === 'warn' ? 'warn' : 'error';

  return (
    <div style={{
      margin: 4, borderRadius: 10, overflow: 'hidden',
      background: `linear-gradient(150deg, rgba(${rgb},0.08), rgba(${rgb},0.01))`,
      border: `1px solid rgba(${rgb},0.15)`,
      borderLeft: `3px solid var(--${cssVar})`,
      transition: 'box-shadow 0.2s, border-color 0.2s',
    }}
      onMouseEnter={e => e.currentTarget.style.boxShadow = `0 2px 16px rgba(${rgb},0.12)`}
      onMouseLeave={e => e.currentTarget.style.boxShadow = 'none'}
    >
      <div
        onClick={() => hasDetail && setOpen(o => !o)}
        style={{
          padding: '12px 14px', cursor: hasDetail ? 'pointer' : 'default',
          display: 'flex', alignItems: 'flex-start', gap: 10,
        }}
      >
        <span style={{ fontSize: '1.15rem', flexShrink: 0, marginTop: 1 }}>{icon}</span>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 6 }}>
            <span style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text)' }}>{label}</span>
            <div style={{ display: 'flex', alignItems: 'center', gap: 5, flexShrink: 0 }}>
              <Tip text={tip} />
              <Badge status={status} size="sm" />
              {hasDetail && (
                <span style={{
                  color: 'var(--muted)', fontSize: '0.6rem', transition: 'transform 0.25s',
                  transform: open ? 'rotate(180deg)' : 'none',
                }}>▼</span>
              )}
            </div>
          </div>
          <div style={{
            fontSize: '0.7rem', color: 'var(--muted)', fontFamily: 'var(--mono)', marginTop: 5,
            lineHeight: 1.4,
          }}>{detail}</div>
        </div>
      </div>
      {open && hasDetail && (
        <div style={{
          borderTop: `1px solid rgba(${rgb},0.12)`, padding: '14px 16px',
          background: 'rgba(0,0,0,0.2)', animation: 'fadeIn 0.2s ease',
        }}>
          {children}
        </div>
      )}
    </div>
  );
}

/* ── Data table — clean enterprise styling ────────────────────────── */
function DataTable({ cols, children }) {
  return (
    <table style={{ width: '100%', borderCollapse: 'separate', borderSpacing: '0 2px', fontSize: '0.68rem', fontFamily: 'var(--mono)' }}>
      <thead>
        <tr>
          {cols.map(h => (
            <th key={h} style={{
              textAlign: 'left', padding: '6px 10px',
              color: 'var(--accent)', fontWeight: 600, fontSize: '0.62rem',
              textTransform: 'uppercase', letterSpacing: '0.8px',
              borderBottom: '1px solid rgba(0,212,170,0.15)',
            }}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {React.Children.map(children, (row, i) => {
          if (!row) return null;
          return React.cloneElement(row, {
            style: {
              background: i % 2 === 0 ? 'rgba(255,255,255,0.02)' : 'transparent',
              ...row.props.style,
            },
            children: React.Children.map(row.props.children, td => {
              if (!td) return null;
              return React.cloneElement(td, { style: { padding: '5px 10px', ...td.props.style } });
            }),
          });
        })}
      </tbody>
    </table>
  );
}

/* ── Table sizes bar chart ────────────────────────────────────────── */
function SizeBar({ label, size, bytes, maxBytes }) {
  const pct = maxBytes > 0 ? (bytes / maxBytes) * 100 : 0;
  return (
    <div style={{ marginBottom: 8 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
        <span style={{ fontFamily: 'var(--mono)', fontSize: '0.68rem', color: 'var(--text)' }}>{label}</span>
        <span style={{ fontFamily: 'var(--mono)', fontSize: '0.68rem', color: 'var(--accent)', fontWeight: 600 }}>{size}</span>
      </div>
      <div style={{ height: 5, background: 'var(--surface3)', borderRadius: 999, overflow: 'hidden' }}>
        <div style={{
          height: '100%', width: `${pct}%`, borderRadius: 999, transition: 'width 0.6s ease',
          background: 'linear-gradient(90deg, var(--accent), var(--accent2))',
        }} />
      </div>
    </div>
  );
}

/* ── Paginator ────────────────────────────────────────────────────── */
const PAGE_SIZE = 10;

function Paginator({ page, pages, total, pageSize, onPrev, onNext }) {
  if (pages <= 1) return null;
  const from = page * pageSize + 1;
  const to = Math.min((page + 1) * pageSize, total);
  return (
    <div style={{
      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      marginTop: 10, padding: '6px 0', fontFamily: 'var(--mono)', fontSize: '0.65rem', color: 'var(--muted)',
    }}>
      <span>Showing {from}–{to} of {total}</span>
      <div style={{ display: 'flex', gap: 6 }}>
        <button onClick={onPrev} disabled={page === 0} style={{
          padding: '3px 12px', borderRadius: 5, border: '1px solid var(--border)',
          background: page === 0 ? 'transparent' : 'var(--surface2)',
          color: page === 0 ? 'var(--muted)' : 'var(--text)',
          cursor: page === 0 ? 'default' : 'pointer', fontFamily: 'var(--mono)', fontSize: '0.65rem',
        }}>← Prev</button>
        <span style={{ padding: '3px 8px', alignSelf: 'center', color: 'var(--accent)' }}>{page + 1}/{pages}</span>
        <button onClick={onNext} disabled={page >= pages - 1} style={{
          padding: '3px 12px', borderRadius: 5, border: '1px solid var(--border)',
          background: page >= pages - 1 ? 'transparent' : 'var(--surface2)',
          color: page >= pages - 1 ? 'var(--muted)' : 'var(--text)',
          cursor: page >= pages - 1 ? 'default' : 'pointer', fontFamily: 'var(--mono)', fontSize: '0.65rem',
        }}>Next →</button>
      </div>
    </div>
  );
}

/* ── Engine group icons & colors ──────────────────────────────────── */
const ENGINE_META = {
  ReplicatedMergeTree: { icon: '🔁', color: '0,212,170' },
  MergeTree:           { icon: '🌲', color: '0,212,170' },
  Distributed:         { icon: '🌐', color: '0,153,255' },
  Kafka:               { icon: '⚡', color: '245,158,11' },
  MaterializedView:    { icon: '👁️', color: '168,85,247' },
  View:                { icon: '👁️', color: '100,116,139' },
  Other:               { icon: '📦', color: '100,116,139' },
};

/* ── Engine group section (collapsible table list) ───────────────── */
function EngineGroup({ engine, tables }) {
  const [open, setOpen] = useState(false);
  const [page, setPage] = useState(0);
  const meta = ENGINE_META[engine] || ENGINE_META.Other;
  const pages = Math.ceil(tables.length / PAGE_SIZE);
  const slice = tables.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  return (
    <div style={{
      borderRadius: 8, overflow: 'hidden', marginBottom: 6,
      border: `1px solid rgba(${meta.color},0.15)`,
      background: `rgba(${meta.color},0.03)`,
    }}>
      <div onClick={() => { setOpen(o => !o); setPage(0); }} style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '8px 12px', cursor: 'pointer',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '0.85rem' }}>{meta.icon}</span>
          <span style={{ fontSize: '0.72rem', fontWeight: 600, fontFamily: 'var(--mono)', color: 'var(--text)' }}>
            {engine}
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{
            padding: '2px 8px', borderRadius: 4, fontSize: '0.62rem', fontWeight: 700,
            background: `rgba(${meta.color},0.12)`, color: `rgb(${meta.color})`,
            fontFamily: 'var(--mono)',
          }}>{tables.length}</span>
          <span style={{
            color: 'var(--muted)', fontSize: '0.6rem', transition: 'transform 0.2s',
            transform: open ? 'rotate(180deg)' : 'none',
          }}>▼</span>
        </div>
      </div>
      {open && (
        <div style={{ padding: '4px 12px 10px', borderTop: `1px solid rgba(${meta.color},0.1)` }}>
          <DataTable cols={['Table', 'Rows', 'Size']}>
            {slice.map((t, i) => (
              <tr key={i}>
                <td style={{ fontWeight: 600 }}>{t.name}</td>
                <td>{fmt(t.rows)}</td>
                <td style={{ color: 'var(--accent)', fontWeight: 600 }}>{t.size}</td>
              </tr>
            ))}
          </DataTable>
          <Paginator page={page} pages={pages} total={tables.length} pageSize={PAGE_SIZE}
            onPrev={() => setPage(p => p - 1)} onNext={() => setPage(p => p + 1)} />
        </div>
      )}
    </div>
  );
}

/* ── Database breakdown — expandable per-DB with engine groups ────── */
function DatabaseBreakdown({ databases }) {
  const [expandedDb, setExpandedDb] = useState(null);
  const [groups, setGroups] = useState({});
  const [loading, setLoading] = useState(false);

  const handleClick = useCallback(async (db) => {
    if (expandedDb === db) { setExpandedDb(null); return; }
    setExpandedDb(db);
    setGroups({});
    setLoading(true);
    try {
      const res = await fetch(`${BASE}/api/ch-tables/${encodeURIComponent(db)}`);
      const json = await res.json();
      setGroups(json.groups || {});
    } catch (e) {
      console.error('fetch tables failed', e);
      setGroups({});
    } finally {
      setLoading(false);
    }
  }, [expandedDb]);

  // Engine group display order
  const engineOrder = ['ReplicatedMergeTree', 'MergeTree', 'Distributed', 'Kafka', 'MaterializedView', 'View', 'Other'];
  const sortedEngines = Object.keys(groups).sort((a, b) => {
    const ai = engineOrder.indexOf(a), bi = engineOrder.indexOf(b);
    return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
  });

  return (
    <div style={{ padding: '0 20px 12px' }}>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))', gap: 8 }}>
        {databases.map(d => {
          const isOpen = expandedDb === d.database;
          return (
            <div key={d.database}
              onClick={() => handleClick(d.database)}
              style={{
                borderRadius: 8, padding: '10px 14px', cursor: 'pointer',
                background: isOpen ? 'rgba(0,212,170,0.08)' : 'var(--surface2)',
                border: isOpen ? '1px solid rgba(0,212,170,0.3)' : '1px solid var(--border)',
                transition: 'all 0.2s', textAlign: 'center',
              }}
              onMouseEnter={e => { if (!isOpen) e.currentTarget.style.borderColor = 'rgba(0,212,170,0.2)'; }}
              onMouseLeave={e => { if (!isOpen) e.currentTarget.style.borderColor = 'var(--border)'; }}
            >
              <div style={{ fontSize: '0.72rem', fontWeight: 600, fontFamily: 'var(--mono)', color: 'var(--text)' }}>
                {d.database}
              </div>
              <div style={{
                fontSize: '1.1rem', fontWeight: 700, color: 'var(--accent)',
                fontFamily: 'var(--mono)', marginTop: 4,
              }}>{d.count}</div>
              <div style={{ fontSize: '0.6rem', color: 'var(--muted)', marginTop: 2 }}>tables</div>
            </div>
          );
        })}
      </div>

      {expandedDb && (
        <div style={{
          marginTop: 10, borderRadius: 10, overflow: 'hidden',
          background: 'rgba(0,0,0,0.2)', border: '1px solid rgba(0,212,170,0.15)',
          animation: 'fadeIn 0.2s ease',
        }}>
          <div style={{
            padding: '10px 14px', borderBottom: '1px solid rgba(0,212,170,0.1)',
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          }}>
            <span style={{
              fontSize: '0.72rem', fontWeight: 700, fontFamily: 'var(--mono)', color: 'var(--accent)',
            }}>📋 {expandedDb}</span>
            <span style={{
              fontSize: '0.62rem', color: 'var(--muted)', fontFamily: 'var(--mono)',
            }}>
              {Object.values(groups).reduce((sum, arr) => sum + arr.length, 0)} table(s) · {sortedEngines.length} engine type(s)
            </span>
          </div>
          <div style={{ padding: '8px 12px' }}>
            {loading && (
              <div style={{ fontSize: '0.68rem', color: 'var(--muted)', fontFamily: 'var(--mono)', padding: '8px 0' }}>
                Loading tables...
              </div>
            )}
            {!loading && sortedEngines.length > 0 && sortedEngines.map(eng => (
              <EngineGroup key={eng} engine={eng} tables={groups[eng]} />
            ))}
            {!loading && sortedEngines.length === 0 && !loading && (
              <div style={{ fontSize: '0.68rem', color: 'var(--muted)', fontFamily: 'var(--mono)', padding: '8px 0' }}>
                No tables found
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

/* ── Sub-check components ─────────────────────────────────────────── */
function ReplicaInconsistencyRow({ replicaIncons }) {
  const [page, setPage] = useState(0);
  const rows = replicaIncons.tables || [];
  const total = rows.length;
  const pages = Math.ceil(total / PAGE_SIZE);
  const slice = rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  return (
    <ChCheckRow icon="🔂" label="Keeper Replica Inconsistency"
      status={replicaIncons.status} detail={replicaIncons.detail}
      tip="Tables where the actual replica count is less than expected. Indicates a missing or unregistered replica in Keeper."
    >
      {total > 0 && (
        <div>
          <DataTable cols={['Table', 'Expected', 'Actual']}>
            {slice.map((t, i) => (
              <tr key={i}>
                <td style={{ fontWeight: 600 }}>{t.table}</td>
                <td style={{ color: 'var(--ok)' }}>{replicaIncons.expected_replicas}</td>
                <td style={{ color: 'var(--error)', fontWeight: 700 }}>{t.actual_replicas}</td>
              </tr>
            ))}
          </DataTable>
          <Paginator page={page} pages={pages} total={total} pageSize={PAGE_SIZE}
            onPrev={() => setPage(p => p - 1)} onNext={() => setPage(p => p + 1)} />
        </div>
      )}
    </ChCheckRow>
  );
}

function ChKafkaEngineLag({ chKafkaLag }) {
  const [page, setPage] = useState(0);
  const allRows = Object.entries(chKafkaLag || {}).sort((a, b) => b[1].total_lag - a[1].total_lag);
  const highRows = allRows.filter(([, v]) => v.total_lag > 10000);
  const anyHigh = highRows.length > 0;
  const status = anyHigh ? 'warn' : 'ok';
  const detail = allRows.length === 0 ? 'No Kafka engine tables'
    : anyHigh ? `${highRows.length} table(s) high lag`
    : `${allRows.length} table(s) · all lag normal`;
  const rows = anyHigh ? highRows : [];
  const total = rows.length;
  const pages = Math.ceil(total / PAGE_SIZE);
  const slice = rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  return (
    <ChCheckRow icon="⚡" label="Kafka Engine Consumer Lag" status={status} detail={detail}
      tip="How far behind each CH Kafka Engine table is. Computed from system.kafka_consumers vs Kafka end_offset. Threshold: >10k = HIGH."
    >
      {total > 0 && (
        <div>
          <DataTable cols={['CH Table', 'Topics', 'Total Lag']}>
            {slice.map(([key, v], i) => {
              const topics = Object.keys(v.topics || {}).join(', ') || '—';
              return (
                <tr key={i}>
                  <td style={{ fontWeight: 600 }}>{key}</td>
                  <td style={{ color: 'var(--muted)', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={topics}>{topics}</td>
                  <td style={{ color: 'var(--warn)', fontWeight: 700 }}>{fmt(v.total_lag)}</td>
                </tr>
              );
            })}
          </DataTable>
          <Paginator page={page} pages={pages} total={total} pageSize={PAGE_SIZE}
            onPrev={() => setPage(p => p - 1)} onNext={() => setPage(p => p + 1)} />
        </div>
      )}
    </ChCheckRow>
  );
}

function KafkaPipelineHealth({ data }) {
  const [page, setPage] = useState(0);
  const rows = (data?.tables || []).filter(r => r.status !== 'ok');
  const total = rows.length;
  const pages = Math.ceil(total / PAGE_SIZE);
  const slice = rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  return (
    <ChCheckRow icon="🔌" label="Kafka Pipeline Health" status={data?.status || 'ok'} detail={data?.detail || ''}
      tip="Per-table staleness from system.kafka_consumers. Warns when seconds_since_last_commit > 300s. Flags NEVER committed tables."
    >
      {total > 0 && (
        <div>
          <DataTable cols={['Table', 'Last Commit', 'Ago', 'State']}>
            {slice.map((r, i) => (
              <tr key={i}>
                <td style={{ fontWeight: 600 }}>{r.database}.{r.table}</td>
                <td style={{ color: 'var(--muted)' }}>{r.never_committed ? '—' : r.last_commit_time}</td>
                <td style={{ color: r.status === 'error' ? 'var(--error)' : 'var(--warn)', fontWeight: 700 }}>
                  {r.never_committed ? 'never' : fmt(r.seconds_since_commit) + 's'}
                </td>
                <td>
                  <span style={{
                    padding: '2px 8px', borderRadius: 4, fontSize: '0.62rem', fontWeight: 700,
                    background: r.never_committed ? 'rgba(239,68,68,0.15)' : 'rgba(245,158,11,0.15)',
                    color: r.never_committed ? 'var(--error)' : 'var(--warn)',
                  }}>{r.never_committed ? 'NEVER' : 'STALE'}</span>
                </td>
              </tr>
            ))}
          </DataTable>
          <Paginator page={page} pages={pages} total={total} pageSize={PAGE_SIZE}
            onPrev={() => setPage(p => p - 1)} onNext={() => setPage(p => p + 1)} />
        </div>
      )}
    </ChCheckRow>
  );
}

function IngestionRate({ data }) {
  const [page, setPage] = useState(0);
  const rows = (data?.tables || []).filter(r => r.stopped);
  const total = rows.length;
  const pages = Math.ceil(total / PAGE_SIZE);
  const slice = rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  return (
    <ChCheckRow icon="📥" label="Ingestion Rate" status={data?.status || 'ok'} detail={data?.detail || ''}
      tip="Rows per hour/sec from system.part_log. Warns when a table stops inserting for >10 min."
    >
      {total > 0 && (
        <div>
          <DataTable cols={['Table', 'Rows/hr', '/sec', 'Last Insert', 'Stopped']}>
            {slice.map((r, i) => (
              <tr key={i}>
                <td style={{ fontWeight: 600 }}>{r.database}.{r.table}</td>
                <td>{fmt(r.rows_1h)}</td>
                <td>{r.rows_per_sec}</td>
                <td style={{ color: 'var(--muted)' }}>{r.last_insert}</td>
                <td style={{ color: 'var(--warn)', fontWeight: 700 }}>{r.mins_since_insert != null ? `${r.mins_since_insert}m` : '—'}</td>
              </tr>
            ))}
          </DataTable>
          <Paginator page={page} pages={pages} total={total} pageSize={PAGE_SIZE}
            onPrev={() => setPage(p => p - 1)} onNext={() => setPage(p => p + 1)} />
        </div>
      )}
    </ChCheckRow>
  );
}

function ReplicaExceptions({ data }) {
  const [page, setPage] = useState(0);
  const rows = data?.tables || [];
  const total = rows.length;
  const pages = Math.ceil(total / PAGE_SIZE);
  const slice = rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  return (
    <ChCheckRow icon="🚨" label="Replica Exceptions" status={data?.status || 'ok'} detail={data?.detail || ''}
      tip="Tables with queue/ZK exceptions in system.replicas. Also flags parts_to_check > 300."
    >
      {total > 0 && (
        <div>
          <DataTable cols={['Table', 'Queue Exception', 'ZK Exception', 'Parts']}>
            {slice.map((r, i) => (
              <tr key={i}>
                <td style={{ fontWeight: 600 }}>{r.database}.{r.table}</td>
                <td style={{ color: 'var(--warn)', maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={r.queue_exception}>{r.queue_exception || '—'}</td>
                <td style={{ color: 'var(--warn)', maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={r.zk_exception}>{r.zk_exception || '—'}</td>
                <td style={{
                  color: r.parts_to_check > 500 ? 'var(--error)' : r.parts_to_check > 300 ? 'var(--warn)' : 'var(--muted)',
                  fontWeight: r.parts_to_check > 300 ? 700 : 400,
                }}>{r.parts_to_check}</td>
              </tr>
            ))}
          </DataTable>
          <Paginator page={page} pages={pages} total={total} pageSize={PAGE_SIZE}
            onPrev={() => setPage(p => p - 1)} onNext={() => setPage(p => p + 1)} />
        </div>
      )}
    </ChCheckRow>
  );
}

/* ── ClickHouse Errors — click a row to fetch actual error messages ── */
const _err_code_map = {
  MEMORY_LIMIT_EXCEEDED: 241, UNKNOWN_TABLE: 60,
  SCHEMA_MISMATCH: 517, TOO_MANY_PARTS: 252, PARSE_ERROR: 62,
  MERGE_MEMORY_LIMIT: 0,
};

function ChErrors({ data }) {
  const rows = data?.errors || [];
  const [expandedIdx, setExpandedIdx] = useState(null);
  const [messages, setMessages] = useState([]);
  const [loadingMsgs, setLoadingMsgs] = useState(false);

  const handleRowClick = useCallback(async (idx, errorName) => {
    if (expandedIdx === idx) { setExpandedIdx(null); return; }
    setExpandedIdx(idx);
    setMessages([]);
    setLoadingMsgs(true);
    try {
      const code = _err_code_map[errorName] ?? -1;
      const res = await fetch(`${BASE}/api/ch-error-messages?code=${code}`);
      const json = await res.json();
      setMessages(json.messages || []);
    } catch (e) {
      console.error('fetch error messages failed', e);
      setMessages([]);
    } finally {
      setLoadingMsgs(false);
    }
  }, [expandedIdx]);

  return (
    <ChCheckRow icon="🔴" label="ClickHouse Errors (1hr)" status={data?.status || 'ok'} detail={data?.detail || ''}
      tip="Query/insert failures from system.query_log + merge memory failures from system.text_log. Click a row to see actual error messages."
    >
      {rows.length > 0 && (
        <div>
          <DataTable cols={['Error Type', 'Count', 'Severity', '']}>
            {rows.map((r, i) => (
              <tr key={i} onClick={() => handleRowClick(i, r.error)}
                style={{ cursor: 'pointer' }}>
                <td style={{ fontWeight: 600 }}>{r.error}</td>
                <td style={{ color: r.status === 'critical' ? 'var(--error)' : 'var(--warn)', fontWeight: 700 }}>{fmt(r.count)}</td>
                <td>
                  <span style={{
                    padding: '2px 8px', borderRadius: 4, fontSize: '0.62rem', fontWeight: 700,
                    background: r.status === 'critical' ? 'rgba(239,68,68,0.15)' : 'rgba(245,158,11,0.15)',
                    color: r.status === 'critical' ? 'var(--error)' : 'var(--warn)',
                  }}>{r.status.toUpperCase()}</span>
                </td>
                <td style={{ color: 'var(--muted)', fontSize: '0.6rem' }}>
                  {expandedIdx === i ? '▲' : '▼'}
                </td>
              </tr>
            ))}
          </DataTable>
          {expandedIdx != null && (
            <div style={{
              marginTop: 8, padding: '10px 12px', borderRadius: 8,
              background: 'rgba(0,0,0,0.3)', border: '1px solid var(--border)',
            }}>
              <div style={{ fontSize: '0.65rem', color: 'var(--accent)', fontWeight: 600, marginBottom: 6 }}>
                Error Messages — {rows[expandedIdx]?.error}
              </div>
              {loadingMsgs && (
                <div style={{ fontSize: '0.62rem', color: 'var(--muted)', fontFamily: 'var(--mono)' }}>Loading...</div>
              )}
              {!loadingMsgs && messages.length === 0 && (
                <div style={{ fontSize: '0.62rem', color: 'var(--muted)', fontFamily: 'var(--mono)' }}>No recent error messages found</div>
              )}
              {!loadingMsgs && messages.map((msg, mi) => (
                <div key={mi} style={{
                  fontSize: '0.62rem', fontFamily: 'var(--mono)', color: 'var(--error)',
                  padding: '6px 8px', marginBottom: 4, borderRadius: 5,
                  background: 'rgba(239,68,68,0.05)', wordBreak: 'break-all',
                  borderLeft: '2px solid var(--error)',
                }}>{msg}</div>
              ))}
            </div>
          )}
        </div>
      )}
    </ChCheckRow>
  );
}

/* ── Table Detail display ─────────────────────────────────────────── */
function TableDetail({ data }) {
  const [partPage, setPartPage] = useState(0);
  const [colPage, setColPage] = useState(0);

  if (!data.found) {
    return (
      <div style={{ padding: 12, color: 'var(--error)', fontFamily: 'var(--mono)', fontSize: '0.75rem' }}>
        Table not found{data.error ? `: ${data.error}` : ''}
      </div>
    );
  }

  const { info, parts = [], columns = [], replicas = [], mutations = [] } = data;
  const partPages = Math.ceil(parts.length / PAGE_SIZE);
  const partSlice = parts.slice(partPage * PAGE_SIZE, (partPage + 1) * PAGE_SIZE);
  const colPages = Math.ceil(columns.length / PAGE_SIZE);
  const colSlice = columns.slice(colPage * PAGE_SIZE, (colPage + 1) * PAGE_SIZE);

  const kv = (label, val, color) => (
    <div style={{ marginBottom: 6 }}>
      <div style={{ fontSize: '0.58rem', color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>{label}</div>
      <div style={{ fontSize: '0.72rem', fontFamily: 'var(--mono)', fontWeight: 600, color: color || 'var(--text)', marginTop: 2 }}>{val}</div>
    </div>
  );

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
      {/* Summary grid */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '8px 16px' }}>
        {kv('Engine', info.engine, 'var(--accent)')}
        {kv('Rows', fmt(info.total_rows))}
        {kv('Size', info.total_size, 'var(--accent)')}
        {kv('Partition Key', info.partition_key)}
        {kv('Sorting Key', info.sorting_key)}
        {kv('Last Modified', info.last_modified)}
      </div>

      {/* Replicas */}
      {replicas.length > 0 && (
        <div>
          <div style={{ fontSize: '0.65rem', fontWeight: 700, color: 'var(--accent)', marginBottom: 6, fontFamily: 'var(--mono)', textTransform: 'uppercase' }}>
            Replicas
          </div>
          <DataTable cols={['Replica', 'Leader', 'Readonly', 'Queue', 'Delay']}>
            {replicas.map((r, i) => (
              <tr key={i}>
                <td style={{ fontWeight: 600 }}>{r.replica}</td>
                <td style={{ color: r.is_leader ? 'var(--ok)' : 'var(--muted)' }}>{r.is_leader ? 'YES' : 'no'}</td>
                <td style={{ color: r.is_readonly ? 'var(--error)' : 'var(--ok)' }}>{r.is_readonly ? 'YES' : 'no'}</td>
                <td>{r.queue_size}</td>
                <td style={{ color: r.delay_seconds > 10 ? 'var(--warn)' : 'var(--muted)' }}>{r.delay_seconds}s</td>
              </tr>
            ))}
          </DataTable>
        </div>
      )}

      {/* Partitions */}
      {parts.length > 0 && (
        <div>
          <div style={{ fontSize: '0.65rem', fontWeight: 700, color: 'var(--accent)', marginBottom: 6, fontFamily: 'var(--mono)', textTransform: 'uppercase' }}>
            Partitions ({parts.length})
          </div>
          <DataTable cols={['Partition', 'Parts', 'Rows', 'Size']}>
            {partSlice.map((p, i) => (
              <tr key={i}>
                <td style={{ fontWeight: 600 }}>{p.partition}</td>
                <td>{p.parts}</td>
                <td>{fmt(p.rows)}</td>
                <td style={{ color: 'var(--accent)', fontWeight: 600 }}>{p.size}</td>
              </tr>
            ))}
          </DataTable>
          <Paginator page={partPage} pages={partPages} total={parts.length} pageSize={PAGE_SIZE}
            onPrev={() => setPartPage(p => p - 1)} onNext={() => setPartPage(p => p + 1)} />
        </div>
      )}

      {/* Columns */}
      {columns.length > 0 && (
        <div>
          <div style={{ fontSize: '0.65rem', fontWeight: 700, color: 'var(--accent)', marginBottom: 6, fontFamily: 'var(--mono)', textTransform: 'uppercase' }}>
            Columns ({columns.length})
          </div>
          <DataTable cols={['Name', 'Type', 'Default']}>
            {colSlice.map((c, i) => (
              <tr key={i}>
                <td style={{ fontWeight: 600 }}>{c.name}</td>
                <td style={{ color: 'var(--muted)' }}>{c.type}</td>
                <td style={{ color: 'var(--muted)' }}>{c.default}</td>
              </tr>
            ))}
          </DataTable>
          <Paginator page={colPage} pages={colPages} total={columns.length} pageSize={PAGE_SIZE}
            onPrev={() => setColPage(p => p - 1)} onNext={() => setColPage(p => p + 1)} />
        </div>
      )}

      {/* Mutations */}
      {mutations.length > 0 && (
        <div>
          <div style={{ fontSize: '0.65rem', fontWeight: 700, color: 'var(--accent)', marginBottom: 6, fontFamily: 'var(--mono)', textTransform: 'uppercase' }}>
            Recent Mutations
          </div>
          {mutations.map((m, i) => (
            <div key={i} style={{
              padding: '6px 10px', marginBottom: 4, borderRadius: 6, fontSize: '0.65rem',
              fontFamily: 'var(--mono)', background: 'var(--surface)',
              borderLeft: m.is_done ? '2px solid var(--ok)' : '2px solid var(--warn)',
            }}>
              <div style={{ color: 'var(--text)' }}>{m.command}</div>
              <div style={{ color: 'var(--muted)', marginTop: 3, fontSize: '0.6rem' }}>
                {m.create_time} · {m.is_done ? 'Done' : `${m.parts_to_do} parts left`}
                {m.fail_reason ? ` · ${m.fail_reason}` : ''}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Create query */}
      {info.create_query && (
        <div>
          <div style={{ fontSize: '0.65rem', fontWeight: 700, color: 'var(--accent)', marginBottom: 6, fontFamily: 'var(--mono)', textTransform: 'uppercase' }}>
            Create Query
          </div>
          <div style={{
            padding: '8px 10px', borderRadius: 6, fontSize: '0.6rem', fontFamily: 'var(--mono)',
            color: 'var(--muted)', background: 'var(--surface)', wordBreak: 'break-all',
            maxHeight: 120, overflowY: 'auto', lineHeight: 1.5,
          }}>{info.create_query}</div>
        </div>
      )}
    </div>
  );
}

/* ── Table Diagnosis Modal ────────────────────────────────────────── */
function TableDiagModal({ open, onClose, databases }) {
  const [dbName, setDbName] = useState('');
  const [tableName, setTableName] = useState('');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [knownTables, setKnownTables] = useState([]);

  // Fetch table list when database changes
  const handleDbChange = useCallback(async (db) => {
    setDbName(db);
    setTableName('');
    setResult(null);
    setKnownTables([]);
    if (!db) return;
    try {
      const res = await fetch(`${BASE}/api/ch-tables/${encodeURIComponent(db)}`);
      const json = await res.json();
      setKnownTables((json.tables || []).map(t => t.name));
    } catch (e) { /* ignore */ }
  }, []);

  function handleClose() { onClose(); setDbName(''); setTableName(''); setResult(null); setKnownTables([]); }

  async function inspect() {
    const db = dbName.trim();
    const tbl = tableName.trim();
    if (!db || !tbl) return;
    setLoading(true); setResult(null);
    try {
      const res = await fetch(`${BASE}/api/ch-table-detail/${encodeURIComponent(db)}/${encodeURIComponent(tbl)}`);
      setResult(await res.json());
    } catch (e) {
      setResult({ found: false, database: db, table: tbl, error: String(e.message) });
    } finally {
      setLoading(false);
    }
  }

  if (!open) return null;

  const allDbs = (databases || []).map(d => d.database);

  return (
    <Modal open={open} onClose={handleClose} title="🔍 Table Diagnosis">
      <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
          <div>
            <div style={{ fontSize: '0.65rem', color: 'var(--muted)', fontFamily: 'var(--mono)', textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: 5 }}>
              Database
            </div>
            <input
              list="hw-db-list"
              value={dbName}
              onChange={e => handleDbChange(e.target.value)}
              placeholder="Select database…"
              style={{
                width: '100%', padding: '8px 10px', borderRadius: 7, boxSizing: 'border-box',
                background: 'var(--surface)', border: '1px solid var(--border)',
                color: 'var(--text)', fontFamily: 'var(--mono)', fontSize: '0.75rem', outline: 'none',
              }}
            />
            <datalist id="hw-db-list">
              {allDbs.map(d => <option key={d} value={d} />)}
            </datalist>
          </div>
          <div>
            <div style={{ fontSize: '0.65rem', color: 'var(--muted)', fontFamily: 'var(--mono)', textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: 5 }}>
              Table name
            </div>
            <input
              list="hw-table-list"
              value={tableName}
              onChange={e => { setTableName(e.target.value); setResult(null); }}
              onKeyDown={e => e.key === 'Enter' && inspect()}
              placeholder="Select or type table…"
              style={{
                width: '100%', padding: '8px 10px', borderRadius: 7, boxSizing: 'border-box',
                background: 'var(--surface)', border: '1px solid var(--border)',
                color: 'var(--text)', fontFamily: 'var(--mono)', fontSize: '0.75rem', outline: 'none',
              }}
            />
            <datalist id="hw-table-list">
              {knownTables.map(t => <option key={t} value={t} />)}
            </datalist>
          </div>
        </div>

        <button
          onClick={inspect}
          disabled={loading || !dbName.trim() || !tableName.trim()}
          style={{
            display: 'inline-flex', alignItems: 'center', justifyContent: 'center', gap: 6,
            background: (dbName.trim() && tableName.trim()) ? 'var(--accent)' : 'rgba(0,212,170,0.2)',
            color: 'white', border: 'none', borderRadius: 7, padding: '9px 18px',
            cursor: (dbName.trim() && tableName.trim()) ? 'pointer' : 'default',
            fontFamily: 'var(--mono)', fontSize: '0.75rem', fontWeight: 700,
          }}
        >
          {loading ? '⏳ Fetching...' : '▶ INSPECT TABLE'}
        </button>

        {result && (
          <div style={{
            background: 'var(--surface)', border: '1px solid var(--border)',
            borderRadius: 10, overflow: 'hidden',
          }}>
            <div style={{
              display: 'flex', alignItems: 'center', justifyContent: 'space-between',
              padding: '10px 14px', borderBottom: '1px solid var(--border)',
              background: 'rgba(0,212,170,0.07)',
            }}>
              <span style={{ fontFamily: 'var(--mono)', fontSize: '0.78rem', fontWeight: 700, color: 'var(--accent)' }}>
                {result.database}.{result.table}
              </span>
              <span style={{ cursor: 'pointer', color: 'var(--muted)' }} onClick={() => setResult(null)}>✕</span>
            </div>
            <div style={{ padding: '12px 14px' }}>
              <TableDetail data={result} />
            </div>
          </div>
        )}
      </div>
    </Modal>
  );
}

/* ── Main ClickHouse Panel ────────────────────────────────────────── */
export default function ClickHousePanel({ checks, diagModalOpen, onDiagClose }) {
  const chTables = checks.__ch_tables__ || {};

  const unusedKafka   = chTables.unused_kafka_tables   || {};
  const readOnly      = chTables.readonly_tables        || {};
  const inactiveQ     = chTables.inactive_queries       || {};
  const longMut       = chTables.long_mutations         || {};
  const noTTL         = chTables.tables_without_ttl     || {};
  const detached      = chTables.detached_parts         || {};
  const tableSizes    = chTables.table_sizes            || {};
  const replStuck     = chTables.replication_stuck_jobs || {};
  const replicaIncons = chTables.replica_inconsistency  || {};
  const chKafkaLag    = chTables.ch_kafka_lag           || {};
  const kafkaPipeline = chTables.kafka_pipeline_health  || {};
  const ingestionRate = chTables.ingestion_rate         || {};
  const replicaExcept = chTables.replica_exceptions     || {};
  const chErrors      = chTables.ch_errors              || {};

  const maxBytes = Math.max(...(tableSizes.tables || []).map(t => t.bytes || 0), 1);
  const maxBytesVusmart = Math.max(...(tableSizes.tables_vusmart || []).map(t => t.bytes || 0), 1);

  /* collapsible section state */
  const [sections, setSections] = useState({ kafka: true, replication: true, schema: true, storage: true });
  const toggle = (key) => setSections(s => ({ ...s, [key]: !s[key] }));

  return (
    <>
      {/* KPI Status Strip */}
      <StatusStrip checks={checks} />

      {/* Per-database table breakdown */}
      {checks['Total Tables']?.databases?.length > 0 && (
        <DatabaseBreakdown databases={checks['Total Tables'].databases} />
      )}

      {/* Deep Checks header */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '8px 20px 4px', borderTop: '1px solid var(--border)',
      }}>
        <span style={{ fontSize: '0.82rem', fontWeight: 600, color: 'var(--text)' }}>
          🔬 Deep Checks
        </span>
        <div style={{ display: 'flex', gap: 4 }}>
          {['critical', 'warn'].includes(chErrors.status) && <Chip n={chErrors.errors?.length} color="239,68,68" label="errors" />}
          {['critical', 'warn'].includes(replicaIncons.status) && <Chip n={replicaIncons.tables?.length} color="239,68,68" label="replica" />}
          {['error', 'warn'].includes(kafkaPipeline.status) && <Chip n={kafkaPipeline.tables?.filter(r => r.status !== 'ok').length} color="245,158,11" label="pipeline" />}
          {['warn'].includes(unusedKafka.status) && <Chip n={unusedKafka.count} color="245,158,11" label="unused" />}
        </div>
      </div>

      {/* 2-column check card grid */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', padding: '0 12px 16px' }}>

        {/* ── Kafka & Pipeline ── */}
        <GroupDivider label="Kafka &amp; Pipeline" open={sections.kafka} onToggle={() => toggle('kafka')} />
        {sections.kafka && (<>
          <div style={{ gridColumn: 'span 2' }}><ChKafkaEngineLag chKafkaLag={chKafkaLag} /></div>
          <ChCheckRow icon="📭" label="Unused Kafka Tables" status={unusedKafka.status} detail={unusedKafka.detail}
            tip="Kafka Engine tables that never consumed a single message — likely misconfigured or abandoned.">
            {(unusedKafka.tables || []).length > 0 && (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
                {unusedKafka.tables.map((t, i) => (
                  <span key={i} style={{
                    background: 'rgba(245,158,11,0.12)', border: '1px solid rgba(245,158,11,0.3)',
                    color: 'var(--warn)', borderRadius: 5, padding: '3px 8px', fontFamily: 'var(--mono)', fontSize: '0.65rem',
                  }}>{t.database}.{t.table}</span>
                ))}
              </div>
            )}
          </ChCheckRow>
          <div style={{ gridColumn: 'span 2' }}><KafkaPipelineHealth data={kafkaPipeline} /></div>
          <div style={{ gridColumn: 'span 2' }}><IngestionRate data={ingestionRate} /></div>
        </>)}

        {/* ── Replication & Consistency ── */}
        <GroupDivider label="Replication &amp; Consistency" open={sections.replication} onToggle={() => toggle('replication')} />
        {sections.replication && (<>
          <ChCheckRow icon="🔒" label="Read-Only Tables" status={readOnly.status} detail={readOnly.detail}
            tip="ReplicatedMergeTree tables in is_readonly=1 state. Caused by lost ZK connection.">
            {(readOnly.tables || []).length > 0 && (
              <DataTable cols={['Database', 'Table', 'Engine']}>
                {readOnly.tables.map((t, i) => (
                  <tr key={i}><td style={{ color: 'var(--error)' }}>{t.database}</td><td>{t.table}</td><td style={{ color: 'var(--muted)' }}>{t.engine}</td></tr>
                ))}
              </DataTable>
            )}
          </ChCheckRow>
          <ChCheckRow icon="🔁" label="Replication Queue Stuck" status={replStuck.status} detail={replStuck.detail}
            tip="Replication queue entries postponed >100 times. High counts mean a replica is failing to sync.">
            {(replStuck.jobs || []).length > 0 && (
              <DataTable cols={['Database', 'Table', 'Count']}>
                {replStuck.jobs.map((j, i) => (
                  <tr key={i}><td style={{ color: 'var(--error)' }}>{j.database}</td><td>{j.table}</td><td style={{ fontWeight: 700 }}>{j.count}</td></tr>
                ))}
              </DataTable>
            )}
          </ChCheckRow>
          <div style={{ gridColumn: 'span 2' }}><ReplicaInconsistencyRow replicaIncons={replicaIncons} /></div>
          <div style={{ gridColumn: 'span 2' }}><ReplicaExceptions data={replicaExcept} /></div>
        </>)}

        {/* ── Schema & Config ── */}
        <GroupDivider label="Schema &amp; Config" open={sections.schema} onToggle={() => toggle('schema')} />
        {sections.schema && (<>
          <ChCheckRow icon="💤" label="Inactive DDL Queries" status={inactiveQ.status} detail={inactiveQ.detail}
            tip="DDL queries stuck in Inactive state. Block schema changes cluster-wide.">
            {(inactiveQ.queries || []).length > 0 && (
              <div>
                {inactiveQ.queries.map((q, i) => (
                  <div key={i} style={{ background: 'var(--surface)', borderRadius: 6, padding: '8px 10px', marginBottom: 5 }}>
                    <div style={{ fontSize: '0.65rem', color: 'var(--warn)', fontFamily: 'var(--mono)' }}>{q.query}</div>
                    <div style={{ fontSize: '0.6rem', color: 'var(--muted)', marginTop: 3, fontFamily: 'var(--mono)' }}>Created: {q.query_create_time}</div>
                  </div>
                ))}
              </div>
            )}
          </ChCheckRow>
          <ChCheckRow icon="⚗️" label="Long-Running Mutations" status={longMut.status} detail={longMut.detail}
            tip="ALTER TABLE mutations running >30 min. Hold resources and slow merges.">
            {(longMut.mutations || []).length > 0 && (
              <div>
                {longMut.mutations.map((m, i) => (
                  <div key={i} style={{ background: 'var(--surface)', borderRadius: 6, padding: '8px 10px', marginBottom: 5 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                      <span style={{ fontFamily: 'var(--mono)', fontSize: '0.68rem', fontWeight: 700 }}>{m.database}.{m.table}</span>
                      <Badge status="warn" label={`${fmt(m.parts_to_do)} parts`} size="sm" />
                    </div>
                    <div style={{ fontFamily: 'var(--mono)', fontSize: '0.62rem', color: 'var(--accent2)' }}>{m.command}</div>
                    <div style={{ fontFamily: 'var(--mono)', fontSize: '0.6rem', color: 'var(--muted)', marginTop: 3 }}>Started: {m.create_time}</div>
                  </div>
                ))}
              </div>
            )}
          </ChCheckRow>
          <ChCheckRow icon="⏱️" label="Missing Timestamp Partition" status={noTTL.status} detail={noTTL.detail}
            tip="MergeTree tables without timestamp-based PARTITION BY. Required for tiered storage.">
            {(noTTL.tables || []).length > 0 && (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
                {noTTL.tables.map((t, i) => (
                  <span key={i} style={{
                    background: 'rgba(245,158,11,0.12)', border: '1px solid rgba(245,158,11,0.3)',
                    color: 'var(--warn)', borderRadius: 5, padding: '3px 8px', fontFamily: 'var(--mono)', fontSize: '0.65rem',
                  }}>{t.database}.{t.table}</span>
                ))}
              </div>
            )}
          </ChCheckRow>
        </>)}

        {/* ── Storage & Errors ── */}
        <GroupDivider label="Storage &amp; Errors" open={sections.storage} onToggle={() => toggle('storage')} />
        {sections.storage && (<>
          <div style={{ gridColumn: 'span 2' }}>
            <ChCheckRow icon="💀" label="Detached / Corrupted Parts" status={detached.status} detail={detached.detail}
              tip="Parts in detached/ due to corruption or manual detach. Excluded from queries.">
              {(detached.parts || []).length > 0 && (
                <DataTable cols={['Database', 'Table', 'Reason', 'Count']}>
                  {detached.parts.map((p, i) => (
                    <tr key={i}>
                      <td style={{ color: 'var(--error)' }}>{p.database}</td><td>{p.table}</td>
                      <td style={{ color: 'var(--warn)' }}>{p.reason}</td>
                      <td style={{ fontWeight: 700, color: 'var(--error)' }}>{p.count}</td>
                    </tr>
                  ))}
                </DataTable>
              )}
            </ChCheckRow>
          </div>
          {/* Top Table Sizes — All DBs (left) + vusmart only (right) */}
          <ChCheckRow icon="📊" label="Top Table Sizes (All)" status={tableSizes.status} detail={tableSizes.detail}
            tip="Top 5 tables by compressed disk usage across all databases.">
            {(tableSizes.tables || []).length > 0 && (
              <div style={{ paddingTop: 4 }}>
                {tableSizes.tables.map((t, i) => (
                  <SizeBar key={i} label={`${t.database}.${t.table}`} size={t.size} bytes={t.bytes || 0} maxBytes={maxBytes} />
                ))}
              </div>
            )}
          </ChCheckRow>
          <ChCheckRow icon="📊" label="Top Table Sizes (vusmart)" status={tableSizes.status}
            detail={`Top ${(tableSizes.tables_vusmart || []).length} vusmart tables`}
            tip="Top 5 tables by compressed disk usage in the vusmart database only.">
            {(tableSizes.tables_vusmart || []).length > 0 && (
              <div style={{ paddingTop: 4 }}>
                {tableSizes.tables_vusmart.map((t, i) => (
                  <SizeBar key={i} label={t.table} size={t.size} bytes={t.bytes || 0} maxBytes={maxBytesVusmart} />
                ))}
              </div>
            )}
          </ChCheckRow>
          <div style={{ gridColumn: 'span 2' }}><ChErrors data={chErrors} /></div>
        </>)}
      </div>

      {/* Table Diagnosis Modal */}
      <TableDiagModal
        open={diagModalOpen}
        onClose={onDiagClose}
        databases={checks['Total Tables']?.databases || []}
      />
    </>
  );
}
