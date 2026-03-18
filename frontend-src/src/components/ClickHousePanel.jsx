import React, { useState } from 'react';
import { Badge, Tip, Chip } from './Shared';
import { fmt } from '../utils';

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

/* ── Group divider — full-width with horizontal rule ─────────────── */
function GroupDivider({ label }) {
  return (
    <div style={{
      gridColumn: 'span 2', display: 'flex', alignItems: 'center', gap: 12,
      padding: '16px 4px 6px', marginTop: 4,
    }}>
      <span style={{
        fontSize: '0.68rem', fontFamily: 'var(--mono)', fontWeight: 700,
        color: 'var(--accent)', textTransform: 'uppercase', letterSpacing: '1.5px',
        whiteSpace: 'nowrap',
      }}>{label}</span>
      <div style={{ flex: 1, height: 1, background: 'linear-gradient(90deg, rgba(0,212,170,0.3), transparent)' }} />
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

function ChErrors({ data }) {
  const rows = data?.errors || [];
  return (
    <ChCheckRow icon="🔴" label="ClickHouse Errors (1hr)" status={data?.status || 'ok'} detail={data?.detail || ''}
      tip="Query/insert failures from system.query_log + merge memory failures from system.text_log."
    >
      {rows.length > 0 && (
        <DataTable cols={['Error Type', 'Count', 'Severity']}>
          {rows.map((r, i) => (
            <tr key={i}>
              <td style={{ fontWeight: 600 }}>{r.error}</td>
              <td style={{ color: r.status === 'critical' ? 'var(--error)' : 'var(--warn)', fontWeight: 700 }}>{fmt(r.count)}</td>
              <td>
                <span style={{
                  padding: '2px 8px', borderRadius: 4, fontSize: '0.62rem', fontWeight: 700,
                  background: r.status === 'critical' ? 'rgba(239,68,68,0.15)' : 'rgba(245,158,11,0.15)',
                  color: r.status === 'critical' ? 'var(--error)' : 'var(--warn)',
                }}>{r.status.toUpperCase()}</span>
              </td>
            </tr>
          ))}
        </DataTable>
      )}
    </ChCheckRow>
  );
}

/* ── Main ClickHouse Panel ────────────────────────────────────────── */
export default function ClickHousePanel({ checks }) {
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

  return (
    <>
      {/* KPI Status Strip */}
      <StatusStrip checks={checks} />

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

        <GroupDivider label="Kafka &amp; Pipeline" />
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

        <GroupDivider label="Replication &amp; Consistency" />
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

        <GroupDivider label="Schema &amp; Config" />
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

        <GroupDivider label="Storage &amp; Errors" />
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
        <ChCheckRow icon="📊" label="Top Table Sizes" status={tableSizes.status} detail={tableSizes.detail}
          tip="Top 5 tables by compressed disk usage. Useful for capacity planning.">
          {(tableSizes.tables || []).length > 0 && (
            <div style={{ paddingTop: 4 }}>
              {tableSizes.tables.map((t, i) => (
                <SizeBar key={i} label={`${t.database}.${t.table}`} size={t.size} bytes={t.bytes || 0} maxBytes={maxBytes} />
              ))}
            </div>
          )}
        </ChCheckRow>
        <div style={{ gridColumn: 'span 2' }}><ChErrors data={chErrors} /></div>
      </div>
    </>
  );
}
