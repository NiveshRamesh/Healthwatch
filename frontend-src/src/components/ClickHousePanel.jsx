import React, { useState } from 'react';
import { Badge, SubSection, ProgressBar, Tip, Chip } from './Shared';
import { fmt } from '../utils';

/* ── Existing connectivity rows (unchanged look) ─────────────────── */
function ConnRow({ name, check }) {
  const { status, detail } = check;
  return (
    <div style={{
      display:'flex', alignItems:'center', justifyContent:'space-between',
      padding:'11px 22px 11px 38px', borderBottom:'1px solid rgba(30,45,69,0.5)',
    }}
      onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
      onMouseLeave={e => e.currentTarget.style.background='transparent'}
    >
      <span style={{ fontSize:'0.82rem' }}>{name}</span>
      <div style={{ display:'flex', alignItems:'center', gap:8 }}>
        <span style={{ fontSize:'0.72rem', color:'var(--muted)', fontFamily:'var(--mono)' }}>{detail}</span>
        <Badge status={status} size="sm" />
      </div>
    </div>
  );
}

/* ── Single CH check row ─────────────────────────────────────────── */
function ChCheckRow({ icon, label, status, detail, children, tip }) {
  const [open, setOpen] = useState(false);
  const hasDetail = !!children;
  return (
    <div style={{ borderBottom:'1px solid rgba(30,45,69,0.4)', gridColumn: open ? 'span 2' : 'auto' }}>
      <div
        style={{
          display:'flex', alignItems:'center', justifyContent:'space-between',
          padding:'10px 22px 10px 38px', cursor: hasDetail ? 'pointer' : 'default',
        }}
        onClick={() => hasDetail && setOpen(o => !o)}
        onMouseEnter={e => { if (hasDetail) e.currentTarget.style.background='var(--surface2)'; }}
        onMouseLeave={e => e.currentTarget.style.background='transparent'}
      >
        <div style={{ display:'flex', alignItems:'center', gap:7 }}>
          <span style={{ fontSize:'0.85rem' }}>{icon}</span>
          <span style={{ fontSize:'0.82rem' }}>{label}</span>
          <Tip text={tip} />
        </div>
        <div style={{ display:'flex', alignItems:'center', gap:8 }}>
          <span style={{ fontSize:'0.72rem', color:'var(--muted)', fontFamily:'var(--mono)' }}>{detail}</span>
          <Badge status={status} size="sm" />
          {hasDetail && (
            <span style={{ color:'var(--muted)', fontSize:'0.65rem', transition:'transform 0.2s',
              transform: open ? 'rotate(180deg)' : 'none' }}>▼</span>
          )}
        </div>
      </div>
      {open && hasDetail && (
        <div style={{ background:'var(--surface2)', padding:'10px 22px 14px 38px',
          borderTop:'1px solid var(--border)', animation:'fadeIn 0.2s ease' }}>
          {children}
        </div>
      )}
    </div>
  );
}

/* ── Table sizes bar chart ────────────────────────────────────────── */
function SizeBar({ label, size, bytes, maxBytes }) {
  const pct = maxBytes > 0 ? (bytes / maxBytes) * 100 : 0;
  return (
    <div style={{ marginBottom:7 }}>
      <div style={{ display:'flex', justifyContent:'space-between', marginBottom:3 }}>
        <span style={{ fontFamily:'var(--mono)', fontSize:'0.65rem', color:'var(--text)' }}>{label}</span>
        <span style={{ fontFamily:'var(--mono)', fontSize:'0.65rem', color:'var(--accent)' }}>{size}</span>
      </div>
      <div style={{ height:4, background:'var(--surface3)', borderRadius:999, overflow:'hidden' }}>
        <div style={{ height:'100%', width:`${pct}%`, background:'var(--accent2)',
          borderRadius:999, transition:'width 0.6s ease' }} />
      </div>
    </div>
  );
}

/* ── Paginated replica inconsistency row ─────────────────────────── */
const PAGE_SIZE = 10;

function ReplicaInconsistencyRow({ replicaIncons }) {
  const [page, setPage] = useState(0);
  const rows  = replicaIncons.tables || [];
  const total = rows.length;
  const pages = Math.ceil(total / PAGE_SIZE);
  const slice = rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  return (
    <ChCheckRow
      icon="🔂" label="Keeper Replica Inconsistency"
      status={replicaIncons.status} detail={replicaIncons.detail}
      tip="Tables where the actual replica count across the cluster is less than the expected 2 replicas. Detected via clusterAllReplicas(). Indicates a replica is missing or not registered in Keeper (ZooKeeper)."
    >
      {total > 0 && (
        <div>
          <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.68rem', fontFamily:'var(--mono)' }}>
            <thead><tr>
              {['Table','Expected','Actual'].map(h => (
                <th key={h} style={{ textAlign:'left', padding:'4px 8px', color:'var(--muted)',
                  borderBottom:'1px solid var(--border)' }}>{h}</th>
              ))}
            </tr></thead>
            <tbody>{slice.map((t, i) => (
              <tr key={i} style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(239,68,68,0.03)' }}>
                <td style={{ padding:'4px 8px' }}>{t.table}</td>
                <td style={{ padding:'4px 8px', color:'var(--ok)' }}>{replicaIncons.expected_replicas}</td>
                <td style={{ padding:'4px 8px', color:'var(--error)', fontWeight:700 }}>{t.actual_replicas}</td>
              </tr>
            ))}</tbody>
          </table>

          <Paginator page={page} pages={pages} total={total} pageSize={PAGE_SIZE}
            onPrev={() => setPage(p => p - 1)} onNext={() => setPage(p => p + 1)} />
        </div>
      )}
    </ChCheckRow>
  );
}

/* ── Shared paginator ─────────────────────────────────────────────── */
function Paginator({ page, pages, total, pageSize, onPrev, onNext }) {
  if (pages <= 1) return null;
  const from = page * pageSize + 1;
  const to   = Math.min((page + 1) * pageSize, total);
  return (
    <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between',
      marginTop:8, fontFamily:'var(--mono)', fontSize:'0.65rem', color:'var(--muted)' }}>
      <span>Showing {from}–{to} of {total}</span>
      <div style={{ display:'flex', gap:6 }}>
        <button onClick={onPrev} disabled={page === 0}
          style={{ padding:'2px 10px', borderRadius:4, border:'1px solid var(--border)',
            background: page === 0 ? 'transparent' : 'var(--surface)',
            color: page === 0 ? 'var(--muted)' : 'var(--text)',
            cursor: page === 0 ? 'default' : 'pointer', fontFamily:'var(--mono)', fontSize:'0.65rem' }}>
          ← Prev
        </button>
        <span style={{ padding:'2px 6px', alignSelf:'center' }}>{page + 1} / {pages}</span>
        <button onClick={onNext} disabled={page >= pages - 1}
          style={{ padding:'2px 10px', borderRadius:4, border:'1px solid var(--border)',
            background: page >= pages - 1 ? 'transparent' : 'var(--surface)',
            color: page >= pages - 1 ? 'var(--muted)' : 'var(--text)',
            cursor: page >= pages - 1 ? 'default' : 'pointer', fontFamily:'var(--mono)', fontSize:'0.65rem' }}>
          Next →
        </button>
      </div>
    </div>
  );
}

/* ── CH Kafka Engine Lag section ──────────────────────────────────── */
function ChKafkaEngineLag({ chKafkaLag }) {
  const [page, setPage] = useState(0);
  const allRows  = Object.entries(chKafkaLag || {}).sort((a, b) => b[1].total_lag - a[1].total_lag);
  const highRows = allRows.filter(([, v]) => v.total_lag > 10000);
  const anyHigh  = highRows.length > 0;
  const status   = anyHigh ? 'warn' : 'ok';
  const detail   = allRows.length === 0
    ? 'No Kafka engine tables'
    : anyHigh
      ? `${highRows.length} table(s) high lag`
      : `${allRows.length} table(s) · all lag normal`;

  const rows  = anyHigh ? highRows : [];   // only show table when there's high lag
  const total = rows.length;
  const pages = Math.ceil(total / PAGE_SIZE);
  const slice = rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  return (
    <ChCheckRow
      icon="⚡" label="Kafka Engine Consumer Lag"
      status={status} detail={detail}
      tip="How far behind each ClickHouse Kafka Engine table is from the Kafka topic's latest offset. Computed from system.kafka_consumers current_offset vs Kafka end_offset. Threshold: >10,000 messages = HIGH LAG."
    >
      {total > 0 && (
        <div>
          <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.68rem', fontFamily:'var(--mono)' }}>
            <thead><tr>
              {['CH Table','Topics','Total Lag'].map(h => (
                <th key={h} style={{ textAlign:'left', padding:'4px 8px', color:'var(--muted)',
                  borderBottom:'1px solid var(--border)' }}>{h}</th>
              ))}
            </tr></thead>
            <tbody>
              {slice.map(([key, v], i) => {
                const topics = Object.keys(v.topics || {}).join(', ') || '—';
                return (
                  <tr key={i} style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(245,158,11,0.04)' }}>
                    <td style={{ padding:'4px 8px', color:'var(--text)', fontWeight:600 }}>{key}</td>
                    <td style={{ padding:'4px 8px', color:'var(--muted)', fontSize:'0.62rem',
                      maxWidth:220, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}
                      title={topics}>{topics}</td>
                    <td style={{ padding:'4px 8px', color:'var(--warn)', fontWeight:700 }}>{fmt(v.total_lag)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          <Paginator page={page} pages={pages} total={total} pageSize={PAGE_SIZE}
            onPrev={() => setPage(p => p - 1)} onNext={() => setPage(p => p + 1)} />
        </div>
      )}
    </ChCheckRow>
  );
}

/* ── Kafka Pipeline Health ────────────────────────────────────────── */
function KafkaPipelineHealth({ data }) {
  const [page, setPage] = useState(0);
  const rows  = (data?.tables || []).filter(r => r.status !== 'ok');
  const total = rows.length;
  const pages = Math.ceil(total / PAGE_SIZE);
  const slice = rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  return (
    <ChCheckRow
      icon="🔌" label="Kafka Pipeline Health"
      status={data?.status || 'ok'} detail={data?.detail || ''}
      tip="Per-table staleness from system.kafka_consumers. Warns when seconds_since_last_commit > 300s (5 min). Flags tables that have NEVER committed (last_commit_time = epoch 1970) — likely misconfigured or stalled."
    >
      {total > 0 && (
        <div>
          <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.68rem', fontFamily:'var(--mono)' }}>
            <thead><tr>
              {['Table','Last Commit','Secs Ago','State'].map(h => (
                <th key={h} style={{ textAlign:'left', padding:'4px 8px', color:'var(--muted)',
                  borderBottom:'1px solid var(--border)' }}>{h}</th>
              ))}
            </tr></thead>
            <tbody>
              {slice.map((r, i) => (
                <tr key={i} style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(30,45,69,0.3)' }}>
                  <td style={{ padding:'4px 8px', fontWeight:600 }}>{r.database}.{r.table}</td>
                  <td style={{ padding:'4px 8px', color:'var(--muted)', fontSize:'0.62rem' }}>
                    {r.never_committed ? '—' : r.last_commit_time}
                  </td>
                  <td style={{ padding:'4px 8px', color: r.status === 'error' ? 'var(--error)' : 'var(--warn)', fontWeight:700 }}>
                    {r.never_committed ? 'never' : fmt(r.seconds_since_commit) + 's'}
                  </td>
                  <td style={{ padding:'4px 8px' }}>
                    <span style={{
                      display:'inline-flex', padding:'1px 6px', borderRadius:3,
                      fontSize:'0.62rem', fontWeight:700,
                      background: r.never_committed ? 'rgba(239,68,68,0.15)' : 'rgba(245,158,11,0.15)',
                      color: r.never_committed ? 'var(--error)' : 'var(--warn)',
                    }}>{r.never_committed ? 'NEVER COMMITTED' : 'STALE'}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <Paginator page={page} pages={pages} total={total} pageSize={PAGE_SIZE}
            onPrev={() => setPage(p => p - 1)} onNext={() => setPage(p => p + 1)} />
        </div>
      )}
    </ChCheckRow>
  );
}

/* ── Ingestion Rate ───────────────────────────────────────────────── */
function IngestionRate({ data }) {
  const [page, setPage] = useState(0);
  const rows  = (data?.tables || []).filter(r => r.stopped);
  const total = rows.length;
  const pages = Math.ceil(total / PAGE_SIZE);
  const slice = rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  return (
    <ChCheckRow
      icon="📥" label="Ingestion Rate"
      status={data?.status || 'ok'} detail={data?.detail || ''}
      tip="Rows written per hour and per second per table from system.part_log (last 1 hour). Warns when a table has stopped inserting for more than 10 minutes — may indicate a stalled agent or connector."
    >
      {total > 0 && (
        <div>
          <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.68rem', fontFamily:'var(--mono)' }}>
            <thead><tr>
              {['Table','Rows/hr','Rows/sec','Last Insert','Stopped'].map(h => (
                <th key={h} style={{ textAlign:'left', padding:'4px 8px', color:'var(--muted)',
                  borderBottom:'1px solid var(--border)' }}>{h}</th>
              ))}
            </tr></thead>
            <tbody>
              {slice.map((r, i) => (
                <tr key={i} style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(30,45,69,0.3)' }}>
                  <td style={{ padding:'4px 8px', fontWeight:600 }}>{r.database}.{r.table}</td>
                  <td style={{ padding:'4px 8px' }}>{fmt(r.rows_1h)}</td>
                  <td style={{ padding:'4px 8px' }}>{r.rows_per_sec}</td>
                  <td style={{ padding:'4px 8px', color:'var(--muted)', fontSize:'0.62rem' }}>{r.last_insert}</td>
                  <td style={{ padding:'4px 8px', color:'var(--warn)', fontWeight:700 }}>
                    {r.mins_since_insert != null ? `${r.mins_since_insert}m ago` : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <Paginator page={page} pages={pages} total={total} pageSize={PAGE_SIZE}
            onPrev={() => setPage(p => p - 1)} onNext={() => setPage(p => p + 1)} />
        </div>
      )}
    </ChCheckRow>
  );
}

/* ── Replica Exceptions ───────────────────────────────────────────── */
function ReplicaExceptions({ data }) {
  const [page, setPage] = useState(0);
  const rows  = data?.tables || [];
  const total = rows.length;
  const pages = Math.ceil(total / PAGE_SIZE);
  const slice = rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  return (
    <ChCheckRow
      icon="🚨" label="Replica Exceptions"
      status={data?.status || 'ok'} detail={data?.detail || ''}
      tip="Tables with last_queue_update_exception or zookeeper_exception in system.replicas. Also flags tables with parts_to_check > 300 (warn) or > 500 (critical) — high part counts cause slow merges and memory pressure."
    >
      {total > 0 && (
        <div>
          <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.68rem', fontFamily:'var(--mono)' }}>
            <thead><tr>
              {['Table','Queue Exception','ZK Exception','Parts'].map(h => (
                <th key={h} style={{ textAlign:'left', padding:'4px 8px', color:'var(--muted)',
                  borderBottom:'1px solid var(--border)' }}>{h}</th>
              ))}
            </tr></thead>
            <tbody>
              {slice.map((r, i) => (
                <tr key={i} style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(239,68,68,0.04)' }}>
                  <td style={{ padding:'4px 8px', fontWeight:600 }}>{r.database}.{r.table}</td>
                  <td style={{ padding:'4px 8px', color:'var(--warn)', fontSize:'0.62rem',
                    maxWidth:180, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}
                    title={r.queue_exception}>{r.queue_exception || '—'}</td>
                  <td style={{ padding:'4px 8px', color:'var(--warn)', fontSize:'0.62rem',
                    maxWidth:180, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}
                    title={r.zk_exception}>{r.zk_exception || '—'}</td>
                  <td style={{ padding:'4px 8px',
                    color: r.parts_to_check > 500 ? 'var(--error)' : r.parts_to_check > 300 ? 'var(--warn)' : 'var(--muted)',
                    fontWeight: r.parts_to_check > 300 ? 700 : 400 }}>
                    {r.parts_to_check}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <Paginator page={page} pages={pages} total={total} pageSize={PAGE_SIZE}
            onPrev={() => setPage(p => p - 1)} onNext={() => setPage(p => p + 1)} />
        </div>
      )}
    </ChCheckRow>
  );
}

/* ── ClickHouse Errors ────────────────────────────────────────────── */
function ChErrors({ data }) {
  const rows = data?.errors || [];
  return (
    <ChCheckRow
      icon="🔴" label="ClickHouse Errors (Last 1hr)"
      status={data?.status || 'ok'} detail={data?.detail || ''}
      tip="Insert and query failures from system.query_log grouped by error type (last 1 hour). Also checks system.text_log for background merge memory failures. Error types tracked: MEMORY_LIMIT_EXCEEDED, UNKNOWN_TABLE, SCHEMA_MISMATCH, TOO_MANY_PARTS, PARSE_ERROR, MERGE_MEMORY_LIMIT."
    >
      {rows.length > 0 && (
        <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.68rem', fontFamily:'var(--mono)' }}>
          <thead><tr>
            {['Error Type','Count','Severity'].map(h => (
              <th key={h} style={{ textAlign:'left', padding:'4px 8px', color:'var(--muted)',
                borderBottom:'1px solid var(--border)' }}>{h}</th>
            ))}
          </tr></thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(239,68,68,0.04)' }}>
                <td style={{ padding:'4px 8px', fontWeight:600, color:'var(--text)' }}>{r.error}</td>
                <td style={{ padding:'4px 8px',
                  color: r.status === 'critical' ? 'var(--error)' : 'var(--warn)',
                  fontWeight:700 }}>{fmt(r.count)}</td>
                <td style={{ padding:'4px 8px' }}>
                  <span style={{
                    display:'inline-flex', padding:'1px 6px', borderRadius:3,
                    fontSize:'0.62rem', fontWeight:700,
                    background: r.status === 'critical' ? 'rgba(239,68,68,0.15)' : 'rgba(245,158,11,0.15)',
                    color: r.status === 'critical' ? 'var(--error)' : 'var(--warn)',
                  }}>{r.status.toUpperCase()}</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </ChCheckRow>
  );
}

/* ── Main ClickHouse Panel ────────────────────────────────────────── */
export default function ClickHousePanel({ checks }) {
  const chTables = checks.__ch_tables__ || {};

  // Map check data
  const unusedKafka    = chTables.unused_kafka_tables    || {};
  const readOnly       = chTables.readonly_tables         || {};
  const inactiveQ      = chTables.inactive_queries        || {};
  const longMut        = chTables.long_mutations          || {};
  const noTTL          = chTables.tables_without_ttl      || {};
  const detached       = chTables.detached_parts          || {};
  const tableSizes     = chTables.table_sizes             || {};
  const replStuck      = chTables.replication_stuck_jobs  || {};
  const replicaIncons  = chTables.replica_inconsistency   || {};
  const chKafkaLag     = chTables.ch_kafka_lag            || {};
  const kafkaPipeline  = chTables.kafka_pipeline_health   || {};
  const ingestionRate  = chTables.ingestion_rate          || {};
  const replicaExcept  = chTables.replica_exceptions      || {};
  const chErrors       = chTables.ch_errors               || {};

  const maxBytes = Math.max(...(tableSizes.tables || []).map(t => t.bytes || 0), 1);

  return (
    <>
      {/* Existing connectivity checks */}
      {Object.entries(checks).map(([k, v]) => {
        if (k.startsWith('__') || !v?.status) return null;
        return <ConnRow key={k} name={k} check={v} />;
      })}

      {/* New CH table checks section */}
      <SubSection
        icon="🔬"
        title="ClickHouse Deep Checks"
        defaultOpen={true}
        badge={
          <span style={{ display:'flex', gap:4 }}>
            {['critical','warn'].includes(detached.status)      && <Chip n={detached.parts?.length}        color="239,68,68"  label="critical" />}
            {['critical','warn'].includes(chErrors.status)      && <Chip n={chErrors.errors?.length}       color="239,68,68"  label="errors" />}
            {['critical','warn'].includes(replicaExcept.status) && <Chip n={replicaExcept.tables?.length}  color="239,68,68"  label="replica exc" />}
            {['warn'].includes(unusedKafka.status)              && <Chip n={unusedKafka.count}             color="245,158,11" label="warn" />}
            {['warn'].includes(noTTL.status)                    && <Chip n={noTTL.tables?.length}          color="245,158,11" label="warn" />}
            {['warn'].includes(longMut.status)                  && <Chip n={longMut.mutations?.length}     color="245,158,11" label="warn" />}
            {['error','warn'].includes(kafkaPipeline.status)    && <Chip n={kafkaPipeline.tables?.filter(r=>r.status!=='ok').length} color="245,158,11" label="pipeline" />}
            {['warn'].includes(ingestionRate.status)            && <Chip n={ingestionRate.tables?.filter(r=>r.stopped).length}      color="245,158,11" label="stopped" />}
          </span>
        }
      >

        {/* ── 2-column check grid ── */}
        <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr' }}>

        {/* Kafka Engine Consumer Lag */}
        <ChKafkaEngineLag chKafkaLag={chKafkaLag} />

        {/* #11 Unused Kafka Engine Tables */}
        <ChCheckRow
          icon="📭" label="Unused Kafka Engine Tables"
          status={unusedKafka.status} detail={unusedKafka.detail}
          tip="Kafka Engine tables whose last_commit_time is still epoch (1970). These tables exist but have never consumed a single message — likely misconfigured or abandoned."
        >
          {(unusedKafka.tables || []).length > 0 && (
            <div style={{ display:'flex', flexWrap:'wrap', gap:5 }}>
              {(unusedKafka.tables || []).map((t, i) => (
                <span key={i} style={{
                  background:'rgba(245,158,11,0.1)', border:'1px solid rgba(245,158,11,0.25)',
                  color:'var(--warn)', borderRadius:4, padding:'2px 7px',
                  fontFamily:'var(--mono)', fontSize:'0.62rem',
                }}>{t.database}.{t.table}</span>
              ))}
            </div>
          )}
        </ChCheckRow>

        {/* #12 Read-only tables */}
        <ChCheckRow
          icon="🔒" label="Read-Only Replicated Tables"
          status={readOnly.status} detail={readOnly.detail}
          tip="ReplicatedMergeTree tables in is_readonly=1 state. These tables reject all INSERT/ALTER operations. Usually caused by lost ZooKeeper connection or missing replica path."
        >
          {(readOnly.tables || []).length > 0 && (
            <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.68rem', fontFamily:'var(--mono)' }}>
              <thead><tr>
                {['Database','Table','Engine'].map(h=>(
                  <th key={h} style={{ textAlign:'left', padding:'4px 8px', color:'var(--muted)', borderBottom:'1px solid var(--border)' }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>{(readOnly.tables||[]).map((t,i)=>(
                <tr key={i}><td style={{ padding:'4px 8px', color:'var(--error)' }}>{t.database}</td>
                <td style={{ padding:'4px 8px' }}>{t.table}</td>
                <td style={{ padding:'4px 8px', color:'var(--muted)' }}>{t.engine}</td></tr>
              ))}</tbody>
            </table>
          )}
        </ChCheckRow>

        {/* #13 Inactive DDL queries */}
        <ChCheckRow
          icon="💤" label="Inactive DDL Queries"
          status={inactiveQ.status} detail={inactiveQ.detail}
          tip="DDL queries (CREATE/DROP/ALTER) stuck in Inactive state in system.distributed_ddl_queue. These block schema changes cluster-wide until resolved or manually removed."
        >
          {(inactiveQ.queries || []).length > 0 && (
            <div>{(inactiveQ.queries||[]).map((q,i)=>(
              <div key={i} style={{ marginBottom:6 }}>
                <div style={{ fontSize:'0.65rem', color:'var(--warn)', fontFamily:'var(--mono)' }}>{q.query}</div>
                <div style={{ fontSize:'0.62rem', color:'var(--muted)', marginTop:2, fontFamily:'var(--mono)' }}>Created: {q.query_create_time}</div>
              </div>
            ))}</div>
          )}
        </ChCheckRow>

        {/* #14 Long-running mutations */}
        <ChCheckRow
          icon="⚗️" label="Long-Running Mutations (>30min)"
          status={longMut.status} detail={longMut.detail}
          tip="ALTER TABLE mutations (UPDATE/DELETE/column changes) running for more than 30 minutes. Threshold: 30 min. Long mutations hold resources and slow down merges."
        >
          {(longMut.mutations || []).length > 0 && (
            <div>
              {(longMut.mutations||[]).map((m,i)=>(
                <div key={i} style={{ background:'var(--surface3)', borderRadius:6, padding:'8px 12px', marginBottom:6 }}>
                  <div style={{ display:'flex', justifyContent:'space-between', marginBottom:4 }}>
                    <span style={{ fontFamily:'var(--mono)', fontSize:'0.68rem', color:'var(--text)', fontWeight:700 }}>
                      {m.database}.{m.table}
                    </span>
                    <Badge status="warn" label={`${fmt(m.parts_to_do)} parts left`} size="sm" />
                  </div>
                  <div style={{ fontFamily:'var(--mono)', fontSize:'0.62rem', color:'var(--accent2)', marginBottom:2 }}>{m.command}</div>
                  <div style={{ fontFamily:'var(--mono)', fontSize:'0.6rem', color:'var(--muted)' }}>Started: {m.create_time}</div>
                </div>
              ))}
            </div>
          )}
        </ChCheckRow>

        {/* #15 Tables without TTL */}
        <ChCheckRow
          icon="⏱️" label="Tables Without Timestamp Partition"
          status={noTTL.status} detail={noTTL.detail}
          tip="MergeTree tables missing a timestamp-based PARTITION BY (toYYYYMMDD, toYYYYMM, toDate, toStartOf*). Timestamp partitioning is required for hot→warm tiered storage. Tables without it cannot be moved between storage tiers."
        >
          {(noTTL.tables || []).length > 0 && (
            <div style={{ display:'flex', flexWrap:'wrap', gap:5 }}>
              {(noTTL.tables||[]).map((t,i)=>(
                <span key={i} style={{
                  background:'rgba(245,158,11,0.1)', border:'1px solid rgba(245,158,11,0.25)',
                  color:'var(--warn)', borderRadius:4, padding:'2px 7px',
                  fontFamily:'var(--mono)', fontSize:'0.62rem',
                }}>{t.database}.{t.table}</span>
              ))}
            </div>
          )}
        </ChCheckRow>

        {/* #16 Detached/corrupted parts */}
        <ChCheckRow
          icon="💀" label="Detached / Corrupted Parts"
          status={detached.status} detail={detached.detail}
          tip="Data parts moved to the detached/ folder due to corruption, checksum errors, or manual detach. Detached parts are excluded from queries — data may be lost or unreadable."
        >
          {(detached.parts || []).length > 0 && (
            <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.68rem', fontFamily:'var(--mono)' }}>
              <thead><tr>
                {['Database','Table','Reason','Count'].map(h=>(
                  <th key={h} style={{ textAlign:'left', padding:'4px 8px', color:'var(--muted)', borderBottom:'1px solid var(--border)' }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>{(detached.parts||[]).map((p,i)=>(
                <tr key={i} style={{ background: i%2===0?'transparent':'rgba(239,68,68,0.04)' }}>
                  <td style={{ padding:'4px 8px', color:'var(--error)' }}>{p.database}</td>
                  <td style={{ padding:'4px 8px' }}>{p.table}</td>
                  <td style={{ padding:'4px 8px', color:'var(--warn)' }}>{p.reason}</td>
                  <td style={{ padding:'4px 8px', fontWeight:700, color:'var(--error)' }}>{p.count}</td>
                </tr>
              ))}</tbody>
            </table>
          )}
        </ChCheckRow>

        {/* #17 Table sizes — always shown */}
        <ChCheckRow
          icon="📊" label="Top Table Sizes"
          status={tableSizes.status} detail={tableSizes.detail}
          tip="Top 5 tables by compressed disk usage from system.parts. Useful for tracking which tables consume the most storage and planning capacity."
        >
          {(tableSizes.tables || []).length > 0 && (
            <div style={{ paddingTop:4 }}>
              {(tableSizes.tables||[]).map((t,i)=>(
                <SizeBar key={i}
                  label={`${t.database}.${t.table}`}
                  size={t.size}
                  bytes={t.bytes||0}
                  maxBytes={maxBytes}
                />
              ))}
            </div>
          )}
        </ChCheckRow>

        {/* #18 Replication queue stuck */}
        <ChCheckRow
          icon="🔁" label="Replication Queue Stuck Jobs"
          status={replStuck.status} detail={replStuck.detail}
          tip="Replication queue entries postponed more than 100 times (threshold: 100). High postpone counts mean a replica is consistently failing to replicate — data may fall behind."
        >
          {(replStuck.jobs || []).length > 0 && (
            <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.68rem', fontFamily:'var(--mono)' }}>
              <thead><tr>
                {['Database','Table','Count'].map(h=>(
                  <th key={h} style={{ textAlign:'left', padding:'4px 8px', color:'var(--muted)', borderBottom:'1px solid var(--border)' }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>{(replStuck.jobs||[]).map((j,i)=>(
                <tr key={i}><td style={{ padding:'4px 8px', color:'var(--error)' }}>{j.database}</td>
                <td style={{ padding:'4px 8px' }}>{j.table}</td>
                <td style={{ padding:'4px 8px', fontWeight:700 }}>{j.count}</td></tr>
              ))}</tbody>
            </table>
          )}
        </ChCheckRow>

        {/* #19 Replica inconsistency — paginated */}
        <ReplicaInconsistencyRow replicaIncons={replicaIncons} />

        {/* #21 Kafka pipeline health */}
        <KafkaPipelineHealth data={kafkaPipeline} />

        {/* #22 Ingestion rate */}
        <IngestionRate data={ingestionRate} />

        {/* #23 Replica exceptions */}
        <ReplicaExceptions data={replicaExcept} />

        {/* #24 ClickHouse errors */}
        <ChErrors data={chErrors} />

        </div>{/* end 2-col check grid */}
      </SubSection>
    </>
  );
}
