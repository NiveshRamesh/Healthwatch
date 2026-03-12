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
    <div style={{ borderBottom:'1px solid rgba(30,45,69,0.4)' }}>
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

/* ── Main ClickHouse Panel ────────────────────────────────────────── */
export default function ClickHousePanel({ checks }) {
  const chTables = checks.__ch_tables__ || {};

  // Map check data
  const unusedKafka   = chTables.unused_kafka_tables   || {};
  const readOnly      = chTables.readonly_tables        || {};
  const inactiveQ     = chTables.inactive_queries       || {};
  const longMut       = chTables.long_mutations         || {};
  const noTTL         = chTables.tables_without_ttl     || {};
  const detached      = chTables.detached_parts         || {};
  const tableSizes    = chTables.table_sizes            || {};
  const replStuck     = chTables.replication_stuck_jobs || {};
  const replicaIncons = chTables.replica_inconsistency  || {};

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
            {['critical','warn'].includes(detached.status)   && <Chip n={detached.parts?.length}   color="239,68,68"  label="critical" />}
            {['warn'].includes(unusedKafka.status)           && <Chip n={unusedKafka.count}        color="245,158,11" label="warn" />}
            {['warn'].includes(noTTL.status)                 && <Chip n={noTTL.tables?.length}     color="245,158,11" label="warn" />}
            {['warn'].includes(longMut.status)               && <Chip n={longMut.mutations?.length} color="245,158,11" label="warn" />}
          </span>
        }
      >

        {/* #11 Unused Kafka engine tables */}
        <ChCheckRow
          icon="📭" label="Unused Kafka Engine Tables"
          status={unusedKafka.status} detail={unusedKafka.detail}
          tip="Kafka consumer tables in system.kafka_consumers with last_commit_time=epoch — never consumed"
        >
          {unusedKafka.count > 0 && (
            <div>
              <p style={{ fontSize:'0.72rem', color:'var(--warn)', marginBottom:6, fontFamily:'var(--mono)' }}>
                ⚠ {unusedKafka.count} tables never committed since epoch
              </p>
              <div style={{ background:'#060910', borderRadius:6, padding:'8px 12px',
                fontFamily:'var(--mono)', fontSize:'0.62rem', color:'#7eb8f7', lineHeight:1.7 }}>
                <div style={{ color:'var(--muted)', marginBottom:3 }}># Run to list them:</div>
                {unusedKafka.remedy}
              </div>
            </div>
          )}
        </ChCheckRow>

        {/* #12 Read-only tables */}
        <ChCheckRow
          icon="🔒" label="Read-Only Replicated Tables"
          status={readOnly.status} detail={readOnly.detail}
          tip="Replicated MergeTree tables stuck in read-only mode — writes will fail"
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
          tip="DDL operations stuck in 'Inactive' state in system.distributed_ddl_queue"
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
          tip="ALTER TABLE mutations that have been running for more than 30 minutes"
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
          icon="⏱️" label="Tables Without TTL Policy"
          status={noTTL.status} detail={noTTL.detail}
          tip="MergeTree tables without a table-level TTL — disk will grow unbounded"
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
          tip="Data parts detached due to corruption, checksum mismatch etc. — data integrity issue"
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
          tip="Top 5 largest tables per database — disk growth awareness"
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
          tip="Replication queue entries postponed >100 times — replication degraded"
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

        {/* #19 Replica inconsistency */}
        <ChCheckRow
          icon="🔂" label="Keeper Replica Inconsistency"
          status={replicaIncons.status} detail={replicaIncons.detail}
          tip="Tables where actual replica count differs from expected cluster replicas"
        >
          {(replicaIncons.tables || []).length > 0 && (
            <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.68rem', fontFamily:'var(--mono)' }}>
              <thead><tr>
                {['Table','Expected','Actual'].map(h=>(
                  <th key={h} style={{ textAlign:'left', padding:'4px 8px', color:'var(--muted)', borderBottom:'1px solid var(--border)' }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>{(replicaIncons.tables||[]).map((t,i)=>(
                <tr key={i}><td style={{ padding:'4px 8px' }}>{t.table}</td>
                <td style={{ padding:'4px 8px', color:'var(--ok)' }}>{replicaIncons.expected_replicas}</td>
                <td style={{ padding:'4px 8px', color:'var(--error)', fontWeight:700 }}>{t.actual_replicas}</td></tr>
              ))}</tbody>
            </table>
          )}
        </ChCheckRow>

      </SubSection>
    </>
  );
}
