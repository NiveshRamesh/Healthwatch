import React, { useState } from 'react';
import { Badge, SubSection, Chip, Tip } from './Shared';

/* ── Single pod row — expandable ─────────────────────────────────── */
function PodRow({ pod }) {
  const [open, setOpen] = useState(false);
  const { name, phase, containers, alerts, status } = pod;
  const totalRestarts = containers.reduce((s, c) => s + (c.restarts || 0), 0);
  const missingLimits = containers.some(c => !c.cpu_limit || !c.mem_limit);
  const nonRunning    = containers.some(c => c.state !== 'running');

  return (
    <div style={{ borderBottom:'1px solid rgba(30,45,69,0.4)' }}>
      <div
        style={{ display:'flex', alignItems:'center', justifyContent:'space-between',
          padding:'10px 22px 10px 16px', cursor:'pointer' }}
        onClick={() => setOpen(o => !o)}
        onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
        onMouseLeave={e => e.currentTarget.style.background='transparent'}
      >
        <div style={{ display:'flex', alignItems:'center', gap:8 }}>
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.75rem', color:'var(--text)' }}>{name}</span>
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.62rem',
            color: phase === 'Running' ? 'var(--ok)' : phase === 'Pending' ? 'var(--warn)' : 'var(--error)',
            background:'var(--surface3)', borderRadius:4, padding:'1px 6px' }}>{phase}</span>
          {totalRestarts > 0 && (
            <span style={{ fontFamily:'var(--mono)', fontSize:'0.62rem', color:'var(--warn)',
              background:'rgba(245,158,11,0.1)', border:'1px solid rgba(245,158,11,0.3)',
              borderRadius:4, padding:'1px 6px' }}>↺ {totalRestarts}</span>
          )}
          {missingLimits && (
            <span style={{ fontFamily:'var(--mono)', fontSize:'0.62rem', color:'var(--warn)',
              background:'rgba(245,158,11,0.1)', border:'1px solid rgba(245,158,11,0.3)',
              borderRadius:4, padding:'1px 6px' }}>⚠ no limits</span>
          )}
          {nonRunning && (
            <span style={{ fontFamily:'var(--mono)', fontSize:'0.62rem', color:'var(--error)',
              background:'rgba(239,68,68,0.1)', border:'1px solid rgba(239,68,68,0.3)',
              borderRadius:4, padding:'1px 6px' }}>⬤ not running</span>
          )}
        </div>
        <div style={{ display:'flex', alignItems:'center', gap:6 }}>
          <Badge status={status} size="sm" />
          <span style={{ color:'var(--muted)', fontSize:'0.65rem', transition:'transform 0.2s',
            transform: open ? 'rotate(180deg)' : 'none' }}>▼</span>
        </div>
      </div>

      {open && (
        <div style={{ background:'var(--surface2)', padding:'10px 22px 14px 24px',
          borderTop:'1px solid var(--border)', animation:'fadeIn 0.2s ease' }}>
          {containers.map((c, i) => (
            <div key={i} style={{
              background:'var(--surface3)', borderRadius:7, padding:'8px 12px', marginBottom:6,
              border: c.restarts > 10 ? '1px solid rgba(245,158,11,0.3)' : '1px solid transparent',
            }}>
              <div style={{ display:'flex', justifyContent:'space-between', marginBottom:4 }}>
                <span style={{ fontFamily:'var(--mono)', fontSize:'0.7rem', fontWeight:700 }}>{c.name}</span>
                <span style={{ fontFamily:'var(--mono)', fontSize:'0.65rem',
                  color: c.state === 'running' ? 'var(--ok)' : 'var(--warn)' }}>
                  {c.state}
                </span>
              </div>
              <div style={{ display:'grid', gridTemplateColumns:'repeat(4,1fr)', gap:6 }}>
                {[
                  ['Restarts', c.restarts, c.restarts > 10 ? 'var(--warn)' : 'var(--muted)'],
                  ['CPU Limit', c.cpu_limit || '⚠ none', !c.cpu_limit ? 'var(--warn)' : 'var(--text)'],
                  ['Mem Limit', c.mem_limit || '⚠ none', !c.mem_limit ? 'var(--warn)' : 'var(--text)'],
                  ['CPU Req',   c.cpu_req   || '—',       'var(--muted)'],
                ].map(([lbl, val, color]) => (
                  <div key={lbl}>
                    <div style={{ fontSize:'0.58rem', color:'var(--muted)', textTransform:'uppercase',
                      letterSpacing:'0.5px', fontFamily:'var(--mono)', marginBottom:2 }}>{lbl}</div>
                    <div style={{ fontFamily:'var(--mono)', fontSize:'0.68rem', color }}>{val}</div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/* ── PVC row ─────────────────────────────────────────────────────── */
function PVCRow({ pvc }) {
  const { name, phase, capacity, orphaned, status } = pvc;
  return (
    <div style={{
      display:'flex', alignItems:'center', justifyContent:'space-between',
      padding:'9px 22px 9px 16px', borderBottom:'1px solid rgba(30,45,69,0.4)',
    }}
      onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
      onMouseLeave={e => e.currentTarget.style.background='transparent'}
    >
      <div style={{ display:'flex', alignItems:'center', gap:8 }}>
        <span style={{ fontFamily:'var(--mono)', fontSize:'0.75rem' }}>{name}</span>
        <span style={{ fontFamily:'var(--mono)', fontSize:'0.62rem',
          color: phase === 'Bound' ? 'var(--ok)' : phase === 'Lost' ? 'var(--error)' : 'var(--warn)',
          background:'var(--surface3)', borderRadius:4, padding:'1px 6px' }}>{phase}</span>
        <span style={{ fontFamily:'var(--mono)', fontSize:'0.62rem', color:'var(--muted)' }}>{capacity}</span>
        {orphaned && (
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.62rem', color:'var(--warn)',
            background:'rgba(245,158,11,0.1)', border:'1px solid rgba(245,158,11,0.3)',
            borderRadius:4, padding:'1px 6px' }}>
            orphan <Tip text="PVC is bound but no pod mounts it — may be leftover from a deleted pod" />
          </span>
        )}
      </div>
      <Badge status={status} size="sm" />
    </div>
  );
}

/* ── Main Pods & PVCs Panel ───────────────────────────────────────── */
export default function PodsPVCsPanel({ data }) {
  if (!data) return null;
  const { pods = [], pvcs = [] } = data;

  const podWarn     = pods.filter(p => p.status === 'warn').length;
  const podErr      = pods.filter(p => p.status === 'error' || p.status === 'critical').length;
  const pvcWarn     = pvcs.filter(p => p.status !== 'ok').length;
  const orphanCount = pvcs.filter(p => p.orphaned).length;
  const lostCount   = pvcs.filter(p => p.phase === 'Lost').length;

  return (
    <>
      <SubSection
        icon="🫙" title="Pod Container Health"
        defaultOpen={true}
        badge={
          <span style={{ display:'flex', gap:4 }}>
            {podErr  > 0 && <Chip n={podErr}  color="239,68,68"  label="error" />}
            {podWarn > 0 && <Chip n={podWarn} color="245,158,11" label="warn"  />}
            <Chip n={pods.length} color="100,116,139" label="pods" />
          </span>
        }
      >
        {pods.map((p, i) => <PodRow key={i} pod={p} />)}
      </SubSection>

      <SubSection
        icon="💿" title="Persistent Volume Claims"
        defaultOpen={true}
        badge={
          <span style={{ display:'flex', gap:4 }}>
            {lostCount   > 0 && <Chip n={lostCount}   color="239,68,68"  label="lost"   />}
            {orphanCount > 0 && <Chip n={orphanCount} color="245,158,11" label="orphan" />}
            {pvcWarn     > 0 && <Chip n={pvcWarn}     color="245,158,11" label="warn"   />}
            <Chip n={pvcs.length} color="100,116,139" label="pvcs" />
          </span>
        }
      >
        {pvcs.map((p, i) => <PVCRow key={i} pvc={p} />)}
      </SubSection>
    </>
  );
}
