import React, { useState } from 'react';
import { Badge, SubSection, Chip } from './Shared';

/* ── Pod status strip (moved from KubernetesPanel) ───────────────── */
function PodStatusStrip({ checks }) {
  const imgData = checks.__images_crashes__ || {};
  const allPods = imgData.pods || [];

  const items = Object.entries(checks)
    .filter(([k, v]) => k.startsWith('Pod:') && v?.status)
    .map(([name, check]) => {
      const podName = (check.detail || '').split(' ')[0];
      const podInfo = allPods.find(p => p.name === podName);
      const tags = podInfo
        ? [...new Set(podInfo.containers.map(c => c.tag))].filter(t => t && t !== 'latest')
        : [];
      return { name, ...check, tags };
    });

  if (!items.length) return null;
  const cols = Math.min(items.length, 3);

  return (
    <div style={{ display: 'grid', gridTemplateColumns: `repeat(${cols}, 1fr)`, gap: 10, padding: '16px 20px' }}>
      {items.map(({ name, status, detail, tags }) => {
        const rgb = status === 'ok' ? '16,185,129' : status === 'warn' ? '245,158,11' : '239,68,68';
        return (
          <div key={name} style={{
            background: `linear-gradient(160deg, rgba(${rgb},0.1), rgba(${rgb},0.02))`,
            border: `1px solid rgba(${rgb},0.18)`, borderTop: `3px solid rgba(${rgb},0.7)`,
            borderRadius: 10, padding: '14px 14px',
          }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
              <span style={{ fontSize: '0.78rem', fontWeight: 600, color: 'var(--text)' }}>{name}</span>
              <Badge status={status} size="sm" />
            </div>
            <div style={{ fontSize: '0.68rem', color: 'var(--muted)', fontFamily: 'var(--mono)', lineHeight: 1.4 }}>{detail}</div>
            {tags.length > 0 && (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, marginTop: 6 }}>
                {tags.map(tag => (
                  <span key={tag} style={{
                    fontSize: '0.55rem', fontFamily: 'var(--mono)', fontWeight: 700,
                    padding: '2px 6px', borderRadius: 4,
                    background: 'rgba(0,153,255,0.1)', color: 'var(--accent2)',
                    border: '1px solid rgba(0,153,255,0.2)',
                  }}>{tag}</span>
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

/* ── Single pod container card ───────────────────────────────────── */
function PodRow({ pod }) {
  const [open, setOpen] = useState(false);
  const { name, phase, containers, status } = pod;
  const totalRestarts = (containers || []).reduce((s, c) => s + (c.restarts || 0), 0);
  const nonRunning = (containers || []).some(c => c.state !== 'running');

  return (
    <div style={{
      margin: 4, background: 'var(--surface2)', borderRadius: 8, overflow: 'hidden',
      border: status !== 'ok'
        ? `1px solid rgba(${status === 'warn' ? '245,158,11' : '239,68,68'},0.3)`
        : '1px solid var(--border)',
    }}>
      <div onClick={() => setOpen(o => !o)} style={{
        padding: '8px 10px', cursor: 'pointer', display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between',
      }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 5, flexWrap: 'wrap' }}>
            <span style={{ fontFamily: 'var(--mono)', fontSize: '0.7rem', fontWeight: 700, color: 'var(--text)',
                           overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 160 }}>{name}</span>
            <span style={{ fontFamily: 'var(--mono)', fontSize: '0.58rem',
                           color: phase === 'Running' ? 'var(--ok)' : phase === 'Pending' ? 'var(--warn)' : 'var(--error)',
                           background: 'var(--surface3)', borderRadius: 3, padding: '1px 5px' }}>{phase}</span>
          </div>
          <div style={{ display: 'flex', gap: 4, marginTop: 4, flexWrap: 'wrap' }}>
            {totalRestarts > 0 && (
              <span style={{ fontFamily: 'var(--mono)', fontSize: '0.58rem', color: 'var(--warn)',
                             background: 'rgba(245,158,11,0.1)', border: '1px solid rgba(245,158,11,0.3)',
                             borderRadius: 3, padding: '1px 5px' }}>↺ {totalRestarts}</span>
            )}
            {nonRunning && (
              <span style={{ fontFamily: 'var(--mono)', fontSize: '0.58rem', color: 'var(--error)',
                             background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)',
                             borderRadius: 3, padding: '1px 5px' }}>not running</span>
            )}
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexShrink: 0 }}>
          <Badge status={status} size="sm" />
          <span style={{ color: 'var(--muted)', fontSize: '0.6rem', transition: 'transform 0.2s',
                         transform: open ? 'rotate(180deg)' : 'none' }}>▼</span>
        </div>
      </div>
      {open && (
        <div style={{ background: 'var(--surface3)', padding: '8px 10px',
                      borderTop: '1px solid var(--border)', animation: 'fadeIn 0.2s ease' }}>
          {(containers || []).map((c, i) => (
            <div key={i} style={{ background: 'var(--surface)', borderRadius: 5, padding: '6px 8px', marginBottom: 4,
                                  border: c.restarts > 10 ? '1px solid rgba(245,158,11,0.3)' : '1px solid transparent' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                <span style={{ fontFamily: 'var(--mono)', fontSize: '0.65rem', fontWeight: 700 }}>{c.name}</span>
                <span style={{ fontFamily: 'var(--mono)', fontSize: '0.6rem',
                               color: c.state === 'running' ? 'var(--ok)' : 'var(--warn)' }}>{c.state}</span>
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 4 }}>
                {[
                  ['Restarts', c.restarts, c.restarts > 10 ? 'var(--warn)' : 'var(--muted)'],
                  ['CPU Lim', c.cpu_limit || '\u2014', 'var(--muted)'],
                  ['Mem Lim', c.mem_limit || '\u2014', 'var(--muted)'],
                  ['CPU Req', c.cpu_req || '\u2014', 'var(--muted)'],
                ].map(([lbl, val, color]) => (
                  <div key={lbl}>
                    <div style={{ fontSize: '0.55rem', color: 'var(--muted)', textTransform: 'uppercase',
                                  letterSpacing: '0.5px', fontFamily: 'var(--mono)', marginBottom: 1 }}>{lbl}</div>
                    <div style={{ fontFamily: 'var(--mono)', fontSize: '0.62rem', color }}>{val}</div>
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

/* ── PVC compact card ─────────────────────────────────────────────── */
function PVCRow({ pvc }) {
  const { name, phase, capacity, orphaned, status } = pvc;
  return (
    <div style={{
      margin: 4, background: 'var(--surface2)', borderRadius: 7,
      border: status !== 'ok'
        ? `1px solid rgba(${status === 'warn' ? '245,158,11' : '239,68,68'},0.3)`
        : '1px solid var(--border)',
      padding: '8px 10px', display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between',
    }}>
      <div>
        <span style={{ fontFamily: 'var(--mono)', fontSize: '0.7rem', fontWeight: 600,
                       display: 'block', marginBottom: 4 }}>{name}</span>
        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', alignItems: 'center' }}>
          <span style={{ fontFamily: 'var(--mono)', fontSize: '0.58rem',
                         color: phase === 'Bound' ? 'var(--ok)' : phase === 'Lost' ? 'var(--error)' : 'var(--warn)',
                         background: 'var(--surface3)', borderRadius: 3, padding: '1px 5px' }}>{phase}</span>
          <span style={{ fontFamily: 'var(--mono)', fontSize: '0.58rem', color: 'var(--muted)' }}>{capacity}</span>
          {orphaned && (
            <span style={{ fontFamily: 'var(--mono)', fontSize: '0.58rem', color: 'var(--warn)',
                           background: 'rgba(245,158,11,0.1)', border: '1px solid rgba(245,158,11,0.3)',
                           borderRadius: 3, padding: '1px 5px' }}>orphan</span>
          )}
        </div>
      </div>
      <Badge status={status} size="sm" />
    </div>
  );
}

/* ── Main Pods & PVCs Panel ──────────────────────────────────────── */
export default function PodsPVCsPanel({ data }) {
  if (!data) return null;
  const pvcData = data.__pods_pvcs__ || {};
  const pods = pvcData.pods || [];
  const pvcs = pvcData.pvcs || [];

  const podWarn = pods.filter(p => p.status === 'warn' || p.status === 'error' || p.status === 'critical').length;
  const pvcWarn = pvcs.filter(p => p.status !== 'ok').length;
  const orphanCount = pvcs.filter(p => p.orphaned).length;
  const lostCount = pvcs.filter(p => p.phase === 'Lost').length;

  return (
    <>
      {/* Pod Status Cards with image tags */}
      <PodStatusStrip checks={data} />

      {/* Pod Container Health */}
      {pods.length > 0 && (
        <SubSection
          icon="🫙" title="Pod Container Health"
          defaultOpen={true}
          badge={
            <span style={{ display: 'flex', gap: 4 }}>
              {podWarn > 0 && <Chip n={podWarn} color="245,158,11" label="warn" />}
              <Chip n={pods.length} color="100,116,139" label="pods" />
            </span>
          }
        >
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', padding: '4px 2px' }}>
            {pods.map((p, i) => <PodRow key={i} pod={p} />)}
          </div>
        </SubSection>
      )}

      {/* PVCs */}
      <SubSection
        icon="💿" title="Persistent Volume Claims"
        defaultOpen={true}
        badge={
          <span style={{ display: 'flex', gap: 4 }}>
            {lostCount > 0 && <Chip n={lostCount} color="239,68,68" label="lost" />}
            {orphanCount > 0 && <Chip n={orphanCount} color="245,158,11" label="orphan" />}
            {pvcWarn > 0 && <Chip n={pvcWarn} color="245,158,11" label="warn" />}
            <Chip n={pvcs.length} color="100,116,139" label="pvcs" />
          </span>
        }
      >
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', padding: '4px 2px' }}>
          {pvcs.map((p, i) => <PVCRow key={i} pvc={p} />)}
        </div>
      </SubSection>
    </>
  );
}
