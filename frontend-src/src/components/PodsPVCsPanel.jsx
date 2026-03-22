import React, { useState } from 'react';
import { Badge, SubSection, Chip } from './Shared';

/* ── Pod status card with expandable container details ───────────── */
function PodStatusCard({ name, status, detail, tags, containers }) {
  const [open, setOpen] = useState(false);
  const rgb = status === 'ok' ? '16,185,129' : status === 'warn' ? '245,158,11' : '239,68,68';
  const hasContainers = containers && containers.length > 0;

  return (
    <div style={{
      background: `linear-gradient(160deg, rgba(${rgb},0.1), rgba(${rgb},0.02))`,
      border: `1px solid rgba(${rgb},0.18)`, borderTop: `3px solid rgba(${rgb},0.7)`,
      borderRadius: 10, overflow: 'hidden',
    }}>
      <div onClick={() => hasContainers && setOpen(o => !o)} style={{
        padding: '14px 14px', cursor: hasContainers ? 'pointer' : 'default',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
          <span style={{ fontSize: '0.78rem', fontWeight: 600, color: 'var(--text)' }}>{name}</span>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <Badge status={status} size="sm" />
            {hasContainers && (
              <span style={{ color: 'var(--muted)', fontSize: '0.55rem', transition: 'transform 0.2s',
                             transform: open ? 'rotate(180deg)' : 'none' }}>▼</span>
            )}
          </div>
        </div>
        {/* Pod name + phase + restarts with ↺ symbol */}
        <div style={{ fontSize: '0.68rem', color: 'var(--muted)', fontFamily: 'var(--mono)', lineHeight: 1.4 }}>
          {(() => {
            const parts = (detail || '').split(',');
            const podInfo = parts[0] || '';
            const restartPart = parts[1] || '';
            const restartNum = restartPart.match(/(\d+)/);
            const restarts = restartNum ? parseInt(restartNum[1]) : 0;
            return (
              <>
                {podInfo.trim()}
                {restarts > 0 ? (
                  <span style={{ color: 'var(--warn)', fontWeight: 700 }}> · ↺ {restarts}</span>
                ) : (
                  <span> · ↺ 0</span>
                )}
              </>
            );
          })()}
        </div>
        {tags && tags.length > 0 && (
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
      {/* Expandable container details */}
      {open && hasContainers && (
        <div style={{ borderTop: `1px solid rgba(${rgb},0.15)`, padding: '8px 10px',
                      background: 'rgba(0,0,0,0.1)', animation: 'fadeIn 0.2s ease' }}>
          {containers.map((c, i) => (
            <div key={i} style={{ background: 'var(--surface2)', borderRadius: 5, padding: '6px 8px', marginBottom: 4,
                                  border: c.restarts > 10 ? '1px solid rgba(245,158,11,0.3)' : '1px solid var(--border)' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                <span style={{ fontFamily: 'var(--mono)', fontSize: '0.65rem', fontWeight: 700 }}>{c.name}</span>
                <span style={{ fontFamily: 'var(--mono)', fontSize: '0.6rem',
                               color: c.state === 'running' ? 'var(--ok)' : 'var(--warn)' }}>{c.state}</span>
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 4 }}>
                {[
                  ['Restarts', c.restarts, c.restarts > 10 ? 'var(--warn)' : 'var(--muted)'],
                  ['CPU Lim', c.cpu_limit || '\u2014', 'var(--muted)'],
                  ['Mem Lim', c.mem_limit || '\u2014', 'var(--muted)'],
                ].map(([lbl, val, color]) => (
                  <div key={lbl}>
                    <div style={{ fontSize: '0.55rem', color: 'var(--muted)', textTransform: 'uppercase',
                                  letterSpacing: '0.5px', fontFamily: 'var(--mono)', marginBottom: 1 }}>{lbl}</div>
                    <div style={{ fontFamily: 'var(--mono)', fontSize: '0.62rem', color }}>
                      {lbl === 'Restarts' ? `↺ ${val}` : val}
                    </div>
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
  const imgData = data.__images_crashes__ || {};
  const allImgPods = imgData.pods || [];

  // Build pod status cards with image tags + container details
  const podItems = Object.entries(data)
    .filter(([k, v]) => k.startsWith('Pod:') && v?.status)
    .map(([name, check]) => {
      const podName = (check.detail || '').split(' ')[0];
      const podInfo = allImgPods.find(p => p.name === podName);
      const tags = podInfo
        ? [...new Set(podInfo.containers.map(c => c.tag))].filter(t => t && t !== 'latest')
        : [];
      // Find container details from pods_pvcs data
      const podContainers = pods.find(p => p.name === podName);
      return { name, ...check, tags, containers: podContainers?.containers || [] };
    });

  const pvcWarn = pvcs.filter(p => p.status !== 'ok').length;
  const orphanCount = pvcs.filter(p => p.orphaned).length;
  const lostCount = pvcs.filter(p => p.phase === 'Lost').length;

  const cols = Math.min(podItems.length, 3);

  return (
    <>
      {/* Pod Status Cards (expandable with container details) */}
      {podItems.length > 0 && (
        <div style={{ display: 'grid', gridTemplateColumns: `repeat(${cols}, 1fr)`, gap: 10, padding: '16px 20px' }}>
          {podItems.map(item => (
            <PodStatusCard key={item.name} {...item} />
          ))}
        </div>
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
