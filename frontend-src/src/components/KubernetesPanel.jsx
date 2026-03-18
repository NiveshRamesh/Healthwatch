import React, { useState } from 'react';
import { Badge, ProgressBar, Chip } from './Shared';

/* ── KPI Status Strip — pod status checks as horizontal cards ──── */
function StatusStrip({ checks }) {
  const items = Object.entries(checks)
    .filter(([k, v]) => !k.startsWith('__') && v?.status)
    .map(([name, check]) => ({ name, ...check }));

  if (!items.length) return null;

  // Show in a grid — 3 columns for pod checks
  const cols = Math.min(items.length, 3);

  return (
    <div style={{ display: 'grid', gridTemplateColumns: `repeat(${cols}, 1fr)`, gap: 10, padding: '16px 20px' }}>
      {items.map(({ name, status, detail }) => {
        const rgb = status === 'ok' ? '16,185,129' : status === 'warn' ? '245,158,11' : '239,68,68';
        return (
          <div key={name} style={{
            background: `linear-gradient(160deg, rgba(${rgb},0.1), rgba(${rgb},0.02))`,
            border: `1px solid rgba(${rgb},0.18)`, borderTop: `3px solid rgba(${rgb},0.7)`,
            borderRadius: 10, padding: '12px 14px',
          }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
              <span style={{ fontSize: '0.78rem', fontWeight: 600, color: 'var(--text)' }}>{name}</span>
              <Badge status={status} size="sm" />
            </div>
            <div style={{ fontSize: '0.68rem', color: 'var(--muted)', fontFamily: 'var(--mono)', lineHeight: 1.4 }}>{detail}</div>
          </div>
        );
      })}
    </div>
  );
}

/* ── Group divider ─────────────────────────────────────────────── */
function GroupDivider({ label }) {
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 12,
      padding: '16px 20px 6px', marginTop: 4,
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

/* ── Node resource card — enterprise styling ──────────────────── */
function NodeCard({ node }) {
  const { node: name, cpu_used_pct, cpu_threshold, memory_used_pct, memory_threshold,
    memory_used_gb, memory_total_gb, cpu_used_cores, cpu_total_cores, status } = node;

  const cpuStatus = cpu_used_pct >= 90 ? 'critical' : cpu_used_pct >= cpu_threshold ? 'warn' : 'ok';
  const memStatus = memory_used_pct >= 90 ? 'critical' : memory_used_pct >= memory_threshold ? 'warn' : 'ok';
  const rgb = status === 'ok' ? '16,185,129' : status === 'warn' ? '245,158,11' : '239,68,68';

  return (
    <div style={{
      borderRadius: 10, overflow: 'hidden',
      background: `linear-gradient(150deg, rgba(${rgb},0.06), rgba(${rgb},0.01))`,
      border: `1px solid rgba(${rgb},0.15)`,
      borderLeft: `3px solid rgba(${rgb},0.6)`,
      transition: 'box-shadow 0.2s',
    }}
      onMouseEnter={e => e.currentTarget.style.boxShadow = `0 2px 16px rgba(${rgb},0.1)`}
      onMouseLeave={e => e.currentTarget.style.boxShadow = 'none'}
    >
      {/* Header */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '10px 14px', borderBottom: `1px solid rgba(${rgb},0.1)`,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '0.95rem' }}>🖥️</span>
          <span style={{ fontFamily: 'var(--mono)', fontSize: '0.78rem', fontWeight: 700 }}>{name}</span>
        </div>
        <Badge status={status} size="sm" />
      </div>

      {/* CPU & Memory side by side */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 0 }}>
        {/* CPU */}
        <div style={{ padding: '12px 14px', borderRight: `1px solid rgba(${rgb},0.08)` }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
            <span style={{
              fontSize: '0.65rem', fontWeight: 700, color: 'var(--accent)',
              fontFamily: 'var(--mono)', textTransform: 'uppercase', letterSpacing: '0.8px',
            }}>CPU</span>
            <Badge status={cpuStatus} label={`${cpu_used_pct?.toFixed(1)}%`} size="sm" />
          </div>
          <ProgressBar pct={cpu_used_pct} status={cpuStatus} height={5} showLabel={false} />
          <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 5 }}>
            <span style={{ fontFamily: 'var(--mono)', fontSize: '0.6rem', color: 'var(--muted)' }}>
              {cpu_used_cores?.toFixed(1)} / {cpu_total_cores} cores
            </span>
            <span style={{ fontFamily: 'var(--mono)', fontSize: '0.6rem', color: 'var(--muted)' }}>
              thresh {cpu_threshold}%
            </span>
          </div>
        </div>
        {/* Memory */}
        <div style={{ padding: '12px 14px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
            <span style={{
              fontSize: '0.65rem', fontWeight: 700, color: 'var(--accent)',
              fontFamily: 'var(--mono)', textTransform: 'uppercase', letterSpacing: '0.8px',
            }}>Memory</span>
            <Badge status={memStatus} label={`${memory_used_pct?.toFixed(1)}%`} size="sm" />
          </div>
          <ProgressBar pct={memory_used_pct} status={memStatus} height={5} showLabel={false} />
          <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 5 }}>
            <span style={{ fontFamily: 'var(--mono)', fontSize: '0.6rem', color: 'var(--muted)' }}>
              {memory_used_gb?.toFixed(1)} / {memory_total_gb?.toFixed(1)} GB
            </span>
            <span style={{ fontFamily: 'var(--mono)', fontSize: '0.6rem', color: 'var(--muted)' }}>
              thresh {memory_threshold}%
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}

const STATUS_SORT = { critical: 0, error: 0, warn: 1, ok: 2 };

/* ── Pod resource row — compact with inline bars ──────────────── */
function PodResourceRow({ pod, even }) {
  const { pod: name, cpu_used_pct, memory_used_pct, status } = pod;
  const cpuStatus = cpu_used_pct >= 90 ? 'critical' : cpu_used_pct >= 70 ? 'warn' : 'ok';
  const memStatus = memory_used_pct >= 90 ? 'critical' : memory_used_pct >= 80 ? 'warn' : 'ok';
  const cpuColor = cpuStatus === 'ok' ? 'var(--ok)' : cpuStatus === 'warn' ? 'var(--warn)' : 'var(--error)';
  const memColor = memStatus === 'ok' ? 'var(--ok)' : memStatus === 'warn' ? 'var(--warn)' : 'var(--error)';

  return (
    <div style={{
      display: 'grid', gridTemplateColumns: '1.8fr 1fr 1fr 56px',
      alignItems: 'center', gap: 8,
      padding: '7px 14px 7px 16px',
      background: even ? 'rgba(255,255,255,0.02)' : 'transparent',
      transition: 'background 0.15s',
    }}
      onMouseEnter={e => e.currentTarget.style.background = 'rgba(0,212,170,0.04)'}
      onMouseLeave={e => e.currentTarget.style.background = even ? 'rgba(255,255,255,0.02)' : 'transparent'}
    >
      <span style={{
        fontFamily: 'var(--mono)', fontSize: '0.68rem',
        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
      }}>{name}</span>

      <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
        <span style={{ fontFamily: 'var(--mono)', fontSize: '0.55rem', color: 'var(--accent)', minWidth: 22, fontWeight: 600 }}>CPU</span>
        <div style={{ flex: 1, height: 4, background: 'var(--surface3)', borderRadius: 999, overflow: 'hidden', minWidth: 28 }}>
          <div style={{ height: '100%', width: `${Math.min(100, cpu_used_pct || 0)}%`, background: cpuColor, borderRadius: 999, transition: 'width 0.4s ease' }} />
        </div>
        <span style={{ fontFamily: 'var(--mono)', fontSize: '0.6rem', color: cpuColor, minWidth: 34, textAlign: 'right', fontWeight: cpuStatus !== 'ok' ? 700 : 400 }}>
          {(cpu_used_pct || 0).toFixed(1)}%
        </span>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
        <span style={{ fontFamily: 'var(--mono)', fontSize: '0.55rem', color: 'var(--accent)', minWidth: 22, fontWeight: 600 }}>MEM</span>
        <div style={{ flex: 1, height: 4, background: 'var(--surface3)', borderRadius: 999, overflow: 'hidden', minWidth: 28 }}>
          <div style={{ height: '100%', width: `${Math.min(100, memory_used_pct || 0)}%`, background: memColor, borderRadius: 999, transition: 'width 0.4s ease' }} />
        </div>
        <span style={{ fontFamily: 'var(--mono)', fontSize: '0.6rem', color: memColor, minWidth: 34, textAlign: 'right', fontWeight: memStatus !== 'ok' ? 700 : 400 }}>
          {(memory_used_pct || 0).toFixed(1)}%
        </span>
      </div>

      <div style={{ textAlign: 'right' }}>
        <Badge status={status} size="sm" />
      </div>
    </div>
  );
}

/* ── Main Kubernetes Panel ─────────────────────────────────────── */
export default function KubernetesPanel({ checks }) {
  const [nodesOpen, setNodesOpen] = useState(true);
  const [podsOpen, setPodsOpen] = useState(true);

  const resources = checks.__resources__ || {};
  const nodeRes = resources.node_resources || [];
  const podRes = resources.pod_resources || [];

  const podWarn = podRes.filter(p => p.status !== 'ok').length;
  const nodeWarn = nodeRes.filter(n => n.status !== 'ok').length;

  const sortedPods = [...podRes].sort((a, b) => {
    const sa = STATUS_SORT[a.status] ?? 2;
    const sb = STATUS_SORT[b.status] ?? 2;
    if (sa !== sb) return sa - sb;
    return (b.cpu_used_pct + b.memory_used_pct) - (a.cpu_used_pct + a.memory_used_pct);
  });

  return (
    <>
      {/* KPI Status Strip — pod status checks */}
      <StatusStrip checks={checks} />

      {/* Node Resources */}
      {nodeRes.length > 0 && (
        <>
          <GroupDivider label="Node Resources" />
          <div style={{
            margin: '0 16px 10px', borderRadius: 10, overflow: 'hidden',
            border: '1px solid var(--border)',
          }}>
            <div
              onClick={() => setNodesOpen(o => !o)}
              style={{
                display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                padding: '10px 16px', cursor: 'pointer',
                background: 'rgba(0,212,170,0.03)',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ fontSize: '1rem' }}>📈</span>
                <span style={{ fontSize: '0.78rem', fontWeight: 600 }}>{nodeRes.length} Node(s)</span>
                {nodeWarn > 0 && <Chip n={nodeWarn} color="245,158,11" label="warn" />}
              </div>
              <span style={{
                color: 'var(--muted)', fontSize: '0.6rem', transition: 'transform 0.25s',
                transform: nodesOpen ? 'rotate(180deg)' : 'none',
              }}>▼</span>
            </div>
            {nodesOpen && (
              <div style={{ display: 'grid', gridTemplateColumns: nodeRes.length > 1 ? '1fr 1fr' : '1fr', gap: 10, padding: '12px 12px', animation: 'fadeIn 0.2s ease' }}>
                {nodeRes.map((n, i) => <NodeCard key={i} node={n} />)}
              </div>
            )}
          </div>
        </>
      )}

      {/* Pod Resources */}
      {podRes.length > 0 && (
        <>
          <GroupDivider label="Pod Resources" />
          <div style={{
            margin: '0 16px 16px', borderRadius: 10, overflow: 'hidden',
            border: '1px solid var(--border)',
          }}>
            <div
              onClick={() => setPodsOpen(o => !o)}
              style={{
                display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                padding: '10px 16px', cursor: 'pointer',
                background: 'rgba(0,212,170,0.03)',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ fontSize: '1rem' }}>📉</span>
                <span style={{ fontSize: '0.78rem', fontWeight: 600 }}>{podRes.length} Pod(s)</span>
                {podWarn > 0 && <Chip n={podWarn} color="245,158,11" label="warn" />}
              </div>
              <span style={{
                color: 'var(--muted)', fontSize: '0.6rem', transition: 'transform 0.25s',
                transform: podsOpen ? 'rotate(180deg)' : 'none',
              }}>▼</span>
            </div>
            {podsOpen && (
              <div style={{ animation: 'fadeIn 0.2s ease' }}>
                {/* Table header */}
                <div style={{
                  display: 'grid', gridTemplateColumns: '1.8fr 1fr 1fr 56px',
                  gap: 8, padding: '8px 14px 6px 16px',
                  borderBottom: '1px solid rgba(0,212,170,0.15)',
                }}>
                  {['Pod', 'CPU Usage', 'Memory Usage', 'Status'].map(h => (
                    <span key={h} style={{
                      fontSize: '0.62rem', fontFamily: 'var(--mono)', fontWeight: 600,
                      color: 'var(--accent)', textTransform: 'uppercase', letterSpacing: '0.8px',
                    }}>{h}</span>
                  ))}
                </div>
                {sortedPods.map((p, i) => <PodResourceRow key={i} pod={p} even={i % 2 === 0} />)}
              </div>
            )}
          </div>
        </>
      )}
    </>
  );
}
