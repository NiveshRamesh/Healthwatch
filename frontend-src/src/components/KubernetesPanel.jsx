import React, { useState } from 'react';
import { Badge, ProgressBar, Chip } from './Shared';

/* ── KPI Status Strip — pod status checks as horizontal cards ──── */
function StatusStrip({ checks }) {
  const imgData = checks.__images_crashes__ || {};
  const allPods = imgData.pods || [];

  const items = Object.entries(checks)
    .filter(([k, v]) => !k.startsWith('__') && v?.status)
    .map(([name, check]) => {
      // Match pod image tag: detail starts with pod name
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
      display: 'grid', gridTemplateColumns: 'minmax(0, 1.8fr) minmax(0, 1fr) minmax(0, 1fr) 64px',
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
      {/* Cluster Nodes status strip */}
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
                  display: 'grid', gridTemplateColumns: 'minmax(0, 1.8fr) minmax(0, 1fr) minmax(0, 1fr) 64px',
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

      {/* Pod Connectivity */}
      <ConnectivitySection data={checks.__connectivity__} />

      {/* Images & Crash Logs */}
      <ImagesCrashSection data={checks.__images_crashes__} />
    </>
  );
}

/* ── Pod Connectivity Section ────────────────────────────────────── */
/* ── Pod Container Health Section ─────────────────────────────────── */
function PodContainerSection({ pods }) {
  const [open, setOpen] = useState(false);
  if (!pods || pods.length === 0) return null;

  const podWarn = pods.filter(p => p.status === 'warn' || p.status === 'error' || p.status === 'critical').length;

  return (
    <>
      <GroupDivider label="Pod Container Health" />
      <div style={{ margin: '0 16px 16px', borderRadius: 10, overflow: 'hidden', border: '1px solid var(--border)' }}>
        <div onClick={() => setOpen(o => !o)} style={{
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '10px 16px', cursor: 'pointer', background: 'rgba(0,212,170,0.03)',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: '1rem' }}>🫙</span>
            <span style={{ fontSize: '0.78rem', fontWeight: 600 }}>{pods.length} Pod(s)</span>
            {podWarn > 0 && <Chip n={podWarn} color="245,158,11" label="warn" />}
          </div>
          <span style={{ color: 'var(--muted)', fontSize: '0.6rem', transition: 'transform 0.25s',
                         transform: open ? 'rotate(180deg)' : 'none' }}>▼</span>
        </div>
        {open && (
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', padding: '4px 2px', animation: 'fadeIn 0.2s ease' }}>
            {pods.map((pod, i) => <PodContainerCard key={i} pod={pod} />)}
          </div>
        )}
      </div>
    </>
  );
}

function PodContainerCard({ pod }) {
  const [open, setOpen] = useState(false);
  const { name, phase, containers, status } = pod;
  const totalRestarts = (containers || []).reduce((s, c) => s + (c.restarts || 0), 0);
  const nonRunning = (containers || []).some(c => c.state !== 'running');
  const borderColor = status !== 'ok'
    ? `rgba(${status === 'warn' ? '245,158,11' : '239,68,68'},0.3)` : 'var(--border)';

  return (
    <div style={{ margin: 4, background: 'var(--surface2)', borderRadius: 8, overflow: 'hidden',
                  border: `1px solid ${borderColor}` }}>
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

function ConnectivitySection({ data }) {
  const [open, setOpen] = useState(false);
  if (!data || !data.pods) return null;

  const pods = data.pods || [];
  const failedPods = pods.filter(p => p.status === 'error');
  const okPods = pods.filter(p => p.status === 'ok');

  return (
    <>
      <GroupDivider label="Pod Connectivity" />
      <div style={{ margin: '0 16px 16px', borderRadius: 10, overflow: 'hidden', border: '1px solid var(--border)' }}>
        <div onClick={() => setOpen(o => !o)} style={{
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '10px 16px', cursor: 'pointer', background: 'rgba(0,212,170,0.03)',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: '1rem' }}>🔗</span>
            <span style={{ fontSize: '0.78rem', fontWeight: 600 }}>{pods.length} Pod Groups</span>
            {failedPods.length > 0 && <Chip n={failedPods.length} color="239,68,68" label="issues" />}
            {failedPods.length === 0 && <Chip n={okPods.length} color="16,185,129" label="all ok" />}
          </div>
          <span style={{ color: 'var(--muted)', fontSize: '0.6rem', transition: 'transform 0.25s',
                         transform: open ? 'rotate(180deg)' : 'none' }}>▼</span>
        </div>
        {open && (
          <div style={{ padding: '8px 12px', animation: 'fadeIn 0.2s ease' }}>
            {/* Show failed pods first */}
            {[...failedPods, ...okPods].map(pod => (
              <ConnectivityPod key={pod.pod_prefix} pod={pod} />
            ))}
          </div>
        )}
      </div>
    </>
  );
}

function ConnectivityPod({ pod }) {
  const [expanded, setExpanded] = useState(pod.status === 'error');
  const failed = pod.connections.filter(c => c.status === 'error');
  const ok = pod.connections.filter(c => c.status === 'ok');
  const isOk = pod.status === 'ok';

  return (
    <div style={{ marginBottom: 6, borderRadius: 8, border: `1px solid ${isOk ? 'var(--border)' : 'rgba(239,68,68,0.25)'}`,
                  background: isOk ? 'transparent' : 'rgba(239,68,68,0.03)' }}>
      <div onClick={() => setExpanded(e => !e)} style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '8px 12px', cursor: 'pointer',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 }}>
          <span style={{ fontSize: '0.7rem' }}>{isOk ? '🟢' : '🔴'}</span>
          <span style={{ fontSize: '0.7rem', fontWeight: 600, fontFamily: 'var(--mono)', color: 'var(--text)',
                         overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {pod.actual_pod}
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ fontSize: '0.58rem', fontFamily: 'var(--mono)', color: 'var(--muted)' }}>
            {ok.length}/{pod.connections.length} ok
          </span>
          {failed.length > 0 && (
            <span style={{ fontSize: '0.55rem', fontWeight: 700, padding: '1px 5px', borderRadius: 3,
                           background: 'rgba(239,68,68,0.12)', color: 'var(--error)', fontFamily: 'var(--mono)' }}>
              {failed.length} FAIL
            </span>
          )}
          <span style={{ color: 'var(--muted)', fontSize: '0.55rem', transform: expanded ? 'rotate(180deg)' : 'none',
                         transition: 'transform 0.2s' }}>▼</span>
        </div>
      </div>
      {expanded && (
        <div style={{ padding: '0 12px 8px', display: 'flex', flexWrap: 'wrap', gap: 4 }}>
          {pod.connections.map(c => {
            const ok = c.status === 'ok';
            return (
              <span key={c.service} style={{
                display: 'inline-flex', alignItems: 'center', gap: 4,
                padding: '3px 8px', borderRadius: 4, fontSize: '0.58rem', fontFamily: 'var(--mono)',
                background: ok ? 'rgba(16,185,129,0.08)' : 'rgba(239,68,68,0.08)',
                border: `1px solid ${ok ? 'rgba(16,185,129,0.2)' : 'rgba(239,68,68,0.2)'}`,
                color: ok ? 'var(--ok)' : 'var(--error)',
              }}>
                <span style={{ fontSize: '0.55rem' }}>{ok ? '✓' : '✕'}</span>
                {c.service}
                <span style={{ color: 'var(--muted)', fontSize: '0.5rem' }}>{c.detail}</span>
              </span>
            );
          })}
        </div>
      )}
    </div>
  );
}

/* ── Crash Logs Section ───────────────────────────────────────────── */
function ImagesCrashSection({ data }) {
  const [crashOpen, setCrashOpen] = useState(false);
  if (!data) return null;

  const crashPods = data.crash_pods || [];
  const allPods = data.pods || [];

  return (
    <>
      {crashPods.length > 0 && (
        <>
          <GroupDivider label="Crash Logs" />
          <div style={{ margin: '0 16px 16px', borderRadius: 10, overflow: 'hidden', border: '1px solid rgba(239,68,68,0.25)' }}>
            <div onClick={() => setCrashOpen(o => !o)} style={{
              display: 'flex', alignItems: 'center', justifyContent: 'space-between',
              padding: '10px 16px', cursor: 'pointer', background: 'rgba(239,68,68,0.04)',
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ fontSize: '1rem' }}>💥</span>
                <span style={{ fontSize: '0.78rem', fontWeight: 600 }}>{crashPods.length} Containers with Restarts</span>
              </div>
              <span style={{ color: 'var(--muted)', fontSize: '0.6rem', transition: 'transform 0.25s',
                             transform: crashOpen ? 'rotate(180deg)' : 'none' }}>▼</span>
            </div>
            {crashOpen && (
              <div style={{ padding: '4px 0', animation: 'fadeIn 0.2s ease' }}>
                {crashPods.map((cp, i) => (
                  <CrashPodRow key={`${cp.pod}-${cp.container}-${i}`} crash={cp} allPods={allPods} />
                ))}
              </div>
            )}
          </div>
        </>
      )}
    </>
  );
}

function CrashPodRow({ crash, allPods }) {
  const [logsOpen, setLogsOpen] = useState(false);
  const podData = allPods.find(p => p.name === crash.pod);
  const crashLog = podData?.crash_logs?.find(cl => cl.container === crash.container);
  const lastLines = crashLog?.last_lines || [];

  return (
    <div style={{ borderBottom: '1px solid var(--border)' }}>
      <div onClick={() => setLogsOpen(o => !o)} style={{
        display: 'grid', gridTemplateColumns: 'minmax(0, 1.5fr) minmax(0, 1fr) 60px minmax(0, 2fr)',
        gap: 8, padding: '8px 16px', cursor: 'pointer', alignItems: 'center',
      }}>
        <span style={{ fontSize: '0.62rem', fontFamily: 'var(--mono)', color: 'var(--text)',
                       overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {crash.pod}
        </span>
        <span style={{ fontSize: '0.58rem', fontFamily: 'var(--mono)', color: 'var(--muted)' }}>
          {crash.container}
        </span>
        <span style={{ fontSize: '0.62rem', fontWeight: 700, fontFamily: 'var(--mono)',
                       color: crash.restarts > 10 ? 'var(--error)' : 'var(--warn)' }}>
          {crash.restarts}x
        </span>
        <span style={{ fontSize: '0.58rem', fontFamily: 'var(--mono)', color: 'var(--error)',
                       overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {crash.crash_reason || 'Unknown'}
        </span>
      </div>
      {logsOpen && lastLines.length > 0 && (
        <div style={{ margin: '0 16px 8px', padding: '8px 10px', borderRadius: 6,
                      background: 'rgba(0,0,0,0.3)', maxHeight: 200, overflowY: 'auto' }}>
          {lastLines.map((line, i) => (
            <div key={i} style={{
              fontSize: '0.55rem', fontFamily: 'var(--mono)', color: 'var(--muted)',
              lineHeight: 1.6, whiteSpace: 'pre-wrap', wordBreak: 'break-all',
            }}>{line}</div>
          ))}
        </div>
      )}
    </div>
  );
}
