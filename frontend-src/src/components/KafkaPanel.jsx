import React, { useState } from 'react';
import { Badge, Chip, Tip } from './Shared';
import { fmt } from '../utils';
import LiveDataPanel from './LiveDataPanel';
import ConsumerLagPanel from './ConsumerLagPanel';
import ZookeeperStatsPanel from './ZookeeperStatsPanel';
import Modal from './Modal';
import { TopicDetail } from './TopicDiagBar';

/* ── KPI Status Strip — horizontal cards for top-level checks ──── */
function StatusStrip({ checks }) {
  const items = Object.entries(checks)
    .filter(([k, v]) => !k.startsWith('__') && v?.status && k !== 'Kafka Connectors' && k !== 'Zookeeper Stats')
    .map(([name, check]) => ({ name, ...check }));

  if (!items.length) return null;

  return (
    <div style={{ display: 'grid', gridTemplateColumns: `repeat(${Math.min(items.length, 4)}, 1fr)`, gap: 12, padding: '16px 20px' }}>
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

/* ── Connector state chip colours ──────────────────────────────── */
const STATE_STYLE = {
  RUNNING:     { color: 'var(--ok)',    bg: 'rgba(16,185,129,0.1)',   border: 'rgba(16,185,129,0.3)' },
  PAUSED:      { color: 'var(--warn)',  bg: 'rgba(245,158,11,0.1)',   border: 'rgba(245,158,11,0.3)' },
  STOPPED:     { color: 'var(--warn)',  bg: 'rgba(245,158,11,0.1)',   border: 'rgba(245,158,11,0.3)' },
  FAILED:      { color: 'var(--error)', bg: 'rgba(239,68,68,0.1)',    border: 'rgba(239,68,68,0.3)' },
  MISSING:     { color: 'var(--error)', bg: 'rgba(239,68,68,0.1)',    border: 'rgba(239,68,68,0.3)' },
  UNREACHABLE: { color: 'var(--error)', bg: 'rgba(239,68,68,0.1)',    border: 'rgba(239,68,68,0.3)' },
  UNASSIGNED:  { color: 'var(--warn)',  bg: 'rgba(245,158,11,0.1)',   border: 'rgba(245,158,11,0.3)' },
  UNKNOWN:     { color: 'var(--muted)', bg: 'rgba(255,255,255,0.05)', border: 'rgba(255,255,255,0.1)' },
};

function StateChip({ state }) {
  const s = STATE_STYLE[state] || STATE_STYLE.UNKNOWN;
  return (
    <span style={{
      fontFamily: 'var(--mono)', fontSize: '0.62rem', fontWeight: 700,
      color: s.color, background: s.bg, border: `1px solid ${s.border}`,
      borderRadius: 4, padding: '2px 8px', letterSpacing: '0.4px',
    }}>{state}</span>
  );
}

/* ── Connector card — enterprise styling ──────────────────────── */
function ConnectorCard({ conn }) {
  const [open, setOpen] = useState(false);
  const tasks = conn.tasks || [];
  const failedTasks = tasks.filter(t => t.state !== 'RUNNING');
  const hasIssues = failedTasks.length > 0 && conn.connector_state !== 'MISSING' && conn.connector_state !== 'UNREACHABLE';
  const rgb = conn.status === 'ok' ? '16,185,129' : conn.status === 'warn' ? '245,158,11' : '239,68,68';

  return (
    <div style={{
      margin: '0 16px 10px', borderRadius: 10, overflow: 'hidden',
      background: `linear-gradient(150deg, rgba(${rgb},0.06), rgba(${rgb},0.01))`,
      border: `1px solid rgba(${rgb},0.15)`,
      borderLeft: `3px solid rgba(${rgb},0.6)`,
      transition: 'box-shadow 0.2s',
    }}
      onMouseEnter={e => e.currentTarget.style.boxShadow = `0 2px 16px rgba(${rgb},0.1)`}
      onMouseLeave={e => e.currentTarget.style.boxShadow = 'none'}
    >
      <div
        onClick={() => hasIssues && setOpen(o => !o)}
        style={{
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '12px 16px', cursor: hasIssues ? 'pointer' : 'default',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ fontSize: '1.1rem' }}>🔌</span>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ fontFamily: 'var(--mono)', fontSize: '0.78rem', fontWeight: 600 }}>{conn.name}</span>
              <StateChip state={conn.connector_state} />
            </div>
            <div style={{ fontSize: '0.68rem', color: 'var(--muted)', fontFamily: 'var(--mono)', marginTop: 3 }}>{conn.detail}</div>
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {tasks.length > 0 && (
            <span style={{ fontFamily: 'var(--mono)', fontSize: '0.62rem', color: 'var(--muted)' }}>
              {tasks.filter(t => t.state === 'RUNNING').length}/{tasks.length} tasks
            </span>
          )}
          <Badge status={conn.status} size="sm" />
          {hasIssues && (
            <span style={{
              color: 'var(--muted)', fontSize: '0.6rem', transition: 'transform 0.25s',
              transform: open ? 'rotate(180deg)' : 'none',
            }}>▼</span>
          )}
        </div>
      </div>

      {open && hasIssues && (
        <div style={{
          borderTop: `1px solid rgba(${rgb},0.12)`, padding: '10px 16px',
          background: 'rgba(0,0,0,0.15)',
        }}>
          <table style={{ width: '100%', borderCollapse: 'separate', borderSpacing: '0 2px', fontSize: '0.68rem', fontFamily: 'var(--mono)' }}>
            <thead>
              <tr>
                {['Task', 'State', 'Worker'].map(h => (
                  <th key={h} style={{
                    textAlign: 'left', padding: '4px 10px',
                    color: 'var(--accent)', fontWeight: 600, fontSize: '0.62rem',
                    textTransform: 'uppercase', letterSpacing: '0.8px',
                    borderBottom: '1px solid rgba(0,212,170,0.15)',
                  }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {tasks.map(t => (
                <tr key={t.id} style={{ background: t.state !== 'RUNNING' ? 'rgba(239,68,68,0.04)' : 'transparent' }}>
                  <td style={{ padding: '5px 10px', color: 'var(--muted)' }}>task[{t.id}]</td>
                  <td style={{ padding: '5px 10px' }}><StateChip state={t.state} /></td>
                  <td style={{ padding: '5px 10px', color: 'var(--muted)', fontSize: '0.62rem' }}>{t.worker_id || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

/* ── Zookeeper Stats — enterprise card styling ─────────────────── */
function ZkStatsCard({ zkStats }) {
  const [open, setOpen] = useState(false);
  if (!zkStats) return null;

  const {
    ruok, server_state,
    avg_latency_ms, max_latency_ms,
    outstanding_requests, outstanding_warn_threshold,
    alive_connections, znode_count, watch_count,
    open_fds, max_fds, uptime_hours,
    status,
  } = zkStats;

  const rgb = status === 'ok' ? '16,185,129' : status === 'warn' ? '245,158,11' : '239,68,68';
  const outstandingStatus = outstanding_requests > outstanding_warn_threshold ? 'warn' : 'ok';
  const fdPct = max_fds > 0 ? open_fds / max_fds : 0;
  const fdStatus = fdPct > 0.8 ? 'warn' : 'ok';

  const ZK_COLORS = {
    leader:     { color: 'var(--accent)',  bg: 'rgba(0,212,170,0.1)',   border: 'rgba(0,212,170,0.3)' },
    follower:   { color: 'var(--accent2)', bg: 'rgba(0,153,255,0.1)',   border: 'rgba(0,153,255,0.3)' },
    standalone: { color: 'var(--muted)',   bg: 'rgba(255,255,255,0.05)', border: 'rgba(255,255,255,0.1)' },
    observer:   { color: 'var(--warn)',    bg: 'rgba(245,158,11,0.1)',  border: 'rgba(245,158,11,0.3)' },
  };

  const sc = ZK_COLORS[(server_state || '').toLowerCase()] || ZK_COLORS.standalone;

  const metrics = [
    { label: 'Avg Latency', value: `${avg_latency_ms} ms` },
    { label: 'Max Latency', value: `${max_latency_ms} ms` },
    { label: 'Outstanding Reqs', value: outstanding_requests, status: outstandingStatus, threshold: outstanding_warn_threshold },
    { label: 'Connections', value: alive_connections },
    { label: 'Znodes', value: znode_count },
    { label: 'Watches', value: watch_count },
    { label: 'Open FDs', value: max_fds > 0 ? `${open_fds} / ${max_fds}` : open_fds, status: fdStatus },
  ];

  return (
    <div style={{
      margin: '0 16px 10px', borderRadius: 10, overflow: 'hidden',
      background: `linear-gradient(150deg, rgba(${rgb},0.06), rgba(${rgb},0.01))`,
      border: `1px solid rgba(${rgb},0.15)`,
      borderLeft: `3px solid rgba(${rgb},0.6)`,
      transition: 'box-shadow 0.2s',
    }}
      onMouseEnter={e => e.currentTarget.style.boxShadow = `0 2px 16px rgba(${rgb},0.1)`}
      onMouseLeave={e => e.currentTarget.style.boxShadow = 'none'}
    >
      <div
        onClick={() => setOpen(o => !o)}
        style={{
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '12px 16px', cursor: 'pointer',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ fontSize: '1.1rem' }}>🦓</span>
          <div>
            <span style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text)' }}>Zookeeper Ensemble</span>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 4 }}>
              <span style={{
                fontFamily: 'var(--mono)', fontSize: '0.62rem', fontWeight: 700,
                color: ruok ? 'var(--ok)' : 'var(--error)',
                background: ruok ? 'rgba(16,185,129,0.1)' : 'rgba(239,68,68,0.1)',
                border: ruok ? '1px solid rgba(16,185,129,0.3)' : '1px solid rgba(239,68,68,0.3)',
                borderRadius: 4, padding: '1px 6px',
              }}>
                ruok: {ruok ? 'imok' : 'FAIL'}
              </span>
              <span style={{
                fontFamily: 'var(--mono)', fontSize: '0.62rem', fontWeight: 700,
                color: sc.color, background: sc.bg, border: `1px solid ${sc.border}`,
                borderRadius: 4, padding: '1px 6px',
              }}>{(server_state || 'UNKNOWN').toUpperCase()}</span>
              {uptime_hours != null && (
                <span style={{ fontFamily: 'var(--mono)', fontSize: '0.62rem', color: 'var(--muted)' }}>up {uptime_hours}h</span>
              )}
            </div>
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <Badge status={status} size="sm" />
          <span style={{
            color: 'var(--muted)', fontSize: '0.6rem', transition: 'transform 0.25s',
            transform: open ? 'rotate(180deg)' : 'none',
          }}>▼</span>
        </div>
      </div>

      {open && (
        <div style={{
          borderTop: `1px solid rgba(${rgb},0.12)`,
          background: 'rgba(0,0,0,0.15)', padding: '12px 16px',
        }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
            {metrics.map(m => {
              const mRgb = m.status === 'warn' ? '245,158,11' : m.status === 'error' ? '239,68,68' : '255,255,255';
              return (
                <div key={m.label} style={{
                  display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                  padding: '6px 10px', borderRadius: 6,
                  background: m.status && m.status !== 'ok' ? `rgba(${mRgb},0.05)` : 'rgba(255,255,255,0.02)',
                }}>
                  <span style={{ fontSize: '0.68rem', color: 'var(--muted)' }}>{m.label}</span>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    {m.threshold != null && (
                      <span style={{ fontFamily: 'var(--mono)', fontSize: '0.58rem', color: 'var(--muted)' }}>
                        warn &gt; {m.threshold}
                      </span>
                    )}
                    <span style={{
                      fontFamily: 'var(--mono)', fontSize: '0.72rem', fontWeight: m.status && m.status !== 'ok' ? 700 : 400,
                      color: m.status === 'warn' ? 'var(--warn)' : m.status === 'error' ? 'var(--error)' : 'var(--text)',
                    }}>{m.value}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

/* ── Topic Diagnosis Modal ─────────────────────────────────────── */
function TopicDiagModal({ open, onClose, tls, fetchTopic }) {
  const [query, setQuery] = useState('');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);

  const allKnown = Object.keys(tls || {}).sort();

  function handleClose() { onClose(); setQuery(''); setResult(null); }

  async function inspect() {
    const name = query.trim();
    if (!name) return;
    setLoading(true); setResult(null);
    try {
      const d = await fetchTopic(name);
      setResult(d);
    } catch (e) {
      setResult({ found: false, topic: name, error: String(e.message) });
    } finally {
      setLoading(false);
    }
  }

  if (!open) return null;

  return (
    <Modal open={open} onClose={handleClose} title="🔍 Topic Diagnosis">
      <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        <div>
          <div style={{
            fontSize: '0.65rem', color: 'var(--muted)', fontFamily: 'var(--mono)',
            textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: 5,
          }}>Topic name</div>
          <input
            list="hw-topics-list"
            value={query}
            onChange={e => { setQuery(e.target.value); setResult(null); }}
            onKeyDown={e => e.key === 'Enter' && inspect()}
            placeholder="Select or type a topic name…"
            style={{
              width: '100%', padding: '8px 10px', borderRadius: 7, boxSizing: 'border-box',
              background: 'var(--surface)', border: '1px solid var(--border)',
              color: 'var(--text)', fontFamily: 'var(--mono)', fontSize: '0.75rem', outline: 'none',
            }}
          />
          <datalist id="hw-topics-list">
            {allKnown.map(t => <option key={t} value={t} />)}
          </datalist>
        </div>

        <button
          onClick={inspect}
          disabled={loading || !query.trim()}
          style={{
            display: 'inline-flex', alignItems: 'center', justifyContent: 'center', gap: 6,
            background: query.trim() ? 'var(--accent2)' : 'rgba(0,153,255,0.2)',
            color: 'white', border: 'none', borderRadius: 7, padding: '9px 18px',
            cursor: query.trim() ? 'pointer' : 'default',
            fontFamily: 'var(--mono)', fontSize: '0.75rem', fontWeight: 700,
          }}
        >
          {loading ? '⏳ Fetching...' : '▶ INSPECT TOPIC'}
        </button>

        {result && (
          <div style={{
            background: 'var(--surface)', border: '1px solid var(--border)',
            borderRadius: 10, overflow: 'hidden',
          }}>
            <div style={{
              display: 'flex', alignItems: 'center', justifyContent: 'space-between',
              padding: '10px 14px', borderBottom: '1px solid var(--border)',
              background: 'rgba(0,153,255,0.07)',
            }}>
              <span style={{ fontFamily: 'var(--mono)', fontSize: '0.78rem', fontWeight: 700, color: 'var(--accent2)' }}>
                Topic: {result.topic}
              </span>
              <span style={{ cursor: 'pointer', color: 'var(--muted)' }} onClick={() => setResult(null)}>✕</span>
            </div>
            <div style={{ padding: '12px 14px' }}>
              <TopicDetail data={result} />
            </div>
          </div>
        )}
      </div>
    </Modal>
  );
}

/* ── Inline expand panels — styled wrappers ───────────────────── */
function ExpandableSection({ icon, label, count, open, onToggle, children }) {
  return (
    <div style={{ margin: '0 16px 10px', borderRadius: 10, overflow: 'hidden', border: '1px solid rgba(0,153,255,0.15)', background: 'rgba(0,153,255,0.03)' }}>
      <div
        onClick={onToggle}
        style={{
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '10px 16px', cursor: 'pointer',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '1rem' }}>{icon}</span>
          <span style={{ fontSize: '0.78rem', fontWeight: 600, color: 'var(--text)' }}>{label}</span>
          {count != null && (
            <span style={{
              fontFamily: 'var(--mono)', fontSize: '0.62rem', color: 'var(--accent2)',
              background: 'rgba(0,153,255,0.12)', border: '1px solid rgba(0,153,255,0.3)',
              borderRadius: 10, padding: '1px 7px',
            }}>{count}</span>
          )}
        </div>
        <span style={{
          color: 'var(--muted)', fontSize: '0.6rem', transition: 'transform 0.25s',
          transform: open ? 'rotate(180deg)' : 'none',
        }}>▼</span>
      </div>
      {open && (
        <div style={{ borderTop: '1px solid rgba(0,153,255,0.1)', animation: 'fadeIn 0.2s ease' }}>
          {children}
        </div>
      )}
    </div>
  );
}

/* ── Main Kafka Panel ──────────────────────────────────────────── */
export default function KafkaPanel({ checks, fetchTopic, diagModalOpen, onDiagClose }) {
  const [livePanelOpen, setLivePanelOpen] = useState(false);
  const [lagPanelOpen, setLagPanelOpen] = useState(false);
  const [topicModal, setTopicModal] = useState({ open: false, name: '', data: null });

  const details = checks.__details__;
  const connectors = checks['Kafka Connectors']?._connectors;
  const zkStats = checks['Zookeeper Stats']?._zk_stats;
  const tls = details?.topic_live_status || {};
  const lagMap = details?.consumer_lag;

  const live = Object.values(tls).filter(t => t.is_live).length;
  const stale = Object.values(tls).filter(t => t.has_data && !t.is_live).length;
  const empty = Object.values(tls).filter(t => !t.has_data).length;
  const highLag = lagMap ? Object.values(lagMap).filter(v => v.total_lag > 10000).length : 0;

  const connList = connectors?.connectors || [];
  const connProblems = connectors?.problems?.length || 0;

  function openTopicModal(name) {
    setTopicModal({ open: true, name, data: null });
    fetchTopic(name).then(d => setTopicModal(p => ({ ...p, data: d })));
  }

  return (
    <>
      {/* KPI Status Strip */}
      <StatusStrip checks={checks} />

      {/* Topic & Lag Section */}
      <GroupDivider label="Topics & Data Flow" />

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, padding: '0 16px 10px' }}>
        {/* Live Data summary card */}
        <div
          onClick={() => setLivePanelOpen(o => !o)}
          style={{
            borderRadius: 10, padding: '14px 16px', cursor: 'pointer',
            background: 'linear-gradient(150deg, rgba(0,153,255,0.08), rgba(0,153,255,0.01))',
            border: '1px solid rgba(0,153,255,0.18)',
            borderLeft: '3px solid rgba(0,153,255,0.5)',
            transition: 'box-shadow 0.2s',
          }}
          onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 16px rgba(0,153,255,0.1)'}
          onMouseLeave={e => e.currentTarget.style.boxShadow = 'none'}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ fontSize: '1.1rem' }}>📊</span>
              <span style={{ fontSize: '0.8rem', fontWeight: 600 }}>Live Data Status</span>
            </div>
            <span style={{ color: 'var(--muted)', fontSize: '0.6rem', transform: livePanelOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.25s' }}>▼</span>
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            <Chip n={live} color="16,185,129" label="live" />
            <Chip n={stale} color="245,158,11" label="stale" />
            <Chip n={empty} color="107,114,128" label="empty" />
          </div>
        </div>

        {/* Consumer Lag summary card */}
        <div
          onClick={() => setLagPanelOpen(o => !o)}
          style={{
            borderRadius: 10, padding: '14px 16px', cursor: 'pointer',
            background: highLag > 0
              ? 'linear-gradient(150deg, rgba(245,158,11,0.08), rgba(245,158,11,0.01))'
              : 'linear-gradient(150deg, rgba(16,185,129,0.08), rgba(16,185,129,0.01))',
            border: highLag > 0
              ? '1px solid rgba(245,158,11,0.18)'
              : '1px solid rgba(16,185,129,0.18)',
            borderLeft: highLag > 0
              ? '3px solid rgba(245,158,11,0.5)'
              : '3px solid rgba(16,185,129,0.5)',
            transition: 'box-shadow 0.2s',
          }}
          onMouseEnter={e => e.currentTarget.style.boxShadow = `0 2px 16px rgba(${highLag > 0 ? '245,158,11' : '16,185,129'},0.1)`}
          onMouseLeave={e => e.currentTarget.style.boxShadow = 'none'}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ fontSize: '1.1rem' }}>📈</span>
              <span style={{ fontSize: '0.8rem', fontWeight: 600 }}>Consumer Lag</span>
            </div>
            <span style={{ color: 'var(--muted)', fontSize: '0.6rem', transform: lagPanelOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.25s' }}>▼</span>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            {highLag > 0
              ? <Chip n={highLag} color="245,158,11" label="high lag" />
              : <span style={{ fontFamily: 'var(--mono)', fontSize: '0.68rem', color: 'var(--ok)' }}>All normal</span>}
          </div>
        </div>
      </div>

      {/* Expanded Live Data panel */}
      {livePanelOpen && (
        <div style={{ margin: '0 16px 10px', borderRadius: 10, overflow: 'hidden', border: '1px solid rgba(0,153,255,0.15)' }}>
          <LiveDataPanel details={details} onInspect={openTopicModal} />
        </div>
      )}

      {/* Expanded Consumer Lag panel */}
      {lagPanelOpen && (
        <div style={{ margin: '0 16px 10px', borderRadius: 10, overflow: 'hidden', border: '1px solid rgba(245,158,11,0.15)' }}>
          <ConsumerLagPanel details={details} onInspect={openTopicModal} />
        </div>
      )}

      {/* Connectors Section */}
      <GroupDivider label="Connectors" />
      <div style={{ padding: '0 4px 6px' }}>
        {connList.length > 0 ? (
          <>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '0 16px 8px' }}>
              <span style={{ fontFamily: 'var(--mono)', fontSize: '0.68rem', color: 'var(--muted)' }}>
                {connList.length} connector(s)
              </span>
              {connProblems > 0 && <Chip n={connProblems} color="239,68,68" label="problem" />}
              <Badge status={connectors?.status} size="sm" />
            </div>
            {connList.map(conn => <ConnectorCard key={conn.name} conn={conn} />)}
          </>
        ) : (
          <div style={{ padding: '12px 20px', fontFamily: 'var(--mono)', fontSize: '0.72rem', color: 'var(--muted)' }}>
            No connectors found
          </div>
        )}
      </div>

      {/* Zookeeper Section */}
      <GroupDivider label="Zookeeper" />
      <ZkStatsCard zkStats={zkStats} />

      {/* Topic Diagnosis modal */}
      <TopicDiagModal
        open={!!diagModalOpen}
        onClose={onDiagClose}
        tls={tls}
        fetchTopic={fetchTopic}
      />

      {/* Topic detail modal (from inspect links) */}
      <Modal open={topicModal.open} onClose={() => setTopicModal(p => ({ ...p, open: false }))}
        title={`Topic: ${topicModal.name}`}>
        {topicModal.data
          ? <TopicDetail data={topicModal.data} />
          : <div style={{ fontFamily: 'var(--mono)', fontSize: '0.72rem', color: 'var(--muted)' }}>⏳ Loading...</div>
        }
      </Modal>
    </>
  );
}
