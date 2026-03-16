import React from 'react';
import { SubSection, Badge } from './Shared';

/* ── Server-state chip (leader / follower / standalone) ─────────── */
const STATE_COLORS = {
  leader:     { color:'var(--accent)',  bg:'rgba(0,212,170,0.1)',   border:'rgba(0,212,170,0.3)'   },
  follower:   { color:'var(--accent2)', bg:'rgba(0,153,255,0.1)',   border:'rgba(0,153,255,0.3)'   },
  standalone: { color:'var(--muted)',   bg:'rgba(255,255,255,0.05)',border:'rgba(255,255,255,0.1)' },
  observer:   { color:'var(--warn)',    bg:'rgba(245,158,11,0.1)',  border:'rgba(245,158,11,0.3)'  },
};

function StateChip({ state }) {
  const s = STATE_COLORS[(state || '').toLowerCase()] || STATE_COLORS.standalone;
  return (
    <span style={{
      fontFamily:'var(--mono)', fontSize:'0.65rem', fontWeight:700,
      color:s.color, background:s.bg, border:`1px solid ${s.border}`,
      borderRadius:4, padding:'2px 8px', letterSpacing:'0.5px',
    }}>{(state || 'UNKNOWN').toUpperCase()}</span>
  );
}

/* ── Individual metric row ──────────────────────────────────────── */
function MetricRow({ label, value, status, threshold }) {
  const textColor = status === 'warn'  ? 'var(--warn)'
                  : status === 'error' ? 'var(--error)'
                  : status === 'ok'    ? 'var(--ok)'
                  : 'var(--muted)';
  return (
    <div style={{
      display:'flex', alignItems:'center', justifyContent:'space-between',
      padding:'8px 22px 8px 38px', borderBottom:'1px solid rgba(30,45,69,0.4)',
    }}
      onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
      onMouseLeave={e => e.currentTarget.style.background='transparent'}
    >
      <span style={{ fontSize:'0.78rem', color:'var(--text)' }}>{label}</span>
      <div style={{ display:'flex', alignItems:'center', gap:10 }}>
        {threshold != null && (
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.62rem', color:'var(--muted)' }}>
            warn &gt; {threshold}
          </span>
        )}
        <span style={{
          fontFamily:'var(--mono)', fontSize:'0.75rem',
          color: textColor,
          fontWeight: status && status !== 'ok' ? 700 : 400,
        }}>{value}</span>
      </div>
    </div>
  );
}

/* ── Main panel ─────────────────────────────────────────────────── */
export default function ZookeeperStatsPanel({ zkStats }) {
  if (!zkStats) return null;

  const {
    ruok, server_state,
    avg_latency_ms, max_latency_ms,
    outstanding_requests, outstanding_warn_threshold,
    alive_connections, znode_count, watch_count,
    open_fds, max_fds, uptime_hours,
    status,
  } = zkStats;

  const outstandingStatus = outstanding_requests > outstanding_warn_threshold ? 'warn' : 'ok';
  const fdPct   = max_fds > 0 ? open_fds / max_fds : 0;
  const fdStatus = fdPct > 0.8 ? 'warn' : 'ok';

  return (
    <SubSection
      icon="🦓"
      title="Zookeeper Stats"
      defaultOpen={false}
      badge={<Badge status={status} size="sm" />}
    >
      {/* ── ruok + server state header ── */}
      <div style={{
        display:'flex', alignItems:'center', gap:12, flexWrap:'wrap',
        padding:'10px 22px', borderBottom:'1px solid rgba(30,45,69,0.4)',
      }}>
        <span style={{
          fontFamily:'var(--mono)', fontSize:'0.65rem', fontWeight:700,
          color:  ruok ? 'var(--ok)'  : 'var(--error)',
          background: ruok ? 'rgba(16,185,129,0.1)' : 'rgba(239,68,68,0.1)',
          border: ruok ? '1px solid rgba(16,185,129,0.3)' : '1px solid rgba(239,68,68,0.3)',
          borderRadius:4, padding:'2px 8px',
        }}>
          ruok: {ruok ? 'imok ✓' : 'FAILED ✗'}
        </span>

        <StateChip state={server_state} />

        {uptime_hours != null && (
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.65rem', color:'var(--muted)' }}>
            up {uptime_hours}h
          </span>
        )}
      </div>

      {/* ── Latency ── */}
      <MetricRow label="Avg Latency"  value={`${avg_latency_ms} ms`} />
      <MetricRow label="Max Latency"  value={`${max_latency_ms} ms`} />

      {/* ── Outstanding requests — warn if above threshold ── */}
      <MetricRow
        label="Outstanding Requests"
        value={outstanding_requests}
        status={outstandingStatus}
        threshold={outstanding_warn_threshold}
      />

      {/* ── Connections & structure ── */}
      <MetricRow label="Alive Connections" value={alive_connections} />
      <MetricRow label="Znode Count"       value={znode_count} />
      <MetricRow label="Watch Count"       value={watch_count} />

      {/* ── File descriptors ── */}
      <MetricRow
        label="Open File Descriptors"
        value={max_fds > 0 ? `${open_fds} / ${max_fds}` : open_fds}
        status={fdStatus}
      />
    </SubSection>
  );
}
