import React from 'react';
import { Badge, Tip } from './Shared';
import { fmt } from '../utils';

/* ── Status strip ────────────────────────────────────────────────── */
function StatusStrip({ checks }) {
  const items = Object.entries(checks)
    .filter(([k, v]) => !k.startsWith('__') && v?.status)
    .map(([name, check]) => ({ name, ...check }));

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

/* ── Size bar ────────────────────────────────────────────────────── */
function SizeBar({ bytes, maxBytes }) {
  const pct = maxBytes > 0 ? (bytes / maxBytes) * 100 : 0;
  return (
    <div style={{ height: 6, background: 'var(--surface3)', borderRadius: 999, overflow: 'hidden', width: '100%' }}>
      <div style={{
        height: '100%', width: `${pct}%`, borderRadius: 999, transition: 'width 0.6s ease',
        background: 'linear-gradient(90deg, var(--accent), var(--accent2))',
      }} />
    </div>
  );
}

/* ── Change badge styles ──────────────────────────────────────────── */
const CHANGE_STYLES = {
  added:    { icon: '＋', bg: 'rgba(16,185,129,0.12)', color: 'var(--ok)',   border: 'rgba(16,185,129,0.3)' },
  deleted:  { icon: '−',  bg: 'rgba(239,68,68,0.12)',  color: 'var(--error)', border: 'rgba(239,68,68,0.3)' },
  modified: { icon: '~',  bg: 'rgba(245,158,11,0.12)', color: 'var(--warn)',  border: 'rgba(245,158,11,0.3)' },
};

/* ── Bucket Card ─────────────────────────────────────────────────── */
function BucketCard({ bucket, maxBytes }) {
  const b = bucket;
  const changes = b.changes || [];
  const modifiedColor = b.recently_modified ? 'var(--warn)' : 'var(--muted)';

  return (
    <div style={{
      background: 'var(--surface2)',
      border: '1px solid var(--border)',
      borderRadius: 10,
      padding: '14px 16px',
      display: 'flex', flexDirection: 'column', gap: 10,
      transition: 'border-color 0.2s, background 0.2s',
    }}
      onMouseEnter={e => { e.currentTarget.style.borderColor = 'rgba(0,212,170,0.3)'; e.currentTarget.style.background = 'var(--surface3)'; }}
      onMouseLeave={e => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.background = 'var(--surface2)'; }}
    >
      {/* Header: name + status badge */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '1rem' }}>🪣</span>
          <span style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--text)', fontFamily: 'var(--mono)' }}>
            {b.name}
          </span>
        </div>
        {b.recently_modified ? (
          <span style={{
            padding: '2px 8px', borderRadius: 4, fontSize: '0.58rem', fontWeight: 700,
            background: 'rgba(245,158,11,0.15)', color: 'var(--warn)',
            border: '1px solid rgba(245,158,11,0.3)', letterSpacing: '0.5px',
          }}>MODIFIED</span>
        ) : (
          <span style={{
            padding: '2px 8px', borderRadius: 4, fontSize: '0.58rem', fontWeight: 600,
            background: 'rgba(16,185,129,0.1)', color: 'var(--ok)', letterSpacing: '0.5px',
          }}>OK</span>
        )}
      </div>

      {/* Size bar */}
      <SizeBar bytes={b.total_size} maxBytes={maxBytes} />

      {/* Stats row */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <span style={{ fontSize: '0.58rem', color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.8px', fontWeight: 600 }}>Size</span>
          <span style={{ fontSize: '0.82rem', fontWeight: 700, color: 'var(--accent)', fontFamily: 'var(--mono)' }}>
            {b.total_size_human}
          </span>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <span style={{ fontSize: '0.58rem', color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.8px', fontWeight: 600 }}>Objects</span>
          <span style={{ fontSize: '0.82rem', fontWeight: 700, color: 'var(--text)', fontFamily: 'var(--mono)' }}>
            {fmt(b.object_count)}
          </span>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2, marginLeft: 'auto' }}>
          <span style={{ fontSize: '0.58rem', color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.8px', fontWeight: 600 }}>Last Modified</span>
          <span style={{ fontSize: '0.75rem', fontWeight: b.recently_modified ? 700 : 400, color: modifiedColor, fontFamily: 'var(--mono)' }}>
            {b.last_modified_ago || 'N/A'}
          </span>
        </div>
      </div>

      {/* Change indicators — shown when objects are added/deleted/modified since last check */}
      {changes.length > 0 && (
        <div style={{
          borderTop: '1px solid var(--border)', paddingTop: 8,
          display: 'flex', flexDirection: 'column', gap: 5,
        }}>
          <span style={{ fontSize: '0.56rem', color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.8px', fontWeight: 600 }}>
            Changes since last check
          </span>
          {changes.map((c, i) => {
            const s = CHANGE_STYLES[c.type] || CHANGE_STYLES.modified;
            return (
              <div key={i} style={{
                display: 'flex', alignItems: 'center', gap: 8,
                padding: '4px 8px', borderRadius: 6,
                background: s.bg, border: `1px solid ${s.border}`,
              }}>
                <span style={{ fontSize: '0.75rem', fontWeight: 700, color: s.color, fontFamily: 'var(--mono)', lineHeight: 1 }}>{s.icon}</span>
                <span style={{ fontSize: '0.68rem', fontWeight: 600, color: s.color, fontFamily: 'var(--mono)' }}>{c.label}</span>
                {c.detail && (
                  <span style={{ fontSize: '0.6rem', color: 'var(--muted)', fontFamily: 'var(--mono)', marginLeft: 'auto' }}>{c.detail}</span>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

/* ── Main MinIO Panel ────────────────────────────────────────────── */
export default function MinIOPanel({ checks }) {
  const buckets = checks.__minio_buckets__ || [];
  const maxBytes = Math.max(...buckets.map(b => b.total_size || 0), 1);

  return (
    <>
      <StatusStrip checks={checks} />

      {buckets.length > 0 && (
        <>
          <div style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            padding: '8px 20px 4px', borderTop: '1px solid var(--border)',
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ fontSize: '0.82rem', fontWeight: 600, color: 'var(--text)' }}>
                📦 Bucket Details
              </span>
              <Tip text="All MinIO buckets with disk usage, object count, and last modification time. Buckets modified in the last 24h are flagged as MODIFIED." />
            </div>
            <span style={{
              fontSize: '0.68rem', fontFamily: 'var(--mono)', color: 'var(--muted)',
            }}>{buckets.length} bucket(s)</span>
          </div>
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
            gap: 10,
            padding: '10px 20px 16px',
          }}>
            {buckets.map(b => (
              <BucketCard key={b.name} bucket={b} maxBytes={maxBytes} />
            ))}
          </div>
        </>
      )}
    </>
  );
}
