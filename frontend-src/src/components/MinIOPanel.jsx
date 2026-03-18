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
    <div style={{ height: 6, background: 'var(--surface3)', borderRadius: 999, overflow: 'hidden', minWidth: 80 }}>
      <div style={{
        height: '100%', width: `${pct}%`, borderRadius: 999, transition: 'width 0.6s ease',
        background: 'linear-gradient(90deg, var(--accent), var(--accent2))',
      }} />
    </div>
  );
}

/* ── Bucket table — all buckets visible at once ──────────────────── */
function BucketTable({ buckets, maxBytes }) {
  return (
    <table style={{
      width: '100%', borderCollapse: 'separate', borderSpacing: '0 4px',
      fontSize: '0.7rem', fontFamily: 'var(--mono)',
    }}>
      <thead>
        <tr>
          {['Bucket', 'Objects', 'Size', 'Usage', 'Last Modified', 'Status'].map(h => (
            <th key={h} style={{
              textAlign: 'left', padding: '8px 10px',
              color: 'var(--accent)', fontWeight: 600, fontSize: '0.62rem',
              textTransform: 'uppercase', letterSpacing: '0.8px',
              borderBottom: '1px solid rgba(0,212,170,0.15)',
            }}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {buckets.map((b, i) => (
          <tr key={b.name} style={{
            background: i % 2 === 0 ? 'rgba(255,255,255,0.02)' : 'transparent',
          }}>
            <td style={{ padding: '8px 10px', fontWeight: 700 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{ fontSize: '0.85rem' }}>🪣</span>
                {b.name}
              </div>
            </td>
            <td style={{ padding: '8px 10px', color: 'var(--muted)' }}>{fmt(b.object_count)}</td>
            <td style={{ padding: '8px 10px', color: 'var(--accent)', fontWeight: 600 }}>{b.total_size_human}</td>
            <td style={{ padding: '8px 10px', minWidth: 100 }}>
              <SizeBar bytes={b.total_size} maxBytes={maxBytes} />
            </td>
            <td style={{
              padding: '8px 10px',
              color: b.recently_modified ? 'var(--warn)' : 'var(--muted)',
              fontWeight: b.recently_modified ? 700 : 400,
            }}>
              {b.last_modified_ago || 'N/A'}
            </td>
            <td style={{ padding: '8px 10px' }}>
              {b.recently_modified ? (
                <span style={{
                  padding: '2px 8px', borderRadius: 4, fontSize: '0.6rem', fontWeight: 700,
                  background: 'rgba(245,158,11,0.15)', color: 'var(--warn)',
                  border: '1px solid rgba(245,158,11,0.3)',
                }}>MODIFIED</span>
              ) : (
                <span style={{
                  padding: '2px 8px', borderRadius: 4, fontSize: '0.6rem', fontWeight: 600,
                  background: 'rgba(16,185,129,0.1)', color: 'var(--ok)',
                }}>OK</span>
              )}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
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
          <div style={{ padding: '4px 16px 16px', overflowX: 'auto' }}>
            <BucketTable buckets={buckets} maxBytes={maxBytes} />
          </div>
        </>
      )}
    </>
  );
}
