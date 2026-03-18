import React, { useState } from 'react';
import { Badge, Tip } from './Shared';
import { fmt } from '../utils';

/* ── Status strip — same style as ClickHouse panel ──────────────── */
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

/* ── Size bar ───────────────────────────────────────────────────── */
function SizeBar({ bytes, maxBytes }) {
  const pct = maxBytes > 0 ? (bytes / maxBytes) * 100 : 0;
  return (
    <div style={{ height: 5, background: 'var(--surface3)', borderRadius: 999, overflow: 'hidden', minWidth: 60 }}>
      <div style={{
        height: '100%', width: `${pct}%`, borderRadius: 999, transition: 'width 0.6s ease',
        background: 'linear-gradient(90deg, var(--accent), var(--accent2))',
      }} />
    </div>
  );
}

/* ── Bucket card ────────────────────────────────────────────────── */
function BucketCard({ bucket, maxBytes }) {
  const [open, setOpen] = useState(false);
  const mod = bucket.recently_modified;
  const rgb = mod ? '245,158,11' : '16,185,129';

  return (
    <div style={{
      margin: 4, borderRadius: 10, overflow: 'hidden',
      background: `linear-gradient(150deg, rgba(${rgb},0.06), rgba(${rgb},0.01))`,
      border: `1px solid rgba(${rgb},0.12)`,
      borderLeft: `3px solid rgba(${rgb},0.5)`,
    }}>
      <div
        onClick={() => setOpen(o => !o)}
        style={{ padding: '12px 14px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 12 }}
      >
        <span style={{ fontSize: '1.1rem' }}>🪣</span>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 }}>
            <span style={{ fontSize: '0.8rem', fontWeight: 700, color: 'var(--text)', fontFamily: 'var(--mono)' }}>
              {bucket.name}
            </span>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              {mod && (
                <span style={{
                  padding: '2px 8px', borderRadius: 4, fontSize: '0.6rem', fontWeight: 700,
                  background: 'rgba(245,158,11,0.15)', color: 'var(--warn)',
                  border: '1px solid rgba(245,158,11,0.3)',
                }}>MODIFIED</span>
              )}
              <span style={{
                fontSize: '0.72rem', fontWeight: 600, color: 'var(--accent)',
                fontFamily: 'var(--mono)',
              }}>{bucket.total_size_human}</span>
              <span style={{
                color: 'var(--muted)', fontSize: '0.6rem', transition: 'transform 0.25s',
                transform: open ? 'rotate(180deg)' : 'none',
              }}>▼</span>
            </div>
          </div>
          {/* Size bar */}
          <div style={{ marginTop: 6 }}>
            <SizeBar bytes={bucket.total_size} maxBytes={maxBytes} />
          </div>
        </div>
      </div>

      {open && (
        <div style={{
          borderTop: `1px solid rgba(${rgb},0.1)`, padding: '12px 16px',
          background: 'rgba(0,0,0,0.2)', animation: 'fadeIn 0.2s ease',
        }}>
          <div style={{
            display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px 24px',
            fontSize: '0.68rem', fontFamily: 'var(--mono)',
          }}>
            <div>
              <span style={{ color: 'var(--muted)' }}>Objects</span>
              <div style={{ color: 'var(--text)', fontWeight: 600, marginTop: 2 }}>{fmt(bucket.object_count)}</div>
            </div>
            <div>
              <span style={{ color: 'var(--muted)' }}>Disk Usage</span>
              <div style={{ color: 'var(--accent)', fontWeight: 600, marginTop: 2 }}>{bucket.total_size_human}</div>
            </div>
            <div>
              <span style={{ color: 'var(--muted)' }}>Last Modified</span>
              <div style={{
                color: mod ? 'var(--warn)' : 'var(--text)',
                fontWeight: mod ? 700 : 400, marginTop: 2,
              }}>
                {bucket.last_modified_ago || 'N/A'}
              </div>
            </div>
            <div>
              <span style={{ color: 'var(--muted)' }}>Created</span>
              <div style={{ color: 'var(--text)', marginTop: 2 }}>
                {bucket.created ? new Date(bucket.created).toLocaleDateString() : 'N/A'}
              </div>
            </div>
          </div>
          {bucket.error && (
            <div style={{
              marginTop: 8, padding: '6px 8px', borderRadius: 5,
              background: 'rgba(239,68,68,0.08)', color: 'var(--error)',
              fontSize: '0.62rem', fontFamily: 'var(--mono)', wordBreak: 'break-all',
            }}>{bucket.error}</div>
          )}
        </div>
      )}
    </div>
  );
}

/* ── Main MinIO Panel ───────────────────────────────────────────── */
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
                📦 Buckets
              </span>
              <Tip text="All MinIO buckets with disk usage, object count, and last modification time. Buckets modified in the last 24h are flagged." />
            </div>
            <span style={{
              fontSize: '0.68rem', fontFamily: 'var(--mono)', color: 'var(--muted)',
            }}>{buckets.length} bucket(s)</span>
          </div>
          <div style={{ padding: '0 12px 16px' }}>
            {buckets.map(b => (
              <BucketCard key={b.name} bucket={b} maxBytes={maxBytes} />
            ))}
          </div>
        </>
      )}
    </>
  );
}
