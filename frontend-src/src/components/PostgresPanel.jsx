import React, { useState } from 'react';
import { Badge, Chip } from './Shared';

/* ── Status strip ────────────────────────────────────────────────── */
function StatusStrip({ checks }) {
  const items = Object.entries(checks)
    .filter(([k, v]) => !k.startsWith('__') && v?.status)
    .map(([name, check]) => ({ name, ...check }));

  if (!items.length) return null;
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

/* ── WAL Size Bar ────────────────────────────────────────────────── */
function WALBar({ details }) {
  if (!details || !details.wal_mb) return null;
  const { wal_mb, wal_warn_mb, wal_crit_mb, total_mb } = details;
  const maxBar = Math.max(wal_crit_mb * 1.5, wal_mb * 1.2, 200);
  const walPct = Math.min(100, (wal_mb / maxBar) * 100);
  const warnPct = (wal_warn_mb / maxBar) * 100;
  const critPct = (wal_crit_mb / maxBar) * 100;
  const color = wal_mb >= wal_crit_mb ? 'var(--error)' : wal_mb >= wal_warn_mb ? 'var(--warn)' : 'var(--ok)';

  return (
    <div style={{ margin: '0 20px 16px', background: 'var(--surface2)', border: '1px solid var(--border)',
                  borderRadius: 10, padding: '14px 16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '0.9rem' }}>📊</span>
          <span style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--text)' }}>pg_wal Size</span>
        </div>
        <span style={{ fontSize: '1rem', fontWeight: 700, fontFamily: 'var(--mono)', color }}>
          {wal_mb}MB
        </span>
      </div>
      {/* Bar with warn/crit markers */}
      <div style={{ position: 'relative', height: 16, background: 'var(--surface3)', borderRadius: 8, overflow: 'hidden' }}>
        <div style={{ height: '100%', width: `${walPct}%`, background: color, borderRadius: 8,
                      transition: 'width 0.5s ease' }} />
        {/* Warn marker */}
        <div style={{ position: 'absolute', top: 0, bottom: 0, left: `${warnPct}%`, width: 2,
                      background: 'var(--warn)', opacity: 0.6 }} />
        {/* Crit marker */}
        <div style={{ position: 'absolute', top: 0, bottom: 0, left: `${critPct}%`, width: 2,
                      background: 'var(--error)', opacity: 0.6 }} />
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 6,
                    fontSize: '0.55rem', fontFamily: 'var(--mono)', color: 'var(--muted)' }}>
        <span>0</span>
        <span style={{ color: 'var(--warn)' }}>warn: {wal_warn_mb}MB</span>
        <span style={{ color: 'var(--error)' }}>crit: {wal_crit_mb}MB</span>
      </div>
      <div style={{ marginTop: 8, fontSize: '0.62rem', fontFamily: 'var(--mono)', color: 'var(--muted)' }}>
        PGDATA total: {total_mb}MB · pg_wal: {wal_mb}MB ({total_mb > 0 ? Math.round(wal_mb / total_mb * 100) : 0}% of total)
      </div>
    </div>
  );
}

/* ── Database Sizes ──────────────────────────────────────────────── */
function DatabaseSizes({ dbSizes }) {
  const [open, setOpen] = useState(true);
  if (!dbSizes || dbSizes.length === 0) return null;
  const maxSize = Math.max(...dbSizes.map(d => d.size_bytes || 0), 1);

  return (
    <div style={{ margin: '0 20px 16px', background: 'var(--surface2)', border: '1px solid var(--border)',
                  borderRadius: 10, overflow: 'hidden' }}>
      <div onClick={() => setOpen(o => !o)} style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '10px 16px', cursor: 'pointer', background: 'rgba(0,212,170,0.03)',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '0.9rem' }}>🗄️</span>
          <span style={{ fontSize: '0.78rem', fontWeight: 600 }}>Database Sizes</span>
          <Chip n={dbSizes.length} color="100,116,139" label="dbs" />
        </div>
        <span style={{ color: 'var(--muted)', fontSize: '0.6rem', transition: 'transform 0.25s',
                       transform: open ? 'rotate(180deg)' : 'none' }}>▼</span>
      </div>
      {open && (
        <div style={{ padding: '8px 16px 12px' }}>
          {dbSizes.map((db, i) => {
            const pct = (db.size_bytes / maxSize) * 100;
            return (
              <div key={db.name} style={{ marginBottom: 8 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                  <span style={{ fontSize: '0.68rem', fontFamily: 'var(--mono)', fontWeight: 600, color: 'var(--text)' }}>
                    {db.name}
                  </span>
                  <span style={{ fontSize: '0.65rem', fontFamily: 'var(--mono)', color: 'var(--accent2)', fontWeight: 700 }}>
                    {db.size}
                  </span>
                </div>
                <div style={{ height: 6, background: 'var(--surface3)', borderRadius: 3, overflow: 'hidden' }}>
                  <div style={{ height: '100%', width: `${pct}%`, borderRadius: 3,
                                background: 'linear-gradient(90deg, var(--accent), var(--accent2))',
                                transition: 'width 0.4s ease' }} />
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

/* ── Replication Slots ───────────────────────────────────────────── */
function ReplicationSlots({ slots }) {
  if (!slots || slots.length === 0) return null;

  return (
    <div style={{ margin: '0 20px 16px', background: 'var(--surface2)', border: '1px solid var(--border)',
                  borderRadius: 10, padding: '12px 16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
        <span style={{ fontSize: '0.9rem' }}>🔄</span>
        <span style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--text)' }}>Replication Slots</span>
      </div>
      {slots.map(slot => {
        const isActive = slot.active;
        const color = isActive ? 'var(--ok)' : 'var(--error)';
        return (
          <div key={slot.name} style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            padding: '6px 10px', borderRadius: 6, marginBottom: 4,
            background: isActive ? 'rgba(16,185,129,0.06)' : 'rgba(239,68,68,0.06)',
            border: `1px solid ${isActive ? 'rgba(16,185,129,0.18)' : 'rgba(239,68,68,0.18)'}`,
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ fontSize: '0.65rem' }}>{isActive ? '🟢' : '🔴'}</span>
              <span style={{ fontSize: '0.68rem', fontFamily: 'var(--mono)', fontWeight: 600, color: 'var(--text)' }}>
                {slot.name}
              </span>
              <span style={{ fontSize: '0.55rem', fontFamily: 'var(--mono)', color: 'var(--muted)',
                             background: 'var(--surface3)', borderRadius: 3, padding: '1px 5px' }}>
                {slot.type}
              </span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              {!isActive && (
                <span style={{ fontSize: '0.55rem', fontWeight: 700, fontFamily: 'var(--mono)',
                               color: 'var(--error)', background: 'rgba(239,68,68,0.1)',
                               padding: '1px 5px', borderRadius: 3 }}>INACTIVE</span>
              )}
              <span style={{ fontSize: '0.62rem', fontFamily: 'var(--mono)', color, fontWeight: 700 }}>
                {slot.lag_mb}MB lag
              </span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

/* ── Main PostgreSQL Panel ───────────────────────────────────────── */
export default function PostgresPanel({ checks }) {
  const details = checks.__pg_details__ || {};

  return (
    <>
      <StatusStrip checks={checks} />
      <WALBar details={details} />
      <DatabaseSizes dbSizes={details.db_sizes} />
      <ReplicationSlots slots={details.repl_slots} />
    </>
  );
}
