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

/* ── PVC Disk + WAL breakdown ─────────────────────────────────────── */
function DiskBar({ details }) {
  if (!details) return null;
  const { wal_mb, total_mb, pvc_total_gb, pvc_used_gb, pvc_avail_gb, pvc_used_pct } = details;
  if (!pvc_total_gb && !wal_mb) return null;

  const pvcTotalMB = pvc_total_gb * 1024;
  const walPct = pvcTotalMB > 0 ? Math.min(100, (wal_mb / pvcTotalMB) * 100) : 0;
  const dataPct = pvcTotalMB > 0 ? Math.min(100, ((total_mb - wal_mb) / pvcTotalMB) * 100) : 0;
  const usedPct = pvc_used_pct || 0;
  const diskColor = usedPct >= 90 ? 'var(--error)' : usedPct >= 80 ? 'var(--warn)' : 'var(--ok)';
  const walColor = wal_mb > 200 ? 'var(--error)' : wal_mb > 100 ? 'var(--warn)' : 'var(--accent2)';

  return (
    <div style={{ margin: '0 20px 16px', background: 'var(--surface2)', border: '1px solid var(--border)',
                  borderRadius: 10, padding: '14px 16px' }}>
      {/* PVC Usage */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '0.9rem' }}>💾</span>
          <span style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--text)' }}>PVC Disk Usage</span>
        </div>
        <span style={{ fontSize: '0.9rem', fontWeight: 700, fontFamily: 'var(--mono)', color: diskColor }}>
          {pvc_used_gb}GB / {pvc_total_gb}GB
        </span>
      </div>
      <div style={{ position: 'relative', height: 20, background: 'var(--surface3)', borderRadius: 8, overflow: 'hidden' }}>
        {/* Data portion */}
        <div style={{ position: 'absolute', left: 0, top: 0, bottom: 0, width: `${dataPct}%`,
                      background: 'var(--accent)', opacity: 0.7, transition: 'width 0.5s ease' }} />
        {/* WAL portion (stacked after data) */}
        <div style={{ position: 'absolute', left: `${dataPct}%`, top: 0, bottom: 0, width: `${walPct}%`,
                      background: walColor, opacity: 0.8, transition: 'all 0.5s ease' }} />
        {/* 80% warn line */}
        <div style={{ position: 'absolute', top: 0, bottom: 0, left: '80%', width: 2,
                      background: 'var(--warn)', opacity: 0.5 }} />
        {/* 90% crit line */}
        <div style={{ position: 'absolute', top: 0, bottom: 0, left: '90%', width: 2,
                      background: 'var(--error)', opacity: 0.5 }} />
        {/* Percentage label */}
        <span style={{ position: 'absolute', right: 8, top: 2, fontSize: '0.6rem', fontFamily: 'var(--mono)',
                       fontWeight: 700, color: 'var(--text)' }}>{usedPct}%</span>
      </div>
      {/* Legend */}
      <div style={{ display: 'flex', gap: 16, marginTop: 8, fontSize: '0.58rem', fontFamily: 'var(--mono)', color: 'var(--muted)' }}>
        <span><span style={{ display: 'inline-block', width: 8, height: 8, borderRadius: 2,
                             background: 'var(--accent)', opacity: 0.7, marginRight: 4, verticalAlign: 'middle' }} />
          Data: {total_mb - wal_mb}MB</span>
        <span><span style={{ display: 'inline-block', width: 8, height: 8, borderRadius: 2,
                             background: walColor, opacity: 0.8, marginRight: 4, verticalAlign: 'middle' }} />
          pg_wal: {wal_mb}MB</span>
        <span>Free: {pvc_avail_gb}GB</span>
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
      <DiskBar details={details} />
      <DatabaseSizes dbSizes={details.db_sizes} />
    </>
  );
}
