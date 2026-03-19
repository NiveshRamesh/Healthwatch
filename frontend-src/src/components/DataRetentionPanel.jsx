import React from 'react';
import { Badge, Tip } from './Shared';
import { fmt } from '../utils';

/* ── Status strip (Retention Policy + Data Age Check) ────────────── */
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

/* ── Age bar — visual indicator of data age vs retention limit ───── */
function AgeBar({ daysDiff, retentionDays }) {
  const pct = retentionDays > 0 ? Math.min((daysDiff / retentionDays) * 100, 100) : 0;
  const overLimit = daysDiff > retentionDays;
  return (
    <div style={{ height: 6, background: 'var(--surface3)', borderRadius: 999, overflow: 'hidden', width: '100%' }}>
      <div style={{
        height: '100%', width: `${pct}%`, borderRadius: 999, transition: 'width 0.6s ease',
        background: overLimit
          ? 'linear-gradient(90deg, rgba(245,158,11,0.8), rgba(239,68,68,0.9))'
          : 'linear-gradient(90deg, var(--accent), var(--accent2))',
      }} />
    </div>
  );
}

/* ── Table retention card ────────────────────────────────────────── */
function RetentionCard({ table }) {
  const t = table;
  const overLimit = t.days_diff > t.retention_days;
  const overBy = t.days_diff - t.retention_days;

  return (
    <div style={{
      background: 'var(--surface2)',
      border: overLimit ? '1px solid rgba(245,158,11,0.35)' : '1px solid var(--border)',
      borderRadius: 10,
      padding: '14px 16px',
      display: 'flex', flexDirection: 'column', gap: 10,
      transition: 'border-color 0.2s, background 0.2s',
    }}
      onMouseEnter={e => { e.currentTarget.style.borderColor = 'rgba(0,212,170,0.3)'; e.currentTarget.style.background = 'var(--surface3)'; }}
      onMouseLeave={e => { e.currentTarget.style.borderColor = overLimit ? 'rgba(245,158,11,0.35)' : 'var(--border)'; e.currentTarget.style.background = 'var(--surface2)'; }}
    >
      {/* Header: table name + status */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <span style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--text)', fontFamily: 'var(--mono)',
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1,
        }} title={t.table}>
          {t.table}
        </span>
        {overLimit ? (
          <span style={{
            padding: '2px 8px', borderRadius: 4, fontSize: '0.56rem', fontWeight: 700,
            background: 'rgba(245,158,11,0.15)', color: 'var(--warn)',
            border: '1px solid rgba(245,158,11,0.3)', letterSpacing: '0.5px', flexShrink: 0, marginLeft: 8,
          }}>EXCEEDED +{overBy}d</span>
        ) : (
          <span style={{
            padding: '2px 8px', borderRadius: 4, fontSize: '0.56rem', fontWeight: 600,
            background: 'rgba(16,185,129,0.1)', color: 'var(--ok)', letterSpacing: '0.5px', flexShrink: 0, marginLeft: 8,
          }}>OK</span>
        )}
      </div>

      {/* Age bar */}
      <AgeBar daysDiff={t.days_diff} retentionDays={t.retention_days} />

      {/* Stats row */}
      <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <span style={{ fontSize: '0.56rem', color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.8px', fontWeight: 600 }}>Data Span</span>
          <span style={{ fontSize: '0.82rem', fontWeight: 700, color: overLimit ? 'var(--warn)' : 'var(--accent)', fontFamily: 'var(--mono)' }}>
            {t.days_diff}d
          </span>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <span style={{ fontSize: '0.56rem', color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.8px', fontWeight: 600 }}>Retention</span>
          <span style={{ fontSize: '0.82rem', fontWeight: 700, color: 'var(--text)', fontFamily: 'var(--mono)' }}>
            {t.retention_days}d
          </span>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2, marginLeft: 'auto', textAlign: 'right' }}>
          <span style={{ fontSize: '0.56rem', color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.8px', fontWeight: 600 }}>Oldest Data</span>
          <span style={{ fontSize: '0.68rem', fontWeight: 400, color: 'var(--muted)', fontFamily: 'var(--mono)' }}>
            {t.min_timestamp ? t.min_timestamp.split('.')[0] : 'N/A'}
          </span>
        </div>
      </div>
    </div>
  );
}

/* ── Main Data Retention Panel ───────────────────────────────────── */
export default function DataRetentionPanel({ checks }) {
  const tables = checks.__retention_tables__ || [];
  const meta = checks.__retention_meta__ || {};
  const exceeded = tables.filter(t => t.status === 'warn');
  const healthy = tables.filter(t => t.status === 'ok');

  return (
    <>
      <StatusStrip checks={checks} />

      {tables.length > 0 && (
        <>
          {/* Exceeded tables section */}
          {exceeded.length > 0 && (
            <>
              <div style={{
                display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                padding: '8px 20px 4px', borderTop: '1px solid var(--border)',
              }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ fontSize: '0.82rem', fontWeight: 600, color: 'var(--warn)' }}>
                    Exceeded Retention
                  </span>
                  <Tip text="These tables contain data older than the configured retention period. The backend partition movement to MinIO cold storage may have failed or not run." />
                </div>
                <span style={{
                  fontSize: '0.68rem', fontFamily: 'var(--mono)', color: 'var(--warn)', fontWeight: 600,
                }}>{exceeded.length} table(s)</span>
              </div>
              <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
                gap: 10, padding: '10px 20px 16px',
              }}>
                {exceeded.map(t => <RetentionCard key={t.table} table={t} />)}
              </div>
            </>
          )}

          {/* Healthy tables section */}
          {healthy.length > 0 && (
            <>
              <div style={{
                display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                padding: '8px 20px 4px', borderTop: '1px solid var(--border)',
              }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ fontSize: '0.82rem', fontWeight: 600, color: 'var(--text)' }}>
                    Within Retention
                  </span>
                  <Tip text="These tables have data within the configured retention window. Partition movement is working correctly." />
                </div>
                <span style={{
                  fontSize: '0.68rem', fontFamily: 'var(--mono)', color: 'var(--muted)',
                }}>{healthy.length} table(s)</span>
              </div>
              <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
                gap: 10, padding: '10px 20px 16px',
              }}>
                {healthy.map(t => <RetentionCard key={t.table} table={t} />)}
              </div>
            </>
          )}
        </>
      )}

      {tables.length === 0 && (
        <div style={{ textAlign: 'center', padding: '40px 20px', fontFamily: 'var(--mono)', color: 'var(--muted)', fontSize: '0.78rem' }}>
          No _data tables found or check hasn't run yet.
        </div>
      )}
    </>
  );
}
