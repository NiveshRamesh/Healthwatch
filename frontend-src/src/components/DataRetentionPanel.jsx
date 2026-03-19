import React from 'react';
import { Badge, Tip } from './Shared';

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

/* ── Age bar with hot/warm segments ──────────────────────────────── */
function AgeBar({ daysDiff, hotDays, warmDays, retentionDays }) {
  const total = retentionDays || 1;
  const hotPct = Math.min((hotDays / total) * 100, 100);
  const warmPct = Math.min((warmDays / total) * 100, 100 - hotPct);
  const fillPct = Math.min((daysDiff / total) * 100, 100);
  const overLimit = daysDiff > retentionDays;

  return (
    <div style={{ position: 'relative' }}>
      {/* Background track with hot/warm zones */}
      <div style={{ height: 8, background: 'var(--surface3)', borderRadius: 999, overflow: 'hidden', width: '100%', position: 'relative' }}>
        {/* Hot zone marker */}
        <div style={{
          position: 'absolute', left: 0, top: 0, bottom: 0,
          width: `${hotPct}%`, background: 'rgba(239,147,56,0.12)',
          borderRight: '1px dashed rgba(239,147,56,0.4)',
        }} />
        {/* Warm zone marker */}
        <div style={{
          position: 'absolute', left: `${hotPct}%`, top: 0, bottom: 0,
          width: `${warmPct}%`, background: 'rgba(59,130,246,0.1)',
          borderRight: '1px dashed rgba(59,130,246,0.4)',
        }} />
        {/* Actual fill */}
        <div style={{
          height: '100%', width: `${fillPct}%`, borderRadius: 999, transition: 'width 0.6s ease',
          position: 'relative', zIndex: 1,
          background: overLimit
            ? 'linear-gradient(90deg, rgba(245,158,11,0.8), rgba(239,68,68,0.9))'
            : 'linear-gradient(90deg, var(--accent), var(--accent2))',
        }} />
      </div>
      {/* Labels */}
      <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 3 }}>
        <span style={{ fontSize: '0.48rem', color: 'rgba(239,147,56,0.7)', fontFamily: 'var(--mono)' }}>
          Hot {hotDays}d
        </span>
        <span style={{ fontSize: '0.48rem', color: 'rgba(59,130,246,0.7)', fontFamily: 'var(--mono)' }}>
          Warm {warmDays}d
        </span>
      </div>
    </div>
  );
}

/* ── Tier pill — shows data on a disk tier ───────────────────────── */
function TierPill({ label, info, color }) {
  if (!info) return (
    <span style={{
      padding: '2px 6px', borderRadius: 4, fontSize: '0.52rem', fontWeight: 600,
      fontFamily: 'var(--mono)', background: 'var(--surface3)', color: 'var(--muted)',
      border: '1px solid var(--border)',
    }}>
      {label}: empty
    </span>
  );
  return (
    <span style={{
      padding: '2px 6px', borderRadius: 4, fontSize: '0.52rem', fontWeight: 600,
      fontFamily: 'var(--mono)', background: `rgba(${color},0.1)`, color: `rgb(${color})`,
      border: `1px solid rgba(${color},0.3)`,
    }}>
      {label}: {info.parts}p · {info.size}
    </span>
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
      display: 'flex', flexDirection: 'column', gap: 8,
      transition: 'border-color 0.2s, background 0.2s',
    }}
      onMouseEnter={e => { e.currentTarget.style.borderColor = 'rgba(0,212,170,0.3)'; e.currentTarget.style.background = 'var(--surface3)'; }}
      onMouseLeave={e => { e.currentTarget.style.borderColor = overLimit ? 'rgba(245,158,11,0.35)' : 'var(--border)'; e.currentTarget.style.background = 'var(--surface2)'; }}
    >
      {/* Header: table name + status */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <span style={{ fontSize: '0.72rem', fontWeight: 700, color: 'var(--text)', fontFamily: 'var(--mono)',
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1,
        }} title={t.table}>
          {t.table}
        </span>
        {overLimit ? (
          <span style={{
            padding: '2px 8px', borderRadius: 4, fontSize: '0.54rem', fontWeight: 700,
            background: 'rgba(245,158,11,0.15)', color: 'var(--warn)',
            border: '1px solid rgba(245,158,11,0.3)', letterSpacing: '0.5px', flexShrink: 0, marginLeft: 8,
          }}>EXCEEDED +{overBy}d</span>
        ) : (
          <span style={{
            padding: '2px 8px', borderRadius: 4, fontSize: '0.54rem', fontWeight: 600,
            background: 'rgba(16,185,129,0.1)', color: 'var(--ok)', letterSpacing: '0.5px', flexShrink: 0, marginLeft: 8,
          }}>OK</span>
        )}
      </div>

      {/* Policy name badge */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <span style={{
          padding: '2px 7px', borderRadius: 4, fontSize: '0.52rem', fontWeight: 600,
          fontFamily: 'var(--mono)', background: 'rgba(139,92,246,0.12)', color: 'rgb(139,92,246)',
          border: '1px solid rgba(139,92,246,0.3)', letterSpacing: '0.3px',
        }}>
          {t.policy_name}
        </span>
        <span style={{ fontSize: '0.52rem', color: 'var(--muted)', fontFamily: 'var(--mono)' }}>
          Hot={t.hot_days}d + Warm={t.warm_days}d = {t.retention_days}d
        </span>
      </div>

      {/* Age bar */}
      <AgeBar daysDiff={t.days_diff} hotDays={t.hot_days} warmDays={t.warm_days} retentionDays={t.retention_days} />

      {/* Stats row */}
      <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <span style={{ fontSize: '0.54rem', color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.8px', fontWeight: 600 }}>Data Span</span>
          <span style={{ fontSize: '0.82rem', fontWeight: 700, color: overLimit ? 'var(--warn)' : 'var(--accent)', fontFamily: 'var(--mono)' }}>
            {t.days_diff}d
          </span>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <span style={{ fontSize: '0.54rem', color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.8px', fontWeight: 600 }}>Retention</span>
          <span style={{ fontSize: '0.82rem', fontWeight: 700, color: 'var(--text)', fontFamily: 'var(--mono)' }}>
            {t.retention_days}d
          </span>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2, marginLeft: 'auto', textAlign: 'right' }}>
          <span style={{ fontSize: '0.54rem', color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.8px', fontWeight: 600 }}>Oldest Data</span>
          <span style={{ fontSize: '0.65rem', fontWeight: 400, color: 'var(--muted)', fontFamily: 'var(--mono)' }}>
            {t.min_timestamp ? t.min_timestamp.split('.')[0] : 'N/A'}
          </span>
        </div>
      </div>

      {/* Disk tier distribution */}
      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', borderTop: '1px solid var(--border)', paddingTop: 7 }}>
        <TierPill label="HOT" info={t.hot_tier} color="239,147,56" />
        <TierPill label="WARM" info={t.warm_tier} color="59,130,246" />
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

      {/* Policy summary */}
      {meta.policies && meta.policies.length > 0 && (
        <div style={{ padding: '4px 20px 8px', borderTop: '1px solid var(--border)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
            <span style={{ fontSize: '0.78rem', fontWeight: 600, color: 'var(--text)' }}>Policies</span>
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {/* Default policy */}
            <span style={{
              padding: '4px 10px', borderRadius: 6, fontSize: '0.62rem', fontWeight: 600,
              fontFamily: 'var(--mono)', background: 'rgba(16,185,129,0.08)',
              border: '1px solid rgba(16,185,129,0.2)', color: 'var(--ok)',
            }}>
              Default: Hot={meta.default_hot}d + Warm={meta.default_warm}d = {meta.default_retention_days}d
            </span>
            {/* Custom policies */}
            {meta.policies.map((p, i) => (
              <span key={i} style={{
                padding: '4px 10px', borderRadius: 6, fontSize: '0.62rem', fontWeight: 600,
                fontFamily: 'var(--mono)', background: 'rgba(139,92,246,0.08)',
                border: '1px solid rgba(139,92,246,0.2)', color: 'rgb(139,92,246)',
              }}>
                {p.name}: {p.prefixes.join(', ')} = {p.total}d
              </span>
            ))}
          </div>
        </div>
      )}

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
                gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))',
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
                gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))',
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
