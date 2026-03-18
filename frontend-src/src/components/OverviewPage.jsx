import React from 'react';
import { SECTIONS_META, ICON_BG, sectionOverallStatus } from '../utils';
import { Badge, Chip } from './Shared';

/* ── Extract 2-3 key metrics per service ──────────────────────────── */
function getMetrics(key, checks) {
  if (!checks) return [];
  switch (key) {
    case 'clickhouse': {
      const conn = Object.keys(checks).filter(k => !k.startsWith('__') && checks[k]?.status).length;
      const deep = checks.__ch_tables__ ? 14 : 0;
      return [`${conn} connectivity`, `${deep} deep checks`];
    }
    case 'kafka': {
      const tls = checks.__details__?.topic_live_status;
      const topics = tls ? Object.keys(tls).length : 0;
      const conn = checks['Kafka Connectors']?._connectors?.connectors?.length || 0;
      return [`${topics} topics`, `${conn} connector(s)`];
    }
    case 'kubernetes': {
      const res = checks.__resources__ || {};
      return [`${(res.node_resources||[]).length} node(s)`, `${(res.pod_resources||[]).length} pods`];
    }
    case 'pods_pvcs': {
      const pp = checks.__pods_pvcs__ || {};
      return [`${(pp.pods||[]).length} pods`, `${(pp.pvcs||[]).length} PVCs`];
    }
    default: {
      const count = Object.keys(checks).filter(k => !k.startsWith('__') && checks[k]?.status).length;
      return [`${count} checks`];
    }
  }
}

/* ── Count ok / warn / error checks in a section ─────────────────── */
function countStatuses(checks) {
  let ok = 0, warn = 0, err = 0;
  for (const [k, v] of Object.entries(checks)) {
    if (k.startsWith('__') || !v?.status) continue;
    if (v.status === 'ok') ok++;
    else if (v.status === 'warn') warn++;
    else err++;
  }
  return { ok, warn, err };
}

export default function OverviewPage({ results, running, onNavigate }) {
  // Overall counts
  let okS = 0, warnS = 0, errS = 0;
  for (const checks of Object.values(results)) {
    const s = sectionOverallStatus(checks);
    if (s === 'ok') okS++; else if (s === 'warn') warnS++; else errS++;
  }

  let overallIcon = '🟢', overallTitle = 'All systems operational', overallSub = `${okS + warnS + errS} services healthy`;
  if (errS > 0) { overallIcon = '🔴'; overallTitle = `${errS} service${errS > 1 ? 's' : ''} failing`; overallSub = 'Immediate attention required'; }
  else if (warnS > 0) { overallIcon = '🟡'; overallTitle = `${warnS} service${warnS > 1 ? 's' : ''} with warnings`; overallSub = 'Review recommended'; }

  const svcKeys = Object.keys(SECTIONS_META);

  return (
    <div>
      {/* Overall status banner */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 16,
        background: 'var(--surface)', border: '1px solid var(--border)',
        borderRadius: 14, padding: '20px 28px', marginBottom: 24,
      }}>
        <div style={{ fontSize: '2.2rem' }}>{overallIcon}</div>
        <div style={{ flex: 1 }}>
          <h2 style={{ fontSize: '1.1rem', fontWeight: 600, margin: 0 }}>{overallTitle}</h2>
          <p style={{ fontSize: '0.8rem', color: 'var(--muted)', margin: '4px 0 0', fontFamily: 'var(--mono)' }}>{overallSub}</p>
        </div>
        <div style={{ display: 'flex', gap: 10 }}>
          {[['ok', okS, 'Healthy', '16,185,129'], ['warn', warnS, 'Warning', '245,158,11'], ['error', errS, 'Failed', '239,68,68']].map(([type, count, label, rgb]) => (
            <div key={type} style={{
              display: 'flex', alignItems: 'center', gap: 5,
              background: `rgba(${rgb},0.1)`, border: `1px solid rgba(${rgb},0.25)`, borderRadius: 20,
              padding: '5px 12px', fontFamily: 'var(--mono)', fontSize: '0.75rem',
              color: `var(--${type === 'ok' ? 'ok' : type === 'warn' ? 'warn' : 'error'})`,
            }}>
              <span style={{ width: 7, height: 7, borderRadius: '50%', background: `var(--${type === 'ok' ? 'ok' : type === 'warn' ? 'warn' : 'error'})` }} />
              {count} {label}
            </div>
          ))}
        </div>
      </div>

      {/* Service cards grid */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 16 }}>
        {svcKeys.map(key => {
          const checks = results[key];
          const meta = SECTIONS_META[key];
          if (!checks) return (
            <div key={key} style={{
              background: 'var(--surface)', border: '1px solid var(--border)',
              borderRadius: 14, padding: '22px 20px', opacity: 0.5,
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                <div style={{
                  width: 38, height: 38, borderRadius: 10,
                  background: ICON_BG[meta.cls] || 'rgba(255,255,255,0.05)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '1.1rem',
                }}>{meta.icon}</div>
                <div>
                  <div style={{ fontSize: '0.9rem', fontWeight: 600 }}>{meta.label}</div>
                  <div style={{ fontSize: '0.72rem', color: 'var(--muted)', fontFamily: 'var(--mono)' }}>No data</div>
                </div>
              </div>
            </div>
          );

          const overall = sectionOverallStatus(checks);
          const metrics = getMetrics(key, checks);
          const { ok, warn, err } = countStatuses(checks);

          return (
            <div
              key={key}
              onClick={() => onNavigate(key)}
              style={{
                background: 'var(--surface)', border: '1px solid var(--border)',
                borderRadius: 14, padding: '22px 20px', cursor: 'pointer',
                transition: 'border-color 0.2s, transform 0.15s',
              }}
              onMouseEnter={e => { e.currentTarget.style.borderColor = 'rgba(0,212,170,0.3)'; e.currentTarget.style.transform = 'translateY(-2px)'; }}
              onMouseLeave={e => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.transform = 'none'; }}
            >
              {/* Header */}
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <div style={{
                    width: 38, height: 38, borderRadius: 10,
                    background: ICON_BG[meta.cls] || 'rgba(255,255,255,0.05)',
                    display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '1.1rem',
                  }}>{meta.icon}</div>
                  <div>
                    <div style={{ fontSize: '0.9rem', fontWeight: 600 }}>{meta.label}</div>
                    <div style={{ fontSize: '0.68rem', color: 'var(--muted)', fontFamily: 'var(--mono)', marginTop: 1 }}>{meta.sub}</div>
                  </div>
                </div>
                <Badge status={overall} />
              </div>

              {/* Metrics */}
              <div style={{ display: 'flex', gap: 8, marginBottom: 12, flexWrap: 'wrap' }}>
                {metrics.filter(Boolean).map((m, i) => (
                  <span key={i} style={{
                    fontFamily: 'var(--mono)', fontSize: '0.68rem', color: 'var(--muted)',
                    background: 'var(--surface2)', borderRadius: 5, padding: '3px 8px',
                  }}>{m}</span>
                ))}
              </div>

              {/* Status chips */}
              <div style={{ display: 'flex', gap: 6 }}>
                {ok > 0 && <Chip n={ok} color="16,185,129" label="ok" />}
                {warn > 0 && <Chip n={warn} color="245,158,11" label="warn" />}
                {err > 0 && <Chip n={err} color="239,68,68" label="err" />}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
