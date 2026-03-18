import React from 'react';
import { SECTIONS_META, sectionOverallStatus } from '../utils';

const STATUS_DOT = { ok: 'var(--ok)', warn: 'var(--warn)', error: 'var(--error)', critical: 'var(--error)' };

const NAV_ITEMS = [
  { key: 'overview', icon: '⬡', label: 'Overview' },
  ...Object.entries(SECTIONS_META).map(([key, meta]) => ({ key, icon: meta.icon, label: meta.label })),
];

export default function Sidebar({ activePage, onNavigate, results, running, triggerRun, lastChecked }) {
  return (
    <nav style={{
      position: 'fixed', left: 0, top: 0, bottom: 0, width: 220, zIndex: 100,
      background: 'var(--surface)', borderRight: '1px solid var(--border)',
      display: 'flex', flexDirection: 'column',
      fontFamily: 'var(--sans)',
    }}>
      {/* Logo */}
      <div style={{ padding: '24px 18px 16px', borderBottom: '1px solid var(--border)' }}>
        <div style={{
          fontFamily: 'var(--mono)', fontSize: '1.1rem', fontWeight: 700, letterSpacing: '-0.5px',
          background: 'linear-gradient(135deg,var(--accent),var(--accent2))',
          WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent',
        }}>
          ⬡ HealthWatch
        </div>
        <div style={{ fontSize: '0.65rem', color: 'var(--muted)', fontFamily: 'var(--mono)', marginTop: 4 }}>
          Infrastructure Diagnostics
        </div>
      </div>

      {/* Navigation */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '8px 0' }}>
        {NAV_ITEMS.map(({ key, icon, label }) => {
          const isActive = activePage === key;
          const checks = results[key];
          const status = checks ? sectionOverallStatus(checks) : null;

          return (
            <div
              key={key}
              onClick={() => onNavigate(key)}
              style={{
                display: 'flex', alignItems: 'center', gap: 10,
                padding: '10px 16px', cursor: 'pointer',
                borderLeft: isActive ? '3px solid var(--accent)' : '3px solid transparent',
                background: isActive ? 'var(--surface2)' : 'transparent',
                transition: 'background 0.15s',
              }}
              onMouseEnter={e => { if (!isActive) e.currentTarget.style.background = 'var(--surface2)'; }}
              onMouseLeave={e => { if (!isActive) e.currentTarget.style.background = 'transparent'; }}
            >
              <span style={{ fontSize: '1rem', width: 24, textAlign: 'center', flexShrink: 0 }}>{icon}</span>
              <span style={{
                fontSize: '0.82rem', fontWeight: isActive ? 600 : 400,
                color: isActive ? 'var(--text)' : 'var(--muted)',
                flex: 1,
              }}>{label}</span>
              {status && (
                <span style={{
                  width: 8, height: 8, borderRadius: '50%', flexShrink: 0,
                  background: STATUS_DOT[status] || 'var(--muted)',
                  boxShadow: status !== 'ok' ? `0 0 6px ${STATUS_DOT[status]}` : 'none',
                }} />
              )}
            </div>
          );
        })}
      </div>

      {/* Bottom: Run button + timestamp */}
      <div style={{ padding: '12px 16px', borderTop: '1px solid var(--border)' }}>
        <button
          onClick={triggerRun} disabled={running}
          style={{
            width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
            background: 'linear-gradient(135deg,var(--accent),var(--accent2))',
            color: '#0a0e1a', fontFamily: 'var(--mono)', fontSize: '0.72rem', fontWeight: 700,
            border: 'none', borderRadius: 7, padding: '9px 0',
            cursor: running ? 'not-allowed' : 'pointer', opacity: running ? 0.5 : 1,
            letterSpacing: '0.5px',
          }}
        >
          {running ? '⏳ RUNNING...' : '▶ RUN DIAGNOSTICS'}
        </button>
        <div style={{
          marginTop: 8, textAlign: 'center',
          fontFamily: 'var(--mono)', fontSize: '0.62rem', color: 'var(--muted)',
        }}>
          Last: <span style={{ color: 'var(--accent)' }}>{lastChecked || '—'}</span>
        </div>
      </div>
    </nav>
  );
}
