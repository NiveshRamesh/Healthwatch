import React, { useState, useEffect, useCallback } from 'react';
import { Badge, Tip } from './Shared';

const BASE = '/healthwatch';

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

/* ── Cert expiry card (enhanced with subject/issuer/serial) ──────── */
function CertCard({ cert }) {
  const c = cert;
  const isOk = c.status === 'ok';
  const isWarn = c.status === 'warn';
  const borderColor = isOk ? 'var(--border)' : isWarn ? 'rgba(245,158,11,0.35)' : 'rgba(239,68,68,0.35)';
  const daysColor = isOk ? 'var(--ok)' : isWarn ? 'var(--warn)' : 'var(--error)';

  // Expiry bar: fraction of 365 days
  const maxDays = 365;
  const fraction = Math.max(0, Math.min(1, (c.days_left || 0) / maxDays));
  const barColor = isOk ? 'var(--ok)' : isWarn ? 'var(--warn)' : 'var(--error)';

  // Friendly expiry display
  const expiryStr = c.not_after
    ? new Date(c.not_after).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' })
    : c.expiry || 'unknown';

  return (
    <div style={{
      background: 'var(--surface2)', border: `1px solid ${borderColor}`,
      borderRadius: 10, padding: '14px 16px', display: 'flex', flexDirection: 'column', gap: 8,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0, flex: 1 }}>
          <span style={{ fontSize: '1rem', flexShrink: 0 }}>
            {isOk ? '🟢' : isWarn ? '🟡' : '🔴'}
          </span>
          <span style={{ fontSize: '0.76rem', fontWeight: 700, color: 'var(--text)', fontFamily: 'var(--mono)',
                         overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {c.name}
          </span>
        </div>
        <div style={{ textAlign: 'right', flexShrink: 0, marginLeft: 8 }}>
          <div style={{ fontSize: '1rem', fontWeight: 700, color: daysColor, fontFamily: 'var(--mono)' }}>
            {c.days_left > 0 ? `${c.days_left}d` : 'EXPIRED'}
          </div>
        </div>
      </div>

      {/* Expiry bar */}
      <div style={{ height: 4, background: 'var(--surface3)', borderRadius: 2, overflow: 'hidden' }}>
        <div style={{ height: '100%', width: `${fraction * 100}%`, background: barColor, borderRadius: 2,
                      transition: 'width 0.5s ease' }} />
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
        <div style={{ fontSize: '0.6rem', color: 'var(--muted)', fontFamily: 'var(--mono)' }}>
          Expires: {expiryStr}
        </div>
        {c.subject && (
          <div style={{ fontSize: '0.58rem', color: 'var(--muted)', fontFamily: 'var(--mono)',
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            Subject: {c.subject}
          </div>
        )}
        {c.issuer && (
          <div style={{ fontSize: '0.58rem', color: 'var(--muted)', fontFamily: 'var(--mono)',
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            Issuer: {c.issuer}
          </div>
        )}
      </div>
    </div>
  );
}

/* ── SA Key card ──────────────────────────────────────────────────── */
function SAKeyCard({ item }) {
  const isOk = item.status === 'ok';
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 8, padding: '8px 12px', borderRadius: 6,
      background: isOk ? 'rgba(16,185,129,0.06)' : 'rgba(245,158,11,0.06)',
      border: `1px solid ${isOk ? 'rgba(16,185,129,0.18)' : 'rgba(245,158,11,0.18)'}`,
      fontFamily: 'var(--mono)', fontSize: '0.65rem',
    }}>
      <span>{isOk ? '🟢' : '🟡'}</span>
      <span style={{ fontWeight: 600, color: 'var(--text)' }}>{item.name}</span>
      <span style={{ color: 'var(--muted)', fontSize: '0.58rem' }}>{item.detail}</span>
    </div>
  );
}

/* ── Category group for cert cards ───────────────────────────────── */
const CATEGORY_META = {
  ca: { label: 'CA Certificates', sublabel: 'Root CAs — 10yr validity, kubeadm cannot renew', icon: '🏛️' },
  pki: { label: 'PKI Certificates', sublabel: 'API server & proxy certs — 1yr validity', icon: '🔑' },
  etcd: { label: 'etcd Certificates', sublabel: 'etcd cluster TLS — 1yr validity', icon: '💾' },
  kubeconfig: { label: 'Kubeconfig Certificates', sublabel: 'Embedded client certs — 1yr validity', icon: '📋' },
  live: { label: 'Live TLS Check', sublabel: 'Real-time SSL socket verification', icon: '🌐' },
};

function CertGroup({ category, certs }) {
  const meta = CATEGORY_META[category] || { label: category, sublabel: '', icon: '📄' };
  const [collapsed, setCollapsed] = useState(false);
  const allOk = certs.every(c => c.status === 'ok');
  const countBad = certs.filter(c => c.status !== 'ok').length;

  return (
    <div style={{ margin: '0 20px 12px' }}>
      <div onClick={() => setCollapsed(!collapsed)}
           style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer',
                    padding: '8px 0', userSelect: 'none' }}>
        <span style={{ fontSize: '0.85rem' }}>{meta.icon}</span>
        <span style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--text)' }}>{meta.label}</span>
        <span style={{ fontSize: '0.6rem', color: 'var(--muted)', fontFamily: 'var(--mono)' }}>
          {meta.sublabel}
        </span>
        <span style={{ marginLeft: 'auto', fontSize: '0.6rem', fontWeight: 700, fontFamily: 'var(--mono)',
                       padding: '2px 6px', borderRadius: 4,
                       background: allOk ? 'rgba(16,185,129,0.1)' : 'rgba(239,68,68,0.1)',
                       color: allOk ? 'var(--ok)' : 'var(--error)' }}>
          {allOk ? `${certs.length}/${certs.length} OK` : `${countBad} ISSUE${countBad > 1 ? 'S' : ''}`}
        </span>
        <span style={{ fontSize: '0.65rem', color: 'var(--muted)', transform: collapsed ? 'rotate(-90deg)' : 'rotate(0)',
                       transition: 'transform 0.2s ease' }}>▼</span>
      </div>
      {!collapsed && (
        <div style={{
          display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
          gap: 10,
        }}>
          {certs.map(c => <CertCard key={c.name} cert={c} />)}
        </div>
      )}
    </div>
  );
}

/* ── Backup info bar ─────────────────────────────────────────────── */
function BackupBar({ backup }) {
  if (!backup || !backup.latest) return null;
  const isOk = backup.status === 'ok';
  const [showHistory, setShowHistory] = useState(false);

  return (
    <div style={{ margin: '0 20px 12px', background: 'var(--surface2)', border: '1px solid var(--border)',
                  borderRadius: 10, overflow: 'hidden' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                    padding: '12px 16px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '0.9rem' }}>{isOk ? '💾' : '⚠️'}</span>
          <div>
            <div style={{ fontSize: '0.72rem', fontWeight: 700, color: 'var(--text)' }}>PKI Backup</div>
            <div style={{ fontSize: '0.6rem', fontFamily: 'var(--mono)', color: 'var(--muted)', marginTop: 2 }}>
              {backup.latest} ({backup.size_display || `${backup.size_mb || 0}MB`})
            </div>
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '0.58rem', fontFamily: 'var(--mono)', color: 'var(--muted)' }}>
            {backup.total_backups} backup{backup.total_backups !== 1 ? 's' : ''} retained
          </span>
          {backup.history && backup.history.length > 0 && (
            <button onClick={() => setShowHistory(!showHistory)} style={{
              background: 'var(--surface3)', border: '1px solid var(--border)', borderRadius: 4,
              padding: '3px 8px', fontSize: '0.58rem', fontFamily: 'var(--mono)', color: 'var(--muted)',
              cursor: 'pointer',
            }}>
              {showHistory ? 'Hide' : 'History'}
            </button>
          )}
        </div>
      </div>
      {showHistory && backup.history && (
        <div style={{ borderTop: '1px solid var(--border)', padding: '8px 16px' }}>
          {backup.history.map((b, i) => (
            <div key={b.name} style={{ display: 'flex', alignItems: 'center', gap: 8,
                                        padding: '4px 0', fontSize: '0.6rem', fontFamily: 'var(--mono)',
                                        color: 'var(--muted)' }}>
              <span style={{ color: i === 0 ? 'var(--ok)' : 'var(--muted)' }}>
                {i === 0 ? '● Latest' : '○'}
              </span>
              <span>{b.path}</span>
              <span style={{ marginLeft: 'auto' }}>{b.size_display || `${b.size_mb || 0}MB`}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/* ── ConfigMap scanner status bar ─────────────────────────────────── */
function ScannerBar({ timestamp, node }) {
  if (!timestamp) {
    return (
      <div style={{ margin: '0 20px 12px', padding: '10px 16px', borderRadius: 8,
                    background: 'rgba(245,158,11,0.08)', border: '1px solid rgba(245,158,11,0.2)',
                    fontFamily: 'var(--mono)', fontSize: '0.65rem', color: 'var(--warn)' }}>
        Cert-checker CronJob has not run yet — deploy and wait for first scan
      </div>
    );
  }

  const scanned = new Date(timestamp);
  const ageHours = ((Date.now() - scanned.getTime()) / 3600000).toFixed(1);
  const isStale = ageHours > 12;

  return (
    <div style={{ margin: '0 20px 12px', display: 'flex', alignItems: 'center', gap: 12,
                  padding: '8px 16px', borderRadius: 8,
                  background: isStale ? 'rgba(245,158,11,0.06)' : 'rgba(16,185,129,0.04)',
                  border: `1px solid ${isStale ? 'rgba(245,158,11,0.15)' : 'rgba(16,185,129,0.1)'}` }}>
      <span style={{ fontSize: '0.75rem' }}>{isStale ? '⚠️' : '🔍'}</span>
      <div style={{ fontSize: '0.62rem', fontFamily: 'var(--mono)', color: 'var(--muted)', display: 'flex', gap: 16 }}>
        <span>Last scan: <strong style={{ color: 'var(--text)' }}>
          {scanned.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })}
        </strong></span>
        <span>Node: <strong style={{ color: 'var(--text)' }}>{node}</strong></span>
        <span>Age: <strong style={{ color: isStale ? 'var(--warn)' : 'var(--text)' }}>{ageHours}h</strong></span>
      </div>
    </div>
  );
}

/* ── Animated precheck bullet ────────────────────────────────────── */
function PrecheckItem({ check, index, activeIndex }) {
  const isActive = index === activeIndex;
  const isDone = index < activeIndex;
  const isPending = index > activeIndex;

  const bulletStyle = {
    width: 20, height: 20, borderRadius: '50%', flexShrink: 0,
    display: 'flex', alignItems: 'center', justifyContent: 'center',
    fontSize: '0.65rem', fontWeight: 700, transition: 'all 0.3s ease',
  };

  let bullet;
  if (isDone) {
    const st = check.status;
    const bg = st === 'pass' ? 'rgba(16,185,129,0.2)' : st === 'warn' ? 'rgba(245,158,11,0.2)' :
               st === 'skip' ? 'rgba(100,116,139,0.2)' : 'rgba(239,68,68,0.2)';
    const color = st === 'pass' ? 'var(--ok)' : st === 'warn' ? 'var(--warn)' :
                  st === 'skip' ? 'var(--muted)' : 'var(--error)';
    const icon = st === 'pass' ? '✓' : st === 'warn' ? '!' : st === 'skip' ? '⊘' : '✕';
    bullet = <div style={{ ...bulletStyle, background: bg, color, border: `2px solid ${color}` }}>{icon}</div>;
  } else if (isActive) {
    bullet = (
      <div style={{ ...bulletStyle, background: 'rgba(0,212,170,0.15)', border: '2px solid var(--accent)' }}>
        <div style={{
          width: 8, height: 8, borderRadius: '50%', border: '2px solid var(--accent)',
          borderTopColor: 'transparent', animation: 'spin 0.8s linear infinite',
        }} />
      </div>
    );
  } else {
    bullet = <div style={{ ...bulletStyle, background: 'var(--surface3)', border: '2px solid var(--border)' }}>
      <div style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--border)' }} />
    </div>;
  }

  // Detect [Node] prefix for visual grouping
  const isNodeCheck = check.label?.startsWith('[Node]');
  const displayLabel = isNodeCheck ? check.label.replace('[Node] ', '') : check.label;

  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 12, padding: '8px 12px',
      opacity: isPending ? 0.4 : 1, transition: 'opacity 0.3s ease',
      background: isActive ? 'rgba(0,212,170,0.04)' : 'transparent',
      borderRadius: 8,
    }}>
      {bullet}
      <div style={{ flex: 1 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          {isNodeCheck && (
            <span style={{ fontSize: '0.5rem', fontWeight: 700, padding: '1px 4px', borderRadius: 3,
                           background: 'rgba(0,153,255,0.12)', color: 'var(--accent2)',
                           border: '1px solid rgba(0,153,255,0.25)', letterSpacing: '0.3px' }}>NODE</span>
          )}
          <span style={{
            fontSize: '0.72rem', fontWeight: isActive ? 700 : isDone ? 600 : 400,
            color: isPending ? 'var(--muted)' : 'var(--text)', fontFamily: 'var(--mono)',
          }}>{displayLabel}</span>
        </div>
        {isDone && check.detail && (
          <div style={{
            fontSize: '0.6rem', fontFamily: 'var(--mono)', marginTop: 2,
            color: check.status === 'pass' ? 'var(--muted)' : check.status === 'warn' ? 'var(--warn)' :
                   check.status === 'skip' ? 'var(--muted)' : 'var(--error)',
          }}>{check.detail}</div>
        )}
      </div>
      {isDone && (
        <span style={{
          fontSize: '0.52rem', fontWeight: 700, fontFamily: 'var(--mono)',
          padding: '2px 6px', borderRadius: 4, letterSpacing: '0.5px',
          background: check.status === 'pass' ? 'rgba(16,185,129,0.1)' : check.status === 'warn' ? 'rgba(245,158,11,0.1)' :
                      check.status === 'skip' ? 'rgba(100,116,139,0.1)' : 'rgba(239,68,68,0.1)',
          color: check.status === 'pass' ? 'var(--ok)' : check.status === 'warn' ? 'var(--warn)' :
                 check.status === 'skip' ? 'var(--muted)' : 'var(--error)',
        }}>
          {check.status === 'pass' ? 'PASS' : check.status === 'warn' ? 'WARN' :
           check.status === 'skip' ? 'SKIP' : 'FAIL'}
        </span>
      )}
    </div>
  );
}

/* ── Precheck wizard (enhanced with post-checks) ─────────────────── */
function PrecheckWizard() {
  const [running, setRunning] = useState(false);
  const [checks, setChecks] = useState([]);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [done, setDone] = useState(false);
  const [result, setResult] = useState(null);

  // Post-checks state
  const [postRunning, setPostRunning] = useState(false);
  const [postChecks, setPostChecks] = useState([]);
  const [postActiveIndex, setPostActiveIndex] = useState(-1);
  const [postDone, setPostDone] = useState(false);

  const runPrechecks = useCallback(async () => {
    setRunning(true);
    setDone(false);
    setChecks([]);
    setActiveIndex(0);
    setResult(null);
    setPostChecks([]);
    setPostDone(false);

    try {
      const res = await fetch(`${BASE}/api/cert-prechecks`, { method: 'POST' });
      const data = await res.json();
      const allChecks = data.checks || [];
      setResult(data);

      for (let i = 0; i < allChecks.length; i++) {
        setActiveIndex(i);
        setChecks(prev => [...prev, allChecks[i]]);
        await new Promise(r => setTimeout(r, 300 + Math.random() * 200));
      }
      setActiveIndex(allChecks.length);
      setDone(true);
    } catch (e) {
      setChecks(prev => [...prev, { id: 'error', label: 'Precheck Failed', status: 'fail', detail: String(e) }]);
      setDone(true);
    }
    setRunning(false);
  }, []);

  const runPostchecks = useCallback(async () => {
    setPostRunning(true);
    setPostDone(false);
    setPostChecks([]);
    setPostActiveIndex(0);

    try {
      const res = await fetch(`${BASE}/api/cert-postchecks`, { method: 'POST' });
      const data = await res.json();
      const allChecks = data.checks || [];

      for (let i = 0; i < allChecks.length; i++) {
        setPostActiveIndex(i);
        setPostChecks(prev => [...prev, allChecks[i]]);
        await new Promise(r => setTimeout(r, 300 + Math.random() * 200));
      }
      setPostActiveIndex(allChecks.length);
      setPostDone(true);
    } catch (e) {
      setPostChecks(prev => [...prev, { id: 'error', label: 'Post-check Failed', status: 'fail', detail: String(e) }]);
      setPostDone(true);
    }
    setPostRunning(false);
  }, []);

  const passed = checks.filter(c => c.status === 'pass').length;
  const failed = checks.filter(c => c.status === 'fail').length;
  const warned = checks.filter(c => c.status === 'warn').length;
  const skipped = checks.filter(c => c.status === 'skip').length;
  const canRenew = done && result && !result.any_fail;

  const postPassed = postChecks.filter(c => c.status === 'pass').length;
  const postFailed = postChecks.filter(c => c.status === 'fail').length;
  const postWarned = postChecks.filter(c => c.status === 'warn').length;

  return (
    <div style={{
      margin: '0 20px 16px', background: 'var(--surface2)', border: '1px solid var(--border)',
      borderRadius: 12, overflow: 'hidden',
    }}>
      {/* Header */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '14px 16px', borderBottom: '1px solid var(--border)',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '0.82rem', fontWeight: 600, color: 'var(--text)' }}>
            Renewal Prechecks
          </span>
          <Tip text="Runs preflight checks before certificate renewal. Includes live K8s API checks + node-level checks from the cert-checker CronJob. Currently in DRY RUN mode." />
        </div>
        <button
          onClick={runPrechecks}
          disabled={running}
          style={{
            display: 'inline-flex', alignItems: 'center', gap: 5,
            background: running ? 'var(--surface3)' : 'rgba(0,212,170,0.12)',
            color: running ? 'var(--muted)' : 'var(--accent)',
            border: `1px solid ${running ? 'var(--border)' : 'rgba(0,212,170,0.3)'}`,
            borderRadius: 6, padding: '5px 12px', fontFamily: 'var(--mono)',
            fontSize: '0.65rem', fontWeight: 700, cursor: running ? 'not-allowed' : 'pointer',
          }}
        >
          {running ? '⏳ Running...' : '▶ Run Prechecks'}
        </button>
      </div>

      {/* Checks list */}
      {checks.length > 0 && (
        <div style={{ padding: '8px 4px', maxHeight: 500, overflowY: 'auto' }}>
          {checks.map((check, i) => (
            <PrecheckItem key={check.id} check={check} index={i} activeIndex={activeIndex} />
          ))}
        </div>
      )}

      {/* Summary + Renew button */}
      {done && (
        <div style={{
          padding: '12px 16px', borderTop: '1px solid var(--border)',
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          flexWrap: 'wrap', gap: 8,
          animation: 'fadeIn 0.3s ease',
        }}>
          <div style={{ display: 'flex', gap: 10, fontFamily: 'var(--mono)', fontSize: '0.65rem', flexWrap: 'wrap' }}>
            <span style={{ color: 'var(--ok)', fontWeight: 600 }}>{passed} passed</span>
            {warned > 0 && <span style={{ color: 'var(--warn)', fontWeight: 600 }}>{warned} warn</span>}
            {failed > 0 && <span style={{ color: 'var(--error)', fontWeight: 600 }}>{failed} failed</span>}
            {skipped > 0 && <span style={{ color: 'var(--muted)', fontWeight: 600 }}>{skipped} skipped</span>}
            <span style={{
              padding: '1px 6px', borderRadius: 3, fontSize: '0.55rem', fontWeight: 700,
              background: 'rgba(0,153,255,0.12)', color: 'var(--accent2)',
              border: '1px solid rgba(0,153,255,0.3)',
            }}>DRY RUN</span>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            {canRenew ? (
              <button style={{
                display: 'inline-flex', alignItems: 'center', gap: 5,
                background: 'linear-gradient(135deg, var(--accent), var(--accent2))',
                color: '#0a0e1a', border: 'none', borderRadius: 6,
                padding: '6px 14px', fontFamily: 'var(--mono)',
                fontSize: '0.65rem', fontWeight: 700, cursor: 'pointer',
                boxShadow: '0 2px 8px rgba(0,212,170,0.3)',
              }}
                onClick={() => alert('DRY RUN: Would execute:\n\nsudo kubeadm certs renew all\n\nRun this on the control plane node.\nAfter renewal, run Post-Checks to verify.')}
              >
                🔄 Renew Certificates
              </button>
            ) : failed > 0 ? (
              <span style={{
                fontFamily: 'var(--mono)', fontSize: '0.62rem', color: 'var(--error)', fontWeight: 600, opacity: 0.6,
              }}>Fix failures before renewal</span>
            ) : null}
            {done && (
              <button
                onClick={runPostchecks}
                disabled={postRunning}
                style={{
                  display: 'inline-flex', alignItems: 'center', gap: 5,
                  background: postRunning ? 'var(--surface3)' : 'rgba(0,153,255,0.12)',
                  color: postRunning ? 'var(--muted)' : 'var(--accent2)',
                  border: `1px solid ${postRunning ? 'var(--border)' : 'rgba(0,153,255,0.3)'}`,
                  borderRadius: 6, padding: '5px 12px', fontFamily: 'var(--mono)',
                  fontSize: '0.65rem', fontWeight: 700, cursor: postRunning ? 'not-allowed' : 'pointer',
                }}
              >
                {postRunning ? '⏳ Checking...' : '🔍 Post-Checks'}
              </button>
            )}
          </div>
        </div>
      )}

      {/* Post-checks section */}
      {postChecks.length > 0 && (
        <>
          <div style={{ padding: '10px 16px 4px', borderTop: '1px solid var(--border)' }}>
            <span style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text)' }}>
              Post-Renewal Checks
            </span>
          </div>
          <div style={{ padding: '4px 4px 8px' }}>
            {postChecks.map((check, i) => (
              <PrecheckItem key={check.id} check={check} index={i} activeIndex={postActiveIndex} />
            ))}
          </div>
          {postDone && (
            <div style={{ padding: '8px 16px 12px', borderTop: '1px solid var(--border)',
                          fontFamily: 'var(--mono)', fontSize: '0.65rem', display: 'flex', gap: 10 }}>
              <span style={{ color: 'var(--ok)', fontWeight: 600 }}>{postPassed} passed</span>
              {postWarned > 0 && <span style={{ color: 'var(--warn)', fontWeight: 600 }}>{postWarned} warn</span>}
              {postFailed > 0 && <span style={{ color: 'var(--error)', fontWeight: 600 }}>{postFailed} failed</span>}
            </div>
          )}
        </>
      )}

      {/* Empty state */}
      {checks.length === 0 && !running && (
        <div style={{
          padding: '24px 16px', textAlign: 'center',
          fontFamily: 'var(--mono)', fontSize: '0.7rem', color: 'var(--muted)',
        }}>
          Click "Run Prechecks" to verify cluster readiness for cert renewal
        </div>
      )}

      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  );
}

/* ── Main Cert Health Panel ──────────────────────────────────────── */
export default function CertHealthPanel({ checks }) {
  const details = checks.__cert_details__ || {};
  const liveCerts = details.certificates || [];
  const pkiCerts = details.pki_certificates || [];
  const saKeys = details.sa_keys || [];
  const cpStatus = details.control_plane || [];
  const cmTimestamp = details.configmap_timestamp || '';
  const cmNode = details.configmap_node || '';
  const backup = details.backup || null;

  // Group PKI certs by category, live certs go in their own group
  const allCerts = [...liveCerts, ...pkiCerts];
  const categoryOrder = ['live', 'ca', 'pki', 'etcd', 'kubeconfig'];
  const grouped = {};
  for (const cert of allCerts) {
    const cat = cert.category || 'other';
    if (!grouped[cat]) grouped[cat] = [];
    grouped[cat].push(cert);
  }

  return (
    <>
      <StatusStrip checks={checks} />

      {/* Scanner status bar */}
      <ScannerBar timestamp={cmTimestamp} node={cmNode} />

      {/* Backup info */}
      <BackupBar backup={backup} />

      {/* Grouped cert cards */}
      {categoryOrder.map(cat => {
        const certs = grouped[cat];
        if (!certs || certs.length === 0) return null;
        return <CertGroup key={cat} category={cat} certs={certs} />;
      })}

      {/* SA Keys */}
      {saKeys.length > 0 && (
        <div style={{ margin: '0 20px 12px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '8px 0' }}>
            <span style={{ fontSize: '0.85rem' }}>🔐</span>
            <span style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--text)' }}>SA Signing Keys</span>
            <span style={{ fontSize: '0.6rem', color: 'var(--muted)', fontFamily: 'var(--mono)' }}>
              ServiceAccount token signing keypair
            </span>
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {saKeys.map(k => <SAKeyCard key={k.name} item={k} />)}
          </div>
        </div>
      )}

      {/* Control plane status */}
      {cpStatus.length > 0 && (
        <>
          <div style={{ padding: '8px 20px 4px', borderTop: '1px solid var(--border)' }}>
            <span style={{ fontSize: '0.82rem', fontWeight: 600, color: 'var(--text)' }}>Control Plane Components</span>
          </div>
          <div style={{ display: 'flex', gap: 8, padding: '8px 20px 16px', flexWrap: 'wrap' }}>
            {cpStatus.map(cp => {
              const isOk = cp.status === 'ok';
              return (
                <div key={cp.component} style={{
                  display: 'flex', alignItems: 'center', gap: 6,
                  padding: '6px 10px', borderRadius: 6,
                  background: isOk ? 'rgba(16,185,129,0.08)' : 'rgba(239,68,68,0.08)',
                  border: `1px solid ${isOk ? 'rgba(16,185,129,0.2)' : 'rgba(239,68,68,0.2)'}`,
                  fontFamily: 'var(--mono)', fontSize: '0.65rem',
                }}>
                  <span style={{ fontSize: '0.7rem' }}>{isOk ? '🟢' : '🔴'}</span>
                  <span style={{ fontWeight: 600, color: 'var(--text)' }}>{cp.component}</span>
                  <span style={{ color: 'var(--muted)', fontSize: '0.58rem' }}>{cp.phase}</span>
                </div>
              );
            })}
          </div>
        </>
      )}

      {/* Extra details */}
      {(details.nodes_cordoned?.length > 0 || details.stuck_pods?.length > 0 || details.pending_csrs?.length > 0) && (
        <div style={{ padding: '8px 20px 16px', borderTop: '1px solid var(--border)' }}>
          {details.nodes_cordoned?.length > 0 && (
            <div style={{ fontSize: '0.68rem', fontFamily: 'var(--mono)', color: 'var(--warn)', marginBottom: 4 }}>
              Cordoned nodes: {details.nodes_cordoned.join(', ')}
            </div>
          )}
          {details.stuck_pods?.length > 0 && (
            <div style={{ fontSize: '0.68rem', fontFamily: 'var(--mono)', color: 'var(--error)', marginBottom: 4 }}>
              Stuck pods (kube-system): {details.stuck_pods.map(p => p.name).join(', ')}
            </div>
          )}
          {details.pending_csrs?.length > 0 && (
            <div style={{ fontSize: '0.68rem', fontFamily: 'var(--mono)', color: 'var(--warn)' }}>
              Pending CSRs: {details.pending_csrs.join(', ')}
            </div>
          )}
        </div>
      )}

      {/* Precheck wizard — always available */}
      <div style={{ padding: '0 0 8px', borderTop: '1px solid var(--border)', marginTop: 4 }}>
        <PrecheckWizard />
      </div>
    </>
  );
}
