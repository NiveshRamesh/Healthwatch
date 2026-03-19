import React, { useState, useEffect, useCallback } from 'react';
import { Badge, Tip } from './Shared';

const BASE = '';

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

/* ── Cert expiry card ────────────────────────────────────────────── */
function CertCard({ cert }) {
  const c = cert;
  const isOk = c.status === 'ok';
  const isWarn = c.status === 'warn';
  const borderColor = isOk ? 'var(--border)' : isWarn ? 'rgba(245,158,11,0.35)' : 'rgba(239,68,68,0.35)';
  const daysColor = isOk ? 'var(--ok)' : isWarn ? 'var(--warn)' : 'var(--error)';

  return (
    <div style={{
      background: 'var(--surface2)', border: `1px solid ${borderColor}`,
      borderRadius: 10, padding: '14px 16px',
      display: 'flex', alignItems: 'center', gap: 14,
    }}>
      <div style={{
        width: 42, height: 42, borderRadius: 10,
        background: isOk ? 'rgba(16,185,129,0.1)' : isWarn ? 'rgba(245,158,11,0.1)' : 'rgba(239,68,68,0.1)',
        display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '1.2rem', flexShrink: 0,
      }}>
        {isOk ? '🟢' : isWarn ? '🟡' : '🔴'}
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: '0.78rem', fontWeight: 700, color: 'var(--text)', fontFamily: 'var(--mono)' }}>{c.name}</div>
        <div style={{ fontSize: '0.65rem', color: 'var(--muted)', fontFamily: 'var(--mono)', marginTop: 2 }}>
          Expires: {c.expiry}
        </div>
      </div>
      <div style={{ textAlign: 'right', flexShrink: 0 }}>
        <div style={{ fontSize: '1.1rem', fontWeight: 700, color: daysColor, fontFamily: 'var(--mono)' }}>
          {c.days_left > 0 ? `${c.days_left}d` : 'EXPIRED'}
        </div>
        <div style={{ fontSize: '0.55rem', color: 'var(--muted)', fontFamily: 'var(--mono)' }}>remaining</div>
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
    const bg = check.status === 'pass' ? 'rgba(16,185,129,0.2)' : check.status === 'warn' ? 'rgba(245,158,11,0.2)' : 'rgba(239,68,68,0.2)';
    const color = check.status === 'pass' ? 'var(--ok)' : check.status === 'warn' ? 'var(--warn)' : 'var(--error)';
    const icon = check.status === 'pass' ? '✓' : check.status === 'warn' ? '!' : '✕';
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

  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 12, padding: '8px 12px',
      opacity: isPending ? 0.4 : 1, transition: 'opacity 0.3s ease',
      background: isActive ? 'rgba(0,212,170,0.04)' : 'transparent',
      borderRadius: 8,
    }}>
      {bullet}
      <div style={{ flex: 1 }}>
        <div style={{
          fontSize: '0.72rem', fontWeight: isActive ? 700 : isDone ? 600 : 400,
          color: isPending ? 'var(--muted)' : 'var(--text)', fontFamily: 'var(--mono)',
        }}>{check.label}</div>
        {isDone && check.detail && (
          <div style={{
            fontSize: '0.6rem', fontFamily: 'var(--mono)', marginTop: 2,
            color: check.status === 'pass' ? 'var(--muted)' : check.status === 'warn' ? 'var(--warn)' : 'var(--error)',
          }}>{check.detail}</div>
        )}
      </div>
      {isDone && (
        <span style={{
          fontSize: '0.52rem', fontWeight: 700, fontFamily: 'var(--mono)',
          padding: '2px 6px', borderRadius: 4, letterSpacing: '0.5px',
          background: check.status === 'pass' ? 'rgba(16,185,129,0.1)' : check.status === 'warn' ? 'rgba(245,158,11,0.1)' : 'rgba(239,68,68,0.1)',
          color: check.status === 'pass' ? 'var(--ok)' : check.status === 'warn' ? 'var(--warn)' : 'var(--error)',
        }}>
          {check.status === 'pass' ? 'PASS' : check.status === 'warn' ? 'WARN' : 'FAIL'}
        </span>
      )}
    </div>
  );
}

/* ── Precheck wizard ─────────────────────────────────────────────── */
function PrecheckWizard() {
  const [running, setRunning] = useState(false);
  const [checks, setChecks] = useState([]);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [done, setDone] = useState(false);
  const [result, setResult] = useState(null);

  const runPrechecks = useCallback(async () => {
    setRunning(true);
    setDone(false);
    setChecks([]);
    setActiveIndex(0);
    setResult(null);

    try {
      const res = await fetch(`${BASE}/api/cert-prechecks`, { method: 'POST' });
      const data = await res.json();
      const allChecks = data.checks || [];
      setResult(data);

      // Animate through checks one by one
      for (let i = 0; i < allChecks.length; i++) {
        setActiveIndex(i);
        setChecks(prev => [...prev, allChecks[i]]);
        await new Promise(r => setTimeout(r, 400 + Math.random() * 300));
      }
      setActiveIndex(allChecks.length);
      setDone(true);
    } catch (e) {
      setChecks(prev => [...prev, { id: 'error', label: 'Precheck Failed', status: 'fail', detail: String(e) }]);
      setDone(true);
    }
    setRunning(false);
  }, []);

  const passed = checks.filter(c => c.status === 'pass').length;
  const failed = checks.filter(c => c.status === 'fail').length;
  const warned = checks.filter(c => c.status === 'warn').length;
  const canRenew = done && result && !result.any_fail;

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
          <Tip text="Runs preflight checks before certificate renewal. All checks must pass before renewal can proceed. Currently in DRY RUN mode." />
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
        <div style={{ padding: '8px 4px' }}>
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
          animation: 'fadeIn 0.3s ease',
        }}>
          <div style={{ display: 'flex', gap: 10, fontFamily: 'var(--mono)', fontSize: '0.65rem' }}>
            <span style={{ color: 'var(--ok)', fontWeight: 600 }}>{passed} passed</span>
            {warned > 0 && <span style={{ color: 'var(--warn)', fontWeight: 600 }}>{warned} warn</span>}
            {failed > 0 && <span style={{ color: 'var(--error)', fontWeight: 600 }}>{failed} failed</span>}
            <span style={{
              padding: '1px 6px', borderRadius: 3, fontSize: '0.55rem', fontWeight: 700,
              background: 'rgba(0,153,255,0.12)', color: 'var(--accent2)',
              border: '1px solid rgba(0,153,255,0.3)',
            }}>DRY RUN</span>
          </div>
          {canRenew ? (
            <button style={{
              display: 'inline-flex', alignItems: 'center', gap: 5,
              background: 'linear-gradient(135deg, var(--accent), var(--accent2))',
              color: '#0a0e1a', border: 'none', borderRadius: 6,
              padding: '6px 14px', fontFamily: 'var(--mono)',
              fontSize: '0.65rem', fontWeight: 700, cursor: 'pointer',
              boxShadow: '0 2px 8px rgba(0,212,170,0.3)',
            }}
              onClick={() => alert('DRY RUN: Would execute:\n\nsudo kubeadm certs renew all\n\nRun this on the control plane node.')}
            >
              🔄 Renew Certificates
            </button>
          ) : failed > 0 ? (
            <span style={{
              fontFamily: 'var(--mono)', fontSize: '0.62rem', color: 'var(--error)', fontWeight: 600,
              opacity: 0.6,
            }}>
              Fix failures before renewal
            </span>
          ) : null}
        </div>
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

      {/* Inject spinner keyframes */}
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  );
}

/* ── Main Cert Health Panel ──────────────────────────────────────── */
export default function CertHealthPanel({ checks }) {
  const details = checks.__cert_details__ || {};
  const certs = details.certificates || [];
  const cpStatus = details.control_plane || [];
  const allHealthy = certs.length > 0 && certs.every(c => c.status === 'ok') &&
                     cpStatus.every(c => c.status === 'ok');

  return (
    <>
      <StatusStrip checks={checks} />

      {/* Cert cards */}
      {certs.length > 0 && (
        <>
          <div style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            padding: '8px 20px 4px', borderTop: '1px solid var(--border)',
          }}>
            <span style={{ fontSize: '0.82rem', fontWeight: 600, color: 'var(--text)' }}>Certificates</span>
            <span style={{ fontSize: '0.68rem', fontFamily: 'var(--mono)', color: 'var(--muted)' }}>
              Threshold: {details.warn_threshold_days || 30}d
            </span>
          </div>
          <div style={{
            display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))',
            gap: 10, padding: '10px 20px 16px',
          }}>
            {certs.map(c => <CertCard key={c.name} cert={c} />)}
          </div>
        </>
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
