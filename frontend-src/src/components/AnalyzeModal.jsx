import React, { useState, useEffect, useRef } from 'react';

const BASE = '/healthwatch';

const SEV_COLOR = {
  critical: '#ff4d4d',
  high: '#ff8c00',
  medium: '#ffd700',
  low: '#00c896',
};

function CopyBtn({ text }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      onClick={() => { navigator.clipboard.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 1500); }}
      style={{ background: 'none', border: '1px solid var(--border)', borderRadius: 4,
               color: copied ? 'var(--ok)' : 'var(--muted)', cursor: 'pointer',
               fontSize: '0.6rem', padding: '2px 8px', marginLeft: 8, whiteSpace: 'nowrap',
               fontFamily: 'var(--mono)' }}>
      {copied ? '✓ Copied' : 'Copy'}
    </button>
  );
}

function CommandBlock({ cmd, description, warning }) {
  return (
    <div style={{ marginBottom: 10 }}>
      {description && (
        <div style={{ color: 'var(--muted)', fontSize: '0.68rem', marginBottom: 4 }}>{description}</div>
      )}
      <div style={{ background: 'rgba(0,0,0,0.3)', border: '1px solid var(--border)', borderRadius: 6,
                    padding: '8px 12px', display: 'flex', alignItems: 'center',
                    justifyContent: 'space-between', gap: 8 }}>
        <code style={{ color: 'var(--ok)', fontSize: '0.65rem', wordBreak: 'break-all',
                       fontFamily: 'var(--mono)', flex: 1 }}>{cmd}</code>
        <CopyBtn text={cmd} />
      </div>
      {warning && (
        <div style={{ color: 'var(--warn)', fontSize: '0.6rem', marginTop: 4, fontFamily: 'var(--mono)' }}>
          ⚠ {warning}
        </div>
      )}
    </div>
  );
}

export default function AnalyzeModal({ checkName, section, status, detail, data, onClose }) {
  const [analysis, setAnalysis] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [tab, setTab] = useState('rca');
  const modalRef = useRef();

  useEffect(() => {
    const fetchAnalysis = async () => {
      try {
        setLoading(true);
        const res = await fetch(`${BASE}/api/analyze`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ check_name: checkName, section, status, detail, data }),
        });
        const json = await res.json();
        if (json.error && !json.rca) { setError(json.error); }
        else { setAnalysis(json); }
      } catch (e) {
        setError(e.message);
      } finally {
        setLoading(false);
      }
    };
    fetchAnalysis();
  }, [checkName, section, status, detail, data]);

  // Close on backdrop click
  useEffect(() => {
    const handler = (e) => { if (modalRef.current && !modalRef.current.contains(e.target)) onClose(); };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [onClose]);

  // Close on Escape
  useEffect(() => {
    const handler = (e) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [onClose]);

  const sev = analysis?.severity || 'high';
  const sevColor = SEV_COLOR[sev] || '#ff8c00';

  const TABS = [
    { id: 'rca', label: 'RCA' },
    { id: 'steps', label: 'Investigation' },
    { id: 'fix', label: 'Fix Commands' },
    { id: 'logs', label: 'Logs' },
  ];

  return (
    <div style={{
      position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.75)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      zIndex: 9999, padding: 20,
    }}>
      <div ref={modalRef} style={{
        background: 'var(--surface)', border: '1px solid var(--border)',
        borderRadius: 12, width: '100%', maxWidth: 720,
        maxHeight: '90vh', display: 'flex', flexDirection: 'column',
        boxShadow: '0 24px 64px rgba(0,0,0,0.6)',
      }}>
        {/* Header */}
        <div style={{ padding: '18px 24px', borderBottom: '1px solid var(--border)',
                      display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 4 }}>
              <span style={{ fontSize: '1.1rem' }}>🤖</span>
              <span style={{ color: 'var(--text)', fontWeight: 700, fontSize: '0.9rem' }}>
                AI Analysis — {checkName}
              </span>
              {analysis && (
                <span style={{
                  background: sevColor + '22', color: sevColor,
                  border: `1px solid ${sevColor}44`, borderRadius: 20,
                  fontSize: '0.6rem', padding: '2px 10px', fontWeight: 600,
                  textTransform: 'uppercase', fontFamily: 'var(--mono)',
                }}>{sev}</span>
              )}
            </div>
            <div style={{ color: 'var(--muted)', fontSize: '0.68rem', fontFamily: 'var(--mono)' }}>
              {status.toUpperCase()} · {detail}
            </div>
          </div>
          <button onClick={onClose} style={{
            background: 'none', border: 'none', color: 'var(--muted)',
            fontSize: '1.1rem', cursor: 'pointer', padding: '0 4px', lineHeight: 1,
          }}>✕</button>
        </div>

        {/* Content */}
        <div style={{ flex: 1, overflow: 'auto', padding: '0 24px 24px' }}>
          {loading && (
            <div style={{ display: 'flex', flexDirection: 'column',
                          alignItems: 'center', justifyContent: 'center', padding: 60, gap: 16 }}>
              <div style={{
                width: 36, height: 36, borderRadius: '50%',
                border: '3px solid var(--border)', borderTopColor: 'var(--accent)',
                animation: 'spin 0.8s linear infinite',
              }} />
              <div style={{ color: 'var(--muted)', fontSize: '0.78rem' }}>
                Analyzing with AI...
              </div>
              <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
            </div>
          )}

          {error && (
            <div style={{ padding: 24, color: 'var(--error)', textAlign: 'center' }}>
              <div style={{ fontSize: '1.5rem', marginBottom: 8 }}>⚠</div>
              <div style={{ fontWeight: 600, marginBottom: 4 }}>Analysis failed</div>
              <div style={{ color: 'var(--muted)', fontSize: '0.72rem' }}>{error}</div>
              {(error.includes('API key') || error.includes('API_KEY')) && (
                <div style={{ marginTop: 12, color: 'var(--warn)', fontSize: '0.68rem', fontFamily: 'var(--mono)' }}>
                  Add GROK_API_KEY or GEMINI_API_KEY to Helm secrets to enable AI analysis.
                </div>
              )}
            </div>
          )}

          {analysis && (
            <>
              {/* Impact banner */}
              {analysis.impact && (
                <div style={{
                  margin: '16px 0 8px', padding: '10px 14px',
                  background: sevColor + '11', border: `1px solid ${sevColor}33`,
                  borderRadius: 8, color: 'var(--text)', fontSize: '0.72rem',
                }}>
                  <span style={{ color: sevColor, fontWeight: 600 }}>Impact: </span>
                  {analysis.impact}
                  {analysis.estimated_recovery_time && (
                    <span style={{ color: 'var(--muted)', marginLeft: 12, fontSize: '0.65rem', fontFamily: 'var(--mono)' }}>
                      · Est. recovery: {analysis.estimated_recovery_time}
                    </span>
                  )}
                </div>
              )}

              {/* Tabs */}
              <div style={{ display: 'flex', gap: 4, marginBottom: 16, marginTop: 12,
                            borderBottom: '1px solid var(--border)', paddingBottom: 0 }}>
                {TABS.map(t => (
                  <button key={t.id} onClick={() => setTab(t.id)} style={{
                    background: 'none', border: 'none',
                    borderBottom: tab === t.id ? '2px solid var(--accent)' : '2px solid transparent',
                    color: tab === t.id ? 'var(--text)' : 'var(--muted)',
                    cursor: 'pointer', fontSize: '0.72rem', padding: '8px 14px',
                    fontWeight: tab === t.id ? 600 : 400, fontFamily: 'var(--mono)',
                  }}>{t.label}</button>
                ))}
              </div>

              {/* RCA Tab */}
              {tab === 'rca' && (
                <div>
                  <div style={{ color: 'var(--text)', fontSize: '0.76rem', lineHeight: 1.7, marginBottom: 20 }}>
                    {analysis.rca}
                  </div>
                  {analysis.likely_causes?.length > 0 && (
                    <>
                      <div style={{ color: 'var(--muted)', fontSize: '0.62rem', fontWeight: 600, marginBottom: 8,
                                    textTransform: 'uppercase', letterSpacing: 1, fontFamily: 'var(--mono)' }}>
                        Likely Causes
                      </div>
                      <ul style={{ margin: 0, padding: '0 0 0 20px' }}>
                        {analysis.likely_causes.map((c, i) => (
                          <li key={i} style={{ color: 'var(--text)', fontSize: '0.72rem', marginBottom: 6, lineHeight: 1.6 }}>{c}</li>
                        ))}
                      </ul>
                    </>
                  )}
                </div>
              )}

              {/* Investigation Tab */}
              {tab === 'steps' && (
                <div>
                  {(analysis.investigation_steps || []).map((s, i) => (
                    <div key={i} style={{ marginBottom: 16 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
                        <span style={{
                          background: 'rgba(0,153,255,0.15)', border: '1px solid rgba(0,153,255,0.3)',
                          borderRadius: '50%', width: 22, height: 22,
                          display: 'flex', alignItems: 'center', justifyContent: 'center',
                          fontSize: '0.6rem', color: 'var(--accent2)', fontWeight: 700, flexShrink: 0,
                        }}>{i + 1}</span>
                        <span style={{ color: 'var(--text)', fontSize: '0.72rem' }}>
                          {s.description || s}
                        </span>
                      </div>
                      {s.command && <CommandBlock cmd={s.command} />}
                    </div>
                  ))}
                </div>
              )}

              {/* Fix Commands Tab */}
              {tab === 'fix' && (
                <div>
                  {(analysis.fix_commands || []).map((f, i) => (
                    <CommandBlock key={i} cmd={f.command || f} description={f.description} warning={f.warning} />
                  ))}
                </div>
              )}

              {/* Logs Tab */}
              {tab === 'logs' && (
                <div>
                  {(analysis.relevant_logs || []).map((l, i) => (
                    <CommandBlock key={i} cmd={l.command || l} description={l.description} />
                  ))}
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
