import React, { useState, useRef, useEffect } from 'react';

export default function TopicDiagBar({ open, fetchTopic }) {
  const [query,  setQuery]  = useState('');
  const [result, setResult] = useState(null);
  const [loading,setLoading]= useState(false);
  const inputRef = useRef(null);

  useEffect(() => {
    if (open) setTimeout(() => inputRef.current?.focus(), 100);
    else { setQuery(''); setResult(null); }
  }, [open]);

  async function inspect() {
    const name = query.trim();
    if (!name) return;
    setLoading(true); setResult(null);
    try {
      const d = await fetchTopic(name);
      setResult(d);
    } catch(e) {
      setResult({ found: false, topic: name, error: String(e.message) });
    } finally {
      setLoading(false);
    }
  }

  if (!open) return null;

  return (
    <div style={{ background: 'var(--surface2)', borderBottom: '1px solid var(--border)', padding: '12px 22px', animation: 'fadeIn 0.2s ease' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
        <span style={{ fontSize: '0.72rem', fontFamily: 'var(--mono)', color: 'var(--muted)', whiteSpace: 'nowrap' }}>TOPIC NAME:</span>
        <input
          ref={inputRef}
          value={query}
          onChange={e => setQuery(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && inspect()}
          placeholder="e.g. linux-monitor-input"
          style={{
            flex: 1, minWidth: 200, maxWidth: 420, padding: '7px 12px',
            background: 'var(--surface)', border: '1px solid var(--border)',
            borderRadius: 7, color: 'var(--text)', fontFamily: 'var(--mono)',
            fontSize: '0.75rem', outline: 'none',
          }}
        />
        <button
          onClick={inspect}
          style={{
            display: 'inline-flex', alignItems: 'center', gap: 6,
            background: 'var(--accent2)', color: 'white', border: 'none',
            borderRadius: 7, padding: '7px 16px', cursor: 'pointer',
            fontFamily: 'var(--mono)', fontSize: '0.72rem', fontWeight: 700,
          }}
        >
          {loading ? '⏳' : '▶'} INSPECT
        </button>
      </div>

      {result && (
        <div style={{ marginTop: 10, background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 10, overflow: 'hidden' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 14px', borderBottom: '1px solid var(--border)', background: 'rgba(0,153,255,0.07)' }}>
            <span style={{ fontFamily: 'var(--mono)', fontSize: '0.78rem', fontWeight: 700, color: 'var(--accent2)' }}>
              Topic: {result.topic}
            </span>
            <span style={{ cursor: 'pointer', color: 'var(--muted)' }} onClick={() => setResult(null)}>✕</span>
          </div>
          <div style={{ padding: '12px 14px' }}>
            <TopicDetail data={result} />
          </div>
        </div>
      )}
    </div>
  );
}

export function TopicDetail({ data }) {
  if (!data) return null;

  if (!data.found) {
    return (
      <div style={{ fontFamily: 'var(--mono)', fontSize: '0.72rem' }}>
        <div style={{ color: 'var(--error)', marginBottom: 8 }}>
          ⚠ Topic "<strong>{data.topic}</strong>" was not found in the current diagnostic run.
        </div>
        <div style={{ color: 'var(--muted)', fontSize: '0.65rem', lineHeight: 1.8 }}>
          This could mean:<br />
          • The topic name is misspelled<br />
          • The topic doesn't exist on this broker<br />
          • The topic was created after the last diagnostic run<br /><br />
          <span style={{ color: 'var(--accent2)' }}>Try running diagnostics again and searching after.</span>
        </div>
      </div>
    );
  }

  const info = data.info || {};
  const ret  = data.retention || {};
  const lag  = data.lag || {};
  const groups = Object.keys(lag.groups || {}).join(', ') || '—';

  return (
    <>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill,minmax(160px,1fr))', gap: 8, marginBottom: 12 }}>
        {[
          ['Partitions',      info.partition_count || '—'],
          ['Replication',     info.replication_factor || '—'],
          ['Total Messages',  (data.total_messages || 0).toLocaleString()],
          ['Retention',       ret.retention_ms || 'Broker default'],
          ['Total Lag',       (lag.total_lag || 0).toLocaleString(), (lag.total_lag || 0) > 10000 ? 'var(--warn)' : 'var(--ok)'],
          ['Consumer Groups', Object.keys(lag.groups || {}).length || '—'],
        ].map(([label, val, color]) => (
          <div key={label} style={{ background: 'var(--surface2)', borderRadius: 7, padding: '8px 12px', border: '1px solid var(--border)' }}>
            <div style={{ fontSize: '0.62rem', color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: 3, fontFamily: 'var(--mono)' }}>{label}</div>
            <div style={{ fontFamily: 'var(--mono)', fontSize: '0.78rem', fontWeight: 700, color: color || 'var(--text)' }}>{val}</div>
          </div>
        ))}
      </div>

      {groups && (
        <>
          <div style={{ fontFamily: 'var(--mono)', fontSize: '0.65rem', fontWeight: 700, color: 'var(--muted)', marginBottom: 4 }}>CONSUMER GROUPS</div>
          <div style={{ fontFamily: 'var(--mono)', fontSize: '0.72rem', color: 'var(--text)', background: 'var(--surface2)', borderRadius: 7, padding: '8px 12px', border: '1px solid var(--border)', lineHeight: 1.8 }}>
            {groups}
          </div>
        </>
      )}

      {(data.partition_offsets || []).length > 0 && (
        <>
          <div style={{ fontFamily: 'var(--mono)', fontSize: '0.65rem', fontWeight: 700, color: 'var(--muted)', margin: '12px 0 6px' }}>PARTITION DETAILS</div>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.72rem', fontFamily: 'var(--mono)', background: 'var(--surface2)', borderRadius: 7, overflow: 'hidden', border: '1px solid var(--border)' }}>
            <thead>
              <tr>{['Partition','Earliest','Latest','Messages','Live'].map(h => (
                <th key={h} style={{ textAlign: 'left', padding: '7px 14px', color: 'var(--muted)', fontWeight: 700, borderBottom: '1px solid var(--border)', fontSize: '0.65rem', letterSpacing: '0.5px', textTransform: 'uppercase' }}>{h}</th>
              ))}</tr>
            </thead>
            <tbody>
              {data.partition_offsets.map(p => (
                <tr key={p.partition}>
                  <td style={tdStyle}>{p.partition}</td>
                  <td style={tdStyle}>{(p.earliest || 0).toLocaleString()}</td>
                  <td style={tdStyle}>{(p.latest || 0).toLocaleString()}</td>
                  <td style={tdStyle}>{(p.messages || 0).toLocaleString()}</td>
                  <td style={tdStyle}><Badge type={p.is_live ? 'live' : 'stale'}>{p.is_live ? 'LIVE' : 'STALE'}</Badge></td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </>
  );
}

const tdStyle = { padding: '7px 14px', borderBottom: '1px solid rgba(30,45,69,0.5)', color: 'var(--muted)' };

function Badge({ type, children }) {
  const colors = {
    live:  { bg: 'rgba(16,185,129,0.15)',  color: 'var(--ok)'      },
    stale: { bg: 'rgba(245,158,11,0.15)',  color: 'var(--warn)'    },
    empty: { bg: 'rgba(107,114,128,0.15)', color: 'var(--unknown)' },
    ok:    { bg: 'rgba(16,185,129,0.15)',  color: 'var(--ok)'      },
    high:  { bg: 'rgba(239,68,68,0.15)',   color: 'var(--error)'   },
  };
  const c = colors[type] || colors.ok;
  return <span style={{ display: 'inline-flex', alignItems: 'center', padding: '1px 6px', borderRadius: 3, fontSize: '0.62rem', fontWeight: 700, background: c.bg, color: c.color }}>{children}</span>;
}
