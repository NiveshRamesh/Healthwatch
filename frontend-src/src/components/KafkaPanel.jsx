import React, { useState } from 'react';
import { Badge, SubSection, Chip } from './Shared';
import { STATUS_ICONS } from '../utils';
import LiveDataPanel from './LiveDataPanel';
import ConsumerLagPanel from './ConsumerLagPanel';
import ZookeeperStatsPanel from './ZookeeperStatsPanel';
import Modal from './Modal';
import { TopicDetail } from './TopicDiagBar';

const LED_ANIM = {
  ok:    'pulse-ok 2s infinite',
  error: 'pulse-err 1s infinite',
};

/* ── Standard check row (same style as original) ─────────────────── */
function CheckRow({ name, check, onExpandLive, onExpandLag, onInspect, details }) {
  const { status, detail = '' } = check;

  const tls      = details?.topic_live_status;
  const live     = tls ? Object.values(tls).filter(t =>  t.is_live).length : 0;
  const stale    = tls ? Object.values(tls).filter(t =>  t.has_data && !t.is_live).length : 0;
  const empty    = tls ? Object.values(tls).filter(t => !t.has_data).length : 0;
  const showLive = name === 'Live Data' && tls;

  const lagMap    = details?.consumer_lag;
  const highCount = lagMap ? Object.values(lagMap).filter(v => v.total_lag > 10000).length : 0;
  const showLag   = name === 'Consumer Lag' && lagMap;

  return (
    <div style={{
      display:'flex', alignItems:'center', justifyContent:'space-between',
      padding:'11px 22px 11px 38px', borderBottom:'1px solid rgba(30,45,69,0.5)',
    }}
      onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
      onMouseLeave={e => e.currentTarget.style.background='transparent'}
    >
      <span style={{ fontSize:'0.82rem' }}>{name}</span>
      <div style={{ display:'flex', alignItems:'center', gap:8 }}>
        <span style={{ fontSize:'0.72rem', color:'var(--muted)', fontFamily:'var(--mono)' }}>{detail}</span>

        {showLive && (
          <span onClick={onExpandLive} style={{
            display:'inline-flex', alignItems:'center', gap:4, fontSize:'0.7rem',
            fontFamily:'var(--mono)', fontWeight:700, padding:'2px 8px', borderRadius:5, cursor:'pointer',
            border:'1px solid rgba(0,153,255,0.4)', color:'var(--accent2)',
            background:'rgba(0,153,255,0.08)',
          }}>📊 {live} live · {stale} stale · {empty} empty ▾</span>
        )}
        {showLag && highCount > 0 && (
          <span onClick={onExpandLag} style={{
            display:'inline-flex', alignItems:'center', gap:4, fontSize:'0.7rem',
            fontFamily:'var(--mono)', fontWeight:700, padding:'2px 8px', borderRadius:5, cursor:'pointer',
            border:'1px solid rgba(245,158,11,0.4)', color:'var(--warn)',
            background:'rgba(245,158,11,0.08)',
          }}>⚠ {highCount} high lag ▾</span>
        )}

        <Badge status={status} size="sm" />
      </div>
    </div>
  );
}

/* ── Connector state chip colours ───────────────────────────────── */
const STATE_STYLE = {
  RUNNING:     { color:'var(--ok)',   bg:'rgba(16,185,129,0.1)',  border:'rgba(16,185,129,0.3)'  },
  PAUSED:      { color:'var(--warn)', bg:'rgba(245,158,11,0.1)',  border:'rgba(245,158,11,0.3)'  },
  STOPPED:     { color:'var(--warn)', bg:'rgba(245,158,11,0.1)',  border:'rgba(245,158,11,0.3)'  },
  FAILED:      { color:'var(--error)',bg:'rgba(239,68,68,0.1)',   border:'rgba(239,68,68,0.3)'   },
  MISSING:     { color:'var(--error)',bg:'rgba(239,68,68,0.1)',   border:'rgba(239,68,68,0.3)'   },
  UNREACHABLE: { color:'var(--error)',bg:'rgba(239,68,68,0.1)',   border:'rgba(239,68,68,0.3)'   },
  UNASSIGNED:  { color:'var(--warn)', bg:'rgba(245,158,11,0.1)',  border:'rgba(245,158,11,0.3)'  },
  UNKNOWN:     { color:'var(--muted)',bg:'rgba(255,255,255,0.05)',border:'rgba(255,255,255,0.1)' },
};

function StateChip({ state }) {
  const s = STATE_STYLE[state] || STATE_STYLE.UNKNOWN;
  return (
    <span style={{
      fontFamily:'var(--mono)', fontSize:'0.62rem', fontWeight:700,
      color:s.color, background:s.bg, border:`1px solid ${s.border}`,
      borderRadius:4, padding:'1px 7px', letterSpacing:'0.4px',
    }}>{state}</span>
  );
}

/* ── Connector row — inline in Kafka section ─────────────────────── */
function ConnectorSubPanel({ connectors: data }) {
  if (!data) return null;
  const { connectors = [], status, problems = [] } = data;
  const problemCount = problems.length;

  return (
    <SubSection
      icon="🔌" title="Kafka Connectors"
      defaultOpen={true}
      badge={
        <span style={{ display:'flex', gap:4 }}>
          {problemCount > 0 && <Chip n={problemCount} color="239,68,68" label="problem" />}
          <Badge status={status} size="sm" />
        </span>
      }
    >
      {connectors.map((conn) => {
        const tasks = conn.tasks || [];
        const failedTasks = tasks.filter(t => t.state !== 'RUNNING');
        return (
          <div key={conn.name} style={{
            borderBottom:'1px solid rgba(30,45,69,0.4)',
            background: conn.status !== 'ok' ? `rgba(${conn.status === 'warn' ? '245,158,11' : '239,68,68'},0.03)` : 'transparent',
          }}>
            {/* Connector header row */}
            <div style={{
              display:'flex', alignItems:'center', justifyContent:'space-between',
              padding:'9px 22px 9px 24px',
            }}
              onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
              onMouseLeave={e => e.currentTarget.style.background='transparent'}
            >
              <div style={{ display:'flex', alignItems:'center', gap:8 }}>
                <span style={{ fontFamily:'var(--mono)', fontSize:'0.75rem' }}>{conn.name}</span>
                <StateChip state={conn.connector_state} />
              </div>
              <div style={{ display:'flex', alignItems:'center', gap:8 }}>
                <span style={{ fontFamily:'var(--mono)', fontSize:'0.68rem', color:'var(--muted)' }}>
                  {conn.detail}
                </span>
                <Badge status={conn.status} size="sm" />
              </div>
            </div>

            {/* Task rows — only shown when something is wrong */}
            {(conn.connector_state !== 'MISSING' && conn.connector_state !== 'UNREACHABLE') && tasks.length > 0 && failedTasks.length > 0 && (
              <div style={{ paddingLeft:40, paddingBottom:6 }}>
                {tasks.map(t => (
                  <div key={t.id} style={{
                    display:'flex', alignItems:'center', gap:8,
                    padding:'3px 22px 3px 0', fontSize:'0.68rem', fontFamily:'var(--mono)',
                  }}>
                    <span style={{ color:'var(--muted)' }}>task[{t.id}]</span>
                    <StateChip state={t.state} />
                    {t.worker_id && (
                      <span style={{ color:'var(--muted)', fontSize:'0.62rem' }}>{t.worker_id}</span>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        );
      })}
    </SubSection>
  );
}

/* ── Topic Diagnosis Modal ───────────────────────────────────────── */
function TopicDiagModal({ open, onClose, tls, fetchTopic }) {
  const [selected, setSelected] = useState('');
  const [custom,   setCustom]   = useState('');
  const [result,   setResult]   = useState(null);
  const [loading,  setLoading]  = useState(false);

  const live  = Object.entries(tls || {}).filter(([,v]) =>  v.is_live).map(([k]) => k).sort();
  const stale = Object.entries(tls || {}).filter(([,v]) =>  v.has_data && !v.is_live).map(([k]) => k).sort();
  const empty = Object.entries(tls || {}).filter(([,v]) => !v.has_data).map(([k]) => k).sort();
  const allKnown = [...live, ...stale, ...empty];

  function handleClose() {
    onClose(); setSelected(''); setCustom(''); setResult(null);
  }

  async function inspect() {
    const name = (custom.trim() || selected).trim();
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
    <Modal open={open} onClose={handleClose} title="🔍 Topic Diagnosis">
      {/* ── Selector row ── */}
      <div style={{ display:'flex', flexDirection:'column', gap:10 }}>
        {allKnown.length > 0 && (
          <div>
            <div style={{ fontSize:'0.65rem', color:'var(--muted)', fontFamily:'var(--mono)',
              textTransform:'uppercase', letterSpacing:'0.5px', marginBottom:5 }}>
              Select from known topics
            </div>
            <select
              value={selected}
              onChange={e => { setSelected(e.target.value); setCustom(''); setResult(null); }}
              style={{
                width:'100%', padding:'8px 10px', borderRadius:7,
                background:'var(--surface)', border:'1px solid var(--border)',
                color:'var(--text)', fontFamily:'var(--mono)', fontSize:'0.75rem',
                cursor:'pointer', outline:'none',
              }}
            >
              <option value="">— choose a topic —</option>
              {live.length  > 0 && <optgroup label="🟢 Live">  {live.map(t  => <option key={t} value={t}>{t}</option>)}</optgroup>}
              {stale.length > 0 && <optgroup label="🟡 Stale"> {stale.map(t => <option key={t} value={t}>{t}</option>)}</optgroup>}
              {empty.length > 0 && <optgroup label="⚪ Empty"> {empty.map(t => <option key={t} value={t}>{t}</option>)}</optgroup>}
            </select>
          </div>
        )}

        <div>
          <div style={{ fontSize:'0.65rem', color:'var(--muted)', fontFamily:'var(--mono)',
            textTransform:'uppercase', letterSpacing:'0.5px', marginBottom:5 }}>
            Or type a topic name
          </div>
          <input
            value={custom}
            onChange={e => { setCustom(e.target.value); setSelected(''); setResult(null); }}
            onKeyDown={e => e.key === 'Enter' && inspect()}
            placeholder="e.g. linux-monitor-input"
            style={{
              width:'100%', padding:'8px 10px', borderRadius:7, boxSizing:'border-box',
              background:'var(--surface)', border:'1px solid var(--border)',
              color:'var(--text)', fontFamily:'var(--mono)', fontSize:'0.75rem', outline:'none',
            }}
          />
        </div>

        <button
          onClick={inspect}
          disabled={loading || !(custom.trim() || selected)}
          style={{
            display:'inline-flex', alignItems:'center', justifyContent:'center', gap:6,
            background: (custom.trim() || selected) ? 'var(--accent2)' : 'rgba(0,153,255,0.2)',
            color:'white', border:'none', borderRadius:7, padding:'9px 18px',
            cursor: (custom.trim() || selected) ? 'pointer' : 'default',
            fontFamily:'var(--mono)', fontSize:'0.75rem', fontWeight:700,
          }}
        >
          {loading ? '⏳ Fetching...' : '▶ INSPECT TOPIC'}
        </button>

        {/* ── Result ── */}
        {result && (
          <div style={{ marginTop:4, background:'var(--surface)', border:'1px solid var(--border)',
            borderRadius:10, overflow:'hidden' }}>
            <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between',
              padding:'10px 14px', borderBottom:'1px solid var(--border)',
              background:'rgba(0,153,255,0.07)' }}>
              <span style={{ fontFamily:'var(--mono)', fontSize:'0.78rem', fontWeight:700,
                color:'var(--accent2)' }}>Topic: {result.topic}</span>
              <span style={{ cursor:'pointer', color:'var(--muted)' }} onClick={() => setResult(null)}>✕</span>
            </div>
            <div style={{ padding:'12px 14px' }}>
              <TopicDetail data={result} />
            </div>
          </div>
        )}
      </div>
    </Modal>
  );
}

/* ── Main Kafka Panel ────────────────────────────────────────────── */
export default function KafkaPanel({ checks, fetchTopic }) {
  const [diagModalOpen,  setDiagModalOpen] = useState(false);
  const [livePanelOpen,  setLivePanelOpen] = useState(false);
  const [lagPanelOpen,   setLagPanelOpen]  = useState(false);
  const [topicModal,     setTopicModal]    = useState({ open:false, name:'', data:null });

  const details    = checks.__details__;
  const connectors = checks['Kafka Connectors']?._connectors;
  const zkStats    = checks['Zookeeper Stats']?._zk_stats;
  const tls        = details?.topic_live_status || {};

  function openTopicModal(name) {
    setTopicModal({ open:true, name, data:null });
    fetchTopic(name).then(d => setTopicModal(p => ({ ...p, data:d })));
  }

  return (
    <>
      {/* ── Topic Diagnosis button ── */}
      <div style={{ padding:'10px 22px 4px', display:'flex', justifyContent:'flex-end' }}>
        <button
          onClick={() => setDiagModalOpen(true)}
          style={{
            display:'inline-flex', alignItems:'center', gap:6,
            background:'rgba(0,153,255,0.1)', color:'var(--accent2)',
            border:'1px solid rgba(0,153,255,0.3)', borderRadius:7,
            padding:'6px 14px', fontFamily:'var(--mono)', fontSize:'0.7rem',
            fontWeight:700, cursor:'pointer',
          }}
        >🔍 TOPIC DIAGNOSIS</button>
      </div>

      {/* Standard check rows */}
      {Object.entries(checks).map(([k, v]) => {
        if (k.startsWith('__') || !v?.status || k === 'Kafka Connectors' || k === 'Zookeeper Stats') return null;
        return (
          <CheckRow key={k} name={k} check={v} details={details}
            onExpandLive={() => setLivePanelOpen(o => !o)}
            onExpandLag={() => setLagPanelOpen(o => !o)}
            onInspect={openTopicModal}
          />
        );
      })}

      {/* Live data panel */}
      {livePanelOpen && <LiveDataPanel details={details} onInspect={openTopicModal} />}
      {/* Consumer lag panel */}
      {lagPanelOpen  && <ConsumerLagPanel details={details} onInspect={openTopicModal} />}

      {/* Zookeeper Stats — summary row + expandable metrics panel */}
      {checks['Zookeeper Stats'] && (
        <CheckRow
          key="Zookeeper Stats" name="Zookeeper Stats"
          check={checks['Zookeeper Stats']} details={details}
          onRestartClick={() => {}} onExpandLive={() => {}} onExpandLag={() => {}} onInspect={() => {}}
        />
      )}
      <ZookeeperStatsPanel zkStats={zkStats} />

      {/* Connector check — new panel inline */}
      <ConnectorSubPanel connectors={connectors} />

      {/* Topic Diagnosis modal */}
      <TopicDiagModal
        open={diagModalOpen}
        onClose={() => setDiagModalOpen(false)}
        tls={tls}
        fetchTopic={fetchTopic}
      />

      {/* Topic detail modal (from inspect → links) */}
      <Modal open={topicModal.open} onClose={() => setTopicModal(p=>({...p,open:false}))}
        title={`Topic: ${topicModal.name}`}>
        {topicModal.data
          ? <TopicDetail data={topicModal.data} />
          : <div style={{ fontFamily:'var(--mono)', fontSize:'0.72rem', color:'var(--muted)' }}>⏳ Loading...</div>
        }
      </Modal>
    </>
  );
}
