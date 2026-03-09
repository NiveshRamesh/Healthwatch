import React, { useState } from 'react';
import { STATUS_LABELS, sectionOverallStatus } from '../utils';
import CheckRow from './CheckRow';
import TopicDiagBar from './TopicDiagBar';
import LiveDataPanel from './LiveDataPanel';
import ConsumerLagPanel from './ConsumerLagPanel';
import Modal from './Modal';
import { TopicDetail } from './TopicDiagBar';

const ICON_BG = {
  clickhouse: 'rgba(255,183,0,0.12)',
  kafka:      'rgba(0,153,255,0.12)',
  postgres:   'rgba(51,102,204,0.12)',
  minio:      'rgba(198,53,40,0.12)',
  kubernetes: 'rgba(50,108,229,0.12)',
};

const LED_ANIM = {
  ok:    'pulse-ok 2s infinite',
  error: 'pulse-err 1s infinite',
};

export default function ServiceSection({ svcKey, meta, checks, fetchTopic }) {
  const [open,          setOpen]         = useState(true);
  const [diagBarOpen,   setDiagBarOpen]  = useState(false);
  const [livePanelOpen, setLivePanelOpen]= useState(false);
  const [lagPanelOpen,  setLagPanelOpen] = useState(false);

  // Restart modal
  const [restartModal, setRestartModal] = useState({ open: false, podName: '', detail: '', logs: '' });

  // Topic inspect modal (opened from lag/live tables, not from diag bar)
  const [topicModal,   setTopicModal]   = useState({ open: false, name: '', data: null });

  const overall   = sectionOverallStatus(checks);
  const details   = checks.__details__;
  const isKafka   = svcKey === 'kafka';

  function openTopicModal(name) {
    setTopicModal({ open: true, name, data: null });
    fetchTopic(name).then(d => setTopicModal(prev => ({ ...prev, data: d })));
  }

  return (
    <div style={{
      background:'var(--surface)', border:'1px solid var(--border)',
      borderRadius:14, marginBottom:16, overflow:'hidden', transition:'border-color 0.3s',
    }}
      onMouseEnter={e => e.currentTarget.style.borderColor='rgba(0,212,170,0.2)'}
      onMouseLeave={e => e.currentTarget.style.borderColor='var(--border)'}
    >
      {/* ── Section header ── */}
      <div
        onClick={() => setOpen(o => !o)}
        style={{
          display:'flex', alignItems:'center', justifyContent:'space-between',
          padding:'18px 22px', cursor:'pointer', userSelect:'none',
          borderBottom: open ? '1px solid var(--border)' : '1px solid transparent',
          transition:'background 0.2s',
        }}
        onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
        onMouseLeave={e => e.currentTarget.style.background='transparent'}
      >
        {/* Left: icon + title */}
        <div style={{ display:'flex', alignItems:'center', gap:12 }}>
          <div style={{ width:36, height:36, borderRadius:10, background:ICON_BG[meta.cls]||'rgba(255,255,255,0.05)', display:'flex', alignItems:'center', justifyContent:'center', fontSize:'1.1rem' }}>
            {meta.icon}
          </div>
          <div>
            <div style={{ fontSize:'0.95rem', fontWeight:600 }}>{meta.label}</div>
            <div style={{ fontSize:'0.75rem', color:'var(--muted)', marginTop:1, fontFamily:'var(--mono)' }}>{meta.sub}</div>
          </div>
        </div>

        {/* Right: kafka diag btn + status badge + chevron */}
        <div style={{ display:'flex', alignItems:'center', gap:10 }}>
          {isKafka && (
            <button
              onClick={e => { e.stopPropagation(); setDiagBarOpen(o => !o); }}
              style={{
                display:'inline-flex', alignItems:'center', gap:5,
                background:'rgba(0,153,255,0.12)', color:'var(--accent2)',
                border:'1px solid rgba(0,153,255,0.3)', borderRadius:6,
                padding:'4px 10px', fontFamily:'var(--mono)', fontSize:'0.65rem',
                fontWeight:700, cursor:'pointer', marginRight:4,
              }}
              onMouseEnter={e => e.currentTarget.style.background='rgba(0,153,255,0.22)'}
              onMouseLeave={e => e.currentTarget.style.background='rgba(0,153,255,0.12)'}
            >
              🔍 TOPIC DIAGNOSIS
            </button>
          )}

          <div style={{
            display:'flex', alignItems:'center', gap:6,
            fontFamily:'var(--mono)', fontSize:'0.72rem', fontWeight:700,
            padding:'4px 10px', borderRadius:6, letterSpacing:'0.5px',
            background: `rgba(${overall==='ok'?'16,185,129':overall==='warn'?'245,158,11':'239,68,68'},0.12)`,
            color: `var(--${overall==='ok'?'ok':overall==='warn'?'warn':'error'})`,
            border: `1px solid rgba(${overall==='ok'?'16,185,129':overall==='warn'?'245,158,11':'239,68,68'},0.25)`,
          }}>
            <div style={{
              width:7, height:7, borderRadius:'50%',
              background: `var(--${overall==='ok'?'ok':overall==='warn'?'warn':'error'})`,
              animation: LED_ANIM[overall] || 'none',
            }} />
            {STATUS_LABELS[overall]}
          </div>

          <span style={{ color:'var(--muted)', fontSize:'0.7rem', transition:'transform 0.25s', transform: open ? 'rotate(180deg)' : 'none' }}>▼</span>
        </div>
      </div>

      {/* ── Section body ── */}
      {open && (
        <div style={{ padding:'8px 0' }}>

          {/* Topic diag bar (kafka only) */}
          {isKafka && (
            <TopicDiagBar open={diagBarOpen} fetchTopic={fetchTopic} />
          )}

          {/* Check rows */}
          {Object.entries(checks).map(([name, c]) => {
            if (!c || typeof c.status !== 'string' || name === '__details__') return null;
            return (
              <CheckRow
                key={name}
                name={name}
                check={c}
                details={details}
                onRestartClick={() => setRestartModal({ open: true, podName: name, detail: c.detail || '', logs: c.logs || '' })}
                onExpandLive={() => setLivePanelOpen(o => !o)}
                onExpandLag={() => setLagPanelOpen(o => !o)}
                onInspect={openTopicModal}
              />
            );
          })}

          {/* Live data panel */}
          {isKafka && livePanelOpen && (
            <LiveDataPanel details={details} onInspect={openTopicModal} />
          )}

          {/* Consumer lag panel */}
          {isKafka && lagPanelOpen && (
            <ConsumerLagPanel details={details} onInspect={openTopicModal} />
          )}
        </div>
      )}

      {/* ── Restart modal ── */}
      <Modal
        open={restartModal.open}
        onClose={() => setRestartModal(p => ({ ...p, open: false }))}
        title={restartModal.podName}
      >
        <RestartModalBody {...restartModal} />
      </Modal>

      {/* ── Topic inspect modal (from lag/live tables) ── */}
      <Modal
        open={topicModal.open}
        onClose={() => setTopicModal(p => ({ ...p, open: false }))}
        title={`Topic: ${topicModal.name}`}
      >
        {topicModal.data
          ? <TopicDetail data={topicModal.data} />
          : <div style={{ fontFamily:'var(--mono)', fontSize:'0.72rem', color:'var(--muted)' }}>⏳ Loading...</div>
        }
      </Modal>
    </div>
  );
}

function RestartModalBody({ podName, detail, logs }) {
  const rm       = detail.match(/Restarts:\s*(\d+)/);
  const restarts = rm ? rm[1] : '?';
  const reason   = logs.includes('OOMKill') || logs.includes('OutOfMemory') ? 'OOMKilled (exit 137)'
                 : logs.includes('CrashLoop') ? 'CrashLoopBackOff' : 'Check logs below';

  return (
    <>
      <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fill,minmax(160px,1fr))', gap:8, marginBottom:12 }}>
        {[['Restarts', restarts, 'var(--warn)'], ['Last Reason', reason]].map(([l, v, color]) => (
          <div key={l} style={{ background:'var(--surface2)', borderRadius:7, padding:'8px 12px', border:'1px solid var(--border)' }}>
            <div style={{ fontSize:'0.62rem', color:'var(--muted)', textTransform:'uppercase', letterSpacing:'0.5px', marginBottom:3, fontFamily:'var(--mono)' }}>{l}</div>
            <div style={{ fontFamily:'var(--mono)', fontSize:'0.78rem', fontWeight:700, color: color || 'var(--text)', wordBreak:'break-word' }}>{v}</div>
          </div>
        ))}
      </div>
      {logs && (
        <>
          <div style={{ fontFamily:'var(--mono)', fontSize:'0.65rem', fontWeight:700, color:'var(--muted)', marginBottom:4 }}>RECENT LOGS</div>
          <div style={{ background:'#060910', border:'1px solid var(--border)', borderRadius:7, padding:'10px 13px', fontFamily:'var(--mono)', fontSize:'0.65rem', color:'#7eb8f7', maxHeight:160, overflowY:'auto', whiteSpace:'pre-wrap', wordBreak:'break-all', lineHeight:1.6 }}>
            {logs}
          </div>
          <div style={{ marginTop:8, fontSize:'0.65rem', color:'var(--muted)', fontFamily:'var(--mono)' }}>
            💡 Look for ERROR, OOMKilled, or exit code lines above.
          </div>
        </>
      )}
    </>
  );
}
