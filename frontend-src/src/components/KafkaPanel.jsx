import React, { useState } from 'react';
import { Badge, SubSection, Chip } from './Shared';
import { STATUS_ICONS } from '../utils';
import TopicDiagBar from './TopicDiagBar';
import LiveDataPanel from './LiveDataPanel';
import ConsumerLagPanel from './ConsumerLagPanel';
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

/* ── Connector row — inline in Kafka section ─────────────────────── */
function ConnectorSubPanel({ connectors }) {
  if (!connectors) return null;
  const { pod_status, active = [], required = [], missing = [], status } = connectors;

  return (
    <SubSection
      icon="🔌" title="Kafka Connectors"
      defaultOpen={true}
      badge={
        <span style={{ display:'flex', gap:4 }}>
          {missing.length > 0 && <Chip n={missing.length} color="239,68,68" label="missing" />}
          <Badge status={status} size="sm" />
        </span>
      }
    >
      {/* Pod status */}
      <div style={{ padding:'8px 22px 8px 24px', borderBottom:'1px solid rgba(30,45,69,0.4)',
        display:'flex', alignItems:'center', gap:8 }}>
        <span style={{ fontSize:'0.75rem', color:'var(--muted)' }}>cp-kafka-connect pod</span>
        <span style={{ fontFamily:'var(--mono)', fontSize:'0.65rem',
          color: pod_status === 'Running' ? 'var(--ok)' : 'var(--error)',
          background: pod_status === 'Running' ? 'rgba(16,185,129,0.1)' : 'rgba(239,68,68,0.1)',
          border: pod_status === 'Running' ? '1px solid rgba(16,185,129,0.3)' : '1px solid rgba(239,68,68,0.3)',
          borderRadius:4, padding:'1px 7px' }}>
          {pod_status}
        </span>
      </div>

      {/* Active connectors */}
      {active.map((c, i) => {
        const isMissing  = false; // active = present
        const isRequired = required.includes(c);
        return (
          <div key={i} style={{
            display:'flex', alignItems:'center', justifyContent:'space-between',
            padding:'8px 22px 8px 24px', borderBottom:'1px solid rgba(30,45,69,0.3)',
          }}
            onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
            onMouseLeave={e => e.currentTarget.style.background='transparent'}
          >
            <div style={{ display:'flex', alignItems:'center', gap:7 }}>
              <span style={{ fontFamily:'var(--mono)', fontSize:'0.72rem' }}>{c}</span>
              {isRequired && (
                <span style={{ fontFamily:'var(--mono)', fontSize:'0.58rem', color:'var(--accent)',
                  background:'rgba(0,212,170,0.1)', border:'1px solid rgba(0,212,170,0.25)',
                  borderRadius:3, padding:'0px 5px' }}>required</span>
              )}
            </div>
            <Badge status="ok" label="ACTIVE" size="sm" />
          </div>
        );
      })}

      {/* Missing connectors */}
      {missing.map((c, i) => (
        <div key={i} style={{
          display:'flex', alignItems:'center', justifyContent:'space-between',
          padding:'8px 22px 8px 24px', borderBottom:'1px solid rgba(30,45,69,0.3)',
          background:'rgba(239,68,68,0.04)',
        }}>
          <div style={{ display:'flex', alignItems:'center', gap:7 }}>
            <span style={{ fontFamily:'var(--mono)', fontSize:'0.72rem', color:'var(--error)' }}>{c}</span>
            <span style={{ fontFamily:'var(--mono)', fontSize:'0.58rem', color:'var(--error)',
              background:'rgba(239,68,68,0.1)', border:'1px solid rgba(239,68,68,0.25)',
              borderRadius:3, padding:'0px 5px' }}>required · missing</span>
          </div>
          <Badge status="error" label="MISSING" size="sm" />
        </div>
      ))}
    </SubSection>
  );
}

/* ── Main Kafka Panel ────────────────────────────────────────────── */
export default function KafkaPanel({ checks, fetchTopic }) {
  const [diagBarOpen,    setDiagBarOpen]   = useState(false);
  const [livePanelOpen,  setLivePanelOpen] = useState(false);
  const [lagPanelOpen,   setLagPanelOpen]  = useState(false);
  const [topicModal,     setTopicModal]    = useState({ open:false, name:'', data:null });

  const details    = checks.__details__;
  const connectors = checks['Kafka Connectors']?._connectors;

  function openTopicModal(name) {
    setTopicModal({ open:true, name, data:null });
    fetchTopic(name).then(d => setTopicModal(p => ({ ...p, data:d })));
  }

  return (
    <>
      {/* Topic diag bar */}
      <TopicDiagBar open={diagBarOpen} fetchTopic={fetchTopic} />

      {/* Standard check rows */}
      {Object.entries(checks).map(([k, v]) => {
        if (k.startsWith('__') || !v?.status || k === 'Kafka Connectors') return null;
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

      {/* Connector check — new panel inline */}
      <ConnectorSubPanel connectors={connectors} />

      {/* Topic modal */}
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
