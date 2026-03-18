import React, { useState } from 'react';
import { STATUS_LABELS, ICON_BG, sectionOverallStatus } from '../utils';
import CheckRow from './CheckRow';
import ClickHousePanel from './ClickHousePanel';
import KafkaPanel from './KafkaPanel';
import KubernetesPanel from './KubernetesPanel';
import PodsPVCsPanel from './PodsPVCsPanel';
import MinIOPanel from './MinIOPanel';
import Modal from './Modal';

const LED_ANIM = {
  ok:    'pulse-ok 2s infinite',
  error: 'pulse-err 1s infinite',
};

const STATUS_RGB = {
  ok:   '16,185,129',
  warn: '245,158,11',
  error:'239,68,68',
};

export default function ServiceSection({ svcKey, meta, checks, fetchTopic, bodyProps = {} }) {
  const [open,          setOpen]          = useState(true);
  const [diagModalOpen, setDiagModalOpen] = useState(false);

  const overall = sectionOverallStatus(checks);
  const rgb     = STATUS_RGB[overall] || STATUS_RGB.error;

  // Which panel body to render
  function renderBody() {
    switch (svcKey) {
      case 'clickhouse':
        return <ClickHousePanel checks={checks} />;

      case 'kafka':
        return <KafkaPanel checks={checks} fetchTopic={fetchTopic}
                  diagModalOpen={diagModalOpen} onDiagClose={() => setDiagModalOpen(false)} />;

      case 'kubernetes':
        return <KubernetesPanel checks={checks} />;

      case 'pods_pvcs':
        return <PodsPVCsPanel data={checks.__pods_pvcs__} {...bodyProps} />;

      case 'minio':
        return <MinIOPanel checks={checks} />;

      // postgres — plain check rows
      default:
        return (
          <>
            {Object.entries(checks).map(([name, c]) => {
              if (!c || typeof c.status !== 'string' || name.startsWith('__')) return null;
              return (
                <CheckRow key={name} name={name} check={c} details={checks.__details__}
                  onRestartClick={() => {}}
                  onExpandLive={() => {}}
                  onExpandLag={() => {}}
                  onInspect={() => {}}
                />
              );
            })}
          </>
        );
    }
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
        }}
        onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
        onMouseLeave={e => e.currentTarget.style.background='transparent'}
      >
        <div style={{ display:'flex', alignItems:'center', gap:12 }}>
          <div style={{
            width:36, height:36, borderRadius:10,
            background: ICON_BG[meta.cls] || 'rgba(255,255,255,0.05)',
            display:'flex', alignItems:'center', justifyContent:'center', fontSize:'1.1rem',
          }}>
            {meta.icon}
          </div>
          <div>
            <div style={{ fontSize:'0.95rem', fontWeight:600 }}>{meta.label}</div>
            <div style={{ fontSize:'0.75rem', color:'var(--muted)', marginTop:1, fontFamily:'var(--mono)' }}>
              {meta.sub}
            </div>
          </div>
        </div>

        <div style={{ display:'flex', alignItems:'center', gap:10 }}>

          {svcKey === 'kafka' && (
            <button
              onClick={e => { e.stopPropagation(); setDiagModalOpen(true); }}
              style={{
                display:'inline-flex', alignItems:'center', gap:5,
                background:'rgba(0,153,255,0.12)', color:'var(--accent2)',
                border:'1px solid rgba(0,153,255,0.3)', borderRadius:6,
                padding:'4px 10px', fontFamily:'var(--mono)', fontSize:'0.65rem',
                fontWeight:700, cursor:'pointer',
              }}
            >🔍 TOPIC DIAGNOSIS</button>
          )}

          <div style={{
            display:'flex', alignItems:'center', gap:6,
            fontFamily:'var(--mono)', fontSize:'0.72rem', fontWeight:700,
            padding:'4px 10px', borderRadius:6, letterSpacing:'0.5px',
            background:`rgba(${rgb},0.12)`,
            color:`var(--${overall === 'ok' ? 'ok' : overall === 'warn' ? 'warn' : 'error'})`,
            border:`1px solid rgba(${rgb},0.25)`,
          }}>
            <div style={{
              width:7, height:7, borderRadius:'50%',
              background:`var(--${overall === 'ok' ? 'ok' : overall === 'warn' ? 'warn' : 'error'})`,
              animation: LED_ANIM[overall] || 'none',
            }} />
            {STATUS_LABELS[overall] || overall.toUpperCase()}
          </div>

          <span style={{ color:'var(--muted)', fontSize:'0.7rem', transition:'transform 0.25s',
            transform: open ? 'rotate(180deg)' : 'none' }}>▼</span>
        </div>
      </div>

      {/* ── Body ── */}
      {open && (
        <div style={{ padding:'8px 0', animation:'fadeIn 0.2s ease' }}>
          {renderBody()}
        </div>
      )}
    </div>
  );
}
