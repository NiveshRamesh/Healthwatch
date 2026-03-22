import React, { useState } from 'react';
import { DetailTooltip } from './Tooltip';
import { STATUS_ICONS } from '../utils';
import AnalyzeModal from './AnalyzeModal';

const rowStyle = {
  display:'flex', alignItems:'center', justifyContent:'space-between',
  padding:'12px 22px 12px 70px', borderBottom:'1px solid rgba(30,45,69,0.5)',
  animation:'fadeIn 0.3s ease', cursor:'default',
};

function ExpandBtn({ cls, onClick, children }) {
  const themes = {
    warn: { color:'var(--warn)',   border:'rgba(245,158,11,0.4)',  bg:'rgba(245,158,11,0.08)',  hover:'rgba(245,158,11,0.18)' },
    blue: { color:'var(--accent2)', border:'rgba(0,153,255,0.4)', bg:'rgba(0,153,255,0.08)',   hover:'rgba(0,153,255,0.15)'  },
  };
  const t = themes[cls] || themes.blue;
  const [hov, setHov] = useState(false);
  return (
    <span
      onClick={onClick}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
      style={{
        display:'inline-flex', alignItems:'center', gap:4,
        fontSize:'0.7rem', fontFamily:'var(--mono)', fontWeight:700,
        padding:'2px 8px', borderRadius:5, cursor:'pointer',
        border:`1px solid ${t.border}`, color:t.color,
        background: hov ? t.hover : t.bg,
        whiteSpace:'nowrap', flexShrink:0, transition:'background 0.15s',
      }}
    >{children}</span>
  );
}

export default function CheckRow({ name, check, details, onRestartClick, onExpandLive, onExpandLag, onInspect, section }) {
  const [showAnalyze, setShowAnalyze] = useState(false);
  const { status, detail = '', logs } = check;
  const icon = STATUS_ICONS[status] || '⬜';
  const canAnalyze = ['error', 'critical', 'warn'].includes(status);

  // Extract restart count
  const restartMatch = detail.match(/Restarts:\s*(\d+)/);
  const restarts     = restartMatch ? parseInt(restartMatch[1]) : 0;

  // Pod row: show only if Running + restarts, or if non-running show logs btn
  const isPod       = name.startsWith('Pod:');
  const isRunning   = detail.includes('Running');
  const showLogBtn  = isPod && logs && !isRunning; // only if NOT running
  const showRestart = isPod && restarts > 0;

  // Live data counts
  const tls     = details?.topic_live_status;
  const live    = tls ? Object.values(tls).filter(t =>  t.is_live).length : 0;
  const stale   = tls ? Object.values(tls).filter(t =>  t.has_data && !t.is_live).length : 0;
  const empty   = tls ? Object.values(tls).filter(t => !t.has_data).length : 0;
  const showLive= name === 'Live Data' && tls;

  // Consumer lag high count
  const lagMap    = details?.consumer_lag;
  const highCount = lagMap ? Object.values(lagMap).filter(v => v.total_lag > 10000).length : 0;
  const showLag   = name === 'Consumer Lag' && lagMap;

  return (
    <div style={rowStyle} onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'} onMouseLeave={e => e.currentTarget.style.background='transparent'}>
      <span style={{ fontSize:'0.88rem', color:'var(--text)' }}>{name}</span>
      <div style={{ display:'flex', alignItems:'center', gap:10 }}>
        <DetailTooltip text={detail} />

        {showRestart && (
          <span
            onClick={onRestartClick}
            style={{
              display:'inline-flex', alignItems:'center', gap:4, padding:'2px 7px',
              borderRadius:4, background:'rgba(245,158,11,0.12)', color:'var(--warn)',
              fontSize:'0.65rem', cursor:'pointer', border:'1px solid rgba(245,158,11,0.3)',
              fontFamily:'var(--mono)', flexShrink:0,
            }}
          >↺ {restarts} restarts — why?</span>
        )}

        {showLogBtn && (
          <ExpandBtn cls="warn" onClick={onExpandLive}>📋 logs ▾</ExpandBtn>
        )}

        {showLive && (
          <ExpandBtn cls="blue" onClick={onExpandLive}>
            📊 {live} live · {stale} stale · {empty} empty ▾
          </ExpandBtn>
        )}

        {showLag && highCount > 0 && (
          <ExpandBtn cls="warn" onClick={onExpandLag}>
            ⚠ {highCount} high lag ▾
          </ExpandBtn>
        )}

        {canAnalyze && (
          <span
            onClick={() => setShowAnalyze(true)}
            title="AI-powered RCA and fix suggestions"
            style={{
              display:'inline-flex', alignItems:'center', justifyContent:'center',
              width:20, height:20, borderRadius:'50%', cursor:'pointer',
              border:'1px solid var(--border)', color:'var(--muted)',
              background:'transparent', flexShrink:0,
              transition:'all 0.15s', fontSize:'0.6rem',
            }}
            onMouseEnter={e => { e.currentTarget.style.borderColor='var(--accent)'; e.currentTarget.style.color='var(--accent)'; e.currentTarget.style.background='rgba(0,212,170,0.1)'; }}
            onMouseLeave={e => { e.currentTarget.style.borderColor='var(--border)'; e.currentTarget.style.color='var(--muted)'; e.currentTarget.style.background='transparent'; }}
          >🔍</span>
        )}

        <span style={{ fontSize:'1rem', color:`var(--${status === 'ok' ? 'ok' : status === 'warn' ? 'warn' : status === 'error' ? 'error' : 'unknown'})` }}>
          {icon}
        </span>
      </div>

      {showAnalyze && (
        <AnalyzeModal
          checkName={name}
          section={section || ''}
          status={status}
          detail={detail}
          data={check}
          onClose={() => setShowAnalyze(false)}
        />
      )}
    </div>
  );
}
