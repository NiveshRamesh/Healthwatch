import React, { useState } from 'react';
import { statusColor, statusRgb, STATUS_LABELS } from '../utils';

/* ── Status Badge ─────────────────────────────────────────────────── */
export function Badge({ status, label, size = 'md' }) {
  const s   = status || 'unknown';
  const rgb = statusRgb(s);
  const sz  = size === 'sm' ? { fontSize:'0.62rem', padding:'2px 7px' }
                             : { fontSize:'0.72rem', padding:'4px 10px' };
  return (
    <span style={{
      display:'inline-flex', alignItems:'center', gap:5,
      fontFamily:'var(--mono)', fontWeight:700, letterSpacing:'0.5px',
      borderRadius:6, border:`1px solid rgba(${rgb},0.3)`,
      background:`rgba(${rgb},0.1)`, color:`var(--${s==='ok'?'ok':s==='warn'?'warn':'error'})`,
      ...sz,
    }}>
      <span style={{ width:6, height:6, borderRadius:'50%', background:`var(--${s==='ok'?'ok':s==='warn'?'warn':'error'})`,
        animation: s === 'ok' ? 'pulse-ok 2s infinite' : s === 'error' || s === 'critical' ? 'pulse-err 1s infinite' : 'none'
      }} />
      {label || STATUS_LABELS[s] || s.toUpperCase()}
    </span>
  );
}

/* ── Progress Bar ─────────────────────────────────────────────────── */
export function ProgressBar({ pct, status, height = 6, showLabel = true }) {
  const color = statusColor(status);
  const w     = Math.min(100, Math.max(0, pct || 0));
  return (
    <div style={{ display:'flex', alignItems:'center', gap:8, width:'100%' }}>
      <div style={{ flex:1, height, background:'var(--surface3)', borderRadius:999, overflow:'hidden' }}>
        <div style={{
          height:'100%', width:`${w}%`, background:color, borderRadius:999,
          transition:'width 0.6s ease', boxShadow:`0 0 6px ${color}44`,
        }} />
      </div>
      {showLabel && (
        <span style={{ fontFamily:'var(--mono)', fontSize:'0.68rem', color, width:36, textAlign:'right', flexShrink:0 }}>
          {w.toFixed(1)}%
        </span>
      )}
    </div>
  );
}

/* ── Expandable Section within a panel ───────────────────────────── */
export function SubSection({ title, icon, badge, children, defaultOpen = true }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div style={{ borderTop:'1px solid var(--border)', marginTop:0 }}>
      <div
        onClick={() => setOpen(o => !o)}
        style={{
          display:'flex', alignItems:'center', justifyContent:'space-between',
          padding:'10px 22px', cursor:'pointer', userSelect:'none',
        }}
        onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
        onMouseLeave={e => e.currentTarget.style.background='transparent'}
      >
        <div style={{ display:'flex', alignItems:'center', gap:8 }}>
          <span style={{ fontSize:'0.85rem' }}>{icon}</span>
          <span style={{ fontSize:'0.82rem', fontWeight:600, color:'var(--text)' }}>{title}</span>
          {badge}
        </div>
        <span style={{ color:'var(--muted)', fontSize:'0.7rem', transition:'transform 0.2s',
          transform: open ? 'rotate(180deg)' : 'none' }}>▼</span>
      </div>
      {open && <div style={{ animation:'fadeIn 0.2s ease' }}>{children}</div>}
    </div>
  );
}

/* ── Info Row ─────────────────────────────────────────────────────── */
export function InfoRow({ label, value, status, mono = false }) {
  return (
    <div style={{
      display:'flex', alignItems:'center', justifyContent:'space-between',
      padding:'9px 22px 9px 38px', borderBottom:'1px solid rgba(30,45,69,0.5)',
    }}
      onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
      onMouseLeave={e => e.currentTarget.style.background='transparent'}
    >
      <span style={{ fontSize:'0.82rem', color:'var(--text)' }}>{label}</span>
      <span style={{
        fontSize:'0.75rem', color: status ? statusColor(status) : 'var(--muted)',
        fontFamily: mono ? 'var(--mono)' : 'var(--sans)',
      }}>{value}</span>
    </div>
  );
}

/* ── Small count chip ─────────────────────────────────────────────── */
export function Chip({ n, color, label }) {
  if (!n) return null;
  return (
    <span style={{
      display:'inline-flex', alignItems:'center', gap:4,
      background:`rgba(${color},0.12)`, border:`1px solid rgba(${color},0.3)`,
      color:`rgb(${color})`, borderRadius:12, padding:'1px 7px',
      fontFamily:'var(--mono)', fontSize:'0.62rem', fontWeight:700,
    }}>
      {n} {label}
    </span>
  );
}

/* ── Tooltip ──────────────────────────────────────────────────────── */
export function Tip({ text }) {
  const [vis, setVis] = useState(false);
  if (!text) return null;
  return (
    <span style={{ position:'relative', display:'inline-flex' }}
      onMouseEnter={() => setVis(true)} onMouseLeave={() => setVis(false)}>
      <span style={{ fontSize:'0.7rem', color:'var(--muted)', cursor:'help', padding:'0 4px' }}>ⓘ</span>
      {vis && (
        <span style={{
          position:'absolute', bottom:'calc(100% + 6px)', right:0,
          background:'#0d1526', border:'1px solid var(--border)', borderRadius:7,
          padding:'6px 10px', fontSize:'0.7rem', color:'var(--muted)',
          whiteSpace:'nowrap', zIndex:100, maxWidth:300, fontFamily:'var(--mono)',
          boxShadow:'0 4px 16px rgba(0,0,0,0.5)',
        }}>{text}</span>
      )}
    </span>
  );
}
