import React, { useState } from 'react';
import { fmt } from '../utils';

const thStyle = { textAlign:'left', padding:'7px 14px', color:'var(--muted)', fontWeight:700, borderBottom:'1px solid var(--border)', fontSize:'0.65rem', letterSpacing:'0.5px', textTransform:'uppercase' };
const tdStyle = { padding:'7px 14px', borderBottom:'1px solid rgba(30,45,69,0.5)', color:'var(--muted)', fontSize:'0.72rem', fontFamily:'var(--mono)' };

const BADGE = {
  live:  { bg:'rgba(16,185,129,0.15)',  color:'var(--ok)'      },
  stale: { bg:'rgba(245,158,11,0.15)',  color:'var(--warn)'    },
  empty: { bg:'rgba(107,114,128,0.15)', color:'var(--unknown)' },
};

function Badge({ type }) {
  const c = BADGE[type];
  return <span style={{ display:'inline-flex', alignItems:'center', padding:'1px 6px', borderRadius:3, fontSize:'0.62rem', fontWeight:700, background:c.bg, color:c.color }}>{type.toUpperCase()}</span>;
}

function TopicTable({ entries, type, onInspect }) {
  if (!entries.length)
    return <div style={{ padding:'12px 16px', color:'var(--muted)', fontFamily:'var(--mono)', fontSize:'0.72rem' }}>No {type} topics</div>;
  return (
    <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.72rem', fontFamily:'var(--mono)' }}>
      <thead><tr>
        <th style={thStyle}>Topic</th>
        <th style={thStyle}>Messages</th>
        <th style={thStyle}>Status</th>
        <th style={thStyle}></th>
      </tr></thead>
      <tbody>
        {entries.map(([topic, v]) => (
          <tr key={topic}>
            <td style={tdStyle}>{topic}</td>
            <td style={tdStyle}>{fmt(v.total_messages)}</td>
            <td style={tdStyle}><Badge type={type} /></td>
            <td style={tdStyle}><span onClick={() => onInspect(topic)} style={{ color:'var(--accent2)', cursor:'pointer', fontSize:'0.65rem' }}>inspect →</span></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default function LiveDataPanel({ details, onInspect }) {
  const [activeTab, setActiveTab] = useState('stale');
  if (!details?.topic_live_status) return null;

  const tls   = details.topic_live_status;
  const live  = Object.entries(tls).filter(([,v]) =>  v.is_live);
  const stale = Object.entries(tls).filter(([,v]) =>  v.has_data && !v.is_live);
  const empty = Object.entries(tls).filter(([,v]) => !v.has_data);

  const tabs = [
    { id:'live',  label:`🟢 Live (${live.length})`,   entries: live  },
    { id:'stale', label:`🟡 Stale (${stale.length})`, entries: stale },
    { id:'empty', label:`⚪ Empty (${empty.length})`,  entries: empty },
  ];

  return (
    <div style={{ borderTop:'1px solid var(--border)', background:'var(--surface2)' }}>
      <div style={{ display:'flex', gap:2, padding:'10px 14px 0', borderBottom:'1px solid var(--border)' }}>
        {tabs.map(t => (
          <div key={t.id}
            onClick={() => setActiveTab(t.id)}
            style={{
              padding:'5px 12px', fontSize:'0.7rem', fontFamily:'var(--mono)',
              cursor:'pointer', borderRadius:'5px 5px 0 0',
              color: activeTab===t.id ? 'var(--text)' : 'var(--muted)',
              background: activeTab===t.id ? 'var(--surface)' : 'transparent',
              border: activeTab===t.id ? '1px solid var(--border)' : '1px solid transparent',
              borderBottom: activeTab===t.id ? '1px solid var(--surface)' : 'none',
              marginBottom: -1,
            }}
          >{t.label}</div>
        ))}
      </div>
      {tabs.map(t => t.id===activeTab && (
        <div key={t.id}><TopicTable entries={t.entries} type={t.id} onInspect={onInspect} /></div>
      ))}
    </div>
  );
}
