import React from 'react';
import { Badge, ProgressBar, SubSection, Chip } from './Shared';

function VolumeRow({ vol }) {
  const { name, pod, pvc, state, ready, actual_gb, csize_gb, used_pct, status } = vol;
  return (
    <div style={{
      padding:'10px 22px 10px 16px', borderBottom:'1px solid rgba(30,45,69,0.4)',
      animation:'fadeIn 0.3s ease',
    }}
      onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
      onMouseLeave={e => e.currentTarget.style.background='transparent'}
    >
      <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:6 }}>
        <div style={{ display:'flex', alignItems:'center', gap:8 }}>
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.7rem', color:'var(--accent2)' }}>
            {name?.slice(-6)}
          </span>
          <span style={{ fontSize:'0.72rem', color:'var(--muted)' }}>→</span>
          <span style={{ fontSize:'0.75rem', color:'var(--text)' }}>{pod}</span>
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.62rem', color:'var(--muted)',
            background:'var(--surface3)', borderRadius:4, padding:'1px 6px' }}>{pvc}</span>
        </div>
        <div style={{ display:'flex', alignItems:'center', gap:6 }}>
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.65rem',
            color: state === 'attached' && ready ? 'var(--ok)' : 'var(--error)' }}>
            {state === 'attached' && ready ? '⬤ attached' : `⬤ ${state}`}
          </span>
          <Badge status={status} size="sm" />
        </div>
      </div>
      <div style={{ display:'flex', alignItems:'center', gap:12 }}>
        <div style={{ flex:1 }}>
          <ProgressBar pct={used_pct} status={status} height={5} />
        </div>
        <span style={{ fontFamily:'var(--mono)', fontSize:'0.62rem', color:'var(--muted)', whiteSpace:'nowrap' }}>
          {actual_gb?.toFixed(1)}G / {csize_gb?.toFixed(1)}G
        </span>
      </div>
    </div>
  );
}

function NodeDiskRow({ n }) {
  const { node, disk, path, scheduled_gb, available_gb, used_pct, status } = n;
  return (
    <div style={{
      padding:'10px 22px 10px 16px', borderBottom:'1px solid rgba(30,45,69,0.4)',
    }}
      onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
      onMouseLeave={e => e.currentTarget.style.background='transparent'}
    >
      <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:6 }}>
        <div style={{ display:'flex', alignItems:'center', gap:8 }}>
          <span style={{ fontSize:'0.75rem', fontWeight:600 }}>{node}</span>
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.62rem', color:'var(--muted)',
            background:'var(--surface3)', borderRadius:4, padding:'1px 6px' }}>{path}</span>
        </div>
        <Badge status={status} size="sm" />
      </div>
      <div style={{ display:'flex', alignItems:'center', gap:12 }}>
        <div style={{ flex:1 }}>
          <ProgressBar pct={used_pct} status={status} height={5} />
        </div>
        <span style={{ fontFamily:'var(--mono)', fontSize:'0.62rem', color:'var(--muted)', whiteSpace:'nowrap' }}>
          {scheduled_gb?.toFixed(0)}G scheduled / {available_gb?.toFixed(0)}G available
        </span>
      </div>
    </div>
  );
}

export default function LonghornPanel({ data }) {
  if (!data) return null;
  const { volumes = [], nodes = [] } = data;

  const volWarn     = volumes.filter(v => v.status === 'warn').length;
  const volCritical = volumes.filter(v => v.status === 'critical').length;
  const nodeWarn    = nodes.filter(n => n.status === 'warn').length;

  return (
    <>
      <SubSection
        icon="📀" title="Longhorn Volumes"
        defaultOpen={true}
        badge={
          <span style={{ display:'flex', gap:4 }}>
            {volCritical > 0 && <Chip n={volCritical} color="239,68,68"  label="critical" />}
            {volWarn     > 0 && <Chip n={volWarn}     color="245,158,11" label="warn" />}
            <Chip n={volumes.length} color="100,116,139" label="volumes" />
          </span>
        }
      >
        {volumes.length === 0
          ? <div style={{ padding:'12px 22px', fontSize:'0.75rem', color:'var(--muted)' }}>No volumes found</div>
          : volumes.map((v,i) => <VolumeRow key={i} vol={v} />)
        }
      </SubSection>

      <SubSection
        icon="🖴" title="Longhorn Node Disks"
        defaultOpen={true}
        badge={nodeWarn > 0 ? <Chip n={nodeWarn} color="245,158,11" label="warn" /> : null}
      >
        {nodes.length === 0
          ? <div style={{ padding:'12px 22px', fontSize:'0.75rem', color:'var(--muted)' }}>No nodes found</div>
          : nodes.map((n,i) => <NodeDiskRow key={i} n={n} />)
        }
      </SubSection>
    </>
  );
}
