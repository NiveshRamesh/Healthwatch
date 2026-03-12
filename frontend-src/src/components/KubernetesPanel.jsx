import React from 'react';
import { Badge, ProgressBar, SubSection, Chip, InfoRow } from './Shared';

/* ── Node resource card ───────────────────────────────────────────── */
function NodeCard({ node }) {
  const { node: name, cpu_used_pct, cpu_threshold, memory_used_pct, memory_threshold,
          memory_used_gb, memory_total_gb, cpu_used_cores, cpu_total_cores, status } = node;

  const cpuStatus = cpu_used_pct  >= 90 ? 'critical' : cpu_used_pct  >= cpu_threshold  ? 'warn' : 'ok';
  const memStatus = memory_used_pct >= 90 ? 'critical' : memory_used_pct >= memory_threshold ? 'warn' : 'ok';

  return (
    <div style={{
      margin:'10px 16px', background:'var(--surface2)', borderRadius:10,
      border:`1px solid ${status === 'ok' ? 'var(--border)' : status === 'warn' ? 'rgba(245,158,11,0.3)' : 'rgba(239,68,68,0.3)'}`,
      overflow:'hidden', animation:'fadeIn 0.3s ease',
    }}>
      <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between',
        padding:'10px 14px', borderBottom:'1px solid var(--border)' }}>
        <div style={{ display:'flex', alignItems:'center', gap:8 }}>
          <span style={{ fontSize:'0.9rem' }}>🖥️</span>
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.75rem', fontWeight:700 }}>{name}</span>
        </div>
        <Badge status={status} size="sm" />
      </div>

      <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:0 }}>
        {/* CPU */}
        <div style={{ padding:'12px 14px', borderRight:'1px solid var(--border)' }}>
          <div style={{ display:'flex', justifyContent:'space-between', marginBottom:6 }}>
            <span style={{ fontSize:'0.7rem', fontWeight:600, color:'var(--muted)',
              fontFamily:'var(--mono)', textTransform:'uppercase', letterSpacing:'0.5px' }}>CPU</span>
            <Badge status={cpuStatus} label={`${cpu_used_pct?.toFixed(1)}%`} size="sm" />
          </div>
          <ProgressBar pct={cpu_used_pct} status={cpuStatus} height={6} showLabel={false} />
          <div style={{ display:'flex', justifyContent:'space-between', marginTop:5 }}>
            <span style={{ fontFamily:'var(--mono)', fontSize:'0.6rem', color:'var(--muted)' }}>
              {cpu_used_cores?.toFixed(1)} / {cpu_total_cores} cores
            </span>
            <span style={{ fontFamily:'var(--mono)', fontSize:'0.6rem', color:'var(--muted)' }}>
              thresh {cpu_threshold}%
            </span>
          </div>
        </div>
        {/* Memory */}
        <div style={{ padding:'12px 14px' }}>
          <div style={{ display:'flex', justifyContent:'space-between', marginBottom:6 }}>
            <span style={{ fontSize:'0.7rem', fontWeight:600, color:'var(--muted)',
              fontFamily:'var(--mono)', textTransform:'uppercase', letterSpacing:'0.5px' }}>Memory</span>
            <Badge status={memStatus} label={`${memory_used_pct?.toFixed(1)}%`} size="sm" />
          </div>
          <ProgressBar pct={memory_used_pct} status={memStatus} height={6} showLabel={false} />
          <div style={{ display:'flex', justifyContent:'space-between', marginTop:5 }}>
            <span style={{ fontFamily:'var(--mono)', fontSize:'0.6rem', color:'var(--muted)' }}>
              {memory_used_gb?.toFixed(1)} / {memory_total_gb?.toFixed(1)} GB
            </span>
            <span style={{ fontFamily:'var(--mono)', fontSize:'0.6rem', color:'var(--muted)' }}>
              thresh {memory_threshold}%
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Pod resource table row ───────────────────────────────────────── */
function PodResourceRow({ pod }) {
  const { pod: name, cpu_used_pct, memory_used_pct, status } = pod;
  const cpuStatus = cpu_used_pct  >= 90 ? 'critical' : cpu_used_pct  >= 70 ? 'warn' : 'ok';
  const memStatus = memory_used_pct >= 90 ? 'critical' : memory_used_pct >= 80 ? 'warn' : 'ok';

  return (
    <div style={{
      display:'grid', gridTemplateColumns:'2fr 1fr 1fr 80px',
      alignItems:'center', gap:12,
      padding:'9px 22px 9px 16px', borderBottom:'1px solid rgba(30,45,69,0.4)',
    }}
      onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
      onMouseLeave={e => e.currentTarget.style.background='transparent'}
    >
      <span style={{ fontFamily:'var(--mono)', fontSize:'0.72rem' }}>{name}</span>
      <div>
        <div style={{ display:'flex', justifyContent:'space-between', marginBottom:3 }}>
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.6rem', color:'var(--muted)' }}>CPU</span>
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.6rem',
            color: cpuStatus === 'ok' ? 'var(--ok)' : cpuStatus === 'warn' ? 'var(--warn)' : 'var(--error)' }}>
            {cpu_used_pct?.toFixed(1)}%
          </span>
        </div>
        <ProgressBar pct={cpu_used_pct} status={cpuStatus} height={4} showLabel={false} />
      </div>
      <div>
        <div style={{ display:'flex', justifyContent:'space-between', marginBottom:3 }}>
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.6rem', color:'var(--muted)' }}>MEM</span>
          <span style={{ fontFamily:'var(--mono)', fontSize:'0.6rem',
            color: memStatus === 'ok' ? 'var(--ok)' : memStatus === 'warn' ? 'var(--warn)' : 'var(--error)' }}>
            {memory_used_pct?.toFixed(1)}%
          </span>
        </div>
        <ProgressBar pct={memory_used_pct} status={memStatus} height={4} showLabel={false} />
      </div>
      <div style={{ textAlign:'right' }}>
        <Badge status={status} size="sm" />
      </div>
    </div>
  );
}

/* ── Main Kubernetes Panel ────────────────────────────────────────── */
export default function KubernetesPanel({ checks }) {
  const resources   = checks.__resources__ || {};
  const nodeRes     = resources.node_resources || [];
  const podRes      = resources.pod_resources  || [];

  const podWarn = podRes.filter(p => p.status !== 'ok').length;
  const nodeWarn= nodeRes.filter(n => n.status !== 'ok').length;

  return (
    <>
      {/* Existing pod status checks */}
      {Object.entries(checks).map(([k, v]) => {
        if (k.startsWith('__') || !v?.status) return null;
        return (
          <div key={k} style={{
            display:'flex', alignItems:'center', justifyContent:'space-between',
            padding:'11px 22px 11px 38px', borderBottom:'1px solid rgba(30,45,69,0.5)',
          }}
            onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
            onMouseLeave={e => e.currentTarget.style.background='transparent'}
          >
            <span style={{ fontSize:'0.82rem' }}>{k}</span>
            <div style={{ display:'flex', alignItems:'center', gap:8 }}>
              <span style={{ fontSize:'0.72rem', color:'var(--muted)', fontFamily:'var(--mono)' }}>{v.detail}</span>
              <Badge status={v.status} size="sm" />
            </div>
          </div>
        );
      })}

      {/* Node Resource Checks */}
      {nodeRes.length > 0 && (
        <SubSection
          icon="📈" title="Node Resource Usage"
          defaultOpen={true}
          badge={nodeWarn > 0 ? <Chip n={nodeWarn} color="245,158,11" label="warn" /> : null}
        >
          {nodeRes.map((n, i) => <NodeCard key={i} node={n} />)}
          <div style={{ height:8 }} />
        </SubSection>
      )}

      {/* Pod Resource Checks */}
      {podRes.length > 0 && (
        <SubSection
          icon="📉" title="Pod Resource Usage"
          defaultOpen={true}
          badge={podWarn > 0 ? <Chip n={podWarn} color="245,158,11" label="warn" /> : null}
        >
          {/* header */}
          <div style={{
            display:'grid', gridTemplateColumns:'2fr 1fr 1fr 80px',
            gap:12, padding:'7px 22px 7px 16px',
            borderBottom:'1px solid var(--border)',
          }}>
            {['Pod','CPU','Memory','Status'].map(h => (
              <span key={h} style={{ fontFamily:'var(--mono)', fontSize:'0.6rem',
                color:'var(--muted)', textTransform:'uppercase', letterSpacing:'0.5px',
                textAlign: h === 'Status' ? 'right' : 'left' }}>{h}</span>
            ))}
          </div>
          {podRes.map((p, i) => <PodResourceRow key={i} pod={p} />)}
        </SubSection>
      )}
    </>
  );
}
