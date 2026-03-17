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

const STATUS_SORT = { critical:0, error:0, warn:1, ok:2 };

/* ── Pod resource table row — compact single-line ─────────────────── */
function PodResourceRow({ pod }) {
  const { pod: name, cpu_used_pct, memory_used_pct, status } = pod;
  const cpuStatus = cpu_used_pct  >= 90 ? 'critical' : cpu_used_pct  >= 70 ? 'warn' : 'ok';
  const memStatus = memory_used_pct >= 90 ? 'critical' : memory_used_pct >= 80 ? 'warn' : 'ok';
  const cpuColor  = cpuStatus === 'ok' ? 'var(--ok)' : cpuStatus === 'warn' ? 'var(--warn)' : 'var(--error)';
  const memColor  = memStatus === 'ok' ? 'var(--ok)' : memStatus === 'warn' ? 'var(--warn)' : 'var(--error)';

  return (
    <div style={{
      display:'grid', gridTemplateColumns:'1.8fr 1fr 1fr 56px',
      alignItems:'center', gap:8,
      padding:'6px 14px 6px 16px', borderBottom:'1px solid rgba(30,45,69,0.4)',
    }}
      onMouseEnter={e => e.currentTarget.style.background='var(--surface2)'}
      onMouseLeave={e => e.currentTarget.style.background='transparent'}
    >
      <span style={{ fontFamily:'var(--mono)', fontSize:'0.68rem',
        overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{name}</span>

      <div style={{ display:'flex', alignItems:'center', gap:4 }}>
        <span style={{ fontFamily:'var(--mono)', fontSize:'0.55rem', color:'var(--muted)', minWidth:22 }}>CPU</span>
        <div style={{ flex:1, height:4, background:'var(--surface3)', borderRadius:999, overflow:'hidden', minWidth:28 }}>
          <div style={{ height:'100%', width:`${Math.min(100,cpu_used_pct||0)}%`, background:cpuColor, borderRadius:999 }} />
        </div>
        <span style={{ fontFamily:'var(--mono)', fontSize:'0.6rem', color:cpuColor, minWidth:34, textAlign:'right' }}>
          {(cpu_used_pct||0).toFixed(1)}%
        </span>
      </div>

      <div style={{ display:'flex', alignItems:'center', gap:4 }}>
        <span style={{ fontFamily:'var(--mono)', fontSize:'0.55rem', color:'var(--muted)', minWidth:22 }}>MEM</span>
        <div style={{ flex:1, height:4, background:'var(--surface3)', borderRadius:999, overflow:'hidden', minWidth:28 }}>
          <div style={{ height:'100%', width:`${Math.min(100,memory_used_pct||0)}%`, background:memColor, borderRadius:999 }} />
        </div>
        <span style={{ fontFamily:'var(--mono)', fontSize:'0.6rem', color:memColor, minWidth:34, textAlign:'right' }}>
          {(memory_used_pct||0).toFixed(1)}%
        </span>
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
          {[...podRes]
            .sort((a, b) => {
              const sa = STATUS_SORT[a.status] ?? 2;
              const sb = STATUS_SORT[b.status] ?? 2;
              if (sa !== sb) return sa - sb;
              return (b.cpu_used_pct + b.memory_used_pct) - (a.cpu_used_pct + a.memory_used_pct);
            })
            .map((p, i) => <PodResourceRow key={i} pod={p} />)
          }
        </SubSection>
      )}
    </>
  );
}
