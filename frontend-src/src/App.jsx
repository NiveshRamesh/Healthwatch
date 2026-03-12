import React from 'react';
import { useHealthWatch } from './hooks/useHealthWatch';
import { SECTIONS_META, sectionOverallStatus } from './utils';
import ServiceSection from './components/ServiceSection';

export default function App() {
  const { data, running, triggerRun, fetchTopic } = useHealthWatch();

  const results = data?.results || {};
  const hasData = Object.keys(results).length > 0;

  let okC = 0, warnC = 0, errC = 0;
  if (hasData) {
    for (const checks of Object.values(results)) {
      const s = sectionOverallStatus(checks);
      if (s === 'ok') okC++; else if (s === 'warn') warnC++; else errC++;
    }
  }

  let overallIcon = '🔍', overallTitle = 'Awaiting diagnostics', overallSub = 'Click "Run Diagnostics" or wait for the scheduled check';
  if (running) { overallIcon = '⏳'; overallTitle = 'Running diagnostics...'; overallSub = 'Checking all services, please wait'; }
  else if (hasData) {
    if (errC > 0)        { overallIcon = '🔴'; overallTitle = `${errC} service${errC>1?'s':''} failing`;         overallSub = 'Immediate attention required'; }
    else if (warnC > 0)  { overallIcon = '🟡'; overallTitle = `${warnC} service${warnC>1?'s':''} with warnings`; overallSub = 'Services degraded — review recommended'; }
    else                 { overallIcon = '🟢'; overallTitle = 'All systems operational';                           overallSub = `${okC+warnC+errC} services healthy — no issues detected`; }
  }

  // Section order — new sections added
  const sectionOrder = ['clickhouse','kafka','postgres','minio','kubernetes','longhorn','pods_pvcs'];

  return (
    <div style={{ position:'relative', zIndex:1, maxWidth:920, margin:'0 auto', padding:'32px 20px 60px' }}>

      {/* ── Header ── */}
      <div style={{ display:'flex', alignItems:'flex-start', justifyContent:'space-between', marginBottom:36, flexWrap:'wrap', gap:16 }}>
        <div>
          <h1 style={{
            fontFamily:'var(--mono)', fontSize:'1.6rem', letterSpacing:'-0.5px',
            background:'linear-gradient(135deg,var(--accent),var(--accent2))',
            WebkitBackgroundClip:'text', WebkitTextFillColor:'transparent',
          }}>
            ⬡ HealthWatch
          </h1>
          <p style={{ fontSize:'0.82rem', color:'var(--muted)', marginTop:4, fontFamily:'var(--mono)' }}>
            Infrastructure Diagnostics · Phase 2 · 22 Checks
          </p>
        </div>
        <div style={{ display:'flex', flexDirection:'column', alignItems:'flex-end', gap:10 }}>
          <button
            onClick={triggerRun} disabled={running}
            style={{
              display:'inline-flex', alignItems:'center', gap:8,
              background:'linear-gradient(135deg,var(--accent),var(--accent2))',
              color:'#0a0e1a', fontFamily:'var(--mono)', fontSize:'0.78rem', fontWeight:700,
              border:'none', borderRadius:8, padding:'10px 20px',
              cursor: running ? 'not-allowed' : 'pointer', opacity: running ? 0.5 : 1,
              letterSpacing:'0.5px',
            }}
          >
            {running ? <><Spinner /> RUNNING...</> : <>▶ RUN DIAGNOSTICS</>}
          </button>
          <div style={{ fontFamily:'var(--mono)', fontSize:'0.72rem', color:'var(--muted)',
            background:'var(--surface)', border:'1px solid var(--border)', padding:'6px 12px', borderRadius:6 }}>
            Last checked: <span style={{ color:'var(--accent)' }}>{data?.last_checked || '—'}</span>
          </div>
        </div>
      </div>

      {/* ── Overall bar ── */}
      <div style={{ display:'flex', alignItems:'center', gap:14, background:'var(--surface)',
        border:'1px solid var(--border)', borderRadius:12, padding:'16px 22px', marginBottom:20 }}>
        <div style={{ fontSize:'1.8rem' }}>{overallIcon}</div>
        <div>
          <h2 style={{ fontSize:'1rem', fontWeight:600 }}>{overallTitle}</h2>
          <p style={{ fontSize:'0.78rem', color:'var(--muted)', marginTop:2, fontFamily:'var(--mono)' }}>{overallSub}</p>
        </div>
      </div>

      {/* ── Summary pills ── */}
      {hasData && (
        <div style={{ display:'flex', gap:10, marginBottom:28, flexWrap:'wrap' }}>
          {[['ok',okC,'Healthy','16,185,129'],['warn',warnC,'Warning','245,158,11'],['error',errC,'Failed','239,68,68']].map(([type,count,label,rgb])=>(
            <div key={type} style={{
              display:'flex', alignItems:'center', gap:6, background:'var(--surface)',
              border:`1px solid rgba(${rgb},0.25)`, borderRadius:20, padding:'6px 14px',
              fontSize:'0.78rem', fontFamily:'var(--mono)',
              color:`var(--${type==='ok'?'ok':type==='warn'?'warn':'error'})`,
            }}>
              <div style={{ width:8, height:8, borderRadius:'50%',
                background:`var(--${type==='ok'?'ok':type==='warn'?'warn':'error'})`,
                boxShadow:`0 0 6px var(--${type==='ok'?'ok':type==='warn'?'warn':'error'})` }} />
              {count} {label}
            </div>
          ))}
          {/* Check count */}
          <div style={{ display:'flex', alignItems:'center', gap:6, background:'var(--surface)',
            border:'1px solid var(--border)', borderRadius:20, padding:'6px 14px',
            fontSize:'0.78rem', fontFamily:'var(--mono)', color:'var(--muted)', marginLeft:'auto' }}>
            22 checks · 7 services
          </div>
        </div>
      )}

      {/* ── Sections ── */}
      {running && !hasData
        ? <Skeletons />
        : !hasData
          ? (
            <div style={{ textAlign:'center', padding:'60px 20px', fontFamily:'var(--mono)', color:'var(--muted)' }}>
              <div style={{ fontSize:'2rem', marginBottom:12 }}>🛰️</div>
              <p style={{ fontSize:'0.82rem' }}>No diagnostic data yet.<br />Run a check to see results.</p>
            </div>
          )
          : sectionOrder.map(key => {
              const checks = results[key];
              if (!checks) return null;
              const meta = SECTIONS_META[key] || { label:key, sub:'', icon:'🔧', cls:'' };
              return (
                <ServiceSection
                  key={key}
                  svcKey={key}
                  meta={meta}
                  checks={checks}
                  fetchTopic={fetchTopic}
                />
              );
            })
      }

      {/* ── Footer ── */}
      <div style={{ textAlign:'center', marginTop:48, fontFamily:'var(--mono)', fontSize:'0.72rem', color:'var(--muted)' }}>
        Auto-scheduled at <span style={{ color:'var(--accent)' }}>08:00</span> &amp; <span style={{ color:'var(--accent)' }}>20:00</span> daily &nbsp;·&nbsp;
        22 checks · Kafka · ClickHouse · Kubernetes · Longhorn · Pods/PVCs · PostgreSQL · MinIO
      </div>
    </div>
  );
}

function Spinner() {
  return <span style={{ width:14, height:14, border:'2px solid rgba(0,0,0,0.3)',
    borderTopColor:'#0a0e1a', borderRadius:'50%', animation:'spin 0.7s linear infinite', display:'inline-block' }} />;
}

function Skeletons() {
  return (
    <>
      {Object.entries(SECTIONS_META).map(([key, meta]) => (
        <div key={key} style={{ background:'var(--surface)', border:'1px solid var(--border)',
          borderRadius:14, marginBottom:16, overflow:'hidden' }}>
          <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', padding:'18px 22px' }}>
            <div style={{ display:'flex', alignItems:'center', gap:12 }}>
              <div style={{ width:36, height:36, borderRadius:10, background:'var(--surface2)', animation:'shimmer 1.5s infinite' }} />
              <div>
                <div style={{ height:14, width:120, background:'var(--surface2)', borderRadius:4, animation:'shimmer 1.5s infinite' }} />
                <div style={{ height:10, width:80, background:'var(--surface2)', borderRadius:4, animation:'shimmer 1.5s infinite', marginTop:4 }} />
              </div>
            </div>
            <div style={{ height:22, width:70, background:'var(--surface2)', borderRadius:6, animation:'shimmer 1.5s infinite' }} />
          </div>
        </div>
      ))}
    </>
  );
}
