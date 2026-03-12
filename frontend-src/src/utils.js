export const STATUS_ICONS  = { ok:'✅', warn:'⚠️', error:'❌', critical:'🔴', unknown:'⬜' };
export const STATUS_LABELS = { ok:'HEALTHY', warn:'WARNING', error:'FAILED', critical:'CRITICAL', unknown:'UNKNOWN' };

export const SECTIONS_META = {
  clickhouse: { label:'ClickHouse',         sub:'Database · Query Engine',         icon:'🗄️',  cls:'clickhouse' },
  kafka:      { label:'Kafka & Zookeeper',  sub:'Message Broker · Streaming',      icon:'📨',  cls:'kafka'      },
  postgres:   { label:'PostgreSQL',         sub:'App DB · Metadata Store',         icon:'🐘',  cls:'postgres'   },
  minio:      { label:'MinIO',              sub:'Object Storage · Config Store',   icon:'🪣',  cls:'minio'      },
  kubernetes: { label:'Kubernetes',         sub:'Orchestration · Node Resources',  icon:'☸️',  cls:'kubernetes' },
  longhorn:   { label:'Longhorn Storage',   sub:'Persistent Volumes · Disk Health',icon:'💾',  cls:'longhorn'   },
  pods_pvcs:  { label:'Pods & PVCs',        sub:'Container Health · Volume Claims',icon:'📦',  cls:'pods_pvcs'  },
};

export const ICON_BG = {
  clickhouse: 'rgba(255,183,0,0.12)',
  kafka:      'rgba(0,153,255,0.12)',
  postgres:   'rgba(51,102,204,0.12)',
  minio:      'rgba(198,53,40,0.12)',
  kubernetes: 'rgba(50,108,229,0.12)',
  longhorn:   'rgba(0,212,170,0.12)',
  pods_pvcs:  'rgba(139,92,246,0.12)',
};

export function sectionOverallStatus(checks) {
  const all = [];
  // Check standard check rows
  for (const [k, v] of Object.entries(checks)) {
    if (k.startsWith('__')) continue;
    if (v && typeof v.status === 'string') all.push(v.status);
  }
  // Check nested __data__ blobs
  const chTables   = checks.__ch_tables__;
  const resources  = checks.__resources__;
  const longhorn   = checks.__longhorn__;
  const podsPvcs   = checks.__pods_pvcs__;
  for (const blob of [chTables, resources, longhorn, podsPvcs]) {
    if (blob?.status) all.push(blob.status);
  }
  if (all.includes('critical') || all.includes('error')) return 'error';
  if (all.includes('warn'))    return 'warn';
  if (all.includes('unknown')) return 'unknown';
  return 'ok';
}

export function statusColor(s) {
  return s === 'ok' ? 'var(--ok)' : s === 'warn' ? 'var(--warn)' : s === 'critical' ? 'var(--critical)' : 'var(--error)';
}

export function statusRgb(s) {
  return s === 'ok' ? '16,185,129' : s === 'warn' ? '245,158,11' : '239,68,68';
}

export function fmt(n) { return (n||0).toLocaleString(); }
