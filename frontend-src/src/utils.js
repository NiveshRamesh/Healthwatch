export const STATUS_ICONS  = { ok: '✅', warn: '⚠️', error: '❌', unknown: '⬜' };
export const STATUS_LABELS = { ok: 'HEALTHY', warn: 'WARNING', error: 'FAILED', unknown: 'UNKNOWN' };

export const SECTIONS_META = {
  clickhouse: { label: 'ClickHouse',        sub: 'Database · Query Engine',       icon: '🗄️', cls: 'clickhouse' },
  kafka:      { label: 'Kafka & Zookeeper', sub: 'Message Broker · Streaming',    icon: '📨', cls: 'kafka'      },
  postgres:   { label: 'PostgreSQL',        sub: 'App DB · Metadata Store',       icon: '🐘', cls: 'postgres'   },
  minio:      { label: 'MinIO',             sub: 'Object Storage · Config Store', icon: '🪣', cls: 'minio'      },
  kubernetes: { label: 'Kubernetes',        sub: 'Orchestration · Pod Health',    icon: '☸️', cls: 'kubernetes' },
};

export function sectionOverallStatus(checks) {
  const statuses = Object.values(checks)
    .filter(c => c && typeof c.status === 'string')
    .map(c => c.status);
  if (statuses.includes('error'))   return 'error';
  if (statuses.includes('warn'))    return 'warn';
  if (statuses.includes('unknown')) return 'unknown';
  return 'ok';
}

export function fmt(n) {
  return (n || 0).toLocaleString();
}
