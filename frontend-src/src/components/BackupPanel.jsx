import React, { useState, useEffect, useCallback } from 'react';
import { Badge, Chip } from './Shared';

const BASE = '/healthwatch';

/* ── Service backup result card ──────────────────────────────────── */
function ServiceCard({ name, data }) {
  const [open, setOpen] = useState(false);
  if (!data) return null;
  const isOk = data.status === 'ok';
  const rgb = isOk ? '16,185,129' : '239,68,68';

  return (
    <div style={{
      background: 'var(--surface2)', border: `1px solid rgba(${rgb},0.2)`,
      borderRadius: 10, overflow: 'hidden',
    }}>
      <div onClick={() => setOpen(o => !o)} style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '12px 14px', cursor: 'pointer',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '0.85rem' }}>
            {name === 'postgresql' ? '🐘' : name === 'clickhouse' ? '🏠' :
             name === 'minio' ? '🪣' : name === 'k8s_objects' ? '☸️' : '📦'}
          </span>
          <span style={{ fontSize: '0.76rem', fontWeight: 700, color: 'var(--text)' }}>
            {data.service || name}
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '0.68rem', fontFamily: 'var(--mono)', fontWeight: 700,
                         color: 'var(--accent2)' }}>{data.total_size || '0B'}</span>
          <Badge status={isOk ? 'ok' : 'error'} size="sm" />
          <span style={{ color: 'var(--muted)', fontSize: '0.55rem', transition: 'transform 0.2s',
                         transform: open ? 'rotate(180deg)' : 'none' }}>▼</span>
        </div>
      </div>
      {open && (
        <div style={{ borderTop: '1px solid var(--border)', padding: '8px 14px',
                      fontSize: '0.62rem', fontFamily: 'var(--mono)', color: 'var(--muted)',
                      animation: 'fadeIn 0.2s ease' }}>
          {data.databases && data.databases.map((db, i) => (
            <div key={i} style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0' }}>
              <span>{db.database || db.bucket || db.type}</span>
              <span style={{ color: db.status === 'ok' ? 'var(--ok)' : 'var(--error)' }}>
                {db.size || db.error || `${db.count || 0} items`}
              </span>
            </div>
          ))}
          {data.buckets && data.buckets.map((b, i) => (
            <div key={i} style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0' }}>
              <span>{b.bucket}</span>
              <span style={{ color: b.status === 'ok' ? 'var(--ok)' : 'var(--error)' }}>
                {b.size || b.error} ({b.objects || 0} objects)
              </span>
            </div>
          ))}
          {data.resource_types && data.resource_types.map((r, i) => (
            <div key={i} style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0' }}>
              <span>{r.type}</span>
              <span style={{ color: r.status === 'ok' ? 'var(--ok)' : 'var(--error)' }}>
                {r.count} objects
              </span>
            </div>
          ))}
          {data.error && <div style={{ color: 'var(--error)' }}>{data.error}</div>}
        </div>
      )}
    </div>
  );
}

/* ── Backup history card ─────────────────────────────────────────── */
function BackupHistoryCard({ backup, isLatest }) {
  const [open, setOpen] = useState(false);
  const services = backup.services || {};

  return (
    <div style={{
      background: 'var(--surface2)', borderRadius: 10, overflow: 'hidden',
      border: isLatest ? '1px solid rgba(0,212,170,0.25)' : '1px solid var(--border)',
    }}>
      <div onClick={() => setOpen(o => !o)} style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '10px 14px', cursor: 'pointer',
        background: isLatest ? 'rgba(0,212,170,0.04)' : 'transparent',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {isLatest && <span style={{ fontSize: '0.5rem', fontWeight: 700, padding: '1px 5px',
                                      borderRadius: 3, background: 'rgba(0,212,170,0.15)',
                                      color: 'var(--accent)', fontFamily: 'var(--mono)' }}>LATEST</span>}
          <span style={{ fontSize: '0.72rem', fontFamily: 'var(--mono)', fontWeight: 600, color: 'var(--text)' }}>
            {backup.name}
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '0.62rem', fontFamily: 'var(--mono)', color: 'var(--accent2)', fontWeight: 700 }}>
            {backup.actual_size || backup.total_size || '?'}
          </span>
          <a href={`${BASE}/api/backup/download/${backup.name}`}
             onClick={e => e.stopPropagation()}
             style={{ fontSize: '0.55rem', fontFamily: 'var(--mono)', fontWeight: 700,
                      padding: '2px 8px', borderRadius: 4, textDecoration: 'none',
                      background: 'rgba(0,153,255,0.12)', color: 'var(--accent2)',
                      border: '1px solid rgba(0,153,255,0.25)' }}>
            DOWNLOAD
          </a>
          <span style={{ color: 'var(--muted)', fontSize: '0.55rem', transition: 'transform 0.2s',
                         transform: open ? 'rotate(180deg)' : 'none' }}>▼</span>
        </div>
      </div>
      {open && Object.keys(services).length > 0 && (
        <div style={{ borderTop: '1px solid var(--border)', padding: '8px 10px',
                      display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6 }}>
          {Object.entries(services).map(([key, svc]) => (
            <ServiceCard key={key} name={key} data={svc} />
          ))}
        </div>
      )}
    </div>
  );
}

/* ── Main Backup Panel ───────────────────────────────────────────── */
export default function BackupPanel({ checks }) {
  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState(null);
  const [backups, setBackups] = useState([]);
  const [backupDir, setBackupDir] = useState('');

  const fetchBackups = useCallback(async () => {
    try {
      const res = await fetch(`${BASE}/api/backup/list`);
      const data = await res.json();
      setBackups(data.backups || []);
      setBackupDir(data.backup_dir || '');
    } catch (e) {
      console.error('Failed to fetch backups:', e);
    }
  }, []);

  useEffect(() => { fetchBackups(); }, [fetchBackups]);

  // Poll progress while running
  useEffect(() => {
    if (!running) return;
    const interval = setInterval(async () => {
      try {
        const res = await fetch(`${BASE}/api/backup/status`);
        const data = await res.json();
        setProgress(data.progress);
        if (!data.running) {
          setRunning(false);
          fetchBackups();
        }
      } catch (e) {}
    }, 2000);
    return () => clearInterval(interval);
  }, [running, fetchBackups]);

  const triggerBackup = async () => {
    setRunning(true);
    setProgress({ current: 'starting', services: {} });
    try {
      await fetch(`${BASE}/api/backup/trigger`, { method: 'POST' });
    } catch (e) {
      setRunning(false);
    }
  };

  const completedServices = progress ? Object.keys(progress.services || {}) : [];
  const currentService = progress?.current || '';

  return (
    <>
      {/* Trigger section */}
      <div style={{ padding: '16px 20px' }}>
        <div style={{
          background: 'var(--surface2)', border: '1px solid var(--border)', borderRadius: 12,
          padding: '16px 20px',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
            <div>
              <div style={{ fontSize: '0.85rem', fontWeight: 700, color: 'var(--text)' }}>Cluster Backup</div>
              <div style={{ fontSize: '0.62rem', fontFamily: 'var(--mono)', color: 'var(--muted)', marginTop: 2 }}>
                PostgreSQL · ClickHouse · MinIO · K8s Objects
              </div>
            </div>
            <button onClick={triggerBackup} disabled={running} style={{
              display: 'inline-flex', alignItems: 'center', gap: 6,
              background: running ? 'var(--surface3)' : 'rgba(0,212,170,0.12)',
              color: running ? 'var(--muted)' : 'var(--accent)',
              border: `1px solid ${running ? 'var(--border)' : 'rgba(0,212,170,0.3)'}`,
              borderRadius: 8, padding: '8px 16px', fontFamily: 'var(--mono)',
              fontSize: '0.72rem', fontWeight: 700, cursor: running ? 'not-allowed' : 'pointer',
            }}>
              {running ? '⏳ Backing up...' : '▶ Run Backup'}
            </button>
          </div>

          {/* Progress */}
          {running && progress && (
            <div style={{ marginTop: 8 }}>
              {['postgresql', 'clickhouse', 'minio', 'k8s_objects'].map(svc => {
                const done = completedServices.includes(svc);
                const isCurrent = currentService === svc;
                const result = progress.services?.[svc];
                const icon = done ? (result?.status === 'ok' ? '✓' : '✕') : isCurrent ? '⏳' : '○';
                const color = done ? (result?.status === 'ok' ? 'var(--ok)' : 'var(--error)') :
                              isCurrent ? 'var(--accent)' : 'var(--muted)';
                const label = { postgresql: 'PostgreSQL', clickhouse: 'ClickHouse',
                                minio: 'MinIO', k8s_objects: 'K8s Objects' }[svc];
                return (
                  <div key={svc} style={{ display: 'flex', alignItems: 'center', gap: 8,
                                          padding: '4px 0', opacity: !done && !isCurrent ? 0.4 : 1 }}>
                    <span style={{ fontSize: '0.7rem', color, fontWeight: 700, width: 16, textAlign: 'center' }}>
                      {icon}
                    </span>
                    <span style={{ fontSize: '0.68rem', fontFamily: 'var(--mono)',
                                   fontWeight: isCurrent ? 700 : 400, color: 'var(--text)' }}>{label}</span>
                    {done && result?.size && (
                      <span style={{ fontSize: '0.58rem', fontFamily: 'var(--mono)', color: 'var(--muted)',
                                     marginLeft: 'auto' }}>{result.size}</span>
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {/* Storage path */}
          {backupDir && (
            <div style={{ marginTop: 10, fontSize: '0.58rem', fontFamily: 'var(--mono)', color: 'var(--muted)' }}>
              Storage: {backupDir}
            </div>
          )}
        </div>
      </div>

      {/* Backup history */}
      {backups.length > 0 && (
        <div style={{ padding: '0 20px 16px' }}>
          <div style={{ fontSize: '0.72rem', fontWeight: 700, color: 'var(--text)', marginBottom: 8,
                        display: 'flex', alignItems: 'center', gap: 8 }}>
            Backup History
            <Chip n={backups.length} color="100,116,139" label="backups" />
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {backups.map((b, i) => (
              <BackupHistoryCard key={b.name} backup={b} isLatest={i === 0} />
            ))}
          </div>
        </div>
      )}
    </>
  );
}
