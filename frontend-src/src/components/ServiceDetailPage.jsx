import React from 'react';
import { ICON_BG, sectionOverallStatus, STATUS_LABELS } from '../utils';
import { Badge } from './Shared';
import ClickHousePanel from './ClickHousePanel';
import KafkaPanel from './KafkaPanel';
import KubernetesPanel from './KubernetesPanel';
import PodsPVCsPanel from './PodsPVCsPanel';
import MinIOPanel from './MinIOPanel';
import DataRetentionPanel from './DataRetentionPanel';
import CertHealthPanel from './CertHealthPanel';
import CheckRow from './CheckRow';

const STATUS_RGB = { ok: '16,185,129', warn: '245,158,11', error: '239,68,68' };

export default function ServiceDetailPage({ svcKey, meta, checks, fetchTopic, diagModalOpen, onDiagClose, onOpenDiag }) {
  if (!meta || !checks) {
    return (
      <div style={{ textAlign: 'center', padding: '80px 20px', fontFamily: 'var(--mono)', color: 'var(--muted)' }}>
        <div style={{ fontSize: '2rem', marginBottom: 12 }}>🛰️</div>
        <p style={{ fontSize: '0.82rem' }}>No data for this service yet.<br />Run diagnostics to see results.</p>
      </div>
    );
  }

  const overall = sectionOverallStatus(checks);
  const rgb = STATUS_RGB[overall] || STATUS_RGB.error;

  function renderBody() {
    switch (svcKey) {
      case 'clickhouse':
        return <ClickHousePanel checks={checks} />;
      case 'kafka':
        return <KafkaPanel checks={checks} fetchTopic={fetchTopic}
                  diagModalOpen={diagModalOpen} onDiagClose={onDiagClose} />;
      case 'kubernetes':
        return <KubernetesPanel checks={checks} />;
      case 'pods_pvcs':
        return <PodsPVCsPanel data={checks} />;
      case 'minio':
        return <MinIOPanel checks={checks} />;
      case 'data_retention':
        return <DataRetentionPanel checks={checks} />;
      case 'cert_health':
        return <CertHealthPanel checks={checks} />;
      default:
        return (
          <>
            {Object.entries(checks).map(([name, c]) => {
              if (!c || typeof c.status !== 'string' || name.startsWith('__')) return null;
              return (
                <CheckRow key={name} name={name} check={c} details={checks.__details__}
                  onRestartClick={() => {}} onExpandLive={() => {}} onExpandLag={() => {}} onInspect={() => {}}
                />
              );
            })}
          </>
        );
    }
  }

  return (
    <div>
      {/* Page-level header */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        background: 'var(--surface)', border: '1px solid var(--border)',
        borderRadius: 14, padding: '22px 28px', marginBottom: 20,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
          <div style={{
            width: 46, height: 46, borderRadius: 12,
            background: ICON_BG[meta.cls] || 'rgba(255,255,255,0.05)',
            display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '1.4rem',
          }}>{meta.icon}</div>
          <div>
            <h1 style={{ fontSize: '1.2rem', fontWeight: 700, margin: 0 }}>{meta.label}</h1>
            <p style={{ fontSize: '0.78rem', color: 'var(--muted)', margin: '3px 0 0', fontFamily: 'var(--mono)' }}>{meta.sub}</p>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          {svcKey === 'kafka' && onOpenDiag && (
            <button
              onClick={onOpenDiag}
              style={{
                display: 'inline-flex', alignItems: 'center', gap: 5,
                background: 'rgba(0,153,255,0.12)', color: 'var(--accent2)',
                border: '1px solid rgba(0,153,255,0.3)', borderRadius: 6,
                padding: '6px 12px', fontFamily: 'var(--mono)', fontSize: '0.68rem',
                fontWeight: 700, cursor: 'pointer',
              }}
            >🔍 TOPIC DIAGNOSIS</button>
          )}
          <div style={{
            display: 'flex', alignItems: 'center', gap: 6,
            fontFamily: 'var(--mono)', fontSize: '0.78rem', fontWeight: 700,
            padding: '6px 14px', borderRadius: 8,
            background: `rgba(${rgb},0.12)`,
            color: `var(--${overall === 'ok' ? 'ok' : overall === 'warn' ? 'warn' : 'error'})`,
            border: `1px solid rgba(${rgb},0.25)`,
          }}>
            <span style={{
              width: 8, height: 8, borderRadius: '50%',
              background: `var(--${overall === 'ok' ? 'ok' : overall === 'warn' ? 'warn' : 'error'})`,
            }} />
            {STATUS_LABELS[overall] || overall.toUpperCase()}
          </div>
        </div>
      </div>

      {/* Body content */}
      <div style={{
        background: 'var(--surface)', border: '1px solid var(--border)',
        borderRadius: 14, overflow: 'hidden',
      }}>
        <div style={{ padding: '8px 0' }}>
          {renderBody()}
        </div>
      </div>
    </div>
  );
}
