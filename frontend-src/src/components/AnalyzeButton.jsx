import React, { useState } from 'react';
import AnalyzeModal from './AnalyzeModal';

export default function AnalyzeButton({ checkName, section, status, detail, data, style }) {
  const [show, setShow] = useState(false);
  const canAnalyze = ['error', 'critical', 'warn', 'warning'].includes(status?.toLowerCase?.());
  if (!canAnalyze) return null;

  return (
    <>
      <span
        onClick={(e) => { e.stopPropagation(); setShow(true); }}
        title="AI-powered RCA and fix suggestions"
        style={{
          display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
          width: 18, height: 18, borderRadius: '50%', cursor: 'pointer',
          border: '1px solid var(--border)', color: 'var(--muted)',
          background: 'transparent', flexShrink: 0,
          transition: 'all 0.15s', fontSize: '0.55rem',
          ...style,
        }}
        onMouseEnter={e => { e.currentTarget.style.borderColor = 'var(--accent)'; e.currentTarget.style.color = 'var(--accent)'; e.currentTarget.style.background = 'rgba(0,212,170,0.1)'; }}
        onMouseLeave={e => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.color = 'var(--muted)'; e.currentTarget.style.background = 'transparent'; }}
      >🔍</span>
      {show && (
        <AnalyzeModal
          checkName={checkName}
          section={section || ''}
          status={status}
          detail={detail || ''}
          data={data || {}}
          onClose={() => setShow(false)}
        />
      )}
    </>
  );
}
