import React, { useState } from 'react';
import AnalyzeModal from './AnalyzeModal';

export default function AnalyzeButton({ checkName, section, status, detail, data }) {
  const [show, setShow] = useState(false);
  const canAnalyze = ['error', 'critical', 'warn', 'warning'].includes(status?.toLowerCase?.());
  if (!canAnalyze) return null;

  return (
    <>
      <span
        onClick={(e) => { e.stopPropagation(); setShow(true); }}
        title="AI-powered RCA and fix suggestions"
        style={{
          display: 'inline-flex', alignItems: 'center', gap: 2,
          fontSize: '0.5rem', fontFamily: 'var(--mono)', fontWeight: 700,
          padding: '1px 5px', borderRadius: 4, cursor: 'pointer',
          border: '1px solid var(--border)', color: 'var(--muted)',
          background: 'transparent', whiteSpace: 'nowrap', flexShrink: 0,
          transition: 'all 0.15s', lineHeight: 1.4,
        }}
        onMouseEnter={e => { e.currentTarget.style.borderColor = 'var(--accent)'; e.currentTarget.style.color = 'var(--accent)'; }}
        onMouseLeave={e => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.color = 'var(--muted)'; }}
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
