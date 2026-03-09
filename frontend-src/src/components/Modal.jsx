import React, { useEffect } from 'react';

export default function Modal({ open, onClose, title, children }) {
  useEffect(() => {
    const handler = (e) => { if (e.key === 'Escape') onClose(); };
    if (open) document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [open, onClose]);

  if (!open) return null;

  const styles = {
    overlay: {
      position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.7)',
      zIndex: 500, display: 'flex', alignItems: 'center', justifyContent: 'center',
      backdropFilter: 'blur(3px)',
    },
    modal: {
      background: 'var(--surface)', border: '1px solid var(--border)',
      borderRadius: 14, width: '90%', maxWidth: 640, maxHeight: '85vh', overflowY: 'auto',
    },
    header: {
      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      padding: '16px 20px', borderBottom: '1px solid var(--border)',
      position: 'sticky', top: 0, background: 'var(--surface)',
    },
    title: { fontFamily: 'var(--mono)', fontSize: '0.82rem', fontWeight: 700 },
    close: { cursor: 'pointer', fontSize: 16, color: 'var(--muted)' },
    body:  { padding: '18px 20px' },
  };

  return (
    <div style={styles.overlay} onClick={(e) => e.target === e.currentTarget && onClose()}>
      <div style={styles.modal}>
        <div style={styles.header}>
          <div style={styles.title}>{title}</div>
          <div style={styles.close} onClick={onClose}>✕</div>
        </div>
        <div style={styles.body}>{children}</div>
      </div>
    </div>
  );
}
