import React from 'react';

const style = {
  fontSize: '0.75rem', color: 'var(--muted)', fontFamily: 'var(--mono)',
  maxWidth: 320, textAlign: 'right', whiteSpace: 'nowrap',
  overflow: 'hidden', textOverflow: 'ellipsis',
  cursor: 'default', position: 'relative',
};

// Injects CSS once
let injected = false;
function injectCSS() {
  if (injected) return;
  injected = true;
  const s = document.createElement('style');
  s.textContent = `
    .hw-tooltip:hover::after {
      content: attr(data-full);
      position: absolute; right: 0; top: calc(100% + 6px);
      background: var(--surface2); border: 1px solid var(--border);
      border-radius: 8px; padding: 8px 12px;
      font-size: 0.72rem; color: var(--text);
      white-space: pre-wrap; word-break: break-word;
      max-width: 480px; min-width: 200px;
      box-shadow: 0 8px 28px rgba(0,0,0,0.55);
      z-index: 999; line-height: 1.6; text-align: left; pointer-events: none;
    }
    .hw-groups-tooltip:hover::after {
      content: attr(data-full);
      position: absolute; left: 0; top: calc(100% + 4px);
      background: var(--surface2); border: 1px solid var(--border);
      border-radius: 8px; padding: 8px 12px;
      font-size: 0.68rem; color: var(--text);
      white-space: pre-wrap; word-break: break-word;
      max-width: 340px; min-width: 160px;
      box-shadow: 0 8px 28px rgba(0,0,0,0.55);
      z-index: 999; line-height: 1.8; text-align: left; pointer-events: none;
    }
  `;
  document.head.appendChild(s);
}

export function DetailTooltip({ text }) {
  injectCSS();
  return <span className="hw-tooltip" style={style} data-full={text}>{text}</span>;
}

export function GroupsTooltip({ short, full }) {
  injectCSS();
  return (
    <td
      className="hw-groups-tooltip"
      data-full={full}
      style={{
        maxWidth: 180, whiteSpace: 'nowrap', overflow: 'hidden',
        textOverflow: 'ellipsis', cursor: 'default', position: 'relative',
        padding: '7px 14px', borderBottom: '1px solid rgba(30,45,69,0.5)',
        color: 'var(--muted)', fontSize: '0.72rem', fontFamily: 'var(--mono)',
      }}
    >
      {short}
    </td>
  );
}
