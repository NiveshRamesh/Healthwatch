import React from 'react';
import { GroupsTooltip } from './Tooltip';
import { fmt } from '../utils';

const thStyle = {
  textAlign: 'left', padding: '7px 14px', color: 'var(--muted)', fontWeight: 700,
  borderBottom: '1px solid var(--border)', fontSize: '0.65rem',
  letterSpacing: '0.5px', textTransform: 'uppercase',
};
const tdStyle = {
  padding: '7px 14px', borderBottom: '1px solid rgba(30,45,69,0.5)',
  color: 'var(--muted)', fontSize: '0.72rem', fontFamily: 'var(--mono)',
  verticalAlign: 'middle',
};

function Badge({ type, children }) {
  const colors = {
    ok:   { bg: 'rgba(16,185,129,0.15)',  color: 'var(--ok)'   },
    high: { bg: 'rgba(239,68,68,0.15)',   color: 'var(--error)'},
  };
  const c = colors[type] || colors.ok;
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', padding: '1px 6px', borderRadius: 3, fontSize: '0.62rem', fontWeight: 700, background: c.bg, color: c.color }}>
      {children}
    </span>
  );
}

export default function ConsumerLagPanel({ details, onInspect }) {
  if (!details?.consumer_lag) return null;

  const rows = Object.entries(details.consumer_lag)
    .sort((a, b) => b[1].total_lag - a[1].total_lag);

  return (
    <div style={{ borderTop: '1px solid var(--border)' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.72rem', fontFamily: 'var(--mono)' }}>
        <thead>
          <tr>
            <th style={{ ...thStyle, width: '18%' }}>Topic</th>
            <th style={{ ...thStyle, width: '12%' }}>Total Lag</th>
            <th style={{ ...thStyle, width: '10%' }}>Max Lag</th>
            <th style={thStyle}>
              Consumer Groups{' '}
              <span style={{ color: 'var(--muted)', fontWeight: 400, fontSize: '0.6rem' }}>(hover for all)</span>
            </th>
            <th style={{ ...thStyle, width: '10%' }}>Status</th>
            <th style={{ ...thStyle, width: '8%' }}></th>
          </tr>
        </thead>
        <tbody>
          {rows.map(([topic, v]) => {
            const isHigh   = v.total_lag > 10000;
            const groupList= Object.keys(v.groups || {});
            const shortStr = groupList.slice(0, 2).join(', ') + (groupList.length > 2 ? `... +${groupList.length - 2} more` : '');
            const fullStr  = groupList.join('\n');

            return (
              <tr key={topic} style={{ cursor: 'default' }}>
                <td style={tdStyle}>{topic}</td>
                <td style={{ ...tdStyle, color: isHigh ? 'var(--warn)' : 'inherit', fontWeight: isHigh ? 700 : 400 }}>
                  {fmt(v.total_lag)}
                </td>
                <td style={tdStyle}>{fmt(v.max_lag)}</td>
                <GroupsTooltip short={shortStr} full={fullStr} />
                <td style={tdStyle}>
                  <Badge type={isHigh ? 'high' : 'ok'}>{isHigh ? 'HIGH LAG' : 'OK'}</Badge>
                </td>
                <td style={tdStyle}>
                  <span
                    onClick={() => onInspect(topic)}
                    style={{ color: 'var(--accent2)', cursor: 'pointer', fontSize: '0.65rem', whiteSpace: 'nowrap' }}
                  >
                    inspect →
                  </span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
