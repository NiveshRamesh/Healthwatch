import React, { useState } from 'react';
import { useHealthWatch } from './hooks/useHealthWatch';
import { SECTIONS_META } from './utils';
import Sidebar from './components/Sidebar';
import OverviewPage from './components/OverviewPage';
import ServiceDetailPage from './components/ServiceDetailPage';

export default function App() {
  const { data, running, triggerRun, fetchTopic } = useHealthWatch();
  const [activePage, setActivePage] = useState('overview');
  const [diagModalOpen, setDiagModalOpen] = useState(false);

  const results = data?.results || {};
  const hasData = Object.keys(results).length > 0;

  return (
    <div style={{ display: 'flex', minHeight: '100vh' }}>
      {/* Fixed sidebar */}
      <Sidebar
        activePage={activePage}
        onNavigate={setActivePage}
        results={results}
        running={running}
        triggerRun={triggerRun}
        lastChecked={data?.last_checked}
      />

      {/* Main content */}
      <main style={{ marginLeft: 220, flex: 1, padding: '32px 40px 60px', maxWidth: 1180 }}>
        {running && !hasData ? (
          <Skeletons />
        ) : !hasData ? (
          <EmptyState />
        ) : activePage === 'overview' ? (
          <OverviewPage results={results} running={running} onNavigate={setActivePage} />
        ) : (
          <ServiceDetailPage
            svcKey={activePage}
            meta={SECTIONS_META[activePage]}
            checks={results[activePage]}
            fetchTopic={fetchTopic}
            diagModalOpen={diagModalOpen}
            onDiagClose={() => setDiagModalOpen(false)}
            onOpenDiag={() => setDiagModalOpen(true)}
          />
        )}

        {/* Footer */}
        <div style={{ textAlign: 'center', marginTop: 48, fontFamily: 'var(--mono)', fontSize: '0.68rem', color: 'var(--muted)' }}>
          Auto-scheduled at <span style={{ color: 'var(--accent)' }}>08:00</span> &amp; <span style={{ color: 'var(--accent)' }}>20:00</span> daily
        </div>
      </main>
    </div>
  );
}

function EmptyState() {
  return (
    <div style={{ textAlign: 'center', padding: '80px 20px', fontFamily: 'var(--mono)', color: 'var(--muted)' }}>
      <div style={{ fontSize: '2.5rem', marginBottom: 16 }}>🛰️</div>
      <p style={{ fontSize: '0.9rem' }}>No diagnostic data yet.</p>
      <p style={{ fontSize: '0.78rem', marginTop: 6 }}>Click <strong>Run Diagnostics</strong> in the sidebar to start.</p>
    </div>
  );
}

function Skeletons() {
  return (
    <>
      {Object.entries(SECTIONS_META).map(([key, meta]) => (
        <div key={key} style={{ background: 'var(--surface)', border: '1px solid var(--border)',
          borderRadius: 14, marginBottom: 16, overflow: 'hidden' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '18px 22px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div style={{ width: 36, height: 36, borderRadius: 10, background: 'var(--surface2)', animation: 'shimmer 1.5s infinite' }} />
              <div>
                <div style={{ height: 14, width: 120, background: 'var(--surface2)', borderRadius: 4, animation: 'shimmer 1.5s infinite' }} />
                <div style={{ height: 10, width: 80, background: 'var(--surface2)', borderRadius: 4, animation: 'shimmer 1.5s infinite', marginTop: 4 }} />
              </div>
            </div>
            <div style={{ height: 22, width: 70, background: 'var(--surface2)', borderRadius: 6, animation: 'shimmer 1.5s infinite' }} />
          </div>
        </div>
      ))}
    </>
  );
}
