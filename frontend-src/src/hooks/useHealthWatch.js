import { useState, useEffect, useRef, useCallback } from 'react';

const BASE = '/healthwatch';

export function useHealthWatch() {
  const [data,    setData]    = useState(null);
  const [running, setRunning] = useState(false);
  const pollRef               = useRef(null);

  const fetchStatus = useCallback(async () => {
    try {
      const res  = await fetch(`${BASE}/api/status`);
      const json = await res.json();
      setData(json);
      setRunning(json.is_running || false);
      return json;
    } catch (e) { console.error('fetchStatus error', e); }
  }, []);

  const stopPolling  = useCallback(() => {
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
  }, []);

  const startPolling = useCallback(() => {
    stopPolling();
    pollRef.current = setInterval(async () => {
      const d = await fetchStatus();
      if (d && !d.is_running) { stopPolling(); setRunning(false); }
    }, 1500);
  }, [fetchStatus, stopPolling]);

  const triggerRun = useCallback(async () => {
    setRunning(true);
    await fetch(`${BASE}/api/run`, { method:'POST' });
    startPolling();
  }, [startPolling]);

  const fetchTopic = useCallback(async (name) => {
    const res = await fetch(`${BASE}/api/topic/${encodeURIComponent(name)}`);
    return res.json();
  }, []);

  useEffect(() => {
    fetchStatus().then(d => { if (d?.is_running) startPolling(); });
    const passive = setInterval(() => { if (!pollRef.current) fetchStatus(); }, 30000);
    return () => { clearInterval(passive); stopPolling(); };
  }, [fetchStatus, startPolling, stopPolling]);

  return { data, running, triggerRun, fetchTopic };
}
