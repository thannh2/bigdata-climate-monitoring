import { useState, useEffect, useCallback, useRef } from 'react';
import type { ForecastData } from '../types/weather';
import { fetchForecast } from '../services/mongoService';

interface UseForecastDataResult {
  data: ForecastData | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useForecastData(locationId: string, date?: string, hour?: number): UseForecastDataResult {
  const [data, setData]       = useState<ForecastData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState<string | null>(null);
  const abortRef              = useRef<AbortController | null>(null);

  const load = useCallback(async () => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setData(null);
    setLoading(true);
    setError(null);

    try {
      const result = await fetchForecast(locationId, date, hour);
      if (controller.signal.aborted) return;
      setData(result);
    } catch (err) {
      if (controller.signal.aborted) return;
      // 404 means no forecast data for this selection — not an error worth showing
      if (err instanceof Error && err.message.includes('404')) {
        setData(null);
      } else {
        setError(err instanceof Error ? err.message : 'Failed to load forecast');
      }
    } finally {
      if (!controller.signal.aborted) setLoading(false);
    }
  }, [locationId, date, hour]);

  useEffect(() => { load(); }, [load]);

  return { data, loading, error, refetch: load };
}
