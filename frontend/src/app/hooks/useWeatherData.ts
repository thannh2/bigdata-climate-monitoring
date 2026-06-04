import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import type { WeatherData, HourlyData, AirQualityData, CurrentWeather } from '../types/weather';
import { fetchHourlyWeather, fetchAirQuality } from '../services/mongoService';

interface UseWeatherDataResult {
  data: WeatherData | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

function hourlyToCurrentWeather(h: HourlyData): CurrentWeather {
  return {
    temperature:        h.temperature,
    humidity:           h.humidity,
    windSpeed:          h.windSpeed,
    windDir:            h.windDir,
    pressure:           h.pressure,
    precipitation:      h.precipitation,
    cloudCover:         h.cloudCover,
    soilTemperature:    h.soilTemperature,
    shortwaveRadiation: h.shortwaveRadiation,
    condition:          h.condition,
  };
}

// Finds the hourly entry whose time label is closest to `hour` (0-23).
function pickHour(hourly: HourlyData[], hour: number): HourlyData | null {
  if (!hourly.length) return null;
  const target = `${String(hour).padStart(2, '0')}:00`;
  // Exact match first
  const exact = hourly.find((h) => h.time === target);
  if (exact) return exact;
  // Otherwise nearest by hour number
  return hourly.reduce((best, h) => {
    const hNum = parseInt(h.time.slice(0, 2), 10);
    const bNum = parseInt(best.time.slice(0, 2), 10);
    return Math.abs(hNum - hour) < Math.abs(bNum - hour) ? h : best;
  });
}

// date: "YYYY-MM-DD" for a specific day, or undefined for the live last-24h window.
// hour: 0-23 — selects which hour's snapshot to show as "current conditions".
//       Changing hour does NOT re-fetch; data is derived from the cached hourly array.
export function useWeatherData(locationId: string, date?: string, hour?: number): UseWeatherDataResult {
  const [hourlyCache, setHourlyCache]         = useState<HourlyData[] | null>(null);
  const [airQualityCache, setAirQualityCache] = useState<AirQualityData[] | null>(null);
  const [loading, setLoading]                 = useState(true);
  const [error, setError]                     = useState<string | null>(null);
  const abortRef                              = useRef<AbortController | null>(null);

  // Only re-fetch when location or date changes — NOT when hour changes.
  const load = useCallback(async () => {
    // Cancel any in-flight request so stale responses can't overwrite new ones.
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    // Clear stale cache immediately so old data never bleeds into the new view.
    setHourlyCache(null);
    setAirQualityCache(null);
    setLoading(true);
    setError(null);

    try {
      const [hourly, airQuality] = await Promise.all([
        fetchHourlyWeather(locationId, date),
        fetchAirQuality(locationId, date),
      ]);
      if (controller.signal.aborted) return;
      setHourlyCache(hourly);
      setAirQualityCache(airQuality);
    } catch (err) {
      if (controller.signal.aborted) return;
      setError(err instanceof Error ? err.message : 'Failed to load weather data');
    } finally {
      if (!controller.signal.aborted) setLoading(false);
    }
  }, [locationId, date]);

  useEffect(() => { load(); }, [load]);

  // Derive the current-conditions snapshot from the cached array — zero latency.
  const data = useMemo<WeatherData | null>(() => {
    if (!hourlyCache || !airQualityCache) return null;

    const selectedHourly =
      hour !== undefined
        ? pickHour(hourlyCache, hour)
        : hourlyCache[hourlyCache.length - 1] ?? null;

    if (!selectedHourly) return null;

    return {
      current:    hourlyToCurrentWeather(selectedHourly),
      hourly:     hourlyCache,
      airQuality: airQualityCache,
    };
  }, [hourlyCache, airQualityCache, hour]);

  return { data, loading, error, refetch: load };
}
