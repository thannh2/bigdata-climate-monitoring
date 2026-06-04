// Base URL for the Express proxy server (server/index.ts)
// Run the server with: cd server && npm install && npx ts-node index.ts
// Or set the VITE_API_URL env var to point to a deployed instance.
const API_BASE = import.meta.env.VITE_API_URL ?? 'http://localhost:3001';

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`API error ${res.status}: ${body}`);
  }
  return res.json() as Promise<T>;
}

// ─── Types ────────────────────────────────────────────────────────────────────

interface HourlyWeatherResponse {
  time: string;
  temperature: number;
  humidity: number;
  windSpeed: number;
  windDir: number;
  pressure: number;
  precipitation: number;
  cloudCover: number;
  soilTemperature: number;
  shortwaveRadiation: number;
  condition: string;
}

interface AirQualityResponse {
  time: string;
  aqi: number;
  pm25: number;
  pm25Acc12h: number;
  isStagnantAir: boolean;
}

// ─── Public API ───────────────────────────────────────────────────────────────

export async function fetchHourlyWeather(locationId: string, date?: string): Promise<HourlyWeatherResponse[]> {
  const q = date ? `?date=${date}` : '';
  return get<HourlyWeatherResponse[]>(`/api/weather/hourly/${locationId}${q}`);
}

export async function fetchAirQuality(locationId: string, date?: string): Promise<AirQualityResponse[]> {
  const q = date ? `?date=${date}` : '';
  return get<AirQualityResponse[]>(`/api/air-quality/${locationId}${q}`);
}

export async function fetchForecast(locationId: string, date?: string, hour?: number) {
  const params = new URLSearchParams();
  if (date) params.set('date', date);
  if (hour !== undefined) params.set('hour', String(hour));
  const q = params.toString() ? `?${params}` : '';
  return get<import('../types/weather').ForecastData>(`/api/forecast/${locationId}${q}`);
}

