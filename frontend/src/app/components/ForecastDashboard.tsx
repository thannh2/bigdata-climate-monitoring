import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend, BarChart, Bar,
} from 'recharts';
import type { ForecastData, ForecastHour } from '../types/weather';
import { ForecastAdvisories } from './ForecastAdvisories';

interface ForecastDashboardProps {
  data: ForecastData;
  locationName: string;
}

function fmt(v: number | null, decimals = 1): string {
  return v === null ? '—' : v.toFixed(decimals);
}

function cloudLabel(v: number | null): string {
  if (v === null) return '—';
  if (v <= 10) return 'Clear';
  if (v <= 30) return 'Mostly Clear';
  if (v <= 60) return 'Partly Cloudy';
  if (v <= 85) return 'Mostly Cloudy';
  return 'Overcast';
}

function pm25Category(v: number | null): { label: string; color: string } {
  if (v === null) return { label: '—', color: 'text-gray-400' };
  if (v <= 12)    return { label: 'Good',        color: 'text-green-600' };
  if (v <= 35.4)  return { label: 'Moderate',    color: 'text-yellow-600' };
  if (v <= 55.4)  return { label: 'Unhealthy (Sensitive)', color: 'text-orange-500' };
  if (v <= 150.4) return { label: 'Unhealthy',   color: 'text-red-600' };
  return           { label: 'Very Unhealthy',    color: 'text-purple-700' };
}

// Build chart data: base hour (actual) + h+1 to h+6 (forecast)
function buildChartData(current: ForecastData['current'], hours: ForecastHour[], baseTime: string) {
  return [
    {
      time: baseTime,
      temperature:   current.temperature,
      precipitation: current.precipitation,
      pressure:      current.pressure,
      windSpeed:     current.windSpeed,
      pm25:          current.pm25,
      cloudCover:    current.cloudCover,
    },
    ...hours.map((h) => ({
      time:          h.time,
      temperature:   h.temperature,
      precipitation: h.precipitation,
      pressure:      h.pressure,
      windSpeed:     h.windSpeed,
      pm25:          h.pm25,
      cloudCover:    h.cloudCover,
    })),
  ];
}

export function ForecastDashboard({ data, locationName }: ForecastDashboardProps) {
  const chartData = buildChartData(data.current, data.hours, data.baseTime);
  const mockTime = `${String(new Date().getHours()).padStart(2, '0')}:00`;
  
  return (
    <div className="space-y-6">
      {/* Base info banner */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg px-5 py-3 flex flex-wrap items-center gap-x-6 gap-y-1 text-sm text-blue-800">
        <span>Forecast base: <strong>{locationName}</strong></span>
        <span>Reference hour: <strong>{mockTime}</strong></span>
        <span className="text-blue-500 italic">Values for +1h through +6h ahead</span>
      </div>

      {/* Hourly forecast cards */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
        {data.hours.map((h) => {
          const pm25 = pm25Category(h.pm25);
          const mockFuture = `${String(new Date().getHours() + h.hour).padStart(2, '0')}:00`;
          return (
            <div key={h.hour} className="bg-white rounded-lg shadow p-4 flex flex-col gap-2">
              <div className="flex items-center justify-between">
                <span className="text-xs font-medium text-gray-500">+{h.hour}h</span>
                {/*<span className="text-sm font-semibold text-gray-800">{h.time}</span>*/}
                <span className="text-sm font-semibold text-gray-800">{mockFuture}</span>
              </div>
              <div className="text-2xl font-bold text-gray-900 text-center py-1">
                {h.temperature !== null ? `${fmt(h.temperature)} °C` : '—'}
              </div>
              <div className="space-y-1 text-xs text-gray-600">
                <div className="flex justify-between">
                  <span>Wind</span>
                  <span className="font-medium">{fmt(h.windSpeed)} m/s</span>
                </div>
                <div className="flex justify-between">
                  <span>Precip.</span>
                  <span className="font-medium">{fmt(h.precipitation)} mm</span>
                </div>
                <div className="flex justify-between">
                  <span>Pressure</span>
                  <span className="font-medium">{fmt(h.pressure, 0)} hPa</span>
                </div>
                <div className="flex justify-between">
                  <span>Cloud</span>
                  <span className="font-medium">{cloudLabel(h.cloudCover)}</span>
                </div>
                <div className="flex justify-between border-t border-gray-100 pt-1">
                  <span>PM2.5</span>
                  <span className={`font-semibold ${pm25.color}`}>{fmt(h.pm25)} µg/m³</span>
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Advisories */}
      <ForecastAdvisories hours={data.hours} />

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Temperature forecast */}
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-base font-semibold text-gray-900 mb-4">Temperature Forecast (°C)</h3>
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="time" tick={{ fontSize: 12 }} stroke="#6b7280" />
              <YAxis tick={{ fontSize: 12 }} stroke="#6b7280" />
              <Tooltip contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '8px' }} />
              <Line type="monotone" dataKey="temperature" stroke="#ef4444" strokeWidth={2} dot={{ r: 4 }} name="Temp (°C)" />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* PM2.5 forecast */}
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-base font-semibold text-gray-900 mb-4">PM2.5 Forecast (µg/m³)</h3>
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="time" tick={{ fontSize: 12 }} stroke="#6b7280" />
              <YAxis tick={{ fontSize: 12 }} stroke="#6b7280" />
              <Tooltip contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '8px' }} />
              <Line type="monotone" dataKey="pm25" stroke="#10b981" strokeWidth={2} dot={{ r: 4 }} name="PM2.5 (µg/m³)" />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Wind speed forecast */}
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-base font-semibold text-gray-900 mb-4">Wind Speed Forecast (m/s)</h3>
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="time" tick={{ fontSize: 12 }} stroke="#6b7280" />
              <YAxis tick={{ fontSize: 12 }} stroke="#6b7280" />
              <Tooltip contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '8px' }} />
              <Line type="monotone" dataKey="windSpeed" stroke="#6366f1" strokeWidth={2} dot={{ r: 4 }} name="Wind (m/s)" />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Precipitation forecast */}
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-base font-semibold text-gray-900 mb-4">Precipitation Forecast (mm)</h3>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="time" tick={{ fontSize: 12 }} stroke="#6b7280" />
              <YAxis tick={{ fontSize: 12 }} stroke="#6b7280" />
              <Tooltip contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '8px' }} />
              <Bar dataKey="precipitation" fill="#3b82f6" name="Precip. (mm)" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Pressure trend */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-base font-semibold text-gray-900 mb-4">Pressure Forecast (hPa)</h3>
        <ResponsiveContainer width="100%" height={200}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis dataKey="time" tick={{ fontSize: 12 }} stroke="#6b7280" />
            <YAxis tick={{ fontSize: 12 }} stroke="#6b7280" domain={['auto', 'auto']} />
            <Tooltip contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '8px' }} />
            <Legend />
            <Line type="monotone" dataKey="pressure" stroke="#f59e0b" strokeWidth={2} dot={{ r: 4 }} name="Pressure (hPa)" />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
