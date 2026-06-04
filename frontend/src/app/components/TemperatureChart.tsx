import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, ReferenceLine } from 'recharts';
import type { HourlyData } from '../types/weather';

interface TemperatureChartProps {
  data: HourlyData[];
  selectedHour?: number;
}

export function TemperatureChart({ data, selectedHour }: TemperatureChartProps) {
  const hourLabel = selectedHour !== undefined ? `${String(selectedHour).padStart(2, '0')}:00` : null;
  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h3 className="text-lg font-semibold text-gray-900 mb-4">Temperature Trends (24 Hours)</h3>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
          <XAxis
            dataKey="time"
            stroke="#6b7280"
            tick={{ fontSize: 12 }}
            tickFormatter={(value) => value.split('-')[0]}
          />
          <YAxis
            stroke="#6b7280"
            tick={{ fontSize: 12 }}
            label={{ value: '°C', angle: -90, position: 'insideLeft' }}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: '#fff',
              border: '1px solid #e5e7eb',
              borderRadius: '8px',
            }}
          />
          <Legend />
          <Line
            type="monotone"
            dataKey="temperature"
            stroke="#ef4444"
            strokeWidth={2}
            dot={false}
            name="Temperature (°C)"
          />
          {hourLabel && (
            <ReferenceLine x={hourLabel} stroke="#3b82f6" strokeDasharray="4 2" label={{ value: hourLabel, position: 'top', fontSize: 11, fill: '#3b82f6' }} />
          )}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
