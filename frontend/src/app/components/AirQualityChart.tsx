import {
  ComposedChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
  ReferenceLine,
} from "recharts";
import type { AirQualityData } from "../types/weather";

interface AirQualityChartProps {
  data: AirQualityData[];
  selectedEntry?: AirQualityData | null;
}

export function AirQualityChart({
  data,
  selectedEntry,
}: AirQualityChartProps) {
  const display = selectedEntry ?? data[data.length - 1];

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h3 className="text-lg font-semibold text-gray-900 mb-4">
        Air Quality Index (24 Hours)
      </h3>

      <ResponsiveContainer width="100%" height={300}>
        <ComposedChart data={data}>
          <CartesianGrid
            strokeDasharray="3 3"
            stroke="#e5e7eb"
          />
          <XAxis
            dataKey="time"
            stroke="#6b7280"
            tick={{ fontSize: 12 }}
          />
          <YAxis
            stroke="#6b7280"
            tick={{ fontSize: 12 }}
            domain={[0, "auto"]}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: "#fff",
              border: "1px solid #e5e7eb",
              borderRadius: "8px",
            }}
          />
          <Legend />
          <Line
            type="monotone"
            dataKey="pm25"
            stroke="#10b981"
            dot={false}
            strokeWidth={2}
            name="PM2.5 (µg/m³)"
          />
          {display?.time && (
            <ReferenceLine
              x={display.time}
              stroke="#6366f1"
              strokeDasharray="4 3"
              strokeWidth={2}
            />
          )}
        </ComposedChart>
      </ResponsiveContainer>

      <div className="mt-6 grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="p-3 bg-gray-50 rounded-lg">
          <p className="text-xs text-gray-600">PM2.5</p>
          <p className="text-lg font-semibold text-gray-900">
            {display?.pm25.toFixed(2) ?? "—"} µg/m³
          </p>
        </div>
        <div className="p-3 bg-gray-50 rounded-lg">
          <p className="text-xs text-gray-600">
            PM2.5 Acc. (12h)
          </p>
          <p className="text-lg font-semibold text-gray-900">
            {display?.pm25Acc12h.toFixed(2) ?? "—"} µg/m³
          </p>
        </div>
        <div
          className={`p-3 rounded-lg ${display?.isStagnantAir ? "bg-orange-50" : "bg-green-50"}`}
        >
          <p className="text-xs text-gray-600">Stagnant Air</p>
          <p
            className={`text-lg font-semibold ${display?.isStagnantAir ? "text-orange-600" : "text-green-600"}`}
          >
            {display?.isStagnantAir
              ? "Yes — poor dispersion"
              : "No"}
          </p>
        </div>
      </div>
    </div>
  );
}