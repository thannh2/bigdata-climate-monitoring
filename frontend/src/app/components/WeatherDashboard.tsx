import { CurrentWeather } from './CurrentWeather';
import { TemperatureChart } from './TemperatureChart';
import { HumidityChart } from './HumidityChart';
import { AirQualityChart } from './AirQualityChart';
import { WeatherMetrics } from './WeatherMetrics';
import { WeatherAlerts } from './WeatherAlerts';
import type { WeatherData, AirQualityData } from '../types/weather';

interface WeatherDashboardProps {
  data: WeatherData;
  locationName: string;
  selectedHour?: number;
}

function pickAirQualityForHour(airQuality: AirQualityData[], hour?: number): AirQualityData | null {
  if (!airQuality.length) return null;
  if (hour === undefined) return airQuality[airQuality.length - 1];
  const target = `${String(hour).padStart(2, '0')}:00`;
  const exact = airQuality.find((d) => d.time === target);
  if (exact) return exact;
  return airQuality.reduce((best, d) => {
    const dH = parseInt(d.time.slice(0, 2), 10);
    const bH = parseInt(best.time.slice(0, 2), 10);
    return Math.abs(dH - hour) < Math.abs(bH - hour) ? d : best;
  });
}

export function WeatherDashboard({ data, locationName, selectedHour }: WeatherDashboardProps) {
  const currentAirQuality = pickAirQualityForHour(data.airQuality, selectedHour);

  return (
    <div className="space-y-6">
      {/* Current Weather */}
      <CurrentWeather current={data.current} locationName={locationName} />

      {/* Rule-based Advisories */}
      <WeatherAlerts
        current={data.current}
        hourly={data.hourly}
        latestAirQuality={currentAirQuality}
      />

      {/* Weather Metrics Grid */}
      <WeatherMetrics current={data.current} />

      {/* Charts Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <TemperatureChart data={data.hourly} selectedHour={selectedHour} />
        <HumidityChart data={data.hourly} selectedHour={selectedHour} />
      </div>

      {/* Air Quality Chart */}
      <AirQualityChart data={data.airQuality} selectedEntry={currentAirQuality} />
    </div>
  );
}
