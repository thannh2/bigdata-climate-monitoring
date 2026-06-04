import { Cloud, Droplets, Wind, Gauge, Compass } from 'lucide-react';
import type { CurrentWeather as CurrentWeatherType } from '../types/weather';

interface CurrentWeatherProps {
  current: CurrentWeatherType;
  locationName: string;
}

function windDirLabel(deg: number): string {
  const dirs = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'];
  return dirs[Math.round(deg / 45) % 8];
}

export function CurrentWeather({ current, locationName }: CurrentWeatherProps) {
  return (
    <div className="bg-white rounded-xl shadow-lg p-8">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-6">
          <Cloud className="w-16 h-16 text-blue-500" />
          <div>
            <h2 className="text-5xl font-bold text-gray-900">{current.temperature}°C</h2>
            <p className="text-xl text-gray-600 mt-1">{current.condition}</p>
            <p className="text-gray-500 mt-1">{locationName}</p>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-6">
          <div className="flex items-center gap-3">
            <Droplets className="w-5 h-5 text-blue-500" />
            <div>
              <p className="text-sm text-gray-600">Humidity</p>
              <p className="text-lg font-semibold text-gray-900">{current.humidity}%</p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <Wind className="w-5 h-5 text-blue-500" />
            <div>
              <p className="text-sm text-gray-600">Wind Speed</p>
              <p className="text-lg font-semibold text-gray-900">{(current.windSpeed * 3.6).toFixed(2)} km/h</p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <Gauge className="w-5 h-5 text-blue-500" />
            <div>
              <p className="text-sm text-gray-600">Pressure</p>
              <p className="text-lg font-semibold text-gray-900">{current.pressure} hPa</p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <Compass className="w-5 h-5 text-blue-500" />
            <div>
              <p className="text-sm text-gray-600">Wind Direction</p>
              <p className="text-lg font-semibold text-gray-900">
                {current.windDir}° {windDirLabel(current.windDir)}
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
