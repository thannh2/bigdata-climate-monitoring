export interface CurrentWeather {
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

// HourlyData carries all fields so current conditions can be derived
// from the cached array without an extra server round-trip.
export interface HourlyData {
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

export interface AirQualityData {
  time: string;
  aqi: number;
  pm25: number;
  pm25Acc12h: number;
  isStagnantAir: boolean;
}

export interface WeatherData {
  current: CurrentWeather;
  hourly: HourlyData[];
  airQuality: AirQualityData[];
}

export interface ForecastHour {
  hour: number;          // 1–6 (offset from base)
  time: string;          // "HH:00" of the forecast hour
  temperature:   number | null;
  cloudCover:    number | null;
  precipitation: number | null;
  pressure:      number | null;
  windSpeed:     number | null;
  pm25:          number | null;
}

export interface ForecastCurrent {
  temperature:      number;
  humidity:         number;
  windSpeed:        number;
  pressure:         number;
  cloudCover:       number;
  precipitation:    number;
  pm25:             number;
  dewPoint:         number | null;
  pressureDelta3h:  number | null;
  tempMean6h:       number | null;
}

export interface ForecastData {
  baseHour:      number;
  baseTime:      string;
  baseTimestamp: string;
  current:       ForecastCurrent;
  hours:         ForecastHour[];
}
