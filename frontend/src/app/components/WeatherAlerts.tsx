import type { CurrentWeather, HourlyData, AirQualityData } from '../types/weather';

interface WeatherAlertsProps {
  current: CurrentWeather;
  hourly: HourlyData[];
  latestAirQuality: AirQualityData | null;
}

type Severity = 'info' | 'warning' | 'danger' | 'extreme';

interface Alert {
  id: string;
  severity: Severity;
  title: string;
  description: string;
}

// ─── Rule evaluators ──────────────────────────────────────────────────────────

function temperatureAlert(temp: number): Alert | null {
  if (temp >= 40)
    return { id: 'temp', severity: 'extreme', title: 'Extreme Heat', description: `${temp}°C — Extremely dangerous. Avoid all outdoor activity.` };
  if (temp >= 37)
    return { id: 'temp', severity: 'danger', title: 'Severe Heat', description: `${temp}°C — High health risk. Stay hydrated and seek shade.` };
  if (temp >= 35)
    return { id: 'temp', severity: 'warning', title: 'Hot Conditions', description: `${temp}°C — Limit strenuous outdoor activity during midday.` };
  if (temp >= 13 && temp <= 15)
    return { id: 'temp', severity: 'warning', title: 'Cold Snap', description: `${temp}°C — Agricultural warning: protect crops and livestock.` };
  if (temp < 13)
    return { id: 'temp', severity: 'danger', title: 'Harmful Cold', description: `${temp}°C — Risk of frost in highlands. Schools may close. Protect vulnerable individuals.` };
  return null;
}

function precipitationAlert(precip: number): Alert | null {
  if (precip > 50)
    return { id: 'precip', severity: 'extreme', title: 'Flash Flood & Landslide Risk', description: `${precip} mm/h — Extremely heavy rain. Evacuate flood-prone and hillside areas immediately.` };
  if (precip >= 7.6)
    return { id: 'precip', severity: 'danger', title: 'Heavy Rain', description: `${precip} mm/h — Local urban flooding likely. Avoid low-lying roads.` };
  if (precip >= 2.6)
    return { id: 'precip', severity: 'warning', title: 'Moderate Rain', description: `${precip} mm/h — Slippery roads, reduced visibility. Drive with caution.` };
  if (precip >= 0.1)
    return { id: 'precip', severity: 'info', title: 'Light Rain / Drizzle', description: `${precip} mm/h — Light precipitation expected.` };
  return null;
}

function windAlert(speed: number): Alert | null {
  if (speed > 20.8)
    return { id: 'wind', severity: 'extreme', title: 'Severe Storm / Typhoon', description: `${speed} m/s (Beaufort 9+) — Life-threatening. Take shelter immediately.` };
  if (speed >= 17.2)
    return { id: 'wind', severity: 'danger', title: 'Gale Force Wind', description: `${speed} m/s (Beaufort 8) — Small branches breaking. Structural damage possible.` };
  if (speed >= 13.9)
    return { id: 'wind', severity: 'danger', title: 'Very Strong Wind', description: `${speed} m/s (Beaufort 7) — Walking against the wind is difficult.` };
  if (speed >= 10.8)
    return { id: 'wind', severity: 'warning', title: 'Strong Wind', description: `${speed} m/s (Beaufort 6) — Marine advisory issued. Small trees swaying.` };
  return null;
}

function cloudAlert(cover: number): Alert | null {
  if (cover > 70)
    return { id: 'cloud', severity: 'info', title: 'Overcast Sky', description: `${cover}% cloud cover — Gloomy conditions, rain likely.` };
  if (cover >= 41)
    return { id: 'cloud', severity: 'info', title: 'Mostly Cloudy', description: `${cover}% cloud cover — Overcast and cool.` };
  return null;
}

function pressureDropAlert(current: CurrentWeather, hourly: HourlyData[]): Alert | null {
  if (hourly.length < 3) return null;
  const strongWind = current.windSpeed >= 10.8;
  const lowPressure = current.pressure < 1000;
  if (strongWind && lowPressure)
    return {
      id: 'pressure',
      severity: 'danger',
      title: 'Possible Tropical Depression / Storm Approach',
      description: `Pressure ${current.pressure} hPa with wind ${current.windSpeed} m/s — Signs of an approaching storm or tropical low. Monitor official advisories.`,
    };
  return null;
}

// PM2.5 — EPA standard breakpoints
function pm25Alert(pm25: number): Alert | null {
  if (pm25 > 150.4)
    return { id: 'pm25', severity: 'extreme', title: 'Hazardous Air Quality (PM2.5)', description: `${pm25} μg/m³ — Very unhealthy to hazardous. Everyone should avoid outdoor exposure.` };
  if (pm25 >= 55.5)
    return { id: 'pm25', severity: 'danger', title: 'Unhealthy Air Quality (PM2.5)', description: `${pm25} μg/m³ — Health effects for the general public. Reduce prolonged outdoor exertion.` };
  if (pm25 >= 35.5)
    return { id: 'pm25', severity: 'warning', title: 'Poor Air Quality (PM2.5)', description: `${pm25} μg/m³ — Sensitive groups (children, elderly, respiratory conditions) should limit outdoor activity.` };
  if (pm25 >= 12.1)
    return { id: 'pm25', severity: 'info', title: 'Moderate Air Quality (PM2.5)', description: `${pm25} μg/m³ — Acceptable for most, but unusually sensitive individuals may experience minor symptoms.` };
  return null; // 0–12 Good — no alert needed
}

// Solar radiation — day/night indicator and renewable energy tiers
function solarAlert(radiation: number): Alert {
  if (radiation === 0)
    return { id: 'solar', severity: 'info', title: 'Nighttime', description: 'Solar radiation: 0 W/m² — No solar generation. Night-mode operations active.' };
  if (radiation <= 100)
    return { id: 'solar', severity: 'info', title: 'Low Solar Radiation', description: `${radiation} W/m² — Dawn / dusk or heavy overcast. Minimal solar panel output.` };
  if (radiation <= 400)
    return { id: 'solar', severity: 'info', title: 'Moderate Solar Radiation', description: `${radiation} W/m² — Partial generation possible. Panels operating at reduced capacity.` };
  if (radiation <= 700)
    return { id: 'solar', severity: 'info', title: 'High Solar Radiation', description: `${radiation} W/m² — Good generation conditions. Solar panels near rated output.` };
  return { id: 'solar', severity: 'info', title: 'Peak Solar Radiation', description: `${radiation} W/m² — Optimal conditions for solar generation. Maximum output expected.` };
}

// ─── Severity styling ─────────────────────────────────────────────────────────

const SEVERITY_STYLES: Record<Severity, { bg: string; border: string; icon: string; label: string; labelColor: string; textColor: string }> = {
  info:    { bg: 'bg-blue-50',   border: 'border-blue-200',  icon: 'ℹ️', label: 'Info',    labelColor: 'text-blue-700',   textColor: 'text-blue-800' },
  warning: { bg: 'bg-yellow-50', border: 'border-yellow-300',icon: '⚠️', label: 'Warning', labelColor: 'text-yellow-700', textColor: 'text-yellow-900' },
  danger:  { bg: 'bg-orange-50', border: 'border-orange-300',icon: '🔶', label: 'Danger',  labelColor: 'text-orange-700', textColor: 'text-orange-900' },
  extreme: { bg: 'bg-red-50',    border: 'border-red-400',   icon: '🚨', label: 'Extreme', labelColor: 'text-red-700',    textColor: 'text-red-900' },
};

// PM2.5 colour badge (matches EPA colour scale)
function pm25Badge(pm25: number): { bg: string; text: string; label: string } {
  if (pm25 <= 12)    return { bg: 'bg-green-100',  text: 'text-green-800',  label: 'Good' };
  if (pm25 <= 35.4)  return { bg: 'bg-yellow-100', text: 'text-yellow-800', label: 'Moderate' };
  if (pm25 <= 55.4)  return { bg: 'bg-orange-100', text: 'text-orange-800', label: 'Poor' };
  if (pm25 <= 150.4) return { bg: 'bg-red-100',    text: 'text-red-800',    label: 'Unhealthy' };
  return               { bg: 'bg-purple-100', text: 'text-purple-800', label: 'Hazardous' };
}

// ─── Component ────────────────────────────────────────────────────────────────

export function WeatherAlerts({ current, hourly, latestAirQuality }: WeatherAlertsProps) {
  const pm25 = latestAirQuality?.pm25 ?? null;

  const alerts: Alert[] = [
    temperatureAlert(current.temperature),
    precipitationAlert(current.precipitation),
    windAlert(current.windSpeed),
    cloudAlert(current.cloudCover),
    pressureDropAlert(current, hourly),
    pm25 !== null ? pm25Alert(pm25) : null,
  ].filter((a): a is Alert => a !== null);

  const solar = solarAlert(current.shortwaveRadiation);

  const skyLabel =
    current.cloudCover <= 10  ? 'Clear / Sunny' :
    current.cloudCover <= 40  ? 'Partly Cloudy' :
    current.cloudCover <= 70  ? 'Mostly Cloudy' : 'Overcast';

  // Alerts that are not purely informational (for the "all clear" check)
  const actionableAlerts = alerts.filter(a => a.severity !== 'info' || a.id === 'pm25');

  return (
    <div className="bg-white rounded-lg shadow p-6 space-y-4">
      <div className="flex items-center justify-between flex-wrap gap-2">
        <h3 className="text-lg font-semibold text-gray-900">Weather Advisories</h3>
        <span className="text-sm text-gray-500">Vietnamese meteorological &amp; EPA guidelines</span>
      </div>

      {/* Alert list */}
      {actionableAlerts.length === 0 ? (
        <div className="flex items-center gap-3 p-4 bg-green-50 border border-green-200 rounded-lg">
          <span className="text-2xl">✅</span>
          <div>
            <p className="font-medium text-green-800">All conditions normal</p>
            <p className="text-sm text-green-700">
              {current.temperature}°C · {skyLabel} · Wind {current.windSpeed} m/s · Pressure {current.pressure} hPa
            </p>
          </div>
        </div>
      ) : (
        <div className="space-y-3">
          {actionableAlerts.map((alert) => {
            const s = SEVERITY_STYLES[alert.severity];
            return (
              <div key={alert.id} className={`flex items-start gap-3 p-4 rounded-lg border ${s.bg} ${s.border}`}>
                <span className="text-xl mt-0.5 shrink-0">{s.icon}</span>
                <div className="min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <p className={`font-semibold ${s.textColor}`}>{alert.title}</p>
                    <span className={`text-xs px-2 py-0.5 rounded-full border ${s.border} ${s.labelColor} bg-white`}>
                      {s.label}
                    </span>
                  </div>
                  <p className={`text-sm mt-0.5 ${s.textColor} opacity-90`}>{alert.description}</p>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Informational solar detail (always shown when daytime) */}
      {current.shortwaveRadiation > 0 && (
        <div className="flex items-start gap-3 p-3 rounded-lg border bg-blue-50 border-blue-200">
          <span className="text-xl mt-0.5 shrink-0">☀️</span>
          <div>
            <p className="font-semibold text-blue-800 text-sm">{solar.title}</p>
            <p className="text-sm text-blue-700 opacity-90">{solar.description}</p>
          </div>
        </div>
      )}

      {/* Quick reference legend */}
      <details className="text-xs text-gray-500 pt-2 border-t border-gray-100">
        <summary className="cursor-pointer hover:text-gray-700 select-none">Guideline thresholds reference</summary>
        <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-1 leading-relaxed">
          <span>🌡️ ≥40°C Extreme heat</span>
          <span>🌡️ 37–39.9°C Severe heat</span>
          <span>🌡️ 35–36.9°C Hot</span>
          <span>🌡️ 13–15°C Cold snap (agri. warning)</span>
          <span>🌡️ &lt;13°C Harmful cold (school closure)</span>
          <span>🌧️ &gt;50 mm/h Flash flood &amp; landslide</span>
          <span>🌧️ 7.6–50 mm/h Heavy rain (flood risk)</span>
          <span>🌧️ 2.6–7.5 mm/h Moderate rain</span>
          <span>💨 &gt;20.8 m/s Typhoon (Beaufort 9+)</span>
          <span>💨 17.2–20.7 m/s Gale (Beaufort 8)</span>
          <span>💨 13.9–17.1 m/s Very strong (Beaufort 7)</span>
          <span>💨 10.8–13.8 m/s Strong (Beaufort 6)</span>
          <span>☁️ &gt;70% Overcast / rain likely</span>
          <span>📉 P&lt;1000 hPa + strong wind → storm</span>
          <span>🫧 PM2.5 0–12 Good (EPA)</span>
          <span>🫧 PM2.5 12.1–35.4 Moderate</span>
          <span>🫧 PM2.5 35.5–55.4 Poor (sensitive groups)</span>
          <span>🫧 PM2.5 55.5–150.4 Unhealthy (all)</span>
          <span>🫧 PM2.5 &gt;150.5 Hazardous</span>
          <span>☀️ &gt;0 W/m² Daytime · 0 W/m² Nighttime</span>
          <span>☀️ &gt;400 W/m² Good solar generation</span>
          <span>☀️ &gt;700 W/m² Peak solar output</span>
        </div>
      </details>
    </div>
  );
}
