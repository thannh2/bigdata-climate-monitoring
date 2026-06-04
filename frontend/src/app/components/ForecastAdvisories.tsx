import type { ForecastHour } from '../types/weather';

interface ForecastAdvisoriesProps {
  hours: ForecastHour[];
}

type Severity = 'info' | 'warning' | 'danger' | 'extreme';

interface Alert {
  id: string;
  severity: Severity;
  title: string;
  description: string;
}

// ─── Rule evaluators (same thresholds as WeatherAlerts) ───────────────────────

function temperatureAlert(temp: number): Alert | null {
  if (temp >= 40)   return { id: 'temp', severity: 'extreme', title: 'Extreme Heat',    description: `${temp.toFixed(1)}°C — Extremely dangerous. Avoid all outdoor activity.` };
  if (temp >= 37)   return { id: 'temp', severity: 'danger',  title: 'Severe Heat',     description: `${temp.toFixed(1)}°C — High health risk. Stay hydrated and seek shade.` };
  if (temp >= 35)   return { id: 'temp', severity: 'warning', title: 'Hot Conditions',  description: `${temp.toFixed(1)}°C — Limit strenuous outdoor activity during midday.` };
  if (temp <= 15 && temp >= 13) return { id: 'temp', severity: 'warning', title: 'Cold Snap', description: `${temp.toFixed(1)}°C — Agricultural warning: protect crops and livestock.` };
  if (temp < 13)    return { id: 'temp', severity: 'danger',  title: 'Harmful Cold',    description: `${temp.toFixed(1)}°C — Risk of frost in highlands. Protect vulnerable individuals.` };
  return null;
}

function precipitationAlert(precip: number): Alert | null {
  if (precip > 50)  return { id: 'precip', severity: 'extreme', title: 'Flash Flood & Landslide Risk', description: `${precip.toFixed(1)} mm/h — Evacuate flood-prone and hillside areas immediately.` };
  if (precip >= 7.6) return { id: 'precip', severity: 'danger',  title: 'Heavy Rain',     description: `${precip.toFixed(1)} mm/h — Local urban flooding likely.` };
  if (precip >= 2.6) return { id: 'precip', severity: 'warning', title: 'Moderate Rain',  description: `${precip.toFixed(1)} mm/h — Slippery roads, reduced visibility.` };
  if (precip >= 0.1) return { id: 'precip', severity: 'info',    title: 'Light Rain',     description: `${precip.toFixed(1)} mm/h — Light precipitation expected.` };
  return null;
}

function windAlert(speed: number): Alert | null {
  if (speed > 20.8)  return { id: 'wind', severity: 'extreme', title: 'Severe Storm / Typhoon',    description: `${speed.toFixed(1)} m/s (Beaufort 9+) — Life-threatening. Take shelter immediately.` };
  if (speed >= 17.2) return { id: 'wind', severity: 'danger',  title: 'Gale Force Wind',           description: `${speed.toFixed(1)} m/s (Beaufort 8) — Structural damage possible.` };
  if (speed >= 13.9) return { id: 'wind', severity: 'danger',  title: 'Very Strong Wind',          description: `${speed.toFixed(1)} m/s (Beaufort 7) — Walking against the wind is difficult.` };
  if (speed >= 10.8) return { id: 'wind', severity: 'warning', title: 'Strong Wind',               description: `${speed.toFixed(1)} m/s (Beaufort 6) — Marine advisory. Small trees swaying.` };
  return null;
}

function cloudAlert(cover: number): Alert | null {
  if (cover > 70)  return { id: 'cloud', severity: 'info', title: 'Overcast Sky',    description: `${cover}% cloud cover — Gloomy conditions, rain likely.` };
  if (cover >= 41) return { id: 'cloud', severity: 'info', title: 'Mostly Cloudy',  description: `${cover}% cloud cover — Overcast and cool.` };
  return null;
}

function pm25Alert(pm25: number): Alert | null {
  if (pm25 > 150.4)  return { id: 'pm25', severity: 'extreme', title: 'Hazardous Air Quality',        description: `${pm25.toFixed(1)} μg/m³ — Everyone should avoid outdoor exposure.` };
  if (pm25 >= 55.5)  return { id: 'pm25', severity: 'danger',  title: 'Unhealthy Air Quality',        description: `${pm25.toFixed(1)} μg/m³ — Reduce prolonged outdoor exertion.` };
  if (pm25 >= 35.5)  return { id: 'pm25', severity: 'warning', title: 'Poor Air Quality',             description: `${pm25.toFixed(1)} μg/m³ — Sensitive groups should limit outdoor activity.` };
  if (pm25 >= 12.1)  return { id: 'pm25', severity: 'info',    title: 'Moderate Air Quality',         description: `${pm25.toFixed(1)} μg/m³ — Acceptable for most people.` };
  return null;
}

// ─── Severity helpers ─────────────────────────────────────────────────────────

const SEVERITY_ORDER: Severity[] = ['info', 'warning', 'danger', 'extreme'];

function higherSeverity(a: Severity, b: Severity): Severity {
  return SEVERITY_ORDER.indexOf(a) >= SEVERITY_ORDER.indexOf(b) ? a : b;
}

const SEVERITY_STYLES: Record<Severity, { bg: string; border: string; icon: string; label: string; labelColor: string; textColor: string }> = {
  info:    { bg: 'bg-blue-50',   border: 'border-blue-200',   icon: 'ℹ️', label: 'Info',    labelColor: 'text-blue-700',   textColor: 'text-blue-800' },
  warning: { bg: 'bg-yellow-50', border: 'border-yellow-300', icon: '⚠️', label: 'Warning', labelColor: 'text-yellow-700', textColor: 'text-yellow-900' },
  danger:  { bg: 'bg-orange-50', border: 'border-orange-300', icon: '🔶', label: 'Danger',  labelColor: 'text-orange-700', textColor: 'text-orange-900' },
  extreme: { bg: 'bg-red-50',    border: 'border-red-400',    icon: '🚨', label: 'Extreme', labelColor: 'text-red-700',    textColor: 'text-red-900' },
};

// ─── Component ────────────────────────────────────────────────────────────────

export function ForecastAdvisories({ hours }: ForecastAdvisoriesProps) {
  // For each alert type, collect which hours trigger it and the worst severity/description
  type AggregatedAlert = {
    id: string;
    severity: Severity;
    title: string;
    worstDescription: string;
    triggeredHours: string[]; // e.g. ["+1h (13:00)", "+2h (14:00)"]
  };

  const byId = new Map<string, AggregatedAlert>();

  for (const h of hours) {
    const candidates: (Alert | null)[] = [
      h.temperature  !== null ? temperatureAlert(h.temperature)   : null,
      h.precipitation !== null ? precipitationAlert(h.precipitation) : null,
      h.windSpeed    !== null ? windAlert(h.windSpeed)             : null,
      h.cloudCover   !== null ? cloudAlert(h.cloudCover)           : null,
      h.pm25         !== null ? pm25Alert(h.pm25)                  : null,
    ];

    for (const alert of candidates) {
      if (!alert) continue;
      const label = `+${h.hour}h (${h.time})`;
      const existing = byId.get(alert.id);
      if (!existing) {
        byId.set(alert.id, {
          id: alert.id,
          severity: alert.severity,
          title: alert.title,
          worstDescription: alert.description,
          triggeredHours: [label],
        });
      } else {
        const newSev = higherSeverity(existing.severity, alert.severity);
        byId.set(alert.id, {
          ...existing,
          severity: newSev,
          // keep description from the worst hour
          worstDescription: newSev !== existing.severity ? alert.description : existing.worstDescription,
          triggeredHours: [...existing.triggeredHours, label],
        });
      }
    }
  }

  const alerts = Array.from(byId.values()).sort(
    (a, b) => SEVERITY_ORDER.indexOf(b.severity) - SEVERITY_ORDER.indexOf(a.severity)
  );

  const actionable = alerts.filter(a => a.severity !== 'info' || a.id === 'pm25');

  return (
    <div className="bg-white rounded-lg shadow p-6 space-y-4">
      <div className="flex items-center justify-between flex-wrap gap-2">
        <h3 className="text-lg font-semibold text-gray-900">Forecast Advisories</h3>
        <span className="text-sm text-gray-500">Next 6 hours · Vietnamese meteorological &amp; EPA guidelines</span>
      </div>

      {actionable.length === 0 ? (
        <div className="flex items-center gap-3 p-4 bg-green-50 border border-green-200 rounded-lg">
          <span className="text-2xl">✅</span>
          <div>
            <p className="font-medium text-green-800">All clear for the next 6 hours</p>
            <p className="text-sm text-green-700">No significant weather or air quality events forecast.</p>
          </div>
        </div>
      ) : (
        <div className="space-y-3">
          {actionable.map((alert) => {
            const s = SEVERITY_STYLES[alert.severity];
            return (
              <div key={alert.id} className={`flex items-start gap-3 p-4 rounded-lg border ${s.bg} ${s.border}`}>
                <span className="text-xl mt-0.5 shrink-0">{s.icon}</span>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 flex-wrap">
                    <p className={`font-semibold ${s.textColor}`}>{alert.title}</p>
                    <span className={`text-xs px-2 py-0.5 rounded-full border ${s.border} ${s.labelColor} bg-white`}>
                      {s.label}
                    </span>
                  </div>
                  <p className={`text-sm mt-0.5 ${s.textColor} opacity-90`}>{alert.worstDescription}</p>
                  <p className={`text-xs mt-1.5 ${s.textColor} opacity-70`}>
                    Expected at: {alert.triggeredHours.join(', ')}
                  </p>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Informational cloud/pm25 alerts (non-actionable) */}
      {alerts.filter(a => a.severity === 'info' && a.id !== 'pm25').length > 0 && (
        <div className="space-y-2 pt-1">
          {alerts.filter(a => a.severity === 'info' && a.id !== 'pm25').map((alert) => {
            const s = SEVERITY_STYLES.info;
            return (
              <div key={alert.id} className={`flex items-start gap-3 p-3 rounded-lg border ${s.bg} ${s.border}`}>
                <span className="text-lg mt-0.5 shrink-0">{s.icon}</span>
                <div className="min-w-0">
                  <p className={`text-sm font-semibold ${s.textColor}`}>{alert.title}</p>
                  <p className={`text-xs ${s.textColor} opacity-80`}>{alert.worstDescription}</p>
                  <p className={`text-xs mt-1 ${s.textColor} opacity-60`}>
                    At: {alert.triggeredHours.join(', ')}
                  </p>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
