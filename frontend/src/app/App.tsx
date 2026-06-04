import { useState } from "react";
import { WeatherDashboard } from "./components/WeatherDashboard";
import { ForecastDashboard } from "./components/ForecastDashboard";
import { LocationSelector } from "./components/LocationSelector";
import { useWeatherData } from "./hooks/useWeatherData";
import { useForecastData } from "./hooks/useForecastData";

const locations = [
  { id: "angiang", name: "An Giang" },
  { id: "brvt", name: "Ba Ria - Vung Tau" },
  { id: "bacgiang", name: "Bac Giang" },
  { id: "backan", name: "Bac Kan" },
  { id: "baclieu", name: "Bac Lieu" },
  { id: "bacninh", name: "Bac Ninh" },
  { id: "bentre", name: "Ben Tre" },
  { id: "binhdinh", name: "Binh Dinh" },
  { id: "binhduong", name: "Binh Duong" },
  { id: "binhphuoc", name: "Binh Phuoc" },
  { id: "binhthuan", name: "Binh Thuan" },
  { id: "camau", name: "Ca Mau" },
  { id: "cantho", name: "Can Tho" },
  { id: "caobang", name: "Cao Bang" },
  { id: "danang", name: "Da Nang" },
  { id: "daklak", name: "Dak Lak" },
  { id: "daknong", name: "Dak Nong" },
  { id: "dienbien", name: "Dien Bien" },
  { id: "dongnai", name: "Dong Nai" },
  { id: "dongthap", name: "Dong Thap" },
  { id: "gialai", name: "Gia Lai" },
  { id: "hagiang", name: "Ha Giang" },
  { id: "hanam", name: "Ha Nam" },
  { id: "hatinh", name: "Ha Tinh" },
  { id: "haiduong", name: "Hai Duong" },
  { id: "haiphong", name: "Hai Phong" },
  { id: "hanoi", name: "Ha Noi" },
  { id: "haugiang", name: "Hau Giang" },
  { id: "hcmc", name: "Ho Chi Minh City" },
  { id: "hoabinh", name: "Hoa Binh" },
  { id: "hue", name: "Hue" },
  { id: "hungyen", name: "Hung Yen" },
  { id: "khanhhoa", name: "Khanh Hoa" },
  { id: "kiengiang", name: "Kien Giang" },
  { id: "kontum", name: "Kon Tum" },
  { id: "laichau", name: "Lai Chau" },
  { id: "lamdong", name: "Lam Dong" },
  { id: "langson", name: "Lang Son" },
  { id: "laocai", name: "Lao Cai" },
  { id: "longan", name: "Long An" },
  { id: "namdinh", name: "Nam Dinh" },
  { id: "nghean", name: "Nghe An" },
  { id: "ninhbinh", name: "Ninh Binh" },
  { id: "ninhthuan", name: "Ninh Thuan" },
  { id: "phutho", name: "Phu Tho" },
  { id: "phuyen", name: "Phu Yen" },
  { id: "quangbinh", name: "Quang Binh" },
  { id: "quangnam", name: "Quang Nam" },
  { id: "quangngai", name: "Quang Ngai" },
  { id: "quangninh", name: "Quang Ninh" },
  { id: "quangtri", name: "Quang Tri" },
  { id: "soctrang", name: "Soc Trang" },
  { id: "sonla", name: "Son La" },
  { id: "tayninh", name: "Tay Ninh" },
  { id: "thaibinh", name: "Thai Binh" },
  { id: "thainguyen", name: "Thai Nguyen" },
  { id: "thanhhoa", name: "Thanh Hoa" },
  { id: "tiengiang", name: "Tien Giang" },
  { id: "travinh", name: "Tra Vinh" },
  { id: "tuyenquang", name: "Tuyen Quang" },
  { id: "vinhlong", name: "Vinh Long" },
  { id: "vinhphuc", name: "Vinh Phuc" },
  { id: "yenbai", name: "Yen Bai" }
];

const TODAY = new Date().toISOString().slice(0, 10);
const CURRENT_HOUR = new Date().getHours();

type Tab = 'overview' | 'forecast';

export default function App() {
  const [selectedLocation, setSelectedLocation] = useState("hanoi");
  const [selectedDate, setSelectedDate] = useState<string>(TODAY);
  const [selectedHour, setSelectedHour] = useState<number>(CURRENT_HOUR);
  const [activeTab, setActiveTab] = useState<Tab>('overview');

  const isLive = selectedDate === TODAY && selectedHour === CURRENT_HOUR;
  const dateParam = selectedDate === TODAY ? undefined : selectedDate;

  const { data, loading, error, refetch } = useWeatherData(selectedLocation, dateParam, selectedHour);
  const { data: forecastData, loading: forecastLoading, error: forecastError, refetch: forecastRefetch } =
    useForecastData(selectedLocation, dateParam, selectedHour);
  const locationName = locations.find((l) => l.id === selectedLocation)?.name ?? "";

  return (
    <div className="size-full bg-gray-50">
      <div className="h-full overflow-auto">
        <div className="max-w-7xl mx-auto p-6 space-y-6">
          {/* Header */}
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">
                Climate Monitoring Dashboard
              </h1>
              <p className="text-gray-600 mt-1">
                {isLive
                  ? 'Real-time weather and air quality data'
                  : `Historical data — ${selectedDate} at ${String(selectedHour).padStart(2, '0')}:00`}
              </p>
            </div>
            <div className="flex items-center gap-3 flex-wrap justify-end">
              {/* Date picker */}
              <div className="flex items-center gap-2">
                <label htmlFor="date-picker" className="text-sm text-gray-600 whitespace-nowrap">
                  Date
                </label>
                <input
                  id="date-picker"
                  type="date"
                  value={selectedDate}
                  max={TODAY}
                  onChange={(e) => {
                    const d = e.target.value || TODAY;
                    setSelectedDate(d);
                    // When switching to today, snap hour to current; otherwise start at noon
                    setSelectedHour(d === TODAY ? CURRENT_HOUR : 12);
                  }}
                  className="border border-gray-300 rounded-md px-3 py-2 text-sm text-gray-900 bg-white shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              {/* Hour selector */}
              <div className="flex items-center gap-2">
                <label htmlFor="hour-picker" className="text-sm text-gray-600 whitespace-nowrap">
                  Hour
                </label>
                <div className="flex items-center gap-2">
                  <input
                    id="hour-picker"
                    type="range"
                    min={0}
                    max={23}
                    value={selectedHour}
                    onChange={(e) => setSelectedHour(Number(e.target.value))}
                    className="w-28 accent-blue-500"
                  />
                  <span className="text-sm font-mono text-gray-800 w-12 text-center bg-white border border-gray-300 rounded-md px-2 py-1">
                    {String(selectedHour).padStart(2, '0')}:00
                  </span>
                </div>
                {!isLive && (
                  <button
                    onClick={() => { setSelectedDate(TODAY); setSelectedHour(CURRENT_HOUR); }}
                    className="px-3 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 whitespace-nowrap"
                  >
                    Back to Live
                  </button>
                )}
                {isLive && (
                  <span className="flex items-center gap-1 text-xs text-green-700 bg-green-50 border border-green-200 px-2 py-1 rounded-full">
                    <span className="w-1.5 h-1.5 rounded-full bg-green-500 animate-pulse inline-block" />
                    Live
                  </span>
                )}
              </div>
              <LocationSelector
                locations={locations}
                selectedLocation={selectedLocation}
                onLocationChange={setSelectedLocation}
              />
            </div>
          </div>

          {/* Tabs */}
          <div className="flex border-b border-gray-200">
            {([['overview', 'Overview'], ['forecast', '6-Hour Forecast']] as [Tab, string][]).map(([id, label]) => (
              <button
                key={id}
                onClick={() => setActiveTab(id)}
                className={`px-5 py-2.5 text-sm font-medium border-b-2 transition-colors ${
                  activeTab === id
                    ? 'border-blue-600 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700'
                }`}
              >
                {label}
              </button>
            ))}
          </div>

          {/* ── Overview tab ── */}
          {activeTab === 'overview' && (
            <>
              {loading && (
                <div className="flex items-center justify-center h-64">
                  <div className="flex flex-col items-center gap-3 text-gray-500">
                    <div className="w-10 h-10 border-4 border-blue-500 border-t-transparent rounded-full animate-spin" />
                    <p>Loading data from MongoDB…</p>
                  </div>
                </div>
              )}
              {!loading && error && (
                <div className="rounded-lg border border-red-200 bg-red-50 p-6 text-center space-y-3">
                  <p className="text-red-700 font-medium">Failed to load weather data</p>
                  <p className="text-red-600 text-sm">{error}</p>
                  <p className="text-gray-500 text-xs">
                    Make sure the Express server in{" "}
                    <code className="bg-red-100 px-1 rounded">server/index.ts</code>{" "}
                    is running and{" "}
                    <code className="bg-red-100 px-1 rounded">MONGODB_URI</code> is set.
                  </p>
                  <button onClick={refetch} className="mt-2 px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700 text-sm">
                    Retry
                  </button>
                </div>
              )}
              {!loading && !error && !data && (
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-10 text-center text-gray-500">
                  No data available for {locationName} on {selectedDate} at {String(selectedHour).padStart(2, '0')}:00.
                </div>
              )}
              {!loading && !error && data && (
                <WeatherDashboard data={data} locationName={locationName} selectedHour={selectedHour} />
              )}
            </>
          )}

          {/* ── Forecast tab ── */}
          {activeTab === 'forecast' && (
            <>
              {forecastLoading && (
                <div className="flex items-center justify-center h-64">
                  <div className="flex flex-col items-center gap-3 text-gray-500">
                    <div className="w-10 h-10 border-4 border-blue-500 border-t-transparent rounded-full animate-spin" />
                    <p>Loading forecast data…</p>
                  </div>
                </div>
              )}
              {!forecastLoading && forecastError && (
                <div className="rounded-lg border border-red-200 bg-red-50 p-6 text-center space-y-3">
                  <p className="text-red-700 font-medium">Failed to load forecast</p>
                  <p className="text-red-600 text-sm">{forecastError}</p>
                  <button onClick={forecastRefetch} className="mt-2 px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700 text-sm">
                    Retry
                  </button>
                </div>
              )}
              {!forecastLoading && !forecastError && !forecastData && (
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-10 text-center text-gray-500">
                  No forecast data available for {locationName} on {selectedDate} at {String(selectedHour).padStart(2, '0')}:00.
                </div>
              )}
              {!forecastLoading && !forecastError && forecastData && (
                <ForecastDashboard data={forecastData} locationName={locationName} />
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}