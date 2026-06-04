import dotenv from 'dotenv';
import express, { Request, Response } from 'express';
import cors from 'cors';
import { MongoClient, Document } from 'mongodb';

dotenv.config({ path: `${__dirname}/../../.env` });

// ─── Configuration ────────────────────────────────────────────────────────────
const { MONGO_ROOT_USER, MONGO_ROOT_PASS } = process.env;
const MONGODB_URI = `mongodb://${MONGO_ROOT_USER}:${MONGO_ROOT_PASS}@localhost:27017`;

const DATABASE               = process.env.DB_NAME               ?? "climate_db";
const COLLECTION_HISTORICAL  = process.env.COLLECTION            ?? "climate_observations";
const COLLECTION_REALTIME    = process.env.COLLECTION_REALTIME   ?? "realtime_observations";
const COLLECTION_ALERTS      = process.env.COLLECTION_ALERTS     ?? "realtime_alerts";
const PORT                   = process.env.PORT                   ?? 3001;

// Pick the right collection based on whether the request is live or historical.
// Live = no date param (last-24h window). Historical = explicit YYYY-MM-DD date.
function collectionFor(dateParam?: string): string {
  return dateParam ? COLLECTION_HISTORICAL : COLLECTION_REALTIME;
}

// Map frontend location IDs → station_id values stored in MongoDB.
// Update these values to match the station_id strings in your collection.
const LOCATION_MAP: Record<string, string> = {
  angiang:      'an giang',
  brvt:         'ba ria - vung tau',
  bacgiang:     'bac giang',
  backan:       'bac kan',
  baclieu:      'bac lieu',
  bacninh:      'bac ninh',
  bentre:       'ben tre',
  binhdinh:     'binh dinh',
  binhduong:    'binh duong',
  binhphuoc:    'binh phuoc',
  binhthuan:    'binh thuan',
  camau:        'ca mau',
  cantho:       'can tho',
  caobang:      'cao bang',
  danang:       'da nang',
  daklak:       'dak lak',
  daknong:      'dak nong',
  dienbien:     'dien bien',
  dongnai:      'dong nai',
  dongthap:     'dong thap',
  gialai:       'gia lai',
  hagiang:      'ha giang',
  hanam:        'ha nam',
  hatinh:       'ha tinh',
  haiduong:     'hai duong',
  haiphong:     'hai phong',
  hanoi:        'hanoi',
  haugiang:     'hau giang',
  hcmc:         'hcmc',
  hoabinh:      'hoa binh',
  hue:          'hue',
  hungyen:      'hung yen',
  khanhhoa:     'khanh hoa',
  kiengiang:    'kien giang',
  kontum:       'kon tum',
  laichau:      'lai chau',
  lamdong:      'lam dong',
  langson:      'lang son',
  laocai:       'lao cai',
  longan:       'long an',
  namdinh:      'nam dinh',
  nghean:       'nghe an',
  ninhbinh:     'ninh binh',
  ninhthuan:    'ninh thuan',
  phutho:       'phu tho',
  phuyen:       'phu yen',
  quangbinh:    'quang binh',
  quangnam:     'quang nam',
  quangngai:    'quang ngai',
  quangninh:    'quang ninh',
  quangtri:     'quang tri',
  soctrang:     'soc trang',
  sonla:        'son la',
  tayninh:      'tay ninh',
  thaibinh:     'thai binh',
  thainguyen:   'thai nguyen',
  thanhhoa:     'thanh hoa',
  tiengiang:    'tien giang',
  travinh:      'tra vinh',
  tuyenquang:   'tuyen quang',
  vinhlong:     'vinh long',
  vinhphuc:     'vinh phuc',
  yenbai:       'yen bai',
};
// ─────────────────────────────────────────────────────────────────────────────

const client = new MongoClient(MONGODB_URI);
let db: ReturnType<typeof client.db>;

async function connectDB() {
  await client.connect();
  db = client.db(DATABASE);
  console.log(`Connected to MongoDB — database: "${DATABASE}"`);
}

const app = express();
app.use(cors());
app.use(express.json());

app.get("/api/health", (_req: Request, res: Response) =>
  res.json({ status: "ok" }),
);

// Resolve the 24-hour window for a given date string ("YYYY-MM-DD") or live mode.
// Returns { start, end } as UTC Date objects covering the full calendar day in UTC+7.
function dateWindow(dateParam?: string): { start: Date; end: Date } {
  if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
    // Treat the date as a calendar day in UTC+7 (Vietnam Standard Time)
    const start = new Date(`${dateParam}T00:00:00+07:00`);
    const end   = new Date(`${dateParam}T23:59:59+07:00`);
    return { start, end };
  }
  // Live: last 24 hours
  return { start: new Date(Date.now() - 24 * 60 * 60 * 1000), end: new Date() };
}

// GET /api/weather/hourly/:locationId?date=YYYY-MM-DD
// Returns all hourly observations within the window, oldest→newest
app.get(
  "/api/weather/hourly/:locationId",
  async (req: Request, res: Response) => {
    const stationId = LOCATION_MAP[req.params.locationId as string];
    if (!stationId) { res.status(400).json({ error: "Unknown location" }); return; }

    const { start, end } = dateWindow(req.query.date as string | undefined);

    try {
      const docs = (await db
        .collection(collectionFor(req.query.date as string | undefined))
        .find({ station_id: stationId, timestamp: { $gte: start, $lte: end } })
        .sort({ timestamp: 1 })
        .toArray()) as Document[];

      res.json(
        docs.map((d) => {
          const hour = new Date(d.timestamp).getHours().toString().padStart(2, "0");
          return {
            time:               `${hour}:00`,
            temperature:        d.temp_c,
            humidity:           d.humidity,
            windSpeed:          d.wind_speed,
            windDir:            d.wind_dir           ?? 0,
            pressure:           d.pressure           ?? 1013,
            precipitation:      d.precipitation      ?? 0,
            cloudCover:         d.cloud_cover         ?? 0,
            soilTemperature:    d.soil_temperature    ?? 0,
            shortwaveRadiation: d.shortwave_radiation ?? 0,
            condition:          cloudCoverToCondition(d.cloud_cover ?? 0),
          };
        }),
      );
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: (err as Error).message });
    }
  },
);

// GET /api/air-quality/:locationId?date=YYYY-MM-DD
// Returns all hourly air quality readings within the window, oldest→newest
app.get(
  "/api/air-quality/:locationId",
  async (req: Request, res: Response) => {
    const stationId = LOCATION_MAP[req.params.locationId as string];
    if (!stationId) { res.status(400).json({ error: "Unknown location" }); return; }

    const { start, end } = dateWindow(req.query.date as string | undefined);

    try {
      const docs = (await db
        .collection(collectionFor(req.query.date as string | undefined))
        .find({ station_id: stationId, timestamp: { $gte: start, $lte: end } })
        .sort({ timestamp: 1 })
        .toArray()) as Document[];

      res.json(
        docs.map((d) => {
          const hour = new Date(d.timestamp).getHours().toString().padStart(2, "0");
          return {
            time:          `${hour}:00`,
            aqi:           d.us_aqi        ?? 0,
            pm25:          d.pm2_5         ?? 0,
            pm25Acc12h:    d.pm25_acc_12h  ?? 0,
            isStagnantAir: !!d.is_stagnant_air,
          };
        }),
      );
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: (err as Error).message });
    }
  },
);

// GET /api/forecast/:locationId?date=YYYY-MM-DD&hour=0-23
// Live (no date): reads forecast fields from realtime_alerts.
// Historical (with date): reads forecast fields from climate_observations.
app.get(
  "/api/forecast/:locationId",
  async (req: Request, res: Response) => {
    const stationId = LOCATION_MAP[req.params.locationId as string];
    if (!stationId) { res.status(400).json({ error: "Unknown location" }); return; }

    const dateParam = req.query.date as string | undefined;
    const isLive = !dateParam;
    const { start, end } = dateWindow(dateParam);
    const hourParam = req.query.hour !== undefined ? parseInt(req.query.hour as string, 10) : undefined;

    // Live forecasts come from realtime_alerts; historical from climate_observations.
    const forecastCollection = isLive ? COLLECTION_ALERTS : COLLECTION_HISTORICAL;

    try {
      const candidates = (await db
        .collection(forecastCollection)
        .find({ station_id: stationId, timestamp: { $gte: start, $lte: end } })
        .toArray()) as Document[];

      if (!candidates.length) {
        res.status(404).json({ error: "No data found" });
        return;
      }

      // Pick the document closest to the requested hour (or most recent if no hour given)
      let doc: Document;
      if (hourParam !== undefined && !isNaN(hourParam)) {
        const targetMs = start.getTime() + hourParam * 60 * 60 * 1000;
        const target = new Date(targetMs);
        doc = candidates.reduce((best, d) => {
          const dDiff = Math.abs(new Date(d.timestamp).getTime() - target.getTime());
          const bDiff = Math.abs(new Date(best.timestamp).getTime() - target.getTime());
          return dDiff < bDiff ? d : best;
        });
      } else {
        doc = candidates.reduce((a, b) =>
          new Date(a.timestamp) > new Date(b.timestamp) ? a : b
        );
      }

      const baseHour = new Date(doc.timestamp).getHours();
      const baseDate = new Date(doc.timestamp);

      const hours = Array.from({ length: 6 }, (_, i) => {
        const h = i + 1;
        const forecastTime = new Date(baseDate.getTime() + h * 60 * 60 * 1000);
        return {
          hour: h,
          time: `${String(forecastTime.getHours()).padStart(2, '0')}:00`,
          temperature:  doc[`weather_temp_forecast_h${h}`]           ?? null,
          cloudCover:   doc[`weather_cloud_cover_forecast_h${h}`]    ?? null,
          precipitation:doc[`weather_precipitation_forecast_h${h}`]  ?? null,
          pressure:     doc[`weather_pressure_forecast_h${h}`]       ?? null,
          windSpeed:    doc[`weather_wind_speed_forecast_h${h}`]      ?? null,
          pm25:         doc[`aq_pm25_forecast_h${h}`]                ?? null,
        };
      });

      res.json({
        baseHour,
        baseTime: `${String(baseHour).padStart(2, '0')}:00`,
        baseTimestamp: doc.timestamp,
        current: {
          temperature:        doc.temp_c,
          humidity:           doc.humidity,
          windSpeed:          doc.wind_speed,
          pressure:           doc.pressure,
          cloudCover:         doc.cloud_cover,
          precipitation:      doc.precipitation,
          pm25:               doc.pm2_5,
          dewPoint:           doc.dew_point        ?? null,
          pressureDelta3h:    doc.pressure_delta_3h ?? null,
          tempMean6h:         doc.temp_mean_6h      ?? null,
        },
        hours,
      });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: (err as Error).message });
    }
  },
);

// Derive a weather condition string from cloud_cover percentage
function cloudCoverToCondition(cloudCover: number): string {
  if (cloudCover <= 10) return "Clear";
  if (cloudCover <= 30) return "Mostly Clear";
  if (cloudCover <= 60) return "Partly Cloudy";
  if (cloudCover <= 85) return "Mostly Cloudy";
  return "Overcast";
}

connectDB()
  .then(() =>
    app.listen(PORT, () =>
      console.log(`Server running on http://localhost:${PORT}`),
    ),
  )
  .catch((err) => {
    console.error("Failed to connect to MongoDB:", err);
    process.exit(1);
  });
