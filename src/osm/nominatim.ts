const NOMINATIM_BASE_URL = "https://nominatim.openstreetmap.org";
const USER_AGENT = "MCP-OSM-DuckDB/1.0 (educational project)";

// Rate limiting: Nominatim requires max 1 request per second
let lastRequestTime = 0;
const MIN_REQUEST_INTERVAL_MS = 1100;

async function throttle(): Promise<void> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < MIN_REQUEST_INTERVAL_MS) {
    await new Promise((resolve) =>
      setTimeout(resolve, MIN_REQUEST_INTERVAL_MS - elapsed)
    );
  }
  lastRequestTime = Date.now();
}

export interface GeocodeResult {
  display_name: string;
  lat: number;
  lon: number;
  bbox?: [number, number, number, number]; // [south, north, west, east]
  osm_type?: string;
  osm_id?: number;
  class?: string;
  type?: string;
}

interface NominatimResponse {
  display_name: string;
  lat: string;
  lon: string;
  boundingbox?: [string, string, string, string];
  osm_type?: string;
  osm_id?: number;
  class?: string;
  type?: string;
}

export async function geocode(query: string): Promise<GeocodeResult[]> {
  await throttle();

  const params = new URLSearchParams({
    q: query,
    format: "json",
    limit: "5",
    addressdetails: "1",
  });

  const url = `${NOMINATIM_BASE_URL}/search?${params}`;

  const response = await fetch(url, {
    headers: {
      "User-Agent": USER_AGENT,
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Nominatim error: ${response.status} ${response.statusText}`);
  }

  const data = (await response.json()) as NominatimResponse[];

  return data.map((item) => ({
    display_name: item.display_name,
    lat: parseFloat(item.lat),
    lon: parseFloat(item.lon),
    bbox: item.boundingbox
      ? [
          parseFloat(item.boundingbox[0]),
          parseFloat(item.boundingbox[1]),
          parseFloat(item.boundingbox[2]),
          parseFloat(item.boundingbox[3]),
        ]
      : undefined,
    osm_type: item.osm_type,
    osm_id: item.osm_id,
    class: item.class,
    type: item.type,
  }));
}

export interface ReverseGeocodeResult {
  display_name: string;
  lat: number;
  lon: number;
  address?: Record<string, string>;
}

export async function reverseGeocode(
  lat: number,
  lon: number
): Promise<ReverseGeocodeResult | null> {
  await throttle();

  const params = new URLSearchParams({
    lat: lat.toString(),
    lon: lon.toString(),
    format: "json",
    addressdetails: "1",
  });

  const url = `${NOMINATIM_BASE_URL}/reverse?${params}`;

  const response = await fetch(url, {
    headers: {
      "User-Agent": USER_AGENT,
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    if (response.status === 404) return null;
    throw new Error(`Nominatim error: ${response.status} ${response.statusText}`);
  }

  const data = (await response.json()) as NominatimResponse & {
    address?: Record<string, string>;
  };

  if (!data.lat || !data.lon) return null;

  return {
    display_name: data.display_name,
    lat: parseFloat(data.lat),
    lon: parseFloat(data.lon),
    address: data.address,
  };
}
