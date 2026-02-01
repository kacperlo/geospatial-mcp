const USER_AGENT = "MCP-OSM-DuckDB/1.0 (educational project)";

function getOverpassApiUrls(): string[] {
  // Allow overriding via env:
  // - OVERPASS_API_URL="https://.../interpreter"
  // - OVERPASS_API_URLS="https://a/.../interpreter,https://b/.../interpreter"
  const urlsEnv =
    process.env.OVERPASS_API_URLS?.trim() ?? process.env.OVERPASS_API_URL?.trim();

  const fromEnv = urlsEnv
    ? urlsEnv
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : [];

  const defaults = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
  ];

  const all = [...fromEnv, ...defaults];
  return Array.from(new Set(all));
}

let lastRequestTime = 0;
const MIN_REQUEST_INTERVAL_MS = 2000;

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

export interface OverpassElement {
  type: "node" | "way" | "relation";
  id: number;
  lat?: number;
  lon?: number;
  center?: { lat: number; lon: number };
  geometry?: Array<{ lat: number; lon: number }>;
  tags?: Record<string, string>;
  members?: Array<{
    type: string;
    ref: number;
    role: string;
    geometry?: Array<{ lat: number; lon: number }>;
  }>;
}

export interface OverpassResponse {
  version: number;
  generator: string;
  elements: OverpassElement[];
}

export async function runOverpass(query: string): Promise<OverpassResponse> {
  await throttle();

  const urls = getOverpassApiUrls();
  const body = `data=${encodeURIComponent(query)}`;

  const transientStatuses = new Set([
    408,
    425,
    429,
    500,
    502,
    503,
    504,
    520,
  ]);

  const maxAttempts = Math.max(2, urls.length);
  let lastError: unknown;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const url = urls[attempt % urls.length];

    try {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "User-Agent": USER_AGENT,
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body,
      });

      if (response.ok) {
        return (await response.json()) as OverpassResponse;
      }

      const text = await response.text();
      const err = new Error(
        `Overpass error: ${response.status} - ${text.slice(0, 200)}`
      );

      if (!transientStatuses.has(response.status)) {
        throw err;
      }

      lastError = err;
    } catch (err) {
      lastError = err;
    }

    const backoffMs = Math.min(8000, 500 * 2 ** attempt);
    await new Promise((r) => setTimeout(r, backoffMs));
  }

  const msg = lastError instanceof Error ? lastError.message : String(lastError);
  throw new Error(`Overpass failed after retries. Last error: ${msg}`);
}

export interface TagFilter {
  key: string;
  value?: string;
  regex?: boolean;
}

export interface OverpassQueryOptions {
  center?: { lat: number; lon: number };
  radius_m?: number;
  bbox?: { south: number; west: number; north: number; east: number };
  elements?: Array<"node" | "way" | "relation" | "nwr">;
  tags: TagFilter[];
  output?: "center" | "geom";
  timeout?: number;
}

export function buildOverpassQuery(options: OverpassQueryOptions): string {
  const timeout = options.timeout ?? 30;
  const output = options.output ?? "center";
  const elements = options.elements ?? ["nwr"];

  let areaFilter = "";
  if (options.center && options.radius_m) {
    areaFilter = `(around:${options.radius_m},${options.center.lat},${options.center.lon})`;
  } else if (options.bbox) {
    areaFilter = `(${options.bbox.south},${options.bbox.west},${options.bbox.north},${options.bbox.east})`;
  }

  const tagFilters = options.tags
    .map((t) => {
      if (t.value === undefined) {
        return `["${t.key}"]`;
      }
      if (t.regex) {
        return `["${t.key}"~"${t.value}"]`;
      }
      return `["${t.key}"="${t.value}"]`;
    })
    .join("");

  const queries = elements
    .map((el) => `${el}${tagFilters}${areaFilter};`)
    .join("\n  ");

  const outFormat = output === "geom" ? "out geom;" : "out center;";

  return `
[out:json][timeout:${timeout}];
(
  ${queries}
);
${outFormat}
`.trim();
}
