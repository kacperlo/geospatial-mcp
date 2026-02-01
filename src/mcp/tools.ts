import { z } from "zod";
import { geocode as nominatimGeocode } from "../osm/nominatim.js";
import {
  runOverpass,
  buildOverpassQuery,
  type TagFilter,
} from "../osm/overpass.js";
import { normalizeElements } from "../osm/normalize.js";
import { insertFeatures, runQuery } from "../db/duckdb.js";

// Tool: geocode

export const geocodeSchema = z.object({
  query: z.string().describe("Address or place name to geocode"),
});

export type GeocodeInput = z.infer<typeof geocodeSchema>;

export async function geocodeTool(input: GeocodeInput) {
  const results = await nominatimGeocode(input.query);

  return {
    success: true,
    count: results.length,
    results: results.map((r) => ({
      display_name: r.display_name,
      lat: r.lat,
      lon: r.lon,
      bbox: r.bbox,
      osm_type: r.osm_type,
      osm_id: r.osm_id,
    })),
  };
}

// Tool: osm_fetch

const tagFilterSchema = z.object({
  key: z.string().describe("OSM tag key (e.g., 'amenity', 'brand', 'name')"),
  value: z.string().optional().describe("Tag value to match (optional, if omitted just checks key exists)"),
  regex: z.boolean().optional().describe("If true, value is treated as regex pattern"),
});

export const osmFetchSchema = z.object({
  center: z
    .object({
      lat: z.number().describe("Latitude of center point"),
      lon: z.number().describe("Longitude of center point"),
    })
    .optional()
    .describe("Center point for radius search"),
  radius_m: z
    .number()
    .optional()
    .describe("Search radius in meters (used with center)"),
  bbox: z
    .object({
      south: z.number(),
      west: z.number(),
      north: z.number(),
      east: z.number(),
    })
    .optional()
    .describe("Bounding box for search area"),
  elements: z
    .array(z.enum(["node", "way", "relation", "nwr"]))
    .optional()
    .describe("OSM element types to fetch (default: ['nwr'] = all)"),
  tags: z
    .array(tagFilterSchema)
    .describe("Tag filters for OSM features"),
  overpass_ql: z
    .string()
    .optional()
    .describe("Raw Overpass QL query (if provided, other filters are ignored)"),
  output: z
    .enum(["center", "geom"])
    .optional()
    .describe("Output mode: 'center' for centroids, 'geom' for full geometry"),
});

export type OsmFetchInput = z.infer<typeof osmFetchSchema>;

export async function osmFetchTool(input: OsmFetchInput) {
  let query: string;

  if (input.overpass_ql) {
    // Use raw OverpassQL if provided
    query = input.overpass_ql;
  } else {
    // Build query from structured options
    if (!input.tags || input.tags.length === 0) {
      throw new Error("Either 'overpass_ql' or 'tags' must be provided");
    }

    const tagFilters: TagFilter[] = input.tags.map((t) => ({
      key: t.key,
      value: t.value,
      regex: t.regex,
    }));

    query = buildOverpassQuery({
      center: input.center,
      radius_m: input.radius_m,
      bbox: input.bbox,
      elements: input.elements,
      tags: tagFilters,
      output: input.output,
    });
  }

  // Execute Overpass query
  const response = await runOverpass(query);

  // Normalize elements to features
  const features = normalizeElements(response.elements);

  // Debug: log first feature
  if (features.length > 0) {
    console.error("First feature sample:", JSON.stringify(features[0], null, 2));
  } else {
    console.error("No features normalized from", response.elements.length, "elements");
    if (response.elements.length > 0) {
      console.error("First element sample:", JSON.stringify(response.elements[0], null, 2));
    }
  }

  // Insert into DuckDB cache
  const insertedCount = await insertFeatures(features);

  return {
    success: true,
    fetched: response.elements.length,
    normalized: features.length,
    inserted: insertedCount,
    query_used: query,
    timestamp: new Date().toISOString(),
  };
}

// Tool: spatial_sql

export const spatialSqlSchema = z.object({
  sql: z.string().describe("SQL query to execute (SELECT only)"),
  params: z
    .record(z.union([z.string(), z.number(), z.boolean()]))
    .optional()
    .describe("Named parameters for the query (use $paramName in SQL)"),
});

export type SpatialSqlInput = z.infer<typeof spatialSqlSchema>;

// SQL validation - only allow SELECT statements
const FORBIDDEN_KEYWORDS = [
  "INSERT",
  "UPDATE",
  "DELETE",
  "DROP",
  "CREATE",
  "ALTER",
  "TRUNCATE",
  "ATTACH",
  "DETACH",
  "COPY",
  "EXPORT",
  "IMPORT",
  "PRAGMA",
  "VACUUM",
];

function validateSql(sql: string): void {
  const upperSql = sql.toUpperCase().trim();

  // Must start with SELECT or WITH (for CTEs)
  if (!upperSql.startsWith("SELECT") && !upperSql.startsWith("WITH")) {
    throw new Error("Only SELECT queries are allowed");
  }

  // Check for forbidden keywords
  for (const keyword of FORBIDDEN_KEYWORDS) {
    // Match keyword as whole word
    const regex = new RegExp(`\\b${keyword}\\b`, "i");
    if (regex.test(sql)) {
      throw new Error(`Forbidden keyword: ${keyword}`);
    }
  }

  // Block file access functions
  const filePatterns = [
    /read_csv/i,
    /read_parquet/i,
    /read_json/i,
    /read_blob/i,
    /glob\s*\(/i,
  ];

  for (const pattern of filePatterns) {
    if (pattern.test(sql)) {
      throw new Error("File access functions are not allowed");
    }
  }
}

export async function spatialSqlTool(input: SpatialSqlInput) {
  // Validate SQL for safety
  validateSql(input.sql);

  // Execute query
  const rows = await runQuery(input.sql, input.params);

  return {
    success: true,
    row_count: rows.length,
    rows: rows,
  };
}

// Tool definitions for MCP registration

export const toolDefinitions = [
  {
    name: "geocode",
    description:
      "Geocode an address or place name to get coordinates (lat, lon). Uses Nominatim API. Returns up to 5 candidates with display name, coordinates, and bounding box.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Address or place name to geocode (e.g., 'Plac Politechniki 1, Warszawa')",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "osm_fetch",
    description:
      "Fetch OSM features from Overpass API and cache them in DuckDB. Use tag filters to find any type of feature (restaurants, parks, shops, etc.). Results are stored locally for subsequent spatial queries.",
    inputSchema: {
      type: "object" as const,
      properties: {
        center: {
          type: "object",
          properties: {
            lat: { type: "number", description: "Latitude" },
            lon: { type: "number", description: "Longitude" },
          },
          required: ["lat", "lon"],
          description: "Center point for radius search",
        },
        radius_m: {
          type: "number",
          description: "Search radius in meters (use with center)",
        },
        bbox: {
          type: "object",
          properties: {
            south: { type: "number" },
            west: { type: "number" },
            north: { type: "number" },
            east: { type: "number" },
          },
          required: ["south", "west", "north", "east"],
          description: "Bounding box for search",
        },
        elements: {
          type: "array",
          items: { type: "string", enum: ["node", "way", "relation", "nwr"] },
          description: "OSM element types (default: ['nwr'] = all)",
        },
        tags: {
          type: "array",
          items: {
            type: "object",
            properties: {
              key: { type: "string", description: "Tag key (e.g., 'amenity', 'brand', 'name')" },
              value: { type: "string", description: "Tag value (optional)" },
              regex: { type: "boolean", description: "Treat value as regex" },
            },
            required: ["key"],
          },
          description: "Tag filters (e.g., [{key:'amenity', value:'restaurant'}])",
        },
        overpass_ql: {
          type: "string",
          description: "Raw Overpass QL query (overrides other filters)",
        },
        output: {
          type: "string",
          enum: ["center", "geom"],
          description: "Output mode: 'center' or 'geom' (full geometry)",
        },
      },
      required: [],
    },
  },
  {
    name: "spatial_sql",
    description:
      "Execute spatial SQL queries on cached OSM data in DuckDB. The osm_features table has columns: osm_type, osm_id, tags_json (JSON), geom_wkt (text), geom (GEOMETRY). Use DuckDB Spatial functions like ST_Distance_Sphere, ST_DWithin_Spheroid, ST_X, ST_Y, etc.",
    inputSchema: {
      type: "object" as const,
      properties: {
        sql: {
          type: "string",
          description: "SELECT query to execute. Use $paramName for parameters.",
        },
        params: {
          type: "object",
          additionalProperties: {
            oneOf: [{ type: "string" }, { type: "number" }, { type: "boolean" }],
          },
          description: "Named parameters for the query",
        },
      },
      required: ["sql"],
    },
  },
];
