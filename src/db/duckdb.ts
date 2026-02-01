import duckdb from "duckdb";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_PATH = path.join(__dirname, "../../data/osm.duckdb");

let db: duckdb.Database | null = null;
let connection: duckdb.Connection | null = null;

export async function initDatabase(): Promise<void> {
  return new Promise((resolve, reject) => {
    db = new duckdb.Database(DB_PATH, (err) => {
      if (err) {
        reject(err);
        return;
      }

      connection = db!.connect();

      connection.run("INSTALL spatial;", (err) => {
        if (err) {
          console.error("Note: spatial install:", err.message);
        }

        connection!.run("LOAD spatial;", (err) => {
          if (err) {
            reject(err);
            return;
          }

          const createTableSQL = `
            CREATE TABLE IF NOT EXISTS osm_features (
              osm_type VARCHAR NOT NULL,
              osm_id BIGINT NOT NULL,
              tags_json JSON,
              geom_wkt VARCHAR,
              geom GEOMETRY,
              fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (osm_type, osm_id)
            );
          `;

          connection!.run(createTableSQL, (err) => {
            if (err) {
              reject(err);
              return;
            }

            resolve();
          });
        });
      });
    });
  });
}

export function getConnection(): duckdb.Connection {
  if (!connection) {
    throw new Error("Database not initialized. Call initDatabase() first.");
  }
  return connection;
}

export async function runQuery<T = Record<string, unknown>>(
  sql: string,
  params?: Record<string, string | number | boolean>
): Promise<T[]> {
  const conn = getConnection();

  return new Promise((resolve, reject) => {
    let finalSql = sql;
    if (params) {
      for (const [key, value] of Object.entries(params)) {
        const placeholder = `$${key}`;
        const escapedValue =
          typeof value === "string"
            ? `'${value.replace(/'/g, "''")}'`
            : String(value);
        finalSql = finalSql.split(placeholder).join(escapedValue);
      }
    }

    conn.all(finalSql, (err, rows) => {
      if (err) {
        reject(err);
        return;
      }
      resolve((rows || []) as T[]);
    });
  });
}

export async function runStatement(sql: string): Promise<void> {
  const conn = getConnection();

  return new Promise((resolve, reject) => {
    conn.run(sql, (err) => {
      if (err) {
        reject(err);
        return;
      }
      resolve();
    });
  });
}

export interface OsmFeature {
  osm_type: string;
  osm_id: number;
  tags_json: string;
  geom_wkt: string;
}

export async function insertFeatures(features: OsmFeature[]): Promise<number> {
  if (features.length === 0) return 0;

  const conn = getConnection();
  let inserted = 0;

  for (const feature of features) {
    if (!feature.geom_wkt || typeof feature.geom_wkt !== 'string' || feature.geom_wkt.trim() === '') {
      console.error(`Skipping feature ${feature.osm_type}/${feature.osm_id}: invalid geom_wkt`);
      continue;
    }

    const escapedWkt = feature.geom_wkt.replace(/'/g, "''");
    const escapedTags = feature.tags_json.replace(/'/g, "''");
    
    const sql = `
      INSERT OR REPLACE INTO osm_features (osm_type, osm_id, tags_json, geom_wkt, geom)
      VALUES ('${feature.osm_type}', ${feature.osm_id}, '${escapedTags}', '${escapedWkt}', ST_GeomFromText('${escapedWkt}'))
    `;

    await new Promise<void>((resolve) => {
      conn.run(sql, (err) => {
        if (err) {
          console.error(`Insert error for ${feature.osm_type}/${feature.osm_id}:`, err.message);
          resolve();
        } else {
          inserted++;
          resolve();
        }
      });
    });
  }

  return inserted;
}

export async function closeDatabase(): Promise<void> {
  return new Promise((resolve) => {
    if (db) {
      db.close(() => {
        db = null;
        connection = null;
        resolve();
      });
    } else {
      resolve();
    }
  });
}
