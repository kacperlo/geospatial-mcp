import type { OverpassElement } from "./overpass.js";
import type { OsmFeature } from "../db/duckdb.js";

export function normalizeElements(elements: OverpassElement[]): OsmFeature[] {
  const features: OsmFeature[] = [];

  for (const el of elements) {
    const feature = normalizeElement(el);
    if (feature) {
      features.push(feature);
    }
  }

  return features;
}

function normalizeElement(el: OverpassElement): OsmFeature | null {
  const osm_type = el.type;
  const osm_id = el.id;
  const tags_json = JSON.stringify(el.tags || {});

  let geom_wkt: string | null = null;

  if (el.type === "node") {
    // Node: use lat/lon directly
    if (el.lat !== undefined && el.lon !== undefined) {
      geom_wkt = `POINT(${el.lon} ${el.lat})`;
    }
  } else if (el.type === "way") {
    // Way: use geometry array or center
    if (el.geometry && el.geometry.length > 0) {
      geom_wkt = geometryToWkt(el.geometry);
    } else if (el.center) {
      geom_wkt = `POINT(${el.center.lon} ${el.center.lat})`;
    }
  } else if (el.type === "relation") {
    // Relation: use center if available
    if (el.center) {
      geom_wkt = `POINT(${el.center.lon} ${el.center.lat})`;
    } else if (el.members) {
      // Try to extract geometry from members
      const points: Array<{ lat: number; lon: number }> = [];
      for (const member of el.members) {
        if (member.geometry) {
          points.push(...member.geometry);
        }
      }
      if (points.length > 0) {
        // Use centroid of all points
        const avgLat = points.reduce((sum, p) => sum + p.lat, 0) / points.length;
        const avgLon = points.reduce((sum, p) => sum + p.lon, 0) / points.length;
        geom_wkt = `POINT(${avgLon} ${avgLat})`;
      }
    }
  }

  if (!geom_wkt) {
    return null;
  }

  return {
    osm_type,
    osm_id,
    tags_json,
    geom_wkt,
  };
}

function geometryToWkt(
  geometry: Array<{ lat: number; lon: number }>
): string | null {
  if (geometry.length === 0) return null;

  if (geometry.length === 1) {
    return `POINT(${geometry[0].lon} ${geometry[0].lat})`;
  }

  // Check if it's a closed polygon (first point == last point)
  const first = geometry[0];
  const last = geometry[geometry.length - 1];
  const isClosed = first.lat === last.lat && first.lon === last.lon;

  const coords = geometry.map((p) => `${p.lon} ${p.lat}`).join(", ");

  if (isClosed && geometry.length >= 4) {
    // Polygon requires at least 4 points (3 unique + closing)
    return `POLYGON((${coords}))`;
  }

  return `LINESTRING(${coords})`;
}
