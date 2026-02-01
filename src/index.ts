#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

import { initDatabase } from "./db/duckdb.js";
import {
  toolDefinitions,
  geocodeTool,
  geocodeSchema,
  osmFetchTool,
  osmFetchSchema,
  spatialSqlTool,
  spatialSqlSchema,
} from "./mcp/tools.js";

const server = new Server(
  {
    name: "mcp-osm-duckdb",
    version: "1.0.0",
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

// Handle tool listing
server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: toolDefinitions,
  };
});

// Handle tool execution
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    switch (name) {
      case "geocode": {
        const input = geocodeSchema.parse(args);
        const result = await geocodeTool(input);
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(result, null, 2),
            },
          ],
        };
      }

      case "osm_fetch": {
        const input = osmFetchSchema.parse(args);
        const result = await osmFetchTool(input);
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(result, null, 2),
            },
          ],
        };
      }

      case "spatial_sql": {
        const input = spatialSqlSchema.parse(args);
        const result = await spatialSqlTool(input);
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(result, null, 2),
            },
          ],
        };
      }

      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify({ success: false, error: message }, null, 2),
        },
      ],
      isError: true,
    };
  }
});

async function main() {
  // Initialize database
  console.error("Initializing DuckDB with spatial extension...");
  await initDatabase();
  console.error("Database initialized.");

  // Start MCP server
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("MCP server running on stdio");
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
