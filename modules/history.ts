/**
 * History Module — queries tentacle-history's TimescaleDB
 *
 * Provides time-series queries with automatic downsampling,
 * aggregation, and left-edge value injection for chart continuity.
 */

//@deno-types="npm:@types/pg@^8.11.10"
import pg from "npm:pg@^8.13.0";
const { Pool } = pg;

import { createLogger, LogLevel } from "@joyautomation/coral";

const log = createLogger("history", LogLevel.info);

let pool: pg.Pool | null = null;

type HistoryConfig = {
  host: string;
  port: string;
  user: string;
  password: string;
  name: string;
  ssl: boolean;
};

export function initHistoryDb(): HistoryConfig | null {
  const host = Deno.env.get("TENTACLE_DB_HOST");
  if (!host) {
    log.info("TENTACLE_DB_HOST not set — history queries disabled");
    return null;
  }

  const config: HistoryConfig = {
    host,
    port: Deno.env.get("TENTACLE_DB_PORT") ?? "5432",
    user: Deno.env.get("TENTACLE_DB_USER") ?? "postgres",
    password: Deno.env.get("TENTACLE_DB_PASSWORD") ?? "postgres",
    name: Deno.env.get("TENTACLE_DB_NAME") ?? "tentacle",
    ssl: Deno.env.get("TENTACLE_DB_SSL") === "true",
  };

  const connectionString = `postgres://${config.user}:${config.password}@${config.host}:${config.port}/${config.name}`;
  pool = new Pool({
    connectionString,
    ssl: config.ssl ? true : undefined,
    max: 5,
  });

  log.info(`History DB connected: ${config.host}:${config.port}/${config.name}`);
  return config;
}

export function getHistoryPool(): pg.Pool | null {
  return pool;
}

export type VariableHistoryInput = {
  moduleId: string;
  variableId: string;
};

export type HistoryPoint = {
  value: string | null;
  timestamp: Date;
};

export type VariableHistory = {
  moduleId: string;
  variableId: string;
  history: HistoryPoint[];
};

/**
 * Query historical variable data with optional downsampling.
 */
export async function getHistory(params: {
  variables: VariableHistoryInput[];
  start: Date;
  end: Date;
  interval?: string | null;
  samples?: number | null;
  raw?: boolean | null;
}): Promise<VariableHistory[]> {
  if (!pool) throw new Error("History database not configured");

  const { variables, start, end, interval, samples, raw } = params;
  const effectiveSamples = samples ?? 100;
  const autoInterval = `${Math.max(1, Math.floor((end.getTime() - start.getTime()) / (1000 * effectiveSamples)))} seconds`;
  const bucketInterval = interval ?? autoInterval;

  const client = await pool.connect();
  try {
    // Build the metric filter
    const metricFilter = variables
      .map((v) => `('${escapeSql(v.moduleId)}', '${escapeSql(v.variableId)}')`)
      .join(", ");

    // Left-edge query: get the most recent value before the start time
    const leftEdgeResult = await client.query(`
      SELECT module_id, variable_id,
             COALESCE(float_value, int_value::real, bool_value::int::real) as value
      FROM history
      WHERE (module_id, variable_id, timestamp) IN (
        SELECT module_id, variable_id, MAX(timestamp)
        FROM history
        WHERE (module_id, variable_id) IN (${metricFilter})
          AND timestamp < $1
        GROUP BY module_id, variable_id
      )
    `, [start]);

    const leftEdgeValues = new Map<string, number>();
    for (const row of leftEdgeResult.rows) {
      if (row.value !== null) {
        leftEdgeValues.set(`${row.module_id}:${row.variable_id}`, Number(row.value));
      }
    }

    // Main history query
    let query: string;
    if (raw === true) {
      query = `
        SELECT timestamp as time,
               CONCAT(module_id, ':', variable_id) as name,
               COALESCE(float_value, int_value::real, bool_value::int::real) as value
        FROM history
        WHERE (module_id, variable_id) IN (${metricFilter})
          AND timestamp BETWEEN $1 AND $2
        ORDER BY time ASC
      `;
    } else {
      query = `
        SELECT time, json_object_agg(name, value) as data
        FROM (
          SELECT time_bucket('${escapeSql(bucketInterval)}', timestamp) as time,
                 CONCAT(module_id, ':', variable_id) as name,
                 COALESCE(AVG(float_value), AVG(int_value::real), AVG(bool_value::int::real)) as value
          FROM history
          WHERE (module_id, variable_id) IN (${metricFilter})
            AND timestamp BETWEEN $1 AND $2
          GROUP BY time, name
          ORDER BY time ASC
        ) as bucketed
        GROUP BY time
        ORDER BY time ASC
      `;
    }

    const result = await client.query(query, [start, end]);

    // Build response per variable
    return variables.map((v) => {
      const key = `${v.moduleId}:${v.variableId}`;
      let points: HistoryPoint[];

      if (raw === true) {
        points = result.rows
          .filter((r: Record<string, unknown>) => r.name === key)
          .map((r: Record<string, unknown>) => ({
            timestamp: new Date(r.time as string),
            value: r.value != null ? String(r.value) : null,
          }));
      } else {
        points = result.rows
          .map((r: Record<string, unknown>) => ({
            timestamp: new Date(r.time as string),
            value: (r.data as Record<string, unknown>)?.[key] != null
              ? String((r.data as Record<string, unknown>)[key])
              : null,
          }))
          .filter((p: HistoryPoint) => p.value !== null);
      }

      // Prepend left edge value for chart continuity
      const leftEdge = leftEdgeValues.get(key);
      if (leftEdge !== undefined) {
        const first = points[0];
        if (!first || first.timestamp.getTime() > start.getTime()) {
          points.unshift({ timestamp: start, value: String(leftEdge) });
        }
      }

      return { moduleId: v.moduleId, variableId: v.variableId, history: points };
    });
  } finally {
    client.release();
  }
}

/**
 * Get usage statistics
 */
export async function getUsage(): Promise<{ totalCount: number; byMonth: Array<{ year: number; month: number; count: number }> }> {
  if (!pool) throw new Error("History database not configured");

  const client = await pool.connect();
  try {
    // Total count
    let totalCount: number;
    try {
      const approx = await client.query(`SELECT approximate_row_count('history') as count`);
      totalCount = Number(approx.rows[0]?.count ?? 0);
    } catch {
      const exact = await client.query(`SELECT COUNT(*)::text as count FROM history`);
      totalCount = parseInt(exact.rows[0]?.count || "0", 10);
    }

    // By month
    let byMonth: Array<{ year: number; month: number; count: number }> = [];
    try {
      const monthResult = await client.query(`
        SELECT
          EXTRACT(YEAR FROM range_start)::int as year,
          EXTRACT(MONTH FROM range_start)::int as month,
          SUM(c.reltuples)::int as count
        FROM timescaledb_information.chunks ch
        JOIN pg_class c ON c.relname = ch.chunk_name
        WHERE ch.hypertable_name = 'history'
        GROUP BY year, month
        ORDER BY year DESC, month DESC
      `);
      byMonth = monthResult.rows;
    } catch {
      const monthResult = await client.query(`
        SELECT
          EXTRACT(YEAR FROM timestamp)::int as year,
          EXTRACT(MONTH FROM timestamp)::int as month,
          COUNT(*)::int as count
        FROM history
        GROUP BY year, month
        ORDER BY year DESC, month DESC
      `);
      byMonth = monthResult.rows;
    }

    return { totalCount, byMonth };
  } finally {
    client.release();
  }
}

function escapeSql(str: string): string {
  return str.replace(/'/g, "''");
}
