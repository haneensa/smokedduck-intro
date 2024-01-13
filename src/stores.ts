import { writable } from "svelte/store"
import * as duckdb from './../duckdb-browser.mjs';

export let selectedOpids = writable([])
export let lineageData = writable(null)

export async function initDuckDB() {
    try {
        const DUCKDB_CONFIG = await duckdb.selectBundle({
            mvp: {
                mainModule: './duckdb-mvp.wasm',
                mainWorker: './duckdb-browser-mvp.worker.js',
            },
            eh: {
                mainModule: './duckdb-eh.wasm',
                mainWorker: './duckdb-browser-eh.worker.js',
            },
        });

        const logger = new duckdb.ConsoleLogger();
        const worker = new Worker(DUCKDB_CONFIG.mainWorker);
        const db = new duckdb.AsyncDuckDB(logger, worker);
        await db.instantiate(DUCKDB_CONFIG.mainModule, DUCKDB_CONFIG.pthreadWorker);

        let conn = await db.connect();
        await conn.query(`CREATE TABLE t1(i INTEGER, j INTEGER);`);
        const schema = await conn.query(`pragma show_tables`);
        console.log("Schema: ", schema)
        return conn;
    } catch (e) {
        console.error(e);
    }
}
