import * as duckdb from '@duckdb/duckdb-wasm';
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import mvp_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';
import duckdb_wasm_eh from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import eh_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';

import { writable } from "svelte/store"

export let selectedOpids = writable([])
export let lineageData = writable(null)

export async function initDuckDB() {
    try {

      const MANUAL_BUNDLES: duckdb.DuckDBBundles = {
          mvp: {
              mainModule: duckdb_wasm,
              mainWorker: mvp_worker,
          },
          eh: {
              mainModule: duckdb_wasm_eh,
              mainWorker: eh_worker,
          },
      };
        // Select a bundle based on browser checks
        const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);
        // Instantiate the asynchronus version of DuckDB-wasm
        const logger = new duckdb.ConsoleLogger();
        const worker = new Worker(bundle.mainWorker!);
        const db = new duckdb.AsyncDuckDB(logger, worker);
        await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

        let conn = await db.connect();
        
        let customers_csv =
        `cid,name
      1,Alice
      2,Bob
      3,Ellen`;
        
      let sales_csv =
        `oid,cid,amount
      101,1,50
      102,1,30
      103,2,25
      104,3,40
      105,3,15`;

        await db.registerFileText(`data.csv`, customers_csv);
        await conn.insertCSVFromPath('data.csv', {
          schema: 'main',
          name: `customers`,
          detect: true,
          header: true,
          delimiter: ','
          });
        
      await db.registerFileText(`data.csv`, sales_csv);
        await conn.insertCSVFromPath('data.csv', {
          schema: 'main',
          name: `sales`,
          detect: true,
          header: true,
          delimiter: ','
          });
      

        return [db, conn];
    } catch (e) {
        console.error(e);
    }
}
