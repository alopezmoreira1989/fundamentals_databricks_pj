import * as duckdb from "@duckdb/duckdb-wasm";

// Single shared DuckDB-WASM instance for the whole app — instantiating per query is
// slow (downloads + compiles the WASM bundle each time). Browser-only: this must only
// be called from client components (it constructs a Web Worker).
let dbPromise: Promise<duckdb.AsyncDuckDB> | null = null;

export async function getDuckDb(): Promise<duckdb.AsyncDuckDB> {
  if (dbPromise) return dbPromise;
  dbPromise = (async () => {
    // Auto-select the right WASM build (mvp / eh / coi) for this browser, served from jsDelivr.
    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);

    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker!}");`], { type: "text/javascript" }),
    );
    const worker = new Worker(workerUrl);
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);
    return db;
  })();
  return dbPromise;
}
