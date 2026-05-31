"""ONE-OFF RECOVERY — run tomorrow once the Databricks serverless daily quota resets.

Context (2026-05-31): a tz round-trip bug in an ad-hoc cleanup deleted ALL THREE of
today's financials_raw scrapes instead of just the two superseded ones. The cleaned/
published tables (main.financials.financials, *_metrics) are intact — only the RAW
audit rows for today were removed. They are fully recoverable: the bad DELETE is Delta
version 120, and version 119 (pre-delete) still holds today's 33,448,710 rows. VACUUM
ran with RETAIN 168 HOURS, so the files are kept for ~7 days from 2026-05-31 ~21:00.

This script:
  1. RESTOREs financials_raw to v119 (brings back all 3 of today's scrapes).
  2. Deletes ONLY today's superseded scrapes (keeps the latest = 19:00:52 UTC), entirely
     in SQL so there's no Python timestamp round-trip (that was the original bug).
  3. OPTIMIZEs.

Run:  PYTHONIOENCODING=utf-8 PYTHONUTF8=1 python recover_financials_raw.py
"""
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()
RAW = "main.financials.financials_raw"

print("=== RESTORE to version 119 (pre-bad-delete) ===")
spark.sql(f"RESTORE TABLE {RAW} TO VERSION AS OF 119")
spark.sql(f"""SELECT scraped_at, COUNT(*) AS rows FROM {RAW}
             WHERE DATE(scraped_at)=CURRENT_DATE() GROUP BY scraped_at ORDER BY scraped_at""").show(truncate=False)

print("=== tz-safe delete of today's superseded scrapes (keep latest) ===")
spark.sql(f"""CREATE OR REPLACE TEMP VIEW _today_max AS
              SELECT MAX(scraped_at) AS m FROM {RAW} WHERE DATE(scraped_at)=CURRENT_DATE()""")
spark.sql(f"""DELETE FROM {RAW}
              WHERE DATE(scraped_at)=CURRENT_DATE()
                AND scraped_at < (SELECT m FROM _today_max)""")
print("DELETE done")

print("OPTIMIZE...")
spark.sql(f"OPTIMIZE {RAW}").show(truncate=False)

print("=== AFTER (today should show only the single latest scrape) ===")
spark.sql(f"SELECT COUNT(*) AS total_rows FROM {RAW}").show()
spark.sql(f"""SELECT scraped_at, COUNT(*) AS rows FROM {RAW}
             WHERE DATE(scraped_at)=CURRENT_DATE() GROUP BY scraped_at ORDER BY scraped_at""").show(truncate=False)
