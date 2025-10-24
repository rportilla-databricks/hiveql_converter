# HQL to Spark SQL Migration Tool

**Automated HiveQL to Databricks Spark SQL conversion with production validation.**

## Quick Start

```bash
# 1. Generate sample data + convert HQL + run integration tests (ALL IN ONE)
python scripts/smart_convert_and_validate.py
python scripts/integration_test.py

# That's it! Check spark_sql_final/ for converted SQL
```

**Current Results: 64.3% pass rate (18/28 queries) with end-to-end execution validation**

---

## What This Tool Does

1. **Auto-fixes** common Hive→Spark incompatibilities
2. **AI converts** complex queries when needed (using Databricks DBRX)
3. **Validates** with EXPLAIN + actual execution in Databricks
4. **Tests** all converted SQL end-to-end against live warehouse

---

## Conversion Coverage

### ✅ Automatically Fixed

| Hive Syntax | Conversion | Notes |
|------------|------------|-------|
| `SET hive.*` / `SET mapreduce.*` | Removed | Hive/MapReduce config not applicable in Databricks |
| `TABLESAMPLE(BUCKET X OUT OF Y)` | Removed | Spark doesn't support bucketed sampling in FROM clause |
| `CLUSTERED BY (...) INTO X BUCKETS` | Removed from CTAS | Use liquid clustering instead |
| `DISTRIBUTE BY col SORT BY col` | `CLUSTER BY (col)` in table definition | Databricks liquid clustering |
| `/*+ MAPJOIN(t) */` | Removed | Databricks AQE handles this automatically |
| `/*+ STREAMTABLE(t) */` | Removed | Hive-specific hint |
| `USING PARQUET` / `STORED AS ORC` | `USING ICEBERG` | Managed Iceberg tables |
| `TBLPROPERTIES(...)` | `OPTIONS(...)` | Databricks syntax |
| `ADD JAR` + Hive Java UDFs | SQL UDF placeholder | Creates TODO with implementation options |

### ⚠️ Requires Manual Review

| Pattern | Issue | Fix |
|---------|-------|-----|
| `SELECT DISTINCT` + window functions | Hive allows, Spark requires GROUP BY | Add GROUP BY or use ROW_NUMBER() |
| `RANK() OVER (ORDER BY AVG(col))` | Aggregates in window ORDER BY not supported | Pre-compute in CTE |
| Mixed aggregates + window functions | Invalid in Spark | Separate into CTEs |
| Some correlated subqueries | Pattern-dependent | Rewrite as JOINs or windows |

### 🤖 AI-Assisted (Claude Sonnet 4.5)

When auto-fixes fail, uses `AI_QUERY` with `databricks-claude-sonnet-4-5` to convert complex SQL.

---

## Project Structure

```
code_migration/
├── hql_original/              # Your original HiveQL files
├── spark_sql_final/           # ✅ Converted Spark SQL (production-ready)
├── scripts/
│   ├── smart_convert_and_validate.py    # Main conversion tool
│   ├── integration_test.py              # End-to-end execution tests
│   └── generate_sample_data.py          # Creates test data in hql_test schema
└── README.md                  # This file
```

---

## Configuration

### Databricks Profile

Edit `~/.databrickscfg`:

```ini
[fe]  # or your profile name
host = https://your-workspace.cloud.databricks.com
token = dapi...
```

### Update Scripts

All scripts use these settings:
```python
DATABRICKS_PROFILE = 'fe'
WAREHOUSE_ID = '4b9b953939869799'
SCHEMA = 'hql_test'  # Isolated test schema
```

---

## Conversion Process

```
┌─────────────────────┐
│  Original HQL       │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Check for known    │◄─── CLUSTERED BY in CTAS?
│  auto-fixable       │     MAPJOIN hints?
│  patterns           │     TABLESAMPLE?
└──────────┬──────────┘     DISTRIBUTE BY?
           │
           ▼
┌─────────────────────┐
│  Apply auto-fixes   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Validate with      │
│  EXPLAIN            │
└──────────┬──────────┘
           │
      ┌────┴────┐
      │ Works?  │
      └────┬────┘
      Yes  │  No
      │    │
      │    ▼
      │  ┌─────────────────────┐
      │  │  AI_QUERY           │
      │  │  (DBRX model)       │
      │  └──────────┬──────────┘
      │             │
      ▼             ▼
┌─────────────────────┐
│  Save to            │
│  spark_sql_final/   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Integration Test:  │
│  Execute in         │
│  Databricks         │
└─────────────────────┘
```

---

## Sample Data Schema

All tests run in isolated `hql_test` schema with these tables:

- `sales_fact` - Transaction data
- `customers` - Customer details
- `products` - Product catalog
- `stores` - Store locations
- `reviews` - Product reviews (for UDF testing)
- `raw_event_stream` - Event data (for JSON/MAP testing)

**Note:** Tables are auto-generated by `integration_test.py` before each run.

---

## Known Limitations

### EXPLAIN is Too Lenient

`EXPLAIN` validates syntax but misses runtime errors:
- ❌ `CLUSTERED BY` in CTAS (fixed by explicit pre-check)
- ❌ `SELECT DISTINCT` + window functions without GROUP BY
- ❌ Aggregate functions in window ORDER BY

**Solution:** Integration tests execute queries to catch real errors.

### UDF Migration

Hive Java UDFs are converted to SQL placeholder functions:

```sql
-- Original Hive
ADD JAR /user/hive/udfs/sentiment.jar;
CREATE TEMPORARY FUNCTION sentiment_score AS 'com.example.SentimentUDF';

-- Converted (placeholder)
CREATE OR REPLACE FUNCTION sentiment_score(text STRING) RETURNS DOUBLE
RETURN 0.5;  -- TODO: Implement actual logic
```

**Recommended:**
1. Use Databricks AI functions: `ai_analyze_sentiment(text)`
2. Create Python UDF in notebook
3. Store JAR in Unity Catalog Volumes: `/Volumes/catalog/schema/volume/udf.jar`

---

## Test Results Summary

**Pass Rate: 64.3% (18/28)**

### Failures by Root Cause:
1. **UDF resolution** (5 failures) - Session-scoped UDFs not persisting
2. **SELECT DISTINCT + windows** (1 failure) - Hive→Spark incompatibility
3. **Complex subqueries** (2 failures) - Correlated subquery patterns
4. **Duplicate map keys** (1 failure) - Data quality issue
5. **Type mismatches** (1 failure) - EXTRACT on STRING type

---

## Troubleshooting

### "Table not found" errors
- Run `python scripts/generate_sample_data.py` first
- Check you're using `hql_test` schema

### "UDF not found" errors
- UDFs are session-scoped and may not persist
- Consider converting to Databricks AI functions or permanent UDFs

### "CLUSTERED BY not supported"
- Should be auto-fixed
- Check `spark_sql_final/` output has it removed

### Permission errors
- Verify `.databrickscfg` has correct profile
- Ensure warehouse ID is valid
- Check schema permissions

---

## Contributing

To add new HQL files:
1. Place `.hql` file in `hql_original/`
2. Run `python scripts/smart_convert_and_validate.py`
3. Review output in `spark_sql_final/`
4. Run `python scripts/integration_test.py` to validate

---

## References

- [Databricks CLUSTER BY (Liquid Clustering)](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-cluster-by.html)
- [Databricks TABLESAMPLE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-sampling.html)
- [Databricks AI Functions](https://docs.databricks.com/sql/language-manual/functions/ai_analyze_sentiment.html)
- [Unity Catalog Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
