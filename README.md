# Simple Pipeline in Fabric

An end-to-end ETL (Extract, Transform, Load) project in **Microsoft Fabric**. A Data Pipeline ingests a CSV file from an external source into a Lakehouse, a PySpark notebook transforms it, and the result is loaded into a Delta table. The notebook is parameterized and orchestrated by the pipeline, so the process is reusable.

![Pipeline run succeeded](screenshots/pipeline-run-success.png)

## Architecture

```
HTTP source (sales.csv)
        |
        v
[Delete data]  -->  [Copy data]  -->  [Notebook: Load Sales]
 clears old CSVs     lands CSV in       transforms and writes
 in Files/new_data   Files/new_data     Delta table (new_sales)
```

## Tools and Services

- Microsoft Fabric (Workspace, Lakehouse, Data Pipeline, Notebook)
- PySpark
- Delta Lake
- OneLake

## What I Built

1. **Lakehouse** with a `new_data` subfolder under `Files` as the landing zone.
2. **Data pipeline (`Ingest Sales Data`)** with a Copy data activity that pulls `sales.csv` over HTTP into the lakehouse.
3. **Notebook (`Load Sales`)** that:
   - reads the CSV files from `Files/new_data`
   - adds `Year` and `Month` columns from `OrderDate`
   - splits `CustomerName` into `FirstName` and `LastName`
   - selects and reorders the final columns
   - appends the result to a Delta table
4. **Parameter cell** (`table_name`) so the target table can be overridden by the pipeline.
5. **Pipeline orchestration**: a Delete data activity (clears old CSVs), then Copy data, then the Notebook activity, with `table_name = dbo.new_sales` passed in as a base parameter.

## Notebook Code

**Cell 1 (parameter cell)**

```python
table_name = "dbo.sales"
```

**Cell 2 (transformation)**

```python
from pyspark.sql.functions import *

# Read the new sales data
df = spark.read.format("csv").option("header", "true").load("Files/new_data/*.csv")

# Add month and year columns
df = df.withColumn("Year", year(col("OrderDate"))).withColumn("Month", month(col("OrderDate")))

# Derive FirstName and LastName columns
df = (df.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0))
        .withColumn("LastName", split(col("CustomerName"), " ").getItem(1)))

# Filter and reorder columns
df = df.select("SalesOrderNumber", "SalesOrderLineNumber", "OrderDate", "Year", "Month",
               "FirstName", "LastName", "EmailAddress", "Item", "Quantity", "UnitPrice", "TaxAmount")

# Load the data into a table
df.write.format("delta").mode("append").saveAsTable(table_name)
```

## Challenges and Fixes

| Problem | Cause | Fix |
|---|---|---|
| `Bad Request 400 ... WorkspaceId and ArtifactId should be either valid Guids or valid Names` | The relative path `Files/new_data/*.csv` needs a default lakehouse, and none was attached to the session | Added the lakehouse under **Data items**, set it as default, and restarted the session |
| `Spark SQL queries are only possible in the context of a lakehouse` when run from the pipeline | Lakehouse attachment not picked up by the pipeline run | Removed all lakehouses from the notebook, re-added the lakehouse, saved, and re-ran the pipeline |

## Key Concepts Practiced

- Pipeline orchestration with activity dependencies (On completion)
- Parameterizing notebooks and passing values from a pipeline
- Lakehouse `Files` vs `Tables` areas
- Delta table writes in append mode
- Relative paths and the default lakehouse in Fabric notebooks

## Possible Improvements

- Define an explicit schema instead of reading every column as a string
- Use `inferSchema` or typed casts for `OrderDate`, `Quantity`, and `UnitPrice`
- Switch from append to merge/upsert to avoid duplicate loads
- Add a schedule trigger and failure alerts
- Add data quality checks (nulls, duplicates) before the write

## Based On

Microsoft Learn lab: *Ingest data with a pipeline in Microsoft Fabric*. Source data: [MicrosoftLearning/dp-data](https://github.com/MicrosoftLearning/dp-data).
