# FreedomWings Fabric Spark Utils 🚀

Load Δ‑tables ⬇️ → Register Temp Views 🪄 → Query with Spark SQL 📊

A lightweight utility toolbox for FreedomWings teams working on Microsoft Fabric / Azure Lakehouse projects.create_spark_tables quickly reads Delta tables from your Lakehouse, registers them as temporary views, and lets you run any SQL script against them—all in a single call.

## 🧰 Features

One‑liner ingestion of one or many Delta tables into Spark.

Automatic temp‑view registration ✨—no manual createOrReplaceTempView calls.

Simple debugging prints for workspace, layer, and row counts.

Works across base / raw / enriched layers.

Returns a ready‑to‑use DataFrame so you can chain transformations or analytics right away.

## 🚀 Quick Start

Prerequisites

| Requirement | Min Version | Notes |
|-------------|------------|-------|
| Python      | 3.8+        | Tested on 3.10 |
| PySpark     | 3.4+        | Cluster / Fabric runtime should match |
| Access      | ✅          | Fabric workspace & Lakehouse + Delta tables |


##  Access

 ✅ Fabric workspace & Lakehouse + Delta tables

## Installation

In a Databricks or Fabric notebook cell
(No PyPI package yet—simply copy the .py file or git‑clone this repo)
!git clone https://github.com/your‑org/FW_fabric_spark_utils.git
%pip install pyspark  # if not already available in the runtime

## 🔧 Usage Example

``` from create_spark_tables import create_spark_tables

sql = """
SELECT user_id, created_at
FROM name_of_dbo
WHERE date >= '2025‑01‑01'
"""

df = create_spark_tables(
    workspace="name_of_workspace",
    lakehouse_layer="base",
    table_names=["your_table_name"],
    sql_script=sql,
)

df.show(5)
```

## 🛠️ Function Reference

create_spark_tables(workspace, lakehouse_layer, table_names, sql_script) → pyspark.sql.DataFrame

| Parameter       | Type        | Description                                                                 |
|----------------|-------------|-----------------------------------------------------------------------------|
| workspace       | str         | Fabric workspace name (e.g. `Financial_Lakhouse`)                     |
| lakehouse_layer | str         | Lakehouse layer: `base`, `raw`, or `enriched` (case‑insensitive)            |
| table_names     | list[str]   | One or more Delta table names in the layer                                 |
| sql_script      | str         | Any Spark SQL query that references the registered views                   |


Returns: A Spark DataFrame with the query result.

Raises: Propagates any exceptions that occur during table load or SQL execution.

## 🚨 Error Handling & Debugging

Each table load prints its row count and schema (optional) so you can verify data quickly.

Errors per table are logged but don’t stop the loop—handy when you’re loading many views.

A final count of the result set is printed after the SQL executes.

## 🤝 Contributing

Fork ➡️ clone ➡️ create a feature branch.

Follow PEP 8 and include docstrings & type hints.

Submit a pull request with a clear description & example.

## 📄 License

Distributed under the MIT License. See LICENSE for more information.

👋 Contact & Support

Made with ❤️ by FreedomWings. If you find a bug or have a feature request, please open an issue.

